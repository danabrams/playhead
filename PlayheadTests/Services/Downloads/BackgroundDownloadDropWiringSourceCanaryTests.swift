// BackgroundDownloadDropWiringSourceCanaryTests.swift
// playhead-7dgx — the six properties of this instrument that NO RUNTIME TEST
// CAN SEE, pinned in source.
//
// Each one is here because a unit test genuinely cannot reach it, not because
// a source check was easier:
//
//   1. THE LADDER REGISTRATION. A rung called from `runSchemaMigration` but not
//      from `migrateOnlyForTesting` leaves every fixture-driven test one rung
//      short while every `currentSchemaVersion` assertion still passes, because
//      the constant moved with it. It cost V60 a commit; the V58 canary is the
//      precedent this copies.
//   2. THE PRODUCTION WIRING. `PlayheadRuntime.init` is reachable from no unit
//      test in this tree, and the recorder it injects has a NO-OP default. That
//      exact shape has already shipped broken once, in the same actor:
//      `DownloadManager.workJournalRecorder` defaults to
//      `NoopWorkJournalRecorder` and production never replaces it, so every
//      `recordFailed` the download path makes goes nowhere. Nothing failed when
//      that happened, and nothing would fail here either.
//   3. WHICH BOUND EACH SITE RECORDS. `sessionCreationIO` is built by
//      `sessionIO.onItsOwnQueue(labelled:)`, which COPIES the timeout — so the
//      two bounds are numerically equal by construction and no assertion over
//      the recorded value can tell them apart. If they are ever given separate
//      deadlines, the source is the only place that says which site meant which.
//   4. THE PAIRING. Every path in `backgroundDownload` that DELETES the
//      attribution sidecar destroys the last trace of the request, so every one
//      of them owes a row. A runtime test proves the three paths that exist
//      today; only a source count notices a FOURTH one added later without one.
//   5. THE ORDER OF THE NEW SUSPENSION POINT. The row is written AFTER the
//      cleanup on every path. Both orders pass every behavioural rail — the
//      row lands either way — and they differ only in what a re-entrant caller
//      finds during the `await`, which is a property of the source text and of
//      nothing a test can observe from outside.
//   6. WHERE THE LEDGER IS ARMED. Arming must happen only after the store is
//      known open, and `bootstrap()` must not touch the ledger at all. A
//      runtime test sees the arming row either way; only the call SITE says
//      whether an unopened store was armed.
//
// XCTest rather than Swift Testing, matching every other source canary here:
// `xctestplan` can only filter XCTest classes, so a canary that might one day
// need excluding stays XCTest-shaped.

import Foundation
import XCTest
@testable import Playhead

final class BackgroundDownloadDropWiringSourceCanaryTests: XCTestCase {

    private static let storePath = "Playhead/Persistence/AnalysisStore/AnalysisStore.swift"
    private static let runtimePath = "Playhead/App/PlayheadRuntime.swift"
    private static let managerPath = "Playhead/Services/Downloads/DownloadManager.swift"

    /// Comments AND string literals stripped. Comments because this file's own
    /// prose names every symbol it checks; strings because the SQL and the
    /// `os_log` messages in `DownloadManager` name them too.
    private func code(_ repoRelativePath: String) throws -> String {
        SwiftSourceInspector.strippingCommentsAndStrings(
            try SwiftSourceInspector.loadSource(repoRelativePath: repoRelativePath)
        )
    }

    private func backgroundDownloadBody() throws -> String {
        let manager = try code(Self.managerPath)
        return try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: manager, after: "func backgroundDownload("),
            "could not isolate backgroundDownload's body — this canary's anchor has drifted"
        )
    }

    // MARK: - 1. The rung is REGISTERED, in both ladders and nowhere else

    func testV62IsRegisteredInBothLaddersExactlyOnceEach() throws {
        let store = try code(Self.storePath)
        let symbol = #"\bmigrateBackgroundDownloadDropsV62IfNeeded\b"#

        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: store), 3,
            "playhead-7dgx: the V62 rung must appear exactly three times in AnalysisStore.swift — "
            + "its declaration, the `runSchemaMigration` call and the `migrateOnlyForTesting` call. "
            + "Fewer means a ladder cannot reach it; more means a call site nobody enumerated."
        )

        let production = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: store, after: "private func runSchemaMigration() throws"),
            "could not isolate runSchemaMigration's body — the canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: production), 1,
            "playhead-7dgx: the production ladder must call V62 exactly once."
        )

        let testing = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: store, after: "func migrateOnlyForTesting() throws"),
            "could not isolate migrateOnlyForTesting's body — the canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: testing), 1,
            "playhead-7dgx: the ladder-only test seam must call V62 exactly once, or every "
            + "fixture-driven migration test silently stops one rung short."
        )
    }

    /// The DDL is written once and called from two places, rather than pasted
    /// into both. The house rule (V49) is that `createTables()` and the rung
    /// carry the same `CREATE TABLE`; two literal copies satisfy that on the
    /// day they are written and drift on the day one of them gains a column.
    func testTheDDLIsSharedRatherThanCopied() throws {
        let store = try code(Self.storePath)
        let symbol = #"\bcreateBackgroundDownloadDropTables\b"#
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: store), 3,
            "playhead-7dgx: the shared DDL helper must appear exactly three times — "
            + "its declaration, the V62 rung's call, and createTables()'s call."
        )
        let createTables = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: store, after: "private func createTables() throws"),
            "could not isolate createTables()'s body — the canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: createTables), 1,
            "playhead-7dgx: a fresh install must build the same shape an upgrade does."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"CREATE TABLE IF NOT EXISTS background_download_drops"#,
                in: try SwiftSourceInspector.strippingComments(
                    SwiftSourceInspector.loadSource(repoRelativePath: Self.storePath)
                )
            ),
            1,
            "playhead-7dgx: exactly ONE copy of the drops DDL. A second copy is the drift this "
            + "helper exists to prevent, and it would be invisible on the day it was added."
        )
    }

    // MARK: - 2. Production really wires the durable recorder

    func testProductionWiresTheStoreBackedDropRecorder() throws {
        let runtime = try code(Self.runtimePath)
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"dropRecorder:\s*AnalysisStoreBackgroundDownloadDropRecorder\("#,
                in: runtime
            ),
            1,
            "playhead-7dgx: PlayheadRuntime must construct DownloadManager with the "
            + "AnalysisStore-backed drop recorder. Without it the ledger's table exists, the "
            + "migration runs, every test passes, and the device records nothing — which is "
            + "exactly the state `workJournalRecorder` has been in since it was introduced."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"\bNoopBackgroundDownloadDropRecorder\b"#, in: runtime
            ),
            0,
            "playhead-7dgx: the no-op recorder belongs to tests and previews. Naming it in the "
            + "composition root is how an instrument becomes decorative."
        )
        // The recorder is passed to the INITIALISER, not installed afterwards.
        // A post-init hop is precisely what does not run on the sceneless
        // relaunch — the launch a dropped download matters most on.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"setDropRecorder|dropRecorder\s*="#, in: runtime
            ),
            0,
            "playhead-7dgx: inject at construction. A deferred setter would leave the recorder "
            + "nil exactly on the launches this ledger exists to observe."
        )
    }

    // MARK: - 3. Which bound each site records

    func testEachDropSiteRecordsTheBoundThatActuallyExpired() throws {
        let body = try backgroundDownloadBody()
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"boundSeconds:\s*sessionCreationIO\.timeout"#, in: body
            ),
            1,
            "playhead-7dgx: the session-refusal site must record the CREATION bound. "
            + "`sessionCreationIO` and `sessionIO` are separate queues with separate deadlines; "
            + "they merely share a timeout VALUE today because `onItsOwnQueue` copies it, so no "
            + "runtime assertion can tell a swap apart — this line is the only guard."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"boundSeconds:\s*sessionIO\.timeout"#, in: body
            ),
            2,
            "playhead-7dgx: the task-vending and resume sites both cross `sessionIO`, so both "
            + "record its bound."
        )
    }

    // MARK: - 4. Every path that destroys the evidence owes a row

    /// `deleteDownloadAttribution` is what makes a dropped download
    /// unattributable after the fact — it removes the only record of which show
    /// the transfer belonged to. So inside this one function the two calls are
    /// a matched pair, and a future fourth abandonment path that deletes the
    /// sidecar without recording a row would restore the silence this bead
    /// closed, in a branch no existing test covers.
    func testEveryAttributionDeletionInBackgroundDownloadOwesARow() throws {
        let body = try backgroundDownloadBody()
        let deletions = SwiftSourceInspector.regexOccurrences(
            of: #"\bdeleteDownloadAttribution\("#, in: body
        )
        let records = SwiftSourceInspector.regexOccurrences(
            of: #"\brecordBackgroundDownloadDrop\("#, in: body
        )
        XCTAssertEqual(
            deletions, 3,
            "playhead-7dgx: backgroundDownload has three abandonment paths. If this count moved, "
            + "read the diff before touching the number — a new path is a new silent drop."
        )
        XCTAssertEqual(
            records, deletions,
            "playhead-7dgx: every path that deletes the attribution sidecar destroys the last "
            + "trace of the request and therefore owes a durable row. Found \(deletions) "
            + "deletion(s) and \(records) record(s)."
        )
    }

    // MARK: - 5. The ORDER of the new suspension point

    /// THE SAFETY ARGUMENT FOR ADDING AN `await` TO `backgroundDownload` IS AN
    /// ORDERING, AND NOTHING AT RUNTIME CAN SEE IT.
    ///
    /// `recordBackgroundDownloadDrop` is a suspension point inside a function
    /// whose reservation region three beads exist to protect (playhead-nsjn,
    /// -gpdb, -7l6n). It is safe only because it sits AFTER the cleanup: by the
    /// time the actor is released, the in-flight slot is back and the sidecar
    /// is gone, so a re-entrant caller finds the episode retryable — exactly
    /// what the cleanup promised it. Hoist any of the three above its release
    /// and a second caller for the same episode can observe the episode still
    /// held, or a sidecar for a transfer that no longer exists — and every
    /// behavioural rail in this bead stays green, because the row is still
    /// written with the same contents.
    ///
    /// So the sequence is pinned literally. It is brittle on purpose: a diff
    /// that reorders these calls should have to say so.
    func testTheDropRowIsRecordedAFTERTheCleanupOnEveryPath() throws {
        let body = try backgroundDownloadBody()
        let pattern = #"\b(releaseInFlightReservationIfUnclaimed|abandonUnstartedTransfer|deleteDownloadAttribution|recordBackgroundDownloadDrop)\("#
        let regex = try NSRegularExpression(pattern: pattern)
        let range = NSRange(body.startIndex..<body.endIndex, in: body)
        let sequence = regex.matches(in: body, range: range).compactMap { match -> String? in
            guard let r = Range(match.range(at: 1), in: body) else { return nil }
            return String(body[r])
        }
        XCTAssertEqual(
            sequence,
            [
                // path A — the daemon would not vend a session
                "releaseInFlightReservationIfUnclaimed",
                "deleteDownloadAttribution",
                "recordBackgroundDownloadDrop",
                // path B — downloadTask(with:) went unanswered
                "releaseInFlightReservationIfUnclaimed",
                "deleteDownloadAttribution",
                "recordBackgroundDownloadDrop",
                // path C — created, never resumed
                "abandonUnstartedTransfer",
                "deleteDownloadAttribution",
                "recordBackgroundDownloadDrop",
            ],
            "playhead-7dgx: on every abandonment path the cleanup must complete BEFORE the "
            + "durable row's suspension point. This is the whole safety argument for adding an "
            + "`await` to this function and nothing at runtime can observe it."
        )
        // The sequence above is TEXTUAL, and `defer` is the one construct that
        // inverts execution order without moving a token: a
        // `defer { deleteDownloadAttribution(…) }` written above the record
        // call leaves this list identical while moving the deletion to after
        // the suspension point — precisely what the list exists to prevent.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\bdefer\b"#, in: body), 0,
            "playhead-7dgx: backgroundDownload must contain no `defer`. The ordering rail above "
            + "reads source order, and `defer` is how source order stops meaning execution order."
        )
    }

    /// The ledger records DAEMON DROPS. Nothing bounds the helper's call sites
    /// to `backgroundDownload`, and the neighbours are tempting: `cancelDownload`
    /// deletes the attribution sidecar too, and so do four delegate failure
    /// arms. A row minted from any of them would make `count(*)` a count of
    /// user cancels and network failures wearing a drop counter's name.
    func testTheDropRecorderIsCalledFromBackgroundDownloadAndNowhereElse() throws {
        let manager = try code(Self.managerPath)
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"\brecordBackgroundDownloadDrop\("#, in: manager
            ),
            4,
            "playhead-7dgx: exactly four — the private helper's declaration and its three call "
            + "sites, all inside backgroundDownload. A fifth means some other path is minting "
            + "rows into a table whose name says it counts daemon drops."
        )
    }

    // MARK: - 6. Where the ledger is ARMED

    /// `armedLaunches` is the denominator that lets zero drop rows be read as a
    /// positive claim, and its whole meaning is the POSITION of one call.
    ///
    /// Above `openAtLaunch`'s guard it would count degraded launches, where
    /// analysis is off and no row could ever be written — so a run of
    /// unopenable launches would read as evidence that no download was dropped.
    /// `PlayheadRuntime.init` is reachable from no unit test, so this ordering
    /// is checkable nowhere else.
    func testTheLedgerIsArmedOnlyAfterTheStoreIsKnownOpen() throws {
        let runtime = try code(Self.runtimePath)
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\barmDropLedger\(\)"#, in: runtime), 1,
            "playhead-7dgx: exactly one production arming site."
        )
        let openIndex = try XCTUnwrap(
            runtime.range(of: "analysisStoreRecovery.openAtLaunch(")?.lowerBound,
            "could not find the launch open — this canary's anchor has drifted"
        )
        let guardIndex = try XCTUnwrap(
            runtime.range(of: "guard storeOutcome.isOpen else", range: openIndex..<runtime.endIndex)?.upperBound,
            "could not find the degraded-launch guard — this canary's anchor has drifted"
        )
        let armIndex = try XCTUnwrap(
            runtime.range(of: "armDropLedger()")?.lowerBound,
            "could not find the arming call"
        )
        XCTAssertTrue(
            armIndex > guardIndex,
            "playhead-7dgx: the ledger must be armed only AFTER the degraded-launch guard. "
            + "Armed above it, `armedLaunches` counts launches on which the store never opened, "
            + "and the ledger's central claim becomes unreadable."
        )
        // AND OUTSIDE THE GUARD'S OWN BODY. "After the guard header" alone is
        // satisfied by `guard … else { await downloadManager.armDropLedger(); return }`
        // — arming ONLY the degraded launches, the exact inversion of what the
        // assertion above says it prevents.
        let elseBody = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: runtime, after: "guard storeOutcome.isOpen else"),
            "could not isolate the degraded-launch guard's body"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\barmDropLedger\b"#, in: elseBody), 0,
            "playhead-7dgx: the arming must be on the SUCCESS path, not inside the guard's else."
        )
    }

    /// And it is NOT armed from `DownloadManager.bootstrap()`, where the first
    /// cut of this bead put it. That made `bootstrap()` `async` and put its
    /// cache-directory creation behind a full `AnalysisStore` open, and it made
    /// `DownloadManager` an unmanaged opener of `analysis.sqlite` racing
    /// `AnalysisStoreRecoveryCoordinator`.
    func testBootstrapDoesNotTouchTheDropLedger() throws {
        let manager = try code(Self.managerPath)
        let body = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: manager, after: "func bootstrap() throws"),
            "bootstrap() must stay SYNCHRONOUS — if this anchor no longer matches, the arming "
            + "hop has probably come back"
        )
        // THREE SPELLINGS, because banning only `dropRecorder` is out-spelled
        // by the idiomatic re-introduction: `bootstrap()` is synchronous, so a
        // contributor cannot write `await dropRecorder…` there — but
        // `Task { await armDropLedger() }` compiles, restores the exact hop
        // this canary forbids, and mentions neither `dropRecorder` nor `await`.
        for symbol in [#"\bdropRecorder\b"#, #"\barmDropLedger\b"#, #"\brecordInstrumentArmed\b"#] {
            XCTAssertEqual(
                SwiftSourceInspector.regexOccurrences(of: symbol, in: body), 0,
                "playhead-7dgx: bootstrap() must not reach the drop ledger by any route (\(symbol))."
            )
        }
    }

    /// The three reasons are distinct at the three sites. A copy-paste that
    /// left two sites reporting the same reason would pass every count above
    /// and quietly merge two populations with unrelated remedies.
    func testTheThreeDropSitesUseThreeDistinctReasons() throws {
        let body = try backgroundDownloadBody()
        for reason in BackgroundDownloadDropReason.allCases {
            XCTAssertEqual(
                SwiftSourceInspector.regexOccurrences(
                    of: #"reason:\s*\."# + String(describing: reason) + #"\b"#, in: body
                ),
                1,
                "playhead-7dgx: `\(reason)` must be recorded at exactly one site in "
                + "backgroundDownload."
            )
        }
        XCTAssertEqual(
            BackgroundDownloadDropReason.allCases.count, 3,
            "playhead-7dgx: three reasons, three sites. Adding a case without a site — or a site "
            + "without a case — makes one of the two counts above a lie."
        )
    }
}
