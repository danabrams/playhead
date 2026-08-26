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
//      `DownloadManager.workJournalRecorder` defaulted to
//      `NoopWorkJournalRecorder` and production never replaced it, so every
//      `recordFailed` the download path made went nowhere, for four months.
//      Nothing failed when that happened, and nothing would fail here either.
//      (That one is playhead-4xmz and is fixed; `DownloadWorkJournalWiringSourceCanaryTests`
//      is this file's twin for it. The past tense is deliberate — it is the
//      shipped instance this argument rests on, so it is kept rather than
//      deleted.)
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
// FIVE MORE AT V64 (playhead-sdis), same rule, spelled 7a-7e beside the
// section that carries them:
//
//   7a. the V64 rung is registered in BOTH ladders;
//   7b. production constructs exactly ONE `DownloadManager` and passes it no
//       `launchId:` — the two halves of the one property that lets the column
//       be called a LAUNCH identity at all;
//   7c. every drop site STATES its crossing rather than inheriting a default;
//   7d. only `armDropLedger()` moves the arming state, so the column really
//       does report what this process knew about its own arming;
//   7e. every V64 column owes an `addColumnIfNeeded`, because a column
//       declared only in the DDL BRICKS a store the previous build created.
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

    /// The DDL is written once and called from every rung that changes the
    /// shape, rather than pasted into each. The house rule (V49) is that
    /// `createTables()` and the rung carry the same `CREATE TABLE`; two literal
    /// copies satisfy that on the day they are written and drift on the day one
    /// of them gains a column.
    ///
    /// THE COUNT IS ONE PER RUNG PLUS TWO, and it moved at V64 (playhead-sdis).
    /// The V64 rung adds four columns to these two tables and therefore calls
    /// the SAME helper — which is the correct design and is what
    /// `testEveryV64ColumnIsBothDeclaredAndRepaired` below rests on — so the
    /// occurrence count went 3 -> 4 and this rail went RED. That is the rail
    /// working: a rung that changed the shape without touching the shared
    /// helper would have left the count at 3 and said nothing. Any future rung
    /// over these tables owes the same +1, and a rung that does NOT need one is
    /// a rung that did not change the shape.
    func testTheDDLIsSharedRatherThanCopied() throws {
        let store = try code(Self.storePath)
        let symbol = #"\bcreateBackgroundDownloadDropTables\b"#
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: store), 4,
            "playhead-7dgx/playhead-sdis: the shared DDL helper must appear exactly four times "
            + "— its declaration, the V62 rung's call, the V64 rung's call, and createTables()'s "
            + "call. FEWER means a rung that changes the shape pasted its own copy of the DDL, "
            + "which is the drift this helper exists to prevent; MORE means a call site nobody "
            + "enumerated."
        )
        // …and each of the two RUNGS calls it exactly once, which the total
        // alone cannot show: a V64 rung that called it twice while the V62 rung
        // called it not at all would still total four.
        for (rung, bead) in [
            ("private func migrateBackgroundDownloadDropsV62IfNeeded() throws", "playhead-7dgx"),
            (
                "private func migrateBackgroundDownloadDropLaunchIdentityV64IfNeeded() throws",
                "playhead-sdis"
            ),
        ] {
            let body = try XCTUnwrap(
                SwiftSourceInspector.firstBody(in: store, after: rung),
                "could not isolate \(rung) — this canary's anchor has drifted"
            )
            XCTAssertEqual(
                SwiftSourceInspector.regexOccurrences(of: symbol, in: body), 1,
                "\(bead): the rung must build the shape through the SHARED helper, exactly once."
            )
        }
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
            + "exactly the state `workJournalRecorder` was in for four months (playhead-4xmz)."
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

    // MARK: - 7. The identities (playhead-sdis)
    //
    // Three more properties no runtime test can see, and every one of them is
    // a claim the shipped source already MAKES in a doc comment. A doc comment
    // that names a canary which does not exist is the standing defect class
    // wearing a citation, so the three are pinned here rather than asserted in
    // prose:
    //
    //   7a. THE V64 RUNG IS IN BOTH LADDERS. Same argument as V62 above, and
    //       the V64 comment in `AnalysisStore.swift` says outright that this
    //       file "checks this pairing mechanically".
    //   7b. PRODUCTION MINTS ONE LAUNCH ID AND NAMES NONE OF ITS OWN. The
    //       `launchId` doc comment claims both halves for this canary. A
    //       `static` slot, or a literal passed at the composition root, would
    //       make every launch on every device read as ONE launch under
    //       `count(DISTINCT launchId)` — the exact collapse the column exists
    //       to prevent, and a runtime test cannot see it because one process
    //       constructs one manager and any spelling reads the same from inside.
    //   7c. EVERY DROP SITE STATES ITS CROSSING. `sessionCrossingId` has no
    //       default, so a fourth abandonment path has to decide; the source is
    //       the only place that can say all three existing sites decided.

    /// 7a. Exactly like V62, and for the reason V60 cost a commit: a rung
    /// called from `runSchemaMigration` but not from `migrateOnlyForTesting`
    /// leaves every fixture-driven test one rung short while every
    /// `currentSchemaVersion` assertion still passes, because the constant
    /// moved with it.
    func testV64IsRegisteredInBothLaddersExactlyOnceEach() throws {
        let store = try code(Self.storePath)
        let symbol = #"\bmigrateBackgroundDownloadDropLaunchIdentityV64IfNeeded\b"#

        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: store), 3,
            "playhead-sdis: the V64 rung must appear exactly three times in AnalysisStore.swift — "
            + "its declaration, the `runSchemaMigration` call and the `migrateOnlyForTesting` "
            + "call. Fewer means a ladder cannot reach it; more means a call site nobody "
            + "enumerated."
        )

        let production = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: store, after: "private func runSchemaMigration() throws"),
            "could not isolate runSchemaMigration's body — the canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: production), 1,
            "playhead-sdis: the production ladder must call V64 exactly once."
        )

        let testing = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: store, after: "func migrateOnlyForTesting() throws"),
            "could not isolate migrateOnlyForTesting's body — the canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: testing), 1,
            "playhead-sdis: the ladder-only test seam must call V64 exactly once, or every "
            + "fixture-driven migration test silently stops one rung short — which is exactly "
            + "how a rung that adds COLUMNS goes missing, since the fresh-install path builds "
            + "them anyway through createTables() and nothing looks wrong."
        )
    }

    /// 7b, first half: ONE `DownloadManager` in the whole app target.
    ///
    /// `launchId` is called a launch id because in production it is one, and
    /// that is a property of the COMPOSITION ROOT rather than of the manager.
    /// A second construction site would make one process two "launches",
    /// double-count `armedLaunches`, and leave `count(DISTINCT launchId)` a
    /// count of managers wearing a launch counter's name.
    func testTheAppConstructsExactlyOneDownloadManager() throws {
        let sources = try Self.productionSources()
        var sites: [String] = []
        for (path, text) in sources {
            // The manager's own file declares the type; `DownloadManager(` there
            // would be a self-construction and there is none, but excluding it
            // keeps the count about the COMPOSITION ROOT.
            if path.hasSuffix("/Services/Downloads/DownloadManager.swift") { continue }
            let hits = SwiftSourceInspector.regexOccurrences(
                of: #"\bDownloadManager\("#, in: text
            )
            sites.append(contentsOf: Array(repeating: path, count: hits))
        }
        XCTAssertEqual(
            sites.count, 1,
            "playhead-sdis: exactly one `DownloadManager(...)` in Playhead/**, and it is "
            + "PlayheadRuntime's. Found: \(sites.sorted()). `launchId` is documented as a LAUNCH "
            + "identity on the strength of this; a second site makes one process two launches."
        )
        XCTAssertTrue(
            sites.first?.hasSuffix("/App/PlayheadRuntime.swift") == true,
            "playhead-sdis: the one construction site must be the composition root, not "
            + "\(sites.first ?? "nowhere")."
        )
    }

    /// 7b, second half: production passes NO `launchId:`, and the slot is not
    /// process-wide.
    ///
    /// Both halves are the SAME failure with two spellings — every launch
    /// reading as one launch — and neither is observable from inside a process
    /// that only ever has one. A `static let launchId = UUID().uuidString`
    /// looks per-launch and is per-PROCESS-IMAGE only by accident of when the
    /// static is initialised; worse, it would be shared by a second manager,
    /// which is precisely the case the doc comment says must break
    /// `count(DISTINCT launchId) <= armedLaunches` rather than hide.
    func testProductionNamesNoLaunchIdAndTheSlotIsPerInstance() throws {
        let runtime = try code(Self.runtimePath)
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\blaunchId\s*:"#, in: runtime), 0,
            "playhead-sdis: the composition root must NOT pass a launchId. A literal there is a "
            + "constant on every device, and `count(DISTINCT launchId)` would read every launch "
            + "ever as one."
        )

        let manager = try code(Self.managerPath)
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"\binternal\s+let\s+launchId\s*:\s*String\b"#, in: manager
            ),
            1,
            "playhead-sdis: `launchId` is one per INSTANCE — an immutable stored property, not a "
            + "computed one that could mint a fresh id per read."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"\bstatic\s+(let|var)\s+launchId\b"#, in: manager
            ),
            0,
            "playhead-sdis: NOT a static. A process-wide slot is shared by a second manager, and "
            + "two independent recorders arming the ledger twice under one id is the one state "
            + "`count(DISTINCT launchId) <= armedLaunches` must be allowed to break on."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"launchId\s*:\s*String\s*=\s*UUID\(\)\.uuidString"#, in: manager
            ),
            1,
            "playhead-sdis: the DEFAULT is a fresh UUID, so a caller that says nothing gets a "
            + "distinct launch. The parameter exists only so a test can pin an exact value."
        )
    }

    /// 7c. Every drop site STATES its crossing, and the helper supplies no
    /// default to inherit.
    ///
    /// Only `sessionNotVended` rides a crossing other callers can join, so the
    /// other two sites pass `nil` — and they pass it out loud. A defaulted
    /// `nil` would let a future joinable refusal be recorded as an isolated
    /// one, and every rail in this bead would stay green: the column would be
    /// NULL, which on a post-V64 row is a STATEMENT ("this reason rides no
    /// crossing") rather than a gap, so nothing downstream could tell the
    /// forgotten case from the deliberate one.
    func testEveryDropSiteStatesItsCrossingAndTheHelperHasNoDefault() throws {
        let manager = try code(Self.managerPath)
        // The SIGNATURE, sliced out by hand: `firstBody(in:after:)` returns
        // what is between the braces, and a default lives in the parameter
        // list. Everything from the declaration to the opening brace.
        let declaration = "func recordBackgroundDownloadDrop("
        let start = try XCTUnwrap(
            manager.range(of: declaration)?.lowerBound,
            "could not find recordBackgroundDownloadDrop — this canary's anchor has drifted"
        )
        let signature = try XCTUnwrap(
            manager[start...].range(of: ") async {").map { String(manager[start..<$0.upperBound]) },
            "could not isolate recordBackgroundDownloadDrop's parameter list"
        )
        XCTAssertTrue(
            signature.contains("sessionCrossingId: String?"),
            "playhead-sdis: the helper must TAKE a crossing id; without the parameter the "
            + "session-refusal site has nowhere to put the one thing that separates one daemon "
            + "refusal from forty. Signature was:\n\(signature)"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"sessionCrossingId\s*:\s*String\?\s*="#, in: signature
            ),
            0,
            "playhead-sdis: `sessionCrossingId` must be declared WITHOUT a default. `= nil` "
            + "there is how a fourth abandonment path inherits a claim instead of making one — "
            + "and on a post-V64 row NULL is a STATEMENT (\"this reason rides no crossing\"), so "
            + "nothing downstream could tell the inherited nil from the decided one. "
            + "Signature was:\n\(signature)"
        )

        let body = try backgroundDownloadBody()
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"sessionCrossingId\s*:\s*sessionCrossingId\b"#, in: body
            ),
            1,
            "playhead-sdis: exactly ONE site records a real crossing id — the session refusal. "
            + "Two would mean some other reason is claiming a joinable crossing it never rode."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"sessionCrossingId\s*:\s*nil\b"#, in: body
            ),
            2,
            "playhead-sdis: the other two sites must pass nil EXPLICITLY. One + two = the three "
            + "sites section 4 counts, so a fourth site added without a decision fails here "
            + "rather than defaulting into the wrong population."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"\bbackgroundSessionRidingCrossing\("#, in: body
            ),
            1,
            "playhead-sdis: the drop-writing path must take the CROSSING-CARRYING spelling of "
            + "the session lookup. The plain `backgroundSession(for:requestedBy:)` returns the "
            + "session and discards the identity, so a site that used it would record NULL for "
            + "a refusal that really did ride a crossing — indistinguishable, on disk, from a "
            + "reason that rides none."
        )
    }

    // MARK: - 7d. One writer for the arming state

    /// `launchArmingState` claims to be "what this process knew about its own
    /// arming". That is only true while `armDropLedger()` is the sole writer:
    /// a second assignment anywhere would make the column report something
    /// else, and every assertion over the VALUE would still pass.
    func testOnlyArmDropLedgerMovesTheArmingState() throws {
        let manager = try code(Self.managerPath)
        let assignment = #"dropLedgerArming\s*="#
        let total = SwiftSourceInspector.regexOccurrences(of: assignment, in: manager)
        let armBody = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: manager, after: "func armDropLedger() async"),
            "could not isolate armDropLedger's body — this canary's anchor has drifted"
        )
        let inArm = SwiftSourceInspector.regexOccurrences(of: assignment, in: armBody)
        XCTAssertEqual(
            inArm, 3,
            "playhead-sdis: armDropLedger must set the state on all THREE outcomes. A missing arm "
            + "leaves a launch reporting the previous value, and `.landed` is as much a fact worth "
            + "stamping as `.writeFailed` is."
        )
        XCTAssertEqual(
            total, inArm,
            "playhead-sdis: nothing outside `armDropLedger()` may assign the arming state. Found "
            + "\(total) assignment(s) in the file and \(inArm) inside the arming method."
        )
        // And the drop helper READS it rather than taking it as a parameter —
        // a parameter would let a caller state an arming this process never had.
        let recordBody = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: manager, after: "private func recordBackgroundDownloadDrop("
            ),
            "could not isolate recordBackgroundDownloadDrop's body"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"launchArmingState:\s*dropLedgerArming\b"#, in: recordBody
            ),
            1,
            "playhead-sdis: the row's arming state must be READ from the actor at write time."
        )
    }

    // MARK: - 7e. Every V64 column owes an `addColumnIfNeeded`

    /// THE BRICKING OBLIGATION, and it is a matched pair rather than a count.
    ///
    /// `CREATE TABLE IF NOT EXISTS` is a NO-OP against a table that already
    /// exists in an older shape, so a column that appears only in the DDL is
    /// missing on every store the previous build created — and the seed
    /// `INSERT` beside it then names a column that is not there, `createTables()`
    /// throws, and THE STORE STOPS OPENING. That is measured, not hypothetical:
    /// it happened at V62 with `dropWriteFailures` and surfaced as an unrelated
    /// trust-profile test failing.
    ///
    /// A runtime rail covers the four columns that exist today. This is what
    /// covers the fifth.
    func testEveryV64ColumnIsBothDeclaredAndRepaired() throws {
        // Comments stripped, STRINGS KEPT — the DDL lives in a string literal,
        // so `code()` would delete the very text this checks.
        let store = try SwiftSourceInspector.strippingComments(
            SwiftSourceInspector.loadSource(repoRelativePath: Self.storePath)
        )
        let helper = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: store, after: "private func createBackgroundDownloadDropTables() throws"
            ),
            "could not isolate the shared DDL helper — this canary's anchor has drifted"
        )
        let repairs = try XCTUnwrap(
            helper.range(of: "addColumnIfNeeded"),
            "playhead-sdis: the shared DDL helper must carry the shape repair at all"
        )
        let declarationRegion = String(helper[helper.startIndex..<repairs.lowerBound])
        let repairRegion = String(helper[repairs.lowerBound...])

        for column in ["launchId", "sessionCrossingId", "launchArmingState", "lastArmedLaunchId"] {
            XCTAssertGreaterThanOrEqual(
                SwiftSourceInspector.regexOccurrences(
                    of: #"\b"# + column + #"\b"#, in: declarationRegion
                ),
                1,
                "playhead-sdis: \(column) must be declared in the CREATE TABLE a fresh install builds."
            )
            XCTAssertGreaterThanOrEqual(
                SwiftSourceInspector.regexOccurrences(
                    of: #"\b"# + column + #"\b"#, in: repairRegion
                ),
                1,
                "playhead-sdis: \(column) must ALSO be re-added with `addColumnIfNeeded`. A column "
                + "declared only in the DDL is missing on every store the previous build created, "
                + "and the seed INSERT below then bricks the OPEN — measured at V62."
            )
        }
    }

    /// Every `Playhead/**` Swift source, comments and string contents stripped.
    /// Cached, on `DownloadWorkJournalWiringSourceCanaryTests`' precedent: the
    /// tree is ~480 files and re-reading it per assertion is the difference
    /// between a rail people run and one they route around.
    private static let productionSourceCache = ProductionSources()

    private final class ProductionSources: @unchecked Sendable {
        private let lock = NSLock()
        private var loaded: [String: String]?

        func load() throws -> [String: String] {
            lock.lock()
            defer { lock.unlock() }
            if let loaded { return loaded }
            guard let root = SwiftSourceInspector.repositoryRoot(from: #filePath) else {
                throw NSError(
                    domain: "BackgroundDownloadDropWiringSourceCanary",
                    code: 1,
                    userInfo: [
                        NSLocalizedDescriptionKey:
                            "could not locate the repository root from \(#filePath)",
                    ]
                )
            }
            let appRoot = root.appendingPathComponent("Playhead", isDirectory: true)
            var out: [String: String] = [:]
            let walker = FileManager.default.enumerator(
                at: appRoot, includingPropertiesForKeys: nil
            )
            while let url = walker?.nextObject() as? URL {
                guard url.pathExtension == "swift" else { continue }
                let text = try String(contentsOf: url, encoding: .utf8)
                out[url.path] = SwiftSourceInspector.strippingCommentsAndStrings(text)
            }
            loaded = out
            return out
        }
    }

    private static func productionSources() throws -> [String: String] {
        try productionSourceCache.load()
    }
}
