// DownloadWorkJournalWiringSourceCanaryTests.swift
// playhead-4xmz — the properties of this instrument that NO RUNTIME TEST CAN
// SEE, pinned in source.
//
// THE BEAD IS A WIRING DEFECT, SO THE WIRING IS THE THING TO PIN.
// `DownloadManager.workJournalRecorder` had a NO-OP default and
// `PlayheadRuntime` never replaced it — for four months, across every gate,
// with nothing red. `PlayheadRuntime.init` is reachable from no unit test in
// this tree, so the only way that state can be made to fail is here. Ask the
// question this bead exists to ask: **could the fix revert to a no-op without
// anything failing?** Without this file, yes — silently, in one line of a
// refactor.
//
// Six properties, and each is here because a behavioural test genuinely cannot
// reach it:
//
//   1. THE LADDER REGISTRATION. A rung called from `runSchemaMigration` but not
//      from `migrateOnlyForTesting` leaves every fixture-driven test one rung
//      short while every `currentSchemaVersion` assertion still passes, because
//      the constant moved with it. It cost V60 a commit; V58 and V62 are the
//      precedents this copies.
//   2. THE PRODUCTION WIRING ITSELF — that `DownloadManager` is constructed
//      with a store-backed recorder, and that the no-op is not named in the
//      composition root at all.
//   3. THAT IT IS INJECTED AT CONSTRUCTION rather than through a post-init
//      setter. Both spellings pass every behavioural rail; only one of them
//      runs on the sceneless relaunch these records exist for.
//   4. THAT THE ARMED INSTANCE IS THE INJECTED INSTANCE. A second recorder
//      constructed at the arming site would count launches for a recorder
//      nobody installed — an `armedLaunches` that names one thing and measures
//      another, which is the exact shape this bead removes.
//   5. WHERE THE ARMING SITS relative to the degraded-launch guard. Armed above
//      it, the denominator counts launches on which the store never opened, and
//      a run of those reads as evidence that no download ever failed.
//   6. THAT THE DOWNLOAD PATH DOES NOT WRITE `work_journal`. That table's
//      `event_type` is an input to `AnalysisCoordinator.recoverOrphans`, so a
//      download failure written under an analysis generation would tell
//      cold-launch recovery the ANALYSIS work is terminal. Nothing at runtime
//      distinguishes "wrote to the right table" from "wrote to the wrong one"
//      without a fixture that has both a live analysis lease and a failing
//      download, and the source is where the decision actually lives.

import Foundation
import XCTest
@testable import Playhead

final class DownloadWorkJournalWiringSourceCanaryTests: XCTestCase {

    private static let storePath = "Playhead/Persistence/AnalysisStore/AnalysisStore.swift"
    private static let runtimePath = "Playhead/App/PlayheadRuntime.swift"
    private static let managerPath = "Playhead/Services/Downloads/DownloadManager.swift"
    private static let ledgerPath = "Playhead/Services/Downloads/DownloadWorkJournalLedger.swift"

    private func code(_ repoRelativePath: String) throws -> String {
        SwiftSourceInspector.strippingCommentsAndStrings(
            try SwiftSourceInspector.loadSource(repoRelativePath: repoRelativePath)
        )
    }

    // MARK: - 1. The ladder

    func testV63IsRegisteredInBothLaddersExactlyOnceEach() throws {
        let store = try code(Self.storePath)
        let symbol = #"\bmigrateDownloadWorkJournalV63IfNeeded\b"#

        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: store), 3,
            "playhead-4xmz: the V63 rung must appear exactly three times in AnalysisStore.swift — "
            + "its declaration, the `runSchemaMigration` call and the `migrateOnlyForTesting` call. "
            + "Fewer means a ladder cannot reach it; more means a call site nobody enumerated."
        )

        let production = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: store, after: "private func runSchemaMigration() throws"),
            "could not isolate runSchemaMigration's body — the canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: production), 1,
            "playhead-4xmz: the production ladder must call V63 exactly once."
        )

        let testing = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: store, after: "func migrateOnlyForTesting() throws"),
            "could not isolate migrateOnlyForTesting's body — the canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: testing), 1,
            "playhead-4xmz: the ladder-only test seam must call V63 exactly once, or every "
            + "fixture-driven migration test silently stops one rung short."
        )
    }

    func testTheDDLIsSharedRatherThanCopied() throws {
        let store = try code(Self.storePath)
        let symbol = #"\bcreateDownloadWorkJournalTables\b"#
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: store), 3,
            "playhead-4xmz: the shared DDL helper must appear exactly three times — "
            + "its declaration, the V63 rung's call, and createTables()'s call."
        )
        let createTables = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: store, after: "private func createTables() throws"),
            "could not isolate createTables()'s body — the canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: createTables), 1,
            "playhead-4xmz: a fresh install must build the same shape an upgrade does."
        )
        let withStringsIntact = try SwiftSourceInspector.strippingComments(
            SwiftSourceInspector.loadSource(repoRelativePath: Self.storePath)
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"CREATE TABLE IF NOT EXISTS download_work_journal\b"#,
                in: withStringsIntact
            ),
            1,
            "playhead-4xmz: exactly ONE copy of the journal DDL. A second copy is the drift this "
            + "helper exists to prevent, and it would be invisible on the day it was added."
        )
    }

    // MARK: - 2 & 3. The wiring, at construction

    func testProductionWiresTheStoreBackedWorkJournalRecorder() throws {
        let runtime = try code(Self.runtimePath)
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"AnalysisStoreDownloadWorkJournalRecorder\("#, in: runtime
            ),
            1,
            "playhead-4xmz: PlayheadRuntime must construct exactly one store-backed download "
            + "work-journal recorder. Zero is the four-month defect this bead fixes — the table "
            + "exists, the migration runs, every test passes, and the device records nothing. "
            + "Two means the instance that gets ARMED may not be the instance that gets INJECTED."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"workJournalRecorder:\s*downloadWorkJournalRecorder\b"#, in: runtime
            ),
            1,
            "playhead-4xmz: the recorder must be passed to DownloadManager as its "
            + "`workJournalRecorder`, by the same identifier that is armed below. Nothing at "
            + "runtime can see that those two are the same object."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"\bNoopWorkJournalRecorder\b"#, in: runtime
            ),
            0,
            "playhead-4xmz: the no-op recorder belongs to tests and previews. Naming it in the "
            + "composition root is how an instrument becomes decorative — which is precisely "
            + "what the DEFAULT did here, silently, without ever being named at all."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"setWorkJournalRecorder|workJournalRecorder\s*="#, in: runtime
            ),
            0,
            "playhead-4xmz: inject at construction. A deferred setter would leave the recorder "
            + "no-op exactly on the sceneless relaunches this journal exists to observe."
        )
    }

    /// The property above is a claim about `PlayheadRuntime`; this one is what
    /// makes a post-init setter UNAVAILABLE rather than merely unused. A `var`
    /// is one line away from being assigned by a future refactor, and that
    /// assignment would satisfy every behavioural rail in the tree.
    func testTheRecorderSlotCannotBeReassignedAfterInit() throws {
        let manager = try code(Self.managerPath)
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"internal let workJournalRecorder: WorkJournalRecording"#, in: manager
            ),
            1,
            "playhead-4xmz: the slot must be a `let`. As a `var` it is one line away from a "
            + "post-init setter, which is the shape that does not run on a sceneless launch."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"var workJournalRecorder\b"#, in: manager
            ),
            0,
            "playhead-4xmz: no mutable spelling of the slot."
        )
    }

    // MARK: - 4 & 5. The arming

    func testTheJournalIsArmedOnceOnTheInjectedInstanceAfterTheStoreIsKnownOpen() throws {
        let runtime = try code(Self.runtimePath)
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"downloadWorkJournalRecorder\.recordInstrumentArmed\("#, in: runtime
            ),
            1,
            "playhead-4xmz: exactly one production arming site, and it must go through the "
            + "STORED recorder — the one that was injected. Arming a freshly-constructed second "
            + "recorder would count launches for an instrument nobody installed."
        )
        let openIndex = try XCTUnwrap(
            runtime.range(of: "analysisStoreRecovery.openAtLaunch(")?.lowerBound,
            "could not find the launch open — this canary's anchor has drifted"
        )
        let guardIndex = try XCTUnwrap(
            runtime.range(
                of: "guard storeOutcome.isOpen else", range: openIndex..<runtime.endIndex
            )?.upperBound,
            "could not find the degraded-launch guard — this canary's anchor has drifted"
        )
        let armIndex = try XCTUnwrap(
            runtime.range(of: "downloadWorkJournalRecorder.recordInstrumentArmed(")?.lowerBound,
            "could not find the arming call"
        )
        XCTAssertTrue(
            armIndex > guardIndex,
            "playhead-4xmz: the journal must be armed only AFTER the degraded-launch guard. "
            + "Armed above it, `armedLaunches` counts launches on which the store never opened, "
            + "and zero rows beside a healthy-looking denominator becomes unreadable."
        )
        let elseBody = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: runtime, after: "guard storeOutcome.isOpen else"),
            "could not isolate the degraded-launch guard's body"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"\brecordInstrumentArmed\b"#, in: elseBody
            ),
            0,
            "playhead-4xmz: the arming must be on the SUCCESS path, not inside the guard's else."
        )
    }

    // MARK: - 6. Not `work_journal`

    /// The reason this bead is not a one-line fix, pinned where the decision
    /// lives.
    ///
    /// `AnalysisStoreWorkJournalRecorder` writes `work_journal`, whose
    /// `event_type` `AnalysisCoordinator.recoverOrphans` ROUTES ON:
    /// `.failed`/`.finalized` mean `terminalNoRequeue`. Wiring it here would
    /// make a DOWNLOAD failure clear an ANALYSIS lease with no requeue. It is
    /// also the change a future reader is most likely to make, because it looks
    /// like tidying two recorders into one.
    func testTheDownloadPathDoesNotWriteTheAnalysisWorkJournal() throws {
        let runtime = try code(Self.runtimePath)
        // The ANALYSIS recorder is legitimately constructed in this file — it
        // is what `AnalysisWorkScheduler` gets — so its mere presence proves
        // nothing. What must never exist is the ARGUMENT.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"workJournalRecorder:\s*AnalysisStoreWorkJournalRecorder"#, in: runtime
            ),
            0,
            "playhead-4xmz: the download path must NOT be given the ANALYSIS work-journal "
            + "recorder. `work_journal.event_type` is a cold-launch input to "
            + "`AnalysisCoordinator.recoverOrphans`, which routes `.failed`/`.finalized` to "
            + "terminalNoRequeue — so a download failure written under an analysis generation "
            + "tells recovery the ANALYSIS work is over. That is playhead-rqgr's defect from a "
            + "new writer, and it is what makes the obvious one-line fix worse than the bug. "
            + "The INDIRECT route (binding the analysis recorder to the identifier passed "
            + "below) is closed by the compiler instead: the stored property is annotated "
            + "`AnalysisStoreDownloadWorkJournalRecorder`, so the substitution does not type."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"let downloadWorkJournalRecorder = AnalysisStoreDownloadWorkJournalRecorder\("#,
                in: runtime
            ),
            1,
            "playhead-4xmz: the identifier passed as `workJournalRecorder:` must be bound to "
            + "the store-backed DOWNLOAD recorder, at exactly one place."
        )
        let ledger = try code(Self.ledgerPath)
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\bWorkJournalEntry\b"#, in: ledger),
            0,
            "playhead-4xmz: the download ledger must not construct a `WorkJournalEntry`. That "
            + "type is the `work_journal` row, and its `EventType`'s defining property is "
            + "`orphanRecoveryRouting`, which is false of every row this table holds."
        )
        let ledgerWithStrings = try SwiftSourceInspector.strippingComments(
            SwiftSourceInspector.loadSource(repoRelativePath: Self.ledgerPath)
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\bwork_journal\b"#, in: ledgerWithStrings),
            0,
            "playhead-4xmz: the download ledger must name no SQL against `work_journal` itself. "
            + "`download_work_journal` and `download_work_journal_arming` do not match — the "
            + "leading `\\b` fails against the `_` before `work`, and the trailing one against "
            + "the `_` after `journal` — which is why this pattern is spelled with boundaries "
            + "rather than as a bare substring."
        )
    }

    /// Every `WorkJournalRecording` method the download path calls must reach
    /// the store, and the count is what notices a method quietly turned back
    /// into `{}` — the exact defect, one layer in rather than at the wiring.
    func testEveryProtocolMethodOnTheStoreBackedRecorderAppends() throws {
        let ledger = try code(Self.ledgerPath)
        let recorderBody = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: ledger,
                after: "struct AnalysisStoreDownloadWorkJournalRecorder: WorkJournalRecording"
            ),
            "could not isolate the recorder — this canary's anchor has drifted"
        )
        // Four protocol requirements plus `append`'s own declaration.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\bawait append\("#, in: recorderBody), 4,
            "playhead-4xmz: all FOUR `WorkJournalRecording` requirements must append. A body "
            + "that quietly became `{}` is this bead's defect one layer in, and it would pass "
            + "every test that does not happen to drive that particular overload."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"\bstore\.insertDownloadWorkJournalEntry\("#, in: recorderBody
            ),
            1,
            "playhead-4xmz: exactly one write path, so every event goes through the same "
            + "failure handling."
        )
    }

    /// The event vocabulary and the emission sites must stay the same size.
    /// A case with no site can never be produced; a site with no case cannot
    /// compile — so this pins the direction the compiler cannot.
    func testEveryEventTypeIsProducedBySomeSite() throws {
        let ledger = try code(Self.ledgerPath)
        XCTAssertEqual(
            DownloadWorkJournalEventType.allCases.count, 3,
            "playhead-4xmz: three events, three emission shapes. A fourth case needs a site "
            + "that can produce it, or the vocabulary is wider than anything can write."
        )
        var total = 0
        for event in DownloadWorkJournalEventType.allCases {
            let sites = SwiftSourceInspector.regexOccurrences(
                of: #"eventType:\s*\."# + event.rawValue + #"\b"#, in: ledger
            )
            total += sites
            XCTAssertGreaterThanOrEqual(
                sites, 1,
                "playhead-4xmz: `\(event.rawValue)` must be produced by at least one site. A "
                + "case nothing can write is a vocabulary entry no pull will ever see."
            )
        }
        // FOUR, not three: `failed` is produced twice because
        // `WorkJournalRecording` carries two `recordFailed` overloads and BOTH
        // must reach the store — the metadata-less one is unused by the
        // download path today, and a `fatalError` or an empty body there is a
        // trap for the first caller that adds one.
        XCTAssertEqual(
            total, 4,
            "playhead-4xmz: four emission sites — one per protocol requirement. A fifth means a "
            + "site nobody enumerated; a fourth missing means a requirement quietly stopped "
            + "writing."
        )
    }
}
