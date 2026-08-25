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
// SEVEN properties, in nine tests. The count was "six" for two review rounds
// after the sixth was added, which is the same drift this file exists to catch
// living in its own header — re-derive it when you add a rail:
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
//   6. THAT CANCELLING A JOURNAL FINALIZATION ONLY EVER HAPPENS ON A PATH THAT
//      UNLINKS THE BYTES — which is a property of the CALL GRAPH
//      (`retireBackgroundTransfers` <- `clearCache` | `cancelDownload` <-
//      `removeCache`) and not of any one call site. Review 2 tried to make it a
//      flag and the flag disarmed the delete path; review 3 caught that with a
//      behavioural rail this file cannot replace.
//   7. THAT THE DOWNLOAD PATH DOES NOT WRITE `work_journal`. That table's
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

    /// Every `Playhead/**` Swift source, comments and string contents stripped.
    ///
    /// Cached across the test methods in this class: the tree is 483 files /
    /// 12.75 MB measured, and
    /// re-reading it per assertion is the difference between a rail people run
    /// and one they route around.
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
                    domain: "DownloadWorkJournalWiringSourceCanary",
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
            + "Two means the instance that gets ARMED is not the instance that gets INJECTED — "
            + "which today writes the same row, and stops doing so the moment this recorder "
            + "acquires any per-instance state. The second instance already differs in one way: "
            + "it carries no invariantRecorder, so a failed arming goes unreported."
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
        // The residual medium L-2 and L-3 rest on. The recorder's
        // `invariantRecorder` DEFAULTS TO NIL, so dropping this one argument
        // turns off the only report of an event that neither the row nor the
        // failure counter could hold — with every runtime rail still green.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"AnalysisStoreDownloadWorkJournalRecorder\(\s*store:\s*analysisStore,\s*invariantRecorder:"#,
                in: runtime
            ),
            1,
            "playhead-4xmz: production must pass the surface-status recorder as well as the "
            + "store. It defaults to nil, so omitting it is silent — and it is the only "
            + "medium left when both durable writes fail."
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
        // A post-init setter on the DOWNLOAD recorder is impossible by
        // construction — `DownloadManager.workJournalRecorder` is a `let`, and
        // `testTheRecorderSlotCannotBeReassignedAfterInit` pins that. What is
        // checked here is the thing the compiler cannot: that nobody reaches
        // for the ANALYSIS scheduler's setter shape against the download
        // manager. `setWorkJournalRecorder` DOES legitimately exist in this
        // file — `AnalysisWorkScheduler` takes its recorder that way — so a
        // bare count of the symbol is not the test, and reading it as one is
        // how this rail failed its first run.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"\bsetWorkJournalRecorder\("#, in: runtime
            ),
            1,
            "playhead-4xmz: exactly one setter call in the composition root, and it belongs to "
            + "AnalysisWorkScheduler. A second one is a download-side setter, which would leave "
            + "the recorder no-op exactly on the sceneless relaunches this journal exists for."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"analysisWorkScheduler\.setWorkJournalRecorder\("#, in: runtime
            ),
            1,
            "playhead-4xmz: …and that one setter's receiver is the SCHEDULER. Without this the "
            + "count above would be satisfied by a download-side setter that replaced it."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"downloadManager\.setWorkJournalRecorder|downloadWorkJournalRecorder\s*=\s*Noop"#,
                in: runtime
            ),
            0,
            "playhead-4xmz: inject at construction, and never rebind to the no-op."
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

    // MARK: - 7. Not `work_journal`

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
        // FOUR, and the four are the protocol requirements. `append`'s own
        // declaration is `private func append(` and does NOT match this
        // pattern — an earlier gloss said it did, adding a fifth thing to a
        // count of four.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\bawait append\("#, in: recorderBody), 4,
            "playhead-4xmz: all FOUR `WorkJournalRecording` requirements must append. A body "
            + "that quietly became `{}` is this bead's defect one layer in, and it would pass "
            + "every test that does not happen to drive that particular overload."
        )
        // TWO write spellings since review 1, and the count has to name both.
        // The pattern above ends in `\(`, so it happens not to match
        // `…EntryUnlessCancelled(` — the count was right BY ACCIDENT while the
        // message said "exactly one write path", which is the shape this file
        // exists to catch.
        let plainWrites = SwiftSourceInspector.regexOccurrences(
            of: #"\bstore\.insertDownloadWorkJournalEntry\("#, in: recorderBody
        )
        let cancellableWrites = SwiftSourceInspector.regexOccurrences(
            of: #"\bstore\.insertDownloadWorkJournalEntryUnlessCancelled\("#,
            in: recorderBody
        )
        XCTAssertEqual(
            plainWrites, 1,
            "playhead-4xmz: exactly one PLAIN write, so every non-finalized event goes "
            + "through the same failure handling."
        )
        XCTAssertEqual(
            cancellableWrites, 1,
            "playhead-4xmz: exactly one CANCELLATION-HONOURING write. The in-actor check is "
            + "the only one that can see a cancellation landing during the hop; a second "
            + "spelling here would mean some event bypasses it."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"honoringCancellation:\s*true"#, in: ledger
            ),
            1,
            "playhead-4xmz: exactly ONE requirement honours cancellation — `finalized`. "
            + "Dropping a FAILURE because an enclosing task was cancelled would lose the "
            + "record this bead exists to create, which is the opposite trade."
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

    // MARK: - 6. Cancelling a finalization is only correct where bytes die

    /// `retireBackgroundTransfers` cancels any in-flight
    /// `download_work_journal` finalization, which DESTROYS the row for a
    /// transfer that completed. That is correct only when the artifact is
    /// about to be unlinked, and whether it is depends on the CALL GRAPH:
    /// `clearCache` unlinks a few lines later, and `cancelDownload` unlinks
    /// nothing itself but has exactly one production caller — `removeCache`,
    /// which unlinks three lines later.
    ///
    /// **AND TODAY NOTHING IN PRODUCTION TAKES THAT CHAIN**: `removeCache(for:)`
    /// has no caller outside `PlayheadTests/` either, so the whole retire
    /// mechanism is dormant in shipping builds. Three review rounds each wrote
    /// "the one production path" one call frame further out, so this rail pins
    /// COUNTS for all three — `cancelDownload`, `removeCache` and
    /// `clearCache()` — and every number below is a measurement rather than a
    /// sentence. A caller appearing is not a failure to bump past: it makes the
    /// race live, and L-7 has to be re-read.
    func testTheRetireChainIsDormantAndItsOneCallerWouldDelete() throws {
        let manager = try code(Self.managerPath)
        // TREE-WIDE, not one file. `cancelDownload` is `internal`, so a caller
        // in `PlayheadRuntime`, a view model or any other app file is invisible
        // to a check that reads only `DownloadManager.swift` — and the failure
        // message below makes a claim about PRODUCTION, which a one-file scan
        // cannot support. Found at review 4, one round after the same blindness
        // let `clearCache()` be described as a live caller when it has none.
        let production = try Self.productionSources()
        // POSITIVE CONTROL on the walk itself. Every assertion below counts
        // occurrences and passes on ZERO, so a walk that collapsed — a bad
        // root, an exception swallowed, `strippingCommentsAndStrings` blanking
        // a file (playhead-kf3b6) — would report "no callers" and read as the
        // strongest possible pass. 483 files measured; the floor is loose on
        // purpose so it survives ordinary growth and still catches a collapse.
        XCTAssertGreaterThan(
            production.count, 400,
            "playhead-4xmz: the production walk found \(production.count) Swift files, which is "
            + "too few to be the tree. Every count below passes on zero, so a collapsed walk "
            + "reads as a clean result — this is the control that stops it."
        )
        let callSites = production.values.reduce(0) { total, source in
            total + SwiftSourceInspector.regexOccurrences(
                of: #"\bcancelDownload\(\s*episodeId\s*:"#, in: source
            )
        }
        XCTAssertEqual(
            callSites, 2,
            "playhead-4xmz: exactly TWO occurrences tree-wide — the declaration and ONE call "
            + "site. `cancelDownload` retires background transfers, which cancels an in-flight "
            + "journal finalization; that is correct only because its caller deletes the bytes. "
            + "A second caller that does not delete drops a `finalized` row for an artifact "
            + "still on disk, out of the column that makes finalized/failed a real split."
        )
        XCTAssertEqual(
            production.values.reduce(0) { total, source in
                total + SwiftSourceInspector.regexOccurrences(
                    of: #"\bself\.cancelDownload\("#, in: source
                )
            },
            0,
            "playhead-4xmz: no `self.`-qualified spelling, which the count above cannot see. "
            + "It is idiomatic inside a closure and would be an invisible second call site."
        )
        // AND `removeCache(for:)`'s OWN COUNT. Review 4 established that
        // `clearCache()` has no production caller and then described
        // `removeCache` as "the one production path"; review 5 found it has
        // none either. ONE occurrence = the declaration and no caller.
        let removeCacheCalls = production.reduce(into: [String]()) { hits, entry in
            let (path, source) = entry
            let count = SwiftSourceInspector.regexOccurrences(
                of: #"(?<!func )\bremoveCache\(\s*for\s*:"#, in: source
            )
            if count > 0 { hits.append("\(path) x\(count)") }
        }
        XCTAssertEqual(
            removeCacheCalls, [],
            "playhead-4xmz: `removeCache(for:)` has NO production caller, so the retire chain "
            + "it heads is DORMANT in shipping builds. This is not a number to bump: a caller "
            + "appearing makes the delete-vs-finalization race live, and limit L-7 plus the "
            + "test-only marker on the header's cancellation state both have to be re-read. "
            + "Found: \(removeCacheCalls)"
        )

        // AND `clearCache()`'s OWN COUNT, because for one review round three
        // comments and this file described it as the second deleting caller. It
        // has none: Settings' bulk clear enumerates the cache directory from a
        // detached Task without entering the actor (limit L-7). Asserting the
        // zero is what stops that sentence being written again.
        // `(?<!func )` because the bare token also matches the DECLARATION, and
        // `(?<!Probe\.)` because `FoundationModelsUsabilityProbe.clearCache()`
        // is an unrelated method with the same name on a different type — the
        // first cut of this rail counted it and failed with "2 is not 1",
        // which is a name matching one thing while standing for another, in the
        // rail written to catch exactly that.
        let clearCacheCalls = production.reduce(into: [String]()) { hits, entry in
            let (path, source) = entry
            let count = SwiftSourceInspector.regexOccurrences(
                of: #"(?<!func )(?<!Probe\.)\bclearCache\s*\(\s*\)"#, in: source
            )
            if count > 0 { hits.append("\(path) x\(count)") }
        }
        XCTAssertEqual(
            clearCacheCalls, [],
            "playhead-4xmz: `DownloadManager.clearCache()` has NO production caller, so it must "
            + "not be described as a deleting path that runs — it was, in four places, for a "
            + "review round. If this list stops being empty, the L-7 limit and three doc "
            + "comments need re-reading rather than this assertion relaxing. Found: "
            + "\(clearCacheCalls)"
        )
        let removeCache = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: manager, after: "func removeCache(for episodeId: String) async throws"
            ),
            "could not isolate removeCache's body — this canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"await cancelDownload\("#, in: removeCache
            ),
            1,
            "playhead-4xmz: …and that one call site is inside `removeCache`."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"\bremoveAllAudioArtifacts\("#, in: removeCache
            ),
            1,
            "playhead-4xmz: …and `removeCache` really does unlink the artifact. Without this "
            + "the count above would be satisfied by a caller that keeps the bytes."
        )
        let clearCache = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: manager, after: "func clearCache() async throws"
            ),
            "could not isolate clearCache's body — this canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"retireBackgroundTransfers\("#, in: clearCache
            ),
            1,
            "playhead-4xmz: the other retiring caller, and it unlinks everything below."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"retiringJournalFinalizations"#, in: manager
            ),
            0,
            "playhead-4xmz: NO conditional-retirement parameter. Review 2 added one and it "
            + "disarmed the delete path; the property is the call graph above, and a flag "
            + "here would let a caller opt out of it one argument at a time."
        )
    }
}
