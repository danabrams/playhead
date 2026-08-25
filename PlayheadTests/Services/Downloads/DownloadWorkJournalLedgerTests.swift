// DownloadWorkJournalLedgerTests.swift
// playhead-4xmz — the download half of the work journal actually reaches disk.
//
// WHAT THESE RAILS ARE FOR, AND WHAT THEY DELIBERATELY ARE NOT.
//
// The defect was NOT that the emission sites were missing — they were all
// there, all five of them, calling a recorder whose body is `{}`. Every
// existing suite that drives them (`ForceQuitResumeTests`,
// `BackgroundURLSessionTests`, `BackgroundDownloadCompletionTests`) passes a
// TEST DOUBLE and asserts the call happened, and every one of them was green
// throughout the four months production recorded nothing. So a rail that
// asserts "the site called the recorder" reproduces the blind spot rather than
// closing it.
//
// What these rails assert instead is that the PRODUCTION conformer puts a row
// on DISK, that the row says what happened, and that the three states a device
// pull must tell apart are all reachable and distinguishable. The wiring
// itself — the part no runtime test in this tree can see, because
// `PlayheadRuntime.init` is unreachable from a unit test — is
// `DownloadWorkJournalWiringSourceCanaryTests`.
//
// EVERY `@Test` DISPLAY NAME IN THIS FILE AND ITS MIGRATION SIBLING IS UNIQUE
// ACROSS THE TREE, DELIBERATELY. Two instruments key on the display name and
// nothing else — `gate_baseline.py`'s crashed-host census (CLAUDE.md names the
// collision as a known blind spot) and `mutation-battery.sh`, which scores a
// mutant by grepping the failing set for the expected name. A duplicate name
// therefore lets one suite's test be credited for another's, which is this
// repo's standing defect class arriving through a test title. Three names here
// collided with `BackgroundDownloadDropsV62MigrationTests` /
// `BackgroundDownloadDropLedgerTests` on the first draft — the neighbouring
// suites this one is modelled on, which is exactly where collisions come from —
// and were renamed. **The TREE still holds 59 such duplicates, measured; that
// is playhead-4wk9 and not this bead.**

import Foundation
import Testing
import SQLite3
@testable import Playhead

@Suite("Download work journal — the download path's events reach disk (playhead-4xmz)")
struct DownloadWorkJournalLedgerTests {

    // MARK: - Helpers

    private static func freshStore(
        prefix: String
    ) async throws -> (AnalysisStore, URL) {
        let dir = try makeTempDir(prefix: prefix)
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        return (store, dir)
    }

    private static func openRawReadWrite(_ directory: URL) throws -> OpaquePointer? {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
            throw NSError(domain: "OpenRawReadWrite", code: 1)
        }
        sqlite3_busy_timeout(db, 3000)
        return db
    }

    /// Collects the surface-status lines the recorder raises, so the residual
    /// — "both durable writes failed" — is observable rather than inferred.
    private final class InvariantSpy: @unchecked Sendable {
        private let lock = NSLock()
        private var lines: [(InvariantViolation.Code, String)] = []

        var recorder: @Sendable (InvariantViolation.Code, String) -> Void {
            { [self] code, description in
                lock.lock()
                lines.append((code, description))
                lock.unlock()
            }
        }

        func descriptions(of code: InvariantViolation.Code) -> [String] {
            lock.lock()
            defer { lock.unlock() }
            return lines.filter { $0.0 == code }.map(\.1)
        }
    }

    // MARK: - 1. Every protocol requirement lands a row that says what happened

    /// All FOUR `WorkJournalRecording` requirements, including the
    /// metadata-less `recordFailed` overload the download path does not call
    /// today. That one is here because an unimplemented requirement is a trap
    /// for whoever adds the first caller, and because "the method exists" and
    /// "the method writes" are exactly the two things this bead found are not
    /// the same.
    @Test("each of the four requirements appends a row carrying its event, cause and metadata")
    func everyRequirementAppendsARowThatSaysWhatHappened() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzEvents")
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(store: store)

        await recorder.recordFinalized(episodeId: "ep-done")
        await recorder.recordFailed(episodeId: "ep-bare", cause: .noNetwork)
        await recorder.recordFailed(
            episodeId: "ep-blob",
            cause: .taskExpired,
            metadataJSON: #"{"stage":"backgroundTransfer","bytes_processed":"4096"}"#
        )
        await recorder.recordPreempted(
            episodeId: "ep-quit",
            cause: .appForceQuitRequiresRelaunch,
            metadataJSON: #"{"stage":"forceQuitResumeScan.resumable"}"#
        )

        let page = try await store.fetchDownloadWorkJournal()
        #expect(page.rows.count == 4)
        #expect(page.unrecognizedEventTypeRows == 0)
        #expect(page.truncated == false)
        #expect(page.totalRowsSeen == 4)

        let byEpisode = Dictionary(
            uniqueKeysWithValues: page.rows.map { ($0.episodeId, $0) }
        )

        let done = try #require(byEpisode["ep-done"])
        #expect(done.eventType == .finalized)
        #expect(
            done.cause == nil,
            """
            a successful transfer has no miss cause; a value here would put a reason in a
            column whose entire job is to carry one
            """
        )
        #expect(done.metadataJSON == "{}")

        let bare = try #require(byEpisode["ep-bare"])
        #expect(bare.eventType == .failed)
        #expect(bare.cause == .noNetwork)

        let blob = try #require(byEpisode["ep-blob"])
        #expect(blob.eventType == .failed)
        #expect(blob.cause == .taskExpired)
        #expect(
            blob.metadataJSON.contains("backgroundTransfer"),
            """
            the SliceMetadata blob is the payload playhead-1nl6 removed a protocol default for;
            this conformer's decision is to KEEP it
            """
        )

        let quit = try #require(byEpisode["ep-quit"])
        #expect(quit.eventType == .preempted)
        #expect(quit.cause == .appForceQuitRequiresRelaunch)
        #expect(quit.metadataJSON.contains("forceQuitResumeScan.resumable"))
    }

    /// Append-only: two failures for one episode are two rows, because a
    /// REPEATED failure is among the most interesting things this table can
    /// show and a key that collapsed it would hide exactly that population.
    @Test("a repeated failure for one episode is two rows, not one")
    func theJournalIsAppendOnly() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzAppend")
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(store: store)

        await recorder.recordFailed(episodeId: "ep-flaky", cause: .noNetwork, metadataJSON: "{}")
        await recorder.recordFailed(episodeId: "ep-flaky", cause: .taskExpired, metadataJSON: "{}")

        let page = try await store.fetchDownloadWorkJournal()
        #expect(page.rows.count == 2)
        #expect(Set(page.rows.map(\.cause)) == Set([.noNetwork, .taskExpired]))
    }

    // MARK: - 2. It survives the process that wrote it

    /// The point of the whole bead. The rows are written through one
    /// `AnalysisStore` handle and read through a SECOND one opened on the same
    /// directory — the closest a unit test gets to "the app was killed and
    /// relaunched", and strictly stronger than reading back through the writer,
    /// which an in-memory cache would also satisfy.
    @Test("the download-journal rows are on disk, not in memory — a second store on the same file reads them")
    func theRowsSurviveTheProcessThatWroteThem() async throws {
        let (writer, dir) = try await Self.freshStore(prefix: "4xmzDurable")
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(store: writer)

        // In production this call is `PlayheadRuntime`'s, made after the store
        // is known open. Here it stands for one launch.
        await recorder.recordInstrumentArmed(at: 1_000.0)
        await recorder.recordFailed(
            episodeId: "ep-durable",
            cause: .pipelineError,
            metadataJSON: #"{"stage":"forceQuitResumeScan.corrupted"}"#
        )

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let page = try await reopened.fetchDownloadWorkJournal()
        #expect(page.rows.count == 1)
        let row = try #require(page.rows.first)
        #expect(row.episodeId == "ep-durable")
        #expect(row.eventType == .failed)
        #expect(row.cause == .pipelineError)
        #expect(row.metadataJSON.contains("forceQuitResumeScan.corrupted"))

        // And the arming claim survives too, or a pull could read a durable
        // event beside a census that says nobody was recording.
        let arming = try #require(try await reopened.fetchDownloadWorkJournalArming())
        #expect(arming.armedLaunches == 1)
        #expect(arming.firstArmedAt == 1_000.0)
    }

    // MARK: - 3. Empty-because-nothing-happened vs empty-because-nobody-recorded

    /// The three-state truth table, all of it on disk, and this rail is the
    /// reason the bead is not "wire the recorder and stop". An instrument whose
    /// silence cannot be distinguished from its absence is the defect, not the
    /// fix.
    @Test("a fresh store reads INSTALLED BUT NEVER ARMED, which is not the same as no instrument")
    func installedButNeverArmedIsItsOwnState() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzUnarmed")
        let fetched = try await store.fetchDownloadWorkJournalArming()
        let arming = try #require(
            fetched,
            """
            the V63 rung must SEED the arming row; an ABSENT row and a ZEROED row are different
            claims — one says the migration ran, the other says nothing at all did
            """
        )
        #expect(arming.armedLaunches == 0)
        #expect(
            arming.firstArmedAt == nil,
            "a zero here would date an arming that never happened"
        )
        #expect(arming.lastArmedAt == nil)
        #expect(arming.writeFailures == 0)
        #expect(arming.installedAt > 0)
        #expect(try await store.fetchDownloadWorkJournal().rows.isEmpty)
    }

    @Test("the download journal arming counts a launch, and a second counts again without moving firstArmedAt")
    func armingCountsEachLaunch() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzArming")
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(store: store)

        await recorder.recordInstrumentArmed(at: 100.0)
        let afterOne = try #require(try await store.fetchDownloadWorkJournalArming())
        #expect(afterOne.armedLaunches == 1)
        #expect(afterOne.firstArmedAt == 100.0)
        #expect(afterOne.lastArmedAt == 100.0)

        await recorder.recordInstrumentArmed(at: 200.0)
        let afterTwo = try #require(try await store.fetchDownloadWorkJournalArming())
        #expect(afterTwo.armedLaunches == 2)
        #expect(
            afterTwo.firstArmedAt == 100.0,
            """
            firstArmedAt means THE FIRST TIME; if it follows the latest arming it is a second
            lastArmedAt under a misleading name
            """
        )
        // Pinned to the INJECTED times rather than to an ordering, because
        // `lastArmedAt >= firstArmedAt` holds however the column is written
        // and so cannot fail.
        #expect(afterTwo.lastArmedAt == 200.0)
    }

    /// The third cell. Without it, `armedLaunches > 0` beside zero rows is
    /// reachable by a store that simply could not be written to, and that
    /// state is byte-identical to this journal's strongest positive claim.
    @Test("an event whose row cannot be written increments writeFailures instead of vanishing")
    func aLostRowIsCountedRatherThanSilent() async throws {
        let (store, dir) = try await Self.freshStore(prefix: "4xmzWriteFail")
        let spy = InvariantSpy()
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(
            store: store, invariantRecorder: spy.recorder
        )
        await recorder.recordInstrumentArmed(at: 10.0)

        // Drop ONLY the journal table, leaving the arming row writable: the
        // shape of a store with one corrupt/dropped table rather than a dead
        // device.
        let db = try Self.openRawReadWrite(dir)
        #expect(sqlite3_exec(db, "DROP TABLE download_work_journal", nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        await recorder.recordFailed(episodeId: "ep-lost", cause: .noNetwork, metadataJSON: "{}")

        let arming = try #require(try await store.fetchDownloadWorkJournalArming())
        #expect(
            arming.writeFailures == 1,
            """
            a lost event must move a durable counter; otherwise `armedLaunches > 0` with zero
            rows is reachable by silence and the positive claim is worthless
            """
        )
        #expect(arming.armedLaunches == 1, "a failure is not an arming")
        #expect(
            spy.descriptions(of: .downloadWorkJournalNotRecorded).isEmpty,
            """
            the surface-status line is the RESIDUAL — for when both durable writes fail.
            Raising it here would send a reader to diagnose a database that just successfully
            recorded the loss.
            """
        )
    }

    /// The residual, named rather than hidden: when the failure counter is
    /// itself a store write and the store is gone, only the second medium is
    /// left. It is a different FILE and not a different failure domain
    /// (playhead-dyvh2) — which is exactly why the durable counter above is the
    /// primary mechanism and this is the backstop.
    @Test("when BOTH durable writes fail, the loss is raised on the surface-status stream")
    func theResidualReachesTheSecondMedium() async throws {
        let (store, dir) = try await Self.freshStore(prefix: "4xmzResidual")
        let spy = InvariantSpy()
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(
            store: store, invariantRecorder: spy.recorder
        )

        let db = try Self.openRawReadWrite(dir)
        for sql in [
            "DROP TABLE download_work_journal",
            "DROP TABLE download_work_journal_arming",
        ] {
            #expect(sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK, "\(sql)")
        }
        sqlite3_close_v2(db)

        await recorder.recordFailed(
            episodeId: "ep-gone", cause: .taskExpired, metadataJSON: "{}"
        )

        let lines = spy.descriptions(of: .downloadWorkJournalNotRecorded)
        #expect(lines.count == 1)
        let line = try #require(lines.first)
        // Everything the lost row would have carried, so the loss is
        // recoverable from this line alone.
        #expect(line.contains("ep-gone"))
        #expect(line.contains("failed"))
        #expect(line.contains("task_expired"))
        #expect(line.contains("row=failed counter=failed"))
    }

    /// The denominator has the same hole the numerator does, and it is closed
    /// the same way.
    @Test("an arming that cannot be written says so on the second medium")
    func aLostArmingIsRaisedToo() async throws {
        let (store, dir) = try await Self.freshStore(prefix: "4xmzArmFail")
        let spy = InvariantSpy()
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(
            store: store, invariantRecorder: spy.recorder
        )

        let db = try Self.openRawReadWrite(dir)
        #expect(
            sqlite3_exec(db, "DROP TABLE download_work_journal_arming", nil, nil, nil) == SQLITE_OK
        )
        sqlite3_close_v2(db)

        await recorder.recordInstrumentArmed(at: 5.0)

        let lines = spy.descriptions(of: .downloadWorkJournalNotRecorded)
        #expect(lines.count == 1)
        #expect(try #require(lines.first).contains("arming=failed"))
    }

    // MARK: - 4. The reader does not over-report

    /// A row this build cannot decode is DROPPED and COUNTED, never folded
    /// into a default case: a wider-vocabulary build's row collapsed into
    /// `failed` would silently inflate that population.
    @Test("a row with an unrecognized eventType is counted, not folded into failed")
    func anUnreadableRowIsCountedRatherThanCoerced() async throws {
        let (store, dir) = try await Self.freshStore(prefix: "4xmzUnknownEvent")
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(store: store)
        await recorder.recordFailed(episodeId: "ep-real", cause: .noNetwork, metadataJSON: "{}")

        let db = try Self.openRawReadWrite(dir)
        let future = """
            INSERT INTO download_work_journal
            (id, episodeId, eventType, cause, occurredAt, metadata)
            VALUES ('future-1', 'ep-future', 'abandoned', 'thermal', 9999.0, '{}')
            """
        #expect(sqlite3_exec(db, future, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        let page = try await store.fetchDownloadWorkJournal()
        #expect(page.rows.count == 1)
        #expect(page.rows.first?.episodeId == "ep-real")
        #expect(page.unrecognizedEventTypeRows == 1)
        #expect(
            page.totalRowsSeen == 2,
            """
            \"one event\" and \"one event I can read\" are different claims, and the page must
            be able to state the second
            """
        )
    }

    /// The `cause` column needs no refusal counter, and this rail says why:
    /// `InternalMissCause` carries a forward-compat `.unknown(String)` case, so
    /// an unrecognized cause round-trips verbatim instead of being lost or
    /// coerced to nil — and a nil there would say "this event had no reason",
    /// which is a different claim.
    @Test("an unrecognized cause round-trips as .unknown rather than becoming nil")
    func anUnknownCauseRoundTrips() async throws {
        let (store, dir) = try await Self.freshStore(prefix: "4xmzUnknownCause")
        let db = try Self.openRawReadWrite(dir)
        let future = """
            INSERT INTO download_work_journal
            (id, episodeId, eventType, cause, occurredAt, metadata)
            VALUES ('fc-1', 'ep-fc', 'failed', 'daemon_went_to_lunch', 1.0, '{}')
            """
        #expect(sqlite3_exec(db, future, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        let page = try await store.fetchDownloadWorkJournal()
        #expect(page.unrecognizedEventTypeRows == 0)
        let row = try #require(page.rows.first)
        #expect(row.cause == .unknown("daemon_went_to_lunch"))
        #expect(row.cause?.rawValue == "daemon_went_to_lunch")
    }

    /// A window that stops at its own ceiling must say so, or it reports
    /// "this is what happened" while meaning "this is what fitted".
    @Test("a window that hits its limit reports truncated, and most-recent-first")
    func aTruncatedWindowSaysSo() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzTruncate")
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(store: store)
        for index in 0..<4 {
            await recorder.recordFailed(
                episodeId: "ep-\(index)", cause: .noNetwork, metadataJSON: "{}"
            )
            // The rows carry `Date()`; without a gap two of them can share a
            // timestamp and the ORDER assertion below would be measuring the
            // clock's resolution rather than the query's ORDER BY.
            try await Task.sleep(nanoseconds: 2_000_000)
        }

        let full = try await store.fetchDownloadWorkJournal()
        #expect(full.rows.count == 4)
        #expect(full.truncated == false)
        #expect(
            full.rows.first?.episodeId == "ep-3",
            "most recent first — a forensic tail read oldest-first is the wrong end of the table"
        )

        let capped = try await store.fetchDownloadWorkJournal(limit: 2)
        #expect(capped.rows.count == 2)
        #expect(capped.truncated)
        #expect(capped.rows.map(\.episodeId) == ["ep-3", "ep-2"])

        // The clamp exists because `bind(_:_:Int)` goes through the TRAPPING
        // `Int32(_:)`, and `limit: .max` is the spelling a later reader is most
        // likely to write for "everything". It must return rows, not crash.
        let everything = try await store.fetchDownloadWorkJournal(limit: .max)
        #expect(everything.rows.count == 4)
        #expect(everything.truncated == false)
    }

    // MARK: - 5. End to end, through the actor that owns the emission sites

    /// The one rail that drives a REAL emission site with the REAL recorder.
    ///
    /// Every other suite that reaches `scanForSuspendedTransfers` passes a test
    /// double and asserts the call happened — and every one of them was green
    /// for the four months production wrote nothing. This is the same drive
    /// with the production conformer behind it, so what it proves is that the
    /// event reaches `analysis.sqlite`, which is the file a device pull copies.
    @Test("the force-quit resume scan preempted event reaches the store through DownloadManager")
    func aRealEmissionSiteReachesTheStore() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzEndToEnd")
        let cache = try makeTempDir(prefix: "4xmzEndToEndCache")
        defer { try? FileManager.default.removeItem(at: cache) }

        let recorder = AnalysisStoreDownloadWorkJournalRecorder(store: store)
        let manager = DownloadManager(
            cacheDirectory: cache,
            workJournalRecorder: recorder
        )
        try await manager.bootstrap()
        await recorder.recordInstrumentArmed(at: 42.0)

        try await manager.persistResumeData(episodeId: "ep-e2e", data: Data([0x01, 0x02]))
        let outcome = try await manager.scanForSuspendedTransfers()
        #expect(outcome.resumableTransferIds == Set(["ep-e2e"]))

        let page = try await store.fetchDownloadWorkJournal()
        #expect(
            page.rows.count == 1,
            """
            before playhead-4xmz this read ZERO on every production device, while this same
            drive against a test double read one
            """
        )
        let row = try #require(page.rows.first)
        #expect(row.episodeId == "ep-e2e")
        #expect(row.eventType == .preempted)
        #expect(row.cause == .appForceQuitRequiresRelaunch)
        #expect(
            row.metadataJSON.contains("ep-e2e"),
            "the SliceMetadata blob the emission site built, persisted rather than dropped"
        )

        let arming = try #require(try await store.fetchDownloadWorkJournalArming())
        #expect(arming.armedLaunches == 1)
        #expect(arming.writeFailures == 0)
    }

    /// The default is still a no-op, and it must stay one: it is what every
    /// test and preview gets. This rail pins that the no-op really records
    /// nothing, so `armedLaunches = 0` beside an empty table keeps meaning
    /// "nobody was recording" rather than "nothing happened".
    @Test("the DEFAULT recorder still writes nothing, which is what makes armedLaunches readable")
    func theDefaultRemainsANoop() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzDefaultNoop")
        let cache = try makeTempDir(prefix: "4xmzDefaultNoopCache")
        defer { try? FileManager.default.removeItem(at: cache) }

        // No `workJournalRecorder:` argument — the state production was in.
        let manager = DownloadManager(cacheDirectory: cache)
        try await manager.bootstrap()
        try await manager.persistResumeData(episodeId: "ep-noop", data: Data([0x09]))
        _ = try await manager.scanForSuspendedTransfers()

        #expect(try await store.fetchDownloadWorkJournal().rows.isEmpty)
        let arming = try #require(try await store.fetchDownloadWorkJournalArming())
        #expect(
            arming.armedLaunches == 0,
            """
            and THIS is the pair a device pull sees on an unwired build: an empty table beside
            a zero denominator. It is not the same reading as an empty table beside
            armedLaunches = 37, and telling those two apart is the whole point of the row.
            """
        )
    }
}
