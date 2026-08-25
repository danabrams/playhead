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
// ACROSS THE TREE, DELIBERATELY — AND THE FIRST FIX ROUND BROKE IT, WHICH IS
// WHY THIS PARAGRAPH IS LONGER THAN IT LOOKS LIKE IT NEEDS TO BE. A rail added
// to close one of round 1's findings was named `an unbounded limit returns
// everything instead of trapping`, byte-identical to a test in
// `BackgroundDownloadDropLedgerTests` — the neighbouring suite this one is
// modelled on, again — under a header that claims in capitals that no such
// collision exists. Caught at review 2. Re-check this whenever a rail is added
// here; the claim is only as good as its last measurement. Two instruments key on the display name and
// nothing else — `gate_baseline.py`'s crashed-host census (CLAUDE.md names the
// collision as a known blind spot) and `mutation-battery.sh`, which scores a
// mutant by grepping the failing set for the expected name. A duplicate name
// therefore lets one suite's test be credited for another's, which is this
// repo's standing defect class arriving through a test title. Three names here
// collided with `BackgroundDownloadDropsV62MigrationTests` /
// `BackgroundDownloadDropLedgerTests` on the first draft — the neighbouring
// suites this one is modelled on, which is exactly where collisions come from —
// and were renamed. **The tree still holds duplicates, and there are two
// different counts, MEASURED at base `64078664` and re-measured at review 2
// after this branch's renames — say which you mean:**
//
//     names appearing in MORE THAN ONE FILE   base 56  ->  this branch 56
//     names occurring more than once at all   base 64  ->  this branch 64
//
// (An earlier version of this paragraph said 59/56, taking the pre-rename
// figure as the base — it was measured on THIS BRANCH before the renames, not
// on base, so it counted this branch's own three collisions as pre-existing.
// The base numbers are 56 and 64, and the branch returns to them.) That is
// playhead-0dsti and not this bead.**

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

        // `occurredAt` IS THE ONLY TIMESTAMP ON THE TABLE and the sole ordering
        // key, and until review 2 nothing asserted the value the RECORDER
        // writes — every other assertion here is on a record the test itself
        // constructed with a literal. A recorder hardcoding 0 would have
        // survived: `ORDER BY occurredAt DESC, rowid DESC` degenerates to
        // `rowid DESC`, which is the order the truncation rail already expects.
        let now = Date().timeIntervalSince1970
        for row in page.rows {
            #expect(
                row.occurredAt > now - 300 && row.occurredAt <= now + 5,
                """
                the recorder must stamp the row with the wall clock, not a
                constant and not a value derived from the record's own contents
                """
            )
        }
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

    /// The truth table, all of it on disk, and this rail is the reason the bead
    /// is not "wire the recorder and stop". The header of
    /// `DownloadWorkJournalLedger.swift` enumerates SIX reachable states — the
    /// count has been wrong twice in that file, so read the list rather than
    /// the number. An instrument whose
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

    /// The cell that INVERTS the positive claim. Without it, `armedLaunches > 0`
    /// beside zero rows is
    /// reachable by a store that simply could not be written to, and that
    /// state is byte-identical to this journal's strongest positive claim.
    @Test("an event whose row cannot be written increments writeFailures instead of vanishing")
    func aLostRowIsCountedRatherThanSilent() async throws {
        let (store, dir) = try await Self.freshStore(prefix: "4xmzWriteFail")
        let spy = RecordedInvariantViolations()
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
        let spy = RecordedInvariantViolations()
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
        let spy = RecordedInvariantViolations()
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

    /// The row-CREATING branch of the write-failure writer, which review 3
    /// found NO test executed: `aLostRowIsCountedRatherThanSilent` arms first
    /// so the row exists, and `theResidualReachesTheSecondMedium` drops the
    /// table so neither branch runs. Its doc makes an explicit claim — "a
    /// failure is not an arming, and inventing one here would manufacture the
    /// very claim this column exists to withhold" — and changing the literal
    /// `0` to `1` survived everything.
    @Test("a write failure on a store with NO arming row creates one WITHOUT inventing an arming")
    func aWriteFailureOnAStorelessOfItsArmingRowInventsNoArming() async throws {
        let (store, dir) = try await Self.freshStore(prefix: "4xmzArmMissing")
        // The state a hand-edited fixture or a partially-rolled-back rung is
        // in: the tables exist and the seeded row does not.
        let db = try Self.openRawReadWrite(dir)
        #expect(
            sqlite3_exec(db, "DELETE FROM download_work_journal_arming", nil, nil, nil)
                == SQLITE_OK
        )
        sqlite3_close_v2(db)
        #expect(try await store.fetchDownloadWorkJournalArming() == nil)

        try await store.noteDownloadWorkJournalWriteFailure(at: 77.0)

        let arming = try #require(try await store.fetchDownloadWorkJournalArming())
        #expect(arming.writeFailures == 1)
        #expect(
            arming.armedLaunches == 0,
            """
            a FAILURE is not an ARMING. Counting one here would manufacture the
            denominator out of the very event that proves nothing was recorded.
            """
        )
        #expect(arming.firstArmedAt == nil)
        #expect(arming.lastArmedAt == nil)
        #expect(
            arming.installedAt == 77.0,
            """
            and on this path installedAt dates the FAILURE, which is the third
            writer that stamp can have — read it as "the earliest moment this
            install is known to have carried the instrument"
            """
        )
    }

    /// The mirror: the arming writer's own row-CREATING branch, equally
    /// unexercised until review 3.
    @Test("an arming on a store with NO arming row creates one and counts the launch")
    func anArmingOnAStorelessOfItsArmingRowCreatesIt() async throws {
        let (store, dir) = try await Self.freshStore(prefix: "4xmzArmCreate")
        let db = try Self.openRawReadWrite(dir)
        #expect(
            sqlite3_exec(db, "DELETE FROM download_work_journal_arming", nil, nil, nil)
                == SQLITE_OK
        )
        sqlite3_close_v2(db)

        try await store.noteDownloadWorkJournalInstrumentArmed(at: 88.0)

        let arming = try #require(try await store.fetchDownloadWorkJournalArming())
        #expect(arming.armedLaunches == 1)
        #expect(arming.firstArmedAt == 88.0)
        #expect(arming.lastArmedAt == 88.0)
        #expect(arming.writeFailures == 0)
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
        // NO SLEEP BETWEEN THESE, deliberately. An earlier cut inserted 2 ms so
        // two rows could not share an `occurredAt` — a test masking a defect in
        // the query it is testing. `ORDER BY occurredAt DESC, rowid DESC` makes
        // the order total, so writing these as fast as the machine allows is
        // now the STRONGER fixture: it is exactly the case that used to be
        // arbitrary.
        for index in 0..<4 {
            await recorder.recordFailed(
                episodeId: "ep-\(index)", cause: .noNetwork, metadataJSON: "{}"
            )
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

    }

    /// SEPARATE FROM THE TRUNCATION RAIL ON PURPOSE. `bind(_:_:Int)` goes
    /// through the TRAPPING `Int32(_:)`, so a mutant that removes the clamp
    /// KILLS THE HOST rather than failing an expectation — and
    /// `mutation-battery.sh` scores a test with no verdict as a PASS
    /// (playhead-gjlp0). Sharing one test with the `+ 1` probe made that
    /// mutant's verdict depend on whether the failure line flushed before the
    /// crash. Two tests, two independent verdicts.
    @Test("an unbounded download-journal limit returns everything instead of trapping")
    func anUnboundedLimitDoesNotTrap() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzUnbounded")
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(store: store)
        for index in 0..<4 {
            await recorder.recordFailed(
                episodeId: "ep-\(index)", cause: .noNetwork, metadataJSON: "{}"
            )
        }
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

    /// The default recorder writes nothing, driven DIRECTLY.
    ///
    /// An earlier cut of this rail built `DownloadManager(cacheDirectory:)`,
    /// drove the scan, and asserted an unrelated store was empty — which is a
    /// TAUTOLOGY: `DownloadManager` holds no store reference at all, so that
    /// store was never reachable from it and the assertion held whatever
    /// `NoopWorkJournalRecorder`'s bodies did. It would have passed if the
    /// no-op had been changed to write rows, which is the one thing it exists
    /// to deny. Driving the conformer against the store directly is the honest
    /// version.
    @Test("the DEFAULT recorder writes nothing, which is what makes armedLaunches readable")
    func theDefaultRemainsANoop() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzDefaultNoop")
        let noop: WorkJournalRecording = NoopWorkJournalRecorder()

        await noop.recordFinalized(episodeId: "ep-noop")
        await noop.recordFailed(episodeId: "ep-noop", cause: .noNetwork)
        await noop.recordFailed(
            episodeId: "ep-noop", cause: .noNetwork, metadataJSON: "{}"
        )
        await noop.recordPreempted(
            episodeId: "ep-noop",
            cause: .appForceQuitRequiresRelaunch,
            metadataJSON: "{}"
        )

        #expect(try await store.fetchDownloadWorkJournal().rows.isEmpty)
        let arming = try #require(try await store.fetchDownloadWorkJournalArming())
        #expect(
            arming.armedLaunches == 0,
            """
            and THIS is the pair a device pull sees on an unwired build: an empty
            table beside a zero denominator. It is not the same reading as an
            empty table beside armedLaunches = 37, and telling those two apart is
            the whole point of the row.
            """
        )
    }

    // MARK: - 6. Cancellation

    /// A CANCELLED FINALIZATION MUST NOT PUBLISH A ROW.
    ///
    /// `DownloadManager.retireBackgroundTransfers` cancels the finalization
    /// `Task` before the cache deletion unlinks the bytes, and
    /// `backgroundJournalFinalizations`' own doc says that is what stops a
    /// recorder "suspended before its durable append" claiming an artifact
    /// that is gone. Before this bead the window was ~0 because the recorder
    /// returned immediately; it is now however long the store actor is busy.
    @Test("a finalization cancelled before it runs writes no row and counts no write failure")
    func aCancelledFinalizationPublishesNothing() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzCancelFin")
        let spy = RecordedInvariantViolations()
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(
            store: store, invariantRecorder: spy.recorder
        )

        let task = Task {
            await recorder.recordFinalized(episodeId: "ep-cancelled")
        }
        task.cancel()
        await task.value

        #expect(try await store.fetchDownloadWorkJournal().rows.isEmpty)
        // The counter is the SUBJECT here, not decoration. A cancelled write
        // routed into `writeFailures` would say this database could not hold a
        // row — a claim about the STORE, and the one reading that counter
        // exists to make.
        let arming = try #require(try await store.fetchDownloadWorkJournalArming())
        #expect(arming.writeFailures == 0)
        #expect(spy.descriptions(of: .downloadWorkJournalNotRecorded).isEmpty)
    }

    /// The check that has to happen INSIDE the actor, tested where it lives.
    ///
    /// `Task.isCancelled` read before an `await` onto an actor cannot see a
    /// cancellation that lands DURING the hop, because an actor hop is not a
    /// cancellation point. This is the `appendWorkJournalEntryUnlessCancelled`
    /// shape one table over, and it needs its own rail because the recorder's
    /// pre-hop guard would mask a missing post-hop check in every test that
    /// cancels before calling.
    @Test("the store's UnlessCancelled append refuses inside the actor, not just at the caller")
    func theStoreRefusesACancelledAppend() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzCancelStore")
        let record = DownloadWorkJournalRecord(
            episodeId: "ep-inside",
            eventType: .finalized,
            cause: nil,
            occurredAt: 1.0,
            metadataJSON: "{}"
        )
        // `catch { error is CancellationError }` rather than
        // `catch is CancellationError`: the narrow form leaves the closure
        // throwing, which makes this a `Task<Bool, Error>` whose `.value` needs
        // its own `try` — and a `try` here would report a store error as this
        // rail's own failure rather than as the WRONG error kind. The broad
        // catch says which error arrived.
        let task = Task { () -> Bool in
            do {
                try await store.insertDownloadWorkJournalEntryUnlessCancelled(record)
                return false
            } catch {
                return error is CancellationError
            }
        }
        task.cancel()
        let threw = await task.value
        #expect(threw, "the store method must THROW CancellationError, not swallow it")
        #expect(try await store.fetchDownloadWorkJournal().rows.isEmpty)
    }

    /// The mirror, and it is what stops the rail above from being satisfied by
    /// a recorder that simply drops everything: a FAILURE is recorded even
    /// when the enclosing task is cancelled. Losing one of those is exactly
    /// the record this bead exists to create.
    @Test("a cancelled FAILURE is still recorded — the asymmetry is deliberate")
    func aCancelledFailureIsStillRecorded() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzCancelFail")
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(store: store)

        let task = Task {
            await recorder.recordFailed(
                episodeId: "ep-still-recorded",
                cause: .taskExpired,
                metadataJSON: "{}"
            )
        }
        task.cancel()
        await task.value

        let page = try await store.fetchDownloadWorkJournal()
        #expect(page.rows.count == 1)
        #expect(page.rows.first?.eventType == .failed)
    }

    // MARK: - 7. The hazard the separate table exists to avoid

    /// **THE PREMISE OF THE WHOLE DESIGN, DEMONSTRATED RATHER THAN ASSERTED.**
    ///
    /// The one-line fix for this bead is to hand `DownloadManager` the
    /// ANALYSIS recorder. This rail shows what that does: with an
    /// `analysis_jobs` row present, a DOWNLOAD failure written through
    /// `AnalysisStoreWorkJournalRecorder` lands in `work_journal` carrying THAT
    /// JOB'S generation and epoch — and `work_journal.event_type` is the input
    /// `AnalysisCoordinator.recoverOrphans` routes on, where `.failed` means
    /// `terminalNoRequeue` (pinned by `ZeroCoverageRecoveryRoutingTests`). So
    /// the transfer failure would tell cold-launch recovery that the ANALYSIS
    /// work is over.
    ///
    /// Without this rail the claim lives only in prose and in two regexes that
    /// cannot currently fail.
    @Test("the ANALYSIS recorder really would write a work_journal row under the job generation")
    func theHazardTheSeparateTableAvoidsIsReal() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzHazard")
        let generation = UUID().uuidString
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-hazard",
            episodeId: "ep-hazard",
            leaseOwner: "worker-1",
            leaseExpiresAt: 1.0,
            generationID: generation,
            schedulerEpoch: 1
        ))

        let analysisRecorder = AnalysisStoreWorkJournalRecorder(store: store)
        await analysisRecorder.recordFailed(
            episodeId: "ep-hazard",
            cause: .taskExpired,
            metadataJSON: #"{"stage":"backgroundTransfer"}"#
        )

        let row = try #require(
            try await store.fetchLastWorkJournalEntry(
                episodeId: "ep-hazard", generationID: generation
            ),
            """
            if this is nil the premise is refuted and the whole separate-table
            argument has to be re-derived — say so loudly rather than deleting
            the rail
            """
        )
        #expect(row.eventType == .failed)
        #expect(row.generationID.uuidString == generation)
        #expect(
            row.eventType.orphanRecoveryRouting == .terminalNoRequeue,
            """
            …and this is why it matters: recoverOrphans reads the LAST row for
            {episode, generation} of a job whose lease expired and takes this arm
            """
        )
    }

    /// The same fixture, the recorder this bead actually wires. The download
    /// event lands in `download_work_journal` and `work_journal` stays EMPTY —
    /// which is only a meaningful claim because the rail above proves the other
    /// recorder would have written one here.
    @Test("the DOWNLOAD recorder writes no work_journal row even when a job EXISTS")
    func theDownloadRecorderLeavesTheAnalysisJournalAlone() async throws {
        let (store, _) = try await Self.freshStore(prefix: "4xmzNoHazard")
        let generation = UUID().uuidString
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-safe",
            episodeId: "ep-safe",
            leaseOwner: "worker-1",
            leaseExpiresAt: 1.0,
            generationID: generation,
            schedulerEpoch: 1
        ))

        let recorder = AnalysisStoreDownloadWorkJournalRecorder(store: store)
        await recorder.recordFailed(
            episodeId: "ep-safe",
            cause: .taskExpired,
            metadataJSON: #"{"stage":"backgroundTransfer"}"#
        )

        #expect(
            try await store.fetchLastWorkJournalEntry(
                episodeId: "ep-safe", generationID: generation
            ) == nil,
            """
            a download event in work_journal would terminate this generation at
            the next cold launch, for a reason that has nothing to do with
            analysis
            """
        )
        let page = try await store.fetchDownloadWorkJournal()
        #expect(page.rows.count == 1)
        #expect(page.rows.first?.episodeId == "ep-safe")
    }
}
