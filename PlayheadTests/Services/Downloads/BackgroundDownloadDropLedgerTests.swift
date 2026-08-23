// BackgroundDownloadDropLedgerTests.swift
// playhead-7dgx — the three abandonment paths in
// `DownloadManager.backgroundDownload` leave a durable, countable, and
// DISTINGUISHABLE row.
//
// WHY EACH PATH NEEDS ITS OWN SEAM. Since playhead-gpdb the FIRST crossing
// `backgroundDownload` makes is the session CONSTRUCTION, so a blanket
// `.neverAnswers` reaches path A and can never reach B or C — a suite built on
// it would report three green rails while exercising one branch three times.
// Every rail here refuses exactly one crossing BY ITS LABEL, and the `reason`
// each row carries is what proves the drive landed where it was aimed.
//
// NO RAIL HERE USES `BackgroundSessionIO.shared`, and every seam below owns a
// uniquely-labelled queue: the shared serial queue is a measured head-of-line
// hazard across concurrently-running suites (playhead-et2d, seven gate-baseline
// entries). Note what that does NOT buy. The background-session IDENTIFIERS are
// process-wide constants, so the four rails that construct a real
// `URLSessionConfiguration.background(withIdentifier:)` still share that
// resource with `DownloadShowAttributionTests`, `BackgroundURLSessionTests` and
// `ForceQuitResumeTests` under the parallel plan. Each of those four
// invalidates its sessions when it is done, and the one whose pass depends on
// the daemon ANSWERING carries a deliberately generous bound — not because
// waiting is a fix, but because a bound that only bites under starvation is the
// difference between measuring the code and measuring the box.

import Foundation
import Testing
import SQLite3
@testable import Playhead

@Suite("DownloadManager – a dropped background download leaves a durable row (playhead-7dgx)")
struct BackgroundDownloadDropLedgerTests {

    // MARK: - Seams

    /// Refuses ONLY the session-construction crossing. Reaches path A.
    private static func creationRefusingIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .refusesCallsLabelled(
                DownloadManager.sessionCreationLabelPrefix
            ),
            timeout: 0.1,
            queueLabel: "7dgx.test.creation-refused.\(UUID().uuidString)"
        )
    }

    /// Refuses ONLY `downloadTask(with:)`. Reaches path B.
    ///
    /// `defaultTimeout` rather than 0.1 deliberately, on the precedent in
    /// `BackgroundSessionIOTests`: with a 0.1 s bound the CREATION crossing
    /// misses its own deadline on a loaded box and the run silently takes path
    /// A instead — a rail reporting green while testing the wrong branch. The
    /// bound costs no wall clock, because `refusesCallsLabelled` refuses
    /// synchronously and nothing here ever waits one out.
    private static func taskRefusingIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .refusesCallsLabelled("downloadTask(with:) for"),
            timeout: nonDefaultBound,
            queueLabel: "7dgx.test.task-refused.\(UUID().uuidString)"
        )
    }

    /// Deliberately NOT `BackgroundSessionIO.defaultTimeout`.
    ///
    /// Asserting `row.boundSeconds == BackgroundSessionIO.defaultTimeout`
    /// compares the shipped constant against itself and cannot tell a recorded
    /// bound from a hardcoded one. 7.5 s is distinguishable from 10, and is
    /// generous enough that the real session CONSTRUCTION these rails need to
    /// succeed does not become a latency measurement of `nsurlsessiond`. It is
    /// never waited out: `refusesCallsLabelled` refuses synchronously.
    private static let nonDefaultBound: TimeInterval = 7.5

    /// Refuses ONLY `resume()`. Reaches path C.
    private static func resumeRefusingIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .refusesCallsLabelled("resume() for"),
            timeout: nonDefaultBound,
            queueLabel: "7dgx.test.resume-refused.\(UUID().uuidString)"
        )
    }

    /// Refuses nothing — production behaviour on a private queue.
    ///
    /// The bound is THREE TIMES the shipped one, and that is anti-flake design
    /// rather than a workaround. This is the only rail here whose pass depends
    /// on `nsurlsessiond` genuinely answering; at 10 s under a saturated box
    /// the construction can miss its deadline, the run silently takes the
    /// session-refusal branch instead, and the anti-vacuity rail fails while
    /// nothing is wrong in production — the exact failure
    /// `BackgroundSessionIOTests` recorded at 0.1 s. A wider bound is never
    /// WAITED on when the daemon is healthy; it only stops the box's load from
    /// choosing which branch runs.
    private static func answeringIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .dedicatedThread,
            timeout: 30,
            queueLabel: "7dgx.test.answering.\(UUID().uuidString)"
        )
    }

    private static func manager(
        cacheDirectory: URL,
        store: AnalysisStore,
        sessionIO: BackgroundSessionIO
    ) -> DownloadManager {
        DownloadManager(
            cacheDirectory: cacheDirectory,
            sessionIO: sessionIO,
            dropRecorder: AnalysisStoreBackgroundDownloadDropRecorder(store: store)
        )
    }

    private static func context(
        podcastId: String = "show-7dgx",
        isExplicitDownload: Bool = false
    ) -> DownloadContext {
        DownloadContext(
            podcastId: podcastId,
            isExplicitDownload: isExplicitDownload,
            podcastTitle: "Show",
            episodeTitle: "Episode"
        )
    }

    private static func drive(
        _ manager: DownloadManager,
        episodeId: String,
        context: DownloadContext
    ) async {
        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
            context: context
        )
    }

    // MARK: - 1. Each path writes a row, and the three are told apart

    @Test("a session the daemon will not vend leaves a row naming THAT bound")
    func sessionRefusalIsRecorded() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let dir = try makeTempDir(prefix: "7dgxSessionRefusal")
        let manager = Self.manager(
            cacheDirectory: dir, store: store, sessionIO: Self.creationRefusingIO()
        )
        try await manager.bootstrap()

        await Self.drive(manager, episodeId: "ep-A", context: Self.context())

        let ledger = try await store.fetchBackgroundDownloadDrops()
        #expect(ledger.unrecognizedReasonRows == 0)
        #expect(ledger.rows.count == 1, "exactly one drop, or the table counts something else")
        let row = try #require(ledger.rows.first)
        #expect(row.episodeId == "ep-A")
        #expect(row.reason == .sessionNotVended)
        // The bound travels from the INJECTED io, not from a hardcoded
        // constant — otherwise the column would report the shipped default on
        // a device whose bound had been changed, which is the whole reason it
        // is recorded rather than assumed.
        #expect(row.boundSeconds == 0.1)
        // Nothing was memoized, so this really is the no-session branch.
        #expect(await manager.instantiatedSessionIdentifiersForTesting().isEmpty)
    }

    @Test("a downloadTask(with:) the daemon never answers leaves a row naming THAT bound")
    func taskRefusalIsRecorded() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let dir = try makeTempDir(prefix: "7dgxTaskRefusal")
        let manager = Self.manager(
            cacheDirectory: dir, store: store, sessionIO: Self.taskRefusingIO()
        )
        try await manager.bootstrap()

        await Self.drive(manager, episodeId: "ep-B", context: Self.context())

        let ledger = try await store.fetchBackgroundDownloadDrops()
        #expect(ledger.rows.count == 1)
        let row = try #require(ledger.rows.first)
        #expect(row.reason == .transferTaskNotVended)
        #expect(
            row.boundSeconds == Self.nonDefaultBound,
            "the recorded bound must be the INJECTED one; comparing against the shipped default compares a constant with itself"
        )
        // The seam landed where it was aimed: a session WAS vended, which is
        // the whole difference between this row and the one above.
        #expect(await manager.instantiatedSessionIdentifiersForTesting().isEmpty == false)
        #expect(await manager._backgroundDownloadAdmissionCountForTesting() == 0)
        await manager.invalidateBackgroundSessionsForTesting()
    }

    @Test("a transfer created but never resumed leaves a row naming THAT bound")
    func resumeRefusalIsRecorded() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let dir = try makeTempDir(prefix: "7dgxResumeRefusal")
        let manager = Self.manager(
            cacheDirectory: dir, store: store, sessionIO: Self.resumeRefusingIO()
        )
        try await manager.bootstrap()

        await Self.drive(manager, episodeId: "ep-C", context: Self.context())

        // The task really WAS admitted — this is the abandon path, not either
        // of the never-started ones.
        #expect(await manager._backgroundDownloadAdmissionCountForTesting() == 1)
        let ledger = try await store.fetchBackgroundDownloadDrops()
        #expect(ledger.rows.count == 1)
        let row = try #require(ledger.rows.first)
        #expect(row.reason == .transferNotResumed)
        // The name of this rail says "naming THAT bound", so it has to read it.
        #expect(row.boundSeconds == Self.nonDefaultBound)
        await manager.invalidateBackgroundSessionsForTesting()
    }

    /// THE ACCEPTANCE RAIL: two drops, two different bounds missed, and the
    /// rows say WHICH. A ledger that recorded both as "the daemon was slow"
    /// would satisfy every rail above and still be useless — the two failures
    /// have unrelated remedies (a refused session is a per-LAUNCH outage of the
    /// whole download subsystem; a refused task is one episode).
    @Test("the two bounds produce DISTINGUISHABLE rows in one store")
    func theTwoBoundsAreDistinguishable() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()

        let dirA = try makeTempDir(prefix: "7dgxBothA")
        let managerA = Self.manager(
            cacheDirectory: dirA, store: store, sessionIO: Self.creationRefusingIO()
        )
        try await managerA.bootstrap()
        await Self.drive(managerA, episodeId: "ep-session", context: Self.context())

        let dirB = try makeTempDir(prefix: "7dgxBothB")
        let managerB = Self.manager(
            cacheDirectory: dirB, store: store, sessionIO: Self.taskRefusingIO()
        )
        try await managerB.bootstrap()
        await Self.drive(managerB, episodeId: "ep-task", context: Self.context())

        let ledger = try await store.fetchBackgroundDownloadDrops()
        #expect(ledger.rows.count == 2)
        let byEpisode = Dictionary(
            uniqueKeysWithValues: ledger.rows.map { ($0.episodeId, $0.reason) }
        )
        #expect(byEpisode["ep-session"] == .sessionNotVended)
        #expect(byEpisode["ep-task"] == .transferTaskNotVended)
        #expect(
            byEpisode["ep-session"] != byEpisode["ep-task"],
            "if these ever collapse to one reason the table stops answering the only question it exists for"
        )
        // MOST RECENT FIRST, and `ep-task` was driven second. Nothing else in
        // this suite reads the order, so without this the `ORDER BY occurredAt
        // DESC` — and `occurredAt` itself — could be anything at all.
        #expect(ledger.rows.map(\.episodeId) == ["ep-task", "ep-session"])
        let newest = try #require(ledger.rows.first).occurredAt
        let oldest = try #require(ledger.rows.last).occurredAt
        #expect(
            newest >= oldest,
            "a stamp that is not the time of the drop makes every ordering above a coincidence"
        )
        #expect(newest > 0, "occurredAt must be the time of the drop, not a placeholder")
        #expect(ledger.truncated == false)
        #expect(ledger.totalRowsSeen == 2)

        // And the window REPORTS when it stops at its own ceiling. A page that
        // returned the newest row silently would say "this is what happened"
        // while meaning "this is what fitted".
        let capped = try await store.fetchBackgroundDownloadDrops(limit: 1)
        #expect(capped.rows.count == 1)
        #expect(capped.rows.first?.episodeId == "ep-task")
        #expect(capped.truncated)

        await managerB.invalidateBackgroundSessionsForTesting()
    }

    // MARK: - 2. It survives process death

    /// The point of the whole bead. The row is written through one
    /// `AnalysisStore` handle and read through a SECOND one opened on the same
    /// directory — the closest a unit test gets to "the app was killed and
    /// relaunched", and strictly stronger than reading back through the writer,
    /// which an in-memory cache would also satisfy.
    @Test("the row is on disk, not in memory — a second store on the same file reads it")
    func theRowSurvivesTheProcessThatWroteIt() async throws {
        let dir = try makeTempDir(prefix: "7dgxDurable")
        AnalysisStore.resetMigratedPathsForTesting()
        let writer = try AnalysisStore(directory: dir)
        try await writer.migrate()
        TestScratchReaper.shared.adopt(dir, owner: writer)

        let cache = try makeTempDir(prefix: "7dgxDurableCache")
        let manager = Self.manager(
            cacheDirectory: cache, store: writer, sessionIO: Self.creationRefusingIO()
        )
        try await manager.bootstrap()
        // In production this call is `PlayheadRuntime`'s, made after the store
        // is known open. Here it stands for one launch.
        await manager.armDropLedger()
        await Self.drive(
            manager,
            episodeId: "ep-durable",
            context: Self.context(podcastId: "show-durable", isExplicitDownload: true)
        )

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        let ledger = try await reopened.fetchBackgroundDownloadDrops()
        #expect(ledger.rows.count == 1)
        let row = try #require(ledger.rows.first)
        #expect(row.episodeId == "ep-durable")
        #expect(row.reason == .sessionNotVended)
        // "to whom", carried across the reopen.
        #expect(row.podcastId == "show-durable")
        #expect(row.unattributedReason == nil)
        #expect(row.isExplicitDownload)
        // And the arming claim survives too, or a pull could read a durable
        // drop beside a census that says nobody was counting.
        let arming = try #require(try await reopened.fetchBackgroundDownloadDropArming())
        #expect(arming.armedLaunches == 1)
    }

    // MARK: - 3. To whom

    /// A drop the caller could not attribute records the MEASURED absence, not
    /// a blank. `DownloadContext` has no nil-taking initializer, so a null
    /// `podcastId` here always carries the reason it is null — and reading that
    /// null as "no show" rather than as "nobody said" is the pooling defect
    /// playhead-kkzu was filed for, one table over.
    @Test("an unattributed download records WHY it carries no show")
    func anUnattributedDropCarriesItsReason() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let dir = try makeTempDir(prefix: "7dgxUnattributed")
        let manager = Self.manager(
            cacheDirectory: dir, store: store, sessionIO: Self.creationRefusingIO()
        )
        try await manager.bootstrap()

        await Self.drive(
            manager,
            episodeId: "ep-unattributed",
            // An empty identity is not a show: `DownloadContext` canonicalises
            // it to a NAMED absence rather than to `""`.
            context: DownloadContext(podcastId: "", isExplicitDownload: false)
        )

        let ledger = try await store.fetchBackgroundDownloadDrops()
        let row = try #require(ledger.rows.first)
        #expect(row.podcastId == nil)
        #expect(row.unattributedReason == .showIdentityUnresolvable)
        #expect(row.isExplicitDownload == false)
    }

    // MARK: - 4. Anti-vacuity: the table counts DROPS and nothing else

    /// Without this rail every assertion above is satisfied by a ledger that
    /// writes a row on every background-download REQUEST — which would make
    /// `count(*)` a request counter wearing a drop counter's name, i.e. this
    /// repo's standing defect class shipped as the instrument meant to catch it.
    ///
    /// Driven TWICE on purpose. The second call is refused by the idempotence
    /// guard before the daemon is asked anything, so it covers the other
    /// direction as well: a return that never reached a bound is not a drop.
    @Test("a download the daemon answers writes NO row, and neither does a duplicate request")
    func aHealthyDownloadWritesNothing() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let dir = try makeTempDir(prefix: "7dgxHealthy")
        let manager = Self.manager(
            cacheDirectory: dir, store: store, sessionIO: Self.answeringIO()
        )
        try await manager.bootstrap()

        await Self.drive(manager, episodeId: "ep-healthy", context: Self.context())
        // The transfer really was admitted — otherwise the empty ledger below
        // would prove nothing more than that the download never happened.
        #expect(await manager._backgroundDownloadAdmissionCountForTesting() == 1)
        #expect(await manager._isBackgroundDownloadInFlightForTesting(episodeId: "ep-healthy"))

        await Self.drive(manager, episodeId: "ep-healthy", context: Self.context())
        #expect(
            await manager._backgroundDownloadAdmissionCountForTesting() == 1,
            "the duplicate must have been refused by the in-flight guard, not started"
        )

        // The fetch happens AFTER the cancel on purpose. `cancelDownload` also
        // deletes the attribution sidecar, so it is the likeliest place for a
        // later contributor to add a fourth `recordBackgroundDownloadDrop` —
        // and a fetch taken before it could not see that at all.
        await manager.cancelDownload(episodeId: "ep-healthy")
        let ledger = try await store.fetchBackgroundDownloadDrops()
        #expect(ledger.rows.isEmpty)
        #expect(ledger.totalRowsSeen == 0)
        await manager.invalidateBackgroundSessionsForTesting()
    }

    // MARK: - 5. UNKNOWN IS NOT ZERO

    /// A migrated store that nobody armed reads `armedLaunches == 0` with a
    /// NULL first-armed time. That is the state the arming row exists to make
    /// expressible: without it, "the recorder was never installed" and "the
    /// recorder was installed and saw no drops" are the same empty table, and
    /// the honest reading of the second — a positive claim — is unavailable.
    @Test("a migrated but never-armed store says so: zero launches and a NULL first-armed time")
    func aNeverArmedStoreSaysSo() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let fetched = try await store.fetchBackgroundDownloadDropArming()
        let arming = try #require(
            fetched,
            "the V62 rung must SEED the arming row; an absent row and a zeroed row are different claims"
        )
        #expect(arming.armedLaunches == 0)
        #expect(arming.firstArmedAt == nil, "a zero here would date an arming that never happened")
        #expect(arming.lastArmedAt == nil)
        #expect(arming.installedAt > 0)
    }

    @Test("arming counts a launch, and a second launch counts again without moving firstArmedAt")
    func armingCountsEachLaunch() async throws {
        let dir = try makeTempDir(prefix: "7dgxArming")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        let recorder = AnalysisStoreBackgroundDownloadDropRecorder(store: store)

        let first = DownloadManager(
            cacheDirectory: try makeTempDir(prefix: "7dgxArm1"),
            sessionIO: Self.creationRefusingIO(),
            dropRecorder: recorder
        )
        await first.armDropLedger()
        let afterOneFetch = try await store.fetchBackgroundDownloadDropArming()
        let afterOne = try #require(afterOneFetch)
        #expect(afterOne.armedLaunches == 1)
        let firstArmedAt = try #require(afterOne.firstArmedAt)
        #expect(afterOne.lastArmedAt == firstArmedAt)

        let second = DownloadManager(
            cacheDirectory: try makeTempDir(prefix: "7dgxArm2"),
            sessionIO: Self.creationRefusingIO(),
            dropRecorder: recorder
        )
        await second.armDropLedger()
        let afterTwoFetch = try await store.fetchBackgroundDownloadDropArming()
        let afterTwo = try #require(afterTwoFetch)
        #expect(afterTwo.armedLaunches == 2)
        #expect(
            afterTwo.firstArmedAt == firstArmedAt,
            "firstArmedAt means THE FIRST TIME; if it follows the latest arming it is a second lastArmedAt under a misleading name"
        )
        let lastArmedAt = try #require(afterTwo.lastArmedAt)
        #expect(lastArmedAt >= firstArmedAt)
    }

    /// The DEFAULT recorder records nothing, and that has to stay visible.
    /// A `DownloadManager` built without an explicit recorder must leave the
    /// arming row at zero — so a device pull showing `armedLaunches = 0`
    /// really does mean nobody was counting, rather than meaning the count
    /// happened somewhere else.
    /// THE DEFAULT IS THE NO-OP, asserted on the manager itself.
    ///
    /// An earlier version of this rail built an unwired manager, drove a drop,
    /// and then asserted an UNRELATED store was empty — a store that manager
    /// had no reference to, and which is empty whatever the default does. It
    /// could not fail for the reason its name gave, which is the exact defect
    /// this suite exists to guard the product against, living in the suite.
    ///
    /// The type assertion is compile-enforced rather than mutation-provable:
    /// any edit to the default that changed its TYPE would not compile without
    /// a store in scope, so no one-line mutant can reach it. That is why the
    /// assertion is about identity and not about behaviour.
    @Test("the default recorder is the no-op, and the no-op reports that nothing landed")
    func theDefaultRecorderIsTheNoOp() async throws {
        let dir = try makeTempDir(prefix: "7dgxDefaultRecorder")
        let manager = DownloadManager(
            cacheDirectory: dir,
            sessionIO: Self.creationRefusingIO()
        )
        #expect(
            await manager.dropRecorder is NoopBackgroundDownloadDropRecorder,
            "a production manager holding this is a defect; a TEST manager holding it is the default, and the wiring canary is what tells them apart"
        )

        // And the no-op must REPORT that nothing landed. A no-op that returned
        // `true` would make `DownloadManager` skip the surface-status fallback,
        // so an unwired build would lose the drop on both media at once.
        let landed = await NoopBackgroundDownloadDropRecorder().recordDrop(
            BackgroundDownloadDropRecord(
                episodeId: "ep-noop",
                reason: .sessionNotVended,
                context: Self.context(),
                boundSeconds: 1
            )
        )
        #expect(landed == false)
    }

    /// A DROP THE DATABASE COULD NOT HOLD IS THE INSTRUMENT'S OWN BLIND SPOT,
    /// and this is the rail for it.
    ///
    /// `armedLaunches > 0` beside zero rows is the ledger's strongest claim. A
    /// store that simply cannot be written to produces that exact state by
    /// silence — `SQLITE_FULL`, or a background relaunch before first unlock,
    /// since `analysis.sqlite` is `completeUntilFirstUserAuthentication`. So
    /// the failure has to leave two traces, in two independent media, and this
    /// asserts both.
    @Test("a drop whose row cannot be written is counted AND raised on the second medium")
    func aFailedDropWriteIsCountedAndRaised() async throws {
        let dir = try makeTempDir(prefix: "7dgxWriteFailure")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)

        // Break exactly the drops table, behind the store's back, leaving the
        // arming row intact — the shape a partial write failure has.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        sqlite3_busy_timeout(db, 3000)
        #expect(sqlite3_exec(db, "DROP TABLE background_download_drops", nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        let recording = RecordedInvariantViolations()
        let manager = DownloadManager(
            cacheDirectory: try makeTempDir(prefix: "7dgxWriteFailureCache"),
            sessionIO: Self.creationRefusingIO(),
            invariantRecorder: recording.recorder,
            dropRecorder: AnalysisStoreBackgroundDownloadDropRecorder(store: store)
        )
        try await manager.bootstrap()
        await manager.armDropLedger()
        await Self.drive(manager, episodeId: "ep-unwritable", context: Self.context())

        // Medium one: the arming row's failure counter, in the same database.
        let fetched = try await store.fetchBackgroundDownloadDropArming()
        let arming = try #require(fetched)
        #expect(arming.armedLaunches == 1)
        #expect(
            arming.dropWriteFailures == 1,
            "without this, `armedLaunches = 1, rows = 0` is byte-identical to a launch that armed and saw no drop"
        )

        // Medium two: the surface-status stream, which is a JSON Lines file
        // under Caches/ rather than this database — so the two can only go
        // quiet together if the device has no writable storage at all.
        let raised = recording.unrecordedDrops
        #expect(raised.count == 1)
        let description = try #require(raised.first)
        #expect(description.contains("episodeId=ep-unwritable"))
        #expect(description.contains("reason=session_not_vended"))
    }

    /// The arming row's `nil` contract, which nothing else reaches.
    ///
    /// Every other rail here runs against a store whose arming row the V62
    /// rung seeded, so the `else` branch of `fetchBackgroundDownloadDropArming`
    /// is never taken — and `aNeverArmedStoreSaysSo` is killed entirely through
    /// its `#require`, which means the seed's own rail rests on an untested
    /// branch. A reader synthesizing a zeroed row here would erase the
    /// difference between "the migration ran and nothing else did" and "nobody
    /// has ever opened this table".
    @Test("an absent arming row reads as nil, never as a synthesized zero")
    func anAbsentArmingRowReadsAsNil() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        _ = try await store.fetchBackgroundDownloadDropArming()

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        defer { sqlite3_close_v2(db) }
        sqlite3_busy_timeout(db, 3000)
        #expect(sqlite3_exec(db, "DELETE FROM background_download_drop_arming", nil, nil, nil) == SQLITE_OK)

        #expect(try await store.fetchBackgroundDownloadDropArming() == nil)

        // And the arming writer REBUILDS it rather than counting into a row
        // that is not there — the branch that makes a hand-edited or
        // partially-rolled-back store still countable.
        try await store.noteBackgroundDownloadDropInstrumentArmed(at: 1234.0)
        let rebuilt = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(rebuilt.armedLaunches == 1)
        #expect(rebuilt.firstArmedAt == 1234.0)
        #expect(rebuilt.dropWriteFailures == 0)
    }

    /// THE RAW VALUES ARE A SCHEMA. Every behavioural rail here writes and
    /// reads through the same enum, so a rename is invisible to all of them —
    /// while on a device it silently converts every historical row into an
    /// `unrecognizedReasonRows` count. These strings are pinned byte-exactly
    /// so that renaming one is a decision somebody has to make twice.
    @Test("the persisted reason strings are pinned — renaming one is a migration")
    func theReasonRawValuesArePinned() {
        #expect(BackgroundDownloadDropReason.sessionNotVended.rawValue == "session_not_vended")
        #expect(BackgroundDownloadDropReason.transferTaskNotVended.rawValue == "transfer_task_not_vended")
        #expect(BackgroundDownloadDropReason.transferNotResumed.rawValue == "transfer_not_resumed")
        #expect(
            BackgroundDownloadDropReason.allCases.count == 3,
            "a fourth case needs a fourth pin here and a fourth site in backgroundDownload"
        )
    }

    /// A REPEAT DROP FOR THE SAME EPISODE IS THE POINT OF THE TABLE, and until
    /// this rail nothing exercised it: every other test uses distinct episode
    /// ids, so a primary key that collapsed same-episode rows would pass them
    /// all. The record's own doc calls this "the single most interesting thing
    /// this table can show".
    @Test("two drops for the SAME episode are two rows, not one")
    func repeatedDropsForOneEpisodeAccumulate() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        // Two managers, because the in-flight guard is per-manager — this is
        // one episode failing on two launches, which is the field shape.
        for attempt in 1...2 {
            let manager = Self.manager(
                cacheDirectory: try makeTempDir(prefix: "7dgxRepeat\(attempt)"),
                store: store,
                sessionIO: Self.creationRefusingIO()
            )
            try await manager.bootstrap()
            await Self.drive(manager, episodeId: "ep-repeat", context: Self.context())
        }

        let ledger = try await store.fetchBackgroundDownloadDrops()
        #expect(ledger.rows.count == 2)
        #expect(ledger.rows.allSatisfy { $0.episodeId == "ep-repeat" })
        #expect(
            Set(ledger.rows.map(\.id)).count == 2,
            "a shared primary key would swallow the second row into the best-effort catch and the ledger would report one drop no matter how many happened"
        )
    }

    // MARK: - 6. A reason this build cannot read is REPORTED, not coerced

    /// A row written by a build with a wider vocabulary must not be folded
    /// into a case this build happens to know — that would inflate whichever
    /// population the fallback names, silently, by exactly the rows a reader
    /// most needs to notice. It is skipped AND counted.
    @Test("an unrecognized reason is skipped and counted, never coerced into a known case")
    func anUnknownReasonIsReportedRatherThanCoerced() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        // Force the store to open and migrate before writing behind its back.
        _ = try await store.fetchBackgroundDownloadDrops()

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        defer { sqlite3_close_v2(db) }
        // The store's own connection is live on this file; without a matching
        // busy timeout a concurrent write returns SQLITE_BUSY at once instead
        // of waiting, and the test fails for a reason that is not the subject.
        sqlite3_busy_timeout(db, 3000)
        let sql = """
            INSERT INTO background_download_drops
            (id, episodeId, reason, occurredAt, podcastId, unattributedReason,
             isExplicitDownload, boundSeconds)
            VALUES ('row-future', 'ep-future', 'a_reason_from_2027', 1.0, NULL, NULL, 0, 10.0)
            """
        #expect(sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK)

        let ledger = try await store.fetchBackgroundDownloadDrops()
        #expect(ledger.rows.isEmpty, "an unreadable row must not be materialized as some other reason")
        #expect(
            ledger.unrecognizedReasonRows == 1,
            "…and it must not vanish either: 'three drops' and 'three drops I can read' are different claims"
        )
        #expect(ledger.unrecognizedUnattributedReasonRows == 0, "the two losses are counted separately")
        #expect(ledger.totalRowsSeen == 1)
    }

    /// THE SAME RULE ONE COLUMN ALONG, and it has to be the same rule.
    ///
    /// `reason` is deliberately skipped-and-counted when this build cannot
    /// decode it. An earlier cut mapped an undecodable `unattributedReason` to
    /// `nil` three declarations later — which on a row whose `podcastId` is
    /// NULL says "the caller named a show", the exact opposite of what the row
    /// records, and precisely the measured-absence-versus-unnoticed-absence
    /// distinction `DownloadContext` was built to preserve.
    @Test("an unrecognized unattributedReason is counted too, never read as an attributed row")
    func anUnknownUnattributedReasonIsReportedRatherThanCoerced() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        _ = try await store.fetchBackgroundDownloadDrops()

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        defer { sqlite3_close_v2(db) }
        sqlite3_busy_timeout(db, 3000)
        let sql = """
            INSERT INTO background_download_drops
            (id, episodeId, reason, occurredAt, podcastId, unattributedReason,
             isExplicitDownload, boundSeconds)
            VALUES ('row-future-2', 'ep-future-2', 'session_not_vended', 1.0, NULL,
                    'a_reason_from_2027', 0, 10.0)
            """
        #expect(sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK)

        let ledger = try await store.fetchBackgroundDownloadDrops()
        #expect(
            ledger.rows.isEmpty,
            "a row whose show-absence cannot be explained must not come back saying the show WAS named"
        )
        #expect(ledger.unrecognizedUnattributedReasonRows == 1)
        #expect(ledger.unrecognizedReasonRows == 0, "the reason itself decoded fine; only the attribution did not")
        #expect(ledger.totalRowsSeen == 1)
    }
}
