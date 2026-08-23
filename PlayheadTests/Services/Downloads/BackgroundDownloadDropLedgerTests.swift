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
// NO RAIL HERE USES `BackgroundSessionIO.shared`. The shared serial queue is a
// measured head-of-line hazard across concurrently-running suites
// (playhead-et2d, seven gate-baseline entries), so every seam below owns a
// uniquely-labelled queue.

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
            timeout: BackgroundSessionIO.defaultTimeout,
            queueLabel: "7dgx.test.task-refused.\(UUID().uuidString)"
        )
    }

    /// Refuses ONLY `resume()`. Reaches path C.
    private static func resumeRefusingIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .refusesCallsLabelled("resume() for"),
            timeout: BackgroundSessionIO.defaultTimeout,
            queueLabel: "7dgx.test.resume-refused.\(UUID().uuidString)"
        )
    }

    /// Refuses nothing — production behaviour on a private queue.
    private static func answeringIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .dedicatedThread,
            timeout: BackgroundSessionIO.defaultTimeout,
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
        #expect(row.boundSeconds == BackgroundSessionIO.defaultTimeout)
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

        let cache = try makeTempDir(prefix: "7dgxDurableCache")
        let manager = Self.manager(
            cacheDirectory: cache, store: writer, sessionIO: Self.creationRefusingIO()
        )
        try await manager.bootstrap()
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

        let ledger = try await store.fetchBackgroundDownloadDrops()
        #expect(ledger.rows.isEmpty)
        await manager.cancelDownload(episodeId: "ep-healthy")
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

    @Test("bootstrap arms the ledger, and a second launch counts again without moving firstArmedAt")
    func bootstrapArmsTheLedger() async throws {
        let dir = try makeTempDir(prefix: "7dgxArming")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let recorder = AnalysisStoreBackgroundDownloadDropRecorder(store: store)

        let first = DownloadManager(
            cacheDirectory: try makeTempDir(prefix: "7dgxArm1"),
            dropRecorder: recorder
        )
        try await first.bootstrap()
        let afterOneFetch = try await store.fetchBackgroundDownloadDropArming()
        let afterOne = try #require(afterOneFetch)
        #expect(afterOne.armedLaunches == 1)
        let firstArmedAt = try #require(afterOne.firstArmedAt)
        #expect(afterOne.lastArmedAt == firstArmedAt)

        let second = DownloadManager(
            cacheDirectory: try makeTempDir(prefix: "7dgxArm2"),
            dropRecorder: recorder
        )
        try await second.bootstrap()
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
    @Test("the default recorder is a genuine no-op: it neither arms nor records")
    func theDefaultRecorderRecordsNothing() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let dir = try makeTempDir(prefix: "7dgxDefaultRecorder")
        let manager = DownloadManager(
            cacheDirectory: dir,
            sessionIO: Self.creationRefusingIO()
        )
        try await manager.bootstrap()
        await Self.drive(manager, episodeId: "ep-unwired", context: Self.context())

        let fetched = try await store.fetchBackgroundDownloadDropArming()
        let arming = try #require(fetched)
        #expect(arming.armedLaunches == 0)
        let ledger = try await store.fetchBackgroundDownloadDrops()
        #expect(ledger.rows.isEmpty)
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
    }
}
