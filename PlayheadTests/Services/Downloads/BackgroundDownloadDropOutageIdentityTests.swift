// BackgroundDownloadDropOutageIdentityTests.swift
// playhead-sdis — ONE daemon OUTAGE stops being indistinguishable from FORTY
// EPISODES.
//
// playhead-7dgx shipped a table whose only count was `count(*)`, which counts
// EPISODES AFFECTED. Its own retry recommendation was written against that
// table claiming a `session_not_vended` refusal is a per-LAUNCH outage whose
// right retry unit is the launch rather than the episode — and nothing in the
// table could falsify it. These rails are what the columns bought.
//
// WHY `launchId` ALONE IS NOT ENOUGH, which is the finding this suite exists
// to make measurable rather than argued. A refusal is NEVER CACHED
// (`_sessionsByRole` is written only on success), so ONE launch holds MANY
// refusals — `sequentialRefusalsInOneLaunchAreThreeDistinctCrossings` is the
// rail, and it is deterministic. And ONE refusal mints N rows, because
// `backgroundSession(for:requestedBy:)` lets concurrent callers JOIN an
// in-flight crossing — `concurrentJoinersOfOneCrossingShareOneCrossingId` is
// that rail, and it is the money rail of the bead.
//
// THE TWO RAILS ARE MIRRORS AND BOTH ARE NEEDED. One launch / three crossings
// and one crossing / three episodes are the two readings `count(*)` collapses,
// and a suite carrying only one of them proves the column is WRITTEN without
// proving it DISCRIMINATES.
//
// SESSION HYGIENE, on `BackgroundDownloadDropLedgerTests`' precedent: the
// background-session IDENTIFIERS are process-wide constants, so any rail that
// constructs a real `URLSessionConfiguration.background(withIdentifier:)`
// shares that resource with three neighbouring suites under the parallel plan.
//
// AND TWO OF THE FOUR SEAMS REALLY DO CONSTRUCT ONE. Say it plainly, because
// an earlier version of this paragraph claimed "every rail here refuses the
// CREATION crossing, so no real session is ever vended" and that has never
// been true: `taskRefusingIO` and `resumeRefusingIO` refuse a DOWNSTREAM call
// and let the creation succeed, by construction — a rail that reaches
// `transferTaskNotVended` has to get past the session first. That is exactly
// why every rail here calls `invalidateBackgroundSessionsForTesting()`:
// `discardingLateResult` is what hands a late session back to be cancelled,
// and a live session left registered on that identifier is what the next
// construction collides with.
//
// EXACTLY ONE CLOCK IS LOAD-BEARING AND IT IS `downstreamBound`. The join
// rail's clock is gone (an earlier version stranded the crossing on a held
// queue and let a 6 s deadline expire, making it a race against the test's own
// arrival barrier — see `suspendingRefusalIO`). What is left is the bound the
// two downstream seams give the calls that must SUCCEED, and the 2026-08-26
// merge gate is why it is no longer 7.5 s.
//
// `waited(until:)` carries a deadline too, and the distinction is worth being
// exact about rather than claiming there is no second clock: its Bool IS
// asserted, so it is not "never an input to an assertion". What makes it not
// load-bearing is the DIRECTION — it waits for an event both a correct
// implementation and a broken one reach, so on any build that arrives at all
// the deadline is unreachable, and the only run it can decide is one that was
// going to hang.

import Foundation
import Testing
import SQLite3
@testable import Playhead

@Suite("DownloadManager – one outage is not forty episodes (playhead-sdis)")
struct BackgroundDownloadDropOutageIdentityTests {

    // MARK: - Seams

    /// Refuses ONLY the session-construction crossing, SYNCHRONOUSLY. Every
    /// call mints its own crossing, which is exactly what the
    /// sequential-refusal rail needs.
    private static func creationRefusingIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .refusesCallsLabelled(
                DownloadManager.sessionCreationLabelPrefix
            ),
            timeout: 0.1,
            queueLabel: "sdis.test.creation-refused.\(UUID().uuidString)"
        )
    }

    /// The bound the two DOWNSTREAM seams give the calls that must SUCCEED.
    ///
    /// IT IS NEVER WAITED OUT. `refusesCallsLabelled` short-circuits before the
    /// queue is touched and returns instantly, so the refused call never
    /// approaches it. What this number actually bounds is the calls the rail
    /// needs to GET PAST — a real
    /// `URLSessionConfiguration.background(withIdentifier:)` plus
    /// `URLSession.init` for both seams, and a real `downloadTask(with:)` for
    /// the resume seam — which is why raising it costs no wall clock on a
    /// healthy run and everything on a saturated one.
    ///
    /// MEASURED, on the 2026-08-26 merge gate: at 7.5 s the resume rail's
    /// `downloadTask(with:)` missed this bound under the full plan (11,866
    /// tests, fd at 94.6 % of `RLIMIT_NOFILE`, 31 tests denied a file), so the
    /// drive landed on `transferTaskNotVended` and never reached `resume()`.
    /// The rail did not report a wrong answer — `row.reason == reason` caught
    /// it and said `resume: the drive must land on the path it aimed at` — but
    /// a rail whose aimed-at path depends on real system calls beating a
    /// wall-clock deadline while ~11,000 tests saturate the box is measuring
    /// the box. This is the quantity to raise, and it is the ONLY clock left in
    /// this file.
    private static let downstreamBound: TimeInterval = 120.0

    /// Refuses ONLY `downloadTask(with:)`. Reaches the `transferTaskNotVended`
    /// path, whose rows must carry NO crossing id.
    private static func taskRefusingIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .refusesCallsLabelled("downloadTask(with:) for"),
            timeout: downstreamBound,
            queueLabel: "sdis.test.task-refused.\(UUID().uuidString)"
        )
    }

    /// Refuses ONLY `resume()`. Reaches the `transferNotResumed` path.
    private static func resumeRefusingIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .refusesCallsLabelled("resume() for"),
            timeout: downstreamBound,
            queueLabel: "sdis.test.resume-refused.\(UUID().uuidString)"
        )
    }

    /// Refuses the session crossing only once `gate` opens — the JOINED-CROSSING
    /// population, and THE ONLY WAY IT IS REACHABLE.
    ///
    /// The join can only be exercised while the crossing is IN FLIGHT, and
    /// every OTHER refusing behaviour answers synchronously: `neverAnswers`,
    /// `refusesCallsLabelled` and `intermittentlyRefusesCallsLabelled` all
    /// short-circuit before the queue is touched and return instantly, so none
    /// of them leaves a window for a second caller to arrive in.
    ///
    /// THIS FILE USED TO HOLD THE CREATION QUEUE AND LET A 6 s DEADLINE
    /// EXPIRE, and that made the rail a RACE between the test's own arrival
    /// barrier and the bound — the shape the 2026-08-13 merge gate lost. The
    /// seam removes the clock entirely: the crossing stays in flight until the
    /// rail opens the gate, so a slow box makes the wait longer and can never
    /// change the answer. There is now NO wall-clock quantity in this file that
    /// any assertion depends on.
    private static func suspendingRefusalIO(
        gate: SuspendingSeamGate
    ) -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .suspendsThenRefusesCallsLabelled(
                DownloadManager.sessionCreationLabelPrefix,
                until: { await gate.wait() }
            ),
            timeout: 7.5,
            queueLabel: "sdis.test.joined-crossing.\(UUID().uuidString)"
        )
    }

    private static func manager(
        cacheDirectory: URL,
        store: AnalysisStore,
        sessionIO: BackgroundSessionIO,
        invariantRecorder: (@Sendable (InvariantViolation.Code, String) -> Void)? = nil
    ) -> DownloadManager {
        DownloadManager(
            cacheDirectory: cacheDirectory,
            sessionIO: sessionIO,
            invariantRecorder: invariantRecorder,
            dropRecorder: AnalysisStoreBackgroundDownloadDropRecorder(store: store)
        )
    }

    private static func context() -> DownloadContext {
        DownloadContext(
            podcastId: "show-sdis",
            isExplicitDownload: false,
            podcastTitle: "Show",
            episodeTitle: "Episode"
        )
    }

    private static func drive(_ manager: DownloadManager, episodeId: String) async {
        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
            context: context()
        )
    }

    /// Waits until `condition` holds, and reports whether it ever did. The
    /// deadline is a safety net so a broken build fails instead of hanging the
    /// plan, never an input to an assertion.
    private static func waited(until condition: () async -> Bool) async -> Bool {
        let deadline = ContinuousClock.now + .seconds(120)
        while ContinuousClock.now < deadline {
            if await condition() { return true }
            try? await Task.sleep(for: .milliseconds(2))
        }
        return await condition()
    }

    // MARK: - 1. Every row names the process that wrote it

    @Test("every drop row carries THIS manager's launch id, on all three paths")
    func everyDropRowCarriesTheWritingLaunchId() async throws {
        for (label, io, reason) in [
            ("session", Self.creationRefusingIO(), BackgroundDownloadDropReason.sessionNotVended),
            ("task", Self.taskRefusingIO(), .transferTaskNotVended),
            ("resume", Self.resumeRefusingIO(), .transferNotResumed),
        ] {
            let (store, _) = try await makeTestStoreWithDirectory()
            let dir = try makeTempDir(prefix: "sdisLaunchId\(label)")
            let manager = Self.manager(cacheDirectory: dir, store: store, sessionIO: io)
            try await manager.bootstrap()
            let launchId = await manager.launchId

            await Self.drive(manager, episodeId: "ep-\(label)")

            let page = try await store.fetchBackgroundDownloadDrops()
            #expect(page.rows.count == 1, "\(label): exactly one drop")
            let row = try #require(page.rows.first)
            #expect(row.reason == reason, "\(label): the drive must land on the path it aimed at")
            #expect(
                row.launchId == launchId,
                """
                \(label): the row must name the manager that wrote it. A row \
                whose launchId is anything else makes count(DISTINCT launchId) \
                a count of something other than launches.
                """
            )
            #expect(!launchId.isEmpty)
            await manager.invalidateBackgroundSessionsForTesting()
        }
    }

    /// The crossing id is NOT stamped on every row, and that asymmetry is a
    /// statement rather than an oversight. Only `sessionNotVended` rides a
    /// crossing other callers can join; the other two each follow a
    /// `sessionIO.perform` this caller submitted alone, so for them a row IS a
    /// call and `count(*)` already answers "how many bounded calls expired".
    @Test("only a session refusal carries a crossing id — the other two rows carry NONE")
    func onlyASessionRefusalCarriesACrossingId() async throws {
        let (sessionStore, _) = try await makeTestStoreWithDirectory()
        let sessionManager = Self.manager(
            cacheDirectory: try makeTempDir(prefix: "sdisCrossingYes"),
            store: sessionStore, sessionIO: Self.creationRefusingIO()
        )
        try await sessionManager.bootstrap()
        await Self.drive(sessionManager, episodeId: "ep-session")
        let sessionRow = try #require(
            try await sessionStore.fetchBackgroundDownloadDrops().rows.first
        )
        #expect(sessionRow.reason == .sessionNotVended)
        let crossing = try #require(
            sessionRow.sessionCrossingId,
            "a refusal reaches the drop site only THROUGH the crossing, so this can never be nil"
        )
        #expect(!crossing.isEmpty)
        await sessionManager.invalidateBackgroundSessionsForTesting()

        for (label, io) in [
            ("task", Self.taskRefusingIO()),
            ("resume", Self.resumeRefusingIO()),
        ] {
            let (store, dir) = try await makeTestStoreWithDirectory()
            let manager = Self.manager(
                cacheDirectory: try makeTempDir(prefix: "sdisCrossingNo\(label)"),
                store: store, sessionIO: io
            )
            try await manager.bootstrap()
            await Self.drive(manager, episodeId: "ep-\(label)")
            let row = try #require(try await store.fetchBackgroundDownloadDrops().rows.first)
            #expect(row.sessionCrossingId == nil, "\(label): nothing joins this submission")
            // …and it is NOT nil because the row predates V64. The
            // discriminator is `launchId`, and this is the pairing a device
            // pull has to read together.
            #expect(row.launchId != nil, "\(label): a post-V64 row always names its launch")

            // AND SQL DECLINES TO COUNT THE NULL, which is the half a Swift
            // read cannot show. A table holding only this row reports ZERO
            // daemon refusals — the number a reader wants — where a per-row
            // sentinel would have reported ONE and invented a refusal that
            // never happened. Same argument as `launchId`'s NULL, one column
            // along, and the reason `sessionCrossingId` is not "filled in".
            #expect(
                try Self.scalar(
                    dir,
                    """
                    SELECT count(DISTINCT sessionCrossingId)
                    FROM background_download_drops
                    """
                ) == 0,
                "\(label): no session refusal happened here at all, and the file must say zero"
            )
            await manager.invalidateBackgroundSessionsForTesting()
        }
    }

    /// One row, one column, as an `Int`, through a SECOND connection — the
    /// query a device pull runs. Throws rather than defaulting: a zero invented
    /// from a missing row is exactly the reading this bead exists to refuse.
    ///
    /// `SQLITE_OPEN_READWRITE` rather than `READONLY` because `AnalysisStore`
    /// sets `journal_mode = WAL`, and a WAL database admits a read-only
    /// connection only while some other connection holds the `-shm` — so a
    /// read-only probe would pass or fail depending on whether the store's own
    /// handle happened to still be up.
    private static func scalar(_ directory: URL, _ sql: String) throws -> Int {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
            throw NSError(
                domain: "SdisScalar", code: 1,
                userInfo: [NSLocalizedDescriptionKey: "could not open \(dbURL.path)"]
            )
        }
        defer { sqlite3_close_v2(db) }
        sqlite3_busy_timeout(db, 5000)
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(
                domain: "SdisScalar", code: 2,
                userInfo: [NSLocalizedDescriptionKey: String(cString: sqlite3_errmsg(db))]
            )
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else {
            throw NSError(
                domain: "SdisScalar", code: 3,
                userInfo: [NSLocalizedDescriptionKey: "`\(sql)` returned no row"]
            )
        }
        return Int(sqlite3_column_int64(stmt, 0))
    }

    // MARK: - 2. THE MIRROR RAILS — why `launchId` alone is not enough

    /// A REFUSAL IS NEVER CACHED, so one launch holds many refusals.
    ///
    /// This is the rail that refutes "a launch id is sufficient". Three drops,
    /// ONE launch, THREE crossings: `count(DISTINCT launchId)` reports 1 and
    /// `count(DISTINCT sessionCrossingId)` reports 3, and only the second is
    /// the number of times the daemon refused. It is fully deterministic —
    /// `refusesCallsLabelled` refuses synchronously, so the three drives cannot
    /// overlap and cannot share a crossing.
    @Test("three SEQUENTIAL refusals in ONE launch are three distinct crossings")
    func sequentialRefusalsInOneLaunchAreThreeDistinctCrossings() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let dir = try makeTempDir(prefix: "sdisSequential")
        let manager = Self.manager(
            cacheDirectory: dir, store: store, sessionIO: Self.creationRefusingIO()
        )
        try await manager.bootstrap()
        await manager.armDropLedger()
        let launchId = await manager.launchId

        for index in 1...3 {
            await Self.drive(manager, episodeId: "ep-seq-\(index)")
        }

        let page = try await store.fetchBackgroundDownloadDrops()
        #expect(page.rows.count == 3, "three episodes")
        #expect(Set(page.rows.map(\.launchId)) == [launchId], "ONE launch")
        let crossings = Set(page.rows.compactMap(\.sessionCrossingId))
        #expect(
            crossings.count == 3,
            """
            THREE refusals, and this is the reading `launchId` alone cannot \
            produce: one launch that retried three times is not one outage. \
            Saw \(crossings.count) distinct crossing(s) across 3 rows.
            """
        )
        await manager.invalidateBackgroundSessionsForTesting()
    }

    /// THE MONEY RAIL. One refusal, three episodes, ONE crossing id.
    ///
    /// Three callers reach the crossing decision while the construction is
    /// stranded on a held queue; two of them JOIN the first one's crossing
    /// rather than opening a second `URLSession` on the same background
    /// identifier. All three give up on the FIRST caller's deadline and all
    /// three write a row — and before this bead those rows shared no marker at
    /// all, which is precisely how `count(*) = 3` came to be read as three
    /// daemon refusals.
    ///
    /// TWO THINGS MUST BE TRUE AT ONCE or the rail is vacuous, and both are
    /// asserted rather than assumed:
    ///   1. the crossing is IN FLIGHT while the callers arrive — guaranteed by
    ///      construction, because the seam does not answer until this rail
    ///      opens the gate, and the gate is opened only after the barrier;
    ///   2. all three reach the crossing DECISION, which is an event both a
    ///      correct implementation and a join-less one reach
    ///      (`sessionCrossingArrivalsForTesting == 3`), so waiting for it is a
    ///      WAIT rather than a race.
    ///
    /// The third condition the earlier version of this rail carried — "the
    /// barrier completed inside the window" — is GONE, along with the window.
    /// See `suspendingRefusalIO`.
    @Test("three CONCURRENT joiners of ONE crossing write three rows sharing ONE crossing id")
    func concurrentJoinersOfOneCrossingShareOneCrossingId() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let dir = try makeTempDir(prefix: "sdisJoin")
        let gate = SuspendingSeamGate()
        let manager = Self.manager(
            cacheDirectory: dir, store: store,
            sessionIO: Self.suspendingRefusalIO(gate: gate)
        )
        try await manager.bootstrap()
        await manager.armDropLedger()
        let launchId = await manager.launchId

        async let first: Void = Self.drive(manager, episodeId: "ep-join-1")
        async let second: Void = Self.drive(manager, episodeId: "ep-join-2")
        async let third: Void = Self.drive(manager, episodeId: "ep-join-3")

        // Read the arrivals while the crossing is STILL HELD, so the number
        // describes the barrier rather than whatever survived the gate.
        let allArrived = await Self.waited {
            await manager.sessionCrossingArrivalsForTesting == 3
        }
        let arrivals = await manager.sessionCrossingArrivalsForTesting
        await gate.open()
        _ = await (first, second, third)

        #expect(
            allArrived,
            "the rail is vacuous unless all three callers reached the crossing decision: \(arrivals) of 3"
        )

        let page = try await store.fetchBackgroundDownloadDrops()
        #expect(page.rows.count == 3, "three episodes were abandoned")
        #expect(page.rows.allSatisfy { $0.reason == .sessionNotVended })
        #expect(Set(page.rows.map(\.launchId)) == [launchId])
        let crossings = Set(page.rows.compactMap(\.sessionCrossingId))
        #expect(
            crossings.count == 1,
            """
            ONE daemon refusal cost three episodes, so the three rows must \
            share ONE crossing id. \(crossings.count) distinct value(s) means \
            each joiner minted its own, and `count(DISTINCT sessionCrossingId)` \
            goes back to reporting episodes under a name that says outages.
            """
        )
        // AND THE COUNTS DISAGREE, which is the whole point: three episodes,
        // one outage, one launch.
        #expect(page.rows.count == 3 && crossings.count == 1)
        await manager.invalidateBackgroundSessionsForTesting()
    }

    /// TWO MANAGERS ARE TWO LAUNCHES, which is what puts the numerator in the
    /// same unit as `armedLaunches`.
    ///
    /// A process-wide `static` launch id would pass every rail above and fail
    /// this one: two independent recorders each arm the ledger, so
    /// `armedLaunches` reaches 2 while `count(DISTINCT launchId)` would still
    /// read 1 — a numerator and a denominator that have quietly stopped being
    /// the same unit, which is the defect this bead exists to remove.
    @Test("two managers are two launches, and the numerator matches armedLaunches' unit")
    func twoManagersAreTwoLaunches() async throws {
        let dir = try makeTempDir(prefix: "sdisTwoLaunches")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)

        var launchIds: [String] = []
        for index in 1...2 {
            let manager = Self.manager(
                cacheDirectory: try makeTempDir(prefix: "sdisLaunch\(index)"),
                store: store, sessionIO: Self.creationRefusingIO()
            )
            try await manager.bootstrap()
            await manager.armDropLedger()
            launchIds.append(await manager.launchId)
            // TWO episodes each, and the second one is the discriminator: with
            // one apiece `count(*)`, `count(DISTINCT launchId)` and
            // `armedLaunches` would all read 2, and the rail could not tell an
            // EPISODE count from a LAUNCH count — which is the exact confusion
            // the bead exists to remove.
            await Self.drive(manager, episodeId: "ep-launch-\(index)-a")
            await Self.drive(manager, episodeId: "ep-launch-\(index)-b")
            await manager.invalidateBackgroundSessionsForTesting()
        }

        #expect(Set(launchIds).count == 2, "two managers must not share one identity")
        let page = try await store.fetchBackgroundDownloadDrops()
        #expect(page.rows.count == 4, "FOUR episodes were lost…")
        #expect(Set(page.rows.compactMap(\.launchId)) == Set(launchIds), "…on TWO launches")
        let arming = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(arming.armedLaunches == 2)
        #expect(
            Set(page.rows.compactMap(\.launchId)).count == arming.armedLaunches,
            """
            two launches armed and two launches dropped: the ratio is finally a \
            ratio. `count(*) / armedLaunches` reads 2.0 on this same fixture and \
            is episodes-per-launch wearing a rate's name.
            """
        )
        #expect(
            arming.lastArmedLaunchId == launchIds[1],
            "the arming row must name the launch that armed most recently, so the two tables share one vocabulary"
        )
    }

    // MARK: - 3. A drop on a DEGRADED launch says so

    /// THE READING HAZARD playhead-7dgx DOCUMENTED AND COULD NOT MEASURE.
    ///
    /// `AnalysisStore` opens LAZILY, so a launch whose `openAtLaunch` failed
    /// returns from `PlayheadRuntime`'s launch Task before `armDropLedger()`
    /// and STILL lands a drop row through `insertBackgroundDownloadDrop`. That
    /// row's launch is in no denominator, and nothing on disk said which rows
    /// those were. This is the degraded launch, reproduced by the only property
    /// that defines it: the arming never ran.
    @Test("a drop written by a launch that never armed carries not_attempted, and the row is real")
    func aDropOnAnUnarmedLaunchSaysNotAttempted() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let manager = Self.manager(
            cacheDirectory: try makeTempDir(prefix: "sdisDegraded"),
            store: store, sessionIO: Self.creationRefusingIO()
        )
        try await manager.bootstrap()
        // NO armDropLedger() — this is the degraded launch.
        await Self.drive(manager, episodeId: "ep-degraded")

        let row = try #require(try await store.fetchBackgroundDownloadDrops().rows.first)
        #expect(
            row.launchArmingState == .notAttempted,
            "the row must say its launch is NOT in the denominator"
        )
        let arming = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(
            arming.armedLaunches == 0,
            "and the denominator must agree: a real row beside a zero count is a REACHABLE state, not a contradiction"
        )
        #expect(arming.lastArmedLaunchId == nil)
        await manager.invalidateBackgroundSessionsForTesting()
    }

    @Test("a drop written after a successful arming carries armed, the only positive value")
    func aDropAfterArmingSaysArmed() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let manager = Self.manager(
            cacheDirectory: try makeTempDir(prefix: "sdisArmed"),
            store: store, sessionIO: Self.creationRefusingIO()
        )
        try await manager.bootstrap()
        await manager.armDropLedger()
        await Self.drive(manager, episodeId: "ep-armed")

        let row = try #require(try await store.fetchBackgroundDownloadDrops().rows.first)
        #expect(row.launchArmingState == .armed)
        let arming = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(arming.armedLaunches == 1)
        #expect(
            arming.lastArmedLaunchId == row.launchId,
            "the row and the arming row must name the SAME launch, or the join between the two tables is decorative"
        )
        await manager.invalidateBackgroundSessionsForTesting()
    }

    /// AN ARMING THAT FAILED IS NOT AN ARMING THAT NEVER RAN, and the row must
    /// not collapse them. `arming_failed` says the launch will NEVER enter the
    /// denominator; `not_attempted` leaves it open. Reproduced by breaking
    /// exactly the arming table and leaving the drops table intact — the shape
    /// a partial write failure has.
    @Test("a drop written after a FAILED arming carries arming_failed, not not_attempted")
    func aDropAfterAFailedArmingSaysArmingFailed() async throws {
        let dir = try makeTempDir(prefix: "sdisArmingFailed")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        sqlite3_busy_timeout(db, 3000)
        #expect(
            sqlite3_exec(db, "DROP TABLE background_download_drop_arming", nil, nil, nil) == SQLITE_OK
        )
        sqlite3_close_v2(db)

        let recording = RecordedInvariantViolations()
        let manager = Self.manager(
            cacheDirectory: try makeTempDir(prefix: "sdisArmingFailedCache"),
            store: store, sessionIO: Self.creationRefusingIO(),
            invariantRecorder: recording.recorder
        )
        try await manager.bootstrap()
        await manager.armDropLedger()
        await Self.drive(manager, episodeId: "ep-arming-failed")

        let row = try #require(try await store.fetchBackgroundDownloadDrops().rows.first)
        #expect(
            row.launchArmingState == .armingFailed,
            "the arming write failed, so this launch is not in armedLaunches and never will be"
        )
        #expect(row.launchId != nil, "the row is otherwise complete — only the DENOMINATOR was lost")
        // And the second medium names the launch, so the two surfaces can be
        // matched rather than correlated by timestamp.
        let launchId = await manager.launchId
        #expect(
            recording.unrecordedDrops.contains { $0.contains("arming=failed") && $0.contains("launch=\(launchId)") },
            "the arming failure must NAME the launch whose rows it invalidates: \(recording.unrecordedDrops)"
        )
        await manager.invalidateBackgroundSessionsForTesting()
    }

    // MARK: - 4. The fallback medium carries the identities too

    /// The surface-status stream is reached exactly when the durable row did
    /// NOT land, so it is the only place the launch and the crossing survive. A
    /// fallback that dropped them would leave the second medium unable to
    /// answer the question the first one was extended to answer.
    @Test("a drop whose row cannot be written raises the launch, the crossing and the arming state")
    func theFallbackMediumCarriesTheIdentities() async throws {
        let dir = try makeTempDir(prefix: "sdisFallback")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)

        // Break exactly the DROPS table, leaving the arming row intact, so the
        // arming lands and the row cannot.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        sqlite3_busy_timeout(db, 3000)
        #expect(
            sqlite3_exec(db, "DROP TABLE background_download_drops", nil, nil, nil) == SQLITE_OK
        )
        sqlite3_close_v2(db)

        let recording = RecordedInvariantViolations()
        let manager = Self.manager(
            cacheDirectory: try makeTempDir(prefix: "sdisFallbackCache"),
            store: store, sessionIO: Self.creationRefusingIO(),
            invariantRecorder: recording.recorder
        )
        try await manager.bootstrap()
        await manager.armDropLedger()
        let launchId = await manager.launchId
        await Self.drive(manager, episodeId: "ep-fallback")

        let raised = try #require(
            recording.unrecordedDrops.first { $0.contains("episodeId=ep-fallback") },
            "an unwritable row must reach the second medium: \(recording.unrecordedDrops)"
        )
        #expect(raised.contains("launch=\(launchId)"))
        #expect(raised.contains("arming=armed"))
        #expect(
            raised.contains("crossing=") && !raised.contains("crossing=none"),
            "a session refusal always rides a crossing, so the fallback must NAME it: \(raised)"
        )
        await manager.invalidateBackgroundSessionsForTesting()
    }

    /// AND `crossing=none` IS SPELLED OUT rather than omitted, for the two
    /// reasons that ride no crossing. An absent field reads as a forgotten one;
    /// for these rows the absence IS the measurement.
    @Test("a task-vending drop that cannot be written says crossing=none rather than omitting it")
    func theFallbackSpellsOutAnAbsentCrossing() async throws {
        let dir = try makeTempDir(prefix: "sdisFallbackNone")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        sqlite3_busy_timeout(db, 3000)
        #expect(
            sqlite3_exec(db, "DROP TABLE background_download_drops", nil, nil, nil) == SQLITE_OK
        )
        sqlite3_close_v2(db)

        let recording = RecordedInvariantViolations()
        let manager = Self.manager(
            cacheDirectory: try makeTempDir(prefix: "sdisFallbackNoneCache"),
            store: store, sessionIO: Self.taskRefusingIO(),
            invariantRecorder: recording.recorder
        )
        try await manager.bootstrap()
        await Self.drive(manager, episodeId: "ep-fallback-none")

        let raised = try #require(
            recording.unrecordedDrops.first { $0.contains("episodeId=ep-fallback-none") },
            "an unwritable row must reach the second medium: \(recording.unrecordedDrops)"
        )
        #expect(raised.contains("reason=transfer_task_not_vended"))
        #expect(raised.contains("crossing=none"))
        #expect(raised.contains("arming=not_attempted"))
        await manager.invalidateBackgroundSessionsForTesting()
    }

    // MARK: - 5. The refusal record and the ledger name the SAME crossing

    /// The surface-status refusal record counts REFUSALS — only the caller that
    /// STARTED the crossing reaches that arm, a joiner returns before it — while
    /// the table counts EPISODES. The crossing id is what lets the two be
    /// compared instead of correlated by timestamp, so it has to be the same
    /// value on both.
    @Test("the gpdb refusal record and the drop row name ONE crossing")
    func theRefusalRecordAndTheRowNameOneCrossing() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let recording = RecordedInvariantViolations()
        let manager = Self.manager(
            cacheDirectory: try makeTempDir(prefix: "sdisTwoSurfaces"),
            store: store, sessionIO: Self.creationRefusingIO(),
            invariantRecorder: recording.recorder
        )
        try await manager.bootstrap()
        await Self.drive(manager, episodeId: "ep-two-surfaces")

        let row = try #require(try await store.fetchBackgroundDownloadDrops().rows.first)
        let crossing = try #require(row.sessionCrossingId)
        let refusals = recording.sessionRefusals(from: "background_download")
        #expect(refusals.count == 1, "one refusal, from the caller that started the crossing")
        #expect(
            try #require(refusals.first).contains("crossing=\(crossing)"),
            """
            the two surfaces must name the same crossing, or a reader holding \
            both a JSONL and a database has no way to tell how many refusals \
            the rows came from: \(refusals)
            """
        )
        await manager.invalidateBackgroundSessionsForTesting()
    }
}
