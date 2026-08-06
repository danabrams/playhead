// BackgroundGrantBudgetTests.swift
// playhead-lmrx: a ~295 s background grant must be spent on work that can
// finish or checkpoint inside it, and what the grant achieved must be readable
// afterwards.
//
// Every assertion here is anchored to a measurement from
// `scratchpad/db-new-t2h/analysis.sqlite` (2026-08-06 device pull), and the
// derivation of each number lives in `BackgroundGrantBudget`'s doc comments.
// The three defects under test, in the order the bead states them:
//
//   1. The unit of work attempted per grant was bounded by `25 * 60` seconds —
//      5.07x the p90 of the 203 expired backfill grants in that pull.
//   2. The expiration path wrote outcome/cause/expiration and nothing else, so
//      `jobsSeen`/`jobsAdmitted`/`jobsCompleted` are NULL on ALL 203 expired
//      rows and the ledger cannot say what 80 % of grants achieved.
//   3. A BGTask expiry cancelled the handler's own task but never told
//      `AnalysisWorkScheduler` the grant was over, so the analysis job the
//      grant dispatched ran on unaware into process suspension — no checkpoint,
//      no requeue, and a leased `running` row for the reaper.

import BackgroundTasks
import Foundation
import Testing

@testable import Playhead

// MARK: - The budget arithmetic

@Suite("playhead-lmrx: the background grant budget")
struct BackgroundGrantBudgetArithmeticTests {

    @Test("workBudget is the grant minus the teardown reserve")
    func workBudgetSubtractsReserve() {
        let budget = BackgroundGrantBudget(
            designGrant: .seconds(255),
            teardownReserve: .seconds(36),
            minimumUnitBudget: .seconds(60)
        )
        #expect(budget.workBudget == .seconds(219))
    }

    @Test("workBudget clamps at zero rather than going negative")
    func workBudgetClampsAtZero() {
        // A reserve larger than the grant is a coherent (useless) budget. A
        // NEGATIVE `Duration` here would make `workDeadline(from:)` return an
        // instant in the PAST, which every caller reads as "already expired" —
        // the right outcome for the wrong reason, and one that would mask a
        // mis-specified budget instead of surfacing it.
        let budget = BackgroundGrantBudget(
            designGrant: .seconds(10),
            teardownReserve: .seconds(60),
            minimumUnitBudget: .seconds(1)
        )
        #expect(budget.workBudget == .zero)
    }

    @Test("workDeadline is measured from the grant's start, not from the call")
    func workDeadlineIsAnchoredToGrantStart() {
        let budget = BackgroundGrantBudget.backfillProcessing
        let grantStart = ContinuousClock.now
        let deadline = budget.workDeadline(from: grantStart)
        #expect(grantStart.duration(to: deadline) == budget.workBudget)
    }

    @Test("canStartUnit admits exactly the floor and refuses below it")
    func canStartUnitBoundary() {
        let budget = BackgroundGrantBudget(
            designGrant: .seconds(255),
            teardownReserve: .seconds(36),
            minimumUnitBudget: .seconds(60)
        )
        #expect(budget.canStartUnit(remaining: .seconds(61)))
        #expect(budget.canStartUnit(remaining: .seconds(60)),
                "a unit that costs exactly its own cost fits")
        #expect(!budget.canStartUnit(remaining: .seconds(59)))
        #expect(!budget.canStartUnit(remaining: .zero))
        #expect(!budget.canStartUnit(remaining: .seconds(-5)),
                "a deadline already in the past yields a negative remainder")
    }

    @Test("canStartUnit(before:now:) agrees with the duration form")
    func canStartUnitDeadlineForm() {
        let budget = BackgroundGrantBudget.backfillProcessing
        let now = ContinuousClock.now
        #expect(budget.canStartUnit(before: now + .seconds(120), now: now))
        #expect(!budget.canStartUnit(before: now + .seconds(5), now: now))
        #expect(!budget.canStartUnit(before: now - .seconds(5), now: now))
    }
}

// MARK: - The shipped constants, against the measurement they came from

@Suite("playhead-lmrx: the shipped budget is inside the measured grant")
struct BackgroundGrantBudgetMeasurementTests {

    /// p95 of `finishedAt - startedAt` over the 203 `background_task_runs` rows
    /// with `taskIdentifier = 'com.playhead.app.analysis.backfill'` and
    /// `outcome = 'expired'` in the 2026-08-06 pull. p50 was 294.0 and p90
    /// 295.9; 296.0 is the point past which a "grant" is not evidence, it is a
    /// guess.
    private static let measuredGrantP95: Duration = .seconds(296)

    /// The largest end-to-end duration of a backfill run that did NO work
    /// (`no_eligible_work` + `deferred_capability`, n = 30) — a full handler
    /// entry, a pending-count query, a ledger `finishRun` and a
    /// `setTaskCompleted`. p50 1.4 s, p90 17.3 s, p95 19.4 s, max 36.0 s.
    private static let measuredTeardownMax: Duration = .seconds(36)

    /// p95 of the 142 `semantic_scan_results.latencyMs` values — one FM coarse
    /// window, the smallest unit playhead-26od makes durable. p50 6.0 s,
    /// p90 49.0 s, p95 57.5 s.
    private static let measuredWindowP95: Duration = .seconds(58)

    @Test("the backfill design grant does not exceed the measured grant")
    func designGrantFitsTheMeasurement() {
        // The specific implementation this kills is the one that shipped:
        // `ContinuousClock.now + .seconds(25 * 60)`, which is 5.07x this bound.
        // It also kills any future inflation past what the OS was observed to
        // give, which is the same defect with a smaller multiplier.
        #expect(BackgroundGrantBudget.backfillProcessing.designGrant <= Self.measuredGrantP95)
        #expect(BackgroundGrantBudget.preAnalysisRecovery.designGrant <= Self.measuredGrantP95)
    }

    @Test("the teardown reserve covers the worst handler teardown actually observed")
    func teardownReserveCoversTheObservedMax() {
        // A reserve smaller than the worst observed teardown reproduces the
        // defect this bead is about from the other end: a handler reclaimed
        // mid-teardown leaves an `expired` row with no counters, which is
        // precisely the unreadable 80 % the bead measured.
        #expect(BackgroundGrantBudget.backfillProcessing.teardownReserve >= Self.measuredTeardownMax)
        #expect(BackgroundGrantBudget.preAnalysisRecovery.teardownReserve >= Self.measuredTeardownMax)
    }

    @Test("the start-gate floor is large enough to bank one durable window")
    func minimumUnitBudgetCoversOneWindow() {
        #expect(BackgroundGrantBudget.backfillProcessing.minimumUnitBudget >= Self.measuredWindowP95)
    }

    @Test("the budget still leaves room to start work")
    func workBudgetExceedsTheFloor() {
        // Bounding the grant is only half the contract. A budget shrunk below
        // the start-gate floor would satisfy every assertion above and dispatch
        // NOTHING, ever — trading 199-of-203 barren windows for 203-of-203.
        let budget = BackgroundGrantBudget.backfillProcessing
        #expect(budget.workBudget > budget.minimumUnitBudget)
        #expect(BackgroundGrantBudget.preAnalysisRecovery.workBudget
            > BackgroundGrantBudget.preAnalysisRecovery.minimumUnitBudget)
    }
}

// MARK: - The counters box

@Suite("playhead-lmrx: background grant counters")
struct BackgroundGrantCountersTests {

    @Test("an untouched box reports nil, not zero")
    func untouchedBoxIsNil() {
        // NULL and 0 are different findings — "the handler never got far enough
        // to count" versus "the handler counted zero" — and `finishRun` binds
        // through `COALESCE(?, col)`, so writing a fabricated 0 would also
        // overwrite a counter an earlier path had legitimately measured.
        let counters = BackgroundGrantCounters()
        let snapshot = counters.snapshot
        #expect(snapshot.jobsSeen == nil)
        #expect(snapshot.jobsAdmitted == nil)
        #expect(snapshot.jobsCompleted == nil)
    }

    @Test("noteBaseline publishes seen and admitted, and leaves completed nil")
    func baselineDoesNotClaimCompletion() {
        let counters = BackgroundGrantCounters()
        counters.noteBaseline(pending: 7)
        let snapshot = counters.snapshot
        #expect(snapshot.jobsSeen == 7)
        #expect(snapshot.jobsAdmitted == 7)
        #expect(snapshot.jobsCompleted == nil,
                "a baseline is a denominator; it says nothing about completion")
    }

    @Test("noteCompleted records zero as a measurement, not as absence")
    func completedZeroIsRecorded() {
        let counters = BackgroundGrantCounters()
        counters.noteBaseline(pending: 3)
        counters.noteCompleted(0)
        #expect(counters.snapshot.jobsCompleted == 0,
                "0 completed is the bead's headline finding; it must not read as NULL")
    }
}

// MARK: - The drain start-gate

@Suite("playhead-lmrx: drainEligible will not start a unit it cannot finish")
struct DrainEligibleStartGateTests {

    /// Always throws, so one dispatch drives a pre-stamped high-`attemptCount`
    /// job to a terminal state — a deterministic fixed point for the drain.
    private final class FailingDecodeStub: AnalysisAudioProviding, @unchecked Sendable {
        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            throw AnalysisAudioError.decodingFailed("Operation Interrupted")
        }
    }

    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: FailingDecodeStub(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
            ),
            downloadManager: downloads,
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider()
        )
    }

    @discardableResult
    private func insertComputeOnlyJob(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        jobId: String,
        episodeId: String
    ) async throws -> AnalysisJob {
        downloads.cachedURLs[episodeId] = URL(fileURLWithPath: "/tmp/\(episodeId).m4a")
        let job = makeAnalysisJob(
            jobId: jobId,
            jobType: "preAnalysis",
            episodeId: episodeId,
            analysisAssetId: nil,
            workKey: AnalysisJob.computeWorkKey(
                fingerprint: "fp-\(jobId)",
                analysisVersion: PreAnalysisConfig.analysisVersion,
                jobType: "preAnalysis"
            ),
            sourceFingerprint: "fp-\(jobId)",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 4
        )
        try await store.insertJob(job)
        return job
    }

    @Test("a pass is NOT started when the remaining grant is below the unit floor",
          .timeLimit(.minutes(1)))
    func belowFloorDispatchesNothing() async throws {
        // A dispatch pass is a whole analysis job. The pre-fix loop condition
        // was a bare `now < deadline`, which admits a pass with a millisecond
        // left — converting the tail of a grant into work that is guaranteed to
        // be abandoned. The floor is the measured cost of the smallest unit the
        // pipeline makes durable (one FM coarse window, p95 57.5 s of the 142
        // `semantic_scan_results.latencyMs` values).
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertComputeOnlyJob(
            store: store, downloads: downloads,
            jobId: "gate-below", episodeId: "ep-gate-below"
        )
        let scheduler = makeScheduler(store: store, downloads: downloads)

        // 10 s of grant left, against a 60 s floor.
        await scheduler.drainEligible(
            deadline: ContinuousClock.now + .seconds(10),
            minimumUnitBudget: .seconds(60)
        )

        let stillQueued = try await store.fetchJobsByState("queued")
        #expect(
            stillQueued.contains { $0.jobId == "gate-below" },
            """
            With less remaining grant than one durable unit costs, the drain \
            must start NOTHING. Pre-fix (`while now < deadline`) it dispatched, \
            and the job was abandoned unfinished when the OS reclaimed.
            """
        )
    }

    @Test("a pass IS started when the remaining grant clears the unit floor",
          .timeLimit(.minutes(1)))
    func aboveFloorStillDispatches() async throws {
        // The complement, and the reason it is here: a start-gate that refuses
        // everything satisfies the test above and dispatches nothing, ever.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertComputeOnlyJob(
            store: store, downloads: downloads,
            jobId: "gate-above", episodeId: "ep-gate-above"
        )
        let scheduler = makeScheduler(store: store, downloads: downloads)

        await scheduler.drainEligible(
            deadline: ContinuousClock.now + .seconds(600),
            minimumUnitBudget: .seconds(60)
        )

        let stillQueued = try await store.fetchJobsByState("queued")
        #expect(
            !stillQueued.contains { $0.jobId == "gate-above" },
            "with ample grant remaining the drain must still dispatch"
        )
    }

    @Test("the default floor is zero, so non-grant callers are unchanged",
          .timeLimit(.minutes(1)))
    func defaultFloorPreservesPreFixBehaviour() async throws {
        // The parameter defaults to `.zero` precisely so foreground drains and
        // the existing suites keep the pre-lmrx condition. If that default ever
        // becomes non-zero, every caller that omits it silently acquires a
        // budget nobody measured for it.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertComputeOnlyJob(
            store: store, downloads: downloads,
            jobId: "gate-default", episodeId: "ep-gate-default"
        )
        let scheduler = makeScheduler(store: store, downloads: downloads)

        // 10 s of deadline and NO floor argument: must behave as `now < deadline`.
        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(10))

        let stillQueued = try await store.fetchJobsByState("queued")
        #expect(
            !stillQueued.contains { $0.jobId == "gate-default" },
            "with the default floor the drain must dispatch exactly as before"
        )
    }
}

// MARK: - The handler spends a grant-shaped budget

@Suite("playhead-lmrx: the backfill handler bounds its grant")
struct BackfillGrantBoundingTests {

    private func makeBPS(
        coordinator: StubAnalysisCoordinator,
        ledger: any BackgroundTaskRunLedger = NoOpBackgroundTaskRunLedger()
    ) -> BackgroundProcessingService {
        BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
    }

    @Test("the poll loop is handed a deadline inside the measured grant, not 25 minutes",
          .timeLimit(.minutes(1)))
    func pollLoopDeadlineIsGrantShaped() async throws {
        let coordinator = StubAnalysisCoordinator()
        let bps = makeBPS(coordinator: coordinator)
        let task = StubBackgroundTask()

        let before = ContinuousClock.now
        await bps.handleBackfillTask(task)
        await task.awaitCompletion()
        let after = ContinuousClock.now

        let deadline = try #require(coordinator.runPendingBackfillDeadlines.first,
                                    "the handler must hand the coordinator a deadline")
        let budget = BackgroundGrantBudget.backfillProcessing

        // The handler anchors the deadline at the instant the grant opened,
        // which it reads somewhere inside `[before, after]`. So the deadline
        // lies in `[before + workBudget, after + workBudget]`, and BOTH ends
        // are worth pinning:
        //
        //   upper — measured from `after`, the horizon can be at most the whole
        //           work budget. This is the bound that kills the shipped
        //           defect: `ContinuousClock.now + .seconds(25 * 60)` gives
        //           1500 s here against a budget of 219 s.
        //   lower — measured from `before`, it must be at least the whole
        //           budget. Without this, a deadline of `now` (or one in the
        //           past) would satisfy the upper bound while polling for
        //           nothing at all — trading 199-of-203 barren windows for
        //           203-of-203.
        #expect(after.duration(to: deadline) <= budget.workBudget,
                """
                The poll loop's deadline must be inside the measured grant. \
                Got \(after.duration(to: deadline)) past the handler's return, \
                budget is \(budget.workBudget); the pre-fix value was 1500 s \
                against a 294 s (p50, n=203) grant.
                """)
        #expect(before.duration(to: deadline) >= budget.workBudget,
                "a bounded deadline must still hand over the whole work budget")
        #expect(before.duration(to: deadline) > budget.minimumUnitBudget,
                "and that budget must leave room to start at least one unit")
    }
}

// MARK: - The expiry leaves a durable resume point

@Suite("playhead-lmrx: a backfill expiry is durable")
struct BackfillExpiryDurabilityTests {

    private func makeScheduler(store: AnalysisStore) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(
                    thermalState: .nominal,
                    isLowPowerMode: false,
                    isCharging: true
                )
            ),
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider()
        )
    }

    @Test("expiry tells the SCHEDULER the grant is over, so the in-flight job requeues",
          .timeLimit(.minutes(1)))
    func expiryCancelsTheInFlightAnalysisJob() async throws {
        // THE RESUME POINT. `workTask.cancel()` reaches this handler's own
        // task, but the analysis job the drain dispatched lives on
        // `AnalysisWorkScheduler`'s task tree and never sees it. Pre-fix the
        // backfill expiration handler did not call `cancelCurrentJob`, so the
        // job ran on unaware into process suspension: nothing checkpointed,
        // nothing requeued, and a `running` row with a stale lease. The healthy
        // sibling — pre-analysis recovery, 86 of 97 wakes at `recovered_work` —
        // has called it since playhead-1nl6.
        //
        // `pendingCancelCause` is the observable because it is precisely what
        // the scheduler's cancel-catch arm consumes to commit
        // `state = 'queued'` with a backoff `nextEligibleAt` and to journal a
        // `preempted` row carrying `task_expired`.
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
            )
        )

        let coordinator = StubAnalysisCoordinator()
        coordinator.runPendingBackfillDuration = .seconds(30)
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider()
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: reconciler)

        let causeBefore = await scheduler.pendingCancelCauseForTesting()
        #expect(causeBefore == nil, "nothing has expired yet")

        let task = StubBackgroundTask()
        let workTask = Task { await bps.handleBackfillTask(task) }
        await task.awaitExpirationHandlerInstalled()
        task.simulateExpiration()
        _ = await workTask.value
        await task.awaitCompletion()

        let causeAfter = await scheduler.pendingCancelCauseForTesting()
        #expect(causeAfter == .taskExpired,
                """
                A backfill BGTask expiry must tell the scheduler its grant \
                ended, with cause .taskExpired. Pre-fix this was nil: the \
                in-flight analysis job was never cancelled, so it left no \
                checkpoint and no requeued row for the next granted window to \
                continue from.
                """)
    }

    @Test("an expired run records what the window achieved, not only that it ended",
          .timeLimit(.minutes(1)))
    func expiredRunPersistsItsCounters() async throws {
        // Pre-fix: `finishRun` on the expiration path wrote outcome, cause and
        // expiration only, so all 203 expired rows in the 2026-08-06 pull carry
        // NULL `jobsSeen`/`jobsAdmitted`/`jobsCompleted`. The "4 of 203 wrote a
        // durable scan row" figure in the bead had to be reconstructed by
        // joining `semantic_scan_results` timestamps against run windows —
        // which is exactly the reconstruction this makes unnecessary.
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let coordinator = StubAnalysisCoordinator()
        coordinator.runPendingBackfillDuration = .seconds(30)
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )

        let task = StubBackgroundTask()
        let workTask = Task { await bps.handleBackfillTask(task) }
        // The baseline is read before `runPendingBackfill` parks, so waiting
        // for the poll loop to be ENTERED proves the counters were published
        // before expiration fires. Without this the test would be racing the
        // very publication it is asserting.
        await coordinator.runPendingBackfillEntries.wait(for: 1)
        task.simulateExpiration()
        _ = await workTask.value
        await task.awaitCompletion()

        let latest = try #require(await ledger.fetchLatestRun(for: .backfill))
        #expect(latest.outcome == .expired)
        #expect(latest.expiration == true)
        #expect(latest.jobsSeen != nil,
                """
                An expired run must record the queue depth it observed. NULL \
                here is the state the bead measured on 203 of 254 backfill \
                wakes — the ledger could not answer "how much did this window \
                achieve" for 80 % of the grants it recorded.
                """)
        #expect(latest.jobsAdmitted != nil,
                "an expired run must record how much work it admitted")
    }
}
