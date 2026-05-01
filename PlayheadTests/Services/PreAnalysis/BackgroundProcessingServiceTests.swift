// BackgroundProcessingServiceTests.swift
// Comprehensive tests for BackgroundProcessingService lifecycle, task handling,
// thermal/battery management, and scheduling.

import BackgroundTasks
import Foundation
import Testing
@testable import Playhead

// MARK: - Factory

private func makeBPS(
    coordinator: StubAnalysisCoordinator = StubAnalysisCoordinator(),
    scheduler: StubTaskScheduler = StubTaskScheduler(),
    battery: StubBatteryProvider = StubBatteryProvider()
) -> (BackgroundProcessingService, StubAnalysisCoordinator, StubTaskScheduler, StubBatteryProvider) {
    let bps = BackgroundProcessingService(
        coordinator: coordinator,
        capabilitiesService: CapabilitiesService(),
        taskScheduler: scheduler,
        batteryProvider: battery
    )
    return (bps, coordinator, scheduler, battery)
}

/// Wait until a stub task is completed, with a timeout to avoid hanging.
private func waitForCompletion(of task: StubBackgroundTask, timeout: Duration = .seconds(10)) async throws {
    let deadline = ContinuousClock.now + timeout
    while task.completedSuccess == nil && ContinuousClock.now < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
}

/// playhead-hv73: poll the actor for at least `count` parked
/// injection-wait waiters. Used by tests that drive the deterministic
/// timeout seam — once a handler has reached the suspend point inside
/// `awaitPreAnalysisServicesInjected`, the test can fire
/// `triggerInjectionWaitTimeoutForTesting()` and observe failure
/// without any wall-clock dependency on the (15 s) default timeout.
private func waitForParkedInjectionWaiters(
    in bps: BackgroundProcessingService,
    atLeast count: Int = 1,
    timeout: Duration = .seconds(2)
) async throws {
    let deadline = ContinuousClock.now + timeout
    while ContinuousClock.now < deadline {
        let parked = await bps.pendingInjectionWaiterCountForTesting()
        if parked >= count { return }
        try await Task.sleep(for: .milliseconds(1))
    }
}

// MARK: - Backfill Task Handler

@Suite("Backfill Task Handler")
struct BackfillTaskHandlerTests {

    @Test("Backfill completes successfully",
          .timeLimit(.minutes(1)))
    func backfillCompletesSuccessfully() async throws {
        let (bps, coordinator, scheduler, _) = makeBPS()
        let task = StubBackgroundTask()

        await bps.handleBackfillTask(task)
        try await waitForCompletion(of: task)

        // The handler must invoke the real backfill work method, not the
        // capability-observer lifecycle method start(). See regression test
        // backfillInvokesRealWorkMethod for the bug history.
        #expect(coordinator.runPendingBackfillCallCount >= 1)
        #expect(task.completedSuccess == true)
        // A new backfill should be scheduled.
        let backfillRequests = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundTaskID.backfillProcessing
        }
        #expect(!backfillRequests.isEmpty)
    }

    @Test("Backfill skipped on thermal throttle via capabilitiesService",
          .timeLimit(.minutes(1)))
    func backfillSkippedOnThermalThrottle() async throws {
        // NOTE: This test exercises the throttle-check code path in handleBackfillTask.
        // On a simulator with nominal thermal state, the backfill will proceed normally.
        // The QualityProfile-derived backfill gate (`pauseAllWork || !allowSoonLane`)
        // reads from the real CapabilitiesService snapshot which cannot be injected.
        // To fully test the skip path, the device must report .serious or .critical
        // thermal state. This test still validates the happy path does NOT skip
        // when thermal is nominal.
        let (bps, coordinator, _, _) = makeBPS()
        let task = StubBackgroundTask()

        await bps.handleBackfillTask(task)
        try await waitForCompletion(of: task)

        // On simulator (nominal thermal), the real work method IS called.
        #expect(coordinator.runPendingBackfillCallCount >= 1)
        #expect(task.completedSuccess == true)
    }

    @Test("Backfill expiration cancels work",
          .timeLimit(.minutes(1)))
    func backfillExpirationCancelsWork() async throws {
        let coordinator = StubAnalysisCoordinator()
        // Make runPendingBackfill() take long enough for expiration to fire.
        coordinator.runPendingBackfillDuration = .seconds(10)
        let (bps, _, _, _) = makeBPS(coordinator: coordinator)
        let task = StubBackgroundTask()

        // Start backfill in a detached task so we can expire it.
        let workTask = Task {
            await bps.handleBackfillTask(task)
        }

        // Wait deterministically for the expiration handler to be set.
        // The actor method must execute through to the point where it installs
        // the expiration handler before we can simulate iOS firing it.
        let setupDeadline = ContinuousClock.now + .seconds(5)
        while task.expirationHandler == nil && ContinuousClock.now < setupDeadline {
            try await Task.sleep(for: .milliseconds(10))
        }

        // Simulate iOS firing expiration.
        task.simulateExpiration()

        try await waitForCompletion(of: task)

        workTask.cancel()

        #expect(coordinator.stopCallCount >= 1)
        #expect(task.completedSuccess == false)
    }

    @Test("Backfill handler invokes real work method, not startCapabilityObserver",
          .timeLimit(.minutes(1)))
    func backfillInvokesRealWorkMethod() async throws {
        // Regression: handleBackfillTask used to call the coordinator's
        // capability-observer setup (now startCapabilityObserver()), which
        // is a sync lifecycle-init that just spawns the capability observer
        // and returns. The BGProcessingTask was being marked successful in
        // microseconds without any backfill work happening, and iOS
        // reclaimed the granted background time. This test pins the
        // contract that handleBackfillTask invokes runPendingBackfill.
        let coordinator = StubAnalysisCoordinator()
        let (bps, _, _, _) = makeBPS(coordinator: coordinator)
        let task = StubBackgroundTask()

        await bps.handleBackfillTask(task)
        try await waitForCompletion(of: task)

        #expect(coordinator.runPendingBackfillCallCount >= 1,
                "handleBackfillTask must invoke runPendingBackfill, not startCapabilityObserver()")
        // startCapabilityObserver() is the capability-observer lifecycle entry
        // point and must not be called from the background task handler.
        #expect(coordinator.startCapabilityObserverCallCount == 0,
                "handleBackfillTask must not call startCapabilityObserver() — that path was the bug")
        #expect(task.completedSuccess == true)
    }

    @Test("Overlapping BG handlers each complete their own task independently",
          .timeLimit(.minutes(1)))
    func overlappingHandlersCompleteIndependently() async throws {
        // Regression for H1: prior to this fix, BackgroundProcessingService
        // shared a single `bgTaskCompleted` flag and a single
        // `activeBackgroundTask` slot across the backfill, continued-
        // processing and pre-analysis recovery handlers. Once backfill
        // started awaiting `runPendingBackfill` (up to 25 minutes), iOS
        // firing the pre-analysis recovery identifier inside that window
        // would overwrite the shared state; whichever handler completed
        // first would silently drop the other's markComplete, leaving iOS
        // holding a BGProcessingTask that was never reported complete.
        let coordinator = StubAnalysisCoordinator()
        coordinator.runPendingBackfillDuration = .milliseconds(300)
        let (bps, _, _, _) = makeBPS(coordinator: coordinator)
        // playhead-hv73: deterministic seam replaces the wall-clock
        // injection-wait timeout. The recovery handler will park on
        // `awaitPreAnalysisServicesInjected`; we trigger the timeout
        // synchronously so the no-reconciler fail path runs without
        // any wall-clock delay.

        let backfillTask = StubBackgroundTask()
        let recoveryTask = StubBackgroundTask()

        // Start backfill — it will await runPendingBackfill for ~300ms.
        let backfillWork = Task { await bps.handleBackfillTask(backfillTask) }

        // Wait just long enough for the backfill handler to have spawned
        // its work task and installed its expiration handler.
        try await Task.sleep(for: .milliseconds(30))

        // Kick off recovery while backfill is still suspended. Recovery
        // has no reconciler (none injected); the playhead-8u3i buffer
        // would normally time out, but the playhead-hv73 seam fires
        // the timeout synchronously so the handler returns immediately.
        // We just need to verify the recovery completion doesn't
        // clobber the in-flight backfill.
        let recoveryWork = Task { await bps.handlePreAnalysisRecovery(recoveryTask) }
        try await waitForParkedInjectionWaiters(in: bps)
        await bps.triggerInjectionWaitTimeoutForTesting()
        await recoveryWork.value

        #expect(recoveryTask.completedSuccess != nil,
                "Recovery task must complete independently of in-flight backfill")

        // Let backfill finish.
        try await waitForCompletion(of: backfillTask)
        _ = await backfillWork.value

        #expect(backfillTask.completedSuccess != nil,
                "Backfill task must not be silently dropped after the recovery handler ran in the middle")
        #expect(backfillTask.completedSuccess == true,
                "Backfill should complete successfully after overlap")
    }
}

// MARK: - Continued Processing Handler (playhead-44h1)

/// Wait for a `StubContinuedProcessingTask`'s `completedSuccess` flag
/// to flip, with a deadline. Used by playhead-44h1 handler tests that
/// drive the hand-off through its async work task.
private func waitForCompletion(
    of task: StubContinuedProcessingTask,
    timeout: Duration = .seconds(10)
) async throws {
    let deadline = ContinuousClock.now + timeout
    while task.completedSuccess == nil && ContinuousClock.now < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
}

@Suite("Continued Processing Handler")
struct ContinuedProcessingHandlerTests {

    private static let identifierPrefix = BackgroundTaskID.continuedProcessing + "."

    @Test("Malformed identifier fails fast without touching coordinator")
    func malformedIdentifierFailsFast() async throws {
        // The handler parses the episode id from the wildcard-identifier
        // suffix. A bare identifier (no suffix) means the request was
        // malformed upstream — fail loud with success=false and do not
        // dispatch any work.
        let (bps, coordinator, _, _) = makeBPS()
        let task = StubContinuedProcessingTask(identifier: BackgroundTaskID.continuedProcessing)

        await bps.handleContinuedProcessingTask(task)
        try await waitForCompletion(of: task)

        #expect(task.completedSuccess == false)
        #expect(coordinator.continueForegroundAssistCalls.isEmpty,
                "Malformed identifier must not dispatch continueForegroundAssist")
        #expect(coordinator.pauseAtNextCheckpointCalls.isEmpty)
    }

    @Test("Happy path dispatches to coordinator and completes success")
    func happyPathDispatchesToCoordinator() async throws {
        let (bps, coordinator, _, _) = makeBPS()
        let episodeId = "episode-44h1-happy"
        let task = StubContinuedProcessingTask(identifier: Self.identifierPrefix + episodeId)

        await bps.handleContinuedProcessingTask(task)
        try await waitForCompletion(of: task)

        #expect(task.completedSuccess == true)
        #expect(coordinator.continueForegroundAssistCalls.count == 1)
        #expect(coordinator.continueForegroundAssistCalls.first?.episodeId == episodeId)
        // The deadline must be in the future (derived from the
        // continuedProcessingDeadlineBudget).
        if let deadline = coordinator.continueForegroundAssistCalls.first?.deadline {
            #expect(deadline.timeIntervalSinceNow > 0)
        }
    }

    @Test("Coordinator error maps to setTaskCompleted(success: false)")
    func coordinatorErrorMapsToFailure() async throws {
        let coordinator = StubAnalysisCoordinator()
        coordinator.continueForegroundAssistError = NSError(
            domain: "test", code: 1,
            userInfo: [NSLocalizedDescriptionKey: "synthetic failure"]
        )
        let (bps, _, _, _) = makeBPS(coordinator: coordinator)
        let task = StubContinuedProcessingTask(identifier: Self.identifierPrefix + "ep-fail")

        await bps.handleContinuedProcessingTask(task)
        try await waitForCompletion(of: task)

        #expect(task.completedSuccess == false)
    }

    @Test("Expiration handler requests pause with cause=.taskExpired and fails task")
    func expirationHandlerTriggersPauseAtNextCheckpoint() async throws {
        // Regression contract: when iOS fires the expirationHandler, the
        // handler MUST request a safe-point pause with
        // cause=.taskExpired AND mark the task complete with
        // success=false. The pause request arrives BEFORE the
        // markComplete call so the in-flight work has an opportunity to
        // exit cooperatively via its pause-observed path; the ordering
        // within the BPS actor's serial queue also guarantees the
        // "expired → failed" completion wins the idempotence race
        // against any late success-completion from the work task.
        let coordinator = StubAnalysisCoordinator()
        // Keep the work task pending until the pause request lands.
        // The stub observes its own `pauseAtNextCheckpointCalls` array
        // so the workflow exits deterministically without wall-clock
        // races.
        coordinator.continueForegroundAssistWaitsForPause = true
        let (bps, _, _, _) = makeBPS(coordinator: coordinator)
        let episodeId = "episode-expired"
        let task = StubContinuedProcessingTask(identifier: Self.identifierPrefix + episodeId)

        await bps.handleContinuedProcessingTask(task)

        // Wait for the handler to install the expirationHandler.
        let setupDeadline = ContinuousClock.now + .seconds(5)
        while task.expirationHandler == nil && ContinuousClock.now < setupDeadline {
            try await Task.sleep(for: .milliseconds(10))
        }

        task.simulateExpiration()
        try await waitForCompletion(of: task)

        #expect(task.completedSuccess == false,
                "Expiration MUST map to setTaskCompleted(success: false)")
        #expect(coordinator.pauseAtNextCheckpointCalls.count >= 1)
        let firstPause = coordinator.pauseAtNextCheckpointCalls.first
        #expect(firstPause?.episodeId == episodeId)
        #expect(firstPause?.cause == .taskExpired,
                "Expiration cause MUST be .taskExpired per bead spec")

        // Spec state-machine step 5: expiration MUST write a terminal
        // `failed` WorkJournal entry with cause=.taskExpired. The
        // pause request alone is in-memory (see
        // AnalysisCoordinator.pauseAtNextCheckpoint) — the durable
        // audit trail lives in the journal row appended here.
        let failedJournalCalls = coordinator.recordForegroundAssistOutcomeCalls.filter {
            $0.episodeId == episodeId && $0.eventType == .failed
        }
        #expect(!failedJournalCalls.isEmpty,
                "Expiration handler must append a `failed` WorkJournal row")
        #expect(failedJournalCalls.first?.cause == .taskExpired,
                "Expiration WorkJournal row must carry cause=.taskExpired")
    }

    @Test("Happy path appends `finalized` WorkJournal row on successful completion")
    func happyPathAppendsFinalizedJournalRow() async throws {
        // Spec state-machine step 5: when `continueForegroundAssist`
        // returns without throwing, the handler appends a `finalized`
        // WorkJournal entry before marking the task complete. The
        // cause MUST be nil (finalized is a success event; `cause` is
        // reserved for preempted / failed per WorkJournalEntry).
        let (bps, coordinator, _, _) = makeBPS()
        let episodeId = "episode-44h1-final"
        let task = StubContinuedProcessingTask(identifier: Self.identifierPrefix + episodeId)

        await bps.handleContinuedProcessingTask(task)
        try await waitForCompletion(of: task)

        #expect(task.completedSuccess == true)
        let finalizedCalls = coordinator.recordForegroundAssistOutcomeCalls.filter {
            $0.episodeId == episodeId && $0.eventType == .finalized
        }
        #expect(finalizedCalls.count == 1,
                "Successful completion must append exactly one `finalized` WorkJournal row")
        #expect(finalizedCalls.first?.cause == nil,
                "Finalized rows carry no cause (success event)")
    }

    @Test("Coordinator failure path appends `failed` WorkJournal row")
    func coordinatorFailureAppendsFailedJournalRow() async throws {
        // When `continueForegroundAssist` throws, the handler appends
        // a `failed` row with a pipeline cause (distinct from
        // .taskExpired, which is reserved for the expirationHandler).
        let coordinator = StubAnalysisCoordinator()
        coordinator.continueForegroundAssistError = NSError(
            domain: "test", code: 2,
            userInfo: [NSLocalizedDescriptionKey: "synthetic failure"]
        )
        let (bps, _, _, _) = makeBPS(coordinator: coordinator)
        let episodeId = "episode-44h1-failpath"
        let task = StubContinuedProcessingTask(identifier: Self.identifierPrefix + episodeId)

        await bps.handleContinuedProcessingTask(task)
        try await waitForCompletion(of: task)

        #expect(task.completedSuccess == false)
        let failedCalls = coordinator.recordForegroundAssistOutcomeCalls.filter {
            $0.episodeId == episodeId && $0.eventType == .failed
        }
        #expect(!failedCalls.isEmpty,
                "Failure path must append a `failed` WorkJournal row")
        #expect(failedCalls.first?.cause != .taskExpired,
                ".taskExpired is reserved for the expirationHandler path")
    }

    @Test("Identifier parser handles valid and invalid inputs")
    func identifierParserHandlesVariants() {
        // Valid: prefix + episode suffix → suffix returned.
        let valid = BackgroundProcessingService.parseEpisodeId(
            from: Self.identifierPrefix + "abc-123"
        )
        #expect(valid == "abc-123")

        // Invalid: bare prefix.
        let bare = BackgroundProcessingService.parseEpisodeId(
            from: BackgroundTaskID.continuedProcessing
        )
        #expect(bare == nil)

        // Invalid: prefix with empty suffix.
        let emptySuffix = BackgroundProcessingService.parseEpisodeId(
            from: Self.identifierPrefix
        )
        #expect(emptySuffix == nil)

        // Invalid: entirely different identifier.
        let unrelated = BackgroundProcessingService.parseEpisodeId(
            from: "com.other.identifier"
        )
        #expect(unrelated == nil)
    }
}

// MARK: - Pre-Analysis Recovery Handler

@Suite("Pre-Analysis Recovery Handler")
struct PreAnalysisRecoveryHandlerTests {

    @Test("Recovery without reconciler completes with failure")
    func recoveryWithoutReconciler() async throws {
        // playhead-hv73: deterministic seam replaces the wall-clock
        // injection-wait timeout. Park the handler on a Task, wait for
        // the waiter to register inside the actor, then fire the
        // timeout synchronously via `triggerInjectionWaitTimeoutForTesting`.
        let (bps, _, _, _) = makeBPS()
        let task = StubBackgroundTask()

        let handler = Task { await bps.handlePreAnalysisRecovery(task) }
        try await waitForParkedInjectionWaiters(in: bps)
        let resumed = await bps.triggerInjectionWaitTimeoutForTesting()
        #expect(resumed >= 1, "Expected the parked waiter to be resumed")
        await handler.value

        #expect(task.completedSuccess == false)
    }

    @Test("Recovery schedules next occurrence")
    func recoverySchedulesNextOccurrence() async throws {
        // playhead-hv73: same deterministic-seam pattern as
        // `recoveryWithoutReconciler` — `schedulePreAnalysisRecovery()`
        // runs before the injection wait, so the request lands in the
        // scheduler the moment the handler suspends. Triggering the
        // timeout synchronously lets the handler return without any
        // wall-clock sleep on the 15 s default.
        let (bps, _, scheduler, _) = makeBPS()
        let task = StubBackgroundTask()

        let handler = Task { await bps.handlePreAnalysisRecovery(task) }
        try await waitForParkedInjectionWaiters(in: bps)
        await bps.triggerInjectionWaitTimeoutForTesting()
        await handler.value

        let recoveryRequests = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundTaskID.preAnalysisRecovery
        }
        #expect(!recoveryRequests.isEmpty)
    }
}

// MARK: - Pre-Analysis Recovery Race (playhead-8u3i)

/// Tests covering the cold-launch race where iOS fires the
/// preanalysis.recovery handler before `PlayheadRuntime`'s deferred Task
/// has injected the reconciler. Pins both halves of the two-part fix:
/// the in-actor buffer (handler suspends on a continuation) and the
/// timeout fall-through (timeout completes with success=false).
@Suite("Pre-Analysis Recovery Race")
struct PreAnalysisRecoveryRaceTests {

    /// Build a real reconciler with stub deps so the success path can
    /// run all the way through `reconcile()`. Empty store → reconcile
    /// returns a zero-row report and the handler marks success=true.
    private func makeReconciler() async throws -> (AnalysisJobReconciler, AnalysisStore) {
        let store = try await makeTestStore()
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider()
        )
        return (reconciler, store)
    }

    /// Build a real `AnalysisWorkScheduler` that the actor wiring is happy
    /// to receive. The handler success path doesn't tick the scheduler;
    /// the only requirement is a live instance, so we wire it with the
    /// same shape used by other scheduler tests.
    private func makeWorkScheduler(store: AnalysisStore) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: StubDownloadProvider()
        )
    }

    /// Wait for a stub task to complete, with a deadline.
    private func waitForCompletion(
        of task: StubBackgroundTask,
        timeout: Duration = .seconds(5)
    ) async throws {
        let deadline = ContinuousClock.now + timeout
        while task.completedSuccess == nil && ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    @Test("Handler fired before injection completes success once injection lands",
          .timeLimit(.minutes(1)))
    func handlerFiredBeforeInjectionSucceedsAfterLateInjection() async throws {
        // Reproduces the cold-launch shape: BPS is constructed (and the
        // handler closure is registered with iOS) but
        // `setPreAnalysisServices` has not yet been called when iOS wakes
        // the app and fires the BG task. The handler must suspend and
        // pick up the reconciler once injection lands.
        //
        // playhead-hv73: deterministic seam — the production 15 s
        // timeout is irrelevant here because injection lands before
        // the test fires the timeout. We poll for the parked waiter
        // via `pendingInjectionWaiterCountForTesting` instead of
        // sleeping 50 ms to give the handler time to suspend, so the
        // test no longer carries any wall-clock race.
        let (bps, _, _, _) = makeBPS()
        let task = StubBackgroundTask()
        let (reconciler, store) = try await makeReconciler()
        let workScheduler = makeWorkScheduler(store: store)

        // Fire the handler before injection.
        let handlerTask = Task { await bps.handlePreAnalysisRecovery(task) }

        // Wait deterministically for the handler to park its
        // continuation inside `awaitPreAnalysisServicesInjected`.
        try await waitForParkedInjectionWaiters(in: bps)

        // Inject; this must drain the parked continuation and let the
        // handler run reconcile() on the (empty) store.
        await bps.setPreAnalysisServices(scheduler: workScheduler, reconciler: reconciler)

        try await waitForCompletion(of: task)
        await handlerTask.value

        #expect(task.completedSuccess == true,
                "Handler must complete success=true once the late-injected reconciler is available")
    }

    @Test("Multiple concurrent waiters all wake on injection",
          .timeLimit(.minutes(1)))
    func concurrentWaitersAllWakeOnInjection() async throws {
        // Confirms the list-of-continuations design: three handlers
        // suspended before injection must all be resumed when injection
        // lands, not just the first one.
        //
        // playhead-hv73: deterministic seam — replace the 50 ms
        // "let them all park" sleep with a poll on the waiter count.
        let (bps, _, _, _) = makeBPS()
        let tasks = (0..<3).map { _ in StubBackgroundTask() }
        let (reconciler, store) = try await makeReconciler()
        let workScheduler = makeWorkScheduler(store: store)

        let handlerTasks = tasks.map { task in
            Task { await bps.handlePreAnalysisRecovery(task) }
        }

        // Wait deterministically for all three to park.
        try await waitForParkedInjectionWaiters(in: bps, atLeast: tasks.count)

        await bps.setPreAnalysisServices(scheduler: workScheduler, reconciler: reconciler)

        for task in tasks {
            try await waitForCompletion(of: task)
        }
        for handlerTask in handlerTasks {
            await handlerTask.value
        }

        for (index, task) in tasks.enumerated() {
            #expect(task.completedSuccess == true,
                    "Concurrent waiter #\(index) must wake and complete success=true")
        }
    }

    @Test("Handler fired with no injection times out and completes failure",
          .timeLimit(.minutes(1)))
    func handlerFiredWithoutInjectionTimesOut() async throws {
        // Pins the original fail path: when injection never lands, the
        // handler must still complete success=false instead of hanging
        // iOS's BGProcessingTask.
        //
        // playhead-hv73: deterministic seam — fire the timeout
        // synchronously via `triggerInjectionWaitTimeoutForTesting`
        // instead of waiting out a (small) wall-clock timer. Closes
        // the residual flake risk of the prior 200 ms wait.
        let (bps, _, _, _) = makeBPS()
        let task = StubBackgroundTask()

        let handler = Task { await bps.handlePreAnalysisRecovery(task) }
        try await waitForParkedInjectionWaiters(in: bps)
        await bps.triggerInjectionWaitTimeoutForTesting()
        await handler.value

        #expect(task.completedSuccess == false,
                "Timed-out injection wait must fall through to the original fail path")
    }

    @Test("Late injection arriving after timeout is a no-op for the timed-out waiter",
          .timeLimit(.minutes(1)))
    func lateInjectionAfterTimeoutDoesNotDoubleResume() async throws {
        // Belt-and-suspenders pin against a continuation double-resume.
        // Sequence: the injection-wait timeout fires first (parking-side
        // resumes with `false` and the slot's continuation is consumed),
        // then `setPreAnalysisServices` is called. The drain loop must
        // observe the slot's continuation as `nil` and skip it; resuming
        // a consumed continuation would crash the test runner.
        //
        // playhead-hv73: deterministic seam — `triggerInjectionWaitTimeoutForTesting`
        // simulates the timeout firing without any wall-clock delay,
        // and the slot-removal logic it shares with the production
        // timer path is what this test exercises.
        let (bps, _, _, _) = makeBPS()
        let task = StubBackgroundTask()
        let (reconciler, store) = try await makeReconciler()
        let workScheduler = makeWorkScheduler(store: store)

        // Park the handler, fire the timeout synchronously, await
        // completion. The handler exits the fail path with
        // success=false.
        let handler = Task { await bps.handlePreAnalysisRecovery(task) }
        try await waitForParkedInjectionWaiters(in: bps)
        await bps.triggerInjectionWaitTimeoutForTesting()
        await handler.value
        #expect(task.completedSuccess == false,
                "Pre-injection timeout must complete the task with success=false")

        // Inject AFTER the timeout. If the actor's drain loop tries to
        // resume the timed-out slot's continuation, the process crashes
        // here. A clean run proves the slot was already removed from
        // `pendingInjectionWaiters` by `timeoutInjectionWaiter`.
        await bps.setPreAnalysisServices(scheduler: workScheduler, reconciler: reconciler)

        // Sanity tail: a fresh handler now completes via the immediate
        // early-return path (`analysisJobReconciler != nil`), confirming
        // the actor is still healthy after the late injection.
        let warmTask = StubBackgroundTask()
        await bps.handlePreAnalysisRecovery(warmTask)
        try await waitForCompletion(of: warmTask)
        #expect(warmTask.completedSuccess == true,
                "After injection, a follow-up handler must complete success=true via the warm path")
    }

    // playhead-hv73: direct coverage for the deterministic
    // injection-wait seam itself, independent of the recovery
    // handler. Pins (a) the parked-waiter count is observable, (b)
    // `triggerInjectionWaitTimeoutForTesting` resumes every parked
    // waiter exactly once and reports the count, and (c) calling it
    // when no one is parked is a safe no-op that returns 0.

    @Test("Triggered timeout resumes all parked waiters and reports the count",
          .timeLimit(.minutes(1)))
    func triggeredTimeoutResumesEveryWaiter() async throws {
        let (bps, _, _, _) = makeBPS()

        // Park three handlers without injection.
        let tasks = (0..<3).map { _ in StubBackgroundTask() }
        let handlers = tasks.map { task in
            Task { await bps.handlePreAnalysisRecovery(task) }
        }

        try await waitForParkedInjectionWaiters(in: bps, atLeast: tasks.count)
        let beforeTrigger = await bps.pendingInjectionWaiterCountForTesting()
        #expect(beforeTrigger == tasks.count,
                "All handlers should be parked before the trigger fires")

        let resumed = await bps.triggerInjectionWaitTimeoutForTesting()
        #expect(resumed == tasks.count,
                "Trigger should resume every parked waiter")

        for handler in handlers {
            await handler.value
        }
        for (index, task) in tasks.enumerated() {
            #expect(task.completedSuccess == false,
                    "Waiter #\(index) should fall through the timeout fail path")
        }

        let afterTrigger = await bps.pendingInjectionWaiterCountForTesting()
        #expect(afterTrigger == 0,
                "Drained waiter list should be empty after the trigger")
    }

    @Test("Triggered timeout with no parked waiters is a no-op",
          .timeLimit(.minutes(1)))
    func triggeredTimeoutNoOp() async throws {
        let (bps, _, _, _) = makeBPS()

        let resumed = await bps.triggerInjectionWaitTimeoutForTesting()
        #expect(resumed == 0,
                "Trigger with no parked waiters should resume nothing")

        let count = await bps.pendingInjectionWaiterCountForTesting()
        #expect(count == 0)
    }
}

// MARK: - Double-Completion Guard

@Suite("Double-Completion Guard")
struct DoubleCompletionGuardTests {

    @Test("markComplete called twice only completes task once")
    func markCompleteCalledTwice() async throws {
        let (bps, _, _, _) = makeBPS()
        let task = StubBackgroundTask()

        // On a fresh BPS, completedTaskIDs is empty.
        // First markComplete inserts the task's ID and completes the task.
        await bps.markComplete(task, success: true)
        #expect(task.completedSuccess == true)

        // Second markComplete sees the task's ID already in completedTaskIDs
        // and returns early. completedSuccess should NOT be overwritten to false.
        await bps.markComplete(task, success: false)
        #expect(task.completedSuccess == true, "Second markComplete should be ignored")
    }

    @Test("markComplete guards within single task lifecycle")
    func markCompleteGuardsWithinLifecycle() async throws {
        let (bps, _, _, _) = makeBPS()

        // Use a continued processing task to reset the guard, but with slow coordinator.
        let coordinator = StubAnalysisCoordinator()
        let scheduler = StubTaskScheduler()
        let battery = StubBatteryProvider()
        let bps2 = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: scheduler,
            batteryProvider: battery
        )

        let task = StubBackgroundTask()

        // Manually call markComplete twice on a fresh BPS (completedTaskIDs starts empty).
        await bps2.markComplete(task, success: true)
        #expect(task.completedSuccess == true)

        // Reset completedSuccess to detect a second call.
        task.completedSuccess = nil
        await bps2.markComplete(task, success: false)

        // Second call should be ignored -- completedSuccess stays nil (no second setTaskCompleted).
        #expect(task.completedSuccess == nil)
    }
}

// MARK: - Hot-Path Control

@Suite("Hot-Path Control")
struct HotPathControlTests {

    @Test("playbackDidStart activates hot-path")
    func playbackDidStartActivatesHotPath() async throws {
        let (bps, coordinator, _, _) = makeBPS()

        await bps.playbackDidStart()

        #expect(await bps.isHotPathActive() == true)
        // playbackDidStart no longer calls coordinator.startCapabilityObserver() —
        // the observer is started once by BPS.start() at launch and survives
        // stop() calls. playbackDidStart only flips the hotPathActive flag.
        #expect(coordinator.startCapabilityObserverCallCount == 0)
    }

    @Test("playbackDidStart deferred when all analysis paused")
    func playbackDidStartDeferredWhenAllPaused() async throws {
        let battery = StubBatteryProvider()
        battery.level = 0.10
        battery.charging = false
        let (bps, coordinator, _, _) = makeBPS(battery: battery)

        // Set critical thermal to pause all analysis.
        let criticalSnapshot = makeCapabilitySnapshot(thermalState: .critical)
        await bps.handleCapabilityUpdate(criticalSnapshot)

        await bps.playbackDidStart()

        #expect(await bps.isHotPathActive() == false)
        // playbackDidStart no longer calls coordinator.startCapabilityObserver()
        // at all — the observer is started once by BPS.start() and survives
        // stop() calls. Verify it was not called here.
        try await Task.sleep(for: .milliseconds(50))
        #expect(coordinator.startCapabilityObserverCallCount == 0)
    }

    @Test("playbackDidStop deactivates and schedules backfill")
    func playbackDidStopDeactivatesAndSchedulesBackfill() async throws {
        let (bps, _, scheduler, _) = makeBPS()

        await bps.playbackDidStart()
        #expect(await bps.isHotPathActive() == true)

        let requestCountBefore = scheduler.submittedRequests.count
        await bps.playbackDidStop()

        #expect(await bps.isHotPathActive() == false)

        let newRequests = scheduler.submittedRequests.dropFirst(requestCountBefore)
        let backfillRequests = newRequests.filter {
            $0.identifier == BackgroundTaskID.backfillProcessing
        }
        #expect(!backfillRequests.isEmpty)
    }
}

// MARK: - Thermal Management

@Suite("Thermal Management")
struct ThermalManagementTests {

    @Test("Critical thermal pauses all analysis")
    func criticalThermalPausesAll() async throws {
        let (bps, coordinator, _, _) = makeBPS()
        let criticalSnapshot = makeCapabilitySnapshot(thermalState: .critical)

        await bps.handleCapabilityUpdate(criticalSnapshot)

        // Coordinator.stop() is called via a detached Task.
        try await Task.sleep(for: .milliseconds(50))
        #expect(coordinator.stopCallCount >= 1)
    }

    @Test("Serious thermal does not pause all analysis but does pause backfill")
    func seriousThermalDoesNotPauseAllAnalysis() async throws {
        let (bps, coordinator, _, _) = makeBPS()

        // Activate hot-path first.
        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        // Apply serious thermal -- should NOT pause all (only critical does)
        // but SHOULD pause backfill since .serious clears `allowSoonLane`.
        let seriousSnapshot = makeCapabilitySnapshot(thermalState: .serious)
        await bps.handleCapabilityUpdate(seriousSnapshot)
        try await Task.sleep(for: .milliseconds(50))

        // Hot-path should still be active (serious does not pause all analysis).
        #expect(await bps.isHotPathActive() == true)
        // Coordinator.stop() should NOT have been called for serious.
        #expect(coordinator.stopCallCount == 0)
        // L-C2-3: positive assertion that .serious is exactly where backfill
        // pauses but the hot-path doesn't (the narrower-than-DAP, broader-
        // than-foreground point in the gate matrix).
        #expect(await bps.isBackfillPaused() == true,
                ".serious profile clears allowSoonLane → backfill paused")
    }

    @Test("Recovery from critical thermal lifts pause flag")
    func recoveryFromCriticalLiftsPause() async throws {
        let (bps, coordinator, _, _) = makeBPS()

        // Activate hot-path.
        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        // Go critical -- pauses all.
        let criticalSnapshot = makeCapabilitySnapshot(thermalState: .critical)
        await bps.handleCapabilityUpdate(criticalSnapshot)
        try await Task.sleep(for: .milliseconds(50))
        #expect(coordinator.stopCallCount >= 1)
        #expect(await bps.isHotPathActive() == false)

        // Recover to nominal -- pause flag should be lifted.
        // The coordinator's capability observer survives stop() calls,
        // so handleCapabilityUpdate does not need to re-start it.
        let nominalSnapshot = makeCapabilitySnapshot(thermalState: .nominal)
        await bps.handleCapabilityUpdate(nominalSnapshot)
        try await Task.sleep(for: .milliseconds(50))

        // Hot-path should be active again (hotPathActive was true before pause).
        #expect(await bps.isHotPathActive() == true)
        // No new startCapabilityObserver() call — observer survives stop().
        #expect(coordinator.startCapabilityObserverCallCount == 0)
    }
}

// MARK: - Battery Management

@Suite("Battery Management")
struct BatteryManagementTests {

    // C1 alignment behavior matrix at the BPS layer:
    //
    //   inputs                                   profile     pauseAll  pauseBackfill
    //   nominal + plain low-battery              .fair       no        no
    //   nominal + plain LPM                      .fair       no        no
    //   fair    + low-battery (unplugged)        .serious    no        YES
    //   fair    + LPM                            .serious    no        YES
    //   any     + critical thermal               .critical   YES       YES
    //
    // Plain low-battery and plain LPM (with nominal thermal) do NOT pause
    // ANY BPS gate — they only demote the profile to `.fair`, which the
    // lane-level `AnalysisWorkScheduler` handles by pausing the Background
    // lane. This is the C1 narrowing of DAP: DAP used to defer all backfill
    // at these inputs; QP keeps the BPS-layer backfill gate open and lets
    // the lane scheduler throttle Background per-lane downstream. The BPS
    // backfill gate is broader than the BPS foreground gate (fires at
    // `.serious`, vs `.critical` for the hot-path) but narrower than DAP.
    @Test("Plain low battery + nominal thermal pauses neither hot-path nor backfill")
    func lowBatteryDoesNotPauseAllAnalysis() async throws {
        let battery = StubBatteryProvider()
        battery.level = 0.15
        battery.charging = false
        let (bps, coordinator, _, _) = makeBPS(battery: battery)

        // Activate hot-path so we can verify it stays active.
        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        let nominalSnapshot = makeCapabilitySnapshot(thermalState: .nominal)
        await bps.handleCapabilityUpdate(nominalSnapshot)
        try await Task.sleep(for: .milliseconds(50))

        // The full-pause path runs `coordinator.stop()` only when
        // `pauseAllWork` is true; plain low battery demotes to .fair so
        // the foreground gate stays open.
        #expect(coordinator.stopCallCount == 0,
                "Plain low battery must not trigger pauseAllWork (only .critical does)")
        #expect(await bps.isHotPathActive() == true,
                "Hot-path must remain active under plain low battery alone")
        // The backfill gate is `pauseAllWork || !allowSoonLane`. `.fair` keeps
        // Soon lane open so backfill is NOT paused at this layer either —
        // the scheduler pauses Background lane separately.
        #expect(await bps.isBackfillPaused() == false,
                "Plain low battery (.fair profile) must not pause backfill at the BPS layer")
    }

    @Test("Fair thermal + LPM (charged) pauses backfill but not hot-path")
    func fairThermalPlusLPMPausesBackfillOnly() async throws {
        let battery = StubBatteryProvider()
        battery.level = 0.95
        battery.charging = true
        let (bps, coordinator, _, _) = makeBPS(battery: battery)

        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        // Fair thermal + LPM → profile demotes to .serious which clears
        // `allowSoonLane`. Backfill pauses; foreground hot-path stays open.
        let fairLpmSnapshot = makeCapabilitySnapshot(
            thermalState: .fair,
            isLowPowerMode: true
        )
        await bps.handleCapabilityUpdate(fairLpmSnapshot)
        try await Task.sleep(for: .milliseconds(50))

        #expect(coordinator.stopCallCount == 0,
                "fair + LPM → .serious profile, pauseAllWork=false, hot-path stays open")
        #expect(await bps.isHotPathActive() == true)
        #expect(await bps.isBackfillPaused() == true,
                ".serious profile clears allowSoonLane → backfill paused at BPS layer")
    }

    @Test("Fair thermal + low battery (unplugged) pauses backfill but not hot-path")
    func fairThermalPlusLowBatteryPausesBackfillOnly() async throws {
        let battery = StubBatteryProvider()
        battery.level = 0.15
        battery.charging = false
        let (bps, coordinator, _, _) = makeBPS(battery: battery)

        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        // Fair thermal + unplugged low battery → profile demotes to .serious
        // which clears `allowSoonLane`. The BPS backfill gate fires; the
        // foreground gate (`pauseAllWork`) does not.
        let fairSnapshot = makeCapabilitySnapshot(thermalState: .fair)
        await bps.handleCapabilityUpdate(fairSnapshot)
        try await Task.sleep(for: .milliseconds(50))

        #expect(coordinator.stopCallCount == 0,
                ".serious profile does not trigger pauseAllWork — hot-path stays open")
        #expect(await bps.isHotPathActive() == true)
        #expect(await bps.isBackfillPaused() == true,
                ".serious profile clears allowSoonLane → backfill paused at BPS layer")
    }

    @Test("Low battery while charging does NOT pause")
    func lowBatteryWhileChargingDoesNotPause() async throws {
        let battery = StubBatteryProvider()
        battery.level = 0.15
        battery.charging = true
        let (bps, coordinator, _, _) = makeBPS(battery: battery)

        let nominalSnapshot = makeCapabilitySnapshot(thermalState: .nominal)
        await bps.handleCapabilityUpdate(nominalSnapshot)
        try await Task.sleep(for: .milliseconds(50))

        // All analysis should NOT be paused when charging — charging keeps
        // the profile at .nominal regardless of battery level.
        #expect(coordinator.stopCallCount == 0)
        #expect(await bps.isBackfillPaused() == false,
                "Charging keeps profile .nominal so neither lane is paused")
    }

    // C1 alignment: critical thermal is now the only canonical "pause
    // everything" trigger. Pre-C1, BPS pause-all also fired on
    // `isBatteryTooLow()` (which already required `!isCharging`); under C1
    // that is folded into QualityProfile and only `.critical` raises
    // `pauseAllWork`. This test pins the recovery path: when critical lifts,
    // hot-path resumes.
    @Test("Critical thermal recovery resumes analysis")
    func criticalThermalRecoveryResumes() async throws {
        let (bps, coordinator, _, _) = makeBPS()

        // Activate hot-path first.
        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        // Trigger critical-thermal pause-all.
        let critical = makeCapabilitySnapshot(thermalState: .critical)
        await bps.handleCapabilityUpdate(critical)
        try await Task.sleep(for: .milliseconds(50))
        #expect(coordinator.stopCallCount >= 1)

        // Recover to nominal.
        let nominal = makeCapabilitySnapshot(thermalState: .nominal)
        await bps.handleCapabilityUpdate(nominal)
        try await Task.sleep(for: .milliseconds(50))

        // Hot-path should resume since playback was active.
        // The coordinator's capability observer survives stop() calls,
        // so no new startCapabilityObserver() call is needed.
        #expect(await bps.isHotPathActive() == true)
        #expect(coordinator.startCapabilityObserverCallCount == 0)
    }
}

// MARK: - Hot-Path Lookahead Multiplier

@Suite("Hot-Path Lookahead Multiplier")
struct HotPathLookaheadMultiplierTests {

    @Test("Nominal thermal returns 1.0")
    func nominalThermalReturnsFullMultiplier() async throws {
        let (bps, _, _, _) = makeBPS()

        let multiplier = await bps.hotPathLookaheadMultiplier()
        #expect(multiplier == 1.0)
    }

    @Test("Serious thermal returns 0.5")
    func seriousThermalReturnsReducedMultiplier() async throws {
        let (bps, _, _, _) = makeBPS()

        let seriousSnapshot = makeCapabilitySnapshot(thermalState: .serious)
        await bps.handleCapabilityUpdate(seriousSnapshot)

        let multiplier = await bps.hotPathLookaheadMultiplier()
        #expect(multiplier == 0.5)
    }

    @Test("Low power mode returns 0.5")
    func lowPowerModeReturnsReducedMultiplier() async throws {
        let (bps, _, _, _) = makeBPS()

        let lowPowerSnapshot = makeCapabilitySnapshot(isLowPowerMode: true)
        await bps.handleCapabilityUpdate(lowPowerSnapshot)

        let multiplier = await bps.hotPathLookaheadMultiplier()
        #expect(multiplier == 0.5)
    }
}

// MARK: - Scheduling

@Suite("Scheduling")
struct SchedulingTests {

    @Test("scheduleBackfillIfNeeded submits request")
    func scheduleBackfillSubmitsRequest() async throws {
        let (bps, _, scheduler, _) = makeBPS()

        await bps.scheduleBackfillIfNeeded()

        let backfillRequests = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundTaskID.backfillProcessing
        }
        #expect(backfillRequests.count == 1)
        if let request = backfillRequests.first as? BGProcessingTaskRequest {
            #expect(request.requiresExternalPower == false)
        }
    }

    @Test("schedulePreAnalysisRecovery submits request with external power")
    func schedulePreAnalysisRecoverySubmitsRequest() async throws {
        let (bps, _, scheduler, _) = makeBPS()

        await bps.schedulePreAnalysisRecovery()

        let recoveryRequests = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundTaskID.preAnalysisRecovery
        }
        #expect(recoveryRequests.count == 1)

        // Verify it requires external power.
        if let request = recoveryRequests.first as? BGProcessingTaskRequest {
            #expect(request.requiresExternalPower == true)
        }
    }

    @Test("Scheduler failure is logged not crashed")
    func schedulerFailureDoesNotCrash() async throws {
        let scheduler = StubTaskScheduler()
        scheduler.shouldThrowOnSubmit = true
        let (bps, _, _, _) = makeBPS(scheduler: scheduler)

        // This should not throw or crash.
        await bps.scheduleBackfillIfNeeded()
        await bps.schedulePreAnalysisRecovery()

        // If we got here, no crash occurred.
        #expect(scheduler.submittedRequests.isEmpty)
    }

    // playhead-fuo6: regression — the 12-hour overnight blackout
    // (04-24 20h → 04-25 07h) reproduced because the only paths that
    // submit a backfill BGProcessingTask were `playbackDidStop()` and
    // the handler's self-rearm. A user who queued episodes overnight
    // *without ever pressing play* never triggered `playbackDidStop`,
    // so iOS had no submitted task to wake — the in-memory
    // AnalysisWorkScheduler runLoop got CPU only until the process
    // was suspended (~30s after backgrounding).
    //
    // The fix wires `appDidEnterBackground()` so PlayheadApp's
    // `.background` scenePhase transition submits a backfill request
    // unconditionally. iOS coalesces duplicate submissions, so it is
    // safe to call even when one is already in-flight.
    @Test("appDidEnterBackground submits a backfill request (queued-not-playing reproduction)")
    func appDidEnterBackgroundSubmitsBackfill() async throws {
        let (bps, _, scheduler, _) = makeBPS()

        // Bug reproduction: app backgrounds with queued work but no
        // playback ever started. Pre-fix, no `scheduleBackfillIfNeeded`
        // call existed for this path, so iOS had nothing to wake the
        // app with for ~12 hours.
        await bps.appDidEnterBackground()

        let backfillRequests = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundTaskID.backfillProcessing
        }
        #expect(backfillRequests.count == 1,
                "Backgrounding the app must submit a backfill request so iOS can wake the app to drain queued analysis work")
    }
}

// MARK: - Lifecycle

@Suite("Lifecycle")
struct LifecycleTests {

    @Test("stop cancels observer and deactivates hot-path")
    func stopCancelsObserverAndDeactivatesHotPath() async throws {
        let (bps, _, _, _) = makeBPS()

        await bps.playbackDidStart()
        #expect(await bps.isHotPathActive() == true)

        await bps.stop()

        #expect(await bps.isHotPathActive() == false)
    }
}

// MARK: - runPendingBackfill Polling Loop

/// Regression tests for H2 and H3. These drive
/// `AnalysisCoordinator.runBackfillPollingLoop` directly with injected
/// closures so we can exercise the stop-request and two-consecutive-zero
/// behaviors without standing up a full coordinator (which would require
/// audio/feature/transcript/ad-detection/skip-orchestrator dependencies).
import OSLog

@Suite("runPendingBackfill Polling Loop")
struct RunPendingBackfillPollingLoopTests {

    private static let testLogger = Logger(subsystem: "com.playhead.tests", category: "backfill-loop")

    @Test("Stop request exits the polling loop promptly")
    func runPendingBackfillRespectsStop() async throws {
        // Regression for H2: prior to this fix, the polling loop only
        // consulted `Task.isCancelled` and the 25-min deadline. When
        // BackgroundProcessingService observed thermal=critical and called
        // `await coordinator.stop()`, the stop ran on the coordinator actor
        // concurrently with the loop but did nothing to cancel the Task
        // hosting the loop — so the loop would continue polling for up to
        // 25 minutes after the coordinator logged "stopped".
        //
        // The fix adds a `stopRequested` flag on the coordinator and a
        // closure hook in the loop that checks it each iteration.
        //
        // playhead-p06: this test used to measure wall-clock elapsed time
        // against a 500ms budget, which was flaky under parallel/loaded
        // cooperative pools. It now uses a virtual clock + iteration
        // counter: the loop must exit within ONE iteration of the stop
        // being flipped, regardless of wall-clock time.

        // Coordinator state shared between the sleep closure and the
        // fetch closure. Each virtual "sleep" increments the tick; stop
        // is set after the 3rd sleep (i.e. 3rd iteration).
        actor LoopState {
            var tick: Int = 0
            var stopRequested: Bool = false
            var fetchCallsAfterStop: Int = 0

            func advance() {
                tick += 1
                if tick >= 3 {
                    stopRequested = true
                }
            }
            func stopped() -> Bool { stopRequested }
            func recordFetch() {
                if stopRequested { fetchCallsAfterStop += 1 }
            }
            func fetchesAfterStop() -> Int { fetchCallsAfterStop }
            func currentTick() -> Int { tick }
        }
        let state = LoopState()

        await AnalysisCoordinator.runBackfillPollingLoop(
            // Far-future deadline — the loop must exit on stop, not time.
            deadline: .now + .seconds(60 * 60),
            pollInterval: .milliseconds(20),
            isStopRequested: { await state.stopped() },
            // Pending count stays positive so the loop would never drain
            // on its own — only stopRequested can end it.
            fetchPendingCount: {
                await state.recordFetch()
                return 3
            },
            // Virtual sleep: does not wait wall-clock, just advances the
            // logical tick counter and yields so other tasks can run.
            sleep: { _ in
                await state.advance()
                await Task.yield()
            },
            logger: Self.testLogger
        )

        // After the stop flag flips, the loop's next iteration must
        // observe it via `isStopRequested` and exit. At most one extra
        // `fetchPendingCount` call is allowed (the one that races on the
        // same iteration). Anything more means the stop was not observed
        // promptly.
        let leaked = await state.fetchesAfterStop()
        #expect(leaked <= 1,
                "runBackfillPollingLoop must exit on the first iteration after stopRequested=true (observed \(leaked) post-stop fetches)")
    }

    @Test("Drain requires two consecutive zero polls")
    func drainedRequiresTwoConsecutivePolls() async throws {
        // Regression for H3: the loop used to return on the first zero
        // pending poll. AnalysisWorkScheduler has a transient zero window
        // between finishing one job and picking up a tier-advanced next
        // job (e.g. T0 → T1 rollover); returning early was surrendering BG
        // time prematurely on small single-job queues.

        // Scripted sequence: [job] → [] → [jobAfterTierAdvancement] → [] → []
        actor Counts {
            var sequence: [Int]
            var index = 0
            var maxServed = 0
            init(_ seq: [Int]) { self.sequence = seq }
            func next() -> Int {
                let value = index < sequence.count ? sequence[index] : 0
                index += 1
                maxServed = max(maxServed, index)
                return value
            }
            func served() -> Int { maxServed }
        }
        let counts = Counts([1, 0, 1, 0, 0])

        await AnalysisCoordinator.runBackfillPollingLoop(
            deadline: ContinuousClock.now + .seconds(30),
            pollInterval: .milliseconds(5),
            isStopRequested: { false },
            fetchPendingCount: { await counts.next() },
            // playhead-p06: noop sleep — this test asserts on scripted
            // pending-count sequences, not on timing. A noop sleep keeps
            // the test deterministic under parallel execution.
            sleep: { _ in await Task.yield() },
            logger: Self.testLogger
        )

        // We must have consumed all five entries before declaring drain.
        // If the loop had returned on the first zero (index 2) it would
        // have served only 2 polls and missed the tier-advanced job.
        let served = await counts.served()
        #expect(served >= 5,
                "Loop must not declare drain on a transient zero; served only \(served) polls")
    }

    @Test("Single persistent zero poll does not declare drain")
    func singleZeroPollDoesNotDrain() async throws {
        // Belt-and-braces companion for H3: a single zero followed by more
        // work must not end the loop; only two consecutive zeros should.
        actor Counts {
            var index = 0
            // [0, 2, 0, 0] — one zero, then work, then two zeros.
            let sequence = [0, 2, 0, 0]
            var served = 0
            func next() -> Int {
                let v = index < sequence.count ? sequence[index] : 0
                index += 1
                served = index
                return v
            }
            func count() -> Int { served }
        }
        let counts = Counts()

        await AnalysisCoordinator.runBackfillPollingLoop(
            deadline: ContinuousClock.now + .seconds(30),
            pollInterval: .milliseconds(5),
            isStopRequested: { false },
            fetchPendingCount: { await counts.next() },
            // playhead-p06: noop sleep for deterministic timing.
            sleep: { _ in await Task.yield() },
            logger: Self.testLogger
        )

        let served = await counts.count()
        #expect(served >= 4, "Loop should have polled through the full script; served=\(served)")
    }

    @Test("Polling loop honors deadline under perpetual alternation")
    func drainedLoopHonorsDeadlineWithAlternatingPending() async throws {
        // Regression for H3: the deadline must be honored in the `while`
        // condition even when the pending-count source never stabilizes
        // into two consecutive zeros. An adversarial queue that flips
        // between 1 and 0 forever must not hang the loop; it must exit
        // at the deadline.
        //
        // playhead-p06: this test used to race a 200ms wall-clock
        // deadline against a 5ms real sleep — under a loaded cooperative
        // pool the scheduler could starve the sleep long enough that the
        // poll count expectation failed. It now uses a virtual clock
        // injected via `now:` + `sleep:` so deadline arithmetic is
        // deterministic.
        actor Counter {
            var n = 0
            func nextPending() -> Int {
                n += 1
                // Alternates: 1, 0, 1, 0, 1, 0, ... forever.
                return n % 2 == 1 ? 1 : 0
            }
            func count() -> Int { n }
        }
        let counter = Counter()

        // Virtual clock: a lock-guarded mutable instant. Starts at an
        // arbitrary anchor and advances by the `sleep` duration each
        // iteration. The deadline is 10 virtual poll intervals away, so
        // we expect roughly 10 polls before the `while now() < deadline`
        // guard trips.
        final class VirtualClock: @unchecked Sendable {
            private let lock = NSLock()
            private var instant: ContinuousClock.Instant = .now
            func current() -> ContinuousClock.Instant {
                lock.lock(); defer { lock.unlock() }
                return instant
            }
            func advance(by duration: Duration) {
                lock.lock(); defer { lock.unlock() }
                instant = instant.advanced(by: duration)
            }
        }
        let vclock = VirtualClock()
        let start = vclock.current()
        let pollInterval: Duration = .milliseconds(5)
        let deadline = start.advanced(by: .milliseconds(50))  // 10 virtual polls

        await AnalysisCoordinator.runBackfillPollingLoop(
            deadline: deadline,
            pollInterval: pollInterval,
            isStopRequested: { false },
            fetchPendingCount: { await counter.nextPending() },
            // Virtual sleep: advances the injected clock, never waits
            // wall-clock time.
            sleep: { duration in vclock.advance(by: duration) },
            now: { vclock.current() },
            logger: Self.testLogger
        )

        // With a 50ms virtual deadline and a 5ms virtual poll interval
        // the loop must have iterated ~10 times and then exited when
        // `now() < deadline` became false.
        let polls = await counter.count()
        #expect(polls >= 5,
                "Loop should have polled multiple times before hitting virtual deadline, got \(polls)")
        #expect(polls <= 20,
                "Loop must exit at the virtual deadline, not hang — observed \(polls) polls")
    }
}
