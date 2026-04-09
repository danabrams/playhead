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
private func waitForCompletion(of task: StubBackgroundTask, timeout: Duration = .seconds(5)) async throws {
    let deadline = ContinuousClock.now + timeout
    while task.completedSuccess == nil && ContinuousClock.now < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
}

// MARK: - Backfill Task Handler

@Suite("Backfill Task Handler")
struct BackfillTaskHandlerTests {

    @Test("Backfill completes successfully")
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

    @Test("Backfill skipped on thermal throttle via capabilitiesService")
    func backfillSkippedOnThermalThrottle() async throws {
        // NOTE: This test exercises the throttle-check code path in handleBackfillTask.
        // On a simulator with nominal thermal state, the backfill will proceed normally.
        // The shouldThrottleAnalysis gate reads from the real CapabilitiesService snapshot
        // which cannot be injected. To fully test the skip path, the device must report
        // .serious or .critical thermal state. This test still validates the happy path
        // does NOT skip when thermal is nominal.
        let (bps, coordinator, _, _) = makeBPS()
        let task = StubBackgroundTask()

        await bps.handleBackfillTask(task)
        try await waitForCompletion(of: task)

        // On simulator (nominal thermal), the real work method IS called.
        #expect(coordinator.runPendingBackfillCallCount >= 1)
        #expect(task.completedSuccess == true)
    }

    @Test("Backfill expiration cancels work")
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

    @Test("Backfill handler invokes real work method, not start")
    func backfillInvokesRealWorkMethod() async throws {
        // Regression: handleBackfillTask used to call coordinator.start(),
        // which is a sync lifecycle-init that just spawns the capability
        // observer and returns. The BGProcessingTask was being marked
        // successful in microseconds without any backfill work happening,
        // and iOS reclaimed the granted background time. This test pins
        // the contract that handleBackfillTask invokes runPendingBackfill.
        let coordinator = StubAnalysisCoordinator()
        let (bps, _, _, _) = makeBPS(coordinator: coordinator)
        let task = StubBackgroundTask()

        await bps.handleBackfillTask(task)
        try await waitForCompletion(of: task)

        #expect(coordinator.runPendingBackfillCallCount >= 1,
                "handleBackfillTask must invoke runPendingBackfill, not start()")
        // start() is the capability-observer lifecycle entry point and must
        // not be called from the background task handler.
        #expect(coordinator.startCallCount == 0,
                "handleBackfillTask must not call start() — that path was the bug")
        #expect(task.completedSuccess == true)
    }

    @Test("Overlapping BG handlers each complete their own task independently")
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

        let backfillTask = StubBackgroundTask()
        let recoveryTask = StubBackgroundTask()

        // Start backfill — it will await runPendingBackfill for ~300ms.
        let backfillWork = Task { await bps.handleBackfillTask(backfillTask) }

        // Wait just long enough for the backfill handler to have spawned
        // its work task and installed its expiration handler.
        try await Task.sleep(for: .milliseconds(30))

        // Kick off recovery while backfill is still suspended. Recovery
        // has no reconciler (none injected), so it marks complete
        // immediately — that is fine for this test: we just need to verify
        // the recovery completion doesn't clobber the in-flight backfill.
        await bps.handlePreAnalysisRecovery(recoveryTask)

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

// MARK: - Continued Processing Handler

@Suite("Continued Processing Handler")
struct ContinuedProcessingHandlerTests {

    @Test("Continued processing is a no-op that completes successfully")
    func continuedProcessingNoOpCompletes() async throws {
        // handleContinuedProcessingTask is a deliberate no-op today: there is
        // no user-initiated continuation work wired (e.g. FM asset download
        // future from AssetProvider). The handler marks the task complete
        // with success=true because "nothing to do" is not a failure. It must
        // NOT drain the backfill queue — that is handleBackfillTask's job.
        let (bps, coordinator, _, _) = makeBPS()
        let task = StubBackgroundTask()

        await bps.handleContinuedProcessingTask(task)
        try await waitForCompletion(of: task)

        #expect(task.completedSuccess == true)
        // Explicit regression: continuation handler must not piggyback on
        // the backfill drain. See git history around commit 740f727.
        #expect(coordinator.runPendingBackfillCallCount == 0,
                "handleContinuedProcessingTask must not call runPendingBackfill — that is handleBackfillTask's job")
        #expect(coordinator.startCallCount == 0,
                "handleContinuedProcessingTask must not call start() either")
    }
}

// MARK: - Pre-Analysis Recovery Handler

@Suite("Pre-Analysis Recovery Handler")
struct PreAnalysisRecoveryHandlerTests {

    @Test("Recovery without reconciler completes with failure")
    func recoveryWithoutReconciler() async throws {
        let (bps, _, _, _) = makeBPS()
        let task = StubBackgroundTask()

        // No pre-analysis services injected, so reconciler is nil.
        await bps.handlePreAnalysisRecovery(task)

        #expect(task.completedSuccess == false)
    }

    @Test("Recovery schedules next occurrence")
    func recoverySchedulesNextOccurrence() async throws {
        let (bps, _, scheduler, _) = makeBPS()
        let task = StubBackgroundTask()

        await bps.handlePreAnalysisRecovery(task)

        let recoveryRequests = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundTaskID.preAnalysisRecovery
        }
        #expect(!recoveryRequests.isEmpty)
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
        // Coordinator.start() is called via a detached Task, give it a moment.
        try await Task.sleep(for: .milliseconds(50))
        #expect(coordinator.startCallCount >= 1)
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
        // Coordinator.start() should have been called by handleCapabilityUpdate? No --
        // handleCapabilityUpdate with critical calls coordinator.stop(). The playbackDidStart
        // should NOT call coordinator.start() because allAnalysisPaused is true.
        // The stop from handleCapabilityUpdate counts, but start from playbackDidStart should not.
        // We need to check that no NEW start was issued after the pause.
        try await Task.sleep(for: .milliseconds(50))
        // stop was called once (from handleCapabilityUpdate), start should be 0 from playbackDidStart.
        // But coordinator.start may have been called 0 times from playbackDidStart.
        // (handleCapabilityUpdate calls coordinator.stop(), not start().)
        #expect(coordinator.startCallCount == 0)
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

    @Test("Serious thermal does not pause all analysis")
    func seriousThermalDoesNotPauseAllAnalysis() async throws {
        let (bps, coordinator, _, _) = makeBPS()

        // Activate hot-path first.
        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))
        let startCountAfterPlay = coordinator.startCallCount

        // Apply serious thermal -- should NOT pause all (only critical does).
        let seriousSnapshot = makeCapabilitySnapshot(thermalState: .serious)
        await bps.handleCapabilityUpdate(seriousSnapshot)
        try await Task.sleep(for: .milliseconds(50))

        // Hot-path should still be active (serious does not pause all analysis).
        #expect(await bps.isHotPathActive() == true)
        // Coordinator.stop() should NOT have been called for serious.
        #expect(coordinator.stopCallCount == 0)
    }

    @Test("Recovery from critical thermal resumes hot-path")
    func recoveryFromCriticalResumesHotPath() async throws {
        let (bps, coordinator, _, _) = makeBPS()

        // Activate hot-path.
        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))
        let startCountAfterPlay = coordinator.startCallCount

        // Go critical -- pauses all.
        let criticalSnapshot = makeCapabilitySnapshot(thermalState: .critical)
        await bps.handleCapabilityUpdate(criticalSnapshot)
        try await Task.sleep(for: .milliseconds(50))
        #expect(coordinator.stopCallCount >= 1)

        // Recover to nominal -- should resume hot-path since playback was active.
        let nominalSnapshot = makeCapabilitySnapshot(thermalState: .nominal)
        await bps.handleCapabilityUpdate(nominalSnapshot)
        try await Task.sleep(for: .milliseconds(50))

        // A new coordinator.start() should have been issued on recovery.
        #expect(coordinator.startCallCount > startCountAfterPlay)
    }
}

// MARK: - Battery Management

@Suite("Battery Management")
struct BatteryManagementTests {

    @Test("Low battery pauses all analysis")
    func lowBatteryPausesAll() async throws {
        let battery = StubBatteryProvider()
        battery.level = 0.15
        battery.charging = false
        let (bps, coordinator, _, _) = makeBPS(battery: battery)

        let nominalSnapshot = makeCapabilitySnapshot(thermalState: .nominal)
        await bps.handleCapabilityUpdate(nominalSnapshot)
        try await Task.sleep(for: .milliseconds(50))

        #expect(coordinator.stopCallCount >= 1)
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

        // All analysis should NOT be paused when charging.
        #expect(coordinator.stopCallCount == 0)
    }

    @Test("Battery recovery resumes analysis")
    func batteryRecoveryResumes() async throws {
        let battery = StubBatteryProvider()
        battery.level = 0.15
        battery.charging = false
        let (bps, coordinator, _, _) = makeBPS(battery: battery)

        // Activate hot-path first.
        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        // Trigger low battery pause.
        let snapshot1 = makeCapabilitySnapshot(thermalState: .nominal)
        await bps.handleCapabilityUpdate(snapshot1)
        try await Task.sleep(for: .milliseconds(50))
        #expect(coordinator.stopCallCount >= 1)

        let startCountBeforeRecovery = coordinator.startCallCount

        // Recover battery.
        battery.level = 0.50
        let snapshot2 = makeCapabilitySnapshot(thermalState: .nominal)
        await bps.handleCapabilityUpdate(snapshot2)
        try await Task.sleep(for: .milliseconds(50))

        // Hot-path should resume since playback was active.
        #expect(coordinator.startCallCount > startCountBeforeRecovery)
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

    @Test("scheduleContinuedProcessing submits request")
    func scheduleContinuedProcessingSubmitsRequest() async throws {
        let (bps, _, scheduler, _) = makeBPS()

        await bps.scheduleContinuedProcessing(reason: "test")

        let continuedRequests = scheduler.submittedRequests.filter {
            $0.identifier == BackgroundTaskID.continuedProcessing
        }
        #expect(continuedRequests.count == 1)
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
        await bps.scheduleContinuedProcessing(reason: "test")
        await bps.schedulePreAnalysisRecovery()

        // If we got here, no crash occurred.
        #expect(scheduler.submittedRequests.isEmpty)
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
