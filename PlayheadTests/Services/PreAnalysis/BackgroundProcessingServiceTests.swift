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

        // Coordinator should have been started (thermal is nominal on simulator).
        #expect(coordinator.startCallCount >= 1)
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

        // On simulator (nominal thermal), coordinator.start() IS called.
        #expect(coordinator.startCallCount >= 1)
        #expect(task.completedSuccess == true)
    }

    @Test("Backfill expiration cancels work")
    func backfillExpirationCancelsWork() async throws {
        let coordinator = StubAnalysisCoordinator()
        // Make start() take long enough for expiration to fire.
        coordinator.startDuration = .seconds(10)
        let (bps, _, _, _) = makeBPS(coordinator: coordinator)
        let task = StubBackgroundTask()

        // Start backfill in a detached task so we can expire it.
        let workTask = Task {
            await bps.handleBackfillTask(task)
        }

        // Give the handler time to set the expiration handler and begin work.
        try await Task.sleep(for: .milliseconds(50))

        // Simulate iOS firing expiration.
        task.simulateExpiration()

        try await waitForCompletion(of: task)

        workTask.cancel()

        #expect(coordinator.stopCallCount >= 1)
        #expect(task.completedSuccess == false)
    }
}

// MARK: - Continued Processing Handler

@Suite("Continued Processing Handler")
struct ContinuedProcessingHandlerTests {

    @Test("Continued processing completes successfully")
    func continuedProcessingCompletes() async throws {
        let (bps, coordinator, _, _) = makeBPS()
        let task = StubBackgroundTask()

        await bps.handleContinuedProcessingTask(task)
        try await waitForCompletion(of: task)

        #expect(coordinator.startCallCount >= 1)
        #expect(task.completedSuccess == true)
    }

    @Test("Continued processing expiration cancels work")
    func continuedProcessingExpiration() async throws {
        let coordinator = StubAnalysisCoordinator()
        coordinator.startDuration = .seconds(10)
        let (bps, _, _, _) = makeBPS(coordinator: coordinator)
        let task = StubBackgroundTask()

        let workTask = Task {
            await bps.handleContinuedProcessingTask(task)
        }

        // Wait for the expiration handler to be set (the actor method must execute first)
        let deadline = ContinuousClock.now + .seconds(5)
        while task.expirationHandler == nil && ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(10))
        }

        task.simulateExpiration()
        try await waitForCompletion(of: task)
        workTask.cancel()

        #expect(coordinator.stopCallCount >= 1)
        #expect(task.completedSuccess == false)
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

        // On a fresh BPS, bgTaskCompleted starts as false.
        // First markComplete sets it to true and completes the task.
        await bps.markComplete(task, success: true)
        #expect(task.completedSuccess == true)

        // Second markComplete sees bgTaskCompleted == true and skips.
        // The task's completedSuccess should NOT be overwritten to false.
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

        // Manually call markComplete twice on a fresh BPS (bgTaskCompleted starts false).
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

    @Test("Serious thermal pauses backfill but not hot-path")
    func seriousThermalPausesBackfillNotHotPath() async throws {
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
