// BackgroundProcessingServiceTelemetryTests.swift
// playhead-shpy: assert BackgroundProcessingService emits the four
// BG-task lifecycle events (submit / start / complete / expire) into
// the injected `BGTaskTelemetryLogging` recorder. These tests use a
// protocol-injected recorder so the production code under test never
// touches the real `BGTaskScheduler`.

import BackgroundTasks
import Foundation
import Testing

@testable import Playhead

@Suite("BackgroundProcessingService BG-task telemetry — playhead-shpy")
struct BackgroundProcessingServiceTelemetryTests {

    // MARK: - Submit

    @Test("scheduleBackfillIfNeeded emits a `submit` row with success=true")
    func scheduleBackfillEmitsSubmit() async throws {
        let recorder = RecordingBGTaskTelemetryLogger()
        let bps = BackgroundProcessingService(
            coordinator: StubAnalysisCoordinator(),
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            bgTelemetry: recorder
        )

        await bps.scheduleBackfillIfNeeded()
        let events = try await recorder.eventsMatching(event: "submit", timeout: .seconds(2))

        #expect(events.count >= 1)
        let submit = events.first { $0.identifier == BackgroundTaskID.backfillProcessing }
        #expect(submit != nil)
        #expect(submit?.submitSucceeded == true)
    }

    @Test("scheduleBackfillIfNeeded emits a `submit` row with success=false on throw")
    func scheduleBackfillEmitsSubmitFailureOnThrow() async throws {
        let recorder = RecordingBGTaskTelemetryLogger()
        let scheduler = StubTaskScheduler()
        scheduler.shouldThrowOnSubmit = true
        let bps = BackgroundProcessingService(
            coordinator: StubAnalysisCoordinator(),
            capabilitiesService: CapabilitiesService(),
            taskScheduler: scheduler,
            batteryProvider: StubBatteryProvider(),
            bgTelemetry: recorder
        )

        await bps.scheduleBackfillIfNeeded()
        let events = try await recorder.eventsMatching(event: "submit", timeout: .seconds(2))

        let submit = events.first { $0.identifier == BackgroundTaskID.backfillProcessing }
        #expect(submit?.submitSucceeded == false)
        #expect(submit?.submitError != nil)
    }

    // MARK: - Start / complete pair

    @Test("handleBackfillTask emits matching `start` and `complete` rows")
    func backfillEmitsStartAndComplete() async throws {
        let recorder = RecordingBGTaskTelemetryLogger()
        let bps = BackgroundProcessingService(
            coordinator: StubAnalysisCoordinator(),
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            bgTelemetry: recorder
        )
        let task = StubBackgroundTask()

        await bps.handleBackfillTask(task)

        // Wait for the background work task to finish so the `complete`
        // row is in the recorder. The handler kicks off an unstructured
        // Task that calls runPendingBackfill on the stub — which is
        // synchronous and quick — then markComplete.
        let deadline = ContinuousClock.now + .seconds(10)
        while task.completedSuccess == nil && ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(10))
        }

        let starts = try await recorder.eventsMatching(event: "start", timeout: .seconds(2))
        let completes = try await recorder.eventsMatching(event: "complete", timeout: .seconds(2))

        let start = starts.first { $0.identifier == BackgroundTaskID.backfillProcessing }
        let complete = completes.first { $0.identifier == BackgroundTaskID.backfillProcessing }
        #expect(start != nil)
        #expect(complete != nil)
        // Same instance ID means a downstream tool can pair them.
        #expect(start?.taskInstanceID == complete?.taskInstanceID)
        #expect(complete?.success == true)
    }

    // MARK: - Expire

    @Test("backfill expirationHandler emits an `expire` row")
    func backfillEmitsExpireOnExpiration() async throws {
        let recorder = RecordingBGTaskTelemetryLogger()
        let coordinator = StubAnalysisCoordinator()
        coordinator.runPendingBackfillDuration = .seconds(30) // keep work pending
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            bgTelemetry: recorder
        )
        let task = StubBackgroundTask()

        let workTask = Task {
            await bps.handleBackfillTask(task)
        }

        // Wait for the expiration handler to be installed.
        let setupDeadline = ContinuousClock.now + .seconds(5)
        while task.expirationHandler == nil && ContinuousClock.now < setupDeadline {
            try await Task.sleep(for: .milliseconds(10))
        }

        task.simulateExpiration()
        _ = await workTask.value

        let expires = try await recorder.eventsMatching(event: "expire", timeout: .seconds(5))
        let expire = expires.first { $0.identifier == BackgroundTaskID.backfillProcessing }
        #expect(expire != nil)
        #expect(expire?.detail == "backfill-task-expired")
    }
}

// MARK: - RecordingBGTaskTelemetryLogger

/// Test recorder that captures every event for after-the-fact assertion.
/// Backed by an actor so the recorder is Sendable and thread-safe
/// without resorting to NSLock (which Swift Concurrency forbids in
/// async contexts).
actor RecordingBGTaskTelemetryActor {
    private(set) var events: [BGTaskTelemetryEvent] = []

    func append(_ event: BGTaskTelemetryEvent) {
        events.append(event)
    }

    func snapshot() -> [BGTaskTelemetryEvent] {
        events
    }
}

struct RecordingBGTaskTelemetryLogger: BGTaskTelemetryLogging {
    let inner = RecordingBGTaskTelemetryActor()

    func record(_ event: BGTaskTelemetryEvent) async {
        await inner.append(event)
    }

    func snapshot() async -> [BGTaskTelemetryEvent] {
        await inner.snapshot()
    }

    /// Spin until at least one event with the given discriminator is
    /// observed, or the deadline expires. Production code emits via
    /// fire-and-forget `Task { await logger.record(...) }`, so a test
    /// that observes too early will see an empty buffer.
    func eventsMatching(
        event: String,
        timeout: Duration
    ) async throws -> [BGTaskTelemetryEvent] {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            let matches = await snapshot().filter { $0.event == event }
            if !matches.isEmpty {
                return matches
            }
            try await Task.sleep(for: .milliseconds(10))
        }
        return await snapshot().filter { $0.event == event }
    }
}
