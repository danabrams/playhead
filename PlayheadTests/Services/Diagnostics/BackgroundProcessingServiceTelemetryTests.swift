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
        let events = try await recorder.eventsMatching(event: "submit", timeout: .seconds(10))

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
        let events = try await recorder.eventsMatching(event: "submit", timeout: .seconds(10))

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

        let starts = try await recorder.eventsMatching(event: "start", timeout: .seconds(10))
        let completes = try await recorder.eventsMatching(event: "complete", timeout: .seconds(10))

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
///
/// Subscribers receive a live `AsyncStream` that yields every recorded
/// event the moment `append` returns. Tests use this to wait
/// event-driven (no polling) for the production fire-and-forget
/// telemetry `Task { await bgTelemetry.record(...) }` to land. Polling
/// was previously fragile under cross-suite parallel test load: the
/// poll task and production task competed for cooperative runtime slots,
/// so a 2 s deadline could elapse without the production task ever
/// scheduling — observed as flakes in submit/start/complete tests.
actor RecordingBGTaskTelemetryActor {
    private(set) var events: [BGTaskTelemetryEvent] = []
    private var continuations: [UUID: AsyncStream<BGTaskTelemetryEvent>.Continuation] = [:]

    func append(_ event: BGTaskTelemetryEvent) {
        events.append(event)
        for c in continuations.values { c.yield(event) }
    }

    func snapshot() -> [BGTaskTelemetryEvent] {
        events
    }

    /// Returns a stream that replays all events recorded so far, then
    /// yields each subsequent event as it arrives. Caller MUST invoke
    /// `unsubscribe(id:)` (or finish the iterator) so the continuation
    /// doesn't leak.
    func subscribe() -> (UUID, AsyncStream<BGTaskTelemetryEvent>) {
        let id = UUID()
        var continuation: AsyncStream<BGTaskTelemetryEvent>.Continuation!
        let stream = AsyncStream<BGTaskTelemetryEvent> { c in continuation = c }
        for e in events { continuation.yield(e) }
        continuations[id] = continuation
        return (id, stream)
    }

    func unsubscribe(id: UUID) {
        continuations.removeValue(forKey: id)?.finish()
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

    /// Wait until at least one event with the given discriminator is
    /// observed, or the deadline expires; then return ALL matching
    /// events in the snapshot. Event-driven via `AsyncStream` rather
    /// than polling, so a single runtime slot for the production task
    /// is enough — there is no cadence dependency. The timeout is the
    /// upper bound on how long the test will wait for the production
    /// fire-and-forget `Task { ... await bgTelemetry.record(...) }`
    /// to schedule and run.
    func eventsMatching(
        event: String,
        timeout: Duration
    ) async throws -> [BGTaskTelemetryEvent] {
        let initial = await snapshot().filter { $0.event == event }
        if !initial.isEmpty { return initial }

        let (id, stream) = await inner.subscribe()
        defer {
            let inner = self.inner
            Task { await inner.unsubscribe(id: id) }
        }

        await withTaskGroup(of: Void.self) { group in
            group.addTask {
                for await e in stream where e.event == event { return }
            }
            group.addTask {
                try? await Task.sleep(for: timeout)
            }
            await group.next()
            group.cancelAll()
        }

        return await snapshot().filter { $0.event == event }
    }
}
