// BackgroundProcessingServiceLedgerTests.swift
// playhead-hygc.1.4: pin the wiring between BackgroundProcessingService
// and BackgroundTaskRunLedger. These tests use a real
// AnalysisStoreBackgroundTaskRunLedger over a temp-dir AnalysisStore
// (per project mandate "real AnalysisStore in tests, not mocks") and
// drive the BPS through StubBackgroundTask + StubAnalysisCoordinator
// to assert that each handler path writes the expected ledger row.
//
// Coverage targets (per spec acceptance criteria):
//   - Backfill happy path → admittedWork OR noEligibleWork (depending
//     on baselinePending; on a fresh test store the queue is empty so
//     the outcome is noEligibleWork — that distinction itself is part
//     of the contract).
//   - Backfill expirationHandler → expired with cause=task_expired and
//     expiration=true persisted BEFORE markComplete teardown
//     (acceptance criterion: "Expiration handlers persist final
//     outcome before returning").
//   - Pre-analysis recovery without an injected reconciler →
//     failed/reconciler_unavailable.
//   - Idempotence: the expiration handler firing AFTER the work task
//     has already finished is a no-op on the ledger row.

import BackgroundTasks
import Foundation
import Testing

@testable import Playhead

@Suite("BackgroundProcessingService ↔ Ledger wiring — playhead-hygc.1.4")
struct BackgroundProcessingServiceLedgerTests {

    private func makeBPS(
        ledger: any BackgroundTaskRunLedger,
        coordinator: StubAnalysisCoordinator = StubAnalysisCoordinator()
    ) -> (BackgroundProcessingService, StubAnalysisCoordinator) {
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
        return (bps, coordinator)
    }

    private func waitForCompletion(of task: StubBackgroundTask, timeout: Duration = .seconds(10)) async throws {
        let deadline = ContinuousClock.now + timeout
        while task.completedSuccess == nil && ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    // MARK: - Backfill happy path

    @Test("Backfill happy path writes a terminal ledger row (noEligibleWork on fresh queue)",
          .timeLimit(.minutes(1)))
    func backfillHappyPathPersistsOutcome() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let (bps, _) = makeBPS(ledger: ledger)
        let task = StubBackgroundTask()

        await bps.handleBackfillTask(task)
        try await waitForCompletion(of: task)

        // Give the (potentially fire-and-forget) finishRun call a tick
        // to land. handleBackfillTask awaits the work-task internally
        // before markComplete, so by the time `task.completedSuccess`
        // is set, finishRun has already returned. Read the row directly.
        let latest = await ledger.fetchLatestRun(for: .backfill)
        #expect(latest != nil, "Backfill handler must write a ledger row")
        #expect(latest?.taskIdentifier == BackgroundTaskID.backfillProcessing)
        #expect(latest?.finishedAt != nil)
        // On a fresh test store the queue is empty (baselinePending=0)
        // so the outcome must be noEligibleWork, not admittedWork. This
        // distinction is the whole point of the ledger — it lets dogfood
        // diagnostics tell apart "ran with nothing to do" from
        // "actually drained jobs".
        #expect(latest?.outcome == .noEligibleWork)
        #expect(latest?.expiration == false)
    }

    // MARK: - Backfill expiration

    @Test("Backfill expirationHandler writes expired outcome before markComplete",
          .timeLimit(.minutes(1)))
    func backfillExpirationPersistsExpired() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        // Make runPendingBackfill stall so the expiration handler fires
        // while the work task is still parked.
        let coordinator = StubAnalysisCoordinator()
        coordinator.runPendingBackfillDuration = .seconds(30)
        let (bps, _) = makeBPS(ledger: ledger, coordinator: coordinator)

        let task = StubBackgroundTask()
        let workTask = Task { await bps.handleBackfillTask(task) }

        // Wait for the expiration handler to be installed.
        let setupDeadline = ContinuousClock.now + .seconds(5)
        while task.expirationHandler == nil && ContinuousClock.now < setupDeadline {
            try await Task.sleep(for: .milliseconds(10))
        }

        task.simulateExpiration()
        _ = await workTask.value

        // Wait for the row to flip to terminal. The expirationHandler
        // dispatches the finishRun call inside an unstructured Task so
        // we poll for the terminal write up to a reasonable bound.
        let deadline = ContinuousClock.now + .seconds(5)
        var latest: BackgroundTaskRunRecord?
        while ContinuousClock.now < deadline {
            latest = await ledger.fetchLatestRun(for: .backfill)
            if latest?.outcome == .expired { break }
            try await Task.sleep(for: .milliseconds(10))
        }

        #expect(latest?.outcome == .expired)
        #expect(latest?.cause == InternalMissCause.taskExpired.rawValue)
        #expect(latest?.expiration == true)
    }

    // MARK: - Idempotence under expiration race

    @Test("Backfill expiration after work-task completion is idempotent (work-task wins)",
          .timeLimit(.minutes(1)))
    func backfillExpirationAfterCompletionIsIdempotent() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let coordinator = StubAnalysisCoordinator()
        let (bps, _) = makeBPS(ledger: ledger, coordinator: coordinator)
        let task = StubBackgroundTask()

        await bps.handleBackfillTask(task)
        try await waitForCompletion(of: task)

        // Capture the row BEFORE simulating a late expiration.
        let beforeLate = await ledger.fetchLatestRun(for: .backfill)
        #expect(beforeLate?.outcome == .noEligibleWork)
        // Sanity: the happy-path work task must NOT call coordinator.stop()
        // — only the expiration teardown does. This is what lets us use
        // stopCallCount as a reliable "expiration handler chain ran"
        // barrier below (R5 fix: replace the previous fixed-50ms sleep,
        // which could let the test pass vacuously if the unstructured
        // Task hadn't been scheduled yet on a loaded sim).
        #expect(coordinator.stopCallCount == 0,
                "Pre-expiration: stop() must not have been called")

        // Simulate iOS firing expiration AFTER the work task already
        // finished. The ledger's idempotence guard must reject the
        // racing terminal write.
        task.simulateExpiration()

        // Wait for the expiration handler's unstructured Task to actually
        // run end-to-end. The chain is:
        //     simulateExpiration() → expirationHandler closure →
        //     workTask.cancel() (no-op, already finished) →
        //     Task { ... emitExpire ... finishRun (rejected by
        //     idempotence guard) ... handleExpiredProcessingTask →
        //     coordinator.stop() (observable!) → markComplete }
        // Polling for stopCallCount >= 1 is the only reliable signal
        // that the expiration handler's Task actually ran finishRun
        // and the idempotence guard had a chance to fire. A fixed sleep
        // could let the test pass for the wrong reason (Task never
        // scheduled → finishRun never attempted → guard never tested).
        let deadline = ContinuousClock.now + .seconds(5)
        while coordinator.stopCallCount == 0 && ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(10))
        }
        #expect(coordinator.stopCallCount >= 1,
                "Expiration handler Task must have run end-to-end (stop() observable)")

        let afterLate = await ledger.fetchLatestRun(for: .backfill)
        // Outcome MUST still be the work task's terminal write — the
        // late expiration must not stomp.
        #expect(afterLate?.outcome == .noEligibleWork)
        #expect(afterLate?.cause == nil)
        #expect(afterLate?.expiration == false)
    }

    // MARK: - Recovery — reconciler unavailable

    @Test("Pre-analysis recovery with no reconciler writes failed/reconciler_unavailable",
          .timeLimit(.minutes(1)))
    func recoveryNoReconcilerPersistsFailed() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let (bps, _) = makeBPS(ledger: ledger)
        let task = StubBackgroundTask()

        // Drive the recovery handler concurrently — it will park on
        // `awaitPreAnalysisServicesInjected`. Once parked, fire the
        // deterministic timeout seam so the no-reconciler path runs
        // without a wall-clock dependency on the production timeout.
        let recoveryWork = Task { await bps.handlePreAnalysisRecovery(task) }
        // Wait for the handler to reach the suspend point.
        let setupDeadline = ContinuousClock.now + .seconds(5)
        while ContinuousClock.now < setupDeadline {
            let parked = await bps.pendingInjectionWaiterCountForTesting()
            if parked >= 1 { break }
            try await Task.sleep(for: .milliseconds(1))
        }
        await bps.triggerInjectionWaitTimeoutForTesting()
        _ = await recoveryWork.value
        try await waitForCompletion(of: task)

        let latest = await ledger.fetchLatestRun(for: .preAnalysisRecovery)
        #expect(latest?.outcome == .failed)
        #expect(latest?.lastErrorCode == "reconciler_unavailable")
        #expect(latest?.taskIdentifier == BackgroundTaskID.preAnalysisRecovery)
    }
}
