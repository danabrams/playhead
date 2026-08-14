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

    // playhead-vsot: waits in this file are event-driven —
    // `awaitCompletion()` / `awaitExpirationHandlerInstalled()` on the
    // stub task, `stopCalls.wait(for:)` on the stub coordinator, and
    // the actor's `waitForPendingInjectionWaitersForTesting` seam. The
    // previous 5–10 s deadline polls starved under full-suite load
    // (the handler's ledger start task takes a MainActor hop before
    // markComplete can run), turning behavior tests into scheduler
    // tests. Each test's `.timeLimit` is the hang backstop.

    // MARK: - Backfill happy path

    @Test("Backfill happy path writes a terminal ledger row (noEligibleWork on fresh queue)",
          .timeLimit(.minutes(1)))
    func backfillHappyPathPersistsOutcome() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let (bps, _) = makeBPS(ledger: ledger)
        let task = StubBackgroundTask()

        await bps.handleBackfillTask(task)
        await task.awaitCompletion()

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

        // Wait for the expiration handler to be installed — event-driven
        // (playhead-vsot), no deadline to starve under load.
        await task.awaitExpirationHandlerInstalled()

        task.simulateExpiration()
        _ = await workTask.value

        // The expirationHandler's unstructured Task runs, in order:
        // emitExpire → (await row-insert) → finishRun(.expired) →
        // handleExpiredProcessingTask → markComplete(success: false).
        // The stub's completion signal therefore fires strictly AFTER
        // the terminal `.expired` write — awaiting it replaces the old
        // 5 s fetch-poll with the actual completion signal, and a
        // single fetch afterwards is deterministic.
        await task.awaitCompletion()
        let latest = await ledger.fetchLatestRun(for: .backfill)

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
        await task.awaitCompletion()

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
        // The stop() call is the reliable signal that the expiration
        // Task ran finishRun and the idempotence guard had a chance to
        // fire. Event-driven (playhead-vsot): await the stub's stop
        // signal directly — no deadline to starve, no fixed sleep to
        // pass vacuously.
        await coordinator.stopCalls.wait(for: 1)
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
        // Wait for the handler to reach the suspend point — event-driven
        // seam (playhead-vsot), resumed the moment the waiter parks.
        await bps.waitForPendingInjectionWaitersForTesting()
        await bps.triggerInjectionWaitTimeoutForTesting()
        _ = await recoveryWork.value
        await task.awaitCompletion()

        let latest = await ledger.fetchLatestRun(for: .preAnalysisRecovery)
        #expect(latest?.outcome == .failed)
        #expect(latest?.lastErrorCode == "reconciler_unavailable")
        #expect(latest?.taskIdentifier == BackgroundTaskID.preAnalysisRecovery)
    }

    // MARK: - playhead-8ljj: a barren window must say WHY

    /// **THE BEAD, AS ONE ASSERTION.**
    ///
    /// Two granted windows: one banked 37 scan rows, one banked nothing. On the
    /// 2026-08-14 device pull that pair writes the SAME ledger row — of 73
    /// backfill rows lasting ≥ 60 s, 22 of the 23 that banked zero carry a
    /// tuple (`outcome`, `cause`, `jobsSeen`, `jobsCompleted`, `expiration`) a
    /// productive window also wrote. Everything the runner knew about the
    /// difference went to OSLog and was dropped.
    ///
    /// The test is written as the COMPARISON rather than as a string check on
    /// one row, because "records something" is not the property; "records
    /// something DIFFERENT" is. A fix that wrote a constant would pass a
    /// single-row assertion and fail this one.
    @Test("A barren window and a productive one no longer write the same row",
          .timeLimit(.minutes(1)))
    func barrenAndProductiveWindowsAreDistinguishable() async throws {
        func run(banked: Int) async throws -> BackgroundTaskRunRecord? {
            let store = try await makeTestStore()
            let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
            let coordinator = StubAnalysisCoordinator()
            coordinator.coarseScanPhaseReport = CoarseScanPhaseReport(
                verdict: .drove, scanned: 4, candidates: 4
            )
            coordinator.runPendingCoarseScansResult = 4
            coordinator.semanticScanRowsRecordedResult = banked
            let (bps, _) = makeBPS(ledger: ledger, coordinator: coordinator)
            let task = StubBackgroundTask()
            await bps.handleBackfillTask(task)
            await task.awaitCompletion()
            return await ledger.fetchLatestRun(for: .backfill)
        }

        let productive = try #require(await run(banked: 37))
        let barren = try #require(await run(banked: 0))

        // Every column the ledger carried BEFORE this bead still agrees —
        // which is precisely why it could not answer the question.
        #expect(productive.outcome == barren.outcome)
        #expect(productive.cause == barren.cause)
        #expect(productive.jobsSeen == barren.jobsSeen)
        #expect(productive.jobsCompleted == barren.jobsCompleted)
        #expect(productive.expiration == barren.expiration)

        // And the row now distinguishes them anyway.
        #expect(productive.deferReason == "coarse=drove(4/4) banked=37")
        #expect(barren.deferReason == "coarse=drove(4/4) banked=0")
        #expect(productive.deferReason != barren.deferReason,
                """
                A window that banked 37 durable scan rows and a window that \
                banked none must not leave the same durable record. This is \
                the whole bead.
                """)
    }

    /// The expiration path is the one that matters: 100 of the 181 rows on the
    /// 2026-08-14 pull are `expired`, so for most grants it is the ONLY path
    /// that reaches a terminal ledger write. A reason published only on the
    /// normal return is a reason the majority of windows never file.
    ///
    /// The stub publishes its census and then parks, which is exactly what a
    /// real phase reclaimed mid-asset does — so the verdict under test is
    /// `inflight`, the state that exists precisely because a phase that never
    /// returns has no return value to read.
    @Test("An expired window carries the coarse phase's last true statement",
          .timeLimit(.minutes(1)))
    func expiredWindowCarriesTheCoarseReason() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let coordinator = StubAnalysisCoordinator()
        coordinator.coarseScanPhaseReport = CoarseScanPhaseReport(
            verdict: .inflight, scanned: 0, candidates: 4
        )
        // Park INSIDE the coarse phase, after the census is published.
        coordinator.runPendingCoarseScansDuration = .seconds(30)
        coordinator.semanticScanRowsRecordedResult = 0
        let (bps, _) = makeBPS(ledger: ledger, coordinator: coordinator)

        let task = StubBackgroundTask()
        let workTask = Task { await bps.handleBackfillTask(task) }
        await task.awaitExpirationHandlerInstalled()
        task.simulateExpiration()
        _ = await workTask.value
        await task.awaitCompletion()

        let latest = try #require(await ledger.fetchLatestRun(for: .backfill))
        #expect(latest.outcome == .expired)
        #expect(latest.deferReason == "coarse=inflight(0/4) banked=0",
                """
                The expiration handler must persist the coarse phase's account \
                too. Before this bead every `expired` row carried \
                deferReason=NULL, which is 100 of the 181 rows on the pull.
                """)
    }

    /// "Nothing to do" and "could not tell whether there was anything to do"
    /// are different findings, and the bead's acceptance criterion names the
    /// distinction explicitly. `runPendingCoarseScans` has always treated a
    /// thrown candidate query as empty-for-this-grant — the right call — and
    /// has always logged it as the failure it is. The log is not queryable;
    /// the row is.
    @Test("An unreadable candidate population is not an empty one",
          .timeLimit(.minutes(1)))
    func unreadableCandidatesAreNotAMeasuredAbsence() async throws {
        func run(unreadable: CoarseScanCandidateReadFailures) async throws -> String? {
            let store = try await makeTestStore()
            let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
            let coordinator = StubAnalysisCoordinator()
            coordinator.coarseScanPhaseReport = CoarseScanPhaseReport(
                verdict: .empty, scanned: 0, candidates: 0, unreadable: unreadable
            )
            coordinator.semanticScanRowsRecordedResult = 0
            let (bps, _) = makeBPS(ledger: ledger, coordinator: coordinator)
            let task = StubBackgroundTask()
            await bps.handleBackfillTask(task)
            await task.awaitCompletion()
            return await ledger.fetchLatestRun(for: .backfill)?.deferReason
        }

        #expect(try await run(unreadable: []) == "coarse=empty(0/0) banked=0")
        #expect(try await run(unreadable: .resumable)
                == "coarse=empty(0/0) banked=0 unread=resumable")
        #expect(try await run(unreadable: [.resumable, .missingCoverageLaneRows])
                == "coarse=empty(0/0) banked=0 unread=resumable+missingRows")
    }

    /// A count that could not be TAKEN is not a count of zero. Same rule
    /// `BackgroundGrantCounters` follows for `jobsSeen`/`jobsCompleted`, and
    /// the same reason: this bead exists because a ledger collapsed "not
    /// measured" into "measured zero".
    @Test("An unmeasurable banked count reads `?`, never `0`",
          .timeLimit(.minutes(1)))
    func unmeasurableBankedCountIsNotZero() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let coordinator = StubAnalysisCoordinator()
        coordinator.coarseScanPhaseReport = CoarseScanPhaseReport(
            verdict: .empty, scanned: 0, candidates: 0
        )
        coordinator.semanticScanRowsRecordedResult = nil
        let (bps, _) = makeBPS(ledger: ledger, coordinator: coordinator)
        let task = StubBackgroundTask()

        await bps.handleBackfillTask(task)
        await task.awaitCompletion()

        #expect(await ledger.fetchLatestRun(for: .backfill)?.deferReason
                == "coarse=empty(0/0) banked=?")
    }

    /// The count must be scoped to THIS grant. A count over the whole table
    /// would report every window as productive the moment any window ever was
    /// — the wrong-population error, arrived at through the fix for it.
    @Test("The banked count is asked about this grant, not the whole table",
          .timeLimit(.minutes(1)))
    func bankedCountIsScopedToTheGrant() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let coordinator = StubAnalysisCoordinator()
        coordinator.coarseScanPhaseReport = CoarseScanPhaseReport(
            verdict: .empty, scanned: 0, candidates: 0
        )
        let (bps, _) = makeBPS(ledger: ledger, coordinator: coordinator)
        let task = StubBackgroundTask()

        let before = Date().timeIntervalSince1970
        await bps.handleBackfillTask(task)
        await task.awaitCompletion()
        let after = Date().timeIntervalSince1970

        let asked = try #require(coordinator.semanticScanRowsRecordedCalls.first)
        #expect(coordinator.semanticScanRowsRecordedCalls.count == 1)
        #expect(asked >= before && asked <= after,
                """
                The `since` handed to the count must be this grant's own \
                opening instant — read before the first await, like every \
                other quantity the handler measures the window with.
                """)
    }

    /// A window that never reached the coarse phase at all must leave
    /// `deferReason` alone rather than fabricate a verdict for a phase that
    /// did not run. `finishRun` binds through `COALESCE(?, deferReason)`, so
    /// `nil` is a real "say nothing" and this pins that it stays one.
    @Test("A phase that never reported writes no coarse verdict",
          .timeLimit(.minutes(1)))
    func aPhaseThatNeverReportedWritesNothing() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let coordinator = StubAnalysisCoordinator()
        // No `coarseScanPhaseReport` — the stub publishes nothing, which is
        // what a grant that ended before the candidate queries answered looks
        // like.
        coordinator.semanticScanRowsRecordedResult = 0
        let (bps, _) = makeBPS(ledger: ledger, coordinator: coordinator)
        let task = StubBackgroundTask()

        await bps.handleBackfillTask(task)
        await task.awaitCompletion()

        #expect(await ledger.fetchLatestRun(for: .backfill)?.deferReason == nil)
    }
}
