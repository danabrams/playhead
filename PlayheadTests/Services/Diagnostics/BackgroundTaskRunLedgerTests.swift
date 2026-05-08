// BackgroundTaskRunLedgerTests.swift
// playhead-hygc.1.4: pin the durable per-run outcome ledger contract.
// Tests use a real AnalysisStore (per the project mandate "real
// AnalysisStore in tests, not mocks") so the SQLite read path is also
// exercised. The ledger sits behind the `BackgroundTaskRunLedger`
// protocol, so production wiring is just dependency injection — these
// tests directly construct `AnalysisStoreBackgroundTaskRunLedger`
// against a temp-dir store to exercise both the in-memory protocol
// surface and the persistence layer in one shot.
//
// Coverage targets (closed-enum outcome taxonomy):
//   - admittedWork      — backfill drained pending work
//   - noEligibleWork    — backfill ran with empty queue
//   - deferredThermal   — QualityProfile pauseAllWork
//   - deferredCapability — QualityProfile closed Soon lane (LPM)
//   - expired           — iOS expirationHandler fired
//   - failed            — handler hit unrecoverable error
//   - noOp              — recovery found nothing to repair
//   - recoveredWork     — recovery actually unblocked rows
// Plus:
//   - persistence across re-open (process restart simulation)
//   - idempotence: a second finishRun on a terminal row is a no-op
//   - latest-by-asset diagnostics path
//   - latest-by-entryPoint diagnostics path
//   - fetchRecentRuns ordering and limit semantics

import Foundation
import Testing

@testable import Playhead

@Suite("BackgroundTaskRunLedger — playhead-hygc.1.4")
struct BackgroundTaskRunLedgerTests {

    // MARK: - Start / finish round trip

    @Test("startRun then finishRun(admittedWork) advances the row to terminal")
    func startThenFinishAdmittedWork() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let runId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: "instance-A",
            scenePhase: "background"
        )

        let advanced = await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .admittedWork,
                jobsSeen: 5,
                jobsAdmitted: 5
            )
        )
        #expect(advanced)

        let latest = await ledger.fetchLatestRun(for: .backfill)
        #expect(latest?.runId == runId)
        #expect(latest?.outcome == .admittedWork)
        #expect(latest?.taskIdentifier == BackgroundTaskID.backfillProcessing)
        #expect(latest?.taskInstanceID == "instance-A")
        #expect(latest?.scenePhase == "background")
        #expect(latest?.jobsSeen == 5)
        #expect(latest?.jobsAdmitted == 5)
        #expect(latest?.finishedAt != nil)
    }

    @Test("noEligibleWork outcome persists with zero jobsAdmitted")
    func finishNoEligibleWork() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let runId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: nil,
            scenePhase: nil
        )
        let advanced = await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .noEligibleWork,
                jobsSeen: 0,
                jobsAdmitted: 0
            )
        )
        #expect(advanced)

        let latest = await ledger.fetchLatestRun(for: .backfill)
        #expect(latest?.outcome == .noEligibleWork)
        #expect(latest?.jobsAdmitted == 0)
    }

    // MARK: - Deferral outcomes

    @Test("deferredThermal records deferReason carrying QualityProfile")
    func finishDeferredThermal() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let runId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: nil,
            scenePhase: "background"
        )
        let advanced = await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .deferredThermal,
                deferReason: "profile=critical"
            )
        )
        #expect(advanced)

        let latest = await ledger.fetchLatestRun(for: .backfill)
        #expect(latest?.outcome == .deferredThermal)
        #expect(latest?.deferReason == "profile=critical")
    }

    @Test("deferredCapability outcome distinguishes LPM-stacked-on-fair")
    func finishDeferredCapability() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let runId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: nil,
            scenePhase: nil
        )
        let advanced = await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .deferredCapability,
                deferReason: "profile=fair"
            )
        )
        #expect(advanced)

        let latest = await ledger.fetchLatestRun(for: .backfill)
        #expect(latest?.outcome == .deferredCapability)
        #expect(latest?.deferReason == "profile=fair")
    }

    // MARK: - Expiration

    @Test("expired outcome carries cause=task_expired and expiration=true")
    func finishExpired() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let runId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: nil,
            scenePhase: nil
        )
        let advanced = await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .expired,
                cause: InternalMissCause.taskExpired.rawValue,
                expiration: true
            )
        )
        #expect(advanced)

        let latest = await ledger.fetchLatestRun(for: .backfill)
        #expect(latest?.outcome == .expired)
        #expect(latest?.cause == InternalMissCause.taskExpired.rawValue)
        #expect(latest?.expiration == true)
    }

    // MARK: - Recovery outcomes

    @Test("recoveredWork carries the count of repaired rows in jobsCompleted")
    func recoveryFinishRecoveredWork() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let runId = await ledger.startRun(
            entryPoint: .preAnalysisRecovery,
            taskIdentifier: BackgroundTaskID.preAnalysisRecovery,
            taskInstanceID: nil,
            scenePhase: nil
        )
        let advanced = await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .recoveredWork,
                jobsCompleted: 7
            )
        )
        #expect(advanced)

        let latest = await ledger.fetchLatestRun(for: .preAnalysisRecovery)
        #expect(latest?.outcome == .recoveredWork)
        #expect(latest?.jobsCompleted == 7)
    }

    @Test("recovery noOp outcome distinguishes from noEligibleWork")
    func recoveryFinishNoOp() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let runId = await ledger.startRun(
            entryPoint: .preAnalysisRecovery,
            taskIdentifier: BackgroundTaskID.preAnalysisRecovery,
            taskInstanceID: nil,
            scenePhase: nil
        )
        let advanced = await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .noOp,
                jobsCompleted: 0
            )
        )
        #expect(advanced)

        let latest = await ledger.fetchLatestRun(for: .preAnalysisRecovery)
        #expect(latest?.outcome == .noOp)
    }

    // MARK: - Failure path

    @Test("failed outcome captures lastErrorCode")
    func finishFailed() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let runId = await ledger.startRun(
            entryPoint: .preAnalysisRecovery,
            taskIdentifier: BackgroundTaskID.preAnalysisRecovery,
            taskInstanceID: nil,
            scenePhase: nil
        )
        let advanced = await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .failed,
                lastErrorCode: "reconciler_unavailable"
            )
        )
        #expect(advanced)

        let latest = await ledger.fetchLatestRun(for: .preAnalysisRecovery)
        #expect(latest?.outcome == .failed)
        #expect(latest?.lastErrorCode == "reconciler_unavailable")
    }

    // MARK: - Idempotence

    @Test("finishRun on a terminal row is a no-op (returns false, does not stomp)")
    func finishRunIdempotenceOnTerminalRow() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let runId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: nil,
            scenePhase: nil
        )

        // First finish: terminal write goes through.
        let firstAdvance = await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .admittedWork,
                jobsAdmitted: 3
            )
        )
        #expect(firstAdvance)

        // Second finish (e.g. expirationHandler racing the work task):
        // must NOT advance. The first writer wins.
        let secondAdvance = await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .expired,
                cause: InternalMissCause.taskExpired.rawValue,
                expiration: true
            )
        )
        #expect(!secondAdvance)

        // Verify the row still reflects the FIRST finish (admittedWork),
        // not the racing expiration write.
        let latest = await ledger.fetchLatestRun(for: .backfill)
        #expect(latest?.outcome == .admittedWork)
        #expect(latest?.jobsAdmitted == 3)
        #expect(latest?.expiration == false)
        #expect(latest?.cause == nil)
    }

    @Test("finishRun for an unknown runId returns false (no row created)")
    func finishRunUnknownRunIdReturnsFalse() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let advanced = await ledger.finishRun(
            runId: "no-such-run",
            update: BackgroundTaskRunOutcomeUpdate(outcome: .admittedWork)
        )
        #expect(!advanced)

        let latest = await ledger.fetchLatestRun(for: .backfill)
        #expect(latest == nil)
    }

    // MARK: - Idempotence under contention (R6 stress test)

    @Test("N concurrent finishRun calls on the same runId produce exactly one terminal write",
          .timeLimit(.minutes(1)))
    func finishRunIdempotenceUnderConcurrency() async throws {
        // R6 adversarial review: pin the SQL `WHERE outcome='running'`
        // guard under contention. The production wiring has at most two
        // racing finishRun callers per row (the work task and the
        // expirationHandler), but the contract is that only the FIRST
        // terminal write succeeds — every subsequent caller observes
        // `outcome != 'running'` in the idempotence probe and returns
        // false. This stress test fires N=10 concurrent finishRun calls
        // against a single shared `running` row to exercise the actor's
        // serial executor isolation under load.
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        // Seed one shared `running` row.
        let runId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: "shared",
            scenePhase: nil
        )

        // Fire 10 concurrent finishRun calls. Each carries a distinct
        // outcome so we can verify exactly one wins — and identify
        // which one. The actor's serial executor guarantees the
        // probe→update is atomic per call, so there must be exactly
        // one `advanced=true`.
        let outcomes: [BackgroundTaskRunOutcome] = [
            .admittedWork, .noEligibleWork, .deferredThermal,
            .deferredCapability, .expired, .cancelled, .failed,
            .noOp, .recoveredWork, .rescheduled,
        ]
        let advancedFlags = await withTaskGroup(of: Bool.self) { group in
            for outcome in outcomes {
                group.addTask {
                    await ledger.finishRun(
                        runId: runId,
                        update: BackgroundTaskRunOutcomeUpdate(outcome: outcome)
                    )
                }
            }
            var results: [Bool] = []
            for await result in group {
                results.append(result)
            }
            return results
        }

        // Exactly one caller wins.
        let advancedCount = advancedFlags.filter { $0 }.count
        #expect(advancedCount == 1,
                "Exactly one of \(outcomes.count) concurrent finishRun calls must advance the row; saw \(advancedCount)")

        // The row is at SOME terminal outcome (we don't care which —
        // ordering is non-deterministic by design).
        let latest = await ledger.fetchLatestRun(for: .backfill)
        #expect(latest?.runId == runId)
        #expect(latest?.outcome != .running,
                "Row must have been advanced to a terminal outcome")
        #expect(latest?.finishedAt != nil)
    }

    // MARK: - Persistence across process restart

    @Test("ledger row survives a close/reopen of the AnalysisStore")
    func persistsAcrossReopen() async throws {
        let dir = try makeTempDir(prefix: "BgRunLedgerReopen")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let firstStore = try AnalysisStore(directory: dir)
        try await firstStore.migrate()
        let firstLedger = AnalysisStoreBackgroundTaskRunLedger(store: firstStore)

        let runId = await firstLedger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: "instance-X",
            scenePhase: "background"
        )
        await firstLedger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .admittedWork,
                jobsAdmitted: 12
            )
        )

        // Simulate process restart: drop the migrate cache, open a new
        // AnalysisStore against the same directory, and read.
        AnalysisStore.resetMigratedPathsForTesting()
        let secondStore = try AnalysisStore(directory: dir)
        try await secondStore.migrate()
        let secondLedger = AnalysisStoreBackgroundTaskRunLedger(store: secondStore)

        let latest = await secondLedger.fetchLatestRun(for: .backfill)
        #expect(latest?.runId == runId)
        #expect(latest?.outcome == .admittedWork)
        #expect(latest?.jobsAdmitted == 12)
        #expect(latest?.taskInstanceID == "instance-X")
    }

    // MARK: - Orphan running rows (process-restart recovery)

    @Test("Orphan `.running` row from a prior process survives close/reopen")
    func orphanRunningRowSurvivesReopen() async throws {
        // R1 audit-driven coverage: open a fresh DB, write a `.running`
        // row WITHOUT a finishRun (simulating a process killed mid-
        // handler), close the store, reopen at the same path, and
        // verify the orphan row is still queryable as `.running`. This
        // pins the persistence contract for in-flight rows — the bead
        // spec calls out "persistence across process restart" but the
        // pre-R1 test only covered TERMINAL rows.
        let dir = try makeTempDir(prefix: "BgRunLedgerOrphan")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let firstStore = try AnalysisStore(directory: dir)
        try await firstStore.migrate()
        let firstLedger = AnalysisStoreBackgroundTaskRunLedger(store: firstStore)

        let runId = await firstLedger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: "orphan-instance",
            scenePhase: "background"
        )
        // NOTE: deliberately no finishRun. The row stays at .running.

        // Simulate process restart.
        AnalysisStore.resetMigratedPathsForTesting()
        let secondStore = try AnalysisStore(directory: dir)
        try await secondStore.migrate()
        let secondLedger = AnalysisStoreBackgroundTaskRunLedger(store: secondStore)

        let latest = await secondLedger.fetchLatestRun(for: .backfill)
        #expect(latest?.runId == runId)
        #expect(latest?.outcome == .running,
                "An orphan running row from a prior process must remain visible as .running across reopen")
        #expect(latest?.finishedAt == nil)
    }

    @Test("reapOrphansAtLaunch flips `.running` rows to .failed/orphan_at_launch")
    func reapOrphansAtLaunchSweepsRunningRows() async throws {
        // Pin the launch-time orphan reaper contract. After a prior
        // process leaves a .running row behind, the next launch should
        // call reapOrphansAtLaunch() to flip it to a terminal outcome
        // so dogfood diagnostics doesn't see stale "in-flight" rows.
        // Terminal rows (already finished) MUST NOT be touched by the
        // reaper — only orphan running rows.
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        // Row 1: terminal (admittedWork) — must be left alone.
        let terminalRunId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: nil,
            scenePhase: nil
        )
        await ledger.finishRun(
            runId: terminalRunId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .admittedWork,
                jobsAdmitted: 4
            )
        )

        // Row 2 & 3: orphan running rows from a "prior process".
        let orphanRunId1 = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: "orphan-1",
            scenePhase: "background"
        )
        let orphanRunId2 = await ledger.startRun(
            entryPoint: .preAnalysisRecovery,
            taskIdentifier: BackgroundTaskID.preAnalysisRecovery,
            taskInstanceID: "orphan-2",
            scenePhase: nil
        )

        // Reap.
        let reaped = await ledger.reapOrphansAtLaunch()
        #expect(reaped == 2,
                "reapOrphansAtLaunch must sweep exactly the two orphan running rows")

        // Terminal row untouched.
        let terminalAfter = await ledger.fetchRecentRuns(limit: 10)
            .first { $0.runId == terminalRunId }
        #expect(terminalAfter?.outcome == .admittedWork)
        #expect(terminalAfter?.jobsAdmitted == 4)
        #expect(terminalAfter?.lastErrorCode == nil)

        // Orphan rows flipped to .failed with a stable error code.
        let orphan1After = await ledger.fetchRecentRuns(limit: 10)
            .first { $0.runId == orphanRunId1 }
        #expect(orphan1After?.outcome == .failed)
        #expect(orphan1After?.lastErrorCode == "orphan_at_launch")
        #expect(orphan1After?.finishedAt != nil)

        let orphan2After = await ledger.fetchRecentRuns(limit: 10)
            .first { $0.runId == orphanRunId2 }
        #expect(orphan2After?.outcome == .failed)
        #expect(orphan2After?.lastErrorCode == "orphan_at_launch")
    }

    @Test("reapOrphansAtLaunch is idempotent on a clean store")
    func reapOrphansAtLaunchIsIdempotentNoOp() async throws {
        // No orphan rows → reaper returns 0 and does nothing. Important
        // because reapOrphansAtLaunch is called on every cold launch.
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let firstReap = await ledger.reapOrphansAtLaunch()
        #expect(firstReap == 0)

        // Second call still 0 — no rows changed.
        let secondReap = await ledger.reapOrphansAtLaunch()
        #expect(secondReap == 0)
    }

    @Test("reapOrphansAtLaunch on NoOp ledger returns 0 with no side effects")
    func noOpLedgerReapOrphansReturnsZero() async {
        let ledger = NoOpBackgroundTaskRunLedger()
        let count = await ledger.reapOrphansAtLaunch()
        #expect(count == 0)
    }

    @Test("reapOrphansAtLaunch(startedBefore:) skips rows newer than cutoff (R2 fix)")
    func reapOrphansAtLaunchTemporalFilter() async throws {
        // playhead-hygc.1.4 (R2 fix): rows whose `startedAt` is greater
        // than or equal to the `startedBefore` cutoff must be left
        // alone. This pins the race-safety contract: a BG handler that
        // fires between `registerBackgroundTasks()` and the deferred
        // migrate Task body must not have its fresh `running` row
        // reaped, because the handler's row will have
        // `startedAt > processLaunchTimestamp` by construction.
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        // Capture cutoff THEN start a row — the row's startedAt is
        // strictly after the cutoff, so it must be skipped.
        let cutoff = Date().timeIntervalSince1970
        // Tiny sleep to guarantee Date() inside startRun reads a value
        // strictly greater than `cutoff` even on coarse clocks.
        try await Task.sleep(for: .milliseconds(20))
        let freshRunId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: "fresh-running",
            scenePhase: "background"
        )

        // Reap with the captured cutoff.
        let reaped = await ledger.reapOrphansAtLaunch(startedBefore: cutoff)
        #expect(reaped == 0,
                "Rows newer than the cutoff must NOT be reaped")

        // Verify the fresh row is still .running.
        let fresh = await ledger.fetchRecentRuns(limit: 10)
            .first { $0.runId == freshRunId }
        #expect(fresh?.outcome == .running,
                "Fresh row must remain at .running — reaper must not have touched it")
        #expect(fresh?.lastErrorCode == nil,
                "Fresh row must not carry orphan_at_launch")
    }

    // MARK: - Diagnostic queries

    @Test("fetchRecentRuns orders by startedAt DESC and respects the limit")
    func fetchRecentRunsOrderingAndLimit() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        // Insert three runs, each finished, to make ordering observable.
        for i in 0..<3 {
            let runId = await ledger.startRun(
                entryPoint: .backfill,
                taskIdentifier: BackgroundTaskID.backfillProcessing,
                taskInstanceID: "i-\(i)",
                scenePhase: nil
            )
            await ledger.finishRun(
                runId: runId,
                update: BackgroundTaskRunOutcomeUpdate(outcome: .admittedWork)
            )
            // Small sleep so startedAt timestamps differ.
            try await Task.sleep(for: .milliseconds(10))
        }

        let recent = await ledger.fetchRecentRuns(limit: 2)
        #expect(recent.count == 2)
        // Newest first: instance-2 then instance-1.
        #expect(recent.first?.taskInstanceID == "i-2")
        #expect(recent.last?.taskInstanceID == "i-1")
    }

    @Test("fetchLatestRun(forAssetId:) returns the most recent run scoped to that asset")
    func fetchLatestByAssetId() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let unrelatedRunId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: nil,
            scenePhase: nil
        )
        await ledger.finishRun(
            runId: unrelatedRunId,
            update: BackgroundTaskRunOutcomeUpdate(outcome: .admittedWork)
        )

        let runId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: nil,
            scenePhase: nil
        )
        await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(
                outcome: .admittedWork,
                assetId: "asset-target"
            )
        )

        let latest = await ledger.fetchLatestRun(forAssetId: "asset-target")
        #expect(latest?.runId == runId)
        #expect(latest?.assetId == "asset-target")

        let none = await ledger.fetchLatestRun(forAssetId: "no-such-asset")
        #expect(none == nil)
    }

    @Test("fetchLatestRun(for:) ignores runs from other entry points")
    func fetchLatestByEntryPointIsScoped() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        let backfillRunId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: nil,
            scenePhase: nil
        )
        await ledger.finishRun(
            runId: backfillRunId,
            update: BackgroundTaskRunOutcomeUpdate(outcome: .admittedWork)
        )

        let recoveryRunId = await ledger.startRun(
            entryPoint: .preAnalysisRecovery,
            taskIdentifier: BackgroundTaskID.preAnalysisRecovery,
            taskInstanceID: nil,
            scenePhase: nil
        )
        await ledger.finishRun(
            runId: recoveryRunId,
            update: BackgroundTaskRunOutcomeUpdate(outcome: .recoveredWork)
        )

        let latestBackfill = await ledger.fetchLatestRun(for: .backfill)
        #expect(latestBackfill?.runId == backfillRunId)
        #expect(latestBackfill?.outcome == .admittedWork)

        let latestRecovery = await ledger.fetchLatestRun(for: .preAnalysisRecovery)
        #expect(latestRecovery?.runId == recoveryRunId)
        #expect(latestRecovery?.outcome == .recoveredWork)
    }

    // MARK: - Mid-run snapshot classification

    @Test("Mid-run snapshot returns running outcome before finishRun")
    func midRunSnapshotIsRunning() async throws {
        let store = try await makeTestStore()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)

        _ = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: BackgroundTaskID.backfillProcessing,
            taskInstanceID: "running-i",
            scenePhase: "background"
        )

        let latest = await ledger.fetchLatestRun(for: .backfill)
        #expect(latest?.outcome == .running)
        #expect(latest?.finishedAt == nil)
        #expect(latest?.taskInstanceID == "running-i")
    }

    // MARK: - NoOpBackgroundTaskRunLedger contract

    @Test("NoOpBackgroundTaskRunLedger returns a runId but never advances")
    func noOpLedgerContract() async {
        let ledger = NoOpBackgroundTaskRunLedger()
        let runId = await ledger.startRun(
            entryPoint: .backfill,
            taskIdentifier: "x",
            taskInstanceID: nil,
            scenePhase: nil
        )
        #expect(!runId.isEmpty)

        let advanced = await ledger.finishRun(
            runId: runId,
            update: BackgroundTaskRunOutcomeUpdate(outcome: .admittedWork)
        )
        #expect(!advanced)

        let latest = await ledger.fetchLatestRun(for: .backfill)
        #expect(latest == nil)
        let recent = await ledger.fetchRecentRuns(limit: 5)
        #expect(recent.isEmpty)
        let byAsset = await ledger.fetchLatestRun(forAssetId: "x")
        #expect(byAsset == nil)
    }
}
