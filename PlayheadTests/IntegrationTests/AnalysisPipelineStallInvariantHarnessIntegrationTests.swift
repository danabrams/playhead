// AnalysisPipelineStallInvariantHarnessIntegrationTests.swift
// playhead-pd0q: GENERAL pipeline-stall regression harness.
//
// The two gy2s suites are incident PINS — they reproduce the exact
// 2026-07-20 dogfood signature (4 queued preAnalysis jobs, 0 running) with
// a single fixture:
//   - `AnalysisPipelineStallRegressionTests` (scheduler/store/reconciler)
//   - `AnalysisPipelineStallBackgroundEntryPointsIntegrationTests` (BGTasks)
//
// This suite closes the gap between "the one incident" and "the CLASS of
// pipeline stalls". It asserts the four pd0q invariants GENERALLY — every
// one is a Swift Testing `@Test(arguments:)` parameterized over a RANGE of
// states (queue depths, job-type mixes, epoch gaps, background entry
// points) so a FUTURE stall of the same class goes red, not only a
// byte-for-byte replay of the incident. All drive the REAL
// `AnalysisWorkScheduler` / `AnalysisStore` / `AnalysisJobReconciler` /
// `BackgroundProcessingService` + `AnalysisJobRunner` at the integration
// level, and all are deterministic + event-driven (each drain awaits its
// job to a terminal fixed point via a failing decode; no wall-clock poll).
//
// pd0q invariant → test:
//   1 DRAIN / liveness   — `invariant1_drainIsLiveAcrossDepthsAndJobTypes`
//   2 EPOCH survival     — `invariant2_epochSurvivalIsGeneralAcrossGaps`
//   3 BACKGROUND progress— `invariant3_bothBackgroundEntryPointsDispatch`
//   4 RECOVERY visibility— `invariant4_recoveryJobsSeenMatchesEligibleCount`

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-pd0q: general pipeline-stall invariant harness")
struct AnalysisPipelineStallInvariantHarnessIntegrationTests {

    // MARK: - Test doubles

    /// Always-throwing decode → routes each dispatched job through the
    /// `.failed` arm; a pre-stamped `attemptCount` drives it terminal
    /// (`superseded`) in a single pass, giving every drain a deterministic
    /// fixed point (the proven gy2s pattern). Job-type-agnostic — a
    /// `preAnalysis` and a `backfill` `analysis_jobs` row both fail decode
    /// (or asset-resolution) into the same terminal arm.
    private final class FailingDecodeStub: AnalysisAudioProviding, @unchecked Sendable {
        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            throw AnalysisAudioError.decodingFailed("Operation Interrupted")
        }
    }

    /// Job-type mix axis for the DRAIN invariant. `analysis_jobs` rows carry
    /// a `jobType` (`preAnalysis` / `playback` / `backfill`); the scheduler's
    /// dispatch selection is state-based (only `playback` is special-cased
    /// as T0), so the liveness invariant must hold for the deferred classes
    /// regardless of type. `mixed` interleaves both in one queue to prove
    /// cross-type liveness inside a single drain.
    // Not `private`: used as a Swift Testing `@Test(arguments:)` type on an
    // internal test method, so it must be at least as accessible as the method.
    enum JobMix: String, Sendable, CustomStringConvertible {
        case preAnalysis
        case backfill
        case mixed
        var description: String { rawValue }
        func jobType(index: Int) -> String {
            switch self {
            case .preAnalysis: return "preAnalysis"
            case .backfill: return "backfill"
            case .mixed: return index.isMultiple(of: 2) ? "preAnalysis" : "backfill"
            }
        }
    }

    /// Background entry-point axis for the BACKGROUND-progress invariant.
    /// The RC-1 root cause was that NEITHER BGTask actually dispatched, so
    /// the invariant must be asserted over BOTH.
    // Not `private`: see JobMix — used as a `@Test(arguments:)` type.
    enum BGEntry: String, Sendable, CustomStringConvertible {
        case backfill
        case recovery
        var description: String { rawValue }
    }

    // MARK: - Scheduler factory (mirrors the gy2s pins; re-declared because
    // the pins' factory is file-private)

    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        audio: any AnalysisAudioProviding
    ) -> AnalysisWorkScheduler {
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audio,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: SpeechService(recognizer: StubSpeechRecognizer()),
                store: store
            ),
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

    // MARK: - Seed / probe helpers

    /// Seed `count` eligible, dispatchable, compute-only jobs: a cached file
    /// present (so `processJob` reaches decode instead of blocking on a
    /// missing file), version-matched work key (so reconcile does not
    /// supersede), and a pre-stamped `attemptCount` so one failing decode
    /// terminates each. Returns the seeded job ids.
    @discardableResult
    private func seedEligibleJobs(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        count: Int,
        mix: JobMix = .preAnalysis,
        prefix: String,
        attemptCount: Int = 4
    ) async throws -> [String] {
        var ids: [String] = []
        for i in 0..<count {
            let jobId = "\(prefix)-\(i)"
            let jobType = mix.jobType(index: i)
            let episodeId = "\(prefix)-ep-\(i)"
            downloads.cachedURLs[episodeId] = URL(fileURLWithPath: "/tmp/\(episodeId).m4a")
            try await store.insertJob(makeAnalysisJob(
                jobId: jobId,
                jobType: jobType,
                episodeId: episodeId,
                workKey: AnalysisJob.computeWorkKey(
                    fingerprint: "fp-\(jobId)",
                    analysisVersion: PreAnalysisConfig.analysisVersion,
                    jobType: jobType
                ),
                sourceFingerprint: "fp-\(jobId)",
                priority: 10,
                desiredCoverageSec: 90,
                state: "queued",
                attemptCount: attemptCount
            ))
            ids.append(jobId)
        }
        return ids
    }

    /// Pending == the scheduler's "still owes work" view (queued + running +
    /// paused). The stall left this pinned at its seed value forever.
    private func pendingCount(_ store: AnalysisStore) async throws -> Int {
        let queued = try await store.fetchJobsByState("queued")
        let running = try await store.fetchJobsByState("running")
        let paused = try await store.fetchJobsByState("paused")
        return queued.count + running.count + paused.count
    }

    /// Increment `_meta.scheduler_epoch` until it reaches `target`. Fresh
    /// stores seed at 1, so `target >= 1`.
    private func bringSchedulerEpoch(_ store: AnalysisStore, to target: Int) async throws {
        while (try await store.fetchSchedulerEpoch() ?? 0) < target {
            _ = try await store.incrementSchedulerEpoch()
        }
    }

    // MARK: - Invariant 1 — DRAIN / liveness (general)

    /// Given queued + eligible jobs and free capacity, EVERY eligible job is
    /// dispatched and the queue drains to empty — across a range of queue
    /// depths AND across job types (preAnalysis, backfill, and a mixed
    /// queue). The exact stall signature was "eligible queued + free
    /// capacity, yet 0 dispatched, pending pinned". `drainEligible` awaits
    /// each job to a terminal fixed point, so the per-lane cap never blocks
    /// the next pass — capacity is free by construction, which is precisely
    /// the free-capacity state the stall wedged; depth and job-type are the
    /// meaningful axes.
    ///
    /// Catches (beyond the gy2s single-fixture pin): a future stall that
    /// only manifests at a particular depth, or that special-cases one job
    /// type's dispatch and accidentally never advances it — either leaves a
    /// non-empty queue and fails here for that argument.
    @Test("Invariant 1 (DRAIN): eligible queue + free capacity fully drains — any depth, any job type",
          .timeLimit(.minutes(1)),
          arguments: [1, 3, 8, 16], [JobMix.preAnalysis, JobMix.backfill, JobMix.mixed])
    func invariant1_drainIsLiveAcrossDepthsAndJobTypes(depth: Int, mix: JobMix) async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await seedEligibleJobs(
            store: store, downloads: downloads, count: depth, mix: mix, prefix: "drain-\(mix)"
        )
        let scheduler = makeScheduler(store: store, downloads: downloads, audio: FailingDecodeStub())

        #expect(try await pendingCount(store) == depth,
                "precondition: \(depth) eligible queued jobs (mix=\(mix))")

        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600))

        let queuedAfter = try await store.fetchJobsByState("queued")
        #expect(queuedAfter.isEmpty,
                "drainEligible must dispatch EVERY eligible queued job (depth=\(depth), mix=\(mix)); still queued: \(queuedAfter.map(\.jobId))")
        #expect(try await pendingCount(store) == 0,
                "queue must be fully drained (depth=\(depth), mix=\(mix))")
    }

    // MARK: - Invariant 2 — EPOCH survival (general)

    /// A queued row minted under ANY stale epoch survives an epoch bump /
    /// scheduler restart: it stays SQL-dispatchable, and `reconcile()`
    /// re-adopts it by re-stamping it to the current epoch (any gap, not
    /// just the incident's 0→1). And a FRESH enqueue at the current epoch
    /// stamps that epoch, never the stale-0 default — asserted at the same
    /// arbitrary current epoch, generalizing RC-3 past the fresh-store
    /// epoch==1 case.
    ///
    /// Catches: a regression that makes dispatch consult `schedulerEpoch`
    /// (dropping stale rows), that narrows re-adoption to a specific gap, or
    /// that reverts enqueue to the epoch-0 sentinel — each fails for the
    /// arguments whose gap it no longer handles.
    @Test("Invariant 2 (EPOCH): any stale-epoch queued row survives a bump, reconcile re-adopts it, fresh enqueue stamps current",
          .timeLimit(.minutes(1)),
          arguments: [(0, 1), (0, 4), (1, 2), (2, 6), (4, 9)])
    func invariant2_epochSurvivalIsGeneralAcrossGaps(jobEpoch: Int, currentEpoch: Int) async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()

        // A queued row minted under `jobEpoch`. No cached file so reconcile's
        // discovery/unblock steps leave it alone; version-matched work key so
        // supersede does not fire.
        let staleJob = makeAnalysisJob(
            jobId: "epoch-\(jobEpoch)-\(currentEpoch)",
            jobType: "preAnalysis",
            episodeId: "ep-epoch-\(jobEpoch)-\(currentEpoch)",
            workKey: AnalysisJob.computeWorkKey(
                fingerprint: "fp-epoch-\(jobEpoch)-\(currentEpoch)",
                analysisVersion: PreAnalysisConfig.analysisVersion,
                jobType: "preAnalysis"
            ),
            sourceFingerprint: "fp-epoch-\(jobEpoch)-\(currentEpoch)",
            priority: 0,
            desiredCoverageSec: 90,
            state: "queued",
            schedulerEpoch: jobEpoch
        )
        try await store.insertJob(staleJob)

        // Bump the scheduler epoch to the (arbitrary) current value — a
        // restart / scheduling-pass bump strictly greater than `jobEpoch`.
        try await bringSchedulerEpoch(store, to: currentEpoch)
        #expect(try await store.fetchSchedulerEpoch() == currentEpoch)

        // Survival: dispatch eligibility never consults `schedulerEpoch`, so
        // the stale-epoch row is still selectable after the bump.
        let now = Date().timeIntervalSince1970 + 1
        let selected = try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true, t0ThresholdSec: 0, now: now
        )
        #expect(selected?.jobId == staleJob.jobId,
                "queued row at epoch \(jobEpoch) must remain selectable after a bump to \(currentEpoch)")

        // Re-adoption: reconcile re-stamps the stale queued row to current
        // (fresh empty download manager so `discoverUnEnqueuedDownloads`
        // adds nothing → the restamp count is exactly the one stale row).
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider()
        )
        let report = try await reconciler.reconcile()
        #expect(report.queuedJobEpochsRestamped == 1,
                "reconcile must re-adopt exactly the one stale queued row (gap \(jobEpoch)→\(currentEpoch))")

        let after = try await store.fetchJob(byId: staleJob.jobId)
        #expect(after?.state == "queued", "re-adoption must not change dispatchability")
        #expect(after?.schedulerEpoch == currentEpoch,
                "re-adopted row must carry the current epoch \(currentEpoch), not the stale \(jobEpoch)")

        // Fresh-enqueue half (generalized RC-3): an enqueue at the current
        // epoch stamps THAT epoch, not the stale-0 default.
        let scheduler = makeScheduler(
            store: store, downloads: downloads, audio: StubAnalysisAudioProvider()
        )
        await scheduler.enqueue(
            episodeId: "ep-fresh-\(currentEpoch)",
            podcastId: "pod-fresh",
            downloadId: "dl-fresh-\(currentEpoch)",
            sourceFingerprint: "fp-fresh-\(currentEpoch)",
            isExplicitDownload: true
        )
        let fresh = try #require(
            try await store.fetchJobsByState("queued").first { $0.episodeId == "ep-fresh-\(currentEpoch)" }
        )
        #expect(fresh.schedulerEpoch == currentEpoch,
                "fresh enqueue must stamp the current epoch \(currentEpoch), not the 0 sentinel")
        #expect(!fresh.generationID.isEmpty,
                "fresh enqueue must stamp a non-blank generation id for orphan-recovery routing")
    }

    // MARK: - Invariant 3 — BACKGROUND progress (general, both entry points)

    /// A BGTask run must DISPATCH — make progress, never perpetually
    /// `expired`/`task_expired` with 0 completions. Asserted over BOTH entry
    /// points (backfill AND preanalysis_recovery — the RC-1 root cause was
    /// that neither dispatched) and across a range of queue depths. For each:
    /// the queue fully drains — NOTHING is left `queued` (real dispatch
    /// happened) AND the durable ledger records a PROGRESS outcome
    /// (`admittedWork` / `recoveredWork`), never `.expired` and never a
    /// stall-shaped no-progress terminal.
    ///
    /// Catches: any future regression where a background handler stops
    /// actively draining (reverts to bare `wake()` + poll, or a sceneless
    /// launch leaves the loop unstarted) — jobs would stay pinned `queued`
    /// and the ledger outcome would fall to `.noEligibleWork`/`.noOp`/
    /// `.expired`, failing here for whichever entry point regressed.
    @Test("Invariant 3 (BACKGROUND): both BGTask entry points DISPATCH — queue fully drains + progress outcome, never expired/0-done",
          .timeLimit(.minutes(1)),
          arguments: [
            (BGEntry.backfill, 1), (BGEntry.backfill, 3), (BGEntry.backfill, 7),
            (BGEntry.recovery, 1), (BGEntry.recovery, 3), (BGEntry.recovery, 7),
          ])
    func invariant3_bothBackgroundEntryPointsDispatch(entry: BGEntry, depth: Int) async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await seedEligibleJobs(
            store: store, downloads: downloads, count: depth, prefix: "bg-\(entry)"
        )

        let coordinator = StubAnalysisCoordinator()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
        let scheduler = makeScheduler(store: store, downloads: downloads, audio: FailingDecodeStub())
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider()
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: reconciler)

        let before = try await pendingCount(store)
        #expect(before == depth)

        let task = StubBackgroundTask()
        let ledgerEntryPoint: BackgroundTaskRunEntryPoint
        switch entry {
        case .backfill:
            await bps.handleBackfillTask(task)
            ledgerEntryPoint = .backfill
        case .recovery:
            await bps.handlePreAnalysisRecovery(task)
            ledgerEntryPoint = .preAnalysisRecovery
        }
        await task.awaitCompletion()
        // Quiesce the loop the handler started so the queued-set read below
        // is taken against a settled scheduler (the drain already ran to
        // completion inside the handler).
        await scheduler.stop()

        // Progress: every eligible job was DISPATCHED out of the `queued`
        // state — the whole point of RC-1. The incident signature was
        // "N queued / 0 running, pinned forever", so the faithful
        // anti-stall assertion is that NOTHING remains queued.
        //
        // Asserting on the `queued` set (rather than a `pendingCount`
        // snapshot of queued+running+paused) is also race-free against the
        // `runLoop` that `ensureSchedulerLoopStarted()` spins up
        // concurrently inside the handler. That loop can win the lease CAS
        // on a seeded job; if it is still mid-run when the (non-awaited)
        // `stop()` above cancels it, the job supersedes — the seed stamps
        // `attemptCount` at `maxAttemptCount - 1`, so the cancel-mid-run arm
        // takes the terminal `superseded` branch, never the requeue branch —
        // so it can never reappear as `queued`. A `pendingCount` snapshot,
        // by contrast, could transiently still count that stolen job as
        // `running` if its terminal write lands just after the read, giving
        // a false `after == before` at depth=1. `queued.isEmpty` avoids that
        // flake while asserting something strictly STRONGER: EVERY job left
        // the queue, not merely that pending decreased by one. `before`
        // remains the seeded-count precondition. A regressed handler that
        // fails to dispatch (RC-1: bare `wake()`+poll) leaves every seeded
        // job pinned `queued` and fails here.
        let queuedAfter = try await store.fetchJobsByState("queued")
        #expect(queuedAfter.isEmpty,
                "\(entry) BGTask must actively DISPATCH every eligible queued job; still queued: \(queuedAfter.map(\.jobId)) (depth=\(depth))")

        // Durable classification: a progress outcome, never the stall's
        // silent `.expired`/no-progress terminal.
        let run = try #require(await ledger.fetchLatestRun(for: ledgerEntryPoint),
                               "\(entry) must record a durable ledger row")
        #expect(run.outcome != .expired,
                "\(entry) run must not resolve to .expired with work queued (depth=\(depth))")
        let progressOutcomes: Set<BackgroundTaskRunOutcome> = [.admittedWork, .recoveredWork]
        #expect(progressOutcomes.contains(run.outcome),
                "\(entry) run must record a progress outcome; got \(run.outcome) (depth=\(depth))")
        // The run SAW the eligible work (the visibility half of RC-1).
        #expect((run.jobsSeen ?? -1) == depth,
                "\(entry) run must record jobsSeen == \(depth); got \(String(describing: run.jobsSeen))")
    }

    // MARK: - Invariant 4 — RECOVERY visibility (general)

    /// `preanalysis_recovery` must SEE the eligible queued preAnalysis jobs.
    /// The dogfood bug recorded `jobsSeen = 0` while 4 were queued + eligible
    /// (the stall was invisible in the pulled DB). Assert the recovery run's
    /// `jobsSeen` matches the ACTUAL eligible count across a range of depths.
    /// (Complements Invariant 3's recovery-dispatch check: this pins the
    /// exact visibility contract — count fidelity — over a finer depth range.)
    ///
    /// Catches: any regression that under- (or over-) counts what recovery
    /// observed — including a revert to capturing the baseline AFTER the
    /// drain has mutated state, which would read 0 and reproduce the exact
    /// dogfood invisibility.
    @Test("Invariant 4 (RECOVERY visibility): preanalysis_recovery records jobsSeen == actual eligible count",
          .timeLimit(.minutes(1)),
          arguments: [1, 2, 5, 9])
    func invariant4_recoveryJobsSeenMatchesEligibleCount(depth: Int) async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let seeded = try await seedEligibleJobs(
            store: store, downloads: downloads, count: depth, prefix: "recvis"
        )
        #expect(seeded.count == depth)

        let coordinator = StubAnalysisCoordinator()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
        let scheduler = makeScheduler(store: store, downloads: downloads, audio: FailingDecodeStub())
        // Fresh empty download manager on the reconciler so
        // `discoverUnEnqueuedDownloads` adds nothing — the eligible count
        // stays exactly `depth`, making the `==` assertion meaningful.
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider()
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: reconciler)

        let task = StubBackgroundTask()
        await bps.handlePreAnalysisRecovery(task)
        await task.awaitCompletion()
        await scheduler.stop()

        let latest = try #require(await ledger.fetchLatestRun(for: .preAnalysisRecovery),
                                  "recovery must record a ledger row")
        #expect((latest.jobsSeen ?? -1) == depth,
                "recovery must record jobsSeen == the actual eligible count \(depth); got \(String(describing: latest.jobsSeen))")
    }
}
