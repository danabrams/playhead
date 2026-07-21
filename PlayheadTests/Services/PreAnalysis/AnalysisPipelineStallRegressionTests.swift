// AnalysisPipelineStallRegressionTests.swift
// playhead-gy2s: durable regression pin on the "pipeline stall" incident.
//
// Incident (dogfood, 2026-07-20): after ~10 h a device had 4 preAnalysis
// (transcription) jobs queued + eligible, 0 running, "Nothing running."
// Two co-dominant confirmed causes:
//
//   RC-1 (background): neither background task actually DISPATCHES —
//     `preanalysis_recovery` only reconciles and `backfill` only polls +
//     wakes the loop, so if the scheduler loop was never started (a
//     sceneless background launch never fires the SwiftUI `.task`) or was
//     wedged, pending never dropped (task_expired, jobsCompleted=0).
//   RC-2 (foreground "Nothing running"): the admission gate SILENTLY
//     rejected compute-only pre-analysis (on-device transcription of an
//     already-cached file — ZERO network) because a background-lane job
//     maps to a `.maintenance` transport session and was mis-gated as a
//     Wi-Fi-only transfer. The reject path wrote no state, so the stall
//     was invisible in the pulled DB.
//   RC-3 (routing correctness, bundled): fresh enqueue stamped epoch 0 /
//     blank generation; reconcile never re-stamped stale queued rows.
//
// These tests drive the real `AnalysisWorkScheduler` / `AnalysisStore` /
// `AnalysisJobReconciler` deterministically (event-driven — the drain
// awaits each job to completion; no wall-clock polling) and assert the
// six stall invariants. The four that reproduced the stall (2, 3, 4, 5)
// are red on pre-fix source and green after the RC-1/RC-2/RC-3 fixes.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-gy2s: analysis-pipeline stall regression")
struct AnalysisPipelineStallRegressionTests {

    // MARK: - Test doubles

    /// Storage snapshotter that denies exactly one artifact class so the
    /// storage axis can be driven through a hard reject; mirrors the stub in
    /// `AnalysisWorkSchedulerStorageAdmissionTests`.
    private struct StubStorageBudgetSnapshotter: StorageBudgetSnapshotting {
        let denyClass: ArtifactClass?
        let remaining: Int64

        init(denyClass: ArtifactClass? = nil, remaining: Int64 = 5_000_000_000) {
            self.denyClass = denyClass
            self.remaining = remaining
        }

        func canAdmit(_ cls: ArtifactClass, bytes: Int64) async -> Bool { cls != denyClass }
        func remainingBytes(_ cls: ArtifactClass) async -> Int64 {
            cls == denyClass ? 0 : remaining
        }
    }

    /// Decode stub that always throws — routes `processJob` through the
    /// `.failed` outcome arm. Combined with a pre-stamped high `attemptCount`
    /// this drives a job to the terminal `superseded` state in a single
    /// dispatch, giving `drainEligible` a deterministic fixed point (mirrors
    /// the proven pattern in `AnalysisWorkSchedulerQueueProgressIntegrationTests`).
    private final class FailingDecodeStub: AnalysisAudioProviding, @unchecked Sendable {
        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            throw AnalysisAudioError.decodingFailed("Operation Interrupted")
        }
    }

    // MARK: - Scheduler factory

    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        transport: StubTransportStatusProvider = StubTransportStatusProvider(),
        storage: any StorageBudgetSnapshotting = StubStorageBudgetSnapshotter(),
        audio: any AnalysisAudioProviding = StubAnalysisAudioProvider(),
        config: PreAnalysisConfig = PreAnalysisConfig()
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audio,
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
            transportStatusProvider: transport,
            storageBudgetSnapshotter: storage,
            config: config
        )
    }

    /// Enqueue a compute-only pre-analysis job: state `queued`, a cached file
    /// present (so the admission gate classifies it compute-only), version
    /// matching `PreAnalysisConfig.analysisVersion` so reconcile does not
    /// supersede it.
    @discardableResult
    private func insertComputeOnlyJob(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        jobId: String,
        episodeId: String,
        priority: Int = 0,
        attemptCount: Int = 0,
        schedulerEpoch: Int = 0
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
            priority: priority,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: attemptCount,
            schedulerEpoch: schedulerEpoch
        )
        try await store.insertJob(job)
        return job
    }

    private func pendingCount(_ store: AnalysisStore) async throws -> Int {
        let queued = try await store.fetchJobsByState("queued")
        let running = try await store.fetchJobsByState("running")
        let paused = try await store.fetchJobsByState("paused")
        return queued.count + running.count + paused.count
    }

    // MARK: - Invariant 1 — Drain / liveness

    @Test("Invariant 1: eligible queued work + free capacity drains within bounded passes",
          .timeLimit(.minutes(1)))
    func invariant1_drainEligibleDrainsTheQueue() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        // Pre-stamp attemptCount so a single failing decode terminates each
        // job (superseded) — a deterministic fixed point for the drain.
        for i in 0..<4 {
            try await insertComputeOnlyJob(
                store: store, downloads: downloads,
                jobId: "drain-\(i)", episodeId: "ep-drain-\(i)",
                priority: 10, attemptCount: 4
            )
        }
        let scheduler = makeScheduler(store: store, downloads: downloads, audio: FailingDecodeStub())

        #expect(try await pendingCount(store) == 4, "precondition: 4 eligible queued jobs")

        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600))

        // The stall was: eligible queued + free capacity, yet 0 dispatched.
        // Liveness = every job left `queued` (was leased + run) and the
        // queue drained to empty.
        let queuedAfter = try await store.fetchJobsByState("queued")
        #expect(queuedAfter.isEmpty, "drainEligible must dispatch every eligible queued job; still queued: \(queuedAfter.map(\.jobId))")
        #expect(try await pendingCount(store) == 0, "queue must be fully drained")
    }

    // MARK: - Invariant 2 — Transport must NOT block compute-only pre-analysis

    @Test("Invariant 2: transport does NOT block compute-only pre-analysis (cellular, no-cellular-pref)",
          .timeLimit(.minutes(1)))
    func invariant2_computeOnlyDispatchesOnCellular() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        // Cellular reachability with the user disallowing cellular — this is
        // the exact axis that rejected a `.maintenance` transfer pre-fix.
        let transport = StubTransportStatusProvider(reachability: .cellular, allowsCellular: false)
        let scheduler = makeScheduler(store: store, downloads: downloads, transport: transport)

        let job = try await insertComputeOnlyJob(
            store: store, downloads: downloads,
            jobId: "cell-compute", episodeId: "ep-cell-compute", priority: 0
        )

        // RC-2 repro: pre-fix this returns `.reject(.wifiRequired)`.
        let decision = await scheduler.evaluateAdmissionGate(for: job)
        switch decision {
        case .admit:
            break
        case .reject(let cause):
            Issue.record("compute-only pre-analysis (cached file, zero network) must be admitted on cellular; got reject(\(cause))")
        }
    }

    @Test("Invariant 2: transport does NOT block compute-only pre-analysis (unreachable)",
          .timeLimit(.minutes(1)))
    func invariant2_computeOnlyDispatchesOnUnreachable() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let transport = StubTransportStatusProvider(reachability: .unreachable, allowsCellular: false)
        let scheduler = makeScheduler(store: store, downloads: downloads, transport: transport)

        let job = try await insertComputeOnlyJob(
            store: store, downloads: downloads,
            jobId: "unreach-compute", episodeId: "ep-unreach-compute", priority: 0
        )

        // Pre-fix this returns `.reject(.noNetwork)`.
        let decision = await scheduler.evaluateAdmissionGate(for: job)
        if case .reject(let cause) = decision {
            Issue.record("compute-only pre-analysis must dispatch with no network; got reject(\(cause))")
        }
    }

    @Test("Invariant 2 (control): transport STILL rejects work that is not compute-only pre-analysis",
          .timeLimit(.minutes(1)))
    func invariant2_control_transportGateStillBites() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let transport = StubTransportStatusProvider(reachability: .unreachable, allowsCellular: false)
        let scheduler = makeScheduler(store: store, downloads: downloads, transport: transport)

        // preAnalysis job WITHOUT a cached file → NOT compute-only → the
        // transport gate must still reject. Proves the exemption is scoped to
        // the already-downloaded class and the gate was not nuked wholesale.
        let job = makeAnalysisJob(
            jobId: "no-file-preanalysis",
            jobType: "preAnalysis",
            episodeId: "ep-no-file",
            workKey: "fp-no-file:1:preAnalysis",
            sourceFingerprint: "fp-no-file",
            priority: 0,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let decision = await scheduler.evaluateAdmissionGate(for: job)
        switch decision {
        case .reject(let cause):
            #expect(cause == .noNetwork, "unreachable transport must reject non-compute-only work with .noNetwork; got \(cause)")
        case .admit(let sliceBytes):
            Issue.record("non-compute-only pre-analysis on an unreachable network must be transport-rejected; got admit(\(sliceBytes))")
        }
    }

    // MARK: - Invariant 3 — Reject visibility

    @Test("Invariant 3: a rejected pass leaves a durable, queryable trace (not silence)",
          .timeLimit(.minutes(1)))
    func invariant3_rejectWritesDurableReason() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        // Storage denies media; transport is Wi-Fi and the job is compute-only
        // so ONLY the storage axis rejects → surfaced cause is deterministic.
        let scheduler = makeScheduler(
            store: store, downloads: downloads,
            storage: StubStorageBudgetSnapshotter(denyClass: .media)
        )
        let job = try await insertComputeOnlyJob(
            store: store, downloads: downloads,
            jobId: "storage-blocked", episodeId: "ep-storage-blocked", priority: 0
        )

        // Precondition: no advisory before the pass.
        #expect(try await store.fetchJobAdmissionReject(jobId: job.jobId) == nil)

        let decision = await scheduler.evaluateAdmissionGate(for: job)
        guard case .reject(let cause) = decision else {
            Issue.record("expected a storage reject; got \(decision)")
            return
        }
        #expect(cause == .mediaCap)

        // The stall was invisible because the reject path wrote nothing. Now
        // it leaves a durable, queryable trace.
        let advisory = try await store.fetchJobAdmissionReject(jobId: job.jobId)
        #expect(advisory != nil, "a rejected pass must leave a durable reject reason, not silence")
        #expect(advisory?.reason == InternalMissCause.mediaCap.rawValue)
        #expect((advisory?.at ?? 0) > 0)
    }

    // MARK: - Invariant 4 — Epoch survival across bump / restart

    @Test("Invariant 4: queued job survives an epoch bump and reconcile re-stamps it current",
          .timeLimit(.minutes(1)))
    func invariant4_epochSurvivalAndRestamp() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()

        // A queued job minted under the epoch-0 sentinel (the dogfood shape:
        // _meta.scheduler_epoch=1, queued rows at epoch 0). No cached file so
        // reconcile's discovery/unblock steps leave it alone.
        let job = makeAnalysisJob(
            jobId: "epoch-job",
            jobType: "preAnalysis",
            episodeId: "ep-epoch",
            workKey: "fp-epoch:1:preAnalysis",
            sourceFingerprint: "fp-epoch",
            priority: 0,
            desiredCoverageSec: 90,
            state: "queued",
            schedulerEpoch: 0
        )
        try await store.insertJob(job)

        // Bump the scheduler epoch (fresh DB seeds at 1 → now 2).
        let bumpedEpoch = try await store.incrementSchedulerEpoch()
        #expect(bumpedEpoch >= 2)

        // Dispatch eligibility never consulted schedulerEpoch — the queued
        // row is still SQL-selectable after the bump (this holds pre-fix too).
        let now = Date().timeIntervalSince1970 + 1
        let selected = try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true, t0ThresholdSec: 0, now: now
        )
        #expect(selected?.jobId == "epoch-job", "queued job must remain selectable across an epoch bump")

        // Reconcile must re-stamp the stale queued row to the current epoch so
        // orphan-recovery routing is consistent (fails pre-fix — no re-stamp).
        let reconciler = AnalysisJobReconciler(
            store: store, downloadManager: downloads, capabilitiesService: StubCapabilitiesProvider()
        )
        let report = try await reconciler.reconcile()
        #expect(report.queuedJobEpochsRestamped == 1, "reconcile must re-stamp the stale queued row")

        let after = try await store.fetchJob(byId: "epoch-job")
        #expect(after?.state == "queued", "re-stamp must not change dispatchability")
        #expect(after?.schedulerEpoch == bumpedEpoch, "queued row must carry the current scheduler epoch after reconcile")
    }

    // MARK: - Invariant 5 — Background progress OR valid defer (never silent)

    @Test("Invariant 5a: a drain over eligible work strictly decreases pending",
          .timeLimit(.minutes(1)))
    func invariant5_drainMakesProgress() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        for i in 0..<3 {
            try await insertComputeOnlyJob(
                store: store, downloads: downloads,
                jobId: "prog-\(i)", episodeId: "ep-prog-\(i)",
                priority: 10, attemptCount: 4
            )
        }
        let scheduler = makeScheduler(store: store, downloads: downloads, audio: FailingDecodeStub())

        let before = try await pendingCount(store)
        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600))
        let after = try await pendingCount(store)

        // Never the stall shape (task_expired + 0 done, pending unchanged).
        #expect(after < before, "a BG drain over eligible work must strictly decrease pending (\(before) -> \(after))")
    }

    @Test("Invariant 5b: when every pass is storage-rejected, the defer is VALID — durable reasons, not silence",
          .timeLimit(.minutes(1)))
    func invariant5_validDeferLeavesReasons() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let ids = (0..<3).map { "defer-\($0)" }
        for id in ids {
            try await insertComputeOnlyJob(
                store: store, downloads: downloads,
                jobId: id, episodeId: "ep-\(id)", priority: 10
            )
        }
        // Storage denies media → every dispatch pass hard-rejects.
        let scheduler = makeScheduler(
            store: store, downloads: downloads,
            storage: StubStorageBudgetSnapshotter(denyClass: .media)
        )

        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600))

        // Valid defer: nothing was wrongly dispatched — the queue is intact.
        // (`drainEligible` breaks on the first hard reject by design; one
        // storage-blocked pass is enough to defer the whole window.)
        #expect(try await pendingCount(store) == 3, "storage-blocked work must remain queued (valid defer)")

        // And the defer is DIAGNOSABLE: every rejected admission pass leaves a
        // durable reason — never the silent "Nothing running" of the stall.
        for id in ids {
            let job = try #require(try await store.fetchJob(byId: id))
            let decision = await scheduler.evaluateAdmissionGate(for: job)
            guard case .reject = decision else {
                Issue.record("\(id) must be storage-rejected; got \(decision)")
                continue
            }
            let advisory = try await store.fetchJobAdmissionReject(jobId: id)
            #expect(advisory?.reason == InternalMissCause.mediaCap.rawValue,
                    "a rejected pass must leave a durable reason for \(id), never silence")
        }
    }

    // MARK: - RC-3 — enqueue stamps epoch + non-blank generation

    @Test("RC-3: enqueue stamps the current scheduler epoch and a non-blank generation id",
          .timeLimit(.minutes(1)))
    func rc3_enqueueStampsEpochAndGeneration() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloads: downloads)

        // Fresh DB seeds _meta.scheduler_epoch = 1.
        let currentEpoch = try await store.fetchSchedulerEpoch()
        #expect(currentEpoch == 1)

        await scheduler.enqueue(
            episodeId: "ep-rc3",
            podcastId: "pod-rc3",
            downloadId: "dl-rc3",
            sourceFingerprint: "fp-rc3",
            isExplicitDownload: true
        )

        let jobs = try await store.fetchJobsByState("queued")
        let enqueued = try #require(jobs.first { $0.episodeId == "ep-rc3" })
        #expect(enqueued.schedulerEpoch == 1, "enqueue must stamp the current scheduler epoch, not the 0 sentinel")
        #expect(!enqueued.generationID.isEmpty, "enqueue must stamp a non-blank generation id for orphan-recovery routing")
    }
}
