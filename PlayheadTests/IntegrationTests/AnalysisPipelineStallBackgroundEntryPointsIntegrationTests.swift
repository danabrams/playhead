// AnalysisPipelineStallBackgroundEntryPointsIntegrationTests.swift
// playhead-gy2s: integration pin on the BACKGROUND entry points of the
// pipeline-stall fix (RC-1). Drives the real `BackgroundProcessingService`
// handlers against a real `AnalysisWorkScheduler` + `AnalysisJobReconciler`
// + `AnalysisStore` + `AnalysisStoreBackgroundTaskRunLedger`, and asserts:
//
//   Invariant 5 (background progress): a simulated backfill BGTask window
//     with eligible work drains the queue (pending strictly decreases) —
//     never the stall shape (task_expired + 0 done, pending unchanged).
//   Invariant 6 (recovery visibility): the pre-analysis recovery run
//     records `jobsSeen >= #eligible queued` in the durable ledger, instead
//     of the pre-fix `jobsSeen = NULL` that made the stall invisible.
//
// Pre-fix, `handleBackfillTask` only `wake()`d the loop + polled and
// `handlePreAnalysisRecovery` only reconciled — so with a queue of
// queued-but-unleased jobs neither DISPATCHED, pending never dropped, and
// the recovery ledger row recorded jobsSeen=NULL / jobsCompleted=0.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-gy2s: pipeline-stall background entry points")
struct AnalysisPipelineStallBackgroundEntryPointsIntegrationTests {

    /// Always-throwing decode → routes each dispatched job through the
    /// `.failed` arm; pre-stamped `attemptCount` drives it terminal in one
    /// pass so the drain has a deterministic fixed point.
    private final class FailingDecodeStub: AnalysisAudioProviding, @unchecked Sendable {
        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            throw AnalysisAudioError.decodingFailed("Operation Interrupted")
        }
    }

    private func makeWorkScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider
    ) -> AnalysisWorkScheduler {
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: FailingDecodeStub(),
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

    /// Insert `count` compute-only pre-analysis jobs (cached file present,
    /// version-matched, pre-stamped attemptCount so a single failing decode
    /// terminates them) and return the count actually enqueued.
    @discardableResult
    private func seedEligibleJobs(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        count: Int,
        prefix: String
    ) async throws -> Int {
        for i in 0..<count {
            let episodeId = "\(prefix)-ep-\(i)"
            downloads.cachedURLs[episodeId] = URL(fileURLWithPath: "/tmp/\(episodeId).m4a")
            try await store.insertJob(makeAnalysisJob(
                jobId: "\(prefix)-\(i)",
                jobType: "preAnalysis",
                episodeId: episodeId,
                workKey: AnalysisJob.computeWorkKey(
                    fingerprint: "fp-\(prefix)-\(i)",
                    analysisVersion: PreAnalysisConfig.analysisVersion,
                    jobType: "preAnalysis"
                ),
                sourceFingerprint: "fp-\(prefix)-\(i)",
                priority: 10,
                desiredCoverageSec: 90,
                state: "queued",
                attemptCount: 4
            ))
        }
        return count
    }

    private func pendingCount(_ store: AnalysisStore) async throws -> Int {
        let queued = try await store.fetchJobsByState("queued")
        let running = try await store.fetchJobsByState("running")
        let paused = try await store.fetchJobsByState("paused")
        return queued.count + running.count + paused.count
    }

    // MARK: - Invariant 5 (background progress via the backfill BGTask)

    @Test("Invariant 5: the backfill BGTask window drains eligible work (pending strictly decreases)",
          .timeLimit(.minutes(1)))
    func invariant5_backfillTaskDrainsQueue() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await seedEligibleJobs(store: store, downloads: downloads, count: 4, prefix: "bf")

        let coordinator = StubAnalysisCoordinator()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
        let scheduler = makeWorkScheduler(store: store, downloads: downloads)
        let reconciler = AnalysisJobReconciler(
            store: store, downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider()
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: reconciler)

        let before = try await pendingCount(store)
        #expect(before == 4)

        let task = StubBackgroundTask()
        await bps.handleBackfillTask(task)
        await task.awaitCompletion()
        // Quiesce the loop that the handler started so the pending read is
        // stable (the drain already ran to completion inside the handler).
        await scheduler.stop()

        let after = try await pendingCount(store)
        #expect(after < before, "the backfill BGTask must actively drain eligible work; pending \(before) -> \(after)")
        #expect(coordinator.runPendingBackfillCallCount >= 1, "the poll loop must still run to keep the task alive")
    }

    // MARK: - Invariant 6 (recovery records jobsSeen)

    @Test("Invariant 6: pre-analysis recovery records jobsSeen >= #eligible queued",
          .timeLimit(.minutes(1)))
    func invariant6_recoveryRecordsJobsSeen() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let eligible = try await seedEligibleJobs(store: store, downloads: downloads, count: 3, prefix: "rec")

        let coordinator = StubAnalysisCoordinator()
        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
        let scheduler = makeWorkScheduler(store: store, downloads: downloads)
        let reconciler = AnalysisJobReconciler(
            store: store, downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider()
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: reconciler)

        let task = StubBackgroundTask()
        // Warm path: reconciler is already injected, so the handler runs
        // synchronously to completion.
        await bps.handlePreAnalysisRecovery(task)
        await task.awaitCompletion()
        await scheduler.stop()

        let latest = await ledger.fetchLatestRun(for: .preAnalysisRecovery)
        #expect(latest != nil, "recovery must record a ledger row")
        // Pre-fix: jobsSeen was NULL — the stall was invisible in the pull.
        #expect((latest?.jobsSeen ?? -1) >= eligible,
                "recovery must record jobsSeen >= #eligible queued; got \(String(describing: latest?.jobsSeen)) for \(eligible) eligible")
    }
}
