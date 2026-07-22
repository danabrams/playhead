// AnalysisWorkSchedulerUserIntentTests.swift
// playhead-3xtw: the on-demand "Download & Analyze" control routes its
// analysis to the USER-INTENT (`.now`) lane so it preempts starving
// background work. These tests prove `markEpisodeUserIntent` +
// `enqueue(...)` and `enqueueUserIntentAnalysis(...)` produce a
// priority-20 (`.now`-lane) job, that the flag is per-episode and one-shot,
// and that the enqueue stays work-key idempotent.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — user-intent lane (playhead-3xtw)")
struct AnalysisWorkSchedulerUserIntentTests {

    /// Minimal scheduler over an in-memory store + stub dependencies —
    /// mirrors `AnalysisWorkSchedulerThreeLaneTests.makeScheduler`. Only
    /// `enqueue` / `markEpisodeUserIntent` are exercised here, none of
    /// which runs the job runner.
    private func makeScheduler(store: AnalysisStore) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
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
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            config: PreAnalysisConfig()
        )
    }

    private func queuedJob(
        for episodeId: String,
        in store: AnalysisStore
    ) async throws -> AnalysisJob? {
        let jobs = try await store.fetchJobsByState("queued")
        return jobs.first { $0.episodeId == episodeId }
    }

    // MARK: - mark + enqueue → .now lane

    @Test("marked episode's next enqueue lands at priority 20 (.now lane) with requested coverage")
    func testMarkThenEnqueueIsUserIntent() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.markEpisodeUserIntent(episodeId: "ep-user", desiredCoverageSec: 3600)
        // Simulate the download-completion enqueue (auto flags:
        // isExplicitDownload=false, no explicit coverage).
        await scheduler.enqueue(
            episodeId: "ep-user",
            podcastId: "pod",
            downloadId: "ep-user",
            sourceFingerprint: "fp-user",
            isExplicitDownload: false
        )

        let job = try await queuedJob(for: "ep-user", in: store)
        #expect(job?.priority == 20)
        #expect(job?.schedulerLane == .now)
        #expect(job?.desiredCoverageSec == 3600)
    }

    @Test("enqueueUserIntentAnalysis lands at priority 20 (.now lane)")
    func testEnqueueUserIntentAnalysis() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-direct",
            podcastId: "pod",
            sourceFingerprint: "fp-direct",
            desiredCoverageSec: 1800,
            podcastTitle: "Pod",
            episodeTitle: "Ep"
        )

        let job = try await queuedJob(for: "ep-direct", in: store)
        #expect(job?.priority == 20)
        #expect(job?.schedulerLane == .now)
        #expect(job?.desiredCoverageSec == 1800)
    }

    // MARK: - Control: without the flag, existing behaviour is unchanged

    @Test("without a user-intent mark, enqueue keeps the legacy priority mapping")
    func testLegacyPrioritiesUnchanged() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.enqueue(
            episodeId: "ep-auto",
            podcastId: nil,
            downloadId: "ep-auto",
            sourceFingerprint: "fp-auto",
            isExplicitDownload: false
        )
        await scheduler.enqueue(
            episodeId: "ep-explicit",
            podcastId: nil,
            downloadId: "ep-explicit",
            sourceFingerprint: "fp-explicit",
            isExplicitDownload: true
        )

        let auto = try await queuedJob(for: "ep-auto", in: store)
        let explicit = try await queuedJob(for: "ep-explicit", in: store)
        #expect(auto?.priority == 0)
        #expect(auto?.schedulerLane == .background)
        #expect(explicit?.priority == 10)
        #expect(explicit?.schedulerLane == .soon)
    }

    // MARK: - Flag is per-episode (does not leak to other episodes)

    @Test("user-intent flag applies only to the marked episode")
    func testFlagIsPerEpisode() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.markEpisodeUserIntent(episodeId: "ep-A", desiredCoverageSec: nil)
        // A different episode enqueues at its normal (auto) priority.
        await scheduler.enqueue(
            episodeId: "ep-B",
            podcastId: nil,
            downloadId: "ep-B",
            sourceFingerprint: "fp-B",
            isExplicitDownload: false
        )
        // The marked one gets the user-intent lane.
        await scheduler.enqueue(
            episodeId: "ep-A",
            podcastId: nil,
            downloadId: "ep-A",
            sourceFingerprint: "fp-A",
            isExplicitDownload: false
        )

        let a = try await queuedJob(for: "ep-A", in: store)
        let b = try await queuedJob(for: "ep-B", in: store)
        #expect(a?.priority == 20)
        #expect(b?.priority == 0)
    }

    // MARK: - Idempotent (work-key dedup)

    @Test("re-enqueuing the same episode+fingerprint keeps a single job row")
    func testIdempotentEnqueue() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-dedup",
            podcastId: nil,
            sourceFingerprint: "fp-dedup",
            desiredCoverageSec: 600,
            podcastTitle: nil,
            episodeTitle: nil
        )
        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-dedup",
            podcastId: nil,
            sourceFingerprint: "fp-dedup",
            desiredCoverageSec: 600,
            podcastTitle: nil,
            episodeTitle: nil
        )

        let jobs = try await store.fetchJobsByState("queued")
        #expect(jobs.filter { $0.episodeId == "ep-dedup" }.count == 1)
    }
}
