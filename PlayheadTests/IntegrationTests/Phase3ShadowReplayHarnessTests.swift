// Phase3ShadowReplayHarnessTests.swift
// End-to-end shadow replay harness. Feeds synthetic transcripts through
// BackfillJobRunner with a deterministic TestFMRuntime, asserts the persisted
// telemetry rows are present, the AdWindow path is untouched, and the
// orchestration cost stays under a budget. The benchmark gate intentionally
// uses a zero-latency runtime so it measures orchestration overhead, not FM
// inference time.

import Foundation
import Testing

@testable import Playhead

@Suite("Phase 3 shadow replay harness")
struct Phase3ShadowReplayHarnessTests {

    private struct Episode: Sendable {
        let assetId: String
        let podcastId: String
        let chunks: [TranscriptChunk]
        let duration: Double
    }

    private func makeEpisode(index: Int) -> Episode {
        let assetId = "asset-replay-\(index)"
        let texts = [
            "Welcome back to the show, listeners. Today's deep dive is on something we've been promising for a while.",
            "But first, this episode is brought to you by Squarespace. Use the promo code SHOW for 20 percent off your first website at squarespace dot com slash show.",
            "And we are also supported by BetterHelp. Visit betterhelp dot com slash podcast to get matched with a therapist.",
            "Now back to our regularly scheduled programming. Our guest today has spent fifteen years studying the topic.",
            "Thanks for listening. We'll see you next week."
        ]
        let chunks = texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)-\(index)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
        return Episode(
            assetId: assetId,
            podcastId: "podcast-replay",
            chunks: chunks,
            duration: 150
        )
    }

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeShadowService(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: { store, mode in
                BackfillJobRunner(
                    store: store,
                    admissionController: AdmissionController(),
                    classifier: FoundationModelClassifier(
                        runtime: TestFMRuntime(
                            coarseResponses: [
                                CoarseScreeningSchema(
                                    disposition: .containsAd,
                                    support: CoarseSupportSchema(
                                        supportLineRefs: [1, 2],
                                        certainty: .strong
                                    )
                                )
                            ]
                        ).runtime
                    ),
                    coveragePlanner: CoveragePlanner(),
                    mode: mode,
                    capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                    batteryLevelProvider: { 1.0 },
                    scanCohortJSON: makeTestScanCohortJSON()
                )
            }
        )
    }

    private func makeDisabledService(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .off
            )
        )
    }

    @Test("synthetic episode produces telemetry rows under shadow mode")
    func syntheticEpisodeWritesTelemetry() async throws {
        let store = try await makeTestStore()
        let episode = makeEpisode(index: 0)
        try await store.insertAsset(makeAsset(id: episode.assetId))
        let service = makeShadowService(store: store)

        try await service.runBackfill(
            chunks: episode.chunks,
            analysisAssetId: episode.assetId,
            podcastId: episode.podcastId,
            episodeDuration: episode.duration
        )

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: episode.assetId)
        #expect(!scans.isEmpty, "shadow harness must persist semantic scan results")

        let windows = try await store.fetchAdWindows(assetId: episode.assetId)
        // No FM-attributed AdWindow rows. The detector version pins lexical
        // ownership; if a future change starts attributing FM evidence to a
        // different detector version this assertion will catch it.
        #expect(windows.allSatisfy { $0.detectorVersion == "detection-v1" })
    }

    @Test("shadow harness preserves the lexical-only cue count")
    func shadowHarnessPreservesLexicalCueCount() async throws {
        let episode = makeEpisode(index: 1)

        // Lexical baseline.
        let storeBaseline = try await makeTestStore()
        try await storeBaseline.insertAsset(makeAsset(id: episode.assetId))
        let baselineService = makeDisabledService(store: storeBaseline)
        try await baselineService.runBackfill(
            chunks: episode.chunks,
            analysisAssetId: episode.assetId,
            podcastId: episode.podcastId,
            episodeDuration: episode.duration
        )
        let baselineWindows = try await storeBaseline.fetchAdWindows(assetId: episode.assetId)

        // Shadow run.
        let storeShadow = try await makeTestStore()
        try await storeShadow.insertAsset(makeAsset(id: episode.assetId))
        let shadowService = makeShadowService(store: storeShadow)
        try await shadowService.runBackfill(
            chunks: episode.chunks,
            analysisAssetId: episode.assetId,
            podcastId: episode.podcastId,
            episodeDuration: episode.duration
        )
        let shadowWindows = try await storeShadow.fetchAdWindows(assetId: episode.assetId)

        #expect(baselineWindows.count == shadowWindows.count)
    }

    /// PerfGate'd on measurement, playhead-o89d. The budget IS the assertion
    /// here, so what it reports is whatever share of the box the test got:
    ///
    ///     alone (this suite only, 3 tests)          <1.9 s total   PASS
    ///     52-suite scoped selection (443 tests)     25.2 s         FAIL  5x over
    ///     full PlayheadFastTests plan (~11.9k)      105–162 s      FAIL  21–32x over
    ///
    /// Measured 2026-08-13 over 5 scoped runs and the two preserved full-plan
    /// logs. It has failed every recorded full-plan run since the baseline
    /// began, and each of those was triaged as one more starvation flake — so
    /// a genuine orchestration regression (the N^2 walk the comment below
    /// warns about) would have been indistinguishable from the noise. Serial
    /// pass only, where the number means what it says.
    @Test("benchmark gate: 3 episodes finish under the orchestration budget",
          .enabled(if: PerfGate.runsMeasurementTests, "perf pass only — see playhead-zx0l"))
    func benchmarkGate() async throws {
        let store = try await makeTestStore()
        let episodes = (0..<3).map { makeEpisode(index: $0) }
        for episode in episodes {
            try await store.insertAsset(makeAsset(id: episode.assetId))
        }

        let service = makeShadowService(store: store)
        let clock = ContinuousClock()
        let start = clock.now
        for episode in episodes {
            try await service.runBackfill(
                chunks: episode.chunks,
                analysisAssetId: episode.assetId,
                podcastId: episode.podcastId,
                episodeDuration: episode.duration
            )
        }
        let elapsed = clock.now - start
        let elapsedMillis = Double(elapsed.components.attoseconds) / 1e15
            + Double(elapsed.components.seconds) * 1000.0

        // Generous budget: orchestration alone (no real FM latency) for 3
        // synthetic episodes should be well under 5 seconds. If this trips on
        // CI it usually means a per-episode N^2 walk crept into the runner.
        #expect(elapsedMillis < 5000.0, "shadow orchestration took \(elapsedMillis) ms for 3 episodes")
    }
}
