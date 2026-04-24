// AcousticFeaturePipelineWiringTests.swift
// playhead-gtt9.16: Wire `AcousticFeaturePipeline` into production.
//
// gtt9.12 shipped the pipeline but never called it from `AdDetectionService`.
// This suite verifies the pipeline runs during `runBackfill` (and Tier 1 hot
// path) and that its outputs enter the fusion evidence array.
//
// Acceptance:
//   1. After a backfill with non-trivial FeatureWindows, `AcousticFeatureFunnel`
//      counters register `computed` events for all 8 features.
//   2. Integration: acoustic-pipeline evidence reaches the fusion ledger for
//      at least one synthetic ad window.
//   3. Back-compat: with all-zero feature windows (no music/flux/etc.), the
//      pipeline contributes no additional `.acoustic` weight to fusion.

import Foundation
import Testing
@testable import Playhead

@Suite("AcousticFeaturePipeline production wiring (gtt9.16)")
struct AcousticFeaturePipelineWiringTests {

    // MARK: - Fixtures

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

    /// Synthetic feature windows shaped as host → ad block → host.
    /// Mirrors `AcousticFeaturePipelineTests.syntheticAdEpisode` so the
    /// pipeline's multi-feature firing geometry matches the unit-level
    /// contract.
    private func syntheticAdWindows(assetId: String) -> [FeatureWindow] {
        var out: [FeatureWindow] = []
        // 30 host windows (quiet).
        for i in 0..<30 {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.18,
                spectralFlux: 0.03,
                musicProbability: 0.02,
                speakerChangeProxyScore: 0.05,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .none,
                pauseProbability: 0.05,
                speakerClusterId: 0,
                jingleHash: nil,
                featureVersion: 4
            ))
        }
        // Silence bumper.
        out.append(FeatureWindow(
            analysisAssetId: assetId,
            startTime: 60, endTime: 62,
            rms: 0.002,
            spectralFlux: 0.01,
            musicProbability: 0.0,
            speakerChangeProxyScore: 0.7,
            musicBedChangeScore: 0,
            musicBedOnsetScore: 0,
            musicBedOffsetScore: 0,
            musicBedLevel: .none,
            pauseProbability: 0.9,
            speakerClusterId: 0,
            jingleHash: nil,
            featureVersion: 4
        ))
        // 10 ad-block windows (loud, music bed, different speaker).
        let adStart = out.count
        for i in adStart..<(adStart + 10) {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.70,
                spectralFlux: 0.30,
                musicProbability: 0.80,
                speakerChangeProxyScore: 0.70,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .foreground,
                pauseProbability: 0.02,
                speakerClusterId: 9,
                jingleHash: nil,
                featureVersion: 4
            ))
        }
        // Closing silence.
        let closeStart = out.count
        out.append(FeatureWindow(
            analysisAssetId: assetId,
            startTime: Double(closeStart) * 2,
            endTime: Double(closeStart + 1) * 2,
            rms: 0.003,
            spectralFlux: 0.01,
            musicProbability: 0.0,
            speakerChangeProxyScore: 0.7,
            musicBedChangeScore: 0,
            musicBedOnsetScore: 0,
            musicBedOffsetScore: 0,
            musicBedLevel: .none,
            pauseProbability: 0.9,
            speakerClusterId: 0,
            jingleHash: nil,
            featureVersion: 4
        ))
        // Tail host.
        let tailStart = out.count
        for i in tailStart..<(tailStart + 30) {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.18,
                spectralFlux: 0.03,
                musicProbability: 0.02,
                speakerChangeProxyScore: 0.05,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .none,
                pauseProbability: 0.05,
                speakerClusterId: 0,
                jingleHash: nil,
                featureVersion: 4
            ))
        }
        return out
    }

    /// Zero-signal feature windows — all fields at rest. Used for back-compat:
    /// the pipeline should contribute no additional acoustic weight.
    private func zeroSignalWindows(assetId: String, count: Int = 10, step: Double = 2.0) -> [FeatureWindow] {
        var out: [FeatureWindow] = []
        var t = 0.0
        for _ in 0..<count {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: t,
                endTime: t + step,
                rms: 0.0,
                spectralFlux: 0.0,
                musicProbability: 0.0,
                speakerChangeProxyScore: 0.0,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .none,
                pauseProbability: 0.0,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 4
            ))
            t += step
        }
        return out
    }

    private func makeService(store: AnalysisStore) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "gtt9.16-test",
            fmBackfillMode: .off
        )
        return AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config
        )
    }

    private func lexicalAdChunks(assetId: String) -> [TranscriptChunk] {
        // Place a lexical hit inside the ad block timespan (62–82s) so that
        // a DecodedSpan is produced and the ledger is populated.
        let texts = [
            (0.0, 30.0, "Welcome back to the show today we discuss technology."),
            (60.0, 90.0, "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show."),
            (90.0, 120.0, "Back to our regular conversation about new things.")
        ]
        return texts.enumerated().map { idx, triple in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: triple.0,
                endTime: triple.1,
                text: triple.2,
                normalizedText: triple.2.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    // MARK: - Tests

    @Test("runBackfill records AcousticFeatureFunnel compute events for all 8 features")
    func funnelComputesAllFeaturesDuringBackfill() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.16-funnel"
        try await store.insertAsset(makeAsset(id: assetId))
        let windows = syntheticAdWindows(assetId: assetId)
        try await store.insertFeatureWindows(windows)

        let service = makeService(store: store)
        let chunks = lexicalAdChunks(assetId: assetId)
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "",
            episodeDuration: 200.0
        )

        let funnel = await service.acousticFunnelForTesting()
        for feature in AcousticFeatureKind.allCases {
            let computed = funnel.count(AcousticFeatureFunnelStage.computed, feature)
            #expect(
                computed == windows.count,
                "expected \(windows.count) computed events for \(feature), got \(computed)"
            )
        }
    }

    @Test("pipeline contributes fusion evidence on a synthetic ad block")
    func pipelineContributesFusionEvidence() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.16-fusion"
        try await store.insertAsset(makeAsset(id: assetId))
        let windows = syntheticAdWindows(assetId: assetId)
        try await store.insertFeatureWindows(windows)

        let service = makeService(store: store)
        try await service.runBackfill(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "",
            episodeDuration: 200.0
        )

        // After wiring, the pipeline should have produced at least one window
        // where the fused combined score is > 0 and included in fusion.
        let funnel = await service.acousticFunnelForTesting()
        let includedTotal = funnel.total(AcousticFeatureFunnelStage.includedInFusion)
        #expect(includedTotal > 0, "expected at least one pipeline feature to be included in fusion, got \(includedTotal)")

        // The pipeline surface (captured for inspection) should have fused
        // the ad block into a combined score > 0.
        let lastFusion = await service.lastAcousticPipelineFusionForTesting()
        let nonzero = lastFusion.contains { $0.combinedScore > 0 }
        #expect(nonzero, "expected at least one window with combinedScore > 0 after wiring")
    }

    @Test("zero-signal windows leave behavior unchanged (back-compat)")
    func zeroSignalBackCompat() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.16-zero"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(zeroSignalWindows(assetId: assetId, count: 30))

        let service = makeService(store: store)
        try await service.runBackfill(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "",
            episodeDuration: 60.0
        )

        // Back-compat: with no signal, no pipeline feature should have made
        // it into fusion. The funnel records compute but zero passedGate /
        // zero includedInFusion across all eight features.
        let funnel = await service.acousticFunnelForTesting()
        #expect(funnel.total(AcousticFeatureFunnelStage.includedInFusion) == 0, "expected zero pipeline features in fusion on silent input")

        // Pipeline result was captured but contributed zero combined mass.
        let fusion = await service.lastAcousticPipelineFusionForTesting()
        let anyNonzero = fusion.contains { $0.combinedScore > 0 }
        #expect(!anyNonzero, "expected all-zero combined scores on silent input, got some > 0")
    }
}
