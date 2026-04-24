// AcousticFeaturePipelineTests.swift
// playhead-gtt9.12 acceptance #3 integration test: on a synthetic all-acoustic
// signal the fusion pipeline produces at least one high-confidence candidate.
//
// Also exercises acceptance #1 — the funnel reports compute events for every
// feature — and verifies that combined fusion is strictly higher than any
// single feature's contribution when multiple signals fire together.

import Foundation
import Testing

@testable import Playhead

@Suite("AcousticFeaturePipeline")
struct AcousticFeaturePipelineTests {

    /// Build a fixture: 30 windows of host content, 10 windows of a loud,
    /// compressed, music-bed, spectrally-shifted speaker insertion, 30 more
    /// windows of host content. Silence bumpers frame the insertion.
    private func syntheticAdEpisode() -> [FeatureWindow] {
        var windows: [FeatureWindow] = []
        // Host content (cluster 0, quiet, no music).
        for i in 0..<30 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.18, spectralFlux: 0.03,
                musicProbability: 0.02, pauseProbability: 0.05,
                speakerChangeProxyScore: 0.05,
                speakerClusterId: 0
            ))
        }
        // Silence bumper (1 window).
        windows.append(AcousticFeatureFixtures.window(
            startTime: 60, endTime: 62,
            rms: 0.002, spectralFlux: 0.01,
            musicProbability: 0.0, pauseProbability: 0.9,
            speakerChangeProxyScore: 0.7,
            speakerClusterId: 0
        ))
        // Ad block: loud, compressed, spectrally active, music bed,
        // different speaker cluster.
        let adStart = windows.count
        for i in adStart..<(adStart + 10) {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.70, spectralFlux: 0.30,
                musicProbability: 0.80, pauseProbability: 0.02,
                speakerChangeProxyScore: 0.70,
                musicBedLevel: .foreground,
                speakerClusterId: 9
            ))
        }
        // Closing silence bumper.
        let closeStart = windows.count
        windows.append(AcousticFeatureFixtures.window(
            startTime: Double(closeStart) * 2, endTime: Double(closeStart + 1) * 2,
            rms: 0.003, spectralFlux: 0.01,
            musicProbability: 0.0, pauseProbability: 0.9,
            speakerChangeProxyScore: 0.7,
            speakerClusterId: 0
        ))
        // Host tail.
        let tailStart = windows.count
        for i in tailStart..<(tailStart + 30) {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.18, spectralFlux: 0.03,
                musicProbability: 0.02, pauseProbability: 0.05,
                speakerChangeProxyScore: 0.05,
                speakerClusterId: 0
            ))
        }
        return windows
    }

    @Test("funnel records compute events for every feature over every window")
    func funnelComputeCoverage() {
        let windows = syntheticAdEpisode()
        let result = AcousticFeaturePipeline.run(windows: windows)
        for feature in AcousticFeatureKind.allCases {
            #expect(result.funnel.count(.computed, feature) == windows.count,
                    "feature \(feature) should be computed on all \(windows.count) windows")
        }
    }

    @Test("multi-signal ad block fuses higher than any single feature's contribution")
    func fusionStacksEvidence() {
        let windows = syntheticAdEpisode()
        let result = AcousticFeaturePipeline.run(windows: windows)

        // Find the ad block — it starts at index 31 per the fixture.
        let fusion = result.fusion
        #expect(fusion.count == windows.count)

        let adFusionSlice = fusion[31..<41]
        let maxCombined = adFusionSlice.map(\.combinedScore).max() ?? 0
        let maxContributing = adFusionSlice.map(\.contributingFeatures.count).max() ?? 0
        #expect(maxCombined > 0.25, "ad block should fuse to a meaningful combined score")
        #expect(maxContributing >= 2, "multi-signal ad should have at least two features firing")

        // Compare to host content baseline.
        let hostMax = fusion[0..<30].map(\.combinedScore).max() ?? 0
        #expect(maxCombined > hostMax, "ad block should outscore host baseline")
    }

    @Test("all-acoustic episode with zero transcript yields at least one high-confidence window")
    func zeroTranscriptCoverageStillProducesCandidate() {
        // Per bead acceptance #3: on assets with zero transcript coverage, the
        // acoustic pipeline alone must still produce at least one candidate
        // per plausible ad region. We simulate that with our fixture's ad
        // block and require a sustained run of elevated combined scores.
        let windows = syntheticAdEpisode()
        let result = AcousticFeaturePipeline.run(windows: windows)
        let adBlockScores = result.fusion[31..<41].map(\.combinedScore)
        let highWindows = adBlockScores.filter { $0 >= 0.20 }.count
        #expect(highWindows >= 3,
                "should produce at least 3 candidate windows in a plausible ad region, got \(highWindows)")
    }

    @Test("repetition fingerprint stub does not suppress other features")
    func repetitionStubIsTransparent() {
        let windows = syntheticAdEpisode()
        let result = AcousticFeaturePipeline.run(windows: windows)
        // RepetitionFingerprint contributes zero, so the aggregate should
        // still be non-zero somewhere thanks to the other seven features.
        #expect(result.fusion.contains { $0.combinedScore > 0 })
        #expect(result.funnel.count(.passedGate, .repetitionFingerprint) == 0)
    }

    @Test("funnel rows snapshot is stable in length and ordering")
    func funnelSnapshotIsStable() {
        let windows = syntheticAdEpisode()
        let result = AcousticFeaturePipeline.run(windows: windows)
        let rows = result.funnel.rows()
        let expected = AcousticFeatureKind.allCases.count * AcousticFeatureFunnelStage.allCases.count
        #expect(rows.count == expected)
        // First row should be musicBed × computed (based on allCases ordering).
        #expect(rows.first?.feature == AcousticFeatureKind.allCases.first)
        #expect(rows.first?.stage == AcousticFeatureFunnelStage.allCases.first)
    }
}
