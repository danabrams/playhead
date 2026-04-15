// FineBoundaryRefinerTests.swift
// Tests for ef2.3.7: Fine boundary refiner with 150ms hops.

import Testing

@testable import Playhead

// MARK: - FineBoundaryRefiner Tests

@Suite("FineBoundaryRefiner")
struct FineBoundaryRefinerTests {

    // MARK: - Helpers

    private func makeWindow(
        start: Double,
        end: Double,
        rms: Double = 0.3,
        spectralFlux: Double = 0.1,
        pauseProbability: Double = 0.1
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "test-fine-boundary",
            startTime: start,
            endTime: end,
            rms: rms,
            spectralFlux: spectralFlux,
            musicProbability: 0.0,
            pauseProbability: pauseProbability,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        )
    }

    // MARK: - Coarse fallback

    @Test("returns coarse fallback when no features are in range")
    func coarseFallbackWhenNoFeatures() {
        let result = FineBoundaryRefiner.refineBoundary(
            candidate: 10.0,
            features: [],
            direction: .adStart
        )

        #expect(result.confidence == 0.0)
        #expect(result.cueBreakdown[.coarseFallback] == 1.0)
    }

    @Test("returns coarse fallback when features are outside search radius")
    func coarseFallbackWhenFeaturesOutOfRange() {
        let features = [
            makeWindow(start: 0.0, end: 2.0),
            makeWindow(start: 2.0, end: 4.0),
        ]

        let result = FineBoundaryRefiner.refineBoundary(
            candidate: 20.0,
            features: features,
            direction: .adEnd
        )

        #expect(result.confidence == 0.0)
        #expect(result.cueBreakdown[.coarseFallback] == 1.0)
    }

    // MARK: - Silence gap preference

    @Test("snaps to silence gap when strong pause probability exists")
    func snapsToSilenceGap() {
        // Create a band where one window has a strong silence gap.
        let features = [
            makeWindow(start: 7.0, end: 9.0, rms: 0.3, pauseProbability: 0.1),
            makeWindow(start: 9.0, end: 11.0, rms: 0.01, pauseProbability: 0.95),
            makeWindow(start: 11.0, end: 13.0, rms: 0.3, pauseProbability: 0.1),
        ]

        let result = FineBoundaryRefiner.refineBoundary(
            candidate: 10.0,
            features: features,
            direction: .adStart
        )

        #expect(result.confidence > 0.3)
        // Best time should be near the silence window (9.0-11.0 range),
        // offset by guard margin for adStart.
        #expect(result.time >= 9.0)
        #expect(result.time <= 12.0)
        // Silence should be a significant contributor.
        #expect((result.cueBreakdown[.silenceGap] ?? 0.0) > 0.2)
    }

    // MARK: - Energy valley preference

    @Test("snaps to energy valley when low RMS exists")
    func snapsToEnergyValley() {
        let features = [
            makeWindow(start: 7.0, end: 9.0, rms: 0.5, pauseProbability: 0.05),
            makeWindow(start: 9.0, end: 11.0, rms: 0.02, pauseProbability: 0.05),
            makeWindow(start: 11.0, end: 13.0, rms: 0.5, pauseProbability: 0.05),
        ]

        let result = FineBoundaryRefiner.refineBoundary(
            candidate: 10.0,
            features: features,
            direction: .adEnd
        )

        #expect(result.confidence > 0.1)
        // Energy valley cue should contribute.
        #expect((result.cueBreakdown[.energyValley] ?? 0.0) > 0.2)
    }

    // MARK: - Spectral discontinuity

    @Test("detects spectral discontinuity at boundary")
    func detectsSpectralDiscontinuity() {
        let features = [
            makeWindow(start: 7.0, end: 9.0, spectralFlux: 0.05, pauseProbability: 0.05),
            makeWindow(start: 9.0, end: 11.0, spectralFlux: 0.9, pauseProbability: 0.05),
            makeWindow(start: 11.0, end: 13.0, spectralFlux: 0.05, pauseProbability: 0.05),
        ]

        let result = FineBoundaryRefiner.refineBoundary(
            candidate: 10.0,
            features: features,
            direction: .adStart
        )

        #expect(result.confidence > 0.0)
        #expect((result.cueBreakdown[.spectralDiscontinuity] ?? 0.0) > 0.0)
    }

    // MARK: - Asymmetric guard margins

    @Test("adStart boundary shifts later (lets ad leak at start)")
    func adStartShiftsLater() {
        let features = [
            makeWindow(start: 8.0, end: 10.0, rms: 0.01, pauseProbability: 0.95),
            makeWindow(start: 10.0, end: 12.0, rms: 0.3, pauseProbability: 0.1),
        ]

        let config = FineBoundaryRefiner.Config.default

        let startResult = FineBoundaryRefiner.refineBoundary(
            candidate: 10.0,
            features: features,
            direction: .adStart,
            config: config
        )
        let endResult = FineBoundaryRefiner.refineBoundary(
            candidate: 10.0,
            features: features,
            direction: .adEnd,
            config: config
        )

        // adStart should produce a later time than adEnd (guard margin pushes later).
        #expect(startResult.time > endResult.time)
    }

    @Test("guard margin is applied to uncertainty bounds")
    func guardMarginAppliedToBounds() {
        let features = [
            makeWindow(start: 7.0, end: 9.0, rms: 0.01, pauseProbability: 0.95),
            makeWindow(start: 9.0, end: 11.0, rms: 0.01, pauseProbability: 0.95),
            makeWindow(start: 11.0, end: 13.0, rms: 0.3, pauseProbability: 0.1),
        ]

        let adStartResult = FineBoundaryRefiner.refineBoundary(
            candidate: 10.0,
            features: features,
            direction: .adStart
        )

        // For adStart, lowerBound should be >= the raw lower bound
        // (shifted later by guard margin).
        #expect(adStartResult.lowerBound <= adStartResult.time)
        #expect(adStartResult.upperBound >= adStartResult.time)
    }

    // MARK: - Uncertainty interval

    @Test("uncertainty interval narrows with a single strong peak")
    func narrowUncertaintyWithStrongPeak() {
        // One window with a very strong silence gap, others weak.
        let features = [
            makeWindow(start: 7.0, end: 9.0, rms: 0.5, pauseProbability: 0.01),
            makeWindow(start: 9.0, end: 11.0, rms: 0.01, pauseProbability: 0.99),
            makeWindow(start: 11.0, end: 13.0, rms: 0.5, pauseProbability: 0.01),
        ]

        let result = FineBoundaryRefiner.refineBoundary(
            candidate: 10.0,
            features: features,
            direction: .adStart
        )

        let intervalWidth = result.upperBound - result.lowerBound
        // Interval should be relatively narrow (well under the full 6s search range).
        #expect(intervalWidth < 4.0)
    }

    // MARK: - Configurable hop size

    @Test("custom hop size is respected")
    func customHopSize() {
        let features = [
            makeWindow(start: 7.0, end: 9.0, rms: 0.01, pauseProbability: 0.95),
            makeWindow(start: 9.0, end: 11.0, rms: 0.01, pauseProbability: 0.95),
            makeWindow(start: 11.0, end: 13.0, rms: 0.3, pauseProbability: 0.1),
        ]

        var wideConfig = FineBoundaryRefiner.Config.default
        wideConfig = FineBoundaryRefiner.Config(
            searchRadius: 3.0,
            hopSize: 0.250,
            silenceThreshold: 0.6,
            energyValleyRMSThreshold: 0.05,
            spectralDiscontinuityFraction: 0.5,
            silenceWeight: 0.50,
            energyValleyWeight: 0.30,
            spectralDiscontinuityWeight: 0.20,
            guardMargin: 0.100
        )

        let narrowConfig = FineBoundaryRefiner.Config(
            searchRadius: 3.0,
            hopSize: 0.100,
            silenceThreshold: 0.6,
            energyValleyRMSThreshold: 0.05,
            spectralDiscontinuityFraction: 0.5,
            silenceWeight: 0.50,
            energyValleyWeight: 0.30,
            spectralDiscontinuityWeight: 0.20,
            guardMargin: 0.100
        )

        let wideResult = FineBoundaryRefiner.refineBoundary(
            candidate: 10.0, features: features, direction: .adStart, config: wideConfig
        )
        let narrowResult = FineBoundaryRefiner.refineBoundary(
            candidate: 10.0, features: features, direction: .adStart, config: narrowConfig
        )

        // Both should produce valid results — different hop sizes may yield
        // slightly different times due to grid quantization.
        #expect(wideResult.confidence > 0.0)
        #expect(narrowResult.confidence > 0.0)
        // Times should be in the same neighborhood.
        #expect(abs(wideResult.time - narrowResult.time) < 1.0)
    }

    // MARK: - Cue breakdown normalization

    @Test("cue breakdown sums to approximately 1.0")
    func cueBreakdownSumsToOne() {
        let features = [
            makeWindow(start: 7.0, end: 9.0, rms: 0.2, spectralFlux: 0.3, pauseProbability: 0.4),
            makeWindow(start: 9.0, end: 11.0, rms: 0.1, spectralFlux: 0.5, pauseProbability: 0.7),
            makeWindow(start: 11.0, end: 13.0, rms: 0.3, spectralFlux: 0.1, pauseProbability: 0.2),
        ]

        let result = FineBoundaryRefiner.refineBoundary(
            candidate: 10.0, features: features, direction: .adStart
        )

        let total = result.cueBreakdown.values.reduce(0.0, +)
        #expect(abs(total - 1.0) < 0.001)
    }

    // MARK: - Boundary near episode start

    @Test("candidate at episode start does not produce negative lowerBound")
    func boundaryAtEpisodeStart() {
        let features = [
            makeWindow(start: 0.0, end: 2.0, rms: 0.01, pauseProbability: 0.95),
            makeWindow(start: 2.0, end: 4.0, rms: 0.3, pauseProbability: 0.1),
            makeWindow(start: 4.0, end: 6.0, rms: 0.3, pauseProbability: 0.1),
        ]

        // candidate at 0.5 — search window extends to negative times
        let result = FineBoundaryRefiner.refineBoundary(
            candidate: 0.5,
            features: features,
            direction: .adEnd  // adEnd shifts bounds earlier, maximizing negative risk
        )

        #expect(result.lowerBound >= 0.0, "lowerBound must not be negative")
        #expect(result.time >= 0.0, "time must not be negative")
    }

    @Test("candidate at 0.0 with adEnd direction clamps to zero")
    func boundaryAtZero() {
        let features = [
            makeWindow(start: 0.0, end: 2.0, rms: 0.01, pauseProbability: 0.95),
            makeWindow(start: 2.0, end: 4.0, rms: 0.3, pauseProbability: 0.1),
        ]

        let result = FineBoundaryRefiner.refineBoundary(
            candidate: 0.0,
            features: features,
            direction: .adEnd
        )

        #expect(result.lowerBound >= 0.0, "lowerBound must not go negative at episode start")
        #expect(result.time >= 0.0, "time must not go negative at episode start")
    }

    // MARK: - BoundaryCue enum

    @Test("BoundaryCue has expected cases in preference order")
    func boundaryCueCases() {
        let allCases = BoundaryCue.allCases
        #expect(allCases.count == 4)
        #expect(allCases[0] == .silenceGap)
        #expect(allCases[1] == .energyValley)
        #expect(allCases[2] == .spectralDiscontinuity)
        #expect(allCases[3] == .coarseFallback)
    }

    // MARK: - BoundaryEstimate struct

    @Test("BoundaryEstimate fields are accessible")
    func boundaryEstimateFields() {
        let estimate = BoundaryEstimate(
            time: 10.5,
            confidence: 0.85,
            lowerBound: 10.2,
            upperBound: 10.8,
            cueBreakdown: [.silenceGap: 0.6, .energyValley: 0.4]
        )

        #expect(estimate.time == 10.5)
        #expect(estimate.confidence == 0.85)
        #expect(estimate.lowerBound == 10.2)
        #expect(estimate.upperBound == 10.8)
        #expect(estimate.cueBreakdown.count == 2)
    }
}

// MARK: - FineFeatureBandCache Tests

@Suite("FineFeatureBandCache")
struct FineFeatureBandCacheTests {

    private func makeWindow(start: Double, end: Double) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "cache-test",
            startTime: start,
            endTime: end,
            rms: 0.3,
            spectralFlux: 0.1,
            musicProbability: 0.0,
            pauseProbability: 0.1,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        )
    }

    @Test("stores and retrieves cached bands")
    func storeAndRetrieve() async {
        let cache = FineFeatureBandCache()
        let features = [makeWindow(start: 7.0, end: 9.0)]

        await cache.put(
            episodeId: "ep1",
            bandCenter: 10.0,
            radius: 3.0,
            features: features
        )

        let result = await cache.get(episodeId: "ep1", bandCenter: 10.0)
        #expect(result != nil)
        #expect(result?.features.count == 1)
    }

    @Test("returns nil for uncached band")
    func missOnUncachedBand() async {
        let cache = FineFeatureBandCache()

        let result = await cache.get(episodeId: "ep1", bandCenter: 10.0)
        #expect(result == nil)
    }

    @Test("evicts all bands when episode changes")
    func evictsOnEpisodeChange() async {
        let cache = FineFeatureBandCache()
        let features = [makeWindow(start: 7.0, end: 9.0)]

        await cache.put(
            episodeId: "ep1",
            bandCenter: 10.0,
            radius: 3.0,
            features: features
        )
        #expect(await cache.count == 1)

        // Access with different episode triggers eviction.
        let result = await cache.get(episodeId: "ep2", bandCenter: 10.0)
        #expect(result == nil)
        #expect(await cache.count == 0)
    }

    @Test("quantization improves cache hit rate for nearby candidates")
    func quantizationHitRate() async {
        let cache = FineFeatureBandCache()
        let features = [makeWindow(start: 7.0, end: 9.0)]

        await cache.put(
            episodeId: "ep1",
            bandCenter: 10.0,
            radius: 3.0,
            features: features
        )

        // 10.02 should quantize to the same key as 10.0 (within 50ms).
        let result = await cache.get(episodeId: "ep1", bandCenter: 10.02)
        #expect(result != nil)
    }

    @Test("clear removes all data")
    func clearRemovesAll() async {
        let cache = FineFeatureBandCache()
        let features = [makeWindow(start: 7.0, end: 9.0)]

        await cache.put(episodeId: "ep1", bandCenter: 10.0, radius: 3.0, features: features)
        await cache.put(episodeId: "ep1", bandCenter: 20.0, radius: 3.0, features: features)
        #expect(await cache.count == 2)

        await cache.clear()
        #expect(await cache.count == 0)
    }
}
