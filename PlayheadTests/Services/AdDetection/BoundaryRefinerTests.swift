// BoundaryRefinerTests.swift
// Regression rails for the shared boundary refinement helper that now delegates
// snapping decisions to TimeBoundaryResolver.

import Testing

@testable import Playhead

@Suite("BoundaryRefiner")
struct BoundaryRefinerTests {

    @Test("fewer than three windows leave both adjustments at zero")
    func fewerThanThreeWindowsDoNotAdjust() {
        let windows = [
            makeWindow(start: 7, end: 8, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 8, end: 9, pause: 0.05, spectralFlux: 0.05),
        ]

        let actual = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 8.5,
            candidateEnd: 8.5
        )

        #expect(actual.startAdjust == 0.0)
        #expect(actual.endAdjust == 0.0)
    }

    @Test("resolver-backed refinement snaps both boundaries within the three-second radius")
    func snapsBothBoundaries() {
        let windows = [
            makeWindow(start: 7, end: 8, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 8, end: 9, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 9, end: 10, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 19, end: 20, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 20, end: 21, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 21, end: 22, pause: 0.05, spectralFlux: 0.05),
        ]

        let actual = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 9.5,
            candidateEnd: 20.5
        )

        #expect(actual.startAdjust == -2.5)
        #expect(actual.endAdjust == 0.5)
    }

    @Test("weak or out-of-range windows keep both adjustments at zero")
    func weakOrOutOfRangeWindowsDoNotAdjust() {
        let windows = [
            makeWindow(start: 7, end: 8, pause: 0.10, spectralFlux: 0.05),
            makeWindow(start: 8, end: 9, pause: 0.12, spectralFlux: 0.05),
            makeWindow(start: 9, end: 10, pause: 0.10, spectralFlux: 0.04),
            makeWindow(start: 24, end: 25, pause: 0.98, spectralFlux: 0.99),
            makeWindow(start: 25, end: 26, pause: 0.97, spectralFlux: 0.98),
            makeWindow(start: 26, end: 27, pause: 0.96, spectralFlux: 0.97),
        ]

        let actual = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 9.5,
            candidateEnd: 20.5
        )

        #expect(actual.startAdjust == 0.0)
        #expect(actual.endAdjust == 0.0)
    }

    @Test("window ordering no longer affects resolver-backed adjustments")
    func orderingDoesNotMatter() {
        let sorted = [
            makeWindow(start: 7, end: 8, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 8, end: 9, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 9, end: 10, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 19, end: 20, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 20, end: 21, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 21, end: 22, pause: 0.05, spectralFlux: 0.05),
        ]
        let unsorted = [
            sorted[4],
            sorted[1],
            sorted[5],
            sorted[0],
            sorted[3],
            sorted[2],
        ]

        let sortedAdjustments = BoundaryRefiner.computeAdjustments(
            windows: sorted,
            candidateStart: 9.5,
            candidateEnd: 20.5
        )
        let unsortedAdjustments = BoundaryRefiner.computeAdjustments(
            windows: unsorted,
            candidateStart: 9.5,
            candidateEnd: 20.5
        )

        #expect(unsortedAdjustments.startAdjust == sortedAdjustments.startAdjust)
        #expect(unsortedAdjustments.endAdjust == sortedAdjustments.endAdjust)
    }

    @Test("resolver-backed refinement prefers local maxima over a closer interior shoulder")
    func prefersLocalMaximaOverCloserInteriorShoulder() {
        let windows = [
            makeWindow(start: 7, end: 8, pause: 0.92, spectralFlux: 0.80),
            makeWindow(start: 8, end: 9, pause: 0.62, spectralFlux: 0.05),
            makeWindow(start: 9, end: 10, pause: 0.97, spectralFlux: 0.95),
        ]

        let actual = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 8.4,
            candidateEnd: 30.0
        )

        #expect(abs(actual.startAdjust - 0.6) < 0.000_001)
        #expect(actual.endAdjust == 0.0)
    }

    @Test("snap distance keeps adjustments within plus or minus three seconds")
    func snapDistanceCapsAdjustmentMagnitude() {
        let windows = [
            makeWindow(start: 7, end: 8, pause: 0.98, spectralFlux: 0.98),
            makeWindow(start: 10, end: 11, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 12, end: 13, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 27, end: 28, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 29, end: 30, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 32, end: 33, pause: 0.98, spectralFlux: 0.98),
        ]

        let actual = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 10.0,
            candidateEnd: 30.0
        )

        #expect(actual.startAdjust == -3.0)
        #expect(actual.endAdjust == 3.0)
    }

    private func makeWindow(
        start: Double,
        end: Double,
        pause: Double,
        spectralFlux: Double,
        musicProbability: Double = 0.0,
        speakerChangeProxyScore: Double = 0.0,
        musicBedChangeScore: Double = 0.0
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-boundary",
            startTime: start,
            endTime: end,
            rms: 0.05,
            spectralFlux: spectralFlux,
            musicProbability: musicProbability,
            speakerChangeProxyScore: speakerChangeProxyScore,
            musicBedChangeScore: musicBedChangeScore,
            pauseProbability: pause,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        )
    }
}
