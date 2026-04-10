// BoundaryRefinerTests.swift
// Regression rails for the shared boundary-snapping helper extracted from
// RuleBasedClassifier during Phase 6 prep.

import Testing

@testable import Playhead

@Suite("BoundaryRefiner")
struct BoundaryRefinerTests {

    @Test("matches the legacy classifier boundary adjustment behavior")
    func matchesLegacyClassifierBehavior() {
        let cases: [(name: String, windows: [FeatureWindow], start: Double, end: Double)] = [
            (
                name: "fewer than three windows returns zero adjustments",
                windows: [
                    makeWindow(start: 8, end: 9, rms: 0.10, spectralFlux: 0.15),
                    makeWindow(start: 9, end: 10, rms: 0.40, spectralFlux: 0.20)
                ],
                start: 9.5,
                end: 20.0
            ),
            (
                name: "meaningful acoustic transitions adjust both boundaries",
                windows: [
                    makeWindow(start: 7, end: 8, rms: 0.10, spectralFlux: 0.10),
                    makeWindow(start: 8, end: 9, rms: 0.75, spectralFlux: 0.90),
                    makeWindow(start: 9, end: 10, rms: 0.15, spectralFlux: 0.10),
                    makeWindow(start: 19, end: 20, rms: 0.12, spectralFlux: 0.15),
                    makeWindow(start: 20, end: 21, rms: 0.82, spectralFlux: 0.95),
                    makeWindow(start: 21, end: 22, rms: 0.20, spectralFlux: 0.12)
                ],
                start: 9.5,
                end: 20.5
            ),
            (
                name: "sub-threshold transitions are ignored",
                windows: [
                    makeWindow(start: 7, end: 8, rms: 0.10, spectralFlux: 0.01),
                    makeWindow(start: 8, end: 9, rms: 0.12, spectralFlux: 0.01),
                    makeWindow(start: 9, end: 10, rms: 0.11, spectralFlux: 0.01),
                    makeWindow(start: 19, end: 20, rms: 0.09, spectralFlux: 0.01),
                    makeWindow(start: 20, end: 21, rms: 0.11, spectralFlux: 0.01),
                    makeWindow(start: 21, end: 22, rms: 0.10, spectralFlux: 0.01)
                ],
                start: 9.5,
                end: 20.5
            ),
            (
                name: "unsorted windows preserve the legacy ordering-dependent behavior",
                windows: [
                    makeWindow(start: 20, end: 21, rms: 0.82, spectralFlux: 0.95),
                    makeWindow(start: 7, end: 8, rms: 0.10, spectralFlux: 0.10),
                    makeWindow(start: 21, end: 22, rms: 0.20, spectralFlux: 0.12),
                    makeWindow(start: 19, end: 20, rms: 0.12, spectralFlux: 0.15),
                    makeWindow(start: 9, end: 10, rms: 0.15, spectralFlux: 0.10),
                    makeWindow(start: 8, end: 9, rms: 0.75, spectralFlux: 0.90)
                ],
                start: 9.5,
                end: 20.5
            ),
            (
                name: "legacy clamp still caps extreme adjustments at plus or minus three seconds",
                windows: [
                    makeWindow(start: 5.8, end: 20.0, rms: 0.05, spectralFlux: 0.01),
                    makeWindow(start: 13.0, end: 13.2, rms: 0.95, spectralFlux: 0.95),
                    makeWindow(start: 13.2, end: 13.4, rms: 0.10, spectralFlux: 0.01),
                    makeWindow(start: 25.8, end: 40.0, rms: 0.05, spectralFlux: 0.01),
                    makeWindow(start: 33.0, end: 33.2, rms: 0.95, spectralFlux: 0.95),
                    makeWindow(start: 33.2, end: 33.4, rms: 0.10, spectralFlux: 0.01)
                ],
                start: 10.0,
                end: 30.0
            )
        ]

        for scenario in cases {
            let expected = legacyAdjustments(
                windows: scenario.windows,
                candidateStart: scenario.start,
                candidateEnd: scenario.end
            )
            let actual = BoundaryRefiner.computeAdjustments(
                windows: scenario.windows,
                candidateStart: scenario.start,
                candidateEnd: scenario.end
            )

            #expect(actual.startAdjust == expected.startAdjust, "\(scenario.name) (start)")
            #expect(actual.endAdjust == expected.endAdjust, "\(scenario.name) (end)")
        }
    }

    private func makeWindow(
        start: Double,
        end: Double,
        rms: Double,
        spectralFlux: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-boundary",
            startTime: start,
            endTime: end,
            rms: rms,
            spectralFlux: spectralFlux,
            musicProbability: 0,
            pauseProbability: 0,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        )
    }

    private func legacyAdjustments(
        windows: [FeatureWindow],
        candidateStart: Double,
        candidateEnd: Double
    ) -> (startAdjust: Double, endAdjust: Double) {
        guard windows.count >= 3 else { return (0.0, 0.0) }

        let startAdj = legacyNearestTransition(windows: windows, anchor: candidateStart)
        let endAdj = legacyNearestTransition(windows: windows, anchor: candidateEnd)

        return (
            max(-3.0, min(startAdj, 3.0)),
            max(-3.0, min(endAdj, 3.0))
        )
    }

    private func legacyNearestTransition(
        windows: [FeatureWindow],
        anchor: Double
    ) -> Double {
        let nearbyWindows = windows.filter {
            abs(($0.startTime + $0.endTime) / 2.0 - anchor) <= 3.0
        }
        guard nearbyWindows.count >= 2 else { return 0.0 }

        var bestDelta = 0.0
        var bestTime = anchor

        for index in 0 ..< nearbyWindows.count - 1 {
            let first = nearbyWindows[index]
            let second = nearbyWindows[index + 1]
            let rmsDelta = abs(second.rms - first.rms)
            let fluxBoost = max(first.spectralFlux, second.spectralFlux) * 0.5
            let combined = rmsDelta + fluxBoost

            if combined > bestDelta {
                bestDelta = combined
                bestTime = (first.endTime + second.startTime) / 2.0
            }
        }

        guard bestDelta > 0.05 else { return 0.0 }
        return bestTime - anchor
    }
}
