// BoundaryRefiner.swift
// Shared boundary-snapping helper for classifier-driven ad windows.

import Foundation

enum BoundaryRefiner {
    private static let maxBoundaryAdjust: Double = 3.0
    private static let minimumTransitionStrength: Double = 0.05

    static func computeAdjustments(
        windows: [FeatureWindow],
        candidateStart: Double,
        candidateEnd: Double
    ) -> (startAdjust: Double, endAdjust: Double) {
        guard windows.count >= 3 else { return (0.0, 0.0) }

        let startAdj = findNearestTransition(windows: windows, anchor: candidateStart)
        let endAdj = findNearestTransition(windows: windows, anchor: candidateEnd)

        return (
            clamp(adjustment: startAdj),
            clamp(adjustment: endAdj)
        )
    }

    private static func clamp(adjustment: Double) -> Double {
        max(-maxBoundaryAdjust, min(adjustment, maxBoundaryAdjust))
    }

    private static func findNearestTransition(
        windows: [FeatureWindow],
        anchor: Double
    ) -> Double {
        // Search all windows within maxBoundaryAdjust of the anchor. Acoustic
        // transitions (RMS drops + spectral flux peaks) are direction-neutral —
        // the best boundary snap is the strongest transition regardless of whether
        // we are looking for a start or end edge.
        let nearbyWindows = windows.filter {
            abs(($0.startTime + $0.endTime) / 2.0 - anchor) <= maxBoundaryAdjust
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

        guard bestDelta > minimumTransitionStrength else { return 0.0 }
        return bestTime - anchor
    }
}
