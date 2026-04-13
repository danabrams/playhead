// BoundaryRefiner.swift
// Shared boundary refinement helper for classifier-driven ad windows.

import Foundation

enum BoundaryRefiner {
    private static let maxBoundaryAdjust: Double = 3.0
    private static let resolver = TimeBoundaryResolver()
    private static let resolverConfig = BoundarySnappingConfig(
        startWeights: StartBoundaryCueWeights(
            pauseVAD: 0.90,
            speakerChangeProxy: 0.0,
            musicBedChange: 0.0,
            spectralChange: 0.10,
            lexicalDensityDelta: 0.0
        ),
        endWeights: EndBoundaryCueWeights(
            pauseVAD: 0.90,
            speakerChangeProxy: 0.0,
            musicBedChange: 0.0,
            spectralChange: 0.10,
            explicitReturnMarker: 0.0
        ),
        maxSnapDistanceByAnchorType: [.fmPositive: BoundarySnapDistance(start: maxBoundaryAdjust, end: maxBoundaryAdjust)],
        lambda: 0.05,
        minBoundaryScore: 0.50,
        minImprovementOverOriginal: -0.10
    )

    static func computeAdjustments(
        windows: [FeatureWindow],
        candidateStart: Double,
        candidateEnd: Double
    ) -> (startAdjust: Double, endAdjust: Double) {
        guard windows.count >= 3 else { return (0.0, 0.0) }

        let startBoundary = resolveBoundary(
            candidateTime: candidateStart,
            boundaryType: .start,
            windows: windows
        )
        let endBoundary = resolveBoundary(
            candidateTime: candidateEnd,
            boundaryType: .end,
            windows: windows
        )

        return (
            clamp(adjustment: startBoundary - candidateStart),
            clamp(adjustment: endBoundary - candidateEnd)
        )
    }

    private static func clamp(adjustment: Double) -> Double {
        max(-maxBoundaryAdjust, min(adjustment, maxBoundaryAdjust))
    }

    private static func resolveBoundary(
        candidateTime: Double,
        boundaryType: BoundaryType,
        windows: [FeatureWindow]
    ) -> Double {
        resolver.snap(
            candidateTime: candidateTime,
            boundaryType: boundaryType,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: [],
            config: resolverConfig
        )
    }
}
