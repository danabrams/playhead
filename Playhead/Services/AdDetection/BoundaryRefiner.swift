// BoundaryRefiner.swift
// Shared boundary refinement helper for classifier-driven ad windows.
//
// playhead-kgby: Optionally accepts a `[TranscriptBoundaryHit]` array.
// When the caller passes non-empty transcript hits, the resolver runs
// with a config that allocates a small weight to the transcript cue and
// reduces `pauseVAD` by an equal amount so the per-cue weights still
// sum to 1.0. When transcript hits are absent (or the caller does not
// pass them — the default), the legacy weight schedule is used,
// preserving bit-identical behaviour for every existing call site.

import Foundation
import OSLog

enum BoundaryRefiner {
    static let maxBoundaryAdjust: Double = 3.0
    private static let resolver = TimeBoundaryResolver()

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "BoundaryRefiner"
    )

    /// Legacy resolver config: 90% pauseVAD, 10% spectralChange. Used
    /// whenever `transcriptHits` is empty so we never regress existing
    /// callers that don't pass transcript data.
    private static let legacyResolverConfig = BoundarySnappingConfig(
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

    /// playhead-kgby: When transcript hits are present, allocate a small
    /// (`transcriptWeight`) slice to the transcript-boundary cue and
    /// reduce `pauseVAD` by the same amount so weights still total 1.0.
    /// 0.20 is a deliberately modest value: enough to bias the snap
    /// toward a sentence break when one is in radius, but not enough to
    /// override a strong pause cue (0.70). The minBoundaryScore floor
    /// stays at 0.50 — i.e. transcript-only "near a sentence end" cannot
    /// produce a snap by itself; an acoustic cue still has to corroborate.
    private static let transcriptResolverConfig: BoundarySnappingConfig = {
        let transcriptWeight = 0.20
        let pauseWeight = 0.90 - transcriptWeight  // 0.70
        return BoundarySnappingConfig(
            startWeights: StartBoundaryCueWeights(
                pauseVAD: pauseWeight,
                speakerChangeProxy: 0.0,
                musicBedChange: 0.0,
                spectralChange: 0.10,
                lexicalDensityDelta: 0.0,
                transcriptBoundary: transcriptWeight
            ),
            endWeights: EndBoundaryCueWeights(
                pauseVAD: pauseWeight,
                speakerChangeProxy: 0.0,
                musicBedChange: 0.0,
                spectralChange: 0.10,
                explicitReturnMarker: 0.0,
                transcriptBoundary: transcriptWeight
            ),
            maxSnapDistanceByAnchorType: [.fmPositive: BoundarySnapDistance(start: maxBoundaryAdjust, end: maxBoundaryAdjust)],
            lambda: 0.05,
            minBoundaryScore: 0.50,
            minImprovementOverOriginal: -0.10
        )
    }()

    /// Legacy entry point — preserved with default-empty transcript hits
    /// so every existing caller (and every existing test) compiles and
    /// behaves identically.
    static func computeAdjustments(
        windows: [FeatureWindow],
        candidateStart: Double,
        candidateEnd: Double
    ) -> (startAdjust: Double, endAdjust: Double) {
        computeAdjustments(
            windows: windows,
            candidateStart: candidateStart,
            candidateEnd: candidateEnd,
            transcriptHits: []
        )
    }

    /// playhead-kgby: New entry point with transcript hits. When
    /// `transcriptHits` is empty the resolver uses the legacy config —
    /// numeric output is byte-identical to the original behaviour.
    /// When non-empty, the resolver runs with the transcript-aware
    /// config (transcriptBoundary: 0.20) so a sentence terminal near
    /// the candidate boundary contributes to the snap score.
    static func computeAdjustments(
        windows: [FeatureWindow],
        candidateStart: Double,
        candidateEnd: Double,
        transcriptHits: [TranscriptBoundaryHit]
    ) -> (startAdjust: Double, endAdjust: Double) {
        guard windows.count >= 3 else { return (0.0, 0.0) }

        let activeConfig = transcriptHits.isEmpty
            ? legacyResolverConfig
            : transcriptResolverConfig

        let startBoundary = resolveBoundary(
            candidateTime: candidateStart,
            boundaryType: .start,
            windows: windows,
            transcriptHits: transcriptHits,
            config: activeConfig
        )
        let endBoundary = resolveBoundary(
            candidateTime: candidateEnd,
            boundaryType: .end,
            windows: windows,
            transcriptHits: transcriptHits,
            config: activeConfig
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
        windows: [FeatureWindow],
        transcriptHits: [TranscriptBoundaryHit],
        config: BoundarySnappingConfig
    ) -> Double {
        let resolved = resolver.snap(
            candidateTime: candidateTime,
            boundaryType: boundaryType,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: [],
            transcriptHits: transcriptHits,
            config: config
        )

        // playhead-vn7n.1: diagnostic — log every boundary resolution so we
        // can attribute end-side overshoot to BoundaryRefiner. Both start
        // and end boundaries are logged; reviewers can grep on
        // boundaryType=end for the overshoot triage.
        let adjustment = resolved - candidateTime
        let typeTag: String
        switch boundaryType {
        case .start: typeTag = "start"
        case .end: typeTag = "end"
        }
        logger.info(
            "resolveBoundary: boundaryType=\(typeTag, privacy: .public) candidateTime=\(candidateTime, privacy: .public) resolvedTime=\(resolved, privacy: .public) adjustment=\(adjustment, privacy: .public)"
        )

        return resolved
    }
}
