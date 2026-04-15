// AsymmetricSnapScorer.swift
// ef2.3.8: Signed error penalty + dynamic snap radius for boundary refinement.
//
// SHADOW MODE — not wired into live snap pipeline. Provides:
//   1. Asymmetric scoring that penalizes clipping editorial (1.5×) more than
//      leaking ad audio (1.0× baseline).
//   2. Dynamic snap radius driven by bracket score and cue quality.
//   3. SignedBoundaryError tracking for offline replay harness analysis.
//
// Dependencies (on main):
//   - BracketDetector.swift (ef2.3.6) — BracketEvidence, coarseScore
//   - FineBoundaryRefiner.swift (ef2.3.7) — BoundaryCue, BoundaryDirection, BoundaryEstimate
//   - BoundaryPriorStore.swift (ef2.3.5) — BoundaryPriorDistribution.snapRadiusGuidance

import Foundation

// MARK: - Forward-compatible types for ef2.3.5/6/7 dependencies

/// Direction of an ad-boundary edge. Will be provided by FineBoundaryRefiner (ef2.3.7).
enum BoundaryDirection: String, Sendable, Equatable {
    case start
    case end
}

/// Cue type used for boundary refinement. Will be provided by FineBoundaryRefiner (ef2.3.7).
enum BoundaryCue: String, Sendable, Hashable {
    case bracket        // ef2.3.6 bracket detection
    case silenceGap     // VAD silence
    case spectral       // spectral change point
    case musicBed       // music bed transition
    case lexicalDensity // lexical density delta
}

// MARK: - SignedBoundaryError (replay harness tracking)

/// Offline error record for replay harness analysis.
/// Tracks signed error per boundary to measure asymmetric penalty impact.
struct SignedBoundaryError: Sendable, Equatable {
    let spanId: String
    let direction: BoundaryDirection
    /// (snapTarget - trueTime): positive = snapped too late, negative = snapped too early.
    let signedErrorSeconds: Double
    /// The penalty multiplier applied: 1.0 (baseline) or 1.5 (editorial-clipping).
    let penaltyMultiplier: Double

    /// Weighted error magnitude used for aggregate scoring.
    var penalizedError: Double {
        abs(signedErrorSeconds) * penaltyMultiplier
    }
}

// MARK: - AsymmetricSnapScorer

/// Value type providing asymmetric snap scoring and dynamic radius computation.
/// SHADOW MODE — not wired into live pipeline.
enum AsymmetricSnapScorer {

    // MARK: - Penalty constants

    /// Penalty for clipping editorial content (start too early / end too late).
    static let editorialClipPenalty: Double = 1.5
    /// Baseline penalty for leaking ad audio (start too late / end too early).
    static let adLeakPenalty: Double = 1.0

    // MARK: - Radius tier constants

    /// Strong cues: bracket > 0.7, silence gap present.
    static let strongRadiusRange: ClosedRange<Double> = 3.0...6.0
    /// Moderate cues: bracket 0.4–0.7, spectral present.
    static let moderateRadiusRange: ClosedRange<Double> = 6.0...8.0
    /// Weak cues: no bracket, FM says coarse.
    static let weakRadiusMax: Double = 10.0

    // MARK: - Asymmetric scoring

    /// Score a snap candidate with directional error penalty.
    ///
    /// - Parameters:
    ///   - candidateTime: The candidate snap position (seconds).
    ///   - snapTarget: The target position we snapped to (seconds).
    ///   - direction: Whether this is a `.start` or `.end` boundary.
    ///   - signedError: `(snapTarget - trueTime)`: positive = snapped too late, negative = too early.
    /// - Returns: Penalized error magnitude (lower is better).
    static func score(
        candidateTime: Double,
        snapTarget: Double,
        direction: BoundaryDirection,
        signedError: Double
    ) -> Double {
        let multiplier = penaltyMultiplier(direction: direction, signedError: signedError)
        return abs(signedError) * multiplier
    }

    /// Determine the penalty multiplier for a given direction and signed error.
    ///
    /// For start boundaries: too-early (negative error) clips editorial → 1.5×.
    /// For end boundaries: too-late (positive error) clips editorial → 1.5×.
    /// All other cases: ad leak → 1.0× baseline.
    static func penaltyMultiplier(
        direction: BoundaryDirection,
        signedError: Double
    ) -> Double {
        switch direction {
        case .start:
            // Negative error = snapped too early = clips editorial before ad starts
            return signedError < 0 ? editorialClipPenalty : adLeakPenalty
        case .end:
            // Positive error = snapped too late = clips editorial after ad ends
            return signedError > 0 ? editorialClipPenalty : adLeakPenalty
        }
    }

    // MARK: - Dynamic snap radius

    /// Compute snap radius from local signal quality and cue-conditional prior spread.
    ///
    /// - Parameters:
    ///   - bracketScore: Bracket detection coarse score (0.0–1.0), nil if no bracket evidence.
    ///   - boundaryCues: Map of active cue types to their confidence values.
    ///   - priorSpread: Cue-conditional prior spread in seconds (from BoundaryPriorStore), nil if unavailable.
    /// - Returns: Dynamic snap radius in seconds.
    static func dynamicSnapRadius(
        bracketScore: Double?,
        boundaryCues: [BoundaryCue: Double],
        priorSpread: Double?
    ) -> Double {
        let tier = signalTier(bracketScore: bracketScore, boundaryCues: boundaryCues)
        let baseRadius = tierBaseRadius(tier)

        // If prior spread is available, blend it with the tier base.
        // Clamp final radius to [strongRadiusRange.lowerBound, weakRadiusMax].
        if let spread = priorSpread {
            let blended = 0.6 * baseRadius + 0.4 * spread
            return clampRadius(blended)
        }

        return baseRadius
    }

    // MARK: - Signal tier classification

    enum SignalTier: Sendable, Equatable {
        case strong
        case moderate
        case weak
    }

    /// Classify local signal quality into a tier.
    static func signalTier(
        bracketScore: Double?,
        boundaryCues: [BoundaryCue: Double]
    ) -> SignalTier {
        let hasSilenceGap = (boundaryCues[.silenceGap] ?? 0) > 0.3

        // Strong: bracket > 0.7 OR silence gap with decent confidence
        if let score = bracketScore, score > 0.7 {
            return .strong
        }
        if hasSilenceGap {
            return .strong
        }

        // Moderate: bracket 0.4–0.7 OR spectral cue present
        if let score = bracketScore, score >= 0.4 {
            return .moderate
        }
        let hasSpectral = (boundaryCues[.spectral] ?? 0) > 0.3
        if hasSpectral {
            return .moderate
        }

        // Weak: everything else
        return .weak
    }

    // MARK: - Internal helpers

    private static func tierBaseRadius(_ tier: SignalTier) -> Double {
        switch tier {
        case .strong:
            return strongRadiusRange.upperBound   // 6s — tight radius
        case .moderate:
            return moderateRadiusRange.upperBound  // 8s
        case .weak:
            return weakRadiusMax                   // 10s — wide radius
        }
    }

    private static func clampRadius(_ radius: Double) -> Double {
        min(max(radius, strongRadiusRange.lowerBound), weakRadiusMax)
    }
}

// MARK: - SignedBoundaryError collection for replay harness

extension AsymmetricSnapScorer {

    /// Build a signed boundary error record for replay harness tracking.
    ///
    /// - Parameters:
    ///   - spanId: The decoded span ID.
    ///   - direction: Boundary direction (.start or .end).
    ///   - snapTarget: Where the boundary was snapped to.
    ///   - trueTime: Ground-truth boundary time from the corpus.
    /// - Returns: A `SignedBoundaryError` with the appropriate penalty multiplier.
    static func buildError(
        spanId: String,
        direction: BoundaryDirection,
        snapTarget: Double,
        trueTime: Double
    ) -> SignedBoundaryError {
        let signedError = snapTarget - trueTime
        let multiplier = penaltyMultiplier(direction: direction, signedError: signedError)
        return SignedBoundaryError(
            spanId: spanId,
            direction: direction,
            signedErrorSeconds: signedError,
            penaltyMultiplier: multiplier
        )
    }

    /// Aggregate penalized error across a collection of boundary errors.
    /// Returns mean penalized error, or 0 if empty.
    static func aggregatePenalizedError(_ errors: [SignedBoundaryError]) -> Double {
        guard !errors.isEmpty else { return 0 }
        let total = errors.reduce(0.0) { $0 + $1.penalizedError }
        return total / Double(errors.count)
    }
}
