// FineBoundaryRefiner.swift
// ef2.3.7: Local search ±3s around candidate boundary edges at 100-250ms hops.
// Produces high-precision boundary estimates with uncertainty intervals.
//
// playhead-arf8: graduated from SHADOW MODE. Invoked by
// `BracketAwareBoundaryRefiner.computeAdjustments` once the bracket
// detector clears its trust + coarse-score gates, to lock down each edge
// at silence/energy/spectral cues. Both edges must clear the configured
// fine-confidence floor or the refinement is rejected and the caller
// falls back to the legacy `BoundaryRefiner` path.
//
// Pure computation on value types — no actor needed.

import Foundation

// MARK: - BoundaryCue

/// Signal types that contribute to a fine boundary estimate, in snap preference order.
enum BoundaryCue: String, Sendable, CaseIterable, Hashable {
    /// Silence gap detected (highest priority — cleanest edit point).
    case silenceGap
    /// Local energy minimum (valley between loud segments).
    case energyValley
    /// Sharp spectral flux change (timbral discontinuity).
    case spectralDiscontinuity
    /// No acoustic cue found; falls back to coarse candidate position.
    case coarseFallback
}

// MARK: - BoundaryEstimate

/// High-precision boundary estimate with uncertainty interval and cue breakdown.
struct BoundaryEstimate: Sendable, Equatable {
    /// Best point estimate (seconds).
    let time: Double
    /// Overall confidence in this estimate (0.0-1.0).
    let confidence: Double
    /// Lower bound of uncertainty interval (seconds).
    let lowerBound: Double
    /// Upper bound of uncertainty interval (seconds).
    let upperBound: Double
    /// Which cues contributed and their normalized weight (0.0-1.0 each).
    let cueBreakdown: [BoundaryCue: Double]
}

// MARK: - BoundaryDirection

/// Whether this boundary is the start or end of an ad segment.
/// Drives asymmetric guard margin logic.
enum BoundaryDirection: Sendable {
    /// Start of ad: prefer later (let a fraction of ad leak at the beginning).
    case adStart
    /// End of ad: prefer earlier (let a fraction of ad leak at the end).
    case adEnd
}

// MARK: - FineBoundaryRefiner

/// Stateless refiner that performs a local search around a candidate boundary
/// at fine hop intervals, scoring each position against multiple acoustic cues.
enum FineBoundaryRefiner {

    // MARK: - Configuration

    struct Config: Sendable {
        /// Search radius around the candidate (seconds, each direction).
        let searchRadius: Double
        /// Hop size between evaluation positions (seconds).
        let hopSize: Double
        /// Pause probability threshold for silence gap detection.
        let silenceThreshold: Double
        /// RMS threshold below which a window contributes to energy valley score.
        let energyValleyRMSThreshold: Double
        /// Spectral flux threshold (relative to max in band) for discontinuity.
        let spectralDiscontinuityFraction: Double
        /// Weight for silence gap cue in composite score.
        let silenceWeight: Double
        /// Weight for energy valley cue in composite score.
        let energyValleyWeight: Double
        /// Weight for spectral discontinuity cue in composite score.
        let spectralDiscontinuityWeight: Double
        /// Asymmetric guard margin applied to uncertainty interval (seconds).
        /// For adStart: shifts bounds later. For adEnd: shifts bounds earlier.
        let guardMargin: Double

        static let `default` = Config(
            searchRadius: 3.0,
            hopSize: 0.150,
            silenceThreshold: 0.6,
            energyValleyRMSThreshold: 0.05,
            spectralDiscontinuityFraction: 0.5,
            silenceWeight: 0.50,
            energyValleyWeight: 0.30,
            spectralDiscontinuityWeight: 0.20,
            guardMargin: 0.100
        )
    }

    // MARK: - Public API

    /// Refine a candidate boundary using acoustic features in the surrounding window.
    ///
    /// - Parameters:
    ///   - candidate: Coarse boundary time (seconds).
    ///   - features: Feature windows covering at least ±searchRadius around the candidate.
    ///   - direction: Whether this is an ad start or end boundary (drives guard margins).
    ///   - config: Tuning parameters.
    /// - Returns: A high-precision boundary estimate with uncertainty interval.
    static func refineBoundary(
        candidate: Double,
        features: [FeatureWindow],
        direction: BoundaryDirection,
        config: Config = .default
    ) -> BoundaryEstimate {
        let searchStart = candidate - config.searchRadius
        let searchEnd = candidate + config.searchRadius

        // Filter features to the search band.
        let bandFeatures = features.filter { window in
            window.endTime > searchStart && window.startTime < searchEnd
        }

        // If no features in range, return coarse fallback.
        guard !bandFeatures.isEmpty else {
            return coarseFallback(candidate: candidate, config: config, direction: direction)
        }

        // Generate hop positions.
        let hopCount = Int((config.searchRadius * 2.0) / config.hopSize) + 1
        var hopScores: [(time: Double, score: Double, cues: [BoundaryCue: Double])] = []
        hopScores.reserveCapacity(hopCount)

        for i in 0..<hopCount {
            let t = searchStart + Double(i) * config.hopSize
            guard t <= searchEnd else { break }

            let cues = scoreCues(at: t, features: bandFeatures, config: config)
            let composite = compositeScore(cues: cues, config: config)
            hopScores.append((time: t, score: composite, cues: cues))
        }

        guard !hopScores.isEmpty else {
            return coarseFallback(candidate: candidate, config: config, direction: direction)
        }

        // Find best position.
        let best = hopScores.max(by: { $0.score < $1.score })!

        // Compute confidence: best score normalized. If all scores are zero,
        // confidence is zero.
        let maxScore = best.score
        let confidence = min(max(maxScore, 0.0), 1.0)

        // Compute uncertainty interval from score distribution.
        // Include all positions scoring above 50% of the best score.
        let threshold = maxScore * 0.5
        let highScoring = hopScores.filter { $0.score >= max(threshold, 1e-9) }

        let rawLower: Double
        let rawUpper: Double
        if highScoring.count > 1 {
            rawLower = highScoring.map(\.time).min()!
            rawUpper = highScoring.map(\.time).max()!
        } else {
            // Single point — uncertainty is one hop width.
            rawLower = best.time - config.hopSize
            rawUpper = best.time + config.hopSize
        }

        // Apply asymmetric guard margins.
        let (guardedLower, guardedUpper, guardedTime) = applyGuardMargins(
            bestTime: best.time,
            rawLower: rawLower,
            rawUpper: rawUpper,
            direction: direction,
            guardMargin: config.guardMargin
        )

        // Clamp to non-negative times (candidate near episode start).
        let clampedLower = max(0, guardedLower)
        let clampedUpper = max(0, guardedUpper)
        let clampedTime = max(0, guardedTime)

        // Build cue breakdown: normalize so weights sum to 1.0 (or all zero).
        let breakdown = normalizeCueBreakdown(best.cues)

        return BoundaryEstimate(
            time: clampedTime,
            confidence: confidence,
            lowerBound: clampedLower,
            upperBound: clampedUpper,
            cueBreakdown: breakdown
        )
    }

    // MARK: - Cue Scoring

    /// Score each boundary cue at a given time position against surrounding features.
    private static func scoreCues(
        at time: Double,
        features: [FeatureWindow],
        config: Config
    ) -> [BoundaryCue: Double] {
        var cues: [BoundaryCue: Double] = [:]

        // Find the feature window(s) overlapping this time.
        let overlapping = features.filter { $0.startTime <= time && $0.endTime >= time }

        // Silence gap: high pause probability near this time.
        let silenceScore = scoreSilenceGap(at: time, overlapping: overlapping, config: config)
        cues[.silenceGap] = silenceScore

        // Energy valley: low RMS near this time.
        let energyScore = scoreEnergyValley(at: time, overlapping: overlapping, config: config)
        cues[.energyValley] = energyScore

        // Spectral discontinuity: high spectral flux change near this time.
        let spectralScore = scoreSpectralDiscontinuity(
            at: time, features: features, config: config
        )
        cues[.spectralDiscontinuity] = spectralScore

        return cues
    }

    private static func scoreSilenceGap(
        at time: Double,
        overlapping: [FeatureWindow],
        config: Config
    ) -> Double {
        guard !overlapping.isEmpty else { return 0.0 }
        // Use the maximum pause probability among overlapping windows.
        let maxPause = overlapping.map(\.pauseProbability).max() ?? 0.0
        // Above threshold → 1.0, below → proportional ramp.
        if maxPause >= config.silenceThreshold {
            return 1.0
        }
        return maxPause / max(config.silenceThreshold, 1e-9)
    }

    private static func scoreEnergyValley(
        at time: Double,
        overlapping: [FeatureWindow],
        config: Config
    ) -> Double {
        guard !overlapping.isEmpty else { return 0.0 }
        let minRMS = overlapping.map(\.rms).min() ?? 1.0
        // Lower RMS → higher score. Full score at or below threshold.
        if minRMS <= config.energyValleyRMSThreshold {
            return 1.0
        }
        // Decay linearly up to 5x threshold.
        let ceiling = config.energyValleyRMSThreshold * 5.0
        if minRMS >= ceiling { return 0.0 }
        return 1.0 - (minRMS - config.energyValleyRMSThreshold) / (ceiling - config.energyValleyRMSThreshold)
    }

    private static func scoreSpectralDiscontinuity(
        at time: Double,
        features: [FeatureWindow],
        config: Config
    ) -> Double {
        // Find the two windows straddling this time point (before and after).
        let before = features
            .filter { $0.endTime <= time + 0.01 }
            .max(by: { $0.endTime < $1.endTime })
        let after = features
            .filter { $0.startTime >= time - 0.01 }
            .min(by: { $0.startTime < $1.startTime })

        guard let b = before, let a = after,
              b.startTime != a.startTime || b.endTime != a.endTime else {
            // Single window or no straddling pair — check flux at this point.
            let overlapping = features.filter { $0.startTime <= time && $0.endTime >= time }
            let maxFlux = overlapping.map(\.spectralFlux).max() ?? 0.0
            let bandMax = features.map(\.spectralFlux).max() ?? 1.0
            guard bandMax > 0 else { return 0.0 }
            return min(maxFlux / bandMax, 1.0)
        }

        // Spectral flux difference between adjacent windows.
        let fluxDiff = abs(a.spectralFlux - b.spectralFlux)
        let bandMax = features.map(\.spectralFlux).max() ?? 1.0
        guard bandMax > 0 else { return 0.0 }
        let normalized = fluxDiff / bandMax
        return min(normalized / max(config.spectralDiscontinuityFraction, 1e-9), 1.0)
    }

    // MARK: - Composite Scoring

    private static func compositeScore(
        cues: [BoundaryCue: Double],
        config: Config
    ) -> Double {
        let silence = (cues[.silenceGap] ?? 0.0) * config.silenceWeight
        let energy = (cues[.energyValley] ?? 0.0) * config.energyValleyWeight
        let spectral = (cues[.spectralDiscontinuity] ?? 0.0) * config.spectralDiscontinuityWeight
        return silence + energy + spectral
    }

    // MARK: - Guard Margins

    /// Apply asymmetric guard margins to bias toward letting ad audio leak
    /// rather than clipping editorial content.
    ///
    /// Worked example (guardMargin = 100ms):
    ///   adStart at 30.0s → shifts to 30.1s. The system starts muting later,
    ///   so the first 100ms of ad audio plays (leaks). Editorial before the
    ///   ad is never clipped.
    ///
    ///   adEnd at 60.0s → shifts to 59.9s. The system stops muting earlier,
    ///   so the last 100ms of ad audio plays (leaks). Editorial after the
    ///   ad is never clipped.
    private static func applyGuardMargins(
        bestTime: Double,
        rawLower: Double,
        rawUpper: Double,
        direction: BoundaryDirection,
        guardMargin: Double
    ) -> (lower: Double, upper: Double, time: Double) {
        switch direction {
        case .adStart:
            // Prefer later: shift everything forward (let ad leak at start).
            return (
                lower: rawLower + guardMargin,
                upper: rawUpper + guardMargin,
                time: bestTime + guardMargin
            )
        case .adEnd:
            // Prefer earlier: shift everything backward (let ad leak at end).
            return (
                lower: rawLower - guardMargin,
                upper: rawUpper - guardMargin,
                time: bestTime - guardMargin
            )
        }
    }

    // MARK: - Helpers

    private static func coarseFallback(
        candidate: Double,
        config: Config,
        direction: BoundaryDirection
    ) -> BoundaryEstimate {
        let (guardedLower, guardedUpper, guardedTime) = applyGuardMargins(
            bestTime: candidate,
            rawLower: candidate - config.hopSize,
            rawUpper: candidate + config.hopSize,
            direction: direction,
            guardMargin: config.guardMargin
        )
        return BoundaryEstimate(
            time: max(0, guardedTime),
            confidence: 0.0,
            lowerBound: max(0, guardedLower),
            upperBound: max(0, guardedUpper),
            cueBreakdown: [.coarseFallback: 1.0]
        )
    }

    private static func normalizeCueBreakdown(
        _ raw: [BoundaryCue: Double]
    ) -> [BoundaryCue: Double] {
        let total = raw.values.reduce(0.0, +)
        guard total > 0 else {
            return [.coarseFallback: 1.0]
        }
        return raw.mapValues { $0 / total }
    }
}

