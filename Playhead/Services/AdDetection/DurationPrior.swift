// DurationPrior.swift
// playhead-p2iv: Duration-as-prior — consume `ResolvedPriors.typicalAdDuration`
// in ad-detection evidence fusion as a soft monotonic multiplier.
//
// Design:
//   - Shape is a trapezoid with soft shoulders, peaking across the show-aware
//     `typicalAdDuration` range (commonly 30...90s from GlobalPriorDefaults).
//   - Multiplier range is bounded to [0.75, 1.10]: the prior nudges, it cannot
//     dominate. Strong independent evidence (FM + acoustic + sponsor match) on
//     a 2-minute host-read ad still confirms — the prior does not veto.
//   - Applied as a MULTIPLIER into the fused proposalConfidence, not stacked
//     as an independent voter. Stacking would double-count acoustic/lexical
//     evidence that already correlates with duration.
//   - Very short (< 5s) and very long (> ~2× upper bound) windows receive a
//     mild penalty, not a hard veto.
//
// The multiplier curve (for typicalAdDuration = a...b):
//   d <  5            : 0.75               (very short, plausibility-low)
//   d in [5, a)       : linear 0.90 → 1.10 (bumper → typical, small boost climbing)
//   d in [a, b]       : 1.10               (peak — canonical ad duration)
//   d in (b, 2b]      : linear 1.10 → 0.95 (long-form host-read still weighted)
//   d >  2b           : 0.75               (very long, mild penalty)
//
// GUARDRAIL: This is a scoring nudge. It does NOT grant auto-skip authority
// and is not an eligibility gate. The SkipPolicyMatrix and DecisionMapper's
// eligibility gate remain the sole skip authorities.

import Foundation

// MARK: - DurationPrior

/// Soft monotonic duration prior that modulates fused ad-detection confidence.
///
/// Construct from a `ResolvedPriors.typicalAdDuration` range, or use
/// `.identity` to disable the prior (multiplier == 1.0 everywhere).
struct DurationPrior: Sendable, Equatable {
    /// The show-aware typical ad duration range (seconds).
    /// Values inside this range produce the peak multiplier.
    let typicalAdDuration: ClosedRange<TimeInterval>

    /// The peak multiplier applied inside `typicalAdDuration`.
    /// Fixed to the shape calibration above; exposed for unit-testing / tuning.
    let peakMultiplier: Double

    /// The floor multiplier applied in the very-short and very-long regions.
    /// Fixed to the shape calibration above; exposed for unit-testing / tuning.
    let floorMultiplier: Double

    /// The multiplier at the 5s threshold (low-end bumper, ramping into peak).
    let bumperFloorMultiplier: Double

    /// The multiplier at the 2× upper-bound shoulder (long-form host-read).
    let longFormShoulderMultiplier: Double

    /// Very-short threshold: durations below this receive the floor penalty.
    static let veryShortThreshold: TimeInterval = 5.0

    /// The canonical prior derived from `GlobalPriorDefaults.standard.typicalAdDuration`
    /// (30...90s). Use this when no show-aware prior has been resolved.
    static let standard = DurationPrior(
        typicalAdDuration: GlobalPriorDefaults.standard.typicalAdDuration
    )

    /// A no-op prior. Useful in tests or callers that want to opt out.
    /// `multiplier(forDuration:)` returns 1.0 for all inputs.
    static let identity = DurationPrior(
        typicalAdDuration: 0...TimeInterval.infinity,
        peakMultiplier: 1.0,
        floorMultiplier: 1.0,
        bumperFloorMultiplier: 1.0,
        longFormShoulderMultiplier: 1.0
    )

    init(
        typicalAdDuration: ClosedRange<TimeInterval>,
        peakMultiplier: Double = 1.10,
        floorMultiplier: Double = 0.75,
        bumperFloorMultiplier: Double = 0.90,
        longFormShoulderMultiplier: Double = 0.95
    ) {
        self.typicalAdDuration = typicalAdDuration
        self.peakMultiplier = peakMultiplier
        self.floorMultiplier = floorMultiplier
        self.bumperFloorMultiplier = bumperFloorMultiplier
        self.longFormShoulderMultiplier = longFormShoulderMultiplier
    }

    /// Compute the duration-prior multiplier for a candidate span duration.
    ///
    /// The returned value is in `[floorMultiplier, peakMultiplier]` (default `[0.75, 1.10]`).
    /// Non-finite or negative durations are treated as a minimum-length span and
    /// map to `floorMultiplier`.
    func multiplier(forDuration duration: TimeInterval) -> Double {
        // Defensive: non-finite or negative durations → floor.
        guard duration.isFinite, duration >= 0 else { return floorMultiplier }

        // Identity prior short-circuits (avoids divide-by-zero when upper bound is .infinity).
        if peakMultiplier == 1.0,
           floorMultiplier == 1.0,
           bumperFloorMultiplier == 1.0,
           longFormShoulderMultiplier == 1.0 {
            return 1.0
        }

        let a = typicalAdDuration.lowerBound
        let b = typicalAdDuration.upperBound
        let longShoulder = 2.0 * b

        // Very-short region: d < 5s → floor.
        if duration < Self.veryShortThreshold {
            return floorMultiplier
        }

        // Bumper region: [5s, a). Linear ramp from bumperFloor → peak.
        if duration < a {
            // Guard against degenerate range where a <= 5 (should not happen for
            // typical show priors, but identity prior sets lowerBound = 0).
            let rangeSpan = a - Self.veryShortThreshold
            guard rangeSpan > 0 else { return peakMultiplier }
            let t = (duration - Self.veryShortThreshold) / rangeSpan
            return lerp(bumperFloorMultiplier, peakMultiplier, t)
        }

        // Peak region: [a, b].
        if duration <= b {
            return peakMultiplier
        }

        // Long-form shoulder: (b, 2b]. Linear decay from peak → longFormShoulder.
        if duration <= longShoulder {
            let rangeSpan = longShoulder - b
            guard rangeSpan > 0 else { return peakMultiplier }
            let t = (duration - b) / rangeSpan
            return lerp(peakMultiplier, longFormShoulderMultiplier, t)
        }

        // Very-long region: d > 2b → floor.
        return floorMultiplier
    }

    /// Linear interpolation: lerp(x, y, 0) == x, lerp(x, y, 1) == y.
    private func lerp(_ x: Double, _ y: Double, _ t: Double) -> Double {
        let tClamped = max(0, min(1, t))
        return x * (1.0 - tClamped) + y * tClamped
    }
}

// MARK: - Convenience

extension DurationPrior {
    /// Build from a `ResolvedPriors` value, consuming its `typicalAdDuration`.
    /// Callers that have resolved priors through the 4-level hierarchy should
    /// use this to feed the scoring path.
    init(resolvedPriors: ResolvedPriors) {
        self.init(typicalAdDuration: resolvedPriors.typicalAdDuration)
    }
}
