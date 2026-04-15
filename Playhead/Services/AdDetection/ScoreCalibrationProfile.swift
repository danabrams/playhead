// ScoreCalibrationProfile.swift
// playhead-ef2.1.6: Versioned calibration profiles and feature flags for evidence fusion.
//
// Design:
//   - MonotonicCalibrator: piecewise-linear mapping from raw signal → calibrated evidence space.
//   - ScoreCalibrationProfile: versioned container holding per-source calibrators + thresholds.
//   - CalibrationFeatureFlags: per-phase toggles with shadow/live modes.
//   - v0 profile preserves current identity mapping behavior exactly.

import Foundation

// MARK: - MonotonicCalibrator

/// Piecewise-linear monotonic mapping from raw signal [0,1] to calibrated evidence [0,1].
///
/// Knots are (input, output) pairs sorted by input. Values between knots are linearly
/// interpolated. Values outside the knot range are clamped to the nearest knot output.
/// NaN/Inf inputs return 0.0 (conservative — same as existing `calibrate()` behavior).
struct MonotonicCalibrator: Sendable, Codable, Equatable {
    /// Sorted (input, output) control points for piecewise-linear interpolation.
    let knots: [(input: Double, output: Double)]

    init(knots: [(Double, Double)]) {
        self.knots = knots.sorted { $0.0 < $1.0 }
    }

    /// Identity calibrator: maps raw → raw with [0,1] clamping only.
    static let identity = MonotonicCalibrator(knots: [(0.0, 0.0), (1.0, 1.0)])

    /// Map a raw score through the piecewise-linear function.
    func calibrate(_ raw: Double) -> Double {
        guard raw.isFinite else { return 0.0 }
        guard !knots.isEmpty else { return max(0.0, min(1.0, raw)) }

        // Single knot: constant output
        if knots.count == 1 {
            return knots[0].output
        }

        // Clamp below first knot
        if raw <= knots[0].input {
            return knots[0].output
        }
        // Clamp above last knot
        if raw >= knots[knots.count - 1].input {
            return knots[knots.count - 1].output
        }

        // Find the segment containing raw and interpolate
        for i in 0..<(knots.count - 1) {
            let lo = knots[i]
            let hi = knots[i + 1]
            if raw >= lo.input && raw <= hi.input {
                let span = hi.input - lo.input
                if span == 0 { return lo.output }
                let t = (raw - lo.input) / span
                return lo.output + t * (hi.output - lo.output)
            }
        }

        // Fallback (should not be reached with sorted knots)
        return max(0.0, min(1.0, raw))
    }

    // MARK: - Codable (tuples aren't auto-Codable)

    private enum CodingKeys: String, CodingKey {
        case knots
    }

    private struct KnotPair: Codable, Equatable {
        let input: Double
        let output: Double
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let pairs = try container.decode([KnotPair].self, forKey: .knots)
        self.knots = pairs.map { ($0.input, $0.output) }.sorted { $0.0 < $1.0 }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        let pairs = knots.map { KnotPair(input: $0.input, output: $0.output) }
        try container.encode(pairs, forKey: .knots)
    }

    static func == (lhs: MonotonicCalibrator, rhs: MonotonicCalibrator) -> Bool {
        guard lhs.knots.count == rhs.knots.count else { return false }
        return zip(lhs.knots, rhs.knots).allSatisfy { $0.0.input == $0.1.input && $0.0.output == $0.1.output }
    }
}

// MARK: - DecisionThresholds

/// Minimum thresholds applied after calibration. v0 uses zeros (no filtering).
struct DecisionThresholds: Sendable, Codable, Equatable {
    /// Minimum calibrated score to produce a non-zero skipConfidence.
    let skipMinimum: Double
    /// Minimum calibrated score to produce a non-zero proposalConfidence.
    let proposalMinimum: Double

    /// v0: no minimum thresholds (preserves current behavior).
    static let identity = DecisionThresholds(skipMinimum: 0.0, proposalMinimum: 0.0)
}

// MARK: - ScoreCalibrationProfile

/// Versioned container for per-source calibrators and decision thresholds.
///
/// Each pipeline run references a profile version. v0 = identity (no behavioral change).
/// Future versions (v1+) will carry real calibrators learned from shadow-mode data.
struct ScoreCalibrationProfile: Sendable, Codable, Equatable {
    let version: String
    let calibrators: [String: MonotonicCalibrator]
    let decisionThresholds: DecisionThresholds

    /// Look up the calibrator for a given evidence source. Falls back to identity.
    func calibrator(for source: EvidenceSourceType) -> MonotonicCalibrator {
        calibrators[source.rawValue] ?? .identity
    }

    /// v0: identity calibrators for all sources. Produces identical results to current behavior.
    static let v0: ScoreCalibrationProfile = {
        var cals: [String: MonotonicCalibrator] = [:]
        for source in EvidenceSourceType.allCases {
            cals[source.rawValue] = .identity
        }
        return ScoreCalibrationProfile(
            version: "v0",
            calibrators: cals,
            decisionThresholds: .identity
        )
    }()

    /// v1 placeholder: slot for real calibrators learned from Phase A/B shadow data.
    /// Currently uses identity calibrators — real calibrators will be populated in Phase C.
    static let v1Placeholder: ScoreCalibrationProfile = {
        var cals: [String: MonotonicCalibrator] = [:]
        for source in EvidenceSourceType.allCases {
            cals[source.rawValue] = .identity
        }
        return ScoreCalibrationProfile(
            version: "v1",
            calibrators: cals,
            decisionThresholds: .identity
        )
    }()
}

// MARK: - FeatureFlagMode

/// Activation mode for a calibration phase toggle.
enum FeatureFlagMode: String, Sendable, Codable, Equatable {
    /// Phase is completely disabled.
    case off
    /// Phase runs in shadow mode: computes but does not affect production decisions.
    case shadow
    /// Phase is fully active and affects production decisions.
    case live

    /// True if the phase should execute (shadow or live).
    var isActive: Bool { self != .off }
    /// True only if the phase affects production output.
    var isLive: Bool { self == .live }
}

// MARK: - CalibrationFeatureFlags

/// Per-phase toggles for the calibration rollout. Each phase can be independently
/// activated in shadow or live mode. Phases A through E map to the calibration
/// rollout plan; the exact semantics of each phase are documented in the rollout spec.
struct CalibrationFeatureFlags: Sendable, Codable, Equatable {
    var phaseA: FeatureFlagMode
    var phaseB: FeatureFlagMode
    var phaseC: FeatureFlagMode
    var phaseD: FeatureFlagMode
    var phaseE: FeatureFlagMode

    /// All phases disabled — safe default for production until rollout begins.
    static let allOff = CalibrationFeatureFlags(
        phaseA: .off, phaseB: .off, phaseC: .off, phaseD: .off, phaseE: .off
    )
}
