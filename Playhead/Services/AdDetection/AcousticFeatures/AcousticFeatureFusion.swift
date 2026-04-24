// AcousticFeatureFusion.swift
// playhead-gtt9.12: combines the per-feature [0,1] scores into one per-window
// aggregate acoustic score.
//
// Today each feature contributes its gated score times a fixed prior weight.
// Weights sum to approximately 1.0 so the combined score stays in [0, 1].
// Real weight calibration (grid search vs. the 2026-04-23 corpus) is gtt9.3's
// job — TODO below.
//
// The combiner is deliberately additive-capped rather than maximum-take-all
// so multiple independent acoustic signals reinforce each other (this is the
// mechanism that lets zero-transcript-coverage episodes produce candidates
// via stacked acoustic evidence — gtt9.12 acceptance #3).
//
// Pure computation on value types.

import Foundation

enum AcousticFeatureFusion {

    struct Weights: Sendable, Equatable {
        let musicBed: Double
        let lufsShift: Double
        let dynamicRange: Double
        let speakerShift: Double
        let spectralShift: Double
        let silenceBoundary: Double
        let repetitionFingerprint: Double
        let tempoOnset: Double

        // TODO(gtt9.12 → gtt9.3): tune these against the real 2026-04-23
        // corpus. These are reasonable priors (sum ≈ 1.0); gtt9.3's grid
        // search replaces them once the calibration pipeline is ready.
        static let defaultPriors = Weights(
            musicBed: 0.15,
            lufsShift: 0.15,
            dynamicRange: 0.10,
            speakerShift: 0.15,
            spectralShift: 0.10,
            silenceBoundary: 0.10,
            repetitionFingerprint: 0.15,
            tempoOnset: 0.10
        )

        func weight(for feature: AcousticFeatureKind) -> Double {
            switch feature {
            case .musicBed: return musicBed
            case .lufsShift: return lufsShift
            case .dynamicRange: return dynamicRange
            case .speakerShift: return speakerShift
            case .spectralShift: return spectralShift
            case .silenceBoundary: return silenceBoundary
            case .repetitionFingerprint: return repetitionFingerprint
            case .tempoOnset: return tempoOnset
            }
        }
    }

    /// Output of fusion for a single window — the combined score plus the
    /// list of feature kinds that contributed non-zero mass.
    struct WindowFusion: Sendable, Equatable {
        let windowStart: Double
        let windowEnd: Double
        /// Combined score, `[0, 1]`.
        let combinedScore: Double
        /// Features that actually contributed — i.e. their per-window score
        /// was above the gate (ledger of what fired).
        let contributingFeatures: [AcousticFeatureKind]
    }

    /// Combine the per-feature score arrays emitted by
    /// `<Feature>.scores(for:funnel:)`. All input arrays are aligned to the
    /// same `windows` the caller scored; mismatched window counts are
    /// tolerated by zero-padding (extra windows contribute zero from the
    /// missing feature).
    static func combine(
        featureScores: [AcousticFeatureKind: [AcousticFeatureScore]],
        weights: Weights = .defaultPriors,
        gateFloor: Double = 0.01
    ) -> [WindowFusion] {

        // Determine window count from the longest score array.
        let maxCount = featureScores.values.map(\.count).max() ?? 0
        guard maxCount > 0 else { return [] }

        // Use the longest score array (any feature's output; they're all
        // aligned by construction) to source the (start, end) pairs.
        let reference = featureScores.values.max(by: { $0.count < $1.count }) ?? []

        var out: [WindowFusion] = []
        out.reserveCapacity(maxCount)

        for idx in 0..<maxCount {
            var combined = 0.0
            var contributing: [AcousticFeatureKind] = []
            for (kind, arr) in featureScores where idx < arr.count {
                let s = arr[idx].score
                guard s >= gateFloor else { continue }
                combined += weights.weight(for: kind) * s
                contributing.append(kind)
            }
            let bounded = clampUnit(combined)
            let (start, end): (Double, Double)
            if idx < reference.count {
                start = reference[idx].windowStart
                end = reference[idx].windowEnd
            } else {
                start = 0; end = 0
            }
            // Stable ordering for deterministic snapshots.
            contributing.sort { $0.rawValue < $1.rawValue }
            out.append(WindowFusion(
                windowStart: start,
                windowEnd: end,
                combinedScore: bounded,
                contributingFeatures: contributing
            ))
        }
        return out
    }
}
