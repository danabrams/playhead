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
        // corpus. These are reasonable priors that sum to exactly 1.0
        // across the 7 live features; gtt9.3's grid search replaces them
        // once the calibration pipeline is ready.
        //
        // playhead-rfu-aac (review M5): `repetitionFingerprint` is a
        // stub today (always emits 0). Allocating it 0.15 effectively
        // capped the achievable combined score at ≈0.85 since that mass
        // never materialized. Redistribute the freed 0.15 across the 7
        // live features (proportionally) so the priors sum to exactly
        // 1.0 over signals that actually fire. The weight stays in the
        // struct (assigned 0) so future activation is a single-line
        // change rather than a struct-shape migration.
        static let defaultPriors = Weights(
            musicBed: 0.18,           // 0.15 → 0.18 (+0.03)
            lufsShift: 0.18,          // 0.15 → 0.18 (+0.03)
            dynamicRange: 0.12,       // 0.10 → 0.12 (+0.02)
            speakerShift: 0.18,       // 0.15 → 0.18 (+0.03)
            spectralShift: 0.12,      // 0.10 → 0.12 (+0.02)
            silenceBoundary: 0.10,
            repetitionFingerprint: 0, // stub, no signal — see comment.
            tempoOnset: 0.12          // 0.10 → 0.12 (+0.02)
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
        // Dictionary iteration order is unstable across runs, so use an
        // explicit feature priority (largest scoring buckets first) to
        // pick the reference deterministically. Two `combine` calls over
        // identical input must produce identical (start, end) pairs.
        let referencePriority: [AcousticFeatureKind] = [
            .musicBed,
            .lufsShift,
            .spectralShift,
            .speakerShift,
            .silenceBoundary,
            .dynamicRange,
            .tempoOnset,
            .repetitionFingerprint
        ]
        let reference: [AcousticFeatureScore] = referencePriority
            .lazy
            .compactMap { featureScores[$0] }
            .first(where: { !$0.isEmpty })
            ?? []

        var out: [WindowFusion] = []
        out.reserveCapacity(maxCount)

        // Iterate features in stable enum order (rawValue) so the
        // per-window combined sum is reproducible across runs. Float
        // addition is mostly associative but `Dictionary` iteration is
        // explicitly unstable, so we never trust it for determinism.
        let stableKinds = featureScores.keys.sorted { $0.rawValue < $1.rawValue }

        for idx in 0..<maxCount {
            var combined = 0.0
            var contributing: [AcousticFeatureKind] = []
            for kind in stableKinds {
                guard let arr = featureScores[kind], idx < arr.count else { continue }
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
