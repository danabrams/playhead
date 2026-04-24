// AcousticFeatureFunnel.swift
// playhead-gtt9.12 acceptance criterion #1: per-feature funnel counters.
//
// For each `AcousticFeatureKind` we track four stages:
//
//   computed      — the feature's score(...) was invoked
//   producedSignal — the raw score exceeded its per-feature "interesting" floor
//   passedGate    — the score passed the confidence gate used at fusion time
//   includedInFusion — the (possibly weighted) score was handed to the fusion combiner
//
// Counts are per-episode. The fusion pipeline (or a test) creates one
// `AcousticFeatureFunnel`, threads it through the feature calls, and reads
// the snapshot when the episode completes. This lets gtt9.4's telemetry
// pinpoint *which* stage is dropping a feature out of fusion.
//
// Pure-value counters; the struct is `Sendable`. Callers that need to share
// a funnel across concurrency domains should wrap it in an actor or lock —
// we deliberately keep this type actor-free so unit tests can assert on it
// directly.

import Foundation

// MARK: - AcousticFeatureFunnelStage

enum AcousticFeatureFunnelStage: String, Sendable, Hashable, CaseIterable {
    /// Feature compute was invoked on a window / pair of windows.
    case computed
    /// Raw score was above the feature's "produced a signal" floor
    /// (i.e. something interesting happened — e.g. the delta was non-trivial,
    /// the window was not silent, etc.).
    case producedSignal
    /// Score passed the confidence gate used when deciding whether to
    /// contribute to fusion.
    case passedGate
    /// Score was actually included in the fusion aggregate.
    case includedInFusion
}

// MARK: - AcousticFeatureFunnel

/// Per-episode funnel counters for the acoustic feature family.
/// Value semantics — mutate a local copy, then snapshot.
struct AcousticFeatureFunnel: Sendable, Equatable {

    /// `counts[.computed][.musicBed]` = number of times the musicBed feature was computed.
    /// Missing stage / feature combinations default to zero.
    private(set) var counts: [AcousticFeatureFunnelStage: [AcousticFeatureKind: Int]]

    init() {
        var initial: [AcousticFeatureFunnelStage: [AcousticFeatureKind: Int]] = [:]
        for stage in AcousticFeatureFunnelStage.allCases {
            initial[stage] = [:]
        }
        self.counts = initial
    }

    /// Increment a single (stage, feature) counter.
    mutating func record(_ stage: AcousticFeatureFunnelStage, _ feature: AcousticFeatureKind, by delta: Int = 1) {
        var stageBucket = counts[stage] ?? [:]
        stageBucket[feature, default: 0] += delta
        counts[stage] = stageBucket
    }

    /// Convenience: one call that marks a feature computed, and conditionally
    /// marks the remaining stages based on flags the caller has already evaluated.
    mutating func record(
        feature: AcousticFeatureKind,
        producedSignal: Bool,
        passedGate: Bool,
        includedInFusion: Bool
    ) {
        record(.computed, feature)
        if producedSignal { record(.producedSignal, feature) }
        if passedGate { record(.passedGate, feature) }
        if includedInFusion { record(.includedInFusion, feature) }
    }

    /// Read an individual counter. Returns 0 when the combination has no events.
    func count(_ stage: AcousticFeatureFunnelStage, _ feature: AcousticFeatureKind) -> Int {
        counts[stage]?[feature] ?? 0
    }

    /// Total events across all features for a single stage.
    func total(_ stage: AcousticFeatureFunnelStage) -> Int {
        counts[stage]?.values.reduce(0, +) ?? 0
    }

    /// Flatten the funnel into a deterministic array of rows suitable for
    /// logging / telemetry export. Ordered by `AcousticFeatureKind.allCases`
    /// and `AcousticFeatureFunnelStage.allCases`.
    struct Row: Sendable, Equatable {
        let feature: AcousticFeatureKind
        let stage: AcousticFeatureFunnelStage
        let count: Int
    }

    func rows() -> [Row] {
        var out: [Row] = []
        out.reserveCapacity(AcousticFeatureKind.allCases.count * AcousticFeatureFunnelStage.allCases.count)
        for feature in AcousticFeatureKind.allCases {
            for stage in AcousticFeatureFunnelStage.allCases {
                out.append(Row(feature: feature, stage: stage, count: count(stage, feature)))
            }
        }
        return out
    }
}
