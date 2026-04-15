// MetadataActivationConfig.swift
// ef2.4.7: Configuration gates for metadata activation in the ad detection pipeline.
//
// Each consumption point (lexical injection, classifier prior shift, FM scheduling)
// is independently gated and configurable. All three are off by default; the
// counterfactual evaluator controls activation via feature flags.

import Foundation

// MARK: - MetadataActivationConfig

/// Gates, weights, and thresholds for the three metadata consumption points.
///
/// All consumption points are independently toggleable. When a gate is disabled
/// the corresponding pipeline stage behaves identically to the pre-metadata path.
struct MetadataActivationConfig: Sendable, Equatable {

    // MARK: - Lexical Injection

    /// Whether metadata cues are injected into the ephemeral lexicon.
    let lexicalInjectionEnabled: Bool

    /// Floor metadataTrust below which lexical injection is skipped entirely.
    /// Default 0.0 means any non-zero trust allows injection.
    let lexicalInjectionMinTrust: Float

    /// Discount factor applied: weight = baseCategoryWeight * metadataTrust * discount.
    /// Spec mandates 0.75.
    let lexicalInjectionDiscount: Double

    // MARK: - Classifier Prior Shift

    /// Whether the classifier sigmoid midpoint shifts for metadata-warmed episodes.
    let classifierPriorShiftEnabled: Bool

    /// Minimum metadataTrust required to apply the prior shift.
    /// Spec mandates 0.08.
    let classifierPriorShiftMinTrust: Float

    /// The shifted sigmoid midpoint for metadata-warmed episodes.
    /// Default: 0.22 (vs baseline 0.25).
    let classifierShiftedMidpoint: Double

    /// The baseline sigmoid midpoint (no metadata).
    let classifierBaselineMidpoint: Double

    // MARK: - FM Scheduling

    /// Whether `.metadataSeededRegion` FM scheduling is active.
    let fmSchedulingEnabled: Bool

    /// Floor metadataTrust below which FM scheduling for seeded regions is skipped.
    let fmSchedulingMinTrust: Float

    // MARK: - Counterfactual Gate

    /// Master gate: when false, all three consumption points are disabled
    /// regardless of their individual flags. Tied to counterfactual evaluation.
    let counterfactualGateOpen: Bool

    // MARK: - Defaults

    /// Production default: all consumption points disabled pending counterfactual evaluation.
    static let `default` = MetadataActivationConfig(
        lexicalInjectionEnabled: false,
        lexicalInjectionMinTrust: 0.0,
        lexicalInjectionDiscount: 0.75,
        classifierPriorShiftEnabled: false,
        classifierPriorShiftMinTrust: 0.08,
        classifierShiftedMidpoint: 0.22,
        classifierBaselineMidpoint: 0.25,
        fmSchedulingEnabled: false,
        fmSchedulingMinTrust: 0.0,
        counterfactualGateOpen: false
    )

    /// All consumption points enabled (for testing and counterfactual-approved episodes).
    static let allEnabled = MetadataActivationConfig(
        lexicalInjectionEnabled: true,
        lexicalInjectionMinTrust: 0.0,
        lexicalInjectionDiscount: 0.75,
        classifierPriorShiftEnabled: true,
        classifierPriorShiftMinTrust: 0.08,
        classifierShiftedMidpoint: 0.22,
        classifierBaselineMidpoint: 0.25,
        fmSchedulingEnabled: true,
        fmSchedulingMinTrust: 0.0,
        counterfactualGateOpen: true
    )

    // MARK: - Effective State

    /// Whether lexical injection is effectively active (individual + master gate).
    var isLexicalInjectionActive: Bool {
        counterfactualGateOpen && lexicalInjectionEnabled
    }

    /// Whether classifier prior shift is effectively active.
    var isClassifierPriorShiftActive: Bool {
        counterfactualGateOpen && classifierPriorShiftEnabled
    }

    /// Whether FM scheduling for metadata-seeded regions is effectively active.
    var isFMSchedulingActive: Bool {
        counterfactualGateOpen && fmSchedulingEnabled
    }
}
