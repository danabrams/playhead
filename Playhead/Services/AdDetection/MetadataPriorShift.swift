// MetadataPriorShift.swift
// ef2.4.7: Sigmoid midpoint adjustment for metadata-warmed episodes.
//
// Shifts the RuleBasedClassifier sigmoid midpoint from 0.25 to 0.22
// when metadata trust is sufficient (>= 0.08). This is a prior adjustment,
// not a hard override — it makes the classifier slightly more receptive
// to ad signals in episodes where metadata suggests ad presence.
//
// The shift is small and bounded: 0.25 -> 0.22 (3 percentage points).
// It cannot be stacked or amplified beyond the configured shifted midpoint.

import Foundation

// MARK: - MetadataPriorShift

/// Computes the effective sigmoid midpoint for the classifier given
/// metadata trust and activation config.
///
/// Thread-safe: pure function with no mutable state.
struct MetadataPriorShift: Sendable, Equatable {

    private let config: MetadataActivationConfig

    init(config: MetadataActivationConfig = .default) {
        self.config = config
    }

    /// Compute the effective sigmoid midpoint for this episode.
    ///
    /// - Parameter metadataTrust: Aggregate trust score from the reliability matrix (0...1).
    /// - Returns: The sigmoid midpoint to use for classification.
    ///   Returns `classifierBaselineMidpoint` (0.25) when:
    ///     - Prior shift is not active (gate closed or flag disabled)
    ///     - metadataTrust < classifierPriorShiftMinTrust (0.08)
    ///   Returns `classifierShiftedMidpoint` (0.22) when:
    ///     - Prior shift is active AND metadataTrust >= threshold
    func effectiveMidpoint(metadataTrust: Float) -> Double {
        guard config.isClassifierPriorShiftActive else {
            return config.classifierBaselineMidpoint
        }
        guard metadataTrust >= config.classifierPriorShiftMinTrust else {
            return config.classifierBaselineMidpoint
        }
        return config.classifierShiftedMidpoint
    }

    /// Whether this episode qualifies for the prior shift given its metadata trust.
    func isShiftActive(metadataTrust: Float) -> Bool {
        config.isClassifierPriorShiftActive &&
        metadataTrust >= config.classifierPriorShiftMinTrust
    }
}
