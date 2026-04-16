// SourceTrustProfile.swift
// playhead-ef2.4.4: Bayesian source trust with orthogonal updates.
//
// Design:
//   - BetaPosterior: Beta(alpha, beta) posterior with Bayesian update math.
//   - EvidenceFamily: groups EvidenceSourceType into orthogonal families.
//   - OrthogonalUpdateRule: enforces cross-family, cross-episode corroboration.
//   - SourceTrustProfile: per-source BetaPosterior priors with update tracking.
//
// Integration point: effectiveTrust feeds into fusion as the sourceTrust factor
// in FusionWeightConfig. This file is additive — no existing callers change.

import Foundation

// MARK: - BetaPosterior

/// Beta(alpha, beta) posterior for Bayesian source trust.
///
/// The Beta distribution is conjugate to Bernoulli observations:
/// after observing `s` successes and `f` failures from prior Beta(a,b),
/// the posterior is Beta(a+s, b+f).
///
/// `effectiveTrust` combines the posterior mean with an external confidence
/// factor. Low-observation sources are naturally dampened because their
/// posteriors have high variance — callers should pass a confidence value
/// that reflects the observation count (e.g. posteriorMean itself, or a
/// separate signal quality estimate).
struct BetaPosterior: Sendable, Equatable, Codable {
    let alpha: Double
    let beta: Double

    /// Posterior mean: E[theta] = alpha / (alpha + beta).
    /// Returns 0.5 (uninformative prior) if both alpha and beta are zero.
    var mean: Double {
        let total = alpha + beta
        guard total > 0 else { return 0.5 }
        return alpha / total
    }

    /// Posterior variance: Var[theta] = ab / ((a+b)^2 * (a+b+1)).
    /// Returns 0.0 when both alpha and beta are zero (degenerate case).
    var variance: Double {
        let total = alpha + beta
        guard total > 0 else { return 0.0 }
        return (alpha * beta) / (total * total * (total + 1))
    }

    /// Total pseudo-observations backing this posterior.
    var observationCount: Double {
        alpha + beta
    }

    /// Effective trust = posteriorMean * confidence.
    ///
    /// When no external confidence signal is available, callers may pass
    /// `posteriorMean` itself, which produces a natural dampening effect
    /// for low-observation sources (their mean is pulled toward 0.5 by
    /// the prior, reducing effective trust).
    func effectiveTrust(confidence: Double) -> Double {
        mean * confidence
    }

    /// Return a new posterior after observing additional Bernoulli outcomes.
    /// Negative inputs are clamped to zero to prevent invalid posteriors.
    func updated(successes: Double, failures: Double) -> BetaPosterior {
        BetaPosterior(
            alpha: alpha + max(0, successes),
            beta: beta + max(0, failures)
        )
    }
}

// MARK: - EvidenceFamily

/// Groups `EvidenceSourceType` into orthogonal evidence families.
///
/// The orthogonal update rule requires that corroboration come from a
/// *different* family — sources within the same family share too much
/// underlying signal to provide independent validation.
enum SourceEvidenceFamily: String, Sendable, Equatable, CaseIterable {
    /// Text-derived signals: lexical pattern matching, classifier scores.
    case textual
    /// Audio-derived signals: acoustic break detection, music bed analysis.
    case acoustic
    /// Learned model signals: Foundation Model disposition.
    case model
    /// External reference signals: fingerprint matching, catalog lookup.
    case reference

    /// Map an evidence source type to its family.
    static func `for`(_ source: EvidenceSourceType) -> SourceEvidenceFamily {
        switch source {
        case .lexical, .classifier:
            return .textual
        case .acoustic:
            return .acoustic
        case .fm:
            return .model
        case .fingerprint, .catalog:
            return .reference
        case .fusedScore:
            return .model
        }
    }
}

// MARK: - OrthogonalUpdateRule

/// Validates that a corroboration event satisfies the orthogonal update rule:
/// 1. The corroborating source must be from a *different* evidence family.
/// 2. The corroborating observation must be from a *different* episode.
///
/// These constraints prevent self-corroboration (same family) and
/// overfitting to a single episode's signal characteristics.
enum OrthogonalUpdateRule {

    /// Result of an orthogonal update validation check.
    enum ValidationResult: Sendable, Equatable {
        /// Corroboration is valid: different family, different episode.
        case allowed
        /// Blocked: corroborating source is in the same evidence family.
        case blockedSameFamily
        /// Blocked: corroborating observation is from the same episode.
        case blockedSameEpisode
    }

    /// Validate whether a corroboration event satisfies the orthogonal rule.
    ///
    /// - Parameters:
    ///   - sourceToUpdate: The source whose trust would be updated.
    ///   - corroboratingSource: The source providing the corroboration signal.
    ///   - sourceEpisodeId: Episode that produced the original evidence.
    ///   - corroboratingEpisodeId: Episode that produced the corroborating evidence.
    /// - Returns: `.allowed` if the update is valid, or a blocked reason.
    static func validate(
        sourceToUpdate: EvidenceSourceType,
        corroboratingSource: EvidenceSourceType,
        sourceEpisodeId: String,
        corroboratingEpisodeId: String
    ) -> ValidationResult {
        // Same-family check takes priority: even if episodes differ,
        // same-family corroboration is never valid.
        let sourceFamily = SourceEvidenceFamily.for(sourceToUpdate)
        let corroboratingFamily = SourceEvidenceFamily.for(corroboratingSource)
        guard sourceFamily != corroboratingFamily else {
            return .blockedSameFamily
        }

        // Cross-episode check: same episode cannot both seed and validate.
        guard sourceEpisodeId != corroboratingEpisodeId else {
            return .blockedSameEpisode
        }

        return .allowed
    }
}

// MARK: - UpdateTrace

/// A recorded corroboration event for holdout-compatible validation.
///
/// Traces enable offline analysis of whether the orthogonal update rule
/// is producing well-calibrated trust trajectories across episodes.
struct UpdateTrace: Sendable, Equatable, Codable {
    let sourceToUpdate: EvidenceSourceType
    let corroboratingSource: EvidenceSourceType
    let sourceEpisodeId: String
    let corroboratingEpisodeId: String
    let success: Bool
    let timestamp: Date
}

// MARK: - SourceTrustProfile

/// Per-source Beta-posterior trust with hierarchical priors and orthogonal updates.
///
/// Initial priors (from spec):
///   - classifier: Beta(6,4) -> 0.60  (metadata/legacy reliability)
///   - catalog:    Beta(6,4) -> 0.60  (metadata/catalog reliability)
///   - acoustic:   Beta(5,5) -> 0.50  (MusicBracket equivalent)
///   - fingerprint: Beta(7,3) -> 0.70
///   - fm:         Beta(8,2) -> 0.80
///   - lexical:    Beta(17,3) -> 0.85
///
/// `effectiveTrust(for:confidence:)` returns posteriorMean * confidence.
/// Callers supply the confidence factor (e.g. a signal quality estimate
/// or observation-count dampener); low-observation sources are naturally
/// dampened because their posteriors are pulled toward 0.5 by the prior.
struct SourceTrustProfile: Sendable, Equatable {

    /// Per-source Beta posteriors. Keyed by EvidenceSourceType.
    private var posteriors: [EvidenceSourceType: BetaPosterior]

    /// Ordered log of all accepted corroboration events.
    private(set) var updateTraces: [UpdateTrace]

    /// Create a profile with the spec-defined initial priors.
    init() {
        self.posteriors = Self.defaultPriors
        self.updateTraces = []
    }

    /// Create a profile with custom priors (for testing or migration).
    init(posteriors: [EvidenceSourceType: BetaPosterior]) {
        self.posteriors = posteriors
        self.updateTraces = []
    }

    // MARK: - Spec Priors

    /// Default priors matching the ef2.4.4 specification.
    static let defaultPriors: [EvidenceSourceType: BetaPosterior] = [
        .classifier:  BetaPosterior(alpha: 6, beta: 4),   // 0.60 — metadata reliability
        .catalog:     BetaPosterior(alpha: 6, beta: 4),   // 0.60 — metadata reliability
        .acoustic:    BetaPosterior(alpha: 5, beta: 5),   // 0.50 — MusicBracket
        .fingerprint: BetaPosterior(alpha: 7, beta: 3),   // 0.70
        .fm:          BetaPosterior(alpha: 8, beta: 2),   // 0.80
        .lexical:     BetaPosterior(alpha: 17, beta: 3),  // 0.85
        .fusedScore:  BetaPosterior(alpha: 1, beta: 1),   // 0.50 — uninformative (post-fusion aggregate)
    ]

    // MARK: - Query

    /// Return the current Beta posterior for a source.
    func posterior(for source: EvidenceSourceType) -> BetaPosterior {
        posteriors[source] ?? Self.defaultPriors[source] ?? BetaPosterior(alpha: 1, beta: 1)
    }

    /// Effective trust: posteriorMean * confidence.
    ///
    /// The confidence factor is supplied by the caller (e.g. the fusion
    /// layer's signal quality estimate). Low-observation sources are
    /// already dampened by the Beta prior pulling their mean toward 0.5.
    func effectiveTrust(for source: EvidenceSourceType, confidence: Double) -> Double {
        let p = posterior(for: source)
        return p.effectiveTrust(confidence: confidence)
    }

    // MARK: - Update

    /// Record a corroboration event, updating the source's posterior if the
    /// orthogonal update rule allows it.
    ///
    /// - Parameters:
    ///   - sourceToUpdate: The source whose trust to update.
    ///   - corroboratingSource: The source providing corroboration.
    ///   - sourceEpisodeId: Episode of the original evidence.
    ///   - corroboratingEpisodeId: Episode of the corroborating evidence.
    ///   - success: `true` if the corroboration confirmed the source's
    ///     prediction, `false` if it contradicted it.
    /// - Returns: The validation result. `.allowed` means the posterior
    ///   was updated; any blocked result means no change was made.
    @discardableResult
    mutating func recordCorroboration(
        sourceToUpdate: EvidenceSourceType,
        corroboratingSource: EvidenceSourceType,
        sourceEpisodeId: String,
        corroboratingEpisodeId: String,
        success: Bool
    ) -> OrthogonalUpdateRule.ValidationResult {
        let validation = OrthogonalUpdateRule.validate(
            sourceToUpdate: sourceToUpdate,
            corroboratingSource: corroboratingSource,
            sourceEpisodeId: sourceEpisodeId,
            corroboratingEpisodeId: corroboratingEpisodeId
        )

        guard validation == .allowed else {
            return validation
        }

        let current = posterior(for: sourceToUpdate)
        let updated = current.updated(
            successes: success ? 1 : 0,
            failures: success ? 0 : 1
        )
        posteriors[sourceToUpdate] = updated

        updateTraces.append(UpdateTrace(
            sourceToUpdate: sourceToUpdate,
            corroboratingSource: corroboratingSource,
            sourceEpisodeId: sourceEpisodeId,
            corroboratingEpisodeId: corroboratingEpisodeId,
            success: success,
            timestamp: Date()
        ))

        return validation
    }
}
