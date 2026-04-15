// SpanDecision.swift
// ef2.4.1: Four-stage pipeline output type.
//
// Stage 1 — Proposal:   ProposalAuthority (.strong / .weak) via ProposalQuorum
// Stage 2 — Classification: ContentClass (7 cases)
// Stage 3 — Boundary:   BoundaryEstimate (start/end times + confidence)
// Stage 4 — Policy:     SkipEligibility via SkipPolicyMatrixV2
//
// Design:
//   • Each stage is independently testable.
//   • SpanDecision composes all four stages into a single output.
//   • SkipPolicyMatrixV2 is a pure lookup — policy NEVER influences proposal or classification.
//   • Existing DecisionResult / SkipEligibilityGate / SkipPolicyMatrix remain untouched;
//     SpanDecision is an additive type that will replace them incrementally.

import Foundation

// MARK: - Stage 1: Proposal

/// The authority level of a proposal signal.
enum ProposalAuthority: String, Sendable, Codable, Hashable, CaseIterable {
    /// Strong evidence: a single signal suffices for quorum.
    case strong
    /// Weak evidence: requires 2 from different families for quorum.
    case weak
}

/// The family a signal belongs to, used to enforce cross-family quorum for weak signals.
enum SignalFamily: String, Sendable, Codable, Hashable {
    case lexical
    case model
    case acoustic
    case metadata
    case heuristic
}

/// Individual signals that can contribute to a proposal.
///
/// Each signal has a fixed authority (.strong or .weak) and belongs to exactly one family.
/// Strong signals: URL, promo code, disclosure, FM containsAd, fingerprint.
/// Weak signals: metadata, position prior, music bracket, lexical without anchor.
enum ProposalSignal: String, Sendable, Codable, Hashable, CaseIterable {
    // Strong signals
    case url
    case promoCode
    case disclosure
    case fmContainsAd
    case fingerprint

    // Weak signals
    case metadata
    case positionPrior
    case musicBracket
    case lexicalWithoutAnchor

    var authority: ProposalAuthority {
        switch self {
        case .url, .promoCode, .disclosure, .fmContainsAd, .fingerprint:
            return .strong
        case .metadata, .positionPrior, .musicBracket, .lexicalWithoutAnchor:
            return .weak
        }
    }

    var family: SignalFamily {
        switch self {
        case .url, .promoCode, .disclosure, .lexicalWithoutAnchor:
            return .lexical
        case .fmContainsAd:
            return .model
        case .fingerprint, .musicBracket:
            return .acoustic
        case .metadata:
            return .metadata
        case .positionPrior:
            return .heuristic
        }
    }
}

/// Quorum rules for proposal signals.
///
/// Rule: 1 strong signal OR 2 weak signals from different families.
enum ProposalQuorum {

    /// Whether the given signals satisfy the proposal quorum.
    static func isMet(signals: [ProposalSignal]) -> Bool {
        // Any strong signal satisfies quorum immediately.
        if signals.contains(where: { $0.authority == .strong }) {
            return true
        }
        // Count distinct families among weak signals.
        let weakFamilies = Set(signals.filter { $0.authority == .weak }.map { $0.family })
        return weakFamilies.count >= 2
    }

    /// The resolved authority level when quorum is met, or nil when quorum is not met.
    ///
    /// Returns `.strong` if any strong signal is present, `.weak` if only weak signals
    /// from different families satisfy quorum, or `nil` if quorum is not met.
    static func resolvedAuthority(signals: [ProposalSignal]) -> ProposalAuthority? {
        guard isMet(signals: signals) else { return nil }
        if signals.contains(where: { $0.authority == .strong }) {
            return .strong
        }
        return .weak
    }
}

// MARK: - Stage 2: Classification

/// Content classification for a detected span.
///
/// Classifies the commercial nature of the content independently from proposal
/// authority and skip policy. This classification feeds into SkipPolicyMatrixV2
/// but is computed before policy evaluation.
enum ContentClass: String, Sendable, Codable, Hashable, CaseIterable {
    /// Third-party paid insertion ad (classic podcast ad).
    case thirdPartyPaid
    /// Affiliate/commission-based read (promo code, affiliate link).
    case affiliatePaid
    /// Network-level cross-promotion (other shows on the same network).
    case networkPromo
    /// Show-level self-promotion (merch, Patreon, live shows).
    case showPromo
    /// Host's own product (book, course, app).
    case ownedProduct
    /// Organic editorial mention with no commercial relationship.
    case editorialMention
    /// Insufficient signal to classify.
    case unknown
}

// MARK: - Stage 3: Boundary

/// Estimated time boundaries for a detected span with per-boundary confidence.
///
/// Reusable across pipeline stages — captures where the span starts and ends
/// along with how confident the boundary detector is about each edge.
struct BoundaryEstimate: Sendable, Equatable, Codable, Hashable {
    /// Estimated start time of the span in seconds.
    let startTime: Double
    /// Estimated end time of the span in seconds.
    let endTime: Double
    /// Confidence in the start boundary (0–1).
    let startConfidence: Double
    /// Confidence in the end boundary (0–1).
    let endConfidence: Double

    /// Duration of the estimated span in seconds.
    var duration: Double { endTime - startTime }
}

// MARK: - Stage 4: Policy

/// The skip eligibility determined by policy evaluation.
///
/// This replaces SkipEligibilityGate with a richer set of outcomes. The existing
/// SkipEligibilityGate remains for backward compatibility; SkipEligibility is the
/// forward-looking replacement.
enum SkipEligibility: String, Sendable, Codable, Hashable, CaseIterable {
    /// Eligible for automatic skipping.
    case autoSkipEligible
    /// Show banner/marker only; never auto-skip.
    case markOnly
    /// User can configure whether to skip (e.g., show promos).
    case userConfigurable
    /// Not eligible for any skip action.
    case ineligible
}

/// Maps (ContentClass, ProposalAuthority) to SkipEligibility.
///
/// Pure function/lookup — no side effects, no state. Policy NEVER influences
/// proposal or classification scores.
///
/// Named SkipPolicyMatrixV2 to coexist with the existing SkipPolicyMatrix
/// (which maps CommercialIntent x AdOwnership → SkipPolicyAction).
struct SkipPolicyMatrixV2: Sendable {

    /// Determine skip eligibility from content class and proposal authority.
    ///
    /// All (ContentClass x ProposalAuthority) combinations are enumerated so the
    /// compiler catches any future cases added to either enum.
    static func eligibility(for contentClass: ContentClass, authority: ProposalAuthority) -> SkipEligibility {
        switch (contentClass, authority) {

        // Third-party paid: auto-skip with strong evidence, banner with weak.
        case (.thirdPartyPaid, .strong):
            return .autoSkipEligible
        case (.thirdPartyPaid, .weak):
            return .markOnly

        // Affiliate: always banner-only regardless of evidence strength.
        case (.affiliatePaid, _):
            return .markOnly

        // Network promo: user-configurable with strong, banner with weak.
        case (.networkPromo, .strong):
            return .userConfigurable
        case (.networkPromo, .weak):
            return .markOnly

        // Show promo: user-configurable with strong, banner with weak.
        case (.showPromo, .strong):
            return .userConfigurable
        case (.showPromo, .weak):
            return .markOnly

        // Owned product: always banner-only — host's own product is ambiguous.
        case (.ownedProduct, _):
            return .markOnly

        // Editorial mention: never eligible for any skip action.
        case (.editorialMention, _):
            return .ineligible

        // Unknown class: surface banner if strong evidence, otherwise ineligible.
        case (.unknown, .strong):
            return .markOnly
        case (.unknown, .weak):
            return .ineligible
        }
    }
}

// MARK: - SpanDecision

/// Composite output of the four-stage pipeline.
///
/// Composes all four pipeline stages into a single, immutable value:
///   1. Proposal authority + contributing signals
///   2. Content classification
///   3. Boundary estimate
///   4. Skip eligibility (derived from policy matrix, never from proposal/classification scores)
struct SpanDecision: Sendable, Equatable, Codable, Hashable {
    /// Stage 1: The resolved proposal authority level.
    let proposalAuthority: ProposalAuthority
    /// Stage 1: The signals that contributed to the proposal.
    let proposalSignals: [ProposalSignal]
    /// Stage 2: The content classification.
    let contentClass: ContentClass
    /// Stage 3: The estimated boundaries.
    let boundary: BoundaryEstimate
    /// Stage 4: The skip eligibility determined by policy.
    let skipEligibility: SkipEligibility
}
