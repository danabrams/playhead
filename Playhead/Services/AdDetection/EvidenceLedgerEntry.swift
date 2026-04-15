// EvidenceLedgerEntry.swift
// Phase 6 (playhead-4my.6.1): Per-source evidence ledger entry and eligibility gate.
//
// Design:
//   • Each evidence source contributes capped, trust-scaled EvidenceLedgerEntry items.
//   • SkipEligibilityGate controls whether a decision is actionable without clamping score.
//   • EvidenceLedgerDetail carries source-specific metadata per variant.

import Foundation

// MARK: - SkipEligibilityGate

/// Controls whether a span decision is actionable.
///
/// A gate block prevents action but does NOT clamp the score — `skipConfidence`
/// remains an honest estimate regardless of the gate value.
enum SkipEligibilityGate: String, Sendable, Codable, Equatable {
    /// Decision is actionable; all quorum and policy requirements are met.
    case eligible
    /// FM-only or weak corroboration: evidence quorum not satisfied.
    case blockedByEvidenceQuorum
    /// External policy (e.g. content type, show-level overrides) prevents skip.
    case blockedByPolicy
    /// Span crosses a high-quality content chapter; eligible for banner only, not auto-skip.
    case markOnly
    /// User previously vetoed this span or region.
    case blockedByUserCorrection
}

// MARK: - EvidenceLedgerDetail

/// Source-specific metadata attached to each ledger entry.
enum EvidenceLedgerDetail: Sendable {
    /// Old RuleBasedClassifier score promoted to a ledger entry.
    case classifier(score: Double)
    /// Foundation Model disposition with certainty band and cohort label.
    case fm(disposition: CoarseDisposition, band: CertaintyBand, cohortPromptLabel: String)
    /// Lexical pattern matches — categories that fired.
    case lexical(matchedCategories: [String])
    /// Acoustic break detection strength.
    case acoustic(breakStrength: Double)
    /// Catalog entries matched for this span.
    case catalog(entryCount: Int)
    /// Ad copy fingerprint matches for this span.
    case fingerprint(matchCount: Int, averageSimilarity: Double)
}

// MARK: - EvidenceLedgerEntry

/// A single capped, trust-scaled contribution from one evidence source.
///
/// Multiple entries from the same source are allowed (e.g. multiple FM windows).
/// `BackfillEvidenceFusion` accumulates these; `DecisionMapper` sums `weight` into
/// `proposalConfidence`.
struct EvidenceLedgerEntry: Sendable {
    /// Which evidence source produced this entry.
    let source: EvidenceSourceType
    /// Capped, trust-scaled weight in the range [0, cap] where cap is source-specific.
    let weight: Double
    /// Source-specific metadata for diagnostics and logging.
    let detail: EvidenceLedgerDetail
}
