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
    /// FM noAds consensus suppression: no strong proposal survived, capped to mark-only.
    case cappedByFMSuppression

    /// Restriction severity for ordering: higher means more restrictive.
    /// Used by SpanFinalizer.capEligibility to allow demotions but prevent promotions.
    /// Gates at the same severity level cannot override each other (first writer wins).
    var severity: Int {
        switch self {
        case .eligible: return 0
        case .markOnly: return 1
        case .blockedByEvidenceQuorum: return 2
        case .blockedByPolicy: return 2
        case .blockedByUserCorrection: return 3
        case .cappedByFMSuppression: return 1
        }
    }
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
    /// playhead-z3ch: Pre-seeded evidence derived from RSS feed metadata
    /// (description / summary cues). `cueCount` is the number of distinct
    /// metadata cues that contributed; `sourceField` records which RSS
    /// field contributed the strongest cue; `dominantCueType` is the
    /// strongest contributing cue type for diagnostics.
    case metadata(
        cueCount: Int,
        sourceField: MetadataCueSourceField,
        dominantCueType: MetadataCueType
    )
    /// Music-bed coverage across the span's windows. `presenceFraction`
    /// is the ratio of windows whose `MusicBedLevel != .none`;
    /// `foregroundCount` is how many of those windows were tagged
    /// `.foreground` (jingles/stingers) vs. `.background` (production
    /// beds under voice). Emitted by `MusicBedLedgerEvaluator`.
    case musicBed(presenceFraction: Double, foregroundCount: Int)
}

// MARK: - EvidenceSubSource

/// playhead-epfk: Disambiguates the producer of a `.catalog`-sourced
/// `EvidenceLedgerEntry`. Two distinct subsystems both emit `source =
/// .catalog`, and a single unified label hides which one fired:
///
///   - `.transcriptCatalog` — `EvidenceCatalogBuilder` extracts sponsor
///     tokens / URLs / promo codes / disclosures deterministically from
///     transcript atoms in the *current* episode. Per-backfill, never
///     persisted.
///   - `.fingerprintStore` — `AdCatalogStore` matches a span's acoustic
///     fingerprint against the cross-episode SQLite store accumulated
///     from prior auto-skips and user corrections. Per-span similarity
///     in `[0, 1]`.
///
/// `nil` (the default for back-compat constructors) means "source label
/// is the only producer marker," matching pre-epfk fixtures and call
/// sites that predate the disambiguation.
enum EvidenceSubSource: String, Sendable, Codable, Equatable, Hashable, CaseIterable {
    /// `EvidenceCatalogBuilder`'s sponsor-token catalog (in-pipeline,
    /// transcript-derived). The label NARL replay should attribute to
    /// the per-episode evidence channel.
    case transcriptCatalog
    /// `AdCatalogStore` cross-episode fingerprint match. The label NARL
    /// replay should attribute to the cumulative correction-loop signal.
    case fingerprintStore
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
    /// ef2.4.5: Classification trust factor from (CommercialIntent × Ownership) lookup.
    /// Applied by `BackfillEvidenceFusion.buildLedger()` to modulate FM evidence weight.
    /// Default of 1.0 means no modulation (backward compatible with pre-ef2.4.5 entries).
    let classificationTrust: Double
    /// playhead-epfk: Optional disambiguator for sources that have multiple
    /// distinct producers under one umbrella label. Today only `.catalog`
    /// uses this — the in-pipeline transcript sponsor catalog versus the
    /// cross-episode `AdCatalogStore` fingerprint match. `nil` for every
    /// other source (and pre-epfk callers) so adding the field is purely
    /// additive: existing constructors compile unchanged and the JSONL
    /// schema gains an optional key.
    let subSource: EvidenceSubSource?

    init(
        source: EvidenceSourceType,
        weight: Double,
        detail: EvidenceLedgerDetail,
        classificationTrust: Double = 1.0,
        subSource: EvidenceSubSource? = nil
    ) {
        self.source = source
        self.weight = weight
        self.detail = detail
        self.classificationTrust = classificationTrust
        self.subSource = subSource
    }
}
