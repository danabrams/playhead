// EpisodeMetadataCue.swift
// ef2.2.2: Structured cue extracted from episode RSS description/summary text.
//
// Each cue represents a single signal extracted from feed metadata:
// sponsor disclosures, external URLs, promo codes, sponsor name mentions,
// or domain ownership classifications. Cues are shadow-mode only —
// they are logged and returned but never influence live ad detection scoring.

import Foundation

// MARK: - Cue Type

/// The kind of signal a metadata cue represents.
enum MetadataCueType: String, Sendable, Codable, CaseIterable {
    /// Disclosure phrase: "sponsored by", "brought to you by", "partner", etc.
    case disclosure
    /// URL pointing to a domain not owned by the show or its network.
    case externalDomain
    /// Promotional code: "use code X", "promo code X".
    case promoCode
    /// Sponsor name mention (resolved against known entities).
    case sponsorAlias
    /// URL pointing to the show's own domain.
    case showOwnedDomain
    /// URL pointing to the podcast network's domain.
    case networkOwnedDomain
}

// MARK: - Source Field

/// Which RSS field the cue was extracted from.
enum MetadataCueSourceField: String, Sendable, Codable, CaseIterable {
    case description
    case summary
    /// playhead-gtt9.22: Cue derived from a chapter marker
    /// (`<podcast:chapter>`, `<podcast:chapters>` JSON, or ID3 CHAP).
    /// Distinct from `description`/`summary` so FrozenTrace, the
    /// reliability matrix, and any future per-source policy can treat
    /// chapter-derived signals separately. Trust factor for this field
    /// lives in `FeedDescriptionEvidenceBuilder.sourceFieldTrust`.
    case chapter
    /// playhead-gtt9.22: Cue derived from a structured sponsor mention
    /// in show-notes HTML (e.g. "This episode is sponsored by …" lists).
    /// Distinct from `description` because show-notes (`content:encoded`
    /// or fallback `description`) often contains a richer prose-form
    /// sponsor disclosure than the iTunes summary, and we want telemetry
    /// to attribute hits separately.
    case showNotes
}

// MARK: - EpisodeMetadataCue

/// A single metadata cue extracted from episode RSS description or summary.
/// All fields are immutable after construction.
struct EpisodeMetadataCue: Sendable, Codable, Equatable {
    /// The type of signal this cue represents.
    let cueType: MetadataCueType
    /// The normalized value extracted (e.g. domain name, promo code, disclosure phrase).
    let normalizedValue: String
    /// Which RSS field this cue was extracted from.
    let sourceField: MetadataCueSourceField
    /// Confidence of the extraction (0.0...1.0).
    let confidence: Float
    /// Canonical sponsor entity ID, if resolved. Nil when no entity match.
    let canonicalSponsorId: String?
    /// Canonical owner ID (show/network), if resolved. Nil when unresolved.
    let canonicalOwnerId: String?
}
