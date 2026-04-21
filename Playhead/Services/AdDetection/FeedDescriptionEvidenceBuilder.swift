// FeedDescriptionEvidenceBuilder.swift
// playhead-z3ch: Builds metadata-source EvidenceLedgerEntries from RSS
// feed-description / itunes:summary cues.
//
// Pipeline:
//   FeedDescriptionMetadata
//     → MetadataCueExtractor (already shipping; produces EpisodeMetadataCue[])
//     → FeedDescriptionEvidenceBuilder (this file)
//     → BackfillEvidenceFusion.metadataEntries
//     → buildLedger() applies FusionBudgetClamp(metadataCap = 0.15)
//
// Weight derivation: per-cue weight = cue.confidence × source-field-trust,
// summed across all cues attached to the target span. The hard cap at 0.15
// is enforced downstream in fusion (intentionally — the builder produces
// the honest pre-clamp signal so the audit log captures cases where cap
// suppression is meaningful).
//
// Span attachment policy (v1): metadata is feed-level, not span-level. Every
// span on the asset receives the same metadata entry. This matches the
// "pre-seeding" intent — feed metadata is a coarse prior, not a per-window
// signal. The corroboration gate (in BackfillEvidenceFusion.computeGate)
// ensures this prior never triggers a skip alone.

import Foundation
import OSLog

struct FeedDescriptionEvidenceBuilder: Sendable {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "FeedDescriptionEvidenceBuilder"
    )

    init() {}

    /// Build metadata ledger entries for a single span from extracted cues.
    ///
    /// Returns at most one `.metadata` entry per (sourceField, dominantCueType)
    /// bucket so the clamp's audit log keeps a meaningful per-source record
    /// rather than fragmenting weight across many tiny entries.
    /// Returns an empty array when `cues` is empty.
    func buildEntries(
        cues: [EpisodeMetadataCue],
        for span: DecodedSpan
    ) -> [EvidenceLedgerEntry] {
        guard !cues.isEmpty else { return [] }

        // Bucket cues by source field so description-derived and summary-derived
        // signals each get their own ledger entry. (Both buckets are still
        // independently clamped at metadataCap downstream; this is an honest
        // representation, not a workaround for the clamp.)
        var byField: [MetadataCueSourceField: [EpisodeMetadataCue]] = [:]
        for cue in cues {
            byField[cue.sourceField, default: []].append(cue)
        }

        var entries: [EvidenceLedgerEntry] = []
        for (field, fieldCues) in byField.sorted(by: { $0.key.rawValue < $1.key.rawValue }) {
            let rawWeight = aggregateWeight(of: fieldCues, sourceField: field)
            guard rawWeight > 0 else { continue }
            let dominant = dominantCueType(in: fieldCues) ?? .disclosure
            let entry = EvidenceLedgerEntry(
                source: .metadata,
                weight: rawWeight,
                detail: .metadata(
                    cueCount: fieldCues.count,
                    sourceField: field,
                    dominantCueType: dominant
                )
            )
            entries.append(entry)
        }

        Self.logger.debug(
            "FeedDescriptionEvidenceBuilder: produced \(entries.count) entries for span \(span.id, privacy: .public)"
        )
        return entries
    }

    // MARK: - Private

    /// Aggregate the strongest cue confidence per cue type, then sum across
    /// types and apply the source-field trust factor. Description is treated
    /// as slightly more authoritative than summary because RSS show notes
    /// historically carry the canonical sponsor disclosures.
    private func aggregateWeight(
        of cues: [EpisodeMetadataCue],
        sourceField: MetadataCueSourceField
    ) -> Double {
        guard !cues.isEmpty else { return 0 }
        var bestPerType: [MetadataCueType: Double] = [:]
        for cue in cues {
            let conf = Double(cue.confidence) * cueTypeWeight(cue.cueType)
            bestPerType[cue.cueType] = max(bestPerType[cue.cueType] ?? 0, conf)
        }
        let summed = bestPerType.values.reduce(0.0, +)
        return summed * sourceFieldTrust(sourceField)
    }

    /// Per-type baseline weight reflecting how strongly each cue type
    /// implies an ad segment exists in the episode. Disclosures and promo
    /// codes are stronger evidence than bare external URLs.
    private func cueTypeWeight(_ type: MetadataCueType) -> Double {
        switch type {
        case .disclosure:         return 0.40
        case .promoCode:          return 0.35
        case .sponsorAlias:       return 0.30
        case .externalDomain:     return 0.20
        case .networkOwnedDomain: return 0.10
        case .showOwnedDomain:    return 0.05
        }
    }

    /// Source-field trust factor. Description (`<description>`) is the
    /// canonical home for sponsor disclosures in podcast RSS; summary is a
    /// secondary fallback that often duplicates description content.
    private func sourceFieldTrust(_ field: MetadataCueSourceField) -> Double {
        switch field {
        case .description: return 1.0
        case .summary:     return 0.8
        }
    }

    private func dominantCueType(in cues: [EpisodeMetadataCue]) -> MetadataCueType? {
        cues.max(by: { lhs, rhs in
            let lScore = Double(lhs.confidence) * cueTypeWeight(lhs.cueType)
            let rScore = Double(rhs.confidence) * cueTypeWeight(rhs.cueType)
            return lScore < rScore
        })?.cueType
    }
}
