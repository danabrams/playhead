// CorrectionAttribution.swift
// Phase EF2 (playhead-ef2.3.1): Causal source inference for user corrections.
//
// When a user corrects a region, CorrectionAttribution records what caused the
// error — which pipeline source was most responsible and what specific evidence
// contributed to the incorrect decision.
//
// Design:
//   - CorrectionType captures the semantic nature of the correction (FP/FN/boundary).
//   - CausalSource identifies the pipeline component most responsible for the error.
//   - CorrectionTargetRefs carries optional IDs for downstream analysis (fingerprint,
//     sponsor entity, etc.).
//   - inferCausalSource examines anchor provenance and evidence ledger entries to
//     determine which source to blame.

import Foundation

// MARK: - CorrectionType

/// The semantic nature of a user correction.
///
/// Serialized to/from rawValue (TEXT) for SQLite storage.
enum CorrectionType: String, Sendable, Codable, CaseIterable, Equatable {
    /// System flagged content as an ad, but it is not.
    case falsePositive
    /// System missed an ad that the user identified.
    case falseNegative
    /// The detected ad span starts before the actual ad begins.
    case startTooEarly
    /// The detected ad span starts after the actual ad begins.
    case startTooLate
    /// The detected ad span ends before the actual ad ends.
    case endTooEarly
    /// The detected ad span ends after the actual ad ends.
    case endTooLate
}

// MARK: - CausalSource

/// The pipeline component most likely responsible for the error that the user corrected.
///
/// Serialized to/from rawValue (TEXT) for SQLite storage.
enum CausalSource: String, Sendable, Codable, CaseIterable, Equatable {
    /// Lexical pattern matching (URL, promo code, CTA, disclosure phrases).
    case lexical
    /// Foundation model classifier.
    case foundationModel
    /// Ad-copy fingerprint matching.
    case fingerprint
    /// Music-bed bracket detection.
    case musicBracket
    /// Episode/feed metadata cues.
    case metadata
    /// Positional prior (ads tend to appear at episode start/end).
    case positionPrior
    /// Acoustic break detection.
    case acoustic
}

// MARK: - CorrectionTargetRefs

/// Optional references to specific evidence items involved in the corrected decision.
///
/// JSON-encoded for SQLite storage in the `targetRefsJSON` column.
struct CorrectionTargetRefs: Sendable, Codable, Equatable {
    /// Atom ordinals that the correction targets.
    var atomIds: [Int]?
    /// Evidence reference identifiers (e.g. "[E0]", "[E3]").
    var evidenceRefs: [String]?
    /// Fingerprint ID if a fingerprint match contributed to the error.
    var fingerprintId: String?
    /// Podcast domain/feed identifier for show-level attribution.
    var domain: String?
    /// Sponsor entity name if the error involved a specific sponsor.
    var sponsorEntity: String?
}

// MARK: - CorrectionAttribution

/// Full attribution for a user correction: what went wrong and which pipeline
/// component was most responsible.
struct CorrectionAttribution: Sendable, Equatable {
    let correctionType: CorrectionType
    let causalSource: CausalSource
    let targetRefs: CorrectionTargetRefs?
}

// MARK: - Causal Inference

/// Infer the most likely causal source from a span's anchor provenance and
/// the evidence ledger entries that contributed to the decision.
///
/// Algorithm (ledger-based, when ledger is non-empty):
///   1. Compute per-source total weight.
///   2. If the top source is lexical, return .lexical.
///   3. If FM weight > 0.3 of total weight, return .foundationModel.
///   4. If the top source is fingerprint, return .fingerprint.
///   5. Otherwise, return the highest-weight source mapped to CausalSource.
///
/// Falls back to provenance-only inference when the ledger is empty.
func inferCausalSource(
    provenance: [AnchorRef],
    ledgerEntries: [EvidenceLedgerEntry]
) -> CausalSource {
    // If we have ledger entries, use weight-based inference.
    if !ledgerEntries.isEmpty {
        // Accumulate total weight per source type.
        var weightBySource: [EvidenceSourceType: Double] = [:]
        for entry in ledgerEntries {
            weightBySource[entry.source, default: 0] += entry.weight
        }

        let totalWeight = weightBySource.values.reduce(0, +)
        guard totalWeight > 0 else {
            return inferFromProvenance(provenance)
        }

        // Find the source with the highest weight.
        // Tie-break by rawValue for deterministic ordering when weights are equal.
        let sorted = weightBySource.sorted {
            if $0.value != $1.value { return $0.value > $1.value }
            return $0.key.rawValue < $1.key.rawValue
        }
        let topSource = sorted[0].key

        // Rule 1: Top source is lexical.
        if topSource == .lexical {
            return .lexical
        }

        // Rule 2: FM weight > 0.3 of total.
        let fmWeight = weightBySource[.fm] ?? 0
        if fmWeight / totalWeight > 0.3 {
            return .foundationModel
        }

        // Rule 3: Fingerprint source present with highest weight.
        if topSource == .fingerprint {
            return .fingerprint
        }

        // Rule 4: Map the highest-weight source to CausalSource.
        return mapSourceType(topSource)
    }

    // Ledger is empty — fall back to provenance-only inference.
    return inferFromProvenance(provenance)
}

/// Map an EvidenceSourceType to the corresponding CausalSource.
private func mapSourceType(_ source: EvidenceSourceType) -> CausalSource {
    switch source {
    case .fm:          return .foundationModel
    case .lexical:     return .lexical
    case .acoustic:    return .acoustic
    case .catalog:     return .lexical  // catalog entries are lexical matches
    case .classifier:  return .foundationModel  // legacy classifier ≈ FM
    case .fingerprint: return .fingerprint
    }
}

/// Infer causal source from anchor provenance alone (no ledger entries).
private func inferFromProvenance(_ provenance: [AnchorRef]) -> CausalSource {
    // Count anchor ref types.
    var fmCount = 0
    var evidenceCatalogCount = 0
    var acousticCount = 0

    for ref in provenance {
        switch ref {
        case .fmConsensus:
            fmCount += 1
        case .evidenceCatalog:
            evidenceCatalogCount += 1
        case .fmAcousticCorroborated:
            fmCount += 1
            acousticCount += 1
        }
    }

    // Prefer evidence catalog (lexical) if present, then FM, then acoustic.
    if evidenceCatalogCount > 0 { return .lexical }
    if fmCount > 0 { return .foundationModel }
    if acousticCount > 0 { return .acoustic }

    // No provenance at all — default to FM as the most common source.
    return .foundationModel
}

/// Build a CorrectionTargetRefs from a span's provenance and optional overrides.
///
/// The `ledgerEntries` parameter is reserved for future use (e.g. extracting
/// fingerprint IDs from ledger details); currently only provenance is inspected.
func buildTargetRefs(
    provenance: [AnchorRef],
    ledgerEntries: [EvidenceLedgerEntry],
    fingerprintId: String? = nil,
    domain: String? = nil,
    sponsorEntity: String? = nil
) -> CorrectionTargetRefs? {
    // Extract atom ordinals from evidence catalog entries.
    let atomIds: [Int] = provenance.compactMap { ref in
        if case .evidenceCatalog(let entry) = ref {
            return entry.atomOrdinal
        }
        return nil
    }

    // Extract evidence refs from evidence catalog entries.
    let evidenceRefs: [String] = provenance.compactMap { ref in
        if case .evidenceCatalog(let entry) = ref {
            return "[E\(entry.evidenceRef)]"
        }
        return nil
    }

    // Extract sponsor entity from brandSpan evidence.
    let inferredSponsor = sponsorEntity ?? provenance.compactMap { ref -> String? in
        if case .evidenceCatalog(let entry) = ref, entry.category == .brandSpan {
            return entry.normalizedText
        }
        return nil
    }.first

    let refs = CorrectionTargetRefs(
        atomIds: atomIds.isEmpty ? nil : atomIds,
        evidenceRefs: evidenceRefs.isEmpty ? nil : evidenceRefs,
        fingerprintId: fingerprintId,
        domain: domain,
        sponsorEntity: inferredSponsor
    )

    // Return nil if all fields are nil (no useful refs to store).
    if refs.atomIds == nil && refs.evidenceRefs == nil &&
       refs.fingerprintId == nil && refs.domain == nil && refs.sponsorEntity == nil {
        return nil
    }
    return refs
}
