// AtomEvidence.swift
// Phase 5 (playhead-4my.5.1): Per-atom annotation produced by AtomEvidenceProjector.
// Carries anchor provenance, acoustic break hints, and user correction masks forward
// to MinimalContiguousSpanDecoder and the transcript overlay UI.
//
// Design invariants:
//   • No score thresholds, no calibration, no tunable parameters per-show.
//   • isAnchored == true only if at least one trustworthy anchor source fired.
//   • correctionMask comes from CorrectionMaskProvider (NoCorrectionMaskProvider by default).

import Foundation

// MARK: - AnchorRef

/// Describes which upstream signal caused an atom to be anchored.
///
/// Preserved end-to-end through to the transcript overlay popover so users
/// can see WHY a span was flagged.
enum AnchorRef: Sendable {
    /// FM consensus: two or more overlapping FM windows agreed (strength >= .medium).
    case fmConsensus(regionId: String, consensusStrength: Double)
    /// EvidenceEntry: a trustworthy evidence category (URL, promoCode, disclosurePhrase, ctaPhrase).
    case evidenceCatalog(entry: EvidenceEntry)
    /// Use C corroboration: single-window FM + co-located acoustic break both fired.
    case fmAcousticCorroborated(regionId: String, breakStrength: Double)
    /// User-reported false negative: user tapped "missed ad here" at a specific time.
    /// Creates an episode-local synthetic anchor so the correction takes effect immediately.
    case userCorrection(correctionId: String, reportedTime: Double)
}

extension AnchorRef: Equatable {
    static func == (lhs: AnchorRef, rhs: AnchorRef) -> Bool {
        switch (lhs, rhs) {
        case (.fmConsensus(let lid, let ls), .fmConsensus(let rid, let rs)):
            return lid == rid && ls == rs
        case (.evidenceCatalog(let le), .evidenceCatalog(let re)):
            return le.evidenceRef == re.evidenceRef && le.atomOrdinal == re.atomOrdinal
        case (.fmAcousticCorroborated(let lid, let ls), .fmAcousticCorroborated(let rid, let rs)):
            return lid == rid && ls == rs
        case (.userCorrection(let lid, let lt), .userCorrection(let rid, let rt)):
            return lid == rid && lt == rt
        default:
            return false
        }
    }
}

// MARK: - CorrectionState

/// User correction status for an atom.
enum CorrectionState: Sendable, Equatable {
    case none
    case userVetoed       // user said "this isn't an ad" — prevents span re-detection
    case userConfirmed    // user said "yes this is an ad"
}

// MARK: - CorrectionMaskProvider

/// Protocol for supplying user correction masks to AtomEvidenceProjector.
///
/// Phase 5 ships with NoCorrectionMaskProvider (all .none).
/// Phase 7 will conform UserCorrectionStore to this protocol.
protocol CorrectionMaskProvider: Sendable {
    func correctionMasks(
        for ordinals: ClosedRange<Int>,
        in assetId: String
    ) async -> [Int: CorrectionState]
}

// MARK: - NoCorrectionMaskProvider

/// No-op implementation: all atoms are unmasked.
struct NoCorrectionMaskProvider: CorrectionMaskProvider {
    func correctionMasks(
        for ordinals: ClosedRange<Int>,
        in assetId: String
    ) async -> [Int: CorrectionState] {
        return [:]
    }
}

// MARK: - AtomEvidence

/// Per-atom annotation produced by AtomEvidenceProjector.
///
/// Three fields carry the load:
///   - `isAnchored`: the precision-first gate; no span without an anchor.
///   - `hasAcousticBreakHint`: drives Use A (boundary snap) and Use B (anti-merge).
///   - `correctionMask`: user correction override, stable across transcript reprocessing.
struct AtomEvidence: Sendable {
    /// Stable ordinal from TranscriptAtomKey.
    let atomOrdinal: Int
    /// Start time from the source TranscriptAtom (seconds into episode).
    let startTime: Double
    /// End time from the source TranscriptAtom.
    let endTime: Double
    /// True if at least one trustworthy anchor source covers this atom.
    let isAnchored: Bool
    /// Which upstream signals caused isAnchored == true.
    let anchorProvenance: [AnchorRef]
    /// True if this atom's ordinal falls within an acoustic-origin region's
    /// `firstAtomOrdinal...lastAtomOrdinal` range — **not** necessarily at an
    /// actual acoustic break timestamp. Use A (boundary snap) and Use B
    /// (anti-merge) consume this as a coarse coverage flag; Use C break
    /// proximity checks use the separate `allAcousticBreaks` list which
    /// carries actual break timestamps mapped to ordinals.
    let hasAcousticBreakHint: Bool
    /// User correction override. Stable across transcript versions (keyed by ordinal).
    let correctionMask: CorrectionState
}
