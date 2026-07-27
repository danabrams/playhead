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
    /// Classifier-seeded anchor: a high-confidence `ClassifierResult` covered
    /// this atom. Used so that a classifier-only window (no lexical / acoustic
    /// / sponsor / fingerprint / FM signal) still flows into
    /// `MinimalContiguousSpanDecoder` and `BackfillEvidenceFusion`. The
    /// fusion `metadataCorroborationGate` treats this as an in-audio signal
    /// (ledger surfaces a `.classifier` entry separately with the score).
    case classifierSeed(regionId: String, score: Double)
    /// Sustained-music-offset PRESENCE anchor (playhead-t1py / playhead-xtpf):
    /// a first-class `SustainedMusicOffsetProposer` region — a long
    /// (>= `minRunSeconds`) high-`musicProbability` run whose trailing edge is
    /// a candidate music→speech ad boundary — covered this atom. Unlike the
    /// 1-atom-wide `.acoustic` break hint, the sustained-music proposal is
    /// atom-range WIDE, so it can independently anchor the atoms of an ad the
    /// FM grid missed (the projector Path 5 chokepoint fix).
    ///
    /// It is PRESENCE evidence, NOT a width oracle: `isWidthOwnership` is
    /// `false`. It is a TARGETING signal ("an ad likely begins right after this
    /// music"), never a standalone verdict — `DecisionMapper` demotes any
    /// span whose ONLY presence anchor is `.sustainedMusicOffset` to
    /// `.markOnly` (banner), NEVER auto-skip. Carries `confidence` = the run
    /// strength (mean `musicProbability` over the run) purely for the overlay
    /// popover; no decision path reads the magnitude.
    case sustainedMusicOffset(regionId: String, confidence: Double)
    /// Splice-slot ownership marker (playhead-xsdz.22): the acoustic splice
    /// channel owns this span's WIDTH. This case is BARE by design — unlike
    /// every sibling it carries NO associated values. Presence in
    /// `anchorProvenance` is the entire marker; slot geometry/confidence live
    /// on the span itself and in bead-C breadcrumbs, never in provenance.
    ///
    /// It is INERT to eligibility gating: width provenance is not presence
    /// evidence, so it neither selects a gate branch in
    /// `DecisionMapper.computeGate()` (which pattern-matches only
    /// `.fmConsensus` / `.fmAcousticCorroborated`) nor counts toward the
    /// corroborating-evidence-kind quorum (which is over `scoringLedger`
    /// `EvidenceSourceType`, never `AnchorRef`). No production code appends
    /// this case yet — the slot pass that does is playhead-xsdz.20.
    case spliceSlot
    /// Rediff-slot ownership marker (playhead-xsdz.29): the on-device REDIFF
    /// width-oracle (double-fetch DAI diff) owns this span's WIDTH. Sibling of
    /// `.spliceSlot` — same BARE shape (NO associated values), same inertness
    /// contract — but a DISTINCT provenance so a persisted span records which
    /// oracle set its width (acoustic splice vs rediff). Slot geometry /
    /// confidence live on the span itself and in the rediff shadow rows, never
    /// in provenance.
    ///
    /// Like `.spliceSlot`, it is INERT to eligibility gating: it selects no
    /// branch in `DecisionMapper.computeGate()` and counts toward no
    /// corroborating-evidence-kind quorum. The slot pass that appends it is the
    /// flag-OFF rediff ownership pass (playhead-xsdz.29).
    case rediffSlot
}

extension AnchorRef {
    /// True for the BARE width-ownership markers (`.spliceSlot`, `.rediffSlot`):
    /// a span carrying either has had its WIDTH set by a slot oracle (acoustic
    /// splice or rediff), so the width-integrity proxies — the Phase-5 projector
    /// clobber guard and the boundary-refine bypass — must treat both identically.
    ///
    /// Introduced with `.rediffSlot` (playhead-xsdz.29) so a future width oracle
    /// cannot silently desync those `.contains(.spliceSlot)` proxies again: add
    /// the marker here and every routed site follows. NOTE it does NOT cover the
    /// `.audioForensics`-suppression site, which is DELIBERATELY splice-only —
    /// acoustic width is DERIVED FROM the audio-forensics seam (double-count),
    /// whereas rediff width comes from an independent fingerprint diff.
    var isWidthOwnership: Bool {
        switch self {
        case .spliceSlot, .rediffSlot:
            return true
        case .fmConsensus, .evidenceCatalog, .fmAcousticCorroborated,
             .userCorrection, .classifierSeed, .sustainedMusicOffset:
            return false
        }
    }

    /// True for the user-CORRECTION anchor (`.userCorrection`): a span the user
    /// explicitly ADDED ("missed ad here"). playhead-527u: such spans are SACRED
    /// to the slot rewrite — never superseded, absorbed, or deleted by an
    /// automated width pass (the ADD-direction mirror of how a `.userVetoed`
    /// atom is de-anchored so its region is never re-detected).
    var isUserCorrection: Bool {
        if case .userCorrection = self { return true }
        return false
    }
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
        case (.classifierSeed(let lid, let ls), .classifierSeed(let rid, let rs)):
            return lid == rid && ls == rs
        case (.sustainedMusicOffset(let lid, let lc), .sustainedMusicOffset(let rid, let rc)):
            // Associated-value case: this arm is REQUIRED. The `default: return
            // false` below does NOT flag a missing case at compile time, so
            // omitting it would silently make
            // `.sustainedMusicOffset != .sustainedMusicOffset`, breaking
            // DecodedSpan equality and
            // `anchorProvenance.contains(.sustainedMusicOffset(...))`.
            return lid == rid && lc == rc
        case (.spliceSlot, .spliceSlot):
            // Bare case: identity is the whole marker. This arm is REQUIRED —
            // the `default: return false` below does NOT flag a missing case
            // at compile time, so omitting it would silently make
            // `.spliceSlot != .spliceSlot`, breaking DecodedSpan equality and
            // `anchorProvenance.contains(.spliceSlot)`.
            return true
        case (.rediffSlot, .rediffSlot):
            // Bare case (playhead-xsdz.29): same default:false trap as
            // `.spliceSlot` — this arm is REQUIRED or `.rediffSlot` would
            // silently compare unequal to itself, breaking DecodedSpan equality
            // and `anchorProvenance.contains(.rediffSlot)`.
            return true
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
/// Phase 5 ships with NoCorrectionMaskProvider (all .none). The read-side
/// wiring (playhead-xsdz.34) supplies `StoreBackedCorrectionMaskProvider`
/// behind the `userCorrectionReadSideEnabled` flag — an explicit `.falsePositive`
/// veto becomes a `.userVetoed` atom mask so the veto reaches detection.
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

// MARK: - StoreBackedCorrectionMaskProvider

/// playhead-xsdz.34 (Part 1 / design §2): the real read-side adapter. A
/// `Sendable` snapshot of an asset's explicit `.falsePositive` corrections,
/// constructed PER backfill run at the projector injection point, that turns
/// vetoed spans/time-ranges into `.userVetoed` atom masks.
///
/// Carries the atom time→ordinal index itself because the `CorrectionMaskProvider`
/// signature only passes `(ordinals, assetId)` — a store-as-provider could serve
/// `.exactSpan` (ordinal) vetoes but never resolve `.exactTimeSpan` (time) vetoes
/// without the atom stream. Both scopes flow through this one path.
///
/// Direction split (guardrails 1 + playhead-527u): the `.falsePositive` veto
/// scopes become `.userVetoed` masks (SUPPRESS direction — un-anchor a user-
/// removed region); the `.falseNegative` confirm scopes become `.userConfirmed`
/// masks (ADD direction — FORCE-anchor a user-added region). Both flow through
/// this one adapter. When an atom falls in BOTH a veto and a confirm range,
/// `.userVetoed` WINS (the suppress-direction safety guardrail dominates an
/// otherwise-contradictory pair). Show-wide scopes carry neither ordinals nor
/// times and are ignored (Layer-B suppression is handled separately by
/// `BroadCorrectionEvaluator`).
struct StoreBackedCorrectionMaskProvider: CorrectionMaskProvider {
    /// Vetoed ordinal ranges (from `.falsePositive` `.exactSpan` corrections).
    let vetoedOrdinalRanges: [ClosedRange<Int>]
    /// Vetoed `[start, end)` time ranges (from `.falsePositive` `.exactTimeSpan`).
    let vetoedTimeRanges: [(start: Double, end: Double)]
    /// playhead-527u: confirmed ordinal ranges (`.falseNegative` `.exactSpan`).
    let confirmedOrdinalRanges: [ClosedRange<Int>]
    /// playhead-527u: confirmed `[start, end)` ranges (`.falseNegative` `.exactTimeSpan`).
    let confirmedTimeRanges: [(start: Double, end: Double)]
    /// Atom index for time→ordinal resolution of the time-range masks.
    let atomsByOrdinal: [(ordinal: Int, start: Double, end: Double)]

    /// Direct (testable) initializer.
    init(
        vetoedOrdinalRanges: [ClosedRange<Int>],
        vetoedTimeRanges: [(start: Double, end: Double)],
        confirmedOrdinalRanges: [ClosedRange<Int>] = [],
        confirmedTimeRanges: [(start: Double, end: Double)] = [],
        atomsByOrdinal: [(ordinal: Int, start: Double, end: Double)]
    ) {
        self.vetoedOrdinalRanges = vetoedOrdinalRanges
        self.vetoedTimeRanges = vetoedTimeRanges
        self.confirmedOrdinalRanges = confirmedOrdinalRanges
        self.confirmedTimeRanges = confirmedTimeRanges
        self.atomsByOrdinal = atomsByOrdinal
    }

    /// Build the adapter from an asset's active `.falsePositive` (veto) scopes
    /// plus the transcript atom stream. Back-compat convenience: treats every
    /// scope as a veto and carries no confirm masks. Prefer
    /// `init(fromVetoScopes:confirmedScopes:atoms:)` to also carry the
    /// `.falseNegative` (confirm) direction.
    init(fromScopes scopes: [CorrectionScope], atoms: [TranscriptAtom]) {
        self.init(fromVetoScopes: scopes, confirmedScopes: [], atoms: atoms)
    }

    /// playhead-527u: build the adapter from BOTH correction directions.
    /// `.exactSpan` scopes contribute ordinal ranges, `.exactTimeSpan` scopes
    /// contribute time ranges; every other scope (show-wide) contributes nothing.
    init(
        fromVetoScopes vetoScopes: [CorrectionScope],
        confirmedScopes: [CorrectionScope],
        atoms: [TranscriptAtom]
    ) {
        let veto = Self.splitScopes(vetoScopes)
        let confirmed = Self.splitScopes(confirmedScopes)
        self.vetoedOrdinalRanges = veto.ordinalRanges
        self.vetoedTimeRanges = veto.timeRanges
        self.confirmedOrdinalRanges = confirmed.ordinalRanges
        self.confirmedTimeRanges = confirmed.timeRanges
        self.atomsByOrdinal = atoms.map {
            (ordinal: $0.atomKey.atomOrdinal, start: $0.startTime, end: $0.endTime)
        }
    }

    /// Partition Layer-A scopes into ordinal ranges (`.exactSpan`) and time
    /// ranges (`.exactTimeSpan`); drop show-wide and degenerate/inverted ranges.
    private static func splitScopes(
        _ scopes: [CorrectionScope]
    ) -> (ordinalRanges: [ClosedRange<Int>], timeRanges: [(start: Double, end: Double)]) {
        var ordinalRanges: [ClosedRange<Int>] = []
        var timeRanges: [(start: Double, end: Double)] = []
        for scope in scopes {
            switch scope {
            case .exactSpan(_, let range):
                ordinalRanges.append(range)
            case .exactTimeSpan(_, let start, let end):
                if end > start { timeRanges.append((start: start, end: end)) }
            case .sponsorOnShow, .phraseOnShow, .campaignOnShow,
                 .domainOwnershipOnShow, .jingleOnShow:
                continue
            }
        }
        return (ordinalRanges, timeRanges)
    }

    func correctionMasks(
        for ordinals: ClosedRange<Int>,
        in assetId: String
    ) async -> [Int: CorrectionState] {
        var out: [Int: CorrectionState] = [:]
        // CONFIRMED first, VETO second: a conflicting veto below OVERWRITES a
        // confirm, so `.userVetoed` wins the suppress-direction guardrail.
        applyMasks(
            ordinalRanges: confirmedOrdinalRanges, timeRanges: confirmedTimeRanges,
            state: .userConfirmed, ordinals: ordinals, into: &out
        )
        applyMasks(
            ordinalRanges: vetoedOrdinalRanges, timeRanges: vetoedTimeRanges,
            state: .userVetoed, ordinals: ordinals, into: &out
        )
        return out
    }

    /// Stamp `state` onto every atom the given ordinal/time ranges cover within
    /// the requested `ordinals` window.
    private func applyMasks(
        ordinalRanges: [ClosedRange<Int>],
        timeRanges: [(start: Double, end: Double)],
        state: CorrectionState,
        ordinals: ClosedRange<Int>,
        into out: inout [Int: CorrectionState]
    ) {
        // `.exactSpan`: intersect each ordinal range with the requested range
        // (guard `lo <= hi` so a disjoint range marks nothing).
        for range in ordinalRanges {
            let lo = Swift.max(range.lowerBound, ordinals.lowerBound)
            let hi = Swift.min(range.upperBound, ordinals.upperBound)
            guard lo <= hi else { continue }
            for ordinal in lo...hi { out[ordinal] = state }
        }
        // `.exactTimeSpan`: atoms whose `[start, end)` overlaps a masked time
        // range (positive-duration overlap; touching endpoints do not count).
        guard !timeRanges.isEmpty else { return }
        for atom in atomsByOrdinal where ordinals.contains(atom.ordinal) {
            let overlaps = timeRanges.contains { r in
                atom.start < r.end && r.start < atom.end
            }
            if overlaps { out[atom.ordinal] = state }
        }
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
