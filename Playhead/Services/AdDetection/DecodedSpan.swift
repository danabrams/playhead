// DecodedSpan.swift
// Phase 5 (playhead-4my.5.2): A contiguous ad span produced by MinimalContiguousSpanDecoder.
//
// Design:
//   • Stable `id` is SHA256 prefix of "\(assetId):\(firstAtomOrdinal):\(lastAtomOrdinal)".
//   • Carries anchorProvenance all the way to the overlay UI for tap-to-explain.
//   • Persisted in `decoded_spans` SQLite table (new table, additive-only migration).

import CryptoKit
import Foundation

// MARK: - DecodedSpan

struct DecodedSpan: Sendable, Codable, Equatable, Identifiable {
    /// SHA256 prefix of "\(assetId):\(firstAtomOrdinal):\(lastAtomOrdinal)".
    /// Stable across re-runs — same inputs, same id.
    let id: String
    let assetId: String
    let firstAtomOrdinal: Int
    let lastAtomOrdinal: Int
    let startTime: Double
    let endTime: Double
    /// All anchor sources that contributed to any atom in this span.
    /// Serialized to JSON for persistence and restored on fetch.
    let anchorProvenance: [AnchorRef]

    var duration: Double { endTime - startTime }

    /// playhead-pzy2: True when the byte-exact REDIFF width-oracle owns this
    /// span's width (`.rediffSlot` in `anchorProvenance`) — the
    /// 100%-deterministic DAI-divergence marker (the origin literally served
    /// different ad bytes). This reads the SAME `.contains(.rediffSlot)` that
    /// `SpanExtentSupport.derive` uses to stamp the
    /// `.rediffByteExact` edge anchor, so the "byte-exact rediff" concept has one
    /// definition.
    ///
    /// Deterministic certainty OUTRANKS the low-certainty lexical / self-promo
    /// demotion heuristics: a span the rediff differ proved is a real ad must
    /// keep its width / eligibility even when a self-promo phrase or a creator
    /// "content" chapter label would otherwise demote it — the demotion-path
    /// mirror of how these edges are already EXEMPT from the boundary refiners
    /// (`isWidthOwnership` bypass) and the pre-roll start clamp (`PreRollStartClamp`).
    ///
    /// Deliberately splice-AGNOSTIC: `.spliceSlot` is ACOUSTIC width, NOT
    /// byte-exact, so a splice-owned span is NOT exempt (mirrors
    /// `SpanExtentSupport.derive`, which sets `.rediffByteExact` only for
    /// `.rediffSlot`). FM / lexical-only spans (no `.rediffSlot`) stay demotable.
    ///
    /// playhead-6qvf — WHAT THIS USED TO ANSWER, AND DID NOT. The expression is
    /// unchanged; what changed is that it is now TRUE. `.rediffSlot` used to be
    /// stamped by BOTH rediff differ arms — the byte-run aligner AND the ~1 s
    /// chroma-fingerprint fallback — so this predicate returned `true` for a
    /// chroma-derived width and every consumer read it as byte-exact. The
    /// chroma arm now stamps `.rediffSlotChroma` and fails this test.
    ///
    /// The correction moves ALL SIX consumers together, deliberately: the
    /// `hostReadConfidenceFloor` exemption (wraj), the FM-suppression cap
    /// exemption (qs0d), the creator-chapter and self-promo suppression
    /// exemptions (pzy2 / fl4j), the post-roll guard exemption (sik9), the
    /// `.rediffConfirmed` fusion kind (xsdz.62), and
    /// `SpanExtentSupport.derive`. Forking it for the sharpest consumer alone
    /// would leave five carve-outs meaning one thing by "byte-exact" and the
    /// sixth meaning another — strictly worse than one honest-but-wrong
    /// definition. Every consumer now gets the conservative answer for a chroma
    /// span, and that is the correct direction: each of them is a licence to
    /// SKIP or to decline a demotion, so under-granting costs a banner while
    /// over-granting costs show.
    ///
    /// The measurement behind the change (2026-08-02,
    /// `scripts/l2f-6qvf-chroma-fallback-rate.py`): on the 51 real A/B pairs in
    /// `TestFixtures/Corpus/Audio` the byte gate rejects **9 of 51 (17.6%)** —
    /// 8 non-monotonic chains and 1 re-encoding CDN — and each of those episodes
    /// falls through to the chroma arm. That is a LOWER bound: it counts only
    /// "every staged B gate-rejects", not the absent-B-URL, unresolvable-A-side
    /// (playhead-b8hj) or unreadable-bytes triggers.
    var carriesRediffByteExactWidth: Bool {
        anchorProvenance.carriesRediffByteExactWidth
    }

    /// playhead-6qvf: True when the CHROMA arm of the rediff width oracle owns
    /// this span's width (`.rediffSlotChroma`) — a ~1 s fingerprint alignment,
    /// not a byte-run one.
    ///
    /// This is a DIAGNOSTIC and RAIL surface, not a fifth certainty tier. No
    /// decision path grants anything on it: a chroma-owned span takes the
    /// conservative branch everywhere by failing
    /// `carriesRediffByteExactWidth`, and this property exists so that fact can
    /// be ASSERTED (see `RediffChromaWidthIsNotDeterministicTests`) and logged
    /// rather than merely believed. Mutually exclusive with
    /// `carriesRediffByteExactWidth` in practice — one differ arm runs per pass
    /// — but the two are read independently and neither implies the other's
    /// negation, so do not write one in terms of the other.
    var carriesRediffChromaWidth: Bool {
        anchorProvenance.carriesRediffChromaWidth
    }

    /// Compute the stable id from its components.
    static func makeId(assetId: String, firstAtomOrdinal: Int, lastAtomOrdinal: Int) -> String {
        let input = "\(assetId):\(firstAtomOrdinal):\(lastAtomOrdinal)"
        let hash = SHA256.hash(data: Data(input.utf8))
        return hash.prefix(16).map { String(format: "%02x", $0) }.joined()
    }
}

// MARK: - Rediff width provenance (the ONE definition)

/// playhead-6qvf: the single definition of each rediff width claim, on the
/// provenance ARRAY rather than on `DecodedSpan`, because the two consumers do
/// not agree on what they hold.
///
/// `DecodedSpan.carriesRediffByteExactWidth` has always documented itself as
/// reading "the SAME `.contains(.rediffSlot)` that `SpanExtentSupport.derive`
/// uses … so the 'byte-exact rediff' concept has one definition". It was two
/// expressions that happened to agree: `derive` takes a bare `[AnchorRef]` (it
/// runs after the finalizer, where no `DecodedSpan` is in hand) and so could
/// not call the span's property. The mutation battery found the gap — widening
/// the span predicate to accept the chroma marker left the extent tier, and
/// therefore auto-skip ADMISSION, completely unmoved.
///
/// Both spellings now route here, so the doc comment is a fact rather than an
/// intention: one edit changes the answer everywhere, and a mutation of it is
/// visible at every consumer.
extension [AnchorRef] {
    /// The BYTE-run differ owns this width. See `AnchorRef.rediffSlot`.
    var carriesRediffByteExactWidth: Bool { contains(.rediffSlot) }

    /// The ~1 s CHROMA differ owns this width. See `AnchorRef.rediffSlotChroma`.
    var carriesRediffChromaWidth: Bool { contains(.rediffSlotChroma) }

    /// playhead-d666: `true` IFF the ONLY presence anchor here is
    /// `.sustainedMusicOffset` — a sustained music-bed run and nothing else.
    ///
    /// THE ONE DEFINITION of "music-only", on the provenance ARRAY, for the
    /// same reason the rediff predicates above live here: the two consumers do
    /// not hold the same type. `BackfillEvidenceFusion.DecisionMapper` has a
    /// bare span-under-adjudication; `TranscriptPeekViewModel` has a persisted
    /// `DecodedSpan`. Before this they were one predicate and one absence — the
    /// mapper demoted a music-only span to `.markOnly` while the transcript
    /// drew it as an ad regardless, so the ratified policy held on exactly one
    /// of the two surfaces it names.
    ///
    /// What the anchor MEANS is the whole argument (see `AnchorRef
    /// .sustainedMusicOffset`): it is a TARGETING signal — "an ad likely begins
    /// right AFTER this music" — never a verdict about the audio it covers. The
    /// bare width markers (`.spliceSlot` / `.rediffSlot` / `.rediffSlotChroma`)
    /// are deliberately NOT corroboration: they set WIDTH, not PRESENCE, so a
    /// music+slot span still has no presence evidence beyond the music hint.
    var carriesOnlyMusicPresenceHint: Bool {
        var hasMusic = false
        var hasCorroboratingPresence = false
        for ref in self {
            switch ref {
            case .sustainedMusicOffset:
                hasMusic = true
            case .fmConsensus, .fmAcousticCorroborated, .evidenceCatalog,
                 .classifierSeed, .userCorrection:
                hasCorroboratingPresence = true
            case .spliceSlot, .rediffSlot, .rediffSlotChroma:
                break
            }
        }
        return hasMusic && !hasCorroboratingPresence
    }
}

// MARK: - DecoderConstants

/// Universal duration caps for MinimalContiguousSpanDecoder.
/// Merge and snap radii now live in MinimalContiguousSpanDecoder.Configuration.
enum DecoderConstants {
    /// Minimum span duration (seconds). Spans below this are dropped.
    static let minDurationSeconds: Double = 5
    /// Maximum span duration (seconds). Spans above this are recursively split.
    static let maxDurationSeconds: Double = 180
}

// MARK: - AnchorRef Codable helpers

/// Used for JSON encoding/decoding of anchorProvenance in the `decoded_spans` table.
extension AnchorRef: Codable {
    private enum CodingKeys: String, CodingKey {
        case type, regionId, consensusStrength, entry, breakStrength, correctionId, reportedTime, score, confidence
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let type = try container.decode(String.self, forKey: .type)
        switch type {
        case "fmConsensus":
            let regionId = try container.decode(String.self, forKey: .regionId)
            let strength = try container.decode(Double.self, forKey: .consensusStrength)
            self = .fmConsensus(regionId: regionId, consensusStrength: strength)
        case "evidenceCatalog":
            let entry = try container.decode(EvidenceEntry.self, forKey: .entry)
            self = .evidenceCatalog(entry: entry)
        case "fmAcousticCorroborated":
            let regionId = try container.decode(String.self, forKey: .regionId)
            let strength = try container.decode(Double.self, forKey: .breakStrength)
            self = .fmAcousticCorroborated(regionId: regionId, breakStrength: strength)
        case "userCorrection":
            let correctionId = try container.decode(String.self, forKey: .correctionId)
            let reportedTime = try container.decode(Double.self, forKey: .reportedTime)
            self = .userCorrection(correctionId: correctionId, reportedTime: reportedTime)
        case "classifierSeed":
            let regionId = try container.decode(String.self, forKey: .regionId)
            let score = try container.decode(Double.self, forKey: .score)
            self = .classifierSeed(regionId: regionId, score: score)
        case "sustainedMusicOffset":
            let regionId = try container.decode(String.self, forKey: .regionId)
            let confidence = try container.decode(Double.self, forKey: .confidence)
            self = .sustainedMusicOffset(regionId: regionId, confidence: confidence)
        case "spliceSlot":
            // Bare case (playhead-xsdz.22): the stable "spliceSlot" type string
            // is the entire encoding — no associated values to decode.
            self = .spliceSlot
        case "rediffSlot":
            // Bare case (playhead-xsdz.29): the stable "rediffSlot" type string
            // is the entire encoding — no associated values to decode.
            self = .rediffSlot
        case "rediffSlotChroma":
            // Bare case (playhead-6qvf): a DISTINCT stable type string, never a
            // field added to "rediffSlot". A binary predating this case decodes
            // the element through `LossyAnchorRef`, which yields `nil` and drops
            // it — the span loses its width-ownership marker and its
            // certainty carve-outs, which is the SAFE direction. Encoding chroma
            // as `"rediffSlot"` plus a discriminating field would fail the other
            // way: the old binary would read it as byte-exact.
            self = .rediffSlotChroma
        default:
            throw DecodingError.dataCorruptedError(forKey: .type, in: container, debugDescription: "Unknown AnchorRef type: \(type)")
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .fmConsensus(let regionId, let strength):
            try container.encode("fmConsensus", forKey: .type)
            try container.encode(regionId, forKey: .regionId)
            try container.encode(strength, forKey: .consensusStrength)
        case .evidenceCatalog(let entry):
            try container.encode("evidenceCatalog", forKey: .type)
            try container.encode(entry, forKey: .entry)
        case .fmAcousticCorroborated(let regionId, let strength):
            try container.encode("fmAcousticCorroborated", forKey: .type)
            try container.encode(regionId, forKey: .regionId)
            try container.encode(strength, forKey: .breakStrength)
        case .userCorrection(let correctionId, let reportedTime):
            try container.encode("userCorrection", forKey: .type)
            try container.encode(correctionId, forKey: .correctionId)
            try container.encode(reportedTime, forKey: .reportedTime)
        case .classifierSeed(let regionId, let score):
            try container.encode("classifierSeed", forKey: .type)
            try container.encode(regionId, forKey: .regionId)
            try container.encode(score, forKey: .score)
        case .sustainedMusicOffset(let regionId, let confidence):
            try container.encode("sustainedMusicOffset", forKey: .type)
            try container.encode(regionId, forKey: .regionId)
            try container.encode(confidence, forKey: .confidence)
        case .spliceSlot:
            // Bare case (playhead-xsdz.22): emit only the stable type string.
            try container.encode("spliceSlot", forKey: .type)
        case .rediffSlot:
            // Bare case (playhead-xsdz.29): emit only the stable type string.
            try container.encode("rediffSlot", forKey: .type)
        case .rediffSlotChroma:
            // Bare case (playhead-6qvf): emit only the stable type string.
            try container.encode("rediffSlotChroma", forKey: .type)
        }
    }
}

extension AnchorRef {
    /// playhead-xsdz.36.1.1 (observability-only): the stable discriminator
    /// string for this provenance case — IDENTICAL to the `type` value the
    /// `Codable` conformance above encodes. Surfaced by the pipeline-dump test
    /// seam (`AdDetectionService.evidenceProvenanceByWindowIdForTesting()`) so a
    /// span's anchor-provenance KINDS can be recorded without exposing the
    /// associated values. Behaviour-neutral: no production decision path reads
    /// it. MUST stay in sync with the `Codable` `type` strings above — the
    /// `anchorRefProvenanceKindMatchesCodableDiscriminator` test pins that.
    var provenanceKind: String {
        switch self {
        case .fmConsensus: return "fmConsensus"
        case .evidenceCatalog: return "evidenceCatalog"
        case .fmAcousticCorroborated: return "fmAcousticCorroborated"
        case .userCorrection: return "userCorrection"
        case .classifierSeed: return "classifierSeed"
        case .sustainedMusicOffset: return "sustainedMusicOffset"
        case .spliceSlot: return "spliceSlot"
        case .rediffSlot: return "rediffSlot"
        case .rediffSlotChroma: return "rediffSlotChroma"
        }
    }
}

/// Per-element tolerant wrapper around `AnchorRef` used when decoding arrays
/// from persistence: if an older binary reads a newer-written span that carries
/// an unknown `type` string, that element becomes `nil` instead of throwing
/// the whole array decode. Callers `compactMap(\.value)` to discard unknowns.
struct LossyAnchorRef: Decodable {
    let value: AnchorRef?
    init(from decoder: Decoder) throws {
        self.value = try? AnchorRef(from: decoder)
    }
}

extension EvidenceEntry: Codable {
    private enum CodingKeys: String, CodingKey {
        case evidenceRef, category, matchedText, normalizedText, atomOrdinal, count, firstTime, lastTime, startTime, endTime
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        let count = try c.decodeIfPresent(Int.self, forKey: .count) ?? 1
        let firstTime = try c.decodeIfPresent(Double.self, forKey: .firstTime)
            ?? c.decode(Double.self, forKey: .startTime)
        let lastTime = try c.decodeIfPresent(Double.self, forKey: .lastTime)
            ?? c.decode(Double.self, forKey: .endTime)
        let startTime = try c.decodeIfPresent(Double.self, forKey: .startTime) ?? firstTime
        let endTime = try c.decodeIfPresent(Double.self, forKey: .endTime) ?? lastTime
        self.init(
            evidenceRef: try c.decode(Int.self, forKey: .evidenceRef),
            category: try c.decode(EvidenceCategory.self, forKey: .category),
            matchedText: try c.decode(String.self, forKey: .matchedText),
            normalizedText: try c.decode(String.self, forKey: .normalizedText),
            atomOrdinal: try c.decode(Int.self, forKey: .atomOrdinal),
            startTime: startTime,
            endTime: endTime,
            count: count,
            firstTime: firstTime,
            lastTime: lastTime
        )
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(evidenceRef, forKey: .evidenceRef)
        try c.encode(category, forKey: .category)
        try c.encode(matchedText, forKey: .matchedText)
        try c.encode(normalizedText, forKey: .normalizedText)
        try c.encode(atomOrdinal, forKey: .atomOrdinal)
        try c.encode(count, forKey: .count)
        try c.encode(firstTime, forKey: .firstTime)
        try c.encode(lastTime, forKey: .lastTime)
        // Emit legacy startTime/endTime so older builds can still decode persisted spans.
        try c.encode(startTime, forKey: .startTime)
        try c.encode(endTime, forKey: .endTime)
    }
}
