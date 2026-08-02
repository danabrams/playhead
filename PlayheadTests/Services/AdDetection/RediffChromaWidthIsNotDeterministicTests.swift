// RediffChromaWidthIsNotDeterministicTests.swift
// playhead-6qvf: THE RAIL. A chroma-derived rediff width must never reach the
// `deterministic` extent tier, must never be admitted to auto-skip by
// playhead-2350, and must never be padded from `AutoSkipEdgePadding`'s
// byte-differ margins.
//
// WHAT WENT WRONG, so the rail is read as a claim and not as ceremony.
// `AdDetectionService.applyRediffSlotOwnershipPass` stamped a literal
// `.rediffSlot` for BOTH differ arms — the byte-run aligner AND the ~1 s
// chroma-fingerprint fallback. `DecodedSpan.carriesRediffByteExactWidth` is
// `contains(.rediffSlot)`, so a chroma width read as byte-exact in all six
// consumers; `SpanExtentSupport.derive` stamped `.rediffByteExact` on both
// edges, `ExtentAnchorTier.deterministic` followed, and
// `AutoSkipEdgePadding` handed it the 0.50 s / 0.75 s late-safe margins
// DERIVED FROM MEASURED BYTE-DIFFER EDGE ERROR (xsdz.44 spike: |dEnd| median
// 0.02 s, max 0.22 s). The chroma differ has no such distribution — the
// codebase's own note on the byte path names the alternative as
// "the chroma differ's multi-second edge overshoots — the 88 s Mick Jagger
// incident". Two instruments, one certainty class.
//
// NOT A THEORETICAL BRANCH. Measured 2026-08-02 by
// `scripts/l2f-6qvf-chroma-fallback-rate.py` over the 51 real A/B pairs in
// `TestFixtures/Corpus/Audio`, applying `gateAndDiffBytes`'s gates verbatim at
// the LAGGED path's settings: the byte gate rejects on 9 of 51 (17.6%) — 8
// non-monotonic chains, 1 re-encoding CDN — and every one falls to chroma.
//
// The end-to-end proof that the SERVICE stamps the right arm lives in
// `RediffByteFirstEndToEndTests` (`byteFailFallsBackToChroma` /
// `remoteSourceURLFallsBackToChroma`), which drive the real pass. This suite
// pins the INVARIANTS those two rely on, plus the exhaustive
// "only `.rediffSlot` earns `deterministic`" sweep that a future fifth width
// oracle would otherwise silently break.

import Foundation
import Testing

@testable import Playhead

@Suite("Rediff chroma width is NOT deterministic (playhead-6qvf)")
struct RediffChromaWidthIsNotDeterministicTests {

    private func span(_ provenance: [AnchorRef]) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "a", firstAtomOrdinal: 0, lastAtomOrdinal: 9),
            assetId: "a",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 9,
            startTime: 100,
            endTime: 160,
            anchorProvenance: provenance
        )
    }

    /// One representative value of EVERY `AnchorRef` case. Kept exhaustive by
    /// `everyAnchorCaseIsRepresented` below — `AnchorRef` is deliberately not
    /// `CaseIterable`, so a new case must be added here or that test fails.
    private static let allCases: [AnchorRef] = [
        .fmConsensus(regionId: "r", consensusStrength: 0.9),
        .evidenceCatalog(entry: EvidenceEntry(
            evidenceRef: 1, category: .url, matchedText: "x.com",
            normalizedText: "x.com", atomOrdinal: 0, startTime: 0, endTime: 1)),
        .fmAcousticCorroborated(regionId: "r", breakStrength: 0.8),
        .userCorrection(correctionId: "c", reportedTime: 5),
        .classifierSeed(regionId: "r", score: 0.7),
        .sustainedMusicOffset(regionId: "r", confidence: 0.6),
        .spliceSlot,
        .rediffSlot,
        .rediffSlotChroma
    ]

    // MARK: - The predicate

    @Test("a chroma-owned span FAILS carriesRediffByteExactWidth and PASSES carriesRediffChromaWidth")
    func chromaSpanIsNotByteExact() {
        let chroma = span([.rediffSlotChroma])
        #expect(!chroma.carriesRediffByteExactWidth,
                "the whole bead: a ~1 s chroma alignment is not a byte-exact width")
        #expect(chroma.carriesRediffChromaWidth)
    }

    @Test("a byte-owned span is unchanged — byte-exact true, chroma false")
    func byteSpanIsStillByteExact() {
        let byteSpan = span([.rediffSlot])
        #expect(byteSpan.carriesRediffByteExactWidth,
                "the byte arm must not have been narrowed along with the chroma one")
        #expect(!byteSpan.carriesRediffChromaWidth)
    }

    @Test("the two predicates are independent — a span carrying BOTH answers both true")
    func predicatesAreIndependent() {
        // Not producible by one pass (one arm runs per pass), but the two
        // properties are read separately and neither is written as the other's
        // negation. Pinned so a future `!carriesRediffChromaWidth` shortcut for
        // `carriesRediffByteExactWidth` is caught here rather than in the field.
        let both = span([.rediffSlot, .rediffSlotChroma])
        #expect(both.carriesRediffByteExactWidth)
        #expect(both.carriesRediffChromaWidth)
    }

    // MARK: - Width ownership survives (the thing that must NOT change)

    @Test("chroma STILL owns width — isWidthOwnership is true")
    func chromaStillOwnsWidth() {
        // The clobber guard in the Phase-5 projector and the boundary-refine
        // bypass both key on `isWidthOwnership`. Demoting certainty must not
        // demote OWNERSHIP: the chroma differ really did set this width, and a
        // span that lost ownership would be re-refined and re-minted at seed
        // geometry — a strictly worse outcome than the bug being fixed.
        #expect(AnchorRef.rediffSlotChroma.isWidthOwnership)
        #expect(span([.rediffSlotChroma]).anchorProvenance.contains { $0.isWidthOwnership })
    }

    @Test("chroma is attribution-neutral, like its two sibling width markers")
    func chromaIsAttributionNeutral() {
        // Width provenance answers "how well are the edges known", never
        // "which signal found the ad". A chroma marker must not shift causal
        // attribution any more than `.rediffSlot` or `.spliceSlot` does.
        #expect(!AnchorRef.rediffSlotChroma.isUserCorrection)
    }

    // MARK: - THE RAIL: extent tier

    @Test("a chroma-owned span derives UNANCHORED on both edges — tier .none, not .deterministic")
    func chromaDerivesUnanchored() {
        let support = SpanExtentSupport.derive(
            anchorProvenance: [.rediffSlotChroma],
            stingerTrace: nil
        )
        #expect(support.startAnchor == .unanchored)
        #expect(support.endAnchor == .unanchored)
        #expect(support.tier == .none)
        #expect(support.tier != .deterministic)
        #expect(!support.isFullyAnchored,
                "playhead-2350: a span with no anchored edge is a BANNER, never an auto-skip")
    }

    @Test("RAIL — ONLY .rediffSlot derives the deterministic tier; every other anchor, alone, does not")
    func onlyByteExactReachesDeterministic() {
        // The sweep, not the single case. A future width oracle added to
        // `isWidthOwnership` without a matching decision about extent would
        // otherwise inherit `deterministic` the same way chroma did — silently,
        // and only in production.
        for anchor in Self.allCases {
            let support = SpanExtentSupport.derive(anchorProvenance: [anchor], stingerTrace: nil)
            if anchor == .rediffSlot {
                #expect(support.tier == .deterministic,
                        "the byte differ is the ONE deterministic extent source")
            } else {
                #expect(support.tier != .deterministic,
                        "\(anchor.provenanceKind) must not reach the deterministic tier")
            }
        }
    }

    @Test("RAIL — a chroma span mixed with ordinary presence anchors still does not reach deterministic")
    func chromaMixedWithPresenceStaysUnanchored() {
        // The realistic shape: the chroma slot widened a span the FM/lexical
        // core proposed, so provenance carries both. Presence evidence must not
        // launder the extent claim.
        let support = SpanExtentSupport.derive(
            anchorProvenance: [
                .fmConsensus(regionId: "r", consensusStrength: 0.95),
                .evidenceCatalog(entry: EvidenceEntry(
                    evidenceRef: 1, category: .promoCode, matchedText: "SHOW",
                    normalizedText: "show", atomOrdinal: 3, startTime: 100, endTime: 160)),
                .rediffSlotChroma
            ],
            stingerTrace: nil
        )
        #expect(support.tier == .none)
    }

    // MARK: - THE RAIL: auto-skip padding

    @Test("RAIL — a chroma span's anchors do NOT open the qs0d targeted padding lane")
    func chromaDoesNotOpenTargetedPaddingLane() {
        // `AutoSkipEdgePadding.isActive(masterEnabled: false, ...)` is the
        // playhead-qs0d targeted activation: with the master switch OFF (its
        // shipping default) the padding policy runs for exactly the spans whose
        // BOTH edges are `.rediffByteExact`. That is the lane a chroma span was
        // entering while being padded from the byte differ's error budget.
        let support = SpanExtentSupport.derive(
            anchorProvenance: [.rediffSlotChroma], stingerTrace: nil)
        #expect(!AutoSkipEdgePadding.isActive(
            masterEnabled: false,
            startAnchor: support.startAnchor,
            endAnchor: support.endAnchor
        ))
        // The byte arm still opens it — the fix must not have closed the lane
        // for the population it was derived for.
        let byteSupport = SpanExtentSupport.derive(
            anchorProvenance: [.rediffSlot], stingerTrace: nil)
        #expect(AutoSkipEdgePadding.isActive(
            masterEnabled: false,
            startAnchor: byteSupport.startAnchor,
            endAnchor: byteSupport.endAnchor
        ))
    }

    @Test("RAIL — a chroma span has NO late-safe start margin, so it cannot be auto-skipped")
    func chromaHasNoStartMargin() {
        let support = SpanExtentSupport.derive(
            anchorProvenance: [.rediffSlotChroma], stingerTrace: nil)
        #expect(AutoSkipEdgePadding.startMargin(for: support.startAnchor) == nil,
                "an unanchored start has no cheap late-safe margin (derivation §5)")
        // And specifically: it must NOT be handed the byte tier's 0.50 s.
        #expect(AutoSkipEdgePadding.startMargin(for: support.startAnchor)
                != AutoSkipEdgePadding.startMarginRediffByteExactSeconds)
        #expect(AutoSkipEdgePadding.endMargin(for: support.endAnchor)
                != AutoSkipEdgePadding.endMarginRediffByteExactSeconds,
                "0.75 s off the end was derived from |dEnd| max 0.22 s on the BYTE differ")
    }

    // MARK: - Persistence contract

    @Test("chroma encodes a STABLE bare 'rediffSlotChroma' type string, distinct from 'rediffSlot'")
    func chromaStableTypeString() throws {
        let object = try JSONDecoder().decode(
            [String: String].self,
            from: try JSONEncoder().encode(AnchorRef.rediffSlotChroma))
        #expect(object["type"] == "rediffSlotChroma")
        #expect(object.count == 1, "bare case: no associated values, got \(object)")

        let byteObject = try JSONDecoder().decode(
            [String: String].self,
            from: try JSONEncoder().encode(AnchorRef.rediffSlot))
        #expect(byteObject["type"] == "rediffSlot")
        #expect(object["type"] != byteObject["type"],
                "a shared type string with a discriminating FIELD would decode as byte-exact on an older binary — the unsafe direction")
    }

    @Test("chroma round-trips, equals itself, and differs from every sibling (default:false trap closed)")
    func chromaEqualityAndRoundTrip() throws {
        #expect(AnchorRef.rediffSlotChroma == AnchorRef.rediffSlotChroma)
        #expect(AnchorRef.rediffSlotChroma != AnchorRef.rediffSlot)
        #expect(AnchorRef.rediffSlot != AnchorRef.rediffSlotChroma)
        for other in Self.allCases where other != .rediffSlotChroma {
            #expect(AnchorRef.rediffSlotChroma != other)
            #expect(other != AnchorRef.rediffSlotChroma)
        }
        let decoded = try JSONDecoder().decode(
            AnchorRef.self, from: try JSONEncoder().encode(AnchorRef.rediffSlotChroma))
        #expect(decoded == .rediffSlotChroma)
    }

    @Test("what an OLDER binary sees: an unknown bare type is DROPPED, never mistaken for a known one")
    func olderBinarySeesTheElementDropped() throws {
        // A binary predating this case decodes `anchorProvenanceJSON` through
        // `LossyAnchorRef` + `compactMap`, exactly as `AnalysisStore` does.
        // Simulated with a type string THIS binary also does not know, because
        // that is precisely the position an older binary is in w.r.t.
        // "rediffSlotChroma".
        let json = Data(#"[{"type":"rediffSlot"},{"type":"aFutureWidthOracle"}]"#.utf8)
        let wrapped = try JSONDecoder().decode([LossyAnchorRef].self, from: json)
        let survivors = wrapped.compactMap(\.value)
        #expect(survivors == [.rediffSlot],
                "the unknown element is dropped; the array still decodes")
        // The consequence, stated: the older binary sees a span with NO marker
        // for that element — so it loses width ownership and every certainty
        // carve-out for it. Under-claiming, which is the safe direction.
        let downgraded = span(survivors.filter { $0 != .rediffSlot })
        #expect(!downgraded.carriesRediffByteExactWidth)
        #expect(!downgraded.carriesRediffChromaWidth)
        #expect(!downgraded.anchorProvenance.contains { $0.isWidthOwnership })
    }

    @Test("the provenance discriminator matches the Codable type string")
    func provenanceKindMatchesCodable() throws {
        for anchor in Self.allCases {
            let object = try JSONDecoder().decode(
                [String: AnyCodableProbe].self, from: try JSONEncoder().encode(anchor))
            #expect(object["type"]?.stringValue == anchor.provenanceKind,
                    "discriminator drift for \(anchor.provenanceKind)")
        }
    }

    // MARK: - Exhaustiveness

    @Test("every AnchorRef case is represented in this suite's sweep")
    func everyAnchorCaseIsRepresented() {
        // `AnchorRef` is deliberately NOT `CaseIterable`, so nothing else forces
        // `allCases` to stay complete. This switch does: a new case fails to
        // compile here, which is the point at which someone must decide what
        // extent tier it earns — the decision that was skipped for chroma.
        var seen = Set<String>()
        for anchor in Self.allCases { seen.insert(anchor.provenanceKind) }
        let probe = AnchorRef.rediffSlotChroma
        let expectedKind: String
        switch probe {
        case .fmConsensus: expectedKind = "fmConsensus"
        case .evidenceCatalog: expectedKind = "evidenceCatalog"
        case .fmAcousticCorroborated: expectedKind = "fmAcousticCorroborated"
        case .userCorrection: expectedKind = "userCorrection"
        case .classifierSeed: expectedKind = "classifierSeed"
        case .sustainedMusicOffset: expectedKind = "sustainedMusicOffset"
        case .spliceSlot: expectedKind = "spliceSlot"
        case .rediffSlot: expectedKind = "rediffSlot"
        case .rediffSlotChroma: expectedKind = "rediffSlotChroma"
        }
        #expect(expectedKind == "rediffSlotChroma")
        #expect(seen.count == Self.allCases.count, "duplicate discriminator in allCases")
        #expect(seen.count == 9, "AnchorRef gained or lost a case — extend `allCases` and decide its extent tier")
    }
}

/// Minimal probe so the discriminator sweep can read `type` out of encodings
/// that also carry non-`String` associated values.
private struct AnyCodableProbe: Decodable {
    let stringValue: String?
    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        stringValue = try? container.decode(String.self)
    }
}
