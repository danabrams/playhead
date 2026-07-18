// AnchorRefSustainedMusicOffsetTests.swift
// playhead-t1py / playhead-xtpf: plumbing + gate coverage for the
// `AnchorRef.sustainedMusicOffset(regionId:confidence:)` PRESENCE anchor.
//
// MIRRORS AnchorRefRediffSlotTests for the shared enum-plumbing failure modes
// (the manual `Equatable` default:false trap, the Codable discriminator, the
// LossyAnchorRef rollback drop) — but `.sustainedMusicOffset` is a PRESENCE
// anchor with associated values, NOT a bare width marker, so it adds the
// load-bearing DECISION contract: a span anchored ONLY by music must never
// auto-skip (`DecisionMapper` demotes it to `.markOnly`).

import Foundation
import Testing

@testable import Playhead

@Suite("AnchorRef.sustainedMusicOffset plumbing + markOnly gate")
struct AnchorRefSustainedMusicOffsetTests {

    private static let sample = AnchorRef.sustainedMusicOffset(regionId: "r-music", confidence: 0.85)

    /// One representative value of every OTHER case, for `!=` coverage.
    private static let otherCases: [AnchorRef] = [
        .fmConsensus(regionId: "r1", consensusStrength: 0.9),
        .evidenceCatalog(entry: EvidenceEntry(
            evidenceRef: 7, category: .url, matchedText: "acme.com",
            normalizedText: "acme.com", atomOrdinal: 3, startTime: 3.0, endTime: 3.5
        )),
        .fmAcousticCorroborated(regionId: "r2", breakStrength: 0.7),
        .userCorrection(correctionId: "c1", reportedTime: 12.0),
        .classifierSeed(regionId: "r3", score: 0.8),
        .spliceSlot,
        .rediffSlot,
    ]

    // MARK: - Equatable (default:false trap)

    @Test("sustainedMusicOffset == itself (default:false trap closed)")
    func equalsItself() {
        #expect(Self.sample == .sustainedMusicOffset(regionId: "r-music", confidence: 0.85))
    }

    @Test("sustainedMusicOffset differs on regionId AND on confidence")
    func differsOnPayload() {
        #expect(Self.sample != .sustainedMusicOffset(regionId: "other", confidence: 0.85))
        #expect(Self.sample != .sustainedMusicOffset(regionId: "r-music", confidence: 0.5))
    }

    @Test("sustainedMusicOffset != every other AnchorRef case")
    func notEqualToOthers() {
        for other in Self.otherCases {
            #expect(Self.sample != other, "must differ from \(other)")
            #expect(other != Self.sample, "== must be symmetric for \(other)")
        }
    }

    // MARK: - isWidthOwnership (presence, not a width oracle)

    @Test("isWidthOwnership is false — sustainedMusicOffset is PRESENCE evidence, not a width marker")
    func isNotWidthOwnership() {
        #expect(!Self.sample.isWidthOwnership)
    }

    // MARK: - Codable

    @Test("sustainedMusicOffset Codable round-trips to itself")
    func codableRoundTrip() throws {
        let data = try JSONEncoder().encode(Self.sample)
        let decoded = try JSONDecoder().decode(AnchorRef.self, from: data)
        #expect(decoded == Self.sample)
    }

    @Test("sustainedMusicOffset encodes a STABLE 'sustainedMusicOffset' type string")
    func stableTypeString() throws {
        let data = try JSONEncoder().encode(Self.sample)
        let obj = try JSONDecoder().decode([String: JSONValue].self, from: data)
        #expect(obj["type"] == .string("sustainedMusicOffset"))
        #expect(obj["regionId"] == .string("r-music"))
        #expect(obj["confidence"] == .double(0.85))
    }

    @Test("provenanceKind matches the Codable type discriminator")
    func provenanceKindMatchesDiscriminator() {
        #expect(Self.sample.provenanceKind == "sustainedMusicOffset")
    }

    @Test("Array containing sustainedMusicOffset round-trips with neighbors intact")
    func arrayRoundTrip() throws {
        let provenance: [AnchorRef] = [
            .fmConsensus(regionId: "r1", consensusStrength: 0.5),
            .sustainedMusicOffset(regionId: "r-music", confidence: 0.77),
            .spliceSlot,
        ]
        let data = try JSONEncoder().encode(provenance)
        let decoded = try JSONDecoder().decode([AnchorRef].self, from: data)
        #expect(decoded == provenance)
        #expect(decoded[1] == .sustainedMusicOffset(regionId: "r-music", confidence: 0.77))
    }

    @Test("LossyAnchorRef drops an unknown-type element around a sustainedMusicOffset")
    func lossyDropsUnknownType() throws {
        let mixedJSON = """
        [
          {"type":"sustainedMusicOffset","regionId":"r","confidence":0.8},
          {"type":"futureUnknownCase"}
        ]
        """
        let wrapped = try JSONDecoder().decode([LossyAnchorRef].self, from: Data(mixedJSON.utf8))
        let survivors = wrapped.compactMap(\.value)
        #expect(survivors.count == 1)
        #expect(survivors[0] == .sustainedMusicOffset(regionId: "r", confidence: 0.8))
    }

    // MARK: - Decision gate: music-only ⇒ markOnly (Decision #4)

    /// Build the eligibility gate for a span with the given provenance + ledger.
    private func gate(
        provenance: [AnchorRef],
        ledger: [EvidenceLedgerEntry],
        certaintyTieredEnabled: Bool = false
    ) -> SkipEligibilityGate {
        // 30s span (within the [5,180] quorum window).
        let span = DecodedSpan(
            id: "gate-span",
            assetId: "gate-asset",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: 10.0,
            endTime: 40.0,
            anchorProvenance: provenance
        )
        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(certaintyTieredEnabled: certaintyTieredEnabled),
            transcriptQuality: .good
        )
        return mapper.map().eligibilityGate
    }

    private let acousticLedger: [EvidenceLedgerEntry] = [
        EvidenceLedgerEntry(source: .acoustic, weight: 0.2, detail: .acoustic(breakStrength: 0.5))
    ]

    @Test("a music-ONLY span is demoted from .eligible to .markOnly (never auto-skip)")
    func musicOnlySpanIsMarkOnly() {
        // Sanity: an identical span with a corroborating in-audio anchor (no
        // music) reaches .eligible via the non-FM metadata-corroboration gate.
        let baseline = gate(
            provenance: [.classifierSeed(regionId: "r", score: 0.9)],
            ledger: acousticLedger
        )
        #expect(baseline == .eligible, "sanity: a corroborated non-FM span is eligible")

        // The music-only span takes the same eligible path, then the Decision #4
        // clause demotes it to markOnly.
        let musicOnly = gate(
            provenance: [.sustainedMusicOffset(regionId: "r", confidence: 0.9)],
            ledger: acousticLedger
        )
        #expect(musicOnly == .markOnly, "music-only presence must never auto-skip")
    }

    @Test("music + a corroborating FM anchor is NOT demoted (stays eligible)")
    func musicPlusFMStaysEligible() {
        // fmConsensus gate needs 2+ distinct corroborating kinds.
        let ledger: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .lexical, weight: 0.2, detail: .lexical(matchedCategories: ["cta"])),
            EvidenceLedgerEntry(source: .acoustic, weight: 0.2, detail: .acoustic(breakStrength: 0.5)),
        ]
        let g = gate(
            provenance: [
                .fmConsensus(regionId: "r", consensusStrength: 0.9),
                .sustainedMusicOffset(regionId: "r", confidence: 0.9),
            ],
            ledger: ledger
        )
        #expect(g == .eligible, "an FM-corroborated span is not music-only → not demoted")
    }

    @Test("music + a corroborating evidence-catalog anchor is NOT demoted (stays eligible)")
    func musicPlusCatalogStaysEligible() {
        let g = gate(
            provenance: [
                .sustainedMusicOffset(regionId: "r", confidence: 0.9),
                .evidenceCatalog(entry: EvidenceEntry(
                    evidenceRef: 1, category: .promoCode, matchedText: "SAVE10",
                    normalizedText: "save10", atomOrdinal: 2, startTime: 2.0, endTime: 2.4
                )),
            ],
            ledger: acousticLedger
        )
        #expect(g == .eligible)
    }

    @Test("music + a bare width marker (rediffSlot) is STILL music-only → markOnly")
    func musicPlusWidthMarkerIsStillMarkOnly() {
        // A width marker sets WIDTH, not PRESENCE — it does not corroborate the
        // music hint, so the span remains music-only and must not auto-skip.
        let g = gate(
            provenance: [
                .sustainedMusicOffset(regionId: "r", confidence: 0.9),
                .rediffSlot,
            ],
            ledger: acousticLedger
        )
        #expect(g == .markOnly)
    }
}

/// Minimal JSON value for asserting the exact encoded shape of an AnchorRef
/// without over-constraining number formatting.
private enum JSONValue: Decodable, Equatable {
    case string(String)
    case double(Double)
    case other

    init(from decoder: Decoder) throws {
        let c = try decoder.singleValueContainer()
        if let s = try? c.decode(String.self) { self = .string(s) }
        else if let d = try? c.decode(Double.self) { self = .double(d) }
        else { self = .other }
    }
}
