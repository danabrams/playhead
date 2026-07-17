// UserCorrectionReadSideTests.swift
// playhead-xsdz.34: the user-correction READ side (mechanism B — the per-atom /
// per-span `.userVetoed` mask). These cover the design's T1–T4, T7 and T9:
//   T1 projector unit    — vetoed ordinal → `.userVetoed` + `isAnchored=false`
//   T2 adapter unit      — exactSpan → ordinals; exactTimeSpan → atoms; clamp
//   T3 integration       — persisted `.exactSpan` veto → decoder splits + vetoedRanges
//   T4 flag-off identity — flag-off / nil-store ⇒ NoCorrectionMaskProvider parity
//   T7 rediff gate       — a rediff-widened slot that would newly enclose a veto
//                          is blocked (.vetoNewlyEnclosed) → status-quo width
//   T9 boost exclusion   — the mask path never reads `.falseNegative` / boost
//
// The OVERTURN test (T5) lives in SpliceSlotOwnershipCorrectionTests; the
// resolver-seam gate (T6) is SpliceSlotResolverTests.vetoNewlyEnclosed; the
// act-alone / never-act-alone guards (T8) live in BroadCorrectionEvaluatorTests.

import Foundation
import Testing

@testable import Playhead

// MARK: - Shared builders

private enum ReadSide {
    /// Atom stream: ordinals 0..<count at [0,10),[10,20),… (10 s per atom).
    static func atoms(count: Int, assetId: String = "asset-readside") -> [TranscriptAtom] {
        (0..<count).map { i in
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: assetId,
                    transcriptVersion: "tv1",
                    atomOrdinal: i
                ),
                contentHash: "hash-\(i)",
                startTime: Double(i) * 10,
                endTime: Double(i) * 10 + 10,
                text: "word \(i)",
                chunkIndex: i
            )
        }
    }

    /// FM-consensus (strength .medium) bundle covering `first...last` — anchors
    /// those atoms so a veto can be shown to un-anchor them.
    static func fmBundle(
        first: Int,
        last: Int,
        assetId: String = "asset-readside"
    ) -> RegionFeatureBundle {
        let region = ProposedRegion(
            analysisAssetId: assetId,
            transcriptVersion: "tv1",
            firstAtomOrdinal: first,
            lastAtomOrdinal: last,
            startTime: Double(first) * 10,
            endTime: Double(last) * 10 + 10,
            origins: .foundationModel,
            fmConsensusStrength: .medium,
            lexicalCandidates: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            acousticBreaks: [],
            foundationModelSpans: [],
            resolvedEvidenceAnchors: [],
            fmEvidence: nil
        )
        return RegionFeatureBundle(
            region: region,
            lexicalScore: 0.5,
            lexicalHitCount: 0,
            lexicalCategories: [],
            lexicalEvidenceText: nil,
            rmsDropScore: 0,
            spectralChangeScore: 0,
            musicScore: 0,
            speakerChangeScore: 0,
            priorScore: 0,
            transcriptQuality: RegionTranscriptQuality(
                quality: .good, qualityScore: 1.0, source: .heuristic
            ),
            fmEvidence: RegionFeatureFMEvidence(
                commercialIntent: .paid,
                ownership: .unknown,
                consensusStrength: .medium,
                certainty: .moderate,
                boundaryPrecision: .usable,
                memoryWriteEligible: false,
                alternativeExplanation: .none,
                reasonTags: [],
                resolvedEvidenceAnchors: []
            )
        )
    }

    static let emptyCatalog = EvidenceCatalog(
        analysisAssetId: "asset-readside", transcriptVersion: "tv1", entries: []
    )
}

// MARK: - T2 — StoreBackedCorrectionMaskProvider adapter unit

@Suite("xsdz.34 T2 — StoreBackedCorrectionMaskProvider adapter")
struct StoreBackedCorrectionMaskProviderTests {

    private let atoms = ReadSide.atoms(count: 5)

    @Test(".exactSpan(2...3) masks ordinals 2 and 3 only")
    func exactSpanMasksOrdinals() async {
        let provider = StoreBackedCorrectionMaskProvider(
            fromScopes: [.exactSpan(assetId: "asset-readside", ordinalRange: 2...3)],
            atoms: atoms
        )
        let masks = await provider.correctionMasks(for: 0...4, in: "asset-readside")
        #expect(masks[2] == .userVetoed)
        #expect(masks[3] == .userVetoed)
        #expect(masks[0] == nil)
        #expect(masks[1] == nil)
        #expect(masks[4] == nil)
    }

    @Test(".exactTimeSpan(20,30) masks atoms overlapping [20,30) via the time index")
    func exactTimeSpanMasksAtoms() async {
        // Atom 2 is [20,30). Its half-open span overlaps the vetoed [20,30).
        let provider = StoreBackedCorrectionMaskProvider(
            fromScopes: [.exactTimeSpan(assetId: "asset-readside", startTime: 20, endTime: 30)],
            atoms: atoms
        )
        let masks = await provider.correctionMasks(for: 0...4, in: "asset-readside")
        #expect(masks[2] == .userVetoed)
        // Touching-only neighbours (atom 1 ends at 20, atom 3 starts at 30) do
        // NOT overlap (positive-duration convention).
        #expect(masks[1] == nil)
        #expect(masks[3] == nil)
    }

    @Test(".exactTimeSpan spanning two atoms masks both")
    func exactTimeSpanTwoAtoms() async {
        // [15,35) overlaps atom 1 [10,20), atom 2 [20,30), atom 3 [30,40).
        let provider = StoreBackedCorrectionMaskProvider(
            fromScopes: [.exactTimeSpan(assetId: "asset-readside", startTime: 15, endTime: 35)],
            atoms: atoms
        )
        let masks = await provider.correctionMasks(for: 0...4, in: "asset-readside")
        #expect(masks[1] == .userVetoed)
        #expect(masks[2] == .userVetoed)
        #expect(masks[3] == .userVetoed)
        #expect(masks[0] == nil)
        #expect(masks[4] == nil)
    }

    @Test("show-wide scopes contribute no mask (Layer-A span/time only)")
    func showWideScopesIgnored() async {
        let provider = StoreBackedCorrectionMaskProvider(
            fromScopes: [
                .sponsorOnShow(podcastId: "p", sponsor: "acme"),
                .phraseOnShow(podcastId: "p", phrase: "brought to you by"),
                .campaignOnShow(podcastId: "p", campaign: "c"),
            ],
            atoms: atoms
        )
        let masks = await provider.correctionMasks(for: 0...4, in: "asset-readside")
        #expect(masks.isEmpty)
    }

    @Test("requested-range clamping: out-of-range vetoed ordinals mask nothing")
    func requestedRangeClamps() async {
        // Veto 7...9 but request only 0...4 → no overlap → nothing masked.
        let provider = StoreBackedCorrectionMaskProvider(
            fromScopes: [.exactSpan(assetId: "asset-readside", ordinalRange: 7...9)],
            atoms: atoms
        )
        let masks = await provider.correctionMasks(for: 0...4, in: "asset-readside")
        #expect(masks.isEmpty)
    }

    @Test("partial overlap of a vetoed ordinal range clamps to the requested window")
    func partialOrdinalOverlapClamps() async {
        // Veto 3...9; request 0...4 → only 3 and 4 masked.
        let provider = StoreBackedCorrectionMaskProvider(
            fromScopes: [.exactSpan(assetId: "asset-readside", ordinalRange: 3...9)],
            atoms: atoms
        )
        let masks = await provider.correctionMasks(for: 0...4, in: "asset-readside")
        #expect(masks[3] == .userVetoed)
        #expect(masks[4] == .userVetoed)
        #expect(masks[2] == nil)
        #expect(masks.count == 2)
    }
}

// MARK: - T1 — projector with a real StoreBackedCorrectionMaskProvider

@Suite("xsdz.34 T1 — projector applies the veto mask")
struct ReadSideProjectorTests {

    @Test("vetoed ordinal → .userVetoed AND isAnchored=false; neighbours clean")
    func vetoedOrdinalUnanchored() async {
        let atoms = ReadSide.atoms(count: 5)
        // Anchor all of 0..4 via an FM consensus region.
        let bundle = ReadSide.fmBundle(first: 0, last: 4)
        // Veto ordinal 2 (its atom is [20,30)).
        let provider = StoreBackedCorrectionMaskProvider(
            fromScopes: [.exactSpan(assetId: "asset-readside", ordinalRange: 2...2)],
            atoms: atoms
        )
        let evidence = await AtomEvidenceProjector().project(
            regions: [bundle],
            catalog: ReadSide.emptyCatalog,
            atoms: atoms,
            correctionMaskProvider: provider
        )
        #expect(evidence[2].correctionMask == .userVetoed)
        #expect(!evidence[2].isAnchored, "a vetoed atom is never anchored even under an FM region")
        // Neighbours stay anchored + unmasked.
        for i in [0, 1, 3, 4] {
            #expect(evidence[i].correctionMask == .none)
            #expect(evidence[i].isAnchored)
        }
    }
}

// MARK: - T3 — end-to-end read path through the real store + decoder

@Suite("xsdz.34 T3 — persisted veto reaches the decoder + vetoedRanges")
struct ReadSideIntegrationTests {

    @Test(".exactSpan veto splits the decoded span AND yields a non-empty vetoedRanges")
    func exactSpanVetoReachesDetection() async throws {
        let assetId = "asset-readside"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)

        let atoms = ReadSide.atoms(count: 5, assetId: assetId)

        // Persist an .exactSpan veto on ordinal 2 through the REAL write path.
        let vetoedSpan = DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 2, lastAtomOrdinal: 2),
            assetId: assetId, firstAtomOrdinal: 2, lastAtomOrdinal: 2,
            startTime: 20, endTime: 30,
            anchorProvenance: [.classifierSeed(regionId: "r", score: 0.9)]
        )
        await corrections.recordVeto(span: vetoedSpan)

        // READ side: pull the active FP scopes and build the provider.
        let scopes = await corrections.activeFalsePositiveScopes(for: assetId)
        #expect(scopes.contains(.exactSpan(assetId: assetId, ordinalRange: 2...2)))
        let provider = StoreBackedCorrectionMaskProvider(fromScopes: scopes, atoms: atoms)

        // Project with all atoms anchored (FM region 0..4) so the ONLY reason a
        // span would exclude ordinal 2 is the veto.
        let evidence = await AtomEvidenceProjector().project(
            regions: [ReadSide.fmBundle(first: 0, last: 4, assetId: assetId)],
            catalog: EvidenceCatalog(analysisAssetId: assetId, transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: provider
        )

        // Decoder omits/splits at the vetoed atom: no decoded span covers ordinal 2.
        let spans = MinimalContiguousSpanDecoder().decode(atoms: evidence, assetId: assetId)
        let coversVetoed = spans.contains { $0.firstAtomOrdinal <= 2 && $0.lastAtomOrdinal >= 2 }
        #expect(!coversVetoed, "the vetoed atom is excluded from every decoded span")

        // computeSpliceSlotPass-equivalent vetoedRanges is non-empty and covers
        // the vetoed atom's time span [20,30).
        let vetoedRanges = evidence
            .filter { $0.correctionMask == .userVetoed }
            .map { TimeRange(start: $0.startTime, end: $0.endTime) }
        #expect(vetoedRanges.count == 1)
        #expect(vetoedRanges.first == TimeRange(start: 20, end: 30))
    }
}

// MARK: - T4 — flag-off / nil-store identity with NoCorrectionMaskProvider

@Suite("xsdz.34 T4 — flag-off / nil-store is byte-identical to NoCorrectionMaskProvider")
struct ReadSideFlagIdentityTests {

    @Test("flag OFF selects NoCorrectionMaskProvider even when the store has a veto")
    func flagOffIgnoresStore() async throws {
        let assetId = "asset-readside"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)
        let atoms = ReadSide.atoms(count: 5, assetId: assetId)
        await corrections.recordVeto(span: DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 2, lastAtomOrdinal: 2),
            assetId: assetId, firstAtomOrdinal: 2, lastAtomOrdinal: 2,
            startTime: 20, endTime: 30,
            anchorProvenance: [.classifierSeed(regionId: "r", score: 0.9)]
        ))

        let provider = await AdDetectionService.makeCorrectionMaskProvider(
            enabled: false, store: corrections, analysisAssetId: assetId, atoms: atoms
        )
        #expect(provider is NoCorrectionMaskProvider)
        // Byte-identical to NoCorrectionMaskProvider: no atom masked despite the veto.
        let masks = await provider.correctionMasks(for: 0...4, in: assetId)
        let reference = await NoCorrectionMaskProvider().correctionMasks(for: 0...4, in: assetId)
        #expect(masks == reference)
        #expect(masks.isEmpty)
    }

    @Test("nil store selects NoCorrectionMaskProvider even when flag ON")
    func nilStoreIsNoOp() async {
        let atoms = ReadSide.atoms(count: 5)
        let provider = await AdDetectionService.makeCorrectionMaskProvider(
            enabled: true, store: nil, analysisAssetId: "asset-readside", atoms: atoms
        )
        #expect(provider is NoCorrectionMaskProvider)
    }

    @Test("flag ON + store selects a populated StoreBackedCorrectionMaskProvider")
    func flagOnUsesStore() async throws {
        let assetId = "asset-readside"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)
        let atoms = ReadSide.atoms(count: 5, assetId: assetId)
        await corrections.recordVeto(span: DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 2, lastAtomOrdinal: 2),
            assetId: assetId, firstAtomOrdinal: 2, lastAtomOrdinal: 2,
            startTime: 20, endTime: 30,
            anchorProvenance: [.classifierSeed(regionId: "r", score: 0.9)]
        ))

        let provider = await AdDetectionService.makeCorrectionMaskProvider(
            enabled: true, store: corrections, analysisAssetId: assetId, atoms: atoms
        )
        #expect(provider is StoreBackedCorrectionMaskProvider)
        let masks = await provider.correctionMasks(for: 0...4, in: assetId)
        #expect(masks[2] == .userVetoed)
    }
}

// MARK: - T7 — the rediff veto gate (§5)

@Suite("xsdz.34 T7 — rediff candidate path honours the veto gate")
struct ReadSideRediffGateTests {

    private static func played(_ start: Double, _ end: Double) -> RediffSlotOwnership.PlayedSlot {
        RediffSlotOwnership.PlayedSlot(startSeconds: start, endSeconds: end, leftRunSeconds: 60, rightRunSeconds: 60)
    }

    private static func atom(_ ordinal: Int, _ start: Double, _ end: Double) -> AtomEvidence {
        AtomEvidence(
            atomOrdinal: ordinal, startTime: start, endTime: end,
            isAnchored: true, anchorProvenance: [], hasAcousticBreakHint: false, correctionMask: .none
        )
    }

    private static func span(_ start: Double, _ end: Double, _ first: Int, _ last: Int) -> DecodedSpan {
        DecodedSpan(
            id: "s-\(first)-\(last)", assetId: "asset-1",
            firstAtomOrdinal: first, lastAtomOrdinal: last,
            startTime: start, endTime: end, anchorProvenance: []
        )
    }

    @Test("resolveSpan: a wide slot NEWLY enclosing a veto → nil, .vetoNewlyEnclosed")
    func resolveSpanBlocksNewlyEnclosedVeto() {
        // Narrow core [40,50]; wide slot [0,50] would widen it ~5×. Veto [10,20]
        // is inside the slot but OUTSIDE the core → newly enclosed → blocked.
        let (slot, diag) = RediffSlotOwnership.resolveSpan(
            core: TimeRange(start: 40, end: 50),
            playedSlots: [Self.played(0, 50)],
            vetoedRanges: [TimeRange(start: 10, end: 20)]
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .vetoNewlyEnclosed)
        #expect(diag.bestGeometryValidPair != nil, "the blocked slot is surfaced for shadow visibility")
    }

    @Test("resolveSpan: baseline (no veto) widens the narrow core")
    func resolveSpanBaselineWidens() {
        let (slot, diag) = RediffSlotOwnership.resolveSpan(
            core: TimeRange(start: 40, end: 50),
            playedSlots: [Self.played(0, 50)],
            vetoedRanges: []
        )
        #expect(slot?.startTime == 0 && slot?.endTime == 50)
        #expect(diag.failureReason == nil)
    }

    @Test("resolveSpan: a veto the core ALREADY intersects does not fire")
    func resolveSpanVetoInsideCoreAllowed() {
        // Core [10,50]; slot [0,50]; veto [20,30] is inside the core → allowed.
        let (slot, diag) = RediffSlotOwnership.resolveSpan(
            core: TimeRange(start: 10, end: 50),
            playedSlots: [Self.played(0, 50)],
            vetoedRanges: [TimeRange(start: 20, end: 30)]
        )
        #expect(slot != nil)
        #expect(diag.failureReason == nil)
    }

    @Test("candidates: the veto blocks the widening → status-quo width (no absorption)")
    func candidatesBlockedByVeto() {
        // Only the narrow true-ad core is a decoded span; the vetoed region is
        // not a competing span (Part 1 un-anchors it upstream). The wide slot
        // would widen [40,50]→[0,50] but the veto blocks it.
        let spans = [Self.span(40, 50, 4, 4)]
        let atoms = [Self.atom(4, 40, 50)]
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: spans,
            atomEvidence: atoms,
            playedSlots: [Self.played(0, 50)],
            vetoedRanges: [TimeRange(start: 10, end: 20)],
            coreBankMatch: [false],
            slotBankMatch: [false]
        )
        #expect(bundle.synthesizedSlots[0] == nil, "veto blocks the rediff widening")
        #expect(bundle.diagnostics[0].failureReason == .vetoNewlyEnclosed)

        // End-to-end: nil slot → .noSlot disposition → the rewrite leaves the
        // core at its minted width (no widening, no absorption).
        let result = SpliceSlotDispositionEngine.computeDispositions(bundle.candidates)
        #expect(result.dispositions[0] == .noSlot)
        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: spans, dispositions: result.dispositions,
            atomEvidence: atoms, provenance: .rediffSlot
        )
        #expect(rewrite.finalSpans.count == 1)
        #expect(rewrite.finalSpans[0].startTime == 40 && rewrite.finalSpans[0].endTime == 50)
        #expect(rewrite.absorbedIds.isEmpty)
    }

    @Test("candidates: WITHOUT the veto the same slot widens the core (proves the gate is load-bearing)")
    func candidatesWidenWithoutVeto() {
        let spans = [Self.span(40, 50, 4, 4)]
        let atoms = [Self.atom(4, 40, 50)]
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: spans,
            atomEvidence: atoms,
            playedSlots: [Self.played(0, 50)],
            vetoedRanges: [],
            coreBankMatch: [false],
            slotBankMatch: [false]
        )
        #expect(bundle.synthesizedSlots[0]?.startTime == 0)
        #expect(bundle.synthesizedSlots[0]?.endTime == 50)
    }
}

// MARK: - T9 — the mask path never enters the boost direction

@Suite("xsdz.34 T9 — boost / falseNegative never reaches the veto mask")
struct ReadSideBoostExclusionTests {

    @Test("activeFalsePositiveScopes excludes a .falseNegative correction")
    func falseNegativeExcludedFromScopes() async throws {
        let assetId = "asset-fn"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)

        // A false-negative "missed ad here" report → synthetic negative-ordinal
        // span + a .falseNegative correction event.
        try await corrections.recordFalseNegative(assetId: assetId, reportedTime: 100)
        // And an explicit false-positive veto so the FP path is non-empty.
        await corrections.recordVeto(span: DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 2, lastAtomOrdinal: 2),
            assetId: assetId, firstAtomOrdinal: 2, lastAtomOrdinal: 2,
            startTime: 20, endTime: 30,
            anchorProvenance: [.classifierSeed(regionId: "r", score: 0.9)]
        ))

        let scopes = await corrections.activeFalsePositiveScopes(for: assetId)
        // The FP veto is present…
        #expect(scopes.contains(.exactSpan(assetId: assetId, ordinalRange: 2...2)))
        // …and NO synthetic negative-ordinal (falseNegative) scope leaked in.
        let hasNegativeOrdinal = scopes.contains { scope in
            if case .exactSpan(_, let range) = scope { return range.lowerBound < 0 }
            return false
        }
        #expect(!hasNegativeOrdinal, "boost/.falseNegative scopes never reach the veto mask")
    }

    @Test("correctionBoostFactor stays multiplicative, capped at 2.0 (unchanged by the read side)")
    func boostFactorUnchanged() async throws {
        let assetId = "asset-boost"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)

        // No corrections → no boost.
        let none = await corrections.correctionBoostFactor(for: assetId)
        #expect(none == 1.0)

        // A false-negative report boosts, but never above the 2.0 cap.
        try await corrections.recordFalseNegative(assetId: assetId, reportedTime: 100)
        let boosted = await corrections.correctionBoostFactor(for: assetId)
        #expect(boosted > 1.0)
        #expect(boosted <= 2.0)
    }
}
