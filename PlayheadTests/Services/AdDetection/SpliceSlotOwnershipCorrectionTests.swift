// SpliceSlotOwnershipCorrectionTests.swift
// playhead-xsdz.20 (Bead B): the pinned USER CORRECTIONS obligations.
//
// These exercise how the slot pass's ordinal REWRITE + persistence reconcile
// (SpliceSlotRewriter + AnalysisStore.deleteDecodedSpans(ids:)/upsertDecodedSpans)
// interact with ordinal-scoped `CorrectionScope.exactSpan` corrections recorded
// through the real `PersistentUserCorrectionStore`:
//   (i)   an .exactSpan veto on a SLOT-OWNED (kept, rewritten) span still
//         resolves next run after the ordinal recompute;
//   (ii)  the synthetic-span path (negative ordinals) is untouched by the pass;
//   (iii-v2) OVERTURNED (playhead-xsdz.34): a veto on a would-be-WIDENED region
//         BLOCKS the widening (.vetoNewlyEnclosed) instead of orphaning the
//         gesture — the vetoed span survives, the widened span is never
//         persisted. This replaces the v1 pinned-orphan behavior; see
//         docs/xsdz34-correction-readside-design.md §3.
//
// ARCHITECTURE NOTE (drives the assertions): a `.falsePositive` .exactSpan
// correction resolves ASSET-WIDE via `correctionPassthroughFactor(for:)` — it is
// keyed by assetId, not ordinalRange — so the rewrite never breaks the asset-wide
// suppression. The ordinalRange governs whether the correction still re-associates
// to a LIVE persisted span (its target identity). That re-association is what the
// rewrite can change, and what these tests pin.

import Foundation
import Testing

@testable import Playhead

// MARK: - Builders

private func corrAsset(id: String) -> AnalysisAsset {
    AnalysisAsset(
        id: id, episodeId: "ep-\(id)", assetFingerprint: "fp-\(id)",
        weakFingerprint: nil, sourceURL: "file:///tmp/\(id).m4a",
        featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil, analysisState: "new",
        analysisVersion: 1, capabilitySnapshot: nil
    )
}

private func corrAtomEv(_ ordinal: Int, _ start: Double, _ end: Double) -> AtomEvidence {
    AtomEvidence(
        atomOrdinal: ordinal, startTime: start, endTime: end,
        isAnchored: true,
        anchorProvenance: [.classifierSeed(regionId: "r", score: 0.9)],
        hasAcousticBreakHint: false, correctionMask: .none
    )
}

// Atom stream: ordinals 0..4 at [0,10),[10,20),…,[40,50).
private let corrAtoms: [AtomEvidence] = (0..<5).map { corrAtomEv($0, Double($0) * 10, Double($0) * 10 + 10) }

private func corrSpan(
    assetId: String, first: Int, last: Int, start: Double, end: Double
) -> DecodedSpan {
    DecodedSpan(
        id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: first, lastAtomOrdinal: last),
        assetId: assetId, firstAtomOrdinal: first, lastAtomOrdinal: last,
        startTime: start, endTime: end,
        anchorProvenance: [.classifierSeed(regionId: "r", score: 0.9)]
    )
}

private func corrSlot(_ start: Double, _ end: Double) -> SpliceSlot {
    SpliceSlot(
        startTime: start, endTime: end,
        startEdge: SpliceEdgeEvidence(time: start, stepScore: 0.5, contributingSignals: 1),
        endEdge: SpliceEdgeEvidence(time: end, stepScore: 0.5, contributingSignals: 1),
        slotConfidence: 0.5, coreCoverage: 1.0
    )
}

/// Replicates the slot pass's persistence reconcile exactly (see
/// `AdDetectionService.applySpliceSlotOwnershipPass`).
private func reconcile(_ rewrite: SpliceSlotRewriteResult, store: AnalysisStore) async throws {
    try await store.deleteDecodedSpans(ids: rewrite.supersededIds)
    if !rewrite.finalSpans.isEmpty {
        try await store.upsertDecodedSpans(rewrite.finalSpans)
    }
}

private func exactSpanRange(_ event: CorrectionEvent) -> ClosedRange<Int>? {
    guard case .exactSpan(_, let range)? = CorrectionScope.deserialize(event.scope) else { return nil }
    return range
}

// MARK: - Tests

@Suite("SpliceSlot ownership — user corrections (playhead-xsdz.20)")
struct SpliceSlotOwnershipCorrectionTests {

    @Test("(i) an .exactSpan veto on a slot-owned span still resolves after the ordinal recompute")
    func exactSpanVetoResolvesAfterSlotRewrite() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-corr-resolve"
        try await store.insertAsset(corrAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)

        // Decoder output: a narrow span over ordinal 2 only.
        let original = corrSpan(assetId: assetId, first: 2, last: 2, start: 20, end: 30)
        try await store.upsertDecodedSpans([original])

        // User vetoes it → .exactSpan(asset, 2...2), false-positive.
        await corrections.recordVeto(span: original)
        let recorded = try #require(
            try await corrections.activeCorrections(for: assetId).compactMap(exactSpanRange).first
        )
        #expect(recorded == 2...2)

        // Slot pass: the acoustic pair widens the span to [0,50] (ordinals 0..4,
        // a CHANGED-shape rewrite → new makeId), then re-persists.
        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: [original],
            dispositions: [.keepSlot(corrSlot(0, 50))],
            atomEvidence: corrAtoms
        )
        try await reconcile(rewrite, store: store)

        // "Resolves next run":
        //  (a) the asset-wide passthrough is UNAFFECTED by the ordinal recompute.
        let factor = await corrections.correctionPassthroughFactor(for: assetId)
        #expect(factor < 1.0)
        //  (b) the correction's recorded ordinals still fall WITHIN the surviving
        //      slot-owned span, so it re-associates to a live span.
        let persisted = try await store.fetchDecodedSpans(assetId: assetId)
        let survivor = try #require(persisted.first { $0.anchorProvenance.contains(.spliceSlot) })
        #expect(survivor.firstAtomOrdinal <= recorded.lowerBound)
        #expect(survivor.lastAtomOrdinal >= recorded.upperBound)
    }

    @Test("(ii) synthetic-span path (negative ordinals) is untouched by the slot-pass reconcile")
    func syntheticSpanUnaffectedBySlotPass() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-corr-synth"
        try await store.insertAsset(corrAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)

        // "Missed ad here" → synthetic span with NEGATIVE ordinals is persisted.
        try await corrections.recordFalseNegative(assetId: assetId, reportedTime: 200)
        let synthetic = try #require(
            try await store.fetchDecodedSpans(assetId: assetId).first { $0.firstAtomOrdinal < 0 }
        )

        // A real decoder span that the slot pass will rewrite (changed shape → the
        // reconcile's delete list is NON-empty, so this proves the delete cannot
        // collaterally remove the synthetic row).
        let real = corrSpan(assetId: assetId, first: 2, last: 2, start: 20, end: 30)
        try await store.upsertDecodedSpans([real])

        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: [real],   // slot pass processes ONLY the decoder output
            dispositions: [.keepSlot(corrSlot(0, 50))],
            atomEvidence: corrAtoms
        )
        #expect(!rewrite.supersededIds.isEmpty)               // real's id superseded…
        #expect(!rewrite.supersededIds.contains(synthetic.id)) // …but never the synthetic id
        try await reconcile(rewrite, store: store)

        // The synthetic span survives, byte-for-byte.
        let after = try await store.fetchDecodedSpans(assetId: assetId)
        let survivingSynthetic = try #require(after.first { $0.id == synthetic.id })
        #expect(survivingSynthetic == synthetic)
    }

    // v2 (playhead-xsdz.34): an .exactSpan veto against a WOULD-BE-widened region
    // now BLOCKS the widening instead of orphaning. Deliberate overturn of the v1
    // pinned-orphan behavior: routing the veto into vetoedRanges makes the width
    // oracle return .vetoNewlyEnclosed for the widening slot, so the narrow core
    // is kept and the vetoed region is never absorbed/skipped. See
    // docs/xsdz34-correction-readside-design.md §3.
    //
    // The width oracle here is the rediff path (`RediffSlotOwnership`) — the SOLE
    // production width setter (contract 2026-07-07) and the surface xsdz.36 /
    // auto-skip activate — so the §5 rediff veto gate is exercised end-to-end.
    // The acoustic resolver seam is pinned separately by
    // SpliceSlotResolverTests.vetoNewlyEnclosed (T6).
    @Test("(iii-v2) OVERTURNED: a veto on a newly-enclosed region blocks the widening (no orphan)")
    func vetoBlocksWideningNotOrphaned() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-corr-veto-blocks"
        try await store.insertAsset(corrAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)

        // Prior state: a NARROW true-ad core (ordinal 4, [40,50]) and a separate
        // span at ordinal 1 ([10,20]) the user vetoed as "not an ad". The width
        // oracle proposes a WIDE slot [0,50] that would widen the core ~5× and
        // absorb ordinal 1.
        let narrowCore = corrSpan(assetId: assetId, first: 4, last: 4, start: 40, end: 50)
        let vetoedSpan = corrSpan(assetId: assetId, first: 1, last: 1, start: 10, end: 20)
        try await store.upsertDecodedSpans([narrowCore, vetoedSpan])

        // User vetoes ordinal 1 → .exactSpan(asset, 1...1), false-positive.
        await corrections.recordVeto(span: vetoedSpan)
        let vetoedRanges = [TimeRange(start: 10, end: 20)]
        let playedSlots = [RediffSlotOwnership.PlayedSlot(
            startSeconds: 0, endSeconds: 50, leftRunSeconds: 60, rightRunSeconds: 60)]

        // BASELINE (the v1 danger): absent the veto, the wide slot resolves and
        // the core widens to [0,50] — the geometry that orphaned ordinal 1 in v1.
        let (slotNoVeto, _) = RediffSlotOwnership.resolveSpan(
            core: TimeRange(start: 40, end: 50), playedSlots: playedSlots, vetoedRanges: [])
        #expect(slotNoVeto?.startTime == 0 && slotNoVeto?.endTime == 50)

        // OVERTURN: the veto lands in the region the wide slot NEWLY encloses (the
        // core [40,50] does not touch [10,20]), so the widening is blocked.
        let (slotVetoed, diag) = RediffSlotOwnership.resolveSpan(
            core: TimeRange(start: 40, end: 50), playedSlots: playedSlots, vetoedRanges: vetoedRanges)
        #expect(slotVetoed == nil)
        #expect(diag.failureReason == .vetoNewlyEnclosed)

        // End-to-end: the vetoed atoms are un-anchored upstream (Part 1) so
        // ordinal 1 is not itself a width candidate this run — the width pass
        // sees only the narrow core. nil slot → .noSlot → the rewrite keeps the
        // core at minted width and absorbs nothing.
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: [narrowCore],
            atomEvidence: corrAtoms,
            playedSlots: playedSlots,
            vetoedRanges: vetoedRanges,
            coreBankMatch: [false],
            slotBankMatch: [false]
        )
        let result = SpliceSlotDispositionEngine.computeDispositions(bundle.candidates)
        #expect(result.dispositions[0] == .noSlot)
        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: [narrowCore], dispositions: result.dispositions,
            atomEvidence: corrAtoms, provenance: .rediffSlot
        )
        #expect(rewrite.absorbedIds.isEmpty)
        try await reconcile(rewrite, store: store)

        let persisted = try await store.fetchDecodedSpans(assetId: assetId)
        // The vetoed span at ordinal 1 SURVIVES (row not deleted) — the v1 orphan
        // is overturned.
        #expect(persisted.contains { $0.firstAtomOrdinal == 1 && $0.lastAtomOrdinal == 1 })
        // The narrow core stays at its minted width…
        #expect(persisted.contains { $0.firstAtomOrdinal == 4 && $0.startTime == 40 && $0.endTime == 50 })
        // …and the widened [0,50] span is NEVER persisted (no absorption).
        #expect(!persisted.contains { $0.startTime == 0 && $0.endTime == 50 })

        // Mechanism A still holds: the asset-wide passthrough is < 1.0.
        let events = try await corrections.activeCorrections(for: assetId)
        #expect(events.compactMap(exactSpanRange).contains(1...1))
        let factor = await corrections.correctionPassthroughFactor(for: assetId)
        #expect(factor < 1.0)
    }
}
