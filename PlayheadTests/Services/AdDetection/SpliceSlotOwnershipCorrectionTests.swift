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
//   (iii) v1 PINNED: absorption ORPHANS an .exactSpan veto on the absorbed span.
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

    @Test("(iii) v1 PINNED: absorption ORPHANS an .exactSpan veto on the absorbed span")
    func absorptionOrphansExactSpanVeto() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-corr-orphan"
        try await store.insertAsset(corrAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)

        // A kept slot (absorber) that fully encloses a smaller absorbee span.
        let absorber = corrSpan(assetId: assetId, first: 0, last: 4, start: 0, end: 50)
        let absorbee = corrSpan(assetId: assetId, first: 2, last: 2, start: 20, end: 30)
        try await store.upsertDecodedSpans([absorber, absorbee])

        // User vetoes the (soon-to-be-absorbed) absorbee → .exactSpan(asset, 2...2).
        await corrections.recordVeto(span: absorbee)

        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: [absorber, absorbee],
            dispositions: [.keepSlot(corrSlot(0, 50)), .absorbed(absorberIndex: 0)],
            atomEvidence: corrAtoms
        )
        #expect(rewrite.absorbedIds.contains(absorbee.id))
        try await reconcile(rewrite, store: store)

        let persisted = try await store.fetchDecodedSpans(assetId: assetId)
        // The absorbed span's row is gone…
        #expect(!persisted.contains { $0.id == absorbee.id })
        // …and NO surviving span carries the absorbee's exact ordinal identity
        // (2...2): the correction's target span no longer exists → ORPHANED.
        #expect(!persisted.contains { $0.firstAtomOrdinal == 2 && $0.lastAtomOrdinal == 2 })

        // v1 limitation, pinned: the correction EVENT still persists and still
        // counts asset-wide (a not-an-ad gesture on the absorbee would even leak
        // onto the surviving absorber). The forward path — routing not-an-ad
        // corrections into the resolver's vetoed-time-ranges so
        // `.vetoNewlyEnclosed` blocks the absorption — is the fix a real
        // CorrectionMaskProvider will bring; today the gesture is orphaned.
        let events = try await corrections.activeCorrections(for: assetId)
        #expect(events.compactMap(exactSpanRange).contains(2...2))
        let factor = await corrections.correctionPassthroughFactor(for: assetId)
        #expect(factor < 1.0)
    }
}
