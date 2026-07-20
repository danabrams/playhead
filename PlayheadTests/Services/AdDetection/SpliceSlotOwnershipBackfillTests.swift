// SpliceSlotOwnershipBackfillTests.swift
// playhead-xsdz.20 (Bead B): integration coverage for the wiring of the
// splice-slot ownership pass into `runBackfill` — the flag-OFF byte-identity
// contract and the Phase-5 projector clobber guard. The pure disposition /
// rewrite semantics are covered in SpliceSlotDispositionTests.

import Foundation
import Testing

@testable import Playhead

// MARK: - Helpers

private func makeAsset(id: String) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: "ep-\(id)",
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///tmp/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
}

private func adChunks(assetId: String) -> [TranscriptChunk] {
    let texts = [
        "Welcome back to the show today.",
        "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Sign up today and make your website.",
        "Back to our conversation about technology and the future of podcasting."
    ]
    return texts.enumerated().map { idx, text in
        TranscriptChunk(
            id: "c\(idx)-\(assetId)",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(idx)",
            chunkIndex: idx,
            startTime: Double(idx) * 30,
            endTime: Double(idx + 1) * 30,
            text: text,
            normalizedText: text.lowercased(),
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }
}

private func makeService(
    store: AnalysisStore,
    slotOwnership: Bool
) -> AdDetectionService {
    let config = AdDetectionConfig(
        candidateThreshold: 0.40,
        confirmationThreshold: 0.70,
        suppressionThreshold: 0.25,
        hotPathLookahead: 90.0,
        detectorVersion: "test-detection-v1",
        fmBackfillMode: .off,
        spliceSlotOwnershipEnabled: slotOwnership,
        // playhead-lq6f (Ship Gate 1): rediff ownership now defaults ON;
        // explicitly OFF so the slotOwnership=true arms keep testing the
        // acoustic splice channel in isolation (mutually-exclusive width
        // setters — the config init preconditions on it).
        rediffSlotOwnershipEnabled: false
    )
    return AdDetectionService(
        store: store,
        classifier: RuleBasedClassifier(),
        metadataExtractor: FallbackExtractor(),
        config: config
    )
}

// MARK: - Flag-OFF byte identity

@Suite("SpliceSlot ownership — flag-OFF byte identity (playhead-xsdz.20)")
struct SpliceSlotOwnershipFlagOffTests {

    @Test("flag OFF: no decoded span carries .spliceSlot provenance")
    func flagOffNoSpliceSlotProvenance() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-slot-off"
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(store: store, slotOwnership: false)

        try await service.runBackfill(
            chunks: adChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )

        let spans = try await store.fetchDecodedSpans(assetId: assetId)
        #expect(!spans.isEmpty)
        for span in spans {
            #expect(!span.anchorProvenance.contains(.spliceSlot))
        }
    }

    @Test("flag ON without qualifying acoustics is byte-identical to flag OFF")
    func flagOnWithoutSlotsMatchesFlagOff() async throws {
        // No feature windows are inserted, so the resolver finds no splice edges
        // and the pass produces no rewrite — the ON path must be inert and the
        // persisted decoded spans identical to the OFF run.
        let offStore = try await makeTestStore()
        let onStore = try await makeTestStore()
        let assetId = "asset-slot-parity"
        try await offStore.insertAsset(makeAsset(id: assetId))
        try await onStore.insertAsset(makeAsset(id: assetId))

        try await makeService(store: offStore, slotOwnership: false).runBackfill(
            chunks: adChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )
        try await makeService(store: onStore, slotOwnership: true).runBackfill(
            chunks: adChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )

        let offSpans = try await offStore.fetchDecodedSpans(assetId: assetId).sorted { $0.id < $1.id }
        let onSpans = try await onStore.fetchDecodedSpans(assetId: assetId).sorted { $0.id < $1.id }
        #expect(offSpans == onSpans)
    }
}

// MARK: - Phase-5 clobber guard

@Suite("SpliceSlot ownership — Phase-5 clobber guard (playhead-xsdz.20)")
struct SpliceSlotOwnershipPhase5GuardTests {

    @Test("Phase-5 projector skips its upsert over a slot-owned asset (no superseded-id reappearance)")
    func phase5GuardSkipsUpsertForSlotOwnedAsset() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-slot-phase5"
        try await store.insertAsset(makeAsset(id: assetId))

        // Persist a single slot-owned survivor row. The original (superseded)
        // decode ids are already absent — exactly the post-backfill state.
        let slotOwned = DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 0, lastAtomOrdinal: 3),
            assetId: assetId,
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 3,
            startTime: 12.0,
            endTime: 58.0,
            anchorProvenance: [.classifierSeed(regionId: "r", score: 0.9), .spliceSlot]
        )
        try await store.upsertDecodedSpans([slotOwned])

        let service = makeService(store: store, slotOwnership: true)
        await service.runPhase5ProjectorPhase(
            observer: Phase5ProjectorObserver(),
            bundles: [],
            chunks: adChunks(assetId: assetId),
            analysisAssetId: assetId
        )

        // The guard must have skipped the projector's upsert: the slot-owned row
        // is still the ONLY row, and no freshly-decoded (non-slot) id appeared.
        let after = try await store.fetchDecodedSpans(assetId: assetId)
        #expect(after == [slotOwned])
    }

    @Test("control: Phase-5 projector DOES upsert over a non-slot-owned asset")
    func phase5UpsertsForNonSlotOwnedAsset() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-nonslot-phase5"
        try await store.insertAsset(makeAsset(id: assetId))

        let service = makeService(store: store, slotOwnership: true)
        await service.runPhase5ProjectorPhase(
            observer: Phase5ProjectorObserver(),
            bundles: [],
            chunks: adChunks(assetId: assetId),
            analysisAssetId: assetId
        )

        // With no slot-owned rows present the projector persists its decode.
        let after = try await store.fetchDecodedSpans(assetId: assetId)
        #expect(!after.isEmpty)
        #expect(after.allSatisfy { !$0.anchorProvenance.contains(.spliceSlot) })
    }
}
