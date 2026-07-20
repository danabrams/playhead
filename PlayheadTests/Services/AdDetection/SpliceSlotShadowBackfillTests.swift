// SpliceSlotShadowBackfillTests.swift
// playhead-xsdz.21 (Bead C): integration coverage for the shadow pass wiring
// into `runBackfill` — the both-flag matrix (OFF/OFF byte-identity, shadow-ON
// non-mutation, both-ON shadow silence) and the one-breadcrumb-per-span
// invariant. The pure disposition→row semantics are covered in
// SpliceSlotShadowTests; here we prove the flag gating + observer wiring.

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
    ownership: Bool = false,
    shadow: Bool = false,
    observer: SpliceSlotShadowObserver? = nil
) -> AdDetectionService {
    let config = AdDetectionConfig(
        candidateThreshold: 0.40,
        confirmationThreshold: 0.70,
        suppressionThreshold: 0.25,
        hotPathLookahead: 90.0,
        detectorVersion: "test-detection-v1",
        fmBackfillMode: .off,
        spliceSlotOwnershipEnabled: ownership,
        spliceSlotShadowEnabled: shadow,
        // playhead-lq6f (Ship Gate 1): rediff ownership now defaults ON;
        // explicitly OFF here so the ownership=true arms keep testing the
        // acoustic splice channel in isolation (the two are mutually-
        // exclusive width setters — the config init preconditions on it).
        rediffSlotOwnershipEnabled: false
    )
    return AdDetectionService(
        store: store,
        classifier: RuleBasedClassifier(),
        metadataExtractor: FallbackExtractor(),
        config: config,
        spliceSlotShadowObserver: observer
    )
}

private func run(_ service: AdDetectionService, assetId: String) async throws {
    try await service.runBackfill(
        chunks: adChunks(assetId: assetId),
        analysisAssetId: assetId,
        podcastId: "podcast-test",
        episodeDuration: 90.0
    )
}

// MARK: - Both-flag matrix

@Suite("SpliceSlot shadow — both-flag matrix (playhead-xsdz.21)")
struct SpliceSlotShadowBothFlagMatrixTests {

    @Test("both flags OFF vs shadow-ON: persisted decoded spans are byte-identical (shadow never mutates)")
    func shadowOnByteIdenticalToOff() async throws {
        let offStore = try await makeTestStore()
        let shadowStore = try await makeTestStore()
        let assetId = "asset-shadow-parity"
        try await offStore.insertAsset(makeAsset(id: assetId))
        try await shadowStore.insertAsset(makeAsset(id: assetId))

        try await run(makeService(store: offStore), assetId: assetId)
        let observer = SpliceSlotShadowObserver()
        try await run(makeService(store: shadowStore, shadow: true, observer: observer), assetId: assetId)

        let offSpans = try await offStore.fetchDecodedSpans(assetId: assetId).sorted { $0.id < $1.id }
        let shadowSpans = try await shadowStore.fetchDecodedSpans(assetId: assetId).sorted { $0.id < $1.id }
        #expect(offSpans == shadowSpans)
        // Shadow never stamps `.spliceSlot` provenance.
        #expect(shadowSpans.allSatisfy { !$0.anchorProvenance.contains(.spliceSlot) })
        // But it DID observe: one row per decoded span.
        let rows = await observer.rows(for: assetId) ?? []
        #expect(!rows.isEmpty)
        #expect(rows.count == shadowSpans.count)
    }

    @Test("both flags OFF: shadow observer records nothing")
    func bothOffObserverSilent() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-both-off"
        try await store.insertAsset(makeAsset(id: assetId))
        let observer = SpliceSlotShadowObserver()
        // shadow: false → pass never runs even though an observer is wired.
        try await run(makeService(store: store, shadow: false, observer: observer), assetId: assetId)
        #expect(await observer.recordCount(for: assetId) == 0)
    }

    @Test("both flags ON: shadow is silent; ownership run is identical to ownership-only")
    func bothOnShadowSilent() async throws {
        let ownOnlyStore = try await makeTestStore()
        let bothStore = try await makeTestStore()
        let assetId = "asset-both-on"
        try await ownOnlyStore.insertAsset(makeAsset(id: assetId))
        try await bothStore.insertAsset(makeAsset(id: assetId))

        try await run(makeService(store: ownOnlyStore, ownership: true, shadow: false), assetId: assetId)
        let observer = SpliceSlotShadowObserver()
        try await run(
            makeService(store: bothStore, ownership: true, shadow: true, observer: observer),
            assetId: assetId
        )

        // Shadow emitted NOTHING (ownership pass is the sole disposition owner).
        #expect(await observer.recordCount(for: assetId) == 0)
        // Ownership output is unchanged by the (silent) shadow flag.
        let ownOnly = try await ownOnlyStore.fetchDecodedSpans(assetId: assetId).sorted { $0.id < $1.id }
        let both = try await bothStore.fetchDecodedSpans(assetId: assetId).sorted { $0.id < $1.id }
        #expect(ownOnly == both)
    }

    @Test("shadow-ON emits exactly one row per decoded span, non-qualifying included")
    func oneRowPerSpan() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-shadow-count"
        try await store.insertAsset(makeAsset(id: assetId))
        let observer = SpliceSlotShadowObserver()
        try await run(makeService(store: store, shadow: true, observer: observer), assetId: assetId)

        let spans = try await store.fetchDecodedSpans(assetId: assetId)
        let rows = await observer.rows(for: assetId) ?? []
        #expect(rows.count == spans.count)
        // With no feature windows the resolver finds no pairs → every row is a
        // non-qualifying no-pair sentinel, and each is still emitted.
        #expect(rows.allSatisfy { !$0.qualified })
        #expect(rows.allSatisfy { $0.spanId.isEmpty == false })
    }
}

// MARK: - Same-run projection dump (hermetic; no device audio)

@Suite("SpliceSlot shadow — same-run projection dump (playhead-xsdz.21)")
struct SpliceSlotShadowProjectionDumpTests {

    /// Proves the PREFERRED same-run dump path end-to-end WITHOUT device audio:
    /// a shadow backfill produces the observer rows in the SAME run, the
    /// projection substitutes/removes/keeps per the pinned rule, and the
    /// treatment arm is asserted pairwise-disjoint — the same machinery the
    /// Catalyst dogfood dump will drive on real episodes.
    @Test("shadow rows project to a pairwise-disjoint treatment arm and JSON round-trip")
    func sameRunProjectionAndRoundTrip() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-shadow-projection"
        try await store.insertAsset(makeAsset(id: assetId))
        let observer = SpliceSlotShadowObserver()
        try await run(makeService(store: store, shadow: true, observer: observer), assetId: assetId)

        let rows = await observer.rows(for: assetId) ?? []
        #expect(!rows.isEmpty)

        // Same-run projection: substitution rule + treatment-arm disjointness.
        let projection = SpliceSlotProjection.project(from: rows)
        #expect(projection.disjoint)
        // With no acoustics every row is a no-pair sentinel → minted is kept
        // verbatim (no substitution, no removal).
        #expect(projection.treatmentIntervals.count == rows.count)

        // Structured rows serialize for the dogfood diagnostics export.
        let data = try JSONEncoder().encode(rows)
        let decoded = try JSONDecoder().decode([SpliceSlotShadowRow].self, from: data)
        #expect(decoded == rows)
    }
}
