// SpliceSlotOwnershipEndToEndTests.swift
// playhead-xsdz.20 (Bead B): calibrated end-to-end coverage — a real
// acoustic-splice pair forms through `runBackfill`, so the slot pass rewrites a
// span's WIDTH, appends `.spliceSlot`, and the refiners are skipped (the
// persisted edges land exactly on acoustic-break times, not refiner-nudged).
//
// The feature-window fixture uses a sustained LOUDNESS STEP around the ad
// segment: quiet content (rms 0.2) → loud ad (rms 0.6) → quiet content. That
// produces an energy transition — and a non-zero boundary-discontinuity score —
// EXACTLY at each ad edge (a spectral-flux pulse would instead spike at the
// pulse centre, where no acoustic step exists and the edge scores 0).

import Foundation
import Testing

@testable import Playhead

@Suite("SpliceSlot ownership — calibrated end-to-end (playhead-xsdz.20)")
struct SpliceSlotOwnershipEndToEndTests {

    // The ad occupies [adStart, adEnd]; a sustained LOUDNESS STEP (quiet content
    // → loud ad → quiet content) puts an energy transition exactly at each edge —
    // which is where the boundary-discontinuity score is non-zero (unlike a
    // spectral pulse, whose spike lands at the pulse CENTER where no step exists).
    private static let adStart = 100.0
    private static let adEnd = 160.0

    private func episode(assetId: String, count: Int) -> [FeatureWindow] {
        (0..<count).map { i in
            let start = Double(i) * 2.0
            let end = start + 2.0
            let inAd = start >= Self.adStart && start < Self.adEnd
            return AcousticFeatureFixtures.window(
                assetId: assetId,
                startTime: start,
                endTime: end,
                rms: inAd ? 0.6 : 0.2,   // loud ad segment, quiet content
                spectralFlux: 0.05,
                musicProbability: 0.02
            )
        }
    }

    private func asset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id, episodeId: "ep-\(id)", assetFingerprint: "fp-\(id)",
            weakFingerprint: nil, sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil
        )
    }

    // Host chunks with a single ad chunk centered on [100,160].
    private func chunks(assetId: String) -> [TranscriptChunk] {
        let specs: [(Double, Double, String)] = [
            (0, 100, "Welcome to the show. We talk at length about science and history here."),
            (100, 160, "This segment is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Build your website today."),
            (160, 280, "Back to the conversation about the future and what comes next for all of us.")
        ]
        return specs.enumerated().map { idx, s in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)", analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)", chunkIndex: idx,
                startTime: s.0, endTime: s.1, text: s.2,
                normalizedText: s.2.lowercased(), pass: "final",
                modelVersion: "test-v1", transcriptVersion: nil, atomOrdinal: nil
            )
        }
    }

    private func service(store: AnalysisStore, slotOwnership: Bool) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70,
            suppressionThreshold: 0.25, hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1", fmBackfillMode: .off,
            audioForensicsEnabled: true,
            spliceSlotOwnershipEnabled: slotOwnership,
            // playhead-lq6f (Ship Gate 1): rediff ownership now defaults ON;
            // explicitly OFF so the slotOwnership=true arms keep testing the
            // acoustic splice channel in isolation (mutually-exclusive width
            // setters — the config init preconditions on it).
            rediffSlotOwnershipEnabled: false
        )
        return AdDetectionService(
            store: store, classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(), config: config
        )
    }

    private func runAndFetch(assetId: String, slotOwnership: Bool) async throws -> [DecodedSpan] {
        let store = try await makeTestStore()
        try await store.insertAsset(asset(id: assetId))
        try await store.insertFeatureWindows(episode(assetId: assetId, count: 140))
        try await service(store: store, slotOwnership: slotOwnership).runBackfill(
            chunks: chunks(assetId: assetId), analysisAssetId: assetId,
            podcastId: "podcast-e2e", episodeDuration: 280.0
        )
        return try await store.fetchDecodedSpans(assetId: assetId)
    }

    @Test("flag ON: a qualifying splice pair rewrites the ad span's width and APPENDS .spliceSlot (originals retained)")
    func slotFormsRewritesAndAppendsProvenance() async throws {
        let onSpans = try await runAndFetch(assetId: "asset-e2e-on", slotOwnership: true)
        let slotOwned = onSpans.filter { $0.anchorProvenance.contains(.spliceSlot) }
        #expect(slotOwned.count == 1)
        let span = try #require(slotOwned.first)
        // The width is owned by the acoustic splice pair [100,160].
        #expect(span.startTime == 100.0)
        #expect(span.endTime == 160.0)
        // APPEND, not replace: the original evidence provenance survives.
        #expect(span.anchorProvenance.count >= 2)
        #expect(span.anchorProvenance.contains { if case .evidenceCatalog = $0 { return true } else { return false } })
    }

    @Test("refiner skip: slot-owned edges land EXACTLY on acoustic-break times (no ±refiner nudge)")
    func refinerSkippedEdgesAreExactBreakTimes() async throws {
        let onSpans = try await runAndFetch(assetId: "asset-e2e-refine", slotOwnership: true)
        let span = try #require(onSpans.first { $0.anchorProvenance.contains(.spliceSlot) })
        // The BracketAware / legacy BoundaryRefiner would apply fractional
        // adjustments; the slot-owned span's edges are the bitwise break times.
        #expect(span.startTime == Self.adStart)
        #expect(span.endTime == Self.adEnd)
    }

    @Test("control: flag OFF over the same acoustics produces the ad span WITHOUT .spliceSlot")
    func flagOffSameAcousticsNoSpliceSlot() async throws {
        let offSpans = try await runAndFetch(assetId: "asset-e2e-off", slotOwnership: false)
        #expect(!offSpans.isEmpty)
        for span in offSpans {
            #expect(!span.anchorProvenance.contains(.spliceSlot))
        }
        // The ad-covering span exists (proving the fixture decodes a span there),
        // it just isn't slot-owned when the flag is off.
        #expect(offSpans.contains { $0.startTime <= 130 && $0.endTime >= 130 })
    }
}
