// RediffSlotOwnershipEndToEndTests.swift
// playhead-xsdz.29: service-level coverage for the rediff width-oracle wiring in
// `AdDetectionService.runBackfill` — the flag family, the `RediffBSideProvider`
// seam, the store fetch (A-side fingerprints + current assetFingerprint), the
// double-gate, and the `.rediffSlot` width rewrite / persistence. The pure
// disposition/rewrite/shadow semantics are pinned by `RediffSlotOwnershipTests`;
// this suite proves the GLUE and the flag-OFF / no-provider / gate-rejected
// NO-OPs (byte-identity: no `.rediffSlot` ever appears unless a real signal
// flows all the way through).
//
// The positive case is DETERMINISTIC: the stored A-side fingerprint stream is
// synthesized as B's own content fingerprints with a distinct "ad" block spliced
// in at a known index, so the differ recovers a played slot at a known
// [~100, ~160]s — overlapping the transcript-decoded ad span, which the rediff
// pass then widens.

import Foundation
import Testing

@testable import Playhead

@Suite("Rediff ownership — service wiring end-to-end (playhead-xsdz.29)")
struct RediffSlotOwnershipEndToEndTests {

    // Same host/ad transcript shape as the acoustic e2e harness: an ad chunk at
    // [100,160] decodes to a presence span there.
    private static let adStart = 100.0
    private static let adEnd = 160.0

    private func asset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id, episodeId: "ep-\(id)", assetFingerprint: "fp-\(id)",
            weakFingerprint: nil, sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil
        )
    }

    // A loudness-step feature track around [100,160], so the boundary refiner
    // path is ACTIVE (non-empty featureWindows) — the rediff-owned span must
    // BYPASS it (playhead-xsdz.29 site-2 fix) and keep its fingerprint-diff edges.
    private func episode(assetId: String, count: Int) -> [FeatureWindow] {
        (0..<count).map { i in
            let start = Double(i) * 2.0
            let end = start + 2.0
            let inAd = start >= Self.adStart && start < Self.adEnd
            return AcousticFeatureFixtures.window(
                assetId: assetId,
                startTime: start,
                endTime: end,
                rms: inAd ? 0.6 : 0.2,
                spectralFlux: 0.05,
                musicProbability: 0.02
            )
        }
    }

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

    // MARK: - Deterministic noise PCM + fingerprint construction

    private struct Noise {
        var state: UInt64
        init(seed: UInt64) { state = seed }
        mutating func next() -> UInt64 {
            state &+= 0x9E37_79B9_7F4A_7C15
            var z = state
            z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
            z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
            return z ^ (z >> 31)
        }
    }

    private func noisePCM(seconds: Double, seed: UInt64) -> [Float] {
        var rng = Noise(seed: seed)
        let n = Int(seconds * 16_000)
        return (0..<n).map { _ in Float(Int64(bitPattern: rng.next()) % 2_000_000) / 1_000_000.0 }
    }

    /// A `RediffBSideProvider` returning a fixed PCM buffer for the asset.
    private struct FixedBSideProvider: RediffBSideProvider {
        let assetId: String
        let samples: [Float]
        func refetchedBSideMono16kHz(assetId: String) async -> [Float]? {
            assetId == self.assetId ? samples : nil
        }
    }

    private func service(
        store: AnalysisStore,
        rediffOwnership: Bool,
        rediffShadow: Bool = false,
        provider: RediffBSideProvider?,
        shadowObserver: SpliceSlotShadowObserver? = nil
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70,
            suppressionThreshold: 0.25, hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1", fmBackfillMode: .off,
            rediffSlotOwnershipEnabled: rediffOwnership,
            rediffSlotShadowEnabled: rediffShadow
        )
        return AdDetectionService(
            store: store, classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(), config: config,
            rediffBSideProvider: provider,
            rediffSlotShadowObserver: shadowObserver
        )
    }

    /// Insert asset + host chunks (no features needed — rediff does not consult
    /// acoustic breaks), optionally seed the stored A-side fingerprint stream,
    /// run backfill, and return the persisted spans.
    private func runAndFetch(
        assetId: String,
        rediffOwnership: Bool,
        rediffShadow: Bool = false,
        provider: RediffBSideProvider?,
        storedASide: EpisodeFingerprintRecord? = nil,
        shadowObserver: SpliceSlotShadowObserver? = nil,
        withFeatures: Bool = false
    ) async throws -> [DecodedSpan] {
        let store = try await makeTestStore()
        try await store.insertAsset(asset(id: assetId))
        if withFeatures {
            try await store.insertFeatureWindows(episode(assetId: assetId, count: 140))
        }
        if let storedASide {
            try await store.upsertEpisodeFingerprints(storedASide)
        }
        try await service(
            store: store, rediffOwnership: rediffOwnership, rediffShadow: rediffShadow,
            provider: provider, shadowObserver: shadowObserver
        ).runBackfill(
            chunks: chunks(assetId: assetId), analysisAssetId: assetId,
            podcastId: "podcast-rediff-e2e", episodeDuration: 280.0
        )
        return try await store.fetchDecodedSpans(assetId: assetId)
    }

    /// Build a stored A-side stream = B's own content fingerprints with a distinct
    /// ad block spliced in at the fingerprint index mapping to ~`adStartSeconds`,
    /// so the differ recovers a played slot at [adStart, adStart+adSeconds].
    private func syntheticASide(
        assetId: String,
        contentPCM: [Float],
        adStartSeconds: Double,
        adSeconds: Double,
        identity: String
    ) -> EpisodeFingerprintRecord {
        let secPerFp = ChromaFingerprinter.secondsPerFingerprint
        let fpContent = EpisodeFingerprintCapture.fingerprints(mono16kHz: contentPCM)
        let kIns = Int((adStartSeconds / secPerFp).rounded())
        let adLen = Int((adSeconds / secPerFp).rounded())
        var rng = Noise(seed: 0xADD_5EED)
        // Distinct ad-block fingerprints (won't align with content).
        let adBlock = (0..<adLen).map { _ in UInt32(truncatingIfNeeded: rng.next()) | 0x8000_0000 }
        precondition(kIns < fpContent.count, "content too short for the insertion index")
        var aFps = Array(fpContent[0..<kIns])
        aFps.append(contentsOf: adBlock)
        aFps.append(contentsOf: fpContent[kIns...])
        return EpisodeFingerprintRecord(
            analysisAssetId: assetId,
            algorithmVersion: ChromaFingerprinter.algorithmVersion,
            secondsPerFingerprint: secPerFp,
            fingerprints: aFps,
            sourceAudioIdentity: identity,
            capturedAt: 0
        )
    }

    // MARK: - Flag-OFF / no-provider / gate-rejected NO-OPs

    @Test("flag OFF (default): no rediff pass, no .rediffSlot")
    func flagOffNoRediffSlot() async throws {
        let spans = try await runAndFetch(
            assetId: "rediff-off", rediffOwnership: false, provider: nil)
        #expect(!spans.isEmpty, "the ad span still decodes")
        for span in spans { #expect(!span.anchorProvenance.contains(.rediffSlot)) }
    }

    @Test("flag ON but NO provider injected: no-op, no .rediffSlot (production case)")
    func flagOnNoProviderNoOp() async throws {
        let spans = try await runAndFetch(
            assetId: "rediff-noprov", rediffOwnership: true, provider: nil)
        #expect(!spans.isEmpty)
        for span in spans { #expect(!span.anchorProvenance.contains(.rediffSlot)) }
    }

    @Test("flag ON + provider but NO stored A-side fingerprints: no-op, no .rediffSlot")
    func flagOnNoStoredASideNoOp() async throws {
        let provider = FixedBSideProvider(assetId: "rediff-noaside", samples: noisePCM(seconds: 20, seed: 5))
        let spans = try await runAndFetch(
            assetId: "rediff-noaside", rediffOwnership: true, provider: provider, storedASide: nil)
        #expect(!spans.isEmpty)
        for span in spans { #expect(!span.anchorProvenance.contains(.rediffSlot)) }
    }

    @Test("flag ON + provider + stored A-side but sourceAudioIdentity MISMATCH: gate rejects, no .rediffSlot")
    func flagOnAudioIdentityMismatchRejected() async throws {
        let assetId = "rediff-idmismatch"
        let contentPCM = noisePCM(seconds: 180, seed: 6)
        // Identity deliberately DIFFERENT from the asset's "fp-<assetId>".
        let aSide = syntheticASide(
            assetId: assetId, contentPCM: contentPCM,
            adStartSeconds: 100, adSeconds: 60, identity: "STALE-DIFFERENT-AUDIO")
        let provider = FixedBSideProvider(assetId: assetId, samples: contentPCM)
        let spans = try await runAndFetch(
            assetId: assetId, rediffOwnership: true, provider: provider, storedASide: aSide)
        #expect(!spans.isEmpty)
        for span in spans {
            #expect(!span.anchorProvenance.contains(.rediffSlot),
                    "a version-matching but audio-mismatched A-side must NOT set width")
        }
    }

    // MARK: - Positive integration: rediff width rewrite

    @Test("flag ON + provider + aligned A/B: the ad span is widened to the rediff slot with .rediffSlot")
    func flagOnRediffWidthRewrite() async throws {
        let assetId = "rediff-on"
        let contentPCM = noisePCM(seconds: 180, seed: 7)
        let aSide = syntheticASide(
            assetId: assetId, contentPCM: contentPCM,
            adStartSeconds: 100, adSeconds: 60, identity: "fp-\(assetId)")  // matches asset fingerprint
        let provider = FixedBSideProvider(assetId: assetId, samples: contentPCM)

        // Seed feature windows so the boundary-refiner path is ACTIVE — the
        // rediff-owned span must BYPASS it and keep its exact fingerprint-diff
        // edges (site-2 fix); without the bypass the ±3s snap would move them.
        let spans = try await runAndFetch(
            assetId: assetId, rediffOwnership: true, provider: provider,
            storedASide: aSide, withFeatures: true)

        let rediffOwned = spans.filter { $0.anchorProvenance.contains(.rediffSlot) }
        #expect(rediffOwned.count == 1, "exactly the ad span is rediff-width-owned")
        let span = try #require(rediffOwned.first)
        // The differ recovers the played slot at ~[100.04, 160.09]s (index-exact
        // by construction: A = content with the ad block spliced at index
        // round(100/secPerFp)). The refiner is bypassed, so these are the exact
        // slot boundaries (not ±snap-nudged).
        #expect(span.startTime >= 99.5 && span.startTime <= 100.5, "start ≈ 100, got \(span.startTime)")
        #expect(span.endTime >= 159.5 && span.endTime <= 160.5, "end ≈ 160, got \(span.endTime)")
        // Rediff, NOT acoustic splice.
        #expect(!span.anchorProvenance.contains(.spliceSlot))
        // Width ownership APPENDS — the transcript/evidence provenance survives.
        #expect(span.anchorProvenance.count >= 2)
    }

    @Test("shadow ON: rediff-sourced rows recorded to the injected observer, ownership OFF (no .rediffSlot persisted)")
    func shadowRecordsRediffRowsWithoutRewrite() async throws {
        let assetId = "rediff-shadow"
        let contentPCM = noisePCM(seconds: 180, seed: 8)
        let aSide = syntheticASide(
            assetId: assetId, contentPCM: contentPCM,
            adStartSeconds: 100, adSeconds: 60, identity: "fp-\(assetId)")
        let provider = FixedBSideProvider(assetId: assetId, samples: contentPCM)
        let observer = SpliceSlotShadowObserver()

        let spans = try await runAndFetch(
            assetId: assetId, rediffOwnership: false, rediffShadow: true,
            provider: provider, storedASide: aSide, shadowObserver: observer)

        // Shadow NEVER rewrites: no .rediffSlot persisted.
        for span in spans { #expect(!span.anchorProvenance.contains(.rediffSlot)) }
        // But the observer captured rediff-sourced rows, incl. one qualifying slot.
        let rows = await observer.rows(for: assetId)
        let recorded = try #require(rows)
        #expect(!recorded.isEmpty)
        #expect(recorded.contains { $0.qualified && $0.reason == .qualifying })
    }
}
