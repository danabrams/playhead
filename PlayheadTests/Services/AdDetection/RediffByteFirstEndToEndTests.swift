// RediffByteFirstEndToEndTests.swift
// playhead-xsdz.57: service-level coverage for the BYTE-PRIMARY rediff differ
// wiring in `AdDetectionService.computeRediffSlotPass` — the fallback matrix:
//
//   • byte-success:      the byte aligner sets width; the chroma differ (and
//                        its PCM fetch) is NEVER invoked, and no stored
//                        fingerprint stream is needed at all.
//   • byte-fail→chroma:  a byte-gate rejection (disjoint bytes — the re-encode
//                        CDN shape) falls back to the chroma path, which
//                        behaves EXACTLY as pre-xsdz.57 (PCM fetched, stored
//                        A-side diffed, same widening).
//   • A-side unanchored: a non-file `sourceURL` disables the byte path even
//                        with a staged B file — chroma fallback.
//   • both-unavailable:  no byte URLs, no PCM → status quo (no .rediffSlot).
//
// The synthetic A/B MP3 pairs use the `SyntheticMP3` builder (see
// RediffByteAlignerTests); the chroma-side synthetics mirror
// `RediffSlotOwnershipEndToEndTests` so the fallback assertions pin the SAME
// behavior that suite pins.

import Foundation
import Testing

@testable import Playhead

@Suite("Rediff byte-first — service wiring end-to-end (playhead-xsdz.57)")
struct RediffByteFirstEndToEndTests {

    // Transcript shape: ad chunk at [100,160] decodes to a presence span there.
    private static let adStart = 100.0
    private static let adEnd = 160.0

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

    private func noisePCM(seconds: Double, seed: UInt64) -> [Float] {
        var rng = SyntheticMP3.Noise(seed: seed)
        let n = Int(seconds * 16_000)
        return (0..<n).map { _ in Float(Int64(bitPattern: rng.next()) % 2_000_000) / 1_000_000.0 }
    }

    /// Stored A-side stream = B's content fingerprints with a distinct ad block
    /// spliced in at ~`adStartSeconds` (same construction as the chroma e2e
    /// suite) so the CHROMA differ recovers a played slot at a known interval.
    private func syntheticChromaASide(
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
        var rng = SyntheticMP3.Noise(seed: 0xADD_5EED)
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

    /// Records whether the chroma path's PCM fetch was invoked, so byte-success
    /// can assert the chroma differ NEVER ran.
    private actor RecordingBSideProvider: RediffBSideProvider {
        private let fileURL: URL?
        private let samples: [Float]?
        private(set) var pcmCallCount = 0

        init(fileURL: URL?, samples: [Float]?) {
            self.fileURL = fileURL
            self.samples = samples
        }

        func refetchedBSideMono16kHz(assetId: String) async -> [Float]? {
            pcmCallCount += 1
            return samples
        }

        func refetchedBSideFileURL(assetId: String) async -> URL? { fileURL }
    }

    /// A/B synthetic MP3 pair: A carries an ID3-separated distinct ad block over
    /// [~95, ~165] s; B is the same content without it. Byte slot ≈ [95, 165].
    private struct BytePair {
        let aURL: URL
        let bURL: URL
        static let adStartFrame = 3637   // ≈ 95.008 s
        static let adFrames = 2680       // ≈ 70.008 s
        static let contentFrames = 10719 // ≈ 280.0 s of played (A) audio

        static func stage(in directory: URL) throws -> BytePair {
            let c1 = SyntheticMP3.frames(count: adStartFrame, seed: 0xC0FFEE)
            let c2 = SyntheticMP3.frames(count: contentFrames - adStartFrame - adFrames, seed: 0xFACADE)
            let ad = SyntheticMP3.frames(count: adFrames, seed: 0xAD_B10C)
            let aData = SyntheticMP3.file(c1 + [SyntheticMP3.id3v2(payloadBytes: 32)] + ad + c2)
            let bData = SyntheticMP3.file(c1 + c2)
            let aURL = directory.appendingPathComponent("byte-a.mp3", isDirectory: false)
            let bURL = directory.appendingPathComponent("byte-b.fresh.mp3", isDirectory: false)
            try aData.write(to: aURL)
            try bData.write(to: bURL)
            return BytePair(aURL: aURL, bURL: bURL)
        }
    }

    private func service(
        store: AnalysisStore,
        provider: RediffBSideProvider?
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70,
            suppressionThreshold: 0.25, hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1", fmBackfillMode: .off,
            rediffSlotOwnershipEnabled: true
        )
        return AdDetectionService(
            store: store, classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(), config: config,
            rediffBSideProvider: provider
        )
    }

    /// Insert an asset whose `sourceURL` is the byte-path A-side seam, seed the
    /// optional chroma A-side stream, run backfill, return persisted spans.
    private func runAndFetch(
        assetId: String,
        sourceURL: String,
        provider: RediffBSideProvider?,
        storedASide: EpisodeFingerprintRecord? = nil
    ) async throws -> [DecodedSpan] {
        let store = try await makeTestStore()
        try await store.insertAsset(AnalysisAsset(
            id: assetId, episodeId: "ep-\(assetId)", assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil, sourceURL: sourceURL,
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil
        ))
        if let storedASide {
            try await store.upsertEpisodeFingerprints(storedASide)
        }
        try await service(store: store, provider: provider).runBackfill(
            chunks: chunks(assetId: assetId), analysisAssetId: assetId,
            podcastId: "podcast-byte-first-e2e", episodeDuration: 280.0
        )
        return try await store.fetchDecodedSpans(assetId: assetId)
    }

    // MARK: - Matrix

    @Test("byte-success: byte aligner sets width; chroma differ (PCM fetch) never invoked; no fingerprint stream needed")
    func byteSuccessSetsWidthWithoutChroma() async throws {
        let assetId = "byte-primary"
        let dir = try makeTempDir(prefix: "RediffByteFirst-\(assetId)")
        let pair = try BytePair.stage(in: dir)
        // NO stored fingerprint stream and NO PCM: if the code ever consulted
        // the chroma path here it would no-op — the .rediffSlot below can ONLY
        // come from the byte differ.
        let provider = RecordingBSideProvider(fileURL: pair.bURL, samples: nil)

        let spans = try await runAndFetch(
            assetId: assetId,
            sourceURL: pair.aURL.absoluteString,
            provider: provider
        )

        let rediffOwned = spans.filter { $0.anchorProvenance.contains(.rediffSlot) }
        #expect(rediffOwned.count == 1, "exactly the ad span is byte-rediff-width-owned")
        let span = try #require(rediffOwned.first)
        // The byte slot is [~95.01, ~165.02] — the decoded [100,160] span is
        // widened to the byte-exact splice edges.
        #expect(span.startTime >= 94.5 && span.startTime <= 95.5, "start ≈ 95, got \(span.startTime)")
        #expect(span.endTime >= 164.5 && span.endTime <= 165.5, "end ≈ 165, got \(span.endTime)")
        #expect(!span.anchorProvenance.contains(.spliceSlot))
        // Chroma differ NEVER invoked on the byte-primary path.
        #expect(await provider.pcmCallCount == 0)
    }

    @Test("byte-fail (disjoint bytes, re-encode shape) falls back to chroma, which behaves exactly as pre-xsdz.57")
    func byteFailFallsBackToChroma() async throws {
        let assetId = "byte-fallback"
        let dir = try makeTempDir(prefix: "RediffByteFirst-\(assetId)")
        // Disjoint tiny files: zero anchors → .rejectedNoChainedRuns → fallback.
        let aURL = dir.appendingPathComponent("reencode-a.mp3", isDirectory: false)
        let bURL = dir.appendingPathComponent("reencode-b.fresh.mp3", isDirectory: false)
        try SyntheticMP3.file(SyntheticMP3.frames(count: 40, seed: 41)).write(to: aURL)
        try SyntheticMP3.file(SyntheticMP3.frames(count: 40, seed: 42)).write(to: bURL)

        let contentPCM = noisePCM(seconds: 180, seed: 7)
        let aSide = syntheticChromaASide(
            assetId: assetId, contentPCM: contentPCM,
            adStartSeconds: 100, adSeconds: 60, identity: "fp-\(assetId)")
        let provider = RecordingBSideProvider(fileURL: bURL, samples: contentPCM)

        let spans = try await runAndFetch(
            assetId: assetId,
            sourceURL: aURL.absoluteString,
            provider: provider,
            storedASide: aSide
        )

        let rediffOwned = spans.filter { $0.anchorProvenance.contains(.rediffSlot) }
        #expect(rediffOwned.count == 1, "the chroma fallback still width-owns the ad span")
        let span = try #require(rediffOwned.first)
        // The CHROMA differ's slot (≈[100.04, 160.09]) — NOT a byte slot.
        #expect(span.startTime >= 99.5 && span.startTime <= 100.5, "start ≈ 100, got \(span.startTime)")
        #expect(span.endTime >= 159.5 && span.endTime <= 160.5, "end ≈ 160, got \(span.endTime)")
        // Fallback DID fetch the PCM (the chroma path ran).
        #expect(await provider.pcmCallCount >= 1)
    }

    @Test("non-file A-side sourceURL disables the byte path even with a staged B file (chroma fallback)")
    func remoteSourceURLFallsBackToChroma() async throws {
        let assetId = "byte-remote-a"
        let dir = try makeTempDir(prefix: "RediffByteFirst-\(assetId)")
        let bURL = dir.appendingPathComponent("staged-b.fresh.mp3", isDirectory: false)
        try SyntheticMP3.file(SyntheticMP3.frames(count: 40, seed: 43)).write(to: bURL)

        let contentPCM = noisePCM(seconds: 180, seed: 8)
        let aSide = syntheticChromaASide(
            assetId: assetId, contentPCM: contentPCM,
            adStartSeconds: 100, adSeconds: 60, identity: "fp-\(assetId)")
        let provider = RecordingBSideProvider(fileURL: bURL, samples: contentPCM)

        let spans = try await runAndFetch(
            assetId: assetId,
            sourceURL: "https://example.com/\(assetId).mp3",  // NOT a local file
            provider: provider,
            storedASide: aSide
        )

        let rediffOwned = spans.filter { $0.anchorProvenance.contains(.rediffSlot) }
        #expect(rediffOwned.count == 1)
        let span = try #require(rediffOwned.first)
        #expect(span.startTime >= 99.5 && span.startTime <= 100.5)
        #expect(span.endTime >= 159.5 && span.endTime <= 160.5)
        #expect(await provider.pcmCallCount >= 1)
    }

    @Test("both byte URLs and PCM unavailable: status quo — no .rediffSlot")
    func bothUnavailableStatusQuo() async throws {
        let assetId = "byte-neither"
        // Seed a stored A-side so the chroma fallback proceeds PAST the
        // fingerprint fetch to the PCM fetch (which returns nil) — proving the
        // byte-URL miss falls through to the chroma path, which then also
        // yields no signal → status quo.
        let contentPCM = noisePCM(seconds: 180, seed: 9)
        let aSide = syntheticChromaASide(
            assetId: assetId, contentPCM: contentPCM,
            adStartSeconds: 100, adSeconds: 60, identity: "fp-\(assetId)")
        let provider = RecordingBSideProvider(fileURL: nil, samples: nil)
        let spans = try await runAndFetch(
            assetId: assetId,
            sourceURL: "file:///tmp/\(assetId).m4a",
            provider: provider,
            storedASide: aSide
        )
        #expect(!spans.isEmpty, "the ad span still decodes")
        for span in spans { #expect(!span.anchorProvenance.contains(.rediffSlot)) }
        // The pass DID try the chroma fallback (PCM consulted, returned nil)
        // after the byte path found no B file.
        #expect(await provider.pcmCallCount >= 1)
    }
}
