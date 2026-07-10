// EpisodeFingerprintCaptureTests.swift
// playhead-xsdz.27: unit tests for the played-copy capture core and the
// [UInt32] ⇆ BLOB codec — hermetic (synthetic PCM only), plus a store
// round-trip through the default-OFF capture path and the resampler-identity
// pin that guards the silent-staleness hole.

import Foundation
import Testing

@testable import Playhead

@Suite("EpisodeFingerprint capture + codec (playhead-xsdz.27)")
struct EpisodeFingerprintCaptureTests {

    // MARK: - Blob codec

    @Test("blob codec round-trips an arbitrary [UInt32] stream (little-endian)")
    func codecRoundTrip() {
        let streams: [[UInt32]] = [
            [],
            [0],
            [0xFFFF_FFFF],
            [0x0000_0001, 0xDEAD_BEEF, 0x8000_0000, 0x7FFF_FFFF, 0x0000_0000],
            (0..<1000).map { UInt32(truncatingIfNeeded: $0 &* 2_654_435_761) },  // scrambled sweep
        ]
        for stream in streams {
            let data = EpisodeFingerprintBlobCodec.encode(stream)
            #expect(data.count == stream.count * 4)
            #expect(EpisodeFingerprintBlobCodec.decode(data) == stream)
        }
    }

    @Test("blob codec is explicitly little-endian on the wire")
    func codecIsLittleEndian() {
        // 0x0A0B0C0D packs as 0D 0C 0B 0A.
        let data = EpisodeFingerprintBlobCodec.encode([0x0A0B_0C0D])
        #expect(Array(data) == [0x0D, 0x0C, 0x0B, 0x0A])
    }

    @Test("blob codec rejects a byte count that is not a multiple of 4")
    func codecRejectsTruncatedBlob() {
        #expect(EpisodeFingerprintBlobCodec.decode(Data([0x01, 0x02, 0x03])) == nil)
        #expect(EpisodeFingerprintBlobCodec.decode(Data([0x01, 0x02, 0x03, 0x04, 0x05])) == nil)
        // Whole multiples decode fine.
        #expect(EpisodeFingerprintBlobCodec.decode(Data([0x01, 0x02, 0x03, 0x04]))?.count == 1)
    }

    @Test("blob codec decodes correctly from a sliced (non-zero startIndex) Data")
    func codecHandlesSlicedData() {
        let full = EpisodeFingerprintBlobCodec.encode([0x1111_1111, 0x2222_2222, 0x3333_3333])
        // Prepend a byte then slice it off so the resulting Data has a
        // non-zero startIndex — decode must still be correct.
        var padded = Data([0xFF])
        padded.append(full)
        let sliced = padded.dropFirst()  // Data.SubSequence with startIndex == 1
        // Pass the slice DIRECTLY (not re-based via `Data(sliced)`, which would
        // normalize startIndex to 0 and hide the non-zero-startIndex path this
        // test exists to exercise).
        #expect(EpisodeFingerprintBlobCodec.decode(sliced) == [0x1111_1111, 0x2222_2222, 0x3333_3333])
    }

    // MARK: - Resampler identity pin (silent-staleness guard)

    @Test("resampler rates are pinned to 16 kHz in / 11025 Hz out")
    func resamplerRatesPinned() {
        // These rates are part of the fingerprint EXTRACTOR identity: changing
        // either changes emitted fingerprints WITHOUT changing
        // ChromaFingerprinter.algorithmVersion. If this test fails, you changed
        // the resampler — you MUST bump ChromaFingerprinter.algorithmVersion in
        // the same change so the store's staleness contract invalidates old
        // streams. See EpisodeFingerprintCapture's header.
        #expect(EpisodeFingerprintCapture.captureInputSampleRate == 16_000)
        #expect(EpisodeFingerprintCapture.captureOutputSampleRate == 11_025)
        #expect(EpisodeFingerprintCapture.captureOutputSampleRate == ChromaFingerprinter.requiredSampleRate)
        // captureInputSampleRate is TIED to the decode rate, so this also
        // cross-pins the decode-rate source of truth: if the pipeline's decode
        // rate ever changes, the `== 16_000` assertion above trips, forcing an
        // extractor-identity (algorithmVersion) review rather than silently
        // resampling from the wrong input rate.
        #expect(EpisodeFingerprintCapture.captureInputSampleRate == Int(AnalysisAudioService.targetSampleRate))
    }

    // MARK: - Resampler behavior

    @Test("resampler edge cases: empty and single-sample inputs pass through")
    func resamplerEdgeCases() {
        #expect(EpisodeFingerprintCapture.resampleToFingerprintRate(mono16kHz: []).isEmpty)
        #expect(EpisodeFingerprintCapture.resampleToFingerprintRate(mono16kHz: [0.42]) == [0.42])
    }

    @Test("resampler output length follows floor((n-1)·out/in)+1")
    func resamplerLength() {
        // 1 s of 16 kHz → 11025 samples.
        let oneSecond = [Float](repeating: 0, count: 16_000)
        #expect(EpisodeFingerprintCapture.resampleToFingerprintRate(mono16kHz: oneSecond).count == 11_025)
    }

    @Test("resampler preserves a constant signal exactly")
    func resamplerConstant() {
        let flat = [Float](repeating: 0.5, count: 5_000)
        let out = EpisodeFingerprintCapture.resampleToFingerprintRate(mono16kHz: flat)
        #expect(!out.isEmpty)
        #expect(out.allSatisfy { abs($0 - 0.5) < 1e-6 })
    }

    @Test("resampler interpolation is golden-pinned on a CURVED input")
    func resamplerInterpolationGoldenPinned() {
        // A ramp cannot distinguish interpolation kernels (linear, cubic, and
        // sinc all reproduce a straight line exactly). A CURVED input can:
        // these golden values are the LINEAR-interpolated resample of the
        // quadratic samples[k] = k² at the first output positions. A
        // linear→cubic/sinc swap changes emitted fingerprints WITHOUT changing
        // ChromaFingerprinter.algorithmVersion — the silent-staleness hole this
        // pin closes (the resampler twin of the fingerprinter's
        // goldenOutputPinned). If this fails because you changed the resampler,
        // regenerate the golden AND bump ChromaFingerprinter.algorithmVersion.
        // (A cubic kernel would instead approach the true quadratic — out[1]≈2.11,
        // out[2]≈8.42 — differing by ≫ the 1e-2 tolerance below.)
        let quadratic = (0..<20).map { Float($0 * $0) }
        let out = EpisodeFingerprintCapture.resampleToFingerprintRate(mono16kHz: quadratic)
        let golden: [Float] = [0.0, 2.35374, 8.51247, 19.18367]
        for (j, expected) in golden.enumerated() {
            #expect(abs(out[j] - expected) < 1e-2, "j=\(j) expected \(expected) got \(out[j])")
        }
    }

    @Test("resampler linearly interpolates a ramp (exact for a linear input)")
    func resamplerRampInterpolation() {
        // samples[k] = k → out[j] == j·16000/11025 exactly (linear interp of a
        // linear signal), within Float precision at interior points.
        let ramp = (0..<2_000).map { Float($0) }
        let out = EpisodeFingerprintCapture.resampleToFingerprintRate(mono16kHz: ramp)
        let ratio = 16_000.0 / 11_025.0
        for j in [0, 1, 10, 100, 500] {
            let expected = Float(Double(j) * ratio)
            #expect(abs(out[j] - expected) < 0.05, "j=\(j) expected \(expected) got \(out[j])")
        }
    }

    // MARK: - Capture core

    /// A 16 kHz multi-tone signal long enough to yield a handful of
    /// subfingerprints after the 16 kHz → 11025 Hz resample.
    private func syntheticMono16k(count: Int = 120_000) -> [Float] {
        (0..<count).map { k in
            let t = Double(k) / 16_000.0
            return Float(0.4 * sin(2 * .pi * 220.0 * t)
                + 0.3 * sin(2 * .pi * 523.25 * t)
                + 0.2 * sin(2 * .pi * 1_040.0 * t))
        }
    }

    @Test("capture core resamples-then-fingerprints and yields a non-empty stream")
    func captureCoreProducesStream() {
        let samples = syntheticMono16k()
        let stream = EpisodeFingerprintCapture.fingerprints(mono16kHz: samples)
        #expect(!stream.isEmpty)
        // Consistency: the convenience path equals the explicit two-stage path.
        let explicit = ChromaFingerprinter.fingerprint(
            monoSamples11025: EpisodeFingerprintCapture.resampleToFingerprintRate(mono16kHz: samples))
        #expect(stream == explicit)
    }

    @Test("capture core is deterministic (same input → same stream)")
    func captureCoreDeterministic() {
        let samples = syntheticMono16k(count: 90_000)
        #expect(EpisodeFingerprintCapture.fingerprints(mono16kHz: samples)
            == EpisodeFingerprintCapture.fingerprints(mono16kHz: samples))
    }

    @Test("makeRecord stamps the current algorithmVersion + secondsPerFingerprint")
    func makeRecordStampsIdentity() throws {
        let record = try #require(EpisodeFingerprintCapture.makeRecord(
            assetId: "asset-mk",
            sourceAudioIdentity: "sha-mk",
            mono16kHz: syntheticMono16k(),
            capturedAt: 1_700_000_123))
        #expect(record.algorithmVersion == ChromaFingerprinter.algorithmVersion)
        #expect(record.secondsPerFingerprint == ChromaFingerprinter.secondsPerFingerprint)
        #expect(record.sourceAudioIdentity == "sha-mk")
        #expect(record.capturedAt == 1_700_000_123)
        #expect(record.fingerprints.isEmpty == false)
    }

    @Test("makeRecord returns nil when the episode is too short to fingerprint")
    func makeRecordNilOnTooShort() {
        let record = EpisodeFingerprintCapture.makeRecord(
            assetId: "asset-short",
            sourceAudioIdentity: "sha",
            mono16kHz: [Float](repeating: 0, count: 100),
            capturedAt: 0)
        #expect(record == nil)
    }

    // MARK: - Wiring flag (flag-OFF proof)

    @Test("capture is default-OFF: the live-pipeline branch never fires unbidden")
    func captureFlagDefaultsOff() {
        // AnalysisJobRunner guards its capture branch on exactly this flag, so
        // a `false` here IS the flag-OFF byte-identity proof for the live path.
        #expect(EpisodeFingerprintCapture.captureEnabledByDefault == false)
    }

    // MARK: - captureAndPersist (store round-trip)

    @Test("captureAndPersist concatenates shards in startTime order and persists")
    func captureAndPersistRoundTrips() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(AnalysisAsset(
            id: "asset-cap",
            episodeId: "ep-cap",
            assetFingerprint: "sha-cap",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/cap.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil))

        let mono = syntheticMono16k()
        let mid = mono.count / 2
        // startTimes are the true sample offsets (60_000 samples / 16 kHz = 3.75 s)
        // so the sort key matches reality; capture concatenates in startTime order.
        let shard0 = AnalysisShard(id: 0, episodeID: "ep-cap", startTime: 0, duration: 3.75,
                                   samples: Array(mono[0..<mid]))
        let shard1 = AnalysisShard(id: 1, episodeID: "ep-cap", startTime: 3.75, duration: 3.75,
                                   samples: Array(mono[mid...]))

        // Pass the shards OUT of order — capture must sort by startTime and
        // reconstruct the original continuous stream.
        try await EpisodeFingerprintCapture.captureAndPersist(
            shards: [shard1, shard0],
            assetId: "asset-cap",
            sourceAudioIdentity: "sha-cap",
            store: store,
            capturedAt: 1_700_500_500)

        let fetched = try await store.fetchEpisodeFingerprints(assetId: "asset-cap")
        let expected = EpisodeFingerprintCapture.fingerprints(mono16kHz: mono)
        #expect(fetched?.fingerprints == expected)
        #expect(fetched?.algorithmVersion == ChromaFingerprinter.algorithmVersion)
        #expect(fetched?.sourceAudioIdentity == "sha-cap")
        #expect(fetched?.capturedAt == 1_700_500_500)
    }

    @Test("captureAndPersist is a no-op for an episode too short to fingerprint")
    func captureAndPersistNoOpOnShort() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(AnalysisAsset(
            id: "asset-cap-short",
            episodeId: "ep-cap-short",
            assetFingerprint: "sha",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/s.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil))

        let tiny = AnalysisShard(id: 0, episodeID: "ep-cap-short", startTime: 0, duration: 0.01,
                                 samples: [Float](repeating: 0, count: 100))
        try await EpisodeFingerprintCapture.captureAndPersist(
            shards: [tiny], assetId: "asset-cap-short", sourceAudioIdentity: "sha", store: store)
        #expect(try await store.episodeFingerprintCount() == 0)
    }
}
