// EpisodeFingerprintCapture.swift
// playhead-xsdz.27: capture the played-copy fingerprint stream for an
// analyzed episode and persist it in AnalysisStore.
//
// WIRING SEAM (default-OFF): capture is invoked from
// `AnalysisJobRunner.run(...)` after the audio decode, gated on
// `captureEnabledByDefault` (currently `false`). With the flag off the whole
// branch is skipped, so the live analysis pipeline is byte-for-byte
// unchanged — this bead ships STORAGE + a tested capture core, not a
// behavioral change. xsdz.29 (the rediff width-oracle integration) flips the
// flag / replaces it with a runtime setting once it consumes the stream.
//
// EXTRACTOR IDENTITY (staleness — read this before changing anything here):
// The store's staleness contract keys on `ChromaFingerprinter.algorithmVersion`
// (see `EpisodeFingerprintRecord`). The *fingerprinter* owns that constant,
// but the played-copy stream is produced by resample → fingerprint, and the
// RESAMPLER below is just as bit-determining as any fingerprinter constant:
// the pipeline decodes at 16 kHz (`AnalysisAudioService.targetSampleRate`) and
// the fingerprinter requires 11025 Hz, so a linear resample sits in front of
// it. Changing the resampler (its rates or interpolation) changes emitted
// fingerprints WITHOUT changing `algorithmVersion` — a silent-staleness hole.
// Therefore, treat the resampler as part of the extractor identity: ANY change
// to `captureInputSampleRate`, `captureOutputSampleRate`, or the
// interpolation MUST be accompanied by a `ChromaFingerprinter.algorithmVersion`
// bump (same rule as changing a fingerprinter constant). Both the rates AND
// the interpolation kernel are golden-pinned by `EpisodeFingerprintCaptureTests`
// (`resamplerRatesPinned` + `resamplerInterpolationGoldenPinned`, the latter
// over a CURVED input so a linear→cubic/sinc swap trips it) — the twin of the
// fingerprinter's own `goldenOutputPinned`; together they pin the whole
// resample→fingerprint extractor identity.
//
// xsdz.29 REQUIREMENT: the re-fetched B-side copy MUST be fingerprinted with
// this IDENTICAL resample + fingerprint path, or A-side (captured here) and
// B-side will not align.
//
// AS-PLAYED TAP (evaluated, NOT built here — for xsdz.29): iOS 27's
// `AVPlayerItemSampleBufferOutput` can tap decoded PCM as the user actually
// hears it, which would make the "played copy" fingerprint literally the
// played bytes (and stream incrementally, avoiding the whole-episode buffer
// this download-time path holds — see the memory note on `captureAndPersist`).
// The download-time capture below is the deliverable for xsdz.27; the tap is a
// candidate capture SOURCE for xsdz.29 and is documented in the spike report §8.

import Foundation

/// Pure fingerprint-capture helpers plus the (default-OFF) persistence entry
/// point. Stateless: value/`static` functions only, deterministic, no time or
/// global mutable state except the compile-time flag.
enum EpisodeFingerprintCapture {

    // MARK: - Wiring flag

    /// Compile-time fallback flag for played-copy fingerprint capture on the
    /// live pipeline. STAYS `false` (pinned by `captureFlagDefaultsOff`) —
    /// activation (playhead-xsdz.36) did NOT flip it; the production vehicle
    /// is `AnalysisJobRunner.rediffASideCaptureEnabled`, which
    /// `PlayheadRuntime` drives from `RediffActivation.isEnabledByDefault`
    /// (the runner ORs the two, so this constant remains an escape hatch).
    static let captureEnabledByDefault = false

    // MARK: - Resampler identity (see file header — pinned by test)

    /// The pipeline's decode rate — TIED to `AnalysisAudioService.targetSampleRate`
    /// (the source of truth for `allShards.samples`), not a bare literal, so the
    /// resampler's input rate can never silently desync from the actual decode
    /// rate. `resamplerRatesPinned` additionally asserts the concrete value
    /// (16 kHz today), so a change to the decode rate trips that pin and forces
    /// the `ChromaFingerprinter.algorithmVersion`-bump decision (see header).
    static let captureInputSampleRate = Int(AnalysisAudioService.targetSampleRate)

    /// The fingerprinter's required rate (`ChromaFingerprinter.requiredSampleRate`).
    static let captureOutputSampleRate = ChromaFingerprinter.requiredSampleRate  // 11025

    // MARK: - Pure capture core

    /// Linear-interpolation resample of mono 16 kHz PCM down to the
    /// fingerprinter's 11025 Hz. Deterministic and pure.
    ///
    /// Output length = `floor((n-1) * outRate/inRate) + 1` for `n >= 1`
    /// (`0` for empty input). Output sample `j` reads source position
    /// `j * inRate/outRate`, linearly interpolating between the two bracketing
    /// input samples (clamped at the tail). Linear interpolation is the
    /// conservative, dependency-free choice (no AVFoundation, fully unit
    /// testable); it is part of the extractor identity — see the file header.
    static func resampleToFingerprintRate(mono16kHz samples: [Float]) -> [Float] {
        let inRate = Double(captureInputSampleRate)
        let outRate = Double(captureOutputSampleRate)
        let n = samples.count
        guard n > 1 else { return samples }
        let step = inRate / outRate
        let outputCount = Int((Double(n - 1) * outRate / inRate).rounded(.down)) + 1
        guard outputCount > 0 else { return [] }
        var out = [Float](repeating: 0, count: outputCount)
        let lastIndex = n - 1
        for j in 0..<outputCount {
            let srcPos = Double(j) * step
            let i0 = Int(srcPos.rounded(.down))
            if i0 >= lastIndex {
                out[j] = samples[lastIndex]
                continue
            }
            let frac = Float(srcPos - Double(i0))
            out[j] = samples[i0] * (1 - frac) + samples[i0 + 1] * frac
        }
        return out
    }

    /// Resample 16 kHz mono PCM to 11025 Hz and fingerprint it. The single
    /// canonical "extractor" for played-copy capture; xsdz.29 must reuse it
    /// for the B-side.
    static func fingerprints(mono16kHz samples: [Float]) -> [UInt32] {
        ChromaFingerprinter.fingerprint(
            monoSamples11025: resampleToFingerprintRate(mono16kHz: samples))
    }

    // MARK: - Chunk-aware resample (playhead-xsdz.36 activation memory bound)

    /// Resample ORDERED 16 kHz chunks (e.g. `AnalysisShard.samples` runs) as
    /// ONE virtual continuous stream, WITHOUT materializing the concatenated
    /// input. Output is BIT-IDENTICAL to
    /// `resampleToFingerprintRate(mono16kHz: chunks.flat)` — pinned by
    /// `chunkedResampleMatchesConcatenated*` in the capture tests — so the
    /// extractor identity (file header) is untouched: same rates, same
    /// interpolation kernel, same float-op order.
    ///
    /// Motivation: activation (xsdz.36) turns capture ON for the live
    /// pipeline; the concat-then-resample shape held a FULL extra copy of the
    /// episode's 16 kHz PCM (~230 MB per decoded hour) beyond the shards the
    /// pipeline already holds. This walk keeps the extra transient to just the
    /// 11025 Hz output (~159 MB/h).
    static func resampleToFingerprintRate(chunkedMono16kHz chunks: [[Float]]) -> [Float] {
        let n = chunks.reduce(0) { $0 + $1.count }
        // Mirror the mono path's `guard n > 1 else { return samples }`.
        guard n > 1 else { return chunks.flatMap { $0 } }
        let inRate = Double(captureInputSampleRate)
        let outRate = Double(captureOutputSampleRate)
        let step = inRate / outRate
        let outputCount = Int((Double(n - 1) * outRate / inRate).rounded(.down)) + 1
        guard outputCount > 0 else { return [] }
        var out = [Float](repeating: 0, count: outputCount)
        let lastIndex = n - 1

        // The mono path clamps every source read at the FINAL sample; resolve
        // it once (last element of the last non-empty chunk — exists, n > 1).
        var lastSample: Float = 0
        for chunk in chunks.reversed() where !chunk.isEmpty {
            lastSample = chunk[chunk.count - 1]
            break
        }

        // Monotone cursor: global index of chunks[chunkIdx][0]. Source
        // positions are non-decreasing in j, so the cursor only moves forward.
        var chunkIdx = 0
        var chunkStart = 0
        for j in 0..<outputCount {
            let srcPos = Double(j) * step
            let i0 = Int(srcPos.rounded(.down))
            if i0 >= lastIndex {
                out[j] = lastSample
                continue
            }
            while chunkIdx < chunks.count, i0 >= chunkStart + chunks[chunkIdx].count {
                chunkStart += chunks[chunkIdx].count
                chunkIdx += 1
            }
            let local = i0 - chunkStart
            let s0 = chunks[chunkIdx][local]
            let s1: Float
            if local + 1 < chunks[chunkIdx].count {
                s1 = chunks[chunkIdx][local + 1]
            } else {
                // i0 < lastIndex guarantees a next sample exists in a later
                // non-empty chunk.
                var k = chunkIdx + 1
                while chunks[k].isEmpty { k += 1 }
                s1 = chunks[k][0]
            }
            let frac = Float(srcPos - Double(i0))
            // EXACT float-op order of the mono path.
            out[j] = s0 * (1 - frac) + s1 * frac
        }
        return out
    }

    /// Chunk-aware twin of `fingerprints(mono16kHz:)` — identical output
    /// (same extractor), bounded transient memory.
    static func fingerprints(chunkedMono16kHz chunks: [[Float]]) -> [UInt32] {
        ChromaFingerprinter.fingerprint(
            monoSamples11025: resampleToFingerprintRate(chunkedMono16kHz: chunks))
    }

    /// Build a store record (current `algorithmVersion` + `secondsPerFingerprint`
    /// stamped in) from mono 16 kHz PCM. Returns nil when the input yields no
    /// subfingerprints (too short) — there is nothing to persist.
    static func makeRecord(
        assetId: String,
        sourceAudioIdentity: String,
        mono16kHz samples: [Float],
        capturedAt: Double
    ) -> EpisodeFingerprintRecord? {
        let stream = fingerprints(mono16kHz: samples)
        guard !stream.isEmpty else { return nil }
        return EpisodeFingerprintRecord(
            analysisAssetId: assetId,
            algorithmVersion: ChromaFingerprinter.algorithmVersion,
            secondsPerFingerprint: ChromaFingerprinter.secondsPerFingerprint,
            fingerprints: stream,
            sourceAudioIdentity: sourceAudioIdentity,
            capturedAt: capturedAt
        )
    }

    // MARK: - Persistence entry point (invoked only when the flag is on)

    /// Concatenate the decoded 16 kHz shards into one continuous mono stream,
    /// fingerprint it, and upsert the record into `store`. A no-op when the
    /// episode is too short to yield any subfingerprint.
    ///
    /// Shards are concatenated in `startTime` order so the fingerprinter sees a
    /// CONTINUOUS stream — fingerprinting shards independently would break the
    /// STFT framing and sliding window at every shard boundary and produce a
    /// stream that does not align with a continuously-fingerprinted re-fetch.
    ///
    /// MEMORY NOTE (updated at activation, playhead-xsdz.36): the resample now
    /// walks the ordered shards as one virtual stream
    /// (`resampleToFingerprintRate(chunkedMono16kHz:)`, bit-identical to the
    /// concat path) so the only extra transient beyond the shards the pipeline
    /// already holds is the 11025 Hz output (~159 MB per decoded hour). The
    /// runner additionally caps capture at
    /// `RediffActivation.maxASideCaptureDurationSeconds`. Sizing of the
    /// PERSISTED artifact is tiny (~0.125 s/fp → ~116 KB/hour).
    static func captureAndPersist(
        shards: [AnalysisShard],
        assetId: String,
        sourceAudioIdentity: String,
        store: AnalysisStore,
        capturedAt: Double = Date().timeIntervalSince1970
    ) async throws {
        let ordered = shards.sorted { $0.startTime < $1.startTime }
        let stream = fingerprints(chunkedMono16kHz: ordered.map(\.samples))
        guard !stream.isEmpty else { return }
        let record = EpisodeFingerprintRecord(
            analysisAssetId: assetId,
            algorithmVersion: ChromaFingerprinter.algorithmVersion,
            secondsPerFingerprint: ChromaFingerprinter.secondsPerFingerprint,
            fingerprints: stream,
            sourceAudioIdentity: sourceAudioIdentity,
            capturedAt: capturedAt
        )
        try await store.upsertEpisodeFingerprints(record)
    }
}
