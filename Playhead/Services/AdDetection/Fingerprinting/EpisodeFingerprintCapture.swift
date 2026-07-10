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

    /// Master flag for played-copy fingerprint capture on the live pipeline.
    /// `false` = the `AnalysisJobRunner` capture branch is skipped entirely
    /// (no decode change, no store write). xsdz.29 turns this on once it
    /// consumes the persisted stream.
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
    /// MEMORY NOTE (residual for xsdz.29): this holds the whole episode's PCM
    /// (16 kHz input + 11025 Hz resampled) in memory at once — ~230 MB + ~160 MB
    /// transient for a 60-minute episode. Acceptable for a dormant, flag-OFF
    /// download-time path; the as-played tap (file header) would stream
    /// incrementally and avoid this. Sizing of the PERSISTED artifact is tiny
    /// (~0.125 s/fp → ~116 KB/hour).
    static func captureAndPersist(
        shards: [AnalysisShard],
        assetId: String,
        sourceAudioIdentity: String,
        store: AnalysisStore,
        capturedAt: Double = Date().timeIntervalSince1970
    ) async throws {
        let ordered = shards.sorted { $0.startTime < $1.startTime }
        var mono = [Float]()
        mono.reserveCapacity(ordered.reduce(0) { $0 + $1.samples.count })
        for shard in ordered { mono.append(contentsOf: shard.samples) }
        guard let record = makeRecord(
            assetId: assetId,
            sourceAudioIdentity: sourceAudioIdentity,
            mono16kHz: mono,
            capturedAt: capturedAt
        ) else { return }
        try await store.upsertEpisodeFingerprints(record)
    }
}
