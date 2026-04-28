// AcousticFingerprint.swift
// playhead-gtt9.13: Compact on-device acoustic fingerprint used by
// `AdCatalogStore` to cross-match ad spans across episodes.
//
// Design goals
// ------------
// * Pure value type — easy to ship across actor boundaries and to serialize.
// * Fixed vector length (`vectorLength = 64`) so cosine similarity is a
//   straight dot-product with no allocation beyond the inputs.
// * Deterministic generation from `[Float]` PCM so two spans of identical
//   audio produce identical fingerprints (testable, cacheable).
// * On-device compute only — no network, no cloud (legal mandate).
//
// Why a mel-style summary vector (not MFCC, not a neural embedding)
// ----------------------------------------------------------------
// The MVP similarity job is "was this ad run already fingerprinted from an
// earlier episode?". Ads that recur tend to be byte-identical reads of the
// same creative — a mel-energy-ish summary plus a short "zero-cross rate"
// tail is enough to discriminate. A neural embedding would be stronger but
// would add a model dependency for no precision the corpus actually needs
// today. gtt9.12 owns richer acoustic features; if it produces a better
// vector, the store schema stores a `Data` blob and can migrate.

import Accelerate
import Foundation

// MARK: - AcousticFingerprint

/// Compact fixed-length acoustic fingerprint of an ad span.
///
/// The vector is normalized to unit length, so `similarity(_:_:)` reduces
/// to a plain dot-product in `[0, 1]` for non-negative fingerprints (our
/// case: magnitudes cannot be negative).
///
/// The public API is intentionally thin:
///   * `init(values:)` validates length and normalizes.
///   * `similarity(_:_:)` computes cosine similarity.
///   * `fromPCM(_:sampleRate:)` derives a fingerprint from raw mono PCM.
struct AcousticFingerprint: Sendable, Hashable, Codable {

    /// Fixed vector length. 64 floats is ~256 bytes — compact enough to
    /// store thousands of entries per user with no meaningful disk cost,
    /// large enough to carry mel-band envelope + a few scalar summary
    /// descriptors without saturating.
    static let vectorLength: Int = 64

    /// Normalized, non-negative feature values. Length always equals
    /// `vectorLength`; constructor guards this.
    let values: [Float]

    /// Construct from a non-negative feature vector. The input is padded
    /// or truncated to `vectorLength`, then L2-normalized so
    /// `similarity(a, b)` is a cosine in `[0, 1]`.
    ///
    /// Returns `nil` if any input element is negative — the
    /// `similarity(_:_:)` contract assumes non-negative fingerprints
    /// (clamps negative dot products to 0), and accepting negatives here
    /// would silently invalidate that. Zero-length or all-zero inputs are
    /// valid and produce a canonical zero fingerprint that compares
    /// similarity 0 against everything (including itself).
    init?(values: [Float]) {
        for v in values where v < 0 { return nil }

        let clipped: [Float]
        if values.count >= Self.vectorLength {
            clipped = Array(values.prefix(Self.vectorLength))
        } else {
            clipped = values + [Float](repeating: 0, count: Self.vectorLength - values.count)
        }

        // L2 norm.
        var sumSq: Float = 0
        for v in clipped { sumSq += v * v }
        let norm = sqrtf(sumSq)
        if norm <= .ulpOfOne {
            self.values = [Float](repeating: 0, count: Self.vectorLength)
        } else {
            var out = clipped
            let inv = 1.0 / norm
            for i in 0..<out.count { out[i] = out[i] * inv }
            self.values = out
        }
    }

    /// True iff the fingerprint is the canonical zero fingerprint
    /// (constructed from an empty or all-zero input). Zero fingerprints
    /// should never match — callers can filter them out.
    var isZero: Bool {
        for v in values where v != 0 { return false }
        return true
    }

    /// Canonical zero fingerprint, equivalent to `AcousticFingerprint(values: [])!`.
    /// Used by internal builders that want to bail out without forcing
    /// the failable init's unwrap at every site.
    static var zero: AcousticFingerprint {
        AcousticFingerprint(rawNormalizedValues: [Float](repeating: 0, count: vectorLength))
    }

    // MARK: - Serialization

    /// Encode to a little-endian `Data` blob for SQLite storage.
    var data: Data {
        var out = Data()
        out.reserveCapacity(values.count * MemoryLayout<Float>.size)
        for v in values {
            var bits = v.bitPattern.littleEndian
            withUnsafeBytes(of: &bits) { out.append(contentsOf: $0) }
        }
        return out
    }

    /// Decode from the `data` representation. Returns nil on wrong length.
    init?(data: Data) {
        let stride = MemoryLayout<Float>.size
        guard data.count == Self.vectorLength * stride else { return nil }
        // Block bind the data into a Float buffer rather than walking
        // index-by-index. Layout matches `var data` (little-endian
        // bit-patterns of the L2-normalized vector).
        let vs: [Float] = data.withUnsafeBytes { rawBuf -> [Float] in
            let floats = rawBuf.bindMemory(to: Float.self)
            // bindMemory yields a typed buffer that may be empty if the
            // raw count doesn't divide evenly — the count check above
            // guarantees we get exactly vectorLength entries.
            return Array(floats.prefix(Self.vectorLength))
        }
        guard vs.count == Self.vectorLength else { return nil }
        // Already normalized when we wrote; reconstruct without renormalizing
        // by injecting straight into a bypass initializer.
        self = AcousticFingerprint(rawNormalizedValues: vs)
    }

    /// Internal: construct from values that are already L2-normalized.
    /// Used by `init?(data:)` to avoid touching the norm a second time.
    private init(rawNormalizedValues vs: [Float]) {
        precondition(vs.count == Self.vectorLength)
        self.values = vs
    }

    // MARK: - Similarity

    /// Cosine similarity in `[0, 1]` for non-negative fingerprints.
    /// Symmetric; bounded; identity = 1.0 (modulo zero-vector edge case).
    ///
    /// `init?(values:)` rejects negative inputs so the dot product of two
    /// L2-normalized non-negative vectors is mathematically non-negative;
    /// any sub-zero value is FP drift and is asserted in DEBUG. A second
    /// clamp at `1.0` guards the upper end against the same drift.
    static func similarity(
        _ a: AcousticFingerprint,
        _ b: AcousticFingerprint
    ) -> Float {
        if a.isZero || b.isZero { return 0 }
        var dot: Float = 0
        // Manual loop outperforms vDSP_dotpr for n=64 by avoiding the
        // symbol lookup for negligible inputs; kept simple and in-line.
        for i in 0..<vectorLength {
            dot += a.values[i] * b.values[i]
        }
        assert(dot >= -.ulpOfOne, "AcousticFingerprint.similarity dot=\(dot) negative — non-negative invariant broken")
        // Clamp to guard against tiny FP drift above 1.0 / below 0.0.
        if dot > 1.0 { return 1.0 }
        if dot < 0.0 { return 0.0 }
        return dot
    }

    // MARK: - PCM → fingerprint

    /// Derive a fingerprint from mono PCM at an arbitrary sample rate.
    ///
    /// The vector is built as:
    ///   * 60 mel-ish band energies (log-compressed, non-negative)
    ///   * 1  zero-crossing rate
    ///   * 1  RMS energy
    ///   * 1  spectral centroid (normalized 0..1)
    ///   * 1  spectral flatness (0..1)
    ///
    /// For span durations below ~0.25s this returns a zero fingerprint
    /// (too short to be a reliable match). Callers should check `isZero`.
    static func fromPCM(_ pcm: [Float], sampleRate: Double) -> AcousticFingerprint {
        guard sampleRate > 0, pcm.count >= Int(sampleRate * 0.25) else {
            return AcousticFingerprint.zero
        }

        let bandCount = 60
        var bands = [Float](repeating: 0, count: bandCount)

        // Chunked STFT with 512-sample window, 256 hop. Not a perfect mel
        // filterbank — we approximate by log-spacing the FFT bin groups
        // into `bandCount` bands. Cheap, deterministic, sufficient.
        let windowSize = 512
        let hopSize = 256
        guard pcm.count >= windowSize else {
            return AcousticFingerprint.zero
        }

        let binCount = windowSize / 2
        // Precompute log-spaced band edges across the positive FFT bins.
        var bandEdges = [Int](repeating: 0, count: bandCount + 1)
        for i in 0...bandCount {
            let frac = Double(i) / Double(bandCount)
            // Log mapping: bin = floor(binCount * (2^(frac*log2(binCount+1)) - 1) / binCount)
            // Simpler monotone log: binCount * (exp(frac * ln(binCount)) - 1) / (binCount - 1)
            let logIdx = exp(frac * log(Double(binCount))) - 1.0
            let clamped = max(0, min(binCount - 1, Int(logIdx)))
            bandEdges[i] = clamped
        }

        var frameCount = 0
        var zcrSum: Float = 0
        var rmsSum: Float = 0
        var centroidSum: Float = 0
        var flatnessSum: Float = 0

        // Hann window table.
        var hann = [Float](repeating: 0, count: windowSize)
        for i in 0..<windowSize {
            hann[i] = 0.5 - 0.5 * cosf(2 * .pi * Float(i) / Float(windowSize - 1))
        }

        // Set up a single vDSP DFT plan for the whole episode. The plan is
        // reusable across frames and converts the per-frame work from the
        // pre-fix O(windowSize²) hand-rolled DFT (≈ 246M trig calls for a
        // 30s ad at 16 kHz) to the highly-optimized vDSP path.
        guard let dftSetup = vDSP_DFT_zop_CreateSetup(
            nil,
            vDSP_Length(windowSize),
            .FORWARD
        ) else {
            return AcousticFingerprint.zero
        }
        defer { vDSP_DFT_DestroySetup(dftSetup) }

        // Per-frame scratch buffers. Imag input is zero-filled (real input).
        var realIn = [Float](repeating: 0, count: windowSize)
        var imagIn = [Float](repeating: 0, count: windowSize)
        var realOut = [Float](repeating: 0, count: windowSize)
        var imagOut = [Float](repeating: 0, count: windowSize)

        var offset = 0
        while offset + windowSize <= pcm.count {
            var zc: Int = 0
            var rms: Float = 0
            for i in 0..<windowSize {
                let s = pcm[offset + i]
                realIn[i] = s * hann[i]
                rms += s * s
                if i > 0 {
                    let prev = pcm[offset + i - 1]
                    if (prev >= 0) != (s >= 0) { zc += 1 }
                }
            }
            rms = sqrtf(rms / Float(windowSize))

            // Compute magnitude spectrum via vDSP forward DFT. Replaces the
            // naive O(windowSize²) hand-rolled DFT — ~50× faster on device.
            vDSP_DFT_Execute(dftSetup, realIn, imagIn, &realOut, &imagOut)

            var mags = [Float](repeating: 0, count: binCount)
            var magSum: Float = 0
            var logMagSum: Float = 0
            var weightedBinSum: Float = 0
            for k in 0..<binCount {
                let re = realOut[k]
                let im = imagOut[k]
                let m = sqrtf(re * re + im * im)
                mags[k] = m
                magSum += m
                logMagSum += logf(max(m, 1e-9))
                weightedBinSum += m * Float(k)
            }

            // Accumulate per-band log energy.
            for b in 0..<bandCount {
                let lo = bandEdges[b]
                let hi = max(lo + 1, bandEdges[b + 1])
                var e: Float = 0
                for k in lo..<hi { e += mags[k] }
                bands[b] += logf(1 + e)
            }

            // Scalars.
            zcrSum += Float(zc) / Float(windowSize)
            rmsSum += rms
            if magSum > 1e-9 {
                centroidSum += (weightedBinSum / magSum) / Float(binCount)
                // Flatness = geomean / arithmean on linear magnitudes.
                let geom = expf(logMagSum / Float(binCount))
                let arith = magSum / Float(binCount)
                flatnessSum += (arith > 1e-9) ? (geom / arith) : 0
            }
            frameCount += 1
            offset += hopSize
        }

        guard frameCount > 0 else {
            return AcousticFingerprint.zero
        }

        let fc = Float(frameCount)
        for i in 0..<bandCount { bands[i] /= fc }

        var vector = bands
        vector.append(zcrSum / fc)
        vector.append(rmsSum / fc)
        vector.append(centroidSum / fc)
        vector.append(flatnessSum / fc)

        // Vector is non-negative by construction (log energies, |zcr|,
        // rms, centroid as fraction, flatness as positive ratio). The
        // `?? .zero` is belt-and-suspenders against FP edge cases.
        return AcousticFingerprint(values: vector) ?? .zero
    }

    // MARK: - FeatureWindow → fingerprint (gtt9.17)

    /// Derive a fingerprint from a sequence of `FeatureWindow`s.
    ///
    /// Where `fromPCM` works at the audio layer, this constructor maps the
    /// episode-level acoustic feature stream (the same signal the
    /// `AcousticFeaturePipeline` consumes) to the catalog's fingerprint
    /// space. The main MVP call sites (`AdDetectionService.runBackfill`)
    /// already have `[FeatureWindow]` on hand and do NOT have raw PCM, so
    /// this is the cheap path into `AdCatalogStore` without re-decoding
    /// audio.
    ///
    /// Mapping
    /// -------
    /// 8 feature streams × 8 summary statistics = 64-dim vector, matching
    /// `AcousticFingerprint.vectorLength`. Deliberately TIME-INVARIANT —
    /// no window timestamps enter the summary so the same creative recurring
    /// at a different timestamp in another episode produces the same
    /// fingerprint (which is the whole point of a catalog).
    ///
    /// Feature streams:
    ///   1. rms
    ///   2. spectralFlux
    ///   3. musicProbability
    ///   4. speakerChangeProxyScore
    ///   5. musicBedChangeScore
    ///   6. musicBedOnsetScore
    ///   7. musicBedOffsetScore
    ///   8. pauseProbability
    ///
    /// Stats per stream (all bounded non-negative):
    ///   1. mean
    ///   2. max
    ///   3. min
    ///   4. population standard deviation
    ///   5. sum / N (scaled energy, redundant with mean for fixed-N inputs
    ///      but robust against short vs long span length)
    ///   6. mean of top-3 values (p90-ish)
    ///   7. mean of bottom-3 values (p10-ish)
    ///   8. active-fraction (share of windows with value > 0.5)
    ///
    /// The resulting 64-float vector is passed to `init(values:)`, which
    /// L2-normalizes and zero-guards in the usual way. Zero/empty input
    /// returns a zero fingerprint (filtered out by the catalog insert path).
    static func fromFeatureWindows(_ windows: [FeatureWindow]) -> AcousticFingerprint {
        guard !windows.isEmpty else {
            return AcousticFingerprint.zero
        }

        let streams: [[Double]] = [
            windows.map { $0.rms },
            windows.map { $0.spectralFlux },
            windows.map { $0.musicProbability },
            windows.map { $0.speakerChangeProxyScore },
            windows.map { $0.musicBedChangeScore },
            windows.map { $0.musicBedOnsetScore },
            windows.map { $0.musicBedOffsetScore },
            windows.map { $0.pauseProbability }
        ]

        var vector: [Float] = []
        vector.reserveCapacity(64)

        for stream in streams {
            guard !stream.isEmpty else {
                for _ in 0..<8 { vector.append(0) }
                continue
            }

            let n = Double(stream.count)
            let sum = stream.reduce(0, +)
            let mean = sum / n
            let maxV = stream.max() ?? 0
            let minV = stream.min() ?? 0

            var variance: Double = 0
            for v in stream {
                let d = v - mean
                variance += d * d
            }
            variance /= n
            let stddev = variance > 0 ? variance.squareRoot() : 0

            let energyScaled = sum / n

            let sorted = stream.sorted(by: >)   // descending
            let topK = min(3, sorted.count)
            var topMean: Double = 0
            for i in 0..<topK { topMean += sorted[i] }
            topMean /= Double(topK)

            let ascCount = min(3, sorted.count)
            var bottomMean: Double = 0
            for i in (sorted.count - ascCount)..<sorted.count { bottomMean += sorted[i] }
            bottomMean /= Double(ascCount)

            let activeFraction = Double(stream.filter { $0 > 0.5 }.count) / n

            // Clamp to non-negative — cosine-similarity on our store assumes
            // non-negative fingerprints, and all feature streams are already
            // bounded in `[0, 1]` (or `[0, ∞)` for rms). A rogue negative
            // would be a bug elsewhere.
            vector.append(Float(max(0, mean)))
            vector.append(Float(max(0, maxV)))
            vector.append(Float(max(0, minV)))
            vector.append(Float(max(0, stddev)))
            vector.append(Float(max(0, energyScaled)))
            vector.append(Float(max(0, topMean)))
            vector.append(Float(max(0, bottomMean)))
            vector.append(Float(max(0, activeFraction)))
        }

        // All entries are explicitly clamped non-negative above; the
        // `?? .zero` guards against the failable init contract change
        // without requiring the caller to handle an optional.
        return AcousticFingerprint(values: vector) ?? .zero
    }
}

// MARK: - Debug description

extension AcousticFingerprint: CustomStringConvertible {
    var description: String {
        if isZero { return "AcousticFingerprint(zero)" }
        let head = values.prefix(4).map { String(format: "%.3f", $0) }.joined(separator: ",")
        return "AcousticFingerprint([\(head)...], len=\(values.count))"
    }
}
