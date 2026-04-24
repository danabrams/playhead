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

    /// Construct from an arbitrary-length feature vector. The input is
    /// padded or truncated to `vectorLength`, then L2-normalized so
    /// `similarity(a, b)` is a cosine. Throws nothing — zero-length or
    /// all-zero inputs produce a canonical zero fingerprint that compares
    /// similarity 0 against everything (including itself).
    init(values: [Float]) {
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
        var vs = [Float]()
        vs.reserveCapacity(Self.vectorLength)
        for i in 0..<Self.vectorLength {
            let lo = data.index(data.startIndex, offsetBy: i * stride)
            let hi = data.index(lo, offsetBy: stride)
            let chunk = data[lo..<hi]
            let bits = chunk.withUnsafeBytes { $0.load(as: UInt32.self) }
            vs.append(Float(bitPattern: UInt32(littleEndian: bits)))
        }
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
        // Clamp to guard against tiny FP drift above 1.0.
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
            return AcousticFingerprint(values: [])
        }

        let bandCount = 60
        var bands = [Float](repeating: 0, count: bandCount)

        // Chunked STFT with 512-sample window, 256 hop. Not a perfect mel
        // filterbank — we approximate by log-spacing the FFT bin groups
        // into `bandCount` bands. Cheap, deterministic, sufficient.
        let windowSize = 512
        let hopSize = 256
        guard pcm.count >= windowSize else {
            return AcousticFingerprint(values: [])
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

        var offset = 0
        while offset + windowSize <= pcm.count {
            var frame = [Float](repeating: 0, count: windowSize)
            var zc: Int = 0
            var rms: Float = 0
            for i in 0..<windowSize {
                let s = pcm[offset + i]
                frame[i] = s * hann[i]
                rms += s * s
                if i > 0 {
                    let prev = pcm[offset + i - 1]
                    if (prev >= 0) != (s >= 0) { zc += 1 }
                }
            }
            rms = sqrtf(rms / Float(windowSize))

            // Compute magnitude spectrum via DFT. For windowSize=512 this
            // is O(n^2) = 262k multiplies per frame — acceptable for
            // short ad spans (<30s ≈ 2500 frames) on device, and avoids
            // importing an FFT setup. If perf bites, swap to vDSP_fft.
            var mags = [Float](repeating: 0, count: binCount)
            var magSum: Float = 0
            var logMagSum: Float = 0
            var weightedBinSum: Float = 0
            for k in 0..<binCount {
                var re: Float = 0
                var im: Float = 0
                let twoPiK = 2.0 * Float.pi * Float(k) / Float(windowSize)
                for n in 0..<windowSize {
                    let ang = twoPiK * Float(n)
                    re += frame[n] * cosf(ang)
                    im -= frame[n] * sinf(ang)
                }
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
            return AcousticFingerprint(values: [])
        }

        let fc = Float(frameCount)
        for i in 0..<bandCount { bands[i] /= fc }

        var vector = bands
        vector.append(zcrSum / fc)
        vector.append(rmsSum / fc)
        vector.append(centroidSum / fc)
        vector.append(flatnessSum / fc)

        return AcousticFingerprint(values: vector)
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
