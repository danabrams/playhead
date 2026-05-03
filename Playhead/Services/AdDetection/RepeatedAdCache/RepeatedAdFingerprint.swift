// RepeatedAdFingerprint.swift
// 128-bit perceptual hash used as the cache key.
//
// V1 design: a 128-bit binarised digest derived from the existing
// `AcousticFingerprint` feature vector (which is itself derived from the
// per-window acoustic feature pipeline). For each of the 128 dimensions we
// emit a single bit by binarising against the per-fingerprint median, which
// makes the digest stable under uniform amplitude scaling and small
// perturbations of any single dimension — precisely the property we need
// for "same ad, slightly different mix" to land within `hammingDistance ≤ 6`.
//
// Why not chromaprint: adding a chromaprint dependency would require a
// dependency-policy approval (CLAUDE.md "Decision Authority"). The bead spec
// names chromaprint specifically, but the contract that matters is "128-bit
// audio-derived hash with Hamming-distance match." This v1 hash satisfies
// that contract using only in-tree primitives. The trade-off is documented
// in the PR body and tracked in a follow-up bead.
//
// Algorithm:
//   1. Build an `AcousticFingerprint` from the FeatureWindows covering the
//      ad span (existing helper, already tested).
//   2. Project the 128-dimensional float vector onto a 128-bit code by
//      thresholding each dimension at the vector's median — bit = 1 iff
//      `value > median`. Median-based thresholding is independent of
//      vector scale so two recordings of the same ad at different
//      loudness levels still hash the same.
//   3. Pack the resulting bits into two `UInt64` halves (high, low).
//
// Hamming distance is computed via `nonzeroBitCount` on the XOR of the
// two halves — no allocation, single-cycle popcount on arm64.

import Foundation

/// 128-bit perceptual hash for an ad-span audio segment.
struct RepeatedAdFingerprint: Sendable, Hashable, Codable {

    /// High 64 bits (most-significant). For a 128-element binarised
    /// vector this maps to dimensions 0..63 in big-endian ordering.
    let high: UInt64

    /// Low 64 bits (least-significant). Dimensions 64..127.
    let low: UInt64

    /// Total number of bits in the fingerprint. Pinned at 128 so tests
    /// asserting "Hamming distance ≤ 6 of 128" are explicit about the
    /// denominator.
    static let bitWidth: Int = 128

    /// All-zeros fingerprint — used as a sentinel for "fingerprint not
    /// derivable" (e.g. zero feature windows). A zero fingerprint is
    /// still a legal Codable value but callers SHOULD treat it as a
    /// signal to skip caching, since two unrelated zero-energy spans
    /// would otherwise collide.
    static let zero = RepeatedAdFingerprint(high: 0, low: 0)

    init(high: UInt64, low: UInt64) {
        self.high = high
        self.low = low
    }

    /// Hamming distance to another fingerprint, in bits. Symmetric.
    /// Result range: `0 ... bitWidth`.
    func hammingDistance(to other: RepeatedAdFingerprint) -> Int {
        let xorHigh = high ^ other.high
        let xorLow = low ^ other.low
        return xorHigh.nonzeroBitCount + xorLow.nonzeroBitCount
    }

    /// Returns `true` when this fingerprint is the all-zeros sentinel.
    var isZero: Bool { high == 0 && low == 0 }

    /// SQLite-friendly hex serialisation (32 lowercase hex chars).
    /// Used to store the fingerprint in a `TEXT NOT NULL` column without
    /// pulling in BLOB binding.
    var hexString: String {
        String(format: "%016llx%016llx", high, low)
    }

    /// Parse back from `hexString`. Returns `nil` if the string is not
    /// exactly 32 lowercase hex characters.
    init?(hexString: String) {
        guard hexString.count == 32 else { return nil }
        let highHex = String(hexString.prefix(16))
        let lowHex = String(hexString.suffix(16))
        guard let h = UInt64(highHex, radix: 16),
              let l = UInt64(lowHex, radix: 16)
        else { return nil }
        self.init(high: h, low: l)
    }
}

// MARK: - Derivation from acoustic features

extension RepeatedAdFingerprint {

    /// Build a 128-bit perceptual fingerprint from a sequence of
    /// `FeatureWindow` rows that overlap the ad span.
    ///
    /// Empty input → ``zero``. The caller MUST treat zero as a "do not
    /// cache" sentinel.
    static func from(featureWindows: [FeatureWindow]) -> RepeatedAdFingerprint {
        guard !featureWindows.isEmpty else { return .zero }

        // Reuse the well-tested `AcousticFingerprint.fromFeatureWindows`
        // helper — it already produces a length-`vectorLength` float
        // vector that is L2-normalised and time-invariant. We then
        // binarise it against its own median.
        let acoustic = AcousticFingerprint.fromFeatureWindows(featureWindows)
        return Self.binarise(acoustic.values)
    }

    /// Binarise a float vector into a 128-bit fingerprint using
    /// median thresholding. Vector length is padded with zeros (or
    /// truncated) to exactly 128.
    static func binarise(_ values: [Float]) -> RepeatedAdFingerprint {
        var padded = Array(values.prefix(128))
        if padded.count < 128 {
            padded.append(contentsOf: [Float](repeating: 0, count: 128 - padded.count))
        }
        // All-zero vector → all-zero fingerprint sentinel.
        if padded.allSatisfy({ $0 == 0 }) { return .zero }

        var sorted = padded
        sorted.sort()
        // Median for an even-length vector: average of middle two.
        let median: Float = {
            let mid = sorted.count / 2
            return (sorted[mid - 1] + sorted[mid]) / 2
        }()

        var high: UInt64 = 0
        var low: UInt64 = 0
        for i in 0..<128 {
            // Strict greater-than: ties (== median) → bit 0. This makes
            // a constant vector (all entries equal to its median) hash
            // to all zeros (which is the documented sentinel).
            if padded[i] > median {
                if i < 64 {
                    high |= (1 as UInt64) << (63 - i)
                } else {
                    low |= (1 as UInt64) << (63 - (i - 64))
                }
            }
        }
        return RepeatedAdFingerprint(high: high, low: low)
    }

    /// Test-seam constructor: build a fingerprint from a 128-element
    /// `[Bool]` bit array (most-significant bit at index 0). The
    /// production code path (`from(featureWindows:)`) does not use this;
    /// it exists so unit tests can construct fingerprints with exact
    /// bit patterns when verifying Hamming-distance correctness at the
    /// 6/7-bit boundary.
    static func fromBits(_ bits: [Bool]) -> RepeatedAdFingerprint {
        precondition(bits.count == 128, "RepeatedAdFingerprint.fromBits requires exactly 128 bits, got \(bits.count)")
        var high: UInt64 = 0
        var low: UInt64 = 0
        for i in 0..<128 where bits[i] {
            if i < 64 {
                high |= (1 as UInt64) << (63 - i)
            } else {
                low |= (1 as UInt64) << (63 - (i - 64))
            }
        }
        return RepeatedAdFingerprint(high: high, low: low)
    }
}
