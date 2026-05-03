// RepeatedAdFingerprint.swift
// 64-bit perceptual hash used as the cache key.
//
// V1 design: a 64-bit binarised digest derived from the existing
// `AcousticFingerprint` feature vector (which is itself derived from the
// per-window acoustic feature pipeline). For each of the 64 dimensions we
// emit a single bit by binarising against the per-fingerprint median, which
// makes the digest stable under uniform amplitude scaling and small
// perturbations of any single dimension — precisely the property we need
// for "same ad, slightly different mix" to land within
// `hammingDistance ≤ 3` (≈4.7% bit-error tolerance over 64 bits).
//
// History note (playhead-43ed C3): the type was originally declared as a
// 128-bit pair (`high`/`low`) with threshold `≤ 6 of 128`, but the upstream
// `AcousticFingerprint.vectorLength` is 64, so bits 64..127 were always
// zero in production. The "128-bit" framing was a documentation lie and
// the effective signal-to-noise was `≤ 6 of 64`. The fix is to honestly
// declare 64 bits / threshold 3 — preserving the same effective bit-error
// tolerance the original 6/128 expressed without the misleading
// denominator.
//
// Why not chromaprint: adding a chromaprint dependency would require a
// dependency-policy approval (CLAUDE.md "Decision Authority"). The bead spec
// names chromaprint specifically, but the contract that matters is "audio-
// derived hash with Hamming-distance match." This v1 hash satisfies that
// contract using only in-tree primitives. The trade-off is documented in
// the PR body and tracked in a follow-up bead.
//
// Algorithm:
//   1. Build an `AcousticFingerprint` from the FeatureWindows covering the
//      ad span (existing helper, already tested).
//   2. Project the 64-dimensional float vector onto a 64-bit code by
//      thresholding each dimension at the vector's median — bit = 1 iff
//      `value > median`. Median-based thresholding is independent of
//      vector scale so two recordings of the same ad at different
//      loudness levels still hash the same.
//   3. Pack the resulting bits into a `UInt64`.
//
// Hamming distance is computed via `nonzeroBitCount` on the XOR of the
// two values — no allocation, single-cycle popcount on arm64.

import Foundation

/// 64-bit perceptual hash for an ad-span audio segment.
struct RepeatedAdFingerprint: Sendable, Hashable, Codable {

    /// Packed bits. Dimension 0 maps to the most-significant bit (bit 63)
    /// for big-endian-ish layout.
    let bits: UInt64

    /// Total number of bits in the fingerprint. Pinned at 64 to match the
    /// upstream `AcousticFingerprint.vectorLength`. If that grows, this
    /// must grow with it (asserted by `bitWidthMatchesAcousticFingerprintLength`).
    static let bitWidth: Int = 64

    /// All-zeros fingerprint — used as a sentinel for "fingerprint not
    /// derivable" (e.g. zero feature windows). A zero fingerprint is
    /// still a legal Codable value but callers SHOULD treat it as a
    /// signal to skip caching, since two unrelated zero-energy spans
    /// would otherwise collide.
    static let zero = RepeatedAdFingerprint(bits: 0)

    init(bits: UInt64) {
        self.bits = bits
    }

    /// Hamming distance to another fingerprint, in bits. Symmetric.
    /// Result range: `0 ... bitWidth`.
    func hammingDistance(to other: RepeatedAdFingerprint) -> Int {
        (bits ^ other.bits).nonzeroBitCount
    }

    /// Returns `true` when this fingerprint is the all-zeros sentinel.
    var isZero: Bool { bits == 0 }

    /// SQLite-friendly hex serialisation (16 lowercase hex chars).
    /// Used to store the fingerprint in a `TEXT NOT NULL` column without
    /// pulling in BLOB binding.
    var hexString: String {
        String(format: "%016llx", bits)
    }

    /// Parse back from `hexString`. Returns `nil` if the string is not
    /// exactly 16 lowercase hex characters.
    init?(hexString: String) {
        guard hexString.count == 16 else { return nil }
        guard let v = UInt64(hexString, radix: 16) else { return nil }
        self.init(bits: v)
    }
}

// MARK: - Derivation from acoustic features

extension RepeatedAdFingerprint {

    /// Build a 64-bit perceptual fingerprint from a sequence of
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

    /// Binarise a float vector into a 64-bit fingerprint using median
    /// thresholding. Vector length is padded with zeros (or truncated)
    /// to exactly `bitWidth`.
    static func binarise(_ values: [Float]) -> RepeatedAdFingerprint {
        let n = bitWidth
        var padded = Array(values.prefix(n))
        if padded.count < n {
            padded.append(contentsOf: [Float](repeating: 0, count: n - padded.count))
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

        var bits: UInt64 = 0
        for i in 0..<n {
            // Strict greater-than: ties (== median) → bit 0. This makes
            // a constant vector (all entries equal to its median) hash
            // to all zeros (which is the documented sentinel).
            if padded[i] > median {
                bits |= (1 as UInt64) << (UInt64(n - 1) - UInt64(i))
            }
        }
        return RepeatedAdFingerprint(bits: bits)
    }

    /// Test-seam constructor: build a fingerprint from a `bitWidth`-element
    /// `[Bool]` bit array (most-significant bit at index 0). The
    /// production code path (`from(featureWindows:)`) does not use this;
    /// it exists so unit tests can construct fingerprints with exact
    /// bit patterns when verifying Hamming-distance correctness at the
    /// 3/4-bit boundary.
    static func fromBits(_ source: [Bool]) -> RepeatedAdFingerprint {
        precondition(
            source.count == bitWidth,
            "RepeatedAdFingerprint.fromBits requires exactly \(bitWidth) bits, got \(source.count)"
        )
        var bits: UInt64 = 0
        for i in 0..<bitWidth where source[i] {
            bits |= (1 as UInt64) << (UInt64(bitWidth - 1) - UInt64(i))
        }
        return RepeatedAdFingerprint(bits: bits)
    }
}
