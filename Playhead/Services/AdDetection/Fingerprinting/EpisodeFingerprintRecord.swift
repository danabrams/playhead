// EpisodeFingerprintRecord.swift
// playhead-xsdz.27: the persisted per-episode (played-copy) fingerprint
// stream produced by `ChromaFingerprinter` and stored in `AnalysisStore`
// so the rediff width-oracle (xsdz.29) and cross-episode library matching
// (xsdz.17) never recompute it.
//
// STALENESS CONTRACT (the whole reason this type exists — see
// `ChromaFingerprinter.algorithmVersion`): a fingerprint stream is only
// comparable to another produced by the SAME `algorithmVersion`. The record
// therefore carries `algorithmVersion` so the store can treat a version
// mismatch as ABSENT (re-fingerprint) rather than Hamming-comparing across
// versions, which degrades silently into misalignment. `secondsPerFingerprint`
// is persisted too so a reader can interpret the stream's time granularity
// without assuming today's pipeline constants.
//
// SAME-DEVICE ONLY: like the fingerprinter, persisted streams are
// same-device artifacts (libm/Float rounding may differ across
// architectures/OS versions near a quantization threshold). Do not sync or
// share them across devices/users without revalidating — see the
// `ChromaFingerprinter.algorithmVersion` "SAME-DEVICE CONTRACT" note.

import Foundation

/// One persisted played-copy fingerprint stream for a single analyzed
/// episode, keyed by its `analysisAssetId`.
struct EpisodeFingerprintRecord: Sendable, Equatable {

    /// `analysis_assets.id` this stream was captured for. Primary key in the
    /// store (one played-copy stream per analyzed episode).
    let analysisAssetId: String

    /// `ChromaFingerprinter.algorithmVersion` at capture time. THE staleness
    /// key: the store returns this record from a read ONLY when this equals
    /// the current `ChromaFingerprinter.algorithmVersion`.
    let algorithmVersion: UInt32

    /// Exact seconds represented by each subfingerprint (hop / sampleRate).
    /// Persisted so a reader can map subfingerprint index → episode time even
    /// if the pipeline constant later changes; within a matching
    /// `algorithmVersion` this equals `ChromaFingerprinter.secondsPerFingerprint`.
    let secondsPerFingerprint: Double

    /// The 32-bit subfingerprint stream (`ChromaFingerprinter.fingerprint`).
    let fingerprints: [UInt32]

    /// Source-audio identity of the copy that was fingerprinted — the
    /// `analysis_assets.assetFingerprint` at capture time. Lets a reader
    /// detect that the underlying audio changed (asset id reused for a
    /// re-download of different bytes) even when the row still exists.
    let sourceAudioIdentity: String

    /// Wall-clock capture time (UNIX seconds).
    let capturedAt: Double
}

/// Deterministic, host-endianness-independent codec for the `[UInt32]`
/// subfingerprint stream ⇆ the BLOB column. Explicit little-endian byte
/// packing so a stored blob decodes identically regardless of the host's
/// native byte order (belt-and-suspenders — persisted streams are
/// same-device — but it keeps the on-disk format unambiguous and pinnable).
enum EpisodeFingerprintBlobCodec {

    /// Bytes per packed subfingerprint.
    static let bytesPerFingerprint = MemoryLayout<UInt32>.size  // 4

    /// Pack `[UInt32]` into a little-endian byte blob (4 bytes each).
    static func encode(_ fingerprints: [UInt32]) -> Data {
        guard !fingerprints.isEmpty else { return Data() }
        var data = Data(count: fingerprints.count * bytesPerFingerprint)
        data.withUnsafeMutableBytes { (raw: UnsafeMutableRawBufferPointer) in
            for (index, value) in fingerprints.enumerated() {
                let base = index * bytesPerFingerprint
                raw[base + 0] = UInt8(truncatingIfNeeded: value)
                raw[base + 1] = UInt8(truncatingIfNeeded: value >> 8)
                raw[base + 2] = UInt8(truncatingIfNeeded: value >> 16)
                raw[base + 3] = UInt8(truncatingIfNeeded: value >> 24)
            }
        }
        return data
    }

    /// Decode a little-endian byte blob back into `[UInt32]`. Returns nil when
    /// the byte count is not a whole multiple of 4 (corrupt/truncated blob) so
    /// the caller can treat it as ABSENT (re-fingerprint) rather than
    /// fabricating a mis-sized stream. `withUnsafeBytes` gives a 0-based view
    /// of the actual bytes, so this is correct even for a sliced `Data` whose
    /// `startIndex` is non-zero.
    static func decode(_ data: Data) -> [UInt32]? {
        guard data.count % bytesPerFingerprint == 0 else { return nil }
        let count = data.count / bytesPerFingerprint
        guard count > 0 else { return [] }
        var result = [UInt32](repeating: 0, count: count)
        data.withUnsafeBytes { (raw: UnsafeRawBufferPointer) in
            for index in 0..<count {
                let base = index * bytesPerFingerprint
                let b0 = UInt32(raw[base + 0])
                let b1 = UInt32(raw[base + 1])
                let b2 = UInt32(raw[base + 2])
                let b3 = UInt32(raw[base + 3])
                result[index] = b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)
            }
        }
        return result
    }
}
