import Foundation

/// playhead-66cn: the canonical SHA-256 of a pinned file, hashed once per
/// distinct file rather than once per probe poll.
///
/// The day-0 readiness probe is polled up to `maxAttempts` times, and the
/// fingerprint it needs is `FileHasher.sha256(fileURL:)` — the SAME derivation
/// registration stores as `analysis_assets.assetFingerprint`, so the two can
/// be compared as one quantity. Hashing a ~54 MB file on every poll would be
/// the cost; memoizing by path alone would be the defect, because a
/// re-download replaces the bytes at the SAME per-episode path. So the key is
/// (path, size, modification date): a replaced file is rehashed, an unchanged
/// one is not.
actor PinnedFileFingerprintCache {
    private struct Key: Hashable {
        let path: String
        let size: Int
        let modified: Date?
    }
    private var cache: [Key: String] = [:]
    private(set) var hashCount = 0

    init() {}

    func canonicalFingerprint(of fileURL: URL) throws -> String {
        let attrs = try FileManager.default.attributesOfItem(atPath: fileURL.path)
        let key = Key(
            path: fileURL.path,
            size: (attrs[.size] as? NSNumber)?.intValue ?? -1,
            modified: attrs[.modificationDate] as? Date
        )
        if let hit = cache[key] { return hit }
        hashCount += 1
        let sha = try FileHasher.sha256(fileURL: fileURL)
        cache[key] = sha
        return sha
    }
}
