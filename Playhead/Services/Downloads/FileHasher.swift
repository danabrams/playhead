// FileHasher.swift
// Shared SHA-256 file hashing utility used by DownloadManager for
// strong fingerprinting and integrity checks.

import CryptoKit
import Foundation

// MARK: - FileHasher

/// SHA-256 file hashing utility.
enum FileHasher {
    /// Computes the SHA-256 hash of a file in 1 MB chunks.
    /// Returns the hex-encoded digest string.
    static func sha256(fileURL: URL) throws -> String {
        let handle = try FileHandle(forReadingFrom: fileURL)
        defer { try? handle.close() }

        var hasher = SHA256()
        let chunkSize = 1024 * 1024

        while autoreleasepool(invoking: {
            let chunk = handle.readData(ofLength: chunkSize)
            guard !chunk.isEmpty else { return false }
            hasher.update(data: chunk)
            return true
        }) { }

        let digest = hasher.finalize()
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    /// Verifies that a file matches the expected SHA-256 hash.
    /// Returns true if the hashes match (case-insensitive comparison).
    static func verify(fileURL: URL, expected: String) throws -> Bool {
        let actual = try sha256(fileURL: fileURL)
        return actual.lowercased() == expected.lowercased()
    }
}
