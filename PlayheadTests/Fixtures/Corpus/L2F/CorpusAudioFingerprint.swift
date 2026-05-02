// CorpusAudioFingerprint.swift
// Streaming SHA-256 helper for verifying that an annotation's
// `audio_fingerprint` field still matches the referenced audio file.
//
// We stream-hash so files larger than RAM (multi-GB rare-but-possible
// archive files) can be verified on a phone-class device without an
// out-of-memory crash.

import CryptoKit
import Foundation

// MARK: - Fingerprint Errors

enum CorpusAudioFingerprintError: Error, CustomStringConvertible {
    case fileNotFound(URL)
    case readFailed(URL, Error)

    var description: String {
        switch self {
        case .fileNotFound(let url):
            return "Audio file not found at \(url.path)"
        case .readFailed(let url, let err):
            return "Failed to read audio file at \(url.path): \(err.localizedDescription)"
        }
    }
}

// MARK: - CorpusAudioFingerprint

/// Computes the canonical fingerprint string used in `CorpusAnnotation.audioFingerprint`.
///
/// The fingerprint is `sha256:` followed by the lowercased hex digest of
/// the file's bytes. Streaming reads in 1 MiB chunks keep peak memory
/// bounded regardless of file size.
enum CorpusAudioFingerprint {

    /// Prefix used to namespace the digest. Reserved so future versions
    /// can introduce e.g. `sha512:` without breaking existing files.
    static let prefix = "sha256:"

    /// Default chunk size for streaming reads (1 MiB).
    static let chunkSize = 1024 * 1024

    /// Compute the fingerprint of a file at `url`.
    ///
    /// Streams the file in `chunkSize` chunks via a `FileHandle`, so
    /// memory usage stays bounded for files of arbitrary size.
    static func fingerprint(of url: URL, chunkSize: Int = Self.chunkSize) throws -> String {
        guard FileManager.default.fileExists(atPath: url.path) else {
            throw CorpusAudioFingerprintError.fileNotFound(url)
        }

        let handle: FileHandle
        do {
            handle = try FileHandle(forReadingFrom: url)
        } catch {
            throw CorpusAudioFingerprintError.readFailed(url, error)
        }
        defer { try? handle.close() }

        var hasher = SHA256()
        do {
            while true {
                let data = try handle.read(upToCount: chunkSize) ?? Data()
                if data.isEmpty { break }
                hasher.update(data: data)
            }
        } catch {
            throw CorpusAudioFingerprintError.readFailed(url, error)
        }

        return prefix + Self.hexDigest(hasher.finalize())
    }

    /// Compute the fingerprint of an in-memory blob. Useful in tests
    /// and tooling that already have the bytes in hand.
    static func fingerprint(of data: Data) -> String {
        let digest = SHA256.hash(data: data)
        return prefix + Self.hexDigest(digest)
    }

    /// Verify that `expected` matches the file at `url`.
    static func matches(_ expected: String, file url: URL) throws -> Bool {
        let actual = try fingerprint(of: url)
        return actual == expected
    }

    // MARK: - Private

    private static func hexDigest<D: Sequence>(_ digest: D) -> String where D.Element == UInt8 {
        digest.map { String(format: "%02x", $0) }.joined()
    }
}
