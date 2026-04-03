// AssetCache.swift
// Disk and memory caching for podcast artwork and audio segments.
// Also serves as the entry point for the model asset system:
// ModelInventory tracks availability, AssetProvider handles delivery.

import Foundation
import OSLog
import CryptoKit

// MARK: - AssetCache

/// Coordinates caching of artwork, audio, and ML model assets.
///
/// For model management, use the ``modelInventory`` and ``assetProvider``
/// properties directly — AssetCache owns their lifecycle but does not
/// wrap their APIs.
actor AssetCache {
    private let logger = Logger(subsystem: "com.playhead", category: "AssetCache")

    /// Tracks which ML models are available on disk.
    let modelInventory: ModelInventory

    /// Handles download, verification, and promotion of model files.
    let assetProvider: AssetProvider

    init(manifest: ModelManifest, modelsRootOverride: URL? = nil) {
        let inventory = ModelInventory(manifest: manifest, rootOverride: modelsRootOverride)
        self.modelInventory = inventory
        self.assetProvider = AssetProvider(inventory: inventory)
    }

    /// One-shot setup: create directories, scan existing models.
    func bootstrap() async throws {
        try await modelInventory.ensureDirectories()
        try await modelInventory.scan()

        let missing = await modelInventory.missingModels()
        logger.info("AssetCache bootstrapped. \(missing.count) models need downloading.")
    }
}

// MARK: - FileHasher

/// Shared SHA-256 file hashing utility used by both DownloadManager and AssetProvider.
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
