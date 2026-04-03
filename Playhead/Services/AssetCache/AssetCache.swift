// AssetCache.swift
// Disk and memory caching for podcast artwork and audio segments.
// Also serves as the entry point for the model asset system:
// ModelInventory tracks availability, AssetProvider handles delivery.

import Foundation
import OSLog

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
