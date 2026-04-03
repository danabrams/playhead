// ModelInventory.swift
// Tracks which ML models are available locally, their versions, and
// readiness state. Consults the ModelManifest and scans the on-disk
// model directories to build a unified view of model availability.

import Foundation
import OSLog
import CryptoKit

// MARK: - ModelInventory

/// Actor that maintains a consistent view of locally-available models.
///
/// On launch, call ``scan()`` to reconcile the manifest with the file
/// system. Consumers query ``status(for:)`` to decide whether a model
/// is ready, needs downloading, or has an update staged.
actor ModelInventory {
    private let logger = Logger(subsystem: "com.playhead", category: "ModelInventory")

    /// The active manifest (initially bundled, can be refreshed).
    private(set) var manifest: ModelManifest

    /// Per-model-id status cache, rebuilt on scan.
    private var statusCache: [String: ModelStatus] = [:]

    /// Root directory: Application Support/Playhead/Models/
    nonisolated let modelsRoot: URL

    /// Subdirectory for models actively in use.
    nonisolated let activeDirectory: URL

    /// Subdirectory for staged (downloaded, verified, not yet promoted) models.
    nonisolated let stagingDirectory: URL

    /// Subdirectory for in-progress downloads.
    nonisolated let downloadsDirectory: URL

    /// Subdirectory for rolled-back model versions.
    nonisolated let rollbackDirectory: URL

    // MARK: Lifecycle

    init(manifest: ModelManifest, rootOverride: URL? = nil) {
        self.manifest = manifest

        let root = rootOverride ?? Self.defaultModelsRoot()
        self.modelsRoot = root
        self.activeDirectory = root.appendingPathComponent("Active", isDirectory: true)
        self.stagingDirectory = root.appendingPathComponent("Staging", isDirectory: true)
        self.downloadsDirectory = root.appendingPathComponent("Downloads", isDirectory: true)
        self.rollbackDirectory = root.appendingPathComponent("Rollback", isDirectory: true)
    }

    /// Default root: Application Support/Playhead/Models/
    /// Not in Documents — invisible to the Files app.
    static func defaultModelsRoot() -> URL {
        FileManager.default
            .urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("Playhead", isDirectory: true)
            .appendingPathComponent("Models", isDirectory: true)
    }

    // MARK: - Directory Setup

    /// Creates all required subdirectories. Safe to call multiple times.
    func ensureDirectories() throws {
        let fm = FileManager.default
        for dir in [activeDirectory, stagingDirectory, downloadsDirectory, rollbackDirectory] {
            if !fm.fileExists(atPath: dir.path) {
                try fm.createDirectory(at: dir, withIntermediateDirectories: true)
            }
        }
        // Mark the root directory as excluded from iCloud backup and
        // not visible in Files.app.
        var resourceValues = URLResourceValues()
        resourceValues.isExcludedFromBackup = true
        var mutable = modelsRoot
        try mutable.setResourceValues(resourceValues)

        logger.info("Model directories ready at \(self.modelsRoot.path)")
    }

    // MARK: - Manifest Management

    /// Replace the active manifest (e.g., after fetching an update).
    func updateManifest(_ newManifest: ModelManifest) {
        guard newManifest.version > manifest.version else {
            logger.info("Ignoring manifest v\(newManifest.version); current is v\(self.manifest.version)")
            return
        }
        manifest = newManifest
        logger.info("Manifest updated to v\(newManifest.version) with \(newManifest.models.count) models")
    }

    /// Load the bundled manifest from the app bundle.
    static func loadBundledManifest() throws -> ModelManifest {
        let bundle = Bundle.main
        let candidateURLs = [
            bundle.url(forResource: "ModelManifest", withExtension: "json"),
            bundle.url(forResource: "ModelManifest", withExtension: "json", subdirectory: "Resources"),
        ].compactMap { $0 }

        if let url = candidateURLs.first {
            let data = try Data(contentsOf: url)
            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            return try decoder.decode(ModelManifest.self, from: data)
        }

        Logger(subsystem: "com.playhead", category: "ModelInventory").warning(
            "Bundled ModelManifest.json missing; falling back to baked-in default manifest"
        )
        return defaultBundledManifest()
    }

    private static func defaultBundledManifest() -> ModelManifest {
        ModelManifest(
            version: 1,
            generatedAt: Date(timeIntervalSince1970: 1_741_382_400),
            models: [
                ModelEntry(
                    id: "whisper-tiny-en",
                    role: .asrFast,
                    displayName: "Fast ASR Model",
                    modelVersion: "1.0.0",
                    downloadURL: URL(string: "https://example.com/playhead/models/whisper-tiny-en.zip")!,
                    sha256: String(repeating: "0", count: 64),
                    compressedSizeBytes: 125_000_000,
                    uncompressedSizeBytes: 425_000_000,
                    priority: 300,
                    minimumOS: "26.0",
                    requiredCapabilities: ["arm64", "neural-engine"]
                ),
                ModelEntry(
                    id: "whisper-small-en",
                    role: .asrFinal,
                    displayName: "Final ASR Model",
                    modelVersion: "1.0.0",
                    downloadURL: URL(string: "https://example.com/playhead/models/whisper-small-en.zip")!,
                    sha256: String(repeating: "0", count: 64),
                    compressedSizeBytes: 420_000_000,
                    uncompressedSizeBytes: 1_450_000_000,
                    priority: 200,
                    minimumOS: "26.0",
                    requiredCapabilities: ["arm64", "neural-engine"]
                ),
                ModelEntry(
                    id: "ad-classifier-lite",
                    role: .classifier,
                    displayName: "Ad Classifier",
                    modelVersion: "1.0.0",
                    downloadURL: URL(string: "https://example.com/playhead/models/ad-classifier-lite.zip")!,
                    sha256: String(repeating: "0", count: 64),
                    compressedSizeBytes: 18_000_000,
                    uncompressedSizeBytes: 54_000_000,
                    priority: 100,
                    minimumOS: "26.0",
                    requiredCapabilities: ["arm64", "neural-engine"]
                ),
            ]
        )
    }

    // MARK: - Scanning

    /// Scans the file system to reconcile manifest entries with what is
    /// actually on disk. Rebuilds the entire status cache.
    func scan() throws {
        try ensureDirectories()

        let fm = FileManager.default
        var newCache: [String: ModelStatus] = [:]

        for entry in manifest.models {
            let activePath = activeDirectory.appendingPathComponent(entry.id)
            let stagedPath = stagingDirectory.appendingPathComponent(entry.id)

            if fm.fileExists(atPath: activePath.path) {
                // Read version from the sidecar metadata file.
                let version = readVersionSidecar(for: entry.id, in: activeDirectory)

                if fm.fileExists(atPath: stagedPath.path) {
                    let stagedVersion = readVersionSidecar(for: entry.id, in: stagingDirectory)
                    newCache[entry.id] = .updateAvailable(
                        currentVersion: version ?? "unknown",
                        newVersion: stagedVersion ?? entry.modelVersion
                    )
                } else {
                    newCache[entry.id] = .ready(version: version ?? entry.modelVersion)
                }
            } else if fm.fileExists(atPath: stagedPath.path) {
                newCache[entry.id] = .staged
            } else {
                // Check for partial download.
                let partialPath = downloadsDirectory.appendingPathComponent("\(entry.id).partial")
                if fm.fileExists(atPath: partialPath.path) {
                    newCache[entry.id] = .downloading(progress: 0)
                } else {
                    newCache[entry.id] = .missing
                }
            }
        }

        statusCache = newCache
        logger.info("Inventory scan complete: \(newCache.count) models tracked")
    }

    // MARK: - Queries

    /// Returns the current status of a model by its manifest ID.
    func status(for modelId: String) -> ModelStatus {
        statusCache[modelId] ?? .missing
    }

    /// Returns status for all models with a given role.
    func statuses(for role: ModelRole) -> [(ModelEntry, ModelStatus)] {
        manifest.models(for: role).map { entry in
            (entry, status(for: entry.id))
        }
    }

    /// Returns true if at least one model for the given role is ready.
    func isReady(role: ModelRole) -> Bool {
        manifest.models(for: role).contains { entry in
            if case .ready = status(for: entry.id) { return true }
            return false
        }
    }

    /// Returns the active directory path for a ready model.
    func activeModelURL(for modelId: String) -> URL? {
        guard case .ready = status(for: modelId) else { return nil }
        let url = activeDirectory.appendingPathComponent(modelId)
        return FileManager.default.fileExists(atPath: url.path) ? url : nil
    }

    /// Returns all models that need downloading, ordered by priority.
    func missingModels() -> [ModelEntry] {
        manifest.models
            .filter {
                if case .missing = status(for: $0.id) { return true }
                return false
            }
            .sorted { $0.priority > $1.priority }
    }

    /// Returns the fast-path ASR model entry if it needs downloading.
    func fastPathASRIfMissing() -> ModelEntry? {
        manifest.preferred(for: .asrFast).flatMap { entry in
            if case .missing = status(for: entry.id) { return entry }
            return nil
        }
    }

    // MARK: - Status Updates

    /// Called by AssetProvider to update download progress.
    func updateDownloadProgress(modelId: String, progress: Double) {
        statusCache[modelId] = .downloading(progress: progress)
    }

    /// Called by AssetProvider after a model is staged.
    func markStaged(modelId: String) {
        statusCache[modelId] = .staged
    }

    /// Called by AssetProvider after a model is promoted to active.
    func markReady(modelId: String, version: String) {
        statusCache[modelId] = .ready(version: version)
    }

    /// Called by AssetProvider if a model is rolled back.
    func markMissing(modelId: String) {
        statusCache[modelId] = .missing
    }

    // MARK: - Sidecar Metadata

    /// Writes a version sidecar file next to a model directory/file.
    func writeVersionSidecar(modelId: String, version: String, in directory: URL) throws {
        let sidecarURL = directory.appendingPathComponent("\(modelId).version")
        try version.write(to: sidecarURL, atomically: true, encoding: .utf8)
    }

    /// Reads the version from a sidecar file, if present.
    private func readVersionSidecar(for modelId: String, in directory: URL) -> String? {
        let sidecarURL = directory.appendingPathComponent("\(modelId).version")
        return try? String(contentsOf: sidecarURL, encoding: .utf8).trimmingCharacters(in: .whitespacesAndNewlines)
    }
}

// MARK: - Errors

enum ModelInventoryError: Error, CustomStringConvertible {
    case bundledManifestMissing
    case modelNotInManifest(String)
    case incompatibleDevice(String)

    var description: String {
        switch self {
        case .bundledManifestMissing:
            "Bundled ModelManifest.json not found in app bundle"
        case .modelNotInManifest(let id):
            "Model '\(id)' not found in manifest"
        case .incompatibleDevice(let reason):
            "Device incompatible: \(reason)"
        }
    }
}
