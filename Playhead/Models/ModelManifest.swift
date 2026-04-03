// ModelManifest.swift
// Versioned manifest describing available ML models, their sizes,
// checksums, and compatibility requirements. Bundled in the app
// binary and updatable via remote fetch.

import Foundation

// MARK: - ModelManifest

/// Top-level manifest describing all models the app can use.
struct ModelManifest: Codable, Sendable, Equatable {
    /// Monotonically increasing version number for cache-busting.
    let version: Int

    /// ISO-8601 date when this manifest was generated.
    let generatedAt: Date

    /// All model entries in this manifest.
    let models: [ModelEntry]

    /// Returns entries matching the given role.
    func models(for role: ModelRole) -> [ModelEntry] {
        models.filter { $0.role == role }
    }

    /// Returns the preferred (highest-priority) entry for a given role,
    /// filtered to those compatible with the current device.
    func preferred(for role: ModelRole) -> ModelEntry? {
        models(for: role)
            .sorted { $0.priority > $1.priority }
            .first
    }
}

// MARK: - ModelEntry

/// A single model asset that can be downloaded and used.
struct ModelEntry: Codable, Sendable, Equatable, Identifiable {
    let id: String
    let role: ModelRole
    let displayName: String

    /// Semantic version string, e.g. "1.2.0".
    let modelVersion: String

    /// Download URL for the model archive.
    let downloadURL: URL

    /// Expected SHA-256 hex digest of the downloaded file.
    let sha256: String

    /// Compressed size in bytes (for download progress).
    let compressedSizeBytes: Int64

    /// Uncompressed size in bytes (for disk-space checks).
    let uncompressedSizeBytes: Int64

    /// Higher-priority models are preferred when multiple are compatible.
    let priority: Int

    /// Minimum iOS version required (e.g. "26.0").
    let minimumOS: String

    /// Optional list of required device capabilities (e.g. "arm64", "neural-engine").
    let requiredCapabilities: [String]
}

// MARK: - ModelRole

/// The functional role a model serves in the analysis pipeline.
enum ModelRole: String, Codable, Sendable, CaseIterable {
    /// Fast-path ASR model — small, downloaded first to unblock real-time analysis.
    case asrFast = "asr_fast"

    /// Final-path ASR model — higher accuracy, deferred download.
    case asrFinal = "asr_final"

    /// Ad/content classifier model.
    case classifier = "classifier"
}

// MARK: - ModelStatus

/// Represents the local state of a model on disk.
enum ModelStatus: Sendable, Equatable {
    /// Not downloaded and not in progress.
    case missing

    /// Currently downloading; progress is 0.0...1.0.
    case downloading(progress: Double)

    /// Downloaded and staged but not yet promoted.
    case staged

    /// Active and ready for inference.
    case ready(version: String)

    /// A previous version is active but a newer one is staged.
    case updateAvailable(currentVersion: String, newVersion: String)
}
