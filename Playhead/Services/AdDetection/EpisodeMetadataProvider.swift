// EpisodeMetadataProvider.swift
// playhead-z3ch: Lookup seam for per-asset feed-description metadata.
//
// Production wiring (see PlayheadApp) supplies a SwiftData-backed
// implementation that maps `analysisAssetId` → `AnalysisAsset.episodeId`
// → `Episode.canonicalEpisodeKey` → `Episode.feedMetadata`. Tests inject
// a deterministic stub that returns fixed metadata for known assets.
//
// The protocol intentionally returns `FeedDescriptionMetadata?` (the type
// already persisted on Episode in shadow mode) so this bead is a pure
// signal elevation rather than a new collection step.

import Foundation
import OSLog
import SwiftData

protocol EpisodeMetadataProvider: Sendable {
    /// Look up the persisted feed-description metadata for the given
    /// analysis asset id. Returns `nil` when the asset has no associated
    /// episode, the episode has no metadata, or the lookup fails.
    func metadata(for analysisAssetId: String) async -> FeedDescriptionMetadata?
}

/// Default no-op provider used when the runtime has no metadata lookup
/// wired (e.g. fast unit tests that don't exercise the metadata path).
struct NullEpisodeMetadataProvider: EpisodeMetadataProvider {
    func metadata(for analysisAssetId: String) async -> FeedDescriptionMetadata? {
        nil
    }
}

/// Production EpisodeMetadataProvider. Resolves
/// `analysisAssetId → AnalysisAsset.episodeId` via the AnalysisStore (SQLite),
/// then `episodeId → Episode.feedMetadata` via SwiftData. Both lookups are
/// best-effort and silent on failure — a missing episode or missing metadata
/// simply produces an empty metadata signal (no ad-detection regression).
struct SwiftDataEpisodeMetadataProvider: EpisodeMetadataProvider {
    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "SwiftDataEpisodeMetadataProvider"
    )

    /// Closure-resolves the analysis asset row so this provider does not own
    /// (or retain) the AnalysisStore actor reference directly. The closure
    /// hits the actor on every call so updates to the asset row stay current.
    let assetLookup: @Sendable (String) async -> AnalysisAsset?
    /// Closure-resolves the SwiftData lookup. Resolved on the MainActor (the
    /// only context where `ModelContainer.mainContext` is safe to read in
    /// SwiftData). The closure isolates the cross-actor hop so the protocol
    /// stays a plain `async` lookup.
    let metadataLookup: @MainActor @Sendable (_ episodeId: String) -> FeedDescriptionMetadata?

    func metadata(for analysisAssetId: String) async -> FeedDescriptionMetadata? {
        guard let asset = await assetLookup(analysisAssetId) else {
            return nil
        }
        let episodeId = asset.episodeId
        return await MainActor.run {
            metadataLookup(episodeId)
        }
    }
}
