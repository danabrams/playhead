// EpisodeMetadataProvider.swift
// playhead-z3ch: Lookup seam for per-asset feed-description metadata.
//
// Production wiring (see PlayheadApp) supplies a SwiftData-backed
// implementation that maps `analysisAssetId` → `AnalysisAsset.episodeId`
// → `Episode.canonicalEpisodeKey` → `Episode.feedMetadata` plus the
// podcast-owned domains needed for safe URL cue classification. Tests inject
// a deterministic stub that returns fixed metadata for known assets.
//
// The provider intentionally returns `FeedDescriptionMetadata` (the type
// already persisted on Episode in shadow mode) so this bead is a pure
// signal elevation rather than a new collection step.

import Foundation
import OSLog
import SwiftData

struct EpisodeMetadataSnapshot: Sendable {
    let feedMetadata: FeedDescriptionMetadata
    let showOwnedDomains: Set<String>
    let networkOwnedDomains: Set<String>

    init(
        feedMetadata: FeedDescriptionMetadata,
        showOwnedDomains: Set<String> = [],
        networkOwnedDomains: Set<String> = []
    ) {
        self.feedMetadata = feedMetadata
        self.showOwnedDomains = Self.normalizedDomains(showOwnedDomains)
        self.networkOwnedDomains = Self.normalizedDomains(networkOwnedDomains)
    }

    static func normalizedDomain(from url: URL?) -> String? {
        guard let url else { return nil }
        return MetadataCueExtractor.normalizeDomain(from: url.absoluteString)
    }

    static func showOwnedDomains(
        feedURL: URL?,
        recentMetadata: [FeedDescriptionMetadata],
        podcastId: String
    ) -> Set<String> {
        var graph = OwnershipGraph(podcastId: podcastId)
        if let feedURL {
            graph.ingestFeedURL(feedURL.absoluteString)
        }

        for metadata in recentMetadata {
            var episodeDomains = Set<String>()
            for text in [metadata.feedDescription, metadata.feedSummary].compactMap(\.self) {
                episodeDomains.formUnion(MetadataCueExtractor.extractDomains(from: text))
            }
            for domain in episodeDomains {
                graph.recordShowNotesDomain(domain)
            }
        }

        return Set(graph.showOwnedDomains)
    }

    private static func normalizedDomains(_ domains: Set<String>) -> Set<String> {
        Set(domains.compactMap { domain in
            MetadataCueExtractor.normalizeDomain(from: domain)
                ?? MetadataCueExtractor.normalizeDomain(from: "https://\(domain)")
        })
    }
}

protocol EpisodeMetadataProvider: Sendable {
    /// Look up the persisted feed-description metadata for the given
    /// analysis asset id. Returns `nil` when the asset has no associated
    /// episode, the episode has no metadata, or the lookup fails.
    func metadata(for analysisAssetId: String) async -> FeedDescriptionMetadata?

    /// Look up feed metadata with ownership context for URL cue
    /// classification. Legacy/test providers that only implement
    /// `metadata(for:)` still participate with empty ownership sets.
    func metadataSnapshot(for analysisAssetId: String) async -> EpisodeMetadataSnapshot?
}

extension EpisodeMetadataProvider {
    func metadataSnapshot(for analysisAssetId: String) async -> EpisodeMetadataSnapshot? {
        guard let metadata = await metadata(for: analysisAssetId) else {
            return nil
        }
        return EpisodeMetadataSnapshot(feedMetadata: metadata)
    }
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
    let metadataLookup: @MainActor @Sendable (_ episodeId: String) -> EpisodeMetadataSnapshot?

    func metadata(for analysisAssetId: String) async -> FeedDescriptionMetadata? {
        await metadataSnapshot(for: analysisAssetId)?.feedMetadata
    }

    func metadataSnapshot(for analysisAssetId: String) async -> EpisodeMetadataSnapshot? {
        guard let asset = await assetLookup(analysisAssetId) else {
            return nil
        }
        let episodeId = asset.episodeId
        return await MainActor.run {
            metadataLookup(episodeId)
        }
    }
}
