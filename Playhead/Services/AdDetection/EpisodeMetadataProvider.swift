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
    /// Domains that recur in this show's notes with no structural ownership
    /// signal behind them (playhead-kmw4). Cue extraction emits NO cue for
    /// these, so they contribute nothing in either direction — see
    /// `OwnershipGraph.recurringShowNotesDomains`.
    let ownershipUndeterminedDomains: Set<String>

    init(
        feedMetadata: FeedDescriptionMetadata,
        showOwnedDomains: Set<String> = [],
        networkOwnedDomains: Set<String> = [],
        ownershipUndeterminedDomains: Set<String> = []
    ) {
        self.feedMetadata = feedMetadata
        self.showOwnedDomains = Self.normalizedDomains(showOwnedDomains)
        self.networkOwnedDomains = Self.normalizedDomains(networkOwnedDomains)
        self.ownershipUndeterminedDomains = Self.normalizedDomains(ownershipUndeterminedDomains)
    }

    static func normalizedDomain(from url: URL?) -> String? {
        guard let url else { return nil }
        return MetadataCueExtractor.normalizeDomain(from: url.absoluteString)
    }

    /// The two domain populations a show's feed + notes can produce.
    ///
    /// Returned together because they come from ONE `OwnershipGraph` build
    /// over the same episode corpus: computing them separately would walk
    /// every episode's notes twice and let the two answers drift.
    /// playhead-dyvy: the per-show ownership graph, computed once per feed
    /// change rather than once per lookup.
    ///
    /// `domainOwnership` walks EVERY episode of a show — HTML-strips each
    /// description and summary and runs the URL regex over both — and returns
    /// a per-SHOW constant that does not depend on which episode asked. The
    /// 2026-08-18 device pull had 724 and 872 episodes in its two shows, ~3.7 MB
    /// of text, and `metadataLookup` did that walk on the MainActor for every
    /// `metadataSnapshot(for:)`, twice per analysis run. The walk is also
    /// SwiftData faulting the whole relationship, which is why the bead's
    /// device measurement is still owed: this cache removes the repeat, not the
    /// first cost.
    ///
    /// KEYED by the show and its episode count. A refreshed feed lands new
    /// episodes, so the count moves and the graph is rebuilt. An edited
    /// description on an EXISTING episode is picked up on the next new one —
    /// stated here because a key that names one thing must not be read as
    /// naming another: this is "rebuilt when the feed grows", not "rebuilt
    /// when any text changes".
    @MainActor
    final class ShowDomainOwnershipCache {
        private struct Entry {
            let episodeCount: Int
            let ownership: ShowDomainOwnership
        }
        private var entries: [String: Entry] = [:]
        private(set) var computeCount = 0

        init() {}

        func ownership(
            podcastId: String,
            episodeCount: Int,
            compute: () -> ShowDomainOwnership
        ) -> ShowDomainOwnership {
            if let hit = entries[podcastId], hit.episodeCount == episodeCount {
                return hit.ownership
            }
            computeCount += 1
            let fresh = compute()
            entries[podcastId] = Entry(episodeCount: episodeCount, ownership: fresh)
            return fresh
        }
    }

    struct ShowDomainOwnership: Sendable, Equatable {
        /// Domains a STRUCTURAL signal says the show owns: the channel
        /// `<link>` and the `<itunes:owner>` email's eTLD+1, minus the feed's
        /// own host (playhead-e8mg). Until that bead the only route with a
        /// production caller was the feed URL, which is the one route that is
        /// now gone — it resolved to `simplecast.com` and `flightcast.com` on
        /// the two subscribed shows, i.e. the hosting platform in both cases.
        let showOwned: Set<String>
        /// Domains that recur in the notes with no ownership signal at all.
        let ownershipUndetermined: Set<String>
    }

    /// Build the one `OwnershipGraph` the pipeline reads.
    ///
    /// `feedURL` is an EXCLUSION, not a source. Its eTLD+1 becomes the graph's
    /// `feedHostDomain`, the single domain `<link>` and `<itunes:owner>` are
    /// not allowed to claim. Read `OwnershipGraph.feedHostDomain` before
    /// changing that — the measurement is there.
    static func domainOwnership(
        feedURL: URL?,
        siteURL: URL?,
        ownerEmail: String?,
        recentMetadata: [FeedDescriptionMetadata],
        podcastId: String
    ) -> ShowDomainOwnership {
        var graph = OwnershipGraph(
            podcastId: podcastId,
            feedHostDomain: feedURL.flatMap { DomainNormalizer.etld1(from: $0.absoluteString) }
        )
        graph.ingestRSSFeed(
            linkURL: siteURL?.absoluteString,
            itunesOwnerEmail: ownerEmail
        )

        for metadata in recentMetadata {
            var episodeDomains = Set<String>()
            for text in [metadata.feedDescription, metadata.feedSummary].compactMap(\.self) {
                episodeDomains.formUnion(MetadataCueExtractor.extractDomains(from: text))
            }
            for domain in episodeDomains {
                graph.recordShowNotesDomain(domain)
            }
        }

        return ShowDomainOwnership(
            showOwned: Set(graph.showOwnedDomains),
            ownershipUndetermined: Set(graph.recurringShowNotesDomains)
        )
    }

    static func showOwnedDomains(
        feedURL: URL?,
        siteURL: URL?,
        ownerEmail: String?,
        recentMetadata: [FeedDescriptionMetadata],
        podcastId: String
    ) -> Set<String> {
        domainOwnership(
            feedURL: feedURL,
            siteURL: siteURL,
            ownerEmail: ownerEmail,
            recentMetadata: recentMetadata,
            podcastId: podcastId
        ).showOwned
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
