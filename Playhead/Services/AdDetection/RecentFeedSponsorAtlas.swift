// RecentFeedSponsorAtlas.swift
// ef2.2.5: Per-show atlas of recurring sponsors built from recent episode
// RSS descriptions/summaries.
//
// Parses the last 5-10 episodes' metadata to identify sponsors that appear
// across multiple episodes, enabling elevated seeding confidence for
// recurring sponsors (0.90 vs 0.85 base).
//
// Shadow mode only — the atlas is built and logged but does not influence
// any live ad detection scoring.

import Foundation
import OSLog

// MARK: - EpisodeFeedSnapshot

/// Lightweight value type capturing the metadata fields needed to build
/// a RecentFeedSponsorAtlas. Decouples atlas building from SwiftData's
/// Episode model so the atlas can be built off the main actor.
struct EpisodeFeedSnapshot: Sendable, Codable, Equatable {
    /// Stable identifier for the episode (feedItemGUID or canonical key).
    let episodeId: String
    /// RSS `<description>` text (HTML-stripped, normalized).
    let feedDescription: String?
    /// RSS `<itunes:summary>` or `<content:encoded>` text (normalized).
    let feedSummary: String?
    /// Episode publish date, used for recency sorting.
    let publishedAt: Date?
}

// MARK: - RecentFeedSponsorAtlas

/// Atlas of recurring sponsors for a single podcast, built from the most
/// recent episodes' RSS description/summary text.
///
/// All fields are immutable after construction. The atlas is Codable for
/// persistence and Sendable for safe cross-isolation use.
struct RecentFeedSponsorAtlas: Sendable, Codable, Equatable {

    /// Canonical sponsor ID -> number of episodes the sponsor appeared in.
    let sponsorEpisodeCounts: [String: Int]

    /// Normalized domain (eTLD+1) -> number of episodes the domain appeared in.
    let domainEpisodeCounts: [String: Int]

    /// How many episodes were analyzed to build this atlas.
    let episodesAnalyzed: Int

    /// When this atlas was built.
    let builtAt: Date

    // MARK: - Constants

    /// Minimum episode appearances for a sponsor to be considered "recurring".
    static let recurringThreshold: Int = 3

    /// Seeding confidence for sponsors appearing in 3+ episodes.
    static let elevatedSeedingConfidence: Float = 0.90

    /// Base seeding confidence for non-recurring sponsors.
    static let baseSeedingConfidence: Float = 0.85

    /// Default window size: how many recent episodes to analyze.
    static let defaultWindowSize: Int = 10

    /// Minimum window size.
    static let minimumWindowSize: Int = 5

    // MARK: - Queries

    /// Returns the seeding confidence for a given canonical sponsor ID.
    /// - Returns: 0.90 if the sponsor appears in 3+ analyzed episodes, 0.85 otherwise.
    func seedingConfidence(for sponsorId: String) -> Float {
        isRecurring(sponsorId: sponsorId)
            ? Self.elevatedSeedingConfidence
            : Self.baseSeedingConfidence
    }

    /// Whether a sponsor appears in 3+ of the analyzed episodes.
    func isRecurring(sponsorId: String) -> Bool {
        guard let count = sponsorEpisodeCounts[sponsorId] else { return false }
        return count >= Self.recurringThreshold
    }

    /// Whether a domain appears in 3+ of the analyzed episodes.
    func isRecurringDomain(domain: String) -> Bool {
        guard let count = domainEpisodeCounts[domain.lowercased()] else { return false }
        return count >= Self.recurringThreshold
    }

    // MARK: - Empty Atlas

    /// An empty atlas (no episodes analyzed).
    static let empty = RecentFeedSponsorAtlas(
        sponsorEpisodeCounts: [:],
        domainEpisodeCounts: [:],
        episodesAnalyzed: 0,
        builtAt: .distantPast
    )
}

// MARK: - RecentFeedSponsorAtlasBuilder

/// Builds a RecentFeedSponsorAtlas from a window of recent episodes.
///
/// Pipeline:
/// 1. Sort episodes by publishedAt descending, take last N.
/// 2. For each episode, extract cues via MetadataCueExtractor.
/// 3. Resolve sponsor identity via SponsorEntityGraph (when available).
/// 4. Aggregate sponsor/domain appearances across episodes.
/// 5. Return the atlas.
///
/// Thread-safe: all state is either immutable or isolated to method scope.
struct RecentFeedSponsorAtlasBuilder: Sendable {

    private let logger = Logger(subsystem: "com.playhead", category: "RecentFeedSponsorAtlas")

    /// The cue extractor to use (configured with known sponsors/domains).
    private let extractor: MetadataCueExtractor

    /// Optional entity graph for resolving canonical sponsor IDs.
    private let entityGraph: SponsorEntityGraph?

    init(
        extractor: MetadataCueExtractor = MetadataCueExtractor(),
        entityGraph: SponsorEntityGraph? = nil
    ) {
        self.extractor = extractor
        self.entityGraph = entityGraph
    }

    /// Build an atlas from a collection of episode snapshots.
    ///
    /// - Parameters:
    ///   - episodes: All available episodes for the podcast.
    ///   - windowSize: How many recent episodes to analyze (default 10, min 5).
    /// - Returns: A populated RecentFeedSponsorAtlas.
    func build(
        from episodes: [EpisodeFeedSnapshot],
        windowSize: Int = RecentFeedSponsorAtlas.defaultWindowSize
    ) -> RecentFeedSponsorAtlas {
        let effectiveWindow = max(windowSize, RecentFeedSponsorAtlas.minimumWindowSize)

        // Sort by publishedAt descending (most recent first), then take window.
        let sorted = episodes.sorted { a, b in
            (a.publishedAt ?? .distantPast) > (b.publishedAt ?? .distantPast)
        }
        let windowed = Array(sorted.prefix(effectiveWindow))

        guard !windowed.isEmpty else {
            logger.debug("No episodes to analyze, returning empty atlas")
            return .empty
        }

        // Per-episode: extract cues, collect unique sponsors and domains.
        // We track per-episode sets to count episodes (not total mentions).
        var sponsorEpisodeCounts: [String: Int] = [:]
        var domainEpisodeCounts: [String: Int] = [:]

        for episode in windowed {
            let cues = extractor.extractCues(
                description: episode.feedDescription,
                summary: episode.feedSummary
            )

            // Collect unique sponsors seen in this episode.
            var episodeSponsors = Set<String>()
            // Collect unique domains seen in this episode.
            var episodeDomains = Set<String>()

            for cue in cues {
                switch cue.cueType {
                case .disclosure, .sponsorAlias:
                    let sponsorId = resolveCanonicalId(for: cue)
                    episodeSponsors.insert(sponsorId)

                case .externalDomain:
                    let domain = cue.normalizedValue.lowercased()
                    episodeDomains.insert(domain)
                    // Also try to resolve domain to a canonical sponsor.
                    if let canonId = entityGraph?.canonicalSponsorId(forDomain: domain) {
                        episodeSponsors.insert(canonId)
                    }

                case .promoCode:
                    // Promo codes don't directly identify sponsors without
                    // entity graph resolution, but if we have a canonical ID
                    // from the cue, use it.
                    if let canonId = cue.canonicalSponsorId {
                        episodeSponsors.insert(canonId)
                    }

                case .showOwnedDomain, .networkOwnedDomain:
                    // Not sponsor signals — skip.
                    break
                }
            }

            // Increment episode counts.
            for sponsor in episodeSponsors {
                sponsorEpisodeCounts[sponsor, default: 0] += 1
            }
            for domain in episodeDomains {
                domainEpisodeCounts[domain, default: 0] += 1
            }
        }

        let atlas = RecentFeedSponsorAtlas(
            sponsorEpisodeCounts: sponsorEpisodeCounts,
            domainEpisodeCounts: domainEpisodeCounts,
            episodesAnalyzed: windowed.count,
            builtAt: Date()
        )

        logger.debug(
            "Built atlas: \(atlas.episodesAnalyzed) episodes, \(atlas.sponsorEpisodeCounts.count) sponsors, \(atlas.domainEpisodeCounts.count) domains"
        )

        return atlas
    }

    // MARK: - Private

    /// Resolve a cue to a canonical sponsor ID. Falls back to the cue's
    /// normalizedValue if no entity graph is available or no match found.
    private func resolveCanonicalId(for cue: EpisodeMetadataCue) -> String {
        // If the cue already has a canonical sponsor ID, use it.
        if let canonId = cue.canonicalSponsorId {
            return canonId
        }

        // Try entity graph resolution by name.
        if let graph = entityGraph,
           let canonId = graph.canonicalSponsorId(forName: cue.normalizedValue) {
            return canonId
        }

        // Fallback: use normalized value as the identity key.
        return cue.normalizedValue.lowercased()
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
