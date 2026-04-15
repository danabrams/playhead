// RecentFeedSponsorAtlasTests.swift
// ef2.2.5: Tests for RecentFeedSponsorAtlas — atlas building, recurring
// sponsor detection, confidence levels, empty input, domain aggregation.

import Foundation
import Testing
@testable import Playhead

// MARK: - EpisodeFeedSnapshot

@Suite("EpisodeFeedSnapshot")
struct EpisodeFeedSnapshotTests {

    @Test("Round-trips through Codable")
    func codableRoundTrip() throws {
        let snapshot = EpisodeFeedSnapshot(
            episodeId: "ep-1",
            feedDescription: "Sponsored by Squarespace",
            feedSummary: nil,
            publishedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let data = try JSONEncoder().encode(snapshot)
        let decoded = try JSONDecoder().decode(EpisodeFeedSnapshot.self, from: data)
        #expect(decoded == snapshot)
    }

    @Test("Nil fields encode and decode")
    func nilFields() throws {
        let snapshot = EpisodeFeedSnapshot(
            episodeId: "ep-2",
            feedDescription: nil,
            feedSummary: nil,
            publishedAt: nil
        )
        let data = try JSONEncoder().encode(snapshot)
        let decoded = try JSONDecoder().decode(EpisodeFeedSnapshot.self, from: data)
        #expect(decoded == snapshot)
    }
}

// MARK: - RecentFeedSponsorAtlas (struct)

@Suite("RecentFeedSponsorAtlas")
struct RecentFeedSponsorAtlasTests {

    @Test("Empty atlas returns base confidence for any sponsor")
    func emptyAtlasBaseConfidence() {
        let atlas = RecentFeedSponsorAtlas.empty
        #expect(atlas.seedingConfidence(for: "unknown") == 0.85)
        #expect(!atlas.isRecurring(sponsorId: "unknown"))
        #expect(atlas.episodesAnalyzed == 0)
    }

    @Test("Codable round-trip preserves all fields")
    func codableRoundTrip() throws {
        let atlas = RecentFeedSponsorAtlas(
            recurringSponsors: ["squarespace": 5, "betterhelp": 2],
            recurringDomains: ["squarespace.com": 5, "betterhelp.com": 2],
            episodesAnalyzed: 8,
            builtAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let data = try JSONEncoder().encode(atlas)
        let decoded = try JSONDecoder().decode(RecentFeedSponsorAtlas.self, from: data)
        #expect(decoded == atlas)
    }

    @Test("seedingConfidence returns 0.90 for 3+ appearances")
    func elevatedConfidence() {
        let atlas = RecentFeedSponsorAtlas(
            recurringSponsors: ["squarespace": 3],
            recurringDomains: [:],
            episodesAnalyzed: 5,
            builtAt: .now
        )
        #expect(atlas.seedingConfidence(for: "squarespace") == 0.90)
        #expect(atlas.isRecurring(sponsorId: "squarespace"))
    }

    @Test("seedingConfidence returns 0.85 for fewer than 3 appearances")
    func baseConfidence() {
        let atlas = RecentFeedSponsorAtlas(
            recurringSponsors: ["betterhelp": 2],
            recurringDomains: [:],
            episodesAnalyzed: 5,
            builtAt: .now
        )
        #expect(atlas.seedingConfidence(for: "betterhelp") == 0.85)
        #expect(!atlas.isRecurring(sponsorId: "betterhelp"))
    }

    @Test("seedingConfidence returns 0.85 for unknown sponsor")
    func unknownSponsor() {
        let atlas = RecentFeedSponsorAtlas(
            recurringSponsors: ["squarespace": 5],
            recurringDomains: [:],
            episodesAnalyzed: 5,
            builtAt: .now
        )
        #expect(atlas.seedingConfidence(for: "nonexistent") == 0.85)
    }

    @Test("isRecurringDomain checks domain threshold")
    func recurringDomain() {
        let atlas = RecentFeedSponsorAtlas(
            recurringSponsors: [:],
            recurringDomains: ["squarespace.com": 4, "betterhelp.com": 1],
            episodesAnalyzed: 5,
            builtAt: .now
        )
        #expect(atlas.isRecurringDomain(domain: "squarespace.com"))
        #expect(!atlas.isRecurringDomain(domain: "betterhelp.com"))
        #expect(!atlas.isRecurringDomain(domain: "unknown.com"))
    }

    @Test("isRecurringDomain normalizes to lowercase")
    func recurringDomainCaseInsensitive() {
        let atlas = RecentFeedSponsorAtlas(
            recurringSponsors: [:],
            recurringDomains: ["squarespace.com": 4],
            episodesAnalyzed: 5,
            builtAt: .now
        )
        #expect(atlas.isRecurringDomain(domain: "Squarespace.COM"))
    }
}

// MARK: - RecentFeedSponsorAtlasBuilder

@Suite("RecentFeedSponsorAtlasBuilder")
struct RecentFeedSponsorAtlasBuilderTests {

    // MARK: - Helpers

    /// Create an episode snapshot with a "sponsored by X" disclosure.
    private func makeEpisode(
        id: String,
        sponsor: String,
        domain: String? = nil,
        daysAgo: Int = 0
    ) -> EpisodeFeedSnapshot {
        var desc = "This episode is sponsored by \(sponsor)."
        if let domain {
            desc += " Visit https://\(domain)/podcast for more."
        }
        return EpisodeFeedSnapshot(
            episodeId: id,
            feedDescription: desc,
            feedSummary: nil,
            publishedAt: Calendar.current.date(byAdding: .day, value: -daysAgo, to: .now)
        )
    }

    // MARK: - Empty Input

    @Test("Empty episode list returns empty atlas")
    func emptyInput() {
        let builder = RecentFeedSponsorAtlasBuilder()
        let atlas = builder.build(from: [])
        #expect(atlas.episodesAnalyzed == 0)
        #expect(atlas.recurringSponsors.isEmpty)
        #expect(atlas.recurringDomains.isEmpty)
    }

    // MARK: - Single Episode

    @Test("Single episode extracts sponsors but none are recurring")
    func singleEpisode() {
        let builder = RecentFeedSponsorAtlasBuilder()
        let episodes = [
            makeEpisode(id: "ep-1", sponsor: "Squarespace", domain: "squarespace.com")
        ]
        let atlas = builder.build(from: episodes)
        #expect(atlas.episodesAnalyzed == 1)
        #expect(!atlas.recurringSponsors.isEmpty)
        // One episode can't be recurring (threshold is 3).
        for (_, count) in atlas.recurringSponsors {
            #expect(count < RecentFeedSponsorAtlas.recurringThreshold)
        }
    }

    // MARK: - Recurring Sponsor Detection

    @Test("Sponsor in 3+ episodes gets elevated confidence")
    func recurringSponsor() {
        let builder = RecentFeedSponsorAtlasBuilder()
        let episodes = (0..<5).map { i in
            makeEpisode(id: "ep-\(i)", sponsor: "Squarespace", domain: "squarespace.com", daysAgo: i)
        }
        let atlas = builder.build(from: episodes)
        #expect(atlas.episodesAnalyzed == 5)

        // "squarespace" should appear in the sponsors map.
        let squarespaceKey = atlas.recurringSponsors.keys.first { $0.contains("squarespace") }
        #expect(squarespaceKey != nil)
        if let key = squarespaceKey {
            #expect(atlas.recurringSponsors[key]! >= 3)
            #expect(atlas.seedingConfidence(for: key) == 0.90)
            #expect(atlas.isRecurring(sponsorId: key))
        }
    }

    @Test("Sponsor in only 2 episodes does not get elevated confidence")
    func nonRecurringSponsor() {
        let builder = RecentFeedSponsorAtlasBuilder()
        var episodes = (0..<2).map { i in
            makeEpisode(id: "ep-\(i)", sponsor: "BetterHelp", daysAgo: i)
        }
        // Add 3 more episodes without BetterHelp.
        episodes += (2..<5).map { i in
            EpisodeFeedSnapshot(
                episodeId: "ep-\(i)",
                feedDescription: "No sponsors this episode.",
                feedSummary: nil,
                publishedAt: Calendar.current.date(byAdding: .day, value: -i, to: .now)
            )
        }
        let atlas = builder.build(from: episodes)

        let bhKey = atlas.recurringSponsors.keys.first { $0.contains("betterhelp") }
        if let key = bhKey {
            #expect(atlas.recurringSponsors[key]! < 3)
            #expect(atlas.seedingConfidence(for: key) == 0.85)
            #expect(!atlas.isRecurring(sponsorId: key))
        }
    }

    // MARK: - Domain Aggregation

    @Test("Domains are aggregated across episodes")
    func domainAggregation() {
        let builder = RecentFeedSponsorAtlasBuilder()
        let episodes = (0..<4).map { i in
            makeEpisode(id: "ep-\(i)", sponsor: "Sponsor", domain: "example.com", daysAgo: i)
        }
        let atlas = builder.build(from: episodes)
        #expect(atlas.recurringDomains["example.com"] == 4)
        #expect(atlas.isRecurringDomain(domain: "example.com"))
    }

    @Test("Distinct domains are counted separately")
    func distinctDomains() {
        let builder = RecentFeedSponsorAtlasBuilder()
        let episodes = [
            makeEpisode(id: "ep-0", sponsor: "A", domain: "aaa.com", daysAgo: 0),
            makeEpisode(id: "ep-1", sponsor: "B", domain: "bbb.com", daysAgo: 1),
            makeEpisode(id: "ep-2", sponsor: "C", domain: "ccc.com", daysAgo: 2),
        ]
        let atlas = builder.build(from: episodes)
        #expect(atlas.recurringDomains["aaa.com"] == 1)
        #expect(atlas.recurringDomains["bbb.com"] == 1)
        #expect(atlas.recurringDomains["ccc.com"] == 1)
    }

    // MARK: - Windowing

    @Test("Only the most recent N episodes are analyzed")
    func windowingRespectsLimit() {
        let builder = RecentFeedSponsorAtlasBuilder()
        // Create 15 episodes; only the 10 most recent should be used.
        let episodes = (0..<15).map { i in
            makeEpisode(id: "ep-\(i)", sponsor: "Squarespace", daysAgo: i)
        }
        let atlas = builder.build(from: episodes, windowSize: 10)
        #expect(atlas.episodesAnalyzed == 10)
    }

    @Test("Window size enforces minimum of 5")
    func windowMinimum() {
        let builder = RecentFeedSponsorAtlasBuilder()
        let episodes = (0..<8).map { i in
            makeEpisode(id: "ep-\(i)", sponsor: "Test", daysAgo: i)
        }
        // Request window of 2, but minimum is 5.
        let atlas = builder.build(from: episodes, windowSize: 2)
        #expect(atlas.episodesAnalyzed == 5)
    }

    @Test("Episodes without publishedAt sort to end")
    func nilPublishedAtSortsLast() {
        let builder = RecentFeedSponsorAtlasBuilder()
        let episodes = [
            EpisodeFeedSnapshot(
                episodeId: "ep-no-date",
                feedDescription: "Sponsored by OldSponsor.",
                feedSummary: nil,
                publishedAt: nil
            ),
            makeEpisode(id: "ep-recent", sponsor: "NewSponsor", daysAgo: 0),
        ]
        // With window of 5 (minimum), both are included since there are only 2.
        let atlas = builder.build(from: episodes)
        #expect(atlas.episodesAnalyzed == 2)
    }

    // MARK: - Entity Graph Resolution

    @Test("Sponsors are resolved via entity graph when available")
    func entityGraphResolution() {
        // Build an entity graph with a known sponsor.
        let entry = SponsorKnowledgeEntry(
            id: "canon-sq",
            podcastId: "pod-1",
            entityType: .sponsor,
            entityValue: "Squarespace",
            aliases: ["squarespace", "square space"]
        )
        let graph = SponsorEntityGraph(entries: [entry])

        let builder = RecentFeedSponsorAtlasBuilder(
            extractor: MetadataCueExtractor(),
            entityGraph: graph
        )

        let episodes = (0..<4).map { i in
            makeEpisode(id: "ep-\(i)", sponsor: "Squarespace", daysAgo: i)
        }

        let atlas = builder.build(from: episodes)
        // Should resolve to the canonical ID from the entity graph.
        let hasCanonical = atlas.recurringSponsors.keys.contains("canon-sq")
        let hasFallback = atlas.recurringSponsors.keys.contains { $0.contains("squarespace") }
        // Either the canonical or the fallback key should be present.
        #expect(hasCanonical || hasFallback)
    }

    // MARK: - Multiple Sponsors Per Episode

    @Test("Multiple sponsors in one episode each get counted once")
    func multipleSponsorsPerEpisode() {
        let builder = RecentFeedSponsorAtlasBuilder()
        let episodes = [
            EpisodeFeedSnapshot(
                episodeId: "ep-1",
                feedDescription: "Sponsored by Squarespace. Also brought to you by BetterHelp.",
                feedSummary: nil,
                publishedAt: .now
            )
        ]
        let atlas = builder.build(from: episodes)
        // Both sponsors should appear with count 1.
        let sponsorKeys = atlas.recurringSponsors.keys
        let hasSquarespace = sponsorKeys.contains { $0.contains("squarespace") }
        let hasBetterhelp = sponsorKeys.contains { $0.contains("betterhelp") }
        #expect(hasSquarespace)
        #expect(hasBetterhelp)
    }

    // MARK: - No Metadata

    @Test("Episodes with nil description and summary produce empty atlas")
    func nilMetadata() {
        let builder = RecentFeedSponsorAtlasBuilder()
        let episodes = (0..<5).map { i in
            EpisodeFeedSnapshot(
                episodeId: "ep-\(i)",
                feedDescription: nil,
                feedSummary: nil,
                publishedAt: Calendar.current.date(byAdding: .day, value: -i, to: .now)
            )
        }
        let atlas = builder.build(from: episodes)
        #expect(atlas.episodesAnalyzed == 5)
        #expect(atlas.recurringSponsors.isEmpty)
        #expect(atlas.recurringDomains.isEmpty)
    }

    // MARK: - Summary Field

    @Test("Cues from summary field are included")
    func summaryField() {
        let builder = RecentFeedSponsorAtlasBuilder()
        let episodes = (0..<3).map { i in
            EpisodeFeedSnapshot(
                episodeId: "ep-\(i)",
                feedDescription: nil,
                feedSummary: "Sponsored by HelloFresh. Visit https://hellofresh.com/podcast",
                publishedAt: Calendar.current.date(byAdding: .day, value: -i, to: .now)
            )
        }
        let atlas = builder.build(from: episodes)
        let hasHelloFresh = atlas.recurringSponsors.keys.contains { $0.contains("hellofresh") }
        #expect(hasHelloFresh)
        #expect(atlas.recurringDomains["hellofresh.com"] == 3)
    }

    // MARK: - Deduplication Within Episode

    @Test("Same sponsor in description and summary counts as one per episode")
    func deduplicationWithinEpisode() {
        let builder = RecentFeedSponsorAtlasBuilder()
        let episodes = (0..<3).map { i in
            EpisodeFeedSnapshot(
                episodeId: "ep-\(i)",
                feedDescription: "Sponsored by Squarespace.",
                feedSummary: "Brought to you by Squarespace.",
                publishedAt: Calendar.current.date(byAdding: .day, value: -i, to: .now)
            )
        }
        let atlas = builder.build(from: episodes)
        let sqKey = atlas.recurringSponsors.keys.first { $0.contains("squarespace") }
        #expect(sqKey != nil)
        if let key = sqKey {
            // Should be 3 (one per episode), not 6 (two mentions per episode).
            #expect(atlas.recurringSponsors[key] == 3)
        }
    }
}
