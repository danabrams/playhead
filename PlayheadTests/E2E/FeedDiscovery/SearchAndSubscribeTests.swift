// SearchAndSubscribeTests.swift
// playhead-8u1 — E2E proof of the search → subscribe → library flow.
//
// Scenario 1 from the bead:
//   1. Search "The Daily" → results appear
//   2. Tap result → podcast detail (title/author/artwork) is present
//   3. Subscribe → podcast persists in SwiftData
//   4. Episodes loaded with titles, dates, durations
//   5. Podcast and Episode records present in SwiftData
//
// Drives the REAL `PodcastDiscoveryService` actor with a stubbed
// `URLSession` (network boundary mocked). Subscription is the act of
// `persist(_:from:in:)` writing the parsed feed into SwiftData — this
// is exactly what the Browse view's Subscribe button calls into in
// production.

import Foundation
import SwiftData
import Testing
@testable import Playhead

@Suite("playhead-8u1 - search and subscribe E2E", .serialized)
struct SearchAndSubscribeTests {

    @Test("Search returns iTunes results with title, author, artwork, and feed URL")
    func searchReturnsResults() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        let feedURL = URL(string: "https://example.com/the-daily/feed.xml")!
        let artwork = URL(string: "https://example.com/the-daily/art.jpg")!
        let json = ITunesSearchStubFactory.responseJSON(items: [
            .init(
                collectionId: 1234,
                collectionName: "The Daily",
                artistName: "The New York Times",
                feedURL: feedURL,
                artworkURL: artwork,
                genre: "News",
                trackCount: 100
            )
        ])
        stub.registerStub(
            whereURLContains: "itunes.apple.com/search",
            respondWith: json
        )

        let service = PodcastDiscoveryService(session: session)
        let results = try await service.searchPodcasts(query: "The Daily")

        #expect(results.count == 1)
        let first = try #require(results.first)
        #expect(first.title == "The Daily")
        #expect(first.author == "The New York Times")
        #expect(first.feedURL == feedURL)
        #expect(first.artworkURL == artwork)
        #expect(first.genre == "News")
        #expect(first.episodeCount == 100)
    }

    @Test("Subscribing a search result persists Podcast and Episodes in SwiftData")
    @MainActor
    func subscribePersistsPodcastAndEpisodes() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        let feedURL = URL(string: "https://example.com/the-daily/feed.xml")!
        let artwork = URL(string: "https://example.com/the-daily/art.jpg")!

        // Stub iTunes search.
        let searchJSON = ITunesSearchStubFactory.responseJSON(items: [
            .init(
                collectionId: 1234,
                collectionName: "The Daily",
                artistName: "The New York Times",
                feedURL: feedURL,
                artworkURL: artwork,
                genre: "News",
                trackCount: 3
            )
        ])
        stub.registerStub(
            whereURLContains: "itunes.apple.com/search",
            respondWith: searchJSON
        )

        // Stub the feed body the discovery service will fetch when the
        // user taps Subscribe.
        let feedBody = RSSFeedStubFactory.feedXML(
            title: "The Daily",
            author: "The New York Times",
            episodes: [
                .init(
                    title: "Episode One",
                    guid: "ep-001",
                    enclosureURL: URL(string: "https://example.com/the-daily/ep1.mp3"),
                    pubDate: "Mon, 01 Jan 2024 12:00:00 GMT",
                    durationSeconds: 1800
                ),
                .init(
                    title: "Episode Two",
                    guid: "ep-002",
                    enclosureURL: URL(string: "https://example.com/the-daily/ep2.mp3"),
                    pubDate: "Tue, 02 Jan 2024 12:00:00 GMT",
                    durationSeconds: 2400
                ),
            ]
        )
        stub.registerStub(
            whereURLContains: "the-daily/feed.xml",
            respondWith: feedBody
        )

        let service = PodcastDiscoveryService(session: session)
        let container = try makeFeedDiscoveryContainer()
        let context = ModelContext(container)

        // Search.
        let results = try await service.searchPodcasts(query: "The Daily")
        let chosen = try #require(results.first)
        let resultFeedURL = try #require(chosen.feedURL)

        // Subscribe = fetch feed + persist into SwiftData.
        let parsed = try await service.fetchFeed(url: resultFeedURL)
        _ = await service.persist(parsed, from: resultFeedURL, in: context)
        try context.save()

        // Library should now have the Podcast row.
        let podcasts = try context.fetch(FetchDescriptor<Podcast>())
        #expect(podcasts.count == 1)
        let podcast = try #require(podcasts.first)
        #expect(podcast.title == "The Daily")
        #expect(podcast.author == "The New York Times")
        #expect(podcast.feedURL == feedURL)
        // Artwork comes from the feed body, not the iTunes result, in
        // the persist path — assert it's non-nil rather than pinning a
        // specific URL (the stub feed body uses art.jpg).
        #expect(podcast.artworkURL != nil)

        // Two episodes inserted with titles, dates, durations.
        let episodes = try context.fetch(FetchDescriptor<Episode>())
        #expect(episodes.count == 2)
        let ep1 = try #require(episodes.first { $0.feedItemGUID == "ep-001" })
        #expect(ep1.title == "Episode One")
        #expect(ep1.duration == 1800)
        #expect(ep1.publishedAt != nil)
        let ep2 = try #require(episodes.first { $0.feedItemGUID == "ep-002" })
        #expect(ep2.title == "Episode Two")
        #expect(ep2.duration == 2400)
        #expect(ep2.publishedAt != nil)

        // The Podcast↔Episode relationship hangs together so the Library
        // view (which queries the inverse relationship) renders correctly.
        #expect(podcast.episodes.count == 2)
        #expect(episodes.allSatisfy { $0.podcast?.feedURL == feedURL })
    }
}
