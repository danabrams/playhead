// GuidDeduplicationTests.swift
// playhead-8u1 — E2E proof of GUID-based deduplication on feed refresh.
//
// Scenario 3 from the bead:
//   1. Refresh a feed that REORDERS items but keeps the same GUIDs
//   2. No duplicate Episode rows are created
//   3. Episode metadata (title, duration, etc.) is updated when changed
//
// This isolates the dedup contract from "does refresh add new items"
// (covered separately in `FeedRefreshTests`).

import Foundation
import SwiftData
import Testing
@testable import Playhead

@Suite("playhead-8u1 - GUID-based deduplication E2E", .serialized)
struct GuidDeduplicationTests {

    @Test("Reordering feed items with same GUIDs does not create duplicates")
    @MainActor
    func reorderedItemsNoDuplicates() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        let feedURL = URL(string: "https://example.com/dedup/feed.xml")!

        let initialFeed = RSSFeedStubFactory.feedXML(
            title: "Dedup Show",
            author: "Author",
            episodes: [
                .init(title: "Alpha", guid: "alpha", enclosureURL: URL(string: "https://example.com/a.mp3")),
                .init(title: "Beta",  guid: "beta",  enclosureURL: URL(string: "https://example.com/b.mp3")),
                .init(title: "Gamma", guid: "gamma", enclosureURL: URL(string: "https://example.com/c.mp3")),
            ]
        )
        // Same GUIDs, totally different XML order.
        let reorderedFeed = RSSFeedStubFactory.feedXML(
            title: "Dedup Show",
            author: "Author",
            episodes: [
                .init(title: "Gamma", guid: "gamma", enclosureURL: URL(string: "https://example.com/c.mp3")),
                .init(title: "Alpha", guid: "alpha", enclosureURL: URL(string: "https://example.com/a.mp3")),
                .init(title: "Beta",  guid: "beta",  enclosureURL: URL(string: "https://example.com/b.mp3")),
            ]
        )

        let counter = AtomicCounter()
        stub.register(.init(
            matches: { req in
                req.url?.absoluteString.contains("dedup/feed.xml") ?? false
            },
            response: { _ in
                let n = counter.incrementAndGet()
                return .success(n == 1 ? initialFeed : reorderedFeed, statusCode: 200)
            }
        ))

        let service = PodcastDiscoveryService(session: session)
        let container = try makeFeedDiscoveryContainer()
        let context = ModelContext(container)

        let parsed = try await service.fetchFeed(url: feedURL)
        let podcast = await service.persist(parsed, from: feedURL, in: context)
        try context.save()
        #expect(try context.fetch(FetchDescriptor<Episode>()).count == 3)

        // Refresh — same GUIDs, new order. No dupes expected.
        _ = try await service.refreshEpisodes(for: podcast, in: context)
        try context.save()

        let episodes = try context.fetch(FetchDescriptor<Episode>())
        #expect(episodes.count == 3)
        let guids = Set(episodes.map(\.feedItemGUID))
        #expect(guids == ["alpha", "beta", "gamma"])
    }

    @Test("Episode metadata updates in place when GUID matches but title or duration changes")
    @MainActor
    func metadataUpdatesInPlace() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        let feedURL = URL(string: "https://example.com/meta/feed.xml")!

        let v1 = RSSFeedStubFactory.feedXML(
            title: "Meta Show",
            author: "Author",
            episodes: [
                .init(
                    title: "Working Title",
                    guid: "stable-guid",
                    enclosureURL: URL(string: "https://example.com/m.mp3"),
                    pubDate: "Mon, 01 Jan 2024 12:00:00 GMT",
                    durationSeconds: 1800
                ),
            ]
        )
        let v2 = RSSFeedStubFactory.feedXML(
            title: "Meta Show",
            author: "Author",
            episodes: [
                // Same GUID — but title and duration changed (corrected
                // upstream).
                .init(
                    title: "Final Title",
                    guid: "stable-guid",
                    enclosureURL: URL(string: "https://example.com/m.mp3"),
                    pubDate: "Mon, 01 Jan 2024 12:00:00 GMT",
                    durationSeconds: 2700
                ),
            ]
        )

        let counter = AtomicCounter()
        stub.register(.init(
            matches: { req in
                req.url?.absoluteString.contains("meta/feed.xml") ?? false
            },
            response: { _ in
                let n = counter.incrementAndGet()
                return .success(n == 1 ? v1 : v2, statusCode: 200)
            }
        ))

        let service = PodcastDiscoveryService(session: session)
        let container = try makeFeedDiscoveryContainer()
        let context = ModelContext(container)

        let parsed = try await service.fetchFeed(url: feedURL)
        let podcast = await service.persist(parsed, from: feedURL, in: context)
        try context.save()

        let initial = try context.fetch(FetchDescriptor<Episode>())
        #expect(initial.count == 1)
        #expect(initial.first?.title == "Working Title")
        #expect(initial.first?.duration == 1800)
        let initialEpisodeID = initial.first?.persistentModelID

        _ = try await service.refreshEpisodes(for: podcast, in: context)
        try context.save()

        let updated = try context.fetch(FetchDescriptor<Episode>())
        #expect(updated.count == 1)
        let ep = try #require(updated.first)
        #expect(ep.title == "Final Title")
        #expect(ep.duration == 2700)
        // Same SwiftData row, just updated — not a different object.
        #expect(ep.persistentModelID == initialEpisodeID)
    }
}
