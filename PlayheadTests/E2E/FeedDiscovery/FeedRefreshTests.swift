// FeedRefreshTests.swift
// playhead-8u1 — E2E proof of the pull-to-refresh path on Library.
//
// Scenario 2 from the bead:
//   1. Subscribe to a podcast
//   2. Simulate a new episode added to the feed
//   3. Pull-to-refresh runs
//   4. New episode appears
//   5. Existing episodes are not duplicated
//
// We drive the REAL `PodcastDiscoveryService.refreshEpisodes(for:in:)`
// path — the same entry point the Library's pull-to-refresh modifier
// (via `EpisodeListView`) calls. The stub `URLSession` returns a
// 1-episode feed for the initial subscribe, then a 2-episode feed for
// the refresh.

import Foundation
import SwiftData
import Testing
@testable import Playhead

@Suite("playhead-8u1 - feed refresh E2E", .serialized)
struct FeedRefreshTests {

    @Test("Pull-to-refresh adds the new episode and does not duplicate the existing one")
    @MainActor
    func refreshAddsNewEpisodeNoDuplicates() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        let feedURL = URL(string: "https://example.com/refresh-show/feed.xml")!

        // Stage 1: initial feed body has one episode.
        let firstFeed = RSSFeedStubFactory.feedXML(
            title: "Refresh Show",
            author: "Pod Author",
            episodes: [
                .init(
                    title: "Episode One",
                    guid: "ep-001",
                    enclosureURL: URL(string: "https://example.com/refresh-show/ep1.mp3"),
                    pubDate: "Mon, 01 Jan 2024 12:00:00 GMT",
                    durationSeconds: 1800
                )
            ]
        )

        // Stage 2: refreshed feed body adds a new episode and keeps the
        // original one intact (same GUID).
        let secondFeed = RSSFeedStubFactory.feedXML(
            title: "Refresh Show",
            author: "Pod Author",
            episodes: [
                .init(
                    title: "Episode Two",
                    guid: "ep-002",
                    enclosureURL: URL(string: "https://example.com/refresh-show/ep2.mp3"),
                    pubDate: "Tue, 02 Jan 2024 12:00:00 GMT",
                    durationSeconds: 2400
                ),
                .init(
                    title: "Episode One",
                    guid: "ep-001",
                    enclosureURL: URL(string: "https://example.com/refresh-show/ep1.mp3"),
                    pubDate: "Mon, 01 Jan 2024 12:00:00 GMT",
                    durationSeconds: 1800
                )
            ]
        )

        // Phased handler: count refresh requests and return the matching
        // body. First call → first feed; subsequent calls → second feed.
        let callCounter = AtomicCounter()
        stub.register(.init(
            matches: { req in
                req.url?.absoluteString.contains("refresh-show/feed.xml") ?? false
            },
            response: { _ in
                let n = callCounter.incrementAndGet()
                return .success(n == 1 ? firstFeed : secondFeed, statusCode: 200)
            }
        ))

        let service = PodcastDiscoveryService(session: session)
        let container = try makeFeedDiscoveryContainer()
        let context = ModelContext(container)

        // Initial subscribe.
        let parsed = try await service.fetchFeed(url: feedURL)
        let podcast = await service.persist(parsed, from: feedURL, in: context)
        try context.save()

        let initialEpisodes = try context.fetch(FetchDescriptor<Episode>())
        #expect(initialEpisodes.count == 1)
        #expect(initialEpisodes.first?.feedItemGUID == "ep-001")

        // Pull-to-refresh.
        let newEpisodes = try await service.refreshEpisodes(for: podcast, in: context)
        try context.save()

        // The discovery service reports one new episode.
        #expect(newEpisodes.count == 1)
        #expect(newEpisodes.first?.feedItemGUID == "ep-002")

        // SwiftData has exactly 2 episodes — no duplicates.
        let allEpisodes = try context.fetch(FetchDescriptor<Episode>())
        #expect(allEpisodes.count == 2)
        let guids = Set(allEpisodes.map(\.feedItemGUID))
        #expect(guids == ["ep-001", "ep-002"])

        // Both episodes are linked to the same Podcast.
        #expect(allEpisodes.allSatisfy { $0.podcast?.feedURL == feedURL })
        #expect(podcast.episodes.count == 2)
    }

    @Test("Repeated refreshes with no feed changes do not create duplicates")
    @MainActor
    func repeatedRefreshNoOp() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        let feedURL = URL(string: "https://example.com/static-show/feed.xml")!
        let body = RSSFeedStubFactory.feedXML(
            title: "Static Show",
            author: "Static Author",
            episodes: [
                .init(
                    title: "Only Episode",
                    guid: "only-1",
                    enclosureURL: URL(string: "https://example.com/static-show/e1.mp3")
                ),
            ]
        )
        stub.registerStub(
            whereURLContains: "static-show/feed.xml",
            respondWith: body
        )

        let service = PodcastDiscoveryService(session: session)
        let container = try makeFeedDiscoveryContainer()
        let context = ModelContext(container)

        let parsed = try await service.fetchFeed(url: feedURL)
        let podcast = await service.persist(parsed, from: feedURL, in: context)
        try context.save()

        for _ in 0 ..< 3 {
            let new = try await service.refreshEpisodes(for: podcast, in: context)
            #expect(new.isEmpty)
            try context.save()
        }

        let episodes = try context.fetch(FetchDescriptor<Episode>())
        #expect(episodes.count == 1)
    }
}

// MARK: - Atomic counter for ordered stub responses

/// Minimal Sendable counter used by the phased URLProtocol handler in
/// `refreshAddsNewEpisodeNoDuplicates`. NSLock is preferred over Swift
/// concurrency primitives because the URLProtocol response closure runs
/// synchronously on URLSession's own queue.
final class AtomicCounter: @unchecked Sendable {
    private var value: Int = 0
    private let lock = NSLock()

    func incrementAndGet() -> Int {
        lock.lock(); defer { lock.unlock() }
        value += 1
        return value
    }
}
