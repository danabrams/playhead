// MalformedFeedTests.swift
// playhead-8u1 — E2E proof that malformed feeds don't crash the app.
//
// Scenario 4 from the bead:
//   1. Parse a feed with: missing GUIDs, relative URLs, malformed dates
//   2. No crash
//   3. Episodes with parseable data are created
//   4. Unparseable episodes are skipped (no audio URL, etc.) without
//      tearing down the run
//
// Combines a "kitchen sink" malformed feed with a couple of focused
// pathological cases (HTTP 500 on fetch, totally truncated XML body).
// Each one must surface a regular Swift error — never a process crash.

import Foundation
import SwiftData
import Testing
@testable import Playhead

@Suite("playhead-8u1 - malformed feed handling E2E", .serialized)
struct MalformedFeedTests {

    @Test("Kitchen-sink malformed feed: parses what it can, skips what it can't, no crash")
    @MainActor
    func kitchenSinkMalformedFeed() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        let feedURL = URL(string: "https://example.com/messy/feed.xml")!

        // A feed body that exercises every malformed-input we say we
        // tolerate:
        //   * Item 1: missing GUID, relative enclosure URL, malformed date
        //   * Item 2: well-formed
        //   * Item 3: missing enclosure entirely (no audio URL) — should
        //            be skipped at persist time (no Episode row)
        //   * Item 4: malformed date and zero duration — should still
        //            persist with publishedAt=nil
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"
             xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
          <channel>
            <title>Messy Pod</title>
            <description>An intentionally messy feed</description>
            <itunes:author>Mess Author</itunes:author>
            <item>
              <title>No GUID, Relative URL, Malformed Date</title>
              <enclosure url="audio/relative.mp3" type="audio/mpeg" length="100"/>
              <pubDate>not-a-real-date</pubDate>
              <itunes:duration>1800</itunes:duration>
            </item>
            <item>
              <title>Well Formed</title>
              <guid>good-1</guid>
              <enclosure url="https://example.com/messy/good.mp3" type="audio/mpeg" length="100"/>
              <pubDate>Mon, 01 Jan 2024 12:00:00 GMT</pubDate>
              <itunes:duration>1800</itunes:duration>
            </item>
            <item>
              <title>No Enclosure</title>
              <guid>no-enclosure</guid>
              <pubDate>Tue, 02 Jan 2024 12:00:00 GMT</pubDate>
            </item>
            <item>
              <title>Bad Date Only</title>
              <guid>bad-date-1</guid>
              <enclosure url="https://example.com/messy/bad-date.mp3" type="audio/mpeg" length="100"/>
              <pubDate>2024-13-32T99:99:99Z</pubDate>
              <itunes:duration>0</itunes:duration>
            </item>
          </channel>
        </rss>
        """
        stub.registerStub(
            whereURLContains: "messy/feed.xml",
            respondWith: Data(xml.utf8)
        )

        let service = PodcastDiscoveryService(session: session)
        let container = try makeFeedDiscoveryContainer()
        let context = ModelContext(container)

        // Production parses successfully — that's the no-crash contract.
        let parsed = try await service.fetchFeed(url: feedURL)
        // The parser still surfaces all four items (the no-enclosure one
        // included — it's the persist step that drops it for lack of an
        // audioURL).
        #expect(parsed.episodes.count == 4)

        let podcast = await service.persist(parsed, from: feedURL, in: context)
        try context.save()

        let episodes = try context.fetch(FetchDescriptor<Episode>())
        // The no-enclosure item is dropped at persist time (Episode requires
        // an audioURL); the other three persist.
        #expect(episodes.count == 3)
        let titles = Set(episodes.map(\.title))
        #expect(titles.contains("Well Formed"))
        #expect(titles.contains("Bad Date Only"))
        // The no-GUID/relative-URL/malformed-date item should be present.
        // The parser synthesizes its GUID from the (now absolute, against
        // base feed URL) enclosure URL.
        let synthesizedGUIDEpisode = episodes.first {
            $0.feedItemGUID.contains("relative.mp3")
        }
        #expect(synthesizedGUIDEpisode != nil)
        #expect(synthesizedGUIDEpisode?.audioURL.absoluteString.contains("relative.mp3") == true)
        // Relative URL was resolved against the feed URL.
        #expect(synthesizedGUIDEpisode?.audioURL.scheme == "https")

        // Bad-date episode: persisted with publishedAt=nil, not crashed.
        let badDate = try #require(episodes.first { $0.feedItemGUID == "bad-date-1" })
        #expect(badDate.publishedAt == nil)

        #expect(podcast.title == "Messy Pod")
    }

    @Test("HTTP 500 on feed fetch surfaces a Swift error, no crash")
    @MainActor
    func http500SurfacesError() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        stub.register(.init(
            matches: { req in
                req.url?.absoluteString.contains("server-error/feed.xml") ?? false
            },
            response: { _ in
                .success(Data("<error/>".utf8), statusCode: 500)
            }
        ))

        let service = PodcastDiscoveryService(session: session)
        let feedURL = URL(string: "https://example.com/server-error/feed.xml")!

        await #expect(throws: Error.self) {
            try await service.fetchFeed(url: feedURL)
        }
    }

    @Test("Truncated XML surfaces a parser error, no crash")
    @MainActor
    func truncatedXMLSurfacesError() async throws {
        let (session, stub) = makeStubbedSession()
        defer { stub.release() }

        // Truncated mid-element — XMLParser will report parse failure.
        let body = Data("<?xml version=\"1.0\"?><rss><channel><title>oop".utf8)
        stub.registerStub(
            whereURLContains: "broken/feed.xml",
            respondWith: body
        )

        let service = PodcastDiscoveryService(session: session)
        let feedURL = URL(string: "https://example.com/broken/feed.xml")!

        await #expect(throws: Error.self) {
            try await service.fetchFeed(url: feedURL)
        }
    }
}
