// FeedParserStructuralOwnershipTests.swift
// playhead-e8mg: the channel `<link>` and `<itunes:owner><itunes:email>`.
//
// WHY HALF OF THIS FILE IS PUBLISHER BYTES. A hand-written fixture proves the
// parser parses what its author expected. It cannot prove the parser parses
// what publishers emit, and on this bead that gap is the whole defect: the two
// feeds this device is subscribed to carry two shapes nobody writing a fixture
// would have written.
//
//   * Conan O’Brien Needs A Friend puts `<image><link>` BEFORE the channel
//     `<link>`, and both hold `https://www.siriusxm.com` — the distributor,
//     not `teamcoco.com`. `teamcoco.com` is reachable only through
//     `<itunes:owner>`, which is the route this bead adds.
//   * The Diary Of A CEO has NO channel `<link>` at all. Its only `<link>`
//     element anywhere in the channel is `<image><link>`, and it holds the
//     feed's OWN URL. A parser that does not know `<image>` opens a scope
//     reports `rss2.flightcast.com` as the show's website — the hosting
//     platform, promoted to `.showOwned`, which is NEGATIVE ad evidence.
//
// The fixtures are verbatim channel heads with `</channel></rss>` appended;
// `PlayheadTests/Fixtures/RealFeeds/manifest.json` carries the capture recipe
// and a SHA-256 of each, which `fixtureBytesAreUnmodified` pins so an edit to
// a fixture reddens a rail instead of quietly becoming the thing under test.

import CryptoKit
import Foundation
import SwiftData
import Testing
@testable import Playhead

// MARK: - Synthetic shapes

@Suite("FeedParser – structural ownership signals")
struct FeedParserStructuralOwnershipTests {

    private func parse(_ xml: String) throws -> ParsedFeed {
        try FeedParser().parse(data: Data(xml.utf8))
    }

    private static let itunesNS = "http://www.itunes.com/dtds/podcast-1.0.dtd"

    private func rss(channelExtras: String) -> String {
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0" xmlns:itunes="\(Self.itunesNS)"
             xmlns:atom="http://www.w3.org/2005/Atom">
          <channel>
            <title>Test Podcast</title>
            \(channelExtras)
            <item>
              <title>Ep</title>
              <guid>ep-1</guid>
              <enclosure url="https://cdn.example.com/ep1.mp3" type="audio/mpeg" length="1"/>
            </item>
          </channel>
        </rss>
        """
    }

    @Test("Captures the channel <link>")
    func channelLink() throws {
        let feed = try parse(rss(channelExtras: "<link>https://www.myshow.com/</link>"))
        #expect(feed.siteURL?.absoluteString == "https://www.myshow.com/")
    }

    @Test("Captures the <itunes:owner> email")
    func itunesOwnerEmail() throws {
        let feed = try parse(rss(channelExtras: """
        <itunes:owner>
          <itunes:name>Jane Host</itunes:name>
          <itunes:email>jane@myshow.com</itunes:email>
        </itunes:owner>
        """))
        #expect(feed.ownerEmail == "jane@myshow.com")
    }

    /// The Diary Of A CEO shape, reduced. `<image>` carries its own `<link>`
    /// and it is not the channel's.
    @Test("An <image><link> is NOT the channel link")
    func imageLinkIsNotTheChannelLink() throws {
        let feed = try parse(rss(channelExtras: """
        <image>
          <title>Test Podcast</title>
          <url>https://cdn.example.com/art.png</url>
          <link>https://feeds.example.com/the-feed.xml</link>
        </image>
        """))
        #expect(feed.siteURL == nil)
    }

    /// …and the scope must CLOSE, or the first real `<link>` after the image
    /// block is lost too.
    @Test("A channel <link> after an <image> block is still captured")
    func channelLinkAfterImageBlock() throws {
        let feed = try parse(rss(channelExtras: """
        <image>
          <title>Test Podcast</title>
          <url>https://cdn.example.com/art.png</url>
          <link>https://feeds.example.com/the-feed.xml</link>
        </image>
        <link>https://www.myshow.com/</link>
        """))
        #expect(feed.siteURL?.absoluteString == "https://www.myshow.com/")
    }

    /// The Conan ORDER: channel link second, image link first, and the image
    /// one must not win by arriving first.
    @Test("<image><link> first, channel <link> second — the channel one wins")
    func imageLinkDoesNotWinByArrivingFirst() throws {
        let feed = try parse(rss(channelExtras: """
        <image>
          <link>https://cdn.example.com/artwork-page</link>
          <title>Test Podcast</title>
        </image>
        <link>https://www.myshow.com/</link>
        """))
        #expect(feed.siteURL?.absoluteString == "https://www.myshow.com/")
    }

    /// A self-closing `<itunes:image href="…"/>` reports a start AND an end
    /// element. If the scope counter is not symmetric it never returns to
    /// zero and every later `<link>` is discarded.
    @Test("A self-closing <itunes:image> does not swallow the channel <link>")
    func selfClosingITunesImageKeepsScopeBalanced() throws {
        let feed = try parse(rss(channelExtras: """
        <itunes:image href="https://cdn.example.com/art.png"/>
        <link>https://www.myshow.com/</link>
        """))
        #expect(feed.siteURL?.absoluteString == "https://www.myshow.com/")
        #expect(feed.artworkURL?.absoluteString == "https://cdn.example.com/art.png")
    }

    /// RSS orders `<link>` before `<image>`; first-one-wins is what makes that
    /// ordering decisive, and it is the same rule `title` and `author` use.
    @Test("The FIRST channel <link> wins")
    func firstChannelLinkWins() throws {
        let feed = try parse(rss(channelExtras: """
        <link>https://www.myshow.com/</link>
        <link>https://www.somewhere-else.com/</link>
        """))
        #expect(feed.siteURL?.absoluteString == "https://www.myshow.com/")
    }

    /// `<atom:link rel="self">` is the feed's own address. It appears at
    /// channel level on a huge share of real RSS feeds and is never the site.
    @Test("An atom self link in an RSS channel is not the site link")
    func atomSelfLinkInRSSChannelIgnored() throws {
        let feed = try parse(rss(channelExtras: """
        <atom:link href="https://feeds.example.com/the-feed.xml" rel="self" type="application/rss+xml"/>
        """))
        #expect(feed.siteURL == nil)
    }

    /// An `<itunes:email>` outside `<itunes:owner>` is somebody else's
    /// address. Measured over 918 real feeds on 2026-08-19: zero carry one, so
    /// requiring the wrapper costs nothing and rules out a wrong capture.
    @Test("An <itunes:email> outside <itunes:owner> is ignored")
    func bareITunesEmailIgnored() throws {
        let feed = try parse(rss(channelExtras: "<itunes:email>webmaster@network.com</itunes:email>"))
        #expect(feed.ownerEmail == nil)
    }

    /// …and the owner scope must close, or an item-level element could be
    /// mistaken for the owner's.
    @Test("An <itunes:email> after </itunes:owner> is ignored")
    func itunesEmailAfterOwnerScopeIgnored() throws {
        let feed = try parse(rss(channelExtras: """
        <itunes:owner>
          <itunes:name>Jane Host</itunes:name>
        </itunes:owner>
        <itunes:email>webmaster@network.com</itunes:email>
        """))
        #expect(feed.ownerEmail == nil)
    }

    @Test("Absent signals stay nil rather than becoming an empty string")
    func absentSignalsAreNil() throws {
        let feed = try parse(rss(channelExtras: ""))
        #expect(feed.siteURL == nil)
        #expect(feed.ownerEmail == nil)
    }

    @Test("Atom feeds take the site link from rel=alternate, not rel=self")
    func atomAlternateLink() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
          <title>Atom Show</title>
          <link rel="self" href="https://feeds.example.com/atom.xml"/>
          <link rel="alternate" href="https://www.myshow.com/"/>
          <entry>
            <title>Ep</title>
            <id>ep-1</id>
            <link rel="enclosure" href="https://cdn.example.com/ep1.mp3" type="audio/mpeg"/>
          </entry>
        </feed>
        """
        let feed = try parse(xml)
        #expect(feed.siteURL?.absoluteString == "https://www.myshow.com/")
    }

    /// RFC 4287: a missing `rel` MEANS `alternate`.
    @Test("An Atom link with no rel is the site link")
    func atomLinkWithoutRelIsTheSiteLink() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
          <title>Atom Show</title>
          <link href="https://www.myshow.com/"/>
          <entry><title>Ep</title><id>ep-1</id></entry>
        </feed>
        """
        let feed = try parse(xml)
        #expect(feed.siteURL?.absoluteString == "https://www.myshow.com/")
    }

    /// An item-level `<link>` is the EPISODE's page. It must not be promoted
    /// to the channel's site link — `handleChannelElement` is only reached
    /// outside an item, and this is what proves that still holds.
    @Test("An item-level <link> is not the channel link")
    func itemLinkIsNotTheChannelLink() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel>
          <title>Test Podcast</title>
          <item>
            <title>Ep</title><guid>ep-1</guid>
            <link>https://www.episode-page.com/ep1</link>
            <enclosure url="https://cdn.example.com/ep1.mp3" type="audio/mpeg" length="1"/>
          </item>
        </channel></rss>
        """
        let feed = try parse(xml)
        #expect(feed.siteURL == nil)
    }
}

// MARK: - Real publisher bytes

@Suite("FeedParser – real subscribed feeds")
struct FeedParserRealFeedTests {

    struct Manifest: Decodable {
        let captured: String
        let feeds: [Entry]
        struct Entry: Decodable {
            let feedURL: String
            let title: String
            let file: String
            let bytes: Int
            let sha256: String
        }
    }

    /// `#filePath` is `<repo>/PlayheadTests/Services/PodcastFeed/<this file>`.
    static let fixturesDirectory: URL = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()   // PodcastFeed
        .deletingLastPathComponent()   // Services
        .deletingLastPathComponent()   // PlayheadTests
        .deletingLastPathComponent()   // <repo>
        .appendingPathComponent("PlayheadTests/Fixtures/RealFeeds")

    static func manifest() throws -> Manifest {
        let data = try Data(contentsOf: fixturesDirectory.appendingPathComponent("manifest.json"))
        return try JSONDecoder().decode(Manifest.self, from: data)
    }

    static func feedData(_ file: String) throws -> Data {
        try Data(contentsOf: fixturesDirectory.appendingPathComponent(file))
    }

    private static func sha256Hex(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    /// The fixtures are evidence, so a silent edit must be loud. Without this
    /// a fix that "makes the test pass" by adjusting the bytes would be
    /// indistinguishable from a fix that makes the parser right.
    @Test("The fixture bytes are the bytes the manifest recorded")
    func fixtureBytesAreUnmodified() throws {
        let manifest = try Self.manifest()
        #expect(manifest.feeds.count == 2)
        for entry in manifest.feeds {
            let data = try Self.feedData(entry.file)
            #expect(data.count == entry.bytes, "\(entry.file): size changed")
            #expect(Self.sha256Hex(data) == entry.sha256, "\(entry.file): contents changed")
        }
    }

    private func parsed(_ file: String, feedURL: String) throws -> ParsedFeed {
        try FeedParser().parse(
            data: Self.feedData(file),
            baseURL: URL(string: feedURL)
        )
    }

    /// The headline finding of this bead, in the publisher's own bytes: the
    /// channel `<link>` is the DISTRIBUTOR and only `<itunes:owner>` names the
    /// show. Both are recorded here so a future feed change is visible as a
    /// red rail rather than as a silently different ownership graph.
    @Test("Conan: <link> is siriusxm.com and the owner is @teamcoco.com")
    func conanRealBytes() throws {
        let entry = try #require(try Self.manifest().feeds.first { $0.file.hasPrefix("conan") })
        let feed = try parsed(entry.file, feedURL: entry.feedURL)

        #expect(feed.title == "Conan O’Brien Needs A Friend")
        #expect(feed.siteURL?.absoluteString == "https://www.siriusxm.com")
        #expect(feed.ownerEmail == "conaf@teamcoco.com")
        #expect(DomainNormalizer.etld1(from: feed.siteURL?.absoluteString ?? "") == "siriusxm.com")
        #expect(DomainNormalizer.etld1(from: feed.ownerEmail ?? "") == "teamcoco.com")
        // The channel head carries no items, which is what makes it a head.
        #expect(feed.episodes.isEmpty)
    }

    /// The Diary Of A CEO carries NO channel `<link>`. Its only `<link>`
    /// anywhere in the channel is the artwork's, and that holds the feed's own
    /// URL — so `nil` here is the parser being right, and `flightcast.com`
    /// would be it being wrong in the most expensive direction.
    @Test("Diary Of A CEO: no channel <link>, owner is @stevenbartlett.com")
    func diaryOfACEORealBytes() throws {
        let entry = try #require(try Self.manifest().feeds.first { $0.file.hasPrefix("doac") })
        let feed = try parsed(entry.file, feedURL: entry.feedURL)

        #expect(feed.title == "The Diary Of A CEO with Steven Bartlett")
        #expect(feed.siteURL == nil, "the only <link> in this channel is <image><link>")
        #expect(feed.ownerEmail == "steven@stevenbartlett.com")
        #expect(DomainNormalizer.etld1(from: feed.ownerEmail ?? "") == "stevenbartlett.com")
        #expect(feed.episodes.isEmpty)
    }

    /// End to end on real bytes: parser → graph → the set the pipeline reads.
    /// This is the assertion the bead is actually about.
    @Test("Real bytes through the graph recover teamcoco.com and nothing else")
    func realBytesThroughTheOwnershipGraph() throws {
        for entry in try Self.manifest().feeds {
            let feed = try parsed(entry.file, feedURL: entry.feedURL)
            var graph = OwnershipGraph(
                podcastId: entry.feedURL,
                feedHostDomain: DomainNormalizer.etld1(from: entry.feedURL)
            )
            graph.ingestRSSFeed(
                linkURL: feed.siteURL?.absoluteString,
                itunesOwnerEmail: feed.ownerEmail
            )
            let showOwned = Set(graph.showOwnedDomains)

            if entry.file.hasPrefix("conan") {
                #expect(showOwned == ["teamcoco.com", "siriusxm.com"])
                #expect(!showOwned.contains("simplecast.com"))
            } else {
                #expect(showOwned == ["stevenbartlett.com"])
                #expect(!showOwned.contains("flightcast.com"))
            }
        }
    }
}

// MARK: - Persistence

/// playhead-e8mg: the parsed signals have to survive the trip into
/// `Podcast`, or `PlayheadApp` reads nil on every launch and the graph is
/// empty again. Both the INSERT and the REFRESH path are covered — a
/// publisher that moves its site or hands the feed to a new owner must be
/// able to move the ownership graph with it.
@Suite("PodcastDiscoveryService – structural ownership persistence")
struct PodcastStructuralOwnershipPersistenceTests {

    /// The container is RETURNED, not just its context. A helper that hands
    /// back `container.mainContext` alone lets the container go out of scope,
    /// and the test host dies with `Test crashed with signal trap.` — observed
    /// on this box before this was a `let container` the caller holds.
    @MainActor
    private func makeContainer(_ name: String) throws -> ModelContainer {
        let config = ModelConfiguration(
            name,
            schema: SwiftDataStore.schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        return try ModelContainer(for: SwiftDataStore.schema, configurations: [config])
    }

    private func feed(site: String?, owner: String?) -> ParsedFeed {
        ParsedFeed(
            title: "Test Podcast",
            author: "Host",
            description: "desc",
            artworkURL: nil,
            language: "en",
            categories: [],
            siteURL: site.flatMap(URL.init(string:)),
            ownerEmail: owner,
            episodes: []
        )
    }

    @MainActor
    @Test("A newly persisted Podcast carries the site link and owner address")
    func insertCarriesStructuralSignals() throws {
        let container = try makeContainer("E8MGInsert")
        let context = container.mainContext
        let feedURL = try #require(URL(string: "https://feeds.example.com/rss"))
        let podcast = PodcastDiscoveryService().persist(
            feed(site: "https://www.myshow.com", owner: "host@myshow.com"),
            from: feedURL,
            in: context
        )

        #expect(podcast.siteURL?.absoluteString == "https://www.myshow.com")
        #expect(podcast.ownerEmail == "host@myshow.com")
    }

    @MainActor
    @Test("A refresh updates both, including back to nil")
    func refreshUpdatesStructuralSignals() throws {
        let container = try makeContainer("E8MGRefresh")
        let context = container.mainContext
        let feedURL = try #require(URL(string: "https://feeds.example.com/rss"))
        let service = PodcastDiscoveryService()

        _ = service.persist(
            feed(site: "https://www.old-site.com", owner: "old@old-site.com"),
            from: feedURL,
            in: context
        )
        let moved = service.persist(
            feed(site: "https://www.new-site.com", owner: "new@new-site.com"),
            from: feedURL,
            in: context
        )
        #expect(moved.siteURL?.absoluteString == "https://www.new-site.com")
        #expect(moved.ownerEmail == "new@new-site.com")

        // A feed that STOPS declaring them must clear them: a stale owner
        // address is a `.showOwned` domain nothing in the feed supports any
        // more, and `.showOwned` is negative ad evidence.
        let withdrawn = service.persist(
            feed(site: nil, owner: nil),
            from: feedURL,
            in: context
        )
        #expect(withdrawn.siteURL == nil)
        #expect(withdrawn.ownerEmail == nil)
    }
}
