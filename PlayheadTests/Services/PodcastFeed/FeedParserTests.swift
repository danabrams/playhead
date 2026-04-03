// FeedParserTests.swift
// Unit tests for the RSS/Atom feed parser.

import Foundation
import Testing
@testable import Playhead

// MARK: - Basic RSS Parsing

@Suite("FeedParser – RSS 2.0")
struct FeedParserRSSTests {

    private func parse(_ xml: String) throws -> ParsedFeed {
        let data = Data(xml.utf8)
        return try FeedParser().parse(data: data)
    }

    @Test("Parses channel-level metadata")
    func channelMetadata() throws {
        let feed = try parse(Fixtures.minimalRSS)
        #expect(feed.title == "Test Podcast")
        #expect(feed.author == "Jane Host")
        #expect(feed.description == "A test podcast feed")
        #expect(feed.language == "en-us")
        #expect(feed.artworkURL?.absoluteString == "https://example.com/art.jpg")
    }

    @Test("Parses categories")
    func categories() throws {
        let feed = try parse(Fixtures.minimalRSS)
        #expect(feed.categories.contains("Technology"))
    }

    @Test("Parses episode GUID and enclosure")
    func episodeGUIDAndEnclosure() throws {
        let feed = try parse(Fixtures.minimalRSS)
        #expect(feed.episodes.count == 1)
        let ep = feed.episodes[0]
        #expect(ep.guid == "ep-001")
        #expect(ep.enclosureURL?.absoluteString == "https://example.com/ep1.mp3")
        #expect(ep.enclosureType == "audio/mpeg")
        #expect(ep.enclosureLength == 12345678)
    }

    @Test("Parses pubDate in RFC 2822")
    func pubDate() throws {
        let feed = try parse(Fixtures.minimalRSS)
        let ep = feed.episodes[0]
        #expect(ep.pubDate != nil)
    }

    @Test("Parses iTunes duration HH:MM:SS")
    func duration() throws {
        let feed = try parse(Fixtures.minimalRSS)
        let ep = feed.episodes[0]
        #expect(ep.duration == 3661) // 1:01:01
    }

    @Test("Parses iTunes episode number")
    func episodeNumber() throws {
        let feed = try parse(Fixtures.minimalRSS)
        let ep = feed.episodes[0]
        #expect(ep.itunesEpisodeNumber == 42)
    }

    @Test("Parses description and show notes")
    func descriptionAndShowNotes() throws {
        let feed = try parse(Fixtures.minimalRSS)
        let ep = feed.episodes[0]
        #expect(ep.description == "Episode description")
        #expect(ep.showNotes == "<p>Rich show notes</p>")
    }

    @Test("Parses iTunes image on episode")
    func episodeImage() throws {
        let feed = try parse(Fixtures.minimalRSS)
        let ep = feed.episodes[0]
        #expect(ep.itunesImageURL?.absoluteString == "https://example.com/ep1art.jpg")
    }

    @Test("Enclosure identity combines URL, type, length")
    func enclosureIdentity() throws {
        let feed = try parse(Fixtures.minimalRSS)
        let ep = feed.episodes[0]
        #expect(ep.enclosureIdentity == "https://example.com/ep1.mp3|audio/mpeg|12345678")
    }
}

// MARK: - Missing / Malformed Fields

@Suite("FeedParser – Quirky Feeds")
struct FeedParserQuirkyTests {

    private func parse(_ xml: String) throws -> ParsedFeed {
        let data = Data(xml.utf8)
        return try FeedParser().parse(data: data)
    }

    @Test("Synthesizes GUID from enclosure URL when missing")
    func missingGUID() throws {
        let feed = try parse(Fixtures.missingGUID)
        #expect(feed.episodes.count == 1)
        #expect(feed.episodes[0].guid == "https://example.com/ep.mp3")
    }

    @Test("Synthesizes GUID from title when no enclosure or GUID")
    func missingGUIDAndEnclosure() throws {
        let feed = try parse(Fixtures.missingGUIDAndEnclosure)
        let ep = feed.episodes[0]
        #expect(ep.guid == "Bare Podcast::Bare Episode")
    }

    @Test("Deduplicates episodes with identical GUIDs")
    func duplicateGUID() throws {
        let feed = try parse(Fixtures.duplicateGUID)
        #expect(feed.episodes.count == 1)
    }

    @Test("Handles missing optional fields gracefully")
    func missingOptionals() throws {
        let feed = try parse(Fixtures.bareMinimum)
        #expect(feed.episodes.count == 1)
        let ep = feed.episodes[0]
        #expect(ep.pubDate == nil)
        #expect(ep.duration == nil)
        #expect(ep.itunesEpisodeNumber == nil)
        #expect(ep.chapters.isEmpty)
    }

    @Test("Parses duration from MM:SS format")
    func durationMMSS() throws {
        let feed = try parse(Fixtures.durationMMSS)
        #expect(feed.episodes[0].duration == 3661) // 61:01
    }

    @Test("Parses duration from raw seconds")
    func durationRawSeconds() throws {
        let feed = try parse(Fixtures.durationSeconds)
        #expect(feed.episodes[0].duration == 1800)
    }

    @Test("Empty data throws emptyData error")
    func emptyData() {
        #expect(throws: FeedParserError.emptyData) {
            try FeedParser().parse(data: Data())
        }
    }
}

// MARK: - Podcasting 2.0 Chapters

@Suite("FeedParser – Podcasting 2.0")
struct FeedParserChapterTests {

    private func parse(_ xml: String) throws -> ParsedFeed {
        let data = Data(xml.utf8)
        return try FeedParser().parse(data: data)
    }

    @Test("Parses inline podcast:chapter elements")
    func inlineChapters() throws {
        let feed = try parse(Fixtures.withChapters)
        let ep = feed.episodes[0]
        #expect(ep.chapters.count == 2)
        #expect(ep.chapters[0].startTime == 0)
        #expect(ep.chapters[0].title == "Intro")
        #expect(ep.chapters[1].startTime == 120)
        #expect(ep.chapters[1].title == "Main Topic")
    }
}

// MARK: - Atom Feed Parsing

@Suite("FeedParser – Atom")
struct FeedParserAtomTests {

    private func parse(_ xml: String) throws -> ParsedFeed {
        let data = Data(xml.utf8)
        return try FeedParser().parse(data: data)
    }

    @Test("Parses basic Atom feed")
    func atomBasic() throws {
        let feed = try parse(Fixtures.atomFeed)
        #expect(feed.title == "Atom Podcast")
        #expect(feed.episodes.count == 1)
        let ep = feed.episodes[0]
        #expect(ep.guid == "atom-ep-1")
        #expect(ep.enclosureURL?.absoluteString == "https://example.com/atom.mp3")
        #expect(ep.pubDate != nil)
    }
}

// MARK: - Test Fixtures

private enum Fixtures {
    static let minimalRSS = """
    <?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"
         xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
         xmlns:content="http://purl.org/rss/1.0/modules/content/"
         xmlns:podcast="https://podcastindex.org/namespace/1.0">
      <channel>
        <title>Test Podcast</title>
        <description>A test podcast feed</description>
        <language>en-us</language>
        <itunes:author>Jane Host</itunes:author>
        <itunes:image href="https://example.com/art.jpg"/>
        <itunes:category text="Technology"/>
        <item>
          <title>Episode One</title>
          <guid>ep-001</guid>
          <enclosure url="https://example.com/ep1.mp3" type="audio/mpeg" length="12345678"/>
          <pubDate>Mon, 01 Jan 2024 12:00:00 GMT</pubDate>
          <description>Episode description</description>
          <content:encoded><![CDATA[<p>Rich show notes</p>]]></content:encoded>
          <itunes:duration>1:01:01</itunes:duration>
          <itunes:episode>42</itunes:episode>
          <itunes:author>Jane Host</itunes:author>
          <itunes:image href="https://example.com/ep1art.jpg"/>
        </item>
      </channel>
    </rss>
    """

    static let missingGUID = """
    <?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel><title>No GUID Pod</title>
    <item><title>Ep</title>
    <enclosure url="https://example.com/ep.mp3" type="audio/mpeg" length="100"/>
    </item></channel></rss>
    """

    static let missingGUIDAndEnclosure = """
    <?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel><title>Bare Podcast</title>
    <item><title>Bare Episode</title></item></channel></rss>
    """

    static let duplicateGUID = """
    <?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel><title>Dupe Pod</title>
    <item><title>Ep A</title><guid>same-guid</guid></item>
    <item><title>Ep B</title><guid>same-guid</guid></item>
    </channel></rss>
    """

    static let bareMinimum = """
    <?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel><title>Bare</title>
    <item><title>Ep</title><guid>g1</guid></item>
    </channel></rss>
    """

    static let durationMMSS = """
    <?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
    <channel><title>Dur</title>
    <item><title>Ep</title><guid>g1</guid>
    <itunes:duration>61:01</itunes:duration></item>
    </channel></rss>
    """

    static let durationSeconds = """
    <?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
    <channel><title>Dur</title>
    <item><title>Ep</title><guid>g1</guid>
    <itunes:duration>1800</itunes:duration></item>
    </channel></rss>
    """

    static let withChapters = """
    <?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"
         xmlns:podcast="https://podcastindex.org/namespace/1.0">
    <channel><title>Chap Pod</title>
    <item><title>Ep</title><guid>g1</guid>
    <podcast:chapter startTime="0" title="Intro"/>
    <podcast:chapter startTime="02:00" title="Main Topic"/>
    </item></channel></rss>
    """

    static let atomFeed = """
    <?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <title>Atom Podcast</title>
      <author><name>Atom Author</name></author>
      <entry>
        <title>Atom Episode</title>
        <id>atom-ep-1</id>
        <published>2024-01-01T12:00:00Z</published>
        <link rel="enclosure" href="https://example.com/atom.mp3"
              type="audio/mpeg" length="9999"/>
      </entry>
    </feed>
    """
}
