// PodcastFeedParserChaptersTests.swift
// playhead-gtt9.22: Tests for `<podcast:chapters>` URL capture and
// `<podcast:chapter>` inline parsing on `PodcastFeedParser`.
//
// Acceptance: parser handles inline chapters AND captures the optional
// PC20 external chapters URL without making a network call. Malformed
// inputs degrade gracefully (no crash, empty arrays, no thrown error
// beyond the documented `FeedParserError` cases).

import Foundation
import Testing
@testable import Playhead

// MARK: - Inline `<podcast:chapter>` Parsing

@Suite("PodcastFeedParser – inline `<podcast:chapter>`")
struct PodcastFeedParserInlineChapterTests {

    private func parse(_ xml: String) throws -> ParsedFeed {
        try FeedParser().parse(data: Data(xml.utf8))
    }

    @Test("captures inline chapters with start times and titles")
    func inlineChaptersBasic() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"
             xmlns:podcast="https://podcastindex.org/namespace/1.0">
          <channel>
            <title>Chap Pod</title>
            <item>
              <title>Episode</title>
              <guid>ep-001</guid>
              <enclosure url="https://example.com/ep.mp3" type="audio/mpeg" length="1"/>
              <podcast:chapter startTime="0" title="Intro"/>
              <podcast:chapter startTime="120" title="Sponsored by BetterHelp"/>
              <podcast:chapter startTime="240" title="Main Content"/>
            </item>
          </channel>
        </rss>
        """
        let feed = try parse(xml)
        #expect(feed.episodes.count == 1)
        let ep = feed.episodes[0]
        #expect(ep.chapters.count == 3)
        #expect(ep.chapters[0].title == "Intro")
        #expect(ep.chapters[0].startTime == 0)
        #expect(ep.chapters[1].title == "Sponsored by BetterHelp")
        #expect(ep.chapters[1].startTime == 120)
        #expect(ep.chapters[2].title == "Main Content")
        #expect(ep.chapters[2].startTime == 240)
    }

    @Test("inline chapter timestamps tolerate HH:MM:SS form")
    func inlineChaptersHmsTimestamps() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"
             xmlns:podcast="https://podcastindex.org/namespace/1.0">
          <channel><title>P</title>
            <item><title>E</title><guid>g</guid>
              <podcast:chapter startTime="00:02:00" title="Sponsor"/>
              <podcast:chapter startTime="01:00:30" title="Outro"/>
            </item>
          </channel>
        </rss>
        """
        let feed = try parse(xml)
        #expect(feed.episodes[0].chapters[0].startTime == 120)
        #expect(feed.episodes[0].chapters[1].startTime == 3630)
    }
}

// MARK: - `<podcast:chapters>` URL Capture

@Suite("PodcastFeedParser – `<podcast:chapters>` URL")
struct PodcastFeedParserPC20URLTests {

    private func parse(_ xml: String) throws -> ParsedFeed {
        try FeedParser().parse(data: Data(xml.utf8))
    }

    @Test("captures the PC20 chapters URL from `url` attribute")
    func pc20ChaptersURL() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"
             xmlns:podcast="https://podcastindex.org/namespace/1.0">
          <channel><title>P</title>
            <item><title>E</title><guid>g</guid>
              <podcast:chapters
                  url="https://example.com/ep1-chapters.json"
                  type="application/json+chapters"/>
            </item>
          </channel>
        </rss>
        """
        let feed = try parse(xml)
        #expect(feed.episodes[0].chaptersFeedURL?.absoluteString
                 == "https://example.com/ep1-chapters.json")
    }

    @Test("absent `<podcast:chapters>` leaves chaptersFeedURL == nil")
    func pc20ChaptersAbsent() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel><title>P</title>
          <item><title>E</title><guid>g</guid></item>
        </channel></rss>
        """
        let feed = try parse(xml)
        #expect(feed.episodes[0].chaptersFeedURL == nil)
    }

    @Test("`<podcast:chapters>` without url attribute degrades gracefully")
    func pc20ChaptersMissingURLAttribute() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"
             xmlns:podcast="https://podcastindex.org/namespace/1.0">
          <channel><title>P</title>
            <item><title>E</title><guid>g</guid>
              <podcast:chapters type="application/json+chapters"/>
            </item>
          </channel>
        </rss>
        """
        let feed = try parse(xml)
        #expect(feed.episodes[0].chaptersFeedURL == nil)
    }
}

// MARK: - Malformed / Edge Cases

@Suite("PodcastFeedParser – chapter edge cases")
struct PodcastFeedParserChapterEdgeCaseTests {

    private func parse(_ xml: String) throws -> ParsedFeed {
        try FeedParser().parse(data: Data(xml.utf8))
    }

    @Test("malformed chapter timestamp does not throw; chapter is skipped or zeroed")
    func malformedTimestampGracefulDegradation() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"
             xmlns:podcast="https://podcastindex.org/namespace/1.0">
          <channel><title>P</title>
            <item><title>E</title><guid>g</guid>
              <podcast:chapter startTime="not-a-number" title="Bad"/>
              <podcast:chapter startTime="120" title="Sponsor"/>
            </item>
          </channel>
        </rss>
        """
        let feed = try parse(xml)
        // Bad chapter falls back to 0; valid one preserved.
        #expect(feed.episodes[0].chapters.count == 2)
        #expect(feed.episodes[0].chapters[0].startTime == 0)
        #expect(feed.episodes[0].chapters[0].title == "Bad")
        #expect(feed.episodes[0].chapters[1].startTime == 120)
    }

    @Test("feed with no chapters at all yields empty chapters array")
    func noChaptersIsNoOp() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel><title>P</title>
          <item><title>E</title><guid>g</guid>
            <enclosure url="https://example.com/ep.mp3" type="audio/mpeg" length="1"/>
          </item>
        </channel></rss>
        """
        let feed = try parse(xml)
        #expect(feed.episodes[0].chapters.isEmpty)
        #expect(feed.episodes[0].chaptersFeedURL == nil)
    }

    @Test("multiple PC20 chapters URLs: first wins")
    func multiplePC20URLsFirstWins() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"
             xmlns:podcast="https://podcastindex.org/namespace/1.0"
             xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
          <channel><title>P</title>
            <item><title>E</title><guid>g</guid>
              <podcast:chapters url="https://a.example/c.json"/>
              <itunes:chapters url="https://b.example/c.json"/>
            </item>
          </channel>
        </rss>
        """
        let feed = try parse(xml)
        // podcast:chapters appears first; iTunes namespace branch
        // checks for nil before overwriting, so the first survives.
        #expect(feed.episodes[0].chaptersFeedURL?.absoluteString
                 == "https://a.example/c.json")
    }
}
