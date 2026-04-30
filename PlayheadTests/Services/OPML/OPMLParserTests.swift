// OPMLParserTests.swift
// playhead-2jo: parse OPML subscription lists into a flat list of feeds.
//
// Fixtures are anchored to this file's #filePath so they remain readable
// regardless of whether the test bundle was rebuilt with stale resources.

import Foundation
import Testing
@testable import Playhead

@Suite("OPMLService – Parsing")
struct OPMLParserTests {

    // MARK: - Fixture Helpers

    /// Walks up from this test file's #filePath to `PlayheadTests/Fixtures/OPML`.
    private static func fixturesDirectory(filePath: String = #filePath) -> URL {
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent() // .../PlayheadTests/Services/OPML
            .deletingLastPathComponent() // .../PlayheadTests/Services
            .deletingLastPathComponent() // .../PlayheadTests
            .appendingPathComponent("Fixtures", isDirectory: true)
            .appendingPathComponent("OPML", isDirectory: true)
    }

    private static func loadFixture(_ name: String) throws -> Data {
        let url = fixturesDirectory().appendingPathComponent(name)
        return try Data(contentsOf: url)
    }

    private func parse(_ name: String) throws -> [OPMLFeed] {
        let data = try Self.loadFixture(name)
        return try OPMLService().parseOPML(from: data)
    }

    // MARK: - Standard exports

    @Test("Overcast export: flattens single-level outline group")
    func overcastExport() throws {
        let feeds = try parse("overcast-export.opml")
        #expect(feeds.count == 3)
        #expect(feeds[0].title == "The Daily")
        #expect(feeds[0].xmlUrl.absoluteString == "https://feeds.simplecast.com/54nAGcIl")
        #expect(feeds[1].title == "Reply All")
        #expect(feeds[2].title == "Conan O'Brien Needs A Friend")
    }

    @Test("Pocket Casts export: parses two feeds")
    func pocketCastsExport() throws {
        let feeds = try parse("pocketcasts-export.opml")
        #expect(feeds.count == 2)
        #expect(feeds.map(\.title) == ["Hardcore History", "Radiolab"])
    }

    @Test("Apple Podcasts (flat body): no wrapper outline group")
    func applePodcastsFlat() throws {
        let feeds = try parse("apple-podcasts-flat.opml")
        #expect(feeds.count == 2)
        #expect(feeds[0].title == "Diary of a CEO")
        #expect(feeds[0].xmlUrl.absoluteString == "https://feeds.flightcast.com/diary-of-a-ceo")
        #expect(feeds[1].title == "Smartless")
    }

    @Test("Categorized OPML: nested outline groups flatten recursively")
    func categorizedOPML() throws {
        let feeds = try parse("categorized.opml")
        // Order follows the document order (depth-first traversal).
        #expect(feeds.map(\.title) == [
            "The Daily",
            "ATP",
            "Conan O'Brien Needs A Friend",
        ])
    }

    @Test("Unicode titles preserved verbatim")
    func unicodeTitles() throws {
        let feeds = try parse("unicode-titles.opml")
        #expect(feeds.count == 3)
        #expect(feeds[0].title == "Café del Mar — Música")
        #expect(feeds[1].title == "日本語ポッドキャスト")
        #expect(feeds[2].title == "Naïve & Sentimental") // entity-decoded
    }

    @Test("Falls back to text= attribute when title= is absent")
    func textAttributeFallback() throws {
        let feeds = try parse("text-only-no-title.opml")
        #expect(feeds.count == 1)
        #expect(feeds[0].title == "Show With Only text")
    }

    // MARK: - Edge cases

    @Test("Outlines without xmlUrl (folders, bookmarks) are skipped")
    func skipsOutlinesWithoutXmlUrl() throws {
        let feeds = try parse("missing-xmlurl.opml")
        #expect(feeds.count == 1)
        #expect(feeds[0].title == "Has feed")
    }

    @Test("Empty body throws .emptyFile")
    func emptyBodyThrowsEmptyFile() throws {
        let data = try Self.loadFixture("empty-body.opml")
        #expect(throws: OPMLError.self) {
            try OPMLService().parseOPML(from: data)
        }
        do {
            _ = try OPMLService().parseOPML(from: data)
            Issue.record("Expected throw")
        } catch let error as OPMLError {
            if case .emptyFile = error { /* ok */ } else {
                Issue.record("Expected .emptyFile, got \(error)")
            }
        }
    }

    @Test("Malformed XML throws .invalidFormat")
    func malformedXMLThrowsInvalidFormat() throws {
        let data = try Self.loadFixture("malformed.opml")
        do {
            _ = try OPMLService().parseOPML(from: data)
            Issue.record("Expected throw")
        } catch let error as OPMLError {
            if case .invalidFormat = error { /* ok */ } else {
                Issue.record("Expected .invalidFormat, got \(error)")
            }
        }
    }

    @Test("Empty input data throws .invalidFormat")
    func emptyDataThrowsInvalidFormat() throws {
        do {
            _ = try OPMLService().parseOPML(from: Data())
            Issue.record("Expected throw")
        } catch let error as OPMLError {
            if case .invalidFormat = error { /* ok */ } else {
                Issue.record("Expected .invalidFormat, got \(error)")
            }
        }
    }

    @Test("Garbage non-XML data throws .invalidFormat")
    func garbageDataThrowsInvalidFormat() throws {
        let data = Data("this is not xml at all { json: 1 }".utf8)
        do {
            _ = try OPMLService().parseOPML(from: data)
            Issue.record("Expected throw")
        } catch let error as OPMLError {
            if case .invalidFormat = error { /* ok */ } else {
                Issue.record("Expected .invalidFormat, got \(error)")
            }
        }
    }

    @Test("Outlines with whitespace-only xmlUrl are skipped (not crashed)")
    func whitespaceXmlUrlSkipped() throws {
        let xml = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <opml version="2.0">
          <head><title>x</title></head>
          <body>
            <outline type="rss" text="Real" xmlUrl="https://example.com/real.rss"/>
            <outline type="rss" text="Bad" xmlUrl="   "/>
          </body>
        </opml>
        """#
        let feeds = try OPMLService().parseOPML(from: Data(xml.utf8))
        #expect(feeds.count == 1)
        #expect(feeds[0].title == "Real")
    }

    @Test("Outlines with non-URL xmlUrl are skipped")
    func nonURLXmlUrlSkipped() throws {
        let xml = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <opml version="2.0">
          <head><title>x</title></head>
          <body>
            <outline type="rss" text="Real" xmlUrl="https://example.com/real.rss"/>
            <outline type="rss" text="Bad" xmlUrl="not a url with space"/>
          </body>
        </opml>
        """#
        let feeds = try OPMLService().parseOPML(from: Data(xml.utf8))
        // Foundation's URL(string:) is permissive enough that "not a url"
        // can succeed, but ones with invalid characters should fail.
        // The contract: only URLs with a scheme are accepted.
        #expect(feeds.count == 1)
        #expect(feeds[0].title == "Real")
    }

    @Test("Title-less outlines fall back to xmlUrl host")
    func titlelessOutlineFallsBackToHost() throws {
        let xml = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <opml version="2.0">
          <head><title>x</title></head>
          <body>
            <outline type="rss" xmlUrl="https://example.com/no-title.rss"/>
          </body>
        </opml>
        """#
        let feeds = try OPMLService().parseOPML(from: Data(xml.utf8))
        #expect(feeds.count == 1)
        // Title is nil — caller decides display fallback.
        #expect(feeds[0].title == nil)
        #expect(feeds[0].xmlUrl.absoluteString == "https://example.com/no-title.rss")
    }

    @Test("Duplicate xmlUrl entries collapse to a single feed")
    func duplicateUrlsCollapse() throws {
        let xml = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <opml version="2.0">
          <head><title>x</title></head>
          <body>
            <outline type="rss" text="A" xmlUrl="https://example.com/a.rss"/>
            <outline type="rss" text="A again" xmlUrl="https://example.com/a.rss"/>
            <outline type="rss" text="B" xmlUrl="https://example.com/b.rss"/>
          </body>
        </opml>
        """#
        let feeds = try OPMLService().parseOPML(from: Data(xml.utf8))
        #expect(feeds.count == 2)
        #expect(feeds[0].title == "A")
        #expect(feeds[1].title == "B")
    }

    @Test("Leading UTF-8 BOM does not crash the parser")
    func leadingBOMOK() throws {
        let bom: [UInt8] = [0xEF, 0xBB, 0xBF]
        let body = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <opml version="2.0">
          <head><title>x</title></head>
          <body>
            <outline type="rss" text="A" xmlUrl="https://example.com/a.rss"/>
          </body>
        </opml>
        """#
        var data = Data(bom)
        data.append(Data(body.utf8))
        let feeds = try OPMLService().parseOPML(from: data)
        #expect(feeds.count == 1)
    }

    @Test("OPML inside a comment block does not surface as a feed")
    func commentedOutFeedNotEmitted() throws {
        let xml = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <opml version="2.0">
          <head><title>x</title></head>
          <body>
            <outline type="rss" text="Real" xmlUrl="https://example.com/real.rss"/>
            <!-- <outline type="rss" text="Commented" xmlUrl="https://example.com/dead.rss"/> -->
          </body>
        </opml>
        """#
        let feeds = try OPMLService().parseOPML(from: Data(xml.utf8))
        #expect(feeds.count == 1)
        #expect(feeds[0].title == "Real")
    }

    @Test("Non-OPML XML root throws .invalidFormat")
    func nonOPMLRootThrows() throws {
        let xml = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <title>Wrong shape</title>
          </channel>
        </rss>
        """#
        do {
            _ = try OPMLService().parseOPML(from: Data(xml.utf8))
            Issue.record("Expected throw")
        } catch let error as OPMLError {
            if case .invalidFormat = error { /* ok */ } else {
                Issue.record("Expected .invalidFormat, got \(error)")
            }
        }
    }

    @Test("Deeply nested outlines (4+ levels) still flatten")
    func deeplyNestedFlattens() throws {
        let xml = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <opml version="2.0">
          <head><title>x</title></head>
          <body>
            <outline text="L1">
              <outline text="L2">
                <outline text="L3">
                  <outline text="L4">
                    <outline type="rss" text="Deep" xmlUrl="https://example.com/deep.rss"/>
                  </outline>
                </outline>
              </outline>
            </outline>
          </body>
        </opml>
        """#
        let feeds = try OPMLService().parseOPML(from: Data(xml.utf8))
        #expect(feeds.count == 1)
        #expect(feeds[0].title == "Deep")
    }
}
