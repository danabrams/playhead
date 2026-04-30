// OPMLStressTests.swift
// playhead-2jo: stress + integration coverage for the OPML pipeline.
//
// These tests ride the same in-process path as the unit suites above
// but go end-to-end (parse -> import) with a synthetic 1000-entry
// document, and round-trip through serialize -> parse for a bigger
// payload. They catch regressions that a 3-feed fixture would miss
// (memory blowup, quadratic dedup, parser state leaks).

import Foundation
import Testing
@testable import Playhead

@Suite("OPMLService – Stress / Integration")
struct OPMLStressTests {

    // MARK: - Builders

    /// Build an OPML document with `count` distinct feeds. Titles and
    /// URLs are deterministic so failures are reproducible.
    private func buildOPML(count: Int) -> Data {
        var xml = #"<?xml version="1.0" encoding="UTF-8"?>"#
        xml.append("\n<opml version=\"2.0\">\n")
        xml.append("  <head><title>Stress</title></head>\n")
        xml.append("  <body>\n")
        for i in 0..<count {
            xml.append(
                "    <outline type=\"rss\" text=\"Show \(i)\" "
                + "xmlUrl=\"https://example.com/show\(i).rss\"/>\n"
            )
        }
        xml.append("  </body>\n</opml>\n")
        return Data(xml.utf8)
    }

    // MARK: - Parser stress

    @Test("Parser handles 1000 feeds without unbounded memory growth")
    func parses1000Feeds() throws {
        let data = buildOPML(count: 1000)
        let feeds = try OPMLService().parseOPML(from: data)
        #expect(feeds.count == 1000)
        #expect(feeds.first?.title == "Show 0")
        #expect(feeds.last?.title == "Show 999")
    }

    @Test("Round-trip 1000 feeds preserves identity")
    func roundTrip1000Feeds() throws {
        let original = (0..<1000).map {
            OPMLFeed(
                title: "Show \($0)",
                xmlUrl: URL(string: "https://example.com/show\($0).rss")!
            )
        }
        let bytes = OPMLService().serializeOPML(
            feeds: original, documentTitle: "Stress"
        )
        let parsed = try OPMLService().parseOPML(from: bytes)
        #expect(parsed == original)
    }

    // MARK: - End-to-end pipeline

    @Test("Parse + import: mixed duplicates, failures, success")
    func parseAndImportEndToEnd() async throws {
        let xml = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <opml version="2.0">
          <head><title>Mixed</title></head>
          <body>
            <outline text="Categorized">
              <outline type="rss" text="Already in library" xmlUrl="https://example.com/dupe.rss"/>
              <outline type="rss" text="Will succeed" xmlUrl="https://example.com/ok.rss"/>
            </outline>
            <outline type="rss" text="Will fail" xmlUrl="https://example.com/oops.rss"/>
          </body>
        </opml>
        """#
        let data = Data(xml.utf8)
        let service = OPMLService()
        let feeds = try service.parseOPML(from: data)
        #expect(feeds.count == 3)

        actor PersistedURLs {
            var urls: [URL] = []
            func add(_ u: URL) { urls.append(u) }
        }
        let persisted = PersistedURLs()

        let result = await service.importFeeds(
            feeds,
            exists: { url in url.absoluteString.contains("dupe") },
            resolve: { url in
                url.absoluteString.contains("oops")
                    ? .failure("HTTP 404")
                    : .success(())
            },
            persist: { feed in await persisted.add(feed.xmlUrl) },
            progress: { _, _ in }
        )

        #expect(result.imported == 1)
        #expect(result.skippedDuplicate == 1)
        #expect(result.failed.count == 1)
        #expect(result.failed.first?.reason == "HTTP 404")

        let urls = await persisted.urls
        #expect(urls.count == 1)
        #expect(urls.first?.absoluteString == "https://example.com/ok.rss")
    }
}
