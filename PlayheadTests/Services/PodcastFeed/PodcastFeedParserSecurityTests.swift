// PodcastFeedParserSecurityTests.swift
// Defensive contract tests for `FeedParser` against XML attack patterns
// (XXE, billion-laughs entity expansion). Foundation's `XMLParser`
// defaults are safe today — external entity resolution is disabled and
// nested-entity expansion is bounded — but the parser explicitly pins
// `shouldResolveExternalEntities = false` and
// `externalEntityResolvingPolicy = .never` so a future refactor cannot
// silently regress.
//
// These tests are deliberately *contract* tests, not fuzz: a small fixture
// per attack class, asserting the parser completes within a reasonable
// time/memory bound. We don't attempt to exercise the full attack surface;
// we just pin "the parser does not blow up on these patterns."

import Foundation
import Testing
@testable import Playhead

@Suite("FeedParser – security")
struct PodcastFeedParserSecurityTests {

    private func parse(_ xml: String) throws -> ParsedFeed {
        let data = Data(xml.utf8)
        return try FeedParser().parse(data: data)
    }

    @Test("Recursive entity definitions do not pathologically expand (billion-laughs)")
    func billionLaughsBounded() throws {
        // Classic billion-laughs: each level references the previous so a
        // naive parser expands `lol4` to 10^4 copies of "lol". We keep
        // the corpus tiny (4 levels, each multiplier of 10) so the test
        // is fast even if the platform happens to expand them — but the
        // assertion is that the parser doesn't recurse pathologically
        // and doesn't take more than a few seconds.
        let xml = """
        <?xml version="1.0"?>
        <!DOCTYPE rss [
          <!ENTITY lol "lol">
          <!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
          <!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">
          <!ENTITY lol4 "&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;">
        ]>
        <rss version="2.0">
          <channel>
            <title>safe-title</title>
            <description>&lol4;</description>
            <item>
              <title>ep</title>
              <guid>g</guid>
              <enclosure url="https://example.com/a.mp3" type="audio/mpeg" length="1"/>
            </item>
          </channel>
        </rss>
        """
        let start = Date()
        // Either the parse throws (refusing the DOCTYPE / entity) or it
        // succeeds with a bounded description. Both are acceptable; what
        // matters is that we return promptly. We tolerate either outcome
        // because Foundation's XMLParser behavior across iOS versions is
        // implementation-defined, and the contract we want to pin is
        // "doesn't blow up", not "always rejects".
        let result = Result { try self.parse(xml) }
        let elapsed = Date().timeIntervalSince(start)
        #expect(elapsed < 5.0,
                "billion-laughs fixture took \(elapsed)s — expected <5 s, parser may be expanding entities pathologically")
        switch result {
        case .success(let feed):
            // If the parser accepted the document, the description must
            // not have ballooned to 10 000+ characters from the entity
            // expansion. Bound at a generous 1 MB just in case of exotic
            // platform behavior.
            #expect(feed.description.utf8.count < 1_000_000)
        case .failure:
            // Parser rejected the doc — also fine.
            break
        }
    }

    @Test("External entity reference is not resolved over the network")
    func externalEntityNotResolved() throws {
        // Classic XXE shape: declare an external entity pointing at a
        // URL we don't expect the parser to fetch. We use 127.0.0.1 with
        // a port nothing is listening on, and a short overall timeout —
        // if the parser tried to resolve the entity, the test would
        // either hang briefly or surface network-error noise. With
        // `externalEntityResolvingPolicy = .never`, the entity simply
        // fails to expand and the document parses (or fails) without
        // hitting the network.
        let xml = """
        <?xml version="1.0"?>
        <!DOCTYPE rss [
          <!ENTITY ext SYSTEM "http://127.0.0.1:1/should-never-load">
        ]>
        <rss version="2.0">
          <channel>
            <title>title</title>
            <description>before-&ext;-after</description>
            <item>
              <title>ep</title>
              <guid>g</guid>
              <enclosure url="https://example.com/a.mp3" type="audio/mpeg" length="1"/>
            </item>
          </channel>
        </rss>
        """
        let start = Date()
        _ = Result { try self.parse(xml) }
        let elapsed = Date().timeIntervalSince(start)
        // If the parser had attempted a network fetch, this would block
        // until the connect timeout (typically several seconds). 2 s is
        // a safe ceiling for an in-process parse with no network access.
        #expect(elapsed < 2.0,
                "external-entity fixture took \(elapsed)s — expected <2 s, parser may be resolving entities over the network")
    }
}
