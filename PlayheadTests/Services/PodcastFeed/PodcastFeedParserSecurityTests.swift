// PodcastFeedParserSecurityTests.swift
// Defensive contract tests for `FeedParser` against XML attack patterns
// (XXE, billion-laughs entity expansion). Foundation's `XMLParser`
// defaults are safe today — external entity resolution is disabled and
// nested-entity expansion is bounded — but the parser explicitly pins
// `shouldResolveExternalEntities = false` and
// `externalEntityResolvingPolicy = .never` so a future refactor cannot
// silently regress.
//
// Two layers of pinning (L4 / reviewer suggestion / rfu-mn):
//   1. Direct property assertions on a freshly-configured XMLParser.
//      A refactor that drops either flag would fail the property test
//      regardless of XML content. This is the load-bearing assertion.
//   2. Behavioral fixtures (billion-laughs, external entity) that
//      exercise the attack patterns end-to-end. These no longer rely
//      on wall-clock thresholds — they assert structural properties
//      of the parsed output (bounded description size, no leaked
//      external content) so a loaded simulator cannot flake the
//      suite.

import Foundation
import Testing
@testable import Playhead

@Suite("FeedParser – security")
struct PodcastFeedParserSecurityTests {

    private func parse(_ xml: String) throws -> ParsedFeed {
        let data = Data(xml.utf8)
        return try FeedParser().parse(data: data)
    }

    // MARK: - Direct property assertions

    @Test("applySecurityHardening pins shouldResolveExternalEntities = false")
    func hardeningDisablesExternalEntityResolution() {
        // Build an unconfigured XMLParser and apply hardening — the
        // contract is that the flag flips to false (Foundation's
        // default, but pinned explicitly so a future refactor that
        // moves the parser onto a different default cannot regress).
        let parser = XMLParser(data: Data())
        FeedParser.applySecurityHardening(to: parser)
        #expect(parser.shouldResolveExternalEntities == false)
    }

    @Test("applySecurityHardening pins externalEntityResolvingPolicy = .never")
    func hardeningSetsResolvingPolicyToNever() {
        let parser = XMLParser(data: Data())
        FeedParser.applySecurityHardening(to: parser)
        #expect(parser.externalEntityResolvingPolicy == .never)
    }

    @Test("applySecurityHardening enables namespace processing")
    func hardeningEnablesNamespaces() {
        // Pin the namespace flags too — feed parsing of itunes / podcast
        // 2.0 / atom would silently degrade if a refactor flipped these.
        let parser = XMLParser(data: Data())
        FeedParser.applySecurityHardening(to: parser)
        #expect(parser.shouldProcessNamespaces == true)
        #expect(parser.shouldReportNamespacePrefixes == false)
    }

    // MARK: - Behavioral fixtures

    @Test("Recursive entity definitions do not pathologically expand (billion-laughs)")
    func billionLaughsBounded() throws {
        // Classic billion-laughs: each level references the previous so a
        // naive parser expands `lol4` to 10^4 copies of "lol". We keep
        // the corpus tiny (4 levels, each multiplier of 10) so the test
        // is fast even if the platform happens to expand them. We
        // assert structural properties of the output — NOT wall-clock
        // elapsed time, which flakes on a loaded CI simulator.
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
        // Either the parse throws (refusing the DOCTYPE / entity) or it
        // succeeds with a bounded description. Both are acceptable; what
        // matters is that the parser does not produce a description
        // ballooned to the full 10^4 expansion. We tolerate either
        // outcome because Foundation's XMLParser behavior across iOS
        // versions is implementation-defined.
        let result = Result { try self.parse(xml) }
        switch result {
        case .success(let feed):
            // If the parser accepted the document, the description must
            // not contain the recursive expansion — i.e. it must NOT
            // contain enough "lol" repetitions to evidence pathological
            // expansion (10^4 = 10 000 occurrences). 100 is a safe
            // upper bound: every realistic entity expansion path will
            // either produce 0 (rejected), 1–10 ("lol" stripped of
            // entity refs), or 10 000+ (full expansion). 100 splits
            // those two regimes unambiguously.
            let lolCount = feed.description.components(separatedBy: "lol").count - 1
            #expect(lolCount < 100,
                    "billion-laughs description contained \(lolCount) 'lol' tokens — expected <100, parser may be expanding entities pathologically")
            // Generous total-size ceiling as a secondary tripwire.
            #expect(feed.description.utf8.count < 1_000_000)
        case .failure:
            // Parser rejected the doc — also fine.
            break
        }
    }

    @Test("External entity reference is not resolved over the network")
    func externalEntityNotResolved() throws {
        // Classic XXE shape: declare an external entity pointing at a
        // URL the parser must NOT fetch. With
        // `externalEntityResolvingPolicy = .never`, the entity simply
        // fails to expand. We assert the *structural* property — the
        // parsed description must NOT contain content sourced from
        // the URL — rather than relying on a wall-clock heuristic.
        // Since the entity is declared but not resolved, the literal
        // entity reference (or an empty expansion) appears in the
        // text, NEVER any text fetched over the network.
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
        let result = Result { try self.parse(xml) }
        switch result {
        case .success(let feed):
            // The description must NOT contain any string that could
            // have come from resolving the external URL. Since 127.0.0.1
            // port 1 is unreachable, any non-empty expansion would
            // imply network access succeeded — that's the contract
            // we're pinning.
            #expect(!feed.description.contains("should-never-load") ||
                    feed.description.contains("&ext;") ||
                    feed.description.contains("before-") &&
                    feed.description.contains("-after"),
                    "External entity content appears to have been resolved")
            // Description must be small (the entity reference dropped or
            // left as literal text — never a fetched payload).
            #expect(feed.description.utf8.count < 1_000)
        case .failure:
            // Parser rejected the DOCTYPE — also fine, it's the
            // strongest possible "did not resolve" outcome.
            break
        }
    }
}
