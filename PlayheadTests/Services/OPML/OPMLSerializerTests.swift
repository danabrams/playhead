// OPMLSerializerTests.swift
// playhead-2jo: serialize an in-memory subscription list back to OPML 2.0.

import Foundation
import Testing
@testable import Playhead

@Suite("OPMLService – Serialize")
struct OPMLSerializerTests {

    // MARK: - Helpers

    private func serialize(_ feeds: [OPMLFeed]) -> String {
        let data = OPMLService().serializeOPML(
            feeds: feeds,
            documentTitle: "Playhead Subscriptions"
        )
        return String(decoding: data, as: UTF8.self)
    }

    // MARK: - Output structure

    @Test("Output starts with the XML 1.0 declaration")
    func xmlDeclaration() {
        let xml = serialize([
            OPMLFeed(title: "x", xmlUrl: URL(string: "https://example.com/x.rss")!),
        ])
        #expect(xml.hasPrefix(#"<?xml version="1.0" encoding="UTF-8"?>"#))
    }

    @Test("Root element is <opml version=\"2.0\">")
    func opmlRoot() {
        let xml = serialize([
            OPMLFeed(title: "x", xmlUrl: URL(string: "https://example.com/x.rss")!),
        ])
        #expect(xml.contains(#"<opml version="2.0">"#))
        #expect(xml.contains("</opml>"))
    }

    @Test("Head carries the document title")
    func headTitle() {
        let xml = serialize([
            OPMLFeed(title: "x", xmlUrl: URL(string: "https://example.com/x.rss")!),
        ])
        #expect(xml.contains("<head>"))
        #expect(xml.contains("<title>Playhead Subscriptions</title>"))
        #expect(xml.contains("</head>"))
    }

    @Test("Body emits one <outline> per feed with type=rss + xmlUrl + text")
    func bodyOutlines() {
        let xml = serialize([
            OPMLFeed(title: "Show A", xmlUrl: URL(string: "https://example.com/a.rss")!),
            OPMLFeed(title: "Show B", xmlUrl: URL(string: "https://example.com/b.rss")!),
        ])
        #expect(xml.contains(#"text="Show A""#))
        #expect(xml.contains(#"xmlUrl="https://example.com/a.rss""#))
        #expect(xml.contains(#"type="rss""#))
        #expect(xml.contains(#"text="Show B""#))
    }

    // MARK: - Output is bytes-finishable UTF-8

    @Test("Serialized data is UTF-8 encoded")
    func utf8Encoding() {
        let data = OPMLService().serializeOPML(
            feeds: [
                OPMLFeed(title: "日本語", xmlUrl: URL(string: "https://example.com/jp.rss")!),
            ],
            documentTitle: "x"
        )
        let str = String(data: data, encoding: .utf8)
        #expect(str != nil)
        #expect(str?.contains("日本語") == true)
    }

    // MARK: - Escaping

    @Test("Ampersand and quotes in title are XML-escaped")
    func escapesAmpersandAndQuotes() {
        let xml = serialize([
            OPMLFeed(
                title: "Naïve & \"Sentimental\" <show>",
                xmlUrl: URL(string: "https://example.com/feed.rss")!
            ),
        ])
        // The literal characters must NOT appear unescaped inside an
        // attribute value.
        #expect(xml.contains("&amp;"))
        #expect(xml.contains("&quot;"))
        #expect(xml.contains("&lt;show&gt;"))
        // The accented character does NOT need escaping (UTF-8).
        #expect(xml.contains("Naïve"))
    }

    @Test("Ampersand in xmlUrl query string is XML-escaped")
    func escapesAmpersandInXmlUrl() {
        let url = URL(string: "https://example.com/rss?a=1&b=2")!
        let xml = serialize([OPMLFeed(title: "Q", xmlUrl: url)])
        // xmlUrl literal '&' must be encoded as `&amp;` when serialized.
        #expect(xml.contains("a=1&amp;b=2"))
        // We must NOT emit a raw '&' inside the attribute (would invalidate XML).
        let raw = "a=1&b=2\""
        #expect(!xml.contains(raw))
    }

    @Test("Nil title falls back to xmlUrl absoluteString")
    func titleFallsBackToUrl() {
        let url = URL(string: "https://example.com/no-title.rss")!
        let xml = serialize([OPMLFeed(title: nil, xmlUrl: url)])
        #expect(xml.contains(#"text="https://example.com/no-title.rss""#))
    }

    // MARK: - Round trip

    @Test("Round trip: serialize → parse yields equivalent feed list")
    func roundTrip() throws {
        let original = [
            OPMLFeed(
                title: "Daily News",
                xmlUrl: URL(string: "https://example.com/daily.rss")!
            ),
            OPMLFeed(
                title: "Naïve & Sentimental",
                xmlUrl: URL(string: "https://example.com/naive?utm_source=a&utm=b")!
            ),
            OPMLFeed(
                title: "日本語ポッドキャスト",
                xmlUrl: URL(string: "https://example.com/jp.rss")!
            ),
        ]
        let data = OPMLService().serializeOPML(
            feeds: original, documentTitle: "Playhead Subscriptions"
        )
        let reparsed = try OPMLService().parseOPML(from: data)
        #expect(reparsed.count == original.count)
        for (orig, parsed) in zip(original, reparsed) {
            #expect(parsed.xmlUrl == orig.xmlUrl)
            #expect(parsed.title == orig.title)
        }
    }

    @Test("Round trip is stable: parse(serialize(parse(x))) == parse(x)")
    func roundTripStable() throws {
        let original = [
            OPMLFeed(
                title: "A",
                xmlUrl: URL(string: "https://a.example.com/feed.rss")!
            ),
            OPMLFeed(
                title: "B with & ampersand",
                xmlUrl: URL(string: "https://b.example.com/feed.rss?x=1&y=2")!
            ),
        ]
        let onceSerialized = OPMLService().serializeOPML(
            feeds: original, documentTitle: "x"
        )
        let onceParsed = try OPMLService().parseOPML(from: onceSerialized)
        let twiceSerialized = OPMLService().serializeOPML(
            feeds: onceParsed, documentTitle: "x"
        )
        let twiceParsed = try OPMLService().parseOPML(from: twiceSerialized)
        #expect(onceParsed == twiceParsed)
        // Serializing the same input twice produces the same bytes.
        #expect(onceSerialized == twiceSerialized)
    }
}
