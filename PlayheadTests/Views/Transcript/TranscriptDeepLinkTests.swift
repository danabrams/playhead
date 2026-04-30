// TranscriptDeepLinkTests.swift
// playhead-m8v7: pure-function tests for the deep link encoder/parser
// that round-trips `(episodeId, seconds)` ↔ `playhead://episode/<id>?t=<sec>`.
// The shared-quote feature emits one of these URLs into the share-sheet
// payload; the URL handler in `PlayheadAppDelegate`'s `onOpenURL`
// equivalent uses the parser to navigate.

import Foundation
import Testing
@testable import Playhead

@Suite("TranscriptDeepLink — encode and parse")
struct TranscriptDeepLinkTests {

    // MARK: - Encoding

    @Test("Encoding produces the canonical scheme/host/path/query form")
    func encodingProducesCanonicalForm() {
        let url = TranscriptDeepLink.url(episodeId: "abc-123", startTime: 762)
        // 12:42 → 762 seconds.
        #expect(url.absoluteString == "playhead://episode/abc-123?t=762")
    }

    @Test("Encoding rounds fractional seconds to the nearest integer")
    func encodingRoundsFractionalSeconds() {
        let url = TranscriptDeepLink.url(episodeId: "abc", startTime: 12.4)
        #expect(url.absoluteString == "playhead://episode/abc?t=12")

        let url2 = TranscriptDeepLink.url(episodeId: "abc", startTime: 12.6)
        #expect(url2.absoluteString == "playhead://episode/abc?t=13")
    }

    @Test("Encoding clamps negative times to zero")
    func encodingClampsNegativeTimes() {
        let url = TranscriptDeepLink.url(episodeId: "abc", startTime: -5)
        #expect(url.absoluteString == "playhead://episode/abc?t=0")
    }

    @Test("Encoding percent-encodes episode ids that contain reserved characters")
    func encodingPercentEncodesIds() {
        // Canonical keys in production are URL-safe, but the encoder
        // must not emit a malformed URL if a future id format ever
        // contains reserved characters.
        let url = TranscriptDeepLink.url(episodeId: "abc/def?x=1", startTime: 30)
        // The parser must round-trip the same id back.
        let parsed = TranscriptDeepLink.parse(url)
        #expect(parsed?.episodeId == "abc/def?x=1")
        #expect(parsed?.startTime == 30)
    }

    // MARK: - Parsing

    @Test("Parsing the canonical form returns the original episodeId and time")
    func parseCanonical() {
        let url = URL(string: "playhead://episode/abc-123?t=762")!
        let parsed = TranscriptDeepLink.parse(url)
        #expect(parsed?.episodeId == "abc-123")
        #expect(parsed?.startTime == 762)
    }

    @Test("Parsing tolerates a missing t parameter and defaults to 0")
    func parseMissingTime() {
        let url = URL(string: "playhead://episode/abc-123")!
        let parsed = TranscriptDeepLink.parse(url)
        #expect(parsed?.episodeId == "abc-123")
        #expect(parsed?.startTime == 0)
    }

    @Test("Parsing rejects a wrong scheme")
    func parseRejectsWrongScheme() {
        let url = URL(string: "https://episode/abc?t=10")!
        #expect(TranscriptDeepLink.parse(url) == nil)
    }

    @Test("Parsing rejects a wrong host")
    func parseRejectsWrongHost() {
        let url = URL(string: "playhead://show/abc?t=10")!
        #expect(TranscriptDeepLink.parse(url) == nil)
    }

    @Test("Parsing rejects an empty episode id path")
    func parseRejectsEmptyId() {
        let url = URL(string: "playhead://episode/?t=10")!
        #expect(TranscriptDeepLink.parse(url) == nil)
    }

    @Test("Parsing rejects a non-numeric t value")
    func parseRejectsNonNumericTime() {
        let url = URL(string: "playhead://episode/abc?t=abc")!
        #expect(TranscriptDeepLink.parse(url) == nil)
    }

    @Test("Parsing clamps negative t to zero")
    func parseClampsNegativeTime() {
        let url = URL(string: "playhead://episode/abc?t=-5")!
        let parsed = TranscriptDeepLink.parse(url)
        #expect(parsed?.startTime == 0)
    }

    // MARK: - Round-trip

    @Test("Round-trip preserves the original episode id and integer-second time")
    func roundTrip() {
        let inputs: [(String, TimeInterval)] = [
            ("simple-id", 0),
            ("with-dashes-and-numbers-42", 3725),  // 1:02:05
            ("hash-like-9a8b7c6d", 12.0),
            ("uuid-D8B3F-32-CC0", 599),
        ]
        for (id, t) in inputs {
            let url = TranscriptDeepLink.url(episodeId: id, startTime: t)
            let parsed = TranscriptDeepLink.parse(url)
            #expect(parsed?.episodeId == id)
            #expect(parsed?.startTime == t)
        }
    }
}
