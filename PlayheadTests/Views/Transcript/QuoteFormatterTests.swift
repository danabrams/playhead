// QuoteFormatterTests.swift
// playhead-m8v7: pure-function tests for the editorial-formatted share
// text the transcript-share feature emits into UIActivityViewController.
//
// Format (locked by the bead spec, plain text for max portability):
//
//   "<quote text>"
//
//   — <Show title>, "<Episode title>"
//   <mm:ss timestamp>
//
//   Shared from Playhead

import Foundation
import Testing
@testable import Playhead

@Suite("QuoteFormatter — single-paragraph and multi-paragraph share text")
struct QuoteFormatterTests {

    // MARK: - Single paragraph

    @Test("Single quote with show + episode + timestamp + Playhead attribution")
    func singleQuote() {
        let result = QuoteFormatter.format(
            quotes: ["Mid-roll ads are now optional in the Playhead universe."],
            showTitle: "Diary of a CEO",
            episodeTitle: "On Burnout",
            startTime: 762,
            deepLinkURL: URL(string: "playhead://episode/ep-1?t=762")!
        )
        // 762 → 12:42 via TimeFormatter.formatTime.
        let expected = """
        \u{201C}Mid-roll ads are now optional in the Playhead universe.\u{201D}

        \u{2014} Diary of a CEO, \u{201C}On Burnout\u{201D}
        12:42

        Shared from Playhead
        playhead://episode/ep-1?t=762
        """
        #expect(result == expected)
    }

    // MARK: - Multi-paragraph

    @Test("Multiple paragraphs join with a blank line between them inside a single quote block")
    func multipleParagraphs() {
        let result = QuoteFormatter.format(
            quotes: [
                "First paragraph here.",
                "Second paragraph here."
            ],
            showTitle: "Show",
            episodeTitle: "Ep",
            startTime: 30,
            deepLinkURL: URL(string: "playhead://episode/ep?t=30")!
        )
        // The two paragraphs should sit inside one pair of quotes, with
        // a blank line between them — the standard editorial block-quote
        // form.
        let expected = """
        \u{201C}First paragraph here.

        Second paragraph here.\u{201D}

        \u{2014} Show, \u{201C}Ep\u{201D}
        0:30

        Shared from Playhead
        playhead://episode/ep?t=30
        """
        #expect(result == expected)
    }

    // MARK: - Whitespace / boundary trimming

    @Test("Each quote is trimmed of leading and trailing whitespace before formatting")
    func quoteWhitespaceTrimmed() {
        let result = QuoteFormatter.format(
            quotes: ["   spacy quote   "],
            showTitle: "Show",
            episodeTitle: "Ep",
            startTime: 0,
            deepLinkURL: URL(string: "playhead://episode/ep?t=0")!
        )
        // The opening curly quote sits flush against the trimmed text.
        #expect(result.contains("\u{201C}spacy quote\u{201D}"))
        // No leading-space artifact between opening quote and content.
        #expect(!result.contains("\u{201C} "))
    }

    @Test("Empty quote list returns an empty string")
    func emptyQuotesProducesEmptyString() {
        let result = QuoteFormatter.format(
            quotes: [],
            showTitle: "Show",
            episodeTitle: "Ep",
            startTime: 0,
            deepLinkURL: URL(string: "playhead://episode/ep?t=0")!
        )
        #expect(result.isEmpty)
    }

    // MARK: - Unicode safety

    @Test("Smart quotes / em-dashes inside the source text are preserved verbatim")
    func smartPunctuationPreserved() {
        let result = QuoteFormatter.format(
            quotes: ["She said \u{201C}absolutely\u{201D} \u{2014} and meant it."],
            showTitle: "Show",
            episodeTitle: "Ep",
            startTime: 0,
            deepLinkURL: URL(string: "playhead://episode/ep?t=0")!
        )
        #expect(result.contains("She said \u{201C}absolutely\u{201D} \u{2014} and meant it."))
    }

    @Test("RTL text is preserved verbatim")
    func rtlTextPreserved() {
        let arabic = "هذا اقتباس بالعربية."
        let result = QuoteFormatter.format(
            quotes: [arabic],
            showTitle: "Show",
            episodeTitle: "Ep",
            startTime: 0,
            deepLinkURL: URL(string: "playhead://episode/ep?t=0")!
        )
        #expect(result.contains(arabic))
    }

    // MARK: - Timestamp formatting

    @Test("Timestamps over an hour use H:MM:SS")
    func longTimestamp() {
        let result = QuoteFormatter.format(
            quotes: ["x"],
            showTitle: "Show",
            episodeTitle: "Ep",
            startTime: 3725,  // 1:02:05
            deepLinkURL: URL(string: "playhead://episode/ep?t=3725")!
        )
        #expect(result.contains("1:02:05"))
    }

    // MARK: - Line endings

    @Test("Empty show title omits the leading comma in the attribution line")
    func emptyShowTitleOmitsComma() {
        let result = QuoteFormatter.format(
            quotes: ["Test"],
            showTitle: "",
            episodeTitle: "Ep",
            startTime: 0,
            deepLinkURL: URL(string: "playhead://episode/ep?t=0")!
        )
        // No "—  ," sequence (em-dash space space comma) — the
        // attribution line should fall back to just the episode title
        // when the show title is empty.
        #expect(!result.contains(", \u{201C}Ep\u{201D}") || result.contains(" \u{201C}Ep\u{201D}"))
        // More directly: the attribution line should be either
        // "— Ep" form or the standard "— Show, Ep". Since show is
        // empty, expect "— "Ep"" with no leading comma.
        let attributionLine = result.split(separator: "\n").first(where: { $0.hasPrefix("\u{2014}") })
        #expect(attributionLine == "\u{2014} \u{201C}Ep\u{201D}")
    }

    @Test("Empty episode title falls back to show title in attribution")
    func emptyEpisodeTitleUsesShowOnly() {
        let result = QuoteFormatter.format(
            quotes: ["Test"],
            showTitle: "Show",
            episodeTitle: "",
            startTime: 0,
            deepLinkURL: URL(string: "playhead://episode/ep?t=0")!
        )
        let attributionLine = result.split(separator: "\n").first(where: { $0.hasPrefix("\u{2014}") })
        // Just the show, no trailing comma + empty quotes.
        #expect(attributionLine == "\u{2014} Show")
    }

    @Test("Output uses LF line endings only (no CRLF)")
    func usesLineFeedOnly() {
        let result = QuoteFormatter.format(
            quotes: ["only"],
            showTitle: "Show",
            episodeTitle: "Ep",
            startTime: 0,
            deepLinkURL: URL(string: "playhead://episode/ep?t=0")!
        )
        #expect(!result.contains("\r"))
    }
}
