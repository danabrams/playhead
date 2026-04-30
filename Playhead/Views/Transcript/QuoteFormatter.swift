// QuoteFormatter.swift
// playhead-m8v7: pure-function builder for the editorial-formatted
// transcript-share artifact. Lives next to `TranscriptDeepLink` because
// it consumes one (the URL is appended after the attribution block).
//
// Format (locked by the bead spec, plain text only — no rich formatting
// because share-sheet recipients render plain text identically across
// iMessage / Notes / Mail / Twitter / Slack):
//
//   "<quote text>"
//
//   — <Show title>, "<Episode title>"
//   <H:MM:SS or M:SS>
//
//   Shared from Playhead
//   <deep link URL>
//
// Multi-paragraph quotes sit inside ONE pair of curly quotes with a
// blank line between paragraphs.

import Foundation

// MARK: - QuoteFormatter

enum QuoteFormatter {

    /// Curly-quote codepoints used everywhere in the share artifact.
    /// Pinned as constants so unit tests and future callers can match
    /// on them without hard-coding magic strings.
    static let leftDoubleQuote: Character = "\u{201C}"
    static let rightDoubleQuote: Character = "\u{201D}"
    static let emDash: Character = "\u{2014}"

    /// Compose the share artifact for one or more selected paragraphs.
    /// Returns an empty string when `quotes` is empty so callers can
    /// guard the share-button enabled state on `result.isEmpty == false`
    /// (in practice the view-model already enforces "selection ≥ 1",
    /// so this branch is defensive).
    static func format(
        quotes: [String],
        showTitle: String,
        episodeTitle: String,
        startTime: TimeInterval,
        deepLinkURL: URL
    ) -> String {
        guard !quotes.isEmpty else { return "" }

        // Trim each paragraph independently then join with a blank line
        // — the standard editorial form for a multi-paragraph block
        // quote inside one pair of curly quotes.
        let trimmed = quotes
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
        guard !trimmed.isEmpty else { return "" }

        let body = trimmed.joined(separator: "\n\n")
        let timestamp = TimeFormatter.formatTime(startTime)

        // Plain `\n` line endings — recipients on every platform render
        // them identically. Build the artifact in one literal so the
        // shape is obvious at a glance.
        return """
        \(leftDoubleQuote)\(body)\(rightDoubleQuote)

        \(emDash) \(showTitle), \(leftDoubleQuote)\(episodeTitle)\(rightDoubleQuote)
        \(timestamp)

        Shared from Playhead
        \(deepLinkURL.absoluteString)
        """
    }
}
