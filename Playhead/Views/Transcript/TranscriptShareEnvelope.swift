// TranscriptShareEnvelope.swift
// playhead-m8v7: value type bundling the share-sheet payload — the
// editorial-formatted share text plus the standalone deep-link URL.
// Both are exposed to `ShareLink`/`UIActivityViewController` so the
// recipient can render the rich block quote (text) AND iOS can
// recognize a tappable `playhead://` link (URL).

import Foundation

// MARK: - TranscriptShareEnvelope

/// What the share-sheet receives when the user taps "Share quote".
/// `shareText` is the full editorial artifact (curly quotes, em-dash
/// attribution, timestamp, deep link) — the deep-link URL is INCLUDED
/// inside `shareText` so plain-text-only recipients still get a tappable
/// link, AND it's exposed separately so the share sheet can offer
/// "Copy Link" as a discrete activity.
struct TranscriptShareEnvelope: Equatable, Sendable {
    let shareText: String
    let deepLinkURL: URL
}
