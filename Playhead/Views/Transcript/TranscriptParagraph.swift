// TranscriptParagraph.swift
// Display-level paragraph type used by FullTranscriptView. A paragraph
// is a run of consecutive `TranscriptChunk`s that share the same ad
// status and are not separated by a long pause; the FullTranscriptView
// renders one paragraph per `LazyVStack` row rather than one chunk per
// row, so the editorial reading flow doesn't get chopped into one-line
// fragments.
//
// The grouping rules are pure logic — see `TranscriptParagraphGrouper`
// — so the FullTranscriptViewModel can build paragraphs from a fetched
// snapshot without any SwiftUI dependency. That keeps the bulk of the
// view logic unit-testable.

import Foundation

// MARK: - TranscriptParagraph

/// A consecutive run of `TranscriptChunk`s that render as one block of
/// editorial prose in the full transcript view. Identifier is the first
/// chunk's `segmentFingerprint` so SwiftUI's `ForEach` / `ScrollViewReader`
/// have a stable scroll target.
struct TranscriptParagraph: Identifiable, Sendable {

    /// Stable identifier — the first chunk's `segmentFingerprint`. Every
    /// chunk's fingerprint is unique within an asset, so picking the
    /// first one keeps paragraph identity stable across snapshot
    /// refreshes (a paragraph that "grows" by adding a trailing chunk
    /// keeps the same id and SwiftUI doesn't recreate the row).
    let id: String

    /// Underlying chunks in this paragraph, sorted by `startTime`.
    let chunks: [TranscriptChunk]

    /// First chunk's `startTime`.
    let startTime: TimeInterval

    /// Last chunk's `endTime`.
    let endTime: TimeInterval

    /// True when any chunk in this paragraph overlaps an `AdWindow`.
    /// All chunks in a paragraph share the same `isAd` value because
    /// `TranscriptParagraphGrouper` splits paragraphs at ad-status
    /// transitions.
    let isAd: Bool

    /// Concatenated text — chunk texts joined with a single space.
    /// Used by the in-episode search path so we run one `range(of:)`
    /// per paragraph rather than per-chunk.
    let text: String
}

// MARK: - TranscriptParagraphGrouper

/// Pure grouping logic. Takes segment-level chunks plus the persisted
/// ad windows and produces display-level paragraphs.
///
/// Boundary rules (bead spec):
///   1. Consecutive non-ad chunks coalesce into one paragraph.
///   2. A new paragraph starts when the gap between chunk N's
///      `endTime` and chunk N+1's `startTime` exceeds 2.0 seconds.
///   3. A new paragraph starts when the ad-overlap status flips
///      (non-ad → ad or ad → non-ad).
///
/// The grouper intentionally does not throw or do any I/O; tests pass
/// in synthetic chunks and exercise the full state space.
enum TranscriptParagraphGrouper {

    /// Maximum gap between chunks that keeps them in the same paragraph.
    /// Anything larger triggers a paragraph break (bead rule #2).
    static let maxIntraParagraphGap: TimeInterval = 2.0

    /// Group `chunks` (sorted by `startTime`) into paragraphs. Empty
    /// input yields an empty result.
    static func group(
        chunks: [TranscriptChunk],
        adWindows: [AdWindow]
    ) -> [TranscriptParagraph] {
        guard !chunks.isEmpty else { return [] }

        let sorted = chunks.sorted { $0.startTime < $1.startTime }

        var paragraphs: [TranscriptParagraph] = []
        var currentRun: [TranscriptChunk] = []
        var currentIsAd: Bool = false

        func flush() {
            guard let first = currentRun.first, let last = currentRun.last else {
                return
            }
            paragraphs.append(
                TranscriptParagraph(
                    id: first.segmentFingerprint,
                    chunks: currentRun,
                    startTime: first.startTime,
                    endTime: last.endTime,
                    isAd: currentIsAd,
                    text: currentRun.map(\.text).joined(separator: " ")
                )
            )
            currentRun = []
        }

        for chunk in sorted {
            let chunkIsAd = chunkOverlapsAd(chunk: chunk, adWindows: adWindows)

            if currentRun.isEmpty {
                currentRun = [chunk]
                currentIsAd = chunkIsAd
                continue
            }

            // Detect a paragraph break against the current run's last chunk.
            let lastChunk = currentRun[currentRun.count - 1]
            let gap = chunk.startTime - lastChunk.endTime
            let gapTooLong = gap > maxIntraParagraphGap
            let adFlipped = chunkIsAd != currentIsAd

            if gapTooLong || adFlipped {
                flush()
                currentRun = [chunk]
                currentIsAd = chunkIsAd
            } else {
                currentRun.append(chunk)
            }
        }
        flush()

        return paragraphs
    }

    /// True when `chunk` overlaps any window in `adWindows`.
    /// Half-open overlap matches the convention used by the live peek
    /// view model (`ad.startTime < chunk.endTime && ad.endTime > chunk.startTime`).
    private static func chunkOverlapsAd(
        chunk: TranscriptChunk,
        adWindows: [AdWindow]
    ) -> Bool {
        adWindows.contains { ad in
            ad.startTime < chunk.endTime && ad.endTime > chunk.startTime
        }
    }
}
