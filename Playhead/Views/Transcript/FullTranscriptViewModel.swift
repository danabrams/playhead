// FullTranscriptViewModel.swift
// Drives the FullTranscriptView. Owns the loaded paragraph list, the
// active-paragraph index for highlighting + auto-scroll, the scroll
// state machine (autoScrolling / userScrolling / userScrolled), and
// the in-episode search state.
//
// playhead-9u0:
//   - Sources data via the existing `TranscriptPeekDataSource`
//     boundary so this view-model never imports `AnalysisStore` directly
//     (the SurfaceStatusUILintTests sweep forbids that on UI files).
//   - All state-machine logic is unit-testable: the view-model has no
//     SwiftUI dependency. The corresponding `FullTranscriptView` is the
//     only piece that needs visual / snapshot verification.

import Foundation
import OSLog

// MARK: - Scroll state

/// State machine for whether the transcript auto-follows playback or
/// gives the user manual control. Driven by `userBeganScrolling()` /
/// `userEndedScrolling()` / `jumpToNow()`. `autoScrolling` is the
/// resting state.
enum TranscriptScrollState: Equatable, Sendable {
    /// Transcript follows playback — `autoScrollTarget` returns the
    /// active paragraph's id on each playback update.
    case autoScrolling

    /// User is dragging the scroll view. Auto-scroll suppressed while
    /// the gesture is in progress.
    case userScrolling

    /// User finished a drag. The "Jump to now" affordance is visible;
    /// `jumpToNow()` returns to `autoScrolling` and emits the target.
    case userScrolled
}

// MARK: - FullTranscriptViewModel

@MainActor
@Observable
final class FullTranscriptViewModel {

    // MARK: State

    /// Display-level paragraphs grouped from the fetched chunks. Empty
    /// while loading, after a failed fetch, or for episodes with no
    /// transcript yet.
    private(set) var paragraphs: [TranscriptParagraph] = []

    /// True while the initial snapshot is being fetched. Flips to false
    /// after the first `load()` returns.
    private(set) var isLoading: Bool = true

    /// Index of the paragraph that contains (or precedes) the current
    /// playback time. `nil` when no paragraphs are loaded.
    private(set) var activeParagraphIndex: Int?

    /// Current scroll state machine value. See `TranscriptScrollState`.
    private(set) var scrollState: TranscriptScrollState = .autoScrolling

    /// Search query the user has entered. The empty string disables
    /// search (no matches, no highlights).
    var searchQuery: String = "" {
        didSet { recomputeSearch() }
    }

    /// Indices of paragraphs that contain at least one match for the
    /// current `searchQuery`. Empty when there are no matches or the
    /// query is empty.
    private(set) var matchingParagraphIndices: [Int] = []

    /// Index into `matchingParagraphIndices` of the currently-active
    /// match (the one the up/down nav cycles over). `nil` when there
    /// are no matches.
    private(set) var currentMatchPosition: Int?

    /// playhead-m8v7: ids of paragraphs the user has selected for a
    /// share-quote. Empty when selection mode is inactive. `Set` rather
    /// than `Array` because selection cardinality is order-independent
    /// (the share envelope re-orders into document order from
    /// `paragraphs`).
    private(set) var selectedParagraphIds: Set<String> = []

    /// playhead-m8v7: convenience derived from `selectedParagraphIds`.
    /// Read by the view to branch the tap action between
    /// "tap-to-seek" (default) and "tap-to-toggle" (selection mode).
    var isSelectionModeActive: Bool { !selectedParagraphIds.isEmpty }

    // MARK: Configuration

    let analysisAssetId: String
    private let dataSource: TranscriptPeekDataSource
    private let logger = Logger(subsystem: "com.playhead", category: "FullTranscriptVM")

    // MARK: Init

    init(analysisAssetId: String, dataSource: TranscriptPeekDataSource) {
        self.analysisAssetId = analysisAssetId
        self.dataSource = dataSource
    }

    // MARK: Loading

    /// Fetch a snapshot from the data source and rebuild paragraphs.
    /// Idempotent — callers can re-invoke to refresh.
    func load() async {
        let snapshot = await dataSource.fetchSnapshot(assetId: analysisAssetId)
        if snapshot.fetchFailed {
            logger.error("FullTranscriptVM: snapshot fetch failed for \(self.analysisAssetId)")
        }
        let grouped = TranscriptParagraphGrouper.group(
            chunks: snapshot.chunks,
            adWindows: snapshot.adWindows
        )
        paragraphs = grouped
        isLoading = false
        // Rebuild search results against the new paragraph list.
        recomputeSearch()
    }

    // MARK: Playback position

    /// Update which paragraph is currently active based on a playback
    /// time in seconds. Called from the view's `onChange(of: currentTime)`.
    /// Coalescing of sub-second updates is the caller's responsibility
    /// (mirrors the peek view).
    func updatePlaybackPosition(_ currentTime: TimeInterval) {
        guard !paragraphs.isEmpty else {
            activeParagraphIndex = nil
            return
        }

        // Walk the sorted paragraphs and pick the last one whose
        // startTime is <= currentTime. Falls back to the first
        // paragraph when the time is before the entire transcript.
        var best: Int = 0
        for (index, paragraph) in paragraphs.enumerated() {
            if paragraph.startTime <= currentTime {
                best = index
            } else {
                break
            }
        }
        activeParagraphIndex = best
    }

    // MARK: Scroll state machine

    /// Called by the view when a drag gesture begins on the transcript
    /// scroll view. Suppresses auto-scroll for the rest of the
    /// interaction.
    func userBeganScrolling() {
        scrollState = .userScrolling
    }

    /// Called when the drag gesture ends. Transitions to
    /// `userScrolled`, which keeps auto-scroll suppressed and tells
    /// the view to surface the "Jump to now" affordance.
    func userEndedScrolling() {
        scrollState = .userScrolled
    }

    /// Re-engage auto-scroll. Returns the active paragraph id (or nil
    /// when no paragraph is active) so the view can scroll back to it
    /// in one motion.
    @discardableResult
    func jumpToNow() -> String? {
        scrollState = .autoScrolling
        guard let idx = activeParagraphIndex,
              idx >= 0, idx < paragraphs.count
        else {
            return nil
        }
        return paragraphs[idx].id
    }

    /// The paragraph id the view should scroll to on each playback
    /// update. Returns `nil` while the user is interacting (any state
    /// other than `autoScrolling`).
    var autoScrollTarget: String? {
        guard scrollState == .autoScrolling else { return nil }
        guard let idx = activeParagraphIndex,
              idx >= 0, idx < paragraphs.count
        else {
            return nil
        }
        return paragraphs[idx].id
    }

    // MARK: Tap-to-seek

    /// Returns the seek target (in seconds) for the paragraph at the
    /// supplied index, or `nil` when the index is out of range OR
    /// when selection mode is active (in which case the tap toggles
    /// the paragraph in/out of `selectedParagraphIds` instead). The
    /// view should hand a non-nil result to the playback service and
    /// otherwise treat the tap as a selection toggle that the
    /// view-model has already applied.
    func tappedParagraph(at index: Int) -> TimeInterval? {
        guard index >= 0, index < paragraphs.count else { return nil }

        // playhead-m8v7: while selection mode is active, the tap is a
        // selection toggle — it does NOT seek. Apply the toggle here
        // and return nil so the host knows to skip the seek.
        if isSelectionModeActive {
            let id = paragraphs[index].id
            if selectedParagraphIds.contains(id) {
                selectedParagraphIds.remove(id)
            } else {
                selectedParagraphIds.insert(id)
            }
            return nil
        }
        return paragraphs[index].startTime
    }

    // MARK: Selection (playhead-m8v7)

    /// Long-press is the entry point into selection mode: it adds the
    /// paragraph at `index` to `selectedParagraphIds`. Idempotent —
    /// long-pressing an already-selected paragraph leaves it selected.
    /// Out-of-range indices are silent no-ops.
    func longPressedParagraph(at index: Int) {
        guard index >= 0, index < paragraphs.count else { return }
        selectedParagraphIds.insert(paragraphs[index].id)
    }

    /// Empty the selection set and exit selection mode. Called from
    /// the view's "Done" button or after a successful share.
    func clearSelection() {
        selectedParagraphIds.removeAll()
    }

    /// Build a `(text, deepLinkURL)` envelope for the currently-
    /// selected paragraphs. Returns `nil` when the set is empty.
    /// Paragraphs are emitted in document order; the URL points at
    /// the FIRST selected paragraph's `startTime` (the natural
    /// "where to land me" target).
    ///
    /// `now` is unused today but accepted so a future revision can
    /// stamp share-time provenance without churning the call sites.
    func shareEnvelope(
        episodeId: String,
        showTitle: String,
        episodeTitle: String,
        now: Date
    ) -> TranscriptShareEnvelope? {
        guard !selectedParagraphIds.isEmpty else { return nil }

        // Walk paragraphs in document order; pick the ones whose ids
        // are in `selectedParagraphIds`. Cheap because paragraph counts
        // are O(hundreds), not O(thousands).
        let ordered = paragraphs.filter { selectedParagraphIds.contains($0.id) }
        guard let first = ordered.first else { return nil }

        let url = TranscriptDeepLink.url(
            episodeId: episodeId,
            startTime: first.startTime
        )
        let text = QuoteFormatter.format(
            quotes: ordered.map(\.text),
            showTitle: showTitle,
            episodeTitle: episodeTitle,
            startTime: first.startTime,
            deepLinkURL: url
        )
        _ = now  // currently unused; see docstring.
        return TranscriptShareEnvelope(shareText: text, deepLinkURL: url)
    }

    // MARK: In-episode search

    /// Recompute `matchingParagraphIndices` and `currentMatchPosition`
    /// for the current `searchQuery`. Case-insensitive substring match
    /// against each paragraph's joined text.
    private func recomputeSearch() {
        let query = searchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else {
            matchingParagraphIndices = []
            currentMatchPosition = nil
            return
        }
        var hits: [Int] = []
        for (index, paragraph) in paragraphs.enumerated() {
            if paragraph.text.range(of: query, options: .caseInsensitive) != nil {
                hits.append(index)
            }
        }
        matchingParagraphIndices = hits
        currentMatchPosition = hits.isEmpty ? nil : 0
    }

    /// Advance to the next match in `matchingParagraphIndices`. Wraps
    /// to the first match after the last. No-op when there are no
    /// matches. Returns the paragraph id the view should scroll to, or
    /// `nil` when there is nothing to scroll to.
    @discardableResult
    func nextMatch() -> String? {
        guard !matchingParagraphIndices.isEmpty else { return nil }
        let position = currentMatchPosition ?? -1
        let next = (position + 1) % matchingParagraphIndices.count
        currentMatchPosition = next
        let paragraphIndex = matchingParagraphIndices[next]
        return paragraphs[paragraphIndex].id
    }

    /// Step back to the previous match. Wraps from the first to the
    /// last. No-op when there are no matches.
    @discardableResult
    func previousMatch() -> String? {
        guard !matchingParagraphIndices.isEmpty else { return nil }
        let position = currentMatchPosition ?? 0
        let count = matchingParagraphIndices.count
        let prev = (position - 1 + count) % count
        currentMatchPosition = prev
        let paragraphIndex = matchingParagraphIndices[prev]
        return paragraphs[paragraphIndex].id
    }

    /// Human-readable "X of Y" label for the current match position.
    /// Empty string when the query is empty or there are no matches.
    var matchCountLabel: String {
        guard !matchingParagraphIndices.isEmpty,
              let position = currentMatchPosition
        else {
            return ""
        }
        return "\(position + 1) of \(matchingParagraphIndices.count)"
    }
}
