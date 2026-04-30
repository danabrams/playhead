// FullTranscriptViewModelTests.swift
// Behaviour tests for the full transcript view-model. The view-model
// owns:
//   - paragraphs (grouped from a fetched TranscriptPeekSnapshot)
//   - activeParagraphIndex driven by playback time updates
//   - scroll state machine (autoScrolling / userScrolling / userScrolled)
//   - in-episode search state (query, matches, current match index)
//   - tap-to-seek (returns the seek time for a paragraph)
//
// The view-model is the unit-test seam; the SwiftUI view consumes the
// view-model's @Observable state but holds none of its own
// state-machine logic.

import Foundation
import Testing
@testable import Playhead

// MARK: - StubTranscriptPeekDataSource

/// Test double that returns a fixed `TranscriptPeekSnapshot`. Lets the
/// view-model's load path run synchronously without standing up a
/// SQLite-backed `LiveTranscriptPeekDataSource`.
private struct StubTranscriptPeekDataSource: TranscriptPeekDataSource {
    let snapshot: TranscriptPeekSnapshot

    func fetchSnapshot(assetId: String) async -> TranscriptPeekSnapshot {
        snapshot
    }
}

// MARK: - Fixture helpers

private func chunk(
    index: Int,
    start: Double,
    end: Double,
    text: String = "lorem ipsum dolor"
) -> TranscriptChunk {
    TranscriptChunk(
        id: "asset-1-\(index)",
        analysisAssetId: "asset-1",
        segmentFingerprint: "fp-\(index)",
        chunkIndex: index,
        startTime: start,
        endTime: end,
        text: text,
        normalizedText: text,
        pass: "final",
        modelVersion: "test",
        transcriptVersion: "v1",
        atomOrdinal: index
    )
}

private func snapshot(chunks: [TranscriptChunk], adWindows: [AdWindow] = []) -> TranscriptPeekSnapshot {
    TranscriptPeekSnapshot(
        chunks: chunks,
        rawChunkCount: chunks.count,
        adWindows: adWindows,
        decodedSpans: [],
        featureCoverageEnd: nil,
        fastTranscriptCoverageEnd: nil,
        latestSessionState: nil,
        fetchFailed: false
    )
}

// MARK: - Suite

@Suite("FullTranscriptViewModel — load, active paragraph, scroll state, search")
@MainActor
struct FullTranscriptViewModelTests {

    // MARK: Loading

    @Test("Empty snapshot leaves paragraphs empty and isLoading false after load")
    func emptySnapshotLoad() async {
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: []))
        )
        #expect(vm.isLoading == true)
        await vm.load()
        #expect(vm.isLoading == false)
        #expect(vm.paragraphs.isEmpty)
    }

    @Test("Snapshot with chunks produces grouped paragraphs")
    func snapshotProducesParagraphs() async {
        let chunks = [
            chunk(index: 0, start: 0, end: 2),
            chunk(index: 1, start: 2.0, end: 4),
            // Long pause splits.
            chunk(index: 2, start: 8, end: 10),
        ]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()
        #expect(vm.paragraphs.count == 2)
    }

    // MARK: Active paragraph

    @Test("updatePlaybackPosition selects the paragraph containing the time")
    func activeParagraphTracksTime() async {
        let chunks = [
            chunk(index: 0, start: 0, end: 4),
            chunk(index: 1, start: 4, end: 8),
            // Pause > 2s splits → paragraph 1.
            chunk(index: 2, start: 12, end: 16),
        ]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()
        #expect(vm.paragraphs.count == 2)

        vm.updatePlaybackPosition(1.0)
        #expect(vm.activeParagraphIndex == 0)

        vm.updatePlaybackPosition(13.5)
        #expect(vm.activeParagraphIndex == 1)
    }

    @Test("activeParagraphIndex is nil before load")
    func activeIsNilBeforeLoad() {
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: []))
        )
        vm.updatePlaybackPosition(5.0)
        #expect(vm.activeParagraphIndex == nil)
    }

    @Test("Time before first paragraph clamps to first")
    func timeBeforeFirstClampsToFirst() async {
        let chunks = [chunk(index: 0, start: 5, end: 10)]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()
        vm.updatePlaybackPosition(2.0) // before first paragraph starts
        #expect(vm.activeParagraphIndex == 0)
    }

    @Test("Time past the last paragraph stays at last")
    func timePastLastStaysAtLast() async {
        let chunks = [
            chunk(index: 0, start: 0, end: 4),
            chunk(index: 1, start: 4, end: 8),
        ]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()
        vm.updatePlaybackPosition(100)
        #expect(vm.activeParagraphIndex == 0) // there's only one paragraph (no split)
    }

    // MARK: Scroll state machine

    @Test("Scroll state starts in autoScrolling")
    func defaultScrollState() {
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: []))
        )
        #expect(vm.scrollState == .autoScrolling)
    }

    @Test("userBeganScrolling moves state to userScrolling")
    func userBeganScrolling() {
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: []))
        )
        vm.userBeganScrolling()
        #expect(vm.scrollState == .userScrolling)
    }

    @Test("userEndedScrolling after dragging moves to userScrolled")
    func userEndedScrolling() {
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: []))
        )
        vm.userBeganScrolling()
        vm.userEndedScrolling()
        #expect(vm.scrollState == .userScrolled)
    }

    @Test("jumpToNow returns to autoScrolling and yields the active paragraph id")
    func jumpToNow() async {
        let chunks = [
            chunk(index: 0, start: 0, end: 4),
            chunk(index: 1, start: 8, end: 12),
        ]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()
        vm.updatePlaybackPosition(10)
        vm.userBeganScrolling()
        vm.userEndedScrolling()
        #expect(vm.scrollState == .userScrolled)

        let target = vm.jumpToNow()
        #expect(vm.scrollState == .autoScrolling)
        // The second paragraph is active at t=10
        #expect(target == vm.paragraphs[1].id)
    }

    @Test("Auto-scroll target while autoScrolling tracks the active paragraph")
    func autoScrollTargetWhenActive() async {
        let chunks = [
            chunk(index: 0, start: 0, end: 4),
            chunk(index: 1, start: 8, end: 12),
        ]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()
        vm.updatePlaybackPosition(2)
        #expect(vm.autoScrollTarget == vm.paragraphs[0].id)
    }

    @Test("Auto-scroll target is nil while user is interacting")
    func autoScrollSuppressedWhileUserScrolling() async {
        let chunks = [
            chunk(index: 0, start: 0, end: 4),
            chunk(index: 1, start: 8, end: 12),
        ]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()
        vm.updatePlaybackPosition(2)
        vm.userBeganScrolling()
        #expect(vm.autoScrollTarget == nil)

        vm.userEndedScrolling()
        // userScrolled also suppresses auto-scroll until jumpToNow is called.
        #expect(vm.autoScrollTarget == nil)
    }

    // MARK: Tap-to-seek

    @Test("tappedParagraph returns the paragraph's startTime as the seek target")
    func tapToSeek() async {
        let chunks = [
            chunk(index: 0, start: 0, end: 4),
            chunk(index: 1, start: 4, end: 8),
            chunk(index: 2, start: 12, end: 16),
        ]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()
        // chunk 2 forms paragraph 1 (split at the 4s gap).
        let target = vm.tappedParagraph(at: 1)
        #expect(target == 12)
    }

    @Test("tappedParagraph at out-of-range index returns nil")
    func tapToSeekOutOfRange() async {
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: []))
        )
        await vm.load()
        #expect(vm.tappedParagraph(at: 5) == nil)
    }

    // MARK: Search

    @Test("Search finds case-insensitive substring matches")
    func searchCaseInsensitive() async {
        // Each chunk pair is ~3s apart so the long-pause rule splits them
        // into separate paragraphs (testing that search walks paragraphs).
        let chunks = [
            chunk(index: 0, start: 0, end: 2, text: "Welcome to the show today"),
            chunk(index: 1, start: 6, end: 8, text: "Our sponsor is BetterHelp"),
            chunk(index: 2, start: 14, end: 16, text: "Back to the interview"),
        ]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()
        #expect(vm.paragraphs.count == 3)

        vm.searchQuery = "the"
        // "the show today" and "the interview" both match.
        #expect(vm.matchingParagraphIndices == [0, 2])
        #expect(vm.currentMatchPosition == 0)
        #expect(vm.matchCountLabel == "1 of 2")
    }

    @Test("Empty search clears match state")
    func emptyQueryClearsMatches() async {
        let chunks = [
            chunk(index: 0, start: 0, end: 2, text: "alpha"),
            chunk(index: 1, start: 4, end: 6, text: "beta"),
        ]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()

        vm.searchQuery = "alpha"
        #expect(!vm.matchingParagraphIndices.isEmpty)

        vm.searchQuery = ""
        #expect(vm.matchingParagraphIndices.isEmpty)
        #expect(vm.currentMatchPosition == nil)
        #expect(vm.matchCountLabel == "")
    }

    @Test("nextMatch advances and wraps")
    func nextMatchWraps() async {
        // 3 separate paragraphs, all containing the word "the".
        let chunks = [
            chunk(index: 0, start: 0, end: 2, text: "the alpha"),
            chunk(index: 1, start: 6, end: 8, text: "the beta"),
            chunk(index: 2, start: 14, end: 16, text: "the gamma"),
        ]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()
        vm.searchQuery = "the"
        #expect(vm.matchingParagraphIndices == [0, 1, 2])
        #expect(vm.currentMatchPosition == 0)

        let target1 = vm.nextMatch()
        #expect(vm.currentMatchPosition == 1)
        #expect(target1 == vm.paragraphs[1].id)

        _ = vm.nextMatch()
        #expect(vm.currentMatchPosition == 2)

        let target3 = vm.nextMatch()
        #expect(vm.currentMatchPosition == 0)
        #expect(target3 == vm.paragraphs[0].id)
    }

    @Test("previousMatch wraps from first to last")
    func previousMatchWraps() async {
        let chunks = [
            chunk(index: 0, start: 0, end: 2, text: "the alpha"),
            chunk(index: 1, start: 6, end: 8, text: "the beta"),
        ]
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(snapshot: snapshot(chunks: chunks))
        )
        await vm.load()
        vm.searchQuery = "the"

        // Position 0 -> previous wraps to last (position 1).
        let target = vm.previousMatch()
        #expect(vm.currentMatchPosition == 1)
        #expect(target == vm.paragraphs[1].id)
    }

    @Test("nextMatch with no matches is a no-op and returns nil")
    func nextMatchNoMatches() async {
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubTranscriptPeekDataSource(
                snapshot: snapshot(chunks: [chunk(index: 0, start: 0, end: 2, text: "alpha")])
            )
        )
        await vm.load()
        vm.searchQuery = "missing"
        #expect(vm.matchingParagraphIndices.isEmpty)
        #expect(vm.nextMatch() == nil)
        #expect(vm.previousMatch() == nil)
    }
}
