// FullTranscriptViewModelSelectionTests.swift
// playhead-m8v7: behaviour tests for the transcript-share selection
// state machine that lives on `FullTranscriptViewModel`.
//
// Selection model:
//   - Resting state: `selectedParagraphIds.isEmpty`. Tap-to-seek
//     (existing 9u0 behaviour) is the default tap action.
//   - Long-press on a paragraph enters selection mode by adding that
//     paragraph's id to `selectedParagraphIds`.
//   - While in selection mode, plain tap on a paragraph TOGGLES the
//     paragraph in/out of the set (does NOT seek). The view's
//     `tappedParagraph(at:)` returns `nil` while selection mode is
//     active so the host knows not to seek.
//   - `clearSelection()` empties the set and exits selection mode.
//   - `isSelectionModeActive` mirrors `!selectedParagraphIds.isEmpty`
//     so the view can branch its tap action.
//   - `shareEnvelope(...)` builds a `(text, deepLinkURL)` payload over
//     the currently-selected paragraphs in document order, scoping
//     the timestamp + deep link to the *first* selected paragraph.

import Foundation
import Testing
@testable import Playhead

private struct StubDS: TranscriptPeekDataSource {
    let snapshot: TranscriptPeekSnapshot
    func fetchSnapshot(assetId: String) async -> TranscriptPeekSnapshot { snapshot }
}

private func makeChunk(
    index: Int,
    start: Double,
    end: Double,
    text: String
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

@Suite("FullTranscriptViewModel — selection state machine and share envelope")
@MainActor
struct FullTranscriptViewModelSelectionTests {

    private func loadedVM() async -> FullTranscriptViewModel {
        // 3 well-separated chunks → 3 paragraphs (each gap > 2s).
        let chunks = [
            makeChunk(index: 0, start: 0,  end: 2,  text: "First paragraph"),
            makeChunk(index: 1, start: 8,  end: 10, text: "Second paragraph"),
            makeChunk(index: 2, start: 16, end: 18, text: "Third paragraph"),
        ]
        let snapshot = TranscriptPeekSnapshot(
            chunks: chunks,
            rawChunkCount: chunks.count,
            adWindows: [],
            decodedSpans: [],
            featureCoverageEnd: nil,
            fastTranscriptCoverageEnd: nil,
            latestSessionState: nil,
            fetchFailed: false
        )
        let vm = FullTranscriptViewModel(
            analysisAssetId: "asset-1",
            dataSource: StubDS(snapshot: snapshot)
        )
        await vm.load()
        return vm
    }

    // MARK: - Resting state

    @Test("Selection mode is inactive after load with nothing selected")
    func defaultRestingState() async {
        let vm = await loadedVM()
        #expect(vm.selectedParagraphIds.isEmpty)
        #expect(vm.isSelectionModeActive == false)
    }

    @Test("tappedParagraph still returns the paragraph startTime when selection mode is inactive")
    func tapStillSeeksWhenInactive() async {
        let vm = await loadedVM()
        // Tap-to-seek is the default action when no paragraph is selected.
        #expect(vm.tappedParagraph(at: 0) == 0)
        #expect(vm.tappedParagraph(at: 1) == 8)
    }

    // MARK: - Long-press enters selection mode

    @Test("longPressedParagraph adds the paragraph id and enters selection mode")
    func longPressEntersSelection() async {
        let vm = await loadedVM()
        vm.longPressedParagraph(at: 1)
        #expect(vm.isSelectionModeActive == true)
        #expect(vm.selectedParagraphIds == [vm.paragraphs[1].id])
    }

    @Test("longPress on an already-selected paragraph leaves it selected")
    func longPressOnSelectedIsIdempotent() async {
        let vm = await loadedVM()
        vm.longPressedParagraph(at: 1)
        vm.longPressedParagraph(at: 1)
        #expect(vm.selectedParagraphIds == [vm.paragraphs[1].id])
    }

    @Test("longPress at out-of-range index is a no-op")
    func longPressOutOfRangeNoOp() async {
        let vm = await loadedVM()
        vm.longPressedParagraph(at: 99)
        #expect(vm.isSelectionModeActive == false)
    }

    // MARK: - Tap toggles in selection mode

    @Test("Tap-while-in-selection-mode toggles a NEW paragraph into the set and returns nil")
    func tapTogglesIntoSetWhileInSelection() async {
        let vm = await loadedVM()
        vm.longPressedParagraph(at: 0)

        // Tap on a different paragraph adds it.
        let result = vm.tappedParagraph(at: 1)
        #expect(result == nil)
        #expect(vm.selectedParagraphIds.contains(vm.paragraphs[0].id))
        #expect(vm.selectedParagraphIds.contains(vm.paragraphs[1].id))
    }

    @Test("Tap-while-in-selection-mode on an ALREADY-selected paragraph removes it")
    func tapRemovesFromSetWhileInSelection() async {
        let vm = await loadedVM()
        vm.longPressedParagraph(at: 0)
        vm.longPressedParagraph(at: 1)

        // Tap on paragraph 1 should remove it.
        _ = vm.tappedParagraph(at: 1)
        #expect(vm.selectedParagraphIds == [vm.paragraphs[0].id])
    }

    @Test("Removing the last selected paragraph exits selection mode")
    func emptyingSelectionExitsMode() async {
        let vm = await loadedVM()
        vm.longPressedParagraph(at: 0)
        _ = vm.tappedParagraph(at: 0)
        #expect(vm.isSelectionModeActive == false)
        #expect(vm.selectedParagraphIds.isEmpty)
    }

    // MARK: - clearSelection

    @Test("clearSelection empties the set and exits selection mode")
    func clearSelection() async {
        let vm = await loadedVM()
        vm.longPressedParagraph(at: 0)
        vm.longPressedParagraph(at: 2)
        #expect(vm.isSelectionModeActive == true)

        vm.clearSelection()
        #expect(vm.selectedParagraphIds.isEmpty)
        #expect(vm.isSelectionModeActive == false)
    }

    // MARK: - Share envelope

    @Test("Share envelope is nil when nothing is selected")
    func shareEnvelopeNilWhenEmpty() async {
        let vm = await loadedVM()
        let envelope = vm.shareEnvelope(
            episodeId: "ep-1",
            showTitle: "Show",
            episodeTitle: "Ep",
            now: Date()
        )
        #expect(envelope == nil)
    }

    @Test("Share envelope emits one quote per selected paragraph in document order")
    func shareEnvelopeOrdering() async {
        let vm = await loadedVM()
        // Select paragraph 2 first, then 0 — the envelope should still
        // emit them in document order (0 before 2).
        vm.longPressedParagraph(at: 2)
        _ = vm.tappedParagraph(at: 0)

        let envelope = vm.shareEnvelope(
            episodeId: "ep-1",
            showTitle: "Show",
            episodeTitle: "Ep",
            now: Date()
        )
        #expect(envelope != nil)
        // First-selected-in-document-order paragraph drives the URL.
        let expectedURL = TranscriptDeepLink.url(
            episodeId: "ep-1",
            startTime: vm.paragraphs[0].startTime
        )
        #expect(envelope?.deepLinkURL == expectedURL)
        // Both quotes should appear in the share text, paragraph[0]
        // first.
        let text = envelope?.shareText ?? ""
        let firstRange = text.range(of: vm.paragraphs[0].text)
        let secondRange = text.range(of: vm.paragraphs[2].text)
        #expect(firstRange != nil)
        #expect(secondRange != nil)
        if let f = firstRange, let s = secondRange {
            #expect(f.lowerBound < s.lowerBound)
        }
    }

    @Test("Share envelope timestamp targets the FIRST selected paragraph")
    func shareEnvelopeTimestampUsesFirstSelected() async {
        let vm = await loadedVM()
        vm.longPressedParagraph(at: 1)  // startTime = 8

        let envelope = vm.shareEnvelope(
            episodeId: "ep-1",
            showTitle: "Show",
            episodeTitle: "Ep",
            now: Date()
        )
        #expect(envelope?.deepLinkURL.absoluteString == "playhead://episode/ep-1?t=8")
    }
}
