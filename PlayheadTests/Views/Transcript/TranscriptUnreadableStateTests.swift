// TranscriptUnreadableStateTests.swift
// playhead-0bpb0 — "it said the transcript could not be found", on an episode
// whose 2,219 chunks were sitting on disk.
//
// The store fix (`TranscriptReadFailureTests`) turns a failed transcript read
// into a thrown error instead of an empty array. That alone does not help the
// listener: `TranscriptPeekDataSource` catches, and BOTH view-models used to
// render a reported failure exactly the way they render an episode nothing has
// been transcribed for yet. This suite pins the difference.
//
// The two sentences point opposite ways. "No transcript yet" means WAIT — the
// analysis has not got there. The unreadable state means the rows exist and the
// read did not go through, which on the peek resolves itself on the next
// two-second poll and on the full view resolves on a tap. Telling a listener to
// wait for work that finished, while they are trying to mark an ad they can
// hear, removes the only repair path they have at that moment.

import Foundation
import Testing
@testable import Playhead

// MARK: - Doubles

/// Returns a failing snapshot for the first `failures` fetches, then a good
/// one. `failNext` lets a test flip an ALREADY-LOADED view-model into failure,
/// which is the case the retention claims are about.
///
/// `@unchecked Sendable` with plain mutable state is safe here because every
/// test in this file drives it from the main actor and awaits each call before
/// making the next.
private final class FlakyTranscriptDataSource: TranscriptPeekDataSource, @unchecked Sendable {
    private let good: TranscriptPeekSnapshot
    private var remainingFailures: Int
    var failNext = false
    private(set) var fetchCount = 0

    init(good: TranscriptPeekSnapshot, failures: Int) {
        self.good = good
        self.remainingFailures = failures
    }

    func fetchSnapshot(assetId: String) async -> TranscriptPeekSnapshot {
        fetchCount += 1
        if failNext || remainingFailures > 0 {
            if remainingFailures > 0 { remainingFailures -= 1 }
            return TranscriptPeekSnapshot(
                chunks: [],
                rawChunkCount: 0,
                adWindows: [],
                decodedSpans: [],
                featureCoverageEnd: nil,
                fastTranscriptCoverageEnd: nil,
                latestSessionState: nil,
                fetchFailed: true
            )
        }
        return good
    }
}

private func fixtureChunks(_ count: Int) -> [TranscriptChunk] {
    (0..<count).map { index in
        TranscriptChunk(
            id: "u-\(index)",
            analysisAssetId: "asset-0bpb0",
            segmentFingerprint: "fp-\(index)",
            chunkIndex: index,
            startTime: Double(index) * 5,
            endTime: Double(index) * 5 + 5,
            text: "body \(index)",
            normalizedText: "body \(index)",
            pass: "fast",
            modelVersion: "test",
            transcriptVersion: "v1",
            atomOrdinal: index
        )
    }
}

private func goodSnapshot(_ count: Int) -> TranscriptPeekSnapshot {
    let chunks = fixtureChunks(count)
    return TranscriptPeekSnapshot(
        chunks: chunks,
        rawChunkCount: chunks.count,
        adWindows: [],
        decodedSpans: [],
        featureCoverageEnd: nil,
        fastTranscriptCoverageEnd: nil,
        latestSessionState: nil,
        fetchFailed: false
    )
}

private func emptyButHealthySnapshot() -> TranscriptPeekSnapshot {
    TranscriptPeekSnapshot(
        chunks: [],
        rawChunkCount: 0,
        adWindows: [],
        decodedSpans: [],
        featureCoverageEnd: nil,
        fastTranscriptCoverageEnd: nil,
        latestSessionState: nil,
        fetchFailed: false
    )
}

// MARK: - Suite

@Suite("playhead-0bpb0 — a failed read and an empty episode must not look alike")
@MainActor
struct TranscriptUnreadableStateTests {

    // MARK: The peek

    @Test("peek: a failed FIRST load is marked unreadable, not empty")
    func peekFirstLoadFailure() async {
        let source = FlakyTranscriptDataSource(good: goodSnapshot(40), failures: 1)
        let vm = TranscriptPeekViewModel(analysisAssetId: "asset-0bpb0", dataSource: source)

        await vm.refresh()

        #expect(vm.chunks.isEmpty, "premise: the failed fetch carried no rows")
        #expect(
            vm.lastLoadFailed,
            """
            The view-model reported a failed first load as an ordinary empty episode. That is \
            the field defect: the peek renders `chunks.isEmpty` as "No transcript yet", so a \
            read that did not go through told Dan his transcript did not exist.
            """
        )
    }

    @Test("peek: an episode with nothing transcribed yet is NOT marked unreadable")
    func peekHealthyEmptyIsNotAFailure() async {
        // The mirror claim. Without it, `lastLoadFailed = true` unconditionally
        // would pass the test above and mislabel every genuinely empty episode.
        let source = FlakyTranscriptDataSource(good: emptyButHealthySnapshot(), failures: 0)
        let vm = TranscriptPeekViewModel(analysisAssetId: "asset-0bpb0", dataSource: source)

        await vm.refresh()

        #expect(vm.chunks.isEmpty)
        #expect(!vm.lastLoadFailed, "an empty episode read successfully is not a failed read")
    }

    @Test("peek: a later failure keeps the rows AND clears once the read recovers")
    func peekRecovers() async {
        let source = FlakyTranscriptDataSource(good: goodSnapshot(40), failures: 0)
        let vm = TranscriptPeekViewModel(analysisAssetId: "asset-0bpb0", dataSource: source)
        await vm.refresh()
        #expect(vm.chunks.count == 40, "premise: a good load first")

        // A failure ON TOP of rows we already hold must not blank the sheet.
        // The listener may be mid-selection; the rows are still true.
        source.failNext = true
        await vm.refresh()
        #expect(vm.chunks.count == 40, "a poll failure wiped rows the listener was reading")
        #expect(vm.lastLoadFailed)

        source.failNext = false
        await vm.refresh()
        #expect(!vm.lastLoadFailed, "the flag must clear, or one bad poll marks the sheet forever")
        #expect(vm.chunks.count == 40)
    }

    // MARK: The full transcript

    @Test("full: a failed load does not WIPE paragraphs it already had")
    func fullLoadFailureRetainsParagraphs() async {
        let flaky = FlakyTranscriptDataSource(good: goodSnapshot(40), failures: 0)
        let vm = FullTranscriptViewModel(analysisAssetId: "asset-0bpb0", dataSource: flaky)
        await vm.load()
        let loaded = vm.paragraphs.count
        #expect(loaded > 0, "premise: paragraphs were grouped from the good snapshot")

        // The load path used to GROUP the failed snapshot — an empty chunk
        // list in, an empty paragraph list out — and publish the result. So a
        // transient error both blanked a loaded transcript and rendered as
        // "No transcript yet".
        flaky.failNext = true
        await vm.load()
        #expect(vm.paragraphs.count == loaded, "a failed load erased a transcript that was on screen")
        #expect(vm.loadFailed)
        #expect(!vm.isLoading, "a failed load still ENDS — the spinner must not run forever")

        flaky.failNext = false
        await vm.load()
        #expect(!vm.loadFailed)
        #expect(vm.paragraphs.count == loaded)
    }

    @Test("full: a failed FIRST load says unreadable rather than empty")
    func fullFirstLoadFailure() async {
        let failing = FlakyTranscriptDataSource(good: goodSnapshot(40), failures: 1)
        let vm = FullTranscriptViewModel(analysisAssetId: "asset-0bpb0", dataSource: failing)

        await vm.load()

        #expect(vm.paragraphs.isEmpty, "premise: nothing to show")
        #expect(vm.loadFailed, "with no rows to retain, the flag is the ONLY thing separating the two screens")
        #expect(!vm.isLoading)
    }

    @Test("full: an empty episode read successfully is not a failure")
    func fullHealthyEmptyIsNotAFailure() async {
        let source = FlakyTranscriptDataSource(good: emptyButHealthySnapshot(), failures: 0)
        let vm = FullTranscriptViewModel(analysisAssetId: "asset-0bpb0", dataSource: source)

        await vm.load()

        #expect(vm.paragraphs.isEmpty)
        #expect(!vm.loadFailed)
        #expect(!vm.isLoading)
    }
}
