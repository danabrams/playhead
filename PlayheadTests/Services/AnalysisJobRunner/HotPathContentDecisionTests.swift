// HotPathContentDecisionTests.swift
// playhead-p9yq — the hot-path skip is decided on TRANSCRIBED AUDIO, not rows.
//
// `AnalysisJobRunner.run` used to set `wroteNewChunks = chunks.count >
// existingChunkCount` over RAW `fetchTranscriptChunks` counts. A fast/final
// twin is one utterance in two rows, so a final-pass row landing beside its
// fast twin — same audio, byte-identical text — grew the raw count and forced
// a full hot-path detection re-run over an unchanged transcript. The same raw
// delta fed `chunk_rate_per_sec` in the timeout journal, so a stalled
// transcription that happened to overlap a final pass reported a healthy rate.
// Both now count CANONICAL chunks (fast/final twins collapsed).

import XCTest
@testable import Playhead

final class HotPathContentDecisionTests: XCTestCase {

    private func chunk(
        _ pass: TranscriptPassType,
        _ start: Double,
        _ end: Double,
        _ text: String,
        idx: Int
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "c-\(pass.rawValue)-\(idx)",
            analysisAssetId: "A",
            segmentFingerprint: "fp-\(idx)",
            chunkIndex: idx,
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: text,
            pass: pass.rawValue,
            modelVersion: "test",
            transcriptVersion: pass == .final_ ? "v1" : nil,
            atomOrdinal: pass == .final_ ? idx : nil
        )
    }

    /// The arithmetic the hot-path skip runs on, in the canonicalizer's own
    /// terms. This is the property the decision relies on; if it regresses, a
    /// twin grows the count and the skip re-runs detection on nothing.
    func testAFinalTwinDoesNotGrowTheCanonicalCountButANewSpanDoes() {
        let fast = chunk(.fast, 0, 5, "hello", idx: 0)
        let before = TranscriptChunkCanonicalizer.canonicalize([fast]).chunks.count
        XCTAssertEqual(before, 1, "premise: one fast utterance")

        // A final-pass twin over the SAME audio. The canonicalizer drops the
        // fast row the final union covers, so the canonical count does NOT
        // grow — a raw row count would have grown to 2 and defeated the skip.
        let finalTwin = chunk(.final_, 0, 5, "hello", idx: 0)
        let afterTwin = TranscriptChunkCanonicalizer.canonicalize([fast, finalTwin]).chunks.count
        XCTAssertEqual(
            afterTwin, before,
            "a fast/final twin is one utterance; the canonical count must not grow (playhead-p9yq)"
        )

        // A genuinely new span DOES grow it, so real transcription still
        // triggers a hot-path run.
        let newSpan = chunk(.final_, 5, 10, "world", idx: 1)
        let afterNew = TranscriptChunkCanonicalizer.canonicalize([fast, finalTwin, newSpan]).chunks.count
        XCTAssertGreaterThan(
            afterNew, before,
            "a new span is new transcription; the canonical count must grow so the hot path re-runs"
        )
    }

    /// The wiring rail: the decision reads CANONICAL counts, and the raw-row
    /// comparison that was the bug does not return. Reverting to
    /// `chunks.count > existingChunkCount` fails the second assertion.
    func testHotPathDecisionUsesCanonicalCountNotRawRows() throws {
        let source = SwiftSourceInspector.strippingCommentsAndStrings(
            try SwiftSourceInspector.loadSource(
                repoRelativePath: "Playhead/Services/AnalysisJobRunner/AnalysisJobRunner.swift"
            )
        )

        XCTAssertNotNil(
            source.range(of: "existingCanonicalChunkCount"),
            """
            AnalysisJobRunner no longer holds a canonical existing-chunk count. \
            If the hot-path decision is wired some other way, re-establish what \
            it compares before deleting this canary (playhead-p9yq).
            """
        )

        XCTAssertNil(
            source.range(of: "chunks.count > existingChunkCount"),
            """
            AnalysisJobRunner compares RAW chunk counts again. A fast/final twin \
            adds a row but no audio, so a raw count re-runs the hot path on \
            nothing and inflates chunk_rate — the playhead-p9yq defect. Decide \
            on TranscriptChunkCanonicalizer.canonicalize(...).chunks.count.
            """
        )
    }
}
