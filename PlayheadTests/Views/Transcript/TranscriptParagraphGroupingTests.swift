// TranscriptParagraphGroupingTests.swift
// Pure-function tests for paragraph grouping — the algorithm that takes
// segment-level TranscriptChunks plus the ad-window time ranges and
// produces display-level TranscriptParagraphs.
//
// Grouping rules from the bead spec:
//   1. Consecutive non-ad chunks coalesce into one paragraph.
//   2. A new paragraph starts when the gap between chunk N.endTime and
//      chunk N+1.startTime is greater than 2.0 seconds.
//   3. A new paragraph starts when the ad-overlap status flips
//      (non-ad → ad or ad → non-ad).
//
// These rules are exercised here without instantiating the SwiftUI view
// or the live data source — the grouper is a pure free function so tests
// can call it with synthetic chunks and AdWindow instances.

import Foundation
import Testing
@testable import Playhead

@Suite("TranscriptParagraphGrouping — paragraph boundary rules")
struct TranscriptParagraphGroupingTests {

    // MARK: - Fixture helpers

    private func chunk(
        index: Int,
        start: Double,
        end: Double,
        text: String = "lorem ipsum"
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

    private func adWindow(start: Double, end: Double) -> AdWindow {
        AdWindow(
            id: "ad-\(start)-\(end)",
            analysisAssetId: "asset-1",
            startTime: start,
            endTime: end,
            confidence: 0.9,
            boundaryState: "snapped",
            decisionState: "decided",
            detectorVersion: "v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "synthetic",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: nil,
            catalogStoreMatchSimilarity: nil
        )
    }

    // MARK: - Tests

    @Test("Empty chunks yields no paragraphs")
    func emptyChunks() {
        let result = TranscriptParagraphGrouper.group(
            chunks: [],
            adWindows: []
        )
        #expect(result.isEmpty)
    }

    @Test("Consecutive non-ad chunks with small gaps coalesce into one paragraph")
    func tightChunksGroupTogether() {
        let chunks = [
            chunk(index: 0, start: 0, end: 2),
            chunk(index: 1, start: 2.2, end: 4),
            chunk(index: 2, start: 4.3, end: 6),
        ]
        let result = TranscriptParagraphGrouper.group(chunks: chunks, adWindows: [])
        #expect(result.count == 1)
        #expect(result[0].chunks.count == 3)
        #expect(result[0].startTime == 0)
        #expect(result[0].endTime == 6)
        #expect(result[0].isAd == false)
    }

    @Test("Pause > 2.0s starts a new paragraph")
    func longPauseSplitsParagraph() {
        let chunks = [
            chunk(index: 0, start: 0, end: 2),
            chunk(index: 1, start: 2.5, end: 4),
            // Gap of 2.5s — splits.
            chunk(index: 2, start: 6.5, end: 8),
            chunk(index: 3, start: 8.4, end: 10),
        ]
        let result = TranscriptParagraphGrouper.group(chunks: chunks, adWindows: [])
        #expect(result.count == 2)
        #expect(result[0].chunks.count == 2)
        #expect(result[1].chunks.count == 2)
        #expect(result[0].endTime == 4)
        #expect(result[1].startTime == 6.5)
    }

    @Test("Ad boundary splits paragraph from non-ad to ad")
    func adBoundarySplitsParagraph() {
        let chunks = [
            chunk(index: 0, start: 0, end: 2),
            chunk(index: 1, start: 2.0, end: 4),
            // Chunk 2 falls within an ad window:
            chunk(index: 2, start: 4.0, end: 6),
            chunk(index: 3, start: 6.0, end: 8),
            // Chunk 4 is past the ad window:
            chunk(index: 4, start: 8.0, end: 10),
        ]
        let ads = [adWindow(start: 4.0, end: 8.0)]
        let result = TranscriptParagraphGrouper.group(chunks: chunks, adWindows: ads)
        #expect(result.count == 3)
        #expect(result[0].isAd == false)
        #expect(result[0].chunks.count == 2)
        #expect(result[1].isAd == true)
        #expect(result[1].chunks.count == 2)
        #expect(result[2].isAd == false)
        #expect(result[2].chunks.count == 1)
    }

    @Test("Paragraph text concatenates chunk text with single spaces")
    func textIsJoinedWithSpaces() {
        let chunks = [
            chunk(index: 0, start: 0, end: 2, text: "hello"),
            chunk(index: 1, start: 2.0, end: 4, text: "world"),
        ]
        let result = TranscriptParagraphGrouper.group(chunks: chunks, adWindows: [])
        #expect(result.count == 1)
        #expect(result[0].text == "hello world")
    }

    @Test("Paragraph id derives from first chunk's segmentFingerprint")
    func paragraphIdFromFirstChunk() {
        let chunks = [
            chunk(index: 7, start: 0, end: 2, text: "a"),
            chunk(index: 8, start: 2.0, end: 4, text: "b"),
        ]
        let result = TranscriptParagraphGrouper.group(chunks: chunks, adWindows: [])
        #expect(result.count == 1)
        #expect(result[0].id == "fp-7")
    }
}
