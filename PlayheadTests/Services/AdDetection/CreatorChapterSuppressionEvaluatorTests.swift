// CreatorChapterSuppressionEvaluatorTests.swift
// playhead-rxuv: Unit tests for the content-chapter suppression
// evaluator. This is the false-positive-reduction side of the bead
// (primary value).
//
// Tests verify:
//   * Empty chapter list → no suppression (graceful no-op).
//   * Span outside any content chapter → no suppression.
//   * Span substantially inside (≥ 50%) creator `.content` chapter →
//     suppression.
//   * Quality floor (0.30) is respected — weak content chapters do
//     not suppress.
//   * Inferred chapters never suppress (out-of-scope per the bead).
//   * `.adBreak` / `.ambiguous` chapters do not suppress (only
//     `.content`).
//   * Zero-duration span returns false (overlap fraction undefined).
//   * Open-ended (`endTime == nil`) chapter uses the 60s fallback.
//   * Mixed-disposition list: a single qualifying content chapter
//     is enough to suppress.

import Foundation
import Testing
@testable import Playhead

@Suite("CreatorChapterSuppressionEvaluator")
struct CreatorChapterSuppressionEvaluatorTests {

    // MARK: - Test Fixtures

    private func makeSpan(start: Double, end: Double) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-test", firstAtomOrdinal: 1, lastAtomOrdinal: 10),
            assetId: "asset-test",
            firstAtomOrdinal: 1,
            lastAtomOrdinal: 10,
            startTime: start,
            endTime: end,
            anchorProvenance: []
        )
    }

    private func makeChapter(
        start: TimeInterval,
        end: TimeInterval?,
        title: String? = "Interview",
        disposition: ChapterDisposition = .content,
        qualityScore: Float = 0.8,
        source: ChapterSource = .rssInline
    ) -> ChapterEvidence {
        ChapterEvidence(
            startTime: start,
            endTime: end,
            title: title,
            source: source,
            disposition: disposition,
            qualityScore: qualityScore
        )
    }

    // MARK: - Graceful no-op

    @Test("empty chapter list yields no suppression")
    func emptyChaptersNoSuppression() {
        let span = makeSpan(start: 100, end: 200)
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: []) == false)
    }

    @Test("zero-duration span yields no suppression")
    func zeroDurationSpanNoSuppression() {
        let pointSpan = makeSpan(start: 150, end: 150)
        let content = makeChapter(start: 100, end: 200)
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: pointSpan, chapters: [content]) == false)
    }

    // MARK: - Happy path (suppression fires)

    @Test("span entirely inside content chapter triggers suppression")
    func spanInsideContentChapterSuppresses() {
        let span = makeSpan(start: 120, end: 160)        // 40s span
        let chapter = makeChapter(start: 100, end: 300)  // wholly contains it
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [chapter]) == true)
    }

    @Test("span exactly 50% covered by content chapter suppresses (boundary inclusive)")
    func spanHalfCoveredBoundarySuppresses() {
        // 40s span, content chapter covers exactly 20s of it (50%).
        let span = makeSpan(start: 100, end: 140)
        let chapter = makeChapter(start: 100, end: 120, qualityScore: 0.8)
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [chapter]) == true)
    }

    @Test("span 25% covered does not suppress (below 50% floor)")
    func spanBelowFractionDoesNotSuppress() {
        // 40s span [100, 140]. The chapter is [95, 110] (a 15s chapter)
        // but only the [100, 110] subinterval overlaps the span — 10s
        // of overlap divided by 40s of span = 0.25 fraction, well under
        // the `minSpanOverlapFraction = 0.50` floor.
        let span = makeSpan(start: 100, end: 140)
        let chapter = makeChapter(start: 95, end: 110, qualityScore: 0.8)
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [chapter]) == false)
    }

    @Test("span exactly 49% covered does not suppress (just under the floor)")
    func spanJustBelowBoundaryDoesNotSuppress() {
        // 100s span [0, 100]; chapter overlaps [0, 49] → 49s / 100s = 0.49.
        // The evaluator uses `fraction >= minSpanOverlapFraction`, so a
        // fraction of 0.49 (just under the 0.50 floor) must NOT suppress.
        // Paired with `spanHalfCoveredBoundarySuppresses` above (which
        // pins 0.50 → suppresses), this nails the inclusive-`>=` boundary
        // shape from both sides and guards against an off-by-floating-
        // point regression near the floor.
        let span = makeSpan(start: 0, end: 100)
        let chapter = makeChapter(start: 0, end: 49, qualityScore: 0.8)
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [chapter]) == false)
    }

    // MARK: - Quality floor

    @Test("content chapter below quality floor (0.30) does not suppress")
    func belowQualityFloorDoesNotSuppress() {
        let span = makeSpan(start: 120, end: 160)
        let weak = makeChapter(start: 100, end: 300, qualityScore: 0.20)
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [weak]) == false)
    }

    @Test("content chapter exactly at quality floor (0.30) suppresses")
    func atQualityFloorSuppresses() {
        let span = makeSpan(start: 120, end: 160)
        let atFloor = makeChapter(start: 100, end: 300, qualityScore: 0.30)
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [atFloor]) == true)
    }

    // MARK: - Source filtering (creator-only)

    @Test("inferred (FM-labeled) content chapter does NOT suppress (out of scope)")
    func inferredChapterDoesNotSuppress() {
        // The rxuv bead is scoped to creator-supplied chapters; the
        // follow-on `playhead-w7oi` bead handles LLM-inferred chapters.
        let span = makeSpan(start: 120, end: 160)
        let inferred = makeChapter(
            start: 100, end: 300,
            qualityScore: 0.9,
            source: .inferred
        )
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [inferred]) == false)
    }

    @Test("each creator source (PC20, RSS inline, ID3) is honored as a suppressor")
    func allCreatorSourcesSuppress() {
        let span = makeSpan(start: 120, end: 160)
        for source: ChapterSource in [.pc20, .rssInline, .id3] {
            let chapter = makeChapter(start: 100, end: 300, source: source)
            #expect(
                CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [chapter]) == true,
                "source \(source.rawValue) should suppress"
            )
        }
    }

    // MARK: - Disposition filtering

    @Test(".adBreak chapter does not suppress")
    func adBreakDoesNotSuppress() {
        let span = makeSpan(start: 120, end: 160)
        let adBreak = makeChapter(
            start: 100, end: 300,
            title: "Sponsor",
            disposition: .adBreak,
            qualityScore: 0.9
        )
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [adBreak]) == false)
    }

    @Test(".ambiguous chapter does not suppress")
    func ambiguousDoesNotSuppress() {
        let span = makeSpan(start: 120, end: 160)
        let ambiguous = makeChapter(
            start: 100, end: 300,
            title: nil,
            disposition: .ambiguous,
            qualityScore: 0.8
        )
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [ambiguous]) == false)
    }

    // MARK: - Open-ended chapter

    @Test("open-ended (endTime == nil) content chapter uses 60s fallback")
    func openEndedChapterFallback() {
        // Open content chapter at 600 spans [600, 660] under the fallback.
        // Span at [610, 650] is wholly inside → 100% overlap → suppress.
        let inside = makeSpan(start: 610, end: 650)
        let open = makeChapter(start: 600, end: nil)
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: inside, chapters: [open]) == true)

        // Span at [700, 760] is past the fallback window → no suppress.
        let outside = makeSpan(start: 700, end: 760)
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: outside, chapters: [open]) == false)
    }

    // MARK: - Mixed lists

    @Test("any one qualifying content chapter is enough to suppress")
    func anyQualifyingChapterSuppresses() {
        let span = makeSpan(start: 120, end: 160)
        // Two .content chapters: one too weak, one strong+overlapping.
        let weak = makeChapter(start: 100, end: 300, qualityScore: 0.1)
        let strong = makeChapter(
            start: 110, end: 170,
            title: "Discussion",
            qualityScore: 0.7
        )
        #expect(
            CreatorChapterSuppressionEvaluator.shouldSuppress(
                span: span,
                chapters: [weak, strong]
            ) == true
        )
    }

    @Test("non-overlapping content chapters do not suppress even when ad chapters are present")
    func nonOverlappingMixDoesNotSuppress() {
        let span = makeSpan(start: 120, end: 160)
        // Content chapter far away.
        let elsewhere = makeChapter(start: 500, end: 600)
        // An overlapping AD-break chapter must not trigger suppression
        // either (the suppression path is about content only).
        let adBreak = makeChapter(
            start: 110, end: 170,
            title: "Sponsor",
            disposition: .adBreak,
            qualityScore: 0.9
        )
        #expect(
            CreatorChapterSuppressionEvaluator.shouldSuppress(
                span: span,
                chapters: [elsewhere, adBreak]
            ) == false
        )
    }

    // MARK: - Malformed input

    @Test("chapter with non-finite startTime is ignored")
    func nonFiniteStartTimeIgnored() {
        let span = makeSpan(start: 120, end: 160)
        let corrupt = makeChapter(start: .nan, end: 300)
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [corrupt]) == false)
    }

    @Test("chapter with endTime <= startTime is ignored")
    func zeroLengthChapterIgnored() {
        let span = makeSpan(start: 120, end: 160)
        let bad = makeChapter(start: 150, end: 150)  // zero length
        #expect(CreatorChapterSuppressionEvaluator.shouldSuppress(span: span, chapters: [bad]) == false)
    }
}
