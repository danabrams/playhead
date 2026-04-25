// ChapterMetadataEvidenceBuilderTests.swift
// playhead-gtt9.22: Unit tests for `ChapterMetadataEvidenceBuilder`.
//
// Acceptance: builder is a pure projection from `[ChapterEvidence]` to
// `[EvidenceLedgerEntry]` keyed on a span. Tests verify the documented
// behavior that resolves the bead's design questions:
//   • adBreak-only filter (Q3 cooperative-share is *not* implemented here;
//     each chapter judged on its own merits via qualityScore)
//   • interval-overlap matching (Q2 imprecise timestamps tolerated)
//   • quality floor below `0.30` drops the chapter
//   • base weight is well below `metadataCap = 0.15` so chapter alone
//     can never saturate the metadata family (Q1 confidence weighting)
//   • no I/O whatsoever (Q5 privacy/network — pure value type)

import Foundation
import Testing
@testable import Playhead

@Suite("ChapterMetadataEvidenceBuilder")
struct ChapterMetadataEvidenceBuilderTests {

    // MARK: - Test Fixtures

    /// Build a `DecodedSpan` over the given interval. AssetId/atom ordinals
    /// don't matter for this builder; only `startTime`/`endTime` are read.
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

    /// Build a `ChapterEvidence` value with sensible defaults.
    private func makeChapter(
        start: TimeInterval,
        end: TimeInterval?,
        title: String? = "Sponsor",
        disposition: ChapterDisposition = .adBreak,
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

    // MARK: - Happy Path

    @Test("ad-break chapter inside the span produces exactly one ledger entry")
    func adBreakChapterInsideSpanProducesEntry() {
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 200)
        let chapter = makeChapter(start: 120, end: 180)

        let entries = builder.buildEntries(chapters: [chapter], for: span)

        #expect(entries.count == 1)
        #expect(entries[0].source == .metadata)
        // Weight = baseWeight (0.10) * qualityScore (0.8) = 0.08.
        #expect(abs(entries[0].weight - 0.08) < 1e-6)

        // Detail must be `.metadata` with chapter source-field and disclosure cue.
        switch entries[0].detail {
        case let .metadata(cueCount, sourceField, dominantCueType):
            #expect(cueCount == 1)
            #expect(sourceField == .chapter)
            #expect(dominantCueType == .disclosure)
        default:
            Issue.record("expected `.metadata` detail, got \(entries[0].detail)")
        }
    }

    @Test("weight scales linearly with chapter qualityScore")
    func weightScalesWithQuality() {
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 200)

        let lowQ = makeChapter(start: 120, end: 180, qualityScore: 0.4)
        let highQ = makeChapter(start: 120, end: 180, qualityScore: 1.0)

        let lowEntries = builder.buildEntries(chapters: [lowQ], for: span)
        let highEntries = builder.buildEntries(chapters: [highQ], for: span)

        #expect(lowEntries.count == 1)
        #expect(highEntries.count == 1)
        // 0.10 base × 0.4 = 0.04 vs 0.10 base × 1.0 = 0.10.
        #expect(abs(lowEntries[0].weight - 0.04) < 1e-6)
        #expect(abs(highEntries[0].weight - 0.10) < 1e-6)
    }

    @Test("base weight stays well under metadata family cap")
    func baseWeightUnderMetadataFamilyCap() {
        // The bead requires that even a max-quality chapter cannot saturate
        // the metadata family budget alone. metadataCap is 0.15; max chapter
        // weight is 0.10 (baseWeight × qualityScore=1.0).
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 0, end: 60)
        let chapter = makeChapter(start: 0, end: 60, qualityScore: 1.0)

        let entries = builder.buildEntries(chapters: [chapter], for: span)
        #expect(entries.count == 1)
        #expect(entries[0].weight < 0.15)
    }

    // MARK: - Overlap Semantics (Q2 — imprecise timestamps)

    @Test("chapter wholly outside span yields no entries")
    func nonOverlappingChapterYieldsNoEntries() {
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 200)
        // Far before the span.
        let before = makeChapter(start: 0, end: 30)
        // Far after the span.
        let after = makeChapter(start: 500, end: 560)

        #expect(builder.buildEntries(chapters: [before], for: span).isEmpty)
        #expect(builder.buildEntries(chapters: [after], for: span).isEmpty)
    }

    @Test("chapter overlapping the span boundary still attaches")
    func boundaryOverlapAttaches() {
        // Chapter [80, 120] overlaps span [100, 200] on the leading edge —
        // publisher boundary is imprecise by 20s, builder still attaches.
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 200)
        let leadingEdge = makeChapter(start: 80, end: 120)

        #expect(builder.buildEntries(chapters: [leadingEdge], for: span).count == 1)
    }

    @Test("chapter without endTime falls back to 60s and overlaps appropriately")
    func missingEndTimeUsesFallbackDuration() {
        // With the documented 60s fallback, a chapter starting at 600 with
        // no endTime spans [600, 660]. A span at [610, 650] should attach.
        let builder = ChapterMetadataEvidenceBuilder()
        let attachingSpan = makeSpan(start: 610, end: 650)
        let openChapter = makeChapter(start: 600, end: nil)

        #expect(builder.buildEntries(chapters: [openChapter], for: attachingSpan).count == 1)

        // But a span at [700, 760] is past the 60s fallback window — no attach.
        let detachedSpan = makeSpan(start: 700, end: 760)
        #expect(builder.buildEntries(chapters: [openChapter], for: detachedSpan).isEmpty)
    }

    // MARK: - Disposition Filtering

    @Test(".content disposition does not produce evidence")
    func contentChapterProducesNoEntries() {
        // Builder only emits ad-evidence for `.adBreak` chapters; `.content`
        // is consumed upstream as a soft crossing penalty by candidate-window
        // selection, not by the evidence ledger.
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 200)
        let interview = makeChapter(
            start: 120, end: 180,
            title: "Interview with Guest",
            disposition: .content
        )

        #expect(builder.buildEntries(chapters: [interview], for: span).isEmpty)
    }

    @Test(".ambiguous disposition does not produce evidence")
    func ambiguousChapterProducesNoEntries() {
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 200)
        let untitled = makeChapter(
            start: 120, end: 180,
            title: nil,
            disposition: .ambiguous,
            qualityScore: 0.2
        )

        #expect(builder.buildEntries(chapters: [untitled], for: span).isEmpty)
    }

    // MARK: - Quality Floor

    @Test("chapter below qualityFloor (0.30) is dropped")
    func qualityFloorDropsLowQualityChapter() {
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 200)
        // Quality 0.20 < 0.30 floor.
        let weak = makeChapter(start: 120, end: 180, qualityScore: 0.20)

        #expect(builder.buildEntries(chapters: [weak], for: span).isEmpty)
    }

    @Test("chapter exactly at qualityFloor (0.30) is kept")
    func qualityFloorBoundaryInclusive() {
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 200)
        let atFloor = makeChapter(start: 120, end: 180, qualityScore: 0.30)

        let entries = builder.buildEntries(chapters: [atFloor], for: span)
        #expect(entries.count == 1)
        // 0.10 × 0.30 = 0.03.
        #expect(abs(entries[0].weight - 0.03) < 1e-6)
    }

    // MARK: - Multi-chapter Aggregation

    @Test("multiple overlapping ad chapters emit a single max-quality entry")
    func multipleOverlappingChaptersAggregateToOneEntry() {
        // Two ad-break chapters both overlap the span. Builder picks the
        // higher-quality one and emits exactly one entry, with cueCount
        // reflecting the count of overlapping ad chapters.
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 250)

        let weakerSponsor = makeChapter(
            start: 110, end: 140,
            title: "Sponsor",
            qualityScore: 0.5
        )
        let stronger = makeChapter(
            start: 200, end: 240,
            title: "Sponsored by BetterHelp",
            qualityScore: 0.9
        )

        let entries = builder.buildEntries(
            chapters: [weakerSponsor, stronger],
            for: span
        )
        #expect(entries.count == 1)
        // Higher-quality chapter wins: 0.10 × 0.9 = 0.09.
        #expect(abs(entries[0].weight - 0.09) < 1e-6)

        switch entries[0].detail {
        case let .metadata(cueCount, sourceField, _):
            // Both overlapping ad chapters counted.
            #expect(cueCount == 2)
            #expect(sourceField == .chapter)
        default:
            Issue.record("expected `.metadata` detail")
        }
    }

    @Test("non-overlapping chapters are excluded from the cueCount")
    func nonOverlappingChaptersExcludedFromCount() {
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 200)

        let inSpan = makeChapter(start: 120, end: 180, qualityScore: 0.6)
        let elsewhere = makeChapter(start: 600, end: 660, qualityScore: 0.9)

        let entries = builder.buildEntries(chapters: [inSpan, elsewhere], for: span)
        #expect(entries.count == 1)
        // Even though `elsewhere` has higher quality, it's not in this span,
        // so the in-span chapter wins and cueCount=1.
        #expect(abs(entries[0].weight - 0.06) < 1e-6)
        switch entries[0].detail {
        case let .metadata(cueCount, _, _):
            #expect(cueCount == 1)
        default:
            Issue.record("expected `.metadata` detail")
        }
    }

    @Test("mix of ad-break and content chapters: only ad-breaks contribute")
    func mixedDispositionsOnlyAdBreaksContribute() {
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 250)

        let interview = makeChapter(
            start: 110, end: 150,
            title: "Interview",
            disposition: .content,
            qualityScore: 0.95
        )
        let sponsor = makeChapter(
            start: 200, end: 240,
            title: "Sponsor",
            disposition: .adBreak,
            qualityScore: 0.6
        )

        let entries = builder.buildEntries(chapters: [interview, sponsor], for: span)
        #expect(entries.count == 1)
        #expect(abs(entries[0].weight - 0.06) < 1e-6)
        switch entries[0].detail {
        case let .metadata(cueCount, _, _):
            // Only the ad-break chapter is in the count.
            #expect(cueCount == 1)
        default:
            Issue.record("expected `.metadata` detail")
        }
    }

    // MARK: - Edge Cases

    @Test("empty chapter list yields no entries")
    func emptyChaptersListYieldsNoEntries() {
        let builder = ChapterMetadataEvidenceBuilder()
        let span = makeSpan(start: 100, end: 200)
        #expect(builder.buildEntries(chapters: [], for: span).isEmpty)
    }

    @Test("zero-length span: chapter overlap still works at the point")
    func pointSpanWithCoveringChapter() {
        // Edge case: a span whose start == end (point in time). A chapter
        // covering that point should still attach via the inclusive
        // overlap test (chStart <= spanEnd && chEnd >= spanStart).
        let builder = ChapterMetadataEvidenceBuilder()
        let pointSpan = makeSpan(start: 150, end: 150)
        let cover = makeChapter(start: 100, end: 200)

        #expect(builder.buildEntries(chapters: [cover], for: pointSpan).count == 1)
    }
}
