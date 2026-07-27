// AnalysisCoverageSummaryTests.swift
// playhead-hygc.1.2: tests for the canonical pipeline-progress read model.
//
// Pre-hygc.1.2 the Activity / dogfood paths SUMmed fast-chunk durations
// (which double-counts overlapping chunks AND under-explains gaps) and
// transparently fell through to the asset's stale fast-transcript
// watermark whenever a chunk-derived value was absent. This suite pins
// the canonical reconciliation:
//   - interval-unioned fast coverage seconds (overlap-aware, gap-aware),
//   - high-water `MAX(endTime)` for fast and final-pass chunks,
//   - watermark fallback only when chunks are absent,
//   - per-field provenance tags so dogfood diagnostics expose whether a
//     displayed percentage came from real artifacts or a stale watermark.

import Foundation
import SQLite3
import Testing
@testable import Playhead

@Suite("AnalysisCoverageMath — interval union (playhead-hygc.1.2)")
struct AnalysisCoverageMathTests {

    /// Empty input is the trivial zero-coverage case and must not crash
    /// the pure helper.
    @Test("empty input produces zero seconds")
    func emptyIntervalsProduceZero() {
        let result = AnalysisCoverageMath.unionedSeconds([])
        #expect(result == 0)
    }

    /// Disjoint intervals: union seconds equal the sum of widths.
    @Test("disjoint intervals: union equals sum of widths")
    func disjointIntervalsUnionEqualsSum() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 10),
            (start: 20, end: 25),
            (start: 30, end: 50)
        ])
        // 10 + 5 + 20 = 35
        #expect(result == 35)
    }

    /// Touching intervals (`end1 == start2`) collapse into a single
    /// covered range. The Swift union sweep treats them as a single
    /// continuous run.
    @Test("touching intervals collapse")
    func touchingIntervalsCollapse() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 10),
            (start: 10, end: 20)
        ])
        #expect(result == 20)
    }

    /// Overlapping intervals: the canonical reason the old SUM(...)
    /// query was wrong. Sum would say 100+50=150; union says 100.
    @Test("overlapping intervals — union differs from sum")
    func overlappingIntervalsUnionLessThanSum() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 100),
            (start: 50, end: 100)
        ])
        // Sum-of-widths = 150, union = 100.
        #expect(result == 100)
    }

    /// Fully-contained intervals also collapse to the outer width.
    @Test("contained intervals collapse to outer width")
    func containedIntervalsCollapse() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 100),
            (start: 25, end: 75)
        ])
        #expect(result == 100)
    }

    /// Identical intervals must not double-count.
    @Test("identical intervals — single coverage")
    func identicalIntervalsCoverOnce() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 5, end: 15),
            (start: 5, end: 15)
        ])
        #expect(result == 10)
    }

    /// Degenerate intervals (`end <= start`) contribute zero. The
    /// production query skips them at SQL-emit time but the math helper
    /// must also be defensive (the helper is reachable independently of
    /// the SQL filter).
    @Test("degenerate intervals contribute zero")
    func degenerateIntervalsContributeZero() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 5, end: 5),
            (start: 10, end: 10),
            (start: 100, end: 50),
            (start: 0, end: 7)
        ])
        #expect(result == 7)
    }

    /// NaN endpoints must be filtered (NOT poison the running total).
    /// Subtle: any comparison against `NaN` returns `false`, so the
    /// helper's `end > start` filter naturally excludes `NaN`-valued
    /// intervals — but only because the predicate is `>` (strict). If a
    /// future refactor weakens that to `>=` AND admits NaN-equal cases,
    /// the math would silently start producing NaN totals; this test
    /// pins the current behaviour so that regression is caught.
    @Test("NaN endpoints are filtered, total stays finite")
    func nanIntervalsAreFiltered() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 10),
            (start: .nan, end: 50),
            (start: 0, end: .nan),
            (start: .nan, end: .nan)
        ])
        // Only [0, 10] survives — total is 10s and is finite.
        #expect(result == 10)
        #expect(result.isFinite)
    }

    /// Infinity endpoints must also be filtered. Unlike NaN, an Infinity
    /// endpoint passes a naive `end > start` check (`Inf > 0 == true`,
    /// `0 > -Inf == true`), so a single poisoned interval would have
    /// produced an Infinity total before the helper grew an explicit
    /// `isFinite` guard. R6 pin: an isolated +Inf / -Inf does NOT leak
    /// into the running total, and the result stays finite even when
    /// the poisoned intervals share the input list with healthy ones.
    @Test("Infinity endpoints are filtered, total stays finite")
    func infinityIntervalsAreFiltered() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 10),
            // +Inf as end → would overshoot the union without the guard.
            (start: 0, end: .infinity),
            // -Inf as start → would overshoot the union below zero.
            (start: -.infinity, end: 0),
            // Both +Inf — passes `end > start == false` regardless, but
            // pin alongside its single-endpoint cousins so the table is
            // exhaustive across the 2x2 of {finite, ±Inf} × {start, end}.
            (start: .infinity, end: .infinity),
            // Mixed sign infinities. `+Inf > -Inf == true`, so this
            // one specifically catches drifts in the filter that drop
            // the `isFinite` predicate but keep the strict `>`.
            (start: -.infinity, end: .infinity)
        ])
        #expect(result == 10)
        #expect(result.isFinite)
    }

    /// Intervals presented in arbitrary order must produce the same
    /// answer as sorted input — sort stability is internal to the helper.
    @Test("unsorted input matches sorted result")
    func unsortedInputProducesSameResult() {
        let sorted = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 10),
            (start: 20, end: 30),
            (start: 25, end: 50)
        ])
        let unsorted = AnalysisCoverageMath.unionedSeconds([
            (start: 25, end: 50),
            (start: 0, end: 10),
            (start: 20, end: 30)
        ])
        // Sorted: [0,10] + ([20,30] ∪ [25,50] = [20,50]) → 10 + 30 = 40
        #expect(sorted == 40)
        #expect(sorted == unsorted)
    }

    /// Real-world shape: gapped chunks. Union must reflect the gap,
    /// NOT the extent (so a row with a 10-min gap doesn't claim full
    /// coverage just because chunks reach the end of the audio).
    @Test("gapped chunks — union seconds reflect the gap, not the extent")
    func gappedChunksReflectGap() {
        let result = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 600),
            (start: 1200, end: 1800)
        ])
        // 600 + 600 = 1200 covered, NOT 1800 (extent).
        #expect(result == 1200)
    }
}

@Suite("AnalysisCoverageMath — clipped union / analyzed area (playhead-sd71)")
struct AnalysisCoverageMathClippedTests {

    /// Empty input is zero regardless of the frontier.
    @Test("empty intervals produce zero for any upper bound")
    func emptyIntervalsProduceZero() {
        #expect(AnalysisCoverageMath.unionedSecondsClipped([], upperBound: 100) == 0)
        #expect(AnalysisCoverageMath.unionedSecondsClipped([], upperBound: 0) == 0)
        #expect(AnalysisCoverageMath.unionedSecondsClipped([], upperBound: .infinity) == 0)
    }

    /// Frontier past every interval end → no clipping → equals the full
    /// unclipped union. This is the "analysis reached the end of a gappy
    /// transcript" case: analyzed area == transcript union.
    @Test("upper bound past all intervals → equals unclipped union")
    func upperBoundPastAllEqualsUnclippedUnion() {
        let intervals: [(start: Double, end: Double)] = [
            (start: 0, end: 140),
            (start: 300, end: 390),
            (start: 500, end: 560),
            (start: 900, end: 1000)
        ]
        let clipped = AnalysisCoverageMath.unionedSecondsClipped(intervals, upperBound: 5000)
        let unclipped = AnalysisCoverageMath.unionedSeconds(intervals)
        // 140 + 90 + 60 + 100 = 390 (gap-aware, NOT the 1000 extent).
        #expect(unclipped == 390)
        #expect(clipped == unclipped)
    }

    /// Frontier == 0 clips everything away → zero analyzed seconds.
    @Test("upper bound 0 → full clip → zero")
    func upperBoundZeroClipsEverything() {
        let result = AnalysisCoverageMath.unionedSecondsClipped([
            (start: 0, end: 100),
            (start: 200, end: 300)
        ], upperBound: 0)
        #expect(result == 0)
    }

    /// Negative frontier is not a usable analysis position → zero.
    @Test("negative upper bound → zero")
    func negativeUpperBoundIsZero() {
        let result = AnalysisCoverageMath.unionedSecondsClipped([
            (start: 0, end: 100)
        ], upperBound: -50)
        #expect(result == 0)
    }

    /// Frontier lands mid-interval → that interval is truncated at the
    /// frontier; earlier whole intervals count fully.
    @Test("upper bound mid-interval truncates that interval")
    func upperBoundMidIntervalTruncates() {
        let result = AnalysisCoverageMath.unionedSecondsClipped([
            (start: 0, end: 100),
            (start: 200, end: 400)
        ], upperBound: 300)
        // [0,100] whole (100) + [200,300] truncated (100) = 200.
        #expect(result == 200)
    }

    /// Frontier sits in the GAP between two intervals → only intervals at
    /// or before the frontier count; the later one is excluded entirely.
    @Test("upper bound between intervals excludes the later interval")
    func upperBoundBetweenIntervalsExcludesLater() {
        let result = AnalysisCoverageMath.unionedSecondsClipped([
            (start: 0, end: 200),
            (start: 500, end: 800)
        ], upperBound: 350)
        // [0,200] whole (200); [500,800] is entirely past 350 → excluded.
        #expect(result == 200)
    }

    /// Degenerate / inverted intervals contribute zero even before clipping.
    @Test("degenerate and inverted intervals contribute zero")
    func degenerateIntervalsContributeZero() {
        let result = AnalysisCoverageMath.unionedSecondsClipped([
            (start: 5, end: 5),      // zero-width
            (start: 100, end: 50),   // inverted
            (start: 0, end: 40)      // valid
        ], upperBound: 1000)
        #expect(result == 40)
    }

    /// Non-finite endpoints are filtered even with a finite frontier, so a
    /// poisoned interval cannot survive the max/min clip as a synthetic
    /// [0, upperBound] span.
    @Test("non-finite interval endpoints are filtered, total stays finite")
    func nonFiniteIntervalsFiltered() {
        let result = AnalysisCoverageMath.unionedSecondsClipped([
            (start: 0, end: 100),
            (start: .nan, end: 200),
            (start: 0, end: .nan),
            (start: -.infinity, end: 50),
            (start: 0, end: .infinity)
        ], upperBound: 500)
        // Only [0, 100] survives.
        #expect(result == 100)
        #expect(result.isFinite)
    }

    /// Interval starting before zero is clipped to the [0, upperBound]
    /// window's lower edge (defensive; production timestamps are >= 0).
    @Test("interval starting before zero is clipped to zero")
    func negativeStartClippedToZero() {
        let result = AnalysisCoverageMath.unionedSecondsClipped([
            (start: -30, end: 40)
        ], upperBound: 1000)
        // [-30,40] clipped to [0,40] → 40, not 70.
        #expect(result == 40)
    }

    /// +Infinity frontier means "no upper clip" → the full unclipped union.
    @Test("+Infinity upper bound equals the unclipped union")
    func infiniteUpperBoundEqualsUnclipped() {
        let intervals: [(start: Double, end: Double)] = [
            (start: 0, end: 100),
            (start: 300, end: 500)
        ]
        let clipped = AnalysisCoverageMath.unionedSecondsClipped(intervals, upperBound: .infinity)
        #expect(clipped == AnalysisCoverageMath.unionedSeconds(intervals))
        #expect(clipped == 300)
    }

    /// NaN frontier is not a usable analysis position → zero.
    @Test("NaN upper bound → zero")
    func nanUpperBoundIsZero() {
        let result = AnalysisCoverageMath.unionedSecondsClipped([
            (start: 0, end: 100)
        ], upperBound: .nan)
        #expect(result == 0)
    }

    /// Core invariant (AN <= TX at the math layer): for arbitrary gappy
    /// intervals and ANY frontier, the clipped area never exceeds the
    /// unclipped union, because each clipped interval is a subset of its
    /// source. Swept across a range of frontiers including inside gaps,
    /// mid-interval, and past the end.
    @Test("clipped union never exceeds unclipped union across frontiers")
    func clippedNeverExceedsUnclipped() {
        let intervals: [(start: Double, end: Double)] = [
            (start: 0, end: 140),
            (start: 300, end: 390),
            (start: 500, end: 560),
            (start: 900, end: 1000)
        ]
        let unclipped = AnalysisCoverageMath.unionedSeconds(intervals)
        for frontier in stride(from: -100.0, through: 1200.0, by: 37.0) {
            let clipped = AnalysisCoverageMath.unionedSecondsClipped(
                intervals,
                upperBound: frontier
            )
            #expect(clipped <= unclipped,
                    "clipped \(clipped) exceeded unclipped \(unclipped) at frontier \(frontier)")
            #expect(clipped >= 0)
        }
    }
}

@Suite("AnalysisStore.fetchCoverageSummariesByAssetIds (playhead-hygc.1.2)")
struct AnalysisStoreFetchCoverageSummariesTests {

    private func makeAsset(
        id: String,
        episodeDurationSec: Double? = 300,
        featureCoverageEndTime: Double? = nil,
        fastTranscriptCoverageEndTime: Double? = nil,
        confirmedAdCoverageEndTime: Double? = nil,
        finalPassCoverageEndTime: Double? = nil,
        analysisState: String = "queued"
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///\(id).m4a",
            featureCoverageEndTime: featureCoverageEndTime,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
            confirmedAdCoverageEndTime: confirmedAdCoverageEndTime,
            analysisState: analysisState,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec,
            finalPassCoverageEndTime: finalPassCoverageEndTime
        )
    }

    private func makeChunk(
        assetId: String,
        index: Int,
        start: Double,
        end: Double,
        pass: String = "fast"
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-chunk-\(index)-\(pass)",
            analysisAssetId: assetId,
            segmentFingerprint: "\(assetId)-fp-\(index)-\(pass)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "segment \(index)",
            normalizedText: "segment \(index)",
            pass: pass,
            modelVersion: "test-asr",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    /// (a) overlapping fast chunks: union, NOT sum. The canonical
    /// regression: an asset with chunks 0..200 and 100..300 has true
    /// covered audio of 300s, NOT 200+200=400.
    @Test("(a) overlapping fast chunks union to the unique covered range")
    func overlappingFastChunksUnionNotSum() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-overlap",
            episodeDurationSec: 600,
            fastTranscriptCoverageEndTime: 50 // intentionally stale
        ))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-overlap", index: 0, start: 0, end: 200),
            makeChunk(assetId: "a-overlap", index: 1, start: 100, end: 300)
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-overlap"])
        let summary = try #require(summaries["a-overlap"])
        // Sum-of-widths would be 400; union is 300.
        #expect(summary.fastTranscriptCoveredSec == 300)
        #expect(summary.fastTranscriptCoveredSource == .fastTranscriptChunks)
        // High-water max(endTime) is 300 — disjoint signal.
        #expect(summary.fastTranscriptCoverageEndSec == 300)
        #expect(summary.fastTranscriptCoverageEndSource == .fastTranscriptChunks)
    }

    /// (b) gapped fast chunks: union reflects the gap (500s), high-water
    /// reflects max end (1800s). These are two distinct numbers and must
    /// stay distinct.
    @Test("(b) gapped fast chunks: union ≠ high-water max")
    func gappedFastChunksDistinguishUnionFromHighWater() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-gap",
            episodeDurationSec: 1800
        ))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-gap", index: 0, start: 0, end: 200),
            makeChunk(assetId: "a-gap", index: 1, start: 200, end: 500),
            // Gap of 1000s.
            makeChunk(assetId: "a-gap", index: 2, start: 1500, end: 1800)
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-gap"])
        let summary = try #require(summaries["a-gap"])
        // Union: 500 + 300 = 800.
        #expect(summary.fastTranscriptCoveredSec == 800)
        // High-water: 1800.
        #expect(summary.fastTranscriptCoverageEndSec == 1800)
        #expect(summary.fastTranscriptCoveredSource == .fastTranscriptChunks)
        #expect(summary.fastTranscriptCoverageEndSource == .fastTranscriptChunks)
    }

    /// (c) Stale watermark + complete chunks → display follows the
    /// chunks. This is the asset_004 dogfood signal in a unit test: the
    /// stored watermark says 90s but real chunks cover 3960s.
    @Test("(c) stale watermark + complete chunks → coverage follows chunks")
    func staleWatermarkOverriddenByChunks() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-stale",
            episodeDurationSec: 4000,
            // Mimics asset_004: claimed full coverage with a
            // 90-second watermark.
            fastTranscriptCoverageEndTime: 90,
            analysisState: "completeFull"
        ))
        // Single dense run of fast chunks reaching 3960s.
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-stale", index: 0, start: 0, end: 3960)
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-stale"])
        let summary = try #require(summaries["a-stale"])
        #expect(summary.fastTranscriptCoveredSec == 3960)
        #expect(summary.fastTranscriptCoverageEndSec == 3960)
        #expect(summary.fastTranscriptCoveredSource == .fastTranscriptChunks)
        #expect(summary.fastTranscriptCoverageEndSource == .fastTranscriptChunks)
    }

    /// (e) Feature-only analysis coverage (no confirmed-ad rows) still
    /// produces a meaningful summary. Provenance for an unset
    /// confirmed-ad column is `unknown`, not a synthetic 0.
    @Test("(e) feature coverage populated, no confirmed-ad rows")
    func featureOnlyCoverageProvenance() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-feature",
            featureCoverageEndTime: 250
        ))

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-feature"])
        let summary = try #require(summaries["a-feature"])
        #expect(summary.featureCoverageEndSec == 250)
        #expect(summary.featureCoverageEndSource == .assetWatermark)
        #expect(summary.confirmedAdCoverageEndSec == nil)
        #expect(summary.confirmedAdCoverageEndSource == .unknown)
    }

    /// (f) Confirmed-ad coverage exceeding feature coverage stays in the
    /// summary unchanged — the read model surfaces both numbers; the
    /// caller decides how to combine them. This pins the per-field
    /// provenance.
    @Test("(f) confirmed-ad coverage exceeds feature coverage")
    func confirmedAdExceedsFeatureCoverage() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-ad",
            featureCoverageEndTime: 100,
            confirmedAdCoverageEndTime: 200
        ))

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-ad"])
        let summary = try #require(summaries["a-ad"])
        #expect(summary.featureCoverageEndSec == 100)
        #expect(summary.featureCoverageEndSource == .assetWatermark)
        #expect(summary.confirmedAdCoverageEndSec == 200)
        #expect(summary.confirmedAdCoverageEndSource == .assetWatermark)
    }

    /// (g) Final-pass coverage from chunks beats the asset watermark
    /// column when both exist; provenance becomes `final_pass_chunks`.
    @Test("(g) final-pass chunk MAX(endTime) wins over watermark column")
    func finalPassChunksOverrideFinalPassWatermark() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-final",
            episodeDurationSec: 600,
            // Watermark column says 100 — chunks reach further.
            finalPassCoverageEndTime: 100
        ))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-final", index: 0, start: 0, end: 400, pass: "final"),
            makeChunk(assetId: "a-final", index: 1, start: 0, end: 100, pass: "fast")
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-final"])
        let summary = try #require(summaries["a-final"])
        #expect(summary.finalPassCoverageEndSec == 400)
        #expect(summary.finalPassCoverageEndSource == .finalPassChunks)
    }

    /// Final-pass watermark fallback: when only the asset column has a
    /// value, provenance must be `asset_watermark` (not `unknown`).
    @Test("final-pass: chunks absent → asset watermark used as fallback")
    func finalPassFallsBackToAssetWatermark() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-final-wm",
            episodeDurationSec: 600,
            finalPassCoverageEndTime: 250
        ))

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-final-wm"])
        let summary = try #require(summaries["a-final-wm"])
        #expect(summary.finalPassCoverageEndSec == 250)
        #expect(summary.finalPassCoverageEndSource == .assetWatermark)
    }

    /// (h) All-nil artifacts → unknown progress (not synthetic 0%).
    @Test("(h) all-nil artifacts → unknown provenance, no synthetic zero")
    func allNilArtifactsProduceUnknown() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-empty",
            episodeDurationSec: nil
        ))

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-empty"])
        let summary = try #require(summaries["a-empty"])
        #expect(summary.episodeDurationSec == nil)
        #expect(summary.fastTranscriptCoveredSec == nil)
        #expect(summary.fastTranscriptCoveredSource == .unknown)
        #expect(summary.fastTranscriptCoverageEndSec == nil)
        #expect(summary.fastTranscriptCoverageEndSource == .unknown)
        #expect(summary.featureCoverageEndSec == nil)
        #expect(summary.featureCoverageEndSource == .unknown)
        #expect(summary.confirmedAdCoverageEndSec == nil)
        #expect(summary.confirmedAdCoverageEndSource == .unknown)
        #expect(summary.finalPassCoverageEndSec == nil)
        #expect(summary.finalPassCoverageEndSource == .unknown)
    }

    /// Watermark fallback for fast coverage: chunks absent, watermark
    /// present → covered/end seconds both come from the watermark with
    /// `asset_watermark` provenance. Pins that the read model does NOT
    /// silently emit `unknown` when the asset has at least the
    /// scheduler watermark to show.
    @Test("fast coverage falls back to asset watermark when chunks absent")
    func fastCoverageFallsBackToWatermarkWhenChunksAbsent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-only-wm",
            episodeDurationSec: 600,
            fastTranscriptCoverageEndTime: 120
        ))

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-only-wm"])
        let summary = try #require(summaries["a-only-wm"])
        #expect(summary.fastTranscriptCoveredSec == 120)
        #expect(summary.fastTranscriptCoveredSource == .assetWatermark)
        #expect(summary.fastTranscriptCoverageEndSec == 120)
        #expect(summary.fastTranscriptCoverageEndSource == .assetWatermark)
    }

    /// Empty input must short-circuit to an empty dictionary without
    /// preparing a SQL statement at all (zero placeholders is otherwise
    /// invalid SQL: `WHERE id IN ()`).
    @Test("empty input returns empty dictionary")
    func emptyInputReturnsEmpty() async throws {
        let store = try await makeTestStore()
        let result = try await store.fetchCoverageSummariesByAssetIds([])
        #expect(result.isEmpty)
    }

    /// Inputs larger than the chunk size (500) must still produce a
    /// single dictionary covering every present asset. Mirrors the
    /// `BulkAssetFetchTests` chunk-size regression.
    @Test("input above chunk-size boundary produces full result")
    func inputAboveChunkSizeProducesFullResult() async throws {
        let store = try await makeTestStore()
        let n = 750
        var ids: Set<String> = []
        ids.reserveCapacity(n)
        for i in 0..<n {
            let id = "a-bulk-\(i)"
            ids.insert(id)
            try await store.insertAsset(makeAsset(
                id: id,
                episodeDurationSec: 300,
                fastTranscriptCoverageEndTime: 30
            ))
        }
        let result = try await store.fetchCoverageSummariesByAssetIds(ids)
        #expect(result.count == n)
    }

    // MARK: - playhead-sd71 analyzed-coverage AREA (AN <= TX)

    /// The canonical "AN 100% / TX 39%" antipattern, reproduced at the read
    /// model: a gappy fast transcript (union = 390s of a 1000s episode =
    /// 39%) whose high-water end reaches 1000s, with the analysis frontier
    /// (confirmed-ad coverage) parked at 1000s (100%). The OLD watermark AN
    /// reported 1000/1000 = 100%. The corrected `analysisCoveredSec` is the
    /// transcript union clipped to the frontier — and since the frontier is
    /// past every hole, the clip is a no-op → analyzed area == transcript
    /// union == 390s. AN == TX, never above.
    @Test("(sd71) gappy transcript + frontier past holes → analyzed area == transcript union (AN == TX == 39%)")
    func analyzedAreaEqualsTranscriptUnionWhenFrontierPastHoles() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-sd71-repro",
            episodeDurationSec: 1000,
            confirmedAdCoverageEndTime: 1000 // frontier at the very end
        ))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-sd71-repro", index: 0, start: 0, end: 140),
            makeChunk(assetId: "a-sd71-repro", index: 1, start: 300, end: 390),
            makeChunk(assetId: "a-sd71-repro", index: 2, start: 500, end: 560),
            makeChunk(assetId: "a-sd71-repro", index: 3, start: 900, end: 1000)
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-sd71-repro"])
        let summary = try #require(summaries["a-sd71-repro"])
        // TX numerator: gap-aware union = 140 + 90 + 60 + 100 = 390.
        #expect(summary.fastTranscriptCoveredSec == 390)
        // High-water reaches the end — the value that fooled the old AN.
        #expect(summary.fastTranscriptCoverageEndSec == 1000)
        // AN numerator: analyzed AREA == transcript union (frontier past all
        // holes), NOT the 1000s watermark.
        #expect(summary.analysisCoveredSec == 390)
        // Invariant: analyzed area is a subset of the transcript union.
        let analyzed = try #require(summary.analysisCoveredSec)
        let transcript = try #require(summary.fastTranscriptCoveredSec)
        #expect(analyzed <= transcript)
        // Both fractions land at 39% — NOT the old 100%.
        let duration = try #require(summary.episodeDurationSec)
        #expect(analyzed / duration == 390.0 / 1000.0)
        #expect(transcript / duration == 390.0 / 1000.0)
    }

    /// Frontier landing mid-transcript clips the analyzed area strictly
    /// below the transcript union.
    @Test("(sd71) frontier mid-transcript clips analyzed area below the transcript union")
    func analyzedAreaClippedWhenFrontierMidTranscript() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-sd71-partial",
            episodeDurationSec: 600,
            featureCoverageEndTime: 500 // frontier inside the second chunk
        ))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-sd71-partial", index: 0, start: 0, end: 200),
            makeChunk(assetId: "a-sd71-partial", index: 1, start: 400, end: 600)
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-sd71-partial"])
        let summary = try #require(summaries["a-sd71-partial"])
        // Transcript union = 200 + 200 = 400.
        #expect(summary.fastTranscriptCoveredSec == 400)
        // Analyzed area: [0,200] whole (200) + [400,500] truncated (100) = 300.
        #expect(summary.analysisCoveredSec == 300)
        #expect((summary.analysisCoveredSec ?? 0) < (summary.fastTranscriptCoveredSec ?? 0))
    }

    /// No analysis frontier (no feature / confirmed-ad coverage) → analyzed
    /// area is `nil` even when transcript is present, so AN renders `--%`
    /// rather than a synthetic 0%.
    @Test("(sd71) no analysis frontier → analyzed area nil even with transcript present")
    func analyzedAreaNilWhenNoFrontier() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-sd71-nofrontier",
            episodeDurationSec: 600
        ))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-sd71-nofrontier", index: 0, start: 0, end: 300)
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-sd71-nofrontier"])
        let summary = try #require(summaries["a-sd71-nofrontier"])
        #expect(summary.fastTranscriptCoveredSec == 300)
        #expect(summary.analysisCoveredSec == nil)
    }

    /// Watermark-only transcript (no chunk intervals): the covered region is
    /// modeled as one contiguous [0, transcriptWatermark] span, so the
    /// analyzed area degrades to min(transcriptWatermark, frontier) — capped
    /// at the transcript, so AN can never exceed TX even when the analysis
    /// frontier runs ahead of the transcript watermark.
    @Test("(sd71) watermark-only transcript → analyzed area = min(transcript watermark, frontier)")
    func analyzedAreaWatermarkOnlyTranscript() async throws {
        let store = try await makeTestStore()
        // Frontier (500) AHEAD of the transcript watermark (200): analyzed
        // area caps at the transcript (200).
        try await store.insertAsset(makeAsset(
            id: "a-sd71-wm-ahead",
            episodeDurationSec: 600,
            featureCoverageEndTime: 500,
            fastTranscriptCoverageEndTime: 200
        ))
        // Frontier (120) BEHIND the transcript watermark (300): analyzed
        // area truncates at the frontier (120).
        try await store.insertAsset(makeAsset(
            id: "a-sd71-wm-behind",
            episodeDurationSec: 600,
            featureCoverageEndTime: 120,
            fastTranscriptCoverageEndTime: 300
        ))

        let summaries = try await store.fetchCoverageSummariesByAssetIds([
            "a-sd71-wm-ahead", "a-sd71-wm-behind"
        ])

        let ahead = try #require(summaries["a-sd71-wm-ahead"])
        #expect(ahead.fastTranscriptCoveredSec == 200)
        #expect(ahead.fastTranscriptCoveredSource == .assetWatermark)
        #expect(ahead.analysisCoveredSec == 200) // min(200, 500)
        #expect((ahead.analysisCoveredSec ?? 0) <= (ahead.fastTranscriptCoveredSec ?? 0))

        let behind = try #require(summaries["a-sd71-wm-behind"])
        #expect(behind.fastTranscriptCoveredSec == 300)
        #expect(behind.analysisCoveredSec == 120) // min(300, 120)
        #expect((behind.analysisCoveredSec ?? 0) <= (behind.fastTranscriptCoveredSec ?? 0))
    }
}

@Suite("Dogfood fixture integration: asset_004 chunk-vs-watermark (playhead-hygc.1.2)")
struct DogfoodFixtureCoverageSummaryTests {

    /// Replays the asset_004 contradiction directly through the store-
    /// level read model: insert the asset with the dogfood-captured
    /// stale watermark and a single dense fast-chunk reaching ~3960s,
    /// then assert the canonical summary picks up the chunk maxima
    /// rather than the 90s watermark.
    ///
    /// Why we don't just decode the fixture and replay every row: the
    /// fixture is sanitized for SHARED dogfood signals (correction-row
    /// duplicates, terminal-state contradictions, FA event histograms)
    /// — its `transcript_chunk_maxima` rows are aggregates, not full
    /// chunk dumps, so we can't faithfully repopulate `transcript_chunks`
    /// from it without inventing chunk boundaries the fixture doesn't
    /// pin. Driving the store with the fixture's headline numbers is the
    /// right contract: "given the dogfood-shaped data, the read model
    /// reports chunk-derived coverage, not watermark-derived".
    @Test("fixture-shaped inputs: asset_004 coverage reflects chunk maxima")
    func dogfoodAsset004CoverageReflectsChunkMaxima() async throws {
        let fixture = try DogfoodAnalysisHealthFixtureLoader.load()
        // Pull the named asset's headline shape from the fixture so this
        // test breaks loudly if a future fixture regenerate moves the
        // signal off asset_004.
        let analysisAssets = fixture.analysisAssets
        let assetFixtureOpt = analysisAssets.first(where: { row in
            row.id == "asset_004"
        })
        let assetFixture = try #require(
            assetFixtureOpt,
            "fixture must continue to contain asset_004"
        )
        let chunkMaxima = fixture.transcriptChunkMaxima
        let chunkMaxOpt = chunkMaxima.first(where: { row in
            row.assetId == "asset_004" && row.pass == "fast"
        })
        let chunkMax = try #require(
            chunkMaxOpt,
            "fixture must continue to contain asset_004 fast chunk maxima"
        )

        // Sanity: the contradiction the dogfood capture documents must
        // still be present in the fixture — the watermark must be far
        // smaller than the chunk max. (If a regenerated fixture removed
        // the signal, the rest of this test would silently pass against
        // a healthy asset and stop guarding the regression.)
        let watermark = try #require(assetFixture.fastTranscriptCoverageEndSec)
        #expect(chunkMax.maxEndTimeSec > watermark + 1000,
                "fixture asset_004 no longer carries the >>watermark chunk-coverage signal: chunk \(chunkMax.maxEndTimeSec)s vs. watermark \(watermark)s")

        let store = try await makeTestStore()
        try await store.insertAsset(AnalysisAsset(
            id: "asset_004",
            episodeId: "ep-asset_004",
            assetFingerprint: "fp-asset_004",
            weakFingerprint: nil,
            sourceURL: "file:///asset_004.m4a",
            featureCoverageEndTime: assetFixture.featureCoverageEndSec,
            fastTranscriptCoverageEndTime: assetFixture.fastTranscriptCoverageEndSec,
            confirmedAdCoverageEndTime: assetFixture.confirmedAdCoverageEndSec,
            analysisState: assetFixture.analysisState,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: assetFixture.episodeDurationSec,
            finalPassCoverageEndTime: assetFixture.finalPassCoverageEndSec
        ))
        // Single dense fast chunk reaching the dogfood-captured max
        // end. The fixture aggregates 4334 chunks into a single max-
        // end-time row; replaying that as one dense interval gives the
        // same chunk-derived coverage answer (the read model only cares
        // about the union and the high-water).
        try await store.insertTranscriptChunks([
            TranscriptChunk(
                id: "asset_004-chunk-0-fast",
                analysisAssetId: "asset_004",
                segmentFingerprint: "asset_004-fp-0-fast",
                chunkIndex: 0,
                startTime: 0,
                endTime: chunkMax.maxEndTimeSec,
                text: "dogfood-asset_004",
                normalizedText: "dogfood-asset_004",
                pass: "fast",
                modelVersion: "test-asr",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["asset_004"])
        let summary = try #require(summaries["asset_004"])
        // Chunk maxima dominate, not the 90-second watermark.
        #expect(summary.fastTranscriptCoverageEndSec == chunkMax.maxEndTimeSec)
        #expect(summary.fastTranscriptCoverageEndSource == .fastTranscriptChunks)
        // The 90s watermark must NOT be the surfaced value.
        #expect(summary.fastTranscriptCoverageEndSec != watermark)
        // Bead spec: the asset_004 chunk maxima are ~3959.77s (66 min).
        // Pin the order of magnitude so a future fixture regenerate
        // that subtly weakens the signal can't quietly slip past.
        #expect(summary.fastTranscriptCoverageEndSec ?? 0 > 3000,
                "expected chunk-derived coverage to clear 50 minutes; got \(summary.fastTranscriptCoverageEndSec ?? 0)s")
        #expect(summary.fastTranscriptCoveredSec == chunkMax.maxEndTimeSec)
        #expect(summary.fastTranscriptCoveredSource == .fastTranscriptChunks)
    }
}

// MARK: - playhead-0sro: watermark monotonicity, reconciliation, invariant
//
// The read model above reconciles coverage AT READ TIME. playhead-0sro is
// the other half: the persisted `analysis_assets.fastTranscriptCoverageEndTime`
// column itself was going stale, and consumers that read the raw column —
// the yqax catch-up trigger and, since #285, the glo9 opportunistic backlog
// drain admission gate — have no read-time reconciliation to save them.
//
// Root cause pinned by `advanceRejectsRewindFromLaterShardOrder` below:
// `TranscriptEngineService` wrote the end of the shard it finished LAST,
// and `prioritizeShards` deliberately runs behind-the-playhead shards last
// in DESCENDING start order, so a completed pass persisted the earliest
// non-zero shard's end — exactly 60.0 s with the production 30 s shard.

@Suite("FastTranscriptCoverageInvariant — watermark vs chunks vs duration (playhead-0sro)")
struct FastTranscriptCoverageInvariantTests {

    /// The THEMOVE phone asset from the bead, as persisted state: a fast
    /// transcript covering 3575.88 s of a 3578.10 s episode behind a
    /// watermark frozen at 60 s. This MUST be reported as a violation —
    /// it is the entire premise of the bead.
    @Test("stale THEMOVE watermark (60 s behind 3575.88 s of chunks) violates the invariant")
    func staleThemoveWatermarkViolates() {
        let violations = FastTranscriptCoverageInvariant.violations(
            watermarkSec: 60,
            chunkMaxEndSec: 3575.88,
            episodeDurationSec: 3578.10
        )
        #expect(violations == [
            .watermarkBehindChunks(watermarkSec: 60, chunkMaxEndSec: 3575.88)
        ])
        #expect(!FastTranscriptCoverageInvariant.holds(
            watermarkSec: 60,
            chunkMaxEndSec: 3575.88,
            episodeDurationSec: 3578.10
        ))
    }

    /// The repaired form of the same asset: watermark at the chunk reach,
    /// a hair under the episode duration. Healthy.
    @Test("reconciled THEMOVE watermark satisfies the invariant")
    func reconciledThemoveWatermarkHolds() {
        #expect(FastTranscriptCoverageInvariant.holds(
            watermarkSec: 3575.88,
            chunkMaxEndSec: 3575.88,
            episodeDurationSec: 3578.10
        ))
    }

    /// TOLERATED GAP 1 — sub-second float/timestamp disagreement between
    /// shard arithmetic and ASR segment ends is normal, not a defect.
    @Test("sub-second lag behind chunks is tolerated")
    func subSecondLagTolerated() {
        #expect(FastTranscriptCoverageInvariant.holds(
            watermarkSec: 1799.6,
            chunkMaxEndSec: 1800.0,
            episodeDurationSec: 1800.0
        ))
        // One tick past the tolerance is a violation — the boundary is
        // real, not decorative.
        #expect(!FastTranscriptCoverageInvariant.holds(
            watermarkSec: 1798.9,
            chunkMaxEndSec: 1800.0,
            episodeDurationSec: 1800.0
        ))
    }

    /// TOLERATED GAP 2 — the watermark may exceed `episodeDurationSec` by
    /// up to one shard: feed-declared vs measured duration routinely
    /// disagree by seconds, and the shard-sum can land past a probe.
    @Test("watermark within one shard past the duration is tolerated; beyond it is not")
    func pastDurationToleranceBoundary() {
        #expect(FastTranscriptCoverageInvariant.holds(
            watermarkSec: 1820,
            chunkMaxEndSec: 1820,
            episodeDurationSec: 1800
        ))
        let violations = FastTranscriptCoverageInvariant.violations(
            watermarkSec: 1900,
            chunkMaxEndSec: 1900,
            episodeDurationSec: 1800
        )
        #expect(violations == [
            .watermarkPastDuration(watermarkSec: 1900, episodeDurationSec: 1800)
        ])
    }

    /// The invariant is about REACH, not AREA. A transcript with a large
    /// interior hole still has a legitimate high-water watermark; flagging
    /// that would re-introduce the sd71 watermark-vs-union conflation.
    @Test("interior transcript gaps do not violate the reach invariant")
    func interiorGapsDoNotViolate() {
        // Chunks: [0,600] and [3000,3575.88] — a 40-minute hole. The
        // watermark correctly reports reach 3575.88.
        #expect(FastTranscriptCoverageInvariant.holds(
            watermarkSec: 3575.88,
            chunkMaxEndSec: 3575.88,
            episodeDurationSec: 3578.10
        ))
        // And the AREA measure stays independent and much smaller.
        let area = AnalysisCoverageMath.unionedSeconds([
            (start: 0, end: 600),
            (start: 3000, end: 3575.88)
        ])
        #expect(area < 1200)
    }

    /// A watermark ahead of the chunks is legitimate: the engine advances
    /// coverage for a silent shard that produced no segments at all.
    @Test("watermark ahead of chunk coverage is not a violation")
    func watermarkAheadOfChunksIsFine() {
        #expect(FastTranscriptCoverageInvariant.holds(
            watermarkSec: 900,
            chunkMaxEndSec: 600,
            episodeDurationSec: 1800
        ))
    }

    /// Both directions can be wrong at once; both must be reported.
    @Test("both violations are reported together in a stable order")
    func bothViolationsReported() {
        let violations = FastTranscriptCoverageInvariant.violations(
            watermarkSec: 5000,
            chunkMaxEndSec: 6000,
            episodeDurationSec: 1800
        )
        #expect(violations == [
            .watermarkBehindChunks(watermarkSec: 5000, chunkMaxEndSec: 6000),
            .watermarkPastDuration(watermarkSec: 5000, episodeDurationSec: 1800)
        ])
    }

    /// Absent inputs are vacuously healthy — "never written" is not
    /// "stale", and an unknown duration must not be compared against.
    @Test("nil watermark / nil chunks / nil-or-zero duration are vacuously healthy")
    func absentInputsAreVacuous() {
        #expect(FastTranscriptCoverageInvariant.violations(
            watermarkSec: nil,
            chunkMaxEndSec: 3575.88,
            episodeDurationSec: 3578.10
        ).isEmpty)
        #expect(FastTranscriptCoverageInvariant.violations(
            watermarkSec: 60,
            chunkMaxEndSec: nil,
            episodeDurationSec: nil
        ).isEmpty)
        // A zero / negative duration is "unknown", not "everything is
        // past the end".
        #expect(FastTranscriptCoverageInvariant.violations(
            watermarkSec: 1800,
            chunkMaxEndSec: 1800,
            episodeDurationSec: 0
        ).isEmpty)
    }

    /// Non-finite inputs must not produce unactionable violations.
    @Test("non-finite inputs produce no violations")
    func nonFiniteInputsProduceNoViolations() {
        #expect(FastTranscriptCoverageInvariant.violations(
            watermarkSec: .nan,
            chunkMaxEndSec: 3575.88,
            episodeDurationSec: 3578.10
        ).isEmpty)
        #expect(FastTranscriptCoverageInvariant.violations(
            watermarkSec: 60,
            chunkMaxEndSec: .nan,
            episodeDurationSec: .nan
        ).isEmpty)
    }

    /// Tolerances are configurable; a caller demanding exactness gets it.
    @Test("zero tolerances make the invariant exact")
    func zeroTolerancesAreExact() {
        let strict = FastTranscriptCoverageInvariant.Tolerances(
            watermarkBehindChunksSec: 0,
            watermarkPastDurationSec: 0
        )
        #expect(!FastTranscriptCoverageInvariant.holds(
            watermarkSec: 1799.9,
            chunkMaxEndSec: 1800.0,
            episodeDurationSec: 1800.0,
            tolerances: strict
        ))
        #expect(FastTranscriptCoverageInvariant.holds(
            watermarkSec: 1800.0,
            chunkMaxEndSec: 1800.0,
            episodeDurationSec: 1800.0,
            tolerances: strict
        ))
    }
}

@Suite("AnalysisStore — fast-transcript watermark monotonicity + reconciliation (playhead-0sro)")
struct FastTranscriptCoverageWatermarkTests {

    private func makeAsset(
        id: String,
        episodeDurationSec: Double? = 3578.10,
        fastTranscriptCoverageEndTime: Double? = nil
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
            confirmedAdCoverageEndTime: nil,
            analysisState: "transcribing",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec
        )
    }

    private func makeChunk(
        assetId: String,
        index: Int,
        start: Double,
        end: Double,
        pass: String = "fast"
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-c\(index)-\(pass)",
            analysisAssetId: assetId,
            segmentFingerprint: "\(assetId)-fp\(index)-\(pass)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "segment \(index)",
            normalizedText: "segment \(index)",
            pass: pass,
            modelVersion: "test-asr",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    /// Full 30 s-shard chunk coverage of the THEMOVE episode, ending at
    /// 3575.88 s. Deliberately built from many rows so `MAX(endTime)`
    /// has to do real work.
    private func themoveFastChunks(assetId: String) -> [TranscriptChunk] {
        var chunks: [TranscriptChunk] = []
        var start = 0.0
        var index = 0
        while start < 3540 {
            chunks.append(makeChunk(assetId: assetId, index: index, start: start, end: start + 30))
            start += 30
            index += 1
        }
        chunks.append(makeChunk(assetId: assetId, index: index, start: 3540, end: 3575.88))
        return chunks
    }

    // MARK: - Monotonic advance

    @Test("advance writes through from a NULL watermark")
    func advanceFromNull() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-null"))
        try await store.advanceFastTranscriptCoverage(id: "a-null", endTime: 600)
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "a-null") == 600)
    }

    /// THE ROOT-CAUSE REGRESSION. `TranscriptEngineService.prioritizeShards`
    /// returns `shard0 + hotPath + coldAhead + behindWithoutShard0`, with the
    /// behind-shards sorted DESCENDING — so a pass that reaches the end of
    /// the episode finishes on the EARLIEST non-zero shard, whose end is
    /// 60.0 s at the production 30 s shard duration. Under the old blind
    /// `SET … = ?` that clobbered the true reach. Replayed here as the
    /// exact write sequence.
    @Test("advance rejects the rewind produced by playhead-relative shard order")
    func advanceRejectsRewindFromLaterShardOrder() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-order"))

        // Ahead-of-playhead shards climb to the end of the episode…
        for end in stride(from: 1230.0, through: 3570.0, by: 30.0) {
            try await store.advanceFastTranscriptCoverage(id: "a-order", endTime: end)
        }
        try await store.advanceFastTranscriptCoverage(id: "a-order", endTime: 3575.88)
        // …then the behind-playhead tail runs, descending: 1200, 1170, …,
        // 90, 60. Followed by the shard-0 backfill at 30.
        for end in stride(from: 1200.0, through: 60.0, by: -30.0) {
            try await store.advanceFastTranscriptCoverage(id: "a-order", endTime: end)
        }
        try await store.advanceFastTranscriptCoverage(id: "a-order", endTime: 30)

        let watermark = try await store.fetchFastTranscriptCoverageEndTime(id: "a-order")
        #expect(watermark == 3575.88, "watermark rewound to a behind-playhead shard end; got \(watermark ?? -1)")
        #expect(watermark != 60, "a full fast transcript must never report 60 s coverage")
    }

    @Test("advance is a no-op for an equal value")
    func advanceEqualIsNoOp() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-eq"))
        try await store.advanceFastTranscriptCoverage(id: "a-eq", endTime: 900)
        try await store.advanceFastTranscriptCoverage(id: "a-eq", endTime: 900)
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "a-eq") == 900)
    }

    @Test("advance never touches a different asset's row")
    func advanceIsRowScoped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-one"))
        try await store.insertAsset(makeAsset(id: "a-two", fastTranscriptCoverageEndTime: 42))
        try await store.advanceFastTranscriptCoverage(id: "a-one", endTime: 900)
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "a-two") == 42)
    }

    // MARK: - The sanctioned rewind

    /// The crash-repair path (`analysisState` late-stage but 0 chunks)
    /// still has to be able to rewind — and must use the explicitly-named
    /// method, because the monotonic advance would silently no-op.
    @Test("reset rewinds to zero where advance(0) cannot")
    func resetRewindsToZero() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-reset", fastTranscriptCoverageEndTime: 1800))

        try await store.advanceFastTranscriptCoverage(id: "a-reset", endTime: 0)
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "a-reset") == 1800,
                "advance must not be usable as a rewind")

        try await store.resetFastTranscriptCoverage(id: "a-reset")
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "a-reset") == 0)
    }

    // MARK: - Per-asset reconciliation

    @Test("reconcile raises a stale watermark to canonical chunk coverage")
    func reconcileRaisesStaleWatermark() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-stale", fastTranscriptCoverageEndTime: 60))
        try await store.insertTranscriptChunks(themoveFastChunks(assetId: "a-stale"))

        let reconciled = try await store.reconcileFastTranscriptCoverage(id: "a-stale")
        #expect(reconciled == 3575.88)
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "a-stale") == 3575.88)
        #expect(FastTranscriptCoverageInvariant.holds(
            watermarkSec: reconciled,
            chunkMaxEndSec: 3575.88,
            episodeDurationSec: 3578.10
        ))
    }

    @Test("reconcile never lowers a watermark that runs ahead of the chunks")
    func reconcileNeverLowers() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-ahead", fastTranscriptCoverageEndTime: 900))
        // A silent shard advanced coverage to 900 with chunks only to 600.
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-ahead", index: 0, start: 0, end: 300),
            makeChunk(assetId: "a-ahead", index: 1, start: 300, end: 600)
        ])
        #expect(try await store.reconcileFastTranscriptCoverage(id: "a-ahead") == 900)
    }

    /// MIXED FAST/FINAL. Final-pass chunks re-transcribe AdWindow ranges
    /// and carry their own watermark column; they must not move the FAST
    /// watermark, in either direction.
    @Test("reconcile reads only pass='fast' chunks — final-pass chunks are ignored")
    func reconcileIgnoresFinalPassChunks() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-mixed", fastTranscriptCoverageEndTime: 60))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-mixed", index: 0, start: 0, end: 300),
            makeChunk(assetId: "a-mixed", index: 1, start: 300, end: 600),
            // Final-pass re-transcription reaching much further.
            makeChunk(assetId: "a-mixed", index: 2, start: 2400, end: 2700, pass: "final"),
            makeChunk(assetId: "a-mixed", index: 3, start: 3000, end: 3300, pass: "final")
        ])

        #expect(try await store.reconcileFastTranscriptCoverage(id: "a-mixed") == 600)

        // And the read model still separates the two passes correctly.
        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-mixed"])
        let summary = try #require(summaries["a-mixed"])
        #expect(summary.fastTranscriptCoverageEndSec == 600)
        #expect(summary.fastTranscriptCoverageEndSource == .fastTranscriptChunks)
        #expect(summary.finalPassCoverageEndSec == 3300)
        #expect(summary.finalPassCoverageEndSource == .finalPassChunks)
    }

    @Test("reconcile skips degenerate and inverted chunk rows")
    func reconcileSkipsDegenerateRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-degen", fastTranscriptCoverageEndTime: 60))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-degen", index: 0, start: 0, end: 300),
            // Zero-width and inverted rows must not inflate the watermark.
            makeChunk(assetId: "a-degen", index: 1, start: 9000, end: 9000),
            makeChunk(assetId: "a-degen", index: 2, start: 9999, end: 5000)
        ])
        #expect(try await store.reconcileFastTranscriptCoverage(id: "a-degen") == 300)
    }

    @Test("reconcile is a no-op when no fast chunks exist, and nil for an unknown asset")
    func reconcileNoOpAndUnknownAsset() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-empty", fastTranscriptCoverageEndTime: 120))
        #expect(try await store.reconcileFastTranscriptCoverage(id: "a-empty") == 120)
        #expect(try await store.reconcileFastTranscriptCoverage(id: "a-missing") == nil)
    }

    // MARK: - Whole-table reconciliation (the migration body)

    @Test("bulk reconcile repairs stale rows, leaves healthy rows alone, and is idempotent")
    func bulkReconcileRepairsOnlyStaleRows() async throws {
        let store = try await makeTestStore()
        // Stale: chunks to 3575.88 behind a 60 s watermark.
        try await store.insertAsset(makeAsset(id: "b-stale", fastTranscriptCoverageEndTime: 60))
        try await store.insertTranscriptChunks(themoveFastChunks(assetId: "b-stale"))
        // Stale with a NULL watermark (never written).
        try await store.insertAsset(makeAsset(id: "b-null"))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "b-null", index: 0, start: 0, end: 1200)
        ])
        // Healthy: watermark already at the chunk reach.
        try await store.insertAsset(makeAsset(id: "b-ok", fastTranscriptCoverageEndTime: 600))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "b-ok", index: 0, start: 0, end: 600)
        ])
        // Ahead of its chunks — must not be pulled back down.
        try await store.insertAsset(makeAsset(id: "b-ahead", fastTranscriptCoverageEndTime: 1500))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "b-ahead", index: 0, start: 0, end: 600)
        ])
        // No chunks at all.
        try await store.insertAsset(makeAsset(id: "b-bare", fastTranscriptCoverageEndTime: 90))

        let repaired = try await store.reconcileAllFastTranscriptCoverageWatermarks()
        #expect(repaired == 2)
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "b-stale") == 3575.88)
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "b-null") == 1200)
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "b-ok") == 600)
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "b-ahead") == 1500)
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "b-bare") == 90)

        // Idempotent: a second sweep finds nothing left to do.
        #expect(try await store.reconcileAllFastTranscriptCoverageWatermarks() == 0)
    }
}

@Suite("AnalysisStore — V37 stale-watermark reconciliation migration (playhead-0sro)")
struct FastTranscriptCoverageV37MigrationTests {

    private func dbURL(_ dir: URL) -> URL {
        dir.appendingPathComponent("analysis.sqlite")
    }

    /// Rewinds `_meta.schema_version` on the file at `dir` so a reopen
    /// re-runs the V37 step. Mirrors the seeding style of the other
    /// on-disk upgrade-path suites.
    private func rewindSchemaVersion(in dir: URL, to version: Int) throws {
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL(dir).path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        defer { sqlite3_close_v2(db) }
        let sql = "UPDATE _meta SET value = '\(version)' WHERE key = 'schema_version';"
        #expect(sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK)
    }

    private func makeAsset(id: String, watermark: Double?) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: watermark,
            confirmedAdCoverageEndTime: nil,
            analysisState: "complete",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 3578.10
        )
    }

    private func makeChunk(assetId: String, index: Int, start: Double, end: Double) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-c\(index)",
            analysisAssetId: assetId,
            segmentFingerprint: "\(assetId)-fp\(index)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "segment \(index)",
            normalizedText: "segment \(index)",
            pass: "fast",
            modelVersion: "test-asr",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    @Test("fresh DB migrate() lands at v37")
    func freshDbReachesV37() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(AnalysisStore.currentSchemaVersion == 37)
    }

    /// THE MIGRATION EVIDENCE. An asset already on disk — written by a
    /// pre-0sro build, so its watermark is frozen at the 60 s shard
    /// boundary while its fast transcript covers the whole episode — must
    /// be repaired by simply reopening the store. Nothing re-transcribes
    /// a finished episode, so without this the row stays broken forever.
    @Test("v36 DB carrying a stale 60 s watermark is reconciled on reopen")
    func staleWatermarkRepairedByV37Upgrade() async throws {
        let (bootstrap, dir) = try await makeTestStoreWithDirectory()

        // Seed the THEMOVE shape: full fast transcript to 3575.88 s of a
        // 3578.10 s episode, behind a watermark stuck at 60 s.
        try await bootstrap.insertAsset(makeAsset(id: "m-stale", watermark: 60))
        var chunks: [TranscriptChunk] = []
        var start = 0.0
        var index = 0
        while start < 3540 {
            chunks.append(makeChunk(assetId: "m-stale", index: index, start: start, end: start + 30))
            start += 30
            index += 1
        }
        chunks.append(makeChunk(assetId: "m-stale", index: index, start: 3540, end: 3575.88))
        try await bootstrap.insertTranscriptChunks(chunks)

        // A second asset whose watermark is already honest — the migration
        // must not disturb it.
        try await bootstrap.insertAsset(makeAsset(id: "m-ok", watermark: 600))
        try await bootstrap.insertTranscriptChunks([
            makeChunk(assetId: "m-ok", index: 0, start: 0, end: 600)
        ])

        // The pre-migration state is exactly the bead's defect.
        #expect(try await bootstrap.fetchFastTranscriptCoverageEndTime(id: "m-stale") == 60)
        #expect(!FastTranscriptCoverageInvariant.holds(
            watermarkSec: 60,
            chunkMaxEndSec: 3575.88,
            episodeDurationSec: 3578.10
        ))

        try rewindSchemaVersion(in: dir, to: 36)

        // Reopen: the v36 → v37 step repairs persisted state with no
        // transcription, no playback, and no user action.
        AnalysisStore.resetMigratedPathsForTesting()
        let upgraded = try AnalysisStore(directory: dir)
        try await upgraded.migrate()

        #expect(try await upgraded.schemaVersion() == AnalysisStore.currentSchemaVersion)
        let repaired = try await upgraded.fetchFastTranscriptCoverageEndTime(id: "m-stale")
        #expect(repaired == 3575.88, "migration left the stale watermark at \(repaired ?? -1)")
        #expect(FastTranscriptCoverageInvariant.holds(
            watermarkSec: repaired,
            chunkMaxEndSec: 3575.88,
            episodeDurationSec: 3578.10
        ))
        #expect(try await upgraded.fetchFastTranscriptCoverageEndTime(id: "m-ok") == 600)
    }

    @Test("V37 migration is idempotent across reopens")
    func v37MigrationIsIdempotent() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset(id: "m-idem", watermark: 60))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "m-idem", index: 0, start: 0, end: 1800)
        ])

        try rewindSchemaVersion(in: dir, to: 36)
        AnalysisStore.resetMigratedPathsForTesting()
        let first = try AnalysisStore(directory: dir)
        try await first.migrate()
        let v1 = try await first.schemaVersion()
        #expect(try await first.fetchFastTranscriptCoverageEndTime(id: "m-idem") == 1800)

        AnalysisStore.resetMigratedPathsForTesting()
        try await first.migrate()
        let v2 = try await first.schemaVersion()

        #expect(v1 == AnalysisStore.currentSchemaVersion)
        #expect(v2 == AnalysisStore.currentSchemaVersion)
        #expect(try await first.fetchFastTranscriptCoverageEndTime(id: "m-idem") == 1800)
    }
}
