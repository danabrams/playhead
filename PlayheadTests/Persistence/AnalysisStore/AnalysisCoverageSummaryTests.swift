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

    // MARK: - playhead-pz32: unionedSecondsIntersecting

    /// The canonical case: ONE wide span against a gappy bound set. Bare union
    /// would say 3600; the intersection says 120.
    @Test("(pz32) one wide interval against gappy bounds yields only the covered part")
    func intersectionOfWideSpanWithGappyBounds() {
        let result = AnalysisCoverageMath.unionedSecondsIntersecting(
            [(start: 0, end: 3600)],
            within: [(start: 0, end: 60), (start: 3540, end: 3600)]
        )
        #expect(result == 120)
    }

    @Test("(pz32) empty bounds or empty intervals yield zero")
    func intersectionWithEmptySideIsZero() {
        #expect(AnalysisCoverageMath.unionedSecondsIntersecting([(start: 0, end: 10)], within: []) == 0)
        #expect(AnalysisCoverageMath.unionedSecondsIntersecting([], within: [(start: 0, end: 10)]) == 0)
        #expect(AnalysisCoverageMath.unionedSecondsIntersecting([], within: []) == 0)
    }

    @Test("(pz32) overlapping intervals on BOTH sides are de-overlapped, not double-counted")
    func intersectionDeOverlapsBothSides() {
        // Left union = [0,100]; right union = [50,150]; intersection = [50,100].
        let result = AnalysisCoverageMath.unionedSecondsIntersecting(
            [(start: 0, end: 80), (start: 40, end: 100)],
            within: [(start: 50, end: 120), (start: 90, end: 150)]
        )
        #expect(result == 50)
    }

    @Test("(pz32) disjoint interval and bound sets interleave correctly")
    func intersectionInterleaves() {
        // [0,10]∩[5,20]=5, [30,40]∩[35,50]=5, [100,110] has no bound → 0.
        let result = AnalysisCoverageMath.unionedSecondsIntersecting(
            [(start: 0, end: 10), (start: 30, end: 40), (start: 100, end: 110)],
            within: [(start: 5, end: 20), (start: 35, end: 50)]
        )
        #expect(result == 10)
    }

    @Test("(pz32) touching-only intervals contribute zero (no negative-width overlap)")
    func intersectionOfTouchingIsZero() {
        let result = AnalysisCoverageMath.unionedSecondsIntersecting(
            [(start: 0, end: 10)],
            within: [(start: 10, end: 20)]
        )
        #expect(result == 0)
    }

    @Test("(pz32) non-finite and degenerate endpoints are dropped on both sides")
    func intersectionDropsNonFiniteAndDegenerate() {
        let result = AnalysisCoverageMath.unionedSecondsIntersecting(
            [(start: 0, end: 100), (start: .nan, end: 200), (start: 50, end: 50), (start: 90, end: 10)],
            within: [(start: 0, end: 100), (start: .infinity, end: 500), (start: 20, end: .nan)]
        )
        #expect(result == 100)
        #expect(result.isFinite)
    }

    /// Core invariant: the intersection is a subset of BOTH sides, so it can
    /// never exceed either union. Swept over many bound windows so an
    /// off-by-one in the linear merge sweep shows up as an overshoot.
    @Test("(pz32) intersection never exceeds either side's union")
    func intersectionNeverExceedsEitherUnion() {
        let intervals: [(start: Double, end: Double)] = [
            (start: 0, end: 140),
            (start: 300, end: 390),
            (start: 500, end: 560),
            (start: 900, end: 1000)
        ]
        let leftUnion = AnalysisCoverageMath.unionedSeconds(intervals)
        for width in stride(from: 1.0, through: 400.0, by: 13.0) {
            for offset in stride(from: -50.0, through: 1100.0, by: 71.0) {
                let bounds = [
                    (start: offset, end: offset + width),
                    (start: offset + width * 2, end: offset + width * 3)
                ]
                let rightUnion = AnalysisCoverageMath.unionedSeconds(bounds)
                let intersection = AnalysisCoverageMath.unionedSecondsIntersecting(
                    intervals, within: bounds
                )
                #expect(intersection >= 0)
                #expect(intersection <= leftUnion + 1e-9,
                        "intersection \(intersection) exceeded left union \(leftUnion)")
                #expect(intersection <= rightUnion + 1e-9,
                        "intersection \(intersection) exceeded right union \(rightUnion)")
            }
        }
    }

    /// The BOUNDS side must be de-overlapped too. Without merging the right side,
    /// overlapping bounds double-count the shared region — and overlapping fast
    /// chunks are an expected shape (see the `a-overlap` fixture in the sibling
    /// suite).
    @Test("(pz32) overlapping BOUNDS are de-overlapped, not double-counted")
    func intersectionDeOverlapsBoundsSide() {
        // Bounds union is [10,90] → 80 seconds of [0,100] survive, not 10+75=85.
        let result = AnalysisCoverageMath.unionedSecondsIntersecting(
            [(start: 0, end: 100)],
            within: [(start: 10, end: 20), (start: 15, end: 90)]
        )
        #expect(result == 80)
    }

    /// A brute-force differential oracle for the MULTI-BOUND case, which the
    /// hand-computed points cannot cover exhaustively: sample both interval sets on
    /// a fine grid and count the cells inside both. Randomised over many shapes,
    /// including deliberately overlapping intervals on both sides, so a lost
    /// de-overlap or a skipped pair shows up as a mismatch rather than as a
    /// still-satisfied inequality.
    @Test("(pz32) intersection matches a brute-force grid oracle over random shapes")
    func intersectionMatchesGridOracle() {
        let step = 0.25
        // Must exceed the largest endpoint the generator can produce (179 + 60),
        // or the oracle silently truncates coverage the function counts.
        let limit = 260.0

        func gridSeconds(
            _ a: [(start: Double, end: Double)],
            _ b: [(start: Double, end: Double)]
        ) -> Double {
            var covered = 0
            var t = 0.0
            while t < limit {
                let mid = t + step / 2
                let inA = a.contains { mid > $0.start && mid < $0.end }
                let inB = b.contains { mid > $0.start && mid < $0.end }
                if inA && inB { covered += 1 }
                t += step
            }
            return Double(covered) * step
        }

        // Deterministic pseudo-random so a failure is reproducible.
        var seed: UInt64 = 0x5EED_1234
        func next(_ bound: Int) -> Int {
            seed = seed &* 6364136223846793005 &+ 1442695040888963407
            return Int((seed >> 33) % UInt64(bound))
        }
        func randomSet() -> [(start: Double, end: Double)] {
            (0..<(1 + next(5))).map { _ in
                let start = Double(next(180))
                return (start: start, end: start + Double(1 + next(60)))
            }
        }

        for _ in 0..<200 {
            let left = randomSet()
            let right = randomSet()
            let actual = AnalysisCoverageMath.unionedSecondsIntersecting(left, within: right)
            let expected = gridSeconds(left, right)
            #expect(
                abs(actual - expected) < step,
                "intersection \(actual) != oracle \(expected) for left=\(left) right=\(right)"
            )
        }
    }

    // MARK: - playhead-pz32: bridgingShortGaps

    @Test("(pz32) bridging coalesces gaps at or under the width and no wider")
    func bridgingCoalescesShortGapsOnly() {
        let intervals: [(start: Double, end: Double)] = [
            (start: 0, end: 10),
            (start: 13, end: 20),   // 3 s gap  → bridged at 5
            (start: 25, end: 30),   // 5 s gap  → bridged at 5 (boundary, inclusive)
            (start: 40, end: 50)    // 10 s gap → NOT bridged
        ]
        let bridged = AnalysisCoverageMath.bridgingShortGaps(intervals, upTo: 5)
        #expect(bridged.count == 2)
        #expect(bridged[0] == (start: 0, end: 30))
        #expect(bridged[1] == (start: 40, end: 50))
        // Seconds gained are exactly the bridged gaps (3 + 5), never more.
        #expect(AnalysisCoverageMath.unionedSeconds(bridged)
            == AnalysisCoverageMath.unionedSeconds(intervals) + 8)
    }

    @Test("(pz32) bridging at zero is the plain union; a negative width is treated as zero")
    func bridgingAtZeroIsPlainUnion() {
        let intervals: [(start: Double, end: Double)] = [
            (start: 0, end: 10), (start: 11, end: 20)
        ]
        for width: BridgeToleranceSec in [0.0, -1.0, BridgeToleranceSec(-.infinity)] {
            let bridged = AnalysisCoverageMath.bridgingShortGaps(intervals, upTo: width)
            #expect(AnalysisCoverageMath.unionedSeconds(bridged)
                == AnalysisCoverageMath.unionedSeconds(intervals))
            #expect(bridged.count == 2)
        }
    }

    @Test("(pz32) bridging drops non-finite and degenerate intervals")
    func bridgingDropsJunk() {
        let bridged = AnalysisCoverageMath.bridgingShortGaps([
            (start: 0, end: 10),
            (start: .nan, end: 50),
            (start: 20, end: 20),
            (start: 90, end: 10),
            (start: 12, end: 18)
        ], upTo: 5)
        #expect(bridged.count == 1)
        #expect(bridged[0] == (start: 0, end: 18))
    }

    /// Bridging must never invent coverage OUTSIDE the span of the inputs — it
    /// fills interior gaps only, so the first start and last end are preserved.
    @Test("(pz32) bridging preserves the outer span")
    func bridgingPreservesOuterSpan() {
        let intervals: [(start: Double, end: Double)] = [
            (start: 7, end: 10), (start: 12, end: 20), (start: 100, end: 110)
        ]
        let bridged = AnalysisCoverageMath.bridgingShortGaps(intervals, upTo: 1_000)
        #expect(bridged.count == 1)
        #expect(bridged[0] == (start: 7, end: 110))
        #expect(AnalysisCoverageMath.unionedSeconds(bridged) == 103)
    }

    /// Intersecting with a single `[0, upperBound]` bound must agree exactly with
    /// `unionedSecondsClipped` — two routes to the same clip.
    @Test("(pz32) intersecting with [0, bound] equals unionedSecondsClipped")
    func intersectionAgreesWithClippedForSingleBound() {
        let intervals: [(start: Double, end: Double)] = [
            (start: 0, end: 140),
            (start: 300, end: 390),
            (start: 900, end: 1000)
        ]
        for upperBound in stride(from: 1.0, through: 1200.0, by: 37.0) {
            let clipped = AnalysisCoverageMath.unionedSecondsClipped(
                intervals, upperBound: upperBound
            )
            let intersected = AnalysisCoverageMath.unionedSecondsIntersecting(
                intervals, within: [(start: 0, end: upperBound)]
            )
            #expect(abs(clipped - intersected) < 1e-9,
                    "clip \(clipped) != intersect \(intersected) at \(upperBound)")
        }
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
        // playhead-x0lb R1: an AnalyzedSeconds against a CoveredSeconds — two
        // different areas, so the comparison is in raw values by hand. That is
        // the boundary the split exists to make visible.
        #expect(analyzed.rawValue <= transcript.rawValue)
        // Both fractions land at 39% — NOT the old 100%.
        let duration = try #require(summary.episodeDurationSec)
        #expect(analyzed.rawValue / duration.rawValue == 390.0 / 1000.0)
        #expect(transcript.rawValue / duration.rawValue == 390.0 / 1000.0)
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
        #expect((summary.analysisCoveredSec ?? 0).rawValue < (summary.fastTranscriptCoveredSec ?? 0).rawValue)
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
        #expect((ahead.analysisCoveredSec ?? 0).rawValue <= (ahead.fastTranscriptCoveredSec ?? 0).rawValue)

        let behind = try #require(summaries["a-sd71-wm-behind"])
        #expect(behind.fastTranscriptCoveredSec == 300)
        #expect(behind.analysisCoveredSec == 120) // min(300, 120)
        #expect((behind.analysisCoveredSec ?? 0).rawValue <= (behind.fastTranscriptCoveredSec ?? 0).rawValue)
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
        #expect(summary.fastTranscriptCoverageEndSec?.rawValue == chunkMax.maxEndTimeSec)
        #expect(summary.fastTranscriptCoverageEndSource == .fastTranscriptChunks)
        // The 90s watermark must NOT be the surfaced value.
        #expect(summary.fastTranscriptCoverageEndSec?.rawValue != watermark)
        // Bead spec: the asset_004 chunk maxima are ~3959.77s (66 min).
        // Pin the order of magnitude so a future fixture regenerate
        // that subtly weakens the signal can't quietly slip past.
        #expect((summary.fastTranscriptCoverageEndSec ?? 0).rawValue > 3000,
                "expected chunk-derived coverage to clear 50 minutes; got \(summary.fastTranscriptCoverageEndSec ?? 0)s")
        #expect(summary.fastTranscriptCoveredSec?.rawValue == chunkMax.maxEndTimeSec)
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
        // A badly gapped transcript: [0,600] and [3000,3575.88], a
        // 40-minute hole in the middle. Reach is still 3575.88, so the
        // invariant must be satisfied — flagging this would re-introduce
        // the sd71 watermark-vs-union conflation.
        let gappy: [(start: Double, end: Double)] = [
            (start: 0, end: 600),
            (start: 3000, end: 3575.88)
        ]
        let chunkMaxEnd = gappy.map(\.end).max()
        #expect(chunkMaxEnd == 3575.88)
        #expect(FastTranscriptCoverageInvariant.holds(
            watermarkSec: chunkMaxEnd,
            chunkMaxEndSec: chunkMaxEnd,
            episodeDurationSec: 3578.10
        ))
        // The AREA measure is the one that must report the hole, and it
        // is a different number entirely: 600 + 575.88.
        let area = AnalysisCoverageMath.unionedSeconds(gappy)
        #expect(abs(area - 1175.88) < 0.0001)
        #expect(area / 3578.10 < 0.34, "a 33%-covered episode must not look complete by AREA")
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

    /// The returned flag is what makes "no-op" observable — a blind
    /// setter would report a write for all three of these calls.
    @Test("advance reports whether the watermark actually moved")
    func advanceReportsWhetherItMoved() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-eq"))
        #expect(try await store.advanceFastTranscriptCoverage(id: "a-eq", endTime: 900) == true)
        #expect(try await store.advanceFastTranscriptCoverage(id: "a-eq", endTime: 900) == false,
                "an equal value is not an advance")
        #expect(try await store.advanceFastTranscriptCoverage(id: "a-eq", endTime: 60) == false,
                "a lower value is not an advance")
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "a-eq") == 900)
    }

    /// Under a monotonic contract a poisoned high value is PERMANENT —
    /// the old blind setter self-healed on the next shard write, this one
    /// cannot. So non-finite and negative inputs must be rejected at the
    /// door rather than persisted.
    @Test("advance rejects non-finite and negative endTimes")
    func advanceRejectsNonFiniteAndNegative() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-poison", fastTranscriptCoverageEndTime: 600))
        #expect(try await store.advanceFastTranscriptCoverage(id: "a-poison", endTime: .infinity) == false)
        #expect(try await store.advanceFastTranscriptCoverage(id: "a-poison", endTime: .nan) == false)
        #expect(try await store.advanceFastTranscriptCoverage(id: "a-poison", endTime: -1) == false)
        #expect(try await store.fetchFastTranscriptCoverageEndTime(id: "a-poison") == 600,
                "a poisoned watermark would be unrepairable — nothing may lower it")
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

    /// Both "row has no chunks to raise from" and "no row at all" are
    /// handled; the second is asserted against the row's real absence
    /// rather than against the reconcile return alone, because `nil` is
    /// documented to mean "no value" in both cases.
    @Test("reconcile is a no-op without fast chunks and tolerates an unknown asset")
    func reconcileNoOpAndUnknownAsset() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-empty", fastTranscriptCoverageEndTime: 120))
        #expect(try await store.reconcileFastTranscriptCoverage(id: "a-empty") == 120)

        // A row that exists but was never written keeps its NULL.
        try await store.insertAsset(makeAsset(id: "a-nullwm", fastTranscriptCoverageEndTime: nil))
        #expect(try await store.reconcileFastTranscriptCoverage(id: "a-nullwm") == nil)
        #expect(try await store.fetchAsset(id: "a-nullwm") != nil)

        // An absent row must not be created as a side effect.
        #expect(try await store.reconcileFastTranscriptCoverage(id: "a-missing") == nil)
        #expect(try await store.fetchAsset(id: "a-missing") == nil)
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
        // Drift guard, pinned to the LITERAL head (48 → 49, playhead-mn5e/2qz6's
        // `trust_episode_observations` ledger + the `observationCount` reset).
        // Never `== currentSchemaVersion`: that passes for every value and stops
        // policing anything.
        //
        // Read for THIS suite: the V37 watermark reconciliation this file
        // exercises derives `fastTranscriptCoverageEndTime` from
        // `transcript_chunks`. V49 writes only `podcast_profiles` and a new
        // table, so it cannot move a coverage watermark in either direction.
        // …and 49 → 50 (playhead-e6d3), whose only statement UPDATEs
        // `backfill_jobs.retryCount`, so it cannot move a coverage watermark in
        // either direction either.
        // 50 → 51 read for this rung (playhead-wogi): V51 reads the same
        // `passA` / `didExamineWindow` population this file's coverage numerator
        // is computed from, and writes only `backfill_jobs.progressCursor`. The
        // summary quantities are unaffected in both directions.
        // 52 → 53 read for this rung (playhead-jc42): THIS IS THE FIRST RUNG IN
        // A WHILE THAT WRITES `transcript_chunks`, which is exactly the table
        // this file's `fastTranscriptCoverageEndTime` derives from — so the
        // "names a different table" argument above does not apply and the claim
        // has to be made on the DELETE's own shape.
        //
        // V53 removes a row only when a row with the SAME
        // `(analysisAssetId, pass, startTime, endTime, text)` survives. So every
        // interval it removes is still present, byte-for-byte, on the survivor:
        // no `MAX(endTime)` can move (the maximum is attained by the survivor
        // too), and no interval union can shrink (the union is idempotent under
        // duplication — `AnalysisCoverageMath.unionedSeconds` and the
        // `FastTranscriptRegion` / `FinalTranscriptRegion` readers all merge
        // before measuring). The deletion is coverage-preserving by
        // construction, in BOTH directions, which is stronger than "it happens
        // not to fire on these fixtures".
        //
        // 60 -> 61 read for this rung (playhead-iw7q): V61 ADDS ONE NULLABLE
        // COLUMN to `semantic_scan_results` and writes nothing to it — no
        // UPDATE, no DEFAULT, no row touched. So it cannot move a coverage
        // watermark, and it cannot move the `passA` / `didExamineWindow`
        // population this file's numerator is taken over either: those are
        // functions of `scanPass`, `status` and `errorContext`, none of which
        // it reads or writes. The claim holds in both directions because the
        // rung's whole body is `addColumnIfNeeded`.
        #expect(AnalysisStore.currentSchemaVersion == 61)
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

    /// The migration is VERSION-GATED — a one-time repair, not a
    /// whole-table scan on every cold start. This pins that, and by doing
    /// so makes the `rewindSchemaVersion(in:to: 36)` in the upgrade test
    /// above load-bearing rather than decorative. Ongoing correctness
    /// comes from the monotonic advance plus the per-asset reconciles at
    /// both pipeline entry points and at finalization, not from re-running
    /// this sweep forever.
    @Test("a DB already at v37 does not re-run the whole-table sweep on reopen")
    func v37MigrationDoesNotRescanAtHead() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset(id: "m-head", watermark: 60))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: "m-head", index: 0, start: 0, end: 1800)
        ])
        // Deliberately NOT rewinding the schema version: the DB is at head.
        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        #expect(try await reopened.fetchFastTranscriptCoverageEndTime(id: "m-head") == 60,
                "the v37 step must be gated on schema version; a head DB must not pay for the sweep again")

        // …and the per-asset reconcile is what repairs it from here on.
        #expect(try await reopened.reconcileFastTranscriptCoverage(id: "m-head") == 1800)
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

        // `migrate()` is `ensureOpen()`, which short-circuits on an
        // already-open handle — so re-running it on `first` would prove
        // nothing. Construct a SECOND store over the same file (with the
        // process-global migration cache cleared) so the ladder genuinely
        // re-enters against a v37 database.
        AnalysisStore.resetMigratedPathsForTesting()
        let second = try AnalysisStore(directory: dir)
        try await second.migrate()
        let v2 = try await second.schemaVersion()

        #expect(v1 == AnalysisStore.currentSchemaVersion)
        #expect(v2 == AnalysisStore.currentSchemaVersion)
        #expect(try await second.fetchFastTranscriptCoverageEndTime(id: "m-idem") == 1800)
    }
}

// MARK: - playhead-pz32: semantic ad-scan coverage

/// playhead-pz32: `AnalysisCoverageSummary.adScanCoveredSec` is the ONLY
/// persisted quantity that answers "how much of this episode has been read for
/// ads?". These tests pin it against the three quantities it replaced in the
/// library readiness predicate — the DSP feature watermark, `max(endTime)` of
/// detected ad windows, and the transcript-clipped analyzed area — every one of
/// which can sit at 100% while the semantic scan has barely started.
@Suite("AnalysisStore ad-scan coverage (playhead-pz32)")
struct AnalysisStoreAdScanCoverageTests {

    /// `insertSemanticScanResult` rejects a `scanCohortJSON` that is not a
    /// decodable `ScanCohort`, so the fixtures must carry a real one.
    private static let cohortJSON: String = {
        let cohort = ScanCohort(
            promptLabel: "pz32-test",
            promptHash: "prompt-v1",
            schemaHash: "schema-v1",
            scanPlanHash: "plan-v1",
            normalizationHash: "norm-v1",
            osBuild: "26A123",
            locale: "en_US",
            appBuild: "1"
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        // Pure value types — encoding cannot fail.
        let data = (try? encoder.encode(cohort)) ?? Data()
        return String(decoding: data, as: UTF8.self)
    }()

    /// `fastTranscriptCoverageEndTime` defaults to the full episode duration —
    /// a completely transcribed episode, the case in which the ad-scan area is
    /// bounded only by the scan windows themselves. Tests that need a gappy or
    /// over-reaching transcript pass their own value (or insert real chunks).
    private func makeAsset(
        id: String,
        episodeDurationSec: Double? = 3600,
        featureCoverageEndTime: Double? = nil,
        fastTranscriptCoverageEndTime: Double? = nil,
        confirmedAdCoverageEndTime: Double? = nil,
        analysisState: String = "backfill"
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///\(id).m4a",
            featureCoverageEndTime: featureCoverageEndTime,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime ?? episodeDurationSec,
            confirmedAdCoverageEndTime: confirmedAdCoverageEndTime,
            analysisState: analysisState,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec
        )
    }

    private func makeScan(
        assetId: String,
        index: Int,
        start: Double,
        end: Double,
        status: SemanticScanStatus = .success,
        scanPass: String = SemanticScanCoverage.coverageScanPass,
        errorContext: String? = nil
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: "\(assetId)-scan-\(scanPass)-\(index)",
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: index * 10,
            windowLastAtomOrdinal: index * 10 + 9,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: scanPass,
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: status,
            attemptCount: 1,
            errorContext: errorContext,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: Self.cohortJSON,
            transcriptVersion: "tx-v1",
            reuseScope: "\(assetId)-\(scanPass)-\(index)"
        )
    }

    // MARK: - playhead-gqx4: the narrow, lenient status projection

    /// `fetchSemanticScanStatuses` is the read the terminal classifier uses to
    /// name WHY a scan is short. It must return only the requested pass, and it
    /// must be a narrow `SELECT status` — not a whole-row decode.
    ///
    /// The narrowness is the point. `fetchSemanticScanResults` decodes full rows
    /// through a strict reader that throws on any unrecognised
    /// `status`/`transcriptQuality`/`disposition`, so ONE row from a newer build
    /// would abort the call for the whole asset. Since the fraction read
    /// (`fetchCoverageSummariesByAssetIds`) is already lenient in exactly this
    /// way, a strict second read of the same table would let a cosmetic term
    /// veto the load-bearing one.
    @Test("coverage-lane status projection returns only that pass's statuses")
    func scanStatusProjectionIsPassScoped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-statuses", episodeDurationSec: 1000))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-statuses", index: 0, start: 0, end: 100, status: .success)
        )
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-statuses", index: 1, start: 100, end: 200, status: .refusal)
        )
        // A passB row must not contribute — the coverage lane is passA, and
        // passB rows are localized extent attempts inside already-screened
        // windows.
        try await store.insertSemanticScanResult(
            makeScan(
                assetId: "a-statuses", index: 2, start: 0, end: 50,
                status: .guardrailViolation, scanPass: "passB"
            )
        )

        let statuses = try await store.fetchSemanticScanStatuses(
            analysisAssetId: "a-statuses",
            scanPass: SemanticScanCoverage.coverageScanPass
        )
        #expect(statuses.count == 2)
        #expect(Set(statuses.compactMap { $0 }) == [.success, .refusal])
        // And the classifier reduces them to the most explanatory cause.
        #expect(
            AnalysisCoordinator.adScanLimit(coverageLaneStatuses: statuses) == .refusal
        )

        // An asset with no coverage-lane rows reports an empty list, which the
        // classifier reads as "the scan never ran" — distinct from "it ran and
        // found nothing to say".
        try await store.insertAsset(makeAsset(id: "a-no-scan", episodeDurationSec: 1000))
        let none = try await store.fetchSemanticScanStatuses(
            analysisAssetId: "a-no-scan",
            scanPass: SemanticScanCoverage.coverageScanPass
        )
        #expect(none.isEmpty)
        #expect(AnalysisCoordinator.adScanLimit(coverageLaneStatuses: none) == .neverRan)
    }

    /// THE BEAD'S FIXTURE (asset 820134BF): DSP feature extraction swept the
    /// whole episode and a late ad detection parked `confirmedAdCoverageEndTime`
    /// at the end, but only 47% of the audio was ever screened for ads. The old
    /// predicate's `max(feature, confirmedAd) / duration` reads 1.0 here; the
    /// ad-scan fraction reads 0.47 and must NOT clear the 0.98 threshold.
    @Test("full DSP watermark + late ad detection: ad-scan fraction stays at the real 47%")
    func fullWatermarkLowScanCoverage() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-820134bf",
            episodeDurationSec: 3600,
            featureCoverageEndTime: 3600,
            fastTranscriptCoverageEndTime: 3600,
            confirmedAdCoverageEndTime: 3580
        ))
        // 1692s of 3600s examined == 47%.
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-820134bf", index: 0, start: 0, end: 1692)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-820134bf"])
        let summary = try #require(summaries["a-820134bf"])

        // Both discredited arms are at (or near) full.
        #expect(summary.featureCoverageEndSec == 3600)
        #expect(summary.confirmedAdCoverageEndSec == 3580)
        // The honest quantity is not.
        #expect(summary.adScanCoveredSec == 1692)
        #expect(summary.adScanCoveredSource == .semanticScanResults)
        let fraction = try #require(summary.adScanFraction)
        #expect(abs(fraction - 0.47) < 0.0001)
        #expect(fraction < episodePreparationCompleteThreshold)
    }

    /// Only windows that produced a VERDICT count. A refused / guardrailed /
    /// cancelled window is not audio we screened and found clean — counting it
    /// is exactly how a truncated scan reports itself as complete.
    @Test("refused / guardrailed / cancelled windows do not count as scanned")
    func onlyExaminedWindowsCount() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-refusals", episodeDurationSec: 1000))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-refusals", index: 0, start: 0, end: 100, status: .success)
        )
        try await store.insertSemanticScanResult(
            // `.noAds` is the permissive path's "I looked, nothing here" — a
            // real examination despite living in the failure-accounting list.
            makeScan(assetId: "a-refusals", index: 1, start: 100, end: 200, status: .noAds)
        )
        for (index, status) in [
            SemanticScanStatus.refusal,
            .guardrailViolation,
            .cancelled,
            .thermalDeferred,
            .exceededContextWindow,
            .permissiveRefusal
        ].enumerated() {
            try await store.insertSemanticScanResult(makeScan(
                assetId: "a-refusals",
                index: 10 + index,
                start: 200 + Double(index) * 100,
                end: 300 + Double(index) * 100,
                status: status
            ))
        }

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-refusals"])
        let summary = try #require(summaries["a-refusals"])
        // 0–200 examined; 200–800 attempted but never read.
        #expect(summary.adScanCoveredSec == 200)
        #expect(summary.adScanFraction == 0.2)
    }

    /// Overlapping / duplicated windows must union, not sum — otherwise a
    /// re-scanned region inflates coverage past what was actually read (and a
    /// heavily retried episode would be the first to claim readiness).
    @Test("overlapping scan windows union rather than sum")
    func overlappingWindowsUnion() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-overlap-scan", episodeDurationSec: 1000))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-overlap-scan", index: 0, start: 0, end: 400)
        )
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-overlap-scan", index: 1, start: 200, end: 500)
        )
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-overlap-scan", index: 2, start: 100, end: 300)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-overlap-scan"])
        let summary = try #require(summaries["a-overlap-scan"])
        // Sum of widths would be 400 + 300 + 200 = 900; union is 500.
        #expect(summary.adScanCoveredSec == 500)
        #expect(summary.adScanFraction == 0.5)
    }

    /// `passB` rows are localized extent attempts INSIDE already-screened
    /// `passA` windows. Counting them would double-count, and a `passB`-only
    /// asset has no coverage-lane evidence at all.
    @Test("passB rows are excluded from the coverage lane")
    func passBRowsExcluded() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-passb", episodeDurationSec: 1000))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-passb", index: 0, start: 0, end: 100)
        )
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-passb", index: 1, start: 500, end: 900, scanPass: "passB")
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-passb"])
        let summary = try #require(summaries["a-passb"])
        #expect(summary.adScanCoveredSec == 100)

        // passB-only: no coverage-lane row at all → unknown, not a synthetic 0.
        try await store.insertAsset(makeAsset(id: "a-passb-only", episodeDurationSec: 1000))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-passb-only", index: 0, start: 0, end: 900, scanPass: "passB")
        )
        let onlyB = try await store.fetchCoverageSummariesByAssetIds(["a-passb-only"])
        let bSummary = try #require(onlyB["a-passb-only"])
        #expect(bSummary.adScanCoveredSec == nil)
        #expect(bSummary.adScanCoveredSource == .unknown)
        #expect(bSummary.adScanFraction == nil)
    }

    /// Unknown vs measured-zero must stay distinguishable in the provenance
    /// tag, and BOTH must produce a nil-or-zero fraction that reads not-ready.
    @Test("no scan rows → unknown; rows that all refused → measured zero")
    func unknownVersusMeasuredZero() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-noscan",
            episodeDurationSec: 1000,
            featureCoverageEndTime: 1000,
            confirmedAdCoverageEndTime: 1000,
            analysisState: "completeFull"
        ))
        try await store.insertAsset(makeAsset(id: "a-allrefused", episodeDurationSec: 1000))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-allrefused", index: 0, start: 0, end: 900, status: .refusal)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds([
            "a-noscan", "a-allrefused"
        ])
        let noScan = try #require(summaries["a-noscan"])
        #expect(noScan.adScanCoveredSec == nil)
        #expect(noScan.adScanCoveredSource == .unknown)
        #expect(noScan.adScanFraction == nil)
        // A fully-swept DSP watermark and a `completeFull` terminal buy nothing.
        #expect(noScan.featureCoverageEndSec == 1000)

        let allRefused = try #require(summaries["a-allrefused"])
        #expect(allRefused.adScanCoveredSec == 0)
        #expect(allRefused.adScanCoveredSource == .semanticScanResults)
        #expect(allRefused.adScanFraction == 0)
    }

    /// A missing / zero / non-positive duration makes the fraction unmeasurable.
    /// It must be `nil` (→ not ready), never a divide-by-zero or a synthetic 1.
    @Test("unknown or zero duration → nil fraction, never a claim of readiness")
    func unmeasurableDurationYieldsNilFraction() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-nodur", episodeDurationSec: nil, fastTranscriptCoverageEndTime: 900
        ))
        try await store.insertAsset(makeAsset(
            id: "a-zerodur", episodeDurationSec: 0, fastTranscriptCoverageEndTime: 900
        ))
        for id in ["a-nodur", "a-zerodur"] {
            try await store.insertSemanticScanResult(
                makeScan(assetId: id, index: 0, start: 0, end: 900)
            )
        }

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-nodur", "a-zerodur"])
        for id in ["a-nodur", "a-zerodur"] {
            let summary = try #require(summaries[id])
            #expect(summary.adScanCoveredSec == 900)
            #expect(summary.adScanFraction == nil, "\(id) must not synthesise a fraction")
            #expect(!episodePreparationAnalysisComplete(
                status: .done, adScanFraction: summary.adScanFraction, isDegradedTerminal: false
            ))
        }
    }

    /// A scan window overrunning the declared duration by LESS than one shard is
    /// ordinary feed-vs-measured drift: clamp to 1.0.
    @Test("scan coverage a fraction past the declared duration clamps to 1.0")
    func coveragePastDurationClamps() async throws {
        let store = try await makeTestStore()
        // 510s scanned against a declared 500s — 10s of drift, well under a shard.
        try await store.insertAsset(makeAsset(
            id: "a-over", episodeDurationSec: 500, fastTranscriptCoverageEndTime: 510
        ))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-over", index: 0, start: 0, end: 510)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-over"])
        let summary = try #require(summaries["a-over"])
        #expect(summary.adScanCoveredSec == 510)
        #expect(summary.adScanFraction == 1)
    }

    /// A coverage:duration ratio ABOVE 1 by more than one shard means the two
    /// numbers describe different audio — the 2026-04-27 libsyn/flightcast shape
    /// (704s declared for ~9,700s of real audio, playhead-csbq). Clamping that to
    /// exactly 1.0 would turn a broken denominator into a confident ✓ on ~10% of
    /// the episode, so it must read as UNMEASURABLE instead.
    @Test("a poisoned duration denominator reads unmeasurable, not 100%")
    func poisonedDurationIsUnmeasurable() async throws {
        let store = try await makeTestStore()
        // Declared 704s; the transcript and the scan both reach 970s.
        try await store.insertAsset(makeAsset(
            id: "a-poisoned", episodeDurationSec: 704, fastTranscriptCoverageEndTime: 970
        ))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-poisoned", index: 0, start: 0, end: 970)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-poisoned"])
        let summary = try #require(summaries["a-poisoned"])
        #expect(summary.adScanCoveredSec == 970)
        // 970/704 = 1.378 — a clamp would say "100% scanned"; the honest answer
        // is that this episode cannot be measured.
        #expect(summary.adScanFraction == nil)
        #expect(!episodePreparationAnalysisComplete(
            status: .done, adScanFraction: summary.adScanFraction, isDegradedTerminal: false
        ))
    }

    /// The overshoot tolerance must be RELATIVE. An absolute one-shard allowance
    /// is 0.8% of an hour but 67% of a 45-second trailer, so a short episode whose
    /// real audio runs 64% longer than declared would pass an absolute check and
    /// render a confident ✓ on a denominator that is wildly short — the same
    /// failure, relocated to the short end.
    @Test("the duration tolerance scales with the episode, so a short one cannot cheat")
    func durationToleranceIsRelative() async throws {
        // The pure helper first: a 45 s episode gets 2.25 s of slack, not 30 s.
        #expect(AnalysisCoverageSummary.adScanDurationToleranceSec(episodeDurationSec: 45) == 2.25)
        // Long episodes stay capped at one shard rather than acquiring 5% of an hour.
        #expect(
            AnalysisCoverageSummary.adScanDurationToleranceSec(episodeDurationSec: 3600)
                == AnalysisAudioService.defaultShardDuration
        )

        let store = try await makeTestStore()
        // A 45 s trailer whose real audio (and transcript) runs 74 s.
        try await store.insertAsset(makeAsset(
            id: "a-trailer", episodeDurationSec: 45, fastTranscriptCoverageEndTime: 74
        ))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-trailer", index: 0, start: 0, end: 74)
        )
        let trailer = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-trailer"])["a-trailer"]
        )
        #expect(trailer.adScanCoveredSec == 74)
        // 74 > 45 + 2.25 → unmeasurable. An absolute 30 s tolerance would have
        // clamped 74/45 to 1.0 and lit the ✓.
        #expect(trailer.adScanFraction == nil)

        // Sub-tolerance drift on the same short episode still clamps to 1.
        try await store.insertAsset(makeAsset(
            id: "a-trailer-ok", episodeDurationSec: 45, fastTranscriptCoverageEndTime: 47
        ))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-trailer-ok", index: 0, start: 0, end: 47)
        )
        let ok = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-trailer-ok"])["a-trailer-ok"]
        )
        #expect(ok.adScanFraction == 1)
    }

    /// The RATIO alone cannot detect a denominator that is wrong by a large
    /// factor: when the declared duration is a seventh of the real audio, a scan
    /// that covered roughly `declaredDuration` seconds yields ~1.0 and is
    /// indistinguishable from a legitimately-complete short episode.
    ///
    /// This is asset E8F0F867 from the 2026-04-25 device capture, verbatim:
    /// declared 552.9 s, fast transcript reaching 3,810 s, ad scan 563.8 s. The
    /// overshoot guard passes it (563.8 is within 5% of 552.9) and it would light
    /// a confident ✓ on 14.8% of the audio. The transcript's own reach is the
    /// witness that the denominator is wrong.
    @Test("a duration contradicted by the transcript's reach is unmeasurable, not 100%")
    func durationContradictedByTranscriptReachIsUnmeasurable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-e8f0f867",
            episodeDurationSec: 552.9,
            fastTranscriptCoverageEndTime: 3810
        ))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-e8f0f867", index: 0, start: 0, end: 563.8)
        )

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-e8f0f867"])["a-e8f0f867"]
        )
        #expect(summary.adScanCoveredSec == 563.8)
        // 563.8 / 552.9 = 1.0197 — inside the 5% overshoot tolerance, so the ratio
        // check alone would clamp it to 1.0 and light the ✓.
        #expect(563.8 <= 552.9 + AnalysisCoverageSummary
            .adScanDurationToleranceSec(episodeDurationSec: 552.9))
        #expect(summary.adScanFraction == nil)
        #expect(!episodePreparationAnalysisComplete(
            status: .done, adScanFraction: summary.adScanFraction, isDegradedTerminal: false
        ))

        // Once the duration is repaired to the real length, the SAME rows measure
        // honestly again — the guard withholds a number, it does not blacklist an
        // episode.
        try await store.insertAsset(makeAsset(
            id: "a-repaired",
            episodeDurationSec: 3810,
            fastTranscriptCoverageEndTime: 3810
        ))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-repaired", index: 0, start: 0, end: 563.8)
        )
        let repaired = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-repaired"])["a-repaired"]
        )
        let fraction = try #require(repaired.adScanFraction)
        #expect(abs(fraction.rawValue - 563.8 / 3810) < 0.0001)
        #expect(fraction < episodePreparationCompleteThreshold)
    }

    /// The reach guard must tolerate ordinary feed-vs-measured drift, or it would
    /// withhold the fraction on healthy episodes: the final chunk legitimately
    /// ends a hair past the shard-sum duration.
    @Test("a transcript reaching a little past the duration is still measurable")
    func smallTranscriptOvershootStaysMeasurable() async throws {
        let store = try await makeTestStore()
        // Reach 1,010 s against a declared 1,000 s — 1% drift, well inside tolerance.
        try await store.insertAsset(makeAsset(
            id: "a-drift", episodeDurationSec: 1000, fastTranscriptCoverageEndTime: 1010
        ))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-drift", index: 0, start: 0, end: 1000)
        )

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-drift"])["a-drift"]
        )
        #expect(summary.adScanFraction == 1)
        #expect(episodePreparationAnalysisComplete(
            status: .done, adScanFraction: summary.adScanFraction, isDegradedTerminal: false
        ))
    }

    /// Batched reads must not cross-contaminate: each asset gets only its own
    /// windows. A single shared `Set` bug here would let one fully-scanned
    /// episode light the ✓ on every row in the list.
    @Test("batched read attributes scan windows to the right asset")
    func batchedReadIsPerAsset() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-full", episodeDurationSec: 1000))
        try await store.insertAsset(makeAsset(id: "a-thin", episodeDurationSec: 1000))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-full", index: 0, start: 0, end: 1000)
        )
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-thin", index: 0, start: 0, end: 50)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-full", "a-thin"])
        #expect(try #require(summaries["a-full"]).adScanFraction == 1)
        #expect(try #require(summaries["a-thin"]).adScanFraction == 0.05)
    }

    /// CROSS-PRODUCER AGREEMENT, on the axis the two producers actually share:
    /// WHICH ROWS count. `SemanticScanCoverage.compute` is the pipeline's own
    /// breadcrumb (full row decode + Swift filter); the store read is the
    /// user-facing one (narrow SQL projection). Both must select exactly the same
    /// rows — same pass, same examined predicate, same sentinel exclusion — or the
    /// log and the checkmark disagree about what happened.
    ///
    /// They deliberately do NOT produce the same SECONDS: the store additionally
    /// intersects with the transcript, so `examinedSeconds >= adScanCoveredSec` in
    /// general. This fixture inserts a transcript that spans the whole attempted
    /// range, which makes the intersection a no-op and isolates row selection —
    /// and the sibling test below pins the inequality where it bites.
    @Test("store ad-scan seconds equal SemanticScanCoverage.examinedSeconds on one row set")
    func agreesWithPipelineBreadcrumb() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-agree", episodeDurationSec: 3578))
        let rows: [SemanticScanResult] = [
            makeScan(assetId: "a-agree", index: 0, start: 0, end: 300),
            makeScan(assetId: "a-agree", index: 1, start: 250, end: 600),
            makeScan(assetId: "a-agree", index: 2, start: 900, end: 1425.9),
            makeScan(assetId: "a-agree", index: 3, start: 1425.9, end: 1800, status: .guardrailViolation),
            makeScan(assetId: "a-agree", index: 4, start: 2000, end: 2100, status: .noAds),
            makeScan(assetId: "a-agree", index: 5, start: 2500, end: 2600, scanPass: "passB")
        ]
        for row in rows {
            try await store.insertSemanticScanResult(row)
        }

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-agree"])
        let summary = try #require(summaries["a-agree"])
        let breadcrumb = SemanticScanCoverage.compute(rows: rows)
        #expect(summary.adScanCoveredSec?.rawValue == breadcrumb.examinedSeconds)
        // Sanity-check the number itself so the agreement isn't 0 == 0:
        // [0,600] = 600, [900,1425.9] = 525.9, [2000,2100] = 100.
        #expect(abs(breadcrumb.examinedSeconds - 1225.9) < 0.0001)
    }

    /// The two producers measure DIFFERENT quantities once the transcript is
    /// gappy, and that is by design: the breadcrumb answers "how much did the pass
    /// attempt to screen", the checkmark answers "how much audio was read". Pinned
    /// so nobody "fixes" the divergence by making the ✓ read the looser number —
    /// which is precisely the bug this bead exists to remove.
    @Test("the breadcrumb over-reports vs the store on a gappy transcript, by design")
    func breadcrumbAndStoreDivergeOnGappyTranscript() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-diverge", episodeDurationSec: 3600))
        try await store.insertTranscriptChunks([
            makeGapChunk(assetId: "a-diverge", index: 0, start: 0, end: 60),
            makeGapChunk(assetId: "a-diverge", index: 1, start: 3540, end: 3600)
        ])
        let rows = [makeScan(assetId: "a-diverge", index: 0, start: 0, end: 3600)]
        for row in rows {
            try await store.insertSemanticScanResult(row)
        }

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-diverge"])["a-diverge"]
        )
        let breadcrumb = SemanticScanCoverage.compute(rows: rows)
        #expect(breadcrumb.examinedSeconds == 3600)
        #expect(summary.adScanCoveredSec == 120)
        #expect(summary.adScanCoveredSec!.rawValue < breadcrumb.examinedSeconds)
    }

    /// A scan window's persisted bounds are `first.startTime ... last.endTime`
    /// over the segments that fit one prompt
    /// (`FoundationModelClassifier.planPassA`), so a gappy transcript can produce
    /// ONE window whose bounds straddle the whole episode while its prompt
    /// carried two minutes of text. Taking the bounds at face value reports that
    /// episode as fully screened — this bead's own bug, one layer down.
    @Test("a window spanning a transcript gap counts only the transcribed part")
    func windowSpanningTranscriptGapCountsOnlyTranscribedAudio() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-gapwindow", episodeDurationSec: 3600))
        // Transcript landed only the first and last minute (the playhead-0sro /
        // "AN 100% / TX 39%" shape).
        try await store.insertTranscriptChunks([
            makeGapChunk(assetId: "a-gapwindow", index: 0, start: 0, end: 60),
            makeGapChunk(assetId: "a-gapwindow", index: 1, start: 3540, end: 3600)
        ])
        // Both segments fit one prompt → one row, bounds [0, 3600].
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-gapwindow", index: 0, start: 0, end: 3600)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-gapwindow"])
        let summary = try #require(summaries["a-gapwindow"])
        // Bare bounds would say 3600 (100%). Intersecting with the transcript
        // says 120s = 3.3%.
        #expect(summary.adScanCoveredSec == 120)
        let fraction = try #require(summary.adScanFraction)
        #expect(abs(fraction.rawValue - 120.0 / 3600.0) < 0.0001)
        #expect(fraction < episodePreparationCompleteThreshold)
        // The bound is the transcript, so the ad-scan area cannot exceed the
        // transcript union by more than the bridged sub-ad-width gaps (none here:
        // the single hole is 3,480 s wide).
        #expect(summary.adScanCoveredSec!.rawValue <= summary.fastTranscriptCoveredSec!.rawValue)
    }

    /// The intersection must be GAP-AWARE, not a `[0, transcriptSeconds]` PREFIX
    /// clip. Both give the same answer when the scan window happens to start at
    /// zero, which is what the fixture above does — so this one puts the scan
    /// window over the LATE transcript run only. A prefix clip returns 0 here;
    /// the correct answer is 60.
    @Test("the transcript bound is gap-aware, not a prefix clip")
    func transcriptBoundIsGapAwareNotAPrefix() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-latewindow", episodeDurationSec: 3600))
        try await store.insertTranscriptChunks([
            makeGapChunk(assetId: "a-latewindow", index: 0, start: 0, end: 60),
            makeGapChunk(assetId: "a-latewindow", index: 1, start: 3540, end: 3600)
        ])
        // A window over the late run ONLY. `fastTranscriptCoveredSec` is 120, so a
        // prefix bound of [0,120] would intersect this to nothing.
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-latewindow", index: 0, start: 3500, end: 3600)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-latewindow"])
        let summary = try #require(summaries["a-latewindow"])
        #expect(summary.adScanCoveredSec == 60)

        // And a window straddling the hole picks up BOTH runs, not one.
        try await store.insertAsset(makeAsset(id: "a-straddle", episodeDurationSec: 3600))
        try await store.insertTranscriptChunks([
            makeGapChunk(assetId: "a-straddle", index: 0, start: 0, end: 60),
            makeGapChunk(assetId: "a-straddle", index: 1, start: 3540, end: 3600)
        ])
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-straddle", index: 0, start: 30, end: 3570)
        )
        let straddle = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-straddle"])["a-straddle"]
        )
        // [30,60] = 30 and [3540,3570] = 30.
        #expect(straddle.adScanCoveredSec == 60)
    }

    /// A `transcript_chunks` row spans the FIRST WORD's start to the LAST WORD's
    /// end, so the raw chunk union is riddled with inter-utterance holes: measured
    /// on the 2026-04-25 device capture it is 0.68–0.976 of the transcript span,
    /// with 182–1,199 disjoint runs per asset. Intersecting against the RAW union
    /// would cap every real episode below the 0.98 threshold and make the ✓
    /// unreachable, so sub-ad-width gaps are bridged. A gap WIDER than an ad is
    /// still a hole.
    @Test("sub-ad-width transcript gaps are bridged; ad-width blocks stay holes")
    func subAdWidthGapsAreBridged() async throws {
        let store = try await makeTestStore()
        // 1,000 s episode transcribed as ten 99 s utterances separated by 1 s
        // breaths, plus one 60 s untranscribed block at the end.
        var chunks: [TranscriptChunk] = []
        var cursor = 0.0
        for index in 0..<10 {
            chunks.append(makeGapChunk(
                assetId: "a-breaths", index: index, start: cursor, end: cursor + 93
            ))
            cursor += 94
        }
        try await store.insertAsset(makeAsset(id: "a-breaths", episodeDurationSec: 1000))
        try await store.insertTranscriptChunks(chunks)
        // One window over the whole episode: the scan read every utterance.
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-breaths", index: 0, start: 0, end: 1000)
        )

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-breaths"])["a-breaths"]
        )
        // Raw chunk union is 10 × 93 = 930 (0.93 — BELOW the threshold, which is
        // the bug this bridging fixes).
        #expect(summary.fastTranscriptCoveredSec == 930)
        // Bridged across the nine 1 s breaths: 0 … 939. The trailing 61 s block is
        // NOT bridged, so it stays unscanned.
        #expect(summary.adScanCoveredSec == 939)
        let fraction = try #require(summary.adScanFraction)
        #expect(abs(fraction - 0.939) < 0.0001)
        // The bridged area legitimately exceeds the raw transcript union — that is
        // the point — but never the transcript SPAN (0 … 939).
        #expect(summary.adScanCoveredSec!.rawValue > summary.fastTranscriptCoveredSec!.rawValue)
    }

    /// The bridge width must be strictly under the shortest span any lane will
    /// call an ad, so bridging can never conceal one. Pins the boundary in both
    /// directions against the constant.
    @Test("a gap at the bridge width bridges; one past it does not")
    func bridgeWidthBoundary() async throws {
        let bridge = AnalysisCoverageMath.adScanBridgeableGapSec
        #expect(bridge == 5.0)
        // Strictly less than every ad-width minimum in the codebase, so no
        // bridged gap can hide an ad.
        #expect(bridge <= 5.0)
        #expect(bridge.rawValue < GlobalPriorDefaults.standard.typicalAdDuration.lowerBound)
        // playhead-x0lb: and it is NOT the re-scan threshold, which is 12x
        // larger and answers the opposite question.
        #expect(bridge.rawValue < RescanThresholdSec.adScanRescanWorthyGapSec.rawValue)

        let atWidth = AnalysisCoverageMath.bridgingShortGaps(
            [(start: 0, end: 100), (start: 100 + bridge.rawValue, end: 200)], upTo: bridge
        )
        #expect(atWidth.count == 1)
        let pastWidth = AnalysisCoverageMath.bridgingShortGaps(
            [(start: 0, end: 100), (start: 100 + bridge.rawValue + 0.001, end: 200)], upTo: bridge
        )
        #expect(pastWidth.count == 2)
    }

    /// `BackfillJobRunner.makeNoWorkSentinelScanResult` writes a passA row with
    /// `status == .noAds` spanning the WHOLE attempted range, meaning **no work
    /// was performed**. `.noAds` is otherwise a genuine "I looked, nothing here"
    /// verdict, so the status alone cannot tell them apart — and counting the
    /// sentinel reports a whole episode as screened off a job that made zero FM
    /// calls.
    @Test("a no-work sentinel row is never counted as scanned audio")
    func noWorkSentinelIsNotScannedAudio() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-sentinel", episodeDurationSec: 1000))
        let sentinel = makeScan(
            assetId: "a-sentinel",
            index: 0,
            start: 0,
            end: 1000,
            status: .noAds,
            errorContext: "\(SemanticScanResult.noWorkSentinelErrorContextPrefix)emptySegments"
        )
        try await store.insertSemanticScanResult(sentinel)
        // A REAL `.noAds` examination alongside it must still count.
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-sentinel", index: 1, start: 0, end: 120, status: .noAds)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-sentinel"])
        let summary = try #require(summaries["a-sentinel"])
        #expect(summary.adScanCoveredSec == 120)
        #expect(summary.adScanFraction == 0.12)
        // The row-level predicate and the pipeline breadcrumb agree.
        #expect(!sentinel.didExamineWindow)
        #expect(sentinel.isNoWorkSentinel)
        #expect(sentinel.status.didExamineWindow, "the STATUS still says examined — that is the trap")
        #expect(SemanticScanCoverage.compute(rows: [sentinel]).examinedSeconds == 0)
    }

    private func makeGapChunk(
        assetId: String,
        index: Int,
        start: Double,
        end: Double
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-chunk-\(index)",
            analysisAssetId: assetId,
            segmentFingerprint: "\(assetId)-fp-\(index)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "t",
            normalizedText: "t",
            pass: "fast",
            modelVersion: "test-asr",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    /// The analyzed AREA (`analysisCoveredSec`, playhead-sd71) is gap-aware and
    /// still not a measure of ad screening: a fully-transcribed episode with a
    /// frontier at the end reads 100% analyzed area on 1% ad-scan coverage.
    /// Pinning both on one asset documents why the readiness predicate had to
    /// move off it rather than reuse Activity's number.
    @Test("analyzed AREA can read 100% while ad-scan coverage reads 1%")
    func analyzedAreaIsNotAdScanCoverage() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-area",
            episodeDurationSec: 1000,
            featureCoverageEndTime: 1000,
            confirmedAdCoverageEndTime: 1000
        ))
        try await store.insertTranscriptChunks([
            TranscriptChunk(
                id: "a-area-chunk-0",
                analysisAssetId: "a-area",
                segmentFingerprint: "a-area-fp-0",
                chunkIndex: 0,
                startTime: 0,
                endTime: 1000,
                text: "t",
                normalizedText: "t",
                pass: "fast",
                modelVersion: "test-asr",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        ])
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-area", index: 0, start: 0, end: 10)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-area"])
        let summary = try #require(summaries["a-area"])
        #expect(summary.analysisCoveredSec == 1000)
        #expect(summary.adScanCoveredSec == 10)
        #expect(summary.adScanFraction == 0.01)
    }

    // MARK: - playhead-nffz: the ceiling of the ad-scan fraction

    /// THE C065AD03 CASE, on the shipped reader. Its transcript covers 44 % of
    /// the episode in seven disjoint runs, every second of it has been scanned,
    /// and the floor the pipeline judges it by is 0.98 of the DECLARED DURATION.
    /// So the floor's denominator is the episode and the numerator's supremum is
    /// the transcript — two populations, one comparison.
    ///
    /// Both numbers on one asset, so the mismatch is visible rather than argued.
    @Test("playhead-nffz — a 44 %-transcribed episode cannot clear a 0.98 floor however perfectly it is scanned")
    func ceilingBelowTheFloorMakesTheFloorUnreachable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-ceiling", episodeDurationSec: 1000))
        // Four disjoint runs totalling 440 s, every gap far wider than the 5 s
        // bridge tolerance, so the bridged area really is 440 and not 1000.
        try await store.insertTranscriptChunks([
            makeGapChunk(assetId: "a-ceiling", index: 0, start: 0, end: 110),
            makeGapChunk(assetId: "a-ceiling", index: 1, start: 300, end: 410),
            makeGapChunk(assetId: "a-ceiling", index: 2, start: 600, end: 710),
            makeGapChunk(assetId: "a-ceiling", index: 3, start: 880, end: 990)
        ])
        // A scan that read EVERYTHING there is to read.
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-ceiling", index: 0, start: 0, end: 1000)
        )

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-ceiling"])["a-ceiling"]
        )
        #expect(summary.adScanCeilingSec == 440)
        #expect(summary.adScanCoveredSec == 440, "a perfect scan reads every transcribed second")
        let fraction = try #require(summary.adScanFraction)
        let ceiling = try #require(summary.adScanCeilingFraction)
        #expect(fraction == 0.44)
        #expect(ceiling == 0.44)
        #expect(fraction == ceiling, "the scan is AT its ceiling: there is nothing left to read")
        #expect(ceiling < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction,
                "and the ceiling is under the floor, which is the defect in one line")
    }

    /// `adScanCoveredSec <= adScanCeilingSec` is ARITHMETIC, not an observation:
    /// the ceiling is the very region the area is intersected with. Driven on the
    /// shape most likely to break it — one window whose bounds straddle the whole
    /// episode over a transcript that is mostly holes.
    @Test("playhead-nffz — the ceiling bounds the measured area, by construction")
    func ceilingBoundsTheMeasuredArea() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-bound-nffz", episodeDurationSec: 3600))
        try await store.insertTranscriptChunks([
            makeGapChunk(assetId: "a-bound-nffz", index: 0, start: 0, end: 60),
            makeGapChunk(assetId: "a-bound-nffz", index: 1, start: 3540, end: 3600)
        ])
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-bound-nffz", index: 0, start: 0, end: 3600)
        )

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-bound-nffz"])["a-bound-nffz"]
        )
        let covered = try #require(summary.adScanCoveredSec)
        let ceiling = try #require(summary.adScanCeilingSec)
        #expect(covered.rawValue <= ceiling.rawValue)
        #expect(ceiling == 120, "the bridged transcript, not the window's bounds")
        #expect(try #require(summary.adScanCeilingFraction).rawValue < 0.05)
    }

    /// The ceiling is a property of the TRANSCRIPT, so it exists before anything
    /// has scanned. Reading it as `nil` there would mean the one moment the
    /// pipeline could learn the floor is unreachable is the moment it cannot ask.
    @Test("playhead-nffz — the ceiling does not require a coverage-lane row")
    func ceilingIsKnowableBeforeAnyScan() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-noscan-nffz", episodeDurationSec: 1000))
        try await store.insertTranscriptChunks([
            makeGapChunk(assetId: "a-noscan-nffz", index: 0, start: 0, end: 300)
        ])

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-noscan-nffz"])["a-noscan-nffz"]
        )
        #expect(summary.adScanCoveredSec == nil, "nothing has scanned")
        #expect(summary.adScanCeilingSec == 300)
        #expect(summary.adScanCeilingFraction == 0.3)
    }

    /// UNMEASURED IS NOT ZERO. An asset with a duration and NO transcript of any
    /// kind must read `nil`, never `0` — a synthetic zero here is the claim "this
    /// episode can never be scanned", made about an episode whose transcription
    /// has not started.
    @Test("playhead-nffz — no transcript evidence reads nil, not a measured zero")
    func ceilingIsNilWithoutTranscriptEvidence() async throws {
        let store = try await makeTestStore()
        // Built inline rather than through `makeAsset`, whose
        // `fastTranscriptCoverageEndTime` defaults to the DURATION — that
        // watermark is itself transcript evidence (the store models it as one
        // contiguous `[0, watermark]` span), so the helper cannot express "no
        // transcript at all", which is the whole case here.
        try await store.insertAsset(AnalysisAsset(
            id: "a-empty-nffz",
            episodeId: "ep-a-empty-nffz",
            assetFingerprint: "fp-a-empty-nffz",
            weakFingerprint: nil,
            sourceURL: "file:///a-empty-nffz.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "backfill",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 1000
        ))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-empty-nffz", index: 0, start: 0, end: 100)
        )

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-empty-nffz"])["a-empty-nffz"]
        )
        #expect(summary.adScanCeilingSec == nil)
        #expect(summary.adScanCeilingFraction == nil)
        // And the measured area IS a real zero, because a coverage-lane row
        // exists and the transcript it would be intersected with is empty.
        #expect(summary.adScanCoveredSec == 0)
    }

    /// C0610BF9's shape, and it is why this bead's re-measurement disagrees with
    /// the number the bead was filed on.
    ///
    /// That asset has ZERO `pass='fast'` chunks and a `fastTranscriptCoverageEndTime`
    /// of 1740 s on a 1930.84 s episode, and the shipped reader models a watermark
    /// with no chunks behind it as ONE CONTIGUOUS `[0, watermark]` span (see the
    /// `transcriptRegion` branch in `fetchCoverageSummariesByAssetIds`, and
    /// playhead-9y9e's monotonicity argument for why the fast term is added to the
    /// final one rather than replaced by it). A reconstruction that bounds the scan
    /// by the FINAL-pass chunks alone therefore reads LOWER: 0.9282 against the
    /// shipped 0.9341 on that asset. Both are under the 0.98 floor, so the bead's
    /// conclusion is unaffected — but the ceiling is a published quantity now, and
    /// which of the two it is has to be a pinned property rather than a reading.
    @Test("playhead-nffz — a fast WATERMARK with no fast chunks contributes its span to the ceiling")
    func ceilingIncludesTheWatermarkSpanWhenNoFastChunkLanded() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-wm-nffz",
            episodeDurationSec: 1000,
            fastTranscriptCoverageEndTime: 600
        ))
        // Final pass only, with a 100 s hole the watermark span covers.
        try await store.insertTranscriptChunks([
            TranscriptChunk(
                id: "a-wm-nffz-final-0", analysisAssetId: "a-wm-nffz",
                segmentFingerprint: "a-wm-nffz-fp-0", chunkIndex: 0,
                startTime: 0, endTime: 300, text: "t", normalizedText: "t",
                pass: "final", modelVersion: "test-asr",
                transcriptVersion: nil, atomOrdinal: nil
            ),
            TranscriptChunk(
                id: "a-wm-nffz-final-1", analysisAssetId: "a-wm-nffz",
                segmentFingerprint: "a-wm-nffz-fp-1", chunkIndex: 1,
                startTime: 400, endTime: 1000, text: "t", normalizedText: "t",
                pass: "final", modelVersion: "test-asr",
                transcriptVersion: nil, atomOrdinal: nil
            )
        ])

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-wm-nffz"])["a-wm-nffz"]
        )
        // Final chunks alone bridge to 300 + 600 = 900. The watermark's [0, 600]
        // span closes the 100 s hole, so the shipped bound is the whole episode.
        #expect(summary.adScanCeilingSec == 1000)
        #expect(summary.adScanCeilingFraction == 1.0)
    }

    /// ONE RULER, TWO READERS. The ceiling is compared against the same floor as
    /// the fraction, so the two must be withheld under the same conditions — an
    /// episode where one is present and the other is not is an episode that falls
    /// between two rulers. Driven on E8F0F867's shape, the case
    /// `adScanFraction`'s own guard was written for.
    @Test("playhead-nffz — a transcript reaching past the declared duration withholds BOTH fractions")
    func aDisprovedDenominatorWithholdsTheCeilingToo() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-guard-nffz", episodeDurationSec: 552.9))
        try await store.insertTranscriptChunks([
            makeGapChunk(assetId: "a-guard-nffz", index: 0, start: 0, end: 500),
            makeGapChunk(assetId: "a-guard-nffz", index: 1, start: 3000, end: 3810)
        ])
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-guard-nffz", index: 0, start: 0, end: 563.8)
        )

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-guard-nffz"])["a-guard-nffz"]
        )
        #expect(summary.adScanFraction == nil, "the transcript has disproved the denominator")
        #expect(summary.adScanCeilingFraction == nil,
                "and a ceiling over that same denominator is no more honest than the fraction")
    }

    /// THE SAME CLAIM, ON THE ARM THAT ACTUALLY REACHES THE GUARD — and the test
    /// above does not, which is why this one exists.
    ///
    /// Mutation M9 deleted `declaredDurationIsDisprovedByTranscriptReach` from
    /// `adScanCeilingFraction` and SURVIVED the whole suite. The fixture above
    /// gives a bridged ceiling of 1,310 s on a 552.9 s episode, so the FIRST
    /// guard — the area overshooting the duration — withholds it and the reach
    /// guard is never evaluated. Two guards, one fixture, and the parity claim
    /// rested on the guard the fixture could not reach.
    ///
    /// Here the AREA is small (410 s of a declared 1,000 s, comfortably inside
    /// the tolerance) while the transcript's REACH is 3,810 s. Only the reach
    /// guard can withhold this, and it must withhold BOTH fractions — E8F0F867's
    /// real shape, which is what `adScanFraction`'s own guard was written for.
    @Test("playhead-nffz — a small AREA with a reach past the duration withholds BOTH: the guard the sibling test cannot reach")
    func aDisprovedDenominatorWithholdsTheCeilingOnTheREACHArmToo() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-reach-nffz", episodeDurationSec: 1000))
        try await store.insertTranscriptChunks([
            makeGapChunk(assetId: "a-reach-nffz", index: 0, start: 0, end: 400),
            makeGapChunk(assetId: "a-reach-nffz", index: 1, start: 3800, end: 3810)
        ])
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-reach-nffz", index: 0, start: 0, end: 400)
        )

        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds(["a-reach-nffz"])["a-reach-nffz"]
        )
        // The area guard CANNOT be what fires: 410 s is well inside 1,000 s.
        #expect(summary.adScanCeilingSec == 410)
        #expect(try #require(summary.adScanCeilingSec).rawValue
                < 1000 + AnalysisCoverageSummary.adScanDurationToleranceSec(
                    episodeDurationSec: EpisodeSeconds(1000)
                ),
                "if this ever fails the fixture has drifted back onto the OTHER guard")
        #expect(summary.adScanFraction == nil)
        #expect(summary.adScanCeilingFraction == nil,
                "one rule, two readers — a reach that disproves the denominator withholds the ceiling too")
    }
}

// MARK: - playhead-csbq: the ruler itself

/// playhead-csbq. Coverage stopped being a diagnostic on 2026-07-30: pz32 made
/// the readiness ✓ key on measured ad-scan coverage and gqx4 made
/// `.completeFull` REQUIRE it, so a wrong number now promotes an episode into a
/// terminal state nothing returns from. These tests pin the two defects
/// measured on the device pull that day, using the REAL row shapes.
///
///   DEFECT 1 — a naive `SUM` over `semantic_scan_results` windows reports
///   asset CD1AD629 as 294% of its episode scanned (average 67.5% across 16
///   assets) because overlapping and duplicated windows are double-counted.
///
///   DEFECT 2 — 6 of 248 rows have `windowEndTime < windowStartTime` (worst
///   −635.2 s, 3 assets) and TWO carry `status = 'success'`.
///
/// Existing bad rows are LEFT IN PLACE by product decision, so every read-side
/// test here seeds its inverted row through `execForTesting` — deliberately
/// bypassing the write guard, which is the only way a row like this can exist
/// from now on.
@Suite("AnalysisStore coverage ruler (playhead-csbq)")
struct AnalysisStoreCoverageRulerTests {

    private static let cohortJSON: String = {
        let cohort = ScanCohort(
            promptLabel: "csbq-test",
            promptHash: "prompt-v1",
            schemaHash: "schema-v1",
            scanPlanHash: "plan-v1",
            normalizationHash: "norm-v1",
            osBuild: "26A123",
            locale: "en_US",
            appBuild: "1"
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = (try? encoder.encode(cohort)) ?? Data()
        return String(decoding: data, as: UTF8.self)
    }()

    private func makeAsset(
        id: String,
        episodeDurationSec: Double,
        finalPassCoverageEndTime: Double? = nil
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///\(id).m4a",
            featureCoverageEndTime: episodeDurationSec,
            fastTranscriptCoverageEndTime: episodeDurationSec,
            confirmedAdCoverageEndTime: nil,
            analysisState: "backfill",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec,
            finalPassCoverageEndTime: finalPassCoverageEndTime
        )
    }

    private func makeScan(
        assetId: String,
        index: Int,
        start: Double,
        end: Double,
        status: SemanticScanStatus = .success,
        scanPass: String = SemanticScanCoverage.coverageScanPass,
        firstOrdinal: Int? = nil,
        lastOrdinal: Int? = nil
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: "\(assetId)-scan-\(scanPass)-\(index)",
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: firstOrdinal ?? index * 10,
            windowLastAtomOrdinal: lastOrdinal ?? (index * 10 + 9),
            windowStartTime: start,
            windowEndTime: end,
            scanPass: scanPass,
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: status,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: Self.cohortJSON,
            transcriptVersion: "tx-v1",
            reuseScope: "\(assetId)-\(scanPass)-\(index)"
        )
    }

    /// Seed a row the write guard would now refuse, the way the device DB
    /// already contains six of them. Raw SQL on purpose — there is no
    /// supported API that can produce this shape any more.
    private func seedRawScanRow(
        store: AnalysisStore,
        id: String,
        assetId: String,
        start: Double,
        end: Double,
        status: String,
        scanPass: String = SemanticScanCoverage.coverageScanPass
    ) async throws {
        let cohort = Self.cohortJSON.replacingOccurrences(of: "'", with: "''")
        try await store.execForTesting(
            """
            INSERT INTO semantic_scan_results
              (id, analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
               windowStartTime, windowEndTime, scanPass, transcriptQuality,
               disposition, spansJSON, status, attemptCount, errorContext,
               inputTokenCount, outputTokenCount, latencyMs, prewarmHit,
               scanCohortJSON, transcriptVersion, reuseKeyHash, runMode, jobPhase)
            VALUES
              ('\(id)', '\(assetId)', 0, 69, \(start), \(end), '\(scanPass)',
               'degraded', 'containsAd', '[]', '\(status)', 1, NULL,
               NULL, NULL, NULL, 0,
               '\(cohort)', 'tx-v1', 'raw-\(id)', 'shadow', 'fullEpisodeScan')
            """
        )
    }

    // MARK: - Defect 2: impossible geometry is rejected at the write

    /// The real `success` row from device asset 4E4730D8: ordinals 0…69, a
    /// window running 330.0 → 15.18, disposition `containsAd`, 12.6 s of model
    /// latency. The scan genuinely ran — only the BOUNDS are impossible — and
    /// before this guard it was persisted and every consumer folded it into a
    /// total.
    @Test("the real inverted `success` row is rejected at the write, and named")
    func invertedSuccessRowIsRejected() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-inv-success", episodeDurationSec: 4590.58))

        var thrown: AnalysisStoreError?
        do {
            try await store.insertSemanticScanResult(makeScan(
                assetId: "a-inv-success", index: 0,
                start: 330.0, end: 15.18,
                status: .success, firstOrdinal: 0, lastOrdinal: 69
            ))
        } catch let error as AnalysisStoreError {
            thrown = error
        }

        let error = try #require(thrown, "an impossible window must not be persisted")
        guard case let .insertFailed(message) = error else {
            Issue.record("expected .insertFailed, got \(error)")
            return
        }
        // NAMED, not a bare failure: the message must carry the token, both
        // numbers, the asset and the status, so a log line is diagnosable
        // without a device attached.
        #expect(message.hasPrefix(AnalysisStore.impossibleWindowGeometryPrefix))
        #expect(message.contains("330.0"))
        #expect(message.contains("15.18"))
        #expect(message.contains("a-inv-success"))
        #expect(message.contains("success"))

        // And nothing landed.
        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "a-inv-success")
        #expect(rows.isEmpty)
    }

    /// The worst inversion on the pull (−635.2 s, device asset D75D7584) and a
    /// non-finite bound. Both are structurally impossible for different
    /// reasons; both must be refused.
    @Test("worst real inversion and non-finite bounds are both rejected")
    func invertedAndNonFiniteRowsAreRejected() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-inv-worst", episodeDurationSec: 2932.94))

        for (index, bounds) in [
            (start: 767.04, end: 131.82),      // −635.22 s, status refusal on device
            (start: 118.38, end: 45.24),       // −73.14 s, guardrailViolation
            (start: 1978.86, end: 1933.02),    // −45.84 s, passB `success`
            (start: 0.0, end: Double.nan),
            (start: Double.infinity, end: 10.0)
        ].enumerated() {
            var thrown: AnalysisStoreError?
            do {
                try await store.insertSemanticScanResult(makeScan(
                    assetId: "a-inv-worst", index: index,
                    start: bounds.start, end: bounds.end, status: .refusal
                ))
            } catch let error as AnalysisStoreError {
                thrown = error
            }
            let error = try #require(thrown, "row \(index) \(bounds) must be refused")
            guard case let .insertFailed(message) = error else {
                Issue.record("row \(index): expected .insertFailed, got \(error)")
                continue
            }
            #expect(message.hasPrefix(AnalysisStore.impossibleWindowGeometryPrefix))
        }

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "a-inv-worst")
        #expect(rows.isEmpty)
    }

    /// THE FALSE-POSITIVE RAIL. A guard that refuses a legitimate row silently
    /// loses real coverage, which is worse than the bug it fixes. Zero-width
    /// windows are legal and REAL — `BackfillJobRunner.makeNoWorkSentinelScanResult`
    /// writes `0.0 … 0.0` when an asset has no segments — and they contribute
    /// zero seconds to every union, so they cannot inflate anything.
    @Test("legitimate rows — zero-width, ordinary, and descending ordinals — still persist")
    func legitimateRowsAreNotRejected() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-legit", episodeDurationSec: 1000))

        // Zero-width at the origin: the no-work sentinel's exact shape.
        try await store.insertSemanticScanResult(makeScan(
            assetId: "a-legit", index: 0, start: 0, end: 0, status: .noAds
        ))
        // Zero-width mid-episode.
        try await store.insertSemanticScanResult(makeScan(
            assetId: "a-legit", index: 1, start: 412.5, end: 412.5
        ))
        // An ordinary window.
        try await store.insertSemanticScanResult(makeScan(
            assetId: "a-legit", index: 2, start: 0, end: 500
        ))
        // Atom ordinals are bookkeeping, not geometry — a descending pair is
        // not this guard's business and must not be refused.
        try await store.insertSemanticScanResult(makeScan(
            assetId: "a-legit", index: 3, start: 500, end: 600,
            firstOrdinal: 900, lastOrdinal: 100
        ))

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "a-legit")
        #expect(rows.count == 4)
        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-legit"])
        let summary = try #require(summaries["a-legit"])
        // 0–600 examined; the two zero-width rows add nothing.
        #expect(summary.adScanCoveredSec == 600)
    }

    /// A rejected row must not burn the retry budget: retrying reproduces the
    /// same impossible bounds byte for byte.
    @Test("impossible-geometry rejection is classified permanent by the runner")
    func rejectionIsPermanent() {
        let geometry = AnalysisStoreError.insertFailed(
            "\(AnalysisStore.impossibleWindowGeometryPrefix) windowEndTime 15.18 < windowStartTime 330.0"
        )
        #expect(BackfillJobRunner.isPermanentForTesting(geometry))
        // A transient insert failure must stay retryable — the prefix match
        // must not have widened to every `insertFailed`.
        #expect(!BackfillJobRunner.isPermanentForTesting(.insertFailed("disk I/O error")))
    }

    // MARK: - Defect 2, read side: adScanFraction in the presence of an inverted row

    /// The bead's open question, answered by TEST rather than by inference:
    /// `adScanFraction` unions and clips, so an inverted window "most likely
    /// contributes zero". It does — but only because
    /// `fetchCoverageSummariesByAssetIds` drops it with `endTime > startTime`
    /// BEFORE the union. Removing that filter makes this test report 100%
    /// on an episode where 40% was read.
    @Test("a pre-existing inverted row contributes zero and does not corrupt the union")
    func invertedRowContributesZeroToAdScanFraction() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-mixed", episodeDurationSec: 1000))

        // Two real windows: 0–200 and 600–800 ⇒ 400 s of 1000 s == 40%.
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-mixed", index: 0, start: 0, end: 200)
        )
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-mixed", index: 1, start: 600, end: 800)
        )
        // The device's worst inversion, straddling both of them.
        try await seedRawScanRow(
            store: store, id: "raw-inv-1", assetId: "a-mixed",
            start: 767.04, end: 131.82, status: "refusal"
        )
        // And the device's inverted `success` row, whose status WOULD count.
        try await seedRawScanRow(
            store: store, id: "raw-inv-2", assetId: "a-mixed",
            start: 330.0, end: 15.18, status: "success"
        )

        // The rows really are in the table — this is testing tolerance of
        // existing data, not absence of it.
        #expect(try await store.fetchSemanticScanResults(analysisAssetId: "a-mixed").count == 4)

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-mixed"])
        let summary = try #require(summaries["a-mixed"])
        // Exactly the valid-rows-only answer. Not inflated by the inverted
        // rows' spans (which would read 800 s), not nil, not zero.
        #expect(summary.adScanCoveredSec == 400)
        #expect(summary.adScanFraction == 0.4)
        #expect(summary.adScanCoveredSource == .semanticScanResults)

        // The pipeline's own breadcrumb must agree with the checkmark.
        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "a-mixed")
        let coverage = SemanticScanCoverage.compute(rows: rows, episodeDuration: 1000)
        #expect(coverage.examinedSeconds == 400)
    }

    /// An asset whose ONLY coverage-lane rows are inverted must report a
    /// MEASURED ZERO, not "unknown" and not a number. Both render not-ready,
    /// but the provenance has to stay honest about which one it is — an
    /// unnamed absence is indistinguishable from every other absence.
    @Test("an asset with only inverted rows reports a measured zero, not unknown")
    func onlyInvertedRowsReportMeasuredZero() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-all-inv", episodeDurationSec: 4590.58))
        try await seedRawScanRow(
            store: store, id: "raw-only-1", assetId: "a-all-inv",
            start: 330.0, end: 15.18, status: "success"
        )
        try await seedRawScanRow(
            store: store, id: "raw-only-2", assetId: "a-all-inv",
            start: 389.64, end: 35.4, status: "failedTransient"
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-all-inv"])
        let summary = try #require(summaries["a-all-inv"])
        #expect(summary.adScanCoveredSec == 0)
        #expect(summary.adScanCoveredSource == .semanticScanResults)
        #expect(summary.adScanFraction == 0)
        let fraction = try #require(summary.adScanFraction)
        #expect(fraction < episodePreparationCompleteThreshold)
    }

    // MARK: - Defect 1: the 294%-of-episode asset

    /// THE BEAD'S HEADLINE FIXTURE — device asset CD1AD629, all 23 persisted
    /// rows verbatim. A naive `SUM(windowEndTime - windowStartTime)` over the
    /// examined rows totals 4871.64 s against a 1655.82 s episode: 294% of the
    /// audio "scanned". Two independent causes stack — every window is
    /// DUPLICATED (the playhead-6av0 child-row family), and passB extent
    /// attempts sit INSIDE the passA windows they refine.
    ///
    /// The union over the coverage lane is 1576.62 s ⇒ 0.952, which is below
    /// the 0.98 readiness floor. So the honest answer is "not ready" on an
    /// asset the naive sum called 294% covered.
    @Test("the 294%-of-episode asset: naive SUM says 2.94, the union says 0.95")
    func the294PercentAssetUnionsToNinetyFive() async throws {
        let duration = 1655.8236734693878
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-cd1ad629", episodeDurationSec: duration))

        // Verbatim device rows. `nil` status ⇒ `.success`.
        let deviceRows: [(pass: String, start: Double, end: Double, status: SemanticScanStatus)] = [
            ("passA", 18.96, 105.78, .success),
            ("passA", 18.96, 105.78, .success),
            ("passA", 107.03999999999999, 809.94, .success),
            ("passA", 107.03999999999999, 809.94, .success),
            ("passA", 810.48, 909.9, .success),
            ("passA", 810.48, 909.9, .success),
            ("passA", 913.56, 1556.28, .success),
            ("passA", 913.56, 1556.28, .success),
            ("passA", 1566.0, 1610.76, .success),
            ("passA", 1566.0, 1610.76, .success),
            ("passA", 1612.32, 1641.66, .permissiveDecodingFailure),
            ("passA", 1612.32, 1641.66, .permissiveDecodingFailure),
            ("passB", 18.96, 39.66, .success),
            ("passB", 18.96, 39.66, .success),
            ("passB", 107.03999999999999, 809.94, .success),
            ("passB", 107.03999999999999, 809.94, .success),
            ("passB", 791.58, 909.9, .permissiveDecodingFailure),
            ("passB", 810.48, 880.62, .success),
            ("passB", 810.48, 880.62, .success),
            ("passB", 1535.58, 1556.28, .success),
            ("passB", 1535.58, 1556.28, .success),
            ("passB", 1566.0, 1610.76, .success),
            ("passB", 1566.0, 1610.76, .success)
        ]
        for (index, row) in deviceRows.enumerated() {
            try await store.insertSemanticScanResult(makeScan(
                assetId: "a-cd1ad629", index: index,
                start: row.start, end: row.end,
                status: row.status, scanPass: row.pass
            ))
        }

        // The trap, reproduced from the same rows the store now holds.
        let persisted = try await store.fetchSemanticScanResults(analysisAssetId: "a-cd1ad629")
        #expect(persisted.count == deviceRows.count)
        let naiveSum = persisted
            .filter { $0.status == .success || $0.status == .noAds }
            .reduce(0.0) { $0 + ($1.windowEndTime - $1.windowStartTime) }
        #expect(abs(naiveSum - 4871.64) < 0.01)
        #expect(naiveSum / duration > 2.94)

        // The one true measure disagrees, and is the one the ✓ and the
        // terminal both read.
        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-cd1ad629"])
        let summary = try #require(summaries["a-cd1ad629"])
        #expect(abs(try #require(summary.adScanCoveredSec).rawValue - 1576.62) < 0.01)
        let fraction = try #require(summary.adScanFraction)
        #expect(abs(fraction - 0.9522) < 0.001)
        #expect(fraction <= 1.0)
        // Below the readiness floor: the episode is NOT ready, and the 294%
        // reading would have said it was long ago.
        #expect(fraction < episodePreparationCompleteThreshold)
    }

    /// Whatever the rows say, the persisted fraction is bounded. Windows that
    /// sprawl far past the episode cannot mint a ratio above 1 — the numerator
    /// is clipped to the transcribed region before the quotient is taken.
    @Test("ad-scan fraction can never exceed 1.0 however far the windows sprawl")
    func adScanFractionIsBoundedAboveByOne() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-sprawl", episodeDurationSec: 600))
        for index in 0..<5 {
            try await store.insertSemanticScanResult(makeScan(
                assetId: "a-sprawl", index: index,
                start: 0, end: 600 + Double(index) * 100
            ))
        }
        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-sprawl"])
        let summary = try #require(summaries["a-sprawl"])
        let fraction = try #require(summary.adScanFraction)
        #expect(fraction <= 1.0)
        #expect(fraction == 1.0)
    }

    // MARK: - playhead-9y9e: the bound is BOTH transcript passes

    private func makePassChunk(
        assetId: String,
        index: Int,
        start: Double,
        end: Double,
        pass: String
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-chunk-\(pass)-\(index)",
            analysisAssetId: assetId,
            segmentFingerprint: "\(assetId)-fp-\(pass)-\(index)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "t",
            normalizedText: "t",
            pass: pass,
            modelVersion: "test-asr",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    /// RT04 — THE FIELD SHAPE, asset 0C2FC22E on the 2026-08-03 device pull. Its
    /// two transcript passes are DISJOINT: final chunks hold `[0, 930]` and fast
    /// chunks hold `[930, 2086]` of a 2,086 s episode. The asset's terminal
    /// reason reads `transcript 1.000`, and every other watermark agrees — but
    /// the ad-scan bound was the FAST union alone, so a scan of the first half
    /// of that episode measured ZERO seconds examined.
    ///
    /// Scaled down here; the geometry (disjoint passes, scan window inside the
    /// final-only region) is the field's.
    @Test("a scan window inside FINAL-pass-only transcript is counted, not discarded")
    func adScanAreaCountsFinalPassOnlyRegions() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-9y9e-disjoint", episodeDurationSec: 1000))
        // Final pass backs [0, 400]; fast pass backs [400, 1000]. Nothing is
        // transcribed twice, which is what makes the fast-only reading 60%.
        try await store.insertTranscriptChunks([
            makePassChunk(assetId: "a-9y9e-disjoint", index: 0, start: 0, end: 400, pass: "final"),
            makePassChunk(assetId: "a-9y9e-disjoint", index: 1, start: 400, end: 1000, pass: "fast")
        ])
        // The scan examined the first 400 s — entirely inside the final-only
        // region, i.e. exactly the audio a fast-only bound throws away.
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-9y9e-disjoint", index: 0, start: 0, end: 400)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-9y9e-disjoint"])
        let summary = try #require(summaries["a-9y9e-disjoint"])

        #expect(summary.adScanCoveredSec == 400,
                "the scan read 400 s of real transcript; a fast-only bound reports 0")
        #expect(summary.adScanFraction == 0.4)

        // The TX figure is untouched. It names the FAST pass and must keep
        // naming it — this bead moved the ad-scan bound only.
        #expect(summary.fastTranscriptCoveredSec == 600)
        #expect(summary.fastTranscriptCoveredSource == .fastTranscriptChunks)
    }

    /// RT05 — the CEILING, which is the reach consequence. Under a fast-only
    /// bound, an asset whose transcript is mostly final-pass has an
    /// `adScanFraction` maximum below
    /// `AnalysisJobRunner.semanticBackfillSufficientAdScanFraction`, so NO scan,
    /// however complete, can ever retire it: every re-drive and every claim it
    /// mints is work that cannot satisfy itself, and the library ✓ is
    /// unreachable by construction.
    ///
    /// **The counts, re-derived on the 2026-08-03 pull (R1 review).** NINE of
    /// the twelve assets had a fast-only ceiling under the 0.98 floor. This
    /// change lifts FOUR of them over it — 0C2FC22E (0.554 → 1.000), AD5F3A0A
    /// (0.440 → 0.990), 83592353 (0.966 → 0.995), 53FC53E3 (0.979 → 0.993) —
    /// and leaves FIVE capped: 48E903D7 (0.369 → 0.951), D9B513CD
    /// (0.572 → 0.883), 44F076BB (0.811), 58882C47 (0.975), 2C5C3699
    /// (0.043 → 0.130). 48E903D7 is the trap: it is the most dramatic
    /// fast-vs-both movement on the pull and it still does NOT clear the floor,
    /// so it belongs in the second list, not the first.
    @Test("a fully scanned, mostly-final-pass episode can now reach the sufficiency floor")
    func fullyScannedFinalPassEpisodeReachesTheFloor() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-9y9e-ceiling", episodeDurationSec: 1000))
        // 90% of the transcript is final-pass. Fast alone caps the fraction at
        // 0.10 — far under the 0.98 floor.
        try await store.insertTranscriptChunks([
            makePassChunk(assetId: "a-9y9e-ceiling", index: 0, start: 0, end: 900, pass: "final"),
            makePassChunk(assetId: "a-9y9e-ceiling", index: 1, start: 900, end: 1000, pass: "fast")
        ])
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-9y9e-ceiling", index: 0, start: 0, end: 1000)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-9y9e-ceiling"])
        let summary = try #require(summaries["a-9y9e-ceiling"])
        let fraction = try #require(summary.adScanFraction)
        #expect(fraction == 1.0)
        #expect(fraction >= AnalysisJobRunner.semanticBackfillSufficientAdScanFraction)
        // And the fast-only reading, stated so the test names what it replaced.
        #expect(summary.fastTranscriptCoveredSec == 100)
    }

    /// RT06 — the bound still BINDS. Widening it to both passes must not turn it
    /// into "take the window's bounds at face value": a window whose span
    /// straddles audio NEITHER pass transcribed is still clipped, which is the
    /// whole reason playhead-pz32 intersects at all.
    @Test("widening the bound to both passes does not stop it clipping untranscribed audio")
    func bothPassBoundStillClipsUntranscribedAudio() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-9y9e-clip", episodeDurationSec: 1000))
        // A 300 s hole neither pass covers — wider than
        // `adScanBridgeableGapSec`, so bridging cannot close it.
        try await store.insertTranscriptChunks([
            makePassChunk(assetId: "a-9y9e-clip", index: 0, start: 0, end: 300, pass: "final"),
            makePassChunk(assetId: "a-9y9e-clip", index: 1, start: 600, end: 1000, pass: "fast")
        ])
        // One window whose BOUNDS span the whole episode.
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-9y9e-clip", index: 0, start: 0, end: 1000)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-9y9e-clip"])
        let summary = try #require(summaries["a-9y9e-clip"])
        #expect(summary.adScanCoveredSec == 700, "the 300 s hole must not be claimed as scanned")
        #expect(try #require(summary.adScanFraction) < episodePreparationCompleteThreshold)
        #expect(300 > AnalysisCoverageMath.adScanBridgeableGapSec,
                "fixture premise: the hole is wider than the bridge")
    }

    /// RT07 — `fetchTranscribedRegion` is what every caller outside this
    /// summary uses to ask the same question, so it must agree: both passes, and
    /// degenerate rows dropped (a zero-width row covers no time and must not be
    /// able to authorise anything).
    @Test("fetchTranscribedRegion spans both passes and drops degenerate rows")
    func transcriptCoveredRangesSpanBothPasses() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-9y9e-ranges", episodeDurationSec: 1000))
        try await store.insertTranscriptChunks([
            makePassChunk(assetId: "a-9y9e-ranges", index: 0, start: 0, end: 400, pass: "final"),
            makePassChunk(assetId: "a-9y9e-ranges", index: 1, start: 400, end: 900, pass: "fast"),
            // Degenerate: end == start.
            makePassChunk(assetId: "a-9y9e-ranges", index: 2, start: 950, end: 950, pass: "fast")
        ])

        let all = try await store.fetchTranscribedRegion(assetId: "a-9y9e-ranges")
        #expect(all.intervalCount == 2)
        #expect(all.unionedSeconds == 900)

        // The fast-only sibling is unchanged and still answers the narrower
        // question — "how far has the FAST pass got?" — which is a real question
        // even though playhead-6r4z moved its last production consumer, the
        // transcript engine's shard-ordering index, onto the union above.
        let fastOnly = try await store.fetchFastTranscriptCoveredRanges(assetId: "a-9y9e-ranges")
        #expect(AnalysisCoverageMath.unionedSeconds(fastOnly) == 500)
    }

    /// RT12 (R1 review) — WIDENING A BOUND IS NOT AUTOMATICALLY MONOTONE, and
    /// this is the shape where it is not.
    ///
    /// The ad-scan bound has always had a WATERMARK fallback: with no fast
    /// chunks on disk but a `fastTranscriptCoverageEndTime` on the asset, the
    /// transcribed region is modelled as one contiguous `[0, watermark]` span.
    /// Building the widened bound out of the two CHUNK sets alone discards that
    /// span — so on an asset whose watermark outlives its chunks (playhead-0sro's
    /// shape) the "widened" bound is the final-pass chunks only, which are
    /// neither contiguous nor obliged to reach the watermark, and the measured
    /// ad-scan area goes DOWN. Here: 1,000 s before, 300 s after.
    ///
    /// `adScanCoveredSec` feeds the library ✓ and every "is this owed?" gate, so
    /// an episode silently measuring less scanned than it did yesterday is the
    /// same class of defect as the one this bead fixed, pointing the other way.
    @Test("the widened ad-scan bound never measures LESS than the watermark fallback did")
    func widenedBoundNeverShrinksTheWatermarkFallback() async throws {
        let store = try await makeTestStore()
        // Watermark = duration (this suite's `makeAsset`), and NO fast chunks —
        // so the watermark fallback is the only thing describing [300, 1000].
        try await store.insertAsset(makeAsset(id: "a-9y9e-mono", episodeDurationSec: 1000))
        try await store.insertTranscriptChunks([
            makePassChunk(assetId: "a-9y9e-mono", index: 0, start: 0, end: 300, pass: "final")
        ])
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-9y9e-mono", index: 0, start: 0, end: 1000)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-9y9e-mono"])
        let summary = try #require(summaries["a-9y9e-mono"])
        #expect(summary.adScanCoveredSec == 1000,
                "the watermark fallback bound the whole episode before this bead and must still")
        #expect(summary.adScanFraction == 1.0)

        // The premise, stated so a fixture drift cannot make this vacuous: the
        // fallback really is in play, i.e. no fast chunk backs any of it.
        #expect(summary.fastTranscriptCoveredSource == .assetWatermark)
        #expect(summary.fastTranscriptCoveredSec == 1000)
    }

    /// RT13 (R1 review) — the final-pass query now drops degenerate rows from
    /// `MAX(endTime)` as well as from the intervals, which is the FAST query's
    /// long-standing rule finally applied to its sibling. The consequence worth
    /// pinning is the one nothing else reaches: when EVERY final row is
    /// degenerate there is no final-pass chunk evidence at all, so
    /// `finalPassCoverageEndSec` must fall back to the asset watermark and SAY
    /// SO, rather than reporting a zero-width row's timestamp as a pass reach.
    ///
    /// (On the 2026-08-03 device pull this changes no asset — 16 of 37,498
    /// chunks are degenerate, all `endTime == startTime`, all ordinary
    /// single-token ASR output like `"Oh"` and `"their"`, and none is any
    /// asset's `MAX(endTime)`. The rail exists because the behaviour is new,
    /// not because the field exercises it.)
    @Test("an all-degenerate final pass reports the watermark, not a zero-width row")
    func allDegenerateFinalPassFallsBackToWatermark() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-9y9e-degen", episodeDurationSec: 1000, finalPassCoverageEndTime: 640
        ))
        try await store.insertTranscriptChunks([
            makePassChunk(assetId: "a-9y9e-degen", index: 0, start: 0, end: 500, pass: "fast"),
            // The whole final pass is zero-width. It covers no time, so it is
            // not evidence the final pass reached 950 s.
            makePassChunk(assetId: "a-9y9e-degen", index: 1, start: 950, end: 950, pass: "final"),
            makePassChunk(assetId: "a-9y9e-degen", index: 2, start: 300, end: 300, pass: "final")
        ])

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-9y9e-degen"])
        let summary = try #require(summaries["a-9y9e-degen"])
        #expect(summary.finalPassCoverageEndSec == 640)
        #expect(summary.finalPassCoverageEndSource == .assetWatermark)
        // And the degenerate rows widen no bound either: the fast pass backs
        // [0, 500] and nothing claims [500, 1000].
        #expect(
            try await store.fetchTranscribedRegion(assetId: "a-9y9e-degen").unionedSeconds == 500
        )
    }

    /// RT14 (R2 review) — WIDENING THE BOUND WIDENED WHAT THE SANITY GUARD HAS
    /// TO WATCH, and the guard was left reading one pass.
    ///
    /// `adScanFraction` withholds when the transcript's own reach has already
    /// disproved the declared duration (the E8F0F867 shape: a numerator that
    /// LOOKS proportionate divided by a denominator describing different audio).
    /// That check read `fastTranscriptCoverageEndSec` — correct while
    /// `adScanCoveredSec` was bounded by the fast union, and incoherent the
    /// moment playhead-9y9e bounded it by BOTH passes: a final-pass-heavy asset
    /// can now draw its numerator from audio the fast reach knows nothing about,
    /// so a short fast reach keeps the guard silent over an arbitrarily wrong
    /// duration.
    ///
    /// Here the fast pass backs `[0, 100]` and the final pass `[0, 4000]` of a
    /// declared 600 s episode, with 610 s scanned. The overshoot check passes
    /// (610 is inside 600 + 30), the fast reach is 100 and says nothing, and the
    /// pre-review guard returns a confident `1.0` on a denominator six times
    /// short. No asset on the 2026-08-03 pull exercises this — every
    /// `finalPassCoverageEndTime` sits inside the tolerance — so this is a rail
    /// for behaviour the change made reachable, not a repair of an observed
    /// defect.
    @Test("a final-pass reach past the duration withholds the fraction too")
    func finalPassReachPastDurationWithholdsTheFraction() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-9y9e-reach", episodeDurationSec: 600))
        try await store.insertTranscriptChunks([
            makePassChunk(assetId: "a-9y9e-reach", index: 0, start: 0, end: 100, pass: "fast"),
            // The final pass runs to 4,000 s of a declared 600 s episode.
            makePassChunk(assetId: "a-9y9e-reach", index: 1, start: 0, end: 4000, pass: "final")
        ])
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-9y9e-reach", index: 0, start: 0, end: 610)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-9y9e-reach"])
        let summary = try #require(summaries["a-9y9e-reach"])

        // The premises, stated so fixture drift cannot make this vacuous.
        #expect(summary.adScanCoveredSec == 610, "the numerator is drawn from the final pass")
        let tolerance = AnalysisCoverageSummary.adScanDurationToleranceSec(episodeDurationSec: 600)
        #expect(610 <= 600 + tolerance, "premise: the overshoot check does NOT fire")
        #expect(summary.fastTranscriptCoverageEndSec == 100,
                "premise: the FAST reach is short, so a fast-only guard stays silent")
        #expect(summary.finalPassCoverageEndSec == 4000)

        #expect(summary.adScanFraction == nil,
                "the final pass has disproved the denominator; 610/600 must not read as a ✓")
    }

    /// RT16 (R3 review) — the OTHER direction of RT14's guard, and the one that
    /// can make an episode LESS ready.
    ///
    /// `finalPassCoverageEndSec` is chunks-first with a fallback to the
    /// `analysis_assets.finalPassCoverageEndTime` COLUMN. When no final chunk is
    /// on disk the column is all that is left — playhead-0sro's "watermark
    /// outliving the rows it claims" shape, reachable because playhead-wvdz's
    /// chunk deletion outlives the asset row, and the same shape RT12 and
    /// `watermarkWithoutChunksStillFails` exist for. Admitting it into the
    /// duration-sanity max lets a STALE column withhold a fraction that is
    /// perfectly fine.
    ///
    /// Here the fast pass backs the whole 600 s episode and 590 s are scanned —
    /// a healthy ✓ — while a stale final column claims 4,000 s with not one
    /// final chunk behind it. The fraction must still be produced. This is the
    /// direction the bead's own monotonicity argument forbids moving.
    @Test("a final-pass WATERMARK with no chunks behind it never withholds the fraction")
    func finalPassWatermarkWithoutChunksDoesNotWithhold() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(
            id: "a-9y9e-stalefinal", episodeDurationSec: 600, finalPassCoverageEndTime: 4000
        ))
        // Fast chunks only. Nothing final on disk.
        try await store.insertTranscriptChunks([
            makePassChunk(assetId: "a-9y9e-stalefinal", index: 0, start: 0, end: 600, pass: "fast")
        ])
        try await store.insertSemanticScanResult(
            makeScan(assetId: "a-9y9e-stalefinal", index: 0, start: 0, end: 590)
        )

        let summaries = try await store.fetchCoverageSummariesByAssetIds(["a-9y9e-stalefinal"])
        let summary = try #require(summaries["a-9y9e-stalefinal"])

        // The premises, so fixture drift cannot make this vacuous: the stale
        // column IS what `finalPassCoverageEndSec` reports, it IS past the
        // tolerance, and its provenance is the watermark rather than chunks.
        #expect(summary.finalPassCoverageEndSec == 4000)
        #expect(summary.finalPassCoverageEndSource == .assetWatermark)
        #expect(4000 > 600 + AnalysisCoverageSummary.adScanDurationToleranceSec(
            episodeDurationSec: 600
        ), "premise: the stale column would trip the guard if it were admitted")

        #expect(summary.adScanCoveredSec == 590)
        #expect(summary.adScanFraction != nil,
                "a watermark with no chunks behind it is not evidence, and must not withhold a ✓")
    }

    /// The bound, tested where it lives rather than only through the store, so
    /// a numerator that DOES exceed the denominator is exercised directly.
    /// `adScanFraction` must WITHHOLD — a numerator materially past the
    /// denominator proves the two describe different audio, and clamping such
    /// an overshoot to exactly 1.0 is how a broken denominator becomes a
    /// confident ✓ on a fraction of the audio. It reads as unmeasurable, which
    /// gqx4 then names `unmeasurableDuration`.
    @Test("a numerator past the denominator is withheld, never clamped to 1.0")
    func overshootingNumeratorIsWithheldNotClamped() {
        func summary(
            adScanCoveredSec: AdScanSeconds?,
            episodeDurationSec: EpisodeSeconds?,
            transcriptReach: Double?
        ) -> AnalysisCoverageSummary {
            AnalysisCoverageSummary(
                assetId: "a-bound",
                episodeDurationSec: episodeDurationSec,
                fastTranscriptCoveredSec: transcriptReach.map { CoveredSeconds($0) },
                fastTranscriptCoveredSource: .fastTranscriptChunks,
                fastTranscriptCoverageEndSec: transcriptReach.map { WatermarkSeconds($0) },
                fastTranscriptCoverageEndSource: .fastTranscriptChunks,
                featureCoverageEndSec: episodeDurationSec.map { FrontierSeconds($0.rawValue) },
                featureCoverageEndSource: .assetWatermark,
                confirmedAdCoverageEndSec: nil,
                confirmedAdCoverageEndSource: .unknown,
                finalPassCoverageEndSec: nil,
                finalPassCoverageEndSource: .unknown,
                analysisCoveredSec: nil,
                adScanCoveredSec: adScanCoveredSec,
                adScanCoveredSource: adScanCoveredSec == nil ? .unknown : .semanticScanResults,
                // playhead-nffz: this fixture is about the OVERSHOOT guards, so
                // the ceiling tracks the transcript reach the case supplies.
                adScanCeilingSec: transcriptReach.map { BridgedTranscriptSeconds($0) }
            )
        }

        // The bead's own shape: 3210 s of scan against a declared 608 s.
        #expect(
            summary(adScanCoveredSec: 3210, episodeDurationSec: 608, transcriptReach: 3210)
                .adScanFraction == nil
        )
        // Even a healthy-LOOKING ratio is withheld when the transcript's own
        // reach has already disproved the denominator (device asset E8F0F867:
        // 563.8 s scanned, 552.9 s declared, transcript reaching 3810 s). This
        // is the arm that would otherwise render a confident ✓ on 14.8%.
        #expect(
            summary(adScanCoveredSec: 563.8, episodeDurationSec: 552.9, transcriptReach: 3810)
                .adScanFraction == nil
        )
        // Within tolerance (min(30 s, 5%)), a real ratio is still produced and
        // is capped at 1.0 rather than withheld — decoder tail drift must not
        // cost an episode its ✓.
        let drifted = summary(
            adScanCoveredSec: 1010, episodeDurationSec: 1000, transcriptReach: 1010
        ).adScanFraction
        #expect(drifted == 1.0)
        // And an ordinary partial scan is unaffected.
        #expect(
            summary(adScanCoveredSec: 470, episodeDurationSec: 1000, transcriptReach: 1000)
                .adScanFraction == 0.47
        )
    }
}
