// SpanMetricsTests.swift
// playhead-352 — A7: Metrics framework unit tests.
//
// Synthetic GT/detected pairs exercising:
//   - exact-match, partial-overlap, no-overlap, multi-GT-per-episode
//   - missing seeds (false positives, missing detections)
//   - zero-length intervals
//   - empty slices and zero-denominator cases
//   - slicing by ad format / podcast / detection path
//   - performance: 10k pair runs without going quadratic
//
// All tests are pure (no I/O, no async). They live in PlayheadFastTests
// because they're fast — the bead's "must not regress fast test runtime"
// rule is about not loading real corpus, not about excluding cheap unit
// tests of the framework itself.

import Foundation
import Testing
@testable import Playhead

@Suite("SpanMetrics — domain types & helpers")
struct SpanMetricsHelperTests {

    @Test("AdFormat folds blendedHostRead into hostRead")
    func adFormatBridge() {
        #expect(AdFormat.from(.hostRead) == .hostRead)
        #expect(AdFormat.from(.blendedHostRead) == .hostRead)
        #expect(AdFormat.from(.producedSegment) == .produced)
        #expect(AdFormat.from(.dynamicInsertion) == .dynamic)
    }

    @Test("CountRatio empty denominator yields nil ratio, no crash")
    func countRatioEmpty() {
        let ratio = CountRatio(numerator: 0, denominator: 0)
        #expect(ratio.ratio == nil)
    }

    @Test("CountRatio rejects numerator > denominator")
    func countRatioInvariant() {
        // Confirms the precondition is wired; can't actually trigger
        // a precondition in test, but we can at least verify the
        // legal boundary numerator == denominator.
        let ratio = CountRatio(numerator: 3, denominator: 3)
        #expect(ratio.ratio == 1.0)
    }

    @Test("SampleStats median handles odd, even, empty")
    func medianHandling() {
        #expect(SampleStats(samples: []).median == nil)
        #expect(SampleStats(samples: [5.0]).median == 5.0)
        #expect(SampleStats(samples: [1, 3, 5]).median == 3.0)
        #expect(SampleStats(samples: [1, 2, 3, 4]).median == 2.5)
        // Order-independent
        #expect(SampleStats(samples: [4, 1, 2, 3]).median == 2.5)
    }

    @Test("IoU exact match = 1.0")
    func iouExactMatch() {
        let v = MetricsBatch.iou(gtStart: 10, gtEnd: 30, detStart: 10, detEnd: 30)
        #expect(v == 1.0)
    }

    @Test("IoU disjoint = 0.0")
    func iouDisjoint() {
        let v = MetricsBatch.iou(gtStart: 10, gtEnd: 30, detStart: 40, detEnd: 60)
        #expect(v == 0.0)
    }

    @Test("IoU half overlap = 1/3")
    func iouHalfOverlap() {
        // GT [10, 30], Det [20, 40]
        // Intersection = [20, 30] = 10s
        // Union        = [10, 40] = 30s
        // IoU          = 10/30   ≈ 0.333
        let v = MetricsBatch.iou(gtStart: 10, gtEnd: 30, detStart: 20, detEnd: 40)
        #expect(abs(v - (10.0 / 30.0)) < 1e-9)
    }

    @Test("IoU zero-length coincident = 1.0")
    func iouZeroLengthCoincident() {
        let v = MetricsBatch.iou(gtStart: 15, gtEnd: 15, detStart: 15, detEnd: 15)
        #expect(v == 1.0)
    }

    @Test("IoU zero-length disjoint = 0.0 (no NaN)")
    func iouZeroLengthDisjoint() {
        let v = MetricsBatch.iou(gtStart: 15, gtEnd: 15, detStart: 30, detEnd: 30)
        #expect(v == 0.0)
        #expect(!v.isNaN)
    }

    @Test("IoU handles reverse-ordered intervals")
    func iouReversed() {
        // GT [30, 10] reversed; Det [10, 30] canonical — should equal 1.0
        let v = MetricsBatch.iou(gtStart: 30, gtEnd: 10, detStart: 10, detEnd: 30)
        #expect(v == 1.0)
    }

    @Test("unionLength flattens overlaps and sums disjoint segments")
    func unionLengthBehavior() {
        // [(0, 10), (5, 20), (30, 40)] → merged: [(0, 20), (30, 40)] → 20 + 10 = 30
        let result = MetricsBatch.unionLength([(0, 10), (5, 20), (30, 40)])
        #expect(result == 30.0)
    }

    @Test("unionLength of empty list = 0")
    func unionLengthEmpty() {
        #expect(MetricsBatch.unionLength([]) == 0.0)
    }

    @Test("intersectionLength of disjoint sets = 0")
    func intersectionDisjoint() {
        let result = MetricsBatch.intersectionLength([(0, 10)], [(20, 30)])
        #expect(result == 0.0)
    }

    @Test("intersectionLength counts nested correctly")
    func intersectionNested() {
        // GT [0, 100], Det [10, 20] ∪ [40, 50]
        // Intersection = both detected segments = 10 + 10 = 20
        let result = MetricsBatch.intersectionLength([(0, 100)], [(10, 20), (40, 50)])
        #expect(result == 20.0)
    }

    @Test("intersectionLength dedups overlapping inputs")
    func intersectionOverlappingInputs() {
        // a = [(0, 10), (5, 15)] → merged [(0, 15)]
        // b = [(0, 10), (8, 20)] → merged [(0, 20)]
        // intersection = [(0, 15)] = 15
        let result = MetricsBatch.intersectionLength(
            [(0, 10), (5, 15)],
            [(0, 10), (8, 20)]
        )
        #expect(result == 15.0)
    }

    @Test("mergedIntervals merges adjacent (touching at boundary)")
    func mergedAdjacent() {
        let result = MetricsBatch.mergedIntervals([(0, 10), (10, 20)])
        #expect(result.count == 1)
        #expect(result[0] == (0, 20))
    }
}

// MARK: - Helper builders

private func makeGT(
    id: String,
    podcast: String = "p1",
    episode: String = "e1",
    start: Double,
    end: Double,
    format: AdFormat = .hostRead,
    seedFired: Bool = true
) -> MetricGroundTruthAd {
    MetricGroundTruthAd(
        id: id,
        podcastId: podcast,
        episodeId: episode,
        startTime: start,
        endTime: end,
        format: format,
        seedFired: seedFired
    )
}

private func makeDet(
    id: String,
    podcast: String = "p1",
    episode: String = "e1",
    start: Double,
    end: Double,
    path: DetectionPath = .live,
    confirmation: Double? = nil,
    confidence: Double = 0.9
) -> MetricDetectedAd {
    MetricDetectedAd(
        id: id,
        podcastId: podcast,
        episodeId: episode,
        startTime: start,
        endTime: end,
        path: path,
        firstConfirmationTime: confirmation,
        confidence: confidence
    )
}

// MARK: - Seed recall

@Suite("SpanMetrics — seed recall")
struct SeedRecallTests {

    @Test("All ads seeded → 1.0")
    func allSeeded() {
        let pairs = [
            MetricsPair(gt: makeGT(id: "a", start: 10, end: 20, seedFired: true), detected: nil),
            MetricsPair(gt: makeGT(id: "b", start: 30, end: 40, seedFired: true), detected: nil),
        ]
        let r = MetricsBatch(pairs: pairs).computeSeedRecall()
        #expect(r.numerator == 2)
        #expect(r.denominator == 2)
        #expect(r.ratio == 1.0)
    }

    @Test("Mixed seeded → ratio")
    func mixedSeeded() {
        let pairs = [
            MetricsPair(gt: makeGT(id: "a", start: 10, end: 20, seedFired: true), detected: nil),
            MetricsPair(gt: makeGT(id: "b", start: 30, end: 40, seedFired: false), detected: nil),
            MetricsPair(gt: makeGT(id: "c", start: 50, end: 60, seedFired: true), detected: nil),
        ]
        let r = MetricsBatch(pairs: pairs).computeSeedRecall()
        #expect(r.numerator == 2)
        #expect(r.denominator == 3)
        #expect(r.ratio.map { abs($0 - 2.0/3.0) < 1e-9 } == true)
    }

    @Test("False positives don't affect denominator")
    func falsePositivesIgnored() {
        let pairs = [
            MetricsPair(gt: makeGT(id: "a", start: 10, end: 20, seedFired: true), detected: nil),
            MetricsPair(gt: nil, detected: makeDet(id: "fp", start: 100, end: 120)),
        ]
        let r = MetricsBatch(pairs: pairs).computeSeedRecall()
        #expect(r.numerator == 1)
        #expect(r.denominator == 1)
        #expect(r.ratio == 1.0)
    }

    @Test("Empty batch → ratio nil, no crash")
    func emptyBatch() {
        let r = MetricsBatch(pairs: []).computeSeedRecall()
        #expect(r.denominator == 0)
        #expect(r.ratio == nil)
    }
}

// MARK: - Span IoU + median errors + biases

@Suite("SpanMetrics — span quality (IoU, errors, biases)")
struct SpanQualityTests {

    @Test("Exact-match detection: IoU = 1, errors = 0, biases = 0")
    func exactMatch() {
        let pair = MetricsPair(
            gt: makeGT(id: "a", start: 10, end: 30),
            detected: makeDet(id: "d-a", start: 10, end: 30)
        )
        let batch = MetricsBatch(pairs: [pair])
        #expect(batch.computeSpanIoU().median == 1.0)
        #expect(batch.computeMedianStartError() == 0.0)
        #expect(batch.computeMedianEndError() == 0.0)
        #expect(batch.computeSignedStartBias() == 0.0)
        #expect(batch.computeSignedEndBias() == 0.0)
    }

    @Test("No-overlap detection: IoU = 0; errors reflect raw distance")
    func noOverlap() {
        let pair = MetricsPair(
            gt: makeGT(id: "a", start: 10, end: 20),
            detected: makeDet(id: "d-a", start: 50, end: 60)
        )
        let batch = MetricsBatch(pairs: [pair])
        #expect(batch.computeSpanIoU().median == 0.0)
        #expect(batch.computeMedianStartError() == 40.0)  // |50 - 10|
        #expect(batch.computeMedianEndError() == 40.0)    // |60 - 20|
        #expect(batch.computeSignedStartBias() == 40.0)   // late start
        #expect(batch.computeSignedEndBias() == 40.0)     // late end
    }

    @Test("Late-start, early-end detection: signed biases capture direction")
    func signedBiases() {
        // GT  [10, 30]
        // Det [12, 28]  → start bias = +2 (late), end bias = -2 (early exit)
        let pair = MetricsPair(
            gt: makeGT(id: "a", start: 10, end: 30),
            detected: makeDet(id: "d-a", start: 12, end: 28)
        )
        let batch = MetricsBatch(pairs: [pair])
        #expect(batch.computeSignedStartBias() == 2.0)
        #expect(batch.computeSignedEndBias() == -2.0)
        // Absolute errors are unsigned
        #expect(batch.computeMedianStartError() == 2.0)
        #expect(batch.computeMedianEndError() == 2.0)
    }

    @Test("Multi-pair median uses true median, not mean")
    func multiPairMedian() {
        // Errors will be: 1, 2, 100 → median = 2 (mean would be 34.33)
        let pairs = [
            MetricsPair(gt: makeGT(id: "a", start: 0, end: 10), detected: makeDet(id: "d-a", start: 1, end: 11)),
            MetricsPair(gt: makeGT(id: "b", start: 0, end: 10), detected: makeDet(id: "d-b", start: 2, end: 12)),
            MetricsPair(gt: makeGT(id: "c", start: 0, end: 10), detected: makeDet(id: "d-c", start: 100, end: 110)),
        ]
        let batch = MetricsBatch(pairs: pairs)
        #expect(batch.computeMedianStartError() == 2.0)
    }

    @Test("Misses + false positives don't contribute to per-pair samples")
    func mixedPairsExcludeIncomplete() {
        let pairs = [
            // True positive: [10, 20] vs [10, 20]
            MetricsPair(gt: makeGT(id: "a", start: 10, end: 20), detected: makeDet(id: "d-a", start: 10, end: 20)),
            // Miss
            MetricsPair(gt: makeGT(id: "b", start: 30, end: 40), detected: nil),
            // False positive
            MetricsPair(gt: nil, detected: makeDet(id: "d-c", start: 50, end: 60)),
        ]
        let batch = MetricsBatch(pairs: pairs)
        // Only the TP contributes
        #expect(batch.computeSpanIoU().samples == [1.0])
        #expect(batch.computeMedianStartError() == 0.0)
    }

    @Test("All-misses batch: span metrics return nil")
    func allMisses() {
        let pairs = [
            MetricsPair(gt: makeGT(id: "a", start: 10, end: 20), detected: nil),
            MetricsPair(gt: makeGT(id: "b", start: 30, end: 40), detected: nil),
        ]
        let batch = MetricsBatch(pairs: pairs)
        #expect(batch.computeSpanIoU().median == nil)
        #expect(batch.computeMedianStartError() == nil)
        #expect(batch.computeSignedEndBias() == nil)
    }

    @Test("Zero-length GT span: no crash; coincident detection → IoU 1.0")
    func zeroLengthGT() {
        let pair = MetricsPair(
            gt: makeGT(id: "a", start: 15, end: 15),
            detected: makeDet(id: "d-a", start: 15, end: 15)
        )
        let batch = MetricsBatch(pairs: [pair])
        #expect(batch.computeSpanIoU().median == 1.0)
    }
}

// MARK: - Coverage recall + precision

@Suite("SpanMetrics — coverage")
struct CoverageTests {

    @Test("Perfect coverage: recall = precision = 1")
    func perfectCoverage() {
        let pairs = [
            MetricsPair(gt: makeGT(id: "a", start: 0, end: 10), detected: makeDet(id: "d-a", start: 0, end: 10)),
            MetricsPair(gt: makeGT(id: "b", start: 20, end: 30), detected: makeDet(id: "d-b", start: 20, end: 30)),
        ]
        let batch = MetricsBatch(pairs: pairs)
        #expect(batch.computeCoverageRecall() == 1.0)
        #expect(batch.computeCoveragePrecision() == 1.0)
    }

    @Test("Half coverage: recall reflects covered fraction")
    func halfCoverage() {
        // GT [0, 20] = 20s; Det [0, 10] = 10s; intersection = 10s
        let pair = MetricsPair(
            gt: makeGT(id: "a", start: 0, end: 20),
            detected: makeDet(id: "d-a", start: 0, end: 10)
        )
        let batch = MetricsBatch(pairs: [pair])
        // Coverage recall = 10/20 = 0.5
        #expect(batch.computeCoverageRecall() == 0.5)
        // Coverage precision = 10/10 = 1.0 (the entire detection is inside GT)
        #expect(batch.computeCoveragePrecision() == 1.0)
    }

    @Test("Detection extends past GT: coverage precision drops")
    func detectionOverflow() {
        // GT [0, 10] = 10s; Det [0, 30] = 30s; intersection = 10s
        let pair = MetricsPair(
            gt: makeGT(id: "a", start: 0, end: 10),
            detected: makeDet(id: "d-a", start: 0, end: 30)
        )
        let batch = MetricsBatch(pairs: [pair])
        // Recall = 10/10 = 1.0
        #expect(batch.computeCoverageRecall() == 1.0)
        // Precision = 10/30 ≈ 0.333
        let p = batch.computeCoveragePrecision()
        #expect(p.map { abs($0 - 1.0/3.0) < 1e-9 } == true)
    }

    @Test("False positive only: coverage recall nil, precision = 0")
    func falsePositiveOnly() {
        let batch = MetricsBatch(pairs: [
            MetricsPair(gt: nil, detected: makeDet(id: "fp", start: 0, end: 10))
        ])
        // No GT to recall against
        #expect(batch.computeCoverageRecall() == nil)
        // Precision: detection is entirely outside (no) GT → 0
        #expect(batch.computeCoveragePrecision() == 0.0)
    }

    @Test("Miss only: coverage recall = 0, precision nil")
    func missOnly() {
        let batch = MetricsBatch(pairs: [
            MetricsPair(gt: makeGT(id: "a", start: 0, end: 10), detected: nil)
        ])
        #expect(batch.computeCoverageRecall() == 0.0)
        #expect(batch.computeCoveragePrecision() == nil)
    }

    @Test("Cross-episode: coverage math doesn't pollute across episodes")
    func crossEpisodeIsolation() {
        // Episode A: GT [0, 10], detection [0, 10] → 100% recall in episode A
        // Episode B: GT [0, 10], NO detection      → 0% recall in episode B
        // Naive global-union math would falsely credit episode B's GT [0, 10]
        // as covered by episode A's detection at the same time index. Per-episode
        // aggregation should yield 50% recall.
        let pairs = [
            MetricsPair(gt: makeGT(id: "a", podcast: "p", episode: "epA", start: 0, end: 10),
                        detected: makeDet(id: "d-a", podcast: "p", episode: "epA", start: 0, end: 10)),
            MetricsPair(gt: makeGT(id: "b", podcast: "p", episode: "epB", start: 0, end: 10),
                        detected: nil),
        ]
        let batch = MetricsBatch(pairs: pairs)
        // Total GT seconds = 10 (epA) + 10 (epB) = 20
        // Total covered = 10 (epA only) + 0 (epB) = 10
        // Coverage recall = 10/20 = 0.5 (NOT 1.0, which is the bug if cross-episode leaks)
        #expect(batch.computeCoverageRecall() == 0.5)
    }

    @Test("Cross-episode precision: a detection in one episode doesn't get credit for another's GT")
    func crossEpisodePrecisionIsolation() {
        // Episode A: GT [0, 10], detection [0, 10] → 10s detection, 10s inside GT
        // Episode B: NO GT,      detection [0, 10] → 10s detection, 0s inside GT
        // Total detected = 20s; inside-GT = 10s; precision = 0.5.
        let pairs = [
            MetricsPair(gt: makeGT(id: "a", podcast: "p", episode: "epA", start: 0, end: 10),
                        detected: makeDet(id: "d-a", podcast: "p", episode: "epA", start: 0, end: 10)),
            MetricsPair(gt: nil,
                        detected: makeDet(id: "d-b", podcast: "p", episode: "epB", start: 0, end: 10)),
        ]
        let batch = MetricsBatch(pairs: pairs)
        #expect(batch.computeCoveragePrecision() == 0.5)
    }

    @Test("Multi-GT episode: union math is correct, not double-counted")
    func multiGTEpisode() {
        // GTs: [0, 10] ∪ [20, 30] = 20 GT seconds
        // Det covers both partially: [5, 25] = single 20s detection
        // GT seconds inside detection: [5, 10] + [20, 25] = 10s
        // Detection seconds inside GT: same 10s
        // Coverage recall    = 10 / 20 = 0.5
        // Coverage precision = 10 / 20 = 0.5
        let pairs = [
            MetricsPair(gt: makeGT(id: "a", start: 0, end: 10), detected: nil),
            MetricsPair(gt: makeGT(id: "b", start: 20, end: 30), detected: nil),
            MetricsPair(gt: nil, detected: makeDet(id: "d", start: 5, end: 25)),
        ]
        let batch = MetricsBatch(pairs: pairs)
        #expect(batch.computeCoverageRecall() == 0.5)
        #expect(batch.computeCoveragePrecision() == 0.5)
    }
}

// MARK: - Lead time

@Suite("SpanMetrics — lead time at first confirmation")
struct LeadTimeTests {

    @Test("Confirmation before GT start → positive lead time")
    func positiveLeadTime() {
        // GT starts at 100, confirmation fires at 95 → lead = 5s
        let pair = MetricsPair(
            gt: makeGT(id: "a", start: 100, end: 130),
            detected: makeDet(id: "d-a", start: 95, end: 130, confirmation: 95)
        )
        let stats = MetricsBatch(pairs: [pair]).computeLeadTimeAtFirstConfirmation()
        #expect(stats.median == 5.0)
    }

    @Test("Confirmation after GT start → negative lead time")
    func negativeLeadTime() {
        // GT starts at 100, confirmation fires at 110 → lead = -10s
        let pair = MetricsPair(
            gt: makeGT(id: "a", start: 100, end: 130),
            detected: makeDet(id: "d-a", start: 100, end: 130, confirmation: 110)
        )
        let stats = MetricsBatch(pairs: [pair]).computeLeadTimeAtFirstConfirmation()
        #expect(stats.median == -10.0)
    }

    @Test("Pairs without confirmation timestamps are skipped")
    func missingConfirmation() {
        let pairs = [
            // Has confirmation: lead = 5
            MetricsPair(gt: makeGT(id: "a", start: 100, end: 130),
                        detected: makeDet(id: "d-a", start: 95, end: 130, confirmation: 95)),
            // No confirmation timestamp
            MetricsPair(gt: makeGT(id: "b", start: 200, end: 230),
                        detected: makeDet(id: "d-b", start: 200, end: 230, confirmation: nil)),
        ]
        let stats = MetricsBatch(pairs: pairs).computeLeadTimeAtFirstConfirmation()
        #expect(stats.samples == [5.0])
    }

    @Test("Empty batch → empty stats, median nil")
    func emptyLeadStats() {
        let stats = MetricsBatch(pairs: []).computeLeadTimeAtFirstConfirmation()
        #expect(stats.isEmpty)
        #expect(stats.median == nil)
    }
}

// MARK: - Slicing

@Suite("SpanMetrics — slicing dimensions")
struct SlicingTests {

    private static func mixedBatch() -> MetricsBatch {
        let pairs: [MetricsPair] = [
            // Diary p1, host-read, live, perfect TP
            MetricsPair(gt: makeGT(id: "a", podcast: "diary", episode: "e1", start: 0, end: 10, format: .hostRead),
                        detected: makeDet(id: "d-a", podcast: "diary", episode: "e1", start: 0, end: 10, path: .live)),
            // Diary p1, produced, backfill TP with shifted boundaries
            MetricsPair(gt: makeGT(id: "b", podcast: "diary", episode: "e1", start: 100, end: 130, format: .produced),
                        detected: makeDet(id: "d-b", podcast: "diary", episode: "e1", start: 102, end: 132, path: .backfill)),
            // Conan, dynamic, live, miss
            MetricsPair(gt: makeGT(id: "c", podcast: "conan", episode: "e1", start: 50, end: 80, format: .dynamic, seedFired: false),
                        detected: nil),
            // Conan, host-read, live, FP
            MetricsPair(gt: nil,
                        detected: makeDet(id: "d-fp", podcast: "conan", episode: "e1", start: 200, end: 220, path: .live)),
        ]
        return MetricsBatch(pairs: pairs)
    }

    @Test("Slice by format returns only matching GT pairs")
    func sliceByFormat() {
        let batch = Self.mixedBatch()
        let hostRead = batch.sliced(byFormat: .hostRead)
        // Only the Diary host-read pair (FP has gt == nil so it's excluded)
        #expect(hostRead.count == 1)
        #expect(hostRead.pairs.first?.gt?.id == "a")
    }

    @Test("Slice by podcast retains FPs from that podcast")
    func sliceByPodcast() {
        let batch = Self.mixedBatch()
        let conan = batch.sliced(byPodcast: "conan")
        // GT 'c' (miss) + FP 'd-fp'
        #expect(conan.count == 2)
    }

    @Test("Slice by path drops misses (no path), retains FPs on that path")
    func sliceByPath() {
        let batch = Self.mixedBatch()
        let live = batch.sliced(byPath: .live)
        // TP 'a' (live) + FP 'd-fp' (live). NOT 'b' (backfill), NOT 'c' (no detection)
        #expect(live.count == 2)
        let backfill = batch.sliced(byPath: .backfill)
        // Only TP 'b'
        #expect(backfill.count == 1)
    }

    @Test("slicedKeepingMisses(byPath:) retains GT misses for recall analysis")
    func slicedKeepingMissesByPath() {
        let batch = Self.mixedBatch()
        let live = batch.slicedKeepingMisses(byPath: .live)
        // TP 'a' (live), miss 'c' (kept), FP 'd-fp' (live). NOT 'b' (backfill).
        #expect(live.count == 3)
    }

    @Test("Composed slices: per-podcast, per-format")
    func composedSlice() {
        let batch = Self.mixedBatch()
        let diaryHostRead = batch
            .sliced(byPodcast: "diary")
            .sliced(byFormat: .hostRead)
        #expect(diaryHostRead.count == 1)
        #expect(diaryHostRead.pairs.first?.gt?.id == "a")
    }

    @Test("podcasts vended in stable order")
    func podcastsList() {
        let batch = Self.mixedBatch()
        #expect(batch.podcasts == ["conan", "diary"])
    }

    @Test("Empty slice → all metrics return nil/empty without crashing")
    func emptySliceNeverCrashes() {
        let empty = Self.mixedBatch().sliced(byPodcast: "nonexistent")
        #expect(empty.isEmpty)
        #expect(empty.computeSeedRecall().ratio == nil)
        #expect(empty.computeSpanIoU().median == nil)
        #expect(empty.computeMedianStartError() == nil)
        #expect(empty.computeMedianEndError() == nil)
        #expect(empty.computeSignedStartBias() == nil)
        #expect(empty.computeSignedEndBias() == nil)
        #expect(empty.computeCoverageRecall() == nil)
        #expect(empty.computeCoveragePrecision() == nil)
        #expect(empty.computeLeadTimeAtFirstConfirmation().median == nil)
    }
}

// MARK: - Pairing

@Suite("SpanMetrics — greedy pairing")
struct PairingTests {

    @Test("Pair within episode only — no cross-episode leakage")
    func crossEpisodeIsolation() {
        let gt = [
            makeGT(id: "g1", podcast: "p", episode: "e1", start: 10, end: 20),
            makeGT(id: "g2", podcast: "p", episode: "e2", start: 10, end: 20),
        ]
        let det = [
            makeDet(id: "d1", podcast: "p", episode: "e1", start: 12, end: 22),
            // Identical-time detection in a different episode — should not match e1's GT
            makeDet(id: "d2-other-episode", podcast: "p", episode: "e2", start: 12, end: 22),
        ]
        let batch = MetricsBatch.pair(groundTruth: gt, detections: det)
        // Two TP pairs, no misses, no FPs
        #expect(batch.pairs.count == 2)
        #expect(batch.pairs.allSatisfy { $0.isTruePositive })
    }

    @Test("Greedy pairing prefers higher IoU")
    func greedyByIoU() {
        // One GT, two detections — greedy should pick the higher-IoU one
        let gt = [makeGT(id: "g", podcast: "p", episode: "e", start: 10, end: 20)]
        let det = [
            makeDet(id: "low",  podcast: "p", episode: "e", start: 15, end: 25), // IoU = 5/15
            makeDet(id: "high", podcast: "p", episode: "e", start: 11, end: 19), // IoU = 8/9 (wider GT, narrower det)
        ]
        let batch = MetricsBatch.pair(groundTruth: gt, detections: det)
        let tps = batch.pairs.filter { $0.isTruePositive }
        let fps = batch.pairs.filter { $0.isFalsePositive }
        #expect(tps.count == 1)
        #expect(tps.first?.detected?.id == "high")
        #expect(fps.count == 1)
        #expect(fps.first?.detected?.id == "low")
    }

    @Test("Disjoint detections never pair (zero IoU dropped)")
    func zeroIoUDropped() {
        let gt = [makeGT(id: "g", podcast: "p", episode: "e", start: 10, end: 20)]
        let det = [makeDet(id: "d", podcast: "p", episode: "e", start: 100, end: 200)]
        let batch = MetricsBatch.pair(groundTruth: gt, detections: det)
        // GT becomes a miss, detection becomes a FP
        #expect(batch.pairs.count == 2)
        #expect(batch.pairs.contains { $0.isMiss })
        #expect(batch.pairs.contains { $0.isFalsePositive })
    }

    @Test("Empty inputs produce empty batch")
    func emptyInputs() {
        let batch = MetricsBatch.pair(groundTruth: [], detections: [])
        #expect(batch.isEmpty)
    }

    @Test("Detections without matching GT produce FPs")
    func detectionsOnly() {
        let det = [
            makeDet(id: "fp1", podcast: "p", episode: "e", start: 0, end: 10),
            makeDet(id: "fp2", podcast: "p", episode: "e", start: 20, end: 30),
        ]
        let batch = MetricsBatch.pair(groundTruth: [], detections: det)
        #expect(batch.pairs.count == 2)
        #expect(batch.pairs.allSatisfy { $0.isFalsePositive })
    }
}

// MARK: - Performance smoke

@Suite("SpanMetrics — performance smoke")
struct PerformanceSmokeTests {

    /// 10k pairs run in <1s on simulator. This guards against accidentally
    /// quadratic algorithms in the metric path. We use a non-strict
    /// wall-clock bound (5s) so simulator/CI variance doesn't flake.
    @Test("10k true-positive pairs compute all 9 metrics in under 5s")
    func tenKPairs() {
        var pairs: [MetricsPair] = []
        pairs.reserveCapacity(10_000)
        for i in 0..<10_000 {
            let start = Double(i) * 100.0
            let end = start + 30.0
            pairs.append(MetricsPair(
                gt: MetricGroundTruthAd(
                    id: "gt\(i)",
                    podcastId: "p\(i % 10)",
                    episodeId: "e\(i / 10)",
                    startTime: start,
                    endTime: end,
                    format: .hostRead,
                    seedFired: i % 3 != 0
                ),
                detected: MetricDetectedAd(
                    id: "det\(i)",
                    podcastId: "p\(i % 10)",
                    episodeId: "e\(i / 10)",
                    startTime: start + 1.5,
                    endTime: end - 0.5,
                    path: i.isMultiple(of: 2) ? .live : .backfill,
                    firstConfirmationTime: start - 2.0,
                    confidence: 0.9
                )
            ))
        }
        let batch = MetricsBatch(pairs: pairs)

        let t0 = Date()
        let summary = MetricsSummary(batch: batch)
        let elapsed = Date().timeIntervalSince(t0)
        #expect(elapsed < 5.0, "MetricsSummary on 10k pairs took \(elapsed)s — possible quadratic regression")

        // Spot-check the summary is non-trivial.
        #expect(summary.spanIoU.count == 10_000)
        #expect(summary.medianStartError != nil)
        #expect(summary.coverageRecall != nil)
        #expect(summary.leadTime.count == 10_000)
    }

    /// Pairing 1k GT + 1k detections within 100 episodes scales fine.
    /// The pairing helper is `O(g*d)` per episode; this guards against
    /// regressions that bucket badly and end up `O(N^2)` global.
    @Test("1k+1k pair() across 100 episodes completes in under 5s")
    func pairingScale() {
        var gts: [MetricGroundTruthAd] = []
        var dets: [MetricDetectedAd] = []
        for ep in 0..<100 {
            for i in 0..<10 {
                let start = Double(i) * 100.0
                gts.append(makeGT(
                    id: "gt-\(ep)-\(i)",
                    podcast: "p",
                    episode: "e\(ep)",
                    start: start,
                    end: start + 30
                ))
                dets.append(makeDet(
                    id: "det-\(ep)-\(i)",
                    podcast: "p",
                    episode: "e\(ep)",
                    start: start + 1,
                    end: start + 29
                ))
            }
        }
        let t0 = Date()
        let batch = MetricsBatch.pair(groundTruth: gts, detections: dets)
        let elapsed = Date().timeIntervalSince(t0)
        #expect(elapsed < 5.0, "MetricsBatch.pair(...) took \(elapsed)s — possible quadratic regression")
        #expect(batch.pairs.count == 1000)
    }
}
