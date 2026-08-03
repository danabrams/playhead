// SegmentAggregatorTests.swift
// playhead-gtt9.10: Segment-level candidate aggregator with hysteresis.
//
// The contract under test: turn a stream of per-window scores into coherent
// ad segments via a hysteresis state machine, so that several weak-but-
// consistent sub-threshold windows coalesce into one promotable span —
// without letting a single high-scoring outlier promote by itself.
//
// Motivation: on the 2026-04-23 dogfood capture, several user-marked FN ad
// spans contained many 1-second scored windows with confidence mode in
// [0.30, 0.40), each individually inconclusive but collectively strong
// (see docs/narl/2026-04-23-real-data-findings.md §4, expert response §4).
// DF5C1832 in particular had [1612,1613] @ 0.45 and [1676,1677] @ 0.46
// inside a single user-marked ad span — those windows already exceeded the
// candidate threshold but the pipeline made no attempt to tie them together.
//
// Conversely C22D6EC6 had a single 0.597 window inside a user-marked FP
// region; a good aggregator must NOT promote that one window on its own.
//
// This suite covers:
//   1. start-by-N-nearby: N windows ≥ candidate within proximity opens a segment
//   2. start-by-high-confidence: one window ≥ highConfidence opens a segment
//   3. continuation: a gap ≤ maxInternalGap is bridged
//   4. end: M seconds below continuation closes the segment
//   5. promotion: segmentScore ≥ promotion AND duration ≥ minAd → promoted
//   6. DF5C1832-equivalent coherent-segment coverage of two low-ish windows
//   7. C22D6EC6-equivalent — a single 0.597 window alone does NOT promote
//   8. monotonicity property — adding a ≥-threshold window never lowers score

import Foundation
import Testing
@testable import Playhead

@Suite("SegmentAggregator — hysteresis & duration-weighted segmentScore")
struct SegmentAggregatorTests {

    // MARK: - Helpers

    /// Starting thresholds from the bead spec. Calibration belongs to gtt9.3.
    private static let defaultConfig = SegmentAggregatorConfig(
        candidateThreshold: 0.35,
        continuationThreshold: 0.28,
        promotionThreshold: 0.40,
        highConfidenceThreshold: 0.60,
        nNearbyWindowsForStart: 2,
        nearbyWindowSecondsForStart: 90.0,
        belowContinuationSecondsToEnd: 3.0,
        maxInternalGapSeconds: 5.0,
        minAdDurationSeconds: 30.0
    )

    /// Build a list of contiguous 1-second scored windows at the given
    /// starting offset and with the given per-second scores.
    private func oneSecondWindows(
        startingAt start: Double,
        scores: [Double]
    ) -> [SegmentAggregator.WindowScore] {
        var t = start
        var out: [SegmentAggregator.WindowScore] = []
        for s in scores {
            out.append(.init(startTime: t, endTime: t + 1.0, score: s))
            t += 1.0
        }
        return out
    }

    // MARK: - 1. start-by-N-nearby

    @Test("two candidate windows 60 s apart still qualify as 'nearby' for start (ad-scale corroboration)")
    func startByNNearbySpanningAdScale() {
        // Two candidate-strength windows 60 s apart, with the intervening
        // region holding continuation-grade (>= 0.28) evidence. This is
        // the DF5C1832-shape reduced to its minimum: the N-nearby start
        // criterion uses `nearbyWindowSecondsForStart` (default 90 s =
        // typicalAdDuration.upperBound), not `maxInternalGapSeconds`.
        var windows: [SegmentAggregator.WindowScore] = []
        windows.append(.init(startTime: 0.0, endTime: 1.0, score: 0.40))
        // 58 × 1 s @ 0.30 between the spikes (all ≥ 0.28 continuation)
        windows.append(contentsOf: oneSecondWindows(
            startingAt: 1.0,
            scores: Array(repeating: 0.30, count: 58)
        ))
        windows.append(.init(startTime: 59.0, endTime: 60.0, score: 0.40))
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1,
                "two candidate windows 60 s apart within a continuation-grade run must start ONE segment")
    }

    @Test("two consecutive candidate-threshold windows open a segment")
    func startByNNearbyConsecutive() {
        // Two adjacent 1 s windows each at 0.36 — individually below 0.40
        // promotion, but both ≥ 0.35 candidate. With N=2 this opens a segment.
        let windows = oneSecondWindows(startingAt: 100.0, scores: [0.36, 0.36])
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1, "expected exactly one segment opened by N=2 nearby candidate windows")
        #expect(segments.first?.startTime == 100.0)
    }

    @Test("a single isolated candidate-threshold window does NOT open a segment")
    func singleCandidateBelowThresholdDoesNotStart() {
        // One lone 0.36 with neighbors well below continuationThreshold.
        var windows: [SegmentAggregator.WindowScore] = []
        windows.append(.init(startTime: 10.0, endTime: 11.0, score: 0.10))
        windows.append(.init(startTime: 11.0, endTime: 12.0, score: 0.36))
        windows.append(.init(startTime: 12.0, endTime: 13.0, score: 0.10))
        windows.append(.init(startTime: 13.0, endTime: 14.0, score: 0.10))
        windows.append(.init(startTime: 14.0, endTime: 15.0, score: 0.10))
        windows.append(.init(startTime: 15.0, endTime: 16.0, score: 0.10))
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.isEmpty, "a single candidate window without a neighbor must not open a segment")
    }

    // MARK: - 2. start-by-high-confidence

    @Test("one window >= highConfidenceThreshold opens a segment")
    func startByHighConfidenceSingleWindow() {
        // One 0.80 window surrounded by low scores — still opens a segment
        // because high-confidence branch skips the N-nearby requirement.
        var windows: [SegmentAggregator.WindowScore] = []
        windows.append(.init(startTime: 50.0, endTime: 51.0, score: 0.10))
        windows.append(.init(startTime: 51.0, endTime: 52.0, score: 0.80))
        windows.append(.init(startTime: 52.0, endTime: 53.0, score: 0.10))
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1)
        #expect(segments.first?.startTime == 51.0)
    }

    // MARK: - 3. continuation

    @Test("a gap <= maxInternalGapSeconds is bridged into one segment")
    func continuationBridgesShortGap() {
        // Segment A at [100,102) @ 0.40. Gap of 3 s of silence (no windows
        // at all, i.e. gap <= maxInternalGapSeconds=5). Segment "B" at
        // [105, 107) @ 0.40. With bridging, these merge into a single
        // [100, 107) segment spanning both.
        var windows = oneSecondWindows(startingAt: 100.0, scores: [0.40, 0.40])
        // Skip [102, 105) — no windows at all (gap of 3 s by clock).
        windows.append(contentsOf: oneSecondWindows(startingAt: 105.0, scores: [0.40, 0.40]))
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1, "3 s clock gap ≤ 5 s maxInternalGap must bridge")
        #expect(segments.first?.startTime == 100.0)
        #expect(segments.first?.endTime == 107.0)
    }

    @Test("a gap > maxInternalGapSeconds splits into two segments")
    func continuationDoesNotBridgeLongGap() {
        var windows = oneSecondWindows(startingAt: 100.0, scores: [0.40, 0.40])
        // 10 s clock gap between segments — exceeds both belowContinuationSecondsToEnd
        // and maxInternalGapSeconds, so the first segment closes before the second opens.
        windows.append(contentsOf: oneSecondWindows(startingAt: 112.0, scores: [0.40, 0.40]))
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 2)
    }

    // MARK: - 4. end by M seconds below continuation

    @Test("M seconds below continuation threshold closes the segment")
    func endsAfterMSecondsBelowContinuation() {
        // Two 0.45 windows open a segment via N=2 nearby. Then 4 seconds of
        // 0.10 (below 0.28 continuation) — M=3, so the segment should close
        // at the 3-second cumulative-below mark. End time must be the end
        // of the last qualifying (>= continuation) window, not a point in
        // the trailing below-continuation tail.
        var windows = oneSecondWindows(startingAt: 200.0, scores: [0.45, 0.45])
        windows.append(contentsOf: oneSecondWindows(startingAt: 202.0, scores: [0.10, 0.10, 0.10, 0.10]))
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1)
        // End should be the last ≥-continuation window's endTime (202.0).
        #expect(segments.first?.endTime == 202.0, "segment end must snap back to the last qualifying window")
    }

    // MARK: - 5. promotion

    @Test("segmentScore >= promotionThreshold AND duration >= minAd promotes")
    func promotionWhenBothThresholdsMet() {
        // Forty windows at 0.45 — segmentScore=0.45 (above 0.40 promotion),
        // duration=40 s (above 30 s minAdDuration). Must be promoted=true.
        let scores = Array(repeating: 0.45, count: 40)
        let windows = oneSecondWindows(startingAt: 600.0, scores: scores)
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1)
        guard let s = segments.first else { return }
        #expect(s.segmentScore >= 0.40)
        #expect(s.endTime - s.startTime >= 30.0)
        #expect(s.promoted, "segment must be promoted when score≥promotion and duration≥minAd")
    }

    @Test("segment shorter than minAdDuration is NOT promoted even when score >= promotion")
    func promotionBlockedByShortDuration() {
        // Five windows at 0.90 — segmentScore=0.90, duration=5 s < 30 s
        // minAdDuration. Must not promote.
        let scores = Array(repeating: 0.90, count: 5)
        let windows = oneSecondWindows(startingAt: 0.0, scores: scores)
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1)
        guard let s = segments.first else { return }
        #expect(!s.promoted, "short high-score segment must not promote")
    }

    // MARK: - 6. DF5C1832-equivalent (regression proof)

    @Test("DF5C1832-equivalent: two low-ish windows at [1612,1613]@0.45 and [1676,1677]@0.46 inside a GT span form one coherent segment")
    func df5c1832EquivalentCoherentSegmentCoverage() {
        // Synthesize the DF5C1832 FN shape: between 1609 and 1680 (a
        // plausible ~70 s GT ad span) there are many low-but-nonzero 1 s
        // windows, two of which spike above 0.40 (the candidate threshold).
        // With the old per-window gate at 0.40 these two windows survived
        // in isolation and gtt9.6 saw a partial-coverage FN. The aggregator
        // must knit them into a single contiguous segment covering the GT
        // span, and — with a duration-weighted mean over mostly-0.30 windows
        // — produce a segmentScore that justifies segment-level evidence
        // even if promotion itself waits on calibration (gtt9.3).
        //
        // The key acceptance is that the two 0.45/0.46 windows participate
        // in a single segment (not two isolated ones).
        let preRoll = oneSecondWindows(
            startingAt: 1609.0,
            scores: Array(repeating: 0.30, count: 3)   // 1609..1612 @ 0.30
        )
        let spike1 = oneSecondWindows(startingAt: 1612.0, scores: [0.45])
        let mid = oneSecondWindows(
            startingAt: 1613.0,
            scores: Array(repeating: 0.30, count: 63)  // 1613..1676 @ 0.30
        )
        let spike2 = oneSecondWindows(startingAt: 1676.0, scores: [0.46])
        let postRoll = oneSecondWindows(
            startingAt: 1677.0,
            scores: Array(repeating: 0.30, count: 3)   // 1677..1680 @ 0.30
        )
        let windows = preRoll + spike1 + mid + spike2 + postRoll

        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1,
                "DF5C1832 FN shape must coalesce into a single segment, not two isolated spikes")
        guard let s = segments.first else { return }
        // Segment must cover BOTH spikes — start before 1612 and end after 1677.
        #expect(s.startTime <= 1612.0)
        #expect(s.endTime >= 1677.0)
        #expect(s.windowCount >= 60, "segment should aggregate the bulk of the GT span")
    }

    // MARK: - 7. C22D6EC6-equivalent (no-regression proof)

    @Test("C22D6EC6-equivalent: single 0.597 window alone does NOT promote")
    func c22D6EC6EquivalentSingleHighWindowNoPromotion() {
        // Synthesize the C22D6EC6 FP shape: a single window at 0.597 sitting
        // inside a user-marked FP region. 0.597 is below the 0.60
        // highConfidenceThreshold, so the N-nearby branch gates the start.
        // Without a second nearby candidate window, the aggregator MUST NOT
        // open (and hence MUST NOT promote) a segment.
        var windows: [SegmentAggregator.WindowScore] = []
        // 10 s of sub-continuation noise on either side
        windows.append(contentsOf: oneSecondWindows(
            startingAt: 300.0,
            scores: Array(repeating: 0.15, count: 10)
        ))
        windows.append(.init(startTime: 310.0, endTime: 311.0, score: 0.597))
        windows.append(contentsOf: oneSecondWindows(
            startingAt: 311.0,
            scores: Array(repeating: 0.15, count: 10)
        ))
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.isEmpty || segments.allSatisfy { !$0.promoted },
                "a lone 0.597 window must not escalate to a promoted segment")
    }

    // MARK: - 8. Property: monotonicity of segmentScore

    @Test("appending a >=continuationThreshold window never lowers segmentScore (duration-weighted mean)")
    func segmentScoreMonotonicityWhenAddingAboveMeanWindow() {
        // Open a segment with two 0.30 windows (below continuation but we
        // force-start with a high-confidence window first to actually open).
        var windows: [SegmentAggregator.WindowScore] = []
        windows.append(.init(startTime: 0.0, endTime: 1.0, score: 0.80))  // opens
        windows.append(.init(startTime: 1.0, endTime: 2.0, score: 0.30))  // continues
        windows.append(.init(startTime: 2.0, endTime: 3.0, score: 0.30))  // continues

        let baseline = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(baseline.count == 1)
        guard let baseScore = baseline.first?.segmentScore else { return }

        // Append a window whose score is >= the running mean.
        windows.append(.init(startTime: 3.0, endTime: 4.0, score: baseScore))
        let perturbed = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(perturbed.count == 1)
        guard let newScore = perturbed.first?.segmentScore else { return }
        #expect(newScore >= baseScore - 1e-9,
                "adding a window with score >= running mean must not decrease the duration-weighted mean")
    }

    // MARK: - 9. Heterogeneous window widths (sanity)

    @Test("heterogeneous window widths (1s & 2s) are accepted and weighted by duration")
    func heterogeneousWindowWidthsAccepted() {
        // Mix a 2 s Tier 2 lexical-style window with adjacent 1 s Tier 1
        // windows. The duration-weighted mean should give the 2 s window
        // twice the weight of a 1 s one.
        var windows: [SegmentAggregator.WindowScore] = []
        windows.append(.init(startTime: 0.0, endTime: 2.0, score: 0.80))  // 2 s @ 0.80
        windows.append(.init(startTime: 2.0, endTime: 3.0, score: 0.40))  // 1 s @ 0.40
        windows.append(.init(startTime: 3.0, endTime: 4.0, score: 0.40))  // 1 s @ 0.40
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1)
        guard let s = segments.first else { return }
        // Duration-weighted mean = (2·0.80 + 1·0.40 + 1·0.40) / 4 = 2.4 / 4 = 0.60
        #expect(abs(s.segmentScore - 0.60) < 1e-9)
    }

    // MARK: - 10. playhead-pggn: the score describes the span the segment reports
    //
    // A trailing below-continuation window used to be added to
    // `weightedScoreSum` AND to the `totalDuration` denominator while the
    // emitted `endTime` stayed snapped to `lastQualifyingEndTime`. The row that
    // reached the database was therefore N seconds wide carrying a number that
    // described N + tail seconds.
    //
    // The invariant these tests pin: **no region outside `[startTime, endTime]`
    // contributes to `segmentScore`.**

    /// Build the bead's shape: two adjacent 30 s seed slots at `seed`, then one
    /// trailing below-continuation slot of `tailWidth` seconds at `tail`.
    private func seedThenTail(
        seed: Double,
        tail: Double,
        tailWidth: Double
    ) -> [SegmentAggregator.WindowScore] {
        [
            .init(startTime: 1500.0, endTime: 1530.0, score: seed),
            .init(startTime: 1530.0, endTime: 1560.0, score: seed),
            .init(startTime: 1560.0, endTime: 1560.0 + tailWidth, score: tail)
        ]
    }

    @Test("playhead-pggn worked example: a 0.62 slot with a 0.20 lead-out reports 0.62 over 30 s, not 0.41")
    func pggnWorkedExampleReportsTheSeedScoreNotTheDilutedOne() {
        // The bead's example verbatim, taken from observed device data: a
        // single high-confidence 0.62 slot at [1530,1560) followed by a 0.20
        // slot at [1560,1590).
        //
        // BEFORE: endTime 1560, segmentScore (0.62·30 + 0.20·30)/60 = 0.41.
        // A 30-second row carrying a number that described 60 seconds — and
        // 0.41 sits in the markOnly band while 0.62 does not.
        let windows: [SegmentAggregator.WindowScore] = [
            .init(startTime: 1530.0, endTime: 1560.0, score: 0.62),
            .init(startTime: 1560.0, endTime: 1590.0, score: 0.20)
        ]
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1)
        guard let s = segments.first else { return }
        #expect(abs(s.startTime - 1530.0) < 1e-9)
        #expect(abs(s.endTime - 1560.0) < 1e-9,
                "the reported extent must still snap back off the sub-continuation tail")
        #expect(abs(s.segmentScore - 0.62) < 1e-9,
                "the 30 s the segment reports scored 0.62; the 0.20 tail is outside it")
        #expect(s.windowCount == 1,
                "the tail is counted no more than it is scored")
        #expect(s.promoted)
        // The whole point of the defect: 0.41 is below the 0.55 auto-skip
        // threshold and 0.62 is above it. A true detection was being pushed
        // under the bar by its own lead-out.
        #expect(s.segmentScore >= AutoSkipPrecisionGateConfig.default.autoSkipThreshold)
    }

    @Test("playhead-pggn: the reported score does not depend on the VALUE of a sub-continuation tail")
    func subContinuationTailValueCannotMoveTheReportedScore() {
        // The invariant stated as an experiment: vary the tail across the whole
        // below-continuation range and nothing about the emitted segment may
        // move. This is what fails the instant the tail re-enters `include()`.
        var observed: Set<String> = []
        for tail in stride(from: 0.0, through: 0.27, by: 0.01) {
            let segments = SegmentAggregator.aggregate(
                windows: seedThenTail(seed: 0.50, tail: tail, tailWidth: 30.0),
                config: Self.defaultConfig
            )
            #expect(segments.count == 1)
            guard let s = segments.first else { return }
            observed.insert("\(s.startTime)|\(s.endTime)|\(s.segmentScore)|\(s.windowCount)|\(s.promoted)")
        }
        #expect(observed.count == 1,
                "a region outside the reported extent must not change the reported segment; saw \(observed.count) distinct outcomes: \(observed.sorted())")
        // Positive witness: name the one outcome, so this cannot pass by
        // emitting nothing at all.
        #expect(observed.first == "1500.0|1560.0|0.5|2|true")
    }

    @Test("playhead-pggn: the reported score does not depend on the WIDTH of a sub-continuation tail")
    func subContinuationTailWidthCannotMoveTheReportedScore() {
        // Widths below `belowContinuationSecondsToEnd` (3 s) leave the segment
        // open to the end of the stream and exercise the flush path; widths at
        // or above it close the segment through `ingestIntoOpenSegment`. Both
        // exits must settle the pending tail the same way.
        for width in [0.5, 2.0, 3.0, 10.0, 30.0, 120.0] {
            let segments = SegmentAggregator.aggregate(
                windows: seedThenTail(seed: 0.50, tail: 0.10, tailWidth: width),
                config: Self.defaultConfig
            )
            #expect(segments.count == 1, "width \(width)")
            guard let s = segments.first else { return }
            #expect(abs(s.endTime - 1560.0) < 1e-9, "width \(width)")
            #expect(abs(s.segmentScore - 0.50) < 1e-9,
                    "width \(width): a \(width) s tail outside the extent moved the score to \(s.segmentScore)")
            #expect(s.windowCount == 2, "width \(width)")
        }
    }

    @Test("playhead-pggn: a sub-continuation tail can never LOWER a promoted segment's score")
    func subContinuationTailNeverLowersThePromotedScore() {
        // The direction claim from the file header, checked against the
        // counterfactual computed inline: `dilutedMean` is exactly what the
        // pre-pggn code produced for this shape (every window in the score,
        // extent snapped back).
        //
        // μ_old is a weighted average of μ_new and the tail's mean, so μ_new
        // can only fall below μ_old when it is already below the tail — and
        // every tail window is under `continuationThreshold` (0.28), which is
        // under `promotionThreshold` (0.40). So no PROMOTED segment can lose.
        var strictImprovements = 0
        var comparisons = 0
        for seed in stride(from: 0.35, through: 0.99, by: 0.01) {
            for tail in stride(from: 0.0, through: 0.27, by: 0.03) {
                for width in [2.0, 5.0, 30.0, 90.0] {
                    let segments = SegmentAggregator.aggregate(
                        windows: seedThenTail(seed: seed, tail: tail, tailWidth: width),
                        config: Self.defaultConfig
                    )
                    guard let s = segments.first, s.promoted else { continue }
                    let dilutedMean = (seed * 60.0 + tail * width) / (60.0 + width)
                    comparisons += 1
                    #expect(abs(s.segmentScore - seed) < 1e-9,
                            "seed \(seed) tail \(tail) width \(width): the 60 s this segment reports scored \(seed); got \(s.segmentScore)")
                    #expect(s.segmentScore >= dilutedMean - 1e-12,
                            "seed \(seed) tail \(tail) width \(width): promoted score fell from \(dilutedMean) to \(s.segmentScore)")
                    if s.segmentScore > dilutedMean + 1e-12 { strictImprovements += 1 }
                }
            }
        }
        // Positive witnesses. `comparisons > 0` proves the loop ran at all.
        // The second one is EVERY case, not merely one: every tail here is
        // strictly below its seed and every width is positive, so every single
        // shape must improve strictly. A global `> 0` was too weak — it stayed
        // satisfied by the short-tail cases (which exit through the flush) while
        // a mutation folded the tail at the COUNTDOWN exit, and PG05 walked
        // through it green.
        #expect(comparisons > 0)
        #expect(strictImprovements == comparisons,
                "every one of these shapes must score strictly above the diluted mean; only \(strictImprovements) of \(comparisons) did")
    }

    @Test("playhead-pggn: closing a segment on an unbridgeable gap drops the pending tail rather than scoring it")
    func gapClosingASegmentDropsThePendingTailFromTheScore() {
        // The third way out of an open segment, and the one the other tests do
        // not reach: a qualifying window arrives after a gap too large to
        // bridge, so the segment closes at `lastQualifyingEndTime` while a
        // below-continuation window is still pending. That window is outside
        // the span the segment reports and must not be scored.
        let windows: [SegmentAggregator.WindowScore] = [
            .init(startTime: 1500.0, endTime: 1530.0, score: 0.50),
            .init(startTime: 1530.0, endTime: 1560.0, score: 0.50),
            .init(startTime: 1560.0, endTime: 1561.0, score: 0.10),  // 1 s tail, countdown 1 < 3
            .init(startTime: 1571.0, endTime: 1601.0, score: 0.50)   // gap 11 s > 5 s → close
        ]
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1)
        guard let s = segments.first else { return }
        #expect(abs(s.endTime - 1560.0) < 1e-9)
        // Scoring the tail here would read (0.50·30 + 0.50·30 + 0.10·1)/61 = 0.4934.
        #expect(abs(s.segmentScore - 0.50) < 1e-9, "got \(s.segmentScore)")
        #expect(s.windowCount == 2)
    }

    @Test("playhead-pggn: a below-continuation window the extent LATER covers is scored, not discarded")
    func subContinuationWindowInsideTheFinalExtentIsScored() {
        // The mirror of the defect. Dropping a below-continuation window
        // outright — rather than deferring it until the extent's fate is known
        // — would leave a hole: a score describing LESS than the span it
        // reports. Here a later qualifying window carries the extent past the
        // dip, so the dip is inside `[startTime, endTime]` and must count.
        let windows: [SegmentAggregator.WindowScore] = [
            .init(startTime: 0.0, endTime: 2.0, score: 0.35),   // seed 1
            .init(startTime: 2.0, endTime: 4.0, score: 0.35),   // seed 2 → opens
            .init(startTime: 4.0, endTime: 6.0, score: 0.10),   // dip, below continuation
            .init(startTime: 6.0, endTime: 8.0, score: 0.50)    // carries the extent past it
        ]
        let segments = SegmentAggregator.aggregate(
            windows: windows,
            config: Self.defaultConfig
        )
        #expect(segments.count == 1)
        guard let s = segments.first else { return }
        #expect(abs(s.endTime - 8.0) < 1e-9)
        // (0.35·2 + 0.35·2 + 0.10·2 + 0.50·2) / 8 = 2.6 / 8 = 0.325.
        // Discarding the dip instead of folding it in would read 2.4/6 = 0.40.
        #expect(abs(s.segmentScore - 0.325) < 1e-9,
                "the dip is inside the reported extent, so it belongs in the mean; got \(s.segmentScore)")
        #expect(s.windowCount == 4)
    }

    @Test("playhead-pggn: a below-continuation window already covered by the extent is scored immediately")
    func subContinuationWindowAlreadyInsideTheExtentIsScored() {
        // Tier 1 emits 30 s slots and Tier 2 emits 2 s candidates into the same
        // sorted stream (SegmentAggregator contract, file header), so a narrow
        // low-scoring window can arrive while the extent already reaches past
        // its end. That window is inside the reported span and must count.
        //
        // THE SEGMENT MUST CLOSE RIGHT AFTER THE DIP for this to discriminate,
        // and that is not a stylistic preference — it is the fix for a measured
        // hole. The first version of this rail ended with a QUALIFYING window,
        // which folds a wrongly-deferred dip straight back in; correct and
        // inverted behaviour then agree exactly, and the PG02 mutation (defer
        // what is inside, score what is outside) walked through it green.
        let closing: [SegmentAggregator.WindowScore] = [
            .init(startTime: 0.0, endTime: 30.0, score: 0.65),   // opens; extent → 30
            .init(startTime: 10.0, endTime: 12.0, score: 0.10),  // 2 s dip INSIDE [0,30)
            .init(startTime: 30.0, endTime: 34.0, score: 0.02)   // 4 s beyond it; countdown 6 ≥ 3 → close
        ]
        let closed = SegmentAggregator.aggregate(
            windows: closing,
            config: Self.defaultConfig
        )
        #expect(closed.count == 1)
        guard let c = closed.first else { return }
        #expect(abs(c.endTime - 30.0) < 1e-9)
        // (0.65·30 + 0.10·2) / 32 = 19.7 / 32 = 0.615625. The dip counts
        // because it is inside the reported extent; the 0.02 window does not
        // because it is not. Deferring the dip instead would drop it at close
        // and read 19.58 / 34 = 0.5759.
        #expect(abs(c.segmentScore - (19.7 / 32.0)) < 1e-9,
                "a dip already inside the extent must not be deferred out of the score; got \(c.segmentScore)")
        #expect(c.windowCount == 2)

        // The same containment rule when a qualifying window DOES follow. This
        // arm cannot tell deferral from inclusion on its own — it is kept
        // because it pins the arithmetic of the fold path end to end.
        let continuing: [SegmentAggregator.WindowScore] = [
            .init(startTime: 0.0, endTime: 30.0, score: 0.65),
            .init(startTime: 10.0, endTime: 12.0, score: 0.10),
            .init(startTime: 30.0, endTime: 60.0, score: 0.50)
        ]
        let carried = SegmentAggregator.aggregate(
            windows: continuing,
            config: Self.defaultConfig
        )
        #expect(carried.count == 1)
        guard let s = carried.first else { return }
        #expect(abs(s.endTime - 60.0) < 1e-9)
        // (0.65·30 + 0.10·2 + 0.50·30) / 62 = 34.7 / 62.
        #expect(abs(s.segmentScore - (34.7 / 62.0)) < 1e-9, "got \(s.segmentScore)")
        #expect(s.windowCount == 3)
    }
}
