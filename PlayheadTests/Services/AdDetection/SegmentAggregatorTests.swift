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
}
