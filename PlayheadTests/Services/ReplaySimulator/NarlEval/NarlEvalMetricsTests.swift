// NarlEvalMetricsTests.swift
// playhead-narl.1: Unit tests for the window-level + second-level metrics.

import Foundation
import Testing
@testable import Playhead

@Suite("NarlWindowMetrics – IoU")
struct NarlIoUTests {

    @Test("Identical ranges have IoU 1")
    func iouIdentical() {
        let r = NarlTimeRange(start: 100, end: 200)
        #expect(NarlWindowMetrics.iou(r, r) == 1.0)
    }

    @Test("Disjoint ranges have IoU 0")
    func iouDisjoint() {
        let a = NarlTimeRange(start: 0, end: 100)
        let b = NarlTimeRange(start: 200, end: 300)
        #expect(NarlWindowMetrics.iou(a, b) == 0.0)
    }

    @Test("Half-overlap IoU matches hand calculation")
    func iouHalfOverlap() {
        let a = NarlTimeRange(start: 0, end: 100)
        let b = NarlTimeRange(start: 50, end: 150)
        // inter = 50, union = 150, IoU = 1/3
        let value = NarlWindowMetrics.iou(a, b)
        #expect(abs(value - (1.0 / 3.0)) < 0.0001)
    }
}

@Suite("NarlWindowMetrics – threshold-based matching")
struct NarlWindowMatchingTests {

    @Test("Perfect match at any threshold → precision=recall=1")
    func perfectMatch() {
        let gt = [
            NarlTimeRange(start: 120, end: 180),
            NarlTimeRange(start: 600, end: 660),
        ]
        let pred = gt
        for τ in [0.3, 0.5, 0.7, 0.99] {
            let m = NarlWindowMetrics.compute(predicted: pred, groundTruth: gt, threshold: τ)
            #expect(m.truePositives == 2)
            #expect(m.precision == 1.0)
            #expect(m.recall == 1.0)
            #expect(m.f1 == 1.0)
            #expect(m.meanMatchedIoU == 1.0)
        }
    }

    @Test("IoU just under threshold counts as FP + FN")
    func belowThresholdIsFPFN() {
        // gt: 100-200 (duration 100). pred: 100-220 (duration 120).
        // inter = 100, union = 120, IoU = 100/120 ≈ 0.833.
        // Above 0.7 → TP. Below 0.9 → FP + FN.
        let gt = [NarlTimeRange(start: 100, end: 200)]
        let pred = [NarlTimeRange(start: 100, end: 220)]

        let tpMetrics = NarlWindowMetrics.compute(predicted: pred, groundTruth: gt, threshold: 0.7)
        #expect(tpMetrics.truePositives == 1)

        let fpMetrics = NarlWindowMetrics.compute(predicted: pred, groundTruth: gt, threshold: 0.9)
        #expect(fpMetrics.truePositives == 0)
        #expect(fpMetrics.falsePositives == 1)
        #expect(fpMetrics.falseNegatives == 1)
        #expect(fpMetrics.precision == 0.0)
        #expect(fpMetrics.recall == 0.0)
    }

    @Test("Extra spurious prediction lowers precision")
    func extraPredictionLowersPrecision() {
        let gt = [NarlTimeRange(start: 100, end: 200)]
        let pred = [
            NarlTimeRange(start: 100, end: 200),
            NarlTimeRange(start: 500, end: 600),
        ]
        let m = NarlWindowMetrics.compute(predicted: pred, groundTruth: gt, threshold: 0.5)
        #expect(m.truePositives == 1)
        #expect(m.falsePositives == 1)
        #expect(m.precision == 0.5)
        #expect(m.recall == 1.0)
    }

    @Test("Greedy match assigns best IoU first")
    func greedyMatchPrefersBest() {
        // Two predictions, two GTs. Both predictions overlap GT-1 more than GT-0.
        // Greedy should claim the best pair first.
        let gt = [
            NarlTimeRange(start: 0, end: 100),    // gt-0
            NarlTimeRange(start: 100, end: 200),  // gt-1
        ]
        let pred = [
            NarlTimeRange(start: 50, end: 150),   // 50% overlap with gt-0, 50% with gt-1
            NarlTimeRange(start: 105, end: 195),  // 90% overlap with gt-1
        ]
        let m = NarlWindowMetrics.compute(predicted: pred, groundTruth: gt, threshold: 0.3)
        // Best pair is pred[1] ↔ gt[1]. pred[0] is then matched to gt[0] if iou≥τ.
        #expect(m.truePositives == 2)
    }

    @Test("Empty predictions + empty GT → perfect classification (precision=recall=F1=1)")
    func emptyEverything() {
        // PASCAL-VOC convention: when there are no ads to detect and the
        // detector predicts none, that's a trivial perfect result.
        // Returning 0 here would systematically pull rollups down for
        // every ad-free episode — exactly the population we want to
        // expand coverage on.
        let m = NarlWindowMetrics.compute(predicted: [], groundTruth: [], threshold: 0.5)
        #expect(m.truePositives == 0)
        #expect(m.falsePositives == 0)
        #expect(m.falseNegatives == 0)
        #expect(m.precision == 1)
        #expect(m.recall == 1)
        #expect(m.f1 == 1)
    }
}

@Suite("NarlSecondLevel")
struct NarlSecondLevelTests {

    @Test("Exact second alignment yields perfect F1")
    func exactSeconds() {
        let ranges = [NarlTimeRange(start: 10, end: 20)]
        let m = NarlSecondLevel.compute(predicted: ranges, groundTruth: ranges)
        #expect(m.precision == 1)
        #expect(m.recall == 1)
        #expect(m.f1 == 1)
        #expect(m.truePositiveSeconds == 10)
    }

    @Test("Half-overlap yields precision ≠ 1 and recall ≠ 1")
    func halfOverlap() {
        let gt = [NarlTimeRange(start: 10, end: 20)]
        let pred = [NarlTimeRange(start: 15, end: 25)]
        let m = NarlSecondLevel.compute(predicted: pred, groundTruth: gt)
        // Both cover 10 seconds, overlap = 5
        #expect(m.truePositiveSeconds == 5)
        #expect(m.falsePositiveSeconds == 5)
        #expect(m.falseNegativeSeconds == 5)
        #expect(m.precision == 0.5)
        #expect(m.recall == 0.5)
    }

    @Test("Bin alignment rounds down/up at edges")
    func binAlignmentEdges() {
        // Range 0.25..1.75 → seconds {0, 1} because start rounds down, end rounds up.
        let covered = NarlSecondLevel.secondsCovered(by: [NarlTimeRange(start: 0.25, end: 1.75)])
        #expect(covered == Set([0, 1]))
    }

    @Test("Empty predictions + empty GT → perfect classification (precision=recall=F1=1)")
    func emptyEverything() {
        // Symmetry with `NarlWindowMetrics.compute`: ad-free episode +
        // detector predicted no ads = trivial perfect result. Returning
        // 0 here would surface Window-F1=1 alongside Sec-F1=0 for the
        // same ad-free episode — strictly more confusing than uniform 1.
        let m = NarlSecondLevel.compute(predicted: [], groundTruth: [])
        #expect(m.truePositiveSeconds == 0)
        #expect(m.falsePositiveSeconds == 0)
        #expect(m.falseNegativeSeconds == 0)
        #expect(m.precision == 1)
        #expect(m.recall == 1)
        #expect(m.f1 == 1)
    }
}
