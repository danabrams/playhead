// NarlEvalMetrics.swift
// playhead-narl.1: Window-level IoU + second-level metrics (design §A.5).
//
// Two metric families per (show, config):
//   - Window-level: for τ ∈ {0.3, 0.5, 0.7}, a predicted window is TP if it
//     matches some ground-truth window with IoU ≥ τ. Precision = TP / predicted,
//     recall = TP / gt, F1 = harmonic mean. Also mean IoU over matched pairs.
//   - Second-level: project all windows onto the second-resolution timeline
//     (using 1-second bins), count correctly-classified ad-seconds.

import Foundation
@testable import Playhead

// MARK: - Window-level metrics

/// Window-level metrics at one IoU threshold.
struct NarlWindowMetricsAtThreshold: Sendable, Codable, Equatable {
    let threshold: Double
    let truePositives: Int
    let falsePositives: Int
    let falseNegatives: Int
    let precision: Double
    let recall: Double
    let f1: Double
    /// Mean IoU over matched pairs (TP only). Zero when no pairs matched.
    let meanMatchedIoU: Double
}

enum NarlWindowMetrics {

    /// IoU of two time ranges. Zero when disjoint or either is zero-length.
    static func iou(_ a: NarlTimeRange, _ b: NarlTimeRange) -> Double {
        guard let inter = a.intersection(b) else { return 0 }
        let unionDuration = a.duration + b.duration - inter.duration
        guard unionDuration > 0 else { return 0 }
        return inter.duration / unionDuration
    }

    /// Compute metrics for one threshold via greedy best-IoU matching:
    ///   * Sort predicted windows by max-IoU against GT (descending).
    ///   * For each, claim the best un-matched GT above the threshold.
    ///
    /// Not the optimal bipartite match, but monotonic in threshold and
    /// identical to the standard PASCAL VOC matching rule.
    static func compute(
        predicted: [NarlTimeRange],
        groundTruth: [NarlTimeRange],
        threshold: Double
    ) -> NarlWindowMetricsAtThreshold {
        // Enumerate predictions with stable index for later matching.
        let preds = predicted
        let gts = groundTruth

        // For each pred, find its best IoU vs any gt.
        struct PredMatch { let predIndex: Int; let gtIndex: Int; let iou: Double }
        var matches: [PredMatch] = []
        for (pi, pred) in preds.enumerated() {
            var best: (gi: Int, iou: Double) = (-1, 0)
            for (gi, gt) in gts.enumerated() {
                let value = iou(pred, gt)
                if value > best.iou {
                    best = (gi, value)
                }
            }
            matches.append(PredMatch(predIndex: pi, gtIndex: best.gi, iou: best.iou))
        }

        // Greedy claim: sort by IoU descending, skip already-claimed gts.
        var claimedGT = Set<Int>()
        var tpPairs: [(Int, Int, Double)] = []
        let sorted = matches.sorted { $0.iou > $1.iou }
        for m in sorted {
            guard m.iou >= threshold, m.gtIndex >= 0 else { continue }
            guard !claimedGT.contains(m.gtIndex) else { continue }
            claimedGT.insert(m.gtIndex)
            tpPairs.append((m.predIndex, m.gtIndex, m.iou))
        }

        let tp = tpPairs.count
        let fp = preds.count - tp
        let fn = gts.count - claimedGT.count
        let precision = preds.isEmpty ? 0 : Double(tp) / Double(preds.count)
        let recall = gts.isEmpty ? 0 : Double(tp) / Double(gts.count)
        let f1: Double = {
            let denom = precision + recall
            return denom > 0 ? 2 * precision * recall / denom : 0
        }()
        let meanIoU = tpPairs.isEmpty ? 0 : tpPairs.map(\.2).reduce(0, +) / Double(tpPairs.count)

        return NarlWindowMetricsAtThreshold(
            threshold: threshold,
            truePositives: tp,
            falsePositives: fp,
            falseNegatives: fn,
            precision: precision,
            recall: recall,
            f1: f1,
            meanMatchedIoU: meanIoU
        )
    }
}

// MARK: - Second-level metrics

/// Second-level metrics over the 1-second-binned ad timeline.
struct NarlSecondLevelMetrics: Sendable, Codable, Equatable {
    let truePositiveSeconds: Int
    let falsePositiveSeconds: Int
    let falseNegativeSeconds: Int
    let precision: Double
    let recall: Double
    let f1: Double
}

enum NarlSecondLevel {

    /// Project ranges into a Set<Int> of seconds that are "ad-positive".
    /// A second `k` is positive if the range overlaps [k, k+1). Seconds are
    /// non-negative integers.
    static func secondsCovered(by ranges: [NarlTimeRange]) -> Set<Int> {
        var covered = Set<Int>()
        for r in ranges {
            // r.start inclusive, r.end exclusive.
            let startSec = Int(r.start.rounded(.down))
            let endSec = Int(r.end.rounded(.up)) // one past last included second
            guard endSec > startSec else { continue }
            for s in max(0, startSec)..<max(0, endSec) {
                covered.insert(s)
            }
        }
        return covered
    }

    static func compute(
        predicted: [NarlTimeRange],
        groundTruth: [NarlTimeRange]
    ) -> NarlSecondLevelMetrics {
        let predSet = secondsCovered(by: predicted)
        let gtSet = secondsCovered(by: groundTruth)
        let tp = predSet.intersection(gtSet).count
        let fp = predSet.subtracting(gtSet).count
        let fn = gtSet.subtracting(predSet).count
        let precision = predSet.isEmpty ? 0 : Double(tp) / Double(predSet.count)
        let recall = gtSet.isEmpty ? 0 : Double(tp) / Double(gtSet.count)
        let f1: Double = {
            let denom = precision + recall
            return denom > 0 ? 2 * precision * recall / denom : 0
        }()
        return NarlSecondLevelMetrics(
            truePositiveSeconds: tp,
            falsePositiveSeconds: fp,
            falseNegativeSeconds: fn,
            precision: precision,
            recall: recall,
            f1: f1
        )
    }
}
