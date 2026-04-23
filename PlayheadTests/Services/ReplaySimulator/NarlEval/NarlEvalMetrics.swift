// NarlEvalMetrics.swift
// playhead-narl.1: Window-level IoU + second-level metrics (design §A.5).
// playhead-gtt9.6: Coverage + FN-decomposition metrics (expert-response §10).
//
// Metric families per (show, config):
//   - Window-level: for τ ∈ {0.3, 0.5, 0.7}, a predicted window is TP if it
//     matches some ground-truth window with IoU ≥ τ. Precision = TP / predicted,
//     recall = TP / gt, F1 = harmonic mean. Also mean IoU over matched pairs.
//   - Second-level: project all windows onto the second-resolution timeline
//     (using 1-second bins), count correctly-classified ad-seconds.
//   - Coverage: scored/transcript coverage ratios + auto-skip precision/recall,
//     plus a per-GT-span FN decomposition that distinguishes pipeline coverage
//     failures from classifier / promotion recall failures.

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

// MARK: - Coverage + FN decomposition (gtt9.6, expert-response §10)

/// Kind of ground-truth false-negative span, attributing the miss to the
/// pipeline stage that broke. Ordered by causal precedence (a
/// `pipelineCoverage` miss cannot simultaneously be a `classifierRecall`
/// miss — fix coverage first).
enum NarlFNDecompKind: String, Sendable, Codable, Equatable {
    /// GT span overlaps zero scored windows — the detector never analyzed
    /// this region. Fixing the classifier can't recover it; fix coverage.
    case pipelineCoverage
    /// GT span was scored but no scored window in the overlap crossed the
    /// candidate confidence threshold. Classifier recall failure.
    case classifierRecall
    /// GT span had candidate windows but none were promoted to auto-skip.
    /// Promotion / policy failure (see gtt9.2 reject reasons).
    case promotionRecall
}

/// Per-GT-span FN decomposition row, emitted when a GT span ended up
/// uncovered by the predicted auto-skip set.
struct NarlFNDecomp: Sendable, Codable, Equatable {
    let span: NarlTimeRange
    let kind: NarlFNDecompKind
    let reason: String
}

/// Coverage + FN-rate metrics for one episode. Rollups carry aggregates of
/// the same fields. See docs/narl/2026-04-23-expert-response.md §10.
struct NarlCoverageMetrics: Sendable, Codable, Equatable {
    /// Fraction of episode seconds with at least one emitted scored window.
    let scoredCoverageRatio: Double
    /// Fraction of episode seconds backed by transcript-dependent evidence
    /// (lexical / fm / catalog sources only — this is a lower bound until
    /// gtt9.8 ships the coverage contract).
    let transcriptCoverageRatio: Double
    /// Fraction of GT ad seconds overlapping at least one candidate-threshold
    /// scored window. 0 when GT is empty.
    let candidateRecall: Double
    /// Fraction of predicted auto-skip seconds that are GT ad seconds.
    let autoSkipPrecision: Double
    /// Fraction of GT ad seconds covered by predicted auto-skip seconds.
    let autoSkipRecall: Double
    /// Set-based IoU of merged predicted auto-skip ranges vs. GT ranges.
    let segmentIoU: Double
    /// GT seconds attributable to pipelineCoverage FNs, as fraction of total
    /// GT seconds. Not a classifier error — the detector never ran there.
    let unscoredFNRate: Double
    /// True when `unscoredFNRate > 0.5` — this asset's dominant failure is
    /// coverage, so promoting classifier tuning decisions on it is misleading.
    let pipelineCoverageFailureAsset: Bool
    /// GT seconds in pipelineCoverage FNs.
    let pipelineCoverageFNSeconds: Double
    /// GT seconds in classifierRecall FNs.
    let classifierRecallFNSeconds: Double
    /// GT seconds in promotionRecall FNs.
    let promotionRecallFNSeconds: Double

    static let zero = NarlCoverageMetrics(
        scoredCoverageRatio: 0,
        transcriptCoverageRatio: 0,
        candidateRecall: 0,
        autoSkipPrecision: 0,
        autoSkipRecall: 0,
        segmentIoU: 0,
        unscoredFNRate: 0,
        pipelineCoverageFailureAsset: false,
        pipelineCoverageFNSeconds: 0,
        classifierRecallFNSeconds: 0,
        promotionRecallFNSeconds: 0
    )
}

enum NarlCoverageMetricsCompute {

    /// Default candidate threshold on fusedSkipConfidence. Matches
    /// `hotPathCandidate` gate in production (see expert-response §10).
    static let defaultCandidateThreshold: Double = 0.40

    /// Asset-level coverage-failure threshold: when more than half of GT
    /// seconds are pipelineCoverage FNs, the asset is fundamentally
    /// uncovered and classifier metrics on it are noise.
    static let pipelineCoverageFailureAssetThreshold: Double = 0.5

    /// Transcript-dependent evidence sources. `classifier`, `metadata`,
    /// `acoustic` deliberately excluded — they don't require transcript
    /// to fire. Narrower than the production coverage contract (gtt9.8).
    static let transcriptEvidenceSources: Set<String> = ["lexical", "fm", "catalog"]

    static func compute(
        trace: FrozenTrace,
        predicted: [NarlTimeRange],
        groundTruth: [NarlTimeRange],
        candidateThreshold: Double = NarlCoverageMetricsCompute.defaultCandidateThreshold
    ) -> (metrics: NarlCoverageMetrics, fnDecomposition: [NarlFNDecomp]) {
        // STUB: real implementation to follow test-by-test.
        _ = candidateThreshold
        _ = predicted
        _ = groundTruth
        _ = trace
        return (NarlCoverageMetrics.zero, [])
    }
}
