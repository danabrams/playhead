// NarlReplayPredictor.swift
// playhead-narl.1: Produces predicted ad-windows for a FrozenTrace under a
// given MetadataActivationConfig. Used by the eval harness to compare
// `.default` and `.allEnabled` outputs against ground truth.
//
// Design decisions (see docs/plans/2026-04-21-narl-eval-harness-design.md §A.1):
//
//   * `.default` predictions come from the trace's `baselineReplaySpanDecisions`
//     directly (filtered to isAd=true). This IS the production detector's
//     output at capture time, and is deterministic by construction.
//
//   * `.allEnabled` applies two locally-deterministic transformations:
//
//       (a) **Lexical injection effect**: any evidence entry whose source is
//           "metadata" contributes a boost to overlapping baseline spans.
//           In production, metadata cues feed MetadataLexiconInjector whose
//           entries accumulate into the ledger. The FrozenTrace evidence
//           catalog preserves these entries with their weights; we re-scale
//           them using `lexicalInjectionDiscount` and re-threshold.
//
//       (b) **Classifier prior shift effect**: a baseline span whose confidence
//           sits in the band (shiftedMidpoint, baselineMidpoint] would have
//           been promoted under `.allEnabled` but suppressed under `.default`.
//           We surface those spans as additional predictions. We do NOT have
//           per-window metadataTrust in the FrozenTrace today, so the shift
//           is applied whenever there is ANY metadata evidence for the episode,
//           consistent with the "episode-level metadata trust" coarse gate.
//
//   * `fmSchedulingEnabled`: NOT replayable from FrozenTrace alone (see §A.1).
//     We surface this as `hasShadowCoverage` on the prediction result. narl.2
//     will populate shadow responses; we leave that plumbing lenient so we
//     don't have to re-revv this file when narl.2 lands — we read
//     `shadow-decisions.jsonl` with a version-tolerant parser in the corpus
//     builder and stash the deltas on the trace there.
//
// This predictor is intentionally light. The harness' acceptance criteria are:
// (a) both configs produce predictions without throwing, (b) artifacts are
// written. Metric regressions surface in the report for human judgment.

import Foundation
@testable import Playhead

// MARK: - Prediction result

/// Output of one predictor run against one FrozenTrace under one config.
struct NarlPredictionResult: Sendable {
    /// Config used for this run.
    let config: MetadataActivationConfig
    /// Predicted ad-windows (sorted, merged).
    let windows: [NarlTimeRange]
    /// Whether the run had any shadow FM coverage for the
    /// `fmSchedulingEnabled` code path. Always false in Phase 1 pre-narl.2.
    let hasShadowCoverage: Bool
    /// Diagnostic: number of windows added by the lexical injection branch.
    let lexicalInjectionAdds: Int
    /// Diagnostic: number of windows added/promoted by the prior shift branch.
    let priorShiftAdds: Int
}

// MARK: - Predictor

enum NarlReplayPredictor {

    /// Promotion threshold used by the classifier after sigmoid midpoint.
    /// Matches AdDetectionConfig's default promotion threshold at this layer —
    /// the midpoint shift in MetadataActivationConfig *directly* changes which
    /// spans qualify. We mirror that here so the `.default` and `.allEnabled`
    /// outputs are comparable to what the production path would emit.
    private static let baselineMidpoint: Double = 0.25
    private static let shiftedMidpoint: Double = 0.22

    /// Minimum classificationTrust threshold for metadata evidence to count
    /// as "episode-level metadata present" under the coarse gate.
    private static let metadataTrustGate: Float = 0.08

    static func predict(
        trace: FrozenTrace,
        config: MetadataActivationConfig,
        hasShadowCoverage: Bool = false
    ) -> NarlPredictionResult {
        // Base: spans the production detector already promoted. These are
        // the honest `.default` output — they passed the existing gates at
        // capture time.
        let basePositive = trace.baselineReplaySpanDecisions
            .filter { $0.isAd }
            .map { NarlTimeRange(start: $0.startTime, end: $0.endTime) }

        // If the gate is closed, the config degenerates to `.default` — no
        // transformations apply, regardless of individual flags. This mirrors
        // `MetadataActivationConfig.isLexicalInjectionActive` etc.
        let gateOpen = config.counterfactualGateOpen
        guard gateOpen else {
            return NarlPredictionResult(
                config: config,
                windows: NarlGroundTruth.mergeOverlaps(basePositive),
                hasShadowCoverage: hasShadowCoverage,
                lexicalInjectionAdds: 0,
                priorShiftAdds: 0
            )
        }

        // Episode-level "does metadata exist" signal. Lenient: any catalog
        // entry with source=="metadata" counts.
        let hasMetadataEvidence = trace.evidenceCatalog.contains { $0.source == "metadata" }

        var extraRanges: [NarlTimeRange] = []
        var lexicalAdds = 0
        var priorShiftAdds = 0

        // (a) Lexical injection effect. In production, metadata-derived
        // lexicon hits merge into the ledger. For a FrozenTrace replay, any
        // metadata-evidence window that does NOT already overlap a baseline
        // ad span becomes a candidate when the gate is open.
        if config.lexicalInjectionEnabled && hasMetadataEvidence {
            for entry in trace.evidenceCatalog where entry.source == "metadata" {
                let range = NarlTimeRange(start: entry.windowStart, end: entry.windowEnd)
                let effectiveWeight = entry.weight * config.lexicalInjectionDiscount
                // Only admit if the effective weight crosses the promotion
                // threshold under the (possibly shifted) midpoint.
                let effectiveThreshold = config.classifierPriorShiftEnabled
                    ? shiftedMidpoint
                    : baselineMidpoint
                guard effectiveWeight >= effectiveThreshold else { continue }
                // Don't double-count spans that already overlap existing positives.
                let alreadyCovered = basePositive.contains { $0.overlaps(range) }
                if !alreadyCovered {
                    extraRanges.append(range)
                    lexicalAdds += 1
                }
            }
        }

        // (b) Classifier prior shift effect. A baseline span whose confidence
        // sits in (shiftedMidpoint, baselineMidpoint] — effectively "borderline
        // rejected under .default" — gets promoted under `.allEnabled` when
        // metadata evidence signals trust.
        if config.classifierPriorShiftEnabled && hasMetadataEvidence {
            for span in trace.baselineReplaySpanDecisions
                where !span.isAd
                && span.confidence > shiftedMidpoint
                && span.confidence <= baselineMidpoint {
                let range = NarlTimeRange(start: span.startTime, end: span.endTime)
                let alreadyCovered = basePositive.contains { $0.overlaps(range) }
                if !alreadyCovered {
                    extraRanges.append(range)
                    priorShiftAdds += 1
                }
            }
        }

        // (c) fmScheduling — not honestly replayable from FrozenTrace alone.
        // The narl.2 shadow-capture bead produces `shadow-decisions.jsonl`
        // that the corpus builder folds into FrozenTrace. Until that data is
        // present, we mark the episode as not fully shadow-covered and the
        // fmScheduling branch contributes nothing. This means `.allEnabled`'s
        // fmSchedulingEnabled flag has no effect on Phase 1 reports — by design.
        _ = config.fmSchedulingEnabled
        _ = hasShadowCoverage

        let merged = NarlGroundTruth.mergeOverlaps(basePositive + extraRanges)
        return NarlPredictionResult(
            config: config,
            windows: merged,
            hasShadowCoverage: hasShadowCoverage,
            lexicalInjectionAdds: lexicalAdds,
            priorShiftAdds: priorShiftAdds
        )
    }
}
