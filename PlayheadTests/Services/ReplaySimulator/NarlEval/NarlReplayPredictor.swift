// NarlReplayPredictor.swift
// playhead-narl.1: Produces predicted ad-windows for a FrozenTrace under a
// given MetadataActivationConfig. Used by the eval harness to compare
// `.default` and `.allEnabled` outputs against ground truth.
//
// Design decisions (see docs/plans/2026-04-21-narl-eval-harness-design.md §A.1):
//
// The predictor replays the deterministic stages of the detector at the
// post-fusion layer (fusion output → prior-shift gate → lexical-injection
// boost → policy). It does NOT re-invoke `MetadataLexiconInjector` or
// rebuild the lexicon from atom/cue inputs: corpus-export does not carry
// full transcript atoms, so bit-exact reinjection is not available from a
// FrozenTrace alone. Instead, the predictor uses the per-window
// fused skip confidence + per-window classificationTrust captured in
// `FrozenTrace.windowScores` at capture time, and replays the same gate
// predicates used by production:
//
//   (a) Prior-shift gate (MetadataPriorShift.effectiveMidpoint):
//       When `classifierPriorShiftEnabled` is on AND
//       classificationTrust >= config.classifierPriorShiftMinTrust, the
//       sigmoid midpoint moves from config.classifierBaselineMidpoint to
//       config.classifierShiftedMidpoint. Windows with
//       fusedSkipConfidence in (shiftedMidpoint, baselineMidpoint] flip
//       from "not an ad" under `.default` to "ad" under `.allEnabled`.
//
//   (b) Lexical-injection effect
//       (MetadataLexiconInjector.inject → downstream ledger boost):
//       When `lexicalInjectionEnabled` is on AND
//       classificationTrust >= config.lexicalInjectionMinTrust AND the
//       window has metadata evidence present at capture time
//       (`hasMetadataEvidence == true`), the window receives a calibrated
//       boost proportional to `config.lexicalInjectionDiscount`. Any such
//       window whose post-boost confidence crosses the effective midpoint
//       flips to "ad". This mirrors how the injector's ephemeral lexicon
//       entries contribute additional weight into the ledger; we model
//       the net effect rather than the per-token match count because
//       corpus-export does not persist token-level hit records per window.
//
//   (c) `.default` predictions come from `windowScores` where
//       `isAdUnderDefault == true`. When the capture omitted
//       `windowScores` entirely (v1 fixtures), we fall back to
//       `trace.baselineReplaySpanDecisions.filter { $0.isAd }`.
//
//   (d) `fmSchedulingEnabled`: NOT replayable from FrozenTrace alone (see
//       design §A.1). We surface this as `hasShadowCoverage` on the
//       prediction result. narl.2 populates shadow responses via
//       `shadow-decisions.jsonl`; we leave that plumbing lenient so we
//       don't re-rev this file when narl.2 lands — the corpus builder
//       reads `shadow-decisions.jsonl` with a version-tolerant parser and
//       stashes the deltas on the trace as evidence entries with
//       `source="shadow:<variant>"`.
//
// This predictor's acceptance criteria per the harness are (a) both configs
// produce predictions without throwing and (b) artifacts are written. Metric
// regressions surface in the report for human judgment.

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
    /// Diagnostic: number of windows added or promoted by the lexical
    /// injection branch (i.e. windows that flipped under `.allEnabled` and
    /// would not have flipped under `.default`).
    let lexicalInjectionAdds: Int
    /// Diagnostic: number of windows added or promoted by the prior shift
    /// branch (windows that flipped solely because the midpoint moved).
    let priorShiftAdds: Int
}

// MARK: - Predictor

enum NarlReplayPredictor {

    /// Fallback midpoints used only when `MetadataActivationConfig` does not
    /// expose a per-run override AND the trace has no windowScores (v1
    /// fixtures). Production values live on the config; see
    /// `classifierBaselineMidpoint` / `classifierShiftedMidpoint`.
    ///
    /// TODO(playhead-narl.1 review): if a future version of
    /// MetadataActivationConfig removes the per-instance midpoint fields,
    /// re-route these through `AdDetectionConfig.defaultPromotionThreshold`
    /// or equivalent — the fallback exists only so the predictor degrades
    /// gracefully on legacy fixtures.
    private static let fallbackBaselineMidpoint: Double = 0.25
    private static let fallbackShiftedMidpoint: Double = 0.22

    static func predict(
        trace: FrozenTrace,
        config: MetadataActivationConfig,
        hasShadowCoverage: Bool = false
    ) -> NarlPredictionResult {
        // Resolve midpoints + trust thresholds from the config. The values
        // are the same ones production consults in `MetadataPriorShift`
        // and `MetadataLexiconInjector`.
        let baselineMidpoint = config.classifierBaselineMidpoint
        let shiftedMidpoint = config.classifierShiftedMidpoint
        let priorShiftMinTrust = Double(config.classifierPriorShiftMinTrust)
        let lexicalInjectionMinTrust = Double(config.lexicalInjectionMinTrust)
        let lexicalInjectionDiscount = config.lexicalInjectionDiscount

        // `.default` positive set. Prefer windowScores when available (v2
        // fixtures) since it's the authoritative post-gate bit from capture
        // time; fall back to baseline replay span decisions (v1 fixtures).
        let basePositive: [NarlTimeRange]
        if !trace.windowScores.isEmpty {
            basePositive = trace.windowScores
                .filter { $0.isAdUnderDefault }
                .map { NarlTimeRange(start: $0.windowStart, end: $0.windowEnd) }
        } else {
            basePositive = trace.baselineReplaySpanDecisions
                .filter { $0.isAd }
                .map { NarlTimeRange(start: $0.startTime, end: $0.endTime) }
        }

        // Gate-closed short circuit: `.default` (and any config with
        // counterfactualGateOpen == false) returns the baseline positives
        // unchanged. This mirrors `MetadataActivationConfig.isLexicalInjectionActive`
        // and its siblings.
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

        var extraRanges: [NarlTimeRange] = []
        var lexicalAdds = 0
        var priorShiftAdds = 0

        // Branch A: `windowScores` present (v2 capture path).
        //
        // Iterate per-window decisions and apply both activation effects at
        // their proper predicates.
        if !trace.windowScores.isEmpty {
            for w in trace.windowScores {
                guard !w.isAdUnderDefault else { continue }   // already a positive

                let range = NarlTimeRange(start: w.windowStart, end: w.windowEnd)

                // (a) Prior-shift flip: does this window sit in the
                // (shifted, baseline] band AND qualify for the shift?
                let priorShiftQualifies =
                    config.classifierPriorShiftEnabled &&
                    w.classificationTrust >= priorShiftMinTrust &&
                    w.fusedSkipConfidence > shiftedMidpoint &&
                    w.fusedSkipConfidence <= baselineMidpoint

                if priorShiftQualifies {
                    extraRanges.append(range)
                    priorShiftAdds += 1
                    continue
                }

                // (b) Lexical-injection flip: would an injector boost have
                // pushed this window's confidence past the effective
                // midpoint? Production's injector adds weight to the ledger
                // whenever metadata cues match the transcript. The
                // ledger-to-confidence step is nonlinear and per-window, but
                // the net ceiling is bounded by `lexicalInjectionDiscount`
                // applied to a per-category weight (≤1.0). We use that
                // bound as the replay's inject-magnitude estimate: any
                // window with metadata evidence present whose confidence
                // PLUS `lexicalInjectionDiscount` crosses the effective
                // midpoint is counted as a flip. This is an honest upper
                // bound on injector contribution (matches the "add a
                // baseCategoryWeight*trust*discount to the ledger" shape
                // without re-running the injector's text-matching path).
                //
                // Trust gate matches production `MetadataLexiconInjector`:
                // entries are emitted only when
                // `metadataTrust >= config.lexicalInjectionMinTrust`.
                let effectiveMidpoint: Double = {
                    // When prior-shift is off, the classifier still uses the
                    // baseline midpoint even if lexical injection fires.
                    guard config.classifierPriorShiftEnabled,
                          w.classificationTrust >= priorShiftMinTrust
                    else { return baselineMidpoint }
                    return shiftedMidpoint
                }()

                let lexicalInjectionQualifies =
                    config.lexicalInjectionEnabled &&
                    w.hasMetadataEvidence &&
                    w.classificationTrust >= lexicalInjectionMinTrust &&
                    (w.fusedSkipConfidence + lexicalInjectionDiscount) >= effectiveMidpoint &&
                    w.fusedSkipConfidence < effectiveMidpoint

                if lexicalInjectionQualifies {
                    extraRanges.append(range)
                    lexicalAdds += 1
                }
            }
        } else {
            // Branch B: no windowScores (v1 fixture). Best-effort: use
            // `baselineReplaySpanDecisions` (which does carry `confidence`)
            // and apply the prior-shift rule against whatever confidence
            // the capture recorded. Lexical-injection is not honestly
            // replayable in this branch because we lack per-window
            // metadata-evidence flags — we skip it and document in notes.
            for span in trace.baselineReplaySpanDecisions
                where !span.isAd
                && span.confidence > shiftedMidpoint
                && span.confidence <= baselineMidpoint {

                let range = NarlTimeRange(start: span.startTime, end: span.endTime)
                // Conservative: require *any* evidence catalog entry with
                // source="metadata" somewhere in the trace as the episode-
                // level coarse gate, since v1 fixtures don't carry
                // per-window classificationTrust.
                let hasMetadataEvidence = trace.evidenceCatalog
                    .contains { $0.source == "metadata" }
                guard config.classifierPriorShiftEnabled, hasMetadataEvidence else {
                    continue
                }
                extraRanges.append(range)
                priorShiftAdds += 1
            }
        }

        // (c) fmScheduling — not honestly replayable from FrozenTrace alone.
        // The narl.2 shadow-capture bead produces `shadow-decisions.jsonl`
        // that the corpus builder folds into FrozenTrace. Until that data is
        // present, we mark the episode as not fully shadow-covered and the
        // fmScheduling branch contributes nothing. This means `.allEnabled`'s
        // fmSchedulingEnabled flag has no effect on Phase 1 reports — by design.
        _ = config.fmSchedulingEnabled

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
