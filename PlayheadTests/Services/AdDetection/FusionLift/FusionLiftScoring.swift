// FusionLiftScoring.swift
// playhead-au2v.1.27 — Phase A: hermetic scorer foundation for the
// online chapter-signal fusion-lift A/B eval.
//
// This is test-target-only code (it lives in PlayheadTests, alongside
// `SpanMetrics.swift`, so it never bloats the shipped app binary). It
// builds the reusable, PURE scoring pieces an A/B eval needs without
// touching audio, Foundation Models, or the live pipeline:
//
//   1. Bridges  — map ground-truth (`CorpusAnnotation.AdWindow`) and
//                 pipeline predictions (`AdWindow`, the top-level store
//                 row type) into the metric framework's
//                 `MetricGroundTruthAd` / `MetricDetectedAd` value types.
//   2. F1       — `SpanF1` derives precision/recall/F1 from the TP/FP/miss
//                 counts a `MetricsBatch` already carries (`SpanMetrics`
//                 deliberately does not expose F1 itself).
//   3. Lift     — `FusionLiftResult` diffs an OFF `MetricsSummary` against
//                 an ENABLED `MetricsSummary` into precision/recall/F1
//                 deltas plus a readable description. Pure + `Sendable`.
//
// Phase C (the heavy Mac Catalyst harness that runs the real pipeline and
// feeds real `AnalysisStore.AdWindow` rows + real `CorpusAnnotation`
// ground truth into these helpers) is a SEPARATE follow-on. Phase A only
// builds the foundation and proves it with synthetic spans.

import Foundation
@testable import Playhead

// MARK: - Ground-truth bridge

extension MetricGroundTruthAd {

    /// Bridge a labeled corpus ad window into the metric framework's
    /// ground-truth span type.
    ///
    /// `CorpusAnnotation.AdWindow` carries no episode/podcast identity of
    /// its own (identity lives on the enclosing `CorpusAnnotation`), so the
    /// caller supplies `podcastId`, `episodeId`, and a stable `id` for the
    /// span. The `MetricsBatch.pair(...)` greedy-IoU matcher buckets by
    /// `(podcastId, episodeId)`, so passing the SAME pair of ids to both
    /// the ground-truth and detection bridges is what lets a GT ad pair
    /// with a detection from the same episode.
    ///
    /// `format` folds the corpus `AdType` onto the metric framework's
    /// coarser `AdFormat` slicing dimension. The corpus enum is richer
    /// (`blendedHostRead`, `promo`) than `AdFormat`, so the fold is:
    ///   - `hostRead`, `blendedHostRead` → `.hostRead` (both are
    ///     host-delivered; matches `AdFormat.from(_:)`'s existing fold of
    ///     `blendedHostRead` into `.hostRead`).
    ///   - `producedSegment`, `promo`     → `.produced` (pre-produced audio
    ///     segments; `promo` is a produced cross-promo spot).
    ///   - `dynamicInsertion`             → `.dynamic`.
    ///
    /// `seedFired` defaults to `false`: the corpus annotation has no notion
    /// of "did an anchor fire here". The seed-recall metric is out of scope
    /// for the fusion-lift eval (which scores span coverage P/R/F1), so a
    /// conservative `false` keeps it from polluting any seed-recall slice a
    /// future caller computes off the same batch.
    init(
        annotationWindow: CorpusAnnotation.AdWindow,
        id: String,
        podcastId: String,
        episodeId: String,
        seedFired: Bool = false
    ) {
        self.init(
            id: id,
            podcastId: podcastId,
            episodeId: episodeId,
            startTime: annotationWindow.startSeconds,
            endTime: annotationWindow.endSeconds,
            format: MetricGroundTruthAd.adFormat(for: annotationWindow.adType),
            seedFired: seedFired
        )
    }

    /// Fold the corpus `AdType` onto the coarser `AdFormat` slicing
    /// dimension. Kept separate from `AdFormat.from(_:)` (which bridges the
    /// SYNTHETIC fixture's `TestAdSegment.DeliveryStyle`) because the corpus
    /// `AdType` is a distinct enum with `promo`, which the delivery-style
    /// enum lacks.
    static func adFormat(for adType: CorpusAnnotation.AdType) -> AdFormat {
        switch adType {
        case .hostRead, .blendedHostRead:
            return .hostRead
        case .producedSegment, .promo:
            return .produced
        case .dynamicInsertion:
            return .dynamic
        }
    }
}

// MARK: - Detection bridge

extension MetricDetectedAd {

    /// Decision states that count as a REAL ad prediction for scoring.
    ///
    /// `AdWindow.decisionState` is a free `String` whose
    /// canonical values are `AdDecisionState` raw values. Only states that
    /// represent the pipeline asserting "this span is an ad the listener
    /// would want skipped" should count as detections against ground
    /// truth:
    ///   - `candidate` — hot-path detection, skip-ready, not yet confirmed.
    ///   - `confirmed` — backfill re-classification confirmed it as an ad.
    ///   - `applied`   — the skip was actually applied to the listener.
    ///
    /// The remaining states are NOT real ad predictions and must be
    /// excluded so they don't inflate the false-positive count:
    ///   - `suppressed` — fell below threshold after re-classification;
    ///     the pipeline decided it is NOT an ad. (audit/observability row)
    ///   - `reverted`   — the user tapped "Listen", reverting the skip;
    ///     treated as a non-ad outcome for scoring purposes.
    ///
    /// This mirrors `CrossUserAnalysisSnapshot.Window.isAdDecision`, which
    /// is the existing production definition of "this row is an ad
    /// decision" (candidate / confirmed / applied).
    static let skipEligibleDecisionStates: Set<String> = [
        AdDecisionState.candidate.rawValue,
        AdDecisionState.confirmed.rawValue,
        AdDecisionState.applied.rawValue,
    ]

    /// `true` iff `decisionState` represents a real (skip-eligible) ad
    /// prediction. Audit/observability rows (`suppressed`, `reverted`) and
    /// any unknown string return `false`.
    static func isSkipEligibleDecision(_ decisionState: String) -> Bool {
        skipEligibleDecisionStates.contains(decisionState)
    }

    /// Bridge a persisted pipeline prediction into the metric framework's
    /// detected-span type. Returns `nil` when the row is NOT a real ad
    /// prediction (see `isSkipEligibleDecision`) so the caller can filter
    /// audit/observability rows out before pairing.
    ///
    /// As with the ground-truth bridge, `podcastId` / `episodeId` are
    /// supplied by the caller (the store row only knows its
    /// `analysisAssetId`). `path` defaults to `.backfill` because the
    /// fusion-lift eval scores the backfill detection path (where the
    /// CoveragePlanner chapter-informed branch lives). `firstConfirmationTime`
    /// defaults to `nil` — the store row does not persist the moment of
    /// first skip-eligibility, and the lift eval scores span coverage, not
    /// live lead time.
    init?(
        adWindow: AdWindow,
        podcastId: String,
        episodeId: String,
        path: DetectionPath = .backfill,
        firstConfirmationTime: Double? = nil
    ) {
        guard MetricDetectedAd.isSkipEligibleDecision(adWindow.decisionState) else {
            return nil
        }
        self.init(
            id: adWindow.id,
            podcastId: podcastId,
            episodeId: episodeId,
            startTime: adWindow.startTime,
            endTime: adWindow.endTime,
            path: path,
            firstConfirmationTime: firstConfirmationTime,
            confidence: adWindow.confidence
        )
    }
}

extension MetricsBatch {

    /// Convenience: bridge a flat list of store `AdWindow` rows into
    /// `MetricDetectedAd`s, dropping audit/observability rows
    /// (`suppressed`, `reverted`, unknown) via the failable bridge above.
    /// All rows are attributed to the same `(podcastId, episodeId)`.
    static func skipEligibleDetections(
        from adWindows: [AdWindow],
        podcastId: String,
        episodeId: String,
        path: DetectionPath = .backfill
    ) -> [MetricDetectedAd] {
        adWindows.compactMap { window in
            MetricDetectedAd(
                adWindow: window,
                podcastId: podcastId,
                episodeId: episodeId,
                path: path
            )
        }
    }
}

// MARK: - Span F1 (count-based)

/// Count-based span-level precision / recall / F1 derived from a
/// `MetricsBatch`'s TP / FP / miss pair tallies.
///
/// `SpanMetrics` deliberately does NOT expose F1 directly — it exposes the
/// raw pair classifications (`isTruePositive` / `isMiss` /
/// `isFalsePositive`) and the seconds-based coverage metrics. The
/// fusion-lift eval wants the classic span-detection F1 (how many GT ads
/// did we pair, vs. how many we invented / missed), so this helper rolls
/// the counts into the standard formulas:
///
///   precision = TP / (TP + FP)     — fraction of detections that hit a GT ad
///   recall    = TP / (TP + miss)   — fraction of GT ads that got a detection
///   F1        = 2·P·R / (P + R)    — harmonic mean
///
/// All three are `nil` when their denominator is zero (undefined), never
/// `NaN`:
///   - precision `nil` ⇔ no detections at all (TP + FP == 0).
///   - recall    `nil` ⇔ no ground-truth ads at all (TP + miss == 0).
///   - F1        `nil` ⇔ precision or recall is `nil`, OR both are 0 (the
///     `P + R == 0` harmonic-mean singularity). A batch with detections
///     and GT ads but zero overlap yields P == 0, R == 0, F1 == 0 — not
///     `nil` — because both denominators are non-zero (defined as 0).
struct SpanF1: Sendable, Hashable {
    let truePositives: Int
    let falsePositives: Int
    let misses: Int

    init(truePositives: Int, falsePositives: Int, misses: Int) {
        precondition(truePositives >= 0 && falsePositives >= 0 && misses >= 0,
                     "SpanF1 counts must be non-negative")
        self.truePositives = truePositives
        self.falsePositives = falsePositives
        self.misses = misses
    }

    /// Derive counts from a paired batch.
    init(batch: MetricsBatch) {
        var tp = 0, fp = 0, miss = 0
        for pair in batch.pairs {
            if pair.isTruePositive { tp += 1 }
            else if pair.isFalsePositive { fp += 1 }
            else if pair.isMiss { miss += 1 }
        }
        self.init(truePositives: tp, falsePositives: fp, misses: miss)
    }

    /// TP / (TP + FP). `nil` when there are no detections.
    var precision: Double? {
        let denom = truePositives + falsePositives
        return denom == 0 ? nil : Double(truePositives) / Double(denom)
    }

    /// TP / (TP + miss). `nil` when there are no ground-truth ads.
    var recall: Double? {
        let denom = truePositives + misses
        return denom == 0 ? nil : Double(truePositives) / Double(denom)
    }

    /// 2·P·R / (P + R). `nil` when precision or recall is undefined, or
    /// when both are zero (the harmonic-mean singularity).
    var f1: Double? {
        guard let p = precision, let r = recall else { return nil }
        let sum = p + r
        guard sum > 0 else { return 0.0 }
        return 2 * p * r / sum
    }
}

// MARK: - Fusion-lift diff

/// The measured A/B lift of turning the chapter signal ON (`.enabled`)
/// versus the production default (`.off`), over a single scored slice.
///
/// "Delta" is always `enabled − off`, so a POSITIVE delta means enabling
/// the chapter signal HELPED that metric (higher precision / recall / F1),
/// and a negative delta means it HURT. Zero means no change.
///
/// All three deltas are `Double?`: a delta is `nil` whenever either side's
/// metric is undefined (`nil`) — you cannot subtract an undefined value.
/// This propagates the metric framework's "defined behavior on empty
/// input" contract through the diff: an empty slice produces `nil` deltas,
/// not a misleading `0.0`.
///
/// Pure value type, `Sendable`, no I/O — safe to compute anywhere.
struct FusionLiftResult: Sendable, Hashable {
    /// `enabled.coveragePrecision − off.coveragePrecision`, or `nil` if
    /// either side is undefined.
    let precisionDelta: Double?
    /// `enabled.coverageRecall − off.coverageRecall`, or `nil` if either
    /// side is undefined.
    let recallDelta: Double?
    /// `enabledF1 − offF1`, where each side's F1 is the harmonic mean of
    /// that side's coverage precision and recall. `nil` if either F1 is
    /// undefined.
    let f1Delta: Double?

    /// Coverage F1 (harmonic mean of coverage precision & recall) for the
    /// OFF arm, surfaced so callers can report absolute values alongside
    /// the delta. `nil` when either coverage metric is undefined.
    let offF1: Double?
    /// Coverage F1 for the ENABLED arm. `nil` when either coverage metric
    /// is undefined.
    let enabledF1: Double?

    /// Compute the lift between an OFF-arm summary and an ENABLED-arm
    /// summary. Uses the seconds-based COVERAGE precision/recall on
    /// `MetricsSummary` (`coveragePrecision` / `coverageRecall`) — the
    /// summary type's defined P/R lens — and derives F1 as their harmonic
    /// mean.
    init(off: MetricsSummary, enabled: MetricsSummary) {
        self.precisionDelta = FusionLiftResult.delta(
            off.coveragePrecision, enabled.coveragePrecision
        )
        self.recallDelta = FusionLiftResult.delta(
            off.coverageRecall, enabled.coverageRecall
        )
        let offF1 = FusionLiftResult.harmonicMean(
            off.coveragePrecision, off.coverageRecall
        )
        let enabledF1 = FusionLiftResult.harmonicMean(
            enabled.coveragePrecision, enabled.coverageRecall
        )
        self.offF1 = offF1
        self.enabledF1 = enabledF1
        self.f1Delta = FusionLiftResult.delta(offF1, enabledF1)
    }

    /// Designated init for direct construction (used by callers that
    /// already have count-based F1 in hand, e.g. `SpanF1`, or by tests).
    init(
        precisionDelta: Double?,
        recallDelta: Double?,
        f1Delta: Double?,
        offF1: Double? = nil,
        enabledF1: Double? = nil
    ) {
        self.precisionDelta = precisionDelta
        self.recallDelta = recallDelta
        self.f1Delta = f1Delta
        self.offF1 = offF1
        self.enabledF1 = enabledF1
    }

    /// Build a lift from two count-based `SpanF1` bundles. The deltas use
    /// the span-level (count-based) precision/recall/F1 rather than the
    /// seconds-based coverage metrics — the right lens when the eval cares
    /// about "how many ads did we pair" rather than "how many seconds did
    /// we cover".
    init(off: SpanF1, enabled: SpanF1) {
        self.precisionDelta = FusionLiftResult.delta(off.precision, enabled.precision)
        self.recallDelta = FusionLiftResult.delta(off.recall, enabled.recall)
        self.offF1 = off.f1
        self.enabledF1 = enabled.f1
        self.f1Delta = FusionLiftResult.delta(off.f1, enabled.f1)
    }

    /// `enabled − off`, or `nil` if either side is `nil`.
    private static func delta(_ off: Double?, _ enabled: Double?) -> Double? {
        guard let off, let enabled else { return nil }
        return enabled - off
    }

    /// Harmonic mean of two optionals, or `nil` if either is `nil`.
    /// Returns `0.0` (not `nil`) when both are defined but sum to 0 — the
    /// same singularity rule `SpanF1.f1` uses.
    private static func harmonicMean(_ a: Double?, _ b: Double?) -> Double? {
        guard let a, let b else { return nil }
        let sum = a + b
        guard sum > 0 else { return 0.0 }
        return 2 * a * b / sum
    }

    /// A readable one-line summary of the lift. Undefined deltas render as
    /// `n/a`; defined deltas render signed to 4 decimal places.
    var description: String {
        func fmt(_ d: Double?) -> String {
            guard let d else { return "n/a" }
            return String(format: "%+.4f", d)
        }
        return "FusionLift(precisionΔ=\(fmt(precisionDelta)), "
            + "recallΔ=\(fmt(recallDelta)), f1Δ=\(fmt(f1Delta)))"
    }
}
