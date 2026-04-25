// BracketAwareBoundaryRefiner.swift
// playhead-arf8: Bracket-aware boundary refinement (graduated from shadow).
//
// Composes three previously-shadowed components into a live boundary
// refinement path:
//
//   1. `BracketDetector.scanForBrackets` runs the deterministic envelope
//      state machine over the candidate region's surrounding feature
//      windows. It returns `BracketEvidence` (onset/offset times, template
//      class, coarse score) when a music-bed bracket is found, nil
//      otherwise.
//   2. `FineBoundaryRefiner.refineBoundary` runs a local ±3s search at
//      150ms hops around each bracket edge, snapping to silence /
//      energy / spectral cues. The result is a `BoundaryEstimate` with
//      its own confidence value and asymmetric guard margin already
//      applied.
//   3. The candidate adjustments are clamped to the same `±3s` budget
//      as the legacy `BoundaryRefiner` so the bracket path can never
//      shift a boundary further than the existing TimeBoundaryResolver
//      path can.
//
// **Design contract — scored cue, not an override.**
//   * Activation requires every gate to pass: master flag on,
//     per-show trust above floor, bracket coarse score above floor,
//     fine-boundary confidence above floor for *both* edges. Any gate
//     failing returns `.legacy` so the caller falls back to the
//     existing `BoundaryRefiner.computeAdjustments` output.
//   * The refiner is pure / value-typed. It does not write to
//     `MusicBracketTrustStore`; outcome accumulation is a separate
//     concern to be scoped post-dogfood.
//
// Rollback is a one-line config flip: setting
// `AdDetectionConfig.bracketRefinementEnabled = false` reverts the
// backfill loop to pre-arf8 behaviour without code changes.

import Foundation

// MARK: - BracketAwareBoundaryRefiner

enum BracketAwareBoundaryRefiner {

    // MARK: - Constants

    /// Maximum signed boundary adjustment (seconds) in either direction.
    /// Matches `BoundaryRefiner.maxBoundaryAdjust` so the bracket path
    /// cannot move a boundary further than the legacy path could.
    static let maxBoundaryAdjust: Double = 3.0

    // MARK: - Outcome

    /// Which path produced the returned (startAdj, endAdj). Carried
    /// alongside the numeric adjustments so callers can log + future
    /// telemetry can attribute boundary quality changes to the bracket
    /// graduation.
    enum Path: Sendable, Equatable {
        /// Gate was bypassed: feature flag off, fewer than 3 windows in
        /// scope, or some other "do nothing different from legacy"
        /// condition. Returned adjustments are zero; caller falls back
        /// to `BoundaryRefiner.computeAdjustments`.
        case legacy
        /// Bracket detector found no envelope match for this candidate.
        /// Common case for host-read ad copy with no music bed.
        /// Returned adjustments are zero; caller falls back to legacy.
        case noBracket
        /// Bracket evidence found but show trust too low.
        case trustGated(showTrust: Double)
        /// Bracket evidence found but coarse score below floor.
        case coarseGated(coarseScore: Double)
        /// Bracket + fine refinement applied. Both edges produced a
        /// `BoundaryEstimate` with confidence above floor.
        case bracketRefined(coarseScore: Double, startConfidence: Double, endConfidence: Double, template: BracketTemplate)
        /// Bracket evidence found but fine refinement on at least one
        /// edge fell below the confidence floor — fall back to legacy.
        case fineConfidenceGated(startConfidence: Double, endConfidence: Double)
    }

    /// Result of a bracket-aware refinement call.
    /// `startAdjust` / `endAdjust` are zero when `path == .legacy`,
    /// `.noBracket`, `.trustGated`, `.coarseGated`, or
    /// `.fineConfidenceGated` — the caller is responsible for invoking
    /// the legacy refiner in those cases.
    struct Result: Sendable, Equatable {
        let startAdjust: Double
        let endAdjust: Double
        let path: Path

        /// Sentinel value: do nothing, defer to legacy.
        static let legacyFallback = Result(startAdjust: 0, endAdjust: 0, path: .legacy)
    }

    // MARK: - Public API

    /// Compute bracket-aware adjustments for a candidate span.
    ///
    /// - Parameters:
    ///   - windows: All feature windows for the episode. Used by both
    ///     the bracket scanner (which expands its own search margin) and
    ///     the fine refiner (which filters to its own ±3s band).
    ///   - candidateStart: Pre-refinement span start (seconds).
    ///   - candidateEnd: Pre-refinement span end (seconds).
    ///   - showTrust: Posterior mean from `MusicBracketTrustStore`,
    ///     typically the per-show `Beta(α,β)` mean. The default
    ///     `Beta(5,5)` prior gives 0.50.
    ///   - config: `AdDetectionConfig` slice driving every gate.
    /// - Returns: Adjustments + path. `path == .legacy` etc. signals
    ///   "fall back to `BoundaryRefiner.computeAdjustments`".
    static func computeAdjustments(
        windows: [FeatureWindow],
        candidateStart: Double,
        candidateEnd: Double,
        showTrust: Double,
        config: AdDetectionConfig
    ) -> Result {
        // Master kill switch: skip the entire bracket path.
        guard config.bracketRefinementEnabled else {
            return .legacyFallback
        }

        // Need at least 3 windows for the same reason `BoundaryRefiner`
        // does — fewer windows can't carry a meaningful bracket envelope.
        guard windows.count >= 3 else {
            return .legacyFallback
        }

        // Per-show trust gate. Suppress refinement when the show's
        // bracket reliability is below floor.
        guard showTrust >= config.bracketRefinementMinTrust else {
            return Result(startAdjust: 0, endAdjust: 0, path: .trustGated(showTrust: showTrust))
        }

        guard let evidence = BracketDetector.scanForBrackets(
            around: candidateStart,
            candidateEnd: candidateEnd,
            using: windows,
            showTrust: showTrust
        ) else {
            return Result(startAdjust: 0, endAdjust: 0, path: .noBracket)
        }

        // Coarse score gate: bracket detector found *something* but
        // signal strength is too weak to override the legacy snap.
        guard evidence.coarseScore >= config.bracketRefinementMinCoarseScore else {
            return Result(startAdjust: 0, endAdjust: 0, path: .coarseGated(coarseScore: evidence.coarseScore))
        }

        // Fine-grained refinement on each edge. The bracket onset/offset
        // are the *prior* on where to search; FineBoundaryRefiner does
        // the precision step within ±3s.
        let startEstimate = FineBoundaryRefiner.refineBoundary(
            candidate: evidence.onsetTime,
            features: windows,
            direction: .adStart
        )
        let endEstimate = FineBoundaryRefiner.refineBoundary(
            candidate: evidence.offsetTime,
            features: windows,
            direction: .adEnd
        )

        // Both edges must clear the fine-confidence floor; otherwise we
        // don't trust the refinement enough to override the legacy path.
        guard
            startEstimate.confidence >= config.bracketRefinementMinFineConfidence,
            endEstimate.confidence >= config.bracketRefinementMinFineConfidence
        else {
            return Result(
                startAdjust: 0,
                endAdjust: 0,
                path: .fineConfidenceGated(
                    startConfidence: startEstimate.confidence,
                    endConfidence: endEstimate.confidence
                )
            )
        }

        // Compute signed adjustments and clamp to ±maxBoundaryAdjust.
        // `FineBoundaryRefiner` already applied asymmetric guard margins
        // and non-negative clamping, so we only enforce the magnitude
        // budget here.
        let startAdjust = clamp(startEstimate.time - candidateStart)
        let endAdjust = clamp(endEstimate.time - candidateEnd)

        return Result(
            startAdjust: startAdjust,
            endAdjust: endAdjust,
            path: .bracketRefined(
                coarseScore: evidence.coarseScore,
                startConfidence: startEstimate.confidence,
                endConfidence: endEstimate.confidence,
                template: evidence.templateClass
            )
        )
    }

    // MARK: - Helpers

    private static func clamp(_ adjustment: Double) -> Double {
        max(-maxBoundaryAdjust, min(adjustment, maxBoundaryAdjust))
    }
}
