// ClassifierCalibration.swift
// playhead-gtt9.26: Platt-scaling calibration of the post-fusion classifier
// score that feeds AutoSkipPrecisionGate.
//
// Why this exists
// ---------------
// 2026-04-25 NARL eval shows post-fusion classifier confidence concentrated
// in the 0.30–0.40 mode (78/147 windows = 53%). On the `completeFull`
// cohort, AutoSkip Precision = 1.00 but Recall = 14% — the boundary
// between "ad" and "not ad" lives at a probability much lower than the
// 0.55 auto-skip threshold the gate uses today. The raw score IS predictive
// (the report's distribution shows GT-positive windows are over-represented
// at 0.30–0.40), but the score's CALIBRATION — its meaning as a probability
// — is poor.
//
// Platt scaling fits a one-parameter logistic on labeled (raw, isAd) pairs:
//
//     calibrated = 1 / (1 + exp(A · raw + B))
//
// (A, B) are fit by minimizing the negative log-likelihood of the labels.
// The fit is run OFFLINE on a labeled corpus (user corrections + NARL
// ground truth) and the (A, B) coefficients are baked into the binary
// keyed by (detectorVersion, buildCommitSHA). The calibrated probability
// is then a meaningful confidence — a calibrated 0.85 actually means
// "85% chance this is an ad" instead of "the post-fusion sum hit 0.85".
//
// Where this layer lives
// ----------------------
// **Post-fusion, pre-gate.** The bead's title is "Platt scaling on raw
// classifier output between FM and AutoSkipPrecisionGate". In production,
// the score that flows into AutoSkipPrecisionGate is the post-fusion
// segment score (single-window: ClassifierResult.adProbability;
// aggregator: SegmentAggregator.segmentScore). Both are reduced from the
// fused ledger, both feed AutoSkipPrecisionGate.classify via the same
// `precisionGateLabel(...)` helper. Calibrating at that one site hits
// every auto-skip decision and matches the score the NARL eval already
// mode-analyzes (`FrozenTrace.windowScores[i].fusedSkipConfidence`).
//
// Pre-fusion calibration (calibrate the FM "containsAd / certainty band"
// pseudo-probability before it joins the ledger) was rejected because the
// FM coarse output is not a probability — it's a (disposition, band)
// pair already mapped to a fixed 0.33/0.66/1.00 confidence by
// `ShadowDecisionsExporter.confidenceFor(certainty:)`. There's no raw
// probability to calibrate at that stage; the post-fusion fused score is
// the first place a true probability candidate exists.
//
// Detector-version safety
// -----------------------
// playhead-gtt9.21 stamped `detectorVersion` + `buildCommitSHA` onto each
// FrozenTrace and AnalysisAsset row. A fitted (A, B) trained against
// scores produced by `detection-v1`/`<sha>` is INVALID for any subsequent
// detector version (the underlying score distribution can shift when FM
// prompts, fusion weights, or per-source calibrators change). The
// profile lookup compares both keys; a mismatch returns the identity
// calibrator (pass-through). This is the cold-start contract: a new
// detector version starts uncalibrated, identical to today's behavior,
// until a fresh fit is baked in.

import Foundation
import OSLog

// MARK: - PlattCoefficients

/// (A, B) parameters of a one-parameter logistic calibration:
///
///     calibrated = 1 / (1 + exp(A · raw + B))
///
/// A is the slope, B is the bias. Convention: A < 0 maps a higher raw
/// score to a higher calibrated probability (the standard sign for an
/// ad-classifier whose raw score correlates positively with the "is ad"
/// label).
struct PlattCoefficients: Sendable, Equatable {
    let a: Double
    let b: Double

    /// Identity-equivalent coefficients: A=0, B=0 → constant 0.5. Not
    /// directly the identity function on [0, 1], so callers requesting
    /// "no calibration" must use `ClassifierCalibration.identity`
    /// instead, which short-circuits to pass-through.
    static let zero = PlattCoefficients(a: 0, b: 0)
}

// MARK: - ClassifierCalibration

/// Platt-scaling calibrator. Pure value type. Two flavors:
///
///   - `.identity` — pass-through. Used at cold-start when no fit exists
///     for the current `(detectorVersion, buildCommitSHA)`.
///   - `.platt(coefficients:)` — applies the logistic sigmoid with the
///     given (A, B).
///
/// The struct is intentionally minimal: the `calibrate(_:)` method is
/// allocation-free and deterministic. Callers (the precision-gate wiring
/// in AdDetectionService, and the NARL replay path) construct one via
/// `ClassifierCalibrationProfile.calibrator(for:detectorVersion:buildCommitSHA:)`
/// and apply it per window.
struct ClassifierCalibration: Sendable, Equatable {

    /// Internal kind discriminator. Public callers use the static
    /// constructors below.
    enum Kind: Sendable, Equatable {
        case identity
        case platt(PlattCoefficients)
    }

    let kind: Kind

    /// Pass-through calibrator. `calibrate(x) == x` for all finite x in
    /// [0, 1]. Returned by the profile when no fit matches the active
    /// `(detectorVersion, buildCommitSHA)`.
    static let identity = ClassifierCalibration(kind: .identity)

    /// Platt-scaling calibrator with the given (A, B). Use
    /// `PlattScalingFitter.fit(...)` to derive coefficients from labels.
    static func platt(_ coefficients: PlattCoefficients) -> ClassifierCalibration {
        ClassifierCalibration(kind: .platt(coefficients))
    }

    /// Apply the calibration to a raw score. Input is clamped to
    /// [0, 1]; output is in [0, 1] by construction.
    ///
    /// Non-finite inputs (NaN, ±Infinity) return 0 — a paranoid guard
    /// matching the rest of the AdDetection layer.
    func calibrate(_ raw: Double) -> Double {
        guard raw.isFinite else { return 0 }
        let clamped = max(0, min(1, raw))
        switch kind {
        case .identity:
            return clamped
        case .platt(let c):
            // Platt 1999 form: P(y=1 | f) = 1 / (1 + exp(A·f + B)).
            // Note this is `sigmoid(-z)` where `z = A·x + B` — for the
            // ad-classifier convention (A < 0), increasing the raw score
            // decreases z and thus increases the calibrated probability.
            //
            // Numerically stable: branch on the sign of z to avoid
            // exp() overflow when |z| is large.
            let z = c.a * clamped + c.b
            if z >= 0 {
                // 1 / (1 + e^z) = e^{-z} / (e^{-z} + 1)
                let e = exp(-z)
                return e / (1.0 + e)
            } else {
                // 1 / (1 + e^z) — direct evaluation safe when z < 0
                return 1.0 / (1.0 + exp(z))
            }
        }
    }
}

// MARK: - ClassifierCalibrationProfile

/// Versioned bundle of fitted Platt coefficients keyed by
/// `(detectorVersion, buildCommitSHA)`. Returns `.identity` when no fit
/// matches — that's the cold-start pass-through contract.
///
/// New fits are added by editing `Self.allFits` to include another
/// `Fit` row. The fit's coefficients are the output of running
/// `PlattScalingFitter.fit(_:)` on a labeled corpus collected at that
/// detector + binary version.
struct ClassifierCalibrationProfile: Sendable {

    /// One fitted-coefficient entry. Two fits with the same detector
    /// version but different commit SHAs are independent — a refit
    /// after a code change can ship as an additional row without
    /// retiring the previous one (useful when bisecting a regression).
    struct Fit: Sendable, Equatable {
        let detectorVersion: String
        let buildCommitSHA: String
        let coefficients: PlattCoefficients
        /// Optional human-readable provenance: corpus name + sample
        /// count. Not consumed by production; logged when the profile
        /// is selected so a debug dump can correlate calibration with
        /// the corpus that produced it.
        let corpusLabel: String
        let trainingSampleCount: Int
    }

    /// All fits known to this build. The lookup short-circuits on the
    /// first match — new fits go at the front.
    let fits: [Fit]

    /// Resolve the calibrator for the active detector + build. Returns
    /// `.identity` when no fit matches — this is the cold-start
    /// behaviour and matches the bead's "pass-through, no errors"
    /// acceptance row.
    ///
    /// - Parameter detectorVersion: the active detector version (for
    ///   production, `AdDetectionConfig.detectorVersion`; for replay,
    ///   `FrozenTrace.detectorVersion`).
    /// - Parameter buildCommitSHA: the active binary's commit SHA (for
    ///   production, `BuildInfo.commitSHA`; for replay,
    ///   `FrozenTrace.buildCommitSHA`).
    func calibrator(
        detectorVersion: String,
        buildCommitSHA: String
    ) -> ClassifierCalibration {
        for fit in fits {
            if fit.detectorVersion == detectorVersion,
               fit.buildCommitSHA == buildCommitSHA {
                return .platt(fit.coefficients)
            }
        }
        return .identity
    }

    /// Whether at least one fit matches the active detector + build.
    /// Diagnostic helper for telemetry / replay summaries.
    func hasFit(detectorVersion: String, buildCommitSHA: String) -> Bool {
        fits.contains {
            $0.detectorVersion == detectorVersion &&
            $0.buildCommitSHA == buildCommitSHA
        }
    }

    // MARK: - Bundled profiles

    /// The empty profile: no fits, every lookup returns `.identity`.
    /// Used by tests that want to assert the cold-start contract
    /// without depending on whatever fits the production profile
    /// happens to ship.
    static let empty = ClassifierCalibrationProfile(fits: [])

    /// Production profile shipped with the binary.
    ///
    /// Initial release (gtt9.26): empty. The bead requires the *layer*
    /// to ship; the *fitted coefficients* require a labeled corpus
    /// large enough that the fit isn't overfitting to a handful of
    /// captures. The 2026-04-25 corpus has 226 windowScores across all
    /// FrozenTrace fixtures, but the labeled subset that ties windows
    /// to GT-ad/GT-non-ad is smaller and per-show. Shipping `.empty`
    /// makes the production gate identical to pre-gtt9.26 behavior; a
    /// follow-on bead (or this bead's own AUC-PR validation harness)
    /// adds the first real `Fit` entry once the corpus is large enough
    /// to fit honestly.
    ///
    /// Cold-start is the safe default. The replacement here is a
    /// one-line edit when the fit is ready.
    static let production = ClassifierCalibrationProfile(fits: [])
}

// MARK: - PlattScalingFitter

/// Fits Platt-scaling coefficients (A, B) to labeled (raw, isAd)
/// samples by maximizing the log-likelihood of the labels under the
/// logistic model.
///
/// This is a stand-alone batch fitter. It is NOT used at runtime by the
/// production gate — that path reads pre-baked coefficients from
/// `ClassifierCalibrationProfile`. The fitter exists for the offline
/// AUC-PR validation harness (NarlClassifierCalibrationTests) and for
/// future bake-in workflows.
///
/// Implementation: Newton-Raphson with the Hawley/Lin/Lin (2007)
/// regularized objective. Converges in well under 50 iterations on
/// reasonable inputs and is numerically stable across the
/// [-700, 700] sigmoid input range.
enum PlattScalingFitter {

    /// One labeled training sample. `raw` should be in [0, 1] (the
    /// post-fusion classifier score); `isAd` is the binary label.
    struct Sample: Sendable, Equatable {
        let raw: Double
        let isAd: Bool
    }

    /// Fit (A, B) coefficients. Returns `nil` when the inputs do not
    /// support a meaningful fit (empty corpus, all-positive or
    /// all-negative labels — Platt scaling is undefined when the
    /// labels are degenerate).
    ///
    /// Default hyperparameters mirror the canonical Platt 1999 /
    /// Lin-Lin-Weng 2007 implementation:
    ///   - `maxIterations`: 100 — typically converges in 10–20.
    ///   - `minStep`: 1e-10 — line-search backoff floor.
    ///   - `sigma`: 1e-12 — Hessian regularization.
    ///   - `epsilon`: 1e-5 — convergence tolerance on the gradient norm.
    static func fit(
        samples: [Sample],
        maxIterations: Int = 100,
        minStep: Double = 1e-10,
        sigma: Double = 1e-12,
        epsilon: Double = 1e-5
    ) -> PlattCoefficients? {
        guard !samples.isEmpty else { return nil }

        let prior1 = samples.reduce(0) { $0 + ($1.isAd ? 1 : 0) }
        let prior0 = samples.count - prior1
        guard prior1 > 0, prior0 > 0 else { return nil }

        // Lin-Lin-Weng smoothed targets to stabilize fits when one class
        // is much smaller than the other.
        let hiTarget = (Double(prior1) + 1) / (Double(prior1) + 2)
        let loTarget = 1 / (Double(prior0) + 2)

        // Initial guess: A = 0, B = log(prior0 / prior1). With A = 0
        // the model collapses to a constant, and B picks the constant
        // that matches the empirical prior.
        var a: Double = 0
        var b: Double = log(Double(prior0) / Double(prior1))

        // Targets per sample.
        let targets: [Double] = samples.map { $0.isAd ? hiTarget : loTarget }

        // Initial loss.
        var fval: Double = 0
        for (i, s) in samples.enumerated() {
            let fApB = s.raw * a + b
            if fApB >= 0 {
                fval += targets[i] * fApB + log(1 + exp(-fApB))
            } else {
                fval += (targets[i] - 1) * fApB + log(1 + exp(fApB))
            }
        }

        for _ in 0..<maxIterations {
            // Compute gradient and Hessian.
            var h11 = sigma
            var h22 = sigma
            var h21: Double = 0
            var g1: Double = 0
            var g2: Double = 0

            for (i, s) in samples.enumerated() {
                let fApB = s.raw * a + b
                let p: Double
                let q: Double
                if fApB >= 0 {
                    let e = exp(-fApB)
                    p = e / (1 + e)
                    q = 1 / (1 + e)
                } else {
                    let e = exp(fApB)
                    p = 1 / (1 + e)
                    q = e / (1 + e)
                }
                let d2 = p * q
                h11 += s.raw * s.raw * d2
                h22 += d2
                h21 += s.raw * d2
                let d1 = targets[i] - p
                g1 += s.raw * d1
                g2 += d1
            }

            // Gradient norm convergence test.
            if abs(g1) < epsilon && abs(g2) < epsilon {
                break
            }

            // Newton direction.
            let det = h11 * h22 - h21 * h21
            guard det != 0 else { break }
            let dA = -(h22 * g1 - h21 * g2) / det
            let dB = -(-h21 * g1 + h11 * g2) / det
            let gd = g1 * dA + g2 * dB

            // Backtracking line search.
            var stepSize: Double = 1
            while stepSize >= minStep {
                let newA = a + stepSize * dA
                let newB = b + stepSize * dB
                var newF: Double = 0
                for (i, s) in samples.enumerated() {
                    let fApB = s.raw * newA + newB
                    if fApB >= 0 {
                        newF += targets[i] * fApB + log(1 + exp(-fApB))
                    } else {
                        newF += (targets[i] - 1) * fApB + log(1 + exp(fApB))
                    }
                }
                if newF < fval + 0.0001 * stepSize * gd {
                    a = newA
                    b = newB
                    fval = newF
                    break
                } else {
                    stepSize /= 2
                }
            }
            if stepSize < minStep {
                // No further improvement available.
                break
            }
        }

        return PlattCoefficients(a: a, b: b)
    }
}

// MARK: - AUC-PR helper

/// Standalone area-under-the-precision-recall-curve helper used by the
/// validation harness. Production code does not call this — it lives
/// here next to `ClassifierCalibration` because the only consumer is
/// the calibration test suite, and tucking it under a Tests/Support
/// directory would scatter the calibration story across the codebase.
///
/// Computes AUC-PR via trapezoidal integration of the precision-recall
/// curve traced by sweeping the decision threshold from 1.0 down to
/// 0.0. For ties we follow the standard "sort by score descending,
/// process all positives at a tied score before declaring the next
/// recall step" convention.
enum CalibrationAUCPR {

    /// One labeled scored sample. `score` should be in [0, 1] (raw or
    /// calibrated), `isAd` is the binary label.
    struct ScoredLabel: Sendable, Equatable {
        let score: Double
        let isAd: Bool
    }

    /// Compute AUC-PR. Returns 0 when there are no positive labels in
    /// the corpus (precision is undefined for an empty positive class).
    /// Returns 1 when scores perfectly separate positives from
    /// negatives.
    static func compute(_ samples: [ScoredLabel]) -> Double {
        let positives = samples.reduce(0) { $0 + ($1.isAd ? 1 : 0) }
        guard positives > 0 else { return 0 }

        // Sort by score descending; stable on ties to keep the curve
        // deterministic across runs.
        let sorted = samples.enumerated().sorted { lhs, rhs in
            if lhs.element.score != rhs.element.score {
                return lhs.element.score > rhs.element.score
            }
            return lhs.offset < rhs.offset
        }.map(\.element)

        // Trapezoidal integration over recall steps.
        var tp = 0
        var fp = 0
        var prevRecall: Double = 0
        var prevPrecision: Double = 1
        var auc: Double = 0
        var i = 0
        while i < sorted.count {
            // Process all samples at this score level together.
            let s = sorted[i].score
            var j = i
            while j < sorted.count && sorted[j].score == s {
                if sorted[j].isAd { tp += 1 } else { fp += 1 }
                j += 1
            }
            let recall = Double(tp) / Double(positives)
            let denom = Double(tp + fp)
            let precision = denom > 0 ? Double(tp) / denom : 1
            // Trapezoid between the previous (recall, precision) and
            // the current one.
            auc += (recall - prevRecall) * (prevPrecision + precision) / 2
            prevRecall = recall
            prevPrecision = precision
            i = j
        }
        return auc
    }
}
