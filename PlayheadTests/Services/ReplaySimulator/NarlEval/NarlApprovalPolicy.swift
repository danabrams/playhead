// NarlApprovalPolicy.swift
// playhead-narl.3: Per-episode gate-flip recommendation engine.
//
// Pure helper. Input: the harness's NarlEvalReport plus a policy. Output: a
// per-episode recommendation (recommendFlip / holdOff / insufficientData)
// telling Dan whether `counterfactualGateOpen` appears safe to flip on that
// episode.
//
// Scope: recommend-only. This bead does not mutate any production state. See
// docs/plans/2026-04-22-narl-approval-scope.md for the scope decision and
// rationale. If Phase 2 adds execution, an applier lives elsewhere — this
// helper stays pure.
//
// Policy rule (design §D, bead spec):
//   recommend flip for episode E if:
//     allEnabled.recall(E)    >= default.recall(E)
//     AND allEnabled.precision(E) >= default.precision(E) - ε   (ε default 0.02)
//     AND episode is fully shadow-covered
//
// The helper lives in the test target because its input types (NarlEvalReport
// and friends) are defined in the test target. It does not touch production.

import Foundation

// MARK: - Policy parameters

/// Configuration for the approval policy. All parameters carry defaults so a
/// caller can pass `.default` and still get the documented behavior.
struct NarlApprovalPolicy: Sendable, Codable, Equatable {
    /// The precision-delta tolerance: flip is acceptable if allEnabled.precision
    /// is at most ε below default.precision. Default 0.02 (bead spec).
    let precisionEpsilon: Double

    /// Whether a flip requires the episode to be fully shadow-covered. When
    /// true and the episode is not shadow-covered, the recommender emits
    /// `insufficientData`. Default true — without shadow coverage, fmScheduling
    /// evidence is missing (narl.2 not yet landed), so any fmScheduling-sensitive
    /// recommendation is untrustworthy.
    let requireShadowCoverage: Bool

    /// Which IoU threshold's window-level metrics drive the rule. The harness
    /// emits τ ∈ {0.3, 0.5, 0.7}. The bead spec doesn't pin a single τ; we
    /// default to 0.5 (standard PASCAL VOC) which balances "did the window
    /// exist" (low τ) with "were boundaries clean" (high τ).
    let iouThreshold: Double

    static let `default` = NarlApprovalPolicy(
        precisionEpsilon: 0.02,
        requireShadowCoverage: true,
        iouThreshold: 0.5
    )
}

// MARK: - Recommendation types

/// The three output states spec'd by the bead.
enum NarlRecommendationDecision: String, Sendable, Codable, Equatable {
    case recommendFlip
    case holdOff
    case insufficientData
}

/// A single-episode recommendation. Cross-links the specific metric values
/// that drove the decision (bead acceptance criterion: "recommendations
/// reference the specific metric values that drove each decision").
struct NarlRecommendation: Sendable, Codable, Equatable {
    let episodeId: String
    let podcastId: String
    let show: String
    let decision: NarlRecommendationDecision
    /// Human-readable one-line reasoning. Includes the specific numeric values
    /// that fed the decision so the report is auditable without rerunning.
    let reasoning: String

    // The metric values that drove the decision. Nil for insufficientData
    // cases where the metric couldn't be computed (e.g., excluded episodes).
    let defaultRecall: Double?
    let allEnabledRecall: Double?
    let defaultPrecision: Double?
    let allEnabledPrecision: Double?
    let hasShadowCoverage: Bool
    let thresholdTau: Double

    /// Whether the recall check passed. Nil when metrics unavailable.
    let recallCheckPassed: Bool?
    /// Whether the precision (− ε) check passed. Nil when metrics unavailable.
    let precisionCheckPassed: Bool?
}

// MARK: - Evaluator

enum NarlApprovalPolicyEvaluator {

    /// Evaluate the policy against an eval report. Produces one recommendation
    /// per unique episodeId in the report.
    ///
    /// Contract:
    ///   - Excluded episodes → `insufficientData` with reason "wholeAssetVeto…".
    ///   - Episodes missing either `.default` or `.allEnabled` entries →
    ///     `insufficientData` ("missing <config> metrics"). This catches the
    ///     case where the harness's config loop failed for one side without
    ///     tripping the build.
    ///   - `policy.requireShadowCoverage` && !hasShadowCoverage →
    ///     `insufficientData` ("pending narl.2 shadow coverage"). This is the
    ///     graceful fallback for missing `fmSchedulingEnabled` data required
    ///     by the bead spec.
    ///   - Otherwise apply the policy rule against the (precision, recall)
    ///     pair at `policy.iouThreshold`. If metrics for that threshold are
    ///     absent → `insufficientData`.
    static func evaluate(
        report: NarlEvalReport,
        policy: NarlApprovalPolicy = .default
    ) -> [NarlRecommendation] {
        // Group entries by episodeId. Each episode can have up to two entries
        // (default + allEnabled); excluded episodes still have two (both with
        // isExcluded=true).
        var byEpisode: [String: [NarlReportEpisodeEntry]] = [:]
        for e in report.episodes {
            byEpisode[e.episodeId, default: []].append(e)
        }

        // Sort episodeIds for deterministic output order.
        let episodeIds = byEpisode.keys.sorted()
        return episodeIds.map { episodeId in
            let entries = byEpisode[episodeId] ?? []
            return evaluateSingle(entries: entries, episodeId: episodeId, policy: policy)
        }
    }

    /// Evaluate one episode's entries. Visible for testing.
    static func evaluateSingle(
        entries: [NarlReportEpisodeEntry],
        episodeId: String,
        policy: NarlApprovalPolicy
    ) -> NarlRecommendation {
        // Find a representative entry for podcastId/show labels. Any entry
        // carries these; pick deterministically.
        let sample = entries.sorted(by: { $0.config < $1.config }).first

        // Excluded episodes — any entry with isExcluded=true disqualifies.
        if let firstExcluded = entries.first(where: { $0.isExcluded }) {
            return NarlRecommendation(
                episodeId: episodeId,
                podcastId: firstExcluded.podcastId,
                show: firstExcluded.show,
                decision: .insufficientData,
                reasoning: "excluded: \(firstExcluded.exclusionReason ?? "whole-asset veto")",
                defaultRecall: nil, allEnabledRecall: nil,
                defaultPrecision: nil, allEnabledPrecision: nil,
                hasShadowCoverage: firstExcluded.hasShadowCoverage,
                thresholdTau: policy.iouThreshold,
                recallCheckPassed: nil,
                precisionCheckPassed: nil
            )
        }

        let defaultEntry = entries.first(where: { $0.config == "default" })
        let allEnabledEntry = entries.first(where: { $0.config == "allEnabled" })

        // Missing one or both configs.
        guard let def = defaultEntry, let all = allEnabledEntry else {
            let missing: String
            switch (defaultEntry, allEnabledEntry) {
            case (nil, nil): missing = "both configs"
            case (nil, _): missing = ".default"
            case (_, nil): missing = ".allEnabled"
            default: missing = "unknown"
            }
            return NarlRecommendation(
                episodeId: episodeId,
                podcastId: sample?.podcastId ?? "",
                show: sample?.show ?? "",
                decision: .insufficientData,
                reasoning: "missing \(missing) metrics",
                defaultRecall: nil, allEnabledRecall: nil,
                defaultPrecision: nil, allEnabledPrecision: nil,
                hasShadowCoverage: sample?.hasShadowCoverage ?? false,
                thresholdTau: policy.iouThreshold,
                recallCheckPassed: nil,
                precisionCheckPassed: nil
            )
        }

        // Shadow coverage requirement (graceful fallback when narl.2 data
        // isn't there yet — spec: "emits insufficientData rather than
        // failing").
        let hasShadow = def.hasShadowCoverage && all.hasShadowCoverage
        if policy.requireShadowCoverage && !hasShadow {
            return NarlRecommendation(
                episodeId: episodeId,
                podcastId: def.podcastId,
                show: def.show,
                decision: .insufficientData,
                reasoning: "pending narl.2 shadow coverage; fmSchedulingEnabled evidence unavailable",
                defaultRecall: nil, allEnabledRecall: nil,
                defaultPrecision: nil, allEnabledPrecision: nil,
                hasShadowCoverage: false,
                thresholdTau: policy.iouThreshold,
                recallCheckPassed: nil,
                precisionCheckPassed: nil
            )
        }

        // Fetch the metric row for the chosen τ. Missing τ (shouldn't happen
        // with a well-formed harness report, but be defensive) → insufficientData.
        guard let defMetrics = entryMetric(def, tau: policy.iouThreshold),
              let allMetrics = entryMetric(all, tau: policy.iouThreshold)
        else {
            return NarlRecommendation(
                episodeId: episodeId,
                podcastId: def.podcastId,
                show: def.show,
                decision: .insufficientData,
                reasoning: "harness did not emit τ=\(policy.iouThreshold) window metrics",
                defaultRecall: nil, allEnabledRecall: nil,
                defaultPrecision: nil, allEnabledPrecision: nil,
                hasShadowCoverage: hasShadow,
                thresholdTau: policy.iouThreshold,
                recallCheckPassed: nil,
                precisionCheckPassed: nil
            )
        }

        let recallOK = allMetrics.recall >= defMetrics.recall
        let precisionOK = allMetrics.precision >= defMetrics.precision - policy.precisionEpsilon
        let decision: NarlRecommendationDecision = (recallOK && precisionOK) ? .recommendFlip : .holdOff

        let reasoning = formatReasoning(
            decision: decision,
            defRecall: defMetrics.recall,
            allRecall: allMetrics.recall,
            defPrecision: defMetrics.precision,
            allPrecision: allMetrics.precision,
            epsilon: policy.precisionEpsilon,
            tau: policy.iouThreshold,
            recallOK: recallOK,
            precisionOK: precisionOK
        )

        return NarlRecommendation(
            episodeId: episodeId,
            podcastId: def.podcastId,
            show: def.show,
            decision: decision,
            reasoning: reasoning,
            defaultRecall: defMetrics.recall,
            allEnabledRecall: allMetrics.recall,
            defaultPrecision: defMetrics.precision,
            allEnabledPrecision: allMetrics.precision,
            hasShadowCoverage: hasShadow,
            thresholdTau: policy.iouThreshold,
            recallCheckPassed: recallOK,
            precisionCheckPassed: precisionOK
        )
    }

    // MARK: - Helpers

    private static func entryMetric(
        _ entry: NarlReportEpisodeEntry,
        tau: Double
    ) -> NarlWindowMetricsAtThreshold? {
        return entry.windowMetrics.first(where: { abs($0.threshold - tau) < 1e-6 })
    }

    private static func formatReasoning(
        decision: NarlRecommendationDecision,
        defRecall: Double,
        allRecall: Double,
        defPrecision: Double,
        allPrecision: Double,
        epsilon: Double,
        tau: Double,
        recallOK: Bool,
        precisionOK: Bool
    ) -> String {
        let prefix: String
        switch decision {
        case .recommendFlip: prefix = "flip ok"
        case .holdOff:
            var failures: [String] = []
            if !recallOK { failures.append("recall regressed") }
            if !precisionOK { failures.append("precision regressed beyond ε") }
            prefix = "hold off: " + failures.joined(separator: "; ")
        case .insufficientData: prefix = "insufficient data"
        }
        return String(
            format: "%@ @ τ=%.2f: recall %.3f→%.3f, precision %.3f→%.3f (ε=%.3f)",
            prefix, tau,
            defRecall, allRecall,
            defPrecision, allPrecision,
            epsilon
        )
    }
}
