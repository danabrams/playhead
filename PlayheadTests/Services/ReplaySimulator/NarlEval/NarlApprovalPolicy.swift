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

    /// Which IoU thresholds' window-level metrics drive the rule. AND
    /// semantics: a `recommendFlip` requires every listed τ to pass the
    /// (recall, precision − ε) gate. The default `[0.3, 0.5, 0.7]` mirrors
    /// the harness §A.5 emission so a boundary-precision regression visible
    /// only at τ=0.7 can't slip through a single-τ recommendation.
    let iouThresholds: [Double]

    /// Convenience accessor returning the *first* (primary) τ from
    /// `iouThresholds` — used by dashboards and audit columns that need to
    /// pin one number. **The evaluator does NOT trust this value when
    /// deciding flip / hold-off**: it walks the full `iouThresholds`
    /// array (AND across all τ). The reasoning string emitted on every
    /// recommendation names every τ that drove the decision, while the
    /// numeric `defaultRecall`/`allEnabledRecall`/`thresholdTau` audit
    /// columns reflect *only* this primary τ. Renamed from the bare
    /// `iouThreshold` to make the multi-τ caveat impossible to miss; the
    /// `iouThreshold` legacy Codable key still decodes for backwards
    /// compatibility (see `init(from:)`).
    var primaryIouThreshold: Double {
        iouThresholds.first ?? 0.5
    }

    /// Deprecated single-τ alias retained for backwards source
    /// compatibility with anyone reading `policy.iouThreshold` outside
    /// the eval module. Forwards to `primaryIouThreshold`.
    @available(*, deprecated, renamed: "primaryIouThreshold",
               message: "Renamed to make the multi-τ caveat explicit. The evaluator walks the full `iouThresholds` array; this accessor returns only the first τ for audit-column display.")
    var iouThreshold: Double { primaryIouThreshold }

    static let `default` = NarlApprovalPolicy(
        precisionEpsilon: 0.02,
        requireShadowCoverage: true,
        iouThresholds: [0.3, 0.5, 0.7]
    )

    init(
        precisionEpsilon: Double,
        requireShadowCoverage: Bool,
        iouThresholds: [Double]
    ) {
        precondition(!iouThresholds.isEmpty, "iouThresholds must have at least one τ")
        self.precisionEpsilon = precisionEpsilon
        self.requireShadowCoverage = requireShadowCoverage
        self.iouThresholds = iouThresholds
    }

    /// Single-τ convenience initializer. Equivalent to passing
    /// `iouThresholds: [iouThreshold]`; kept so existing callers (and
    /// anyone preferring the one-τ story for a narrow evaluation) don't
    /// need to allocate a one-element array.
    init(
        precisionEpsilon: Double,
        requireShadowCoverage: Bool,
        iouThreshold: Double
    ) {
        self.init(
            precisionEpsilon: precisionEpsilon,
            requireShadowCoverage: requireShadowCoverage,
            iouThresholds: [iouThreshold]
        )
    }

    // MARK: - Codable (backwards-compat)

    private enum CodingKeys: String, CodingKey {
        case precisionEpsilon
        case requireShadowCoverage
        case iouThresholds
        case iouThreshold  // legacy single-τ field
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.precisionEpsilon = try c.decode(Double.self, forKey: .precisionEpsilon)
        self.requireShadowCoverage = try c.decode(Bool.self, forKey: .requireShadowCoverage)
        // Prefer the multi-τ array; fall back to the legacy single-τ
        // scalar so previously-persisted `recommendations.json` artifacts
        // still decode cleanly.
        if let taus = try c.decodeIfPresent([Double].self, forKey: .iouThresholds) {
            precondition(!taus.isEmpty, "iouThresholds must have at least one τ")
            self.iouThresholds = taus
        } else if let tau = try c.decodeIfPresent(Double.self, forKey: .iouThreshold) {
            self.iouThresholds = [tau]
        } else {
            throw DecodingError.keyNotFound(
                CodingKeys.iouThresholds,
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "missing both iouThresholds and legacy iouThreshold"
                )
            )
        }
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(precisionEpsilon, forKey: .precisionEpsilon)
        try c.encode(requireShadowCoverage, forKey: .requireShadowCoverage)
        try c.encode(iouThresholds, forKey: .iouThresholds)
    }
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
    ///     pairs at every `policy.iouThresholds` τ (AND across all). If
    ///     metrics for any τ are absent → `insufficientData`. Audit
    ///     columns on the recommendation reflect `primaryIouThreshold`.
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
                thresholdTau: policy.primaryIouThreshold,
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
                thresholdTau: policy.primaryIouThreshold,
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
                thresholdTau: policy.primaryIouThreshold,
                recallCheckPassed: nil,
                precisionCheckPassed: nil
            )
        }

        // Multi-τ AND: fetch metrics for every listed threshold; a
        // missing τ row renders the whole evaluation insufficient (be
        // defensive against a malformed harness report).
        var perTau: [(tau: Double, def: NarlWindowMetricsAtThreshold, all: NarlWindowMetricsAtThreshold)] = []
        for tau in policy.iouThresholds {
            guard let d = entryMetric(def, tau: tau),
                  let a = entryMetric(all, tau: tau) else {
                return NarlRecommendation(
                    episodeId: episodeId,
                    podcastId: def.podcastId,
                    show: def.show,
                    decision: .insufficientData,
                    reasoning: "harness did not emit τ=\(tau) window metrics",
                    defaultRecall: nil, allEnabledRecall: nil,
                    defaultPrecision: nil, allEnabledPrecision: nil,
                    hasShadowCoverage: hasShadow,
                    thresholdTau: tau,
                    recallCheckPassed: nil,
                    precisionCheckPassed: nil
                )
            }
            perTau.append((tau, d, a))
        }

        // Per-τ checks. AND across all thresholds: a single τ failure
        // drops the recommendation to holdOff. Collect failures so the
        // reasoning string can name which τ(s) blocked the flip.
        var failures: [(tau: Double, recallOK: Bool, precisionOK: Bool)] = []
        var allRecallOK = true
        var allPrecisionOK = true
        for row in perTau {
            let recallOK = row.all.recall >= row.def.recall
            let precisionOK = row.all.precision >= row.def.precision - policy.precisionEpsilon
            if !recallOK { allRecallOK = false }
            if !precisionOK { allPrecisionOK = false }
            if !(recallOK && precisionOK) {
                failures.append((row.tau, recallOK, precisionOK))
            }
        }
        let decision: NarlRecommendationDecision = failures.isEmpty ? .recommendFlip : .holdOff

        // Use the primary τ (first in the list) to populate the numeric
        // columns on the recommendation — this keeps the existing single-τ
        // reporting surface stable; the full multi-τ story lives in the
        // reasoning string.
        let primary = perTau[0]
        let reasoning = formatMultiTauReasoning(
            decision: decision,
            perTau: perTau,
            failures: failures,
            epsilon: policy.precisionEpsilon
        )

        return NarlRecommendation(
            episodeId: episodeId,
            podcastId: def.podcastId,
            show: def.show,
            decision: decision,
            reasoning: reasoning,
            defaultRecall: primary.def.recall,
            allEnabledRecall: primary.all.recall,
            defaultPrecision: primary.def.precision,
            allEnabledPrecision: primary.all.precision,
            hasShadowCoverage: hasShadow,
            thresholdTau: primary.tau,
            recallCheckPassed: allRecallOK,
            precisionCheckPassed: allPrecisionOK
        )
    }

    // MARK: - Helpers

    private static func entryMetric(
        _ entry: NarlReportEpisodeEntry,
        tau: Double
    ) -> NarlWindowMetricsAtThreshold? {
        return entry.windowMetrics.first(where: { abs($0.threshold - tau) < 1e-6 })
    }

    /// Compose the reasoning string for a multi-τ evaluation. When the
    /// decision is `.holdOff`, the string names which τ(s) failed and how
    /// (recall vs precision) so a mixed-result episode is auditable without
    /// cross-referencing the numeric columns.
    private static func formatMultiTauReasoning(
        decision: NarlRecommendationDecision,
        perTau: [(tau: Double, def: NarlWindowMetricsAtThreshold, all: NarlWindowMetricsAtThreshold)],
        failures: [(tau: Double, recallOK: Bool, precisionOK: Bool)],
        epsilon: Double
    ) -> String {
        let prefix: String
        switch decision {
        case .recommendFlip:
            prefix = "flip ok"
        case .holdOff:
            // Name each failed τ with the axis that tripped it. Keeps the
            // reasoning string scannable for multi-τ AND policies.
            let parts = failures.map { f -> String in
                var axes: [String] = []
                if !f.recallOK { axes.append("recall regressed") }
                if !f.precisionOK { axes.append("precision regressed beyond ε") }
                let axisStr = axes.isEmpty ? "unknown failure" : axes.joined(separator: ", ")
                return String(format: "τ=%.2f %@", f.tau, axisStr)
            }
            prefix = "hold off: " + parts.joined(separator: "; ")
        case .insufficientData:
            prefix = "insufficient data"
        }
        // Append per-τ numeric detail so readers don't have to read two
        // fields to reconstruct the decision.
        let detail = perTau.map { row in
            String(
                format: "τ=%.2f: recall %.3f→%.3f, precision %.3f→%.3f",
                row.tau,
                row.def.recall, row.all.recall,
                row.def.precision, row.all.precision
            )
        }.joined(separator: " | ")
        return String(format: "%@ (ε=%.3f) — %@", prefix, epsilon, detail)
    }
}
