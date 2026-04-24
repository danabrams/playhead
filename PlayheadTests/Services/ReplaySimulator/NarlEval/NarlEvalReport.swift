// NarlEvalReport.swift
// playhead-narl.1: Report schema and rendering for the counterfactual eval harness.
//
// Writes:
//   .eval-out/narl/<timestamp>/report.json   — versioned schema, machine-readable
//   .eval-out/narl/<timestamp>/report.md     — tables per (show × config × metric)
//   .eval-out/narl/trend.jsonl               — append one row per (show, config, metric)

import Foundation

// MARK: - Report schema

/// Versioned schema. Bump on breaking shape changes.
enum NarlEvalReportSchema {
    static let version: Int = 1
}

/// A (show, config) rollup with all metric families.
struct NarlReportRollup: Sendable, Codable {
    let show: String
    let config: String  // "default" | "allEnabled"
    let episodeCount: Int
    let excludedEpisodeCount: Int
    let windowMetrics: [NarlWindowMetricsAtThreshold]
    let secondLevel: NarlSecondLevelMetrics
    /// Diagnostic totals across episodes.
    let totalLexicalInjectionAdds: Int
    let totalPriorShiftAdds: Int
    let totalEpisodesWithShadowCoverage: Int
    /// gtt9.6: mean-per-episode coverage metrics (ratios are averaged,
    /// FN-second counts are summed across episodes).
    let coverageMetrics: NarlCoverageMetrics
    /// gtt9.6: episodes in this rollup whose `pipelineCoverageFailureAsset`
    /// flag fired.
    let pipelineCoverageFailureAssetCount: Int
}

/// gtt9.7 C1: per-episode counts emitted by `CorrectionNormalizer`, persisted
/// alongside the metric fields so report consumers can see how many raw rows
/// were bucketed where. Previously these counts only surfaced as stdout log
/// lines under `narl.normalizer:`; placing them in the typed report makes
/// before/after deltas (raw → span/whole-asset/boundary/unknown) queryable
/// from `.eval-out/narl/<ts>/report.json` rather than test output.
///
/// Field semantics (see `CorrectionNormalizer.swift`):
///   - `rawCount`            — count of FrozenCorrection rows fed into normalize.
///   - `spanFNCount`         — span-level FN corrections after merge.
///   - `spanFPCount`         — span-level FP corrections after merge.
///   - `wholeAssetVetoCount` — whole-asset vetoes after (assetId, kind) dedup.
///   - `wholeAssetEndorseCount` — whole-asset endorsements after dedup.
///   - `unknownCount`        — rows the normalizer couldn't place (ordinal
///                             exactSpans, unrecognized source+type combos,
///                             malformed scope prefixes). Excluded from
///                             span metrics. Does NOT include Layer B rows.
///   - `boundaryRefinementCount` — rows with correctionType in
///                             {startTooEarly/Late, endTooEarly/Late}.
///   - `layerBCount`         — production-valid show-level scopes
///                             (`sponsorOnShow`/`phraseOnShow`/
///                             `campaignOnShow`/`domainOwnershipOnShow`/
///                             `jingleOnShow`) that the harness does not
///                             yet evaluate against. Tracked separately
///                             from `unknownCount` so an operator can
///                             distinguish "5 valid corrections we don't
///                             yet score" from "5 corrections we failed
///                             to parse". See review S1 (2026-04-23).
struct NarlNormalizerCounts: Sendable, Codable, Equatable {
    let rawCount: Int
    let spanFNCount: Int
    let spanFPCount: Int
    let wholeAssetVetoCount: Int
    let wholeAssetEndorseCount: Int
    let unknownCount: Int
    let boundaryRefinementCount: Int
    let layerBCount: Int

    static let zero = NarlNormalizerCounts(
        rawCount: 0,
        spanFNCount: 0,
        spanFPCount: 0,
        wholeAssetVetoCount: 0,
        wholeAssetEndorseCount: 0,
        unknownCount: 0,
        boundaryRefinementCount: 0,
        layerBCount: 0
    )

    /// Build from a `NormalizedCorrections` result + the raw-row count used
    /// as input. Centralizes the veto/endorse split so callers don't have to
    /// hand-filter `wholeAssetCorrections` at every site.
    init(from normalized: NormalizedCorrections, rawCount: Int) {
        self.rawCount = rawCount
        self.spanFNCount = normalized.spanFN.count
        self.spanFPCount = normalized.spanFP.count
        self.wholeAssetVetoCount = normalized.wholeAssetCorrections
            .filter { $0.kind == .veto }.count
        self.wholeAssetEndorseCount = normalized.wholeAssetCorrections
            .filter { $0.kind == .endorse }.count
        self.unknownCount = normalized.unknownCount
        self.boundaryRefinementCount = normalized.boundaryRefinementCount
        self.layerBCount = normalized.layerBCount
    }

    init(
        rawCount: Int,
        spanFNCount: Int,
        spanFPCount: Int,
        wholeAssetVetoCount: Int,
        wholeAssetEndorseCount: Int,
        unknownCount: Int,
        boundaryRefinementCount: Int,
        layerBCount: Int
    ) {
        self.rawCount = rawCount
        self.spanFNCount = spanFNCount
        self.spanFPCount = spanFPCount
        self.wholeAssetVetoCount = wholeAssetVetoCount
        self.wholeAssetEndorseCount = wholeAssetEndorseCount
        self.unknownCount = unknownCount
        self.boundaryRefinementCount = boundaryRefinementCount
        self.layerBCount = layerBCount
    }

    /// Codable with a default-fallback on `layerBCount` so pre-S1 report
    /// artifacts (gtt9.7 initial land) still decode.
    enum CodingKeys: String, CodingKey {
        case rawCount, spanFNCount, spanFPCount
        case wholeAssetVetoCount, wholeAssetEndorseCount
        case unknownCount, boundaryRefinementCount, layerBCount
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.rawCount = try c.decode(Int.self, forKey: .rawCount)
        self.spanFNCount = try c.decode(Int.self, forKey: .spanFNCount)
        self.spanFPCount = try c.decode(Int.self, forKey: .spanFPCount)
        self.wholeAssetVetoCount = try c.decode(Int.self, forKey: .wholeAssetVetoCount)
        self.wholeAssetEndorseCount = try c.decode(Int.self, forKey: .wholeAssetEndorseCount)
        self.unknownCount = try c.decode(Int.self, forKey: .unknownCount)
        self.boundaryRefinementCount = try c.decode(Int.self, forKey: .boundaryRefinementCount)
        self.layerBCount = (try? c.decode(Int.self, forKey: .layerBCount)) ?? 0
    }
}

/// Per-episode entry in the report (one row per trace × config).
struct NarlReportEpisodeEntry: Sendable, Codable {
    let episodeId: String
    let podcastId: String
    let show: String
    let config: String
    let isExcluded: Bool
    let exclusionReason: String?
    let groundTruthWindowCount: Int
    let predictedWindowCount: Int
    let windowMetrics: [NarlWindowMetricsAtThreshold]
    let secondLevel: NarlSecondLevelMetrics
    let lexicalInjectionAdds: Int
    let priorShiftAdds: Int
    let hasShadowCoverage: Bool
    /// gtt9.6: per-episode coverage + FN-rate metrics.
    let coverageMetrics: NarlCoverageMetrics
    /// gtt9.6: per-GT-span FN decomposition. Excluded episodes carry `[]`.
    let fnDecomposition: [NarlFNDecomp]
    /// gtt9.7 C1: per-episode counts emitted by `CorrectionNormalizer`. This
    /// is observability-only — the metric fields above are still derived from
    /// the existing NarlGroundTruth pipeline; the normalizer's output is
    /// carried alongside so the delta is visible in the persisted report.
    let normalizerCounts: NarlNormalizerCounts

    init(
        episodeId: String,
        podcastId: String,
        show: String,
        config: String,
        isExcluded: Bool,
        exclusionReason: String?,
        groundTruthWindowCount: Int,
        predictedWindowCount: Int,
        windowMetrics: [NarlWindowMetricsAtThreshold],
        secondLevel: NarlSecondLevelMetrics,
        lexicalInjectionAdds: Int,
        priorShiftAdds: Int,
        hasShadowCoverage: Bool,
        coverageMetrics: NarlCoverageMetrics = .zero,
        fnDecomposition: [NarlFNDecomp] = [],
        normalizerCounts: NarlNormalizerCounts = .zero
    ) {
        self.episodeId = episodeId
        self.podcastId = podcastId
        self.show = show
        self.config = config
        self.isExcluded = isExcluded
        self.exclusionReason = exclusionReason
        self.groundTruthWindowCount = groundTruthWindowCount
        self.predictedWindowCount = predictedWindowCount
        self.windowMetrics = windowMetrics
        self.secondLevel = secondLevel
        self.lexicalInjectionAdds = lexicalInjectionAdds
        self.priorShiftAdds = priorShiftAdds
        self.hasShadowCoverage = hasShadowCoverage
        self.coverageMetrics = coverageMetrics
        self.fnDecomposition = fnDecomposition
        self.normalizerCounts = normalizerCounts
    }

    /// Codable default handling: older `report.json` artifacts written before
    /// gtt9.7 do not carry a `normalizerCounts` block. Decode gracefully by
    /// defaulting the field to `.zero`. Encode always emits the new field.
    enum CodingKeys: String, CodingKey {
        case episodeId, podcastId, show, config, isExcluded, exclusionReason
        case groundTruthWindowCount, predictedWindowCount
        case windowMetrics, secondLevel
        case lexicalInjectionAdds, priorShiftAdds, hasShadowCoverage
        case coverageMetrics, fnDecomposition
        case normalizerCounts
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.episodeId = try c.decode(String.self, forKey: .episodeId)
        self.podcastId = try c.decode(String.self, forKey: .podcastId)
        self.show = try c.decode(String.self, forKey: .show)
        self.config = try c.decode(String.self, forKey: .config)
        self.isExcluded = try c.decode(Bool.self, forKey: .isExcluded)
        self.exclusionReason = try c.decodeIfPresent(String.self, forKey: .exclusionReason)
        self.groundTruthWindowCount = try c.decode(Int.self, forKey: .groundTruthWindowCount)
        self.predictedWindowCount = try c.decode(Int.self, forKey: .predictedWindowCount)
        self.windowMetrics = try c.decode([NarlWindowMetricsAtThreshold].self, forKey: .windowMetrics)
        self.secondLevel = try c.decode(NarlSecondLevelMetrics.self, forKey: .secondLevel)
        self.lexicalInjectionAdds = try c.decode(Int.self, forKey: .lexicalInjectionAdds)
        self.priorShiftAdds = try c.decode(Int.self, forKey: .priorShiftAdds)
        self.hasShadowCoverage = try c.decode(Bool.self, forKey: .hasShadowCoverage)
        self.coverageMetrics = (try? c.decode(NarlCoverageMetrics.self, forKey: .coverageMetrics)) ?? .zero
        self.fnDecomposition = (try? c.decode([NarlFNDecomp].self, forKey: .fnDecomposition)) ?? []
        self.normalizerCounts = (try? c.decode(NarlNormalizerCounts.self, forKey: .normalizerCounts)) ?? .zero
    }
}

struct NarlEvalReport: Sendable, Codable {
    let schemaVersion: Int
    let generatedAt: Date
    let runId: String
    let iouThresholds: [Double]
    let rollups: [NarlReportRollup]
    let episodes: [NarlReportEpisodeEntry]
    let notes: [String]
}

// MARK: - Trend log row

/// One row in `.eval-out/narl/trend.jsonl`. We emit many per run (one per
/// show × config × metric). Reading via `jq` or the bead's approval engine
/// is straightforward.
struct NarlTrendRow: Sendable, Codable {
    let schemaVersion: Int
    let runId: String
    let generatedAt: Date
    let show: String
    let config: String
    let metric: String
    let thresholdTau: Double?
    let value: Double
}

// MARK: - Renderer

enum NarlEvalRenderer {

    /// Render the markdown report: one table per (show × config × metric family).
    static func renderMarkdown(_ report: NarlEvalReport) -> String {
        var out = ""
        out += "# narl counterfactual eval — run \(report.runId)\n\n"
        out += "Generated: \(isoFormatter().string(from: report.generatedAt))\n"
        out += "Schema: v\(report.schemaVersion)\n\n"

        if !report.notes.isEmpty {
            out += "## Notes\n\n"
            for note in report.notes {
                out += "- \(note)\n"
            }
            out += "\n"
        }

        out += "## Summary (rollups)\n\n"
        out += "| Show | Config | Episodes | Excluded | Win F1 @ τ=0.3 | @ 0.5 | @ 0.7 | Second-level F1 | LexInj adds | PriorShift adds | Shadow-covered |\n"
        out += "|---|---|---|---|---|---|---|---|---|---|---|\n"
        for r in report.rollups {
            let f13 = r.windowMetrics.first(where: { abs($0.threshold - 0.3) < 1e-6 })?.f1 ?? 0
            let f15 = r.windowMetrics.first(where: { abs($0.threshold - 0.5) < 1e-6 })?.f1 ?? 0
            let f17 = r.windowMetrics.first(where: { abs($0.threshold - 0.7) < 1e-6 })?.f1 ?? 0
            out += "| \(r.show) | \(r.config) | \(r.episodeCount) | \(r.excludedEpisodeCount) "
            out += "| \(fmt(f13)) | \(fmt(f15)) | \(fmt(f17)) | \(fmt(r.secondLevel.f1)) "
            out += "| \(r.totalLexicalInjectionAdds) | \(r.totalPriorShiftAdds) | \(r.totalEpisodesWithShadowCoverage) |\n"
        }
        out += "\n"

        // Split into metric table (non-excluded) and a deduped excluded list.
        // Each episode appears once per config in `report.episodes`, so without
        // deduping, an excluded episode would show up twice in the summary
        // with identical zeros (LOW-4).
        let included = report.episodes.filter { !$0.isExcluded }
        let excluded = report.episodes.filter { $0.isExcluded }

        out += "## Per-episode\n\n"
        out += "| Episode | Podcast | Config | GT | Pred | F1@0.3 | F1@0.5 | F1@0.7 | Sec-F1 | PipelineFail |\n"
        out += "|---|---|---|---|---|---|---|---|---|---|\n"
        for e in included {
            let f13 = e.windowMetrics.first(where: { abs($0.threshold - 0.3) < 1e-6 })?.f1 ?? 0
            let f15 = e.windowMetrics.first(where: { abs($0.threshold - 0.5) < 1e-6 })?.f1 ?? 0
            let f17 = e.windowMetrics.first(where: { abs($0.threshold - 0.7) < 1e-6 })?.f1 ?? 0
            let failMarker = e.coverageMetrics.pipelineCoverageFailureAsset ? "WARN pipelineCoverageFailureAsset" : ""
            out += "| \(e.episodeId) | \(e.podcastId) | \(e.config) | \(e.groundTruthWindowCount) | \(e.predictedWindowCount) "
            out += "| \(fmt(f13)) | \(fmt(f15)) | \(fmt(f17)) | \(fmt(e.secondLevel.f1)) | \(failMarker) |\n"
        }
        out += "\n"

        // gtt9.6: coverage + FN decomposition per-rollup.
        out += "## Coverage + FN decomposition\n\n"
        out += "Transcript coverage is a lower bound — counted only from "
        out += "`lexical` / `fm` / `catalog` evidence sources — until gtt9.8 "
        out += "ships the coverage contract.\n\n"
        out += "| Show | Config | ScoredCov | TranscriptCov | UnscoredFN | "
        out += "PipelineFN (s) | ClassifierFN (s) | PromotionFN (s) | PipelineFailAssets |\n"
        out += "|---|---|---|---|---|---|---|---|---|\n"
        for r in report.rollups {
            let cm = r.coverageMetrics
            out += "| \(r.show) | \(r.config) "
            out += "| \(fmt(cm.scoredCoverageRatio)) "
            out += "| \(fmt(cm.transcriptCoverageRatio)) "
            out += "| \(fmt(cm.unscoredFNRate)) "
            out += "| \(fmt(cm.pipelineCoverageFNSeconds)) "
            out += "| \(fmt(cm.classifierRecallFNSeconds)) "
            out += "| \(fmt(cm.promotionRecallFNSeconds)) "
            out += "| \(r.pipelineCoverageFailureAssetCount) |\n"
        }
        out += "\n"

        if !excluded.isEmpty {
            // Dedupe by episodeId — the exclusion reason is the same across
            // configs (ground-truth construction is config-agnostic).
            var seen = Set<String>()
            let dedupedExcluded = excluded.filter { seen.insert($0.episodeId).inserted }
            out += "## Excluded episodes\n\n"
            out += "| Episode | Podcast | Reason |\n"
            out += "|---|---|---|\n"
            for e in dedupedExcluded {
                out += "| \(e.episodeId) | \(e.podcastId) | \(e.exclusionReason ?? "yes") |\n"
            }
            out += "\n"
        }

        return out
    }

    private static func fmt(_ v: Double) -> String {
        if v.isNaN || v.isInfinite { return "-" }
        return String(format: "%.3f", v)
    }

    /// Fresh ISO8601 formatter per call — ISO8601DateFormatter is not Sendable,
    /// so we avoid a module-global static. Rendering is off the hot path.
    private static func isoFormatter() -> ISO8601DateFormatter {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }
}

// MARK: - Trend log builder

enum NarlTrendLog {
    /// Expand a set of rollups into flat trend rows, one per (show, config,
    /// metric, threshold).
    static func rows(from report: NarlEvalReport) -> [NarlTrendRow] {
        var out: [NarlTrendRow] = []
        for r in report.rollups {
            for w in r.windowMetrics {
                out.append(NarlTrendRow(
                    schemaVersion: report.schemaVersion,
                    runId: report.runId,
                    generatedAt: report.generatedAt,
                    show: r.show,
                    config: r.config,
                    metric: "window_f1",
                    thresholdTau: w.threshold,
                    value: w.f1
                ))
                out.append(NarlTrendRow(
                    schemaVersion: report.schemaVersion,
                    runId: report.runId,
                    generatedAt: report.generatedAt,
                    show: r.show,
                    config: r.config,
                    metric: "window_precision",
                    thresholdTau: w.threshold,
                    value: w.precision
                ))
                out.append(NarlTrendRow(
                    schemaVersion: report.schemaVersion,
                    runId: report.runId,
                    generatedAt: report.generatedAt,
                    show: r.show,
                    config: r.config,
                    metric: "window_recall",
                    thresholdTau: w.threshold,
                    value: w.recall
                ))
                out.append(NarlTrendRow(
                    schemaVersion: report.schemaVersion,
                    runId: report.runId,
                    generatedAt: report.generatedAt,
                    show: r.show,
                    config: r.config,
                    metric: "window_mean_matched_iou",
                    thresholdTau: w.threshold,
                    value: w.meanMatchedIoU
                ))
            }
            out.append(NarlTrendRow(
                schemaVersion: report.schemaVersion,
                runId: report.runId,
                generatedAt: report.generatedAt,
                show: r.show,
                config: r.config,
                metric: "second_f1",
                thresholdTau: nil,
                value: r.secondLevel.f1
            ))
            out.append(NarlTrendRow(
                schemaVersion: report.schemaVersion,
                runId: report.runId,
                generatedAt: report.generatedAt,
                show: r.show,
                config: r.config,
                metric: "second_precision",
                thresholdTau: nil,
                value: r.secondLevel.precision
            ))
            out.append(NarlTrendRow(
                schemaVersion: report.schemaVersion,
                runId: report.runId,
                generatedAt: report.generatedAt,
                show: r.show,
                config: r.config,
                metric: "second_recall",
                thresholdTau: nil,
                value: r.secondLevel.recall
            ))
            // gtt9.6 coverage + FN-decomposition metrics.
            let coverageRows: [(String, Double)] = [
                ("scored_coverage_ratio", r.coverageMetrics.scoredCoverageRatio),
                ("transcript_coverage_ratio", r.coverageMetrics.transcriptCoverageRatio),
                ("candidate_recall", r.coverageMetrics.candidateRecall),
                ("auto_skip_precision", r.coverageMetrics.autoSkipPrecision),
                ("auto_skip_recall", r.coverageMetrics.autoSkipRecall),
                ("segment_iou", r.coverageMetrics.segmentIoU),
                ("unscored_fn_rate", r.coverageMetrics.unscoredFNRate),
                ("pipeline_coverage_failure_count",
                 Double(r.pipelineCoverageFailureAssetCount)),
            ]
            for (name, value) in coverageRows {
                out.append(NarlTrendRow(
                    schemaVersion: report.schemaVersion,
                    runId: report.runId,
                    generatedAt: report.generatedAt,
                    show: r.show,
                    config: r.config,
                    metric: name,
                    thresholdTau: nil,
                    value: value
                ))
            }
        }
        return out
    }

    /// Serialize rows as JSONL-ready lines (no trailing newline).
    static func jsonlLines(for rows: [NarlTrendRow]) throws -> [Data] {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        return try rows.map { try encoder.encode($0) }
    }
}
