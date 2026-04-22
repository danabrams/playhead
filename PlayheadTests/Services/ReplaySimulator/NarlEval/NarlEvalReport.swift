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
        out += "| Episode | Podcast | Config | GT | Pred | F1@0.3 | F1@0.5 | F1@0.7 | Sec-F1 |\n"
        out += "|---|---|---|---|---|---|---|---|---|\n"
        for e in included {
            let f13 = e.windowMetrics.first(where: { abs($0.threshold - 0.3) < 1e-6 })?.f1 ?? 0
            let f15 = e.windowMetrics.first(where: { abs($0.threshold - 0.5) < 1e-6 })?.f1 ?? 0
            let f17 = e.windowMetrics.first(where: { abs($0.threshold - 0.7) < 1e-6 })?.f1 ?? 0
            out += "| \(e.episodeId) | \(e.podcastId) | \(e.config) | \(e.groundTruthWindowCount) | \(e.predictedWindowCount) "
            out += "| \(fmt(f13)) | \(fmt(f15)) | \(fmt(f17)) | \(fmt(e.secondLevel.f1)) |\n"
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
