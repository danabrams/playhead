// NarlApprovalReport.swift
// playhead-narl.3: Writes `.eval-out/narl/<runId>/recommendations.{json,md}`
// alongside the harness report. JSON is schema-versioned; markdown is a
// table of (episode, show, decision, reasoning).

import Foundation

// MARK: - Schema

enum NarlApprovalReportSchema {
    /// Bump on breaking shape changes. Keep in sync with any consumers.
    static let version: Int = 1
}

// MARK: - Report

/// The JSON artifact. Carries the policy parameters used so a future reader
/// can reproduce the decision without guessing defaults.
struct NarlApprovalReport: Sendable, Codable {
    let schemaVersion: Int
    let generatedAt: Date
    /// The harness runId this report was derived from — cross-link for audit.
    let sourceRunId: String
    let policy: NarlApprovalPolicy
    let recommendations: [NarlRecommendation]
    /// Flat counts for quick at-a-glance scanning in dashboards.
    let summary: NarlApprovalSummary
    let notes: [String]
}

struct NarlApprovalSummary: Sendable, Codable, Equatable {
    let recommendFlip: Int
    let holdOff: Int
    let insufficientData: Int

    static func compute(from recs: [NarlRecommendation]) -> NarlApprovalSummary {
        NarlApprovalSummary(
            recommendFlip: recs.filter { $0.decision == .recommendFlip }.count,
            holdOff: recs.filter { $0.decision == .holdOff }.count,
            insufficientData: recs.filter { $0.decision == .insufficientData }.count
        )
    }
}

// MARK: - Renderer

enum NarlApprovalRenderer {

    static func renderMarkdown(_ report: NarlApprovalReport) -> String {
        var out = ""
        out += "# narl approval recommendations — source run \(report.sourceRunId)\n\n"
        out += "Generated: \(isoFormatter().string(from: report.generatedAt))\n"
        out += "Schema: v\(report.schemaVersion)\n\n"

        out += "## Policy\n\n"
        out += "- precisionEpsilon: \(fmt(report.policy.precisionEpsilon))\n"
        out += "- requireShadowCoverage: \(report.policy.requireShadowCoverage)\n"
        out += "- iouThreshold: \(fmt(report.policy.iouThreshold))\n\n"

        out += "## Summary\n\n"
        out += "- recommendFlip: \(report.summary.recommendFlip)\n"
        out += "- holdOff: \(report.summary.holdOff)\n"
        out += "- insufficientData: \(report.summary.insufficientData)\n\n"

        if !report.notes.isEmpty {
            out += "## Notes\n\n"
            for n in report.notes { out += "- \(n)\n" }
            out += "\n"
        }

        out += "## Per-episode decisions\n\n"
        out += "| Episode | Show | Decision | Reasoning |\n"
        out += "|---|---|---|---|\n"
        for r in report.recommendations {
            out += "| \(r.episodeId) | \(r.show) | \(r.decision.rawValue) | \(escapeTableCell(r.reasoning)) |\n"
        }
        out += "\n"
        return out
    }

    private static func fmt(_ v: Double) -> String {
        if v.isNaN || v.isInfinite { return "-" }
        return String(format: "%.3f", v)
    }

    private static func escapeTableCell(_ s: String) -> String {
        // Pipes would break the table layout. Replace with a unicode variant
        // that renders identically in most fonts.
        s.replacingOccurrences(of: "|", with: "\u{FF5C}")
    }

    /// Fresh ISO8601 formatter per call — ISO8601DateFormatter is not Sendable.
    private static func isoFormatter() -> ISO8601DateFormatter {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }
}

// MARK: - Writer

enum NarlApprovalWriter {

    /// Write `recommendations.json` and `recommendations.md` into an already-
    /// existing directory (typically the sibling of a harness run's report
    /// directory). Returns the two URLs written.
    @discardableResult
    static func write(
        report: NarlApprovalReport,
        to directory: URL
    ) throws -> (json: URL, markdown: URL) {
        try FileManager.default.createDirectory(
            at: directory, withIntermediateDirectories: true
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        encoder.dateEncodingStrategy = .iso8601
        let jsonData = try encoder.encode(report)
        let jsonURL = directory.appendingPathComponent("recommendations.json")
        try jsonData.write(to: jsonURL)

        let mdURL = directory.appendingPathComponent("recommendations.md")
        let markdown = NarlApprovalRenderer.renderMarkdown(report)
        try markdown.data(using: .utf8)!.write(to: mdURL)

        return (jsonURL, mdURL)
    }
}
