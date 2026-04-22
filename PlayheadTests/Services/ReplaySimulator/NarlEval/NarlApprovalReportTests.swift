// NarlApprovalReportTests.swift
// playhead-narl.3: Tests for the approval report writer + markdown renderer.

import Foundation
import Testing
@testable import Playhead

@Suite("NarlApprovalReport – writer")
struct NarlApprovalReportWriterTests {

    @Test("Writes both recommendations.json and recommendations.md")
    func writesBothArtifacts() throws {
        let tmp = FileManager.default.temporaryDirectory
            .appendingPathComponent("narl-approval-test-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let report = makeReport()
        let (jsonURL, mdURL) = try NarlApprovalWriter.write(report: report, to: tmp)

        #expect(FileManager.default.fileExists(atPath: jsonURL.path))
        #expect(FileManager.default.fileExists(atPath: mdURL.path))

        // JSON round-trips.
        let data = try Data(contentsOf: jsonURL)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(NarlApprovalReport.self, from: data)
        #expect(decoded.schemaVersion == NarlApprovalReportSchema.version)
        #expect(decoded.recommendations.count == report.recommendations.count)
    }

    @Test("Markdown renders one row per recommendation plus summary counts")
    func markdownRowsAndSummary() throws {
        let report = makeReport()
        let md = NarlApprovalRenderer.renderMarkdown(report)
        // Expect a summary block and a table row for each recommendation.
        #expect(md.contains("## Summary"))
        #expect(md.contains("recommendFlip: 1"))
        #expect(md.contains("holdOff: 1"))
        #expect(md.contains("insufficientData: 1"))
        #expect(md.contains("| e-flip |"))
        #expect(md.contains("| e-hold |"))
        #expect(md.contains("| e-insuf |"))
    }

    @Test("Markdown per-episode table includes numeric metric columns")
    func markdownNumericColumns() throws {
        let report = makeReport()
        let md = NarlApprovalRenderer.renderMarkdown(report)
        // Header row carries the new numeric columns.
        #expect(md.contains("| defRecall | allRecall | defPrecision | allPrecision |"))
        // Populated rows render values at 3-decimal precision.
        #expect(md.contains("0.700"))
        #expect(md.contains("0.720"))
        #expect(md.contains("0.800"))
        #expect(md.contains("0.790"))
        // Missing-data row renders an em-dash rather than "0.000", so zero
        // values and missing values are visually distinct.
        #expect(md.contains("| — | — | — | — |"))
    }

    @Test("Markdown escapes newlines in reasoning so tables don't break")
    func markdownEscapesNewlines() throws {
        let newlineRec = NarlRecommendation(
            episodeId: "e-nl",
            podcastId: "p",
            show: "S",
            decision: .holdOff,
            reasoning: "line one\nline two\r\nline three\rline four",
            defaultRecall: 0, allEnabledRecall: 0,
            defaultPrecision: 0, allEnabledPrecision: 0,
            hasShadowCoverage: true,
            thresholdTau: 0.5,
            recallCheckPassed: false,
            precisionCheckPassed: true
        )
        let report = NarlApprovalReport(
            schemaVersion: NarlApprovalReportSchema.version,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            sourceRunId: "r",
            policy: .default,
            recommendations: [newlineRec],
            summary: NarlApprovalSummary.compute(from: [newlineRec]),
            notes: []
        )
        let md = NarlApprovalRenderer.renderMarkdown(report)
        // Raw newlines must not appear inside the rendered reasoning cell;
        // all three forms (LF, CRLF, CR) collapse to `<br/>`.
        #expect(md.contains("line one<br/>line two<br/>line three<br/>line four"))
        // Sanity-check: there are no rogue newlines breaking the "| e-nl |"
        // cell across multiple rows.
        let lines = md.split(separator: "\n", omittingEmptySubsequences: false)
        let nlRows = lines.filter { $0.contains("| e-nl |") }
        #expect(nlRows.count == 1, "reasoning newlines must not spill across table rows")
    }

    @Test("requireSchema throws on mismatch and passes on match")
    func requireSchemaMismatch() throws {
        let matching = makeReport()
        // Default expected value == current schema version → no throw.
        try matching.requireSchema()
        // Hand-build a mismatched report with an older schemaVersion.
        let older = NarlApprovalReport(
            schemaVersion: NarlApprovalReportSchema.version + 1,
            generatedAt: matching.generatedAt,
            sourceRunId: matching.sourceRunId,
            policy: matching.policy,
            recommendations: matching.recommendations,
            summary: matching.summary,
            notes: matching.notes
        )
        #expect(throws: NarlApprovalReportSchemaMismatch.self) {
            try older.requireSchema()
        }
    }

    @Test("Markdown escapes pipes in reasoning so tables don't break")
    func markdownEscapesPipes() throws {
        let pipeRec = NarlRecommendation(
            episodeId: "e-pipe",
            podcastId: "p",
            show: "S",
            decision: .holdOff,
            reasoning: "has | pipe in it",
            defaultRecall: 0, allEnabledRecall: 0,
            defaultPrecision: 0, allEnabledPrecision: 0,
            hasShadowCoverage: true,
            thresholdTau: 0.5,
            recallCheckPassed: false,
            precisionCheckPassed: true
        )
        let report = NarlApprovalReport(
            schemaVersion: NarlApprovalReportSchema.version,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            sourceRunId: "r",
            policy: .default,
            recommendations: [pipeRec],
            summary: NarlApprovalSummary.compute(from: [pipeRec]),
            notes: []
        )
        let md = NarlApprovalRenderer.renderMarkdown(report)
        // The raw ASCII pipe from the reasoning should not appear in a data
        // cell — confirm the escaped unicode variant is used instead.
        #expect(md.contains("has \u{FF5C} pipe in it"))
    }
}

private func makeReport() -> NarlApprovalReport {
    let recs: [NarlRecommendation] = [
        NarlRecommendation(
            episodeId: "e-flip",
            podcastId: "p", show: "S",
            decision: .recommendFlip,
            reasoning: "flip ok",
            defaultRecall: 0.7, allEnabledRecall: 0.72,
            defaultPrecision: 0.8, allEnabledPrecision: 0.79,
            hasShadowCoverage: true,
            thresholdTau: 0.5,
            recallCheckPassed: true,
            precisionCheckPassed: true
        ),
        NarlRecommendation(
            episodeId: "e-hold",
            podcastId: "p", show: "S",
            decision: .holdOff,
            reasoning: "hold off: recall regressed",
            defaultRecall: 0.7, allEnabledRecall: 0.6,
            defaultPrecision: 0.8, allEnabledPrecision: 0.8,
            hasShadowCoverage: true,
            thresholdTau: 0.5,
            recallCheckPassed: false,
            precisionCheckPassed: true
        ),
        NarlRecommendation(
            episodeId: "e-insuf",
            podcastId: "p", show: "S",
            decision: .insufficientData,
            reasoning: "pending narl.2 shadow coverage",
            defaultRecall: nil, allEnabledRecall: nil,
            defaultPrecision: nil, allEnabledPrecision: nil,
            hasShadowCoverage: false,
            thresholdTau: 0.5,
            recallCheckPassed: nil,
            precisionCheckPassed: nil
        ),
    ]
    return NarlApprovalReport(
        schemaVersion: NarlApprovalReportSchema.version,
        generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
        sourceRunId: "test-harness-run",
        policy: .default,
        recommendations: recs,
        summary: NarlApprovalSummary.compute(from: recs),
        notes: []
    )
}
