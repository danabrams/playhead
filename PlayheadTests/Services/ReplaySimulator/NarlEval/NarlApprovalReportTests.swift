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
