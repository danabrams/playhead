// NarlApprovalIntegrationTests.swift
// playhead-narl.3: Integration — reads the latest harness report from
// `.eval-out/narl/<runId>/report.json`, runs the recommender, writes
// `recommendations.{json,md}` into the SAME directory.
//
// Runs in PlayheadFastTests. Deliberately separate from NarlEvalHarnessTests
// (option b in the bead spec: "separate Swift Testing suite that reads the
// latest harness report" — keeps scopes clean; the harness doesn't know about
// the recommender).
//
// Contract:
//   - If no harness report exists (CI clone, first run in fresh clone), the
//     test gracefully no-ops with a note — same philosophy as the harness
//     itself when fixtures are empty.
//   - If a harness report exists, the test writes recommendations artifacts
//     next to it and #expects the files exist.
//   - Metric regressions / policy outputs do NOT fail the build. This is
//     purely an artifact-emission test, consistent with the narl.1 philosophy.

import Foundation
import Testing
@testable import Playhead

@Suite("NarlApprovalIntegration")
struct NarlApprovalIntegrationTests {

    @Test("Recommender runs against the latest harness report and writes artifacts")
    func integrationRun() throws {
        // Locate the eval output root the same way the harness does.
        let evalRoot = try NarlEvalHarnessTests.evalOutputRootURL()
        let fm = FileManager.default

        // Find the latest run dir (a subdirectory containing report.json).
        // If no runs exist, the test is a no-op and still passes.
        guard let latestRunDir = try Self.latestRunDirectory(under: evalRoot) else {
            // No harness report yet: CI-clone scenario. Don't fail.
            return
        }

        let reportURL = latestRunDir.appendingPathComponent("report.json")
        guard fm.fileExists(atPath: reportURL.path) else {
            return
        }

        let data = try Data(contentsOf: reportURL)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let harnessReport = try decoder.decode(NarlEvalReport.self, from: data)

        let policy = NarlApprovalPolicy.default
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: harnessReport, policy: policy)

        var notes: [String] = []
        if recs.isEmpty {
            notes.append("Harness report contains no episode entries.")
        }
        // Surface the narl.2-pending caveat in the artifact itself so the
        // markdown renders a clear provenance note.
        if recs.contains(where: { $0.reasoning.contains("pending narl.2") }) {
            notes.append("Some episodes marked insufficientData pending narl.2 shadow coverage; "
                + "fmSchedulingEnabled evidence will become available once Bead 2 lands.")
        }

        let approvalReport = NarlApprovalReport(
            schemaVersion: NarlApprovalReportSchema.version,
            // playhead-narl: generatedAt derived from harness report for artifact determinism
            generatedAt: harnessReport.generatedAt,
            sourceRunId: harnessReport.runId,
            policy: policy,
            recommendations: recs,
            summary: NarlApprovalSummary.compute(from: recs),
            notes: notes
        )

        // Write into a `recommendations/` subdirectory under the run dir
        // so the approval artifacts don't overlay harness outputs that
        // share filenames. Provenance stays clear: harness owns the run
        // dir root, the recommender owns its subdirectory.
        let recommendationsDir = latestRunDir.appendingPathComponent(
            "recommendations", isDirectory: true
        )
        let (jsonURL, mdURL) = try NarlApprovalWriter.write(
            report: approvalReport,
            to: recommendationsDir
        )
        #expect(FileManager.default.fileExists(atPath: jsonURL.path),
                "recommendations.json should be written at \(jsonURL.path)")
        #expect(FileManager.default.fileExists(atPath: mdURL.path),
                "recommendations.md should be written at \(mdURL.path)")

        // Content assertion: decoding the JSON round-trips the shape and
        // preserves the episode count so a future regression that writes
        // an empty / truncated artifact trips the test. `insufficientData`
        // is the expected dominant state today pre-narl.2 shadow coverage.
        let jsonData = try Data(contentsOf: jsonURL)
        let roundTripDecoder = JSONDecoder()
        roundTripDecoder.dateDecodingStrategy = .iso8601
        let decoded = try roundTripDecoder.decode(NarlApprovalReport.self, from: jsonData)
        try decoded.requireSchema()
        #expect(decoded.recommendations.count == recs.count,
                "decoded recommendation count should match source")
        // Assert at least one recognized decision state appears. Don't
        // pin to `.insufficientData` specifically — once narl.2 shadow
        // coverage lands, flips become valid — but require SOMETHING
        // decodes as a known state, catching a corrupt artifact.
        if !recs.isEmpty {
            let knownStates: Set<NarlRecommendationDecision> = [
                .recommendFlip, .holdOff, .insufficientData
            ]
            #expect(decoded.recommendations.allSatisfy { knownStates.contains($0.decision) },
                    "every decoded recommendation should carry a known decision state")
        }
    }

    /// Find the most-recent `<runId>/` directory under `root` by
    /// runId-lexicographic order (runIds are `yyyyMMdd-HHmmss-<tag>`, so
    /// lexicographic == chronological). Returns nil when no such directory
    /// exists.
    static func latestRunDirectory(under root: URL) throws -> URL? {
        let fm = FileManager.default
        guard fm.fileExists(atPath: root.path) else { return nil }
        let children = try fm.contentsOfDirectory(
            at: root,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: [.skipsHiddenFiles]
        )
        // A run dir contains report.json. Trend file is a regular file, skip.
        let runDirs = children.filter { url in
            var isDir: ObjCBool = false
            let exists = fm.fileExists(atPath: url.path, isDirectory: &isDir)
            guard exists, isDir.boolValue else { return false }
            return fm.fileExists(atPath: url.appendingPathComponent("report.json").path)
        }
        return runDirs.sorted { $0.lastPathComponent < $1.lastPathComponent }.last
    }
}
