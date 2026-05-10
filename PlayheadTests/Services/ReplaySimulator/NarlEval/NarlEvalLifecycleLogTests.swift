// NarlEvalLifecycleLogTests.swift
// Follow-up to gtt9.8 NARL real-data eval (2026-04-24 finding 3):
// the corpus builder didn't thread `asset-lifecycle-log.jsonl` truth into
// FrozenTrace JSON, so the harness couldn't distinguish pipeline-coverage
// failures from classifier failures using lifecycle evidence.
//
// These tests lock the contract:
//   1. Parsing the schema-v1 lifecycle log identifies the terminal row per
//      asset (the row with a non-null terminalReason) and extracts
//      durationSec, analysisState, terminalReason, transcriptCoverageEndSec,
//      featureCoverageEndSec into a BuilderLifecycleSummary.
//   2. Assets with no terminal row (e.g. the 34C7E7CF partial-coverage
//      stall) fall back to the latest-timestamp row, so we still surface
//      the best-known analysisState and coverage snapshot.
//   3. FrozenTrace back-compat: JSON written before the new fields were
//      added still decodes with the new fields defaulting to nil.
//   4. The harness pipeline-coverage classifier prefers lifecycle truth:
//      `analysisState == "completeFull"` suppresses the
//      `pipelineCoverageFailureAsset` flag regardless of what the
//      evidence-ledger-based inference says.

import Foundation
import Testing
@testable import Playhead

@Suite("NarlEvalLifecycleLog")
struct NarlEvalLifecycleLogTests {

    /// Sample embedded lifecycle log taken verbatim from the
    /// 2026-04-23 21:34 xcappdata capture: one asset with a terminal
    /// `completeFull` row, one asset stuck in `backfill` with no terminal.
    static let sampleJSONL: String = """
    {"analysisAssetID":"34C7E7CF-931F-49EE-B51B-49D3080F1FFB","episodeDurationSec":0,"fromState":"queued","schemaVersion":1,"sessionID":"S1","timestamp":1776986536.594979,"toState":"spooling"}
    {"analysisAssetID":"34C7E7CF-931F-49EE-B51B-49D3080F1FFB","episodeDurationSec":900,"featureCoverageEndSec":726,"fromState":"spooling","schemaVersion":1,"sessionID":"S1","timestamp":1776986555.034196,"toState":"featuresReady","transcriptCoverageEndSec":840}
    {"analysisAssetID":"34C7E7CF-931F-49EE-B51B-49D3080F1FFB","episodeDurationSec":900,"featureCoverageEndSec":726,"fromState":"featuresReady","schemaVersion":1,"sessionID":"S1","timestamp":1776986555.0375772,"toState":"hotPathReady","transcriptCoverageEndSec":840}
    {"analysisAssetID":"34C7E7CF-931F-49EE-B51B-49D3080F1FFB","episodeDurationSec":900,"featureCoverageEndSec":726,"fromState":"hotPathReady","schemaVersion":1,"sessionID":"S1","timestamp":1776986555.0398722,"toState":"backfill","transcriptCoverageEndSec":840}
    {"analysisAssetID":"71F0C2AE-7260-4D1E-B41A-BCFD5103A641","episodeDurationSec":7037.834375,"featureCoverageEndSec":7036,"fromState":"spooling","schemaVersion":1,"sessionID":"S2","timestamp":1776989440.644933,"toState":"featuresReady"}
    {"analysisAssetID":"71F0C2AE-7260-4D1E-B41A-BCFD5103A641","episodeDurationSec":7037.834375,"featureCoverageEndSec":7036,"fromState":"featuresReady","schemaVersion":1,"sessionID":"S2","timestamp":1776989440.663513,"toState":"hotPathReady"}
    {"analysisAssetID":"71F0C2AE-7260-4D1E-B41A-BCFD5103A641","episodeDurationSec":7037.834375,"featureCoverageEndSec":7036,"fromState":"hotPathReady","schemaVersion":1,"sessionID":"S2","timestamp":1776989440.665481,"toState":"backfill"}
    {"analysisAssetID":"71F0C2AE-7260-4D1E-B41A-BCFD5103A641","episodeDurationSec":7037.834375,"featureCoverageEndSec":7036,"fromState":"backfill","schemaVersion":1,"sessionID":"S2","terminalReason":"full coverage: transcript 1.000, feature 1.000","timestamp":1776990328.935648,"toState":"completeFull","transcriptCoverageEndSec":7037.34}
    """

    /// Write the sample JSONL to a temp file and return the URL.
    private static func materializeSample(_ body: String = sampleJSONL) throws -> URL {
        try materializeJSONL(filename: "asset-lifecycle-log.jsonl", body: body)
    }

    private static func materializeJSONL(filename: String, body: String) throws -> URL {
        let tempDir = try materializeJSONLDirectory(files: [(filename: filename, body: body)])
        return tempDir.appendingPathComponent(filename)
    }

    private static func materializeJSONLDirectory(
        files: [(filename: String, body: String)]
    ) throws -> URL {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("narl-lifecycle-\(UUID().uuidString)")
        try FileManager.default.createDirectory(
            at: tempDir, withIntermediateDirectories: true
        )
        for file in files {
            let url = tempDir.appendingPathComponent(file.filename)
            try file.body.data(using: .utf8)!.write(to: url)
        }
        return tempDir
    }

    // MARK: - RED 1: lifecycle parser identifies terminal row per asset

    @Test("parseLifecycleLog picks the terminal row with terminalReason when present")
    func parseLifecycleLogPicksTerminalRow() throws {
        let url = try Self.materializeSample()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }

        let summaries = try NarlEvalCorpusBuilderTests.parseLifecycleLog(from: url)

        // Conan 117-minute asset reached completeFull.
        let conan = try #require(summaries["71F0C2AE-7260-4D1E-B41A-BCFD5103A641"])
        #expect(conan.analysisState == "completeFull")
        #expect(conan.terminalReason == "full coverage: transcript 1.000, feature 1.000")
        #expect(abs((conan.durationSec ?? 0) - 7037.834375) < 1e-6)
        #expect(abs((conan.transcriptCoverageEndSec ?? 0) - 7037.34) < 1e-6)
        #expect(abs((conan.featureCoverageEndSec ?? 0) - 7036) < 1e-6)
    }

    @Test("parseLifecycleLog falls back to latest-timestamp row when no terminal reached")
    func parseLifecycleLogFallsBackToLatestTimestamp() throws {
        let url = try Self.materializeSample()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }

        let summaries = try NarlEvalCorpusBuilderTests.parseLifecycleLog(from: url)

        // 34C7E7CF stalled in backfill — no terminal row. The latest
        // timestamp row is the backfill transition which carries all
        // coverage fields.
        let stalled = try #require(summaries["34C7E7CF-931F-49EE-B51B-49D3080F1FFB"])
        #expect(stalled.analysisState == "backfill")
        #expect(stalled.terminalReason == nil)
        #expect(abs((stalled.durationSec ?? 0) - 900) < 1e-6)
        #expect(abs((stalled.transcriptCoverageEndSec ?? 0) - 840) < 1e-6)
        #expect(abs((stalled.featureCoverageEndSec ?? 0) - 726) < 1e-6)
    }

    @Test("parseLifecycleLog keeps terminal row even when a later non-terminal row exists")
    func parseLifecycleLogTerminalReasonWinsOverLaterNonTerminal() throws {
        let url = try Self.materializeSample("""
        {"analysisAssetID":"asset-terminal","episodeDurationSec":1200,"featureCoverageEndSec":1190,"schemaVersion":1,"sessionID":"S1","terminalReason":"full coverage: transcript 0.999, feature 0.992","timestamp":100,"toState":"completeFull","transcriptCoverageEndSec":1199}
        {"analysisAssetID":"asset-terminal","episodeDurationSec":1200,"featureCoverageEndSec":100,"schemaVersion":1,"sessionID":"S1","timestamp":200,"toState":"backfill","transcriptCoverageEndSec":100}
        """)
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }

        let summaries = try NarlEvalCorpusBuilderTests.parseLifecycleLog(from: url)

        let selected = try #require(summaries["asset-terminal"])
        #expect(selected.analysisState == "completeFull")
        #expect(selected.terminalReason == "full coverage: transcript 0.999, feature 0.992")
        #expect(selected.timestamp == 100)
        #expect(selected.transcriptCoverageEndSec == 1199)
        #expect(selected.featureCoverageEndSec == 1190)
    }

    // MARK: - RED 2: FrozenTrace back-compat decode

    @Test("FrozenTrace decodes pre-lifecycle JSON with new fields nil")
    func frozenTraceDecodesPreLifecycleJSON() throws {
        // A minimal v2 FrozenTrace payload that predates the gtt9.8
        // lifecycle fields. Decoding must succeed and all new fields
        // must be nil — older 2026-04-22 / 2026-04-23 fixtures should
        // still load without loss.
        let pre: [String: Any] = [
            "episodeId": "ep-test",
            "podcastId": "test",
            "episodeDuration": 300.0,
            "traceVersion": "frozen-trace-v2",
            "capturedAt": "2026-04-22T00:00:00Z",
            "featureWindows": [],
            "atoms": [],
            "evidenceCatalog": [],
            "corrections": [],
            "decisionEvents": [],
            "baselineReplaySpanDecisions": [],
            "holdoutDesignation": "training",
            "windowScores": []
        ]
        let data = try JSONSerialization.data(withJSONObject: pre)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let trace = try decoder.decode(FrozenTrace.self, from: data)

        #expect(trace.durationSec == nil)
        #expect(trace.analysisState == nil)
        #expect(trace.terminalReason == nil)
        #expect(trace.fastTranscriptCoverageEndTime == nil)
        #expect(trace.featureCoverageEndTime == nil)
    }

    @Test("FrozenTrace round-trips lifecycle fields when populated")
    func frozenTraceRoundTripsLifecycleFields() throws {
        let trace = FrozenTrace(
            episodeId: "ep",
            podcastId: "p",
            episodeDuration: 7037.83,
            traceVersion: "frozen-trace-v2",
            capturedAt: Date(timeIntervalSince1970: 0),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training,
            durationSec: 7037.83,
            analysisState: "completeFull",
            terminalReason: "full coverage: transcript 1.000, feature 1.000",
            fastTranscriptCoverageEndTime: 7037.34,
            featureCoverageEndTime: 7036
        )

        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(trace)

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(FrozenTrace.self, from: data)

        #expect(decoded.durationSec == 7037.83)
        #expect(decoded.analysisState == "completeFull")
        #expect(decoded.terminalReason == "full coverage: transcript 1.000, feature 1.000")
        #expect(decoded.fastTranscriptCoverageEndTime == 7037.34)
        #expect(decoded.featureCoverageEndTime == 7036)
    }

    // MARK: - RED 3: assembleTrace lifecycle wiring

    @Test("assembleTrace writes lifecycle summary fields into FrozenTrace")
    func assembleTraceWritesLifecycleSummaryFields() {
        let asset = NarlEvalCorpusBuilderTests.BuilderAsset(
            id: "asset-life",
            episodeId: "ep-life",
            podcastId: "pod-life",
            detectorVersion: "",
            buildCommitSHA: ""
        )
        let lifecycle = NarlEvalCorpusBuilderTests.BuilderLifecycleSummary(
            analysisAssetID: "asset-life",
            durationSec: 1800,
            analysisState: "completeFull",
            terminalReason: "full coverage: transcript 1.000, feature 1.000",
            transcriptCoverageEndSec: 1798.5,
            featureCoverageEndSec: 1800,
            timestamp: 1776990328
        )

        let trace = NarlEvalCorpusBuilderTests.assembleTrace(
            asset: asset,
            decisions: [],
            corrections: [],
            decisionLog: [],
            shadow: [],
            lifecycle: lifecycle
        )

        #expect(trace.durationSec == 1800)
        #expect(trace.analysisState == "completeFull")
        #expect(trace.terminalReason == "full coverage: transcript 1.000, feature 1.000")
        #expect(trace.fastTranscriptCoverageEndTime == 1798.5)
        #expect(trace.featureCoverageEndTime == 1800)
    }

    @Test("corpus-export asset row lifecycle is used when lifecycle log is absent")
    func corpusExportLifecycleFallbackWhenLogAbsent() throws {
        let url = try Self.materializeJSONL(
            filename: "corpus-export.lifecycle.jsonl",
            body: """
            {"type":"asset","schemaVersion":1,"analysisAssetId":"asset-live","episodeId":"ep-live","podcastId":"pod-live","assetFingerprint":"fp-live","sourceURL":"file:///tmp/live.m4a","analysisState":"completeFull","analysisVersion":1,"episodeDurationSec":3600.5,"terminalReason":"full coverage: transcript 0.999, feature 1.000","fastTranscriptCoverageEndTime":3599.0,"featureCoverageEndTime":3600.0}
            """
        )
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }

        let asset = try #require(
            try NarlEvalCorpusBuilderTests.parseAssets(from: [url]).first
        )
        let lifecycle = try #require(
            NarlEvalCorpusBuilderTests.lifecycleSummary(
                for: asset,
                lifecycleSummaries: [:]
            )
        )
        let trace = NarlEvalCorpusBuilderTests.assembleTrace(
            asset: asset,
            decisions: [],
            corrections: [],
            decisionLog: [],
            shadow: [],
            lifecycle: lifecycle
        )

        #expect(trace.durationSec == 3600.5)
        #expect(trace.analysisState == "completeFull")
        #expect(trace.terminalReason == "full coverage: transcript 0.999, feature 1.000")
        #expect(trace.fastTranscriptCoverageEndTime == 3599.0)
        #expect(trace.featureCoverageEndTime == 3600.0)
    }

    @Test("corpus-export fallback preserves partial lifecycle snapshot when duration is absent")
    func corpusExportLifecycleFallbackPreservesPartialSnapshotWithoutDuration() throws {
        let url = try Self.materializeJSONL(
            filename: "corpus-export.partial-lifecycle.jsonl",
            body: """
            {"type":"asset","schemaVersion":1,"analysisAssetId":"asset-partial","episodeId":"ep-partial","podcastId":"pod-partial","assetFingerprint":"fp-partial","sourceURL":"file:///tmp/partial.m4a","analysisState":"completeFull","analysisVersion":1,"terminalReason":"full coverage: transcript 1.000, feature 1.000","fastTranscriptCoverageEndTime":3599.0,"featureCoverageEndTime":3600.0}
            """
        )
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }

        let asset = try #require(
            try NarlEvalCorpusBuilderTests.parseAssets(from: [url]).first
        )
        let lifecycle = try #require(
            NarlEvalCorpusBuilderTests.lifecycleSummary(
                for: asset,
                lifecycleSummaries: [:]
            )
        )
        let trace = NarlEvalCorpusBuilderTests.assembleTrace(
            asset: asset,
            decisions: [],
            corrections: [],
            decisionLog: [],
            shadow: [],
            lifecycle: lifecycle
        )

        #expect(trace.durationSec == nil)
        #expect(trace.analysisState == "completeFull")
        #expect(trace.terminalReason == "full coverage: transcript 1.000, feature 1.000")
        #expect(trace.fastTranscriptCoverageEndTime == 3599.0)
        #expect(trace.featureCoverageEndTime == 3600.0)
    }

    @Test("newest corpus-export asset snapshot wins when duplicate asset rows exist")
    func newestCorpusExportAssetSnapshotWins() throws {
        let dir = try Self.materializeJSONLDirectory(files: [
            (
                "corpus-export.2026-04-24T00-00-02.000Z.jsonl",
                """
                {"type":"asset","schemaVersion":1,"analysisAssetId":"asset-live","episodeId":"ep-live","podcastId":"pod-live","assetFingerprint":"fp-live","sourceURL":"file:///tmp/live.m4a","analysisState":"completeFull","analysisVersion":1,"episodeDurationSec":3600.5,"terminalReason":"full coverage: transcript 1.000, feature 1.000","fastTranscriptCoverageEndTime":3600.0,"featureCoverageEndTime":3600.0}
                """
            ),
            (
                "corpus-export.2026-04-24T00-00-01.000Z.jsonl",
                """
                {"type":"asset","schemaVersion":1,"analysisAssetId":"asset-live","episodeId":"ep-live","podcastId":"pod-live","assetFingerprint":"fp-live","sourceURL":"file:///tmp/live.m4a","analysisState":"backfill","analysisVersion":1,"episodeDurationSec":900.0,"fastTranscriptCoverageEndTime":840.0,"featureCoverageEndTime":726.0}
                """
            )
        ])
        defer { try? FileManager.default.removeItem(at: dir) }

        let corpusExports = try NarlEvalCorpusBuilderTests.locateCorpusExports(in: dir)
        #expect(corpusExports.map(\.lastPathComponent) == [
            "corpus-export.2026-04-24T00-00-01.000Z.jsonl",
            "corpus-export.2026-04-24T00-00-02.000Z.jsonl",
        ])

        let asset = try #require(
            try NarlEvalCorpusBuilderTests.parseAssets(from: corpusExports).first
        )
        let lifecycle = try #require(
            NarlEvalCorpusBuilderTests.lifecycleSummary(
                for: asset,
                lifecycleSummaries: [:]
            )
        )
        #expect(lifecycle.durationSec == 3600.5)
        #expect(lifecycle.analysisState == "completeFull")
        #expect(lifecycle.terminalReason == "full coverage: transcript 1.000, feature 1.000")
        #expect(lifecycle.transcriptCoverageEndSec == 3600.0)
        #expect(lifecycle.featureCoverageEndSec == 3600.0)
    }

    @Test("asset-lifecycle-log summary wins over corpus-export asset fallback")
    func lifecycleLogSummaryWinsOverExportFallback() throws {
        let asset = NarlEvalCorpusBuilderTests.BuilderAsset(
            id: "asset-live",
            episodeId: "ep-live",
            podcastId: "pod-live",
            detectorVersion: "",
            buildCommitSHA: "",
            durationSec: 3600,
            analysisState: "completeFull",
            terminalReason: "full coverage: transcript 1.000, feature 1.000",
            fastTranscriptCoverageEndTime: 3600,
            featureCoverageEndTime: 3600
        )
        let logSummary = NarlEvalCorpusBuilderTests.BuilderLifecycleSummary(
            analysisAssetID: "asset-live",
            durationSec: 900,
            analysisState: "backfill",
            terminalReason: nil,
            transcriptCoverageEndSec: 840,
            featureCoverageEndSec: 726,
            timestamp: 1776986555
        )

        let selected = try #require(
            NarlEvalCorpusBuilderTests.lifecycleSummary(
                for: asset,
                lifecycleSummaries: ["asset-live": logSummary]
            )
        )
        #expect(selected.analysisState == "backfill")
        #expect(selected.durationSec == 900)
        #expect(selected.terminalReason == nil)
        #expect(selected.transcriptCoverageEndSec == 840)
        #expect(selected.featureCoverageEndSec == 726)
    }

    // MARK: - RED 4: harness classifier honors analysisState == completeFull

    @Test("harness pipelineCoverage classifier suppresses flag on completeFull assets")
    func harnessClassifierSuppressesFlagOnCompleteFull() {
        // Construct a trace that would otherwise flag as a pipeline-
        // coverage failure (unscoredFNRate > 0.5) but carries
        // analysisState == "completeFull" in its lifecycle snapshot.
        // The harness-level classifier must not flag it.
        let gt = [NarlTimeRange(start: 0, end: 60)]
        // Empty windowScores → 100% unscored FN under evidence-ledger
        // inference, which would set pipelineCoverageFailureAsset = true.
        let traceRaw = FrozenTrace(
            episodeId: "ep",
            podcastId: "p",
            episodeDuration: 300,
            traceVersion: "frozen-trace-v2",
            capturedAt: Date(timeIntervalSince1970: 0),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training,
            durationSec: 300,
            analysisState: "completeFull",
            terminalReason: "full coverage: transcript 1.000, feature 1.000",
            fastTranscriptCoverageEndTime: 300,
            featureCoverageEndTime: 300
        )
        let raw = NarlCoverageMetricsCompute.compute(
            trace: traceRaw, predicted: [], groundTruth: gt
        )
        // Sanity: raw metrics would flag the asset (the test case is
        // built around this baseline behavior).
        #expect(raw.metrics.pipelineCoverageFailureAsset == true,
                "raw classifier should flag unscoredFNRate=1.0 asset")

        let adjusted = NarlEvalHarnessTests.adjustPipelineFailureFlag(
            raw: raw.metrics, trace: traceRaw
        )
        #expect(adjusted.pipelineCoverageFailureAsset == false,
                "completeFull lifecycle must suppress pipeline-coverage flag")
    }

    @Test("harness pipelineCoverage classifier keeps flag when analysisState is nil")
    func harnessClassifierKeepsFlagWhenLifecycleAbsent() {
        let gt = [NarlTimeRange(start: 0, end: 60)]
        let traceNoLifecycle = FrozenTrace(
            episodeId: "ep",
            podcastId: "p",
            episodeDuration: 300,
            traceVersion: "frozen-trace-v2",
            capturedAt: Date(timeIntervalSince1970: 0),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training
        )
        let raw = NarlCoverageMetricsCompute.compute(
            trace: traceNoLifecycle, predicted: [], groundTruth: gt
        )
        let adjusted = NarlEvalHarnessTests.adjustPipelineFailureFlag(
            raw: raw.metrics, trace: traceNoLifecycle
        )
        #expect(adjusted.pipelineCoverageFailureAsset == true,
                "nil analysisState falls back to evidence-ledger inference")
    }

    @Test("harness pipelineCoverage classifier keeps flag for non-full completes with misaligned corrections")
    func harnessClassifierFlagsOnPartialCompleteWithMisalignedCorrections() {
        // completeFeatureOnly at 600s — but the user correction (ground
        // truth ad span) lies at 1200-1230s. The asset is beyond the
        // covered range, so it IS a pipeline-coverage failure.
        let gt = [NarlTimeRange(start: 1200, end: 1230)]
        let trace = FrozenTrace(
            episodeId: "ep",
            podcastId: "p",
            episodeDuration: 1800,
            traceVersion: "frozen-trace-v2",
            capturedAt: Date(timeIntervalSince1970: 0),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training,
            durationSec: 1800,
            analysisState: "completeFeatureOnly",
            terminalReason: "partial: feature only",
            fastTranscriptCoverageEndTime: 600,
            featureCoverageEndTime: 1800
        )
        let raw = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: [], groundTruth: gt
        )
        let adjusted = NarlEvalHarnessTests.adjustPipelineFailureFlag(
            raw: raw.metrics, trace: trace
        )
        #expect(adjusted.pipelineCoverageFailureAsset == true,
                "completeFeatureOnly with corrections beyond transcript coverage IS a pipeline failure")
    }
}
