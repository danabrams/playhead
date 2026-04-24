// NarlEvalTerminalReasonStratifiedTests.swift
// Follow-up to gtt9.8-corpus-thread (2026-04-24 findings §"Aggregate coverage caveat").
//
// The bundled ALL / Conan / DoaC rollups in NarlEvalReport hide a real signal:
// episodes that completed transcription fully (`completeFull`) should have
// systematically different detection quality than episodes that stopped early
// (`completeTranscriptPartial`, `cancelledBudget`, `failedTranscript`). Bundled
// F1 lets pre-9.1.1 partially-transcribed captures dilute post-9.1.1 full
// captures.
//
// This suite locks the contract that the harness report gains a
// `terminalReasonBuckets` array — one rollup per (bucket × config) — where
// each bucket is derived from `trace.analysisState` (the SessionState raw
// value). `.unknown` is the catch-all for pre-9.8 traces (nil analysisState)
// AND any non-terminal state that isn't one of the six canonical terminals.
//
// Tests assert:
//   1. `NarlTerminalReasonBucket.classify(_:)` maps the six terminal
//      SessionState values to their enum case; nil and non-terminal states
//      fall to `.unknown`.
//   2. The aggregator — given a set of FrozenTraces and the already-computed
//      NarlReportEpisodeEntry values — groups episodes by bucket × config,
//      producing episode counts, excluded counts, window metrics @ {0.3,
//      0.5, 0.7}, Sec-F1, AutoSkip Prec/Recall.
//   3. `NarlEvalReport.terminalReasonBuckets` is the new optional field on
//      the persisted schema. Codable is additive: older report.json payloads
//      without the field round-trip through encode→decode with `nil`.
//   4. The live harness run (`runHarnessCollectingReport`) writes the field
//      into report.json with at least the `unknown` bucket populated (most
//      2026-04-24 fixtures have analysisState == nil).

import Foundation
import Testing
@testable import Playhead

@Suite("NarlEvalTerminalReasonStratified")
struct NarlEvalTerminalReasonStratifiedTests {

    // MARK: - Classifier

    @Test("classify(trace:) maps each canonical SessionState.rawValue to its bucket")
    func classifyMapsCanonicalStatesToBuckets() {
        let cases: [(String, NarlTerminalReasonBucket)] = [
            ("completeFull", .completeFull),
            ("completeFeatureOnly", .completeFeatureOnly),
            ("completeTranscriptPartial", .completeTranscriptPartial),
            ("cancelledBudget", .cancelledBudget),
            ("failedTranscript", .failedTranscript),
            ("failedFeature", .failedFeature),
        ]
        for (state, expected) in cases {
            let trace = Self.makeTrace(analysisState: state)
            #expect(NarlTerminalReasonBucket.classify(trace) == expected,
                    "analysisState \"\(state)\" should map to \(expected.rawValue)")
        }
    }

    @Test("classify(trace:) returns .unknown when analysisState is nil (pre-9.8 capture)")
    func classifyReturnsUnknownForNilAnalysisState() {
        let trace = Self.makeTrace(analysisState: nil)
        #expect(NarlTerminalReasonBucket.classify(trace) == .unknown)
    }

    @Test("classify(trace:) returns .unknown for non-terminal / unrecognized states")
    func classifyReturnsUnknownForNonTerminalStates() {
        // Stalled in backfill, spooling, the deprecated monolithic complete,
        // and anything else — all fall into the .unknown bucket. These are
        // not valid "terminalReason" buckets the user wants to stratify by
        // (they're still-in-progress or legacy pre-gtt9.8 rows).
        for state in ["backfill", "spooling", "queued", "featuresReady",
                      "hotPathReady", "waitingForBackfill", "complete", "failed"] {
            let trace = Self.makeTrace(analysisState: state)
            #expect(NarlTerminalReasonBucket.classify(trace) == .unknown,
                    "non-terminal/legacy state \"\(state)\" should fall to .unknown")
        }
    }

    // MARK: - Aggregator

    @Test("stratifyByTerminalReason produces one rollup per (bucket × config) with correct episode counts")
    func aggregatorProducesExpectedCountsPerBucketAndConfig() {
        // 6 synthetic traces: 2 completeFull, 2 completeTranscriptPartial,
        // 1 cancelledBudget, 1 nil (→ unknown). Each produces 2 entries (one
        // per config), so we expect 4 bucket-rows: completeFull, completeTP,
        // cancelledBudget, unknown — each × 2 configs = 8 rollup entries.
        let traces: [FrozenTrace] = [
            Self.makeTrace(episodeId: "full-1", analysisState: "completeFull"),
            Self.makeTrace(episodeId: "full-2", analysisState: "completeFull"),
            Self.makeTrace(episodeId: "tp-1",   analysisState: "completeTranscriptPartial"),
            Self.makeTrace(episodeId: "tp-2",   analysisState: "completeTranscriptPartial"),
            Self.makeTrace(episodeId: "cb-1",   analysisState: "cancelledBudget"),
            Self.makeTrace(episodeId: "u-1",    analysisState: nil),
        ]
        let entries = traces.flatMap { t in
            ["default", "allEnabled"].map { cfg in
                Self.makeEntry(episodeId: t.episodeId, config: cfg, gtCount: 1, predCount: 1)
            }
        }

        let rollups = NarlReportTerminalReasonRollup.stratify(
            traces: traces,
            entries: entries,
            pipelinesByEpisodeId: Self.pipelinesByEpisodeId(for: traces, entries: entries)
        )

        #expect(rollups.count == 8,
                "4 active buckets × 2 configs = 8 rollups; got \(rollups.count)")

        // Episode counts per (bucket, config) — both configs symmetric.
        for cfg in ["default", "allEnabled"] {
            let byBucket = Dictionary(uniqueKeysWithValues:
                rollups.filter { $0.config == cfg }.map { ($0.bucket, $0.episodeCount) })
            #expect(byBucket[.completeFull] == 2,
                    "cfg=\(cfg) bucket=completeFull episodeCount should be 2, got \(byBucket[.completeFull] ?? -1)")
            #expect(byBucket[.completeTranscriptPartial] == 2,
                    "cfg=\(cfg) bucket=completeTranscriptPartial episodeCount should be 2, got \(byBucket[.completeTranscriptPartial] ?? -1)")
            #expect(byBucket[.cancelledBudget] == 1,
                    "cfg=\(cfg) bucket=cancelledBudget episodeCount should be 1, got \(byBucket[.cancelledBudget] ?? -1)")
            #expect(byBucket[.unknown] == 1,
                    "cfg=\(cfg) bucket=unknown episodeCount should be 1, got \(byBucket[.unknown] ?? -1)")
        }

        // Absent buckets (no episodes) should NOT appear in the rollup list —
        // an empty bucket row would just be noise in the rendered report.
        let bucketsSeen = Set(rollups.map(\.bucket))
        #expect(!bucketsSeen.contains(.completeFeatureOnly),
                "empty buckets must not appear in the rollup list")
        #expect(!bucketsSeen.contains(.failedTranscript),
                "empty buckets must not appear in the rollup list")
        #expect(!bucketsSeen.contains(.failedFeature),
                "empty buckets must not appear in the rollup list")
    }

    @Test("stratifyByTerminalReason aggregates window metrics + Sec-F1 per bucket")
    func aggregatorAggregatesMetricsPerBucket() {
        // One completeFull trace with perfect prediction, one with perfect miss.
        // The bucket rollup should aggregate across both episodes (corpus-level
        // pool of TP/FP like the existing show rollups).
        let perfect: NarlTimeRange = NarlTimeRange(start: 0, end: 30)
        let traces = [
            Self.makeTrace(episodeId: "hit", analysisState: "completeFull"),
            Self.makeTrace(episodeId: "miss", analysisState: "completeFull"),
        ]
        let entries = traces.flatMap { t in
            [("default", t)].map { (cfg, trace) in
                Self.makeEntry(episodeId: trace.episodeId, config: cfg, gtCount: 1, predCount: trace.episodeId == "hit" ? 1 : 0)
            }
        }
        // Pipeline map: episodeId → [(pred, gt)] windows for the bucket
        // aggregator. List shape handles duplicate episodeIds in the
        // fixture tree (same asset captured on multiple days).
        let pipelines: [String: [(pred: [NarlTimeRange], gt: [NarlTimeRange])]] = [
            "hit|default":  [(pred: [perfect], gt: [perfect])],
            "miss|default": [(pred: [],        gt: [perfect])],
        ]

        let rollups = NarlReportTerminalReasonRollup.stratify(
            traces: traces,
            entries: entries,
            pipelinesByEpisodeId: pipelines
        )

        let row = try! #require(
            rollups.first(where: { $0.bucket == .completeFull && $0.config == "default" })
        )
        #expect(row.episodeCount == 2)
        // Corpus-level pool: 1 TP, 0 FP, 1 FN across both episodes.
        // Precision = 1/1 = 1.0, recall = 1/2 = 0.5, F1 = 0.667.
        let f15 = row.windowMetrics.first(where: { abs($0.threshold - 0.5) < 1e-6 })
        let wf1 = f15?.f1 ?? -1
        #expect(abs(wf1 - (2.0 * 1.0 * 0.5 / (1.0 + 0.5))) < 1e-6,
                "corpus-pool F1@0.5 for 1 TP / 0 FP / 1 FN should be ~0.667, got \(wf1)")
    }

    // MARK: - Report schema — Codable back-compat

    @Test("NarlEvalReport.terminalReasonBuckets round-trips through encode/decode")
    func reportRoundTripsTerminalReasonBuckets() throws {
        let rollup = NarlReportTerminalReasonRollup(
            bucket: .completeFull,
            config: "default",
            episodeCount: 1,
            excludedEpisodeCount: 0,
            windowMetrics: [],
            secondLevel: NarlSecondLevelMetrics(
                truePositiveSeconds: 0, falsePositiveSeconds: 0, falseNegativeSeconds: 0,
                precision: 0, recall: 0, f1: 0),
            autoSkipPrecision: 0.5,
            autoSkipRecall: 0.25
        )
        let report = NarlEvalReport(
            schemaVersion: NarlEvalReportSchema.version,
            generatedAt: Date(timeIntervalSince1970: 0),
            runId: "test",
            iouThresholds: [0.3, 0.5, 0.7],
            rollups: [],
            episodes: [],
            notes: [],
            terminalReasonBuckets: [rollup]
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(report)

        let text = String(decoding: data, as: UTF8.self)
        #expect(text.contains("\"terminalReasonBuckets\""),
                "encoded JSON must include the new terminalReasonBuckets key")

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(NarlEvalReport.self, from: data)
        #expect(decoded.terminalReasonBuckets?.count == 1)
        #expect(decoded.terminalReasonBuckets?.first?.bucket == .completeFull)
        #expect(decoded.terminalReasonBuckets?.first?.autoSkipPrecision == 0.5)
    }

    @Test("pre-stratification report.json (no terminalReasonBuckets key) decodes with nil")
    func preStratificationReportDecodesWithNil() throws {
        // Literal payload matching the pre-stratification schema: every field
        // the old NarlEvalReport emitted is populated; `terminalReasonBuckets`
        // is absent entirely. This pins the `decodeIfPresent` path so the
        // historical `report.json` bundles under `.eval-out/narl/` keep
        // decoding after the stratification schema lands.
        let json = """
        {
          "schemaVersion": 1,
          "generatedAt": "1970-01-01T00:00:00Z",
          "runId": "pre-stratification",
          "iouThresholds": [0.3, 0.5, 0.7],
          "rollups": [],
          "episodes": [],
          "notes": []
        }
        """
        let data = Data(json.utf8)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(NarlEvalReport.self, from: data)

        #expect(decoded.schemaVersion == 1)
        #expect(decoded.runId == "pre-stratification")
        #expect(decoded.terminalReasonBuckets == nil,
                "missing key must decode to nil — additive, back-compat")
    }

    // MARK: - Live harness integration

    @Test("live harness run writes terminalReasonBuckets into report.json")
    func liveHarnessEmitsTerminalReasonBuckets() throws {
        let (_, outputDir) = try NarlEvalHarnessTests.runHarnessCollectingReport()
        let jsonURL = outputDir.appendingPathComponent("report.json")
        let data = try Data(contentsOf: jsonURL)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(NarlEvalReport.self, from: data)

        let buckets = try #require(decoded.terminalReasonBuckets,
                                   "live run must emit terminalReasonBuckets (even when empty)")

        // If the fixture tree exists at all, most traces are pre-9.8 and
        // will land in .unknown — assert the bucket is populated OR the
        // whole buckets array is empty (CI without fixtures). This keeps
        // the assertion meaningful on a dev workstation and vacuous on
        // clones that lack the dated fixture dirs.
        if !decoded.episodes.contains(where: { !$0.isExcluded }) { return }
        let hasUnknown = buckets.contains { $0.bucket == .unknown }
        #expect(hasUnknown,
                "2026-04-24 fixtures have analysisState == nil; expected .unknown bucket")

        // Also assert the rendered markdown mentions the new section so
        // downstream report readers find it by header name.
        let mdData = try Data(contentsOf: outputDir.appendingPathComponent("report.md"))
        let md = String(decoding: mdData, as: UTF8.self)
        #expect(md.contains("Terminal-reason stratification") ||
                md.contains("terminalReasonBuckets"),
                "report.md should include the new stratification section")
    }

    // MARK: - Helpers

    private static func makeTrace(
        episodeId: String = "ep-test",
        analysisState: String?
    ) -> FrozenTrace {
        FrozenTrace(
            episodeId: episodeId,
            podcastId: "test",
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
            windowScores: [],
            showLabel: nil,
            durationSec: nil,
            analysisState: analysisState,
            terminalReason: nil,
            fastTranscriptCoverageEndTime: nil,
            featureCoverageEndTime: nil
        )
    }

    private static func makeEntry(
        episodeId: String,
        config: String,
        gtCount: Int,
        predCount: Int
    ) -> NarlReportEpisodeEntry {
        NarlReportEpisodeEntry(
            episodeId: episodeId,
            podcastId: "test",
            show: "testShow",
            config: config,
            isExcluded: false,
            exclusionReason: nil,
            groundTruthWindowCount: gtCount,
            predictedWindowCount: predCount,
            windowMetrics: [],
            secondLevel: NarlSecondLevelMetrics(
                truePositiveSeconds: 0, falsePositiveSeconds: 0, falseNegativeSeconds: 0,
                precision: 0, recall: 0, f1: 0),
            lexicalInjectionAdds: 0,
            priorShiftAdds: 0,
            hasShadowCoverage: false
        )
    }

    /// Produce a default pipelines map (empty pred + empty gt per entry) so
    /// the aggregator has something to aggregate. Tests that care about real
    /// metric values build their own map inline.
    private static func pipelinesByEpisodeId(
        for traces: [FrozenTrace],
        entries: [NarlReportEpisodeEntry]
    ) -> [String: [(pred: [NarlTimeRange], gt: [NarlTimeRange])]] {
        var out: [String: [(pred: [NarlTimeRange], gt: [NarlTimeRange])]] = [:]
        for entry in entries {
            out["\(entry.episodeId)|\(entry.config)", default: []]
                .append((pred: [], gt: []))
        }
        return out
    }
}
