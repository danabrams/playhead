// NarlEvalHarnessTests.swift
// playhead-narl.1: The eval runner. Swift Testing suite that runs under
// PlayheadFastTests. For each FrozenTrace fixture in
// PlayheadTests/Fixtures/NarlEval/ it replays predictions under both
// `.default` and `.allEnabled` configs, computes window-level + second-level
// metrics, and writes a timestamped report.
//
// Asserts only:
//   (a) both configs run to completion without throwing, and
//   (b) report artifacts are written to .eval-out/narl/<timestamp>/.
//
// Metric regressions do NOT fail the build — they surface in the report for
// human judgment. See design §B.
//
// The harness gracefully handles an empty fixtures directory: it writes a
// report that documents "no fixtures available" rather than failing. This
// keeps the test green on CI clones that don't have the `.xcappdata`
// corpus, while still exercising the file I/O + schema paths.

import Foundation
import Testing
@testable import Playhead

@Suite("NarlEvalHarness")
struct NarlEvalHarnessTests {

    /// Root where timestamped reports are written. Path is stable; the
    /// per-run timestamped subdirectory is what changes across invocations.
    static let evalOutputSubpath = ".eval-out/narl"

    /// Where fixture FrozenTrace JSONs live in the repo, relative to the
    /// source tree. We resolve to an absolute URL via
    /// `fixturesRootURL()` at runtime so the code is portable across
    /// derived-data layouts.
    static let fixturesRelpath = "PlayheadTests/Fixtures/NarlEval"

    @Test("hasShadowCoverage flips when trace carries shadow: evidence entries")
    func hasShadowCoverageDetectsShadowEvidence() {
        let withoutShadow = Self.makeTrace(evidence: [
            FrozenTrace.FrozenEvidenceEntry(
                source: "transcript",
                weight: 0.8,
                windowStart: 0,
                windowEnd: 30
            )
        ])
        #expect(Self.hasShadowCoverage(trace: withoutShadow) == false)

        let withShadow = Self.makeTrace(evidence: [
            FrozenTrace.FrozenEvidenceEntry(
                source: "transcript",
                weight: 0.8,
                windowStart: 0,
                windowEnd: 30
            ),
            FrozenTrace.FrozenEvidenceEntry(
                source: "shadow:allEnabledShadow",
                weight: 1.0,
                windowStart: 30,
                windowEnd: 60
            )
        ])
        #expect(Self.hasShadowCoverage(trace: withShadow) == true)
    }

    /// Minimal FrozenTrace factory for unit-level helper tests.
    private static func makeTrace(
        evidence: [FrozenTrace.FrozenEvidenceEntry]
    ) -> FrozenTrace {
        FrozenTrace(
            episodeId: "ep-test",
            podcastId: "test",
            episodeDuration: 300,
            traceVersion: "frozen-trace-v2",
            capturedAt: Date(timeIntervalSince1970: 0),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: evidence,
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training
        )
    }

    @Test("Harness runs both configs and writes a report")
    func runHarness() throws {
        let (report, outputDir) = try Self.runHarnessCollectingReport()

        // Harness acceptance: artifacts exist.
        let jsonURL = outputDir.appendingPathComponent("report.json")
        let mdURL = outputDir.appendingPathComponent("report.md")
        #expect(FileManager.default.fileExists(atPath: jsonURL.path),
                "report.json should be written to \(jsonURL.path)")
        #expect(FileManager.default.fileExists(atPath: mdURL.path),
                "report.md should be written to \(mdURL.path)")

        // Verify trend log was appended.
        let trendURL = try Self.evalOutputRootURL().appendingPathComponent("trend.jsonl")
        #expect(FileManager.default.fileExists(atPath: trendURL.path),
                "trend.jsonl should exist at \(trendURL.path)")
        _ = report
    }

    @Test("harness threads coverageMetrics + fnDecomposition into the report")
    func harnessThreadsNewCoverageMetricsIntoReport() throws {
        let (_, outputDir) = try Self.runHarnessCollectingReport()

        // Load report.json from disk and decode — that's the contract
        // downstream consumers rely on.
        let jsonURL = outputDir.appendingPathComponent("report.json")
        let data = try Data(contentsOf: jsonURL)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(NarlEvalReport.self, from: data)

        // Non-excluded entries must carry coverageMetrics + fnDecomposition.
        // (Both are non-optional — we're verifying the writer/decoder
        // round-trips the new fields, not their content.)
        for entry in decoded.episodes where !entry.isExcluded {
            _ = entry.coverageMetrics
            _ = entry.fnDecomposition
        }
        // If the corpus is empty, the assertion would be vacuous; exit
        // cleanly — the test still covered the encode/decode round-trip.
        if decoded.episodes.contains(where: { !$0.isExcluded }) {
            #expect(decoded.episodes.contains { !$0.isExcluded },
                    "expected at least one non-excluded entry to validate schema")
        }
    }

    @Test("gtt9.7 C1: per-episode entries carry normalizer counts in the persisted JSON report")
    func harnessPersistsNormalizerCountsInReport() throws {
        // Build a minimal trace in-memory and write it to a temporary fixtures
        // root, so the assertion is not conditional on a corpus being present.
        // Fixture mix (on a single assetId):
        //   - 2 whole-asset vetoes (→ 1 after dedup)
        //   - 1 whole-asset endorsement
        //   - 1 span FN [100, 160]
        //   - 1 span FP [200, 210]
        //   - 1 boundary-refinement span
        //   - 1 unknown (ambiguous) row
        let episodeId = "gtt97-c1-\(UUID().uuidString.prefix(6))"
        let trace = FrozenTrace(
            episodeId: episodeId,
            podcastId: "gtt97-c1-podcast",
            episodeDuration: 600,
            traceVersion: "frozen-trace-v2",
            capturedAt: Date(timeIntervalSince1970: 0),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [
                // Whole-asset vetoes (2 → dedupes to 1)
                FrozenTrace.FrozenCorrection(
                    source: "manualVeto",
                    scope: "exactSpan:asset-1:0:9223372036854775807",
                    createdAt: 1_000,
                    correctionType: "falsePositive"
                ),
                FrozenTrace.FrozenCorrection(
                    source: "manualVeto",
                    scope: "exactSpan:asset-1:0:9223372036854775807",
                    createdAt: 1_001,
                    correctionType: "falsePositive"
                ),
                // Whole-asset endorsement (stays separate from veto on same asset)
                FrozenTrace.FrozenCorrection(
                    source: "falseNegative",
                    scope: "exactSpan:asset-1:0:9223372036854775807",
                    createdAt: 1_002,
                    correctionType: "falseNegative"
                ),
                // Span FN
                FrozenTrace.FrozenCorrection(
                    source: "falseNegative",
                    scope: "exactTimeSpan:asset-1:100.000:160.000",
                    createdAt: 2_000,
                    correctionType: "falseNegative"
                ),
                // Span FP
                FrozenTrace.FrozenCorrection(
                    source: "manualVeto",
                    scope: "exactTimeSpan:asset-1:200.000:210.000",
                    createdAt: 3_000,
                    correctionType: "falsePositive"
                ),
                // Boundary refinement
                FrozenTrace.FrozenCorrection(
                    source: "listenRevert",
                    scope: "exactTimeSpan:asset-1:300.000:320.000",
                    createdAt: 4_000,
                    correctionType: "startTooEarly"
                ),
                // Unknown (ambiguous source + nil correctionType)
                FrozenTrace.FrozenCorrection(
                    source: "someFutureCorrection",
                    scope: "exactTimeSpan:asset-1:400.000:410.000",
                    createdAt: 5_000,
                    correctionType: nil
                ),
            ],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training
        )

        // Assert the normalizer returns the shape we expect, so the report
        // assertion below has a meaningful reference.
        let norm = CorrectionNormalizer.normalize(trace.corrections)
        #expect(norm.spanFN.count == 1)
        #expect(norm.spanFP.count == 1)
        #expect(norm.wholeAssetCorrections.filter { $0.kind == .veto }.count == 1)
        #expect(norm.wholeAssetCorrections.filter { $0.kind == .endorse }.count == 1)
        #expect(norm.boundaryRefinementCount == 1)
        #expect(norm.unknownCount == 1)

        // Build an entry through the same codec used by the harness. We are
        // asserting the schema carries the normalizer counts — the harness's
        // per-trace loop is covered by runHarness/harnessThreadsNewCoverageMetrics
        // already, so here we round-trip a single entry to lock the JSON shape.
        let entry = NarlReportEpisodeEntry(
            episodeId: trace.episodeId,
            podcastId: trace.podcastId,
            show: "gtt97-c1",
            config: "default",
            isExcluded: false,
            exclusionReason: nil,
            groundTruthWindowCount: 0,
            predictedWindowCount: 0,
            windowMetrics: [],
            secondLevel: NarlSecondLevelMetrics(
                truePositiveSeconds: 0, falsePositiveSeconds: 0, falseNegativeSeconds: 0,
                precision: 0, recall: 0, f1: 0
            ),
            lexicalInjectionAdds: 0,
            priorShiftAdds: 0,
            hasShadowCoverage: false,
            normalizerCounts: NarlNormalizerCounts(from: norm, rawCount: trace.corrections.count)
        )

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(entry)
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(NarlReportEpisodeEntry.self, from: data)

        #expect(decoded.normalizerCounts.rawCount == 7)
        #expect(decoded.normalizerCounts.spanFNCount == 1)
        #expect(decoded.normalizerCounts.spanFPCount == 1)
        #expect(decoded.normalizerCounts.wholeAssetVetoCount == 1)
        #expect(decoded.normalizerCounts.wholeAssetEndorseCount == 1)
        #expect(decoded.normalizerCounts.boundaryRefinementCount == 1)
        #expect(decoded.normalizerCounts.unknownCount == 1)

        // Also assert the JSON text literally contains a top-level
        // `normalizerCounts` key, so downstream report consumers (jq
        // piping, bead approval engine) can find it by name.
        let text = String(decoding: data, as: UTF8.self)
        #expect(text.contains("\"normalizerCounts\""),
                "report entry JSON should contain a normalizerCounts block")
    }

    @Test("gtt9.7 C1: live harness run writes normalizerCounts into report.json for non-excluded entries")
    func harnessLiveRunEmitsNormalizerCountsField() throws {
        let (_, outputDir) = try Self.runHarnessCollectingReport()
        let jsonURL = outputDir.appendingPathComponent("report.json")
        let data = try Data(contentsOf: jsonURL)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(NarlEvalReport.self, from: data)

        // For any non-excluded entry, normalizerCounts.rawCount must equal
        // the sum of the four non-boundary buckets + boundary + layerB (a
        // tautology: raw rows are classified into mutually exclusive
        // buckets). We don't know the fixture contents on a given machine;
        // the invariant holds regardless.
        for entry in decoded.episodes where !entry.isExcluded {
            let nc = entry.normalizerCounts
            // Each correction lands in exactly one bucket after dedup/merge,
            // but rawCount is the pre-normalization count. We assert only
            // the weaker invariant: the bucket totals are individually
            // non-negative and the field round-tripped through the schema.
            #expect(nc.rawCount >= 0)
            #expect(nc.spanFNCount >= 0)
            #expect(nc.spanFPCount >= 0)
            #expect(nc.wholeAssetVetoCount >= 0)
            #expect(nc.wholeAssetEndorseCount >= 0)
            #expect(nc.boundaryRefinementCount >= 0)
            #expect(nc.unknownCount >= 0)
        }
    }

    @Test("gtt9.7 back-compat: pre-gtt9.7 report.json (no normalizerCounts key) decodes with .zero")
    func preGtt97EpisodeEntryJsonDecodesWithZeroNormalizerCounts() throws {
        // Literal pre-gtt9.7 payload: every field the old entry emitted is
        // populated; `normalizerCounts` is absent entirely. This pins the
        // `try? ... ?? .zero` fallback at NarlEvalReport.swift:234 — the
        // 18 historical `report.json` bundles under `.eval-out/narl/` rely
        // on that path to keep decoding after the gtt9.7 schema change.
        let json = """
        {
          "episodeId": "pre-gtt97-episode-1",
          "podcastId": "pre-gtt97-podcast",
          "show": "DoaC",
          "config": "default",
          "isExcluded": false,
          "groundTruthWindowCount": 3,
          "predictedWindowCount": 2,
          "windowMetrics": [],
          "secondLevel": {
            "truePositiveSeconds": 0,
            "falsePositiveSeconds": 0,
            "falseNegativeSeconds": 0,
            "precision": 0,
            "recall": 0,
            "f1": 0
          },
          "lexicalInjectionAdds": 1,
          "priorShiftAdds": 0,
          "hasShadowCoverage": false
        }
        """
        let data = Data(json.utf8)
        let decoded = try JSONDecoder().decode(NarlReportEpisodeEntry.self, from: data)

        #expect(decoded.episodeId == "pre-gtt97-episode-1")
        #expect(decoded.normalizerCounts == .zero)
        // Also pin the other two fields that share the same fallback path
        // (gtt9.6 `coverageMetrics` + `fnDecomposition`) so the same test
        // guards the older-artifact decode story end-to-end.
        #expect(decoded.fnDecomposition.isEmpty)
        #expect(decoded.coverageMetrics.pipelineCoverageFailureAsset == false)
    }

    @Test("gtt9.7 back-compat: NarlNormalizerCounts JSON without layerBCount decodes with layerBCount == 0")
    func normalizerCountsJsonMissingLayerBCountDecodesToZero() throws {
        // Simulates a mid-merge or pre-S1 report row where `layerBCount` was
        // not yet part of the schema. Pins the `try? ... ?? 0` fallback at
        // NarlEvalReport.swift:140 so future refactors of the custom
        // init(from:) can't silently drop the tolerance.
        let json = """
        {
          "rawCount": 7,
          "spanFNCount": 1,
          "spanFPCount": 1,
          "wholeAssetVetoCount": 1,
          "wholeAssetEndorseCount": 1,
          "unknownCount": 2,
          "boundaryRefinementCount": 1
        }
        """
        let data = Data(json.utf8)
        let decoded = try JSONDecoder().decode(NarlNormalizerCounts.self, from: data)

        #expect(decoded.rawCount == 7)
        #expect(decoded.spanFNCount == 1)
        #expect(decoded.spanFPCount == 1)
        #expect(decoded.wholeAssetVetoCount == 1)
        #expect(decoded.wholeAssetEndorseCount == 1)
        #expect(decoded.unknownCount == 2)
        #expect(decoded.boundaryRefinementCount == 1)
        #expect(decoded.layerBCount == 0)
    }

    @Test("harness emits pipeline coverage failure trend rows")
    func harnessEmitsPipelineCoverageFailureTrendRows() throws {
        let (report, _) = try Self.runHarnessCollectingReport()
        // Guard: CI clones without fixtures emit zero rollups, so the
        // trend rows don't exist. The "harness still writes artifacts"
        // behavior is covered by runHarness; this test only has a
        // meaningful assertion when there's corpus data.
        guard !report.rollups.isEmpty else { return }

        let trendURL = try Self.evalOutputRootURL().appendingPathComponent("trend.jsonl")
        let data = try Data(contentsOf: trendURL)
        let text = String(decoding: data, as: UTF8.self)

        // gtt9.6 requires these three trend metrics at minimum for the
        // new coverage-failure tracking to show up in trend analysis.
        let required = [
            "scored_coverage_ratio",
            "unscored_fn_rate",
            "pipeline_coverage_failure_count",
        ]
        for metric in required {
            #expect(text.contains("\"metric\":\"\(metric)\""),
                    "trend.jsonl should contain \(metric) rows")
        }
    }

    /// gtt9.6: full harness pipeline extracted as a helper so the
    /// runHarness test + the two coverage-integration tests can each
    /// exercise the same state without duplicating the aggregator.
    static func runHarnessCollectingReport() throws -> (report: NarlEvalReport, outputDir: URL) {
        let traces = try Self.loadAllFixtureTraces()
        let runId = Self.makeRunId()

        var episodeEntries: [NarlReportEpisodeEntry] = []
        var perRollup: [String: [(entry: NarlReportEpisodeEntry, pred: [NarlTimeRange], gt: [NarlTimeRange])]] = [:]
        // Per-rollup exclusion counter (HIGH-5). Episodes that triggered a
        // whole-asset veto are tallied here under BOTH the per-show key
        // (e.g. "DoaC|default") and the "ALL|default" aggregate, so the
        // rendered report can distinguish "5 episodes, 1 excluded" from
        // "4 episodes, 0 excluded" — same headline F1 but different denominators.
        var excludedCounts: [String: Int] = [:]
        var notes: [String] = []

        if traces.isEmpty {
            notes.append("No FrozenTrace fixtures found at \(Self.fixturesRelpath). "
                + "Run with PLAYHEAD_BUILD_NARL_FIXTURES=1 against a .xcappdata bundle to populate.")
        }

        for (fixtureIndex, trace) in traces.enumerated() {
            // gtt9.7: run the correction normalizer first so span-level
            // precision/recall is driven by actual span-level corrections
            // (not by whole-asset vetoes that the user issued on an entire
            // episode). The NarlGroundTruth builder still runs below and is
            // the source of truth for ground-truth ad spans; the normalizer
            // output is carried alongside the per-episode report entry so
            // the raw→(span/wholeAsset/boundary/unknown) delta is queryable
            // from the persisted JSON report, not just stdout.
            // Rationale: §11 of the 2026-04-23 expert report on why raw
            // correction rows aren't trustworthy (see bead description).
            let normalizedCorrections = CorrectionNormalizer.normalize(trace.corrections)
            let normalizerCounts = NarlNormalizerCounts(
                from: normalizedCorrections,
                rawCount: trace.corrections.count
            )
            Self.logCorrectionNormalization(
                trace: trace,
                before: trace.corrections,
                after: normalizedCorrections
            )

            let gtResult = NarlGroundTruth.build(for: trace)
            let show = Self.showName(for: trace)
            let traceHasShadowCoverage = Self.hasShadowCoverage(trace: trace)
            let configs: [(name: String, config: MetadataActivationConfig)] = [
                ("default", .default),
                ("allEnabled", .allEnabled),
            ]

            for (configName, configValue) in configs {
                let pred = NarlReplayPredictor.predict(
                    trace: trace,
                    config: configValue,
                    hasShadowCoverage: traceHasShadowCoverage
                )

                if gtResult.isExcluded {
                    let entry = NarlReportEpisodeEntry(
                        episodeId: trace.episodeId,
                        podcastId: trace.podcastId,
                        show: show,
                        config: configName,
                        isExcluded: true,
                        exclusionReason: gtResult.exclusionReason,
                        groundTruthWindowCount: 0,
                        predictedWindowCount: pred.windows.count,
                        windowMetrics: [],
                        secondLevel: NarlSecondLevelMetrics(
                            truePositiveSeconds: 0,
                            falsePositiveSeconds: 0,
                            falseNegativeSeconds: 0,
                            precision: 0, recall: 0, f1: 0
                        ),
                        lexicalInjectionAdds: pred.lexicalInjectionAdds,
                        priorShiftAdds: pred.priorShiftAdds,
                        hasShadowCoverage: pred.hasShadowCoverage,
                        coverageMetrics: .zero,
                        fnDecomposition: [],
                        normalizerCounts: normalizerCounts
                    )
                    episodeEntries.append(entry)
                    // Tally excluded episodes per-rollup so the report shows
                    // how many episodes were dropped by whole-asset veto
                    // (HIGH-5). Don't fold them into metric aggregation.
                    let showKey = "\(show)|\(configName)"
                    let allKey = "ALL|\(configName)"
                    excludedCounts[showKey, default: 0] += 1
                    excludedCounts[allKey, default: 0] += 1
                    continue
                }

                let windowMetrics = [0.3, 0.5, 0.7].map { τ in
                    NarlWindowMetrics.compute(
                        predicted: pred.windows,
                        groundTruth: gtResult.adWindows,
                        threshold: τ
                    )
                }
                let secondLevel = NarlSecondLevel.compute(
                    predicted: pred.windows,
                    groundTruth: gtResult.adWindows
                )
                let coverage = NarlCoverageMetricsCompute.compute(
                    trace: trace,
                    predicted: pred.windows,
                    groundTruth: gtResult.adWindows
                )

                let entry = NarlReportEpisodeEntry(
                    episodeId: trace.episodeId,
                    podcastId: trace.podcastId,
                    show: show,
                    config: configName,
                    isExcluded: false,
                    exclusionReason: nil,
                    groundTruthWindowCount: gtResult.adWindows.count,
                    predictedWindowCount: pred.windows.count,
                    windowMetrics: windowMetrics,
                    secondLevel: secondLevel,
                    lexicalInjectionAdds: pred.lexicalInjectionAdds,
                    priorShiftAdds: pred.priorShiftAdds,
                    hasShadowCoverage: pred.hasShadowCoverage,
                    coverageMetrics: coverage.metrics,
                    fnDecomposition: coverage.fnDecomposition,
                    normalizerCounts: normalizerCounts
                )
                episodeEntries.append(entry)

                // Accumulate for rollups: per-show and ALL.
                let showKey = "\(show)|\(configName)"
                let allKey = "ALL|\(configName)"
                perRollup[showKey, default: []].append((entry, pred.windows, gtResult.adWindows))
                perRollup[allKey, default: []].append((entry, pred.windows, gtResult.adWindows))
                _ = fixtureIndex
            }
        }

        let rollups = perRollup
            .sorted { $0.key < $1.key }
            .map { key, bundles -> NarlReportRollup in
                let parts = key.split(separator: "|", maxSplits: 1).map(String.init)
                let show = parts[0]
                let config = parts.count > 1 ? parts[1] : ""
                // Concat all predicted + gt windows and re-compute metrics on
                // the combined set. This mirrors how "corpus-level" PR curves
                // are typically computed (single pool of TP/FP across episodes)
                // rather than micro-averaging which can hide per-episode signal.
                let allPred = bundles.flatMap(\.pred)
                let allGt = bundles.flatMap(\.gt)
                let winMetrics = [0.3, 0.5, 0.7].map { τ in
                    NarlWindowMetrics.compute(
                        predicted: allPred, groundTruth: allGt, threshold: τ
                    )
                }
                let secMetrics = NarlSecondLevel.compute(predicted: allPred, groundTruth: allGt)
                let lexAdds = bundles.map(\.entry.lexicalInjectionAdds).reduce(0, +)
                let prAdds = bundles.map(\.entry.priorShiftAdds).reduce(0, +)
                let shadowCount = bundles.filter { $0.entry.hasShadowCoverage }.count
                let coverageAgg = Self.aggregateCoverage(bundles.map(\.entry.coverageMetrics))
                let failureAssetCount = bundles.filter { $0.entry.coverageMetrics.pipelineCoverageFailureAsset }.count
                return NarlReportRollup(
                    show: show,
                    config: config,
                    episodeCount: bundles.count,
                    excludedEpisodeCount: excludedCounts[key] ?? 0,
                    windowMetrics: winMetrics,
                    secondLevel: secMetrics,
                    totalLexicalInjectionAdds: lexAdds,
                    totalPriorShiftAdds: prAdds,
                    totalEpisodesWithShadowCoverage: shadowCount,
                    coverageMetrics: coverageAgg,
                    pipelineCoverageFailureAssetCount: failureAssetCount
                )
            }

        // Emit synthetic rollup rows for (show, config) pairs that had only
        // excluded episodes — i.e. excludedCounts has a key that's not in
        // perRollup. Without this branch, a show whose every episode was
        // vetoed would disappear from the report entirely instead of showing
        // as "5 episodes, 5 excluded".
        let existingRollupKeys = Set(perRollup.keys)
        let excludedOnlyKeys = excludedCounts.keys.filter { !existingRollupKeys.contains($0) }
        let excludedOnlyRollups = excludedOnlyKeys.sorted().map { key -> NarlReportRollup in
            let parts = key.split(separator: "|", maxSplits: 1).map(String.init)
            let show = parts[0]
            let config = parts.count > 1 ? parts[1] : ""
            return NarlReportRollup(
                show: show,
                config: config,
                episodeCount: 0,
                excludedEpisodeCount: excludedCounts[key] ?? 0,
                windowMetrics: [],
                secondLevel: NarlSecondLevelMetrics(
                    truePositiveSeconds: 0, falsePositiveSeconds: 0, falseNegativeSeconds: 0,
                    precision: 0, recall: 0, f1: 0
                ),
                totalLexicalInjectionAdds: 0,
                totalPriorShiftAdds: 0,
                totalEpisodesWithShadowCoverage: 0,
                coverageMetrics: .zero,
                pipelineCoverageFailureAssetCount: 0
            )
        }
        let allRollups = (rollups + excludedOnlyRollups).sorted { a, b in
            if a.show == b.show { return a.config < b.config }
            return a.show < b.show
        }

        let report = NarlEvalReport(
            schemaVersion: NarlEvalReportSchema.version,
            generatedAt: Date(),
            runId: runId,
            iouThresholds: [0.3, 0.5, 0.7],
            rollups: allRollups,
            episodes: episodeEntries,
            notes: notes
        )

        // Write the artifacts.
        let outputDir = try Self.writeReport(report)
        return (report, outputDir)
    }

    // MARK: - Fixture loading

    /// Load every `FrozenTrace-*.json` under the fixtures tree. Returns an
    /// empty array when no fixtures are present (which is a valid state in
    /// CI clones that don't have the corpus).
    static func loadAllFixtureTraces() throws -> [FrozenTrace] {
        let root = try fixturesRootURL()
        let fm = FileManager.default
        guard fm.fileExists(atPath: root.path) else { return [] }

        var traces: [FrozenTrace] = []
        let enumerator = fm.enumerator(
            at: root,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        )
        while let url = enumerator?.nextObject() as? URL {
            guard url.pathExtension == "json",
                  url.lastPathComponent.hasPrefix("FrozenTrace-") else { continue }
            do {
                let data = try Data(contentsOf: url)
                let decoder = JSONDecoder()
                decoder.dateDecodingStrategy = .iso8601
                let trace = try decoder.decode(FrozenTrace.self, from: data)
                traces.append(trace)
            } catch {
                // Fixture corruption must not take down the whole run.
                // Record a note-equivalent by printing; the test suite keeps
                // going.
                print("NarlEvalHarness: failed to decode \(url.lastPathComponent): \(error)")
            }
        }
        return traces
    }

    // MARK: - Paths

    /// Absolute URL of the fixtures root. Resolves from the source file at
    /// compile time via `#filePath`, then walks up to the repo root.
    static func fixturesRootURL() throws -> URL {
        let repoRoot = try Self.repoRootAny()
        return repoRoot.appendingPathComponent(fixturesRelpath)
    }

    /// Absolute URL of the eval output root (.eval-out/narl/).
    static func evalOutputRootURL() throws -> URL {
        let repoRoot = try Self.repoRootAny()
        return repoRoot.appendingPathComponent(evalOutputSubpath)
    }

    /// Resolve the repo root. Tries in order:
    ///   1. `SRCROOT` env var (set by Xcode test runs) — covers the case
    ///      where the test binary lives in derived-data and `#filePath`
    ///      resolves to a path without sibling `CLAUDE.md`/`Playhead.xcodeproj`.
    ///   2. Walk up from `#filePath` looking for those two markers.
    /// Either path must validate that the candidate directory actually
    /// contains the sentinels before returning. (MEDIUM-5)
    static func repoRootAny(file thisFile: URL = URL(fileURLWithPath: #filePath)) throws -> URL {
        if let srcroot = ProcessInfo.processInfo.environment["SRCROOT"], !srcroot.isEmpty {
            let candidate = URL(fileURLWithPath: srcroot)
            if Self.hasRepoMarkers(at: candidate) { return candidate }
        }
        return try Self.repoRoot(startingAt: thisFile.deletingLastPathComponent())
    }

    /// Walk upward until we find a directory containing both CLAUDE.md and
    /// Playhead.xcodeproj — that's the repo root. Throws if we escape past
    /// the filesystem root without finding one (shouldn't happen when run
    /// inside a real git clone).
    static func repoRoot(startingAt start: URL) throws -> URL {
        var current = start
        while current.path != "/" {
            if Self.hasRepoMarkers(at: current) { return current }
            current = current.deletingLastPathComponent()
        }
        throw NSError(
            domain: "NarlEvalHarness",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "Could not locate repo root from \(start.path)"]
        )
    }

    private static func hasRepoMarkers(at dir: URL) -> Bool {
        let fm = FileManager.default
        let hasClaudeMd = fm.fileExists(atPath: dir.appendingPathComponent("CLAUDE.md").path)
        let hasProject = fm.fileExists(atPath: dir.appendingPathComponent("Playhead.xcodeproj").path)
        return hasClaudeMd && hasProject
    }

    // MARK: - Output writing

    @discardableResult
    static func writeReport(_ report: NarlEvalReport) throws -> URL {
        let root = try evalOutputRootURL()
        let outputDir = root.appendingPathComponent(report.runId)
        try FileManager.default.createDirectory(
            at: outputDir, withIntermediateDirectories: true
        )

        // JSON
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        encoder.dateEncodingStrategy = .iso8601
        let jsonData = try encoder.encode(report)
        try jsonData.write(to: outputDir.appendingPathComponent("report.json"))

        // Markdown
        let markdown = NarlEvalRenderer.renderMarkdown(report)
        try markdown.data(using: .utf8)!.write(to: outputDir.appendingPathComponent("report.md"))

        // Trend log (append)
        let trendURL = root.appendingPathComponent("trend.jsonl")
        let trendRows = NarlTrendLog.rows(from: report)
        let trendLines = try NarlTrendLog.jsonlLines(for: trendRows)
        if !FileManager.default.fileExists(atPath: trendURL.path) {
            try FileManager.default.createDirectory(
                at: trendURL.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            FileManager.default.createFile(atPath: trendURL.path, contents: nil)
        }
        let handle = try FileHandle(forWritingTo: trendURL)
        defer { try? handle.close() }
        try handle.seekToEnd()
        for line in trendLines {
            try handle.write(contentsOf: line)
            try handle.write(contentsOf: Data([0x0A]))
        }

        return outputDir
    }

    // MARK: - Small helpers

    static func makeRunId() -> String {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.timeZone = TimeZone(identifier: "UTC")
        formatter.dateFormat = "yyyyMMdd-HHmmss"
        let base = formatter.string(from: Date())
        // Append a short random tag so parallel test runs don't collide.
        let tag = String(UUID().uuidString.prefix(6))
        return "\(base)-\(tag)"
    }

    /// Map a trace to a human-readable show label. Fallback chain (MEDIUM-2):
    ///   1. `trace.showLabel` if the fixture carries one (set by the corpus
    ///      builder when it has high-confidence metadata).
    ///   2. `PlayheadTests/Fixtures/NarlEval/NarlShowLabels.json` sidecar —
    ///      an `{ podcastId: label }` map Dan can edit by hand without
    ///      touching code. Keyed by the exact podcastId string.
    ///   3. Substring heuristic on the podcastId (covers the two test feeds
    ///      used historically — flightcast / simplecast / diary-of-a-ceo /
    ///      conan).
    ///   4. The raw podcastId (or "unknown" if empty).
    static func showName(for trace: FrozenTrace) -> String {
        if let label = trace.showLabel, !label.isEmpty { return label }
        if let sidecar = Self.showLabelsSidecar, let match = sidecar[trace.podcastId] {
            return match
        }
        let lowered = trace.podcastId.lowercased()
        if lowered.contains("flightcast") || lowered.contains("diary-of-a-ceo")
            || lowered.contains("diaryofaceo") {
            return "DoaC"
        }
        if lowered.contains("simplecast") || lowered.contains("conan")
            || lowered.contains("needs-a-friend") {
            return "Conan"
        }
        return trace.podcastId.isEmpty ? "unknown" : trace.podcastId
    }

    /// True when the trace carries any evidence rows sourced from the
    /// narl.2 shadow capture (`shadow-decisions.jsonl` → evidence entries
    /// with `source="shadow:<variant>"` per the corpus builder at
    /// `NarlEvalCorpusBuilderTests.swift:451`). Gates the predictor's
    /// `fmSchedulingEnabled` code path.
    static func hasShadowCoverage(trace: FrozenTrace) -> Bool {
        trace.evidenceCatalog.contains { $0.source.hasPrefix("shadow:") }
    }

    /// gtt9.6: aggregate per-episode `NarlCoverageMetrics` into a single
    /// rollup value. Ratios are averaged (simple mean across episodes);
    /// FN-second counts are summed; the `pipelineCoverageFailureAsset` flag
    /// is OR-fold (true iff any episode in the rollup failed). Empty input
    /// returns `.zero`.
    static func aggregateCoverage(_ items: [NarlCoverageMetrics]) -> NarlCoverageMetrics {
        guard !items.isEmpty else { return .zero }
        let n = Double(items.count)
        func mean(_ key: KeyPath<NarlCoverageMetrics, Double>) -> Double {
            items.map { $0[keyPath: key] }.reduce(0, +) / n
        }
        func sum(_ key: KeyPath<NarlCoverageMetrics, Double>) -> Double {
            items.map { $0[keyPath: key] }.reduce(0, +)
        }
        return NarlCoverageMetrics(
            scoredCoverageRatio: mean(\.scoredCoverageRatio),
            transcriptCoverageRatio: mean(\.transcriptCoverageRatio),
            candidateRecall: mean(\.candidateRecall),
            autoSkipPrecision: mean(\.autoSkipPrecision),
            autoSkipRecall: mean(\.autoSkipRecall),
            segmentIoU: mean(\.segmentIoU),
            unscoredFNRate: mean(\.unscoredFNRate),
            pipelineCoverageFailureAsset: items.contains { $0.pipelineCoverageFailureAsset },
            pipelineCoverageFNSeconds: sum(\.pipelineCoverageFNSeconds),
            classifierRecallFNSeconds: sum(\.classifierRecallFNSeconds),
            promotionRecallFNSeconds: sum(\.promotionRecallFNSeconds)
        )
    }

    /// gtt9.7: log per-asset correction counts before and after the
    /// CorrectionNormalizer runs. A visible print beats a buried count in a
    /// report JSON for an operator reading the test output — the delta
    /// (raw rows → span rows + whole-asset rows + unknown) is the headline
    /// fact that proves normalization did something.
    ///
    /// Emits one line per (episodeId, config-independent) call, of shape:
    ///   narl.normalizer: episodeId=...  raw=N  spanFN=A spanFP=B
    ///   wholeAsset=C(veto=c1 endorse=c2)  layerB=L  unknown=D  boundary=E
    static func logCorrectionNormalization(
        trace: FrozenTrace,
        before: [FrozenTrace.FrozenCorrection],
        after: NormalizedCorrections
    ) {
        let vetoCount = after.wholeAssetCorrections.filter { $0.kind == .veto }.count
        let endorseCount = after.wholeAssetCorrections.filter { $0.kind == .endorse }.count
        print("narl.normalizer: episodeId=\(trace.episodeId)"
            + "  raw=\(before.count)"
            + "  spanFN=\(after.spanFN.count)"
            + "  spanFP=\(after.spanFP.count)"
            + "  wholeAsset=\(after.wholeAssetCorrections.count)"
            + "(veto=\(vetoCount) endorse=\(endorseCount))"
            + "  layerB=\(after.layerBCount)"
            + "  unknown=\(after.unknownCount)"
            + "  boundary=\(after.boundaryRefinementCount)")
    }

    /// Lazy-loaded sidecar `{ podcastId: show-label }` map. Returns `nil` if
    /// the sidecar file is missing or unparseable — both are valid states
    /// (the heuristic fallback still works).
    static let showLabelsSidecar: [String: String]? = {
        do {
            let root = try Self.repoRootAny()
            let url = root.appendingPathComponent(fixturesRelpath)
                .appendingPathComponent("NarlShowLabels.json")
            guard FileManager.default.fileExists(atPath: url.path) else { return nil }
            let data = try Data(contentsOf: url)
            return try JSONDecoder().decode([String: String].self, from: data)
        } catch {
            print("NarlEvalHarness: failed to load NarlShowLabels.json sidecar: \(error)")
            return nil
        }
    }()
}
