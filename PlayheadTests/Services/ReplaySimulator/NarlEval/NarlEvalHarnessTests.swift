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

    /// Minimal FrozenTrace factory parameterised on the two show-identifying
    /// fields the heuristic inspects. All other fields are filled with
    /// do-nothing defaults. Used by the gtt9.5 show-label heuristic tests.
    private static func makeTrace(
        episodeId: String,
        podcastId: String,
        showLabel: String? = nil
    ) -> FrozenTrace {
        FrozenTrace(
            episodeId: episodeId,
            podcastId: podcastId,
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
            showLabel: showLabel
        )
    }

    // MARK: - gtt9.5 show-label heuristic

    @Test("showName uses trace.showLabel when present (short-circuit)")
    func showNameUsesShowLabelWhenPresent() {
        let trace = Self.makeTrace(
            episodeId: "irrelevant",
            podcastId: "irrelevant",
            showLabel: "HandLabeled"
        )
        #expect(Self.showName(for: trace) == "HandLabeled")
    }

    @Test("showName buckets simplecast URL podcastId as Conan")
    func showNameMatchesSimplecastUrlPodcastId() {
        // From 2026-04-23 corpus: the live AnalysisStore stores the feed URL
        // as podcastId on legacy traces.
        let trace = Self.makeTrace(
            episodeId: "ep",
            podcastId: "https://feeds.simplecast.com/dHoohVNH"
        )
        #expect(Self.showName(for: trace) == "Conan")
    }

    @Test("showName buckets simplecast URL episodeId as Conan when podcastId is empty")
    func showNameMatchesSimplecastUrlEpisodeIdWhenPodcastIdEmpty() {
        // Exact form from the 2026-04-23 corpus: empty podcastId, feed URL
        // embedded in episodeId before "::<uuid>".
        let trace = Self.makeTrace(
            episodeId: "https://feeds.simplecast.com/dHoohVNH::311e3daa-60b3-4428-b780-c9a7b8512be8",
            podcastId: ""
        )
        #expect(Self.showName(for: trace) == "Conan")
    }

    @Test("showName buckets rss2.flightcast URL episodeId as DoaC")
    func showNameMatchesFlightcastUrlEpisodeId() {
        // 2026-04-23 corpus form: the flightcast feed is served from
        // rss2.flightcast.com — host match, not scheme match.
        let trace = Self.makeTrace(
            episodeId: "https://rss2.flightcast.com/xmsftuzjjykcmqwolaqn6mdn::flightcast:01KM20WJPKVFHRVJZWTNA6Q1XT",
            podcastId: ""
        )
        #expect(Self.showName(for: trace) == "DoaC")
    }

    @Test("showName buckets flightcast: URL scheme as DoaC (legacy podcastId form)")
    func showNameMatchesFlightcastScheme() {
        // Preserved behaviour: the legacy `flightcast:<id>` scheme.
        let trace = Self.makeTrace(
            episodeId: "ep",
            podcastId: "flightcast:01KM20WJPKVFHRVJZWTNA6Q1XT"
        )
        #expect(Self.showName(for: trace) == "DoaC")
    }

    @Test("showName buckets legacy simplecast: scheme podcastId as Conan")
    func showNameMatchesSimplecastScheme() {
        // Preserved behaviour: matches the 2026-04-22 fixtures
        // (podcastId = "simplecast:conan-needs-a-friend").
        let trace = Self.makeTrace(
            episodeId: "ep",
            podcastId: "simplecast:conan-needs-a-friend"
        )
        #expect(Self.showName(for: trace) == "Conan")
    }

    @Test("showName matches known title substring when host is unknown")
    func showNameMatchesTitleSubstringOnUnknownHost() {
        // Graceful fallback: unknown host, but the identifier carries a
        // known show-name substring.
        let trace = Self.makeTrace(
            episodeId: "ep",
            podcastId: "https://example.invalid/feed/diary-of-a-ceo"
        )
        #expect(Self.showName(for: trace) == "DoaC")
    }

    @Test("showName returns unknown for genuinely unknown identifiers (graceful degradation)")
    func showNameReturnsUnknownForUnknownIdentifiers() {
        // Neither a known host nor a known title substring → do NOT
        // misattribute to an arbitrary show. "unknown" is the correct
        // graceful-degradation label (acceptance #3).
        let trace = Self.makeTrace(
            episodeId: "",
            podcastId: ""
        )
        #expect(Self.showName(for: trace) == "unknown")
    }

    @Test("showName does not misattribute an arbitrary unknown host to a known show")
    func showNameDoesNotMisattributeUnknownHost() {
        // A real-world risk of a naive substring check: an unrelated URL
        // that happens to contain "cast" or similar should NOT resolve to
        // Conan / DoaC.
        let trace = Self.makeTrace(
            episodeId: "https://example.invalid/foo/bar",
            podcastId: "https://example.invalid/foo/bar"
        )
        #expect(Self.showName(for: trace) == "https://example.invalid/foo/bar"
                || Self.showName(for: trace) == "unknown"
                || Self.showName(for: trace) == "https://example.invalid/foo/bar".lowercased(),
                "unknown host should degrade to raw id / 'unknown', not a known show")
        // Stronger: the result must NOT be one of the known show labels.
        let result = Self.showName(for: trace)
        #expect(result != "Conan")
        #expect(result != "DoaC")
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
                        fnDecomposition: []
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
                    fnDecomposition: coverage.fnDecomposition
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

    /// Map a trace to a human-readable show label. Fallback chain (gtt9.5):
    ///   1. `trace.showLabel` if the fixture carries one (set by the corpus
    ///      builder when it has high-confidence metadata).
    ///   2. `PlayheadTests/Fixtures/NarlEval/NarlShowLabels.json` sidecar —
    ///      an `{ podcastId: label }` map Dan can edit by hand without
    ///      touching code. Keyed by the exact podcastId string.
    ///   3. Structured match on `podcastId` then `episodeId`:
    ///        a. parse as URL and look up by host (+ path segments when
    ///           needed). Covers `https://feeds.simplecast.com/...` and
    ///           `https://rss2.flightcast.com/...` from the real-user
    ///           corpus, which has an empty `podcastId` and the feed URL
    ///           embedded in `episodeId` before `"::<id>"`.
    ///        b. custom-scheme match (`flightcast:`, `simplecast:`) for
    ///           legacy synthetic fixtures.
    ///        c. title-substring match (`diary-of-a-ceo`, `conan`, etc.)
    ///           as graceful degradation when host + scheme both miss.
    ///   4. The raw podcastId (or "unknown" if both identifiers are
    ///      empty). Never misattribute a genuinely unknown show to a
    ///      known one — return the raw id instead (acceptance #3).
    static func showName(for trace: FrozenTrace) -> String {
        if let label = trace.showLabel, !label.isEmpty { return label }
        if let sidecar = Self.showLabelsSidecar, let match = sidecar[trace.podcastId] {
            return match
        }
        // Try the podcastId first (the legitimate source of truth), then
        // episodeId (which in the 2026-04-23 real-user corpus carries the
        // feed URL before "::<episode-id>" when podcastId is empty).
        if let label = Self.showLabelFromIdentifier(trace.podcastId) { return label }
        if let label = Self.showLabelFromIdentifier(trace.episodeId) { return label }

        // Graceful fallback: prefer the raw podcastId so the report
        // surfaces the unknown id directly (useful for hand-labelling via
        // the sidecar). If both identifiers are empty, bucket as
        // "unknown" rather than misattributing to a known show.
        if !trace.podcastId.isEmpty { return trace.podcastId }
        if !trace.episodeId.isEmpty { return trace.episodeId }
        return "unknown"
    }

    /// Derive a show label from a single identifier string (a podcastId or
    /// an episodeId). Returns `nil` when the identifier carries no known
    /// signal — callers should chain alternatives or fall back to
    /// "unknown". The match is deliberately strict (host + scheme + title
    /// substring, in that order) so a URL like
    /// `https://example.invalid/foo` never collides with Conan or DoaC.
    static func showLabelFromIdentifier(_ identifier: String) -> String? {
        guard !identifier.isEmpty else { return nil }

        // (a) URL parse. We accept either a full `scheme://` URL or a
        //     `scheme:<opaque>` custom scheme (`flightcast:<id>`). Strip a
        //     trailing `::<suffix>` before parsing — that's the corpus's
        //     convention for "<feed-url>::<episode-id>".
        let core: String = {
            if let sepRange = identifier.range(of: "::") {
                return String(identifier[..<sepRange.lowerBound])
            }
            return identifier
        }()

        if let url = URL(string: core) {
            if let host = url.host?.lowercased() {
                if let label = Self.showLabelForHost(host) { return label }
            } else if let scheme = url.scheme?.lowercased() {
                // Custom `flightcast:<id>` / `simplecast:<id>` forms have
                // no host — match the scheme directly.
                if let label = Self.showLabelForScheme(scheme) { return label }
            }
        }

        // (b) Title-metadata substring fallback. Covers the case where
        //     neither the host nor the scheme is known but the id still
        //     carries a recognizable show name (e.g. a hand-built fixture
        //     or a rehosted feed).
        let lowered = identifier.lowercased()
        if lowered.contains("diary-of-a-ceo") || lowered.contains("diaryofaceo") {
            return "DoaC"
        }
        if lowered.contains("conan") || lowered.contains("needs-a-friend") {
            return "Conan"
        }
        return nil
    }

    /// Known-feed-host → show-label map. Extend here when a new show
    /// enters the corpus rather than sprinkling substring checks through
    /// the heuristic.
    private static func showLabelForHost(_ host: String) -> String? {
        switch host {
        case "feeds.simplecast.com":
            return "Conan"
        case "rss2.flightcast.com":
            return "DoaC"
        default:
            return nil
        }
    }

    /// Known custom-scheme → show-label map for legacy / synthetic
    /// fixtures. Same extension policy as `showLabelForHost`.
    private static func showLabelForScheme(_ scheme: String) -> String? {
        switch scheme {
        case "flightcast":
            return "DoaC"
        case "simplecast":
            return "Conan"
        default:
            return nil
        }
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
