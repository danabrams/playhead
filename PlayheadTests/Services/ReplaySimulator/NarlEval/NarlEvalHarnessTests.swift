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

    @Test("Harness runs both configs and writes a report")
    func runHarness() throws {
        let traces = try Self.loadAllFixtureTraces()
        let runId = Self.makeRunId()

        var episodeEntries: [NarlReportEpisodeEntry] = []
        var perRollup: [String: [(entry: NarlReportEpisodeEntry, pred: [NarlTimeRange], gt: [NarlTimeRange])]] = [:]
        var notes: [String] = []

        if traces.isEmpty {
            notes.append("No FrozenTrace fixtures found at \(Self.fixturesRelpath). "
                + "Run with PLAYHEAD_BUILD_NARL_FIXTURES=1 against a .xcappdata bundle to populate.")
        }

        for (fixtureIndex, trace) in traces.enumerated() {
            let gtResult = NarlGroundTruth.build(for: trace)
            let show = Self.showName(for: trace)
            let configs: [(name: String, config: MetadataActivationConfig)] = [
                ("default", .default),
                ("allEnabled", .allEnabled),
            ]

            for (configName, configValue) in configs {
                let pred = NarlReplayPredictor.predict(
                    trace: trace,
                    config: configValue,
                    hasShadowCoverage: false  // Phase 1: no shadow data until narl.2
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
                        hasShadowCoverage: pred.hasShadowCoverage
                    )
                    episodeEntries.append(entry)
                    // Don't fold excluded episodes into rollups.
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
                    hasShadowCoverage: pred.hasShadowCoverage
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
                return NarlReportRollup(
                    show: show,
                    config: config,
                    episodeCount: bundles.count,
                    excludedEpisodeCount: 0,
                    windowMetrics: winMetrics,
                    secondLevel: secMetrics,
                    totalLexicalInjectionAdds: lexAdds,
                    totalPriorShiftAdds: prAdds,
                    totalEpisodesWithShadowCoverage: shadowCount
                )
            }

        let report = NarlEvalReport(
            schemaVersion: NarlEvalReportSchema.version,
            generatedAt: Date(),
            runId: runId,
            iouThresholds: [0.3, 0.5, 0.7],
            rollups: rollups,
            episodes: episodeEntries,
            notes: notes
        )

        // Write the artifacts.
        let outputDir = try Self.writeReport(report)

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
        let thisFile = URL(fileURLWithPath: #filePath)
        let repoRoot = try Self.repoRoot(startingAt: thisFile.deletingLastPathComponent())
        return repoRoot.appendingPathComponent(fixturesRelpath)
    }

    /// Absolute URL of the eval output root (.eval-out/narl/).
    static func evalOutputRootURL() throws -> URL {
        let thisFile = URL(fileURLWithPath: #filePath)
        let repoRoot = try Self.repoRoot(startingAt: thisFile.deletingLastPathComponent())
        return repoRoot.appendingPathComponent(evalOutputSubpath)
    }

    /// Walk upward until we find a directory containing both CLAUDE.md and
    /// Playhead.xcodeproj — that's the repo root. Throws if we escape past
    /// the filesystem root without finding one (shouldn't happen when run
    /// inside a real git clone).
    static func repoRoot(startingAt start: URL) throws -> URL {
        var current = start
        while current.path != "/" {
            let hasClaudeMd = FileManager.default.fileExists(
                atPath: current.appendingPathComponent("CLAUDE.md").path
            )
            let hasProject = FileManager.default.fileExists(
                atPath: current.appendingPathComponent("Playhead.xcodeproj").path
            )
            if hasClaudeMd && hasProject { return current }
            current = current.deletingLastPathComponent()
        }
        throw NSError(
            domain: "NarlEvalHarness",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "Could not locate repo root from \(start.path)"]
        )
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

    /// Map a podcastId to a human-readable show name. Covers the two test
    /// shows explicitly; everything else uses the podcastId as-is.
    static func showName(for trace: FrozenTrace) -> String {
        // Heuristic: podcastId is a feed URL or a stable ID. We don't commit
        // test-data labels to production code, so we use substring match.
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
}
