// NarlEvalCorpusBuilderTests.swift
// playhead-narl.1: Env-gated corpus builder. Converts a `.xcappdata` bundle
// into one FrozenTrace-<episodeId>.json per episode under
// PlayheadTests/Fixtures/NarlEval/<YYYY-MM-DD>/.
//
// Usage:
//   PLAYHEAD_BUILD_NARL_FIXTURES=1 \
//   PLAYHEAD_NARL_XCAPPDATA=/path/to/export.xcappdata \
//     xcodebuild test -scheme Playhead -testPlan PlayheadFastTests ...
//
// Skips silently when the env var is not "1". This keeps the runtime in the
// default FastTests run at zero (no-op guard).
//
// Source files inside the .xcappdata/AppData/Documents/ directory:
//   - decision-log.jsonl         (narL production; per-window decisions)
//   - corpus-export.<ts>.jsonl   (narE export; asset + decision + correction rows)
//   - shadow-decisions.jsonl     (narl.2 dual-run capture; OPTIONAL — schema-
//                                 versioned, parsed leniently so this bead
//                                 doesn't re-rev when narl.2 lands)
//
// We deliberately do NOT depend on analysis.sqlite here. Parsing sqlite would
// require a GRDB linkage + schema coupling; both files above are
// JSON-per-line and stable-enough to consume directly.

import Foundation
import Testing
@testable import Playhead

@Suite("NarlEvalCorpusBuilder")
struct NarlEvalCorpusBuilderTests {

    private static var envGateOpen: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_BUILD_NARL_FIXTURES"] == "1"
    }

    /// Path to the .xcappdata bundle (or the unpacked Documents/ directory) to
    /// read. Defaults to the dev path.
    private static var xcappdataPath: String {
        ProcessInfo.processInfo.environment["PLAYHEAD_NARL_XCAPPDATA"]
            ?? "\(NSHomeDirectory())/Downloads/playhead-export.xcappdata"
    }

    @Test(
        "Build FrozenTrace fixtures from .xcappdata bundle",
        .enabled(if: NarlEvalCorpusBuilderTests.envGateOpen)
    )
    func buildFixtures() throws {
        let bundleURL = URL(fileURLWithPath: Self.xcappdataPath)
        let fm = FileManager.default
        guard fm.fileExists(atPath: bundleURL.path) else {
            Issue.record("xcappdata bundle not found at \(bundleURL.path)")
            return
        }

        let documentsDir = try Self.resolveDocumentsDir(in: bundleURL)

        // Parse corpus-export.jsonl (asset + decision + correction rows).
        let corpusExportURLs = try Self.locateCorpusExports(in: documentsDir)
        let assets = try Self.parseAssets(from: corpusExportURLs)
        let decisionsByAsset = try Self.parseDecisionSpans(from: corpusExportURLs)
        let correctionsByAsset = try Self.parseCorrections(from: corpusExportURLs)

        // decision-log.jsonl (per-window decisions, optional).
        let decisionLogURL = documentsDir.appendingPathComponent("decision-log.jsonl")
        let decisionLogEntries = fm.fileExists(atPath: decisionLogURL.path)
            ? try Self.parseDecisionLog(from: decisionLogURL)
            : [:]

        // shadow-decisions.jsonl (narl.2, optional). Parsed via schema-tolerant
        // reader so narl.1 ships without a hard binding.
        let shadowLogURL = documentsDir.appendingPathComponent("shadow-decisions.jsonl")
        let shadowEntries = fm.fileExists(atPath: shadowLogURL.path)
            ? (try? Self.parseShadowLog(from: shadowLogURL)) ?? [:]
            : [:]

        // Materialize output dir.
        let dateStamp = Self.dateStamp()
        let outDir = try Self.fixturesOutputDir(dateStamp: dateStamp)

        var written = 0
        for asset in assets {
            let decisions = decisionsByAsset[asset.id] ?? []
            let corrections = correctionsByAsset[asset.id] ?? []
            let decisionLog = decisionLogEntries[asset.id] ?? []
            let shadow = shadowEntries[asset.id] ?? []

            let trace = Self.assembleTrace(
                asset: asset,
                decisions: decisions,
                corrections: corrections,
                decisionLog: decisionLog,
                shadow: shadow
            )

            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            encoder.dateEncodingStrategy = .iso8601
            let data = try encoder.encode(trace)
            let fileURL = outDir.appendingPathComponent("FrozenTrace-\(asset.id).json")
            try data.write(to: fileURL)
            written += 1
        }

        #expect(written > 0, "At least one FrozenTrace fixture should be emitted")
        print("NarlEvalCorpusBuilder: wrote \(written) fixtures to \(outDir.path)")
    }

    // MARK: - Path resolution

    static func resolveDocumentsDir(in bundleURL: URL) throws -> URL {
        // A standard .xcappdata bundle contains AppData/Documents/. If the
        // caller passes a plain directory, accept it verbatim if it already
        // contains the expected files.
        let appDataDocs = bundleURL.appendingPathComponent("AppData/Documents")
        if FileManager.default.fileExists(atPath: appDataDocs.path) {
            return appDataDocs
        }
        return bundleURL
    }

    static func fixturesOutputDir(dateStamp: String) throws -> URL {
        let thisFile = URL(fileURLWithPath: #filePath)
        let repoRoot = try NarlEvalHarnessTests.repoRoot(
            startingAt: thisFile.deletingLastPathComponent()
        )
        let dir = repoRoot
            .appendingPathComponent("PlayheadTests/Fixtures/NarlEval")
            .appendingPathComponent(dateStamp)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    static func dateStamp() -> String {
        let f = DateFormatter()
        f.calendar = Calendar(identifier: .gregorian)
        f.timeZone = TimeZone(identifier: "UTC")
        f.dateFormat = "yyyy-MM-dd"
        return f.string(from: Date())
    }

    // MARK: - corpus-export parsing

    struct BuilderAsset {
        let id: String
        let episodeId: String
        let podcastId: String?
    }

    struct BuilderDecision {
        let assetId: String
        let firstAtomOrdinal: Int?
        let lastAtomOrdinal: Int?
        let startTime: Double
        let endTime: Double
    }

    struct BuilderCorrection {
        let assetId: String
        let scope: String
        let source: String?
        let createdAt: Double
    }

    static func locateCorpusExports(in dir: URL) throws -> [URL] {
        let fm = FileManager.default
        let contents = try fm.contentsOfDirectory(
            at: dir,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        )
        return contents.filter { $0.lastPathComponent.hasPrefix("corpus-export") }
    }

    static func parseAssets(from urls: [URL]) throws -> [BuilderAsset] {
        var seen = Set<String>()
        var out: [BuilderAsset] = []
        for url in urls {
            try Self.forEachJSONL(url: url) { obj in
                guard let t = obj["type"] as? String, t == "asset" else { return }
                guard let id = obj["analysisAssetId"] as? String else { return }
                if seen.contains(id) { return }
                seen.insert(id)
                let episodeId = (obj["episodeId"] as? String) ?? id
                let podcastId = obj["podcastId"] as? String
                out.append(BuilderAsset(id: id, episodeId: episodeId, podcastId: podcastId))
            }
        }
        return out
    }

    static func parseDecisionSpans(from urls: [URL]) throws -> [String: [BuilderDecision]] {
        var byAsset: [String: [BuilderDecision]] = [:]
        for url in urls {
            try Self.forEachJSONL(url: url) { obj in
                guard let t = obj["type"] as? String, t == "decision" else { return }
                guard let asset = obj["analysisAssetId"] as? String else { return }
                let firstOrd = (obj["firstAtomOrdinal"] as? Int)
                let lastOrd = (obj["lastAtomOrdinal"] as? Int)
                guard let start = (obj["startTime"] as? Double) ?? ((obj["startTime"] as? NSNumber).map(\.doubleValue)),
                      let end = (obj["endTime"] as? Double) ?? ((obj["endTime"] as? NSNumber).map(\.doubleValue))
                else { return }
                byAsset[asset, default: []].append(BuilderDecision(
                    assetId: asset, firstAtomOrdinal: firstOrd, lastAtomOrdinal: lastOrd,
                    startTime: start, endTime: end
                ))
            }
        }
        return byAsset
    }

    static func parseCorrections(from urls: [URL]) throws -> [String: [BuilderCorrection]] {
        var byAsset: [String: [BuilderCorrection]] = [:]
        for url in urls {
            try Self.forEachJSONL(url: url) { obj in
                guard let t = obj["type"] as? String, t == "correction" else { return }
                guard let asset = obj["analysisAssetId"] as? String else { return }
                guard let scope = obj["scope"] as? String else { return }
                let source = obj["source"] as? String
                let createdAt = (obj["createdAt"] as? Double) ?? 0
                byAsset[asset, default: []].append(BuilderCorrection(
                    assetId: asset, scope: scope, source: source, createdAt: createdAt
                ))
            }
        }
        return byAsset
    }

    // MARK: - decision-log parsing (per-window evidence + fused confidence)

    struct BuilderDecisionLogEntry {
        let assetId: String
        let windowStart: Double
        let windowEnd: Double
        let evidence: [(source: String, weight: Double)]
        let fusedConfidence: Double
    }

    static func parseDecisionLog(from url: URL) throws -> [String: [BuilderDecisionLogEntry]] {
        var byAsset: [String: [BuilderDecisionLogEntry]] = [:]
        try Self.forEachJSONL(url: url) { obj in
            // schemaVersion 1 used episodeID; v2 renamed to analysisAssetID.
            // Accept both.
            let assetId = (obj["analysisAssetID"] as? String)
                ?? (obj["episodeID"] as? String)
                ?? ""
            guard !assetId.isEmpty else { return }
            guard let bounds = obj["windowBounds"] as? [String: Any],
                  let start = (bounds["start"] as? Double) ?? ((bounds["start"] as? NSNumber).map(\.doubleValue)),
                  let end = (bounds["end"] as? Double) ?? ((bounds["end"] as? NSNumber).map(\.doubleValue))
            else { return }
            var ev: [(String, Double)] = []
            if let evList = obj["evidence"] as? [[String: Any]] {
                for e in evList {
                    let source = (e["source"] as? String) ?? "unknown"
                    let weight = (e["weight"] as? Double)
                        ?? ((e["weight"] as? NSNumber).map(\.doubleValue))
                        ?? 0
                    ev.append((source, weight))
                }
            }
            let fused = (obj["fusedConfidence"] as? [String: Any]).flatMap {
                ($0["skipConfidence"] as? Double)
                    ?? (($0["skipConfidence"] as? NSNumber).map(\.doubleValue))
            } ?? 0
            byAsset[assetId, default: []].append(BuilderDecisionLogEntry(
                assetId: assetId,
                windowStart: start,
                windowEnd: end,
                evidence: ev,
                fusedConfidence: fused
            ))
        }
        return byAsset
    }

    // MARK: - Shadow log parsing (schema-tolerant)

    /// Parsed shadow-decisions record. Uses only the fields narl.1 cares about
    /// so narl.2's schema can evolve without re-revving this bead.
    struct BuilderShadowEntry {
        let assetId: String
        let windowStart: Double
        let windowEnd: Double
        let configVariant: String      // e.g. "allEnabledShadow"
        let shadowIsAd: Bool?           // best-effort from whatever field exists
    }

    static func parseShadowLog(from url: URL) throws -> [String: [BuilderShadowEntry]] {
        var byAsset: [String: [BuilderShadowEntry]] = [:]
        try Self.forEachJSONL(url: url) { obj in
            // Lenient: support a few plausible field names so narl.2 has
            // latitude. Required: assetId, windowStart, windowEnd.
            let assetId = (obj["assetId"] as? String)
                ?? (obj["analysisAssetId"] as? String)
                ?? (obj["analysisAssetID"] as? String)
                ?? ""
            guard !assetId.isEmpty else { return }
            let start = (obj["windowStart"] as? Double)
                ?? ((obj["windowStart"] as? NSNumber).map(\.doubleValue))
                ?? 0
            let end = (obj["windowEnd"] as? Double)
                ?? ((obj["windowEnd"] as? NSNumber).map(\.doubleValue))
                ?? 0
            guard end > start else { return }
            let variant = (obj["configVariant"] as? String) ?? "allEnabledShadow"
            let shadowIsAd: Bool? = (obj["isAd"] as? Bool)
                ?? (obj["shadowIsAd"] as? Bool)
            byAsset[assetId, default: []].append(BuilderShadowEntry(
                assetId: assetId, windowStart: start, windowEnd: end,
                configVariant: variant, shadowIsAd: shadowIsAd
            ))
        }
        return byAsset
    }

    // MARK: - Assemble FrozenTrace

    static func assembleTrace(
        asset: BuilderAsset,
        decisions: [BuilderDecision],
        corrections: [BuilderCorrection],
        decisionLog: [BuilderDecisionLogEntry],
        shadow: [BuilderShadowEntry]
    ) -> FrozenTrace {
        // Baseline span decisions = decoded-span rows. isAd=true by construction
        // (corpus-export only emits promoted spans).
        let baseline = decisions.map { d in
            ReplaySpanDecision(
                startTime: d.startTime,
                endTime: d.endTime,
                confidence: 0.9,  // narE export does not preserve confidence;
                                  // use a conservative default for .default path.
                isAd: true,
                sourceTag: "baseline"
            )
        }

        // Evidence catalog = flatten the decision-log evidence entries for
        // this asset. Each row contributes one FrozenEvidenceEntry per source.
        var evidence: [FrozenTrace.FrozenEvidenceEntry] = []
        for entry in decisionLog {
            for (source, weight) in entry.evidence {
                evidence.append(FrozenTrace.FrozenEvidenceEntry(
                    source: source,
                    weight: weight,
                    windowStart: entry.windowStart,
                    windowEnd: entry.windowEnd
                ))
            }
        }

        // Atoms: narE corpus export doesn't include transcript atoms verbatim.
        // We cannot reconstruct them here without analysis.sqlite access.
        // Leave atoms[] empty; ordinal-based corrections will then be skipped
        // by NarlGroundTruth (documented behavior).
        let atoms: [FrozenTrace.FrozenAtom] = []

        let frozenCorrections = corrections.map { c in
            FrozenTrace.FrozenCorrection(
                source: c.source ?? "unknown",
                scope: c.scope,
                createdAt: c.createdAt
            )
        }

        // Decision events: one per decisionLog entry. We don't reconstruct the
        // full DecisionExplanation JSON here.
        let decisionEvents: [FrozenTrace.FrozenDecisionEvent] = decisionLog.map { entry in
            FrozenTrace.FrozenDecisionEvent(
                windowId: "\(asset.id)-\(entry.windowStart)",
                proposalConfidence: entry.fusedConfidence,
                skipConfidence: entry.fusedConfidence,
                eligibilityGate: "eligible",
                policyAction: "skip",
                explanationJSON: nil
            )
        }

        // Shadow entries are folded into evidenceCatalog with a synthetic
        // source="shadow:<variant>" so downstream consumers can opt in.
        var fullEvidence = evidence
        for s in shadow {
            fullEvidence.append(FrozenTrace.FrozenEvidenceEntry(
                source: "shadow:\(s.configVariant)",
                weight: (s.shadowIsAd ?? false) ? 1.0 : 0.0,
                windowStart: s.windowStart,
                windowEnd: s.windowEnd
            ))
        }

        // Episode duration: derive from the furthest-out decision or 3600 as fallback.
        let duration = (decisions.map(\.endTime) + decisionLog.map(\.windowEnd)).max() ?? 3600

        return FrozenTrace(
            episodeId: asset.episodeId,
            podcastId: asset.podcastId ?? "",
            episodeDuration: duration,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(),
            featureWindows: [],
            atoms: atoms,
            evidenceCatalog: fullEvidence,
            corrections: frozenCorrections,
            decisionEvents: decisionEvents,
            baselineReplaySpanDecisions: baseline,
            holdoutDesignation: .training
        )
    }

    // MARK: - JSONL iteration

    static func forEachJSONL(url: URL, _ handler: ([String: Any]) -> Void) throws {
        let data = try Data(contentsOf: url)
        guard let s = String(data: data, encoding: .utf8) else { return }
        for line in s.split(whereSeparator: { $0 == "\n" || $0 == "\r\n" }) {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.isEmpty { continue }
            guard let lineData = trimmed.data(using: .utf8) else { continue }
            guard let parsed = try? JSONSerialization.jsonObject(with: lineData),
                  let dict = parsed as? [String: Any] else { continue }
            handler(dict)
        }
    }
}
