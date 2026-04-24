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

        // gtt9.8 lifecycle log: optional. Pre-9.8 bundles won't have this
        // file — we gracefully degrade to nil per-asset.
        let lifecycleLogURL = documentsDir.appendingPathComponent("asset-lifecycle-log.jsonl")
        let lifecycleSummaries = fm.fileExists(atPath: lifecycleLogURL.path)
            ? (try? Self.parseLifecycleLog(from: lifecycleLogURL)) ?? [:]
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
            let lifecycle = lifecycleSummaries[asset.id]

            let trace = Self.assembleTrace(
                asset: asset,
                decisions: decisions,
                corrections: corrections,
                decisionLog: decisionLog,
                shadow: shadow,
                lifecycle: lifecycle
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
        /// Raw value of production `CorrectionType` when present on the
        /// correction row (v2 corpus-export emits it alongside `source`).
        /// Preferred by NarlGroundTruth over substring inference on `source`.
        let correctionType: String?
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
                let correctionType = obj["correctionType"] as? String
                let createdAt = (obj["createdAt"] as? Double) ?? 0
                byAsset[asset, default: []].append(BuilderCorrection(
                    assetId: asset,
                    scope: scope,
                    source: source,
                    correctionType: correctionType,
                    createdAt: createdAt
                ))
            }
        }
        return byAsset
    }

    // MARK: - decision-log parsing (per-window evidence + fused confidence)

    struct BuilderDecisionLogEvidence {
        let source: String
        let weight: Double
        /// Per-entry classificationTrust from production DecisionLogger schema v2+.
        /// Defaults to 0 when the log row omits it (v1 fixtures).
        let classificationTrust: Double
    }

    struct BuilderDecisionLogEntry {
        let assetId: String
        let windowStart: Double
        let windowEnd: Double
        let evidence: [BuilderDecisionLogEvidence]
        let fusedConfidence: Double
        /// Production `finalDecision.action` string (e.g. "autoSkipEligible",
        /// "markOnly", "suppressed"). Used to derive `isAdUnderDefault` on
        /// the FrozenWindowScore without needing a separate flag.
        let finalAction: String
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
            var ev: [BuilderDecisionLogEvidence] = []
            if let evList = obj["evidence"] as? [[String: Any]] {
                for e in evList {
                    let source = (e["source"] as? String) ?? "unknown"
                    let weight = (e["weight"] as? Double)
                        ?? ((e["weight"] as? NSNumber).map(\.doubleValue))
                        ?? 0
                    let trust = (e["classificationTrust"] as? Double)
                        ?? ((e["classificationTrust"] as? NSNumber).map(\.doubleValue))
                        ?? 0
                    ev.append(BuilderDecisionLogEvidence(
                        source: source,
                        weight: weight,
                        classificationTrust: trust
                    ))
                }
            }
            let fused = (obj["fusedConfidence"] as? [String: Any]).flatMap {
                ($0["skipConfidence"] as? Double)
                    ?? (($0["skipConfidence"] as? NSNumber).map(\.doubleValue))
            } ?? 0
            let finalAction = (obj["finalDecision"] as? [String: Any])?["action"] as? String ?? ""
            byAsset[assetId, default: []].append(BuilderDecisionLogEntry(
                assetId: assetId,
                windowStart: start,
                windowEnd: end,
                evidence: ev,
                fusedConfidence: fused,
                finalAction: finalAction
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

    // MARK: - Lifecycle log parsing (gtt9.8 follow-up)

    /// Per-asset terminal snapshot of the gtt9.8 lifecycle log. The "terminal
    /// row" is the row with a non-null `terminalReason` when one exists
    /// (the asset reached completeFull / completeFeatureOnly / completeAbandoned
    /// etc.); otherwise we fall back to the latest-timestamp row so stalled
    /// assets still surface their best-known state + coverage snapshot.
    ///
    /// Schema v1 fields consumed:
    ///   - analysisAssetID       (key)
    ///   - episodeDurationSec    → durationSec
    ///   - toState               → analysisState
    ///   - terminalReason        → terminalReason (optional)
    ///   - transcriptCoverageEndSec → transcriptCoverageEndSec (optional)
    ///   - featureCoverageEndSec    → featureCoverageEndSec (optional)
    ///   - timestamp             (used for latest-row fallback)
    struct BuilderLifecycleSummary: Equatable {
        let analysisAssetID: String
        let durationSec: Double
        let analysisState: String
        let terminalReason: String?
        let transcriptCoverageEndSec: Double?
        let featureCoverageEndSec: Double?
        let timestamp: Double
    }

    static func parseLifecycleLog(from url: URL) throws -> [String: BuilderLifecycleSummary] {
        // One pass: group rows by assetID, track best candidate per asset.
        // Priority: a row with non-null terminalReason wins; among terminal
        // rows the latest timestamp wins; absent a terminal row, the latest
        // timestamp overall wins.
        var best: [String: BuilderLifecycleSummary] = [:]
        var bestHasTerminal: [String: Bool] = [:]

        try Self.forEachJSONL(url: url) { obj in
            guard let assetID = obj["analysisAssetID"] as? String,
                  !assetID.isEmpty else { return }
            let duration = (obj["episodeDurationSec"] as? Double)
                ?? ((obj["episodeDurationSec"] as? NSNumber).map(\.doubleValue))
                ?? 0
            let toState = (obj["toState"] as? String) ?? ""
            let terminalReason = obj["terminalReason"] as? String
            let transcriptEnd = (obj["transcriptCoverageEndSec"] as? Double)
                ?? ((obj["transcriptCoverageEndSec"] as? NSNumber).map(\.doubleValue))
            let featureEnd = (obj["featureCoverageEndSec"] as? Double)
                ?? ((obj["featureCoverageEndSec"] as? NSNumber).map(\.doubleValue))
            let timestamp = (obj["timestamp"] as? Double)
                ?? ((obj["timestamp"] as? NSNumber).map(\.doubleValue))
                ?? 0

            let candidate = BuilderLifecycleSummary(
                analysisAssetID: assetID,
                durationSec: duration,
                analysisState: toState,
                terminalReason: terminalReason,
                transcriptCoverageEndSec: transcriptEnd,
                featureCoverageEndSec: featureEnd,
                timestamp: timestamp
            )
            let hasTerminal = (terminalReason != nil)
            let prior = best[assetID]
            let priorHasTerminal = bestHasTerminal[assetID] ?? false

            if prior == nil {
                best[assetID] = candidate
                bestHasTerminal[assetID] = hasTerminal
                return
            }
            // Terminal rows always beat non-terminal rows.
            if hasTerminal && !priorHasTerminal {
                best[assetID] = candidate
                bestHasTerminal[assetID] = true
                return
            }
            if !hasTerminal && priorHasTerminal {
                return
            }
            // Same terminal class → later timestamp wins.
            if let prior, candidate.timestamp > prior.timestamp {
                best[assetID] = candidate
                bestHasTerminal[assetID] = hasTerminal
            }
        }
        return best
    }

    // MARK: - Assemble FrozenTrace

    static func assembleTrace(
        asset: BuilderAsset,
        decisions: [BuilderDecision],
        corrections: [BuilderCorrection],
        decisionLog: [BuilderDecisionLogEntry],
        shadow: [BuilderShadowEntry],
        lifecycle: BuilderLifecycleSummary? = nil
    ) -> FrozenTrace {
        // Baseline span confidence is sourced from the decision-log row that
        // most-overlaps the span (HIGH-4). Promoted spans are always `isAd=true`
        // by construction (corpus-export only persists promoted rows), but we
        // no longer hardcode the confidence scalar — we use the real
        // `fusedConfidence.skipConfidence` so the predictor's counterfactual
        // replay has the correct input magnitude. Fallback when no
        // decision-log row overlaps: use the shifted-midpoint floor (0.22) as
        // the honest lower bound for a promoted span, NOT a made-up 0.9.
        let baseline: [ReplaySpanDecision] = decisions.map { d in
            let bestConf = bestOverlappingConfidence(decisionLog: decisionLog, start: d.startTime, end: d.endTime)
            return ReplaySpanDecision(
                startTime: d.startTime,
                endTime: d.endTime,
                confidence: bestConf,
                isAd: true,
                sourceTag: "baseline"
            )
        }

        // Evidence catalog = flatten the decision-log evidence entries for
        // this asset. Each row contributes one FrozenEvidenceEntry per source.
        // HIGH-6: thread classificationTrust through to the frozen entry so
        // the predictor can honestly apply the prior-shift and lexical-
        // injection gates using per-window trust values.
        var evidence: [FrozenTrace.FrozenEvidenceEntry] = []
        for entry in decisionLog {
            for e in entry.evidence {
                evidence.append(FrozenTrace.FrozenEvidenceEntry(
                    source: e.source,
                    weight: e.weight,
                    windowStart: entry.windowStart,
                    windowEnd: entry.windowEnd,
                    classificationTrust: e.classificationTrust
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
                createdAt: c.createdAt,
                correctionType: c.correctionType
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
                policyAction: entry.finalAction.isEmpty ? "skip" : entry.finalAction,
                explanationJSON: nil
            )
        }

        // Window scores (v2): one per decisionLog row. classificationTrust is
        // the max across evidence entries for this window (mirrors how
        // MetadataPriorShift aggregates trust via an episode-level max before
        // deciding the midpoint shift). hasMetadataEvidence fires when any
        // evidence source starts with "metadata" (production's
        // MetadataLexiconInjector / MetadataPriorShift both key off that
        // string). isAdUnderDefault is derived from the production final
        // action — autoSkip or markOnly means the .default path accepted it
        // as an ad.
        let windowScores: [FrozenTrace.FrozenWindowScore] = decisionLog.map { entry in
            let trust = entry.evidence.map(\.classificationTrust).max() ?? 0
            let hasMetadata = entry.evidence.contains { $0.source.hasPrefix("metadata") }
            let action = entry.finalAction.lowercased()
            let isAdUnderDefault = action.contains("autoskip")
                || action.contains("markonly")
                || action.contains("skip") && !action.contains("suppress")
            return FrozenTrace.FrozenWindowScore(
                windowStart: entry.windowStart,
                windowEnd: entry.windowEnd,
                fusedSkipConfidence: entry.fusedConfidence,
                classificationTrust: trust,
                hasMetadataEvidence: hasMetadata,
                isAdUnderDefault: isAdUnderDefault
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
            holdoutDesignation: .training,
            windowScores: windowScores,
            durationSec: lifecycle?.durationSec,
            analysisState: lifecycle?.analysisState,
            terminalReason: lifecycle?.terminalReason,
            fastTranscriptCoverageEndTime: lifecycle?.transcriptCoverageEndSec,
            featureCoverageEndTime: lifecycle?.featureCoverageEndSec
        )
    }

    /// Find the decision-log window that most overlaps the given time span
    /// and return its fusedSkipConfidence. Honest fallback (0.22, matching
    /// the shifted-midpoint floor for a promoted span) when no row overlaps.
    static func bestOverlappingConfidence(
        decisionLog: [BuilderDecisionLogEntry],
        start: Double,
        end: Double
    ) -> Double {
        guard !decisionLog.isEmpty, end > start else { return 0.22 }
        var bestOverlap: Double = 0
        var bestConf: Double = 0.22
        for entry in decisionLog {
            let lo = max(start, entry.windowStart)
            let hi = min(end, entry.windowEnd)
            guard hi > lo else { continue }
            let overlap = hi - lo
            if overlap > bestOverlap {
                bestOverlap = overlap
                bestConf = entry.fusedConfidence
            }
        }
        return bestConf
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
