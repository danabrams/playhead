// PipelineDumpLiveTests.swift
// One-shot exploratory pipeline-dump harness for the 9 NEW corpus episodes
// snapshotted under `TestFixtures/Corpus/Snapshots/manifest.json`. The
// ORIGINAL legacy dump lane this header describes is NOT an A/B and NOT a
// lift measurement — it runs the PRODUCTION `AdDetectionService.runBackfill`
// once per episode under the shipped `AdDetectionConfig.default` (every
// activation flag off, `fmBackfillMode: .full`, `chapterSignalMode: .off`)
// and dumps the persisted `AdWindow`s so the orchestrator can cross-validate
// them against the drafter's candidate spans.
//
// LANES: this file now hosts FOUR env-gated lanes. The CONTRACT and Output
// sections below describe the legacy dump lane; the other three are
// documented on their own test/capture methods:
//   * `PLAYHEAD_PIPELINE_DUMP=1` — the legacy snapshot dump (this header;
//     `testProductionPipelineDumpOnNewEpisodes`).
//   * `PLAYHEAD_PARTIAL_SILVER_BASELINE=1` — the immutable 27-asset baseline
//     (`capturePartialSilverProductionBaseline`).
//   * `PLAYHEAD_BASELINE_DEVICE_PREFLIGHT=1` — the bounded physical-device
//     transport check (`testPhysicalDeviceBaselinePreflight`).
//   * `PLAYHEAD_PIPELINE_DUMP_REDIFF=1` — playhead-xsdz.36.1: the
//     rediff-ACTIVATION TREATMENT dump, which IS the treatment half of a
//     lift measurement. Same per-episode setup, but the config is `.default`
//     with `rediffSlotOwnershipEnabled` flipped ON plus an injected
//     fresh-B-side provider, written to the DISTINCT
//     `playhead-dogfood-diagnostics-pipeline-dump-rediff-treatment.json` so
//     the orchestrator can diff treatment vs. baseline
//     (`captureRediffTreatmentDump`).
//
// CONTRACT (legacy dump lane; mirrors the sibling live harnesses in this
// directory):
//   * Env-gated. `PLAYHEAD_PIPELINE_DUMP=1` MUST be set in the test process
//     environment. The default `PlayheadFastTests` plan does NOT set it, so
//     the test body skips on Cmd-U (via `XCTSkipUnless`) — no FM, no audio
//     dependency triggered.
//   * Requires iOS 26+ at runtime (`#available`). The live FM
//     `BackfillJobRunner` is unavailable on older runtimes.
//   * READS the episode list from `TestFixtures/Corpus/Snapshots/manifest.json`
//     (the 9 NEW snapshots — no `CorpusAnnotation` golden files yet). For each
//     entry, replicates the SAME per-episode setup the merged harness uses
//     (`FragilityGateLiveABTests` / `LexicalScorerLiveABTests`):
//        1. Fresh `AnalysisStore`, `migrate()`, `insertAsset(...)`.
//        2. Load whisper transcripts via `CorpusTranscriptLoader` (same loader
//           the chapter-plan snapshot capture uses). Rebind chunks to the
//           per-episode asset id; `insertTranscriptChunks(...)`.
//        3. Decode audio shards via `AnalysisAudioService.decode(...)`, extract
//           feature windows via `FeatureExtractionService`, persist via
//           `insertFeatureWindows(...)`. (The CoveragePlanner + acoustic-break
//           snapping read these back during the FM phase, so they must be
//           present for the FM scan to behave like production.)
//        4. Seed `PodcastPlannerState` into the warmed `targetedWithAudit`
//           regime (5 full-rescan observations at 0.9 recall). Mirrors the
//           sibling-harness `seedTargetedWithAuditPlannerState(...)`; the
//           production-default FM-narrowing path is policy-gated on
//           `targetedWithAudit`.
//        5. Construct `AdDetectionService` with `config: .default` (literally
//           `AdDetectionConfig.default` — every activation flag off), inject a
//           LIVE `BackfillJobRunner` (real FM classifier against
//           `SystemLanguageModel.default`) baked with `NarrowingConfig.default`,
//           and attach a `FragilityDiagnosticObserver` ONLY to count decoded
//           spans. The observer is behavior-neutral (it never feeds back into
//           the decision path; see `FragilityDiagnosticObserver` contract); it
//           is reused here as the cheapest existing seam into "how many decoded
//           spans did the pipeline visit?" without any production change.
//        6. Run `runBackfill(...)`. After the run, fetch the persisted
//           `AdWindow`s via `store.fetchAdWindows(assetId:)` and emit them.
//   * Fail-loud guards mirror the siblings:
//        * Effective FM mode must be `.full` (no silent cohort demotion).
//        * Seeded planner must land in `.targetedWithAudit`.
//        * Missing transcript sidecar is a HARD failure (the snapshot
//          pipeline should have produced one for each manifest entry).
//        * Zero episodes scored when the env var is set is a HARD failure.
//   * NO production code change. The injected observer is the existing
//     `FragilityDiagnosticObserver` (already part of the shipped app) — only
//     the test target wires it.
//
// Output: a single git-ignored JSON dump at repo root,
// `playhead-dogfood-diagnostics-pipeline-dump-new9.json` (matches the
// `playhead-dogfood-diagnostics-*.json` pattern in `.gitignore`). Shape (see
// the task spec for the canonical schema):
//   {
//     "config": "...",
//     "runUtc": "ISO8601",
//     "episodes": [
//       { "episodeId": "...", "showSlug": "...", "publishDate": "...",
//         "audioFingerprint": "sha256:...",
//         "episodeDurationSeconds": 1234.5,
//         "candidateDecodedSpans": <int>,
//         "candidateDecodedSpanList": [
//           { "spanId": "...", "startTime": 123.4, "endTime": 234.5,
//             "proposalConfidence": 0.81, "skipConfidence": 0.78,
//             "fragilityScore": 0.42 }, ...
//         ],
//         "adWindows": [
//           { "startTime": 123.4, "endTime": 234.5,
//             "skipConfidence": 0.78,
//             "decisionState": "...",
//             "eligibilityGate": "...",
//             "wasSkipped": false,
//             // promotionTrack / boundaryRefinement* /
//             // spanFinalizerConstraintsFired keys appear ONLY when
//             // non-nil; default JSONEncoder strategy omits nil
//             // optionals, matching the pre-existing wire shape.
//             "boundaryRefinementStartAdjustment": -0.75,
//             "boundaryRefinementEndAdjustment": 1.25,
//             "spanFinalizerConstraintsFired": ["mergedWithAdjacent",
//                                               "policyOverrideApplied"] }, ...
//         ]
//       }, ...
//     ],
//     "summary": { "totalEpisodes": 9, "totalAdWindows": N, "perShow": {...} }
//   }
//
// `candidateDecodedSpanList` was added 2026-06-01 for playhead-4xqf
// (boundary-undersizing investigation). Pairs with rediff slot boundaries to
// answer: do the candidate spans cover the full ad slot (→ fusion/merge is
// the culprit), or are they themselves short (→ candidate gen is the
// culprit)? The list mirrors what `FragilityDiagnosticObserver` already
// records for every span; only the JSON encoding is new. The pre-existing
// `candidateDecodedSpans` int field is preserved for backward compatibility.
//
// `wasSkipped`, `boundaryRefinementStartAdjustment`, and
// `boundaryRefinementEndAdjustment` were added 2026-06-01 as a SECOND
// playhead-4xqf extension targeting the FUSION_DROP suspects from PR #207's
// code-path map.
//   • `wasSkipped` (Bool) — the AdWindow.wasSkipped flag persisted on the
//     store row. Captures the real playback signal independent of
//     `decisionState`: an eligibility-gate demotion to `blockedByEvidenceQuorum`
//     produces a `.candidate` decisionState, but the actual auto-skip toggle
//     lives on this flag. Pairing the two answers "did the demotion change
//     what the user would have heard?".
//   • `boundaryRefinementStartAdjustment` / `boundaryRefinementEndAdjustment`
//     (Double?) — the start/end deltas (seconds) that
//     `BoundaryRefiner.computeAdjustments` (legacy resolver, the fallback path
//     the live inline refiner at `AdDetectionService.swift:~3186-3260` uses
//     when `BracketAwareBoundaryRefiner` doesn't refine) returns when re-fed
//     the PERSISTED AdWindow bounds against the same `featureWindows` the
//     production run used. Under normal flow these are ~0 (the persisted
//     bounds are post-refinement); a non-zero value here flags that production's
//     refinement choice diverged from the legacy snap that would otherwise
//     have applied — the BracketAware path took an alternate snap or further
//     adjustments are available. OMITTED from the encoded object when both
//     deltas are zero AND when `featureWindows.count < 3` (BoundaryRefiner's
//     own guard) — default `JSONEncoder` strategy elides nil optionals,
//     matching the pre-existing handling of `promotionTrack`. Emitted as a
//     Double otherwise. Consumers read via `dict.get(key)` so absent and
//     null are observationally equivalent. This is a TEST-ONLY re-derivation;
//     production code is untouched.
//
// `spanFinalizerConstraintsFired` was added 2026-06-01 by playhead-p56a
// for the SpanFinalizer wire-in. The field is `[String]?` —
// `FinalizerConstraint.rawValue`s in trace-emission order — and is
// populated only when `config.spanFinalizerEnabled == true`. Under the
// shipped `.default` (flag OFF) the key is omitted from the encoded
// object (matches `promotionTrack` / `boundaryRefinement*` handling).
// Read via `AdDetectionService.spanFinalizerConstraintsByWindowIdForTesting()`,
// keyed by the live `AdWindow.id`. Per-AdWindow correlation is exact: the
// service stamps the trace inside the emission loop at the same iteration
// that produces the window.
//
// NOTE on `promotionTrack`: `PromotionTrack` is NOT persisted on the
// `AdWindow` row — it lives on the in-flight `DecisionResult`. The store can
// surface `decisionState` / `eligibilityGate` / `confidence` (== the gate's
// post-fragility `skipConfidence`), but `promotionTrack` is unavailable
// after `runBackfill` returns without a new production tap. Per the task's
// "whatever the existing harness can get from the store + DecisionResult"
// permission, the field is emitted as `null` so the schema stays stable for
// the orchestrator while honoring the "no production code change" rule.
//
// To invoke (orchestrator runs this on Catalyst — DO NOT run by hand without
// staged audio):
//   xcodebuild test \
//     -scheme Playhead -testPlan PlayheadFastTests \
//     -destination 'platform=macOS,variant=Mac Catalyst' \
//     -only-testing:'PlayheadTests/PipelineDumpLiveTests/testProductionPipelineDumpOnNewEpisodes' \
//     PLAYHEAD_PIPELINE_DUMP=1

import AVFoundation
import CryptoKit
import Darwin
import Foundation
#if canImport(FoundationModels)
import FoundationModels
#endif
import Testing
import XCTest
@testable import Playhead

// MARK: - Snapshot manifest loader (test-only)

/// One entry in `TestFixtures/Corpus/Snapshots/manifest.json`. Carries only
/// the fields the pipeline-dump harness reads — additional fields in the
/// manifest are tolerated by `JSONDecoder` ignoring unmapped keys.
struct PipelineDumpSnapshotEntry: Decodable, Equatable, Sendable {
    let show: String?
    let showSlug: String
    let episodeId: String
    let publishDate: String
    let audioPath: String
    let sha256: String?

    enum CodingKeys: String, CodingKey {
        case show
        case showSlug = "showSlug"
        case episodeId = "episodeId"
        case publishDate = "publishDate"
        case audioPath = "audioPath"
        case sha256
    }
}

/// Reads `TestFixtures/Corpus/Snapshots/manifest.json` and returns the list
/// of new-episode entries. Used by the live dump test and pinned hermetically
/// by `PipelineDumpHermeticTests` so a malformed manifest fails on the sim
/// before a 90-minute Catalyst run silently no-ops.
enum PipelineDumpManifestLoader {

    /// Repo-root-relative path to the snapshot manifest.
    static let manifestRelativePath = "TestFixtures/Corpus/Snapshots/manifest.json"

    /// Resolve repo root from `#filePath` — same walk-up
    /// `CorpusAnnotationLoader` uses (file lives at
    /// `PlayheadTests/Services/AdDetection/FusionLift/PipelineDumpLiveTests.swift`,
    /// so strip five path components to reach the repo root).
    static func repoRoot(filePath: String = #filePath) -> URL {
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent()  // FusionLift/
            .deletingLastPathComponent()  // AdDetection/
            .deletingLastPathComponent()  // Services/
            .deletingLastPathComponent()  // PlayheadTests/
            .deletingLastPathComponent()  // <repo root>
    }

    static func manifestURL(repoRoot: URL) -> URL {
        repoRoot.appendingPathComponent(manifestRelativePath, isDirectory: false)
    }

    enum LoadError: Error, CustomStringConvertible {
        case manifestMissing(URL)
        case manifestUnsafe(URL)
        case decodeFailed(URL, Error)
        case invalidEntry(Int, String)

        var description: String {
            switch self {
            case .manifestMissing(let url):
                return "Snapshot manifest not found at \(url.path)"
            case .manifestUnsafe(let url):
                return "Snapshot manifest is not a regular unaliased file at \(url.path)"
            case .decodeFailed(let url, let err):
                return "Failed to decode \(url.lastPathComponent): \(err.localizedDescription)"
            case .invalidEntry(let index, let detail):
                return "Invalid snapshot manifest entry \(index): \(detail)"
            }
        }
    }

    /// Decode the manifest at `url`. Pure: no file-existence side effects
    /// beyond the read.
    static func decode(at url: URL) throws -> [PipelineDumpSnapshotEntry] {
        guard FileManager.default.fileExists(atPath: url.path) else {
            throw LoadError.manifestMissing(url)
        }
        guard let values = try? url.resourceValues(
            forKeys: [.isRegularFileKey, .isSymbolicLinkKey]
        ), values.isRegularFile == true, values.isSymbolicLink != true else {
            throw LoadError.manifestUnsafe(url)
        }
        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            throw LoadError.decodeFailed(url, error)
        }
        do {
            let entries = try JSONDecoder().decode([PipelineDumpSnapshotEntry].self, from: data)
            try validate(entries)
            return entries
        } catch {
            if let loadError = error as? LoadError { throw loadError }
            throw LoadError.decodeFailed(url, error)
        }
    }

    /// Load + decode the manifest from its canonical repo-root location.
    static func load(repoRoot: URL) throws -> [PipelineDumpSnapshotEntry] {
        let url = manifestURL(repoRoot: repoRoot)
        guard !CorpusAnnotationLoader.hasSymbolicLinkComponent(url, relativeTo: repoRoot) else {
            throw LoadError.manifestUnsafe(url)
        }
        return try decode(at: url)
    }

    static func audioURL(
        for entry: PipelineDumpSnapshotEntry,
        repoRoot: URL
    ) throws -> URL {
        try validate([entry])
        let directory = repoRoot
            .appendingPathComponent(CorpusAnnotationLoader.audioRelativePath, isDirectory: true)
            .standardizedFileURL
        let candidate = repoRoot
            .appendingPathComponent(entry.audioPath, isDirectory: false)
            .standardizedFileURL
        guard candidate.deletingLastPathComponent() == directory,
              !CorpusAnnotationLoader.hasSymbolicLinkComponent(candidate, relativeTo: repoRoot)
        else {
            throw LoadError.invalidEntry(0, "audioPath escapes the canonical corpus audio directory")
        }
        return candidate
    }

    private static func validate(_ entries: [PipelineDumpSnapshotEntry]) throws {
        var episodeIds: Set<String> = []
        for (index, entry) in entries.enumerated() {
            let episodeId = entry.episodeId
            guard !episodeId.isEmpty,
                  episodeId != ".",
                  episodeId != "..",
                  !episodeId.contains("/"),
                  !episodeId.contains("\\"),
                  (episodeId as NSString).lastPathComponent == episodeId
            else {
                throw LoadError.invalidEntry(index, "episodeId must be a bare non-empty identifier")
            }
            guard episodeIds.insert(episodeId).inserted else {
                throw LoadError.invalidEntry(index, "duplicate episodeId \(episodeId)")
            }
            guard !entry.showSlug.isEmpty, !entry.publishDate.isEmpty else {
                throw LoadError.invalidEntry(index, "showSlug and publishDate must be non-empty")
            }
            let filename = "\(episodeId).\((entry.audioPath as NSString).pathExtension)"
            let expectedPrefix = "TestFixtures/Corpus/Audio/"
            guard entry.audioPath.hasPrefix(expectedPrefix),
                  entry.audioPath == expectedPrefix + filename,
                  CorpusAnnotationLoader.audioFileExtensions.contains(
                    (entry.audioPath as NSString).pathExtension.lowercased()
                  )
            else {
                throw LoadError.invalidEntry(
                    index,
                    "audioPath must name the episode directly under TestFixtures/Corpus/Audio"
                )
            }
            if let hash = entry.sha256 {
                guard hash.count == 64, hash.unicodeScalars.allSatisfy({
                    CharacterSet(charactersIn: "0123456789abcdef").contains($0)
                }) else {
                    throw LoadError.invalidEntry(index, "sha256 must be 64 lowercase hex characters")
                }
            }
        }
    }
}

// MARK: - Content-addressed partial-silver evaluation (test-only)

// This strict, test-only wire model mirrors the nested immutable artifact.
// swiftlint:disable nesting
struct PartialSilverEvaluation: Decodable, Sendable {
    struct LabelSemantics: Decodable, Equatable, Sendable {
        let contentVetoes: String
        let coverage: String
        let fullBreaks: String
        let presenceAnchors: String
        let quality: String
        let unlabeledAudio: String

        enum CodingKeys: String, CodingKey {
            case contentVetoes = "content_vetoes"
            case coverage
            case fullBreaks = "full_breaks"
            case presenceAnchors = "presence_anchors"
            case quality
            case unlabeledAudio = "unlabeled_audio"
        }
    }

    struct Interval: Decodable, Sendable {
        let startSeconds: Double
        let endSeconds: Double

        enum CodingKeys: String, CodingKey {
            case startSeconds = "start_seconds"
            case endSeconds = "end_seconds"
        }
    }

    struct Asset: Decodable, Sendable {
        let audioFingerprint: String
        let contentVetoes: [Interval]
        let durationSeconds: Double
        let episodeId: String
        let fullBreaks: [Interval]
        let presenceAnchors: [Interval]
        let showName: String

        enum CodingKeys: String, CodingKey {
            case audioFingerprint = "audio_fingerprint"
            case contentVetoes = "content_vetoes"
            case durationSeconds = "duration_seconds"
            case episodeId = "episode_id"
            case fullBreaks = "full_breaks"
            case presenceAnchors = "presence_anchors"
            case showName = "show_name"
        }
    }

    struct Summary: Decodable, Sendable {
        let assets: Int
        let contentVetoes: Int
        let fullBreakAssets: Int
        let fullBreaks: Int
        let labeledRegions: Int
        let presenceAnchors: Int

        enum CodingKeys: String, CodingKey {
            case assets
            case contentVetoes = "content_vetoes"
            case fullBreakAssets = "full_break_assets"
            case fullBreaks = "full_breaks"
            case labeledRegions = "labeled_regions"
            case presenceAnchors = "presence_anchors"
        }
    }

    let artifactKind: String
    let assets: [Asset]
    let labelSemantics: LabelSemantics
    let schemaVersion: Int
    let summary: Summary

    enum CodingKeys: String, CodingKey {
        case artifactKind = "artifact_kind"
        case assets
        case labelSemantics = "label_semantics"
        case schemaVersion = "schema_version"
        case summary
    }
}

enum PartialSilverEvaluationLoader {
    static let expectedAssetCount = 27
    static let evaluationSHA256 =
        "0d85a0ec8bfa30873bad63bbc4bb12a3f7613aca76d5b76149e25db2a0be226f"
    static let relativePath =
        "TestFixtures/Corpus/Evaluations/earaudit-partial-silver-\(evaluationSHA256).json"
    private static let expectedLabelSemantics = PartialSilverEvaluation.LabelSemantics(
        contentVetoes: "only the exact interval is labeled human-reviewed content; "
            + "the separate reject ledger may conservatively block overlapping promotion "
            + "candidates without labeling surrounding audio",
        coverage: "partial",
        fullBreaks: "human-reviewed complete contiguous ad-break boundaries",
        presenceAnchors: "ad presence only; bounds are not full-break boundary truth",
        quality: "silver",
        unlabeledAudio: "unknown_elsewhere"
    )

    enum LoadError: Error, CustomStringConvertible {
        case unsafeFile(URL)
        case oversizedFile(URL)
        case contentAddressMismatch(URL)
        case decodeFailed(URL, Error)
        case invalidArtifact(String)
        case unsafeCorpusRoot(URL)
        case audioMembership(String, String)

        var description: String {
            switch self {
            case .unsafeFile(let url):
                return "baseline input is missing or unsafe: \(url.path)"
            case .oversizedFile(let url):
                return "baseline input exceeds its size limit: \(url.path)"
            case .contentAddressMismatch(let url):
                return "baseline filename does not match its SHA-256: \(url.path)"
            case .decodeFailed(let url, let error):
                return "failed to decode \(url.lastPathComponent): \(error.localizedDescription)"
            case .invalidArtifact(let reason):
                return "invalid partial-silver evaluation: \(reason)"
            case .unsafeCorpusRoot(let url):
                return "PLAYHEAD_CORPUS_ROOT is not a regular unaliased directory: \(url.path)"
            case .audioMembership(let episodeId, let reason):
                return "retained audio for \(episodeId) is invalid: \(reason)"
            }
        }
    }

    static func load(sourceRoot: URL) throws -> PartialSilverEvaluation {
        let url = sourceRoot.appendingPathComponent(relativePath, isDirectory: false)
        guard !hasUnsafeFilesystemComponent(sourceRoot),
              !CorpusAnnotationLoader.hasSymbolicLinkComponent(url, relativeTo: sourceRoot) else {
            throw LoadError.unsafeFile(url)
        }
        return try decode(at: url)
    }

    static func decode(at url: URL) throws -> PartialSilverEvaluation {
        let data = try readRegularBytes(at: url, maximumBytes: 8 * 1_024 * 1_024)
        let digest = sha256Hex(data)
        guard url.lastPathComponent.hasSuffix("-\(digest).json") else {
            throw LoadError.contentAddressMismatch(url)
        }
        let evaluation: PartialSilverEvaluation
        do {
            evaluation = try JSONDecoder().decode(PartialSilverEvaluation.self, from: data)
        } catch {
            throw LoadError.decodeFailed(url, error)
        }
        try validate(evaluation)
        return evaluation
    }

    static func validatedCorpusRoot(sourceRoot: URL) throws -> URL {
        let configured = ProcessInfo.processInfo.environment["PLAYHEAD_CORPUS_ROOT"]
        if let configured,
           !configured.hasPrefix("/") || containsTraversalComponent(configured) {
            throw LoadError.unsafeCorpusRoot(URL(fileURLWithPath: configured))
        }
        let root = configured.map { URL(fileURLWithPath: $0) } ?? sourceRoot
        let standardized = root.standardizedFileURL
        guard let values = try? standardized.resourceValues(
            forKeys: [.isDirectoryKey, .isSymbolicLinkKey, .isAliasFileKey]
        ), values.isDirectory == true,
           values.isSymbolicLink != true,
           values.isAliasFile != true,
           !hasUnsafeFilesystemComponent(standardized) else {
            throw LoadError.unsafeCorpusRoot(standardized)
        }
        return standardized
    }

    static func audioURL(for asset: PartialSilverEvaluation.Asset, corpusRoot: URL) throws -> URL {
        let directory = corpusRoot
            .appendingPathComponent(CorpusAnnotationLoader.audioRelativePath, isDirectory: true)
            .standardizedFileURL
        guard let directoryValues = try? directory.resourceValues(
            forKeys: [.isDirectoryKey, .isSymbolicLinkKey, .isAliasFileKey]
        ), directoryValues.isDirectory == true,
           directoryValues.isSymbolicLink != true,
           directoryValues.isAliasFile != true,
           !hasUnsafeFilesystemComponent(directory),
           !CorpusAnnotationLoader.hasSymbolicLinkComponent(directory, relativeTo: corpusRoot) else {
            throw LoadError.audioMembership(asset.episodeId, "audio directory is missing or aliased")
        }
        let candidates: [URL]
        do {
            candidates = try FileManager.default.contentsOfDirectory(
                at: directory,
                includingPropertiesForKeys: [.isRegularFileKey, .isSymbolicLinkKey, .isAliasFileKey],
                options: [.skipsHiddenFiles]
            ).filter {
                $0.deletingPathExtension().lastPathComponent == asset.episodeId
                    && CorpusAnnotationLoader.audioFileExtensions.contains(
                        $0.pathExtension.lowercased()
                    )
            }
        } catch {
            throw LoadError.audioMembership(asset.episodeId, "cannot enumerate audio directory")
        }
        guard candidates.count == 1, let candidate = candidates.first else {
            throw LoadError.audioMembership(
                asset.episodeId,
                "expected exactly one direct audio file, found \(candidates.count)"
            )
        }
        let values = try candidate.resourceValues(
            forKeys: [.isRegularFileKey, .isSymbolicLinkKey, .isAliasFileKey]
        )
        guard values.isRegularFile == true,
              values.isSymbolicLink != true,
              values.isAliasFile != true,
              !hasUnsafeFilesystemComponent(candidate),
              !CorpusAnnotationLoader.hasSymbolicLinkComponent(candidate, relativeTo: corpusRoot) else {
            throw LoadError.audioMembership(asset.episodeId, "audio is not a regular unaliased file")
        }
        let actual = try CorpusAudioFingerprint.fingerprint(of: candidate)
        guard actual == asset.audioFingerprint else {
            throw LoadError.audioMembership(asset.episodeId, "fingerprint mismatch")
        }
        return candidate
    }

    static func transcriptURL(for episodeId: String, corpusRoot: URL) throws -> URL {
        let directory = corpusRoot
            .appendingPathComponent("TestFixtures/Corpus/Transcripts", isDirectory: true)
            .standardizedFileURL
        let url = directory.appendingPathComponent("\(episodeId).json").standardizedFileURL
        guard url.deletingLastPathComponent() == directory,
              !hasUnsafeFilesystemComponent(url),
              !CorpusAnnotationLoader.hasSymbolicLinkComponent(url, relativeTo: corpusRoot) else {
            throw LoadError.unsafeFile(url)
        }
        _ = try readRegularBytes(at: url, maximumBytes: 128 * 1_024 * 1_024)
        return url
    }

    static func sha256Hex(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    static func fileSHA256Hex(_ url: URL, maximumBytes: Int) throws -> String {
        sha256Hex(try readRegularBytes(at: url, maximumBytes: maximumBytes))
    }

    static func hasUnsafeFilesystemComponent(_ url: URL) -> Bool {
        let absolute = url.standardizedFileURL
        let components = Array(absolute.pathComponents.dropFirst())
        let containerAnchorLength = trustedApplicationContainerAnchorLength(components)
        let trustedPrefixLength = max(0, containerAnchorLength - 1)
        let compatibilityLinks: Set<String> = ["etc", "tmp", "var"]
        var cursor = URL(fileURLWithPath: "/", isDirectory: true)
        for (index, component) in components.enumerated() {
            cursor.appendPathComponent(component)
            // A physical iOS app cannot choose or replace the system-managed
            // data-container ancestors. Inspect the standard app directory
            // itself and every caller-controlled component below it.
            if index < trustedPrefixLength { continue }
            guard let values = try? cursor.resourceValues(
                forKeys: [.isSymbolicLinkKey, .isAliasFileKey]
            ) else {
                continue
            }
            // `isAliasFile` is documented true for symbolic links as well as
            // Finder aliases, so the root compatibility links (/etc, /tmp,
            // /var) need the same exemption in both branches. Without it the
            // Catalyst lane can never stage inputs: the macOS user temp root
            // is /var/folders/…, and /var is a root-level symlink — iOS never
            // hits this because container paths sit inside the trusted-anchor
            // prefix skipped above.
            let isRootCompatibilityLink = index == 0
                && compatibilityLinks.contains(component)
                && values.isSymbolicLink == true
            if values.isAliasFile == true, !isRootCompatibilityLink {
                return true
            }
            if values.isSymbolicLink == true, !isRootCompatibilityLink {
                return true
            }
        }
        return false
    }

    static func trustedApplicationContainerAnchorLength(_ components: [String]) -> Int {
        let prefixes = [
            ["var", "mobile", "Containers", "Data", "Application"],
            ["private", "var", "mobile", "Containers", "Data", "Application"],
        ]
        let anchors: Set<String> = ["Documents", "Library", "tmp"]
        for prefix in prefixes where components.count >= prefix.count + 2 {
            guard Array(components.prefix(prefix.count)) == prefix,
                  UUID(uuidString: components[prefix.count]) != nil,
                  anchors.contains(components[prefix.count + 1]) else {
                continue
            }
            return prefix.count + 2
        }
        return 0
    }

    static func containsTraversalComponent(_ path: String) -> Bool {
        path.split(separator: "/", omittingEmptySubsequences: false).contains {
            $0 == "." || $0 == ".."
        }
    }

    static func readRegularBytes(
        at url: URL,
        maximumBytes: Int,
        afterOpen: (() throws -> Void)? = nil
    ) throws -> Data {
        let descriptor = try openRegularDescriptor(at: url)
        defer { Darwin.close(descriptor) }
        var before = stat()
        guard fstat(descriptor, &before) == 0, isRegular(before) else {
            throw LoadError.unsafeFile(url)
        }
        guard before.st_size >= 0, before.st_size <= off_t(maximumBytes) else {
            throw LoadError.oversizedFile(url)
        }
        try afterOpen?()

        var data = Data()
        data.reserveCapacity(Int(before.st_size))
        var buffer = [UInt8](repeating: 0, count: 1_024 * 1_024)
        while true {
            let count = Darwin.read(descriptor, &buffer, buffer.count)
            if count == 0 { break }
            if count < 0 {
                if errno == EINTR { continue }
                throw LoadError.decodeFailed(url, posixError("read"))
            }
            guard data.count <= maximumBytes - count else {
                throw LoadError.oversizedFile(url)
            }
            data.append(contentsOf: buffer[0..<count])
        }
        var after = stat()
        guard fstat(descriptor, &after) == 0,
              sameFile(before, after),
              before.st_size == after.st_size,
              before.st_mtimespec.tv_sec == after.st_mtimespec.tv_sec,
              before.st_mtimespec.tv_nsec == after.st_mtimespec.tv_nsec,
              before.st_ctimespec.tv_sec == after.st_ctimespec.tv_sec,
              before.st_ctimespec.tv_nsec == after.st_ctimespec.tv_nsec,
              data.count == Int(after.st_size) else {
            throw LoadError.unsafeFile(url)
        }
        return data
    }

    static func snapshotRegularFile(
        at source: URL,
        to destination: URL,
        maximumBytes: Int
    ) throws -> String {
        let data = try readRegularBytes(at: source, maximumBytes: maximumBytes)
        try data.write(to: destination, options: [.withoutOverwriting])
        return sha256Hex(data)
    }

    static func openDirectoryDescriptor(at url: URL) throws -> Int32 {
        guard url.isFileURL else { throw LoadError.unsafeFile(url) }
        let flags = O_RDONLY | O_NONBLOCK | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC
        let traversal = try openTraversalRoot(at: url, flags: flags)
        var current = traversal.descriptor
        let components = traversal.components
        do {
            for component in components {
                var before = stat()
                guard component.withCString({
                    fstatat(current, $0, &before, AT_SYMLINK_NOFOLLOW)
                }) == 0, isDirectory(before) else {
                    throw LoadError.unsafeFile(url)
                }
                let next = component.withCString { openat(current, $0, flags) }
                guard next >= 0 else { throw LoadError.unsafeFile(url) }
                var after = stat()
                guard fstat(next, &after) == 0, sameFile(before, after) else {
                    Darwin.close(next)
                    throw LoadError.unsafeFile(url)
                }
                Darwin.close(current)
                current = next
            }
            return current
        } catch {
            Darwin.close(current)
            throw error
        }
    }

    private static func openRegularDescriptor(at url: URL) throws -> Int32 {
        guard url.isFileURL else { throw LoadError.unsafeFile(url) }
        let directoryFlags = O_RDONLY | O_NONBLOCK | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC
        let traversal = try openTraversalRoot(
            at: url,
            flags: directoryFlags
        )
        var directoryDescriptor = traversal.descriptor
        let components = traversal.components
        do {
            for component in components.dropLast() {
                var before = stat()
                guard component.withCString({
                    fstatat(directoryDescriptor, $0, &before, AT_SYMLINK_NOFOLLOW)
                }) == 0, isDirectory(before) else {
                    throw LoadError.unsafeFile(url)
                }
                let next = component.withCString {
                    openat(directoryDescriptor, $0, directoryFlags)
                }
                guard next >= 0 else { throw LoadError.unsafeFile(url) }
                var after = stat()
                guard fstat(next, &after) == 0, sameFile(before, after) else {
                    Darwin.close(next)
                    throw LoadError.unsafeFile(url)
                }
                Darwin.close(directoryDescriptor)
                directoryDescriptor = next
            }
            guard let filename = components.last else {
                throw LoadError.unsafeFile(url)
            }
            var before = stat()
            guard filename.withCString({
                fstatat(directoryDescriptor, $0, &before, AT_SYMLINK_NOFOLLOW)
            }) == 0, isRegular(before) else {
                throw LoadError.unsafeFile(url)
            }
            let descriptor = filename.withCString {
                openat(directoryDescriptor, $0, O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC)
            }
            guard descriptor >= 0 else { throw LoadError.unsafeFile(url) }
            var after = stat()
            guard fstat(descriptor, &after) == 0, sameFile(before, after) else {
                Darwin.close(descriptor)
                throw LoadError.unsafeFile(url)
            }
            Darwin.close(directoryDescriptor)
            return descriptor
        } catch {
            Darwin.close(directoryDescriptor)
            throw error
        }
    }

    private static func openTraversalRoot(
        at url: URL,
        flags: Int32
    ) throws -> (descriptor: Int32, components: [String]) {
        let components = safePathComponents(url)
        let containerAnchorLength = trustedApplicationContainerAnchorLength(components)
        if containerAnchorLength > 0 {
            let anchorPath = "/" + components.prefix(containerAnchorLength).joined(separator: "/")
            let descriptor = anchorPath.withCString { Darwin.open($0, flags) }
            guard descriptor >= 0 else {
                throw LoadError.decodeFailed(url, posixError("open app container anchor"))
            }
            return (descriptor, Array(components.dropFirst(containerAnchorLength)))
        }
        let descriptor = Darwin.open("/", flags)
        guard descriptor >= 0 else {
            throw LoadError.decodeFailed(url, posixError("open root"))
        }
        return (descriptor, components)
    }

    private static func safePathComponents(_ url: URL) -> [String] {
        var components = url.standardizedFileURL.pathComponents
        guard components.first == "/" else { return [] }
        if components.count > 1, ["etc", "tmp", "var"].contains(components[1]) {
            // Foundation's `resolvingSymlinksInPath()` strips the /private
            // prefix straight back off ("/var" → "/private/var" → "/var"),
            // so it can never rewrite the root compatibility links and the
            // O_NOFOLLOW descriptor walk would die on the /var symlink —
            // which is where every macOS (Catalyst) temp path starts.
            // realpath(3) has no such stripping behavior.
            var buffer = [CChar](repeating: 0, count: Int(PATH_MAX))
            if let resolved = realpath("/\(components[1])", &buffer) {
                let compatibility = URL(
                    fileURLWithPath: String(cString: resolved),
                    isDirectory: true
                ).pathComponents
                components = compatibility + Array(components.dropFirst(2))
            }
        }
        return Array(components.dropFirst())
    }

    private static func sameFile(_ left: stat, _ right: stat) -> Bool {
        left.st_dev == right.st_dev && left.st_ino == right.st_ino
    }

    private static func isRegular(_ metadata: stat) -> Bool {
        metadata.st_mode & S_IFMT == S_IFREG
    }

    private static func isDirectory(_ metadata: stat) -> Bool {
        metadata.st_mode & S_IFMT == S_IFDIR
    }

    private static func posixError(_ operation: String) -> NSError {
        NSError(
            domain: NSPOSIXErrorDomain,
            code: Int(errno),
            userInfo: [NSLocalizedDescriptionKey: "\(operation) failed: \(String(cString: strerror(errno)))"]
        )
    }
}

private extension PartialSilverEvaluationLoader {
    private static func validate(_ evaluation: PartialSilverEvaluation) throws {
        guard evaluation.artifactKind == "retained_audio_partial_silver_evaluation",
              evaluation.schemaVersion == 1 else {
            throw LoadError.invalidArtifact("unexpected artifact kind or schema version")
        }
        guard evaluation.labelSemantics == expectedLabelSemantics else {
            throw LoadError.invalidArtifact("label semantics are not the frozen partial-silver contract")
        }
        guard evaluation.assets.count == expectedAssetCount else {
            throw LoadError.invalidArtifact(
                "expected \(expectedAssetCount) assets, found \(evaluation.assets.count)"
            )
        }
        var episodeIds: Set<String> = []
        var fingerprints: Set<String> = []
        for asset in evaluation.assets {
            try validate(asset)
            guard episodeIds.insert(asset.episodeId).inserted else {
                throw LoadError.invalidArtifact("duplicate episode_id \(asset.episodeId)")
            }
            guard fingerprints.insert(asset.audioFingerprint).inserted else {
                throw LoadError.invalidArtifact("duplicate audio fingerprint")
            }
        }
        try validateSummary(evaluation)
    }

    private static func validate(_ asset: PartialSilverEvaluation.Asset) throws {
        guard isBareIdentifier(asset.episodeId), !asset.showName.isEmpty else {
            throw LoadError.invalidArtifact("invalid episode or show identity")
        }
        guard isSHA256Fingerprint(asset.audioFingerprint) else {
            throw LoadError.invalidArtifact("malformed fingerprint for \(asset.episodeId)")
        }
        guard asset.durationSeconds.isFinite, asset.durationSeconds > 0 else {
            throw LoadError.invalidArtifact("invalid duration for \(asset.episodeId)")
        }
        for interval in asset.fullBreaks + asset.presenceAnchors + asset.contentVetoes {
            guard interval.startSeconds.isFinite,
                  interval.endSeconds.isFinite,
                  interval.startSeconds >= 0,
                  interval.startSeconds < interval.endSeconds,
                  interval.endSeconds <= asset.durationSeconds else {
                throw LoadError.invalidArtifact("invalid label interval for \(asset.episodeId)")
            }
        }
    }

    private static func validateSummary(_ evaluation: PartialSilverEvaluation) throws {
        let fullBreaks = evaluation.assets.reduce(0) { $0 + $1.fullBreaks.count }
        let anchors = evaluation.assets.reduce(0) { $0 + $1.presenceAnchors.count }
        let vetoes = evaluation.assets.reduce(0) { $0 + $1.contentVetoes.count }
        let fullBreakAssets = evaluation.assets.filter { !$0.fullBreaks.isEmpty }.count
        let summary = evaluation.summary
        guard summary.assets == evaluation.assets.count,
              summary.fullBreaks == fullBreaks,
              summary.presenceAnchors == anchors,
              summary.contentVetoes == vetoes,
              summary.fullBreakAssets == fullBreakAssets,
              summary.labeledRegions == fullBreaks + anchors + vetoes else {
            throw LoadError.invalidArtifact("summary disagrees with label arrays")
        }
    }

    private static func isBareIdentifier(_ value: String) -> Bool {
        !value.isEmpty && value != "." && value != ".."
            && !value.contains("/") && !value.contains("\\")
            && (value as NSString).lastPathComponent == value
    }

    private static func isSHA256Fingerprint(_ value: String) -> Bool {
        guard value.count == 71, value.hasPrefix("sha256:") else { return false }
        return value.dropFirst("sha256:".count).unicodeScalars.allSatisfy {
            CharacterSet(charactersIn: "0123456789abcdef").contains($0)
        }
    }
}
// swiftlint:enable nesting

// MARK: - Dump payload (test-only)

/// Per-`AdWindow` row written into the JSON dump. The Codable
/// representation IS the schema the orchestrator parses; ordering is fixed
/// by `CodingKeys` so the dump is diff-friendly across runs.
private struct DumpAdWindow: Encodable {
    let startTime: Double
    let endTime: Double
    let skipConfidence: Double
    let decisionState: String
    let eligibilityGate: String?
    /// Always `nil` in this dump — `PromotionTrack` is not persisted on
    /// `AdWindow`. Emitted to keep the schema stable for the orchestrator.
    let promotionTrack: String?
    /// `AdWindow.wasSkipped` flag on the persisted row. Added 2026-06-01
    /// for playhead-4xqf eligibility-gate-demotion investigation: this is
    /// the actual playback signal (auto-skip vs not), independent of
    /// `decisionState`. Pairing the two answers "did a quorum demotion
    /// flip what the user would have heard?".
    let wasSkipped: Bool
    /// Re-derived legacy `BoundaryRefiner.computeAdjustments` start delta
    /// (seconds) when fed the PERSISTED AdWindow bounds + the same
    /// `featureWindows` production used. Added 2026-06-01 for playhead-4xqf
    /// boundary-refinement suspect. `nil` when both deltas are zero (the
    /// expected normal case — persisted bounds are already post-refinement)
    /// AND when `featureWindows.count < 3` (BoundaryRefiner's own guard).
    /// A non-null value flags that production's refinement choice (likely
    /// the BracketAware path) diverged from the legacy snap, or that
    /// further adjustments remain available.
    let boundaryRefinementStartAdjustment: Double?
    /// Re-derived legacy `BoundaryRefiner.computeAdjustments` end delta
    /// (seconds). Same semantics as `boundaryRefinementStartAdjustment`.
    let boundaryRefinementEndAdjustment: Double?
    /// playhead-p56a: ordered list of `FinalizerConstraint.rawValue`s that
    /// fired on this AdWindow's source span during the most recent
    /// `runBackfill` invocation, AS REPORTED BY
    /// `AdDetectionService.spanFinalizerConstraintsBySpanIdForTesting()`.
    /// `nil` when `config.spanFinalizerEnabled == false` (the OFF path
    /// never runs the finalizer, never records a trace, and this key is
    /// omitted from the encoded object — matching the pre-existing
    /// `promotionTrack` / `boundaryRefinement*` convention). Non-nil only
    /// when the flag is on AND the underlying spanId has at least one
    /// recorded constraint; empty arrays are not emitted (a span the
    /// finalizer kept but didn't modify ends up with no trace entries and
    /// is also represented as a nil here, not `[]`, to keep the wire shape
    /// stable across flag-state changes for unmutated spans).
    let spanFinalizerConstraintsFired: [String]?
    /// playhead-l2f.6: stinger-refinement trace for this AdWindow's source
    /// span during the most recent `runBackfill`, AS REPORTED BY
    /// `AdDetectionService.stingerRefinementTraceByWindowIdForTesting()`.
    /// `nil` when `config.stingerRefinementEnabled == false` (the OFF path
    /// never consults the refiner, never records a trace, and this key is
    /// omitted from the encoded object — matching the pre-existing
    /// `promotionTrack` / `boundaryRefinement*` /
    /// `spanFinalizerConstraintsFired` convention). Non-nil only when the
    /// flag is on AND the window's show had a bank entry (a no-snap
    /// consult still records a trace so the OFF-vs-no-snap distinction is
    /// observable downstream).
    let stingerRefinement: DumpStingerRefinement?
}

/// playhead-l2f.6: dump mirror of `StingerRefinementTrace`. A dedicated
/// Encodable struct (rather than encoding the production type directly)
/// keeps the dump schema locked in this file alongside every other wire
/// shape, so a production-side trace refactor breaks here first instead of
/// silently in a downstream Python reader.
///
/// playhead-xsdz.38 (v4 joint recipe) adds four optional fields explaining
/// the joint decision: per-edge evidence-candidate counts, the chosen
/// pair's score, and which grid term (`"bonus"`/`"penalty"`) the winning
/// pair carried. All follow the absent-when-nil convention. NOTE for
/// offline comparators diffing dumps against oracle traces: the oracle
/// records `pair_score` (0.0) even on zero-evidence consults, while the
/// port keeps the trace pristine there (preserving the OFF-vs-no-snap
/// distinction the wire-in pins) — treat an absent `pairScore` with no
/// candidate counts as oracle `pair_score == 0.0`.
private struct DumpStingerRefinement: Encodable {
    let startSnapped: Bool
    let endSnapped: Bool
    let gridApplied: Bool
    let revertedNoOverlap: Bool
    let startPeak: Double?
    let endPeak: Double?
    let startDeltaSeconds: Double?
    let endDeltaSeconds: Double?
    let startCandidateCount: Int?
    let endCandidateCount: Int?
    let pairScore: Double?
    let gridTermApplied: String?

    init(_ trace: StingerRefinementTrace) {
        startSnapped = trace.startSnapped
        endSnapped = trace.endSnapped
        gridApplied = trace.gridApplied
        revertedNoOverlap = trace.revertedNoOverlap
        startPeak = trace.startPeak
        endPeak = trace.endPeak
        startDeltaSeconds = trace.startDeltaSeconds
        endDeltaSeconds = trace.endDeltaSeconds
        startCandidateCount = trace.startCandidateCount
        endCandidateCount = trace.endCandidateCount
        pairScore = trace.pairScore
        gridTermApplied = trace.gridTermApplied
    }

    init(
        startSnapped: Bool,
        endSnapped: Bool,
        gridApplied: Bool,
        revertedNoOverlap: Bool,
        startPeak: Double?,
        endPeak: Double?,
        startDeltaSeconds: Double?,
        endDeltaSeconds: Double?,
        startCandidateCount: Int?,
        endCandidateCount: Int?,
        pairScore: Double?,
        gridTermApplied: String?
    ) {
        self.startSnapped = startSnapped
        self.endSnapped = endSnapped
        self.gridApplied = gridApplied
        self.revertedNoOverlap = revertedNoOverlap
        self.startPeak = startPeak
        self.endPeak = endPeak
        self.startDeltaSeconds = startDeltaSeconds
        self.endDeltaSeconds = endDeltaSeconds
        self.startCandidateCount = startCandidateCount
        self.endCandidateCount = endCandidateCount
        self.pairScore = pairScore
        self.gridTermApplied = gridTermApplied
    }
}

/// One candidate decoded span observed by the FragilityDiagnosticObserver
/// during this episode's backfill. Added for playhead-4xqf (DAI boundary-
/// undersizing investigation): enables identifying WHICH candidate spans
/// the candidate generator emitted vs which ones the fusion/AdWindow stage
/// kept, by comparing this list against `adWindows` and against rediff
/// slot boundaries. The `fragilityScore` field is "free" — the observer
/// already records it for every span. JSON field order is deliberate to
/// match `DumpAdWindow`'s layout for downstream comparison scripts.
private struct DumpDecodedSpan: Encodable {
    let spanId: String
    let startTime: Double
    let endTime: Double
    let proposalConfidence: Double
    let skipConfidence: Double
    let fragilityScore: Double
}

/// Per-episode dump row.
private struct DumpEpisode: Encodable {
    let episodeId: String
    /// Exact bytes whose timeline all coordinate-bearing fields use.
    let audioFingerprint: String
    let showSlug: String
    let publishDate: String
    let episodeDurationSeconds: Double
    /// Count of candidate decoded spans recorded by the observer. Preserved
    /// for backward compatibility with existing consumers (current dump
    /// readers only check this int field).
    let candidateDecodedSpans: Int
    /// Per-candidate decoded-span boundaries + fragility geometry. Added
    /// 2026-06-01 for playhead-4xqf boundary-undersizing investigation.
    /// One row per `observer.spanRows(for: assetId)` entry, in record order.
    let candidateDecodedSpanList: [DumpDecodedSpan]
    let adWindows: [DumpAdWindow]
}

/// Cross-episode summary.
private struct DumpSummary: Encodable {
    let totalEpisodes: Int
    let totalAdWindows: Int
    /// Map `showSlug` → number of `AdWindow`s. JSON dict iteration order is
    /// not specified by Foundation, so this is for cross-show roll-up
    /// inspection, not byte-stable comparison.
    let perShow: [String: Int]
}

/// playhead-xsdz.36.1 (R4): fresh-B-side staging coverage, stamped into the
/// TREATMENT dump so the go/no-go artifact is self-describing — the pre-flight
/// coverage print lives only in the run transcript, but the JSON dump is what
/// the orchestrator actually diffs, and a 1-of-N partially-staged run must not
/// read as "no lift" without the artifact itself recording how little was
/// staged. `nil` (key omitted — see the encoding pin in
/// `PipelineDumpEncodingTests`) in the baseline/legacy lanes, whose bytes are
/// unchanged.
private struct DumpBSideCoverage: Encodable {
    let stagedEntryCount: Int
    let manifestEntryCount: Int
    /// Sorted; absent OR irregular (the decode path treats both as unstaged).
    let unstagedEpisodeIds: [String]
    /// Sorted subset of `unstagedEpisodeIds`: something exists (or may exist)
    /// at the staged path but the decode path refuses it (symlink — including
    /// dangling — non-regular, empty file, or a path whose metadata cannot be
    /// read for any reason other than not existing; R6 fails those closed).
    let irregularEpisodeIds: [String]
}

/// Root dump payload.
private struct DumpPayload: Encodable {
    let config: String
    let runUtc: String
    /// Treatment lane only; nil → key omitted (baseline artifact unchanged).
    let bSideCoverage: DumpBSideCoverage?
    let episodes: [DumpEpisode]
    let summary: DumpSummary
}

private struct BaselinePrediction: Encodable {
    let startSeconds: Double
    let endSeconds: Double
    let decisionState: String
    let wasSkipped: Bool
    let confidence: Double

    init(_ window: DumpAdWindow) {
        startSeconds = window.startTime
        endSeconds = window.endTime
        decisionState = window.decisionState
        wasSkipped = window.wasSkipped
        confidence = window.skipConfidence
    }
}

private struct BaselineMusicFeature: Encodable {
    let startSeconds: Double
    let endSeconds: Double
    let musicProbability: Double
    let musicBedLevel: String
    let musicBedChangeScore: Double
    let musicBedOnsetScore: Double
    let musicBedOffsetScore: Double

    init(_ window: FeatureWindow) {
        startSeconds = window.startTime
        endSeconds = window.endTime
        musicProbability = window.musicProbability
        musicBedLevel = window.musicBedLevel.rawValue
        musicBedChangeScore = window.musicBedChangeScore
        musicBedOnsetScore = window.musicBedOnsetScore
        musicBedOffsetScore = window.musicBedOffsetScore
    }
}

private struct BaselineEpisode: Encodable {
    let episodeId: String
    let showName: String
    let audioFingerprint: String
    let transcriptSHA256: String
    let durationSeconds: Double
    let predictions: [BaselinePrediction]
    let musicFeatures: [BaselineMusicFeature]
}

private struct BaselinePipelineVersions: Encodable {
    let modelVersion: String
    let policyVersion: String
    let featureSchemaVersion: Int
}

private struct BaselineAdDetectionDefaults: Encodable {
    let audioForensicsEnabled: Bool
    let autoSkipConfidenceThreshold: Double
    let bracketRefinementEnabled: Bool
    let bracketRefinementMinCoarseScore: Double
    let bracketRefinementMinFineConfidence: Double
    let bracketRefinementMinTrust: Double
    let candidateThreshold: Double
    let chapterSignalMode: String
    let classifierSeedQualifiedThreshold: Double
    let confirmationThreshold: Double
    let crossEpisodeMemoryEnabled: Bool
    let crossShowSyndicationEnabled: Bool
    let detectorVersion: String
    let evidenceFragilityPenaltyEnabled: Bool
    let fmBackfillMode: String
    let fmConsensusThreshold: Int
    let fmScanBudgetSeconds: Double
    let fragilityPenalty: Double
    let fragilityThreshold: Double
    let hotPathLookahead: Double
    let lexicalAutoAdEnabled: Bool
    let lexicalAutoAdQualifiedThreshold: Double
    let markOnlyThreshold: Double
    let perShowThresholdControlEnabled: Bool
    let perShowThresholdIntegralGain: Double
    let perShowThresholdMaxOffset: Double
    let perShowThresholdMinSamples: Int
    let perShowThresholdProportionalGain: Double
    let rediffSlotOwnershipEnabled: Bool
    let rediffSlotShadowEnabled: Bool
    let rhetoricalGrammarEnabled: Bool
    let segmentAutoSkipThreshold: Double
    let segmentUICandidateThreshold: Double
    let spanFinalizerEnabled: Bool
    let spliceSlotOwnershipEnabled: Bool
    let spliceSlotShadowEnabled: Bool
    let stingerRefinementEnabled: Bool
    let suppressionThreshold: Double
    let temporalHighConfidenceNeighborThreshold: Double
    let temporalIsolationPenaltyFactor: Double
    let temporalMinDwellPenaltyFactor: Double
    let temporalMinDwellSeconds: Double
    let temporalNeighborWindowSeconds: Double
    let temporalRegularizationEnabled: Bool
    let transcriptBoundaryCueEnabled: Bool
}

private struct BaselineNarrowingDefaults: Encodable {
    let perAnchorPaddingSegments: Int
    let maxNarrowedSegmentsPerPhase: Int
    let acousticBreakSnapMaxDistanceSeconds: Double
    let lexicalClusterSnapEnabled: Bool
    let lexicalClusterGapSeconds: Double
    let lexicalClusterMarginSegments: Int?
    let lexicalClusterMinHits: Int
}

private let baselineAdDetectionDefaultKeys: Set<String> = [
    "audio_forensics_enabled", "auto_skip_confidence_threshold",
    "bracket_refinement_enabled", "bracket_refinement_min_coarse_score",
    "bracket_refinement_min_fine_confidence", "bracket_refinement_min_trust",
    "candidate_threshold", "chapter_signal_mode", "classifier_seed_qualified_threshold",
    "confirmation_threshold", "cross_episode_memory_enabled",
    "cross_show_syndication_enabled", "detector_version",
    "evidence_fragility_penalty_enabled", "fm_backfill_mode", "fm_consensus_threshold",
    "fm_scan_budget_seconds", "fragility_penalty", "fragility_threshold",
    "hot_path_lookahead", "lexical_auto_ad_enabled",
    "lexical_auto_ad_qualified_threshold", "mark_only_threshold",
    "per_show_threshold_control_enabled", "per_show_threshold_integral_gain",
    "per_show_threshold_max_offset", "per_show_threshold_min_samples",
    "per_show_threshold_proportional_gain", "rediff_slot_ownership_enabled",
    "rediff_slot_shadow_enabled", "rhetorical_grammar_enabled",
    "segment_auto_skip_threshold", "segment_ui_candidate_threshold",
    "span_finalizer_enabled", "splice_slot_ownership_enabled",
    "splice_slot_shadow_enabled", "stinger_refinement_enabled",
    "suppression_threshold",
    "temporal_high_confidence_neighbor_threshold", "temporal_isolation_penalty_factor",
    "temporal_min_dwell_penalty_factor", "temporal_min_dwell_seconds",
    "temporal_neighbor_window_seconds", "temporal_regularization_enabled",
    "transcript_boundary_cue_enabled"
]

private struct BaselineProductionConfig: Encodable {
    let entryPoint: String
    let adDetectionConfigIdentity: String
    let narrowingConfigIdentity: String
    let hotPathClassifierIdentity: String
    let foundationModelClassifierIdentity: String
    let foundationModelEnvironmentIdentity: String
    let foundationModelRedactorIdentity: String
    let detectorStateIdentity: String
    let plannerRegime: String
    let plannerSeedObservations: Int
    let runnerAdmissionIdentity: String
    let scanCohortIdentity: String
    let pipelineVersions: BaselinePipelineVersions
    let adDetectionDefaults: BaselineAdDetectionDefaults
    let narrowingDefaults: BaselineNarrowingDefaults

    static func current() -> BaselineProductionConfig {
        let config = AdDetectionConfig.default
        let narrowing = NarrowingConfig.default
        let versions = PipelineVersions.current()
        return BaselineProductionConfig(
            entryPoint: "AdDetectionService.runBackfill",
            adDetectionConfigIdentity: "AdDetectionConfig.default",
            narrowingConfigIdentity: "NarrowingConfig.default",
            hotPathClassifierIdentity: "CoreMLSequenceClassifier()",
            foundationModelClassifierIdentity:
                "PlayheadRuntime.makeFoundationModelClassifier(SystemLanguageModel.default)",
            foundationModelEnvironmentIdentity: "production_no_experiment_overrides",
            foundationModelRedactorIdentity: "PromptRedactor.loadDefault",
            detectorStateIdentity:
                "cold_isolated_per_episode:fresh_store;nil_catalog_cache_learning_"
                + "orchestration;fallback_metadata;production_calibration",
            plannerRegime: "targetedWithAudit",
            plannerSeedObservations: 5,
            runnerAdmissionIdentity: "permissive_capability_snapshot+battery_level_1.0",
            scanCohortIdentity: "ScanCohort.productionJSON",
            pipelineVersions: BaselinePipelineVersions(
                modelVersion: versions.modelVersion,
                policyVersion: versions.policyVersion,
                featureSchemaVersion: versions.featureSchemaVersion
            ),
            adDetectionDefaults: adDetectionDefaults(config),
            narrowingDefaults: narrowingDefaults(narrowing)
        )
    }

    private static func adDetectionDefaults(
        _ config: AdDetectionConfig
    ) -> BaselineAdDetectionDefaults {
        BaselineAdDetectionDefaults(
                audioForensicsEnabled: config.audioForensicsEnabled,
                autoSkipConfidenceThreshold: config.autoSkipConfidenceThreshold,
                bracketRefinementEnabled: config.bracketRefinementEnabled,
                bracketRefinementMinCoarseScore: config.bracketRefinementMinCoarseScore,
                bracketRefinementMinFineConfidence: config.bracketRefinementMinFineConfidence,
                bracketRefinementMinTrust: config.bracketRefinementMinTrust,
                candidateThreshold: config.candidateThreshold,
                chapterSignalMode: String(describing: config.chapterSignalMode),
                classifierSeedQualifiedThreshold: config.classifierSeedQualifiedThreshold,
                confirmationThreshold: config.confirmationThreshold,
                crossEpisodeMemoryEnabled: config.crossEpisodeMemoryEnabled,
                crossShowSyndicationEnabled: config.crossShowSyndicationEnabled,
                detectorVersion: config.detectorVersion,
                evidenceFragilityPenaltyEnabled: config.evidenceFragilityPenaltyEnabled,
                fmBackfillMode: String(describing: config.fmBackfillMode),
                fmConsensusThreshold: config.fmConsensusThreshold,
                fmScanBudgetSeconds: config.fmScanBudgetSeconds,
                fragilityPenalty: config.fragilityPenalty,
                fragilityThreshold: config.fragilityThreshold,
                hotPathLookahead: config.hotPathLookahead,
                lexicalAutoAdEnabled: config.lexicalAutoAdEnabled,
                lexicalAutoAdQualifiedThreshold: config.lexicalAutoAdQualifiedThreshold,
                markOnlyThreshold: config.markOnlyThreshold,
                perShowThresholdControlEnabled: config.perShowThresholdControlEnabled,
                perShowThresholdIntegralGain: config.perShowThresholdIntegralGain,
                perShowThresholdMaxOffset: config.perShowThresholdMaxOffset,
                perShowThresholdMinSamples: config.perShowThresholdMinSamples,
                perShowThresholdProportionalGain: config.perShowThresholdProportionalGain,
                rediffSlotOwnershipEnabled: config.rediffSlotOwnershipEnabled,
                rediffSlotShadowEnabled: config.rediffSlotShadowEnabled,
                rhetoricalGrammarEnabled: config.rhetoricalGrammarEnabled,
                segmentAutoSkipThreshold: config.segmentAutoSkipThreshold,
                segmentUICandidateThreshold: config.segmentUICandidateThreshold,
                spanFinalizerEnabled: config.spanFinalizerEnabled,
                spliceSlotOwnershipEnabled: config.spliceSlotOwnershipEnabled,
                spliceSlotShadowEnabled: config.spliceSlotShadowEnabled,
                stingerRefinementEnabled: config.stingerRefinementEnabled,
                suppressionThreshold: config.suppressionThreshold,
                temporalHighConfidenceNeighborThreshold:
                    config.temporalHighConfidenceNeighborThreshold,
                temporalIsolationPenaltyFactor: config.temporalIsolationPenaltyFactor,
                temporalMinDwellPenaltyFactor: config.temporalMinDwellPenaltyFactor,
                temporalMinDwellSeconds: config.temporalMinDwellSeconds,
                temporalNeighborWindowSeconds: config.temporalNeighborWindowSeconds,
                temporalRegularizationEnabled: config.temporalRegularizationEnabled,
                transcriptBoundaryCueEnabled: config.transcriptBoundaryCueEnabled
        )
    }

    private static func narrowingDefaults(
        _ narrowing: NarrowingConfig
    ) -> BaselineNarrowingDefaults {
        BaselineNarrowingDefaults(
            perAnchorPaddingSegments: narrowing.perAnchorPaddingSegments,
            maxNarrowedSegmentsPerPhase: narrowing.maxNarrowedSegmentsPerPhase,
            acousticBreakSnapMaxDistanceSeconds: narrowing.acousticBreakSnapMaxDistanceSeconds,
            lexicalClusterSnapEnabled: narrowing.lexicalClusterSnapEnabled,
            lexicalClusterGapSeconds: narrowing.lexicalClusterGapSeconds,
            lexicalClusterMarginSegments: narrowing.lexicalClusterMarginSegments,
            lexicalClusterMinHits: narrowing.lexicalClusterMinHits
        )
    }
}

private struct BaselineRuntimeIdentity: Encodable {
    let osVersion: String
    let architecture: String
    let captureLane: String
    let deviceUDID: String
    let deviceOSBuild: String
    let localeIdentifier: String
    let xcodeVersionActual: String
    let executableIdentity: String
    let foundationModelsAvailability: String
    let foundationModelsContextSize: Int

    @available(iOS 26.0, *)
    static func current(
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) throws -> BaselineRuntimeIdentity {
        #if arch(arm64)
        let architecture = "arm64"
        #elseif arch(x86_64)
        let architecture = "x86_64"
        #else
        let architecture = "unknown"
        #endif
        #if canImport(FoundationModels)
        let model = SystemLanguageModel.default
        let availability = String(describing: model.availability)
        #if compiler(>=6.3)
        let contextSize = model.contextSize
        #else
        let contextSize = 0
        #endif
        #else
        let availability = "framework_unavailable"
        let contextSize = 0
        #endif
        let captureLane: String
        let deviceUDID: String
        let deviceOSBuild: String
        switch BaselineCaptureTransport.Runtime.current {
        case .physicalIOS:
            guard environment["PLAYHEAD_BASELINE_DEVICE_MODE"] == "1",
                  let configuredUDID = environment["PLAYHEAD_BASELINE_DEVICE_UDID"],
                  configuredUDID.range(
                    of: #"^[0-9A-F]{8}-[0-9A-F]{16}$"#,
                    options: .regularExpression
                  ) != nil,
                  let configuredBuild = environment["PLAYHEAD_BASELINE_DEVICE_OS_BUILD"],
                  configuredBuild.range(
                    of: #"^[0-9A-Za-z.]+$"#,
                    options: .regularExpression
                  ) != nil else {
                throw PipelineDumpRunError(
                    severity: .hard,
                    reason: "physical capture runtime identity is missing or invalid"
                )
            }
            captureLane = "physical_ios"
            deviceUDID = configuredUDID
            deviceOSBuild = configuredBuild
        case .catalyst:
            captureLane = "mac_catalyst"
            deviceUDID = "not_applicable"
            deviceOSBuild = "not_applicable"
        case .simulator:
            captureLane = "ios_simulator"
            deviceUDID = "not_applicable"
            deviceOSBuild = "not_applicable"
        case .unsupported:
            captureLane = "unsupported"
            deviceUDID = "not_applicable"
            deviceOSBuild = "not_applicable"
        }
        let bundle = Bundle.main
        let bundleIdentity = bundle.bundleIdentifier ?? "unknown-bundle"
        let bundleVersion = bundle.object(forInfoDictionaryKey: "CFBundleVersion") as? String
            ?? "unknown-version"
        return BaselineRuntimeIdentity(
            osVersion: ProcessInfo.processInfo.operatingSystemVersionString,
            architecture: architecture,
            captureLane: captureLane,
            deviceUDID: deviceUDID,
            deviceOSBuild: deviceOSBuild,
            localeIdentifier: Locale.current.identifier,
            xcodeVersionActual: environment["XCODE_VERSION_ACTUAL"] ?? "unavailable",
            executableIdentity: "\(bundleIdentity)@\(bundleVersion)",
            foundationModelsAvailability: availability,
            foundationModelsContextSize: contextSize
        )
    }
}

private struct BaselineRawPayload: Encodable {
    let schemaVersion: Int
    let artifactKind: String
    let runId: String
    let capturedAtUtc: String
    let sourceRevision: String
    let evaluationSHA256: String
    let productionConfig: BaselineProductionConfig
    let runtime: BaselineRuntimeIdentity
    let episodes: [BaselineEpisode]
}

private struct BaselineDevicePreflightPayload: Encodable {
    let schemaVersion: Int
    let artifactKind: String
    let runId: String
    let sourceRevision: String
    let evaluationSHA256: String
    let deviceUDID: String
    let expectedOSVersion: String
    let expectedOSBuild: String
    let runtime: BaselineRuntimeIdentity
    let stagedAssetCount: Int
    let outputTransportWritable: Bool
}

private struct ProductionEpisodeRunResult {
    let episode: DumpEpisode
    let transcriptSHA256: String
    let musicFeatures: [BaselineMusicFeature]
}

private struct StableTranscriptResult {
    let chunks: [TranscriptChunk]
    let sha256: String
    let url: URL
}

private struct StableInputSnapshot {
    let corpusRoot: URL
    let audioURL: URL
}

enum BaselineCaptureTransport {
    enum Runtime: Equatable {
        case physicalIOS
        case catalyst
        case simulator
        case unsupported

        static var current: Runtime {
            #if targetEnvironment(macCatalyst)
            return .catalyst
            #elseif targetEnvironment(simulator)
            return .simulator
            #elseif os(iOS)
            return .physicalIOS
            #else
            return .unsupported
            #endif
        }
    }

    struct Paths {
        let sourceRoot: URL
        let corpusRoot: URL
        let outputURL: URL
    }

    enum TransportError: Error, CustomStringConvertible {
        case deviceModeRequired
        case invalidDevicePath(String)
        case hostOutputPathMissing
        case physicalRuntimeRequired

        var description: String {
            switch self {
            case .deviceModeRequired:
                return "physical baseline capture requires PLAYHEAD_BASELINE_DEVICE_MODE=1"
            case .invalidDevicePath(let key):
                return "\(key) must be a relative path beneath Documents/l2f8"
            case .hostOutputPathMissing:
                return "PLAYHEAD_BASELINE_OUTPUT_PATH is required for Catalyst capture"
            case .physicalRuntimeRequired:
                return "device-mode baseline capture refuses simulator and unsupported runtimes"
            }
        }
    }

    static func resolve(
        environment: [String: String],
        fallbackSourceRoot: URL,
        documentsDirectory: URL,
        runtime: Runtime
    ) throws -> Paths {
        switch runtime {
        case .physicalIOS:
            guard environment["PLAYHEAD_BASELINE_DEVICE_MODE"] == "1" else {
                throw TransportError.deviceModeRequired
            }
            let sourceRoot = try resolveDevicePath(
                key: "PLAYHEAD_BASELINE_DEVICE_INPUT_ROOT",
                environment: environment,
                documentsDirectory: documentsDirectory
            )
            let outputURL = try resolveDevicePath(
                key: "PLAYHEAD_BASELINE_DEVICE_OUTPUT_PATH",
                environment: environment,
                documentsDirectory: documentsDirectory
            )
            return Paths(
                sourceRoot: sourceRoot,
                corpusRoot: sourceRoot,
                outputURL: outputURL
            )
        case .catalyst:
            if environment["PLAYHEAD_BASELINE_DEVICE_MODE"] == "1" {
                throw TransportError.physicalRuntimeRequired
            }
            guard let outputPath = environment["PLAYHEAD_BASELINE_OUTPUT_PATH"] else {
                throw TransportError.hostOutputPathMissing
            }
            let corpusRoot = environment["PLAYHEAD_CORPUS_ROOT"].map {
                URL(fileURLWithPath: $0, isDirectory: true)
            } ?? fallbackSourceRoot
            return Paths(
                sourceRoot: fallbackSourceRoot,
                corpusRoot: corpusRoot,
                outputURL: URL(fileURLWithPath: outputPath)
            )
        case .simulator, .unsupported:
            throw TransportError.physicalRuntimeRequired
        }
    }

    static func resolveDevicePath(
        key: String,
        environment: [String: String],
        documentsDirectory: URL
    ) throws -> URL {
        guard let path = environment[key],
              !path.isEmpty,
              !path.hasPrefix("/"),
              !PartialSilverEvaluationLoader.containsTraversalComponent(path) else {
            throw TransportError.invalidDevicePath(key)
        }
        let components = path.split(separator: "/", omittingEmptySubsequences: false)
        guard components.count >= 2,
              components.first == "l2f8",
              components.allSatisfy({ !$0.isEmpty }) else {
            throw TransportError.invalidDevicePath(key)
        }
        let documents = documentsDirectory.standardizedFileURL
        let resolved = documents.appendingPathComponent(path).standardizedFileURL
        guard resolved.path.hasPrefix(documents.path + "/l2f8/") else {
            throw TransportError.invalidDevicePath(key)
        }
        return resolved
    }
}

private struct BaselineDeviceStagingManifest: Decodable {
    struct Asset: Decodable {
        let episodeId: String
        let audioPath: String
        let audioSHA256: String
        let transcriptPath: String
        let transcriptSHA256: String
        let transcriptAudioFingerprint: String

        enum CodingKeys: String, CodingKey {
            case episodeId = "episode_id"
            case audioPath = "audio_path"
            case audioSHA256 = "audio_sha256"
            case transcriptPath = "transcript_path"
            case transcriptSHA256 = "transcript_sha256"
            case transcriptAudioFingerprint = "transcript_audio_fingerprint"
        }
    }

    let artifactKind: String
    let schemaVersion: Int
    let sourceRevision: String
    let evaluationSHA256: String
    let runId: String
    let assets: [Asset]

    enum CodingKeys: String, CodingKey {
        case artifactKind = "artifact_kind"
        case schemaVersion = "schema_version"
        case sourceRevision = "source_revision"
        case evaluationSHA256 = "evaluation_sha256"
        case runId = "run_id"
        case assets
    }
}

private enum BaselineDeviceStagingValidator {
    static let manifestName = "playhead-l2f8-device-staging.json"

    static func validate(
        root: URL,
        evaluation: PartialSilverEvaluation,
        runId: String,
        sourceRevision: String
    ) throws {
        let manifestURL = root.appendingPathComponent(manifestName)
        let data = try PartialSilverEvaluationLoader.readRegularBytes(
            at: manifestURL,
            maximumBytes: 1_024 * 1_024
        )
        let manifest = try JSONDecoder().decode(BaselineDeviceStagingManifest.self, from: data)
        guard manifest.artifactKind == "physical_device_partial_silver_staging",
              manifest.schemaVersion == 1,
              manifest.sourceRevision == sourceRevision,
              manifest.evaluationSHA256 == PartialSilverEvaluationLoader.evaluationSHA256,
              manifest.runId == runId,
              manifest.assets.count == PartialSilverEvaluationLoader.expectedAssetCount,
              Set(manifest.assets.map(\.episodeId)).count == manifest.assets.count else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "physical-device staging manifest identity is invalid"
            )
        }
        let manifestAssets = Dictionary(
            uniqueKeysWithValues: manifest.assets.map { ($0.episodeId, $0) }
        )
        guard Set(manifestAssets.keys) == Set(evaluation.assets.map(\.episodeId)) else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "physical-device staging is not the exact 27-asset evaluation"
            )
        }
        for asset in evaluation.assets {
            guard let staged = manifestAssets[asset.episodeId],
                  staged.audioSHA256 == String(asset.audioFingerprint.dropFirst("sha256:".count)),
                  isDirectCorpusPath(
                    staged.audioPath,
                    directory: "Audio",
                    episodeId: asset.episodeId
                  ),
                  staged.transcriptPath
                    == "TestFixtures/Corpus/Transcripts/\(asset.episodeId).json",
                  staged.transcriptAudioFingerprint == asset.audioFingerprint else {
                throw PipelineDumpRunError(
                    severity: .hard,
                    reason: "physical-device staging binding is invalid for \(asset.episodeId)"
                )
            }
            let audioURL = try PartialSilverEvaluationLoader.audioURL(
                for: asset,
                corpusRoot: root
            )
            guard audioURL.path.hasSuffix(staged.audioPath) else {
                throw PipelineDumpRunError(
                    severity: .hard,
                    reason: "physical-device audio path differs for \(asset.episodeId)"
                )
            }
            let transcriptURL = try PartialSilverEvaluationLoader.transcriptURL(
                for: asset.episodeId,
                corpusRoot: root
            )
            let transcriptSHA256 = try PartialSilverEvaluationLoader.fileSHA256Hex(
                transcriptURL,
                maximumBytes: 128 * 1_024 * 1_024
            )
            let transcriptData = try PartialSilverEvaluationLoader.readRegularBytes(
                at: transcriptURL,
                maximumBytes: 128 * 1_024 * 1_024
            )
            let transcriptDocument = try JSONSerialization.jsonObject(with: transcriptData)
            guard transcriptSHA256 == staged.transcriptSHA256,
                  let transcriptObject = transcriptDocument as? [String: Any],
                  transcriptObject["source_audio_fingerprint"] as? String
                    == asset.audioFingerprint else {
                throw PipelineDumpRunError(
                    severity: .hard,
                    reason: "physical-device transcript lineage differs for \(asset.episodeId)"
                )
            }
        }
    }

    private static func isDirectCorpusPath(
        _ path: String,
        directory: String,
        episodeId: String
    ) -> Bool {
        let prefix = "TestFixtures/Corpus/\(directory)/"
        guard path.hasPrefix(prefix) else { return false }
        let name = String(path.dropFirst(prefix.count))
        return !name.contains("/")
            && (name as NSString).deletingPathExtension == episodeId
    }
}

private enum BaselineRawValidator {
    private static let decisionStates: Set<String> = [
        "applied", "candidate", "confirmed", "reverted", "suppressed"
    ]
    private static let musicBedLevels: Set<String> = [
        "background", "foreground", "none"
    ]

    static func validate(
        episodes: [BaselineEpisode],
        evaluation: PartialSilverEvaluation
    ) throws {
        let assets = Dictionary(
            uniqueKeysWithValues: evaluation.assets.map { ($0.episodeId, $0) }
        )
        guard episodes.count == PartialSilverEvaluationLoader.expectedAssetCount,
              Set(episodes.map(\.episodeId)).count == episodes.count,
              Set(episodes.map(\.episodeId)) == Set(assets.keys) else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "raw baseline membership is not the exact 27-asset evaluation"
            )
        }
        for episode in episodes {
            guard let asset = assets[episode.episodeId] else {
                throw invalidEpisode(episode.episodeId)
            }
            try validate(episode, against: asset)
        }
    }

    private static func validate(
        _ episode: BaselineEpisode,
        against asset: PartialSilverEvaluation.Asset
    ) throws {
        guard episode.audioFingerprint == asset.audioFingerprint,
              episode.showName == asset.showName,
              episode.durationSeconds.isFinite,
              episode.durationSeconds > 0,
              abs(episode.durationSeconds - asset.durationSeconds) <= 1.0,
              isSHA256(episode.transcriptSHA256),
              episode.predictions.count <= 4_096,
              episode.musicFeatures.count <= 4_096 else {
            throw invalidEpisode(episode.episodeId)
        }
        for prediction in episode.predictions {
            guard prediction.startSeconds.isFinite,
                  prediction.endSeconds.isFinite,
                  prediction.confidence.isFinite,
                  prediction.startSeconds >= 0,
                  prediction.startSeconds < prediction.endSeconds,
                  prediction.endSeconds <= episode.durationSeconds,
                  (0...1).contains(prediction.confidence),
                  decisionStates.contains(prediction.decisionState) else {
                throw invalidEpisode(episode.episodeId)
            }
        }
        var featureIntervals: Set<String> = []
        for feature in episode.musicFeatures {
            let values = [
                feature.startSeconds, feature.endSeconds, feature.musicProbability,
                feature.musicBedChangeScore, feature.musicBedOnsetScore,
                feature.musicBedOffsetScore
            ]
            let interval = "\(feature.startSeconds)|\(feature.endSeconds)"
            guard values.allSatisfy(\.isFinite),
                  feature.startSeconds >= 0,
                  feature.startSeconds < feature.endSeconds,
                  feature.endSeconds <= episode.durationSeconds,
                  values.dropFirst(2).allSatisfy({ (0...1).contains($0) }),
                  musicBedLevels.contains(feature.musicBedLevel),
                  featureIntervals.insert(interval).inserted else {
                throw invalidEpisode(episode.episodeId)
            }
        }
    }

    private static func isSHA256(_ value: String) -> Bool {
        value.count == 64 && value.unicodeScalars.allSatisfy {
            CharacterSet(charactersIn: "0123456789abcdef").contains($0)
        }
    }

    private static func invalidEpisode(_ episodeId: String) -> PipelineDumpRunError {
        PipelineDumpRunError(
            severity: .hard,
            reason: "raw baseline contains invalid bounded data for \(episodeId)"
        )
    }
}

enum BaselineRawPublisher {
    private static let requiredRunIds: Set<String> = [
        "baseline-run-1", "baseline-run-2", "baseline-run-3"
    ]
    private static let runIdCharacters = CharacterSet(
        charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
    )
    private static let firstRunIdCharacters = CharacterSet.alphanumerics
    private static let lowercaseHexCharacters = CharacterSet(
        charactersIn: "0123456789abcdef"
    )

    enum PublishError: Error, CustomStringConvertible {
        case invalidRunId
        case invalidSourceRevision
        case invalidOutputPath
        case experimentOverride(String)
        case unsafeOutputDirectory(URL)
        case outputExists(URL)
        case publicationFailed(String)

        var description: String {
            switch self {
            case .invalidRunId:
                return "PLAYHEAD_BASELINE_RUN_ID must be baseline-run-1, -2, or -3"
            case .invalidSourceRevision:
                return "PLAYHEAD_BASELINE_SOURCE_REVISION must be a 40-digit lowercase Git revision"
            case .invalidOutputPath:
                return "PLAYHEAD_BASELINE_OUTPUT_PATH must be an absolute path named for the run ID"
            case .experimentOverride(let key):
                return "production baseline forbids Foundation Models override \(key)"
            case .unsafeOutputDirectory(let url):
                return "baseline output directory is missing, symlinked, or aliased: \(url.path)"
            case .outputExists(let url):
                return "baseline raw output already exists; refusing overwrite: \(url.path)"
            case .publicationFailed(let reason):
                return "baseline raw publication failed: \(reason)"
            }
        }
    }

    static func validateRunId(_ value: String) throws -> String {
        guard (1...64).contains(value.count),
              value != ".", value != "..",
              value.unicodeScalars.allSatisfy({ runIdCharacters.contains($0) }),
              value.unicodeScalars.first.map({ firstRunIdCharacters.contains($0) }) == true,
              requiredRunIds.contains(value) else {
            throw PublishError.invalidRunId
        }
        return value
    }

    static func validateSourceRevision(_ value: String) throws -> String {
        guard value.count == 40,
              value.unicodeScalars.allSatisfy({ lowercaseHexCharacters.contains($0) }) else {
            throw PublishError.invalidSourceRevision
        }
        return value
    }

    static func outputURL(path: String, runId: String) throws -> URL {
        guard path.hasPrefix("/"),
              !PartialSilverEvaluationLoader.containsTraversalComponent(path) else {
            throw PublishError.invalidOutputPath
        }
        let outputURL = URL(fileURLWithPath: path).standardizedFileURL
        let expectedName = "playhead-partial-silver-baseline-\(runId).json"
        guard outputURL.lastPathComponent == expectedName else {
            throw PublishError.invalidOutputPath
        }
        let directory = outputURL.deletingLastPathComponent()
        guard let values = try? directory.resourceValues(
            forKeys: [.isDirectoryKey, .isSymbolicLinkKey, .isAliasFileKey]
        ), values.isDirectory == true,
           values.isSymbolicLink != true,
           values.isAliasFile != true,
           !PartialSilverEvaluationLoader.hasUnsafeFilesystemComponent(directory) else {
            throw PublishError.unsafeOutputDirectory(directory)
        }
        return outputURL
    }

    static func validateProductionEnvironment(_ environment: [String: String]) throws {
        for key in [
            "PLAYHEAD_FM_DROP_PREAMBLE",
            "PLAYHEAD_FM_PROMPT_VARIANT",
            "PLAYHEAD_FM_REDACT"
        ] where environment[key] != nil {
            throw PublishError.experimentOverride(key)
        }
    }

    static func publish(
        _ data: Data,
        to outputURL: URL,
        beforeLink: (() throws -> Void)? = nil
    ) throws {
        let directoryURL = outputURL.deletingLastPathComponent()
        let directoryDescriptor: Int32
        do {
            directoryDescriptor = try PartialSilverEvaluationLoader.openDirectoryDescriptor(
                at: directoryURL
            )
        } catch {
            throw PublishError.unsafeOutputDirectory(directoryURL)
        }
        defer { Darwin.close(directoryDescriptor) }
        let finalName = outputURL.lastPathComponent
        var existing = stat()
        let existingStatus = finalName.withCString {
            fstatat(directoryDescriptor, $0, &existing, AT_SYMLINK_NOFOLLOW)
        }
        if existingStatus == 0 { throw PublishError.outputExists(outputURL) }
        guard errno == ENOENT else {
            throw PublishError.publicationFailed("cannot inspect destination")
        }

        let temporaryName = ".\(finalName).\(UUID().uuidString).tmp"
        let temporaryDescriptor = temporaryName.withCString {
            openat(
                directoryDescriptor,
                $0,
                O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                S_IRUSR | S_IWUSR
            )
        }
        guard temporaryDescriptor >= 0 else {
            throw PublishError.publicationFailed("cannot create private staging file")
        }
        defer {
            Darwin.close(temporaryDescriptor)
            temporaryName.withCString { _ = unlinkat(directoryDescriptor, $0, 0) }
        }
        try data.withUnsafeBytes { bytes in
            var offset = 0
            while offset < bytes.count {
                guard let baseAddress = bytes.baseAddress else { break }
                let written = Darwin.write(
                    temporaryDescriptor,
                    baseAddress.advanced(by: offset),
                    bytes.count - offset
                )
                if written < 0, errno == EINTR { continue }
                guard written > 0 else {
                    throw PublishError.publicationFailed("cannot write staged bytes")
                }
                offset += written
            }
        }
        guard fsync(temporaryDescriptor) == 0 else {
            throw PublishError.publicationFailed("cannot sync staged bytes")
        }
        try beforeLink?()
        let linkStatus = temporaryName.withCString { temporaryPointer in
            finalName.withCString { finalPointer in
                linkat(
                    directoryDescriptor,
                    temporaryPointer,
                    directoryDescriptor,
                    finalPointer,
                    0
                )
            }
        }
        if linkStatus != 0, errno == EEXIST {
            throw PublishError.outputExists(outputURL)
        }
        guard linkStatus == 0, fsync(directoryDescriptor) == 0 else {
            throw PublishError.publicationFailed("cannot link or sync final output")
        }
        let current: Int32
        do {
            current = try PartialSilverEvaluationLoader.openDirectoryDescriptor(at: directoryURL)
        } catch {
            throw PublishError.unsafeOutputDirectory(directoryURL)
        }
        defer { Darwin.close(current) }
        var pinnedMetadata = stat()
        var currentMetadata = stat()
        guard fstat(directoryDescriptor, &pinnedMetadata) == 0,
              fstat(current, &currentMetadata) == 0,
              pinnedMetadata.st_dev == currentMetadata.st_dev,
              pinnedMetadata.st_ino == currentMetadata.st_ino else {
            throw PublishError.unsafeOutputDirectory(directoryURL)
        }
    }
}

enum BaselineSourceIdentity {
    enum SourceError: Error, CustomStringConvertible {
        case buildRevisionUnavailable
        case revisionMismatch(declared: String, build: String)

        var description: String {
            switch self {
            case .buildRevisionUnavailable:
                return "production baseline build lacks a Git revision stamp"
            case .revisionMismatch(let declared, let build):
                return "declared source revision \(declared) does not match build \(build)"
            }
        }
    }

    static func resolve(declaredRevision: String) throws -> String {
        try validate(
            declaredRevision: declaredRevision,
            buildRevision: BuildInfo.commitSHA
        )
        return declaredRevision
    }

    static func validate(declaredRevision: String, buildRevision: String) throws {
        let declared = try BaselineRawPublisher.validateSourceRevision(declaredRevision)
        let lowercaseHex = CharacterSet(charactersIn: "0123456789abcdef")
        guard (7...40).contains(buildRevision.count),
              buildRevision.unicodeScalars.allSatisfy({ lowercaseHex.contains($0) }) else {
            throw SourceError.buildRevisionUnavailable
        }
        guard declared.hasPrefix(buildRevision) else {
            throw SourceError.revisionMismatch(declared: declared, build: buildRevision)
        }
    }

}

// MARK: - Rediff treatment support (playhead-xsdz.36.1)

/// Real `RediffBSideProvider` for the Catalyst rediff-treatment lane. Resolves
/// the offline-staged fresh B-side at
/// `TestFixtures/Corpus/Audio/<assetId>.fresh.mp3` (the offline re-fetch step
/// stages these for rotated episodes — this provider only CONSUMES them, it
/// never fetches), decodes it via the SAME 16 kHz `AnalysisAudioService` path
/// the A-side uses, concatenates shards in `startTime` order into one mono
/// `[Float]`, and memoizes the MOST RECENT assetId only (see the memo note on
/// the stored properties).
///
/// Deliberately NOT `CorpusAudioFixtures.decodeMono11025`: that pre-resamples to
/// 11025 Hz, but `RediffSlotOwnership.gateAndDiff` fingerprints the B-side with
/// the SAME resample(16k→11025)→ChromaFingerprint extractor it applies to the
/// A-side (the `(resampler+fingerprinter)` versioned unit), so the provider must
/// hand it RAW 16 kHz PCM — the analysis pipeline's decode rate.
///
/// Returns `nil` for an assetId with no staged `.fresh.mp3` — the correct no-op
/// for a non-rotated episode (the rediff pass then falls through to status-quo
/// width, exactly as in production where no provider is injected).
private actor CorpusFreshBSideProvider: RediffBSideProvider {
    private let audioDirectory: URL
    /// Single-entry memo (R1): the lane visits episodes SEQUENTIALLY and each
    /// asks for its own assetId, so memoization only needs to absorb repeat
    /// calls within one episode. A grow-forever `[assetId: [Float]?]` dictionary
    /// would instead pin EVERY decoded B-side (~230 MB of PCM per hour-long
    /// episode) for the whole 90-minute lane — multi-GB on the 16 GB capture
    /// machine that is also running FM inference.
    private var lastAssetId: String?
    private var lastSamples: [Float]?

    init(repoRoot: URL) {
        audioDirectory = Self.audioDirectory(repoRoot: repoRoot)
    }

    /// The corpus audio directory this provider resolves against. Static so the
    /// treatment lane's pre-flight B-side coverage guard (R3) and the env-gated
    /// decode test consult the SAME root the decode path uses — a drift between
    /// the guard's lookup and the provider's lookup would defeat the guard.
    static func audioDirectory(repoRoot: URL) -> URL {
        // R4: anchor to the SAME canonical constant the A-side manifest
        // resolution uses (`PipelineDumpManifestLoader.audioURL`) — a corpus-dir
        // rename must move both sides of the A/B handoff together, not strand
        // the B-side lookup on a stale hard-coded string.
        repoRoot.appendingPathComponent(CorpusAnnotationLoader.audioRelativePath, isDirectory: true)
    }

    /// The exact staged-fresh-B-side URL the decode path consults for `assetId`.
    /// Single source of truth for the `<assetId>.fresh.mp3` naming convention
    /// (the retention side lives in `scripts/l2f-dai-rediff.py --retain-audio`,
    /// whose manifest validation pins `audioPath` stem == episodeId, so both
    /// sides of the handoff agree on this name).
    static func freshURL(assetId: String, audioDirectory: URL) -> URL {
        audioDirectory.appendingPathComponent("\(assetId).fresh.mp3", isDirectory: false)
    }

    /// Three-way staging probe for `<assetId>.fresh.mp3` — the SINGLE
    /// acceptance predicate shared by the decode path and the treatment lane's
    /// pre-flight coverage guard (R4). The guard previously classified
    /// "staged" by bare `fileExists`, which counts a SYMLINKED or otherwise
    /// irregular file the decode path refuses (the R1 anchor check) — an
    /// all-symlink staging would pass pre-flight and still produce the
    /// baseline-identical 90-minute run the guard exists to prevent.
    enum StagedFreshProbe: Equatable {
        /// Nothing at the staged path — the correct non-rotated no-op.
        case absent
        /// SOMETHING is at the staged path but the decode path refuses it — a
        /// symlink (including a DANGLING one), a non-regular file, an empty
        /// (zero-byte) file, or a path whose metadata cannot be read for any
        /// reason OTHER than not existing (R6: permission/I-O failures fail
        /// closed here): a STAGING problem the pre-flight must not count as
        /// staged.
        case irregular
        /// A regular, unaliased, non-empty file the decode path will accept.
        case staged
    }

    static func probeStagedFresh(assetId: String, audioDirectory: URL) -> StagedFreshProbe {
        let url = freshURL(assetId: assetId, audioDirectory: audioDirectory)
        // R5: `resourceValues` reads the URL ITSELF (no traversal of a final
        // symlink), unlike `fileExists(atPath:)`, which FOLLOWS symlinks and
        // would misfile a DANGLING symlink as `.absent` — but something IS
        // staged there and the decode path refuses it, so it must surface as
        // `.irregular` (a staging problem), not a silent non-rotated no-op.
        let values: URLResourceValues
        do {
            values = try url.resourceValues(
                forKeys: [.isRegularFileKey, .isSymbolicLinkKey, .fileSizeKey]
            )
        } catch {
            // R6: only the no-such-file family means "nothing exists at the
            // path" (the ordinary non-rotated `.absent` no-op). Any OTHER read
            // failure — permission, I/O — means something MAY be staged but is
            // unusable, so fail CLOSED to `.irregular`: loud in the pre-flight
            // coverage guard, unstaged in the decode path. R5's `try?`
            // collapsed every failure into the SILENT `.absent` verdict.
            return classifyProbeReadError(error)
        }
        // Regular + unaliased + NON-EMPTY (R5): a zero-byte "staged" file can
        // never decode, so counting it staged would let an empty-file staging
        // pass the pre-flight guard and burn the 90-minute lane on a run that
        // is structurally baseline-identical — the silent-no-lift failure the
        // guard exists to prevent. `fileSize` is always populated for regular
        // files on APFS; a nil reading fails CLOSED (irregular → loud in the
        // pre-flight, unstaged in the decode path).
        guard values.isRegularFile == true,
              values.isSymbolicLink != true,
              let size = values.fileSize, size > 0 else {
            return .irregular
        }
        return .staged
    }

    /// R6: classify a `resourceValues` read failure. `.absent` ONLY for the
    /// no-such-file family (Cocoa `fileReadNoSuchFile`/`fileNoSuchFile`, POSIX
    /// `ENOENT`/`ENOTDIR` — walking the underlying-error chain, since Foundation
    /// wraps the POSIX cause); everything else (permission, I/O) is `.irregular`
    /// — fail closed rather than silently misfiling an unreadable staged file
    /// as a non-rotated episode. Static + pure so the wiring suite can pin the
    /// mapping hermetically without inducing real EACCES/EIO on disk.
    static func classifyProbeReadError(_ error: Error) -> StagedFreshProbe {
        var next: NSError? = error as NSError
        var hops = 0
        while let current = next, hops < 8 {
            if current.domain == NSCocoaErrorDomain,
               current.code == NSFileReadNoSuchFileError
                   || current.code == NSFileNoSuchFileError {
                return .absent
            }
            if current.domain == NSPOSIXErrorDomain,
               current.code == Int(ENOENT) || current.code == Int(ENOTDIR) {
                return .absent
            }
            next = current.userInfo[NSUnderlyingErrorKey] as? NSError
            hops += 1
        }
        return .irregular
    }

    func refetchedBSideMono16kHz(assetId: String) async -> [Float]? {
        if lastAssetId == assetId { return lastSamples }
        let samples = await Self.decodeFresh(assetId: assetId, audioDirectory: audioDirectory)
        lastAssetId = assetId
        lastSamples = samples
        return samples
    }

    private static func decodeFresh(assetId: String, audioDirectory: URL) async -> [Float]? {
        let freshURL = Self.freshURL(assetId: assetId, audioDirectory: audioDirectory)
        // R1: same regular-file / no-symlink anchor the A-side audio check in
        // `runSingleEpisode` applies (bf4a2383 precedent) — a staged B-side must
        // be a real file, not an alias into arbitrary bytes. R4 hoisted the
        // check into `probeStagedFresh` so the pre-flight coverage guard applies
        // the IDENTICAL acceptance predicate. Unlike an UNSTAGED id (silent nil
        // = correct non-rotated no-op), an irregular staged path is a staging
        // problem, so say so in the transcript.
        switch Self.probeStagedFresh(assetId: assetId, audioDirectory: audioDirectory) {
        case .absent:
            return nil
        case .irregular:
            print("[xsdz.36.1] fresh B-side at \(freshURL.path) is not a usable staged file (symlink, non-regular, empty, or unreadable) — treating as unstaged")
            return nil
        case .staged:
            break
        }
        guard let localURL = LocalAudioURL(freshURL) else { return nil }
        // DISTINCT decode episodeID: `AnalysisAudioService.decode` persists to a
        // file-backed ShardCache keyed by episodeID, and the A-side decode in
        // `runSingleEpisode` already populated the cache under the bare `assetId`
        // with the ORIGINAL played audio. Reusing that key here would return the
        // A-side shards for the fresh B-side (B == A ⇒ no diff ⇒ no widening — the
        // treatment would silently degrade to the baseline). A `.fresh`-suffixed
        // key isolates the B-side; evicting first guarantees the CURRENT fresh
        // bytes are decoded even if a prior run cached a superseded re-fetch.
        let bSideEpisodeID = "\(assetId).fresh"
        let audio = AnalysisAudioService()
        await audio.evictCache(episodeID: bSideEpisodeID)
        do {
            let shards = try await audio.decode(
                fileURL: localURL,
                episodeID: bSideEpisodeID,
                shardDuration: AnalysisAudioService.defaultShardDuration
            )
            let samples = shards
                .sorted { $0.startTime < $1.startTime }
                .flatMap { $0.samples }
            // R1: the `.fresh` shard cache is WRITE-only — the evict-first above
            // means no later run ever reads it — so drop it now rather than leave
            // ~230 MB/episode of dead PCM under Application Support for the
            // lane's whole corpus (disk-hygiene mandate).
            await audio.evictCache(episodeID: bSideEpisodeID)
            return samples.isEmpty ? nil : samples
        } catch {
            // A STAGED fresh file that fails to decode is a staging problem, not
            // a non-rotated episode. Still return nil (status-quo width — that
            // episode degrades to baseline rather than crashing the 90-min run),
            // but say so in the transcript so a silent treatment==baseline
            // outcome is diagnosable.
            print("[xsdz.36.1] fresh B-side decode FAILED for \(assetId): \(error)")
            return nil
        }
    }
}

/// Build the rediff TREATMENT config: literally `AdDetectionConfig.default` in
/// every output-affecting field EXCEPT `rediffSlotOwnershipEnabled`, flipped ON.
/// Reads each value from `.default` (single source of truth) so the treatment
/// and baseline lanes differ ONLY by the rediff flag + the injected B-side
/// provider — no hand-copied constant can drift from the shipped default.
/// `spliceSlotOwnershipEnabled` keeps its `.default` value (OFF): the two are
/// mutually-exclusive width setters and the config init preconditions on it.
private func makeRediffTreatmentConfig() -> AdDetectionConfig {
    let base = AdDetectionConfig.default
    return AdDetectionConfig(
        candidateThreshold: base.candidateThreshold,
        confirmationThreshold: base.confirmationThreshold,
        suppressionThreshold: base.suppressionThreshold,
        hotPathLookahead: base.hotPathLookahead,
        detectorVersion: base.detectorVersion,
        fmBackfillMode: base.fmBackfillMode,
        fmScanBudgetSeconds: base.fmScanBudgetSeconds,
        fmConsensusThreshold: base.fmConsensusThreshold,
        markOnlyThreshold: base.markOnlyThreshold,
        autoSkipConfidenceThreshold: base.autoSkipConfidenceThreshold,
        classifierSeedQualifiedThreshold: base.classifierSeedQualifiedThreshold,
        lexicalAutoAdQualifiedThreshold: base.lexicalAutoAdQualifiedThreshold,
        lexicalAutoAdEnabled: base.lexicalAutoAdEnabled,
        segmentUICandidateThreshold: base.segmentUICandidateThreshold,
        segmentAutoSkipThreshold: base.segmentAutoSkipThreshold,
        bracketRefinementEnabled: base.bracketRefinementEnabled,
        bracketRefinementMinTrust: base.bracketRefinementMinTrust,
        bracketRefinementMinCoarseScore: base.bracketRefinementMinCoarseScore,
        bracketRefinementMinFineConfidence: base.bracketRefinementMinFineConfidence,
        transcriptBoundaryCueEnabled: base.transcriptBoundaryCueEnabled,
        evidenceFragilityPenaltyEnabled: base.evidenceFragilityPenaltyEnabled,
        fragilityThreshold: base.fragilityThreshold,
        fragilityPenalty: base.fragilityPenalty,
        chapterSignalMode: base.chapterSignalMode,
        audioForensicsEnabled: base.audioForensicsEnabled,
        crossEpisodeMemoryEnabled: base.crossEpisodeMemoryEnabled,
        rhetoricalGrammarEnabled: base.rhetoricalGrammarEnabled,
        crossShowSyndicationEnabled: base.crossShowSyndicationEnabled,
        temporalRegularizationEnabled: base.temporalRegularizationEnabled,
        temporalNeighborWindowSeconds: base.temporalNeighborWindowSeconds,
        temporalHighConfidenceNeighborThreshold: base.temporalHighConfidenceNeighborThreshold,
        temporalIsolationPenaltyFactor: base.temporalIsolationPenaltyFactor,
        temporalMinDwellSeconds: base.temporalMinDwellSeconds,
        temporalMinDwellPenaltyFactor: base.temporalMinDwellPenaltyFactor,
        perShowThresholdControlEnabled: base.perShowThresholdControlEnabled,
        perShowThresholdProportionalGain: base.perShowThresholdProportionalGain,
        perShowThresholdIntegralGain: base.perShowThresholdIntegralGain,
        perShowThresholdMaxOffset: base.perShowThresholdMaxOffset,
        perShowThresholdMinSamples: base.perShowThresholdMinSamples,
        spanFinalizerEnabled: base.spanFinalizerEnabled,
        spliceSlotOwnershipEnabled: base.spliceSlotOwnershipEnabled,
        spliceSlotShadowEnabled: base.spliceSlotShadowEnabled,
        rediffSlotOwnershipEnabled: true,
        rediffSlotShadowEnabled: base.rediffSlotShadowEnabled,
        userCorrectionReadSideEnabled: base.userCorrectionReadSideEnabled,
        stingerRefinementEnabled: base.stingerRefinementEnabled,
        lexicalAnchorRefinementEnabled: base.lexicalAnchorRefinementEnabled,
        selfPromoSuppressionEnabled: base.selfPromoSuppressionEnabled
    )
}

/// R3 (measurement-integrity guard), R4-hardened: classify each manifest
/// entry's fresh B-side by the provider's EXACT acceptance predicate
/// (`CorpusFreshBSideProvider.probeStagedFresh` — the URL derivation AND the
/// regular-unaliased-file anchor, not bare existence; R3's bare `fileExists`
/// counted a symlinked staging the decode path refuses). The treatment lane
/// fails fast when NO entry is usable — a zero-B-side treatment run would still
/// complete after ~90 minutes and write a dump whose widths are
/// baseline-identical, and the orchestrator's treatment-vs-baseline diff would
/// read "no lift" with nothing actually measured. Pure over its inputs
/// (filesystem read only) so the wiring suite can pin it hermetically.
private struct RediffTreatmentBSideClassification {
    /// Entries the decode path would treat as unstaged (absent OR irregular) —
    /// status-quo width for these episodes. Manifest order.
    var unstagedEpisodeIds: [String] = []
    /// Subset of `unstagedEpisodeIds`: something EXISTS (or may exist) at the
    /// staged path but the decode path refuses it (symlink — including dangling
    /// — non-regular, empty file, or a path whose metadata cannot be read for
    /// any reason other than not existing; R6 fails those closed) — a staging
    /// PROBLEM worth calling out separately from a plain non-rotated episode.
    var irregularEpisodeIds: [String] = []
}

private func classifyRediffTreatmentBSides(
    entries: [PipelineDumpSnapshotEntry],
    audioDirectory: URL
) -> RediffTreatmentBSideClassification {
    var classification = RediffTreatmentBSideClassification()
    for episodeId in entries.map(\.episodeId) {
        switch CorpusFreshBSideProvider.probeStagedFresh(
            assetId: episodeId,
            audioDirectory: audioDirectory
        ) {
        case .staged:
            break
        case .absent:
            classification.unstagedEpisodeIds.append(episodeId)
        case .irregular:
            classification.unstagedEpisodeIds.append(episodeId)
            classification.irregularEpisodeIds.append(episodeId)
        }
    }
    return classification
}

// MARK: - Live test

final class PipelineDumpLiveTests: XCTestCase {

    /// The dump test is opt-in via env var, matching the sibling live
    /// harnesses. Default `PlayheadFastTests` does NOT set it, so the test
    /// body is a no-op on Cmd-U.
    private static var dumpEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_PIPELINE_DUMP"] == "1"
    }

    private static var partialSilverBaselineEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_PARTIAL_SILVER_BASELINE"] == "1"
    }

    private static var devicePreflightEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_BASELINE_DEVICE_PREFLIGHT"] == "1"
    }

    /// playhead-xsdz.36.1: opt-in for the rediff-ACTIVATION (treatment) dump —
    /// the sibling lane that captures the A-side, injects the fresh-B-side
    /// provider + `rediffSlotOwnershipEnabled`, and dumps the WIDENED windows.
    private static var rediffTreatmentEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_PIPELINE_DUMP_REDIFF"] == "1"
    }

    override func setUp() async throws {
        try await super.setUp()
        try XCTSkipUnless(
            Self.dumpEnabled || Self.partialSilverBaselineEnabled
                || Self.devicePreflightEnabled || Self.rediffTreatmentEnabled,
            """
            Live production-path capture is opt-in and slow. Set either \
            PLAYHEAD_PIPELINE_DUMP=1 for the legacy snapshot dump or \
            PLAYHEAD_PARTIAL_SILVER_BASELINE=1 for the immutable 27-asset \
            baseline, PLAYHEAD_BASELINE_DEVICE_PREFLIGHT=1 for the bounded \
            physical-device transport check, or PLAYHEAD_PIPELINE_DUMP_REDIFF=1 \
            for the rediff-activation treatment dump.
            """
        )
    }

    // MARK: - Entry

    func testProductionPipelineDumpOnNewEpisodes() async throws {
        guard #available(iOS 26.0, *) else {
            throw XCTSkip("Live FM BackfillJobRunner requires iOS 26+ / FoundationModels.")
        }
        // This is the BASELINE lane. `setUp` also admits the sibling
        // rediff-treatment var (a separate test method owns it); skip here when
        // that is the only reason we ran, so a treatment request never triggers a
        // full baseline dump. Behavior under the three pre-existing vars is
        // unchanged.
        guard Self.dumpEnabled || Self.partialSilverBaselineEnabled
            || Self.devicePreflightEnabled else {
            throw XCTSkip(
                "Baseline snapshot dump not requested (PLAYHEAD_PIPELINE_DUMP_REDIFF "
                    + "selects the sibling rediff-treatment lane)."
            )
        }

        if Self.partialSilverBaselineEnabled {
            try await capturePartialSilverProductionBaseline()
            return
        }

        try await captureLegacySnapshotDump()
    }

    /// playhead-xsdz.36.1 (Piece B): the rediff-ACTIVATION treatment lane. Same
    /// per-episode setup as the baseline dump, but each episode ALSO captures its
    /// played-copy (A-side) fingerprint stream and the service is handed a live
    /// `RediffBSideProvider` (fresh re-fetched audio staged offline at
    /// `TestFixtures/Corpus/Audio/<episodeId>.fresh.mp3`) plus a config with
    /// `rediffSlotOwnershipEnabled` ON. For a rotated episode whose fresh B-side
    /// diverges from the played copy, the in-app `RediffSlotOwnership` oracle
    /// widens the ad slots; the WIDENED windows are dumped to a DISTINCT file so
    /// the orchestrator can diff treatment vs. baseline. Opt-in via
    /// `PLAYHEAD_PIPELINE_DUMP_REDIFF=1`; the baseline lane is untouched.
    func testRediffTreatmentPipelineDumpOnNewEpisodes() async throws {
        guard #available(iOS 26.0, *) else {
            throw XCTSkip("Live FM BackfillJobRunner requires iOS 26+ / FoundationModels.")
        }
        guard Self.rediffTreatmentEnabled else {
            throw XCTSkip("Rediff-treatment dump is opt-in via PLAYHEAD_PIPELINE_DUMP_REDIFF=1.")
        }
        try await captureRediffTreatmentDump()
    }

    @available(iOS 26.0, *)
    func testPhysicalDeviceBaselinePreflight() throws {
        guard Self.devicePreflightEnabled else {
            throw XCTSkip("Physical-device preflight is opt-in.")
        }
        guard BaselineCaptureTransport.Runtime.current == .physicalIOS else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "physical-device preflight refuses simulator and Catalyst runtimes"
            )
        }
        let environment = ProcessInfo.processInfo.environment
        try BaselineRawPublisher.validateProductionEnvironment(environment)
        let runId = try BaselineRawPublisher.validateRunId(
            XCTUnwrap(environment["PLAYHEAD_BASELINE_RUN_ID"])
        )
        let declaredRevision = try BaselineRawPublisher.validateSourceRevision(
            XCTUnwrap(environment["PLAYHEAD_BASELINE_SOURCE_REVISION"])
        )
        let sourceRevision = try BaselineSourceIdentity.resolve(
            declaredRevision: declaredRevision
        )
        let paths = try Self.capturePaths(environment: environment)
        let evaluation = try PartialSilverEvaluationLoader.load(sourceRoot: paths.sourceRoot)
        try BaselineDeviceStagingValidator.validate(
            root: paths.sourceRoot,
            evaluation: evaluation,
            runId: runId,
            sourceRevision: sourceRevision
        )
        let runtime = try BaselineRuntimeIdentity.current(environment: environment)
        guard runtime.foundationModelsAvailability == "available" else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "Foundation Models runtime is unavailable during device preflight"
            )
        }
        let documents = try Self.documentsDirectory()
        let preflightURL = try BaselineCaptureTransport.resolveDevicePath(
            key: "PLAYHEAD_BASELINE_DEVICE_PREFLIGHT_OUTPUT_PATH",
            environment: environment,
            documentsDirectory: documents
        )
        let payload = BaselineDevicePreflightPayload(
            schemaVersion: 1,
            artifactKind: "physical_device_partial_silver_preflight",
            runId: runId,
            sourceRevision: sourceRevision,
            evaluationSHA256: PartialSilverEvaluationLoader.evaluationSHA256,
            deviceUDID: try XCTUnwrap(environment["PLAYHEAD_BASELINE_DEVICE_UDID"]),
            expectedOSVersion: try XCTUnwrap(
                environment["PLAYHEAD_BASELINE_DEVICE_OS_VERSION"]
            ),
            expectedOSBuild: try XCTUnwrap(
                environment["PLAYHEAD_BASELINE_DEVICE_OS_BUILD"]
            ),
            runtime: runtime,
            stagedAssetCount: evaluation.assets.count,
            outputTransportWritable: true
        )
        let encoder = JSONEncoder()
        encoder.keyEncodingStrategy = .convertToSnakeCase
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        try BaselineRawPublisher.publish(encoder.encode(payload), to: preflightURL)
        print("Physical-device partial-silver preflight: \(preflightURL.path)")
    }

    private static func capturePaths(
        environment: [String: String]
    ) throws -> BaselineCaptureTransport.Paths {
        let documents = try documentsDirectory()
        let fallbackSourceRoot: URL
        switch BaselineCaptureTransport.Runtime.current {
        case .physicalIOS:
            // Device capture must never derive a host path from #filePath.
            fallbackSourceRoot = documents
        case .catalyst, .simulator, .unsupported:
            fallbackSourceRoot = PipelineDumpManifestLoader.repoRoot()
        }
        return try BaselineCaptureTransport.resolve(
            environment: environment,
            fallbackSourceRoot: fallbackSourceRoot,
            documentsDirectory: documents,
            runtime: BaselineCaptureTransport.Runtime.current
        )
    }

    private static func documentsDirectory() throws -> URL {
        guard let documents = FileManager.default.urls(
            for: .documentDirectory,
            in: .userDomainMask
        ).first else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "app Documents directory is unavailable"
            )
        }
        return documents
    }

    private func captureLegacySnapshotDump() async throws {
        try await captureDumpLane(
            config: .default,
            rediffBSideProvider: nil,
            outputFilename: "playhead-dogfood-diagnostics-pipeline-dump-new9.json",
            configLabel: "production .default (all xsdz flags off, fmBackfillMode .full)"
        )
    }

    /// playhead-xsdz.36.1: the rediff-ACTIVATION treatment lane. Constructs the
    /// fresh-B-side provider (rooted at the repo corpus) and the rediff-ON config,
    /// then runs the SAME shared lane. Writes to a DISTINCT dump file so the
    /// orchestrator can diff treatment vs. baseline widths.
    private func captureRediffTreatmentDump() async throws {
        let repoRoot = PipelineDumpManifestLoader.repoRoot()

        // R3 pre-flight (before the ~90-minute lane): at least ONE manifest
        // entry must have a USABLE staged `<episodeId>.fresh.mp3` (by the
        // provider's own acceptance predicate — R4), or the treatment dump is
        // structurally guaranteed to equal the baseline (every episode falls
        // through to status-quo width) while LOOKING like a valid measurement —
        // the exact silent-no-lift failure a go/no-go must not absorb. Partial
        // staging is fine by design (unstaged == non-rotated); print the
        // coverage AND stamp it into the dump artifact (R4) so the go/no-go
        // JSON is self-describing, not transcript-dependent. The manifest is
        // re-loaded inside `captureDumpLane` (shared with the baseline lane,
        // whose behavior this guard must not disturb); the double read is
        // deterministic and cheap.
        let entries: [PipelineDumpSnapshotEntry]
        do {
            entries = try PipelineDumpManifestLoader.load(repoRoot: repoRoot)
        } catch {
            XCTFail("snapshot manifest read failed: \(error)")
            return
        }
        // R6: a ZERO-entry manifest would otherwise fall through to the
        // zero-staged guard below, whose "stage B-sides first" message sends
        // the operator at the wrong problem — there is nothing to stage; the
        // snapshot pipeline produced an empty manifest. (The loader accepts an
        // empty array; `validate` only checks per-entry shape.)
        guard !entries.isEmpty else {
            XCTFail(
                "snapshot manifest at \(PipelineDumpManifestLoader.manifestRelativePath) "
                    + "has ZERO entries — nothing to measure. Regenerate the snapshot "
                    + "manifest before requesting the rediff treatment dump (playhead-xsdz.36.1)."
            )
            return
        }
        let classification = classifyRediffTreatmentBSides(
            entries: entries,
            audioDirectory: CorpusFreshBSideProvider.audioDirectory(repoRoot: repoRoot)
        )
        let unstagedIds = classification.unstagedEpisodeIds
        let stagedCount = entries.count - unstagedIds.count
        print("""
        Rediff treatment B-side coverage: \(stagedCount)/\(entries.count) manifest \
        entries have a staged .fresh.mp3\
        \(unstagedIds.isEmpty
            ? ""
            : "; unstaged (status-quo width by design): \(unstagedIds.sorted().joined(separator: ", "))")
        """)
        if !classification.irregularEpisodeIds.isEmpty {
            print("""
            WARNING: \(classification.irregularEpisodeIds.count) fresh B-side file(s) exist \
            but are NOT usable (symlink — possibly dangling — special file, zero-byte \
            file, or unreadable path) — the decode path refuses these, so they \
            count as UNSTAGED: \
            \(classification.irregularEpisodeIds.sorted().joined(separator: ", "))
            """)
        }
        guard stagedCount > 0 else {
            let irregularNote = classification.irregularEpisodeIds.isEmpty
                ? ""
                : " NOTE: \(classification.irregularEpisodeIds.count) file(s) exist at the "
                    + "staged path but are symlinks (possibly dangling), non-regular, "
                    + "empty, or unreadable, which the decode path refuses."
            XCTFail(
                """
                PLAYHEAD_PIPELINE_DUMP_REDIFF=1 was set but NO manifest entry has a \
                usable staged fresh B-side (TestFixtures/Corpus/Audio/<episodeId>.fresh.mp3 \
                as a regular unaliased non-empty file). The treatment dump would silently \
                equal the \
                baseline. Stage B-sides first: scripts/l2f-dai-rediff.py --retain-audio \
                (playhead-xsdz.36.1).\(irregularNote)
                """
            )
            return
        }

        let provider = CorpusFreshBSideProvider(repoRoot: repoRoot)
        try await captureDumpLane(
            config: makeRediffTreatmentConfig(),
            rediffBSideProvider: provider,
            outputFilename: "playhead-dogfood-diagnostics-pipeline-dump-rediff-treatment.json",
            configLabel: "treatment: .default + rediffSlotOwnershipEnabled + fresh-B-side provider",
            bSideCoverage: DumpBSideCoverage(
                stagedEntryCount: stagedCount,
                manifestEntryCount: entries.count,
                unstagedEpisodeIds: unstagedIds.sorted(),
                irregularEpisodeIds: classification.irregularEpisodeIds.sorted()
            )
        )
    }

    /// Shared body for both dump lanes. Reads the snapshot manifest, runs one
    /// backfill per entry under `config` (with an optional rediff B-side
    /// provider), rolls up the summary, and writes the JSON dump to
    /// `outputFilename` at the repo root. The baseline lane passes `.default` +
    /// nil provider (byte-identical to the pre-xsdz.36.1 behaviour); the treatment
    /// lane passes the rediff-ON config + the fresh-B-side provider. Extracted so
    /// the two lanes share ONE schema/roll-up/guard code path and cannot drift.
    private func captureDumpLane(
        config: AdDetectionConfig,
        rediffBSideProvider: RediffBSideProvider?,
        outputFilename: String,
        configLabel: String,
        bSideCoverage: DumpBSideCoverage? = nil
    ) async throws {
        let repoRoot = PipelineDumpManifestLoader.repoRoot()

        // Read the NEW snapshot entries from the manifest. Missing or malformed
        // manifest is a HARD failure — the snapshot pipeline should have produced
        // it before this dump runs.
        let entries: [PipelineDumpSnapshotEntry]
        do {
            entries = try PipelineDumpManifestLoader.load(repoRoot: repoRoot)
        } catch {
            XCTFail("snapshot manifest read failed: \(error)")
            return
        }
        XCTAssertFalse(
            entries.isEmpty,
            "snapshot manifest is empty — nothing to dump"
        )

        var dumpEpisodes: [DumpEpisode] = []
        var failed: [(episodeId: String, reason: String)] = []
        var skipped: [(episodeId: String, reason: String)] = []

        for entry in entries {
            do {
                let result = try await runSingleEpisode(
                    entry: entry,
                    repoRoot: repoRoot,
                    config: config,
                    rediffBSideProvider: rediffBSideProvider
                )
                dumpEpisodes.append(result.episode)
            } catch let error as PipelineDumpRunError {
                switch error.severity {
                case .hard:
                    failed.append((entry.episodeId, error.reason))
                case .soft:
                    skipped.append((entry.episodeId, error.reason))
                }
            } catch {
                failed.append((entry.episodeId, "episode run failed: \(error.localizedDescription)"))
            }
        }

        // Build summary roll-up.
        var perShow: [String: Int] = [:]
        var totalAdWindows = 0
        for ep in dumpEpisodes {
            perShow[ep.showSlug, default: 0] += ep.adWindows.count
            totalAdWindows += ep.adWindows.count
        }

        let runUtc: String = {
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withInternetDateTime]
            return formatter.string(from: Date())
        }()

        let payload = DumpPayload(
            config: configLabel,
            runUtc: runUtc,
            bSideCoverage: bSideCoverage,
            episodes: dumpEpisodes,
            summary: DumpSummary(
                totalEpisodes: dumpEpisodes.count,
                totalAdWindows: totalAdWindows,
                perShow: perShow
            )
        )

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(payload)
        let dumpURL = repoRoot.appendingPathComponent(
            outputFilename,
            isDirectory: false
        )
        try data.write(to: dumpURL, options: .atomic)

        print("""
        Pipeline dump scored=\(dumpEpisodes.count): \
        \(dumpEpisodes.map(\.episodeId).sorted().joined(separator: ", "))
        skipped=\(skipped.count): \(skipped.map(\.episodeId).sorted().joined(separator: ", "))
        failed=\(failed.count): \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))
        totalAdWindows=\(totalAdWindows) perShow=\(perShow)
        Pipeline dump JSON: \(dumpURL.path) (git-ignored)
        """)

        if !failed.isEmpty {
            XCTFail("Hard failures: \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))")
        } else if dumpEpisodes.isEmpty {
            XCTFail(
                """
                A pipeline dump was requested but no episodes scored — every \
                manifest entry landed in `skipped` because audio or the \
                transcript sidecar is not staged. See the file header.
                """
            )
        }
    }
}

private extension PipelineDumpLiveTests {
    @available(iOS 26.0, *)
    private func capturePartialSilverProductionBaseline() async throws {
        let environment = ProcessInfo.processInfo.environment
        try BaselineRawPublisher.validateProductionEnvironment(environment)
        let paths = try Self.capturePaths(environment: environment)
        let evaluation = try PartialSilverEvaluationLoader.load(sourceRoot: paths.sourceRoot)
        let corpusRoot: URL
        switch BaselineCaptureTransport.Runtime.current {
        case .physicalIOS:
            corpusRoot = paths.corpusRoot
        case .catalyst, .simulator, .unsupported:
            corpusRoot = try PartialSilverEvaluationLoader.validatedCorpusRoot(
                sourceRoot: paths.sourceRoot
            )
        }
        let runId = try BaselineRawPublisher.validateRunId(
            XCTUnwrap(environment["PLAYHEAD_BASELINE_RUN_ID"])
        )
        let declaredSourceRevision = try BaselineRawPublisher.validateSourceRevision(
            XCTUnwrap(environment["PLAYHEAD_BASELINE_SOURCE_REVISION"])
        )
        let sourceRevision = try BaselineSourceIdentity.resolve(
            declaredRevision: declaredSourceRevision
        )
        if BaselineCaptureTransport.Runtime.current == .physicalIOS {
            try BaselineDeviceStagingValidator.validate(
                root: paths.sourceRoot,
                evaluation: evaluation,
                runId: runId,
                sourceRevision: sourceRevision
            )
        }
        let outputURL = try BaselineRawPublisher.outputURL(
            path: paths.outputURL.path,
            runId: runId
        )

        var episodes: [BaselineEpisode] = []
        for asset in evaluation.assets.sorted(by: { $0.episodeId < $1.episodeId }) {
            let audioURL = try PartialSilverEvaluationLoader.audioURL(
                for: asset,
                corpusRoot: corpusRoot
            )
            let showHash = PartialSilverEvaluationLoader.sha256Hex(Data(asset.showName.utf8))
            let entry = PipelineDumpSnapshotEntry(
                show: asset.showName,
                showSlug: "partial-silver-\(showHash.prefix(16))",
                episodeId: asset.episodeId,
                publishDate: "partial-silver",
                audioPath: "TestFixtures/Corpus/Audio/\(audioURL.lastPathComponent)",
                sha256: String(asset.audioFingerprint.dropFirst("sha256:".count))
            )
            let edges = asset.fullBreaks.flatMap { [$0.startSeconds, $0.endSeconds] }
            let result = try await runSingleEpisode(
                entry: entry,
                repoRoot: corpusRoot,
                musicBoundaryEdges: edges
            )
            guard abs(result.episode.episodeDurationSeconds - asset.durationSeconds) <= 1.0 else {
                throw PipelineDumpRunError(
                    severity: .hard,
                    reason: "decoded duration differs from partial-silver artifact for \(asset.episodeId)"
                )
            }
            episodes.append(BaselineEpisode(
                episodeId: asset.episodeId,
                showName: asset.showName,
                audioFingerprint: result.episode.audioFingerprint,
                transcriptSHA256: result.transcriptSHA256,
                durationSeconds: result.episode.episodeDurationSeconds,
                predictions: result.episode.adWindows.map(BaselinePrediction.init),
                musicFeatures: result.musicFeatures
            ))
        }
        guard episodes.count == PartialSilverEvaluationLoader.expectedAssetCount,
              Set(episodes.map(\.episodeId)).count == episodes.count else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "baseline did not produce exactly 27 unique evaluation episodes"
            )
        }
        try BaselineRawValidator.validate(episodes: episodes, evaluation: evaluation)

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        let runtime = try BaselineRuntimeIdentity.current(environment: environment)
        guard runtime.foundationModelsAvailability == "available" else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "Foundation Models runtime is not available for production capture"
            )
        }
        let payload = BaselineRawPayload(
            schemaVersion: 1,
            artifactKind: "unchanged_production_partial_silver_raw",
            runId: runId,
            capturedAtUtc: formatter.string(from: Date()),
            sourceRevision: sourceRevision,
            evaluationSHA256: PartialSilverEvaluationLoader.evaluationSHA256,
            productionConfig: BaselineProductionConfig.current(),
            runtime: runtime,
            episodes: episodes
        )
        let encoder = JSONEncoder()
        encoder.keyEncodingStrategy = .convertToSnakeCase
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        let data = try encoder.encode(payload)
        try BaselineRawPublisher.publish(data, to: outputURL)
        print("Partial-silver unchanged-production raw capture: \(outputURL.path)")
    }
}

private extension PipelineDumpLiveTests {
    // MARK: - Single-episode run

    /// Run `runBackfill` once for a single manifest entry under the lane's
    /// `config` (the baseline/legacy lanes pass the shipped `.default`; the
    /// rediff-treatment lane passes `makeRediffTreatmentConfig()` plus a live
    /// B-side provider — playhead-xsdz.36.1), then return a populated
    /// `DumpEpisode` row. Mirrors the sibling harness's `runArm(...)` setup
    /// (fresh store, asset, transcript chunks, feature windows, seeded planner
    /// state, live FM runner factory) — the only differences are: (a) one
    /// config per lane instead of a per-arm config matrix; (b) the optional
    /// `FragilityDiagnosticObserver` is attached so we can report the
    /// decoded-span count.
    @available(iOS 26.0, *)
    private func runSingleEpisode(
        entry: PipelineDumpSnapshotEntry,
        repoRoot: URL,
        musicBoundaryEdges: [Double] = [],
        config: AdDetectionConfig = .default,
        rediffBSideProvider: RediffBSideProvider? = nil
    ) async throws -> ProductionEpisodeRunResult {
        let episodeId = entry.episodeId

        // Resolve audio file from the manifest's repo-root-relative path.
        let sourceAudioURL = try PipelineDumpManifestLoader.audioURL(for: entry, repoRoot: repoRoot)
        guard FileManager.default.fileExists(atPath: sourceAudioURL.path) else {
            throw PipelineDumpRunError(
                severity: .soft,
                reason: "audio file not staged at \(sourceAudioURL.path)"
            )
        }
        guard let audioValues = try? sourceAudioURL.resourceValues(
            forKeys: [.isRegularFileKey, .isSymbolicLinkKey]
        ), audioValues.isRegularFile == true, audioValues.isSymbolicLink != true else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "audio at \(sourceAudioURL.path) is not a regular unaliased file"
            )
        }
        guard let expectedHash = entry.sha256 else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "snapshot manifest lacks sha256 for \(episodeId)"
            )
        }
        let inputSnapshot = try snapshotInputs(
            episodeId: episodeId,
            sourceAudioURL: sourceAudioURL,
            sourceRoot: repoRoot,
            expectedAudioSHA256: expectedHash
        )
        let audioURL = inputSnapshot.audioURL
        guard let localURL = LocalAudioURL(audioURL) else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "snapshotted audio at \(audioURL.path) is not a file URL"
            )
        }
        let audioFingerprint = try CorpusAudioFingerprint.fingerprint(of: audioURL)
        guard audioFingerprint == "sha256:\(expectedHash)" else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "audio fingerprint differs from snapshot manifest for \(episodeId)"
            )
        }

        // Resolve transcript via the same CorpusTranscriptLoader the
        // chapter-plan snapshot capture uses. A MISSING transcript is a
        // HARD failure here — per the task, the snapshot pipeline should
        // have produced one for each manifest entry.
        let transcriptResult = try loadStableTranscript(
            episodeId: episodeId,
            repoRoot: inputSnapshot.corpusRoot,
            audioURL: audioURL
        )
        let transcript = transcriptResult.chunks
        guard !transcript.isEmpty else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "transcript sidecar empty/absent for \(episodeId) — snapshot pipeline should have produced one"
            )
        }
        // Fresh store so this run never observes any previous episode's
        // persisted windows or planner state.
        let storeDir = try makeTempDir(prefix: "PipelineDumpLive-\(episodeId)")
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        // Asset row. `analysisAssetId == episodeId` so the scored ad
        // windows read back under the id we report on. `assetFingerprint` is
        // hoisted into a single local so the A-side fingerprint capture in the
        // rediff-treatment lane persists the SAME `sourceAudioIdentity` the
        // asset row carries (identity gate (b) in RediffSlotOwnership.gateAndDiff
        // requires stored identity == the asset's current assetFingerprint).
        let assetId = episodeId
        let assetFingerprint = "pipeline-dump-fp-\(episodeId)"
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: assetFingerprint,
            weakFingerprint: nil,
            sourceURL: localURL.absoluteString,
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        // Re-stamp transcript chunks onto this asset id (defensive in case
        // the loader convention changes).
        let chunks = transcript.map { rebindChunk($0, to: assetId) }
        try await store.insertTranscriptChunks(chunks)

        // PERSIST FEATURE WINDOWS: decode → extract → persist. The
        // CoveragePlanner + acoustic-break snapping read these back from
        // the store during the FM phase, so they must be present for the
        // FM scan to behave like production.
        let shards = try await AnalysisAudioService().decode(
            fileURL: localURL,
            episodeID: episodeId,
            shardDuration: AnalysisAudioService.defaultShardDuration
        )
        let featureWindows = await FeatureExtractionService(store: store).extract(
            shards: shards,
            analysisAssetId: assetId
        )
        guard !featureWindows.isEmpty else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "feature extraction produced 0 windows for \(episodeId)"
            )
        }
        try await store.insertFeatureWindows(featureWindows)

        // playhead-xsdz.36.1 (rediff TREATMENT lane only): persist the played-copy
        // (A-side) fingerprint stream so the in-app rediff pass has a stored stream
        // to diff the re-fetched B-side against. Gated on the rediff-ownership flag,
        // so the baseline lane (flag OFF) never writes the row and is byte-identical.
        // `sourceAudioIdentity` MUST equal the asset's `assetFingerprint` (identity
        // gate (b)); the fingerprints are stamped with the current
        // `ChromaFingerprinter.algorithmVersion` (gate (a)). This is the SAME static
        // helper + 16 kHz shards `EpisodeFingerprintCaptureTests` pins; the
        // `captureEnabledByDefault` flag gates only the LIVE `AnalysisJobRunner`
        // branch, NOT this static call, so no production flag is flipped.
        if config.rediffSlotOwnershipEnabled {
            try await EpisodeFingerprintCapture.captureAndPersist(
                shards: shards,
                assetId: assetId,
                sourceAudioIdentity: assetFingerprint,
                store: store
            )
        }

        // Episode duration from the decoded shard span (last shard end).
        let episodeDuration = shards.reduce(0.0) { max($0, $1.startTime + $1.duration) }

        // PRECONDITION — seed the planner into `targetedWithAudit` so the
        // live narrowing path runs (the production FM regime). Five
        // full-rescan observations with a 0.9 recall sample satisfy both
        // `observedEpisodeCount >= 5` and `stableRecall == true`. Same
        // seeding the sibling harnesses use.
        let podcastId = entry.showSlug
        try await seedTargetedWithAuditPlannerState(
            store: store,
            podcastId: podcastId
        )

        // Detection config. The baseline lane passes the shipped `.default`
        // (every activation flag off — the `productionConfigStateIsHeld` hermetic
        // test pins that identity). The rediff-treatment lane passes a config that
        // is `.default` in every field EXCEPT `rediffSlotOwnershipEnabled`, flipped
        // ON (see `makeRediffTreatmentConfig`), so treatment and baseline differ
        // ONLY by the rediff flag + the injected B-side provider.

        // Behavior-neutral diagnostic observer: counts every decoded span
        // so we can report `candidateDecodedSpans`. The observer is the
        // existing `FragilityDiagnosticObserver` (already part of the
        // shipped app); it NEVER feeds back into the decision path. See
        // the observer's contract in `FragilityDiagnosticObserver.swift`.
        let observer = FragilityDiagnosticObserver()

        let service = AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config,
            backfillJobRunnerFactory: Self.makeLiveRunnerFactory(),
            canUseFoundationModelsProvider: { true }, // avoid silent FM demotion
            fragilityDiagnosticObserver: observer,
            // Rediff B-side source. `nil` in the baseline lane → the rediff pass
            // no-ops (byte-identical). In the treatment lane, the real provider
            // supplies the re-fetched fresh audio the pass diffs against.
            rediffBSideProvider: rediffBSideProvider,
            approvedCohortRegistry: nil // avoid cohort-gated FM demotion
        )

        // GUARD #1 (FM-mode gating): confirm cohort/FM gating did not
        // silently demote `.full` → `.off`. Else the FM phase never runs
        // and the dump is a meaningless zero.
        let effectiveMode = await service.effectiveFMBackfillModeForTesting()
        XCTAssertEqual(
            effectiveMode,
            .full,
            "episode=\(episodeId): effective FM mode demoted off .full — FM phase would not run"
        )

        // GUARD #2 (planner regime): confirm the seeded state lands the
        // planner in `targetedWithAudit` — the production FM regime.
        let seededState = try await store.fetchPodcastPlannerState(podcastId: podcastId)
        let seededContext = CoveragePlannerContext(
            observedEpisodeCount: seededState?.observedEpisodeCount ?? 0,
            stableRecall: seededState?.stableRecallFlag ?? false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: seededState?.episodesSinceLastFullRescan ?? 0,
            periodicFullRescanIntervalEpisodes: 10
        )
        XCTAssertEqual(
            CoveragePlanner().plan(for: seededContext).policy,
            .targetedWithAudit,
            "episode=\(episodeId): planner is not in targetedWithAudit (observed=\(seededContext.observedEpisodeCount) stableRecall=\(seededContext.stableRecall)) — FM scan would not match production"
        )

        // SCORED RUN: a single backfill under the lane's `config` (shipped
        // `.default` in the baseline/legacy lanes; rediff-ON treatment config
        // in the treatment lane).
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: podcastId,
            episodeDuration: episodeDuration
        )

        // Fetch the persisted ad windows + the observer's decoded-span
        // count. The store-side AdWindow carries `confidence` (== the
        // post-fragility skip confidence), `decisionState`, and
        // `eligibilityGate`. `promotionTrack` is not persisted; emitted as
        // `null` in the dump.
        let windows = try await store.fetchAdWindows(assetId: assetId)
        let decodedSpanCount = await observer.recordCount(for: assetId)
        // Per-candidate boundaries for playhead-4xqf boundary-undersizing
        // investigation (see DumpDecodedSpan docstring above).
        let decodedSpanRows = await observer.spanRows(for: assetId) ?? []
        // playhead-p56a: per-AdWindow finalizer constraint trace. Empty
        // map when `config.spanFinalizerEnabled == false` — the flag is OFF
        // in `.default` (see `PipelineDumpHermeticTests`'s
        // `productionConfigStateIsHeld` pin) and EVERY lane's config keeps
        // that value (the treatment config differs from `.default` only by
        // the rediff flag). Each map entry surfaces in
        // the dump's `spanFinalizerConstraintsFired` key when present and
        // non-empty; nil/missing when the underlying lookup yielded no
        // trace, matching the default-encoder convention for nil optionals.
        let spanFinalizerConstraintsByWindowId =
            await service.spanFinalizerConstraintsByWindowIdForTesting()
        // playhead-l2f.6: per-AdWindow stinger refinement trace. The flag
        // ships ON in `.default` (2026-07-16 dogfood flip — see
        // `productionConfigStateIsHeld`) and every lane's config keeps that
        // value, so bank-show windows carry live v4 traces here; the map is empty
        // only when the flag is OFF or no window's show resolved a bank
        // entry. Each entry surfaces in the dump's `stingerRefinement` key
        // when present; nil/missing otherwise, matching the
        // default-encoder convention for nil optionals.
        let stingerRefinementByWindowId =
            await service.stingerRefinementTraceByWindowIdForTesting()

        let dumpWindows: [DumpAdWindow] = windows
            .sorted {
                ($0.startTime, $0.endTime, $0.id) < ($1.startTime, $1.endTime, $1.id)
            }
            .map { window in
                // playhead-4xqf FUSION_DROP probe: re-feed the PERSISTED
                // AdWindow bounds back into the legacy
                // `BoundaryRefiner.computeAdjustments` — the fallback path
                // the live inline refiner at AdDetectionService.swift
                // ~3186-3260 uses when BracketAwareBoundaryRefiner doesn't
                // refine — against the same
                // `featureWindows` production used. Under normal flow
                // these are ~0 (the persisted bounds are already
                // post-refinement); a non-zero delta flags that the
                // BracketAware path took an alternate snap or further
                // adjustments remain available. Encoded as `null` when
                // both deltas are zero AND when `featureWindows.count
                // < 3` (BoundaryRefiner's own guard at the entry point);
                // a non-nil Double otherwise so downstream consumers can
                // distinguish "guarded out" / "agreement" from
                // "divergence".
                let (startAdj, endAdj): (Double, Double)
                if featureWindows.count >= 3 {
                    let result = BoundaryRefiner.computeAdjustments(
                        windows: featureWindows,
                        candidateStart: window.startTime,
                        candidateEnd: window.endTime
                    )
                    startAdj = result.startAdjust
                    endAdj = result.endAdjust
                } else {
                    startAdj = 0
                    endAdj = 0
                }
                let dumpStartAdj: Double?
                let dumpEndAdj: Double?
                if startAdj == 0 && endAdj == 0 {
                    dumpStartAdj = nil
                    dumpEndAdj = nil
                } else {
                    dumpStartAdj = startAdj
                    dumpEndAdj = endAdj
                }
                // playhead-p56a: resolve the per-window finalizer trace,
                // omitting nil and empty arrays so the wire shape matches
                // the existing "absent when nil" convention enforced by
                // `dumpAdWindowEncodesBoundaryRefinementAdjustments`.
                let trace = spanFinalizerConstraintsByWindowId[window.id]
                let spanFinalizerConstraints: [String]? =
                    (trace?.isEmpty ?? true) ? nil : trace
                // playhead-l2f.6: nil (key omitted from the encoded
                // object) when this window recorded no trace — flag OFF,
                // or no bank entry for the show. Present with live v4
                // fields under the shipping flag-ON default.
                let stingerRefinement = stingerRefinementByWindowId[window.id]
                    .map(DumpStingerRefinement.init)
                return DumpAdWindow(
                    startTime: window.startTime,
                    endTime: window.endTime,
                    skipConfidence: window.confidence,
                    decisionState: window.decisionState,
                    eligibilityGate: window.eligibilityGate,
                    promotionTrack: nil,
                    wasSkipped: window.wasSkipped,
                    boundaryRefinementStartAdjustment: dumpStartAdj,
                    boundaryRefinementEndAdjustment: dumpEndAdj,
                    spanFinalizerConstraintsFired: spanFinalizerConstraints,
                    stingerRefinement: stingerRefinement
                )
            }

        let dumpDecodedSpans: [DumpDecodedSpan] = decodedSpanRows
            .sorted {
                ($0.spanStart, $0.spanEnd, $0.spanId)
                    < ($1.spanStart, $1.spanEnd, $1.spanId)
            }
            .map {
                DumpDecodedSpan(
                    spanId: $0.spanId,
                    startTime: $0.spanStart,
                    endTime: $0.spanEnd,
                    proposalConfidence: $0.proposalConfidence,
                    skipConfidence: $0.skipConfidence,
                    fragilityScore: $0.fragilityScore
                )
            }

        let musicFeatures = Self.musicFeatures(
            featureWindows,
            around: musicBoundaryEdges
        )
        let finalAudioFingerprint = try CorpusAudioFingerprint.fingerprint(of: audioURL)
        guard finalAudioFingerprint == audioFingerprint else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "audio changed during production capture for \(episodeId)"
            )
        }
        let finalTranscriptSHA256 = try PartialSilverEvaluationLoader.fileSHA256Hex(
            transcriptResult.url,
            maximumBytes: 128 * 1_024 * 1_024
        )
        guard finalTranscriptSHA256 == transcriptResult.sha256 else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "transcript changed during production capture for \(episodeId)"
            )
        }
        return ProductionEpisodeRunResult(
            episode: DumpEpisode(
                episodeId: episodeId,
                audioFingerprint: audioFingerprint,
                showSlug: entry.showSlug,
                publishDate: entry.publishDate,
                episodeDurationSeconds: episodeDuration,
                candidateDecodedSpans: decodedSpanCount,
                candidateDecodedSpanList: dumpDecodedSpans,
                adWindows: dumpWindows
            ),
            transcriptSHA256: finalTranscriptSHA256,
            musicFeatures: musicFeatures
        )
    }

    private static func musicFeatures(
        _ windows: [FeatureWindow],
        around boundaryEdges: [Double]
    ) -> [BaselineMusicFeature] {
        windows
            .filter { window in
                boundaryEdges.contains { edge in
                    window.endTime > edge - 8.0 && window.startTime < edge + 8.0
                }
            }
            .sorted {
                ($0.startTime, $0.endTime) < ($1.startTime, $1.endTime)
            }
            .map(BaselineMusicFeature.init)
    }
}

private extension PipelineDumpLiveTests {
    private func snapshotInputs(
        episodeId: String,
        sourceAudioURL: URL,
        sourceRoot: URL,
        expectedAudioSHA256: String
    ) throws -> StableInputSnapshot {
        let root = try makeTempDir(prefix: "PipelineDumpInput-\(episodeId)")
        let audioDirectory = root.appendingPathComponent(
            "TestFixtures/Corpus/Audio",
            isDirectory: true
        )
        let transcriptDirectory = root.appendingPathComponent(
            "TestFixtures/Corpus/Transcripts",
            isDirectory: true
        )
        try FileManager.default.createDirectory(
            at: audioDirectory,
            withIntermediateDirectories: true
        )
        try FileManager.default.createDirectory(
            at: transcriptDirectory,
            withIntermediateDirectories: true
        )
        let audioURL = audioDirectory.appendingPathComponent(sourceAudioURL.lastPathComponent)
        let audioSHA256 = try PartialSilverEvaluationLoader.snapshotRegularFile(
            at: sourceAudioURL,
            to: audioURL,
            maximumBytes: 4 * 1_024 * 1_024 * 1_024
        )
        guard audioSHA256 == expectedAudioSHA256 else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "audio fingerprint differs while snapshotting \(episodeId)"
            )
        }
        let sourceTranscriptURL = try PartialSilverEvaluationLoader.transcriptURL(
            for: episodeId,
            corpusRoot: sourceRoot
        )
        let transcriptURL = transcriptDirectory.appendingPathComponent("\(episodeId).json")
        _ = try PartialSilverEvaluationLoader.snapshotRegularFile(
            at: sourceTranscriptURL,
            to: transcriptURL,
            maximumBytes: 128 * 1_024 * 1_024
        )
        return StableInputSnapshot(corpusRoot: root, audioURL: audioURL)
    }

    private func loadStableTranscript(
        episodeId: String,
        repoRoot: URL,
        audioURL: URL
    ) throws -> StableTranscriptResult {
        let transcriptURL = try PartialSilverEvaluationLoader.transcriptURL(
            for: episodeId,
            corpusRoot: repoRoot
        )
        let before = try PartialSilverEvaluationLoader.fileSHA256Hex(
            transcriptURL,
            maximumBytes: 128 * 1_024 * 1_024
        )
        let chunks: [TranscriptChunk]
        do {
            chunks = try CorpusTranscriptLoader.load(
                episodeId: episodeId,
                repoRoot: repoRoot,
                audioURL: audioURL
            )
        } catch {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "transcript decode failed: \(error.localizedDescription)"
            )
        }
        let after = try PartialSilverEvaluationLoader.fileSHA256Hex(
            transcriptURL,
            maximumBytes: 128 * 1_024 * 1_024
        )
        guard after == before else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "transcript changed while loading \(episodeId)"
            )
        }
        return StableTranscriptResult(
            chunks: chunks,
            sha256: after,
            url: transcriptURL
        )
    }

    // MARK: - Planner-state seeding

    /// Seed `store`'s `PodcastPlannerState` for `podcastId` into the warmed
    /// `targetedWithAudit` regime: `observedEpisodeCount >= 5` and
    /// `stableRecall == true`. Mirrors `FragilityGateLiveABTests` /
    /// `LexicalScorerLiveABTests` — uses the production observation API so
    /// the seeded row is byte-identical to one a warmed podcast would have
    /// accrued.
    private func seedTargetedWithAuditPlannerState(
        store: AnalysisStore,
        podcastId: String
    ) async throws {
        for _ in 0..<5 {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: podcastId,
                wasFullRescan: true,
                fullRescanPrecisionSample: 0.9,
                now: Date().timeIntervalSince1970
            )
        }
    }

    // MARK: - Live FM runner factory

    /// Build a `backfillJobRunnerFactory` that constructs a LIVE FM
    /// `BackfillJobRunner` with `NarrowingConfig.default` baked in (the
    /// shipped production narrowing config — same as the sibling
    /// harnesses' production-default arm). The `mode` argument is supplied
    /// by `AdDetectionService` (the effective FM mode, `.full` here).
    private static func makeLiveRunnerFactory()
        -> (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)
    {
        { store, mode in
            let redactor: PromptRedactor
            do {
                redactor = try PromptRedactor.loadDefault()
            } catch {
                preconditionFailure("PromptRedactor.loadDefault failed: \(error)")
            }
            let router = SensitiveWindowRouter(redactor: redactor)
            let permissiveClassifierBox: BackfillJobRunner.PermissiveClassifierBox?
            if #available(iOS 26.0, *) {
                permissiveClassifierBox = BackfillJobRunner.PermissiveClassifierBox(PermissiveAdClassifier())
            } else {
                permissiveClassifierBox = nil
            }
            return BackfillJobRunner(
                store: store,
                admissionController: AdmissionController(),
                classifier: PlayheadRuntime.makeFoundationModelClassifier(
                    redactor: redactor
                ),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: ScanCohort.productionJSON(),
                sensitiveRouter: router,
                permissiveClassifier: permissiveClassifierBox,
                // Production-default narrowing — the shipped state for
                // this dump.
                narrowingConfig: NarrowingConfig.default
            )
        }
    }

    // MARK: - Chunk rebinding

    /// Return a copy of `chunk` with its `analysisAssetId` re-pointed at
    /// `assetId`. Keeps every other field byte-identical so the derived
    /// transcript version is stable. Same helper the sibling harnesses use.
    private func rebindChunk(_ chunk: TranscriptChunk, to assetId: String) -> TranscriptChunk {
        guard chunk.analysisAssetId != assetId else { return chunk }
        return TranscriptChunk(
            id: chunk.id,
            analysisAssetId: assetId,
            segmentFingerprint: chunk.segmentFingerprint,
            chunkIndex: chunk.chunkIndex,
            startTime: chunk.startTime,
            endTime: chunk.endTime,
            text: chunk.text,
            normalizedText: chunk.normalizedText,
            pass: chunk.pass,
            modelVersion: chunk.modelVersion,
            transcriptVersion: chunk.transcriptVersion,
            atomOrdinal: chunk.atomOrdinal,
            weakAnchorMetadata: chunk.weakAnchorMetadata,
            speakerId: chunk.speakerId,
            avgConfidence: chunk.avgConfidence
        )
    }
}

// MARK: - Run errors

/// Raised when a single-episode dump cannot complete. `severity` partitions
/// the failure reason: `.soft` lands in the `skipped` bucket (audio not
/// staged); `.hard` lands in `failed` and fails the test (missing transcript,
/// non-file URL, zero feature windows).
private struct PipelineDumpRunError: Error {
    enum Severity { case soft, hard }
    let severity: Severity
    let reason: String
}

// MARK: - Hermetic isolation tests (Swift Testing)

/// Hermetic, sim-runnable tests that guard the two pieces of glue most
/// likely to silently regress: (a) the manifest read returns the expected
/// 9 entries, and (b) the BASELINE dump lanes use literally
/// `AdDetectionConfig.default` (no shadow copy, no flag drift; the
/// treatment lane's single-flag delta from `.default` is pinned separately
/// by `RediffTreatmentHarnessWiringTests`). These run in `PlayheadFastTests`
/// on the simulator with NO env var — they neither read audio nor hit FM.
struct PipelineDumpHermeticTests {

    @Test("physical app-container checks begin at standard app directories")
    func physicalAppContainerTrustBoundaryIsExact() {
        #expect(
            PartialSilverEvaluationLoader.trustedApplicationContainerAnchorLength([
                "var", "mobile", "Containers", "Data", "Application",
                "0F1720F9-935A-4B60-8C1A-34BDE805F570", "Documents", "l2f8",
            ]) == 7
        )
        #expect(
            PartialSilverEvaluationLoader.trustedApplicationContainerAnchorLength([
                "private", "var", "mobile", "Containers", "Data", "Application",
                "0F1720F9-935A-4B60-8C1A-34BDE805F570", "Documents", "l2f8",
            ]) == 8
        )
        #expect(
            PartialSilverEvaluationLoader.trustedApplicationContainerAnchorLength([
                "private", "var", "mobile", "Containers", "Data", "Application",
                "0F1720F9-935A-4B60-8C1A-34BDE805F570", "tmp", "capture",
            ]) == 8
        )
        #expect(
            PartialSilverEvaluationLoader.trustedApplicationContainerAnchorLength([
                "private", "var", "mobile", "Containers", "Data", "Application",
                "0F1720F9-935A-4B60-8C1A-34BDE805F570", "Library", "Caches",
            ]) == 8
        )
        #expect(
            PartialSilverEvaluationLoader.trustedApplicationContainerAnchorLength([
                "var", "mobile", "Containers", "Data", "Application",
                "not-a-container-id", "Documents", "l2f8",
            ]) == 0
        )
        #expect(
            PartialSilverEvaluationLoader.trustedApplicationContainerAnchorLength([
                "var", "mobile", "Containers", "Data", "Application",
                "0F1720F9-935A-4B60-8C1A-34BDE805F570", "unexpected", "l2f8",
            ]) == 0
        )
        #expect(
            PartialSilverEvaluationLoader.trustedApplicationContainerAnchorLength([
                "tmp", "Documents", "l2f8",
            ]) == 0
        )
    }

    @Test("root compatibility symlinks pass the unsafe-component walk")
    func rootCompatibilitySymlinksAreNotUnsafe() throws {
        // The macOS (Catalyst) user temp root is /var/folders/…, and /var is
        // a root-level symlink whose `isAliasFile` is ALSO true. With no
        // container anchor to skip it, the walk must still accept the root
        // compatibility links or the Catalyst lane can never stage
        // per-episode snapshot inputs.
        #expect(
            !PartialSilverEvaluationLoader.hasUnsafeFilesystemComponent(
                URL(fileURLWithPath: "/var/tmp", isDirectory: true)
            )
        )
        #expect(
            !PartialSilverEvaluationLoader.hasUnsafeFilesystemComponent(
                FileManager.default.temporaryDirectory
            )
        )
        // The exemption is root-only: a symlink component deeper in the
        // tree is still rejected.
        let scratch = try makeTempDir(prefix: "GuardProbe")
        let real = scratch.appendingPathComponent("real", isDirectory: true)
        try FileManager.default.createDirectory(at: real, withIntermediateDirectories: true)
        let link = scratch.appendingPathComponent("link", isDirectory: true)
        try FileManager.default.createSymbolicLink(at: link, withDestinationURL: real)
        #expect(
            PartialSilverEvaluationLoader.hasUnsafeFilesystemComponent(
                link.appendingPathComponent("probe.json", isDirectory: false)
            )
        )
    }

    @Test("bounded reader traverses the /var root compatibility link")
    func boundedReaderTraversesVarCompatibilityLink() throws {
        // macOS user temp — and thus every Catalyst per-episode snapshot —
        // lives under /var/folders/…, so the O_NOFOLLOW descriptor walk must
        // rewrite the root compatibility link to private/var or the Catalyst
        // lane can never read its own staged inputs. /var/tmp is the
        // writable stand-in reachable from the simulator (host filesystem)
        // and macOS.
        let directory = URL(fileURLWithPath: "/var/tmp", isDirectory: true)
        let file = directory.appendingPathComponent(
            "playhead-l2f8-guard-probe-\(UUID().uuidString).json",
            isDirectory: false
        )
        let payload = Data("{\"probe\":true}".utf8)
        do {
            try payload.write(to: file, options: [.withoutOverwriting])
        } catch {
            // Sandboxed runtimes (physical device) cannot write /var/tmp;
            // the walk under test is only reachable from Catalyst and the
            // simulator, so there is nothing to pin here.
            return
        }
        defer { try? FileManager.default.removeItem(at: file) }
        let bytes = try PartialSilverEvaluationLoader.readRegularBytes(
            at: file,
            maximumBytes: 1_024
        )
        #expect(bytes == payload)
    }

    @Test("physical capture paths stay beneath the app Documents l2f8 root")
    func physicalCapturePathsResolveInsideDocuments() throws {
        let documents = URL(fileURLWithPath: "/private/app/Documents", isDirectory: true)
        let environment = [
            "PLAYHEAD_BASELINE_DEVICE_MODE": "1",
            "PLAYHEAD_BASELINE_DEVICE_INPUT_ROOT": "l2f8/baseline-run-1/input",
            "PLAYHEAD_BASELINE_DEVICE_OUTPUT_PATH":
                "l2f8/baseline-run-1/output/playhead-partial-silver-baseline-baseline-run-1.json",
        ]

        let paths = try BaselineCaptureTransport.resolve(
            environment: environment,
            fallbackSourceRoot: URL(fileURLWithPath: "/host/source", isDirectory: true),
            documentsDirectory: documents,
            runtime: .physicalIOS
        )

        #expect(paths.sourceRoot.path == "/private/app/Documents/l2f8/baseline-run-1/input")
        #expect(paths.corpusRoot == paths.sourceRoot)
        #expect(
            paths.outputURL.path
                == "/private/app/Documents/l2f8/baseline-run-1/output/"
                    + "playhead-partial-silver-baseline-baseline-run-1.json"
        )

        for invalidRoot in [
            "/host/TestFixtures",
            "../TestFixtures",
            "l2f8/../TestFixtures",
            "other/baseline-run-1/input",
        ] {
            var invalid = environment
            invalid["PLAYHEAD_BASELINE_DEVICE_INPUT_ROOT"] = invalidRoot
            #expect(throws: BaselineCaptureTransport.TransportError.self) {
                _ = try BaselineCaptureTransport.resolve(
                    environment: invalid,
                    fallbackSourceRoot: URL(fileURLWithPath: "/host/source"),
                    documentsDirectory: documents,
                    runtime: .physicalIOS
                )
            }
        }
        #expect(throws: BaselineCaptureTransport.TransportError.self) {
            _ = try BaselineCaptureTransport.resolve(
                environment: environment,
                fallbackSourceRoot: URL(fileURLWithPath: "/host/source"),
                documentsDirectory: documents,
                runtime: .simulator
            )
        }
    }

    @Test("Catalyst capture paths retain host path behavior")
    func catalystCapturePathsRemainHostPaths() throws {
        let source = URL(fileURLWithPath: "/host/source", isDirectory: true)
        let corpus = URL(fileURLWithPath: "/host/corpus", isDirectory: true)
        let output = "/host/output/playhead-partial-silver-baseline-baseline-run-1.json"

        let paths = try BaselineCaptureTransport.resolve(
            environment: [
                "PLAYHEAD_CORPUS_ROOT": corpus.path,
                "PLAYHEAD_BASELINE_OUTPUT_PATH": output,
            ],
            fallbackSourceRoot: source,
            documentsDirectory: URL(fileURLWithPath: "/unused/Documents"),
            runtime: .catalyst
        )

        #expect(paths.sourceRoot == source)
        #expect(paths.corpusRoot == corpus)
        #expect(paths.outputURL.path == output)

        #expect(throws: BaselineCaptureTransport.TransportError.self) {
            _ = try BaselineCaptureTransport.resolve(
                environment: [
                    "PLAYHEAD_BASELINE_DEVICE_MODE": "1",
                    "PLAYHEAD_BASELINE_DEVICE_INPUT_ROOT": "l2f8/input",
                    "PLAYHEAD_BASELINE_DEVICE_OUTPUT_PATH": "l2f8/output/raw.json",
                ],
                fallbackSourceRoot: source,
                documentsDirectory: URL(fileURLWithPath: "/unused/Documents"),
                runtime: .catalyst
            )
        }
    }

    @Test("tracked partial-silver artifact is content-addressed and selects exactly 27 assets")
    func partialSilverArtifactSelectsExactCohort() throws {
        let sourceRoot = PipelineDumpManifestLoader.repoRoot()
        let evaluation = try PartialSilverEvaluationLoader.load(sourceRoot: sourceRoot)

        #expect(evaluation.assets.count == 27)
        #expect(Set(evaluation.assets.map(\.episodeId)).count == 27)
        #expect(Set(evaluation.assets.map(\.audioFingerprint)).count == 27)
        #expect(evaluation.assets.reduce(0) { $0 + $1.fullBreaks.count } == 20)
        #expect(evaluation.assets.reduce(0) { $0 + $1.presenceAnchors.count } == 20)
        #expect(evaluation.assets.reduce(0) { $0 + $1.contentVetoes.count } == 24)
    }

    @Test("partial-silver bounded reader pins the opened inode across parent swaps")
    func partialSilverReaderPinsOpenedInput() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(
            "partial-silver-reader-\(UUID().uuidString)",
            isDirectory: true
        )
        let source = root.appendingPathComponent("source", isDirectory: true)
        let hostile = root.appendingPathComponent("hostile", isDirectory: true)
        try FileManager.default.createDirectory(at: source, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: hostile, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let input = source.appendingPathComponent("input.json")
        try Data("expected".utf8).write(to: input)
        try Data("substituted".utf8).write(
            to: hostile.appendingPathComponent("input.json")
        )

        let data = try PartialSilverEvaluationLoader.readRegularBytes(
            at: input,
            maximumBytes: 64
        ) {
            try FileManager.default.moveItem(
                at: source,
                to: root.appendingPathComponent("original", isDirectory: true)
            )
            try FileManager.default.createSymbolicLink(
                at: source,
                withDestinationURL: hostile
            )
        }

        #expect(data == Data("expected".utf8))
    }

    @Test("partial-silver loader rejects hash, schema, count, identity, and fingerprint drift")
    func partialSilverArtifactRejectsDrift() throws {
        let sourceRoot = PipelineDumpManifestLoader.repoRoot()
        let sourceURL = sourceRoot.appendingPathComponent(
            PartialSilverEvaluationLoader.relativePath
        )
        let sourceData = try Data(contentsOf: sourceURL)
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "partial-silver-loader-\(UUID().uuidString)",
            isDirectory: true
        )
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let wrongName = directory.appendingPathComponent(
            "earaudit-partial-silver-\(String(repeating: "0", count: 64)).json"
        )
        try sourceData.write(to: wrongName)
        #expect(throws: PartialSilverEvaluationLoader.LoadError.self) {
            _ = try PartialSilverEvaluationLoader.decode(at: wrongName)
        }

        var root = try #require(
            JSONSerialization.jsonObject(with: sourceData) as? [String: Any]
        )
        let originalAssets = try #require(root["assets"] as? [[String: Any]])
        let mutations: [([String: Any]) -> [String: Any]] = [
            { document in
                var changed = document
                changed["schema_version"] = 2
                return changed
            },
            { document in
                var changed = document
                changed["assets"] = Array(originalAssets.dropLast())
                return changed
            },
            { document in
                var changed = document
                changed["assets"] = originalAssets + [originalAssets[0]]
                return changed
            },
            { document in
                var changed = document
                var assets = originalAssets
                assets[1]["episode_id"] = assets[0]["episode_id"]
                changed["assets"] = assets
                return changed
            },
            { document in
                var changed = document
                var assets = originalAssets
                assets[1]["audio_fingerprint"] = assets[0]["audio_fingerprint"]
                changed["assets"] = assets
                return changed
            },
            { document in
                var changed = document
                var assets = originalAssets
                assets[0]["audio_fingerprint"] = "sha256:not-a-digest"
                changed["assets"] = assets
                return changed
            },
        ]
        for mutate in mutations {
            root = mutate(root)
            let bytes = try JSONSerialization.data(
                withJSONObject: root,
                options: [.sortedKeys]
            )
            let digest = PartialSilverEvaluationLoader.sha256Hex(bytes)
            let url = directory.appendingPathComponent(
                "earaudit-partial-silver-\(digest).json"
            )
            try bytes.write(to: url)
            #expect(throws: PartialSilverEvaluationLoader.LoadError.self) {
                _ = try PartialSilverEvaluationLoader.decode(at: url)
            }
            root = try #require(
                JSONSerialization.jsonObject(with: sourceData) as? [String: Any]
            )
        }
    }

    @Test("retained audio resolver requires one regular exact-fingerprint member")
    func partialSilverAudioMembershipIsExact() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(
            "partial-silver-audio-\(UUID().uuidString)",
            isDirectory: true
        )
        let directory = root.appendingPathComponent("TestFixtures/Corpus/Audio")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let bytes = Data("retained audio".utf8)
        let fingerprint = "sha256:\(PartialSilverEvaluationLoader.sha256Hex(bytes))"
        let asset = PartialSilverEvaluation.Asset(
            audioFingerprint: fingerprint,
            contentVetoes: [],
            durationSeconds: 1,
            episodeId: "episode",
            fullBreaks: [],
            presenceAnchors: [],
            showName: "Show"
        )

        #expect(throws: PartialSilverEvaluationLoader.LoadError.self) {
            _ = try PartialSilverEvaluationLoader.audioURL(for: asset, corpusRoot: root)
        }
        let first = directory.appendingPathComponent("episode.mp3")
        try bytes.write(to: first)
        #expect(try PartialSilverEvaluationLoader.audioURL(for: asset, corpusRoot: root) == first)

        let second = directory.appendingPathComponent("episode.wav")
        try bytes.write(to: second)
        #expect(throws: PartialSilverEvaluationLoader.LoadError.self) {
            _ = try PartialSilverEvaluationLoader.audioURL(for: asset, corpusRoot: root)
        }
        try FileManager.default.removeItem(at: second)
        try Data("wrong bytes".utf8).write(to: first)
        #expect(throws: PartialSilverEvaluationLoader.LoadError.self) {
            _ = try PartialSilverEvaluationLoader.audioURL(for: asset, corpusRoot: root)
        }

        try FileManager.default.removeItem(at: directory)
        let realDirectory = root.appendingPathComponent("RealAudio", isDirectory: true)
        try FileManager.default.createDirectory(
            at: realDirectory,
            withIntermediateDirectories: false
        )
        try bytes.write(to: realDirectory.appendingPathComponent("episode.mp3"))
        try FileManager.default.createSymbolicLink(
            at: directory,
            withDestinationURL: realDirectory
        )
        #expect(throws: PartialSilverEvaluationLoader.LoadError.self) {
            _ = try PartialSilverEvaluationLoader.audioURL(for: asset, corpusRoot: root)
        }
    }

    @Test("raw publisher validates identities and atomically refuses overwrite")
    func baselineRawPublisherRefusesOverwrite() throws {
        #expect(throws: BaselineRawPublisher.PublishError.self) {
            _ = try BaselineRawPublisher.validateRunId("../escape")
        }
        #expect(throws: BaselineRawPublisher.PublishError.self) {
            _ = try BaselineRawPublisher.validateSourceRevision("not-a-revision")
        }
        #expect(throws: BaselineRawPublisher.PublishError.self) {
            _ = try BaselineRawPublisher.validateRunId("run-001")
        }
        #expect(
            try BaselineRawPublisher.validateRunId("baseline-run-1") == "baseline-run-1"
        )
        #expect(
            try BaselineRawPublisher.validateSourceRevision(String(repeating: "a", count: 40))
                == String(repeating: "a", count: 40)
        )

        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "partial-silver-publisher-\(UUID().uuidString)",
            isDirectory: true
        )
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        #expect(throws: BaselineRawPublisher.PublishError.self) {
            _ = try BaselineRawPublisher.outputURL(
                path: "relative.json",
                runId: "baseline-run-1"
            )
        }
        #expect(throws: BaselineRawPublisher.PublishError.self) {
            _ = try BaselineRawPublisher.outputURL(
                path: directory.appendingPathComponent("wrong.json").path,
                runId: "baseline-run-1"
            )
        }
        #expect(throws: BaselineRawPublisher.PublishError.self) {
            _ = try BaselineRawPublisher.outputURL(
                path: directory.path
                    + "/../playhead-partial-silver-baseline-baseline-run-1.json",
                runId: "baseline-run-1"
            )
        }
        #expect(throws: BaselineRawPublisher.PublishError.self) {
            try BaselineRawPublisher.validateProductionEnvironment([
                "PLAYHEAD_FM_PROMPT_VARIANT": "extract"
            ])
        }
        try BaselineRawPublisher.validateProductionEnvironment([:])
        let output = try BaselineRawPublisher.outputURL(
            path: directory
                .appendingPathComponent(
                    "playhead-partial-silver-baseline-baseline-run-1.json"
                )
                .path,
            runId: "baseline-run-1"
        )
        let first = Data("first".utf8)
        try BaselineRawPublisher.publish(first, to: output)
        #expect(try Data(contentsOf: output) == first)
        #expect(throws: BaselineRawPublisher.PublishError.self) {
            try BaselineRawPublisher.publish(Data("second".utf8), to: output)
        }
        #expect(try Data(contentsOf: output) == first)
    }

    @Test("baseline source identity requires the declared clean HEAD revision")
    func baselineSourceIdentityRejectsMismatch() throws {
        let revision = String(repeating: "a", count: 40)
        try BaselineSourceIdentity.validate(
            declaredRevision: revision,
            buildRevision: String(revision.prefix(12))
        )
        #expect(throws: BaselineSourceIdentity.SourceError.self) {
            try BaselineSourceIdentity.validate(
                declaredRevision: revision,
                buildRevision: String(repeating: "b", count: 12)
            )
        }
        #expect(throws: BaselineSourceIdentity.SourceError.self) {
            try BaselineSourceIdentity.validate(
                declaredRevision: revision,
                buildRevision: "unknown"
            )
        }
    }

    @Test("raw publisher pins its output directory across a parent swap")
    func baselineRawPublisherPinsOutputDirectory() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(
            "partial-silver-publisher-swap-\(UUID().uuidString)",
            isDirectory: true
        )
        let outputDirectory = root.appendingPathComponent("output", isDirectory: true)
        let hostile = root.appendingPathComponent("hostile", isDirectory: true)
        try FileManager.default.createDirectory(
            at: outputDirectory,
            withIntermediateDirectories: true
        )
        try FileManager.default.createDirectory(at: hostile, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let filename = "playhead-partial-silver-baseline-baseline-run-1.json"
        let output = outputDirectory.appendingPathComponent(filename)
        let bytes = Data("pinned bytes".utf8)

        #expect(throws: BaselineRawPublisher.PublishError.self) {
            try BaselineRawPublisher.publish(bytes, to: output) {
                try FileManager.default.moveItem(
                    at: outputDirectory,
                    to: root.appendingPathComponent("original", isDirectory: true)
                )
                try FileManager.default.createSymbolicLink(
                    at: outputDirectory,
                    withDestinationURL: hostile
                )
            }
        }

        #expect(try FileManager.default.contentsOfDirectory(atPath: hostile.path).isEmpty)
        #expect(try Data(contentsOf: root.appendingPathComponent("original/\(filename)")) == bytes)
    }
}

extension PipelineDumpHermeticTests {
    @Test("snapshot manifest decodes and lists the expected 9 episodes")
    func manifestDecodesNineEpisodes() throws {
        let tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "pipeline-dump-nine-\(UUID().uuidString)"
        )
        try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDirectory) }
        let fixtureURL = tempDirectory.appendingPathComponent("manifest.json")
        let fixture: [[String: String]] = (1...9).map { index in
            [
                "show": "Show \(index)",
                "showSlug": "show-\(index)",
                "episodeId": "episode-\(index)",
                "publishDate": "2026-06-\(String(format: "%02d", index))",
                "audioPath": "TestFixtures/Corpus/Audio/episode-\(index).mp3",
                "sha256": String(repeating: "a", count: 64),
            ]
        }
        try JSONSerialization.data(withJSONObject: fixture).write(to: fixtureURL)
        let entries = try PipelineDumpManifestLoader.decode(at: fixtureURL)

        #expect(entries.count == 9, "manifest must list exactly the 9 NEW snapshot episodes (got \(entries.count))")
        assertStructurallyValid(entries)
    }

    @Test("snapshot manifest rejects episode and audio path traversal")
    func manifestRejectsPathTraversal() throws {
        let tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "pipeline-dump-traversal-\(UUID().uuidString)"
        )
        try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDirectory) }
        let fixtureURL = tempDirectory.appendingPathComponent("manifest.json")

        for (episodeId, audioPath) in [
            ("../escape", "TestFixtures/Corpus/Audio/escape.mp3"),
            ("episode-1", "TestFixtures/Corpus/Audio/../../../escape.mp3"),
        ] {
            let fixture: [[String: String]] = [[
                "show": "Show",
                "showSlug": "show",
                "episodeId": episodeId,
                "publishDate": "2026-06-01",
                "audioPath": audioPath,
                "sha256": String(repeating: "a", count: 64),
            ]]
            try JSONSerialization.data(withJSONObject: fixture).write(to: fixtureURL)
            #expect(throws: PipelineDumpManifestLoader.LoadError.self) {
                _ = try PipelineDumpManifestLoader.decode(at: fixtureURL)
            }
        }
    }

    @Test("mutable live snapshot is structurally valid when present")
    func liveManifestIsStructurallyValidWhenPresent() throws {
        let repoRoot = PipelineDumpManifestLoader.repoRoot()
        let manifestURL = PipelineDumpManifestLoader.manifestURL(repoRoot: repoRoot)
        guard FileManager.default.fileExists(atPath: manifestURL.path) else { return }
        let entries = try PipelineDumpManifestLoader.load(repoRoot: repoRoot)
        #expect(!entries.isEmpty, "live snapshot manifest must not be empty")
        assertStructurallyValid(entries)
    }

    private func assertStructurallyValid(_ entries: [PipelineDumpSnapshotEntry]) {
        for entry in entries {
            #expect(!entry.episodeId.isEmpty, "episodeId missing")
            #expect(!entry.showSlug.isEmpty, "showSlug missing for \(entry.episodeId)")
            #expect(!entry.publishDate.isEmpty, "publishDate missing for \(entry.episodeId)")
            #expect(
                entry.audioPath.hasPrefix("TestFixtures/Corpus/Audio/"),
                "audioPath should be repo-root-relative under TestFixtures/Corpus/Audio/ — got \(entry.audioPath)"
            )
        }

        let uniqueIds = Set(entries.map(\.episodeId))
        #expect(
            uniqueIds.count == entries.count,
            "manifest has duplicate episodeIds (\(entries.count - uniqueIds.count) collision(s))"
        )
    }

    @Test("AdDetectionConfig.default carries the production state the dump expects")
    func productionConfigStateIsHeld() {
        // The dump's contract: production `.default`, all activation flags
        // off, FM full, chapter signal off. If any of these regress, the
        // dump's "production" claim is a lie — fail loud here on the sim
        // so the env-gated Catalyst run can't silently dump a different
        // shape than the orchestrator expects.
        let cfg = AdDetectionConfig.default

        #expect(cfg.fmBackfillMode == .full, "fmBackfillMode must be .full in production .default")
        #expect(cfg.chapterSignalMode == .off, "chapterSignalMode must be .off in production .default")
        #expect(cfg.lexicalAutoAdEnabled == false, "lexicalAutoAdEnabled must be off in production .default")
        #expect(cfg.evidenceFragilityPenaltyEnabled == false, "evidenceFragilityPenaltyEnabled must be off in production .default")
        #expect(cfg.audioForensicsEnabled == false, "audioForensicsEnabled must be off in production .default")
        #expect(cfg.crossEpisodeMemoryEnabled == false, "crossEpisodeMemoryEnabled must be off in production .default")
        #expect(cfg.rhetoricalGrammarEnabled == false, "rhetoricalGrammarEnabled must be off in production .default")
        #expect(cfg.crossShowSyndicationEnabled == false, "crossShowSyndicationEnabled must be off in production .default")
        #expect(cfg.temporalRegularizationEnabled == false, "temporalRegularizationEnabled must be off in production .default")
        #expect(cfg.perShowThresholdControlEnabled == false, "perShowThresholdControlEnabled must be off in production .default")
        // playhead-p56a R1 review: the dump's contract that the
        // `spanFinalizerConstraintsFired` JSON key is absent on every
        // window relies on the production .default keeping the finalizer
        // OFF. Without this pin, a future refactor that flipped
        // `spanFinalizerEnabled` to true in `.default` would silently
        // populate the field on every dump row, breaking downstream
        // analyzers that key off "absent = OFF arm" — and the byte-
        // identity-OFF regression test (`SpanFinalizerWireInTests.
        // flagOffMatchesDefaultBaseline`) would catch the AdWindow drift
        // but the dump-schema drift would land here.
        #expect(cfg.spanFinalizerEnabled == false, "spanFinalizerEnabled must be off in production .default")
        // playhead-l2f.6 (+ 2026-07-16 dogfood flip): stinger refinement
        // ships ON in production `.default`, so — unlike the finalizer key
        // above — the dump's `stingerRefinement` key IS populated on
        // bank-show windows (see the flag-ON encoding test). The pin
        // records that flipped state: a silent revert to OFF would flip
        // the dump schema back to key-absent-everywhere and invalidate
        // readers expecting live v4 traces — fail loud here instead.
        #expect(cfg.stingerRefinementEnabled == true, "ships ON per the recorded 2026-07-16 dogfood flip; xsdz.38 tracks the eat-class fix")

        let narrowing = NarrowingConfig.default
        #expect(narrowing.perAnchorPaddingSegments == 5)
        #expect(narrowing.maxNarrowedSegmentsPerPhase == 60)
        #expect(narrowing.acousticBreakSnapMaxDistanceSeconds == 2.0)
        #expect(narrowing.lexicalClusterSnapEnabled == true)
        #expect(narrowing.lexicalClusterGapSeconds == 8.0)
        #expect(narrowing.lexicalClusterMarginSegments == 3)
        #expect(narrowing.lexicalClusterMinHits == 1)
    }

    @Test("raw config captures every output-affecting production default")
    func baselineConfigSchemaIsComplete() throws {
        let encoder = JSONEncoder()
        encoder.keyEncodingStrategy = .convertToSnakeCase
        let data = try encoder.encode(BaselineProductionConfig.current())
        let root = try #require(
            JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        let defaults = try #require(
            root["ad_detection_defaults"] as? [String: Any]
        )
        #expect(Set(defaults.keys) == baselineAdDetectionDefaultKeys)
        #expect(root["entry_point"] as? String == "AdDetectionService.runBackfill")
        #expect(
            root["hot_path_classifier_identity"] as? String
                == "CoreMLSequenceClassifier()"
        )
        #expect(
            root["runner_admission_identity"] as? String
                == "permissive_capability_snapshot+battery_level_1.0"
        )
        #expect(root["scan_cohort_identity"] as? String == "ScanCohort.productionJSON")
        #expect(
            root["foundation_model_redactor_identity"] as? String
                == "PromptRedactor.loadDefault"
        )
        #expect(defaults["bracket_refinement_enabled"] as? Bool == true)
        #expect(defaults["transcript_boundary_cue_enabled"] as? Bool == true)
        #expect(defaults["rediff_slot_ownership_enabled"] as? Bool == false)
    }
}

// MARK: - Hermetic JSON shape tests for the dump schema

/// Locks the JSON wire shape of `DumpDecodedSpan` + the per-episode
/// `candidateDecodedSpanList` field added 2026-06-01 for playhead-4xqf.
/// Lives in this file because the dump structs are intentionally `private`;
/// keeping the encoding test alongside the schema means any future schema
/// refactor breaks here first instead of silently in a downstream Python
/// consumer.
@Suite("PipelineDump JSON encoding")
struct PipelineDumpEncodingTests {

    @Test("DumpDecodedSpan encodes all six fields with deterministic key names")
    func dumpDecodedSpanEncodesAllFields() throws {
        let span = DumpDecodedSpan(
            spanId: "span-42",
            startTime: 123.4,
            endTime: 234.5,
            proposalConfidence: 0.81,
            skipConfidence: 0.78,
            fragilityScore: 0.42
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(span)
        let json = try #require(String(data: data, encoding: .utf8))
        // Lock the wire shape — sortedKeys puts alpha; the downstream Python
        // analysis (in scripts/l2f-corpus-status.py and the bd-4xqf inline
        // analysis) reads by string key, so the keys themselves are the
        // contract.
        let parsed = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        #expect(Set(parsed.keys) == Set([
            "spanId", "startTime", "endTime",
            "proposalConfidence", "skipConfidence", "fragilityScore",
        ]))
        #expect((parsed["spanId"] as? String) == "span-42")
        #expect((parsed["startTime"] as? Double) == 123.4)
        #expect((parsed["endTime"] as? Double) == 234.5)
        #expect((parsed["fragilityScore"] as? Double) == 0.42)
        // Defense in depth: also assert the keys appear in the raw JSON
        // text so a future "rename via CodingKeys" can't silently pass.
        #expect(json.contains("\"spanId\""))
        #expect(json.contains("\"fragilityScore\""))
    }

    @Test("DumpEpisode preserves candidateDecodedSpans int alongside the new list")
    func dumpEpisodeKeepsCountFieldForBackwardCompat() throws {
        // playhead-4xqf added `candidateDecodedSpanList` next to the
        // pre-existing `candidateDecodedSpans` int. The pre-existing field
        // MUST remain — downstream `playhead-dogfood-diagnostics-pipeline-
        // dump-*.json` readers in scripts/ still read it (and the Tier-A
        // auto-promote audit-rejects path on main treats it as a stable
        // integer). If a future refactor removes it, those scripts break
        // silently. Lock it here.
        let episode = DumpEpisode(
            episodeId: "ep-1",
            audioFingerprint: "sha256:" + String(repeating: "a", count: 64),
            showSlug: "show-x",
            publishDate: "2026-06-01",
            episodeDurationSeconds: 1800.0,
            candidateDecodedSpans: 3,
            candidateDecodedSpanList: [
                DumpDecodedSpan(spanId: "a", startTime: 10, endTime: 50,
                                proposalConfidence: 0.6, skipConfidence: 0.5,
                                fragilityScore: 0.3),
                DumpDecodedSpan(spanId: "b", startTime: 100, endTime: 130,
                                proposalConfidence: 0.7, skipConfidence: 0.65,
                                fragilityScore: 0.2),
                DumpDecodedSpan(spanId: "c", startTime: 200, endTime: 260,
                                proposalConfidence: 0.9, skipConfidence: 0.88,
                                fragilityScore: 0.1),
            ],
            adWindows: []
        )
        let data = try JSONEncoder().encode(episode)
        let parsed = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        #expect((parsed["candidateDecodedSpans"] as? Int) == 3)
        #expect((parsed["audioFingerprint"] as? String) == "sha256:" + String(repeating: "a", count: 64))
        let list = try #require(parsed["candidateDecodedSpanList"] as? [[String: Any]])
        #expect(list.count == 3)
        #expect((list[0]["spanId"] as? String) == "a")
        #expect((list[1]["startTime"] as? Double) == 100)
        #expect((list[2]["endTime"] as? Double) == 260)
        // The list count and the count field are independent at the schema
        // level (the live runner happens to compute them from the same
        // observer), so this hermetic test deliberately uses matching
        // values to document the EXPECTED invariant downstream readers
        // should assume holds in real dumps.
        #expect(list.count == (parsed["candidateDecodedSpans"] as? Int))
    }

    // MARK: - playhead-4xqf FUSION_DROP suspect fields
    //
    // The next three tests lock the wire shape for the 2026-06-01 extension:
    // `wasSkipped`, `boundaryRefinementStartAdjustment`,
    // `boundaryRefinementEndAdjustment`. The `dumpAdWindowRetainsPreExistingFields`
    // test exists specifically as a regression guard — if a future refactor
    // drops or renames an existing field (startTime / endTime / skipConfidence
    // / decisionState / eligibilityGate / promotionTrack), `scripts/
    // l2f-bd4xqf-analyze.py` and the dogfood-diagnostics consumers on main
    // would break silently. Lock the schema here so the regression fails on
    // the simulator before a 90-minute Catalyst run silently dumps an
    // incompatible shape.

    @Test("DumpAdWindow encodes wasSkipped=true and wasSkipped=false correctly")
    func dumpAdWindowEncodesWasSkippedBothValues() throws {
        // True case.
        let skipped = DumpAdWindow(
            startTime: 100.0,
            endTime: 130.5,
            skipConfidence: 0.92,
            decisionState: "confirmed",
            eligibilityGate: "autoSkip",
            promotionTrack: nil,
            wasSkipped: true,
            boundaryRefinementStartAdjustment: nil,
            boundaryRefinementEndAdjustment: nil,
            spanFinalizerConstraintsFired: nil,
            stingerRefinement: nil
        )
        let skippedData = try JSONEncoder().encode(skipped)
        let skippedParsed = try #require(
            try JSONSerialization.jsonObject(with: skippedData) as? [String: Any]
        )
        // JSONSerialization decodes JSON booleans as NSNumber bridged to
        // Bool — read via Bool? not Int? so the assertion is unambiguous.
        #expect((skippedParsed["wasSkipped"] as? Bool) == true)

        // False case — independently encoded to exercise both branches of
        // the Bool encoder path (single-value containers can special-case
        // either side).
        let notSkipped = DumpAdWindow(
            startTime: 200.0,
            endTime: 230.5,
            skipConfidence: 0.51,
            decisionState: "candidate",
            eligibilityGate: "blockedByEvidenceQuorum",
            promotionTrack: nil,
            wasSkipped: false,
            boundaryRefinementStartAdjustment: nil,
            boundaryRefinementEndAdjustment: nil,
            spanFinalizerConstraintsFired: nil,
            stingerRefinement: nil
        )
        let notSkippedData = try JSONEncoder().encode(notSkipped)
        let notSkippedParsed = try #require(
            try JSONSerialization.jsonObject(with: notSkippedData) as? [String: Any]
        )
        #expect((notSkippedParsed["wasSkipped"] as? Bool) == false)

        // Defense in depth: the raw JSON text should literally contain
        // `"wasSkipped":true` / `"wasSkipped":false`, not a quoted string
        // or numeric form — downstream Python `json.load(...)` will surface
        // a Python bool only if the wire form is a JSON bool.
        let skippedJSON = try #require(String(data: skippedData, encoding: .utf8))
        let notSkippedJSON = try #require(String(data: notSkippedData, encoding: .utf8))
        #expect(skippedJSON.contains("\"wasSkipped\":true"))
        #expect(notSkippedJSON.contains("\"wasSkipped\":false"))
    }

    @Test("DumpAdWindow omits nil boundary-refinement deltas and encodes non-nil ones as Double")
    func dumpAdWindowEncodesBoundaryRefinementAdjustments() throws {
        // Default `JSONEncoder` strategy is to OMIT nil-valued optional
        // properties from the encoded object (it does NOT emit explicit
        // `null` keys). This matches the pre-existing handling of
        // `promotionTrack` and `eligibilityGate` in this dump — the
        // downstream Python consumers (`scripts/l2f-bd4xqf-analyze.py`
        // + the dogfood-diagnostics readers) use `dict.get(key)`, which
        // returns `None` whether the key is absent or maps to `null`.
        // Both cases are observationally equivalent at the consumer.
        // Lock that contract here: nil → key absent; non-nil → key
        // present as a Double.

        // Nil case — the normal-flow shape: persisted bounds are already
        // post-refinement, so re-feeding them yields zero deltas and the
        // mapper records `nil`. The encoded object should have NEITHER
        // key (consistent with how `promotionTrack: nil` is handled in
        // every dump emitted so far).
        let nilAdj = DumpAdWindow(
            startTime: 100.0,
            endTime: 130.5,
            skipConfidence: 0.92,
            decisionState: "confirmed",
            eligibilityGate: "autoSkip",
            promotionTrack: nil,
            wasSkipped: true,
            boundaryRefinementStartAdjustment: nil,
            boundaryRefinementEndAdjustment: nil,
            spanFinalizerConstraintsFired: nil,
            stingerRefinement: nil
        )
        let nilData = try JSONEncoder().encode(nilAdj)
        let nilParsed = try #require(
            try JSONSerialization.jsonObject(with: nilData) as? [String: Any]
        )
        // Keys must be ABSENT (not present with null), matching the
        // default-encoder convention this schema has used since day one.
        #expect(nilParsed["boundaryRefinementStartAdjustment"] == nil)
        #expect(nilParsed["boundaryRefinementEndAdjustment"] == nil)
        #expect(!nilParsed.keys.contains("boundaryRefinementStartAdjustment"))
        #expect(!nilParsed.keys.contains("boundaryRefinementEndAdjustment"))

        // Non-nil case — the divergence shape: production's refinement
        // choice landed somewhere the legacy refiner thinks could be
        // further snapped. Both positive (push right) and negative (pull
        // left) deltas are exercised so a future "abs()" or "clip-to-
        // positive" regression in the mapper would fail here.
        let nonNilAdj = DumpAdWindow(
            startTime: 100.0,
            endTime: 130.5,
            skipConfidence: 0.92,
            decisionState: "confirmed",
            eligibilityGate: "autoSkip",
            promotionTrack: nil,
            wasSkipped: true,
            boundaryRefinementStartAdjustment: -0.75,
            boundaryRefinementEndAdjustment: 1.25,
            spanFinalizerConstraintsFired: nil,
            stingerRefinement: nil
        )
        let nonNilData = try JSONEncoder().encode(nonNilAdj)
        let nonNilParsed = try #require(
            try JSONSerialization.jsonObject(with: nonNilData) as? [String: Any]
        )
        #expect((nonNilParsed["boundaryRefinementStartAdjustment"] as? Double) == -0.75)
        #expect((nonNilParsed["boundaryRefinementEndAdjustment"] as? Double) == 1.25)
        // Keys must now be PRESENT — flips the "absent when nil" rule.
        #expect(nonNilParsed.keys.contains("boundaryRefinementStartAdjustment"))
        #expect(nonNilParsed.keys.contains("boundaryRefinementEndAdjustment"))
    }

    @Test("DumpAdWindow preserves all pre-existing fields alongside the new ones")
    func dumpAdWindowRetainsPreExistingFields() throws {
        // Regression guard for the playhead-4xqf field-extension. The
        // `scripts/l2f-bd4xqf-analyze.py` pinpoint script + the broader
        // dogfood-diagnostics readers on main consume `startTime`,
        // `endTime`, `skipConfidence`, `decisionState`, `eligibilityGate`,
        // and `promotionTrack` by string key; if a future schema refactor
        // drops or renames any of them, those scripts break silently. Lock
        // the full key set here so the simulator-side hermetic test fails
        // first.
        let window = DumpAdWindow(
            startTime: 123.4,
            endTime: 234.5,
            skipConfidence: 0.78,
            decisionState: "confirmed",
            eligibilityGate: "autoSkip",
            promotionTrack: nil,
            wasSkipped: true,
            boundaryRefinementStartAdjustment: 0.5,
            boundaryRefinementEndAdjustment: nil,
            spanFinalizerConstraintsFired: nil,
            stingerRefinement: nil
        )
        let data = try JSONEncoder().encode(window)
        let parsed = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        // Non-nil keys are required. (Optional nil keys — `promotionTrack`
        // and `boundaryRefinementEndAdjustment` here — are intentionally
        // omitted by the default encoder; see the
        // `dumpAdWindowEncodesBoundaryRefinementAdjustments` test for the
        // rationale.) This still locks the wire shape: any drop or rename
        // of a non-nil-valued field will surface here loudly.
        let requiredKeys: Set<String> = [
            "startTime", "endTime", "skipConfidence", "decisionState",
            "eligibilityGate",
            "wasSkipped",
            "boundaryRefinementStartAdjustment",
        ]
        #expect(requiredKeys.isSubset(of: Set(parsed.keys)))
        // Pre-existing values round-trip.
        #expect((parsed["startTime"] as? Double) == 123.4)
        #expect((parsed["endTime"] as? Double) == 234.5)
        #expect((parsed["skipConfidence"] as? Double) == 0.78)
        #expect((parsed["decisionState"] as? String) == "confirmed")
        #expect((parsed["eligibilityGate"] as? String) == "autoSkip")
        // promotionTrack: nil → key absent (matches every dump shipped so
        // far). Lock the "absent, not null" wire form so a future change
        // to JSONEncoder strategy (e.g. forced-null encoding) is caught.
        #expect(!parsed.keys.contains("promotionTrack"))
        // playhead-p56a: spanFinalizerConstraintsFired: nil → key absent.
        // The flag is OFF in `.default` and EVERY lane's config keeps that
        // value (the treatment config differs from `.default` only by the
        // rediff flag), so live dumps never carry this key.
        #expect(!parsed.keys.contains("spanFinalizerConstraintsFired"))
        // playhead-l2f.6: stingerRefinement: nil → key absent. Unlike the
        // finalizer, stinger refinement ships ON in `.default` (2026-07-16
        // dogfood flip), so live dumps DO carry this key on bank-show
        // windows — it is absent here only because this constructed window
        // records no trace (non-bank show, or flag off).
        #expect(!parsed.keys.contains("stingerRefinement"))
        // New values round-trip (mixed nil/non-nil pair — encoded
        // independently, not as a 2-tuple).
        #expect((parsed["wasSkipped"] as? Bool) == true)
        #expect((parsed["boundaryRefinementStartAdjustment"] as? Double) == 0.5)
        #expect(!parsed.keys.contains("boundaryRefinementEndAdjustment"))
    }

    // MARK: - playhead-p56a SpanFinalizer trace field
    //
    // The next two tests lock the wire shape for the 2026-06-01 extension
    // that surfaces the `SpanFinalizer.finalize(...)` constraint trace per
    // AdWindow. Mirrors the same OFF=absent / ON=array contract the
    // `boundaryRefinement*` adjustments use, so `scripts/l2f-bd4xqf-
    // analyze.py` and the orchestrator's dump readers can flip between
    // arms without a schema migration.

    @Test("DumpAdWindow omits spanFinalizerConstraintsFired when nil (flag-OFF)")
    func dumpAdWindowOmitsSpanFinalizerConstraintsWhenNil() throws {
        // Flag OFF — the production `.default` arm. The live dump never
        // populates the field; the default `JSONEncoder` strategy omits
        // the key entirely (does NOT emit explicit `null`), matching the
        // pre-existing `promotionTrack` / `boundaryRefinement*` convention
        // used by every dump shipped so far. Downstream Python consumers
        // (`scripts/l2f-bd4xqf-analyze.py` + the dogfood-diagnostics
        // readers) use `dict.get(key)`, which returns `None` whether the
        // key is absent or maps to `null` — so this also guards against
        // a future strategy flip that would emit explicit nulls.
        let window = DumpAdWindow(
            startTime: 100.0,
            endTime: 130.5,
            skipConfidence: 0.92,
            decisionState: "confirmed",
            eligibilityGate: "autoSkip",
            promotionTrack: nil,
            wasSkipped: true,
            boundaryRefinementStartAdjustment: nil,
            boundaryRefinementEndAdjustment: nil,
            spanFinalizerConstraintsFired: nil,
            stingerRefinement: nil
        )
        let data = try JSONEncoder().encode(window)
        let parsed = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        #expect(parsed["spanFinalizerConstraintsFired"] == nil)
        #expect(!parsed.keys.contains("spanFinalizerConstraintsFired"))
        // Defense in depth: the raw JSON text must not contain the key in
        // any form. A future CodingKeys rename would silently regress
        // downstream parsers; catch it on the simulator instead.
        let json = try #require(String(data: data, encoding: .utf8))
        #expect(!json.contains("spanFinalizerConstraintsFired"))
    }

    @Test("DumpAdWindow encodes spanFinalizerConstraintsFired as a JSON string array when present (flag-ON)")
    func dumpAdWindowEncodesSpanFinalizerConstraintsWhenPresent() throws {
        // Flag ON — the experimental arm. The live dump pulls the
        // per-window trace from
        // `AdDetectionService.spanFinalizerConstraintsByWindowIdForTesting()`
        // and surfaces it as a JSON array of `FinalizerConstraint.rawValue`
        // strings in trace-emission order. Order is load-bearing for the
        // bd-4xqf attribution analyzer: constraint #2 (merge) firing
        // BEFORE constraint #3 (duration sanity) tells a different story
        // than the reverse.
        let trace = [
            FinalizerConstraint.mergedWithAdjacent.rawValue,
            FinalizerConstraint.policyOverrideApplied.rawValue,
        ]
        let window = DumpAdWindow(
            startTime: 100.0,
            endTime: 130.5,
            skipConfidence: 0.92,
            decisionState: "confirmed",
            eligibilityGate: "autoSkip",
            promotionTrack: nil,
            wasSkipped: true,
            boundaryRefinementStartAdjustment: nil,
            boundaryRefinementEndAdjustment: nil,
            spanFinalizerConstraintsFired: trace,
            stingerRefinement: nil
        )
        let data = try JSONEncoder().encode(window)
        let parsed = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        #expect(parsed.keys.contains("spanFinalizerConstraintsFired"))
        let decoded = try #require(
            parsed["spanFinalizerConstraintsFired"] as? [String]
        )
        #expect(decoded == trace, "trace round-trips in emission order, not alphabetized")
        // Defense in depth: lock the raw-JSON wire form so a future
        // CodingKeys rename surfaces here. JSON arrays preserve order;
        // assert the rawValues appear in trace order, not the opposite.
        let json = try #require(String(data: data, encoding: .utf8))
        #expect(json.contains("\"spanFinalizerConstraintsFired\""))
        let mergedIdx = try #require(json.range(of: "mergedWithAdjacent")?.lowerBound)
        let policyIdx = try #require(json.range(of: "policyOverrideApplied")?.lowerBound)
        #expect(mergedIdx < policyIdx, "trace order must survive the JSON encode")
    }

    // MARK: - playhead-l2f.6 stinger refinement trace field
    //
    // The next two tests lock the wire shape for the `stingerRefinement`
    // extension. Mirrors the same OFF=absent / ON=object contract the
    // `spanFinalizerConstraintsFired` field uses, so the gold scorer and
    // the bd-4xqf-style analyzers can flip between arms without a schema
    // migration.

    @Test("DumpAdWindow omits stingerRefinement when nil (flag-OFF)")
    func dumpAdWindowOmitsStingerRefinementWhenNil() throws {
        // The no-trace arm: flag explicitly OFF, or — under the shipping
        // flag-ON default (2026-07-16 dogfood flip) — a window whose show
        // resolved no bank entry. Either way the live dump leaves the
        // field nil and the default `JSONEncoder` strategy omits the key
        // entirely (does NOT emit explicit `null`), matching the
        // pre-existing optional-field convention used by every dump
        // shipped so far.
        let window = DumpAdWindow(
            startTime: 100.0,
            endTime: 130.5,
            skipConfidence: 0.92,
            decisionState: "confirmed",
            eligibilityGate: "autoSkip",
            promotionTrack: nil,
            wasSkipped: true,
            boundaryRefinementStartAdjustment: nil,
            boundaryRefinementEndAdjustment: nil,
            spanFinalizerConstraintsFired: nil,
            stingerRefinement: nil
        )
        let data = try JSONEncoder().encode(window)
        let parsed = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        #expect(parsed["stingerRefinement"] == nil)
        #expect(!parsed.keys.contains("stingerRefinement"))
        // Defense in depth: the raw JSON text must not contain the key in
        // any form so downstream `dict.get("stingerRefinement")` readers
        // see None on no-trace windows.
        let json = try #require(String(data: data, encoding: .utf8))
        #expect(!json.contains("stingerRefinement"))
    }

    @Test("DumpAdWindow encodes stingerRefinement as a JSON object when present (flag-ON)")
    func dumpAdWindowEncodesStingerRefinementWhenPresent() throws {
        // Flag ON — the production arm. The live dump pulls the
        // per-window trace from
        // `AdDetectionService.stingerRefinementTraceByWindowIdForTesting()`
        // and surfaces it as a nested object. A derived-partner one-sided
        // snap (v4 joint shape) is used so the booleans plus every
        // optional-field group are exercised in one object: startPeak
        // present + endPeak absent (only the start side snapped), both
        // deltas present (the derived candidate moved the end edge too),
        // both candidate counts + pairScore + gridTermApplied present
        // (playhead-xsdz.38 joint-decision fields).
        let window = DumpAdWindow(
            startTime: 100.0,
            endTime: 130.5,
            skipConfidence: 0.92,
            decisionState: "confirmed",
            eligibilityGate: "autoSkip",
            promotionTrack: nil,
            wasSkipped: true,
            boundaryRefinementStartAdjustment: nil,
            boundaryRefinementEndAdjustment: nil,
            spanFinalizerConstraintsFired: nil,
            stingerRefinement: DumpStingerRefinement(
                startSnapped: true,
                endSnapped: false,
                gridApplied: true,
                revertedNoOverlap: false,
                startPeak: 0.874,
                endPeak: nil,
                startDeltaSeconds: -6.42,
                endDeltaSeconds: 2.08,
                startCandidateCount: 2,
                endCandidateCount: 4,
                pairScore: 1.4992,
                gridTermApplied: "bonus"
            )
        )
        let data = try JSONEncoder().encode(window)
        let parsed = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        let refinement = try #require(
            parsed["stingerRefinement"] as? [String: Any]
        )
        #expect((refinement["startSnapped"] as? Bool) == true)
        #expect((refinement["endSnapped"] as? Bool) == false)
        #expect((refinement["gridApplied"] as? Bool) == true)
        #expect((refinement["revertedNoOverlap"] as? Bool) == false)
        #expect((refinement["startPeak"] as? Double) == 0.874)
        // Nil optionals inside the nested object follow the same
        // absent-not-null convention as the top level.
        #expect(!refinement.keys.contains("endPeak"))
        #expect((refinement["startDeltaSeconds"] as? Double) == -6.42)
        #expect((refinement["endDeltaSeconds"] as? Double) == 2.08)
        // playhead-xsdz.38 joint-decision fields: counts as JSON ints,
        // score as Double, grid term as a plain string.
        #expect((refinement["startCandidateCount"] as? Int) == 2)
        #expect((refinement["endCandidateCount"] as? Int) == 4)
        #expect((refinement["pairScore"] as? Double) == 1.4992)
        #expect((refinement["gridTermApplied"] as? String) == "bonus")
        // Defense in depth: raw wire form carries the nested key names.
        let json = try #require(String(data: data, encoding: .utf8))
        #expect(json.contains("\"stingerRefinement\""))
        #expect(json.contains("\"startSnapped\":true"))
        #expect(!json.contains("\"endPeak\""))
        #expect(json.contains("\"gridTermApplied\":\"bonus\""))
    }

    @Test("DumpAdWindow omits the nil xsdz.38 joint fields inside stingerRefinement (no-evidence consult)")
    func dumpAdWindowOmitsNilJointFieldsInsideStingerRefinement() throws {
        // A flag-ON consult with no evidence candidates anywhere (e.g. no
        // PCM) records a pristine trace: the four xsdz.38 fields are nil
        // and must be ABSENT from the nested object, so downstream
        // `refinement.get("pairScore")` readers see None on no-evidence
        // windows exactly as they do on pre-v4 dumps (schema-compatible
        // extension).
        let window = DumpAdWindow(
            startTime: 100.0,
            endTime: 130.5,
            skipConfidence: 0.92,
            decisionState: "confirmed",
            eligibilityGate: "autoSkip",
            promotionTrack: nil,
            wasSkipped: false,
            boundaryRefinementStartAdjustment: nil,
            boundaryRefinementEndAdjustment: nil,
            spanFinalizerConstraintsFired: nil,
            stingerRefinement: DumpStingerRefinement(StingerRefinementTrace())
        )
        let data = try JSONEncoder().encode(window)
        let parsed = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        let refinement = try #require(
            parsed["stingerRefinement"] as? [String: Any]
        )
        // Booleans stay present (non-optional), all optionals absent.
        #expect((refinement["startSnapped"] as? Bool) == false)
        #expect((refinement["revertedNoOverlap"] as? Bool) == false)
        for absentKey in [
            "startPeak", "endPeak", "startDeltaSeconds", "endDeltaSeconds",
            "startCandidateCount", "endCandidateCount", "pairScore",
            "gridTermApplied",
        ] {
            #expect(!refinement.keys.contains(absentKey), "\(absentKey) must be omitted when nil")
        }
        let json = try #require(String(data: data, encoding: .utf8))
        #expect(!json.contains("pairScore"))
        #expect(!json.contains("gridTermApplied"))
    }

    /// playhead-xsdz.36.1 (R4): the treatment dump stamps its fresh-B-side
    /// coverage; the baseline/legacy lanes pass nil and their artifact bytes
    /// must be UNCHANGED — the key must be omitted, not encoded as null.
    @Test("DumpPayload omits bSideCoverage when nil and encodes all four coverage fields when present")
    func dumpPayloadBSideCoverageOmittedWhenNilEncodedWhenPresent() throws {
        let summary = DumpSummary(totalEpisodes: 0, totalAdWindows: 0, perShow: [:])
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]

        let baseline = DumpPayload(
            config: "production .default",
            runUtc: "2026-07-17T00:00:00Z",
            bSideCoverage: nil,
            episodes: [],
            summary: summary
        )
        let baselineParsed = try #require(
            try JSONSerialization.jsonObject(with: encoder.encode(baseline)) as? [String: Any]
        )
        #expect(Set(baselineParsed.keys) == Set(["config", "runUtc", "episodes", "summary"]))

        let treatment = DumpPayload(
            config: "treatment",
            runUtc: "2026-07-17T00:00:00Z",
            bSideCoverage: DumpBSideCoverage(
                stagedEntryCount: 2,
                manifestEntryCount: 5,
                unstagedEpisodeIds: ["ep-b", "ep-c", "ep-d"],
                irregularEpisodeIds: ["ep-d"]
            ),
            episodes: [],
            summary: summary
        )
        let treatmentParsed = try #require(
            try JSONSerialization.jsonObject(with: encoder.encode(treatment)) as? [String: Any]
        )
        // R6: pin the treatment ROOT key set too — exactly the baseline keys
        // plus `bSideCoverage`, symmetric with the baseline pin above.
        #expect(Set(treatmentParsed.keys) == Set([
            "config", "runUtc", "bSideCoverage", "episodes", "summary",
        ]))
        let coverage = try #require(treatmentParsed["bSideCoverage"] as? [String: Any])
        // R5: pin the coverage object's exact key SET, not just the four known
        // values — a stray extra field (or a rename surviving the optional
        // casts below) must fail this schema pin, same as the root payload.
        #expect(Set(coverage.keys) == Set([
            "stagedEntryCount", "manifestEntryCount",
            "unstagedEpisodeIds", "irregularEpisodeIds",
        ]))
        #expect((coverage["stagedEntryCount"] as? Int) == 2)
        #expect((coverage["manifestEntryCount"] as? Int) == 5)
        #expect((coverage["unstagedEpisodeIds"] as? [String]) == ["ep-b", "ep-c", "ep-d"])
        #expect((coverage["irregularEpisodeIds"] as? [String]) == ["ep-d"])
    }
}

// MARK: - Rediff treatment harness wiring (playhead-xsdz.36.1)

/// Hermetic, sim-runnable proof of the rediff-treatment glue, WITHOUT the
/// 90-minute live FM backfill: (1) the treatment config differs from the shipped
/// `.default` ONLY by the rediff flag; (2) the A-side `captureAndPersist` +
/// `fetchEpisodeFingerprints` round-trip carries the exact identity that makes
/// `RediffSlotOwnership.gateAndDiff` return `.accepted` (both identity gates plus
/// the re-encode guard). The one heavy check — decoding a real staged
/// `.fresh.mp3` through the provider — is env-gated so it never burdens
/// `PlayheadFastTests`.
@Suite("Rediff treatment harness wiring (playhead-xsdz.36.1)")
struct RediffTreatmentHarnessWiringTests {

    /// A varying multi-tone waveform so the chroma fingerprinter sees real
    /// spectral content and emits a non-empty subfingerprint stream.
    private static func syntheticTone16k(seconds: Double) -> [Float] {
        let n = Int(seconds * 16_000)
        guard n > 0 else { return [] }
        var out = [Float](repeating: 0, count: n)
        for i in 0..<n {
            let t = Double(i) / 16_000.0
            let v = sin(2 * .pi * 220 * t) * 0.5
                + sin(2 * .pi * 440 * t) * 0.3
                + sin(2 * .pi * 660 * t) * 0.2
            out[i] = Float(v)
        }
        return out
    }

    @Test("treatment config differs from production .default ONLY by the rediff flag")
    func treatmentConfigDiffersOnlyByRediffFlag() {
        let base = AdDetectionConfig.default
        let treatment = makeRediffTreatmentConfig()

        // The single intended difference.
        #expect(treatment.rediffSlotOwnershipEnabled == true)
        #expect(base.rediffSlotOwnershipEnabled == false)
        // Mutually-exclusive width setter stays OFF (gateAndDiff / config init
        // precondition).
        #expect(treatment.spliceSlotOwnershipEnabled == false)

        // Every other output-affecting field must equal the shipped default.
        #expect(treatment.fmBackfillMode == base.fmBackfillMode)
        #expect(treatment.chapterSignalMode == base.chapterSignalMode)
        #expect(treatment.candidateThreshold == base.candidateThreshold)
        #expect(treatment.confirmationThreshold == base.confirmationThreshold)
        #expect(treatment.suppressionThreshold == base.suppressionThreshold)
        #expect(treatment.autoSkipConfidenceThreshold == base.autoSkipConfidenceThreshold)
        #expect(treatment.markOnlyThreshold == base.markOnlyThreshold)
        #expect(treatment.detectorVersion == base.detectorVersion)
        #expect(treatment.hotPathLookahead == base.hotPathLookahead)
        #expect(treatment.lexicalAutoAdEnabled == base.lexicalAutoAdEnabled)
        #expect(treatment.audioForensicsEnabled == base.audioForensicsEnabled)
        #expect(treatment.crossEpisodeMemoryEnabled == base.crossEpisodeMemoryEnabled)
        #expect(treatment.rhetoricalGrammarEnabled == base.rhetoricalGrammarEnabled)
        #expect(treatment.crossShowSyndicationEnabled == base.crossShowSyndicationEnabled)
        #expect(treatment.temporalRegularizationEnabled == base.temporalRegularizationEnabled)
        #expect(treatment.perShowThresholdControlEnabled == base.perShowThresholdControlEnabled)
        #expect(treatment.spanFinalizerEnabled == base.spanFinalizerEnabled)
        #expect(treatment.spliceSlotShadowEnabled == base.spliceSlotShadowEnabled)
        #expect(treatment.rediffSlotShadowEnabled == base.rediffSlotShadowEnabled)
        #expect(treatment.stingerRefinementEnabled == base.stingerRefinementEnabled)
        #expect(treatment.lexicalAnchorRefinementEnabled == base.lexicalAnchorRefinementEnabled)
        #expect(treatment.selfPromoSuppressionEnabled == base.selfPromoSuppressionEnabled)
        #expect(treatment.userCorrectionReadSideEnabled == base.userCorrectionReadSideEnabled)
        #expect(treatment.bracketRefinementEnabled == base.bracketRefinementEnabled)
        #expect(treatment.transcriptBoundaryCueEnabled == base.transcriptBoundaryCueEnabled)
        #expect(treatment.evidenceFragilityPenaltyEnabled == base.evidenceFragilityPenaltyEnabled)
        #expect(treatment.fmScanBudgetSeconds == base.fmScanBudgetSeconds)
        #expect(treatment.fmConsensusThreshold == base.fmConsensusThreshold)
    }

    /// R1: reflection-exhaustive twin of the field-by-field check above. The
    /// explicit `#expect` list can only cover fields that existed when it was
    /// written; if a FUTURE `AdDetectionConfig` field ships with an init default
    /// that differs from the `.default` static's value, `makeRediffTreatmentConfig`
    /// still compiles without it and the treatment arm silently drifts —
    /// invalidating the A/B comparison. Mirroring every STORED property and
    /// requiring exactly one difference (the rediff flag) makes the
    /// differs-only-by-flag invariant structurally future-proof.
    @Test("treatment config differs from .default in EXACTLY one stored property (reflection-exhaustive)")
    func treatmentConfigDiffersInExactlyOneStoredProperty() {
        let baseChildren = Array(Mirror(reflecting: AdDetectionConfig.default).children)
        let treatmentChildren = Array(Mirror(reflecting: makeRediffTreatmentConfig()).children)
        #expect(baseChildren.count == treatmentChildren.count)

        var differing: [String] = []
        for (base, treatment) in zip(baseChildren, treatmentChildren) {
            #expect(base.label == treatment.label)
            // Every stored property is Bool/Int/Double/String or a simple enum,
            // for which `String(describing:)` is a faithful equality proxy.
            if String(describing: base.value) != String(describing: treatment.value) {
                differing.append(base.label ?? "<unlabeled>")
            }
        }
        #expect(differing == ["rediffSlotOwnershipEnabled"])
    }

    @Test("A-side capture round-trips the identity the rediff gate accepts")
    func aSideCaptureIdentityRoundTrips() async throws {
        let store = try await makeTestStore()
        let episodeId = "rediff-treatment-identity-ep"
        // The EXACT identity the treatment lane persists: assetFingerprint ==
        // "pipeline-dump-fp-<id>" == the gate's currentAssetFingerprint.
        let assetFingerprint = "pipeline-dump-fp-\(episodeId)"
        try await store.insertAsset(AnalysisAsset(
            id: episodeId,
            episodeId: episodeId,
            assetFingerprint: assetFingerprint,
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(episodeId).mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        let mono = Self.syntheticTone16k(seconds: 12)
        let shard = AnalysisShard(
            id: 0, episodeID: episodeId, startTime: 0, duration: 12, samples: mono
        )
        // Same static helper + sourceAudioIdentity the treatment lane uses. The
        // `captureEnabledByDefault` flag gates only the live pipeline branch, not
        // this static call — no production flag flipped.
        try await EpisodeFingerprintCapture.captureAndPersist(
            shards: [shard],
            assetId: episodeId,
            sourceAudioIdentity: assetFingerprint,
            store: store
        )

        // Round-trip: identity gate (a) [algorithmVersion] + (b) [sourceAudioIdentity].
        let fetched = try await store.fetchEpisodeFingerprints(assetId: episodeId)
        let record = try #require(
            fetched,
            "capture must persist a fetchable record at the current algorithmVersion"
        )
        #expect(record.algorithmVersion == ChromaFingerprinter.algorithmVersion)
        #expect(record.sourceAudioIdentity == assetFingerprint)
        #expect(!record.fingerprints.isEmpty)

        // The EXACT gate the service applies. Matching identity + B == A ⇒ neither
        // identity rejection fires and the re-encode guard passes ⇒ .accepted.
        let outcome = RediffSlotOwnership.gateAndDiff(
            storedASide: record,
            refetchedBSideSamples16kHz: mono,
            currentAssetFingerprint: assetFingerprint
        )
        guard case .accepted = outcome else {
            Issue.record("rediff gate rejected a matching-identity A/B pair: \(outcome)")
            return
        }
    }

    @Test("gate rejects a mismatched sourceAudioIdentity (identity gate (b))")
    func gateRejectsIdentityMismatch() async throws {
        let store = try await makeTestStore()
        let episodeId = "rediff-treatment-mismatch-ep"
        try await store.insertAsset(AnalysisAsset(
            id: episodeId, episodeId: episodeId,
            assetFingerprint: "pipeline-dump-fp-\(episodeId)",
            weakFingerprint: nil, sourceURL: "file:///tmp/\(episodeId).mp3",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil
        ))
        let mono = Self.syntheticTone16k(seconds: 12)
        // Persist under a DIFFERENT identity than the gate will assert.
        try await EpisodeFingerprintCapture.captureAndPersist(
            shards: [AnalysisShard(id: 0, episodeID: episodeId, startTime: 0, duration: 12, samples: mono)],
            assetId: episodeId,
            sourceAudioIdentity: "some-other-audio-identity",
            store: store
        )
        let fetched = try await store.fetchEpisodeFingerprints(assetId: episodeId)
        let record = try #require(fetched)
        let outcome = RediffSlotOwnership.gateAndDiff(
            storedASide: record,
            refetchedBSideSamples16kHz: mono,
            currentAssetFingerprint: "pipeline-dump-fp-\(episodeId)"
        )
        guard case .rejectedAudioIdentityMismatch = outcome else {
            Issue.record("expected identity-mismatch rejection, got \(outcome)")
            return
        }
    }

    /// Real decode of an offline-staged `.fresh.mp3` through the provider. Heavy
    /// (decodes a full episode), so it is env-gated OFF in `PlayheadFastTests` and
    /// exercised on demand. NOTE (R1, verified empirically): this scheme uses
    /// test PLANS, and Xcode ignores `TEST_RUNNER_`-prefixed command-line env
    /// overrides when a test plan is in use (both `test` and
    /// `test-without-building`) — the SAME quirk the Catalyst capture lane hit
    /// (env only via patched xctestrun). To exercise this test: build-for-testing,
    /// then add `PLAYHEAD_REDIFF_PROVIDER_DECODE_CHECK=1` to every test target's
    /// `EnvironmentVariables` dict in the produced `.xctestrun`, and run
    /// `test-without-building -xctestrun <patched> -only-testing:'PlayheadTests/
    /// RediffTreatmentHarnessWiringTests'`.
    /// The `.enabled(if:)` trait (R1) makes a not-enabled run report SKIPPED —
    /// a silent guard-return here previously looked identical to a real pass,
    /// which is a verification trap for a measurement harness.
    @Test(
        "fresh B-side provider decodes a staged .fresh.mp3 to non-empty 16 kHz PCM",
        .enabled(if: ProcessInfo.processInfo.environment["PLAYHEAD_REDIFF_PROVIDER_DECODE_CHECK"] == "1")
    )
    func freshBSideProviderDecodesStagedAudio() async throws {
        let repoRoot = PipelineDumpManifestLoader.repoRoot()
        // R3: the provider's OWN directory resolution, so this check cannot
        // drift from the decode path's lookup.
        let audioDir = CorpusFreshBSideProvider.audioDirectory(repoRoot: repoRoot)
        let freshFiles = (try? FileManager.default.contentsOfDirectory(atPath: audioDir.path))?
            .filter { $0.hasSuffix(".fresh.mp3") }
            .sorted() ?? []
        guard let firstFresh = freshFiles.first else {
            Issue.record("decode-check enabled but no .fresh.mp3 is staged under \(audioDir.path)")
            return
        }
        let assetId = String(firstFresh.dropLast(".fresh.mp3".count))

        let provider = CorpusFreshBSideProvider(repoRoot: repoRoot)
        let bSide = await provider.refetchedBSideMono16kHz(assetId: assetId)
        let decoded = try #require(
            bSide,
            "provider returned nil for a staged fresh B-side (\(assetId))"
        )
        // At least a second of 16 kHz PCM — a real episode is far longer.
        #expect(decoded.count > 16_000)
        // A non-rotated id (no staged .fresh.mp3) is a correct no-op.
        let absent = await provider.refetchedBSideMono16kHz(
            assetId: "definitely-not-a-real-staged-episode-id"
        )
        #expect(absent == nil)
    }

    /// R3 (R4/R5-hardened): the treatment lane's pre-flight coverage guard must
    /// classify staged vs unstaged entries by the provider's EXACT acceptance
    /// predicate — URL derivation AND the regular-unaliased-non-empty-file anchor. A
    /// zero-USABLE treatment run silently equals the baseline, so
    /// `captureRediffTreatmentDump` fails fast on `unstaged == all`; a
    /// symlinked staging the decode path refuses must count as unstaged here
    /// (R3's bare `fileExists` counted it staged — the exact drift the guard
    /// was built to rule out). Pinned hermetically against a temp directory
    /// (real `FileManager` semantics, no corpus dependency).
    @Test("treatment pre-flight classifies staged fresh B-sides by the provider's acceptance predicate")
    func preflightClassificationMatchesProviderPredicate() async throws {
        let audioDir = try makeTempDir(prefix: "RediffPreflight")
        func entry(_ id: String) -> PipelineDumpSnapshotEntry {
            PipelineDumpSnapshotEntry(
                show: "Show",
                showSlug: "show",
                episodeId: id,
                publishDate: "2026-07-01",
                audioPath: "TestFixtures/Corpus/Audio/\(id).mp3",
                sha256: nil
            )
        }
        // Stage three of eight (two regular files + one hardlink, R6); the
        // plain `.mp3` A-side must NOT count as staged,
        // and every degenerate staging the decode path refuses must classify
        // unstaged + irregular: a symlink to a regular file (ep-d), a DANGLING
        // symlink (ep-e — R5: `fileExists` FOLLOWS symlinks, so a bare
        // existence check misfiles this as absent), a zero-byte file (ep-f —
        // R5: undecodable, so an empty-file staging must not pass the
        // pre-flight guard), and a directory named `*.fresh.mp3` (ep-g).
        try Data("b-side".utf8).write(
            to: CorpusFreshBSideProvider.freshURL(assetId: "ep-a", audioDirectory: audioDir)
        )
        try Data("a-side".utf8).write(
            to: audioDir.appendingPathComponent("ep-b.mp3", isDirectory: false)
        )
        try Data("b-side".utf8).write(
            to: CorpusFreshBSideProvider.freshURL(assetId: "ep-c", audioDirectory: audioDir)
        )
        let symlinkTarget = audioDir.appendingPathComponent("target.mp3", isDirectory: false)
        try Data("b-side".utf8).write(to: symlinkTarget)
        try FileManager.default.createSymbolicLink(
            at: CorpusFreshBSideProvider.freshURL(assetId: "ep-d", audioDirectory: audioDir),
            withDestinationURL: symlinkTarget
        )
        try FileManager.default.createSymbolicLink(
            at: CorpusFreshBSideProvider.freshURL(assetId: "ep-e", audioDirectory: audioDir),
            withDestinationURL: audioDir.appendingPathComponent(
                "does-not-exist.mp3", isDirectory: false
            )
        )
        try Data().write(
            to: CorpusFreshBSideProvider.freshURL(assetId: "ep-f", audioDirectory: audioDir)
        )
        try FileManager.default.createDirectory(
            at: CorpusFreshBSideProvider.freshURL(assetId: "ep-g", audioDirectory: audioDir),
            withIntermediateDirectories: true
        )
        // R6: a HARDLINK to a regular non-empty file (ep-h) completes the probe
        // state matrix — indistinguishable from a regular file at the
        // filesystem level (`isRegularFile` true, `isSymbolicLink` false), so
        // both the probe and the decode path accept it as staged.
        try FileManager.default.linkItem(
            at: symlinkTarget,
            to: CorpusFreshBSideProvider.freshURL(assetId: "ep-h", audioDirectory: audioDir)
        )

        let classification = classifyRediffTreatmentBSides(
            entries: [
                entry("ep-a"), entry("ep-b"), entry("ep-c"), entry("ep-d"),
                entry("ep-e"), entry("ep-f"), entry("ep-g"), entry("ep-h"),
            ],
            audioDirectory: audioDir
        )
        #expect(classification.unstagedEpisodeIds == ["ep-b", "ep-d", "ep-e", "ep-f", "ep-g"])
        #expect(classification.irregularEpisodeIds == ["ep-d", "ep-e", "ep-f", "ep-g"])

        // The shared probe itself, all three verdicts.
        #expect(
            CorpusFreshBSideProvider.probeStagedFresh(assetId: "ep-a", audioDirectory: audioDir)
                == .staged
        )
        #expect(
            CorpusFreshBSideProvider.probeStagedFresh(assetId: "ep-b", audioDirectory: audioDir)
                == .absent
        )
        #expect(
            CorpusFreshBSideProvider.probeStagedFresh(assetId: "ep-d", audioDirectory: audioDir)
                == .irregular
        )
        #expect(
            CorpusFreshBSideProvider.probeStagedFresh(assetId: "ep-e", audioDirectory: audioDir)
                == .irregular
        )
        #expect(
            CorpusFreshBSideProvider.probeStagedFresh(assetId: "ep-f", audioDirectory: audioDir)
                == .irregular
        )
        #expect(
            CorpusFreshBSideProvider.probeStagedFresh(assetId: "ep-g", audioDirectory: audioDir)
                == .irregular
        )
        #expect(
            CorpusFreshBSideProvider.probeStagedFresh(assetId: "ep-h", audioDirectory: audioDir)
                == .staged
        )

        // Zero-staged is the guard's hard-fail condition: EVERY id unstaged.
        let emptyDir = try makeTempDir(prefix: "RediffPreflightEmpty")
        let allUnstaged = classifyRediffTreatmentBSides(
            entries: [entry("ep-a"), entry("ep-b")],
            audioDirectory: emptyDir
        )
        #expect(allUnstaged.unstagedEpisodeIds == ["ep-a", "ep-b"])
        #expect(allUnstaged.irregularEpisodeIds.isEmpty)
    }

    /// R6: pin the probe's read-error mapping directly. The filesystem states
    /// are covered above; inducing a real EACCES/EIO on disk is root- and
    /// platform-dependent, so this pins the pure classifier instead: ONLY the
    /// no-such-file family may read as `.absent` (the silent non-rotated
    /// no-op) — every other read failure must fail CLOSED to `.irregular`, or
    /// an unreadable-but-staged file silently counts as a non-rotated episode
    /// (the exact silent-no-lift shape the pre-flight guard exists to surface).
    @Test("probe read-error classifier: no-such-file family is absent; everything else fails closed to irregular")
    func probeReadErrorClassifierFailsClosed() {
        typealias Probe = CorpusFreshBSideProvider
        // No-such-file family → .absent.
        #expect(Probe.classifyProbeReadError(CocoaError(.fileReadNoSuchFile)) == .absent)
        #expect(Probe.classifyProbeReadError(CocoaError(.fileNoSuchFile)) == .absent)
        #expect(Probe.classifyProbeReadError(
            NSError(domain: NSPOSIXErrorDomain, code: Int(ENOENT))
        ) == .absent)
        #expect(Probe.classifyProbeReadError(
            NSError(domain: NSPOSIXErrorDomain, code: Int(ENOTDIR))
        ) == .absent)
        // Foundation wraps the POSIX cause under NSUnderlyingErrorKey — the
        // chain walk must find it.
        #expect(Probe.classifyProbeReadError(NSError(
            domain: NSCocoaErrorDomain,
            code: NSFileReadUnknownError,
            userInfo: [NSUnderlyingErrorKey: NSError(domain: NSPOSIXErrorDomain, code: Int(ENOENT))]
        )) == .absent)
        // Everything else — permission, I/O, unknown — fails closed.
        #expect(Probe.classifyProbeReadError(CocoaError(.fileReadNoPermission)) == .irregular)
        #expect(Probe.classifyProbeReadError(
            NSError(domain: NSPOSIXErrorDomain, code: Int(EACCES))
        ) == .irregular)
        #expect(Probe.classifyProbeReadError(
            NSError(domain: NSPOSIXErrorDomain, code: Int(EIO))
        ) == .irregular)
        #expect(Probe.classifyProbeReadError(NSError(
            domain: NSCocoaErrorDomain,
            code: NSFileReadUnknownError,
            userInfo: [NSUnderlyingErrorKey: NSError(
                domain: NSPOSIXErrorDomain, code: Int(EACCES)
            )]
        )) == .irregular)
    }
}
