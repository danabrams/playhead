// PipelineDumpLiveTests.swift
// One-shot exploratory pipeline-dump harness for the 9 NEW corpus episodes
// snapshotted under `TestFixtures/Corpus/Snapshots/manifest.json`. This is
// NOT an A/B and NOT a lift measurement — it runs the PRODUCTION
// `AdDetectionService.runBackfill` once per episode under the shipped
// `AdDetectionConfig.default` (every activation flag off, `fmBackfillMode:
// .full`, `chapterSignalMode: .off`) and dumps the persisted `AdWindow`s so
// the orchestrator can cross-validate them against the drafter's candidate
// spans.
//
// CONTRACT (mirrors the sibling live harnesses in this directory):
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
import Foundation
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

/// Root dump payload.
private struct DumpPayload: Encodable {
    let config: String
    let runUtc: String
    let episodes: [DumpEpisode]
    let summary: DumpSummary
}

// MARK: - Live test

final class PipelineDumpLiveTests: XCTestCase {

    /// The dump test is opt-in via env var, matching the sibling live
    /// harnesses. Default `PlayheadFastTests` does NOT set it, so the test
    /// body is a no-op on Cmd-U.
    private static var dumpEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_PIPELINE_DUMP"] == "1"
    }

    override func setUp() async throws {
        try await super.setUp()
        try XCTSkipUnless(
            Self.dumpEnabled,
            """
            One-shot pipeline dump on the 9 NEW corpus episodes is opt-in \
            and SLOW (~9 full-FM passes on Mac Catalyst). Set \
            PLAYHEAD_PIPELINE_DUMP=1 in the test plan env vars and run on \
            Mac Catalyst (or an iOS 26 device) with Apple Intelligence \
            enabled and corpus audio + transcripts staged. See the file \
            header for the full invocation recipe.
            """
        )
    }

    // MARK: - Entry

    func testProductionPipelineDumpOnNewEpisodes() async throws {
        guard #available(iOS 26.0, *) else {
            throw XCTSkip("Live FM BackfillJobRunner requires iOS 26+ / FoundationModels.")
        }

        let repoRoot = PipelineDumpManifestLoader.repoRoot()

        // Read the 9 NEW snapshot entries from the manifest. Missing or
        // malformed manifest is a HARD failure — the snapshot pipeline
        // should have produced it before this dump runs.
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
                let row = try await runSingleEpisode(entry: entry, repoRoot: repoRoot)
                dumpEpisodes.append(row)
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
            config: "production .default (all xsdz flags off, fmBackfillMode .full)",
            runUtc: runUtc,
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
            "playhead-dogfood-diagnostics-pipeline-dump-new9.json",
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
                PLAYHEAD_PIPELINE_DUMP=1 was set but no episodes scored — \
                every manifest entry landed in `skipped` because audio or the \
                transcript sidecar is not staged. See the file header.
                """
            )
        }
    }

    // MARK: - Single-episode run

    /// Run `runBackfill` once for a single manifest entry under production
    /// `.default` config, then return a populated `DumpEpisode` row. Mirrors
    /// the sibling harness's `runArm(...)` setup (fresh store, asset,
    /// transcript chunks, feature windows, seeded planner state, live FM
    /// runner factory) — the only differences are: (a) one config (the
    /// shipped `.default`) instead of a per-arm config matrix; (b) the
    /// optional `FragilityDiagnosticObserver` is attached so we can report
    /// the decoded-span count.
    @available(iOS 26.0, *)
    private func runSingleEpisode(
        entry: PipelineDumpSnapshotEntry,
        repoRoot: URL
    ) async throws -> DumpEpisode {
        let episodeId = entry.episodeId

        // Resolve audio file from the manifest's repo-root-relative path.
        let audioURL = try PipelineDumpManifestLoader.audioURL(for: entry, repoRoot: repoRoot)
        guard FileManager.default.fileExists(atPath: audioURL.path) else {
            throw PipelineDumpRunError(
                severity: .soft,
                reason: "audio file not staged at \(audioURL.path)"
            )
        }
        guard let audioValues = try? audioURL.resourceValues(
            forKeys: [.isRegularFileKey, .isSymbolicLinkKey]
        ), audioValues.isRegularFile == true, audioValues.isSymbolicLink != true else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "audio at \(audioURL.path) is not a regular unaliased file"
            )
        }
        guard let localURL = LocalAudioURL(audioURL) else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "audio at \(audioURL.path) is not a file URL"
            )
        }
        guard let expectedHash = entry.sha256 else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "snapshot manifest lacks sha256 for \(episodeId)"
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
        let transcript: [TranscriptChunk]
        do {
            transcript = try CorpusTranscriptLoader.load(
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
        // windows read back under the id we report on.
        let assetId = episodeId
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "pipeline-dump-fp-\(episodeId)",
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

        // Production config: the shipped `.default`. No flags toggled. No
        // narrowing override. We hold a reference so the hermetic test can
        // verify identity with `AdDetectionConfig.default`.
        let config = AdDetectionConfig.default

        // Behavior-neutral diagnostic observer: counts every decoded span
        // so we can report `candidateDecodedSpans`. The observer is the
        // existing `FragilityDiagnosticObserver` (already part of the
        // shipped app); it NEVER feeds back into the decision path. See
        // the observer's contract in `FragilityDiagnosticObserver.swift`.
        let observer = FragilityDiagnosticObserver()

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            backfillJobRunnerFactory: Self.makeLiveRunnerFactory(),
            canUseFoundationModelsProvider: { true }, // avoid silent FM demotion
            fragilityDiagnosticObserver: observer,
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

        // SCORED RUN: a single backfill under the shipped `.default`.
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
        // map when `config.spanFinalizerEnabled == false` (the production
        // .default this dump runs under — see `PipelineDumpHermeticTests`'s
        // `productionConfigStateIsHeld` pin). Each map entry surfaces in
        // the dump's `spanFinalizerConstraintsFired` key when present and
        // non-empty; nil/missing when the underlying lookup yielded no
        // trace, matching the default-encoder convention for nil optionals.
        let spanFinalizerConstraintsByWindowId =
            await service.spanFinalizerConstraintsByWindowIdForTesting()

        let dumpWindows: [DumpAdWindow] = windows
            .sorted { $0.startTime < $1.startTime }
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
                    spanFinalizerConstraintsFired: spanFinalizerConstraints
                )
            }

        let dumpDecodedSpans: [DumpDecodedSpan] = decodedSpanRows
            .sorted { $0.spanStart < $1.spanStart }
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

        return DumpEpisode(
            episodeId: episodeId,
            audioFingerprint: audioFingerprint,
            showSlug: entry.showSlug,
            publishDate: entry.publishDate,
            episodeDurationSeconds: episodeDuration,
            candidateDecodedSpans: decodedSpanCount,
            candidateDecodedSpanList: dumpDecodedSpans,
            adWindows: dumpWindows
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
            let redactor = (try? PromptRedactor.loadDefault()) ?? .noop
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
                // No `runtime:` argument ⇒ FoundationModelClassifier uses
                // its live runtime against `SystemLanguageModel.default`.
                classifier: FoundationModelClassifier(),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: makeTestScanCohortJSON(promptLabel: "pipeline-dump-new9"),
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
/// 9 entries, and (b) the dump test uses literally `AdDetectionConfig.default`
/// (no shadow copy, no flag drift). These run in `PlayheadFastTests` on the
/// simulator with NO env var — they neither read audio nor hit FM.
struct PipelineDumpHermeticTests {

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
            spanFinalizerConstraintsFired: nil
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
            spanFinalizerConstraintsFired: nil
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
            spanFinalizerConstraintsFired: nil
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
            spanFinalizerConstraintsFired: nil
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
            spanFinalizerConstraintsFired: nil
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
        // playhead-p56a: spanFinalizerConstraintsFired: nil → key absent
        // (matches the production .default OFF state this dump runs under).
        #expect(!parsed.keys.contains("spanFinalizerConstraintsFired"))
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
            spanFinalizerConstraintsFired: nil
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
            spanFinalizerConstraintsFired: trace
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
}
