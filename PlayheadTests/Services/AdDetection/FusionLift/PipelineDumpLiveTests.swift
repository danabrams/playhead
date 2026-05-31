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
//         "episodeDurationSeconds": 1234.5,
//         "candidateDecodedSpans": <int>,
//         "adWindows": [
//           { "startTime": 123.4, "endTime": 234.5,
//             "skipConfidence": 0.78,
//             "decisionState": "...",
//             "eligibilityGate": "...",
//             "promotionTrack": null }, ...
//         ]
//       }, ...
//     ],
//     "summary": { "totalEpisodes": 9, "totalAdWindows": N, "perShow": {...} }
//   }
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
        case decodeFailed(URL, Error)

        var description: String {
            switch self {
            case .manifestMissing(let url):
                return "Snapshot manifest not found at \(url.path)"
            case .decodeFailed(let url, let err):
                return "Failed to decode \(url.lastPathComponent): \(err.localizedDescription)"
            }
        }
    }

    /// Decode the manifest at `url`. Pure: no file-existence side effects
    /// beyond the read.
    static func decode(at url: URL) throws -> [PipelineDumpSnapshotEntry] {
        guard FileManager.default.fileExists(atPath: url.path) else {
            throw LoadError.manifestMissing(url)
        }
        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            throw LoadError.decodeFailed(url, error)
        }
        do {
            return try JSONDecoder().decode([PipelineDumpSnapshotEntry].self, from: data)
        } catch {
            throw LoadError.decodeFailed(url, error)
        }
    }

    /// Load + decode the manifest from its canonical repo-root location.
    static func load(repoRoot: URL) throws -> [PipelineDumpSnapshotEntry] {
        try decode(at: manifestURL(repoRoot: repoRoot))
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
}

/// Per-episode dump row.
private struct DumpEpisode: Encodable {
    let episodeId: String
    let showSlug: String
    let publishDate: String
    let episodeDurationSeconds: Double
    let candidateDecodedSpans: Int
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
        let audioURL = repoRoot.appendingPathComponent(entry.audioPath, isDirectory: false)
        guard FileManager.default.fileExists(atPath: audioURL.path) else {
            throw PipelineDumpRunError(
                severity: .soft,
                reason: "audio file not staged at \(audioURL.path)"
            )
        }
        guard let localURL = LocalAudioURL(audioURL) else {
            throw PipelineDumpRunError(
                severity: .hard,
                reason: "audio at \(audioURL.path) is not a file URL"
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
                repoRoot: repoRoot
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

        let dumpWindows: [DumpAdWindow] = windows
            .sorted { $0.startTime < $1.startTime }
            .map {
                DumpAdWindow(
                    startTime: $0.startTime,
                    endTime: $0.endTime,
                    skipConfidence: $0.confidence,
                    decisionState: $0.decisionState,
                    eligibilityGate: $0.eligibilityGate,
                    promotionTrack: nil
                )
            }

        return DumpEpisode(
            episodeId: episodeId,
            showSlug: entry.showSlug,
            publishDate: entry.publishDate,
            episodeDurationSeconds: episodeDuration,
            candidateDecodedSpans: decodedSpanCount,
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
        let repoRoot = PipelineDumpManifestLoader.repoRoot()
        let entries = try PipelineDumpManifestLoader.load(repoRoot: repoRoot)

        #expect(entries.count == 9, "manifest must list exactly the 9 NEW snapshot episodes (got \(entries.count))")

        // Each entry must carry the four fields the harness reads.
        for entry in entries {
            #expect(!entry.episodeId.isEmpty, "episodeId missing")
            #expect(!entry.showSlug.isEmpty, "showSlug missing for \(entry.episodeId)")
            #expect(!entry.publishDate.isEmpty, "publishDate missing for \(entry.episodeId)")
            #expect(
                entry.audioPath.hasPrefix("TestFixtures/Corpus/Audio/"),
                "audioPath should be repo-root-relative under TestFixtures/Corpus/Audio/ — got \(entry.audioPath)"
            )
        }

        // No duplicates — the dump would otherwise double-write the same
        // episode under a single store/asset id.
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
    }
}
