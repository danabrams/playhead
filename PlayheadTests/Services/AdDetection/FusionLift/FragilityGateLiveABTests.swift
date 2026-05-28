// FragilityGateLiveABTests.swift
// playhead-xsdz.7 live A/B — env-gated Mac Catalyst A/B test that runs a 2-arm
// comparison of the Evidence-Fragility PRECISION GATE through the REAL
// `AdDetectionService.runBackfill` (fmBackfillMode: .full), scored against the
// 12-episode dogfood golden corpus, so a single Catalyst run measures whether
// the xsdz.7 fragility gate actually buys precision live (and at what recall
// cost) end-to-end.
//
// Sibling of `LexicalScorerLiveABTests`: same live runner factory, same
// planner seeding, same store/feature/transcript setup, same scoring helpers.
// The difference is the program under test — here the ONLY toggle is the
// xsdz.7 `AdDetectionConfig.evidenceFragilityPenaltyEnabled` boolean.
//
// What this measures (and why it is not a meaningless zero)
// ---------------------------------------------------------
// xsdz.7 is the Evidence-Fragility precision gate (`applyFragilityPenalty`): a
// pure, deterministic, post-fusion SOFT penalty on `skipConfidence` for spans
// whose evidence geometry looks BRITTLE (one dominant channel, a thin margin
// over the auto-skip threshold, few distinct evidence families) — the hallmark
// of a false positive that cleared the auto-skip threshold on thin support.
// When `evidenceFragilityPenaltyEnabled` is `false` (the production default),
// `applyFragilityPenalty` returns `skipConfidence` UNCHANGED; when `true` and a
// span is fragile, the confidence is multiplied by `fragilityPenalty` (0.85),
// which can drop a span that *just* cleared its threshold back below it.
//
// The penalty only changes the PERSISTED ad windows through the real FM
// scan → fusion ledger → decision path → `store.insertAdWindows` pipeline, and
// that path only runs when `effectiveFMBackfillMode != .off`. THEREFORE both
// arms run with `fmBackfillMode: .full` (real FM ad scanning feeding the fusion
// ledger); the ONLY thing that varies is the one fragility flag. The two arms
// (`FragilityGateArm`):
//   * baseline  — current main PRODUCTION state. `evidenceFragilityPenaltyEnabled:
//                 false`. Snap (xsdz.2/.3) ON (`NarrowingConfig.default`), every
//                 off-by-default evidence channel FALSE, chapter signal OFF.
//   * treatment — identical to baseline EXCEPT `evidenceFragilityPenaltyEnabled:
//                 true` (production-default `fragilityThreshold` 2.0 /
//                 `fragilityPenalty` 0.85).
// Everything else (store / asset / feature windows / transcript chunks /
// planner state / FM runner / NarrowingConfig) is IDENTICAL across the two
// arms. The one-field isolation is pinned hermetically by
// `FragilityGateArmConfigTests` (runs on the sim, no env var).
//
// PRECONDITION (the FM narrowing path must run like production): the live
// targeted-phase narrowing inside `BackfillJobRunner` is guarded by
// `plan.policy == .targetedWithAudit`. A fresh store is cold-start
// (`observedEpisodeCount == 0`) ⇒ `fullCoverage` ⇒ the targeted phases never
// narrow. So each arm SEEDS the planner state into the warmed
// `targetedWithAudit` regime before the scored run (identical to the lexical
// harness). This is symmetric across arms (same podcastId per arm store), so it
// cannot bias the comparison; the only between-arm difference remains the
// fragility flag. The fragility gate itself is policy-independent (it runs on
// the post-fusion decision path regardless of coverage policy), so the seeding
// is conservative for that gate — it exists to keep the FM scan production-like.
//
// How the toggle reaches the pipeline
//   * `evidenceFragilityPenaltyEnabled` on the per-arm `AdDetectionConfig`
//     (no production change — already togglable; xsdz.7 added the flag, OFF by
//     default). The decision path reads it via `config.applyFragilityPenalty`.
//
// Gating (mirrors `LexicalScorerLiveABTests`):
//   `PLAYHEAD_FRAGILITY_AB=1` MUST be set in the test process environment. The
//   default `PlayheadFastTests` plan does NOT set it, so the test body is a
//   no-op on Cmd-U (via `XCTSkipUnless`) — no FM, no audio dependency. The live
//   FM `BackfillJobRunner` also requires iOS 26+ (`#available`), so an older
//   runtime skips too.
//
// Cost (READ before running): this runs the REAL `runBackfill` with full FM ad
// scanning once per arm × 2 arms × N dogfood episodes (~24 full-FM passes for
// the 12-episode dogfood set) — on the order of an hour on Mac Catalyst. It is
// an ORCHESTRATOR-run step, NOT a green gate. To invoke:
//
//   1. Stage corpus audio under `TestFixtures/Corpus/Audio/<episode_id>.<ext>`
//      and whisper transcripts under `TestFixtures/Corpus/Transcripts/`.
//   2. Enable Apple Intelligence on the host Mac (FM classifier is on-device).
//   3. Run on Mac Catalyst with the env var set:
//        xcodebuild test \
//          -scheme Playhead -testPlan PlayheadFastTests \
//          -destination 'platform=macOS,variant=Mac Catalyst' \
//          -only-testing:'PlayheadTests/FragilityGateLiveABTests' \
//          PLAYHEAD_FRAGILITY_AB=1
//   4. Read the printed lift table; the per-run JSON dump lands at the repo
//      root as `playhead-dogfood-diagnostics-fragility-ab.json` (git-ignored —
//      matches the `playhead-dogfood-diagnostics-*.json` pattern in
//      `.gitignore`).
//
// Hermetic helpers (`FragilityGateArm`, `FragilityGateArmConfig`,
// `FusionLiftModeAccumulator`, `FusionLiftReport`) live in
// `FusionLiftHarnessSupport.swift` and are unit-tested on the simulator by
// `FragilityGateArmConfigTests` + `FusionLiftHarnessSupportTests`. Scoring
// reuses Phase A's `FusionLiftScoring.swift` (no reimplementation).

import AVFoundation
import Foundation
import XCTest
@testable import Playhead

final class FragilityGateLiveABTests: XCTestCase {

    /// Gate every test on the A/B env var. The default scheme does not set
    /// it, so the test body is a no-op on Cmd-U / the default sim suite.
    private static var abEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_FRAGILITY_AB"] == "1"
    }

    override func setUp() async throws {
        try await super.setUp()
        try XCTSkipUnless(
            Self.abEnabled,
            """
            Evidence-Fragility gate A/B is opt-in and SLOW (~24 full-FM passes: \
            1 scored pass × 2 arms × 12 episodes). Set PLAYHEAD_FRAGILITY_AB=1 \
            in the test plan env vars and run on Mac Catalyst (or an iOS 26 \
            device) with Apple Intelligence enabled and corpus audio + \
            transcripts staged. See the file header for the full invocation \
            recipe.
            """
        )
    }

    // MARK: - A/B entry

    func testFragilityGateAcrossDogfoodCorpus() async throws {
        guard #available(iOS 26.0, *) else {
            throw XCTSkip("Live FM BackfillJobRunner requires iOS 26+ / FoundationModels.")
        }

        let loader = CorpusAnnotationLoader()

        let annotationURLs: [URL]
        do {
            annotationURLs = try loader.annotationFileURLs()
        } catch {
            throw XCTSkip("corpus annotations dir not present: \(error)")
        }
        try XCTSkipIf(annotationURLs.isEmpty, "no corpus annotations staged")

        // One accumulator per arm. Each arm runs the SAME
        // store/features/transcripts/planner-seeding/FM-mode `.full` per
        // episode; only the fragility flag varies.
        var accumulators: [FragilityGateArm: FusionLiftModeAccumulator] = Dictionary(
            uniqueKeysWithValues: FragilityGateArm.allCases.map { ($0, FusionLiftModeAccumulator()) }
        )

        var scored: [String] = []
        var skipped: [(episodeId: String, reason: String)] = []
        var failed: [(episodeId: String, reason: String)] = []

        for url in annotationURLs {
            let episodeId = url.deletingPathExtension().lastPathComponent

            // Resolve annotation (ground truth).
            let annotation: CorpusAnnotation
            do {
                annotation = try loader.decode(at: url)
            } catch {
                failed.append((episodeId, "annotation decode failed: \(error.localizedDescription)"))
                continue
            }

            // Resolve audio.
            let audioURL: URL
            do {
                audioURL = try loader.audioFileURL(for: annotation)
            } catch {
                skipped.append((
                    episodeId,
                    "audio file not staged under \(loader.audioDirectoryURL.path)"
                ))
                continue
            }

            // Audio-fingerprint check (mirrors the corpus loader's own
            // verification): a mismatch means a different cut than the
            // annotation references — scoring that would compare predictions
            // against the wrong ground truth.
            do {
                try loader.verify(audioFingerprintFor: annotation, jsonURL: url)
            } catch {
                failed.append((episodeId, "audio fingerprint check failed: \(error.localizedDescription)"))
                continue
            }

            // Resolve transcript (whisper sidecar).
            let transcript: [TranscriptChunk]
            do {
                transcript = try CorpusTranscriptLoader.load(
                    episodeId: episodeId,
                    repoRoot: loader.repoRoot
                )
            } catch {
                failed.append((episodeId, "transcript decode failed: \(error.localizedDescription)"))
                continue
            }
            guard !transcript.isEmpty else {
                skipped.append((episodeId, "transcript sidecar empty/absent"))
                continue
            }

            // Run BOTH arms for this episode. Each arm gets its own fresh
            // store / asset / features / transcript / seeded planner state, so
            // the only between-arm difference is the fragility flag. An arm
            // failure aborts the whole episode (so no arm is scored on a
            // partial episode that would bias the comparison).
            do {
                for arm in FragilityGateArm.allCases {
                    let windows = try await runArm(
                        arm: arm,
                        episodeId: episodeId,
                        annotation: annotation,
                        audioURL: audioURL,
                        transcript: transcript
                    )
                    accumulators[arm]?.addEpisode(
                        annotationWindows: annotation.adWindows,
                        adWindows: windows,
                        podcastId: annotation.showName,
                        episodeId: episodeId
                    )
                }
                scored.append(episodeId)
            } catch let error as FragilityABRunError {
                failed.append((episodeId, error.reason))
            } catch {
                failed.append((episodeId, "arm run failed: \(error.localizedDescription)"))
            }
        }

        // Build + print + dump the lift report (off = baseline, enabled =
        // treatment). Reuses the chapter A/B's `FusionLiftReport` verbatim —
        // same 2-arm shape, same fields the bead asks for.
        let report = FusionLiftReport(
            episodeCount: scored.count,
            off: accumulators[.baseline] ?? FusionLiftModeAccumulator(),
            enabled: accumulators[.treatment] ?? FusionLiftModeAccumulator()
        )
        print("=== Evidence-Fragility Gate Live A/B (xsdz.7) ===")
        print("(report rows: off = baseline [fragility OFF], enabled = treatment [fragility ON])")
        print(report.table())
        print("""
        scored=\(scored.count): \(scored.sorted().joined(separator: ", "))
        skipped (audio/transcript missing)=\(skipped.count): \(skipped.map(\.episodeId).sorted().joined(separator: ", "))
        failed=\(failed.count): \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))
        """)

        // Git-ignored JSON dump at repo root (matches the
        // `playhead-dogfood-diagnostics-*.json` gitignored pattern).
        let dumpURL = loader.repoRoot.appendingPathComponent(
            "playhead-dogfood-diagnostics-fragility-ab.json",
            isDirectory: false
        )
        try report.jsonData().write(to: dumpURL, options: .atomic)
        print("Fragility A/B summary JSON: \(dumpURL.path) (git-ignored)")

        // Fail loud on hard failures; fail loud if nothing scored (env was set
        // but every episode landed in skipped — audio/transcript not staged).
        // Otherwise the A/B is informational (no pinned lift number — this is
        // an orchestrator measurement step, not a green gate).
        if !failed.isEmpty {
            XCTFail("Hard failures: \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))")
        } else if scored.isEmpty {
            XCTFail(
                """
                PLAYHEAD_FRAGILITY_AB=1 was set but no episodes scored — every \
                episode landed in `skipped` because audio or the transcript \
                sidecar is not staged. See the file header for the staging recipe.
                """
            )
        }
    }

    // MARK: - One arm

    /// Run the scored backfill once for a single episode in the requested arm
    /// and return the persisted ad windows. Every arm decodes the same audio,
    /// persists the same feature windows + transcript chunks, SEEDs the planner
    /// state into the warmed `targetedWithAudit` regime, and uses
    /// `NarrowingConfig.default` (snap ON — production state). The ONLY thing
    /// that differs is the arm's `evidenceFragilityPenaltyEnabled` flag. Always
    /// uses `fmBackfillMode: .full`.
    @available(iOS 26.0, *)
    private func runArm(
        arm: FragilityGateArm,
        episodeId: String,
        annotation: CorpusAnnotation,
        audioURL: URL,
        transcript: [TranscriptChunk]
    ) async throws -> [AdWindow] {
        guard let localURL = LocalAudioURL(audioURL) else {
            throw FragilityABRunError(reason: "audio at \(audioURL.path) is not a file URL")
        }

        // Fresh store per arm so the two runs never observe each other's
        // persisted windows or planner state.
        let storeDir = try makeTempDir(prefix: "FragilityGateLiveAB-\(arm.rawValue)")
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        // Asset row. `analysisAssetId == episodeId` so the scored ad windows
        // read back under the id the accumulator buckets on.
        let assetId = episodeId
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fragility-gate-fp-\(episodeId)",
            weakFingerprint: nil,
            sourceURL: localURL.absoluteString,
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        // Re-stamp transcript chunks onto this asset id (defensive in case the
        // loader convention changes).
        let chunks = transcript.map { rebindChunk($0, to: assetId) }
        try await store.insertTranscriptChunks(chunks)

        // PERSIST FEATURE WINDOWS: decode → extract → persist. The
        // CoveragePlanner + acoustic-break snapping read these back from the
        // store during the FM phase, so they must be present for the FM scan
        // to behave like production.
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
            throw FragilityABRunError(reason: "feature extraction produced 0 windows for \(episodeId)")
        }
        try await store.insertFeatureWindows(featureWindows)

        // Episode duration from the decoded shard span (last shard end).
        let episodeDuration = shards.reduce(0.0) { max($0, $1.startTime + $1.duration) }

        // PRECONDITION — seed the planner into `targetedWithAudit` so the live
        // narrowing path runs (the production-like FM regime). Five full-rescan
        // observations with a 0.9 recall sample satisfy both
        // `observedEpisodeCount >= 5` and `stableRecall == true`. Symmetric
        // across arms, so it cannot bias the comparison.
        try await seedTargetedWithAuditPlannerState(
            store: store,
            podcastId: annotation.showName
        )

        // Per-arm config: the single source of the fragility toggle. Every
        // other field is held identical to production across both arms.
        let config = FragilityGateArmConfig.adDetectionConfig(for: arm)
        // Both arms use the production-default NarrowingConfig (snap ON).
        let narrowingConfig = FragilityGateArmConfig.narrowingConfig(for: arm)

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            // Inject a LIVE FM BackfillJobRunner with the production-default
            // narrowing config baked in (identical across arms). Without a
            // runner factory the FM scan returns `.skipped` and there is
            // nothing for the fragility gate to influence.
            backfillJobRunnerFactory: Self.makeLiveRunnerFactory(narrowingConfig: narrowingConfig),
            canUseFoundationModelsProvider: { true }, // avoid silent FM demotion
            approvedCohortRegistry: nil // avoid cohort-gated FM demotion
        )

        // GUARD #1 (FM-mode gating): confirm cohort/FM gating did not silently
        // demote `.full` → `.off`. Else the FM phase never runs and the A/B is
        // a meaningless zero.
        let effectiveMode = await service.effectiveFMBackfillModeForTesting()
        XCTAssertEqual(
            effectiveMode,
            .full,
            "arm=\(arm.rawValue): effective FM mode demoted off .full — FM phase would not run"
        )

        // GUARD #2 (planner regime): confirm the seeded state lands the planner
        // in `targetedWithAudit` — the production-like FM regime. If a future
        // change regresses this to `fullCoverage`, the FM scan stops matching
        // production and the A/B no longer measures the live behavior.
        // Reproduce the planner's own decision so the guard fails loud.
        let seededState = try await store.fetchPodcastPlannerState(podcastId: annotation.showName)
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
            "arm=\(arm.rawValue): planner is not in targetedWithAudit (observed=\(seededContext.observedEpisodeCount) stableRecall=\(seededContext.stableRecall)) — the FM scan would not match production"
        )

        // SCORED RUN: a single backfill. With the planner warmed, the FM scan
        // runs the targeted phases under the production-default narrowing
        // config; the persisted ad windows are the scored output, with the
        // fragility gate active iff this is the treatment arm.
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: annotation.showName,
            episodeDuration: episodeDuration
        )

        return try await store.fetchAdWindows(assetId: assetId)
    }

    // MARK: - Planner-state seeding

    /// Seed `store`'s `PodcastPlannerState` for `podcastId` into the warmed
    /// `targetedWithAudit` regime: `observedEpisodeCount >= 5` and
    /// `stableRecall == true` (recall ring full with samples >= 0.85). Uses the
    /// production observation API so the seeded row is byte-identical to one a
    /// warmed podcast would have accrued. Identical to the lexical / chapter
    /// A/B's `seedTargetedWithAuditPlannerState` (same regime is required).
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
    /// `BackfillJobRunner` with `narrowingConfig` baked in (mirrors
    /// `LexicalScorerLiveABTests.makeLiveRunnerFactory`). For this A/B
    /// `narrowingConfig` is `NarrowingConfig.default` for BOTH arms — the
    /// narrowing config does NOT vary; the fragility flag does (and it lives on
    /// the `AdDetectionConfig`, not the runner). The `mode` argument is supplied
    /// by `AdDetectionService` (the effective FM mode, `.full` here).
    private static func makeLiveRunnerFactory(
        narrowingConfig: NarrowingConfig
    ) -> (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner) {
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
                // No `runtime:` argument ⇒ FoundationModelClassifier uses its
                // live runtime against `SystemLanguageModel.default`.
                classifier: FoundationModelClassifier(),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: makeTestScanCohortJSON(promptLabel: "fragility-gate-ab"),
                sensitiveRouter: router,
                permissiveClassifier: permissiveClassifierBox,
                // Production-default narrowing for both arms — the narrowing
                // config does not distinguish the fragility A/B's arms.
                narrowingConfig: narrowingConfig
            )
        }
    }

    // MARK: - Chunk rebinding

    /// Return a copy of `chunk` with its `analysisAssetId` re-pointed at
    /// `assetId`. Keeps every other field byte-identical so the derived
    /// transcript version is stable across arms.
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

// MARK: - Arm-run error

/// Raised when an arm cannot complete for a soft reason (non-file URL, zero
/// feature windows). Carries a one-line operator-facing reason that flows into
/// the test's failure summary.
private struct FragilityABRunError: Error {
    let reason: String
}
