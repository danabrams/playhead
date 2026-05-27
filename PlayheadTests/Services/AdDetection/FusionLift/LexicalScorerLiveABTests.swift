// LexicalScorerLiveABTests.swift
// playhead-xsdz.liveab — env-gated Mac Catalyst A/B test that runs a 4-arm
// PER-FEATURE SWEEP of the shipped lexical-scorer program through the REAL
// `AdDetectionService.runBackfill` (fmBackfillMode: .full), scored against the
// dogfood golden corpus, so a single Catalyst run isolates WHICH lexical-scorer
// feature regresses live ad detection.
//
// Why a sweep (history): the earlier 2-arm cumulative A/B (all-on vs all-off)
// showed the full program is WORSE end-to-end (spanF1 0.689 → 0.645, +2 FP,
// −1 TP). The cumulative number can't say which leg caused it, so this harness
// generalizes to four arms that toggle the two independent gates on their own.
//
// What this measures (and why it is not a meaningless zero)
// ---------------------------------------------------------
// The "lexical-scorer program" is the trio shipped under epic xsdz:
//   * xsdz.1 — the lexical-auto-ad qualified track. A vetted strong
//     co-occurrence (sponsor + promo code / URL CTA, negative guardrails
//     cleared) can auto-skip on its own through
//     `PromotionTrack.lexicalAutoAdQualified`. Gated by
//     `AdDetectionConfig.lexicalAutoAdQualifiedThreshold` (0.50 = on; a
//     value >= `autoSkipConfidenceThreshold` (0.80), here 2.0, makes the
//     track a no-op — it still contributes fusion mass but never promotes
//     alone).
//   * xsdz.2 — inward lexical-cue-cluster REGION TIGHTENING in
//     `TargetedWindowNarrower`: pull each targeted-phase window's outer
//     padding inward toward the ad-dense lexical core. Gated by
//     `NarrowingConfig.lexicalClusterSnapEnabled`.
//   * xsdz.3 — lexically-NOMINATED audit windows in the same narrower: spend
//     the FM's `scanRandomAuditWindows` budget on lexically-flagged ad-likely
//     regions instead of a random block. Gated by the SAME
//     `lexicalClusterSnapEnabled` flag.
//
// All three only influence the PERSISTED ad windows through the real FM
// scan → fusion ledger → `store.insertAdWindows` path, and that path only
// runs when `effectiveFMBackfillMode != .off`. THEREFORE every arm runs with
// `fmBackfillMode: .full` (real FM ad scanning feeding the fusion ledger);
// the ONLY things that vary are the two independent gates. There are two
// toggles, hence four arms (`LexicalScorerArm`):
//   * baseline   — threshold 2.0 (xsdz.1 off), snap false (xsdz.2/.3 off).
//   * xsdz1only  — threshold 0.50 (xsdz.1 on),  snap false (xsdz.2/.3 off).
//   * xsdz23only — threshold 2.0 (xsdz.1 off),  snap true  (xsdz.2/.3 on).
//   * alon       — threshold 0.50 (xsdz.1 on),  snap true  (xsdz.2/.3 on),
//                  i.e. the current main production defaults.
// xsdz.2 and xsdz.3 SHARE the `lexicalClusterSnapEnabled` flag and cannot be
// separated without new production plumbing, so they always move together as
// one arm leg. Everything else (store / asset / feature windows / transcript
// chunks / planner state / FM runner) is IDENTICAL across all four arms.
//
// PRECONDITION (the narrowing program must be OBSERVABLE in persisted windows):
//   xsdz.2/.3 reshape windows ONLY on the LIVE targeted-phase narrowing path
//   inside `BackfillJobRunner.narrowedInputs(...)` /
//   `recordUnrunRandomAuditEvents(...)`, which is guarded by
//   `plan.policy == .targetedWithAudit`. A fresh store is cold-start
//   (`observedEpisodeCount == 0`) ⇒ `fullCoverage` ⇒ the narrower never runs
//   on the targeted phases ⇒ the region/audit knobs can't fire and the A/B
//   reads a false zero on those two legs. So each arm SEEDS the planner state
//   into the warmed `targetedWithAudit` regime before the scored run (mirrors
//   `ChapterFusionLiftABTests`'s PRECONDITION A). This is symmetric across arms
//   (identical podcastId per arm store), so it cannot bias the comparison; the
//   only between-arm difference remains the lexical-scorer program. xsdz.1
//   (the fusion-mass auto-ad rule) is policy-independent and fires under any
//   plan, so the seeding is conservative, not load-bearing, for that leg.
//
// How the toggle reaches the pipeline
//   * xsdz.1: `lexicalAutoAdQualifiedThreshold` on the per-arm
//     `AdDetectionConfig` (no production change — already togglable).
//   * xsdz.2/.3: a per-arm `NarrowingConfig` injected into the
//     `BackfillJobRunner` this harness constructs in its live runner factory.
//     The runner threads it into every `TargetedWindowNarrower.narrow(...)`
//     call site (playhead-xsdz.liveab behavior-neutral plumbing; the param
//     defaults to `.default`, so production wiring is byte-identical).
//
// Gating (mirrors `ChapterFusionLiftABTests`):
//   `PLAYHEAD_LEXICAL_SCORER_AB=1` MUST be set in the test process
//   environment. The default `PlayheadFastTests` plan does NOT set it, so the
//   test body is a no-op on Cmd-U (via `XCTSkipUnless`) — no FM, no audio
//   dependency triggered. The live FM `BackfillJobRunner` also requires iOS
//   26+ (`#available`), so an older runtime skips too.
//
// Cost (READ before running): this runs the REAL `runBackfill` with full FM
// ad scanning once per arm × 4 arms × N dogfood episodes (~48 full-FM passes
// for the 12-episode dogfood set) — on the order of two hours on Mac Catalyst.
// It is an ORCHESTRATOR-run step, NOT a green gate. To invoke:
//
//   1. Stage corpus audio under `TestFixtures/Corpus/Audio/<episode_id>.<ext>`
//      and whisper transcripts under `TestFixtures/Corpus/Transcripts/`.
//   2. Enable Apple Intelligence on the host Mac (FM classifier is on-device).
//   3. Run on Mac Catalyst with the env var set:
//        xcodebuild test \
//          -scheme Playhead -testPlan PlayheadFastTests \
//          -destination 'platform=macOS,variant=Mac Catalyst' \
//          -only-testing:'PlayheadTests/LexicalScorerLiveABTests' \
//          PLAYHEAD_LEXICAL_SCORER_AB=1
//   4. Read the printed 4-row sweep table; the per-run JSON dump lands at the
//      repo root as `playhead-dogfood-diagnostics-lexical-scorer-sweep.json`
//      (git-ignored — matches the `playhead-dogfood-diagnostics-*.json`
//      pattern in `.gitignore`).
//
// Hermetic helpers (`LexicalScorerArm`, `LexicalScorerArmConfig`,
// `LexicalScorerSweepReport`, `FusionLiftModeAccumulator`,
// `FusionLiftTranscriptVersion`) live in
// `FusionLiftHarnessSupport.swift` and are unit-tested on the simulator by
// `FusionLiftHarnessSupportTests`. Scoring reuses Phase A's
// `FusionLiftScoring.swift` (no reimplementation).

import AVFoundation
import Foundation
import XCTest
@testable import Playhead

final class LexicalScorerLiveABTests: XCTestCase {

    /// Gate every test on the A/B env var. The default scheme does not set
    /// it, so the test body is a no-op on Cmd-U / the default sim suite.
    private static var abEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_LEXICAL_SCORER_AB"] == "1"
    }

    override func setUp() async throws {
        try await super.setUp()
        try XCTSkipUnless(
            Self.abEnabled,
            """
            Lexical-scorer per-feature sweep A/B is opt-in and SLOW (~48 \
            full-FM passes: 1 scored pass × 4 arms × 12 episodes). Set \
            PLAYHEAD_LEXICAL_SCORER_AB=1 in the test plan env vars and run on \
            Mac Catalyst (or an iOS 26 device) with Apple Intelligence enabled \
            and corpus audio + transcripts staged. See the file header for the \
            full invocation recipe.
            """
        )
    }

    // MARK: - A/B entry

    func testLexicalScorerPerFeatureSweepAcrossDogfoodCorpus() async throws {
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

        // One accumulator per sweep arm, keyed by arm. Each arm runs the SAME
        // store/features/transcripts/planner-seeding/FM-mode `.full` per
        // episode; only the two lexical-scorer toggles vary.
        var accumulators: [LexicalScorerArm: FusionLiftModeAccumulator] = Dictionary(
            uniqueKeysWithValues: LexicalScorerArm.allCases.map { ($0, FusionLiftModeAccumulator()) }
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

            // Run ALL FOUR arms for this episode. Each arm gets its own fresh
            // store / asset / features / transcript / seeded planner state, so
            // the only between-arm difference is the two lexical-scorer
            // toggles. An arm failure aborts the whole episode (so no arm is
            // scored on a partial episode that would bias the comparison).
            do {
                for arm in LexicalScorerArm.allCases {
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
            } catch let error as LexicalABRunError {
                failed.append((episodeId, error.reason))
            } catch {
                failed.append((episodeId, "arm run failed: \(error.localizedDescription)"))
            }
        }

        // Build + print + dump the per-feature sweep report.
        let report = LexicalScorerSweepReport(
            episodeCount: scored.count,
            accumulators: accumulators
        )
        print(report.table())
        print("""
        scored=\(scored.count): \(scored.sorted().joined(separator: ", "))
        skipped (audio/transcript missing)=\(skipped.count): \(skipped.map(\.episodeId).sorted().joined(separator: ", "))
        failed=\(failed.count): \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))
        """)

        // Git-ignored JSON dump at repo root (matches the
        // `playhead-dogfood-diagnostics-*.json` gitignored pattern).
        let dumpURL = loader.repoRoot.appendingPathComponent(
            "playhead-dogfood-diagnostics-lexical-scorer-sweep.json",
            isDirectory: false
        )
        try report.jsonData().write(to: dumpURL, options: .atomic)
        print("Lift summary JSON: \(dumpURL.path) (git-ignored)")

        // Fail loud on hard failures; fail loud if nothing scored (env was set
        // but every episode landed in skipped — audio/transcript not staged).
        // Otherwise the A/B is informational (no pinned lift number — this is
        // an orchestrator measurement step, not a green gate).
        if !failed.isEmpty {
            XCTFail("Hard failures: \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))")
        } else if scored.isEmpty {
            XCTFail(
                """
                PLAYHEAD_LEXICAL_SCORER_AB=1 was set but no episodes scored — \
                every episode landed in `skipped` because audio or the \
                transcript sidecar is not staged. See the file header for the \
                staging recipe.
                """
            )
        }
    }

    // MARK: - One arm

    /// Run the scored backfill once for a single episode in the requested arm
    /// and return the persisted ad windows. Every arm decodes the same audio,
    /// persists the same feature windows + transcript chunks, and SEEDs the
    /// planner state into the warmed `targetedWithAudit` regime (the only
    /// regime where xsdz.2/.3 reshape the FM scan). Only the two lexical-scorer
    /// toggles differ (the arm's `xsdz1On` threshold + `xsdz23On` snap flag).
    /// Always uses `fmBackfillMode: .full`.
    @available(iOS 26.0, *)
    private func runArm(
        arm: LexicalScorerArm,
        episodeId: String,
        annotation: CorpusAnnotation,
        audioURL: URL,
        transcript: [TranscriptChunk]
    ) async throws -> [AdWindow] {
        guard let localURL = LocalAudioURL(audioURL) else {
            throw LexicalABRunError(reason: "audio at \(audioURL.path) is not a file URL")
        }

        // Fresh store per arm so the two runs never observe each other's
        // persisted windows or planner state.
        let storeDir = try makeTempDir(prefix: "LexicalScorerLiveAB-\(arm.rawValue)")
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        // Asset row. `analysisAssetId == episodeId` so the scored ad windows
        // read back under the id the accumulator buckets on.
        let assetId = episodeId
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "lexical-scorer-fp-\(episodeId)",
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
            throw LexicalABRunError(reason: "feature extraction produced 0 windows for \(episodeId)")
        }
        try await store.insertFeatureWindows(featureWindows)

        // Episode duration from the decoded shard span (last shard end).
        let episodeDuration = shards.reduce(0.0) { max($0, $1.startTime + $1.duration) }

        // PRECONDITION — seed the planner into `targetedWithAudit` so the live
        // narrowing path runs (the only path where xsdz.2/.3 reshape windows).
        // Five full-rescan observations with a 0.9 recall sample satisfy both
        // `observedEpisodeCount >= 5` and `stableRecall == true`. Symmetric
        // across arms, so it cannot bias the comparison.
        try await seedTargetedWithAuditPlannerState(
            store: store,
            podcastId: annotation.showName
        )

        // Per-arm config: the single source of the two program toggles.
        // `adDetectionConfig(for:)` reads the arm's xsdz.1 gate and
        // `narrowingConfig(for:)` reads its xsdz.2/.3 gate — exactly the two
        // toggles that distinguish the four arms.
        let config = LexicalScorerArmConfig.adDetectionConfig(for: arm)
        let narrowingConfig = LexicalScorerArmConfig.narrowingConfig(for: arm)

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            // Inject a LIVE FM BackfillJobRunner that bakes in this arm's
            // `narrowingConfig`. Without a runner factory the FM scan returns
            // `.skipped` and there is nothing for the program to influence.
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
        // in `targetedWithAudit` — the regime where xsdz.2/.3 reshape windows.
        // If a future change regresses this to `fullCoverage`, the
        // region/audit legs silently stop firing and the A/B reads a false
        // zero on those legs. Reproduce the planner's own decision so the guard
        // fails loud.
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
            "arm=\(arm.rawValue): planner is not in targetedWithAudit (observed=\(seededContext.observedEpisodeCount) stableRecall=\(seededContext.stableRecall)) — xsdz.2/.3 would not fire and the A/B reads a false zero"
        )

        // SCORED RUN: a single backfill. With the planner warmed, the FM scan
        // runs the targeted phases under the arm's `narrowingConfig`; the
        // persisted ad windows are the scored output.
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
    /// warmed podcast would have accrued. Identical to the chapter A/B's
    /// `seedTargetedWithAuditPlannerState` (same regime is required).
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
    /// `PlayheadFMSmokeTests.makeLiveSmokeRunner` and `ChapterFusionLiftABTests`,
    /// plus the xsdz.liveab `narrowingConfig:` seam). The `mode` argument is
    /// supplied by `AdDetectionService` (the effective FM mode, `.full` here).
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
                scanCohortJSON: makeTestScanCohortJSON(promptLabel: "lexical-scorer-ab"),
                sensitiveRouter: router,
                permissiveClassifier: permissiveClassifierBox,
                // xsdz.liveab: the per-arm gate for xsdz.2/.3. xsdz23-on arms
                // pass `.default` (snap on); xsdz23-off arms pass the same shape
                // with `lexicalClusterSnapEnabled: false` (snap off).
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
private struct LexicalABRunError: Error {
    let reason: String
}
