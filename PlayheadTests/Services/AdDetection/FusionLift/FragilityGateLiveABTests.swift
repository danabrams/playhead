// FragilityGateLiveABTests.swift
// playhead-xsdz.7 live measurement — env-gated Mac Catalyst test that runs, in
// ONE Catalyst pass over the 12-episode dogfood golden corpus, BOTH halves of
// the fragility investigation:
//
//   Part A — per-span fragility DIAGNOSTIC. The BASELINE (production, gate-OFF)
//   arm runs ONCE with a `FragilityDiagnosticObserver` attached. The observer
//   fires inside the real decision path for EVERY decoded span and records that
//   span's fragility geometry (proposal/skip confidence, maxSingleEntryWeight,
//   distinct evidence-family depth, margin, and the `fragilityScore` from the
//   PRODUCTION helper) computed from the SAME post-suppression ledger the
//   decision used. After the run, each recorded span is labeled TP / FP /
//   correctly-rejected by joining it to the SAME greedy-IoU pairing the metrics
//   scorer uses (`FragilityPerSpanLabeler`). The labeled rows + a FP-vs-TP group
//   summary (mean & median fragility / margin / concentration / depth, plus an
//   explicit "are FP fragilities systematically higher than TP?" verdict) dump
//   to `playhead-dogfood-diagnostics-fragility-perspan.json`.
//
//   Part B — threshold/penalty SWEEP. In addition to the baseline (gate OFF),
//   four TREATMENT arms run the gate ON at distinct operating points
//   ((1.5,0.85), (1.0,0.85), (0.7,0.70), (0.5,0.50)) to probe whether a lower
//   threshold / stronger penalty drops false positives without hurting recall.
//   Every arm = real `runBackfill` (fmBackfillMode .full) scored vs the golden;
//   per-arm TP/FP/miss + span P/R/F1 + coverage P/R + treatment−baseline deltas
//   dump to `playhead-dogfood-diagnostics-fragility-sweep.json`.
//
// The BASELINE arm is run ONCE and serves BOTH halves — it is the diagnostic
// source AND the sweep's baseline. No double-run.
//
// Each arm differs ONLY in the three fragility-tuning fields
// (`evidenceFragilityPenaltyEnabled` / `fragilityThreshold` /
// `fragilityPenalty`); every other `AdDetectionConfig` field equals production
// `.default`, and every arm uses `NarrowingConfig.default` (snap ON). The
// one-axis isolation is pinned hermetically on the sim by
// `FragilitySweepArmConfigTests` (no env var needed). The per-span TP/FP join
// is pinned hermetically by `FragilityPerSpanLabelerTests`.
//
// Sibling of `LexicalScorerLiveABTests` / the original 2-arm fragility A/B:
// same live runner factory, same planner seeding, same store/feature/transcript
// setup, same scoring helpers (`CorpusAnnotationLoader`, `FusionLiftScoring`,
// `FusionLiftModeAccumulator`).
//
// PRECONDITION (the FM narrowing path must run like production): the live
// targeted-phase narrowing inside `BackfillJobRunner` is guarded by
// `plan.policy == .targetedWithAudit`. A fresh store is cold-start
// (`observedEpisodeCount == 0`) ⇒ `fullCoverage` ⇒ the targeted phases never
// narrow. So each arm SEEDS the planner state into the warmed
// `targetedWithAudit` regime before the scored run. This is symmetric across
// arms (same podcastId per arm store), so it cannot bias the comparison; the
// only between-arm difference remains the fragility tuning. The fragility gate
// itself is policy-independent, so the seeding is conservative for that gate.
//
// Gating (mirrors `LexicalScorerLiveABTests`):
//   `PLAYHEAD_FRAGILITY_AB=1` MUST be set in the test process environment. The
//   default `PlayheadFastTests` plan does NOT set it, so the test body is a
//   no-op on Cmd-U (via `XCTSkipUnless`). The live FM `BackfillJobRunner` also
//   requires iOS 26+ (`#available`), so an older runtime skips too.
//
// Cost (READ before running): this runs the REAL `runBackfill` with full FM ad
// scanning once per arm × 5 arms × N dogfood episodes (~60 full-FM passes for
// the 12-episode dogfood set) — on the order of two hours on Mac Catalyst. It is
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
//   4. Read the printed tables; the two per-run JSON dumps land at the repo root
//      (both git-ignored — they match the `playhead-dogfood-diagnostics-*.json`
//      pattern in `.gitignore`):
//        - playhead-dogfood-diagnostics-fragility-perspan.json (Part A)
//        - playhead-dogfood-diagnostics-fragility-sweep.json   (Part B)

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
            Evidence-Fragility diagnostic + sweep is opt-in and SLOW (~60 \
            full-FM passes: 1 scored pass × 5 arms × 12 episodes). Set \
            PLAYHEAD_FRAGILITY_AB=1 in the test plan env vars and run on Mac \
            Catalyst (or an iOS 26 device) with Apple Intelligence enabled and \
            corpus audio + transcripts staged. See the file header for the full \
            invocation recipe.
            """
        )
    }

    // MARK: - Multi-run aggregate (playhead-xsdz.14)

    /// playhead-xsdz.14 — noise-aware multi-run aggregation across the
    /// 5-arm fragility sweep. Runs the SAME arms × episodes as the
    /// single-run sibling N times (default 5, configurable via the
    /// `PLAYHEAD_MULTIRUN_N` env var, clamped to `[2,20]`), then dumps a
    /// `MultiRunReport` with median + IQR + mean + stdev per metric and a
    /// REAL / WITHIN-NOISE / AMBIGUOUS verdict per treatment-vs-baseline
    /// metric (`NoiseBand.activationDefault`). The per-span diagnostic
    /// from the sibling is NOT collected here — multi-run measures the
    /// aggregate metrics, not the per-span per-run rows.
    ///
    /// Cost: each Catalyst N-run is `N × ~60 full-FM passes = N × ~12 min ×
    /// 5 arms ≈ several hours` (default N=5 → ~10–14 hours on a warm Mac).
    /// Design implies overnight runs.
    ///
    /// Gating: requires BOTH `PLAYHEAD_FRAGILITY_AB=1` (per-harness opt-in,
    /// enforced by `setUp()`) AND `PLAYHEAD_MULTIRUN_AB=1` (combined
    /// multi-run gate, enforced inline below).
    func testMultiRunAggregateAcrossDogfoodCorpus() async throws {
        try XCTSkipUnless(
            multiRunABEnabled(),
            "Fragility multi-run aggregation is opt-in via PLAYHEAD_MULTIRUN_AB=1 (combined gate across all 5 activation harnesses)."
        )
        guard #available(iOS 26.0, *) else {
            throw XCTSkip("Live FM BackfillJobRunner requires iOS 26+ / FoundationModels.")
        }

        let loader = CorpusAnnotationLoader()
        let annotationURLs: [URL]
        do {
            annotationURLs = try loader.annotationFileURLs()
        } catch CorpusAnnotationLoaderError.directoryNotFound(let url) {
            throw XCTSkip("corpus annotations dir not present: \(url.path)")
        }
        try XCTSkipIf(annotationURLs.isEmpty, "no corpus annotations staged")
        try loader.preflightGoldEvaluationInputs(annotationURLs: annotationURLs)

        let arms = FragilitySweepArm.allCases
        let runCount = multiRunCountFromEnv()

        let report = try await runMultiRunAggregation(
            arms: arms.map(\.rawValue),
            config: MultiRunDriverConfig(runCount: runCount, configHash: "fragility-gate-ab")
        ) { armLabel, runIndex in
            guard let arm = FragilitySweepArm(rawValue: armLabel) else {
                throw FragilityABRunError(reason: "unknown arm \(armLabel)")
            }
            return try await self.scoreOneRun(
                arm: arm,
                runIndex: runIndex,
                loader: loader,
                annotationURLs: annotationURLs
            )
        }

        print(report.table())
        let dumpURL = loader.repoRoot.appendingPathComponent(
            "playhead-dogfood-diagnostics-multirun-fragility-gate.json",
            isDirectory: false
        )
        try report.toJSON().write(to: dumpURL, options: .atomic)
        print("Multi-run fragility-gate JSON: \(dumpURL.path) (git-ignored)")
    }

    /// One multi-run pass for one arm: replay the same per-episode arm
    /// loop the single-run sibling uses, fold every episode's persisted
    /// windows into a fresh `FusionLiftModeAccumulator`, and return its
    /// `ArmRunResult`. The fragility diagnostic observer is NOT injected
    /// here (its rows are a different output type that multi-run does
    /// not aggregate). A FRESH store is created per episode per arm per
    /// run (existing `runArm` does this via `makeTempDir`).
    @available(iOS 26.0, *)
    private func scoreOneRun(
        arm: FragilitySweepArm,
        runIndex: Int,
        loader: CorpusAnnotationLoader,
        annotationURLs: [URL]
    ) async throws -> ArmRunResult {
        var accumulator = FusionLiftModeAccumulator()
        var scoredCount = 0
        for url in annotationURLs {
            let episodeId = url.deletingPathExtension().lastPathComponent
            let annotation = try loader.loadAndValidate(at: url)
            guard annotation.isEligibleForGoldEvaluation else { continue }
            let audioURL: URL
            do {
                audioURL = try loader.audioFileURL(for: annotation)
            } catch {
                continue
            }
            try loader.verify(audioFingerprintFor: annotation, jsonURL: url)
            let transcript = try CorpusTranscriptLoader.load(
                episodeId: episodeId,
                repoRoot: loader.repoRoot
            )
            guard !transcript.isEmpty else { continue }

            let windows = try await runArm(
                arm: arm,
                episodeId: episodeId,
                annotation: annotation,
                audioURL: audioURL,
                transcript: transcript,
                diagnosticObserver: nil  // multi-run does not aggregate per-span rows
            )
            accumulator.addEpisode(
                annotationWindows: annotation.adWindows,
                adWindows: windows,
                podcastId: annotation.showName,
                episodeId: episodeId
            )
            scoredCount += 1
        }
        return ArmRunResult(episodeCount: scoredCount, accumulator: accumulator)
    }

    // MARK: - Entry: ONE pass, BOTH the per-span diagnostic and the sweep

    func testFragilityDiagnosticAndSweepAcrossDogfoodCorpus() async throws {
        guard #available(iOS 26.0, *) else {
            throw XCTSkip("Live FM BackfillJobRunner requires iOS 26+ / FoundationModels.")
        }

        let loader = CorpusAnnotationLoader()

        let annotationURLs: [URL]
        do {
            annotationURLs = try loader.annotationFileURLs()
        } catch CorpusAnnotationLoaderError.directoryNotFound(let url) {
            throw XCTSkip("corpus annotations dir not present: \(url.path)")
        }
        try XCTSkipIf(annotationURLs.isEmpty, "no corpus annotations staged")
        try loader.preflightGoldEvaluationInputs(annotationURLs: annotationURLs)

        // One accumulator per sweep arm (baseline + 4 treatments). Each arm runs
        // the SAME store/features/transcripts/planner-seeding/FM-mode `.full` per
        // episode; only the fragility tuning varies.
        var accumulators: [FragilitySweepArm: FusionLiftModeAccumulator] = Dictionary(
            uniqueKeysWithValues: FragilitySweepArm.allCases.map { ($0, FusionLiftModeAccumulator()) }
        )

        // Part A: labeled per-span diagnostic rows accumulated across episodes,
        // produced ONLY from the baseline arm (run once below).
        var perSpanRows: [LabeledFragilitySpanRow] = []

        var scored: [String] = []
        var skipped: [(episodeId: String, reason: String)] = []
        var failed: [(episodeId: String, reason: String)] = []

        for url in annotationURLs {
            let episodeId = url.deletingPathExtension().lastPathComponent

            // Resolve annotation (ground truth).
            let annotation: CorpusAnnotation
            do {
                annotation = try loader.loadAndValidate(at: url)
            } catch {
                failed.append((episodeId, "annotation validation failed: \(error.localizedDescription)"))
                continue
            }
            guard annotation.isEligibleForGoldEvaluation else { continue }

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

            // Run ALL FIVE arms for this episode. Each arm gets its own fresh
            // store / asset / features / transcript / seeded planner state, so
            // the only between-arm difference is the fragility tuning. The
            // BASELINE arm additionally attaches the diagnostic observer (Part A);
            // it is run exactly ONCE and reused for both the diagnostic and the
            // sweep's baseline. An arm failure aborts the whole episode (so no arm
            // is scored on a partial episode that would bias the comparison).
            do {
                for arm in FragilitySweepArm.allCases {
                    let observer: FragilityDiagnosticObserver? =
                        arm == .baseline ? FragilityDiagnosticObserver() : nil

                    let windows = try await runArm(
                        arm: arm,
                        episodeId: episodeId,
                        annotation: annotation,
                        audioURL: audioURL,
                        transcript: transcript,
                        diagnosticObserver: observer
                    )
                    accumulators[arm]?.addEpisode(
                        annotationWindows: annotation.adWindows,
                        adWindows: windows,
                        podcastId: annotation.showName,
                        episodeId: episodeId
                    )

                    // Part A: for the baseline arm only, label the recorded
                    // per-span rows against THIS arm's persisted windows + the
                    // golden, using the SAME pairing the scorer uses.
                    if arm == .baseline, let observer {
                        let recorded = await observer.spanRows(for: episodeId) ?? []
                        perSpanRows.append(contentsOf: FragilityPerSpanLabeler.label(
                            rows: recorded,
                            annotationWindows: annotation.adWindows,
                            adWindows: windows,
                            podcastId: annotation.showName,
                            episodeId: episodeId
                        ))
                    }
                }
                scored.append(episodeId)
            } catch let error as FragilityABRunError {
                failed.append((episodeId, error.reason))
            } catch {
                failed.append((episodeId, "arm run failed: \(error.localizedDescription)"))
            }
        }

        guard failed.isEmpty else {
            XCTFail("Hard failures: \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))")
            return
        }
        guard !scored.isEmpty else {
            XCTFail(
                "PLAYHEAD_FRAGILITY_AB=1 was set but no episodes scored; all staged inputs were skipped"
            )
            return
        }

        // ── Part A dump: per-span diagnostic ─────────────────────────────────
        let perSpanReport = FragilityPerSpanDiagnosticReport(
            episodeCount: scored.count,
            rows: perSpanRows
        )
        print(perSpanReport.table())
        let perSpanURL = loader.repoRoot.appendingPathComponent(
            "playhead-dogfood-diagnostics-fragility-perspan.json",
            isDirectory: false
        )
        try perSpanReport.jsonData().write(to: perSpanURL, options: .atomic)
        print("Per-span fragility diagnostic JSON: \(perSpanURL.path) (git-ignored)")

        // ── Part B dump: threshold/penalty sweep ─────────────────────────────
        let sweepReport = FragilitySweepReport(
            episodeCount: scored.count,
            accumulators: accumulators
        )
        print(sweepReport.table())
        print("""
        scored=\(scored.count): \(scored.sorted().joined(separator: ", "))
        skipped (audio/transcript missing)=\(skipped.count): \(skipped.map(\.episodeId).sorted().joined(separator: ", "))
        failed=\(failed.count): \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))
        """)
        let sweepURL = loader.repoRoot.appendingPathComponent(
            "playhead-dogfood-diagnostics-fragility-sweep.json",
            isDirectory: false
        )
        try sweepReport.jsonData().write(to: sweepURL, options: .atomic)
        print("Fragility sweep JSON: \(sweepURL.path) (git-ignored)")

    }

    // MARK: - One arm

    /// Run the scored backfill once for a single episode in the requested sweep
    /// arm and return the persisted ad windows. Every arm decodes the same
    /// audio, persists the same feature windows + transcript chunks, SEEDs the
    /// planner state into the warmed `targetedWithAudit` regime, and uses
    /// `NarrowingConfig.default` (snap ON — production state). The ONLY thing
    /// that differs is the arm's three fragility-tuning fields. Always uses
    /// `fmBackfillMode: .full`. When `diagnosticObserver` is non-nil (the
    /// baseline arm), it is injected into the service so the per-span diagnostic
    /// fires; it is behavior-neutral (it only records, never feeds back).
    @available(iOS 26.0, *)
    private func runArm(
        arm: FragilitySweepArm,
        episodeId: String,
        annotation: CorpusAnnotation,
        audioURL: URL,
        transcript: [TranscriptChunk],
        diagnosticObserver: FragilityDiagnosticObserver?
    ) async throws -> [AdWindow] {
        guard let localURL = LocalAudioURL(audioURL) else {
            throw FragilityABRunError(reason: "audio at \(audioURL.path) is not a file URL")
        }

        // Fresh store per arm so the runs never observe each other's persisted
        // windows or planner state.
        let storeDir = try makeTempDir(prefix: "FragilityGateLiveAB-\(arm.rawValue)")
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        // Asset row. `analysisAssetId == episodeId` so the scored ad windows
        // read back under the id the accumulator buckets on (and the id the
        // diagnostic observer records under).
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

        // Per-arm config: the single source of the fragility tuning. Every other
        // field is held identical (= production `.default`) across all arms.
        let config = FragilityGateArmConfig.adDetectionConfig(for: arm)
        // Every arm uses the production-default NarrowingConfig (snap ON).
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
            // Behavior-neutral per-span diagnostic sink. Non-nil ONLY on the
            // baseline arm; nil everywhere else (and in production). Declared
            // after canUseFoundationModelsProvider to match the init order.
            fragilityDiagnosticObserver: diagnosticObserver,
            approvedCohortRegistry: nil // avoid cohort-gated FM demotion
        )

        // GUARD #1 (FM-mode gating): confirm cohort/FM gating did not silently
        // demote `.full` → `.off`. Else the FM phase never runs and the
        // measurement is a meaningless zero.
        let effectiveMode = await service.effectiveFMBackfillModeForTesting()
        XCTAssertEqual(
            effectiveMode,
            .full,
            "arm=\(arm.rawValue): effective FM mode demoted off .full — FM phase would not run"
        )

        // GUARD #2 (planner regime): confirm the seeded state lands the planner
        // in `targetedWithAudit` — the production-like FM regime.
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
        // runs the targeted phases under the production-default narrowing config;
        // the persisted ad windows are the scored output, with the fragility gate
        // active at this arm's operating point (off for the baseline).
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
    /// `stableRecall == true`. Uses the production observation API so the seeded
    /// row is byte-identical to one a warmed podcast would have accrued.
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
    /// `BackfillJobRunner` with `narrowingConfig` baked in. For this measurement
    /// `narrowingConfig` is `NarrowingConfig.default` for EVERY arm — the
    /// narrowing config does NOT vary; the fragility tuning does (and it lives on
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
                // Production-default narrowing for every arm — the narrowing
                // config does not distinguish the sweep's arms.
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
