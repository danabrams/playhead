// AudioForensicsTemporalRegLiveABTests.swift
// playhead-actempo — env-gated Mac Catalyst A/B tests that measure the two
// off-by-default precision signals through the REAL
// `AdDetectionService.runBackfill` (fmBackfillMode: .full), scored against the
// 12-episode dogfood golden corpus.
//
// SPLIT BY SIGNAL (one signal per Catalyst pass): each signal gets its OWN
// focused 2-arm method (its own independent `.baseline` arm + its one treatment
// arm), so the orchestrator can run, score, and read each signal one at a time:
//   * `testAudioForensicsABAcrossDogfoodCorpus` — [.baseline, .treatment] for
//     xsdz.8, dumps `playhead-dogfood-diagnostics-audioforensics-ab.json`.
//   * `testTemporalRegularizationABAcrossDogfoodCorpus` — [.baseline, .treatment]
//     for xsdz.10, dumps `playhead-dogfood-diagnostics-temporalreg-ab.json`.
//
// The two signals (both OFF in production `.default`; each arm flips ONLY the one
// signal's flag, every other field == `.default`):
//   * playhead-xsdz.8 — composite audio-forensics boundary evidence
//     (`AdDetectionConfig.audioForensicsEnabled`). A LEDGER-ENTRY signal: the
//     treatment arm builds a `.audioForensics` entry per qualifying span.
//   * playhead-xsdz.10 — lightweight temporal regularization
//     (`AdDetectionConfig.temporalRegularizationEnabled`). A POST-FUSION
//     multiplicative penalty on `skipConfidence` for isolated / too-short islands.
//
// ARMS (`ActempoArm`) — every non-toggle field == production `.default`:
//   * baseline  — the signal under test OFF (= production). Anchors every delta.
//   * treatment — `.default` + the one signal under test enabled.
// The one-axis isolation is pinned hermetically on the sim by
// `ActempoArmConfigTests` (no env var needed).
//
// ★ per-signal FIRE instrumentation (so a null result is interpretable) ★
//   A metric delta ≤±2 FP is FM intra-run NOISE on this corpus, so each A/B MUST
//   report whether the signal actually FIRED, else the result is uninterpretable.
//   Each signal has a DIFFERENT fire mechanism — both via behavior-neutral,
//   nil-default observers that are nil in production:
//   * xsdz.8 (LEDGER ENTRY): the harness attaches the nil-default
//     `BrandAppearanceChannelTapObserver` (generalized to count `.audioForensics`
//     entries) and counts, per arm, how many spans received a `.audioForensics`
//     ledger entry. Reads the SAME pre-suppression ledger the decision builds from.
//   * xsdz.10 (POST-FUSION PENALTY, NOT a ledger entry): the harness attaches the
//     nil-default `TemporalRegularizationObserver` and counts, per arm, how many
//     candidate detections had their `skipConfidence` actually CHANGED by the
//     isolation-penalty / min-dwell pass — the EXACT count the production pass
//     computes. (The penalty only runs when the flag is on AND there are >1
//     candidates, so the baseline arm reports 0 fires, as expected.)
//   The fire counts land in each JSON. Both observers are nil at the sole
//   production `PlayheadRuntime` construction site (decisions byte-identical).
//
// PRECONDITION (the FM path must run like production): the live targeted-phase
// narrowing inside `BackfillJobRunner` is guarded by
// `plan.policy == .targetedWithAudit`; a fresh store is cold-start
// (`fullCoverage`). So each arm SEEDS the planner into the warmed
// `targetedWithAudit` regime before the scored run. Symmetric across arms, so it
// cannot bias the comparison.
//
// Gating (mirrors `BrandAppearanceLiveABTests` / `FragilityGateLiveABTests`):
//   `PLAYHEAD_ACTEMPO_AB=1` MUST be set in the test process environment. The
//   default `PlayheadFastTests` plan does NOT set it, so the test body is a no-op
//   on Cmd-U (via `XCTSkipUnless`). The live FM `BackfillJobRunner` also requires
//   iOS 26+ (`#available`), so an older runtime skips too.
//
// Cost (READ before running): each single-signal method runs the REAL
// `runBackfill` with full FM ad scanning once per arm × 2 arms × N dogfood
// episodes (~24 full-FM passes for the 12-episode dogfood set) — on the order of
// an hour on Mac Catalyst per method. These are ORCHESTRATOR-run steps, NOT a
// green gate. To invoke (one method per Catalyst pass):
//
//   1. Stage corpus audio under `TestFixtures/Corpus/Audio/<episode_id>.<ext>`
//      and whisper transcripts under `TestFixtures/Corpus/Transcripts/`.
//   2. Enable Apple Intelligence on the host Mac (FM classifier is on-device).
//   3. Run on Mac Catalyst with the env var set, one signal at a time:
//        # audio-forensics (xsdz.8)
//        xcodebuild test \
//          -scheme Playhead -testPlan PlayheadFastTests \
//          -destination 'platform=macOS,variant=Mac Catalyst' \
//          -only-testing:'PlayheadTests/AudioForensicsTemporalRegLiveABTests/testAudioForensicsABAcrossDogfoodCorpus' \
//          PLAYHEAD_ACTEMPO_AB=1
//        # temporal regularization (xsdz.10)
//        xcodebuild test \
//          -scheme Playhead -testPlan PlayheadFastTests \
//          -destination 'platform=macOS,variant=Mac Catalyst' \
//          -only-testing:'PlayheadTests/AudioForensicsTemporalRegLiveABTests/testTemporalRegularizationABAcrossDogfoodCorpus' \
//          PLAYHEAD_ACTEMPO_AB=1
//   4. Read the printed 2-row table; the JSON dumps land at the repo root as
//      `playhead-dogfood-diagnostics-audioforensics-ab.json` and
//      `playhead-dogfood-diagnostics-temporalreg-ab.json` (git-ignored — they
//      match the `playhead-dogfood-diagnostics-*.json` pattern in .gitignore).
//
// Hermetic helpers (`ActempoSignal`, `ActempoArm`, `ActempoArmConfig`,
// `ActempoFireTally`, `ActempoSweepReport`, `FusionLiftModeAccumulator`) live in
// `FusionLiftHarnessSupport.swift` and are unit-tested on the simulator by
// `ActempoArmConfigTests`. Scoring reuses `FusionLiftScoring.swift`.

import AVFoundation
import Foundation
import XCTest
@testable import Playhead

final class AudioForensicsTemporalRegLiveABTests: XCTestCase {

    /// Gate every test on the A/B env var. The default scheme does not set it,
    /// so the test body is a no-op on Cmd-U / the default sim suite.
    private static var abEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_ACTEMPO_AB"] == "1"
    }

    override func setUp() async throws {
        try await super.setUp()
        try XCTSkipUnless(
            Self.abEnabled,
            """
            Audio-forensics / temporal-reg precision-signal A/B is opt-in and \
            SLOW (each single-signal method runs ~24 full-FM passes: 1 scored \
            pass × 2 arms × 12 episodes). Set PLAYHEAD_ACTEMPO_AB=1 in the test \
            plan env vars and run on Mac Catalyst (or an iOS 26 device) with Apple \
            Intelligence enabled and corpus audio + transcripts staged. See the \
            file header for the full invocation recipe.
            """
        )
    }

    // MARK: - Multi-run aggregate (playhead-xsdz.14)

    /// playhead-xsdz.14 — noise-aware multi-run aggregation across BOTH
    /// xsdz.8 (audio forensics) and xsdz.10 (temporal regularization)
    /// single-signal A/Bs. Runs each signal's 2-arm sweep N times (default
    /// 5, configurable via `PLAYHEAD_MULTIRUN_N`, clamped to `[2,20]`) and
    /// dumps ONE `MultiRunReport` per signal with median + IQR + mean +
    /// stdev per metric and a REAL / WITHIN-NOISE / AMBIGUOUS verdict per
    /// treatment-vs-baseline metric (`NoiseBand.activationDefault`).
    ///
    /// Cost: each Catalyst N-run is `N × 2 signals × ~24 full-FM passes =
    /// N × ~12 min × 2 arms × 2 signals ≈ several hours` (default N=5 →
    /// ~10–12 hours on a warm Mac). Design implies overnight runs.
    ///
    /// Gating: requires BOTH `PLAYHEAD_ACTEMPO_AB=1` (per-harness opt-in,
    /// enforced by `setUp()`) AND `PLAYHEAD_MULTIRUN_AB=1` (combined
    /// multi-run gate, enforced inline below).
    func testMultiRunAggregateAcrossDogfoodCorpus() async throws {
        try XCTSkipUnless(
            multiRunABEnabled(),
            "Actempo multi-run aggregation is opt-in via PLAYHEAD_MULTIRUN_AB=1 (combined gate across all 5 activation harnesses)."
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

        let arms: [ActempoArm] = [.baseline, .treatment]
        let runCount = multiRunCountFromEnv()

        for signal in ActempoSignal.allCases {
            let report = try await runMultiRunAggregation(
                arms: arms.map(\.rawValue),
                config: MultiRunDriverConfig(
                    runCount: runCount,
                    configHash: "actempo-\(signal.rawValue)-ab"
                )
            ) { armLabel, runIndex in
                guard let arm = ActempoArm(rawValue: armLabel) else {
                    throw ActempoABRunError(reason: "unknown arm \(armLabel)")
                }
                return try await self.scoreOneRun(
                    signal: signal,
                    arm: arm,
                    runIndex: runIndex,
                    loader: loader,
                    annotationURLs: annotationURLs
                )
            }
            print(report.table())
            let dumpURL = loader.repoRoot.appendingPathComponent(
                "playhead-dogfood-diagnostics-multirun-actempo-\(signal.rawValue).json",
                isDirectory: false
            )
            try report.toJSON().write(to: dumpURL, options: .atomic)
            print("Multi-run actempo \(signal.label) JSON: \(dumpURL.path) (git-ignored)")
        }
    }

    /// One multi-run pass for one (signal, arm): replay the same
    /// per-episode arm loop the single-run sibling uses, fold every
    /// episode's persisted windows + fire counts into a fresh accumulator
    /// + tally, and return the run's `ArmRunResult`. A FRESH analysis
    /// store is created per episode per arm per run (existing `runArm`
    /// does this via `makeTempDir`).
    @available(iOS 26.0, *)
    private func scoreOneRun(
        signal: ActempoSignal,
        arm: ActempoArm,
        runIndex: Int,
        loader: CorpusAnnotationLoader,
        annotationURLs: [URL]
    ) async throws -> ArmRunResult {
        var accumulator = FusionLiftModeAccumulator()
        var fireTally = ActempoFireTally()
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

            let tap = BrandAppearanceChannelTapObserver()
            let temporalObserver = TemporalRegularizationObserver()

            let windows = try await runArm(
                signal: signal,
                arm: arm,
                episodeId: episodeId,
                annotation: annotation,
                audioURL: audioURL,
                transcript: transcript,
                tap: tap,
                temporalObserver: temporalObserver
            )
            accumulator.addEpisode(
                annotationWindows: annotation.adWindows,
                adWindows: windows,
                podcastId: annotation.showName,
                episodeId: episodeId
            )
            let tapCounts = await tap.fireCounts(for: episodeId)
            fireTally.addAudioForensics(tapCounts)
            let temporalCounts = await temporalObserver.fireCounts(for: episodeId)
            fireTally.addTemporalReg(temporalCounts)
            scoredCount += 1
        }
        return ArmRunResult(
            episodeCount: scoredCount,
            accumulator: accumulator,
            fireCount: fireTally.asMultiRunChannelMap()
        )
    }

    // MARK: - A/B entry points (one focused method per signal)

    /// playhead-xsdz.8 — composite audio-forensics boundary evidence
    /// (`audioForensicsEnabled`). A LEDGER-ENTRY signal. Runs its OWN baseline +
    /// the treatment arm, and dumps a focused 2-arm report with the
    /// `.audioForensics` fire count per arm.
    func testAudioForensicsABAcrossDogfoodCorpus() async throws {
        try await runABPass(
            signal: .audioForensics,
            dumpFilename: "playhead-dogfood-diagnostics-audioforensics-ab.json"
        )
    }

    /// playhead-xsdz.10 — lightweight temporal regularization
    /// (`temporalRegularizationEnabled`). A POST-FUSION skipConfidence penalty.
    /// Runs its OWN baseline + the treatment arm, and dumps a focused 2-arm report
    /// with the penalty-applied-span fire count per arm.
    func testTemporalRegularizationABAcrossDogfoodCorpus() async throws {
        try await runABPass(
            signal: .temporalRegularization,
            dumpFilename: "playhead-dogfood-diagnostics-temporalreg-ab.json"
        )
    }

    // MARK: - Shared A/B pass

    /// Run a single Catalyst A/B pass for `signal` over `[.baseline, .treatment]`
    /// (baseline first), score each arm vs the golden, and dump a focused report
    /// to `dumpFilename`. Each invocation is fully self-contained — it builds its
    /// own accumulators + fire tallies + its OWN baseline — so two methods calling
    /// this never share state.
    private func runABPass(
        signal: ActempoSignal,
        dumpFilename: String
    ) async throws {
        guard #available(iOS 26.0, *) else {
            throw XCTSkip("Live FM BackfillJobRunner requires iOS 26+ / FoundationModels.")
        }
        let arms: [ActempoArm] = [.baseline, .treatment]

        let loader = CorpusAnnotationLoader()

        let annotationURLs: [URL]
        do {
            annotationURLs = try loader.annotationFileURLs()
        } catch CorpusAnnotationLoaderError.directoryNotFound(let url) {
            throw XCTSkip("corpus annotations dir not present: \(url.path)")
        }
        try XCTSkipIf(annotationURLs.isEmpty, "no corpus annotations staged")
        try loader.preflightGoldEvaluationInputs(annotationURLs: annotationURLs)

        // One accumulator + one fire tally per arm IN THIS PASS.
        var accumulators: [ActempoArm: FusionLiftModeAccumulator] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, FusionLiftModeAccumulator()) }
        )
        var fireTallies: [ActempoArm: ActempoFireTally] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, ActempoFireTally()) }
        )

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
                skipped.append((episodeId, "audio file not staged under \(loader.audioDirectoryURL.path)"))
                continue
            }

            // Audio-fingerprint check (a mismatch means a different cut than the
            // annotation references — scoring would compare against wrong GT).
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

            // Run BOTH arms for this episode. Each arm gets its own fresh store /
            // asset / features / transcript / seeded planner state, so the only
            // between-arm difference is the one signal flag. An arm failure aborts
            // the whole episode (so no arm is scored on a partial episode that
            // would bias the comparison).
            do {
                for arm in arms {
                    // Behavior-neutral fire observers (non-nil ONLY here; nil in
                    // production). For xsdz.8 the channel tap counts `.audioForensics`
                    // ledger entries; for xsdz.10 the temporal-reg observer counts
                    // penalty-applied spans. Each is attached for BOTH arms so the
                    // baseline's fire count (expected 0) is measured, not assumed.
                    let tap = BrandAppearanceChannelTapObserver()
                    let temporalObserver = TemporalRegularizationObserver()

                    let windows = try await runArm(
                        signal: signal,
                        arm: arm,
                        episodeId: episodeId,
                        annotation: annotation,
                        audioURL: audioURL,
                        transcript: transcript,
                        tap: tap,
                        temporalObserver: temporalObserver
                    )
                    accumulators[arm]?.addEpisode(
                        annotationWindows: annotation.adWindows,
                        adWindows: windows,
                        podcastId: annotation.showName,
                        episodeId: episodeId
                    )
                    // Fold the per-signal fire counts for this episode into the arm
                    // total. Only the measured signal's counters are meaningful, but
                    // folding both is harmless (the unused one is 0).
                    let tapCounts = await tap.fireCounts(for: episodeId)
                    fireTallies[arm]?.addAudioForensics(tapCounts)
                    let temporalCounts = await temporalObserver.fireCounts(for: episodeId)
                    fireTallies[arm]?.addTemporalReg(temporalCounts)
                }
                scored.append(episodeId)
            } catch let error as ActempoABRunError {
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
                "PLAYHEAD_ACTEMPO_AB=1 was set but no episodes scored; all staged inputs were skipped"
            )
            return
        }

        // Build + print + dump the report over EXACTLY the arms this pass ran.
        let report = ActempoSweepReport(
            signal: signal,
            episodeCount: scored.count,
            arms: arms,
            accumulators: accumulators,
            fireTallies: fireTallies
        )
        print(report.table())
        print("""
        signal=\(signal.label)
        scored=\(scored.count): \(scored.sorted().joined(separator: ", "))
        skipped (audio/transcript missing)=\(skipped.count): \(skipped.map(\.episodeId).sorted().joined(separator: ", "))
        failed=\(failed.count): \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))
        """)

        let dumpURL = loader.repoRoot.appendingPathComponent(dumpFilename, isDirectory: false)
        try report.jsonData().write(to: dumpURL, options: .atomic)
        print("Actempo \(signal.label) A/B JSON: \(dumpURL.path) (git-ignored)")

    }

    // MARK: - One arm

    /// Run the scored backfill once for a single episode in the requested arm of
    /// `signal` and return the persisted ad windows. Every arm decodes the same
    /// audio, persists the same feature windows + transcript chunks, SEEDs the
    /// planner into the warmed `targetedWithAudit` regime, and uses
    /// `NarrowingConfig.default`. The ONLY thing that differs is the one signal's
    /// flag. Always uses `fmBackfillMode: .full`. Both behavior-neutral fire
    /// observers are injected for every arm (they only record).
    @available(iOS 26.0, *)
    private func runArm(
        signal: ActempoSignal,
        arm: ActempoArm,
        episodeId: String,
        annotation: CorpusAnnotation,
        audioURL: URL,
        transcript: [TranscriptChunk],
        tap: BrandAppearanceChannelTapObserver,
        temporalObserver: TemporalRegularizationObserver
    ) async throws -> [AdWindow] {
        guard let localURL = LocalAudioURL(audioURL) else {
            throw ActempoABRunError(reason: "audio at \(audioURL.path) is not a file URL")
        }

        // Fresh store per arm/episode so the runs never observe each other's
        // persisted ad windows or planner state.
        let storeDir = try makeTempDir(prefix: "ActempoAB-\(signal.rawValue)-\(arm.rawValue)")
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        // Asset row. `analysisAssetId == episodeId` so the scored ad windows read
        // back under the id the accumulator buckets on (and the id the observers
        // record under).
        let assetId = episodeId
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "actempo-fp-\(episodeId)",
            weakFingerprint: nil,
            sourceURL: localURL.absoluteString,
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        let chunks = transcript.map { rebindChunk($0, to: assetId) }
        try await store.insertTranscriptChunks(chunks)

        // PERSIST FEATURE WINDOWS: decode → extract → persist. The CoveragePlanner
        // + acoustic-break snapping read these back during the FM phase, and the
        // xsdz.8 detector reads them too — so they must be present.
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
            throw ActempoABRunError(reason: "feature extraction produced 0 windows for \(episodeId)")
        }
        try await store.insertFeatureWindows(featureWindows)

        let episodeDuration = shards.reduce(0.0) { max($0, $1.startTime + $1.duration) }

        // PRECONDITION — seed the planner into `targetedWithAudit`. Symmetric
        // across arms, so it cannot bias the comparison.
        try await seedTargetedWithAuditPlannerState(store: store, podcastId: annotation.showName)

        // Per-arm config: the single source of the one signal flag. Every other
        // field is held identical (= production `.default`) across both arms.
        let config = ActempoArmConfig.adDetectionConfig(signal: signal, for: arm)
        let narrowingConfig = ActempoArmConfig.narrowingConfig(for: arm)

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            backfillJobRunnerFactory: Self.makeLiveRunnerFactory(narrowingConfig: narrowingConfig),
            canUseFoundationModelsProvider: { true }, // avoid silent FM demotion
            // Behavior-neutral fire observers (non-nil ONLY here; nil in
            // production). The channel tap counts the xsdz.8 `.audioForensics`
            // ledger entry; the temporal-reg observer counts xsdz.10
            // penalty-applied spans. Declared after canUseFoundationModelsProvider
            // to match the init order.
            brandAppearanceChannelTapObserver: tap,
            temporalRegularizationObserver: temporalObserver,
            approvedCohortRegistry: nil // avoid cohort-gated FM demotion
        )

        // GUARD #1 (FM-mode gating): confirm cohort/FM gating did not silently
        // demote `.full` → `.off`. Else the FM phase never runs and the A/B is a
        // meaningless zero.
        let effectiveMode = await service.effectiveFMBackfillModeForTesting()
        XCTAssertEqual(
            effectiveMode,
            .full,
            "signal=\(signal.rawValue) arm=\(arm.rawValue): effective FM mode demoted off .full — FM phase would not run"
        )

        // GUARD #2 (planner regime): confirm the seeded state lands the planner in
        // `targetedWithAudit` — the production-like FM regime.
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
            "signal=\(signal.rawValue) arm=\(arm.rawValue): planner is not in targetedWithAudit (observed=\(seededContext.observedEpisodeCount) stableRecall=\(seededContext.stableRecall)) — the FM scan would not match production"
        )

        // SCORED RUN: a single backfill.
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: annotation.showName,
            episodeDuration: episodeDuration
        )

        return try await store.fetchAdWindows(assetId: assetId)
    }

    // MARK: - Planner-state seeding

    /// Seed `store`'s `PodcastPlannerState` into the warmed `targetedWithAudit`
    /// regime. Identical to the sibling harnesses (same regime required).
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
    /// `BackfillJobRunner` with `narrowingConfig` baked in (identical across arms
    /// = `NarrowingConfig.default`). Mirrors the sibling harnesses.
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
                classifier: FoundationModelClassifier(),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: makeTestScanCohortJSON(promptLabel: "actempo-ab"),
                sensitiveRouter: router,
                permissiveClassifier: permissiveClassifierBox,
                narrowingConfig: narrowingConfig
            )
        }
    }

    // MARK: - Chunk rebinding

    /// Return a copy of `chunk` with its `analysisAssetId` re-pointed at `assetId`.
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
private struct ActempoABRunError: Error {
    let reason: String
}
