// CrossEpisodeMemoryPerShowThresholdLiveABTests.swift
// playhead-fbsignals — env-gated Mac Catalyst A/B tests that measure the two
// FEEDBACK/MEMORY-driven off-by-default precision signals through the REAL
// `AdDetectionService.runBackfill` (fmBackfillMode: .full), scored against the
// 12-episode dogfood golden corpus.
//
// SPLIT BY SIGNAL (one signal per Catalyst pass): each signal gets its OWN
// focused 2-arm method (its own independent `.baseline` arm + its one treatment
// arm), so the orchestrator can run, score, and read each signal one at a time:
//   * `testCrossEpisodeMemoryABAcrossDogfoodCorpus` — [.baseline, .treatment] for
//     xsdz.9, dumps `playhead-dogfood-diagnostics-crossepisode-ab.json`.
//   * `testPerShowThresholdABAcrossDogfoodCorpus` — [.baseline, .treatment] for
//     xsdz.11, dumps `playhead-dogfood-diagnostics-pershowthreshold-ab.json`.
//
// ★ GOAL / EXPECTATION (the whole point of this A/B) ★
//   Both signals are FEEDBACK-DRIVEN. On a PURE BACKFILL A/B with NO user actions
//   they are STRUCTURALLY INERT:
//     * xsdz.9 — the HARD-NEGATIVE fingerprint bank only gets writes when a user
//       REVERTS an auto-skip (a confirmed false positive). With no user reverts
//       the bank stays EMPTY, so the suppression READ aligns against nothing and
//       can never move a `skipConfidence`. (The positive `AdCopyFingerprintStore`
//       ships nil in production, so the positive boost is wired nil here too.)
//     * xsdz.11 — the per-show PI controller only accumulates an offset when the
//       user corrects (listens-through an auto-skip = FP, or scrubs through
//       undetected content = miss). With no user actions the controller store
//       stays EMPTY, so the resolved offset is always 0 and the auto-skip gate is
//       unchanged.
//   So the EXPECTED result is 0 fires and NO metric effect beyond FM intra-run
//   noise (a ≤±2 FP delta is noise on this corpus). This A/B EMPIRICALLY CONFIRMS
//   the cold-start inertness rather than assuming it. The stores are therefore
//   wired the way PRODUCTION does at cold-start (constructed-but-EMPTY, gated on
//   the flag) — NO seeding, NO correction injection (that is a separate future
//   effort). The baseline arms construct NO store (gating parity with production,
//   which builds each store iff the flag is on).
//
// ARMS (`FbsignalsArm`) — every non-toggle field == production `.default`:
//   * baseline  — the signal under test OFF (= production). NO store. Anchors
//     every delta.
//   * treatment — `.default` + the one signal under test enabled, with its
//     production-consequence store wired EMPTY (cold-start parity). NO seeding.
// The one-axis isolation is pinned hermetically on the sim by
// `FbsignalsArmConfigTests` (no env var needed).
//
// ★ per-signal FIRE instrumentation (so the expected-0 result is interpretable) ★
//   A metric delta ≤±2 FP is FM intra-run NOISE on this corpus, so each A/B MUST
//   report whether the signal actually FIRED, else the (expected-0) result is
//   uninterpretable. Both signals report fire counts via behavior-neutral,
//   nil-default observers that are nil in production:
//   * xsdz.9 (TWO fire mechanisms):
//       (a) POSITIVE boost (a `.crossEpisodeMemory` LEDGER ENTRY): counted via the
//           nil-default `BrandAppearanceChannelTapObserver` (generalized to count
//           `.crossEpisodeMemory` entries), which reads the SAME pre-suppression
//           ledger the decision builds from. Expected 0 (positive store nil).
//       (b) HARD-NEGATIVE SUPPRESSION (a POST-FUSION penalty, NOT a ledger entry):
//           counted via the nil-default `NegativeBankSuppressionObserver`, which
//           records, per span reached while a bank was wired, whether the
//           suppression actually moved the `skipConfidence`. Expected 0 (empty
//           bank ⇒ no alignment ⇒ no suppression).
//   * xsdz.11 (per-show OFFSET, NOT a ledger entry): counted via the nil-default
//     `PerShowThresholdOffsetObserver`, which records the resolved per-show offset
//     per backfill and how many `.standard`-track spans the offset shifted the
//     effective threshold for. Expected offsetSum=0 and 0 shifted spans (empty
//     controller ⇒ offset 0).
//   The fire counts land in each JSON. All three observers are nil at the sole
//   production `PlayheadRuntime` construction site (decisions byte-identical).
//
// PRECONDITION (the FM path must run like production): the live targeted-phase
// narrowing inside `BackfillJobRunner` is guarded by
// `plan.policy == .targetedWithAudit`; a fresh store is cold-start
// (`fullCoverage`). So each arm SEEDS the planner into the warmed
// `targetedWithAudit` regime before the scored run. Symmetric across arms, so it
// cannot bias the comparison. (Note: this seeds the PLANNER state — it does NOT
// seed either feedback store, which stay EMPTY by design.)
//
// Gating (mirrors `AudioForensicsTemporalRegLiveABTests` / `BrandAppearanceLiveABTests`):
//   `PLAYHEAD_FBSIGNALS_AB=1` MUST be set in the test process environment. The
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
//        # cross-episode memory (xsdz.9)
//        xcodebuild test \
//          -scheme Playhead -testPlan PlayheadFastTests \
//          -destination 'platform=macOS,variant=Mac Catalyst' \
//          -only-testing:'PlayheadTests/CrossEpisodeMemoryPerShowThresholdLiveABTests/testCrossEpisodeMemoryABAcrossDogfoodCorpus' \
//          PLAYHEAD_FBSIGNALS_AB=1
//        # per-show threshold control (xsdz.11)
//        xcodebuild test \
//          -scheme Playhead -testPlan PlayheadFastTests \
//          -destination 'platform=macOS,variant=Mac Catalyst' \
//          -only-testing:'PlayheadTests/CrossEpisodeMemoryPerShowThresholdLiveABTests/testPerShowThresholdABAcrossDogfoodCorpus' \
//          PLAYHEAD_FBSIGNALS_AB=1
//   4. Read the printed 2-row table; the JSON dumps land at the repo root as
//      `playhead-dogfood-diagnostics-crossepisode-ab.json` and
//      `playhead-dogfood-diagnostics-pershowthreshold-ab.json` (git-ignored — they
//      match the `playhead-dogfood-diagnostics-*.json` pattern in .gitignore).
//
// Hermetic helpers (`FbsignalsSignal`, `FbsignalsArm`, `FbsignalsArmConfig`,
// `FbsignalsFireTally`, `FbsignalsSweepReport`, `FusionLiftModeAccumulator`) live
// in `FusionLiftHarnessSupport.swift` and are unit-tested on the simulator by
// `FbsignalsArmConfigTests`. Scoring reuses `FusionLiftScoring.swift`. The
// empty-store wiring mirrors the xsdz.13 `CrossShowSyndicationStore` template in
// `BrandAppearanceLiveABTests` — but unlike xsdz.13, NOTHING is seeded.

import AVFoundation
import Foundation
import XCTest
@testable import Playhead

final class CrossEpisodeMemoryPerShowThresholdLiveABTests: XCTestCase {

    /// Gate every test on the A/B env var. The default scheme does not set it,
    /// so the test body is a no-op on Cmd-U / the default sim suite.
    private static var abEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_FBSIGNALS_AB"] == "1"
    }

    override func setUp() async throws {
        try await super.setUp()
        try XCTSkipUnless(
            Self.abEnabled,
            """
            Feedback/memory-driven precision-signal A/B is opt-in and SLOW (each \
            single-signal method runs ~24 full-FM passes: 1 scored pass × 2 arms × \
            12 episodes). Set PLAYHEAD_FBSIGNALS_AB=1 in the test plan env vars and \
            run on Mac Catalyst (or an iOS 26 device) with Apple Intelligence \
            enabled and corpus audio + transcripts staged. See the file header for \
            the full invocation recipe.
            """
        )
    }

    // MARK: - A/B entry points (one focused method per signal)

    /// playhead-xsdz.9 — cross-episode "memory" (`crossEpisodeMemoryEnabled`),
    /// with the negative fingerprint bank wired EMPTY (cold-start parity) for the
    /// suppression READ and the positive store left nil (as production ships).
    /// Runs its OWN baseline + the treatment arm, and dumps a focused 2-arm report
    /// with the positive-boost + negative-suppression fire counts per arm.
    func testCrossEpisodeMemoryABAcrossDogfoodCorpus() async throws {
        try await runABPass(
            signal: .crossEpisodeMemory,
            dumpFilename: "playhead-dogfood-diagnostics-crossepisode-ab.json"
        )
    }

    /// playhead-xsdz.11 — per-show auto-skip threshold control
    /// (`perShowThresholdControlEnabled`), with the controller store wired EMPTY
    /// (cold-start parity) for the per-show offset READ. Runs its OWN baseline +
    /// the treatment arm, and dumps a focused 2-arm report with the
    /// threshold-shifted-span + resolved-offset fire counts per arm.
    func testPerShowThresholdABAcrossDogfoodCorpus() async throws {
        try await runABPass(
            signal: .perShowThreshold,
            dumpFilename: "playhead-dogfood-diagnostics-pershowthreshold-ab.json"
        )
    }

    // MARK: - Shared A/B pass

    /// Run a single Catalyst A/B pass for `signal` over `[.baseline, .treatment]`
    /// (baseline first), score each arm vs the golden, and dump a focused report
    /// to `dumpFilename`. Each invocation is fully self-contained — it builds its
    /// own accumulators + fire tallies + its OWN baseline — so two methods calling
    /// this never share state.
    private func runABPass(
        signal: FbsignalsSignal,
        dumpFilename: String
    ) async throws {
        guard #available(iOS 26.0, *) else {
            throw XCTSkip("Live FM BackfillJobRunner requires iOS 26+ / FoundationModels.")
        }
        let arms: [FbsignalsArm] = [.baseline, .treatment]

        let loader = CorpusAnnotationLoader()

        let annotationURLs: [URL]
        do {
            annotationURLs = try loader.annotationFileURLs()
        } catch {
            throw XCTSkip("corpus annotations dir not present: \(error)")
        }
        try XCTSkipIf(annotationURLs.isEmpty, "no corpus annotations staged")

        // One accumulator + one fire tally per arm IN THIS PASS.
        var accumulators: [FbsignalsArm: FusionLiftModeAccumulator] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, FusionLiftModeAccumulator()) }
        )
        var fireTallies: [FbsignalsArm: FbsignalsFireTally] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, FbsignalsFireTally()) }
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

            // Run BOTH arms for this episode. Each arm gets its own fresh analysis
            // store / asset / features / transcript / seeded planner state, AND
            // (for the treatment arm) its own fresh EMPTY feedback store, so the
            // only between-arm difference is the one signal flag + its store
            // consequence. An arm failure aborts the whole episode (so no arm is
            // scored on a partial episode that would bias the comparison).
            do {
                for arm in arms {
                    // Behavior-neutral fire observers (non-nil ONLY here; nil in
                    // production). Each is attached for BOTH arms so the baseline's
                    // fire count (expected 0) is MEASURED, not assumed.
                    let tap = BrandAppearanceChannelTapObserver()
                    let suppressionObserver = NegativeBankSuppressionObserver()
                    let offsetObserver = PerShowThresholdOffsetObserver()

                    let windows = try await runArm(
                        signal: signal,
                        arm: arm,
                        episodeId: episodeId,
                        annotation: annotation,
                        audioURL: audioURL,
                        transcript: transcript,
                        tap: tap,
                        suppressionObserver: suppressionObserver,
                        offsetObserver: offsetObserver
                    )
                    accumulators[arm]?.addEpisode(
                        annotationWindows: annotation.adWindows,
                        adWindows: windows,
                        podcastId: annotation.showName,
                        episodeId: episodeId
                    )
                    // Fold the per-signal fire counts for this episode into the arm
                    // total. Only the measured signal's counters are meaningful, but
                    // folding all is harmless (the unused ones stay 0).
                    let tapCounts = await tap.fireCounts(for: episodeId)
                    fireTallies[arm]?.addChannelTap(tapCounts)
                    let suppressionCounts = await suppressionObserver.fireCounts(for: episodeId)
                    fireTallies[arm]?.addNegativeBankSuppression(suppressionCounts)
                    let offsetCounts = await offsetObserver.fireCounts(for: episodeId)
                    fireTallies[arm]?.addPerShowThreshold(offsetCounts)
                }
                scored.append(episodeId)
            } catch let error as FbsignalsABRunError {
                failed.append((episodeId, error.reason))
            } catch {
                failed.append((episodeId, "arm run failed: \(error.localizedDescription)"))
            }
        }

        // Build + print + dump the report over EXACTLY the arms this pass ran.
        let report = FbsignalsSweepReport(
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
        print("Fbsignals \(signal.label) A/B JSON: \(dumpURL.path) (git-ignored)")

        // Fail loud on hard failures; fail loud if nothing scored (env set but
        // every episode landed in skipped — audio/transcript not staged).
        if !failed.isEmpty {
            XCTFail("Hard failures: \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))")
        } else if scored.isEmpty {
            XCTFail(
                """
                PLAYHEAD_FBSIGNALS_AB=1 was set but no episodes scored — every \
                episode landed in `skipped` because audio or the transcript \
                sidecar is not staged. See the file header for the staging recipe.
                """
            )
        }
    }

    // MARK: - One arm

    /// Run the scored backfill once for a single episode in the requested arm of
    /// `signal` and return the persisted ad windows. Every arm decodes the same
    /// audio, persists the same feature windows + transcript chunks, SEEDs the
    /// planner into the warmed `targetedWithAudit` regime, and uses
    /// `NarrowingConfig.default`. The ONLY things that differ are the one signal's
    /// flag AND (its production consequence) whether the matching feedback store is
    /// wired. Always uses `fmBackfillMode: .full`. All three behavior-neutral fire
    /// observers are injected for every arm (they only record).
    ///
    /// ★ EMPTY-STORE WIRING (cold-start parity, NO seeding) ★
    ///   For the treatment arm the matching feedback store is constructed FRESH
    ///   and EMPTY (migrated, never written), gated on the signal — exactly as
    ///   `PlayheadRuntime` does at cold-start. The baseline arm constructs NO store
    ///   (gating parity with production, which builds each store iff its flag is
    ///   on). NOTHING is seeded: the negative bank gets no fingerprints and the
    ///   controller gets no corrections, because measuring the real cold-start
    ///   (expected-0-fire) behavior is the whole point.
    @available(iOS 26.0, *)
    private func runArm(
        signal: FbsignalsSignal,
        arm: FbsignalsArm,
        episodeId: String,
        annotation: CorpusAnnotation,
        audioURL: URL,
        transcript: [TranscriptChunk],
        tap: BrandAppearanceChannelTapObserver,
        suppressionObserver: NegativeBankSuppressionObserver,
        offsetObserver: PerShowThresholdOffsetObserver
    ) async throws -> [AdWindow] {
        guard let localURL = LocalAudioURL(audioURL) else {
            throw FbsignalsABRunError(reason: "audio at \(audioURL.path) is not a file URL")
        }

        // Fresh ANALYSIS store per arm/episode so the runs never observe each
        // other's persisted ad windows or planner state.
        let storeDir = try makeTempDir(prefix: "FbsignalsAB-\(signal.rawValue)-\(arm.rawValue)")
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        // Asset row. `analysisAssetId == episodeId` so the scored ad windows read
        // back under the id the accumulator buckets on (and the id the observers
        // record under).
        let assetId = episodeId
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fbsignals-fp-\(episodeId)",
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
        // + acoustic-break snapping read these back during the FM phase.
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
            throw FbsignalsABRunError(reason: "feature extraction produced 0 windows for \(episodeId)")
        }
        try await store.insertFeatureWindows(featureWindows)

        let episodeDuration = shards.reduce(0.0) { max($0, $1.startTime + $1.duration) }

        // PRECONDITION — seed the PLANNER into `targetedWithAudit`. Symmetric
        // across arms, so it cannot bias the comparison. (This seeds the planner
        // state ONLY — it does NOT seed either feedback store.)
        try await seedTargetedWithAuditPlannerState(store: store, podcastId: annotation.showName)

        // EMPTY feedback stores — constructed-but-EMPTY for the treatment arm,
        // gated on the signal, mirroring PlayheadRuntime cold-start. NO seeding.
        // Baseline (or the other signal's treatment) gets nil (gating parity).
        var negativeBank: NegativeFingerprintBank?
        if arm.signalEnabled && signal.requiresNegativeFingerprintBank {
            let bankDir = try makeTempDir(prefix: "FbsignalsAB-negbank-\(arm.rawValue)")
            let bank = try NegativeFingerprintBank(directoryURL: bankDir)
            try await bank.migrate()
            negativeBank = bank
        }
        var perShowStore: PerShowThresholdControllerStore?
        if arm.signalEnabled && signal.requiresPerShowThresholdControllerStore {
            let psDir = try makeTempDir(prefix: "FbsignalsAB-pershow-\(arm.rawValue)")
            let ps = try PerShowThresholdControllerStore(
                directoryURL: psDir,
                parameters: AdDetectionConfig.default.perShowThresholdControllerParameters
            )
            try await ps.migrate()
            perShowStore = ps
        }

        // Per-arm config: the single source of the one signal flag. Every other
        // field is held identical (= production `.default`) across both arms.
        let config = FbsignalsArmConfig.adDetectionConfig(signal: signal, for: arm)
        let narrowingConfig = FbsignalsArmConfig.narrowingConfig(for: arm)

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            backfillJobRunnerFactory: Self.makeLiveRunnerFactory(narrowingConfig: narrowingConfig),
            canUseFoundationModelsProvider: { true }, // avoid silent FM demotion
            // Behavior-neutral fire observers (non-nil ONLY here; nil in
            // production). The channel tap counts the xsdz.9 `.crossEpisodeMemory`
            // POSITIVE-boost ledger entry; the suppression observer counts xsdz.9
            // negative-bank suppressions; the offset observer counts xsdz.11
            // per-show offset shifts. Declared after canUseFoundationModelsProvider
            // to match the init order.
            brandAppearanceChannelTapObserver: tap,
            negativeBankSuppressionObserver: suppressionObserver,
            perShowThresholdOffsetObserver: offsetObserver,
            // playhead-xsdz.9: the negative bank wired EMPTY for the suppression
            // READ (nil unless this is the crossEpisodeMemory treatment arm —
            // gating parity with production). The positive `adCopyFingerprintStore`
            // is left nil exactly as production ships it.
            negativeFingerprintBank: negativeBank,
            // playhead-xsdz.11: the per-show controller store wired EMPTY for the
            // per-show offset READ (nil unless this is the perShowThreshold
            // treatment arm — gating parity with production).
            perShowThresholdControllerStore: perShowStore,
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

        let windows = try await store.fetchAdWindows(assetId: assetId)

        // Deterministic teardown of any wired feedback store (idempotent close).
        if let perShowStore { await perShowStore.close() }

        return windows
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
                scanCohortJSON: makeTestScanCohortJSON(promptLabel: "fbsignals-ab"),
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
private struct FbsignalsABRunError: Error {
    let reason: String
}
