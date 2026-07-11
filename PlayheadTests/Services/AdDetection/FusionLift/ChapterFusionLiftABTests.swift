// ChapterFusionLiftABTests.swift
// playhead-au2v.1.27 — Phase C: env-gated Mac Catalyst A/B test that
// measures the chapter signal's effect on REAL ad detection.
//
// What this measures (and why it is not a meaningless zero):
//   The chapter signal can only influence persisted ad windows through
//   this path —
//       chapter evidence
//         → CoveragePlannerContext (Phase B wire-in,
//           `AdDetectionService.resolveChapterEvidenceForShadowPhase`,
//           gated on `chapterSignalMode == .enabled`)
//         → CoveragePlanner steers which windows the FM scans
//         → SemanticScanResults → fusion ledger → persisted ad windows.
//   That FM scan phase only runs when `effectiveFMBackfillMode != .off`,
//   and the persisted ad windows the fusion ledger writes only carry FM
//   refinement when the FM phase contributes to fusion. THEREFORE both
//   arms run with `fmBackfillMode: .full` (real FM ad scanning feeding the
//   fusion ledger → persisted ad windows); the ONLY thing that varies is
//   `chapterSignalMode`:
//     * baseline   — chapterSignalMode: .off,     fmBackfillMode: .full
//     * treatment  — chapterSignalMode: .enabled, fmBackfillMode: .full,
//                    with the chapter-generation phase wired (REAL FM
//                    labeler) so a `ChapterPlan` is produced and threaded
//                    into the planner context.
//   If the FM mode were `.off` the FM phase never runs and chapter
//   steering can't affect predictions — the A/B would be a guaranteed
//   zero. That is the single most important design constraint.
//
//   NOTE on the mode name: the bead text says `fmBackfillMode: .enabled`,
//   but `FMBackfillMode` (FMBackfillMode.swift) has NO `.enabled` case —
//   the legacy string `"enabled"` decodes to `.shadow`, which is
//   observation-only and does NOT feed FM refinement into the persisted
//   fusion windows. The mode that actually runs the FM scan AND lets the
//   chapter-steered scan reach the persisted `ad_windows` rows (the
//   `store.insertAdWindows` fusion path the A/B scores) is `.full` — the
//   Phase-6 fusion mode. We use `.full` so the A/B measures a real effect
//   on persisted predictions rather than a guaranteed zero.
//
// Gating (mirrors `ChapterLabelingDiagnosticTests`):
//   `PLAYHEAD_CHAPTER_FUSION_LIFT_AB=1` MUST be set in the test process
//   environment. The default `PlayheadFastTests` plan does NOT set it, so
//   the test body is a no-op on Cmd-U (via `XCTSkipUnless`) — no FM, no
//   audio dependency triggered. The live FM labeler also requires iOS 26+
//   (`#available`), so an older runtime skips too.
//
// Two preconditions make the chapter signal OBSERVABLE in persisted windows
// (both confirmed by reading production code — get either wrong and the A/B
// reads a guaranteed zero):
//
//   PRECONDITION A — the planner must be in `targetedWithAudit`.
//   `CoveragePlanner.plan(...)` only consults `chapterEvidence` on a
//   `targetedWithAudit` plan; `fullCoverage` (the cold-start policy) and
//   `periodicFullRescan` ignore it entirely and scan the whole episode the
//   same way regardless of chapter steering. `targetedWithAudit` requires
//   `observedEpisodeCount >= 5` AND `stableRecall == true` (3 full-rescan
//   recall samples ≥ 0.85) in the persisted `PodcastPlannerState`. A FRESH
//   store has no row ⇒ cold-start (`observedEpisodeCount == 0`) ⇒
//   `fullCoverage` ⇒ chapter steering can never fire. So each arm SEEDS the
//   planner state into the warmed `targetedWithAudit` regime (the only
//   regime where the chapter signal does anything) before the scored run.
//
//   PRECONDITION B — the chapter plan must already be in the cache when the
//   scored backfill's FM scan reads it. Inside ONE `runBackfill`, the FM
//   scan phase (which reads chapter evidence into the planner context) runs
//   BEFORE the chapter-generation phase (which writes the plan). So a single
//   backfill can NEVER consume a plan it writes in the same run. Worse, a
//   naive "run twice and score the second" does NOT help: the FM job ids are
//   `hash(asset, transcriptVersion, phase, offset)` — independent of which
//   windows a phase narrows to — so pass-2's job ids equal pass-1's, pass-1
//   left them `.complete`, and the M5 idempotency check skips them
//   wholesale. Pass 2 runs ZERO new FM scans; its persisted windows == pass
//   1's (the un-steered baseline). The chapter-steered scan would never
//   execute. Therefore the TREATMENT arm pre-seeds the plan into the cache
//   by running the `ChapterGenerationPhase` ONCE up front (the same LIVE FM
//   labeler production uses), then runs the scored backfill EXACTLY ONCE so
//   its `scanRandomAuditWindows` phase reads the pre-present plan and steers.
//   The baseline runs the same single backfill with no plan. Each arm has
//   its OWN fresh store, so there is no cross-arm job-id idempotency
//   collision — the only between-arm difference is the chapter signal.
//
// Cost (READ before running): this runs the REAL `runBackfill` with full
// FM ad scanning once per arm × 2 arms × 12 episodes (~24 full-FM passes,
// plus 12 chapter-generation passes for the treatment arm) —
// on the order of a few hours on Mac Catalyst. It is an ORCHESTRATOR-run
// step, NOT a green gate. To invoke:
//
//   1. Stage corpus audio under `TestFixtures/Corpus/Audio/<episode_id>.<ext>`
//      and whisper transcripts under `TestFixtures/Corpus/Transcripts/`.
//   2. Enable Apple Intelligence on the host Mac (FM labeler is on-device).
//   3. Run on Mac Catalyst with the env var set:
//        xcodebuild test \
//          -scheme Playhead -testPlan PlayheadFastTests \
//          -destination 'platform=macOS,variant=Mac Catalyst' \
//          -only-testing:'PlayheadTests/ChapterFusionLiftABTests' \
//          PLAYHEAD_CHAPTER_FUSION_LIFT_AB=1
//   4. Read the printed lift table; the per-run JSON dump lands at the repo
//      root as `playhead-dogfood-diagnostics-chapter-fusion-lift.json`
//      (git-ignored — see `.gitignore`).
//
// Hermetic helpers (`FusionLiftTranscriptVersion`, `FusionLiftModeAccumulator`,
// `FusionLiftReport`) live in `FusionLiftHarnessSupport.swift` and are unit-
// tested on the simulator by `FusionLiftHarnessSupportTests`. Scoring reuses
// Phase A's `FusionLiftScoring.swift` (no reimplementation).

import AVFoundation
import Foundation
import XCTest
@testable import Playhead

final class ChapterFusionLiftABTests: XCTestCase {

    /// Gate every test on the A/B env var. The default scheme does not set
    /// it, so the test body is a no-op on Cmd-U / the default sim suite.
    private static var abEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_CHAPTER_FUSION_LIFT_AB"] == "1"
    }

    override func setUp() async throws {
        try await super.setUp()
        try XCTSkipUnless(
            Self.abEnabled,
            """
            Chapter-fusion lift A/B is opt-in and SLOW (~24 full-FM \
            passes: 1 scored pass × 2 arms × 12 episodes, plus a \
            chapter-generation pass per treatment episode). Set \
            PLAYHEAD_CHAPTER_FUSION_LIFT_AB=1 \
            in the test plan env vars and run on Mac Catalyst (or an \
            iOS 26 device) with Apple Intelligence enabled and corpus \
            audio + transcripts staged. See file header for the full \
            invocation recipe.
            """
        )
    }

    // MARK: - A/B entry

    func testChapterFusionLiftAcrossDogfoodCorpus() async throws {
        guard #available(iOS 26.0, *) else {
            throw XCTSkip("ChapterLabelingService.live requires iOS 26+ / FoundationModels.")
        }

        let corpusLoader = CorpusAnnotationLoader()
        let goldens = try ChapterPlanGoldenSetLoader.canonicalDogfoodFixtures(
            corpusLoader: corpusLoader
        )
        try corpusLoader.preflightGoldEvaluationInputs(
            annotationURLs: try corpusLoader.annotationFileURLs()
        )

        var offAccumulator = FusionLiftModeAccumulator()
        var enabledAccumulator = FusionLiftModeAccumulator()

        var scored: [String] = []
        var skipped: [(episodeId: String, reason: String)] = []
        var failed: [(episodeId: String, reason: String)] = []

        for fixture in goldens {
            let goldenURL = fixture.url
            let golden = fixture.set
            let episodeId = goldenURL.deletingPathExtension().lastPathComponent

            // Resolve annotation (ground truth) + audio + transcript.
            let annotation = fixture.annotation

            let audioURL: URL
            do {
                audioURL = try corpusLoader.audioFileURL(for: annotation)
            } catch {
                skipped.append((
                    episodeId,
                    "audio file not staged under \(corpusLoader.audioDirectoryURL.path)"
                ))
                continue
            }

            // Audio-fingerprint check (mirrors the capture test): the
            // golden's `episodeContentHash` is the hex suffix of the
            // recorded `sha256:` fingerprint. A mismatch means a different
            // cut than the annotation references — scoring that would
            // compare predictions against the wrong ground truth.
            do {
                let fingerprint = try CorpusAudioFingerprint.fingerprint(of: audioURL)
                let expected = "\(CorpusAudioFingerprint.prefix)\(golden.episodeContentHash)"
                guard fingerprint == expected else {
                    failed.append((
                        episodeId,
                        "audio fingerprint \(fingerprint) ≠ golden \(expected)"
                    ))
                    continue
                }
            } catch {
                failed.append((episodeId, "fingerprint compute failed: \(error.localizedDescription)"))
                continue
            }

            let transcript: [TranscriptChunk]
            do {
                transcript = try CorpusTranscriptLoader.load(
                    episodeId: episodeId,
                    repoRoot: corpusLoader.repoRoot
                )
            } catch {
                failed.append((episodeId, "transcript decode failed: \(error.localizedDescription)"))
                continue
            }
            guard !transcript.isEmpty else {
                skipped.append((episodeId, "transcript sidecar empty/absent"))
                continue
            }

            // Run BOTH arms.
            do {
                let baseline = try await runArm(
                    arm: .off,
                    episodeId: episodeId,
                    annotation: annotation,
                    audioURL: audioURL,
                    transcript: transcript
                )
                offAccumulator.addEpisode(
                    annotationWindows: annotation.adWindows,
                    adWindows: baseline,
                    podcastId: annotation.showName,
                    episodeId: episodeId
                )

                let treatment = try await runArm(
                    arm: .enabled,
                    episodeId: episodeId,
                    annotation: annotation,
                    audioURL: audioURL,
                    transcript: transcript
                )
                enabledAccumulator.addEpisode(
                    annotationWindows: annotation.adWindows,
                    adWindows: treatment,
                    podcastId: annotation.showName,
                    episodeId: episodeId
                )
                scored.append(episodeId)
            } catch let error as ABRunError {
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
                "PLAYHEAD_CHAPTER_FUSION_LIFT_AB=1 was set but no episodes scored; all staged inputs were skipped"
            )
            return
        }

        // Build + print + dump the lift report.
        let report = FusionLiftReport(
            episodeCount: scored.count,
            off: offAccumulator,
            enabled: enabledAccumulator
        )
        print(report.table())
        print("""
        scored=\(scored.count): \(scored.sorted().joined(separator: ", "))
        skipped (audio/transcript missing)=\(skipped.count): \(skipped.map(\.episodeId).sorted().joined(separator: ", "))
        failed=\(failed.count): \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))
        """)

        // Git-ignored JSON dump at repo root (matches the
        // `playhead-dogfood-diagnostics-*.json` gitignored pattern).
        let dumpURL = corpusLoader.repoRoot.appendingPathComponent(
            "playhead-dogfood-diagnostics-chapter-fusion-lift.json",
            isDirectory: false
        )
        try report.jsonData().write(to: dumpURL, options: .atomic)
        print("Lift summary JSON: \(dumpURL.path) (git-ignored)")

    }

    // MARK: - One arm

    /// Run the scored backfill once for a single episode in the requested
    /// arm and return the persisted ad windows. Both arms decode the same
    /// audio, persist the same feature windows + transcript chunks, and SEED
    /// the planner state into the warmed `targetedWithAudit` regime (the only
    /// regime where the chapter signal influences the scan). Only the
    /// `chapterSignalMode` and the pre-seeded chapter plan differ. Always
    /// uses `fmBackfillMode: .full` (see file header NOTE).
    @available(iOS 26.0, *)
    private func runArm(
        arm: FusionLiftArm,
        episodeId: String,
        annotation: CorpusAnnotation,
        audioURL: URL,
        transcript: [TranscriptChunk]
    ) async throws -> [AdWindow] {
        guard let localURL = LocalAudioURL(audioURL) else {
            throw ABRunError(reason: "audio at \(audioURL.path) is not a file URL")
        }

        // Fresh store per arm so the two runs never observe each other's
        // persisted windows.
        let storeDir = try makeTempDir(prefix: "ChapterFusionLiftAB-\(arm.rawValue)")
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        // Asset row. `analysisAssetId == episodeId` here so the chapter
        // phase's asset lookup resolves the episode id we expect.
        let assetId = episodeId
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fusion-lift-fp-\(episodeId)",
            weakFingerprint: nil,
            sourceURL: localURL.absoluteString,
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        // Re-stamp transcript chunks onto this asset id (the corpus loader
        // stamps `analysisAssetId = episodeId` already; this is defensive in
        // case the loader convention changes).
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
            throw ABRunError(reason: "feature extraction produced 0 windows for \(episodeId)")
        }
        try await store.insertFeatureWindows(featureWindows)

        // Episode duration from the decoded shard span (last shard end).
        let episodeDuration = shards.reduce(0.0) { max($0, $1.startTime + $1.duration) }

        // PRECONDITION A — seed the planner state into the warmed
        // `targetedWithAudit` regime. `CoveragePlanner.plan(...)` only reads
        // `chapterEvidence` on a `targetedWithAudit` plan; cold-start
        // (`observedEpisodeCount == 0`) yields `fullCoverage`, which scans
        // the whole episode the same way for both arms and ignores chapter
        // evidence. To flip the planner we need `observedEpisodeCount >= 5`
        // (AnalysisStore.plannerStableObservedEpisodeFloor) AND
        // `stableRecall == true` (the recall ring full with
        // plannerRecallRingSize=3 samples all ≥ plannerRecallThreshold=0.85).
        // Five full-rescan observations with a 0.9 recall sample satisfy
        // both. This is symmetric across arms (identical podcastId per arm
        // store), so it cannot bias the comparison; the only between-arm
        // difference remains the chapter signal.
        try await seedTargetedWithAuditPlannerState(
            store: store,
            podcastId: annotation.showName
        )

        // CHURN RISK #1 (hash match): derive the transcript version from the
        // SAME chunks `runBackfill` will atomize, with the SAME pass filter
        // + norm/source hashes, so the chapter plan is cached under the key
        // the wire-in reads it back under. `CorpusTranscriptLoader` stamps
        // every chunk `pass == "final"`, so `finalChunks == chunks` and the
        // wire-in's full-chunk atomization (runShadowFMPhase) and this
        // final-pass-filtered derivation yield the same hash. Mismatch ⇒
        // silent cache miss ⇒ false zero.
        let transcriptVersion = FusionLiftTranscriptVersion.derive(
            chunks: chunks,
            analysisAssetId: assetId
        )

        // The shared cache for this arm. The pre-seed step (write) and the
        // service's wire-in resolver (read) target the SAME cache instance
        // so the plan round-trips.
        let cache = ChapterPlanCache(directory: try makeTempDir(prefix: "ChapterFusionLiftCache-\(arm.rawValue)"))

        // PRECONDITION B — TREATMENT pre-seeds the chapter plan into the
        // cache BEFORE the scored backfill, by running the REAL FM
        // `ChapterGenerationPhase` once up front. We cannot let the
        // in-backfill phase write it, because that phase (step 11.5) runs
        // AFTER the FM scan (steps 5–6) within the SAME backfill, so the
        // scan would never see it; and a second backfill is a no-op (the FM
        // job ids — hash(asset, transcriptVersion, phase, offset) — collide
        // with pass 1's `.complete` rows and the M5 idempotency check skips
        // them). With the plan pre-present, the single scored backfill's FM
        // scan reads it via `resolveChapterEvidenceForShadowPhase` and the
        // `targetedWithAudit` `scanRandomAuditWindows` phase steers.
        let eventSink = RecordingEventSink()
        if arm == .enabled {
            let snapshot = try await ChapterFeatureSnapshotBuilder.build(
                audioURL: audioURL,
                transcript: chunks,
                fmAvailable: true
            )
            let boundaryDetector = LiveBoundaryDetector(snapshot: snapshot)
            let candidates = try await boundaryDetector.detect()
            let phase = ChapterGenerationPhase(
                admissionPolicy: StubAdmissionPolicy(),
                creatorChapterProvider: StubCreatorChapterProvider(),
                boundaryDetector: boundaryDetector,
                labeler: LiveLabelerAdapter(
                    service: .live(),
                    transcript: chunks,
                    candidates: candidates
                ),
                // The plan is written under whatever hash this provider
                // returns; pin it to the wire-in's read key so the round-trip
                // lands (churn risk #1).
                transcriptHashProvider: StickyHashProvider(hash: transcriptVersion),
                cache: cache,
                eventSink: eventSink
            )
            let outcome = await phase.run(
                mode: .enabled,
                episodeId: episodeId,
                installID: UUID()
            )
            // CHURN RISK #1 + #3 (hash match + cache persistence): the plan
            // must be retrievable under the SAME key the wire-in reads. If
            // the phase did not produce a usable plan, the treatment arm
            // degenerates to baseline and the lift is a false zero — fail
            // loud rather than report a misleading number.
            guard case .cached = outcome else {
                throw ABRunError(
                    reason: "chapter generation did not produce a plan (outcome=\(outcome)); treatment would degenerate to baseline"
                )
            }
            let plan = await cache.get(contentHash: transcriptVersion)
            XCTAssertNotNil(
                plan,
                "treatment arm: ChapterPlanCache.get(contentHash: \(transcriptVersion)) is nil after pre-seed — plan was evicted/never written, treatment degenerates to baseline"
            )
            XCTAssertFalse(
                plan?.chapters.isEmpty ?? true,
                "treatment arm: cached plan has zero chapters — wire-in resolves to nil evidence, no steering"
            )
            let events = await eventSink.snapshot()
            let completed = events.filter { $0.eventType == .completed }.count
            XCTAssertEqual(
                completed, 1,
                "treatment arm: expected exactly one .completed chapter-phase event (got \(completed); events=\(events.map(\.eventType.rawValue)))"
            )
        }

        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "au2v.1.27-phasec",
            // CRITICAL: both arms scan FM for real AND feed the fusion
            // ledger. `.full` is the Phase-6 fusion mode; the bead's
            // ".enabled" has no enum case (it decodes to observation-only
            // .shadow) — see the file header NOTE.
            fmBackfillMode: .full,
            chapterSignalMode: arm == .enabled ? .enabled : .off
        )

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            // The FM scan phase only does real work when a runner factory is
            // wired (else `runShadowFMPhase` returns `.skipped`). Inject a
            // LIVE FM `BackfillJobRunner` so the chapter-steered CoveragePlanner
            // actually drives a real FM scan → fusion ledger → persisted ad
            // windows (mirrors PlayheadRuntime + PlayheadFMSmokeTests wiring).
            backfillJobRunnerFactory: Self.makeLiveRunnerFactory(),
            canUseFoundationModelsProvider: { true }, // avoid silent FM demotion
            // No in-backfill chapter-generation factory: the plan is
            // pre-seeded into `cache` above (treatment) so the scored
            // backfill's FM scan can read it. The in-backfill phase writes
            // AFTER the FM scan, so wiring it here would not affect the
            // scored run.
            chapterPlanCache: cache,
            chapterPhaseInstallIDProvider: { UUID() },
            approvedCohortRegistry: nil // avoid cohort-gated FM demotion
        )

        // CHURN RISK #2 (FM-mode gating): confirm the cohort/FM gating did
        // not silently demote `.full` → `.off`. With `approvedCohortRegistry`
        // nil + `canUseFoundationModelsProvider { true }`, the effective mode
        // must stay `.full`, else the FM phase never runs and the A/B is a
        // meaningless zero.
        let effectiveMode = await service.effectiveFMBackfillModeForTesting()
        XCTAssertEqual(
            effectiveMode,
            .full,
            "arm=\(arm.rawValue): effective FM mode demoted off .full — FM phase would not run"
        )

        // PRECONDITION A guard: confirm the seeded planner state actually
        // lands the planner in `targetedWithAudit` — the ONLY policy that
        // consults chapter evidence. If a future change to the planner
        // thresholds or the seeding helper regresses this to `fullCoverage`,
        // the chapter signal silently stops steering and the A/B reads a
        // false zero. Reproduce the planner's own decision from the
        // persisted state so the guard fails loud instead.
        let seededState = try await store.fetchPodcastPlannerState(podcastId: annotation.showName)
        // Mirror `makeShadowPhaseCoveragePlannerContext`'s mapping of the
        // persisted row (the four event flags are always false there for a
        // freshly seeded row, matching the live shadow-phase construction).
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
            "arm=\(arm.rawValue): planner is not in targetedWithAudit (observed=\(seededContext.observedEpisodeCount) stableRecall=\(seededContext.stableRecall)) — chapter evidence would be ignored and the A/B reads a false zero"
        )

        // SCORED RUN: a single backfill. With PRECONDITION A satisfied the
        // FM scan runs under `targetedWithAudit`; with PRECONDITION B
        // satisfied (treatment) the `scanRandomAuditWindows` phase reads the
        // pre-seeded plan via `resolveChapterEvidenceForShadowPhase` and
        // steers. Baseline runs the identical backfill with no plan. The
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
    /// `stableRecall == true` (recall ring full with samples ≥ 0.85). This
    /// is the ONLY regime in which `CoveragePlanner` consults chapter
    /// evidence; a fresh store is cold-start (`fullCoverage`) and would
    /// ignore the chapter signal entirely. Uses the production observation
    /// API so the seeded row is byte-identical to one a warmed podcast would
    /// have accrued.
    private func seedTargetedWithAuditPlannerState(
        store: AnalysisStore,
        podcastId: String
    ) async throws {
        // Five full-rescan observations, each with a 0.9 recall sample:
        //   observedEpisodeCount → 5 (≥ floor 5)
        //   recall ring → [0.9, 0.9, 0.9] (size 3, all ≥ threshold 0.85)
        //   ⇒ stableRecall == true.
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
    /// `BackfillJobRunner` (mirrors `PlayheadFMSmokeTests.makeLiveSmokeRunner`
    /// and the `PlayheadRuntime` production wiring). Without this, the
    /// service's `runShadowFMPhase` returns `.skipped` (no FM evidence) and
    /// the chapter steering has nothing to steer. The `mode` argument is
    /// supplied by `AdDetectionService` (the effective FM mode, `.full`
    /// here), so the runner scans at the cohort-approved capability set.
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
                // No `runtime:` argument ⇒ FoundationModelClassifier uses its
                // live runtime against `SystemLanguageModel.default`.
                classifier: FoundationModelClassifier(),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: makeTestScanCohortJSON(promptLabel: "chapter-fusion-lift-ab"),
                sensitiveRouter: router,
                permissiveClassifier: permissiveClassifierBox
            )
        }
    }

    // MARK: - Chunk rebinding

    /// Return a copy of `chunk` with its `analysisAssetId` re-pointed at
    /// `assetId`. Keeps every other field (timings, normalized text, pass,
    /// ordinals) byte-identical so the derived transcript version is stable.
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
/// feature windows). Carries a one-line operator-facing reason that flows
/// into the test's failure summary.
private struct ABRunError: Error {
    let reason: String
}

// MARK: - In-test phase doubles
//
// The LIVE `LiveBoundaryDetector` and `LiveLabelerAdapter` adapters are
// shared with the capture harness and live in
// `ChapterLabelingContextSequencer.swift`; that file also unit-tests the
// bookkeeping that threads the real chapter index / total / previous
// disposition into each FM call (playhead-bahc). The admission /
// creator-chapter / hash / event-sink doubles below are minimal; the event
// sink RECORDS so the harness can assert `.completed`.

/// Always admits — the A/B is opt-in via env var, so the operator has
/// already accepted the FM cost.
private struct StubAdmissionPolicy: ChapterPhaseAdmissionPolicy {
    func decide() async -> ChapterPhaseAdmissionDecision { .admit }
}

/// Returns no creator chapters so the FM inference path actually runs.
private struct StubCreatorChapterProvider: CreatorChapterProviding {
    func creatorChapters(episodeId: String) async -> [ChapterEvidence] { [] }
}

/// Returns the same hash on entry and recheck so the in-phase
/// transcript-revision race never fires. The hash MUST equal the
/// `transcriptVersion` the wire-in reads under (churn risk #1).
private struct StickyHashProvider: TranscriptHashProviding {
    let hash: String
    func currentTranscriptHash() async -> String? { hash }
}

/// Records every phase event so the harness can assert `.completed`
/// (not `.preempted`) on the treatment arm.
private actor RecordingEventSink: ChapterPhaseEventSink {
    private(set) var events: [ChapterPhaseEvent] = []
    func record(_ event: ChapterPhaseEvent) async { events.append(event) }
    func snapshot() -> [ChapterPhaseEvent] { events }
}
