// BrandAppearanceLiveABTests.swift
// playhead-brandab — env-gated Mac Catalyst A/B tests that measure the two
// "brand-appearance" precision signals through the REAL
// `AdDetectionService.runBackfill` (fmBackfillMode: .full), scored against the
// 12-episode dogfood golden corpus.
//
// SPLIT BY SIGNAL (one signal per Catalyst pass): each signal gets its OWN
// focused 2-arm method (its own independent `.baseline` arm + its one treatment
// arm), so the orchestrator can run, score, and read each signal one at a time:
//   * `testRhetoricalGrammarABAcrossDogfoodCorpus` — [.baseline, .xsdz12Only],
//     dumps `playhead-dogfood-diagnostics-brandappearance-xsdz12-ab.json`.
//   * `testCrossShowSyndicationABAcrossDogfoodCorpus` — [.baseline, .xsdz13Only],
//     dumps `playhead-dogfood-diagnostics-brandappearance-xsdz13-ab.json`.
// A third OPTIONAL method (`testBothBrandSignalsABAcrossDogfoodCorpus`) retains
// the original combined 4-arm view — the orchestrator runs only the two
// single-signal methods, but the combined method costs nothing to keep because
// it reuses the SAME per-arm machinery.
//
// The two signals:
//   * playhead-xsdz.12 — rhetorical act-sequence grammar
//     (`AdDetectionConfig.rhetoricalGrammarEnabled`). A pure per-span TEXT
//     signal: toggling the flag is sufficient (no store).
//   * playhead-xsdz.13 — cross-show syndication
//     (`AdDetectionConfig.crossShowSyndicationEnabled`). This only fires when a
//     sponsor entity recurs across MANY shows with temporal persistence, so it
//     needs a WIRED `CrossShowSyndicationStore` SHARED across all 12 episodes of
//     the arm, written + read, with observations stamped at each episode's real
//     PUBLISH DATE so the ≥14-day persistence gate can be satisfied within one
//     pass if the corpus spans ≥14 days.
//
// ARMS (`BrandAppearanceArm`) — every non-toggle field == production `.default`.
// Each method runs its OWN baseline + the arm(s) it measures (the passes are
// independent — no shared state between methods):
//   * baseline   — both flags false (production). NO syndication store.
//   * xsdz12Only — `.default` + rhetoricalGrammarEnabled. NO store.
//   * xsdz13Only — `.default` + crossShowSyndicationEnabled (+ shared store).
//   * bothOn     — `.default` + both (+ shared store). ONLY in the optional
//                  combined method.
//
// ★ xsdz.13 store wiring + cross-episode accumulation (the #1 correctness risk) ★
//   For the two arms where `crossShowSyndicationEnabled` is true, the harness
//   constructs ONE `CrossShowSyndicationStore` SHARED across all 12 episodes of
//   that arm (NOT per-episode — a per-episode store would defeat cross-show
//   aggregation entirely). It is wired into BOTH the service (read/boost) AND the
//   write path: the production write path inside `runBackfill` populates it as
//   episodes process. Because the production write stamps "now" (which would give
//   ~0-day persistence within one pass), the harness ADDITIONALLY pre-seeds each
//   episode's sponsor observations at the episode's REAL PUBLISH DATE (parsed
//   from the episode id) into the SAME shared store BEFORE that episode's read,
//   using the SAME production extractor (`crossShowSponsorObservations`) and
//   normalization the in-backfill write uses. Episodes are processed in
//   PUBLISH-DATE order so write→read accumulation is realistic and the earliest
//   first-seen (hence the persistence span) is anchored to the earliest publish
//   date. The baseline + xsdz12-only arms construct NO store (gating parity with
//   production).
//
// ★ fire instrumentation (so a null result is interpretable) ★
//   For EACH arm the harness reports, via a behavior-neutral nil-default
//   `BrandAppearanceChannelTapObserver` (never wired in production), how many
//   spans received a `.rhetoricalGrammar` entry and how many received a
//   `.crossShowSyndication` entry, plus how many DISTINCT sponsor entities reached
//   the syndication store's spread+persistence gate (measured via the production
//   `CrossShowSyndicationEvaluator` over the shared store). Without this,
//   "metrics identical to baseline" is ambiguous (did the channel not fire, or
//   fire-but-no-effect?). The counts land in the JSON.
//
// PRECONDITION (the FM path must run like production): the live targeted-phase
// narrowing inside `BackfillJobRunner` is guarded by
// `plan.policy == .targetedWithAudit`; a fresh store is cold-start
// (`fullCoverage`). So each arm SEEDS the planner into the warmed
// `targetedWithAudit` regime before the scored run. Symmetric across arms, so it
// cannot bias the comparison. The brand-appearance signals themselves are
// policy-independent (they ride the per-span ledger), so the seeding is
// conservative, not load-bearing, for them.
//
// Gating (mirrors `LexicalScorerLiveABTests` / `FragilityGateLiveABTests`):
//   `PLAYHEAD_BRANDAPPEARANCE_AB=1` MUST be set in the test process environment.
//   The default `PlayheadFastTests` plan does NOT set it, so the test body is a
//   no-op on Cmd-U (via `XCTSkipUnless`). The live FM `BackfillJobRunner` also
//   requires iOS 26+ (`#available`), so an older runtime skips too.
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
//        # rhetorical grammar (xsdz.12)
//        xcodebuild test \
//          -scheme Playhead -testPlan PlayheadFastTests \
//          -destination 'platform=macOS,variant=Mac Catalyst' \
//          -only-testing:'PlayheadTests/BrandAppearanceLiveABTests/testRhetoricalGrammarABAcrossDogfoodCorpus' \
//          PLAYHEAD_BRANDAPPEARANCE_AB=1
//        # cross-show syndication (xsdz.13)
//        xcodebuild test \
//          -scheme Playhead -testPlan PlayheadFastTests \
//          -destination 'platform=macOS,variant=Mac Catalyst' \
//          -only-testing:'PlayheadTests/BrandAppearanceLiveABTests/testCrossShowSyndicationABAcrossDogfoodCorpus' \
//          PLAYHEAD_BRANDAPPEARANCE_AB=1
//   4. Read the printed 2-row table; the JSON dumps land at the repo root as
//      `playhead-dogfood-diagnostics-brandappearance-xsdz12-ab.json` and
//      `playhead-dogfood-diagnostics-brandappearance-xsdz13-ab.json` (git-ignored
//      — they match the `playhead-dogfood-diagnostics-*.json` pattern in
//      .gitignore).
//
// Hermetic helpers (`BrandAppearanceArm`, `BrandAppearanceArmConfig`,
// `BrandAppearancePublishDate`, `BrandAppearanceFireTally`,
// `BrandAppearanceSweepReport`, `FusionLiftModeAccumulator`) live in
// `FusionLiftHarnessSupport.swift` and are unit-tested on the simulator by
// `BrandAppearanceArmConfigTests`. Scoring reuses `FusionLiftScoring.swift`.

import AVFoundation
import Foundation
import XCTest
@testable import Playhead

final class BrandAppearanceLiveABTests: XCTestCase {

    /// Gate every test on the A/B env var. The default scheme does not set it,
    /// so the test body is a no-op on Cmd-U / the default sim suite.
    private static var abEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_BRANDAPPEARANCE_AB"] == "1"
    }

    override func setUp() async throws {
        try await super.setUp()
        try XCTSkipUnless(
            Self.abEnabled,
            """
            Brand-appearance precision-signal A/B is opt-in and SLOW (each \
            single-signal method runs ~24 full-FM passes: 1 scored pass × 2 arms \
            × 12 episodes). Set PLAYHEAD_BRANDAPPEARANCE_AB=1 in the test plan env \
            vars and run on Mac Catalyst (or an iOS 26 device) with Apple \
            Intelligence enabled and corpus audio + transcripts staged. See the \
            file header for the full invocation recipe.
            """
        )
    }

    // MARK: - A/B entry points (one focused method per brand-appearance signal)

    /// playhead-xsdz.12 — rhetorical act-sequence grammar
    /// (`rhetoricalGrammarEnabled`). A pure per-span TEXT signal: NO syndication
    /// store. Runs its OWN baseline + the xsdz12-only arm, independently of the
    /// other methods, and dumps a focused 2-arm report.
    func testRhetoricalGrammarABAcrossDogfoodCorpus() async throws {
        try await runABPass(
            arms: [.baseline, .xsdz12Only],
            dumpFilename: "playhead-dogfood-diagnostics-brandappearance-xsdz12-ab.json"
        )
    }

    /// playhead-xsdz.13 — cross-show syndication (`crossShowSyndicationEnabled`),
    /// WITH the SHARED-across-episodes `CrossShowSyndicationStore` wiring +
    /// publish-date ordering + fire/gate instrumentation. Runs its OWN baseline +
    /// the xsdz13-only arm, independently of the other methods, and dumps a
    /// focused 2-arm report.
    func testCrossShowSyndicationABAcrossDogfoodCorpus() async throws {
        try await runABPass(
            arms: [.baseline, .xsdz13Only],
            dumpFilename: "playhead-dogfood-diagnostics-brandappearance-xsdz13-ab.json"
        )
    }

    /// OPTIONAL combined 4-arm view (baseline + both signals + their interaction).
    /// The orchestrator runs only the two single-signal methods above; this method
    /// is retained because it reuses the SAME per-arm machinery at zero extra
    /// cost. It dumps the original combined JSON filename.
    func testBothBrandSignalsABAcrossDogfoodCorpus() async throws {
        try await runABPass(
            arms: BrandAppearanceArm.allCases, // [.baseline, .xsdz12Only, .xsdz13Only, .bothOn]
            dumpFilename: "playhead-dogfood-diagnostics-brandappearance-ab.json"
        )
    }

    // MARK: - Shared A/B pass

    /// Run a single Catalyst A/B pass over EXACTLY `arms` (baseline first), score
    /// each arm vs the golden, and dump a focused report to `dumpFilename`. Each
    /// invocation is fully self-contained — it builds its own accumulators, fire
    /// tallies, and (for the flag-on arms) shared syndication stores — so two
    /// methods calling this never share state and each runs its OWN baseline.
    ///
    /// `arms` MUST begin with `.baseline` (it anchors every delta). Arms that
    /// require a syndication store (`requiresSyndicationStore` == xsdz.13 on) get
    /// ONE store SHARED across all episodes of that arm, written + pre-seeded at
    /// each episode's real publish date; arms without the flag construct NO store
    /// (gating parity with production).
    private func runABPass(
        arms: [BrandAppearanceArm],
        dumpFilename: String
    ) async throws {
        guard #available(iOS 26.0, *) else {
            throw XCTSkip("Live FM BackfillJobRunner requires iOS 26+ / FoundationModels.")
        }
        precondition(arms.first == .baseline, "runABPass requires baseline first (it anchors every delta)")

        let loader = CorpusAnnotationLoader()

        let annotationURLs: [URL]
        do {
            annotationURLs = try loader.annotationFileURLs()
        } catch {
            throw XCTSkip("corpus annotations dir not present: \(error)")
        }
        try XCTSkipIf(annotationURLs.isEmpty, "no corpus annotations staged")

        // Resolve every episode FIRST (annotation + audio + transcript), so the
        // arm loops can process them in PUBLISH-DATE order. An episode that fails
        // resolution lands in `failed`/`skipped` and is excluded from every arm.
        var resolved: [ResolvedEpisode] = []
        var skipped: [(episodeId: String, reason: String)] = []
        var failed: [(episodeId: String, reason: String)] = []

        for url in annotationURLs {
            let episodeId = url.deletingPathExtension().lastPathComponent

            let annotation: CorpusAnnotation
            do {
                annotation = try loader.decode(at: url)
            } catch {
                failed.append((episodeId, "annotation decode failed: \(error.localizedDescription)"))
                continue
            }

            let audioURL: URL
            do {
                audioURL = try loader.audioFileURL(for: annotation)
            } catch {
                skipped.append((episodeId, "audio file not staged under \(loader.audioDirectoryURL.path)"))
                continue
            }

            do {
                try loader.verify(audioFingerprintFor: annotation, jsonURL: url)
            } catch {
                failed.append((episodeId, "audio fingerprint check failed: \(error.localizedDescription)"))
                continue
            }

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

            // PUBLISH DATE from the episode id (`<show>-YYYY-MM-DD-<slug>`). The
            // syndication temporal-persistence gate needs real timestamps; an id
            // without a date token fails LOUD (the corpus convention is invariant
            // and a missing date would silently zero out persistence).
            guard let publishDate = BrandAppearancePublishDate.parse(fromEpisodeId: episodeId) else {
                failed.append((episodeId, "could not parse YYYY-MM-DD publish date from episode id"))
                continue
            }

            resolved.append(ResolvedEpisode(
                episodeId: episodeId,
                annotation: annotation,
                audioURL: audioURL,
                transcript: transcript,
                publishDate: publishDate
            ))
        }

        // Process episodes in PUBLISH-DATE order (ascending), tie-broken by id for
        // determinism. Write→read accumulation in the shared syndication store is
        // then realistic and the earliest first-seen anchors the persistence span.
        resolved.sort {
            if $0.publishDate != $1.publishDate { return $0.publishDate < $1.publishDate }
            return $0.episodeId < $1.episodeId
        }

        // One accumulator + one fire tally per arm IN THIS PASS.
        var accumulators: [BrandAppearanceArm: FusionLiftModeAccumulator] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, FusionLiftModeAccumulator()) }
        )
        var fireTallies: [BrandAppearanceArm: BrandAppearanceFireTally] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, BrandAppearanceFireTally()) }
        )

        // The shared per-arm syndication stores (ONE per flag-on arm, shared
        // across all episodes of the arm). Tracks the set of observed entities so
        // the gate-reached count can be computed after the pass without a new
        // store API.
        var armStores: [BrandAppearanceArm: SharedSyndicationStore] = [:]
        for arm in arms where arm.requiresSyndicationStore {
            let dir = try makeTempDir(prefix: "BrandAppearanceAB-syndication-\(arm.rawValue)")
            let store = try CrossShowSyndicationStore(directoryURL: dir)
            try await store.migrate()
            armStores[arm] = SharedSyndicationStore(store: store)
        }

        var scored: [String] = []

        for episode in resolved {
            do {
                for arm in arms {
                    // The behavior-neutral tap observer is per arm/episode: it
                    // records the per-channel fire counts keyed by assetId.
                    let tap = BrandAppearanceChannelTapObserver()

                    // For flag-on arms: pre-seed THIS episode's sponsor entities
                    // into the SHARED store at the episode's publish date BEFORE
                    // the read, so the syndication signal sees accumulated
                    // cross-episode/cross-show history with realistic timestamps.
                    if let shared = armStores[arm] {
                        try await shared.preSeedObservations(
                            transcript: episode.transcript,
                            episodeId: episode.episodeId,
                            podcastId: episode.annotation.showName,
                            at: episode.publishDate
                        )
                    }

                    let windows = try await runArm(
                        arm: arm,
                        episode: episode,
                        syndicationStore: armStores[arm]?.store,
                        tap: tap
                    )
                    accumulators[arm]?.addEpisode(
                        annotationWindows: episode.annotation.adWindows,
                        adWindows: windows,
                        podcastId: episode.annotation.showName,
                        episodeId: episode.episodeId
                    )
                    let counts = await tap.fireCounts(for: episode.episodeId)
                    fireTallies[arm]?.add(counts)
                }
                scored.append(episode.episodeId)
            } catch let error as BrandAppearanceABRunError {
                failed.append((episode.episodeId, error.reason))
            } catch {
                failed.append((episode.episodeId, "arm run failed: \(error.localizedDescription)"))
            }
        }

        // After the pass, compute the xsdz.13 gate-reached entity count per
        // flag-on arm: how many DISTINCT sponsor entities cleared the production
        // spread+persistence gate in that arm's shared store.
        let evaluator = CrossShowSyndicationEvaluator()
        for (arm, shared) in armStores {
            let gated = await shared.gatedEntityCount(evaluator: evaluator)
            fireTallies[arm]?.syndicationGatedEntities = gated
            await shared.store.close()
        }

        // Build + print + dump the report over EXACTLY the arms this pass ran
        // (baseline first), so the JSON carries no phantom zero rows for arms a
        // single-signal pass never ran.
        let report = BrandAppearanceSweepReport(
            episodeCount: scored.count,
            arms: arms,
            accumulators: accumulators,
            fireTallies: fireTallies
        )
        print(report.table())
        print("""
        scored=\(scored.count): \(scored.sorted().joined(separator: ", "))
        skipped (audio/transcript missing)=\(skipped.count): \(skipped.map(\.episodeId).sorted().joined(separator: ", "))
        failed=\(failed.count): \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))
        """)

        let dumpURL = loader.repoRoot.appendingPathComponent(
            dumpFilename,
            isDirectory: false
        )
        try report.jsonData().write(to: dumpURL, options: .atomic)
        print("Brand-appearance A/B JSON: \(dumpURL.path) (git-ignored)")

        // Fail loud on hard failures; fail loud if nothing scored (env set but
        // every episode landed in skipped — audio/transcript not staged).
        if !failed.isEmpty {
            XCTFail("Hard failures: \(failed.map { "\($0.episodeId): \($0.reason)" }.joined(separator: " | "))")
        } else if scored.isEmpty {
            XCTFail(
                """
                PLAYHEAD_BRANDAPPEARANCE_AB=1 was set but no episodes scored — \
                every episode landed in `skipped` because audio or the transcript \
                sidecar is not staged. See the file header for the staging recipe.
                """
            )
        }
    }

    // MARK: - One arm

    /// Run the scored backfill once for a single episode in the requested arm and
    /// return the persisted ad windows. Every arm decodes the same audio, persists
    /// the same feature windows + transcript chunks, SEEDs the planner into the
    /// warmed `targetedWithAudit` regime, and uses `NarrowingConfig.default`. The
    /// ONLY things that differ are the two brand-appearance flags and (its
    /// production consequence) whether a shared syndication store is wired. Always
    /// uses `fmBackfillMode: .full`.
    @available(iOS 26.0, *)
    private func runArm(
        arm: BrandAppearanceArm,
        episode: ResolvedEpisode,
        syndicationStore: CrossShowSyndicationStore?,
        tap: BrandAppearanceChannelTapObserver
    ) async throws -> [AdWindow] {
        let episodeId = episode.episodeId
        let annotation = episode.annotation
        guard let localURL = LocalAudioURL(episode.audioURL) else {
            throw BrandAppearanceABRunError(reason: "audio at \(episode.audioURL.path) is not a file URL")
        }

        // Fresh ANALYSIS store per arm/episode so the runs never observe each
        // other's persisted ad windows or planner state. (The SYNDICATION store
        // is the deliberate exception — it is shared across episodes of the arm.)
        let storeDir = try makeTempDir(prefix: "BrandAppearanceAB-\(arm.rawValue)")
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        let assetId = episodeId
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "brand-appearance-fp-\(episodeId)",
            weakFingerprint: nil,
            sourceURL: localURL.absoluteString,
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        let chunks = episode.transcript.map { rebindChunk($0, to: assetId) }
        try await store.insertTranscriptChunks(chunks)

        // PERSIST FEATURE WINDOWS: decode → extract → persist.
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
            throw BrandAppearanceABRunError(reason: "feature extraction produced 0 windows for \(episodeId)")
        }
        try await store.insertFeatureWindows(featureWindows)

        let episodeDuration = shards.reduce(0.0) { max($0, $1.startTime + $1.duration) }

        // PRECONDITION — seed the planner into `targetedWithAudit`. Symmetric
        // across arms, so it cannot bias the comparison.
        try await seedTargetedWithAuditPlannerState(store: store, podcastId: annotation.showName)

        let config = BrandAppearanceArmConfig.adDetectionConfig(for: arm)
        let narrowingConfig = BrandAppearanceArmConfig.narrowingConfig(for: arm)

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            backfillJobRunnerFactory: Self.makeLiveRunnerFactory(narrowingConfig: narrowingConfig),
            canUseFoundationModelsProvider: { true }, // avoid silent FM demotion
            // Behavior-neutral per-span channel-tap observer (non-nil ONLY here;
            // nil in production). Declared after canUseFoundationModelsProvider to
            // match the init order.
            brandAppearanceChannelTapObserver: tap,
            // playhead-xsdz.13: the SHARED store (nil for baseline/xsdz12Only —
            // gating parity with production where the store is built iff the flag
            // is on). Wired into BOTH the read/boost path AND the write path.
            crossShowSyndicationStore: syndicationStore,
            approvedCohortRegistry: nil // avoid cohort-gated FM demotion
        )

        // GUARD #1 (FM-mode gating): confirm cohort/FM gating did not silently
        // demote `.full` → `.off`. Else the FM phase never runs and the A/B is a
        // meaningless zero.
        let effectiveMode = await service.effectiveFMBackfillModeForTesting()
        XCTAssertEqual(
            effectiveMode,
            .full,
            "arm=\(arm.rawValue): effective FM mode demoted off .full — FM phase would not run"
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
            "arm=\(arm.rawValue): planner is not in targetedWithAudit (observed=\(seededContext.observedEpisodeCount) stableRecall=\(seededContext.stableRecall)) — the FM scan would not match production"
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
                scanCohortJSON: makeTestScanCohortJSON(promptLabel: "brand-appearance-ab"),
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

// MARK: - Resolved episode

/// One fully-resolved corpus episode the arm loops iterate. Includes the parsed
/// publish date so the harness can process episodes in publish-date order and
/// stamp syndication observations at realistic timestamps.
private struct ResolvedEpisode: Sendable {
    let episodeId: String
    let annotation: CorpusAnnotation
    let audioURL: URL
    let transcript: [TranscriptChunk]
    let publishDate: Date
}

// MARK: - Shared syndication store wrapper

/// Wraps the ONE `CrossShowSyndicationStore` SHARED across all episodes of a
/// flag-on arm. Pre-seeds each episode's sponsor observations at the episode's
/// real PUBLISH DATE using the SAME production extractor + normalization the
/// in-backfill write uses, and tracks the observed entity set so the post-pass
/// gate-reached count can be computed without a new store API.
private actor SharedSyndicationStore {
    let store: CrossShowSyndicationStore
    /// Distinct normalized entities ever pre-seeded (the candidate set for the
    /// gate-reached count).
    private var observedEntities: Set<String> = []

    init(store: CrossShowSyndicationStore) {
        self.store = store
    }

    /// Pre-seed this episode's sponsor entities into the shared store stamped at
    /// `date`. Uses the SAME `AdDetectionService.crossShowSponsorObservations`
    /// extractor + min-write-confidence gate the production write path uses, over
    /// the SAME evidence catalog (built from the SAME atomization
    /// `runBackfill` performs), so the seeded observations are exactly what the
    /// production write would record — only the timestamp is the real publish date
    /// instead of "now".
    func preSeedObservations(
        transcript: [TranscriptChunk],
        episodeId: String,
        podcastId: String,
        at date: Date
    ) async throws {
        // Reproduce runBackfill's atomization (final-pass filter + the same
        // norm/source hashes) so the catalog matches the in-backfill catalog.
        let finalChunks = FusionLiftTranscriptVersion.finalChunks(from: transcript)
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: finalChunks,
            analysisAssetId: episodeId,
            normalizationHash: FusionLiftTranscriptVersion.normalizationHash,
            sourceHash: FusionLiftTranscriptVersion.sourceHash
        )
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: episodeId,
            transcriptVersion: version.transcriptVersion
        )
        let observations = AdDetectionService.crossShowSponsorObservations(from: catalog.entries)
        let now = date.timeIntervalSince1970
        // The production write gate is `confidence >= 0.5`
        // (`crossShowSyndicationMinWriteConfidence`). The extractor's confidence
        // FLOOR is exactly 0.5 (`min(1.0, 0.5 + 0.1 * (count - 1))`), so EVERY
        // observation it returns clears the production gate by construction —
        // recording them all is byte-equivalent to applying the gate, and avoids
        // touching the private production constant (no extra production change).
        for (entity, confidence) in observations {
            try await store.recordObservation(
                normalizedEntity: entity,
                podcastId: podcastId,
                confidence: confidence,
                now: now
            )
            observedEntities.insert(entity)
        }
    }

    /// Count DISTINCT entities in the shared store that clear the production
    /// spread+persistence gate, measured via the production evaluator. This is the
    /// decisive "did xsdz.13 reach its gate at all on this corpus?" number.
    func gatedEntityCount(evaluator: CrossShowSyndicationEvaluator) async -> Int {
        let totalShows = await store.totalObservedShowCount()
        var count = 0
        for entity in observedEntities {
            guard let profile = await store.spreadProfile(
                forEntity: entity,
                totalObservedShows: totalShows
            ) else { continue }
            if evaluator.qualifies(profile) { count += 1 }
        }
        return count
    }
}

// MARK: - Arm-run error

/// Raised when an arm cannot complete for a soft reason (non-file URL, zero
/// feature windows). Carries a one-line operator-facing reason that flows into
/// the test's failure summary.
private struct BrandAppearanceABRunError: Error {
    let reason: String
}
