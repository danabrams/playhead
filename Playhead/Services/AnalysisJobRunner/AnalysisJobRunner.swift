// AnalysisJobRunner.swift
// Bounded-range analysis engine: decode → features → transcription →
// ad detection → cue materialization. Reusable across pre-roll warmup,
// live playback, and background backfill modes.
//
// Accepts an AnalysisRangeRequest describing the audio range to process,
// runs each pipeline stage with cancellation and thermal checks between
// stages, and returns an AnalysisOutcome summarizing what was achieved.

import Foundation
import OSLog

// MARK: - AnalysisJobRunner

actor AnalysisJobRunner {

    private let logger = Logger(subsystem: "com.playhead", category: "AnalysisJobRunner")

    /// Bug 5 (skip-cues-deletion): minimum confidence used to compute
    /// `cueCoverage` (the highest confidence-passing window endTime)
    /// and `newCueCount` (count of confidence-passing windows that
    /// did not exist before this run). Mirrors the 0.7 threshold the
    /// (now-deleted) `SkipCueMaterializer` used.
    private static let cueConfidenceThreshold: Double = 0.7

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let audioProvider: AnalysisAudioProviding
    private let featureService: FeatureExtractionService
    private let transcriptEngine: TranscriptEngineService
    private let adDetection: AdDetectionProviding
    private let thermalStateProvider: @Sendable () -> ProcessInfo.ThermalState
    /// playhead-e2vw: injectable clock for synthetic-time test harnesses.
    /// Defaults to `Date.init` so production behavior is byte-identical;
    /// the cascade-attributed proximal-readiness SLI test
    /// (`CandidateWindowCascadeProximalReadinessSLITest`) installs a
    /// `ManualClock` to drive lease/registration timestamps off
    /// synthetic time.
    private let clock: @Sendable () -> Date
    /// Optional coordinator (playhead-01t8). When non-nil, every
    /// `run(_:)` registers with the coordinator at start-of-work,
    /// threads the returned `PreemptionSignal` down into feature
    /// extraction + transcription, and unregisters on exit. When nil
    /// (unit-test paths that never drive the scheduler), the runner
    /// behaves exactly as it did pre-01t8.
    private let preemptionCoordinator: LanePreemptionCoordinator?

    /// playhead-gtt9.1: shadow-mode acoustic-likelihood gate for the
    /// transcript scheduler. The default ships with `enabled = true,
    /// skipEnabled = false` (shadow logging on, production skip off);
    /// callers that want the pre-gtt9.1 byte-identical behavior pass
    /// `.disabled` to short-circuit both scoring and logging.
    private let acousticGateConfig: AcousticTranscriptGateConfig

    /// playhead-gtt9.1: structured-event sink for shadow-gate decisions.
    /// `NoOpTranscriptShadowGateLogger` is the production default — it
    /// ignores every record call. Tests inject `RecordingTranscriptShadowGateLogger`
    /// to assert the runner emits the expected per-shard rows.
    private let transcriptShadowGateLogger: TranscriptShadowGateLogging

    /// playhead-gtt9.1: deterministic seed source for the safety-sample
    /// coin flip. Production uses `SystemRandomNumberGenerator()` so the
    /// 10% sampling is genuinely random across runs; tests inject a
    /// fixed-seed generator so the would-skip / safety-sample-keep
    /// outcome is reproducible.
    private let safetySampleRNG: @Sendable () -> Double

    /// playhead-zx6i — gate the B4 revalidation short-circuit. Returns
    /// `true` when the `b4_revalidation_from_features_enabled` flag is
    /// ON for THIS process. Production wiring re-reads the flag from
    /// `PreAnalysisConfig.load().b4RevalidationFromFeaturesEnabled` on
    /// EVERY call (this closure is invoked at the top of each
    /// `run(_:)`), giving the runner an **instant rollback** contract:
    /// flipping the flag OFF in Settings disables the short-circuit on
    /// the very next analysis run, not on next app launch. This
    /// diverges from 2hpn / xr3t (which snapshot at consumer-init);
    /// instant rollback is preferred here because the short-circuit
    /// gates a perf optimization with a potential `false_ready_rate`
    /// risk, and minimising blast-radius matters more than caching
    /// the read. The companion stamp-write inside
    /// `AdDetectionService.runBackfill` also re-reads on every write
    /// so the producer and consumer agree on the live value
    /// (R1 doc audit fix: prior wiring snapshotted the writer at
    /// `AdDetectionService` init, producing an asymmetric rollback
    /// where a mid-session flag-ON would never write a stamp because
    /// the producer's cached value was still `false`).
    ///
    /// Performance: `PreAnalysisConfig.load()` does an in-memory
    /// UserDefaults read (Foundation caches the backing plist after
    /// first sync) plus a JSON decode of ~200 bytes. One call per
    /// `run(_:)` and one per successful `runBackfill`. Measured in
    /// microseconds — well below the cost of the analysis pipeline
    /// stages this short-circuit gates. The R2 audit confirmed no
    /// benchmark regression. If the call count ever grows (e.g.
    /// per-shard rather than per-asset), revisit caching.
    ///
    /// Tests inject a fixed `Bool` (or a closure that flips it) so
    /// the short-circuit can be deterministically exercised without
    /// round-tripping through UserDefaults. Returning `false` makes
    /// the short-circuit structurally unreachable — flag-OFF is
    /// byte-identical to pre-zx6i behaviour.
    private let b4RevalidationEnabledProvider: @Sendable () -> Bool

    /// playhead-zx6i — current pipeline-version triple reader. Defaults
    /// to `PipelineVersions.current()` in production. Tests inject a
    /// fixed snapshot so the short-circuit's version-comparison branch
    /// can be exercised without touching the global
    /// `AdDetectionConfig` / `SkipPolicyConfig` / `SharedVersionConstants`
    /// singletons (which are static `let`s and cannot be mutated mid-test).
    private let currentPipelineVersionsProvider: @Sendable () -> PipelineVersions

    /// playhead-zx6i — per-asset "completed versions" loader. Defaults
    /// to `RevalidationStateStore.loadCompletedVersions` against
    /// `.standard`. Tests inject a closure that returns whatever the
    /// scenario calls for (nil → simulates pre-zx6i asset; equal →
    /// simulates "no bump"; different → simulates a version bump).
    private let completedPipelineVersionsLoader: @Sendable (_ assetId: String) -> PipelineVersions?
    /// playhead-bbrv.1 — optional Phase A cross-user sharing seam. The
    /// default provider is disabled and returns no snapshots, so production
    /// behavior is unchanged unless explicit wiring installs a provider.
    private let analysisSharingProvider: CrossUserAnalysisSharingProviding

    /// playhead-xsdz.36 ACTIVATION: instance-level enablement for the
    /// xsdz.27 played-copy (rediff A-side) fingerprint capture branch.
    /// Defaults `false` so every existing construction site (and test) is
    /// byte-identical to the pre-activation runner; `PlayheadRuntime` passes
    /// `RediffActivation.isEnabledByDefault`. The compile-time
    /// `EpisodeFingerprintCapture.captureEnabledByDefault` remains a
    /// separate, still-`false`, still-pinned escape hatch (OR-ed in at the
    /// branch).
    private let rediffASideCaptureEnabled: Bool

    // MARK: - Init

    init(
        store: AnalysisStore,
        audioProvider: AnalysisAudioProviding,
        featureService: FeatureExtractionService,
        transcriptEngine: TranscriptEngineService,
        adDetection: AdDetectionProviding,
        thermalStateProvider: @escaping @Sendable () -> ProcessInfo.ThermalState = {
            ProcessInfo.processInfo.thermalState
        },
        preemptionCoordinator: LanePreemptionCoordinator? = nil,
        clock: @escaping @Sendable () -> Date = { Date() },
        acousticGateConfig: AcousticTranscriptGateConfig = .default,
        transcriptShadowGateLogger: TranscriptShadowGateLogging = NoOpTranscriptShadowGateLogger(),
        safetySampleRNG: @escaping @Sendable () -> Double = { Double.random(in: 0..<1) },
        b4RevalidationEnabledProvider: @escaping @Sendable () -> Bool = {
            PreAnalysisConfig.load().b4RevalidationFromFeaturesEnabled
        },
        currentPipelineVersionsProvider: @escaping @Sendable () -> PipelineVersions = {
            PipelineVersions.current()
        },
        completedPipelineVersionsLoader: @escaping @Sendable (_ assetId: String) -> PipelineVersions? = { assetId in
            RevalidationStateStore.loadCompletedVersions(forAsset: assetId)
        },
        analysisSharingProvider: CrossUserAnalysisSharingProviding = NoOpCrossUserAnalysisSharingProvider(),
        rediffASideCaptureEnabled: Bool = false
    ) {
        self.store = store
        self.audioProvider = audioProvider
        self.featureService = featureService
        self.transcriptEngine = transcriptEngine
        self.adDetection = adDetection
        self.thermalStateProvider = thermalStateProvider
        self.preemptionCoordinator = preemptionCoordinator
        self.clock = clock
        self.acousticGateConfig = acousticGateConfig
        self.transcriptShadowGateLogger = transcriptShadowGateLogger
        self.safetySampleRNG = safetySampleRNG
        self.b4RevalidationEnabledProvider = b4RevalidationEnabledProvider
        self.currentPipelineVersionsProvider = currentPipelineVersionsProvider
        self.completedPipelineVersionsLoader = completedPipelineVersionsLoader
        self.analysisSharingProvider = analysisSharingProvider
        self.rediffASideCaptureEnabled = rediffASideCaptureEnabled
    }

    // MARK: - Run

    /// Execute a bounded analysis pass described by `request`.
    /// Returns an `AnalysisOutcome` summarizing coverage achieved and stop reason.
    func run(_ request: AnalysisRangeRequest) async -> AnalysisOutcome {
        let assetId = request.analysisAssetId
        var shouldAttemptSharedImport = true

        // playhead-0sro: RESUME reconcile for Pipeline B. The scheduler
        // path never goes through `AnalysisCoordinator.resolveSession`, so
        // it needs its own entry-point reconcile — every stage below (the
        // M1 shard gate, the revalidation short-circuit's outcome, the
        // returned `transcriptCoverageSec`) reads the raw watermark, and
        // this run may be resuming an asset a previous run left describing
        // less coverage than its persisted chunks prove. Raises only; a
        // no-op when the two already agree. Best-effort — a failed repair
        // must not fail the run.
        do {
            try await store.reconcileFastTranscriptCoverage(id: assetId)
        } catch {
            logger.warning("Coverage reconcile at job start failed for asset \(assetId): \(error.localizedDescription)")
        }

        // playhead-zx6i — B4 fast revalidation short-circuit. Runs
        // BEFORE the preemption-coordinator registration / decode /
        // feature-extraction / transcription stages because the
        // revalidation path consumes only persisted rows and skips
        // every one of those stages. Structurally, this branch is
        // unreachable unless ALL of the following hold:
        //   1. The `b4_revalidation_from_features_enabled` flag is ON.
        //   2. The asset has persisted `TranscriptChunk` rows from a
        //      prior successful `runBackfill`.
        //   3. The `RevalidationStateStore` recorded a completed
        //      `PipelineVersions` snapshot for this asset (i.e. the
        //      prior run happened AFTER zx6i shipped, so we have a
        //      baseline to compare against).
        //   4. The stored snapshot differs from
        //      `PipelineVersions.current()` (i.e. at least one of
        //      `modelVersion` / `policyVersion` / `featureSchemaVersion`
        //      has bumped since the last successful run).
        //
        // When any condition fails the branch falls through to the
        // existing full-analysis path (decode → features → ASR → ad
        // detection). This is the explicit fail-open path:
        //   - condition 1 OFF → flag rollback (instant revert to
        //     pre-zx6i behaviour).
        //   - condition 2 OFF → cold-start asset (no chunks to
        //     revalidate against).
        //   - condition 3 OFF → pre-zx6i asset (no stamp recorded);
        //     we MUST take the full path to establish a stamp before
        //     the next bump can short-circuit.
        //   - condition 4 OFF → versions match, no revalidation needed.
        //     (Note: the existing skip-hot-path / skip-backfill no-op
        //     branches inside the full-path stage 4 already handle
        //     this case correctly — they detect "no new chunks + windows
        //     already exist" and return without re-running the
        //     classifier. So falling through is safe.)
        if b4RevalidationEnabledProvider() {
            let persistedChunks = (try? await store.fetchTranscriptChunks(assetId: assetId)) ?? []
            if !persistedChunks.isEmpty,
               let completed = completedPipelineVersionsLoader(assetId) {
                let current = currentPipelineVersionsProvider()
                if completed != current {
                    shouldAttemptSharedImport = false
                    logger.info("[zx6i] revalidation triggered for asset \(assetId): completed=\(String(describing: completed)) current=\(String(describing: current))")
                    // Resolve `episodeDuration` from the persisted
                    // asset row (the full-path stage 1 computes this
                    // from decoded shards; the revalidation path skips
                    // decode, so we read the cached value written by
                    // playhead-5uvz.6's gap-7 fix to
                    // `analysis_assets.episodeDurationSec`). If the
                    // column is NULL we fall through to full analysis
                    // — without a duration the classifier's per-span
                    // position priors degrade, so a one-time full
                    // re-analysis to repopulate the column is the
                    // safest answer.
                    let asset = try? await store.fetchAsset(id: assetId)
                    if let duration = asset?.episodeDurationSec, duration > 0 {
                        do {
                            try await adDetection.revalidateFromFeatures(
                                analysisAssetId: assetId,
                                podcastId: request.podcastId,
                                episodeDuration: duration,
                                sessionId: nil
                            )
                            // Feature + transcript coverage are not
                            // re-derived on the revalidation path; we
                            // pass through the persisted asset's
                            // existing watermarks so the scheduler's
                            // tier-advancement bookkeeping sees the
                            // same coverage it would have seen on a
                            // no-op fall-through.
                            //
                            // R1 doc audit fix: derive `cueCoverageSec`
                            // from the freshly produced `AdWindow`
                            // rows so the scheduler sees the honest
                            // post-revalidation cue watermark, not a
                            // hard-coded `0`. The filter (confidence
                            // >= `Self.cueConfidenceThreshold` (0.7),
                            // `endTime > startTime`) is the same one
                            // used by the full-path return below (the
                            // `let cueCoverage = finalWindows.filter
                            // {...}.max() ?? 0` block immediately
                            // after the backfill `finalWindows`
                            // reload). Both call sites share
                            // `Self.cueConfidenceThreshold` so the
                            // threshold cannot drift between the two
                            // paths without a single-edit grep target.
                            //
                            // We leave `newCueCount = 0` (the
                            // `makeOutcome` default) deliberately. The
                            // full path only computes `newCueCount`
                            // when `request.outputPolicy ==
                            // .writeWindowsAndCues`, and in that case
                            // counts windows that did not exist in
                            // `existingWindowsBeforeDetection`. On the
                            // revalidation path EVERY window is a
                            // re-classification of a span that already
                            // had a window pre-revalidation
                            // (`runBackfill` rewrites existing rows
                            // against the new versions), so reporting
                            // those as "new cues" would be misleading
                            // — they are not new ad detections, just
                            // re-derived decisions over the same
                            // audio. Returning `0` is the honest
                            // post-revalidation count. The
                            // `AnalysisWorkScheduler`'s
                            // `shouldRetryCoverageInsufficient`
                            // disjunction still picks up progress via
                            // the live `cueCoverageSec` re-fetch
                            // above, so the scheduler does not stall
                            // on a missing `newCueCount`.
                            let revalidatedWindows = (try? await store.fetchAdWindows(assetId: assetId)) ?? []
                            let revalidatedCueCoverage = revalidatedWindows
                                .filter(Self.isCueWindow)
                                .map(\.endTime)
                                .max() ?? 0
                            if request.outputPolicy == .writeWindowsAndPushLive {
                                await publishSharedAnalysisIfEnabled(
                                    assetId: assetId,
                                    podcastId: request.podcastId,
                                    outputPolicy: request.outputPolicy
                                )
                            }
                            return makeOutcome(
                                assetId: assetId,
                                request: request,
                                featureCoverageSec: asset?.featureCoverageEndTime ?? 0,
                                transcriptCoverageSec: asset?.fastTranscriptCoverageEndTime ?? 0,
                                cueCoverageSec: revalidatedCueCoverage,
                                stopReason: .reachedTarget
                            )
                        } catch {
                            logger.warning("[zx6i] revalidation failed for asset \(assetId): \(error.localizedDescription) — falling back to full analysis")
                            // Intentional fall-through: a revalidation
                            // failure should not be a user-visible
                            // outage; the worst case is we redo work.
                        }
                    } else {
                        logger.info("[zx6i] revalidation skipped for asset \(assetId): episodeDurationSec missing — falling back to full analysis")
                    }
                }
            } else if !persistedChunks.isEmpty {
                shouldAttemptSharedImport = false
            }
        }

        if shouldAttemptSharedImport,
           let sharedOutcome = await importSharedAnalysisIfAvailable(
               assetId: assetId,
               request: request
           ) {
            return sharedOutcome
        }

        // playhead-01t8: register with the preemption coordinator so a
        // higher-lane admission can flip our signal at its next safe
        // point. The signal is threaded into `featureService` and
        // `transcriptEngine`; on observation those services
        // `acknowledge(jobId:)` themselves and exit cleanly. The
        // runner only needs to unregister on exit — acknowledge is
        // idempotent (it no-ops on an already-unregistered id).
        let preemption: PreemptionContext?
        if let coordinator = preemptionCoordinator {
            let lease = makeRegistrationLease(request: request)
            let signal = await coordinator.register(
                jobId: request.jobId,
                lane: request.schedulerLane,
                lease: lease
            )
            preemption = PreemptionContext(
                jobId: request.jobId,
                signal: signal,
                coordinator: coordinator
            )
        } else {
            preemption = nil
        }
        defer {
            // Fire-and-forget unregister. On the preempt path the
            // service already called `acknowledge`, so this is a no-op
            // by design (the id is already gone from the registry).
            // Callers cannot synchronously observe post-run
            // deregistration; the unregister is eventually consistent.
            if let coordinator = preemptionCoordinator {
                let jobId = request.jobId
                Task { await coordinator.unregister(jobId: jobId) }
            }
        }

        // -- Stage 1: Audio decode --

        let decodeSignpost = PreAnalysisInstrumentation.beginStage("decode")
        let allShards: [AnalysisShard]
        do {
            allShards = try await audioProvider.decode(
                fileURL: request.audioURL,
                episodeID: request.episodeId,
                shardDuration: AnalysisAudioService.defaultShardDuration
            )
            PreAnalysisInstrumentation.endStage(decodeSignpost)
        } catch {
            PreAnalysisInstrumentation.endStage(decodeSignpost)
            // playhead-q93o: BOUND ONCE, AND THE LOG CONSUMES THE SAME LOCAL.
            // This payload is carried into `analysis_jobs.lastErrorCode` and
            // `work_journal.metadata` by `AnalysisWorkScheduler`'s `.failed`
            // arms, two hops away in another file, so it is a durable value and
            // not a message. It used to be `"decode: \(error)"`, and
            // `AnalysisAudioError` is `CustomStringConvertible`, so what reached
            // the column was the enum's PROSE naming no case — plus, on three of
            // its other cases, the episode's FILENAME. Two field rows carried it
            // (see `DurableThrowRecord.swift`), one of them on the terminal arm
            // that permanently retired the job. The raw error keeps its place in
            // the LOG, which is 59c8's split: a log line can afford prose, a
            // column cannot.
            let throwRecord = DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: .decode)
            logger.error(
                "Decode failed for job \(request.jobId): \(error) token=\(throwRecord, privacy: .public)"
            )
            return makeOutcome(assetId: assetId, request: request, stopReason: .failed(code: throwRecord))
        }

        // Filter shards to the requested coverage depth.
        let shards = allShards.filter { $0.startTime < request.desiredCoverageSec }
        guard !shards.isEmpty else {
            // NOT a durable-prose site and deliberately left alone: this is a
            // compile-time literal, identical on every device and in every
            // locale, so it already groups. `RunnerMaterializerRegressionTests`
            // greps it verbatim.
            return makeOutcome(assetId: assetId, request: request, stopReason: .failed(code: "no shards within desired coverage"))
        }

        // playhead-5uvz.6 (Gap-7): persist the shard-sum duration onto
        // the `analysis_assets` row if it is still NULL, mirroring
        // ``AnalysisCoordinator/runFromSpooling``. The coverage guard at
        // ``AnalysisCoordinator/runFromBackfill`` needs
        // `episodeDurationSec` as a denominator; without this write,
        // any episode driven exclusively through Pipeline B (scheduler)
        // — e.g. an overnight backfill where the user never presses
        // play — leaves the column NULL and the gtt9.1.1 fail-safe
        // shortcut to `.restart` triggers on every Pipeline-B-only
        // episode. We compute from `allShards` (full decode) rather
        // than `shards` (coverage-bounded slice) so the persisted
        // duration is the true episode length, not the bounded slice
        // requested by this run.
        //
        // Idempotent + lazy: only writes when the column is NULL, so
        // re-running a partially-completed job — or a job whose
        // episodeDurationSec was already populated by Pipeline A — is
        // a no-op. A failed write is non-fatal: the in-memory
        // `episodeDuration` computation in stage 4 still works from
        // `allShards`, and the next run will retry the persist.
        let totalAudio = allShards.map(\.duration).reduce(0, +)
        if totalAudio > 0 {
            do {
                let asset = try await store.fetchAsset(id: assetId)
                if asset?.episodeDurationSec == nil {
                    try await store.updateEpisodeDuration(
                        id: assetId,
                        episodeDurationSec: totalAudio
                    )
                }
            } catch {
                logger.warning("Failed to persist episodeDurationSec=\(totalAudio) for asset \(assetId): \(error)")
            }
        }

        // playhead-xsdz.27 capture / playhead-xsdz.36 activation: fingerprint
        // the FULL decoded episode (`allShards`, not the coverage-bounded
        // `shards`) as the rediff A-side and persist it in AnalysisStore.
        // Live in production when `PlayheadRuntime` passes
        // `rediffASideCaptureEnabled: RediffActivation.isEnabledByDefault`;
        // with both flags off this branch is skipped and the pipeline is
        // byte-for-byte the pre-activation one. Failures are non-fatal and
        // never affect analysis.
        //
        // COST BOUND (xsdz.36): capture is skipped beyond
        // `RediffActivation.maxASideCaptureDurationSeconds` — the resample
        // transient is ~159 MB per decoded hour (chunk-aware walk; see
        // EpisodeFingerprintCapture). NOTE (R4): skipping capture removes the
        // episode from rediff re-fetch candidacy ENTIRELY — candidacy is
        // keyed on a current-version A-side stream row (see
        // `RediffActivation.maxASideCaptureDurationSeconds` for the full
        // consequence chain) — so over-cap episodes get no rediff marks at
        // all. The log line below must stay truthful for dogfood coverage
        // accounting.
        if rediffASideCaptureEnabled || EpisodeFingerprintCapture.captureEnabledByDefault {
            if totalAudio > RediffActivation.maxASideCaptureDurationSeconds {
                logger.info("Episode fingerprint capture skipped for asset \(assetId): duration \(Int(totalAudio))s exceeds the \(Int(RediffActivation.maxASideCaptureDurationSeconds))s capture cap — episode forfeits rediff re-fetch candidacy")
            } else {
                do {
                    // Skip (rather than persist a misleading empty identity) when
                    // the asset can't be read — `sourceAudioIdentity` must be the
                    // real assetFingerprint for the "audio changed under a reused
                    // asset id" check to mean anything.
                    if let asset = try await store.fetchAsset(id: assetId) {
                        // R3 recapture guard: `run(_:)` executes REPEATEDLY per
                        // episode (bounded coverage passes, interruption/resume
                        // cycles, seek re-latch), and the extractor walk is a
                        // full-episode fingerprint (~159 MB transient + real
                        // CPU) every time. When a CURRENT-version stream for
                        // THIS exact audio already exists, skip: the fetch
                        // already gates `algorithmVersion` + blob integrity
                        // (mismatch/corruption reads as nil → recapture), and
                        // the identity compare catches a re-downloaded copy
                        // under a reused asset id. Recapturing would produce
                        // the identical record AND bump `capturedAt` — which
                        // is the re-fetch enumerator's downloaded-at baseline,
                        // so every extra pass would re-arm the ~3d
                        // first-attempt gate and push the B-side re-fetch out
                        // indefinitely for episodes still being analyzed.
                        let alreadyCaptured = (try await store.fetchEpisodeFingerprints(assetId: assetId))
                            .map { $0.sourceAudioIdentity == asset.assetFingerprint } ?? false
                        if alreadyCaptured {
                            logger.debug("Episode fingerprint capture skipped for asset \(assetId): current-version stream already captured for this audio identity")
                        } else {
                            try await EpisodeFingerprintCapture.captureAndPersist(
                                shards: allShards,
                                assetId: assetId,
                                sourceAudioIdentity: asset.assetFingerprint,
                                store: store
                            )
                        }
                    } else {
                        logger.warning("Episode fingerprint capture skipped — asset \(assetId) not found")
                    }
                } catch {
                    logger.warning("Episode fingerprint capture failed for asset \(assetId): \(error)")
                }
            }
        }

        // -- Checkpoint: cancellation + thermal --

        if let earlyStop = checkStopConditions() {
            return makeOutcome(assetId: assetId, request: request, stopReason: earlyStop)
        }

        // -- Stage 2: Feature extraction --

        let featureSignpost = PreAnalysisInstrumentation.beginStage("features")
        let existingFeatureCoverage: Double
        do {
            let asset = try await store.fetchAsset(id: assetId)
            existingFeatureCoverage = asset?.featureCoverageEndTime ?? 0
        } catch {
            existingFeatureCoverage = 0
        }

        do {
            try await featureService.extractAndPersist(
                shards: shards,
                analysisAssetId: assetId,
                existingCoverage: existingFeatureCoverage,
                preemption: preemption
            )
            PreAnalysisInstrumentation.endStage(featureSignpost)
        } catch {
            PreAnalysisInstrumentation.endStage(featureSignpost)
            // playhead-q93o: see the decode catch above.
            let throwRecord = DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: .features)
            logger.error(
                "Feature extraction failed for job \(request.jobId): \(error) token=\(throwRecord, privacy: .public)"
            )
            return makeOutcome(assetId: assetId, request: request, stopReason: .failed(code: throwRecord))
        }

        // playhead-01t8: if the preempt signal flipped during feature
        // extraction, the service acknowledged at its safe point and
        // returned early. Detect it here before we spin up the heavy
        // transcription stage and report `.preempted` with whatever
        // coverage persisted.
        if let preemption, await preemption.isPreemptionRequested() {
            return makeOutcome(
                assetId: assetId,
                request: request,
                featureCoverageSec: await currentFeatureCoverage(assetId: assetId),
                stopReason: .preempted
            )
        }

        let featureCoverage = shards.map { $0.startTime + $0.duration }.max() ?? 0

        if let earlyStop = checkStopConditions() {
            return makeOutcome(
                assetId: assetId,
                request: request,
                featureCoverageSec: featureCoverage,
                stopReason: earlyStop
            )
        }

        // -- Stage 3: Transcription --

        let existingChunkCount = (try? await store.fetchTranscriptChunks(assetId: assetId).count) ?? 0
        // playhead-5uvz.7 (Gap-9): mark stage start so the zero-coverage
        // journal row can compute `chunk_rate_per_sec` against the actual
        // wall-clock spent inside the stage (rather than assuming the
        // 5-minute timeout always elapsed in full — a stream that ends
        // without `.completed` returns much earlier).
        let transcriptStageStart = clock()

        // playhead-pnb5: DO NOT RUN A STAGE THAT HAS NOTHING IN RANGE.
        //
        // `shards` is `allShards.filter { $0.startTime < request.desiredCoverageSec }`.
        // When every one of those shards is already backed by a persisted
        // transcript chunk below the watermark, the engine cannot produce a
        // chunk however long it runs — `runTranscriptionLoop` re-runs full ASR
        // over the lot (playhead-mptr ORDERS already-backed shards last rather
        // than skipping them) and the flat 300 s cap, or the background window,
        // ends the pass first. What that pass returned was a HARDCODED
        // `(0, nil, false)` from the timeout arm of the task group below, which
        // the accounting then read as `transcriptCoverageSec = 0`.
        //
        // MEASURED on Dan's phone, 2026-08-15, off `work_journal`. Six
        // `analysisJobRunner.run.transcriptionAlreadyComplete` rows across the
        // three wedged assets carry 31.4 / 39.3 / 157.6 / 208.6 / 216.0 /
        // 232.3 s of `slice_duration_ms`, and EVERY one is followed by
        // `analysisWorkScheduler.taskExpiredRequeue` in the SAME SECOND. Against
        // the preceding `acquired` row the whole rest of the pass — decode,
        // feature extraction, the asset reads — measures 0.23-1.52 s, so the
        // stage is 98.5-99.9 % of the run and the stage WAS the window. Across
        // the same window none of the three watermarks moved: 3C2FFE10 asked
        // 7,909.0 s of 7,999.0 s and sat at 7,920.0; CD2976E6 1,556.0 of
        // 1,675.8 at 1,560.0; E51B25E4 7,219.0 of 7,325.9 at 7,230.0. And
        // because `taskExpiredRequeue` writes no progress and mints no
        // successor, all three sat at their first-generation target for two days
        // holding the priority-10/20 slot.
        //
        // ONE THING THAT ROW DOES NOT SAY, because an earlier draft of this
        // comment read it as though it did: `transcriptionAlreadyComplete`
        // records no `chunks_persisted` key at all, so its ABSENCE is not a
        // measurement of zero. The sibling `transcriptionTimeout` row does
        // record it, and on E51B25E4 it reads 1,270 / 969 / 2,063 / 1,605 for
        // the runs of 04:38-06:43 — that asset was filling HOLES below its
        // watermark (playhead-mptr sorts an unbacked shard first however low it
        // sits), and those passes were productive. This admission does not fire
        // on them: an unbacked shard is exactly what keeps the stage running.
        // What the six rows above support is the branch they are in and the
        // seconds they cost, which is what this fix is about.
        //
        // THE ESCALATION PREDICATE IS NOT THE FIX, and the same rows are why.
        // `AnalysisWorkScheduler` mints the tier successor at
        // `case .reachedTarget where tierTargetSatisfied(...)`, and widening
        // that arm cannot reach these passes: they never reported an outcome at
        // all. They were cancelled mid-stage and accounted by the cancel-catch
        // arm, which is upstream of the outcome switch entirely. Removing the
        // stage lets the pass finish, report the watermark it really has, and
        // take the existing `.reachedTarget` arm unchanged.
        var transcriptCoverage: Double
        if let backedWatermark = await transcriptWatermarkWhenEveryAdmittedShardIsBacked(
            assetId: assetId,
            admittedShards: shards
        ) {
            await emitTranscriptionStageNotRunJournal(
                request: request,
                assetId: assetId,
                allShards: allShards,
                admittedShardCount: shards.count,
                transcriptStageStart: transcriptStageStart,
                transcriptCoverageSec: backedWatermark
            )
            logger.info(
                """
                Transcription stage not run for asset \(assetId): all \(shards.count) shards \
                within the requested \(request.desiredCoverageSec, format: .fixed(precision: 1))s \
                are already backed by persisted chunks (watermark \
                \(backedWatermark, format: .fixed(precision: 1))s) — continuing to ad detection
                """
            )
            transcriptCoverage = backedWatermark
        } else {
            // playhead-gtt9.1 — Acoustic transcript gate (shadow mode by default):
            //
            // Evaluate per-shard acoustic likelihood from the feature windows
            // we just persisted in stage 2. Each shard is tagged
            // `wouldGate=true` when its likelihood is below
            // `likelihoodThreshold`, with a `safetySampleFraction` of those
            // would-skip shards re-tagged `safety-sample-keep`. Shards
            // covered by an existing fast-transcript watermark (i.e., we're
            // re-running over good transcript) bypass the gate entirely
            // (`quality-precondition-keep`). Decisions are emitted to
            // `transcriptShadowGateLogger`. The default config ships
            // `enabled=true, skipEnabled=false`, so the gate logs but never
            // affects which shards reach the engine — production behavior
            // is unchanged until a follow-up bead flips `skipEnabled` to
            // true with sufficient shadow-eval evidence.
            let gatedShards: [AnalysisShard]
            if acousticGateConfig.isShadowLoggingActive {
                gatedShards = await evaluateAcousticTranscriptGate(
                    shards: shards,
                    assetId: assetId,
                    request: request
                )
            } else {
                // Master kill: no scoring, no logging — pre-gtt9.1 behavior
                // exactly. Hand the full shard list to the transcript engine
                // unchanged.
                gatedShards = shards
            }

            let transcriptSignpost = PreAnalysisInstrumentation.beginStage("transcription")
            let snapshot = PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: false)

            // playhead-ajr subscribe-before-start (RC-4): subscribe to the
            // engine's event stream BEFORE kicking off transcription. `events()`
            // hands back a fresh, NON-replaying `AsyncStream` continuation
            // (`TranscriptEngineService.events()`); any `.completed` emitted before
            // that continuation is registered is silently lost. On the previous
            // ordering (startTranscription + finishAppending, THEN events()), a
            // fast engine task could emit `.completed` in the gap — the runner
            // would miss it, fall through to the sibling 300 s timeout arm below,
            // and return a spurious `.failed("transcription:zeroCoverage")` after a
            // multi-minute hang (the flightcast "Operation Interrupted"/hang class
            // seen under full-suite / device load). Subscribing first closes that
            // window: the continuation is registered on the engine actor before
            // `startTranscription` spawns the transcription task, so no completion
            // can slip past. `events()` reads no post-start state (it only mints a
            // UUID + continuation), so establishing it early is safe.
            let transcriptStream = await transcriptEngine.events()

            // Fire-and-forget: startTranscription kicks off work internally.
            // `gatedShards` equals `shards` unless `skipEnabled` is also
            // active in `acousticGateConfig`; see the gate evaluator above.
            await transcriptEngine.startTranscription(
                shards: gatedShards,
                analysisAssetId: assetId,
                snapshot: snapshot,
                podcastId: request.podcastId,
                preemption: preemption
            )
            // Batch-mode caller: we hand the engine a static shard set and
            // have no streaming producer, so signal end-of-input immediately.
            // Without this the engine will park on `waitForMoreShards()`
            // forever and `.completed` never fires.
            await transcriptEngine.finishAppending(analysisAssetId: assetId)

            // Observe the event stream for completion, with a 5-minute timeout
            // to avoid hanging indefinitely if the stream never emits .completed.
            // playhead-8ysk: the loop now reports a total failure as `.failed`
            // instead of falsely reporting `.completed`, so the observer has to
            // treat it as terminal too — otherwise the runner would wait out the
            // full 300 s timeout for an event that will never come, turning an
            // instant named failure into a `task_expired`. The reason travels out
            // alongside the coverage and is journaled below.
            let transcriptObservation: (
                coverage: Double, failure: TranscriptFailureReason?, sawCompleted: Bool
            ) =
            await withTaskGroup(
                of: (Double, TranscriptFailureReason?, Bool).self
            ) { [weak self] group in
                // Timeout task. playhead-ngev: the `false` is load-bearing — it is
                // what the zero-coverage row reads as `engine_silent_timeout`,
                // separating "nobody said anything for five minutes" from "the
                // engine reported success over an empty transcript". Both used to
                // arrive here as an indistinguishable `(0, nil)`.
                group.addTask {
                    try? await Task.sleep(for: .seconds(300))
                    return (0, nil, false)
                }
                // Event stream task
                group.addTask { [weak self] in
                    // Bind the store out of `self` before the inner closure: a
                    // `[weak self]` capture is a `var`, and a `@Sendable` closure
                    // may not reference one.
                    let store = self?.store
                    return await Self.observeTranscriptEvents(
                        stream: transcriptStream,
                        assetId: assetId,
                        persistedCoverage: {
                            if let asset = try? await store?.fetchAsset(id: assetId) {
                                return asset.fastTranscriptCoverageEndTime ?? 0
                            }
                            return 0
                        }
                    )
                }
                // Return whichever finishes first
                let result = await group.next() ?? (0, nil, false)
                group.cancelAll()
                return result
            }
            transcriptCoverage = transcriptObservation.coverage
            let transcriptFailure = transcriptObservation.failure
            // playhead-ngev, A TRADEOFF TAKEN DELIBERATELY. An interruption is now
            // terminal for this observation, where before the loop said nothing and
            // the group ran to its 300 s ceiling. During that window the observer
            // was still subscribed, so a SUCCESSOR loop (the playback lane
            // re-tasking the shared engine after a scrub) completing the same asset
            // could rescue this job. Bailing immediately gives that up.
            //
            // It is still the right trade: the alternative costs a five-minute hold
            // on the scheduler's single running slot for every scrub — the shape
            // behind 147 acquisitions and 9 finalizations — and the work is not
            // lost, because the successor's coverage is durable and the retry after
            // backoff finds it already persisted. What it buys is the row: an
            // instant, named account of an interruption that used to be exported as
            // `asr_failed` five minutes after a listener touched the scrubber.
            //
            // playhead-ngev: what the runner itself observed, which it always
            // knows — unlike the class, which is absent on most routes here.
            let runObservation = TranscriptRunObservation.classify(
                failure: transcriptFailure,
                sawCompleted: transcriptObservation.sawCompleted
            )

            PreAnalysisInstrumentation.endStage(transcriptSignpost)

            // playhead-5uvz.5 (Gap-6): if the 5-minute timeout fired ahead
            // of `.completed`, the engine is still running in the
            // background — its subsequent `transcript_chunks` writes and
            // coverage updates would target an asset whose owning scheduler
            // has already moved on. Stop the engine for this asset before
            // the runner returns; the engine drops in-flight chunks and
            // gates any late writes/events for the stopped asset id.
            //
            // On every zero-coverage exit EXCEPT an interruption: a normal
            // `.completed` path also yields zero coverage when the engine
            // genuinely produced nothing, and stopping a session that
            // already terminated is a no-op aside from the gate insertion
            // (which is harmless because no further writes can land).
            //
            // playhead-ngev carves out the interruption — see
            // `shouldStopEngine(after:)`. There the engine is not orphaned but
            // re-tasked, and the stop would cancel the listener's own
            // transcription and fence the asset against its appends.
            if transcriptCoverage == 0, Self.shouldStopEngine(after: transcriptFailure) {
                await transcriptEngine.stopTranscription(analysisAssetId: assetId)
            }

            if transcriptCoverage == 0 {
                // playhead-01t8: if a preempt flipped during transcription,
                // the engine threw `TranscriptEnginePreempted` and exited
                // cleanly with whatever coverage it persisted (which may be
                // zero on the very first shard). Report `.preempted` rather
                // than `.failed` so scheduler bookkeeping treats this as a
                // deliberate hand-off, not a pipeline failure.
                if let preemption, await preemption.isPreemptionRequested() {
                    return makeOutcome(
                        assetId: assetId,
                        request: request,
                        featureCoverageSec: featureCoverage,
                        transcriptCoverageSec: 0,
                        stopReason: .preempted
                    )
                }
                // playhead-9y9e: A PASS THAT ADDS NO TRANSCRIPT TO AN ALREADY
                // TRANSCRIBED ASSET IS NOT A TRANSCRIPTION FAILURE, and treating it
                // as one is what stranded the ad scan on episodes the pipeline had
                // otherwise finished.
                //
                // The stage's 300 s cap is FLAT while the work under it is not:
                // `TranscriptEngineService.runTranscriptionLoop` re-runs full ASR
                // over every shard, and playhead-mptr deliberately ORDERS
                // already-backed shards last rather than skipping them (a skipped
                // shard can never take the duplicate-fingerprint arm's `speakerId` /
                // `avgConfidence` upgrade — see `TranscriptCoverageIndex
                // .orderingUncoveredFirst`). So on an asset with nothing left to
                // read, the entire budget is spent re-reading, the cap wins, and the
                // timeout arm of the task group above returns a HARDCODED
                // `(0, nil, false)` — it never consults `persistedCoverage()`, which
                // the `.completed` arm does. Zero here therefore means "this pass
                // added nothing", which on a finished transcript is success.
                //
                // Measured on the 2026-08-03 device pull, asset AD5F3A0A (4,281 s,
                // transcript watermark 4,281 s, 45 semantic scan rows): its ad-scan
                // re-drive `…:adScanRedrive:1` is `superseded` with
                // `maxAttemptsReached:transcription:zeroCoverage` after 5 attempts,
                // and `…:adScanRedrive:2` was still cycling on the same error. Both
                // ordinals of playhead-onn6's budget spent, Stage 4 never reached,
                // and two `fullEpisodeScan` rows left `queued`.
                //
                // The floor is `SemanticScanClaim.transcriptClearsFinalizeFloor`
                // (0.95 of the duration, as a gap-bridged AREA over BOTH transcript
                // passes) — the same judgement playhead-fil5's sweep makes about
                // whether an asset is still the transcript lane's problem, so there
                // is one policy rather than two that drift. BELOW that floor nothing
                // changes: the transcript genuinely is incomplete, the retry and
                // journal accounting below is the correct account of it, and a
                // silent engine must keep reading as a failure.
                // AN INTERRUPTION IS EXCLUDED, and that exclusion is load-bearing.
                // playhead-ngev makes a listener's scrub terminate this observation
                // instantly and hand the scheduler's single running slot back —
                // `.interrupted` costs the job no attempt. Continuing into ad
                // detection here would hold that slot through a whole detection pass
                // while the listener is moving the playhead, which is the cost ngev
                // paid a design tradeoff to avoid. The re-drive it strands is not
                // stranded: no attempt was spent, so the retry comes back.
                if transcriptFailure?.termination != .interrupted,
                   let alreadyTranscribed = await transcriptCoverageOfCompletedTranscript(
                    assetId: assetId
                ) {
                    await emitTranscriptionAlreadyCompleteJournal(
                        request: request,
                        assetId: assetId,
                        allShards: allShards,
                        transcriptStageStart: transcriptStageStart,
                        transcriptCoverageSec: alreadyTranscribed,
                        observation: runObservation
                    )
                    logger.info(
                        """
                        Transcription for asset \(assetId) added no coverage, but the persisted \
                        transcript already covers the episode (\(alreadyTranscribed, format: .fixed(precision: 1))s) \
                        — continuing to ad detection
                        """
                    )
                    transcriptCoverage = alreadyTranscribed
                } else {
                    logger.warning("Transcription for asset \(assetId) finished with zero coverage — stream may have ended prematurely or timed out")
                    // playhead-5uvz.7 (Gap-9): write a structured row to
                    // `work_journal` so a class of episodes that systematically
                    // times out (long, refusal-prone, music-heavy) shows up in
                    // aggregate without operators having to grep `lastErrorCode`
                    // across `analysis_jobs`.
                    //
                    // playhead-rqgr: BOTH RECORDS OF THIS EXIT COME OUT OF ONE
                    // VALUE, and the journal row is not the diagnostic byproduct
                    // this comment used to call "observability gravy". The row's
                    // `eventType` is what `AnalysisCoordinator.recoverOrphans`
                    // routes a cold-launch orphan on, so the journal row and the
                    // stop reason below are two statements about the same event
                    // and both are load-bearing. Minting them together is what
                    // stops them disagreeing.
                    //
                    // Still best-effort in the sense that matters: an append
                    // failure logs and does NOT alter the outcome. What is no
                    // longer true is that nothing reads it.
                    let disposition = Self.zeroCoverageDisposition(
                        failure: transcriptFailure,
                        observation: runObservation
                    )
                    await emitTranscriptionTimeoutJournal(
                        request: request,
                        assetId: assetId,
                        allShards: allShards,
                        existingChunkCount: existingChunkCount,
                        transcriptStageStart: transcriptStageStart,
                        failure: transcriptFailure,
                        observation: runObservation,
                        disposition: disposition
                    )
                    return makeOutcome(
                        assetId: assetId,
                        request: request,
                        featureCoverageSec: featureCoverage,
                        transcriptCoverageSec: 0,
                        // playhead-8ysk: name the cause in `analysis_jobs.lastErrorCode`
                        // too. It was a fixed `transcription:zeroCoverage` for every one
                        // of the nine distinguishable causes.
                        //
                        // playhead-ngev (review r1): and route it by TERMINATION, so a
                        // listener moving the playhead does not spend one of the job's
                        // five permanent retry attempts. See `zeroCoverageDisposition`.
                        stopReason: disposition.stopReason
                    )
                }
            }
        }

        if let earlyStop = checkStopConditions() {
            return makeOutcome(
                assetId: assetId,
                request: request,
                featureCoverageSec: featureCoverage,
                transcriptCoverageSec: transcriptCoverage,
                stopReason: earlyStop
            )
        }

        // playhead-01t8: honor a preempt that landed during
        // transcription. The transcript engine acknowledged at its
        // safe point and exited; we report `.preempted` with the
        // coverage it managed to persist rather than burning the Now
        // lane's admission budget on ad detection.
        if let preemption, await preemption.isPreemptionRequested() {
            return makeOutcome(
                assetId: assetId,
                request: request,
                featureCoverageSec: featureCoverage,
                transcriptCoverageSec: transcriptCoverage,
                stopReason: .preempted
            )
        }

        // -- Stage 4: Ad detection --

        let detectionSignpost = PreAnalysisInstrumentation.beginStage("ad_detection")
        let chunks: [TranscriptChunk]
        do {
            chunks = try await store.fetchTranscriptChunks(assetId: assetId)
        } catch {
            PreAnalysisInstrumentation.endStage(detectionSignpost)
            // playhead-q93o: see the decode catch above.
            let throwRecord = DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: .fetchChunks)
            logger.error(
                "Failed to fetch transcript chunks for job \(request.jobId): \(error) token=\(throwRecord, privacy: .public)"
            )
            return makeOutcome(
                assetId: assetId,
                request: request,
                featureCoverageSec: featureCoverage,
                transcriptCoverageSec: transcriptCoverage,
                stopReason: .failed(code: throwRecord)
            )
        }
        let wroteNewChunks = chunks.count > existingChunkCount
        let existingWindowsBeforeDetection = (try? await store.fetchAdWindows(assetId: assetId)) ?? []
        let existingCandidateWindows = existingWindowsBeforeDetection.filter {
            $0.decisionState == AdDecisionState.candidate.rawValue
        }

        let episodeDuration = allShards.map { $0.startTime + $0.duration }.max() ?? 0

        // Hot path detection.
        var adWindows: [AdWindow] = []
        let skippedHotPath = !wroteNewChunks && !existingWindowsBeforeDetection.isEmpty
        if skippedHotPath {
            logger.info(
                "Skipping hot path for asset \(assetId): transcription produced no new chunks and \(existingWindowsBeforeDetection.count) windows already exist"
            )
        } else {
            do {
                adWindows = try await adDetection.runHotPath(
                    chunks: chunks,
                    analysisAssetId: assetId,
                    episodeDuration: episodeDuration,
                    podcastId: request.podcastId
                )
            } catch {
                PreAnalysisInstrumentation.endStage(detectionSignpost)
                // playhead-q93o: see the decode catch above.
                let throwRecord = DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: .hotPath)
                logger.error(
                    "Hot-path detection failed for job \(request.jobId): \(error) token=\(throwRecord, privacy: .public)"
                )
                return makeOutcome(
                    assetId: assetId,
                    request: request,
                    featureCoverageSec: featureCoverage,
                    transcriptCoverageSec: transcriptCoverage,
                    stopReason: .failed(code: throwRecord)
                )
            }
        }

        if let earlyStop = checkStopConditions() {
            PreAnalysisInstrumentation.endStage(detectionSignpost)
            return makeOutcome(
                assetId: assetId,
                request: request,
                featureCoverageSec: featureCoverage,
                transcriptCoverageSec: transcriptCoverage,
                stopReason: earlyStop
            )
        }

        // Backfill detection.
        //
        // playhead-i7qe: the skip decision now also consults MEASURED ad-scan
        // coverage. See `shouldSkipSemanticBackfill` for why the two original
        // terms were not enough.
        let finalWindows: [AdWindow]
        // Only pay for the coverage read when it can change the answer. The
        // three booleans are free; the read is four prepared statements plus an
        // interval union over the asset's whole fast-chunk set, and during
        // active transcription (`wroteNewChunks == true`) its result is provably
        // discarded. The pre-check is the SAME predicate the decision uses, so
        // the two cannot drift.
        let noOtherWork = Self.semanticBackfillHasNoOtherWork(
            wroteNewChunks: wroteNewChunks,
            hasExistingWindows: !existingWindowsBeforeDetection.isEmpty,
            hasCandidateWindows: !existingCandidateWindows.isEmpty
        )
        let adScanFraction = noOtherWork
            ? await measuredAdScanFraction(assetId: assetId)
            : nil
        let skippedBackfill = Self.shouldSkipSemanticBackfill(
            wroteNewChunks: wroteNewChunks,
            hasExistingWindows: !existingWindowsBeforeDetection.isEmpty,
            hasCandidateWindows: !existingCandidateWindows.isEmpty,
            adScanFraction: adScanFraction
        )
        if skippedBackfill {
            logger.info(
                "Skipping backfill for asset \(assetId): transcription produced no new chunks, there are no candidate windows to resolve, and the semantic ad scan already covers the episode"
            )
            finalWindows = existingWindowsBeforeDetection
        } else {
            do {
                try await adDetection.runBackfill(
                    chunks: chunks,
                    analysisAssetId: assetId,
                    podcastId: request.podcastId,
                    episodeDuration: episodeDuration
                )
            } catch {
                PreAnalysisInstrumentation.endStage(detectionSignpost)
                // playhead-q93o: see the decode catch above.
                let throwRecord = DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: .backfill)
                logger.error(
                    "Backfill detection failed for job \(request.jobId): \(error) token=\(throwRecord, privacy: .public)"
                )
                return makeOutcome(
                    assetId: assetId,
                    request: request,
                    featureCoverageSec: featureCoverage,
                    transcriptCoverageSec: transcriptCoverage,
                    stopReason: .failed(code: throwRecord)
                )
            }

            // Reload windows after backfill may have updated/added them.
            do {
                finalWindows = try await store.fetchAdWindows(assetId: assetId)
            } catch {
                finalWindows = adWindows
            }
        }

        PreAnalysisInstrumentation.endStage(detectionSignpost)

        // Compute coverage from cue-eligible windows only, mirroring
        // the 0.7 threshold the (now-deleted) `SkipCueMaterializer` used
        // while excluding suppressed/non-ad decisions from banner/cue
        // progress.
        let cueCoverage = finalWindows
            .filter(Self.isCueWindow)
            .map(\.endTime)
            .max() ?? 0

        // -- Stage 5: Cue accounting (policy-dependent) --
        //
        // Bug 5 (skip-cues-deletion): the cue materialization stage was
        // removed when the `skip_cues` table was deleted. `newCueCount`
        // is now defined as the count of cue-eligible windows that are
        // newly present after this run (i.e. did not exist in
        // `existingWindowsBeforeDetection`). The scheduler uses
        // `newCueCount > 0` as a "made progress" signal in
        // `shouldRetryCoverageInsufficient`; that signal is preserved.
        //
        // `outputPolicy` is preserved as-is: `.writeWindowsOnly` and
        // `.writeWindowsAndPushLive` continue to mean "do not produce a
        // cue count." `.writeWindowsAndCues` is the only policy that
        // surfaces the count, matching prior semantics from the caller's
        // perspective.
        var newCueCount = 0
        if request.outputPolicy == .writeWindowsAndCues {
            let priorCueIds = Set(
                existingWindowsBeforeDetection
                    .filter(Self.isCueWindow)
                    .map(\.id)
            )
            newCueCount = finalWindows.filter {
                Self.isCueWindow($0) && !priorCueIds.contains($0.id)
            }.count
        }

        await publishSharedAnalysisIfEnabled(
            assetId: assetId,
            podcastId: request.podcastId,
            outputPolicy: request.outputPolicy
        )

        return AnalysisOutcome(
            assetId: assetId,
            requestedCoverageSec: request.desiredCoverageSec,
            featureCoverageSec: featureCoverage,
            transcriptCoverageSec: transcriptCoverage,
            cueCoverageSec: cueCoverage,
            newCueCount: newCueCount,
            stopReason: .reachedTarget
        )
    }

    // MARK: - Stop Condition Checks

    private func importSharedAnalysisIfAvailable(
        assetId: String,
        request: AnalysisRangeRequest
    ) async -> AnalysisOutcome? {
        guard analysisSharingProvider.isEnabled else { return nil }
        guard let asset = try? await store.fetchAsset(id: assetId) else { return nil }

        guard let key = CrossUserAnalysisShareKey.make(
            podcastId: request.podcastId,
            fileSHA: asset.assetFingerprint,
            analysisVersion: asset.analysisVersion
        ) else { return nil }
        guard let snapshot = await analysisSharingProvider.matchingSnapshot(for: key) else {
            return nil
        }
        guard snapshot.analysisCoverageEndSec >= request.desiredCoverageSec else {
            logger.info("Shared analysis snapshot for asset \(assetId) covers \(snapshot.analysisCoverageEndSec)s, below requested \(request.desiredCoverageSec)s — falling back to full analysis")
            return nil
        }

        do {
            let result = try await store.importCrossUserAnalysisSnapshot(
                snapshot,
                targetAssetId: assetId,
                podcastId: request.podcastId
            )
            guard case .imported(let receipt) = result else {
                logger.info("Shared analysis snapshot did not match asset \(assetId): \(String(describing: result))")
                return nil
            }
            logger.info("Imported shared analysis for asset \(assetId): inserted \(receipt.insertedWindowCount) windows, cueCoverage=\(receipt.cueCoverageSec)")
            await publishImportedSharedAdWindows(
                receipt: receipt,
                assetId: assetId,
                outputPolicy: request.outputPolicy
            )
            return makeOutcome(
                assetId: assetId,
                request: request,
                featureCoverageSec: asset.featureCoverageEndTime ?? 0,
                transcriptCoverageSec: asset.fastTranscriptCoverageEndTime ?? 0,
                cueCoverageSec: receipt.cueCoverageSec,
                newCueCount: request.outputPolicy == .writeWindowsAndCues ? receipt.insertedCueCount : 0,
                stopReason: .reachedTarget
            )
        } catch {
            logger.warning("Shared analysis import failed for asset \(assetId): \(error.localizedDescription) — falling back to full analysis")
            return nil
        }
    }

    private func publishImportedSharedAdWindows(
        receipt: CrossUserAnalysisImportReceipt,
        assetId: String,
        outputPolicy: OutputPolicy
    ) async {
        guard outputPolicy != .writeWindowsOnly,
              !receipt.bannerEligibleWindowIds.isEmpty else {
            return
        }

        do {
            let bannerEligibleIds = Set(receipt.bannerEligibleWindowIds)
            let windows = try await store.fetchAdWindows(assetId: assetId)
            let importedWindows = windows.filter {
                bannerEligibleIds.contains($0.id) && Self.isCueWindow($0)
            }
            guard !importedWindows.isEmpty else { return }
            await analysisSharingProvider.didImportSharedAdWindows(importedWindows)
        } catch {
            logger.warning("Shared analysis import notification failed for asset \(assetId): \(error.localizedDescription)")
        }
    }

    private func publishSharedAnalysisIfEnabled(
        assetId: String,
        podcastId: String,
        outputPolicy: OutputPolicy
    ) async {
        guard analysisSharingProvider.isEnabled,
              outputPolicy != .writeWindowsOnly else { return }

        do {
            guard let snapshot = try await store.exportCrossUserAnalysisSnapshot(
                assetId: assetId,
                podcastId: podcastId
            ) else {
                return
            }
            try await analysisSharingProvider.publish(snapshot)
            logger.info("Published shared analysis for asset \(assetId): windows=\(snapshot.windows.count), coverage=\(snapshot.analysisCoverageEndSec)")
        } catch {
            logger.warning("Shared analysis publish failed for asset \(assetId): \(error.localizedDescription)")
        }
    }

    // MARK: - Semantic-backfill admission (playhead-i7qe)

    /// playhead-i7qe: may this run skip the semantic ad-scan backfill?
    ///
    /// THE BUG THIS FIXES. The predicate used to be
    /// `!wroteNewChunks && hasExistingWindows && !hasCandidateWindows` — read
    /// aloud, "the transcript did not grow and there are no candidate windows
    /// to resolve, so there is nothing for backfill to do". Both terms are
    /// about work that ALREADY EXISTS, and neither is about the audio that was
    /// never read:
    ///
    ///   * `!wroteNewChunks` is a transcript-coverage term. It is permanently
    ///     true the moment the transcript completes — which is exactly the
    ///     state of every asset this bead is about.
    ///   * `!hasCandidateWindows` is a QUANTITY THAT NAMES AN ABSENCE. Candidate
    ///     windows are PRODUCED BY the semantic scan. In the part of the
    ///     episode the scan never reached, there are no candidates precisely
    ///     because nothing ever looked. Reading that as "nothing left to do"
    ///     inverts its meaning.
    ///
    /// Together they made the semantic scan unreachable for the exact shape
    /// playhead-i7qe describes: transcript complete, scan truncated, no
    /// candidates outstanding. Re-running the job could not help, however many
    /// times it ran. Measured on the 2026-07-29 device pull, seven assets sat
    /// in that shape, including the audited episode 820134BF (transcript
    /// 2113/2113 s, ad scan 0.388, zero candidate windows).
    ///
    /// The fix adds the term that was missing: measured ad-scan coverage. Skip
    /// only when the audio has demonstrably been read. `nil` (coverage not
    /// honestly measurable) does NOT permit a skip — under-claim, and let the
    /// scan run.
    ///
    /// Bounded, and NOT a spin — but not free either, and the difference is
    /// worth stating precisely. Not skipping means one call into
    /// `AdDetectionService.runBackfill` per job run, and that is the whole
    /// detection pipeline (canonicalization, evidence catalog, lexical scan,
    /// classifier, feature-window fetch, fusion, boundary refinement), not just
    /// the Foundation Models layer. What is budgeted is the FM layer inside it:
    /// `BackfillJobRunner.runPendingBackfill` skips `complete` jobs outright and
    /// refuses to re-enqueue a job whose persisted `retryCount` has reached
    /// `AdmissionController.maxRetries`, and the scheduler caps a job at
    /// `maxAttemptCount` dispatches with backoff. So an asset whose scan cannot
    /// advance costs a bounded number of full detection passes, never an
    /// unbounded loop.
    ///
    /// SCOPE — what this does NOT do. It governs runs that are already being
    /// dispatched; it does not cause a run to happen. Once an episode's coverage
    /// tiers are done `AnalysisWorkScheduler` writes `analysis_jobs.state =
    /// "complete"`, and `insertJob` is `INSERT OR IGNORE` on a `workKey` that
    /// completed row already owns, so no new job is minted. An episode that
    /// finishes its tiers under-scanned therefore keeps its degraded terminal
    /// until something else re-queues it. Minting that re-drive is a change to
    /// the scheduler's terminal accounting and is deliberately not in this bead
    /// — see playhead-i7qe's follow-up.
    ///
    /// - Parameter adScanFraction: ``AnalysisCoverageSummary/adScanFraction``
    ///   for the asset (playhead-pz32), or `nil` when unmeasurable.
    static func shouldSkipSemanticBackfill(
        wroteNewChunks: Bool,
        hasExistingWindows: Bool,
        hasCandidateWindows: Bool,
        adScanFraction: ReachRatio?
    ) -> Bool {
        guard semanticBackfillHasNoOtherWork(
            wroteNewChunks: wroteNewChunks,
            hasExistingWindows: hasExistingWindows,
            hasCandidateWindows: hasCandidateWindows
        ) else {
            return false
        }
        guard let adScanFraction = adScanFraction.finiteValue else { return false }
        return adScanFraction >= semanticBackfillSufficientAdScanFraction
    }

    /// playhead-i7qe: the three ORIGINAL skip terms, unchanged in meaning —
    /// "this run produced no new transcript text and there are no candidate
    /// windows outstanding". True means the only remaining question is whether
    /// the audio has been read for ads.
    ///
    /// Extracted so the call site can decide whether the ad-scan coverage read
    /// is worth paying for WITHOUT restating the condition, which is how the two
    /// would drift apart.
    static func semanticBackfillHasNoOtherWork(
        wroteNewChunks: Bool,
        hasExistingWindows: Bool,
        hasCandidateWindows: Bool
    ) -> Bool {
        !wroteNewChunks && hasExistingWindows && !hasCandidateWindows
    }

    /// playhead-i7qe: measured ad-scan coverage at or above which the semantic
    /// backfill may be skipped for an otherwise-idle run.
    ///
    /// Deliberately the SAME number the library ✓ uses
    /// (``episodePreparationCompleteThreshold``), so the pipeline stops
    /// scanning at exactly the point the surface is willing to call the episode
    /// read. A lower floor here would produce episodes the pipeline considers
    /// done and the UI still marks ◐, with nothing able to close the gap.
    /// playhead-x0lb: a ``ReachRatio``. Typing the FLOOR is what makes the
    /// comparison typed at every consumer — a ``DensityRatio`` can no longer be
    /// measured against the floor a completed AD SCAN is judged by.
    static var semanticBackfillSufficientAdScanFraction: ReachRatio {
        episodePreparationCompleteThreshold
    }

    /// playhead-i7qe: read the asset's measured ad-scan coverage fraction.
    /// Sourced from ``AnalysisCoverageSummary/adScanFraction`` — never
    /// recomputed here, so the runner, the terminal classifier and the library
    /// ✓ all divide the same numerator by the same denominator.
    ///
    /// A store failure returns `nil`, which forbids the skip. Erring towards
    /// running the scan costs one pass; erring the other way is the bug.
    private func measuredAdScanFraction(assetId: String) async -> ReachRatio? {
        do {
            return try await store.fetchCoverageSummariesByAssetIds([assetId])[assetId]?
                .adScanFraction
        } catch {
            logger.warning(
                "Ad-scan coverage fetch failed for asset \(assetId): \(error.localizedDescription); running semantic backfill"
            )
            return nil
        }
    }

    private static func isCueWindow(_ window: AdWindow) -> Bool {
        // playhead-ar60: DETECTION, deliberately. "Does this asset have
        // actionable cues?" is a question about how much ad the pipeline
        // FOUND, and it feeds progress/coverage accounting — not a skip. Before
        // V47 this floor saw the fusion path's actuation number, so an asset
        // could report no cues purely because the user had corrected something
        // else on it. `eligibilityGate` below is what withholds actuation.
        window.confidence >= cueConfidenceThreshold
            && window.endTime > window.startTime
            && isActionableCueEligibilityGate(window.eligibilityGate)
            && (
                window.decisionState == AdDecisionState.candidate.rawValue
                    || window.decisionState == AdDecisionState.confirmed.rawValue
                    || window.decisionState == AdDecisionState.applied.rawValue
            )
    }

    private static func isActionableCueEligibilityGate(_ eligibilityGate: String?) -> Bool {
        guard let eligibilityGate else { return true }
        if eligibilityGate == "autoSkip" { return true }
        guard let decoded = SkipEligibilityGate(rawValue: eligibilityGate) else {
            return false
        }
        return decoded == .eligible
    }

    /// Check for cancellation and critical thermal distress between pipeline
    /// stages.
    /// Returns a stop reason if the runner should bail out, nil to continue.
    private func checkStopConditions() -> AnalysisOutcome.StopReason? {
        if Task.isCancelled {
            return .cancelledByPlayback
        }

        let thermalState = thermalStateProvider()
        if thermalState == .critical {
            logger.warning("Thermal state \(String(describing: thermalState)) — pausing analysis")
            return .pausedForThermal
        }

        return nil
    }

    // MARK: - Acoustic transcript gate (playhead-gtt9.1)

    /// Evaluate the acoustic-likelihood transcript gate over `shards` and
    /// emit one shadow-log row per shard. Returns the shard list to actually
    /// hand to the transcript engine — equal to `shards` unless
    /// `acousticGateConfig.isProductionSkipActive` is true, in which case
    /// `would-skip`-tagged shards are filtered out (production behavior;
    /// not the default).
    ///
    /// Caller invariants:
    ///   * Only invoked when `acousticGateConfig.isShadowLoggingActive` —
    ///     the master-disabled path short-circuits without ever calling
    ///     this method.
    ///   * `shards` is the post-coverage-filter list (already trimmed to
    ///     `[0, desiredCoverageSec]`); we don't re-apply that filter.
    ///
    /// Decision categories per shard:
    ///   * `.qualityPreconditionKeep` — shard is fully covered by an
    ///     existing fast-transcript watermark, i.e. we're re-running over
    ///     good transcript. M1 mitigation: the gate never withdraws shards
    ///     mid-stream from an already-running transcription.
    ///   * `.scoreUnknown` — no overlapping `feature_windows` row exists
    ///     for this shard. We refuse to gate out unknowns.
    ///   * `.aboveThreshold` — likelihood ≥ `likelihoodThreshold`.
    ///     Transcribe.
    ///   * `.safetySampleKeep` — likelihood < threshold but the safety-
    ///     sample coin came up heads. Transcribe so we keep a calibration
    ///     stream of low-likelihood ground truth even after `skipEnabled`
    ///     flips.
    ///   * `.wouldSkip` — likelihood < threshold and the safety-sample
    ///     coin came up tails. In shadow mode we still transcribe; in
    ///     production-skip mode this shard is dropped from the engine
    ///     input.
    private func evaluateAcousticTranscriptGate(
        shards: [AnalysisShard],
        assetId: String,
        request: AnalysisRangeRequest
    ) async -> [AnalysisShard] {
        // Fetch all feature windows that could overlap any shard. We pull
        // from the union span [minStart, maxEnd] in a single query so the
        // per-shard `maxLikelihoodInSpan` walk runs in memory.
        let spanStart = shards.map(\.startTime).min() ?? 0
        let spanEnd = shards.map { $0.startTime + $0.duration }.max() ?? 0
        let featureWindows: [FeatureWindow]
        if spanEnd > spanStart {
            featureWindows = (try? await store.fetchFeatureWindows(
                assetId: assetId,
                from: spanStart,
                to: spanEnd
            )) ?? []
        } else {
            featureWindows = []
        }

        // Resolve M1 mitigation precondition: the asset's persisted
        // fast-transcript watermark. A shard is "already covered by good
        // transcript" iff its end time ≤ that watermark (and the
        // watermark is non-nil). On a fresh run the watermark is nil and
        // every shard is gate-eligible.
        let priorTranscriptCoverage: Double?
        if let asset = try? await store.fetchAsset(id: assetId) {
            priorTranscriptCoverage = asset.fastTranscriptCoverageEndTime
        } else {
            priorTranscriptCoverage = nil
        }

        var keptShards: [AnalysisShard] = []
        keptShards.reserveCapacity(shards.count)
        let now = clock()

        for shard in shards {
            let shardEnd = shard.startTime + shard.duration

            // M1: quality precondition. If the shard's end time falls at
            // or below the persisted fast-transcript watermark, transcript
            // already exists for this region — bypass scoring entirely.
            // Tag the row so the eval side can confirm M1 fired as
            // designed.
            if let cov = priorTranscriptCoverage, cov >= shardEnd, shardEnd > 0 {
                let entry = TranscriptShadowGateEntry(
                    schemaVersion: TranscriptShadowGateEntry.currentSchemaVersion,
                    timestamp: now.timeIntervalSince1970,
                    analysisAssetID: assetId,
                    episodeID: request.episodeId,
                    shardID: shard.id,
                    shardStart: shard.startTime,
                    shardEnd: shardEnd,
                    likelihood: nil,
                    threshold: acousticGateConfig.likelihoodThreshold,
                    decision: .qualityPreconditionKeep,
                    wouldGate: false,
                    transcribed: true,
                    buildCommitSHA: nil
                )
                await transcriptShadowGateLogger.record(entry)
                keptShards.append(shard)
                continue
            }

            // Score the shard. `nil` means no overlapping feature window
            // is persisted yet — treat as unknown and never gate out.
            let likelihood = AcousticLikelihoodScorer.maxLikelihoodInSpan(
                windows: featureWindows,
                startTime: shard.startTime,
                endTime: shardEnd
            )

            let decision: TranscriptShadowGateEntry.Decision
            let wouldGate: Bool
            let transcribed: Bool
            if let s = likelihood {
                if s >= acousticGateConfig.likelihoodThreshold {
                    decision = .aboveThreshold
                    wouldGate = false
                    transcribed = true
                } else {
                    // Below threshold — would-skip candidate. Apply the
                    // safety-sample coin flip. With sample fraction = 0.10
                    // a uniform draw on `[0, 1)` < 0.10 is the keep arm.
                    let coin = safetySampleRNG()
                    if coin < acousticGateConfig.safetySampleFraction {
                        decision = .safetySampleKeep
                        wouldGate = true
                        transcribed = true
                    } else {
                        decision = .wouldSkip
                        wouldGate = true
                        // In production-skip mode the shard is dropped
                        // from the engine input. In shadow mode (default)
                        // we still transcribe.
                        transcribed = !acousticGateConfig.isProductionSkipActive
                    }
                }
            } else {
                decision = .scoreUnknown
                wouldGate = false
                transcribed = true
            }

            let entry = TranscriptShadowGateEntry(
                schemaVersion: TranscriptShadowGateEntry.currentSchemaVersion,
                timestamp: now.timeIntervalSince1970,
                analysisAssetID: assetId,
                episodeID: request.episodeId,
                shardID: shard.id,
                shardStart: shard.startTime,
                shardEnd: shardEnd,
                likelihood: likelihood,
                threshold: acousticGateConfig.likelihoodThreshold,
                decision: decision,
                wouldGate: wouldGate,
                transcribed: transcribed,
                buildCommitSHA: nil
            )
            await transcriptShadowGateLogger.record(entry)

            if transcribed {
                keptShards.append(shard)
            }
        }
        return keptShards
    }

    // MARK: - Preemption helpers (playhead-01t8)

    /// Build a synthetic `EpisodeExecutionLease` purely for
    /// `LanePreemptionCoordinator.register(...)` diagnostics. The
    /// runner is invoked from `AnalysisWorkScheduler`, which uses its
    /// own `analysis_jobs`-row lease — claimed via
    /// `AnalysisStore.acquireLeaseWithJournal(...)` (playhead-5uvz.1)
    /// so the lease takeover and `work_journal.acquired` row commit
    /// atomically — which is structurally different from
    /// `EpisodeExecutionLease` (owned by `AnalysisCoordinator`). The coordinator only stores
    /// the lease value on its `LanePreemptionRegistration` for
    /// diagnostics — it never re-acquires it or reads any of its
    /// fields to make decisions. A synthetic value is therefore
    /// fidelity-preserving for the preemption contract.
    private func makeRegistrationLease(
        request: AnalysisRangeRequest
    ) -> EpisodeExecutionLease {
        let now = clock().timeIntervalSince1970
        return EpisodeExecutionLease(
            episodeId: request.episodeId,
            ownerWorkerId: "preAnalysis:\(request.jobId)",
            generationID: UUID(),
            schedulerEpoch: 0,
            acquiredAt: now,
            expiresAt: now + 300,
            currentCheckpoint: nil,
            preemptionRequested: false
        )
    }

    /// Look up the current persisted feature coverage for an asset
    /// so the `.preempted` outcome can report accurate coverage
    /// without re-walking the feature batch in memory.
    private func currentFeatureCoverage(assetId: String) async -> Double {
        guard let asset = try? await store.fetchAsset(id: assetId) else { return 0 }
        return asset.featureCoverageEndTime ?? 0
    }

    // MARK: - Transcription timeout journaling (playhead-5uvz.7)

    /// Emit a structured `work_journal` row when stage 3 produced zero
    /// coverage (timeout firing ahead of `.completed`, or a stream that
    /// ended prematurely without ever advancing the watermark). The row
    /// carries the caller's ``ZeroCoverageDisposition`` — its `eventType`
    /// and `cause` — plus a JSON metadata blob describing the episode shape
    /// and the engine's progress at the moment of the exit:
    /// `episode_duration`, `transcript_coverage_end_time`,
    /// `chunks_persisted`, and `chunk_rate_per_sec` — so operators can spot
    /// a systematic stall pattern (long, refusal-prone, music-heavy
    /// episodes) in aggregate rather than grepping `lastErrorCode` across
    /// `analysis_jobs`.
    ///
    /// **THE `eventType` IS A RECOVERY INPUT, NOT OBSERVABILITY (playhead-rqgr).**
    /// This doc said the row was "observability gravy" and that
    /// `analysis_jobs.lastErrorCode = 'transcription:zeroCoverage'` was the
    /// primary signal. That was wrong in the way that mattered:
    /// ``AnalysisCoordinator/recoverOrphans(now:graceSeconds:)`` routes a
    /// stranded job on the LAST journal row for its `{episode, generation}`
    /// — `.failed` clears the lease with no requeue, everything else
    /// resumes — and it never looks at `lastErrorCode` at all. On the
    /// strength of the word "gravy" this method wrote a hardcoded `.failed`
    /// for every zero-coverage exit including the interrupted ones, whose
    /// own outcome (`.interrupted`) exists precisely so the job spends no
    /// attempt and comes back. A process death before the scheduler's
    /// requeue then made a cold launch do the opposite.
    ///
    /// The event is therefore NOT chosen here. It arrives in
    /// `disposition`, minted by
    /// ``zeroCoverageDisposition(failure:observation:)`` in the same
    /// expression as the `StopReason` the caller returns, so the two
    /// records cannot disagree. Adding a local `eventType` argument or
    /// literal here would re-open exactly the hole this bead closed.
    ///
    /// Best-effort in the one sense that survives: a fetch / append failure
    /// logs at warning level and does NOT alter the runner's outcome.
    /// playhead-8ysk: the `metadata` keys that make a zero-coverage journal
    /// row diagnostic rather than merely present.
    ///
    /// `failure_class` is a `TranscriptFailureClass` raw value — a closed,
    /// compile-time vocabulary — and `failure_code` is an integer, so neither
    /// can carry PII, and both therefore survive the diagnostics-bundle
    /// projection that (correctly) drops the rest of this blob.
    ///
    /// The keys are ABSENT, not empty, when the engine reported no reason:
    /// "we do not know" and "nothing went wrong" must not look alike in an
    /// export that a support engineer reads without a device attached.
    ///
    /// Hoisted out of `emitTranscriptionTimeoutJournal` so the write side can
    /// be exercised against the same `DiagnosticsFailureKeys` the bundle
    /// projection reads. A runner-level test of the surrounding method is not
    /// tractable — `AnalysisJobRunner` holds a concrete
    /// `TranscriptEngineService` with no protocol seam and a hardcoded 300 s
    /// timeout, the same constraint `AnalysisJobRunnerSubscribeBeforeStartTests`
    /// documents.
    /// playhead-8ysk (review r2): consume the engine's event stream until this
    /// asset reaches a terminal event, and report what happened.
    ///
    /// Extracted from the `withTaskGroup` child in `run(...)` so it can be
    /// measured. Round 1 left the runner's `.failed` handling untested and
    /// said why — the runner holds a concrete `TranscriptEngineService` and a
    /// hardcoded 300 s timeout, so no test can drive the real stream. Nothing
    /// about THIS logic needs either: it is a fold over an `AsyncStream` and a
    /// coverage lookup, both of which a test can supply.
    ///
    /// THE `failure == nil` ON THE FALLBACK IS A FIX, NOT A TIDY-UP. The
    /// fallback exists for an ambiguous ending — the stream closed without a
    /// terminal event for this asset — where `analysis_assets` is the better
    /// authority on what got persisted. It must NOT run after an explicit
    /// `.failed`. The engine has just reported that THIS run produced nothing,
    /// while `fastTranscriptCoverageEndTime` is cumulative and carries
    /// coverage from EARLIER passes over the same asset — and a retry of an
    /// asset that once made progress is the exact shape of this bead's
    /// incident (147 acquisitions, 9 finalizations). A stale non-zero value
    /// makes `transcriptCoverage != 0` at the call site, which skips the whole
    /// zero-coverage branch: no `work_journal` row, no `failure_class`, and
    /// `lastErrorCode` never names the cause. The named failure would be
    /// destroyed one layer ABOVE the catch that used to destroy it.
    ///
    /// playhead-ngev: `sawCompleted` is the third fact, and it is the one the
    /// runner could not previously recover. A `.completed` over an empty
    /// transcript and a 300 s silence both arrive as `(0, nil)`, so the
    /// journal row could not say which had happened — and "the engine said it
    /// finished and produced nothing" and "the engine never said anything" are
    /// different bugs in different files.
    nonisolated static func observeTranscriptEvents(
        stream: AsyncStream<TranscriptEngineEvent>,
        assetId: String,
        persistedCoverage: @Sendable () async -> Double
    ) async -> (coverage: Double, failure: TranscriptFailureReason?, sawCompleted: Bool) {
        var coverage: Double = 0
        var failure: TranscriptFailureReason?
        var sawCompleted = false
        for await event in stream {
            if Task.isCancelled { break }
            if case .completed(let completedAssetId) = event, completedAssetId == assetId {
                // Read coverage from the store after transcription completes.
                sawCompleted = true
                coverage = await persistedCoverage()
                break
            }
            if case .failed(let failedAssetId, let reason) = event, failedAssetId == assetId {
                failure = reason
                break
            }
        }
        if coverage == 0, failure == nil {
            coverage = await persistedCoverage()
        }
        return (coverage, failure, sawCompleted)
    }

    /// playhead-ngev: which of the three ways a zero-coverage run can end
    /// actually happened, from the runner's own vantage.
    ///
    /// WHY THIS EXISTS ALONGSIDE `failure_class`. The class answers "what went
    /// wrong", and it is ABSENT whenever the engine did not report — which is
    /// most of the time, and which is currently overloaded across four
    /// unrelated diagnoses: the engine was cancelled by playback, the engine
    /// was torn down, the engine said `.completed` over an empty transcript,
    /// or the engine said nothing at all for five minutes. A support engineer
    /// reading a bundle sees one blank column for all four.
    ///
    /// The observation is never absent, because it is not a claim about the
    /// engine's internals — it is a description of what the runner itself
    /// observed, which the runner always knows with certainty. Absence of a
    /// class next to `engine_silent_timeout` means "nothing was reported";
    /// absence next to `engine_completed_zero` means "the engine claims
    /// success"; those are different bugs and now look different.
    ///
    /// Three compile-time literals. Nothing here is derived from audio, a
    /// feed, a URL, or user text — the same construction argument
    /// `TranscriptFailureClass` won, and the reason this key may cross the
    /// bundle projection that drops `metadata` wholesale.
    enum TranscriptRunObservation: String, Sendable, Hashable, Codable, CaseIterable {
        /// A `.failed` arrived for this asset. `failure_class` is the answer;
        /// this row's diagnosis is complete.
        case engineReported = "engine_reported"
        /// A `.completed` arrived and the persisted coverage was still zero.
        /// The engine believes it succeeded; the transcript is empty.
        case engineCompletedZero = "engine_completed_zero"
        /// No terminal event for this asset before the observation ended —
        /// either the runner's 300 s timeout fired, or the event stream itself
        /// finished without one. The engine is wedged, gone, or was cancelled
        /// by a build that still returns from the loop in silence.
        ///
        /// The two are deliberately one bucket (review r1): from the runner's
        /// vantage they are the same statement — nobody reported — and it
        /// cannot tell them apart without claiming something about the engine's
        /// internals, which is exactly what this key exists to avoid.
        case engineSilentTimeout = "engine_silent_timeout"

        /// Derive the observation from what the fold actually saw. Called only
        /// on the zero-coverage path, which is what makes
        /// `engine_completed_zero` true rather than merely "completed".
        static func classify(
            failure: TranscriptFailureReason?,
            sawCompleted: Bool
        ) -> TranscriptRunObservation {
            if failure != nil { return .engineReported }
            return sawCompleted ? .engineCompletedZero : .engineSilentTimeout
        }
    }

    /// playhead-ngev: the `work_journal.cause` for a zero-coverage row.
    ///
    /// It was a hardcoded `.asrFailed` at both emission sites, with no
    /// reference to the failure at all — so rows whose own `failure_class`
    /// said `no_shards` or `speech_engine_not_ready` still claimed the
    /// recognizer had failed, and so did every 300 s timeout, where nothing
    /// was reported by anyone.
    ///
    /// `.asrFailed` is now reserved for rows whose class implies recognition
    /// actually ran. Everything else — including every silent timeout, where
    /// the honest statement is "the pipeline did not deliver" — is
    /// `.pipelineError`.
    static func journalCause(
        failure: TranscriptFailureReason?,
        observation: TranscriptRunObservation
    ) -> InternalMissCause {
        // A silent timeout has no reporter, so nothing in it can support a
        // claim about ASR. This is checked first because `failure` is nil
        // there anyway — stating it explicitly keeps the rule readable.
        guard observation != .engineSilentTimeout else { return .pipelineError }
        guard let failure else { return .pipelineError }
        return failure.failureClass.impliesRecognizerRan ? .asrFailed : .pipelineError
    }

    /// playhead-ngev: whether the runner should tear the engine down after a
    /// zero-coverage exit.
    ///
    /// The stop exists (playhead-5uvz.5 Gap-6) to fence an ORPHANED engine:
    /// one still running for an asset whose owning scheduler has moved on, so
    /// its later chunk writes and coverage updates would land behind the
    /// runner's back.
    ///
    /// An INTERRUPTED run is the one case where the engine is not orphaned. It
    /// is shared — `PlayheadRuntime` builds one and hands it to both
    /// `AnalysisCoordinator` and this runner — and the cancel came from
    /// `startTranscription`, i.e. from a live owner that has already re-tasked
    /// it (a scrub, a speed change, a different episode). Stopping it there
    /// would cancel the listener's own transcription and gate the asset
    /// against the appends that owner is about to make.
    ///
    /// This only became reachable in practice with this bead: before it, an
    /// interruption was silent, the runner waited out its 300 s timeout, and
    /// by then the successor was usually done. Reporting the interruption
    /// instantly is what puts the stop in the successor's way.
    static func shouldStopEngine(after failure: TranscriptFailureReason?) -> Bool {
        failure?.termination != .interrupted
    }

    /// playhead-rqgr: BOTH DURABLE RECORDS OF ONE ZERO-COVERAGE EXIT,
    /// MINTED BY ONE EXPRESSION.
    ///
    /// A zero-coverage exit writes itself down twice: as an
    /// ``AnalysisOutcome/StopReason`` handed to `AnalysisWorkScheduler`, and
    /// as a `work_journal` row. Until this bead the two were computed
    /// independently — the stop reason by ``zeroCoverageDisposition``'s
    /// ancestor `zeroCoverageStopReason`, keyed on `termination`, and the
    /// journal event by a hardcoded `.failed` literal at the emission site.
    /// They disagreed for exactly the case the routing exists for.
    ///
    /// Bundling them removes the possibility rather than testing for its
    /// absence: there is one `if`, it produces both fields, and
    /// ``emitTranscriptionTimeoutJournal(request:assetId:allShards:existingChunkCount:transcriptStageStart:failure:disposition:)``
    /// takes this value instead of choosing an event of its own.
    struct ZeroCoverageDisposition: Sendable {
        /// What the scheduler is told, and therefore whether this exit
        /// spends one of the job's five permanent attempts.
        let stopReason: AnalysisOutcome.StopReason

        /// What the `work_journal` row says, and therefore which arm
        /// `AnalysisCoordinator.recoverOrphans` takes if the process dies
        /// before the scheduler commits the line above.
        let journalEvent: WorkJournalEntry.EventType

        /// The row's `cause`. Diagnostic only — no recovery arm reads it.
        let journalCause: InternalMissCause
    }

    /// playhead-ngev (review r1) / playhead-rqgr: which outcome a
    /// zero-coverage transcription reports, and therefore whether it SPENDS
    /// ONE OF THE JOB'S FIVE PERMANENT RETRY ATTEMPTS — together with the
    /// journal row that has to agree with it.
    ///
    /// **THE JOURNAL EVENT IS NOT A LABEL FOR THE ROW, IT IS THE COLD-LAUNCH
    /// RECOVERY DECISION (playhead-rqgr).**
    /// ``AnalysisCoordinator/recoverOrphans(now:graceSeconds:)`` reads the
    /// last row for the stranded `{episode, generation}` and routes on
    /// ``WorkJournalEntry/EventType/orphanRecoveryRouting``. Writing
    /// `.failed` for an interrupted run therefore did not merely mislabel a
    /// diagnostic: it told a cold launch the work was over, so
    /// `clearOrphanedLeaseNoRequeue` freed the lease slot and left the row
    /// `state='running'` with no lease — invisible to `fetchNextEligibleJob`
    /// (which dispatches `queued`/`paused`/`failed`), invisible to
    /// `recoverExpiredLeases` (the lease is NULL, not expired), and
    /// invisible to playhead-btwk's stranded sweep until some unrelated
    /// orphan happens to bump the scheduler epoch. Exactly the opposite of
    /// what the same exit's `.interrupted` outcome had just asked for.
    ///
    /// The window is real but narrow: on the ordinary path the scheduler's
    /// `.interrupted` arm commits `state='queued'` and releases the lease,
    /// and then writes its OWN `.preempted` row — so the last row already
    /// routed to resume and the job is no longer an orphan at all. The harm
    /// needs the process to die between the runner's row and that commit.
    /// It is the same event either way, and only one of the two records used
    /// to say so.
    ///
    /// **The interrupted row is `.preempted`, not `.checkpointed`.** Both
    /// route to resume, but `.checkpointed` claims durable progress and this
    /// pass persisted nothing — that would be a second value naming one
    /// thing and read as another. `.preempted` is what the event IS ("the
    /// owner released because something else took the resource"), and it is
    /// the same event the scheduler's own `.interrupted` arm writes for this
    /// exit via `emitJournalPreempted(cause: .userPreempted)`. Two records,
    /// one story.
    ///
    /// This does NOT contradict ``AnalysisOutcome/StopReason/interrupted(_:)``
    /// being kept separate from ``AnalysisOutcome/StopReason/preempted``.
    /// That separation is about the SCHEDULER ARM — `.preempted` requeues
    /// immediately because a higher-lane job now holds the slot, while an
    /// interruption is reported with playback still running and needs the
    /// `interruptedRequeueDelaySeconds` floor to avoid a hot loop. The
    /// journal vocabulary is coarser than the scheduler's and always has
    /// been: `work_journal` has five events, and both of those outcomes have
    /// mapped to the `.preempted` row since playhead-ngev shipped. What was
    /// missing was the runner's own row agreeing with it.
    ///
    /// **The terminal arm is untouched.** `.ranToConclusion` and a nil
    /// failure still produce `.failed` on BOTH records. A genuinely broken
    /// episode must still exhaust its budget and stop.
    ///
    /// ---
    ///
    /// playhead-ngev (review r1), on why the outcome half is keyed on
    /// `termination` and nothing else:
    ///
    /// A listener moving the playhead is not an analysis failure. But the
    /// scheduler charges every `.failed` one attempt
    /// (`AnalysisWorkScheduler`'s `failed` arm), and at `maxAttemptCount = 5`
    /// the job becomes `superseded` with `nextEligibleAt: nil` — a state it
    /// cannot leave. `analysis_jobs.workKey` is UNIQUE and `insertJob` is
    /// `INSERT OR IGNORE` over a key that is stable across launches, so every
    /// later enqueue for that episode is silently dropped; the only reset,
    /// `requeueOrphanedLease`, rewrites `state = 'running'` rows only and never
    /// touches a superseded one. Five scrubs across an episode's analysis
    /// lifetime is ordinary listening, so charging them would permanently kill
    /// analysis on the episodes a listener engages with most.
    ///
    /// Before ngev the same attempt was still charged, just 300 s later —
    /// but there WAS a rescue: a successor loop completing inside that window
    /// gave the job non-zero coverage and no attempt was spent. Reporting the
    /// interruption instantly removes the rescue, which turns an occasional
    /// loss into a reliable one. This routing is what keeps the honest
    /// reporting without the regression that came with it.
    ///
    /// THE REVERSE HAZARD IS THE REASON THIS IS KEYED ON `termination` AND
    /// NOTHING ELSE. A genuinely broken episode must still exhaust its budget
    /// and stop, or it retries forever. `.ranToConclusion` — the total-failure
    /// gate's verdict — and a nil failure — a silent timeout or a `.completed`
    /// over an empty transcript — both keep `.failed`.
    static func zeroCoverageDisposition(
        failure: TranscriptFailureReason?,
        observation: TranscriptRunObservation
    ) -> ZeroCoverageDisposition {
        let code = failure.map { "transcription:\($0.failureClass.rawValue)" }
            ?? "transcription:zeroCoverage"
        let cause = journalCause(failure: failure, observation: observation)

        // ONE branch, BOTH records. Keyed on `termination` and nothing else,
        // for the reason spelled out below.
        if failure?.termination == .interrupted {
            return ZeroCoverageDisposition(
                stopReason: .interrupted(code: code),
                journalEvent: .preempted,
                journalCause: cause
            )
        }
        return ZeroCoverageDisposition(
            stopReason: .failed(code: code),
            journalEvent: .failed,
            journalCause: cause
        )
    }

    static func failureExtras(_ failure: TranscriptFailureReason?) -> [String: String] {
        guard let failure else { return [:] }
        var extras = [
            DiagnosticsFailureKeys.failureClass: failure.failureClass.rawValue,
            DiagnosticsFailureKeys.failedShardCount: String(failure.failedShardCount),
            // playhead-ngev: whether the run finished or was cut short. Two
            // compile-time literals. It is not redundant with the class: a run
            // whose shards were failing `model_not_loaded` and which was then
            // cancelled by a scrub reports the class it earned and the
            // termination it suffered, and only the second one says the
            // listener's own playback ended this run.
            DiagnosticsFailureKeys.failureTermination: failure.termination.rawValue,
        ]
        if let code = failure.code {
            extras[DiagnosticsFailureKeys.failureCode] = String(code)
        }
        return extras
    }

    /// playhead-pnb5: the persisted watermark, returned ONLY when every shard
    /// this pass admitted is already backed by a transcript artifact — i.e. when
    /// running the transcription stage cannot produce a single chunk.
    ///
    /// **The loop this closes.** `desiredCoverageSec` is the shard filter
    /// (`allShards.filter { $0.startTime < request.desiredCoverageSec }`), and
    /// the transcript watermark is what the previous pass at this same target
    /// already reached. Once the two meet, every subsequent pass at that target
    /// hands the engine a shard set it has already read, and
    /// `TranscriptEngineService.runTranscriptionLoop` re-runs full ASR over all
    /// of it (playhead-mptr ORDERS already-backed shards last rather than
    /// skipping them). Nothing can be produced, so `transcriptCoverage` comes
    /// back zero, and the pass is charged with a failure it did not commit.
    ///
    /// **This is a different question from
    /// ``transcriptCoverageOfCompletedTranscript(assetId:)``, and conflating
    /// them is what made the loop unbreakable.** That helper asks an EPISODE
    /// question — is the two-pass bridged AREA at least
    /// ``SemanticScanClaim/transcriptClearsFinalizeFloor(coveredSec:episodeDurationSec:)``
    /// of the declared duration? — and its answer was being used to settle a
    /// PASS question: did this pass fail? Measured on the 2026-08-14 device pull
    /// (playhead-by07), the discrimination was total, 14 of 14: an asset needed
    /// >= 0.95 episode coverage to be allowed to finish and escalate, and an
    /// asset below 0.95 was denied the short-circuit and sent back through a
    /// stage that could not add anything — the gate was on the very quantity
    /// that was broken. The numerator and denominator here are both about THIS
    /// PASS: shards this request admitted, over shards this request admitted.
    /// The floor is untouched and still governs the case it was written for —
    /// the stage RAN and the engine produced nothing.
    ///
    /// **The rule is the ENGINE'S OWN**, read through
    /// ``TranscriptCoverageIndex/partitioningByTranscriptArtifact(_:watermark:)``
    /// and the same `fetchFastTranscriptCoverageEndTime` /
    /// `fetchTranscribedRegion` pair `runTranscriptionLoop` reads. A shard
    /// counts as backed only when the durable watermark has passed its end AND a
    /// persisted chunk of either pass overlaps it — so the playhead-rfu-aac H3
    /// counterexamples (a watermark outliving its chunks) and the playhead-0sro
    /// crash shape both land in `withNoArtifact` and the stage runs. If the
    /// runner and the engine could disagree about that rule, this would skip a
    /// stage the engine would have found work in.
    ///
    /// **Every failure to measure returns `nil`.** No watermark, an unreadable
    /// region, an empty region, an empty shard set: run the stage. This grants a
    /// pass the right NOT to transcribe, so the safe direction is to withhold
    /// it.
    ///
    /// **The two guards are NOT the independent pair they look like, from THIS
    /// caller.** ``run(_:)`` opens with
    /// ``AnalysisStore/reconcileFastTranscriptCoverage(id:)`` (playhead-0sro),
    /// which raises the column to `MAX(endTime)` over the asset's fast chunks
    /// before any stage runs — so by the time this reads it, a NULL watermark on
    /// an asset that HAS chunks has become the chunks' own reach, and a watermark
    /// sitting behind its own chunks is not a state this path can be in. The
    /// watermark guard therefore only ever fires together with the region guard,
    /// on an asset with no fast chunks at all. It is kept because it is cheap and
    /// because the reconcile is a property of the caller rather than of this
    /// helper; `TranscriptionStageAdmissionTests` pins the interaction so a
    /// reader does not design a fixture around the state that cannot exist.
    ///
    /// **A RESIDUAL, named so it is not re-discovered as a bug.** "Backed" is an
    /// OVERLAP test, so a 30 s shard carrying one 1 s chunk counts as backed and
    /// its other 29 s are never re-read by this pass. That tolerance is
    /// playhead-mptr's, measured there (moved shards run min 0.610 / median
    /// 0.906 union fill), and it is the tolerance already governing which audio
    /// the stage reaches last — under a flat 300 s cap "last" and "never" have
    /// been the same thing for these assets since mptr shipped. What changes is
    /// that the budget is no longer SPENT to arrive at that same place. A hole
    /// wider than one shard still sorts `withNoArtifact` and still runs.
    private func transcriptWatermarkWhenEveryAdmittedShardIsBacked(
        assetId: String,
        admittedShards: [AnalysisShard]
    ) async -> Double? {
        guard !admittedShards.isEmpty else { return nil }
        // `try?` does not add a layer here: the call already returns `Double?`,
        // and Swift flattens the two. A throw and a NULL column are the same
        // answer for this decision — neither is evidence the audio was read.
        guard let watermark = try? await store.fetchFastTranscriptCoverageEndTime(id: assetId),
              watermark.isFinite,
              watermark > 0 else {
            return nil
        }
        guard let region = try? await store.fetchTranscribedRegion(assetId: assetId),
              !region.isEmpty else {
            return nil
        }
        let index = TranscriptCoverageIndex(transcribedRegion: region)
        let unbacked = index.partitioningByTranscriptArtifact(
            admittedShards,
            watermark: watermark
        ).withNoArtifact
        guard unbacked.isEmpty else { return nil }
        return watermark
    }

    /// playhead-9y9e: the transcript coverage a pass should carry forward when
    /// it added none of its own, or `nil` when the asset is not transcribed
    /// enough for that to be the honest reading.
    ///
    /// **What the returned number is.** The asset's
    /// `fastTranscriptCoverageEndTime` WATERMARK — deliberately the same
    /// quantity `observeTranscriptEvents`' `persistedCoverage()` closure returns
    /// on the `.completed` path, so a run that short-circuits here and a run
    /// that completes normally over the same already-transcribed asset report
    /// the identical coverage. Everything downstream
    /// (`tierTargetSatisfied(job:outcome:)`, `shouldRetryCoverageInsufficient`,
    /// the successor job's seed) compares it against `desiredCoverageSec`, which
    /// is a REACH in seconds, so a reach is what it must be.
    ///
    /// **What the GATE is, which is a different quantity on purpose.** An AREA:
    /// the gap-bridged interval union of the asset's transcript across BOTH
    /// passes, over the declared duration, against
    /// ``SemanticScanClaim/transcriptClearsFinalizeFloor(coveredSec:episodeDurationSec:)``.
    /// The watermark cannot be the gate — it is a high-water reach and reads
    /// 100 % over a transcript full of holes (playhead-sd71), which is exactly
    /// the licence a genuinely stalled transcription must not get. Sharing the
    /// floor with playhead-fil5's sweep is deliberate: "is this still the
    /// transcript lane's problem?" is one question and must not have two
    /// answers.
    ///
    /// **A RESIDUAL, named because it will otherwise be re-discovered as a bug**
    /// (playhead-9y9e R1 review). The GATE spans both passes and the RETURNED
    /// VALUE is the FAST watermark, so on a final-pass-heavy asset they can
    /// disagree — and the disagreement is not academic. On the 2026-08-03 pull
    /// 48E903D7 covers 95.1 % as a two-pass area (clears the 0.95 gate) while
    /// its fast watermark is 2,010 s of a 2,113 s episode, which is short of the
    /// deepest tier rung by more than
    /// ``AnalysisWorkScheduler/tierCoverageSlack(target:)``. Such a pass reaches
    /// Stage 4 — the entire point — and then still terminates
    /// `coverageInsufficient:noProgress` rather than `complete`.
    ///
    /// THREE of the twelve assets on that pull are in this shape, not one (R2
    /// review; R1 named 48E903D7 as an example and the population was never
    /// counted). Fast watermark versus duration, each short of the deepest rung
    /// by more than the 30 s slack: 48E903D7 2,010 / 2,113.1, 53FC53E3
    /// 2,490 / 2,528.4, 83592353 7,230 / 7,325.9. The two extra ones cleared the
    /// 0.95 gate under the FAST-only ruler too, so they are not new eligibility
    /// — they are the same residual, three times as wide as stated.
    ///
    /// That is an IMPROVEMENT on what it replaces (`transcription:zeroCoverage`
    /// without reaching Stage 4 at all) and it is bounded — `noProgress` is not
    /// an attempt-cap terminal, so the cap-out rescue does not re-mint on it.
    /// Returning a two-pass reach instead would fix the terminal and BREAK the
    /// deliberate parity with `persistedCoverage()`'s `.completed` arm, so the
    /// two paths would report different coverage for the same asset. Changing
    /// that is a design decision about what the runner's `transcriptCoverageSec`
    /// NAMES, not a review fix; it is left alone here on purpose.
    ///
    /// Every failure to measure returns `nil` — no asset row, no watermark, an
    /// unreadable chunk set, an absent duration. This helper GRANTS a pass the
    /// right to skip the failure accounting, so the safe direction is to
    /// withhold it.
    private func transcriptCoverageOfCompletedTranscript(assetId: String) async -> Double? {
        guard let asset = try? await store.fetchAsset(id: assetId),
              let watermark = asset.fastTranscriptCoverageEndTime,
              watermark.isFinite, watermark > 0 else {
            return nil
        }
        guard let region = try? await store.fetchTranscribedRegion(assetId: assetId),
              !region.isEmpty else {
            return nil
        }
        // playhead-x0lb R6: this call site is why the floor's two parameters
        // carry types. Both were `Double?`, and `watermark` — a REACH, three
        // lines above — plus `region.unionedSeconds` (the RAW union) plus the
        // duration itself were all writable into `coveredSec:`. Three probes,
        // three COMPILED; rails TY35–TY37.
        guard SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: SemanticScanClaim.bridgedTranscriptCoveredSec(region: region),
            episodeDurationSec: asset.episodeDurationSec.map { EpisodeSeconds($0) }
        ) else {
            return nil
        }
        return watermark
    }

    /// playhead-9y9e: the durable trace for a pass whose transcription stage
    /// added nothing because there was nothing left to add.
    ///
    /// A `.checkpointed` row, not a `.failed` one: the pass did not fail and is
    /// about to run ad detection, so recording it as a failure would put back
    /// exactly the misattribution this bead removes (five `asr_failed` rows for
    /// a fully transcribed episode). `cause` is `nil`, which the journal
    /// reserves for non-terminal / success events.
    ///
    /// It is still a ROW, because a silent short-circuit is indistinguishable
    /// from the bug: `SELECT * FROM work_journal WHERE metadata LIKE
    /// '%transcriptionAlreadyComplete%'` is how a device pull counts how often
    /// a pass paid the full 300 s stage cap to learn it had nothing to do.
    /// playhead-pnb5: the durable trace for a pass whose transcription stage was
    /// NEVER STARTED because no shard it admitted still wanted transcribing.
    ///
    /// A separate row from `transcriptionAlreadyComplete`, and the separation is
    /// the point. That row's own contract is "how often a pass paid the full
    /// 300 s stage cap to learn it had nothing to do"; this one paid nothing, so
    /// folding the two together would delete exactly the quantity that says the
    /// fix is working. `SELECT` the two stages side by side and the saving is
    /// the difference in `slice_duration_ms`: on the 2026-08-15 pull those rows
    /// carry 31,368 / 39,342 / 157,645 / 208,572 / 215,980 / 232,346 ms of
    /// window spent inside a stage that added no coverage, and this row should
    /// carry single-digit milliseconds.
    ///
    /// `admittedShardCount` is recorded because the claim being made is about a
    /// population, and a row that named only the outcome could not be audited
    /// against it.
    private func emitTranscriptionStageNotRunJournal(
        request: AnalysisRangeRequest,
        assetId: String,
        allShards: [AnalysisShard],
        admittedShardCount: Int,
        transcriptStageStart: Date,
        transcriptCoverageSec: Double
    ) async {
        let job = try? await store.fetchJob(byId: request.jobId)
        let generationID = (job?.generationID).flatMap { UUID(uuidString: $0) } ?? UUID()
        let schedulerEpoch = job?.schedulerEpoch ?? 0

        let now = clock()
        let elapsedSec = max(0, now.timeIntervalSince(transcriptStageStart))
        let episodeDuration = allShards.map { $0.startTime + $0.duration }.max() ?? 0

        let metadata = SliceCompletionInstrumentation.buildMetadata(
            sliceDurationMs: Int((elapsedSec * 1000).rounded()),
            bytesProcessed: 0,
            shardsCompleted: 0,
            deviceClass: DeviceClass.detect(),
            extras: [
                "stage": "analysisJobRunner.run.transcriptionStageNotRun",
                "job_id": request.jobId,
                "episode_duration": String(format: "%.3f", episodeDuration),
                "transcript_coverage_end_time": String(format: "%.3f", transcriptCoverageSec),
                "requested_coverage": String(format: "%.3f", request.desiredCoverageSec),
                // The population the skip is a claim about: every one of these
                // shards is backed by a persisted chunk of some pass, below a
                // watermark that has passed its end.
                "admitted_shards_all_backed": String(admittedShardCount),
            ]
        )

        let entry = WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: request.episodeId,
            generationID: generationID,
            schedulerEpoch: schedulerEpoch,
            timestamp: now.timeIntervalSince1970,
            eventType: .checkpointed,
            cause: nil,
            metadata: metadata.encodeJSON(),
            artifactClass: .scratch
        )
        do {
            try await store.appendWorkJournalEntry(entry)
        } catch {
            logger.warning(
                "Failed to append transcriptionStageNotRun work_journal row for asset \(assetId): \(error)"
            )
        }
    }

    private func emitTranscriptionAlreadyCompleteJournal(
        request: AnalysisRangeRequest,
        assetId: String,
        allShards: [AnalysisShard],
        transcriptStageStart: Date,
        transcriptCoverageSec: Double,
        observation: TranscriptRunObservation
    ) async {
        let job = try? await store.fetchJob(byId: request.jobId)
        let generationID = (job?.generationID).flatMap { UUID(uuidString: $0) } ?? UUID()
        let schedulerEpoch = job?.schedulerEpoch ?? 0

        let now = clock()
        let elapsedSec = max(0, now.timeIntervalSince(transcriptStageStart))
        let episodeDuration = allShards.map { $0.startTime + $0.duration }.max() ?? 0

        let metadata = SliceCompletionInstrumentation.buildMetadata(
            sliceDurationMs: Int((elapsedSec * 1000).rounded()),
            bytesProcessed: 0,
            shardsCompleted: 0,
            deviceClass: DeviceClass.detect(),
            extras: [
                "stage": "analysisJobRunner.run.transcriptionAlreadyComplete",
                "job_id": request.jobId,
                "episode_duration": String(format: "%.3f", episodeDuration),
                "transcript_coverage_end_time": String(format: "%.3f", transcriptCoverageSec),
                // What the observation WOULD have been charged as. Kept so the
                // rows stay joinable with the `transcriptionTimeout` population
                // they used to be part of.
                DiagnosticsFailureKeys.failureObservation: observation.rawValue,
            ]
        )

        let entry = WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: request.episodeId,
            generationID: generationID,
            schedulerEpoch: schedulerEpoch,
            timestamp: now.timeIntervalSince1970,
            eventType: .checkpointed,
            cause: nil,
            metadata: metadata.encodeJSON(),
            artifactClass: .scratch
        )
        do {
            try await store.appendWorkJournalEntry(entry)
        } catch {
            logger.warning(
                "Failed to append transcriptionAlreadyComplete work_journal row for asset \(assetId): \(error)"
            )
        }
    }

    private func emitTranscriptionTimeoutJournal(
        request: AnalysisRangeRequest,
        assetId: String,
        allShards: [AnalysisShard],
        existingChunkCount: Int,
        transcriptStageStart: Date,
        failure: TranscriptFailureReason?,
        observation: TranscriptRunObservation,
        disposition: ZeroCoverageDisposition
    ) async {
        // Resolve the active job's `{generationID, schedulerEpoch}` so
        // the journal row joins the lease lifecycle written by 5uvz.1.
        // Both fields default to safe scalar values on lookup failure
        // so the row still lands and is grouped under "no-generation".
        let job = try? await store.fetchJob(byId: request.jobId)
        let generationID = (job?.generationID).flatMap { UUID(uuidString: $0) } ?? UUID()
        let schedulerEpoch = job?.schedulerEpoch ?? 0

        // Engine progress at the moment of zero-coverage exit.
        let currentChunkCount = (try? await store.fetchTranscriptChunks(assetId: assetId).count) ?? existingChunkCount
        let chunksPersisted = max(0, currentChunkCount - existingChunkCount)
        let transcriptCoverageEndTime = (try? await store.fetchAsset(id: assetId))?.fastTranscriptCoverageEndTime ?? 0
        let episodeDuration = allShards.map { $0.startTime + $0.duration }.max() ?? 0

        let now = clock()
        let elapsedSec = max(0, now.timeIntervalSince(transcriptStageStart))
        let elapsedMs = Int((elapsedSec * 1000).rounded())
        // Avoid /0 — for elapsed below 1ms the rate becomes meaningless.
        // Encode `0` so consumers don't see an `inf` row; the elapsed_ms
        // field already captures that the stage barely ran.
        let chunkRatePerSec = elapsedSec > 0.001
            ? Double(chunksPersisted) / elapsedSec
            : 0

        // Match the metadata-encoding style of `SliceCompletionInstrumentation`:
        // a flat JSON object with the structural keys promoted by the
        // recordFailed helper and the timeout-specific keys carried as
        // string-typed siblings under `extras`. Numbers go through
        // `String(format:)` so the JSON column stays self-describing
        // without needing a typed schema bump on the consumer side.
        var extras: [String: String] = [
            "stage": "analysisJobRunner.run.transcriptionTimeout",
            "job_id": request.jobId,
            "episode_duration": String(format: "%.3f", episodeDuration),
            "transcript_coverage_end_time": String(format: "%.3f", transcriptCoverageEndTime),
            "chunks_persisted": String(chunksPersisted),
            "chunk_rate_per_sec": String(format: "%.4f", chunkRatePerSec),
            // playhead-ngev: ALWAYS present, unlike the class. Absence of a
            // class is meaningful only when something else says why there is
            // none.
            DiagnosticsFailureKeys.failureObservation: observation.rawValue,
        ]
        extras.merge(Self.failureExtras(failure)) { _, new in new }

        // playhead-ngev: no longer hardcoded. A row whose class proves the
        // recognizer never ran must not be counted as an ASR failure — that
        // contradiction is what sent two dogfood cycles to the wrong stage.
        //
        // playhead-rqgr: and no longer derived HERE either. Both the cause
        // and the event come off the caller's `disposition`, which also
        // produced the `StopReason` this exit reports to the scheduler.
        let cause = disposition.journalCause

        // playhead-rqgr: THE IN-MEMORY COUNTER IS THE THIRD RECORD OF THIS
        // EVENT, and it was the same literal one layer down. `recordFailed`
        // and `recordPaused` build a byte-identical `SliceMetadata`; the
        // only thing that differs is which of `SliceCounters`' two tallies
        // they increment. A hardcoded `recordFailed` therefore counted every
        // listener's scrub as a failed slice, which is the quantity
        // `slicesFailed` exists NOT to be.
        //
        // Routed by the disposition, like everything else about this exit,
        // and the correspondence is exact rather than a coincidence: a row
        // that asks orphan recovery to RESUME is a paused slice, a row that
        // asks it to STOP is a failed one. It is the same pairing the
        // scheduler's own `.interrupted` arm already makes
        // (`recordPaused` + `emitJournalPreempted`).
        let deviceClass = DeviceClass.detect()
        let metadata: SliceMetadata
        switch disposition.journalEvent.orphanRecoveryRouting {
        case .requeue:
            metadata = await SliceCompletionInstrumentation.recordPaused(
                cause: cause,
                deviceClass: deviceClass,
                sliceDurationMs: elapsedMs,
                bytesProcessed: 0,
                shardsCompleted: 0,
                extras: extras
            )
        case .terminalNoRequeue:
            metadata = await SliceCompletionInstrumentation.recordFailed(
                cause: cause,
                deviceClass: deviceClass,
                sliceDurationMs: elapsedMs,
                bytesProcessed: 0,
                shardsCompleted: 0,
                extras: extras
            )
        }

        let entry = WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: request.episodeId,
            generationID: generationID,
            schedulerEpoch: schedulerEpoch,
            timestamp: now.timeIntervalSince1970,
            // playhead-rqgr: the recovery arm this exit is asking a cold
            // launch for — NOT a literal, and not a label. See the method
            // doc: `.failed` here for an interrupted run is what made
            // `recoverOrphans` clear a lease the outcome had just asked to
            // keep.
            eventType: disposition.journalEvent,
            cause: cause,
            metadata: metadata.encodeJSON(),
            artifactClass: .scratch
        )
        do {
            try await store.appendWorkJournalEntry(entry)
        } catch {
            logger.warning("Failed to append transcriptionTimeout work_journal row for asset \(assetId): \(error)")
        }
    }

    // MARK: - Outcome Builder

    private func makeOutcome(
        assetId: String,
        request: AnalysisRangeRequest,
        featureCoverageSec: Double = 0,
        transcriptCoverageSec: Double = 0,
        cueCoverageSec: Double = 0,
        newCueCount: Int = 0,
        stopReason: AnalysisOutcome.StopReason
    ) -> AnalysisOutcome {
        AnalysisOutcome(
            assetId: assetId,
            requestedCoverageSec: request.desiredCoverageSec,
            featureCoverageSec: featureCoverageSec,
            transcriptCoverageSec: transcriptCoverageSec,
            cueCoverageSec: cueCoverageSec,
            newCueCount: newCueCount,
            stopReason: stopReason
        )
    }
}
