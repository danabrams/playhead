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
        analysisSharingProvider: CrossUserAnalysisSharingProviding = NoOpCrossUserAnalysisSharingProvider()
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
    }

    // MARK: - Run

    /// Execute a bounded analysis pass described by `request`.
    /// Returns an `AnalysisOutcome` summarizing coverage achieved and stop reason.
    func run(_ request: AnalysisRangeRequest) async -> AnalysisOutcome {
        let assetId = request.analysisAssetId

        if let sharedOutcome = await importSharedAnalysisIfAvailable(
            assetId: assetId,
            request: request
        ) {
            return sharedOutcome
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
            }
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
            logger.error("Decode failed for job \(request.jobId): \(error)")
            return makeOutcome(assetId: assetId, request: request, stopReason: .failed("decode: \(error)"))
        }

        // Filter shards to the requested coverage depth.
        let shards = allShards.filter { $0.startTime < request.desiredCoverageSec }
        guard !shards.isEmpty else {
            return makeOutcome(assetId: assetId, request: request, stopReason: .failed("no shards within desired coverage"))
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
            logger.error("Feature extraction failed for job \(request.jobId): \(error)")
            return makeOutcome(assetId: assetId, request: request, stopReason: .failed("features: \(error)"))
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

        // -- Stage 3: Transcription --

        let transcriptSignpost = PreAnalysisInstrumentation.beginStage("transcription")
        let snapshot = PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: false)
        let existingChunkCount = (try? await store.fetchTranscriptChunks(assetId: assetId).count) ?? 0
        // playhead-5uvz.7 (Gap-9): mark stage start so the zero-coverage
        // journal row can compute `chunk_rate_per_sec` against the actual
        // wall-clock spent inside the stage (rather than assuming the
        // 5-minute timeout always elapsed in full — a stream that ends
        // without `.completed` returns much earlier).
        let transcriptStageStart = clock()

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
        let transcriptStream = await transcriptEngine.events()

        let transcriptCoverage: Double = await withTaskGroup(of: Double.self) { [weak self] group in
            // Timeout task
            group.addTask {
                try? await Task.sleep(for: .seconds(300))
                return 0
            }
            // Event stream task
            group.addTask { [weak self] in
                var coverage: Double = 0
                for await event in transcriptStream {
                    if Task.isCancelled { break }
                    if case .completed(let completedAssetId) = event, completedAssetId == assetId {
                        // Read coverage from the store after transcription completes.
                        if let asset = try? await self?.store.fetchAsset(id: assetId) {
                            coverage = asset.fastTranscriptCoverageEndTime ?? 0
                        }
                        break
                    }
                }
                // Stream ended without .completed — log and return whatever we have.
                if coverage == 0 {
                    if let asset = try? await self?.store.fetchAsset(id: assetId) {
                        coverage = asset.fastTranscriptCoverageEndTime ?? 0
                    }
                }
                return coverage
            }
            // Return whichever finishes first
            let result = await group.next() ?? 0
            group.cancelAll()
            return result
        }

        PreAnalysisInstrumentation.endStage(transcriptSignpost)

        // playhead-5uvz.5 (Gap-6): if the 5-minute timeout fired ahead
        // of `.completed`, the engine is still running in the
        // background — its subsequent `transcript_chunks` writes and
        // coverage updates would target an asset whose owning scheduler
        // has already moved on. Stop the engine for this asset before
        // the runner returns; the engine drops in-flight chunks and
        // gates any late writes/events for the stopped asset id.
        //
        // Calling unconditionally on zero coverage is safe: a normal
        // `.completed` path also yields zero coverage when the engine
        // genuinely produced nothing, and stopping a session that
        // already terminated is a no-op aside from the gate insertion
        // (which is harmless because no further writes can land).
        if transcriptCoverage == 0 {
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
            logger.warning("Transcription for asset \(assetId) finished with zero coverage — stream may have ended prematurely or timed out")
            // playhead-5uvz.7 (Gap-9): write a structured `failed` row to
            // `work_journal` so a class of episodes that systematically
            // times out (long, refusal-prone, music-heavy) shows up in
            // aggregate without operators having to grep `lastErrorCode`
            // across `analysis_jobs`. Best-effort: a failure here logs but
            // does NOT affect the runner's outcome — the analysis_jobs
            // row's `lastErrorCode = 'transcription:zeroCoverage'` remains
            // the primary signal; the journal row is observability gravy.
            await emitTranscriptionTimeoutJournal(
                request: request,
                assetId: assetId,
                allShards: allShards,
                existingChunkCount: existingChunkCount,
                transcriptStageStart: transcriptStageStart
            )
            return makeOutcome(
                assetId: assetId,
                request: request,
                featureCoverageSec: featureCoverage,
                transcriptCoverageSec: 0,
                stopReason: .failed("transcription:zeroCoverage")
            )
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
            logger.error("Failed to fetch transcript chunks for job \(request.jobId): \(error)")
            return makeOutcome(
                assetId: assetId,
                request: request,
                featureCoverageSec: featureCoverage,
                transcriptCoverageSec: transcriptCoverage,
                stopReason: .failed("fetchChunks: \(error)")
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
                    episodeDuration: episodeDuration
                )
            } catch {
                PreAnalysisInstrumentation.endStage(detectionSignpost)
                logger.error("Hot-path detection failed for job \(request.jobId): \(error)")
                return makeOutcome(
                    assetId: assetId,
                    request: request,
                    featureCoverageSec: featureCoverage,
                    transcriptCoverageSec: transcriptCoverage,
                    stopReason: .failed("hotPath: \(error)")
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
        let finalWindows: [AdWindow]
        let skippedBackfill = skippedHotPath && existingCandidateWindows.isEmpty
        if skippedBackfill {
            logger.info(
                "Skipping backfill for asset \(assetId): transcription produced no new chunks and there are no candidate windows to resolve"
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
                logger.error("Backfill detection failed for job \(request.jobId): \(error)")
                return makeOutcome(
                    assetId: assetId,
                    request: request,
                    featureCoverageSec: featureCoverage,
                    transcriptCoverageSec: transcriptCoverage,
                    stopReason: .failed("backfill: \(error)")
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

    private static func isCueWindow(_ window: AdWindow) -> Bool {
        window.confidence >= cueConfidenceThreshold
            && window.endTime > window.startTime
            && (
                window.decisionState == AdDecisionState.candidate.rawValue
                    || window.decisionState == AdDecisionState.confirmed.rawValue
                    || window.decisionState == AdDecisionState.applied.rawValue
            )
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
    /// carries `eventType = .failed`, `cause = .asrFailed`, and a JSON
    /// metadata blob describing the episode shape and the engine's
    /// progress at the moment of timeout — `episode_duration`,
    /// `transcript_coverage_end_time`, `chunks_persisted`, and
    /// `chunk_rate_per_sec` — so operators can spot a systematic stall
    /// pattern (long, refusal-prone, music-heavy episodes) in aggregate
    /// rather than grepping `lastErrorCode` across `analysis_jobs`.
    ///
    /// Best-effort: a fetch / append failure logs at warning level and
    /// does NOT alter the runner's outcome. The `analysis_jobs` row's
    /// `lastErrorCode = 'transcription:zeroCoverage'` remains the
    /// primary signal; this row is observability gravy.
    private func emitTranscriptionTimeoutJournal(
        request: AnalysisRangeRequest,
        assetId: String,
        allShards: [AnalysisShard],
        existingChunkCount: Int,
        transcriptStageStart: Date
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
        let metadata = await SliceCompletionInstrumentation.recordFailed(
            cause: .asrFailed,
            deviceClass: DeviceClass.detect(),
            sliceDurationMs: elapsedMs,
            bytesProcessed: 0,
            shardsCompleted: 0,
            extras: [
                "stage": "analysisJobRunner.run.transcriptionTimeout",
                "job_id": request.jobId,
                "episode_duration": String(format: "%.3f", episodeDuration),
                "transcript_coverage_end_time": String(format: "%.3f", transcriptCoverageEndTime),
                "chunks_persisted": String(chunksPersisted),
                "chunk_rate_per_sec": String(format: "%.4f", chunkRatePerSec),
            ]
        )

        let entry = WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: request.episodeId,
            generationID: generationID,
            schedulerEpoch: schedulerEpoch,
            timestamp: now.timeIntervalSince1970,
            eventType: .failed,
            cause: .asrFailed,
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
