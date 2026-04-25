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

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let audioProvider: AnalysisAudioProviding
    private let featureService: FeatureExtractionService
    private let transcriptEngine: TranscriptEngineService
    private let adDetection: AdDetectionProviding
    private let cueMaterializer: SkipCueMaterializer
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

    // MARK: - Init

    init(
        store: AnalysisStore,
        audioProvider: AnalysisAudioProviding,
        featureService: FeatureExtractionService,
        transcriptEngine: TranscriptEngineService,
        adDetection: AdDetectionProviding,
        cueMaterializer: SkipCueMaterializer,
        thermalStateProvider: @escaping @Sendable () -> ProcessInfo.ThermalState = {
            ProcessInfo.processInfo.thermalState
        },
        preemptionCoordinator: LanePreemptionCoordinator? = nil,
        clock: @escaping @Sendable () -> Date = { Date() }
    ) {
        self.store = store
        self.audioProvider = audioProvider
        self.featureService = featureService
        self.transcriptEngine = transcriptEngine
        self.adDetection = adDetection
        self.cueMaterializer = cueMaterializer
        self.thermalStateProvider = thermalStateProvider
        self.preemptionCoordinator = preemptionCoordinator
        self.clock = clock
    }

    // MARK: - Run

    /// Execute a bounded analysis pass described by `request`.
    /// Returns an `AnalysisOutcome` summarizing coverage achieved and stop reason.
    func run(_ request: AnalysisRangeRequest) async -> AnalysisOutcome {
        let assetId = request.analysisAssetId

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
        await transcriptEngine.startTranscription(
            shards: shards,
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

        // Compute coverage from confidence-filtered windows only, matching
        // the threshold used by SkipCueMaterializer, so that tier advancement
        // reflects actual materialized cues rather than raw ad detections.
        let confidenceThreshold = cueMaterializer.confidenceThreshold
        let cueCoverage = finalWindows
            .filter { $0.confidence >= confidenceThreshold && $0.endTime > $0.startTime }
            .map(\.endTime)
            .max() ?? 0

        // -- Stage 5: Cue materialization (policy-dependent) --

        var newCueCount = 0

        if request.outputPolicy == .writeWindowsAndCues {
            let cueSignpost = PreAnalysisInstrumentation.beginStage("cue_materialization")
            do {
                let cues = try await cueMaterializer.materialize(
                    windows: finalWindows,
                    analysisAssetId: assetId,
                    source: request.mode == .preRollWarmup ? "preAnalysis" : "playback"
                )
                newCueCount = cues.count
                PreAnalysisInstrumentation.endStage(cueSignpost)
            } catch {
                PreAnalysisInstrumentation.endStage(cueSignpost)
                logger.error("Cue materialization failed for job \(request.jobId): \(error)")
                return makeOutcome(
                    assetId: assetId,
                    request: request,
                    featureCoverageSec: featureCoverage,
                    transcriptCoverageSec: transcriptCoverage,
                    cueCoverageSec: cueCoverage,
                    stopReason: .failed("cueMaterialization: \(error)")
                )
            }
        }

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
