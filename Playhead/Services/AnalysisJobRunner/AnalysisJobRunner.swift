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
        preemptionCoordinator: LanePreemptionCoordinator? = nil
    ) {
        self.store = store
        self.audioProvider = audioProvider
        self.featureService = featureService
        self.transcriptEngine = transcriptEngine
        self.adDetection = adDetection
        self.cueMaterializer = cueMaterializer
        self.thermalStateProvider = thermalStateProvider
        self.preemptionCoordinator = preemptionCoordinator
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

        // Fire-and-forget: startTranscription kicks off work internally.
        await transcriptEngine.startTranscription(
            shards: shards,
            analysisAssetId: assetId,
            snapshot: snapshot,
            podcastId: request.podcastId,
            preemption: preemption
        )

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

        // TODO: Stop the transcript engine to prevent orphaned work if the timeout fired.
        // TranscriptEngineService does not yet expose a stopTranscription() method.

        if transcriptCoverage == 0 {
            logger.warning("Transcription for asset \(assetId) finished with zero coverage — stream may have ended prematurely or timed out")
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
    /// own `analysis_jobs`-row lease (see
    /// `AnalysisStore.acquireLease(jobId:owner:expiresAt:)`) that is
    /// structurally different from `EpisodeExecutionLease` (which is
    /// owned by `AnalysisCoordinator`). The coordinator only stores
    /// the lease value on its `LanePreemptionRegistration` for
    /// diagnostics — it never re-acquires it or reads any of its
    /// fields to make decisions. A synthetic value is therefore
    /// fidelity-preserving for the preemption contract.
    private func makeRegistrationLease(
        request: AnalysisRangeRequest
    ) -> EpisodeExecutionLease {
        let now = Date().timeIntervalSince1970
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
