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

    // MARK: - Init

    init(
        store: AnalysisStore,
        audioProvider: AnalysisAudioProviding,
        featureService: FeatureExtractionService,
        transcriptEngine: TranscriptEngineService,
        adDetection: AdDetectionProviding,
        cueMaterializer: SkipCueMaterializer
    ) {
        self.store = store
        self.audioProvider = audioProvider
        self.featureService = featureService
        self.transcriptEngine = transcriptEngine
        self.adDetection = adDetection
        self.cueMaterializer = cueMaterializer
    }

    // MARK: - Run

    /// Execute a bounded analysis pass described by `request`.
    /// Returns an `AnalysisOutcome` summarizing coverage achieved and stop reason.
    func run(_ request: AnalysisRangeRequest) async -> AnalysisOutcome {
        let assetId = request.analysisAssetId

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
                existingCoverage: existingFeatureCoverage
            )
            PreAnalysisInstrumentation.endStage(featureSignpost)
        } catch {
            PreAnalysisInstrumentation.endStage(featureSignpost)
            logger.error("Feature extraction failed for job \(request.jobId): \(error)")
            return makeOutcome(assetId: assetId, request: request, stopReason: .failed("features: \(error)"))
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

        // Fire-and-forget: startTranscription kicks off work internally.
        await transcriptEngine.startTranscription(
            shards: shards,
            analysisAssetId: assetId,
            snapshot: snapshot
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

        let episodeDuration = allShards.map { $0.startTime + $0.duration }.max() ?? 0

        // Hot path detection.
        var adWindows: [AdWindow] = []
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
        let finalWindows: [AdWindow]
        do {
            finalWindows = try await store.fetchAdWindows(assetId: assetId)
        } catch {
            finalWindows = adWindows
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

    /// Check for cancellation and thermal throttling between pipeline stages.
    /// Returns a stop reason if the runner should bail out, nil to continue.
    private func checkStopConditions() -> AnalysisOutcome.StopReason? {
        if Task.isCancelled {
            return .cancelledByPlayback
        }

        let thermalState = ProcessInfo.processInfo.thermalState
        if thermalState.rawValue >= ProcessInfo.ThermalState.serious.rawValue {
            logger.warning("Thermal state \(String(describing: thermalState)) — pausing analysis")
            return .pausedForThermal
        }

        return nil
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
