// AnalysisCoordinator.swift
// Central orchestrator for the on-device analysis pipeline.
//
// Receives playback events (time, rate, play/pause, scrub) from
// PlaybackService, manages AnalysisSessions through a persisted state
// machine, and dispatches work to TranscriptEngineService,
// FeatureExtractionService, and AdDetectionService.
//
// Actor model: runs on its own cooperative executor. Never blocks on
// playback callbacks. All state transitions are persisted to SQLite so
// sessions survive crashes.

import Foundation
import OSLog

// MARK: - Session State Machine

/// The lifecycle states of an analysis session. Persisted as the `state`
/// column in `analysis_sessions` and the `analysisState` on `analysis_assets`.
enum SessionState: String, Sendable, CaseIterable {
    /// Audio identified, waiting for cached audio to become available.
    case queued
    /// Audio caching in progress, decode starting.
    case spooling
    /// Feature windows extracted for the hot zone around the playhead.
    case featuresReady
    /// Hot-path ad detection complete, skip cues available.
    case hotPathReady
    /// Final-pass ASR and metadata extraction running.
    case backfill
    /// All analysis work complete for this episode.
    case complete
    /// Analysis failed. Check `failureReason`. May be retryable.
    case failed

    /// Valid successor states from each state.
    var validTransitions: Set<SessionState> {
        switch self {
        case .queued:         [.spooling, .failed]
        case .spooling:       [.featuresReady, .failed]
        case .featuresReady:  [.hotPathReady, .failed]
        case .hotPathReady:   [.backfill, .complete, .failed]
        case .backfill:       [.complete, .failed]
        case .complete:       [.queued] // recovery: re-run if no data
        case .failed:         [.queued] // retry
        }
    }

    func canTransition(to next: SessionState) -> Bool {
        validTransitions.contains(next)
    }
}

// MARK: - Coordinator Errors

enum AnalysisCoordinatorError: Error, CustomStringConvertible {
    case invalidTransition(from: SessionState, to: SessionState)
    case noAudioAvailable(episodeId: String)
    case sessionNotFound(id: String)
    case storeError(underlying: Error)

    var description: String {
        switch self {
        case .invalidTransition(let from, let to):
            "Invalid session transition: \(from.rawValue) -> \(to.rawValue)"
        case .noAudioAvailable(let id):
            "No cached audio available for episode \(id)"
        case .sessionNotFound(let id):
            "Analysis session not found: \(id)"
        case .storeError(let err):
            "Analysis store error: \(err)"
        }
    }
}

// MARK: - Playback Event

/// Events from PlaybackService that drive analysis decisions.
enum PlaybackEvent: Sendable {
    /// Playback started or resumed for an episode.
    case playStarted(
        episodeId: String,
        podcastId: String?,
        audioURL: LocalAudioURL,
        time: TimeInterval,
        rate: Float
    )
    /// Periodic time update during playback.
    case timeUpdate(time: TimeInterval, rate: Float)
    /// User paused playback.
    case paused(time: TimeInterval)
    /// User scrubbed to a new position.
    case scrubbed(to: TimeInterval, rate: Float)
    /// Playback speed changed.
    case speedChanged(rate: Float, time: TimeInterval)
    /// Episode ended or user switched episodes.
    case stopped
}

// MARK: - AnalysisCoordinator

/// Central orchestrator coordinating all analysis work. Receives playback
/// events, manages AnalysisSessions, and dispatches work to downstream
/// services. Runs as a Swift actor with explicit handoff boundaries.
actor AnalysisCoordinator {

    private let logger = Logger(subsystem: "com.playhead", category: "AnalysisCoordinator")

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let audioService: AnalysisAudioService
    private let featureService: FeatureExtractionService
    private let transcriptEngine: TranscriptEngineService
    private let capabilitiesService: CapabilitiesService
    private let adDetectionService: AdDetectionService
    private let skipOrchestrator: SkipOrchestrator

    // MARK: - Active Session State

    /// The currently active session, if any.
    private var activeSessionId: String?
    /// The analysis asset ID for the current session.
    private var activeAssetId: String?
    /// The episode ID currently being analyzed.
    private var activeEpisodeId: String?
    /// Podcast identifier for profile-backed backfill decisions.
    private var activePodcastId: String?
    /// Decoded shards for the current episode (cached in memory for reuse).
    private var activeShards: [AnalysisShard]?
    /// Latest playback snapshot for prioritization.
    private var latestSnapshot: PlaybackSnapshot?

    // MARK: - Work Tasks

    /// Background task for the current analysis pipeline stage.
    private var pipelineTask: Task<Void, Never>?
    /// Task bridging persisted transcript chunks into ad detection.
    private var transcriptEventTask: Task<Void, Never>?
    /// Task listening for capability changes.
    private var capabilityObserverTask: Task<Void, Never>?

    // MARK: - Configuration

    /// Minimum scrub distance (seconds) to trigger reprioritization.
    private static let scrubThreshold: TimeInterval = 5.0
    /// Time update interval threshold: ignore updates closer than this.
    private static let timeUpdateMinInterval: TimeInterval = 2.0
    /// Last time we processed a time update, to debounce.
    private var lastTimeUpdateProcessed: TimeInterval = 0

    // MARK: - Init

    init(
        store: AnalysisStore,
        audioService: AnalysisAudioService,
        featureService: FeatureExtractionService,
        transcriptEngine: TranscriptEngineService,
        capabilitiesService: CapabilitiesService,
        adDetectionService: AdDetectionService,
        skipOrchestrator: SkipOrchestrator
    ) {
        self.store = store
        self.audioService = audioService
        self.featureService = featureService
        self.transcriptEngine = transcriptEngine
        self.capabilitiesService = capabilitiesService
        self.adDetectionService = adDetectionService
        self.skipOrchestrator = skipOrchestrator
    }

    // MARK: - Lifecycle

    /// Start observing capability changes. Call once at app launch.
    func start() {
        capabilityObserverTask?.cancel()
        capabilityObserverTask = Task { [weak self] in
            guard let self else { return }
            let updates = await self.capabilitiesService.capabilityUpdates()
            for await snapshot in updates {
                guard !Task.isCancelled else { break }
                await self.handleCapabilityChange(snapshot)
            }
        }
        logger.info("AnalysisCoordinator started")
    }

    /// Stop all work and clean up.
    func stop() {
        cancelPipeline()
        capabilityObserverTask?.cancel()
        capabilityObserverTask = nil
        activeSessionId = nil
        activeAssetId = nil
        activeEpisodeId = nil
        activePodcastId = nil
        activeShards = nil
        latestSnapshot = nil
        logger.info("AnalysisCoordinator stopped")
    }

    // MARK: - Playback Event Handling

    /// Main entry point: receive a playback event and react.
    /// Play-start resolves the analysis asset id so the runtime can wire
    /// the shared analysis session into skip/UI services.
    func handlePlaybackEvent(_ event: PlaybackEvent) async -> String? {
        switch event {
        case .playStarted(let episodeId, let podcastId, let audioURL, let time, let rate):
            logger.info("Play started: episode=\(episodeId) t=\(time, format: .fixed(precision: 1))s")
            return await handlePlayStarted(
                episodeId: episodeId,
                podcastId: podcastId,
                audioURL: audioURL,
                time: time,
                rate: rate
            )

        case .timeUpdate(let time, let rate):
            handleTimeUpdate(time: time, rate: rate)
            return nil

        case .paused(let time):
            logger.info("Paused at \(time, format: .fixed(precision: 1))s")
            latestSnapshot = PlaybackSnapshot(playheadTime: time, playbackRate: 0, isPlaying: false)
            return nil

        case .scrubbed(let time, let rate):
            logger.info("Scrub to \(time, format: .fixed(precision: 1))s")
            handleScrub(to: time, rate: rate)
            return nil

        case .speedChanged(let rate, let time):
            logger.info("Speed changed to \(rate, format: .fixed(precision: 1))x")
            let snapshot = PlaybackSnapshot(playheadTime: time, playbackRate: Double(rate), isPlaying: true)
            latestSnapshot = snapshot
            if let shards = activeShards, let assetId = activeAssetId {
                Task {
                    await transcriptEngine.handleSpeedChange(
                        shards: shards,
                        analysisAssetId: assetId,
                        snapshot: snapshot
                    )
                }
            }
            return nil

        case .stopped:
            logger.info("Playback stopped")
            handleStopped()
            return nil
        }
    }

    // MARK: - Play Started

    private func handlePlayStarted(
        episodeId: String,
        podcastId: String?,
        audioURL: LocalAudioURL,
        time: TimeInterval,
        rate: Float
    ) async -> String? {
        // If same episode is already active, just update the snapshot.
        if episodeId == activeEpisodeId, activeSessionId != nil {
            latestSnapshot = PlaybackSnapshot(playheadTime: time, playbackRate: Double(rate), isPlaying: true)
            return activeAssetId
        }

        // New episode -- stop existing work and start fresh.
        cancelPipeline()

        activeEpisodeId = episodeId
        activePodcastId = podcastId
        latestSnapshot = PlaybackSnapshot(playheadTime: time, playbackRate: Double(rate), isPlaying: true)

        do {
            let (sessionId, assetId, resumeState) = try await resolveSession(
                episodeId: episodeId,
                audioURL: audioURL
            )
            activeSessionId = sessionId
            activeAssetId = assetId

            pipelineTask = Task {
                await self.runPipeline(
                    sessionId: sessionId,
                    assetId: assetId,
                    resumeState: resumeState,
                    episodeId: episodeId,
                    audioURL: audioURL
                )
            }

            if let podcastId {
                logger.info("Resolved analysis asset \(assetId) for podcast \(podcastId)")
            }

            return assetId
        } catch {
            logger.error("Failed to resolve session for episode \(episodeId): \(error)")
            return nil
        }
    }

    /// Continue the analysis pipeline after the session has been resolved.
    private func runPipeline(
        sessionId: String,
        assetId: String,
        resumeState: SessionState,
        episodeId: String,
        audioURL: LocalAudioURL
    ) async {
        do {
            switch resumeState {
            case .queued:
                try await runFromQueued(sessionId: sessionId, assetId: assetId, episodeId: episodeId, audioURL: audioURL)
            case .spooling:
                try await runFromSpooling(sessionId: sessionId, assetId: assetId, episodeId: episodeId, audioURL: audioURL)
            case .featuresReady:
                try await runFromFeaturesReady(sessionId: sessionId, assetId: assetId)
            case .hotPathReady:
                try await runFromHotPathReady(sessionId: sessionId, assetId: assetId)
            case .backfill:
                try await runFromBackfill(sessionId: sessionId, assetId: assetId)
            case .complete:
                // Verify the session actually has transcript data. A crash
                // during backfill can leave the session as "complete" with
                // no chunks. Restart from queued to re-decode audio (shards
                // aren't in memory across app launches).
                let chunks = try await store.fetchTranscriptChunks(assetId: assetId)
                if chunks.isEmpty {
                    logger.info("Session \(sessionId) marked complete but has 0 chunks — restarting from queued")
                    try await transition(sessionId: sessionId, assetId: assetId, to: .queued)
                    try await runFromQueued(sessionId: sessionId, assetId: assetId, episodeId: episodeId, audioURL: audioURL)
                } else {
                    logger.info("Session \(sessionId) already complete (\(chunks.count) chunks)")
                }
            case .failed:
                try await transition(sessionId: sessionId, assetId: assetId, to: .queued)
                try await runFromQueued(sessionId: sessionId, assetId: assetId, episodeId: episodeId, audioURL: audioURL)
            }
        } catch is CancellationError {
            logger.info("Pipeline cancelled for episode \(episodeId)")
        } catch {
            logger.error("Pipeline failed for episode \(episodeId): \(error)")
            // Use the captured sessionId/assetId parameters, not mutable actor
            // state which may have changed if a new episode started.
            try? await transition(
                sessionId: sessionId,
                assetId: assetId,
                to: .failed,
                failureReason: String(describing: error)
            )
        }
    }

    // MARK: - Time Update

    private func handleTimeUpdate(time: TimeInterval, rate: Float) {
        // Debounce: ignore updates that are too frequent.
        guard abs(time - lastTimeUpdateProcessed) >= Self.timeUpdateMinInterval else { return }
        lastTimeUpdateProcessed = time

        latestSnapshot = PlaybackSnapshot(playheadTime: time, playbackRate: Double(rate), isPlaying: rate > 0)
    }

    // MARK: - Scrub

    private func handleScrub(to time: TimeInterval, rate: Float) {
        let previousTime = latestSnapshot?.playheadTime ?? 0
        latestSnapshot = PlaybackSnapshot(playheadTime: time, playbackRate: Double(rate), isPlaying: true)

        // Only reprioritize if scrub is significant.
        guard abs(time - previousTime) >= Self.scrubThreshold else { return }

        guard let shards = activeShards,
              let assetId = activeAssetId,
              let snapshot = latestSnapshot
        else { return }

        // Reprioritize transcript engine without losing existing work.
        // Strong self capture: actor should stay alive to complete reprioritization.
        Task { [shards, assetId, snapshot] in
            await self.transcriptEngine.handleScrub(
                shards: shards,
                analysisAssetId: assetId,
                snapshot: snapshot
            )
        }

        logger.info("Reprioritized analysis for scrub: \(previousTime, format: .fixed(precision: 1))s -> \(time, format: .fixed(precision: 1))s")
    }

    // MARK: - Stop

    private func handleStopped() {
        cancelPipeline()
        Task { await self.transcriptEngine.stop() }
        activeEpisodeId = nil
        activePodcastId = nil
        activeSessionId = nil
        activeAssetId = nil
        activeShards = nil
        latestSnapshot = nil
    }

    // MARK: - Stage: Queued -> Spooling

    private func runFromQueued(sessionId: String, assetId: String, episodeId: String, audioURL: LocalAudioURL) async throws {
        try Task.checkCancellation()
        try await transition(sessionId: sessionId, assetId: assetId, to: .spooling)
        try await runFromSpooling(sessionId: sessionId, assetId: assetId, episodeId: episodeId, audioURL: audioURL)
    }

    // MARK: - Stage: Spooling -> FeaturesReady

    private func runFromSpooling(sessionId: String, assetId: String, episodeId: String, audioURL: LocalAudioURL) async throws {
        try Task.checkCancellation()

        // Decode audio into shards. Truncated files now return partial data
        // instead of throwing, so this catch is only for truly fatal errors
        // (file not found, unreadable asset, decoder failure).
        logger.info("Spooling: decoding audio for episode \(episodeId)")
        let decodeStart = ContinuousClock.now
        let shards: [AnalysisShard]
        do {
            shards = try await audioService.decode(fileURL: audioURL, episodeID: episodeId)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            logger.error("Audio decode failed for episode \(episodeId): \(error)")
            throw AnalysisCoordinatorError.noAudioAvailable(episodeId: episodeId)
        }
        let decodeElapsed = ContinuousClock.now - decodeStart
        guard !shards.isEmpty else {
            throw AnalysisCoordinatorError.noAudioAvailable(episodeId: episodeId)
        }
        let totalAudio = shards.map(\.duration).reduce(0, +)
        logger.info("Spooling: decoded \(shards.count) shards (\(String(format: "%.0f", totalAudio))s audio) in \(decodeElapsed)")
        activeShards = shards

        // Extract features for the hot zone.
        let existingCoverage: Double
        do {
            let asset = try await store.fetchAsset(id: assetId)
            existingCoverage = asset?.featureCoverageEndTime ?? 0
        } catch {
            existingCoverage = 0
        }

        try Task.checkCancellation()
        logger.info("Spooling: extracting features (existing coverage: \(String(format: "%.1f", existingCoverage))s)")
        let featureStart = ContinuousClock.now
        try await featureService.extractAndPersist(
            shards: shards,
            analysisAssetId: assetId,
            existingCoverage: existingCoverage
        )
        logger.info("Spooling: features extracted in \(ContinuousClock.now - featureStart)")

        try await transition(sessionId: sessionId, assetId: assetId, to: .featuresReady)
        try await runFromFeaturesReady(sessionId: sessionId, assetId: assetId)
    }

    // MARK: - Stage: FeaturesReady -> HotPathReady

    private func runFromFeaturesReady(sessionId: String, assetId: String) async throws {
        try Task.checkCancellation()

        // Start transcription with current playback snapshot.
        if let shards = activeShards, let snapshot = latestSnapshot {
            logger.info("FeaturesReady: starting transcription (\(shards.count) shards, playhead at \(String(format: "%.1f", snapshot.playheadTime))s)")
            startObservingTranscriptEvents(sessionId: sessionId, assetId: assetId)
            await transcriptEngine.startTranscription(
                shards: shards,
                analysisAssetId: assetId,
                snapshot: snapshot
            )
        } else {
            logger.warning("FeaturesReady: no shards or snapshot available — skipping transcription")
        }

        try await transition(sessionId: sessionId, assetId: assetId, to: .hotPathReady)
        try await runFromHotPathReady(sessionId: sessionId, assetId: assetId)
    }

    // MARK: - Stage: HotPathReady -> Backfill

    private func runFromHotPathReady(sessionId: String, assetId: String) async throws {
        try Task.checkCancellation()

        // Hot path is ready: skip cues are available for playback.
        // Now move to backfill for final-pass ASR and metadata extraction.
        let capabilities = await capabilitiesService.currentSnapshot
        if capabilities.shouldThrottleAnalysis {
            logger.info("Thermal throttle active, deferring backfill")
            return
        }

        try await transition(sessionId: sessionId, assetId: assetId, to: .backfill)
        try await runFromBackfill(sessionId: sessionId, assetId: assetId)
    }

    // MARK: - Stage: Backfill -> Complete

    private func runFromBackfill(sessionId: String, assetId: String) async throws {
        try Task.checkCancellation()

        // If resuming from a persisted backfill state (e.g. after a crash),
        // there's no active transcript task. Check if transcription actually
        // produced data. If not, throw so runPipeline marks as failed →
        // queued, which will re-decode audio and restart fully.
        if transcriptEventTask == nil {
            let chunks = try await store.fetchTranscriptChunks(assetId: assetId)
            if chunks.isEmpty {
                logger.info("Backfill resumed with 0 chunks — requesting full restart")
                throw AnalysisCoordinatorError.noAudioAvailable(episodeId: activeEpisodeId ?? "unknown")
            }
            try await finalizeBackfill(sessionId: sessionId, assetId: assetId)
            return
        }

        logger.info("Backfill waiting for transcript completion on asset \(assetId)")
    }

    // MARK: - State Machine Transitions

    /// Transition a session to a new state. Validates the transition, persists
    /// to SQLite, and logs the change.
    private func transition(
        sessionId: String,
        assetId: String,
        to newState: SessionState,
        failureReason: String? = nil
    ) async throws {
        // Load current state.
        let currentState: SessionState
        do {
            guard let session = try await store.fetchSession(id: sessionId) else {
                throw AnalysisCoordinatorError.sessionNotFound(id: sessionId)
            }
            guard let state = SessionState(rawValue: session.state) else {
                // Unknown state in DB -- persist failed state and reject transition.
                logger.error("Unknown session state '\(session.state)' for session \(sessionId), persisting as failed")
                try await store.updateSessionState(
                    id: sessionId,
                    state: SessionState.failed.rawValue,
                    failureReason: "Unknown state in DB: \(session.state)"
                )
                try await store.updateAssetState(id: assetId, state: SessionState.failed.rawValue)
                throw AnalysisCoordinatorError.invalidTransition(from: .failed, to: newState)
            }
            currentState = state
        } catch let error as AnalysisCoordinatorError {
            throw error
        } catch {
            throw AnalysisCoordinatorError.storeError(underlying: error)
        }

        // Validate transition.
        guard currentState.canTransition(to: newState) else {
            throw AnalysisCoordinatorError.invalidTransition(from: currentState, to: newState)
        }

        // Persist.
        do {
            try await store.updateSessionState(
                id: sessionId,
                state: newState.rawValue,
                failureReason: failureReason
            )
            try await store.updateAssetState(id: assetId, state: newState.rawValue)
        } catch {
            throw AnalysisCoordinatorError.storeError(underlying: error)
        }

        logger.info("Session \(sessionId): \(currentState.rawValue) -> \(newState.rawValue)")
    }

    /// Resolve or create an analysis asset ID for an episode without starting
    /// the pipeline. Use this to wire up the transcript UI while audio is
    /// still downloading.
    func resolveAssetId(episodeId: String) async -> String? {
        do {
            if let existing = try await store.fetchAssetByEpisodeId(episodeId) {
                return existing.id
            }
            // Create a placeholder asset so the UI has an ID to bind to.
            let assetId = UUID().uuidString
            let asset = AnalysisAsset(
                id: assetId,
                episodeId: episodeId,
                assetFingerprint: assetId,
                weakFingerprint: nil,
                sourceURL: "",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.queued.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
            try await store.insertAsset(asset)
            return assetId
        } catch {
            logger.error("Failed to resolve asset ID for episode \(episodeId): \(error)")
            return nil
        }
    }

    // MARK: - Session Resolution

    /// Find an existing session for this episode or create a new one.
    /// Returns (sessionId, assetId, resumeState).
    private func resolveSession(
        episodeId: String,
        audioURL: LocalAudioURL
    ) async throws -> (String, String, SessionState) {
        // Check if an asset already exists for this episode.
        let assetId: String
        let existingAsset = try await store.fetchAssetByEpisodeId(episodeId)

        if let existing = existingAsset {
            assetId = existing.id

            // Check for an existing session.
            let existingSession = try await store.fetchLatestSessionForAsset(assetId: assetId)
            if let session = existingSession,
               let state = SessionState(rawValue: session.state),
               state != .failed
            {
                // Resume from persisted state.
                return (session.id, assetId, state)
            }

            // No usable session -- create a new one.
            let sessionId = UUID().uuidString
            let now = Date().timeIntervalSince1970
            let session = AnalysisSession(
                id: sessionId,
                analysisAssetId: assetId,
                state: SessionState.queued.rawValue,
                startedAt: now,
                updatedAt: now,
                failureReason: nil
            )
            try await store.insertSession(session)
            return (sessionId, assetId, .queued)
        }

        // Create new asset and session.
        assetId = UUID().uuidString
        let capabilityJSON: String?
        do {
            let snapshot = await capabilitiesService.currentSnapshot
            let data = try JSONEncoder().encode(snapshot)
            capabilityJSON = String(data: data, encoding: .utf8)
        } catch {
            capabilityJSON = nil
        }

        let asset = AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: assetId, // Placeholder until content hashing
            weakFingerprint: nil,
            sourceURL: audioURL.url.absoluteString,
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: capabilityJSON
        )
        try await store.insertAsset(asset)

        let sessionId = UUID().uuidString
        let now = Date().timeIntervalSince1970
        let session = AnalysisSession(
            id: sessionId,
            analysisAssetId: assetId,
            state: SessionState.queued.rawValue,
            startedAt: now,
            updatedAt: now,
            failureReason: nil
        )
        try await store.insertSession(session)

        return (sessionId, assetId, .queued)
    }

    // MARK: - Capability Changes

    private func handleCapabilityChange(_ snapshot: CapabilitySnapshot) {
        if snapshot.shouldThrottleAnalysis {
            logger.warning("Thermal throttle: pausing analysis pipeline")
            // Don't cancel -- just let the current stage finish and
            // the next stage check will defer.
        }
    }

    // MARK: - Pipeline Control

    private func cancelPipeline() {
        pipelineTask?.cancel()
        pipelineTask = nil
        transcriptEventTask?.cancel()
        transcriptEventTask = nil
    }

    private func startObservingTranscriptEvents(sessionId: String, assetId: String) {
        transcriptEventTask?.cancel()
        transcriptEventTask = Task {
            let stream = await self.transcriptEngine.events()
            for await event in stream {
                guard !Task.isCancelled else { return }

                switch event {
                case .chunksPersisted(let analysisAssetId, let chunks):
                    guard analysisAssetId == assetId else { continue }
                    await self.handlePersistedTranscriptChunks(chunks, assetId: assetId)

                case .completed(let analysisAssetId):
                    guard analysisAssetId == assetId else { continue }
                    do {
                        try await self.finalizeBackfill(sessionId: sessionId, assetId: assetId)
                    } catch {
                        self.logger.error("Backfill finalization failed for asset \(assetId): \(error.localizedDescription)")
                    }
                    return
                }
            }
        }
    }

    private func handlePersistedTranscriptChunks(
        _ chunks: [TranscriptChunk],
        assetId: String
    ) async {
        let fastChunks = chunks.filter { $0.pass == TranscriptPassType.fast.rawValue }
        guard !fastChunks.isEmpty else { return }

        do {
            let windows = try await adDetectionService.runHotPath(
                chunks: fastChunks,
                analysisAssetId: assetId,
                episodeDuration: currentEpisodeDuration()
            )
            guard !windows.isEmpty else { return }
            await skipOrchestrator.receiveAdWindows(windows)
        } catch {
            logger.error("Hot-path ad detection failed for asset \(assetId): \(error.localizedDescription)")
        }
    }

    private func finalizeBackfill(sessionId: String, assetId: String) async throws {
        defer {
            transcriptEventTask?.cancel()
            transcriptEventTask = nil
        }

        let allChunks = try await store.fetchTranscriptChunks(assetId: assetId)
        if let podcastId = activePodcastId, !allChunks.isEmpty {
            try await adDetectionService.runBackfill(
                chunks: allChunks,
                analysisAssetId: assetId,
                podcastId: podcastId,
                episodeDuration: currentEpisodeDuration()
            )
        }

        let updatedWindows = try await store.fetchAdWindows(assetId: assetId)
        if !updatedWindows.isEmpty {
            await skipOrchestrator.receiveAdWindows(updatedWindows)
        }

        guard let session = try await store.fetchSession(id: sessionId),
              let sessionState = SessionState(rawValue: session.state)
        else {
            return
        }

        switch sessionState {
        case .featuresReady:
            try await transition(sessionId: sessionId, assetId: assetId, to: .hotPathReady)
            try await transition(sessionId: sessionId, assetId: assetId, to: .backfill)

        case .hotPathReady:
            try await transition(sessionId: sessionId, assetId: assetId, to: .backfill)

        case .backfill:
            break

        case .complete, .failed, .queued, .spooling:
            return
        }

        try await transition(sessionId: sessionId, assetId: assetId, to: .complete)
        logger.info("Analysis complete for asset \(assetId)")
    }

    private func currentEpisodeDuration() -> Double {
        guard let shards = activeShards else { return 0 }
        return shards.reduce(0) { partial, shard in
            max(partial, shard.startTime + shard.duration)
        }
    }
}
