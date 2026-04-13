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
    private let downloadManager: DownloadManager?

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
    /// Audio file URL for incremental re-decode as download progresses.
    private var activeAudioURL: LocalAudioURL?
    /// Latest playback snapshot for prioritization.
    private var latestSnapshot: PlaybackSnapshot?
    /// Snapshot captured when the pipeline started — used for initial
    /// transcription ordering so timeUpdate events don't race with it.
    private var pipelineStartSnapshot: PlaybackSnapshot?

    // MARK: - Work Tasks

    /// Background task for the current analysis pipeline stage.
    private var pipelineTask: Task<Void, Never>?
    /// Task bridging persisted transcript chunks into ad detection.
    private var transcriptEventTask: Task<Void, Never>?
    /// Task listening for capability changes.
    private var capabilityObserverTask: Task<Void, Never>?
    /// Task feeding live download chunks into the streaming decoder.
    private var streamingDecodeTask: Task<Void, Never>?
    /// Task consuming streaming decoder shards.
    private var shardConsumerTask: Task<Void, Never>?

    // MARK: - Configuration

    /// Minimum scrub distance (seconds) to trigger reprioritization.
    private static let scrubThreshold: TimeInterval = 5.0
    /// Time update interval threshold: ignore updates closer than this.
    private static let timeUpdateMinInterval: TimeInterval = 2.0
    /// Last time we processed a time update, to debounce.
    private var lastTimeUpdateProcessed: TimeInterval = 0

    /// Set to `true` when `stop()` is called. Observed by `runPendingBackfill`
    /// so that a coordinator stop initiated mid-backfill (e.g. thermal=critical
    /// triggering `BackgroundProcessingService.handleCapabilityUpdate`) tears
    /// down the polling loop promptly instead of spinning for up to 25 minutes.
    /// Reset in ``startCapabilityObserver()`` and ``runPendingBackfill()``.
    private var stopRequested = false

    // MARK: - Init

    init(
        store: AnalysisStore,
        audioService: AnalysisAudioService,
        featureService: FeatureExtractionService,
        transcriptEngine: TranscriptEngineService,
        capabilitiesService: CapabilitiesService,
        adDetectionService: AdDetectionService,
        skipOrchestrator: SkipOrchestrator,
        downloadManager: DownloadManager? = nil
    ) {
        self.store = store
        self.audioService = audioService
        self.featureService = featureService
        self.transcriptEngine = transcriptEngine
        self.capabilitiesService = capabilitiesService
        self.adDetectionService = adDetectionService
        self.skipOrchestrator = skipOrchestrator
        self.downloadManager = downloadManager
    }

    // MARK: - Lifecycle

    /// Start observing capability changes. Call once at app launch.
    ///
    /// The observer is a long-lived monitoring concern that survives
    /// ``stop()`` calls. ``stop()`` cancels active pipeline work but
    /// leaves this observer alive so it does not need to be re-started
    /// after thermal/battery recovery. The observer's handler
    /// (``handleCapabilityChange``) is purely diagnostic logging and
    /// does not start work, so leaving it alive during a stopped state
    /// is safe.
    func startCapabilityObserver() {
        // Clear any prior stop request so a stop/start cycle re-enables the
        // polling loop in runPendingBackfill.
        stopRequested = false
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

    /// Stop all active work and clean up pipeline state.
    ///
    /// The capability observer started by ``startCapabilityObserver()``
    /// is intentionally left alive. It is a lightweight monitoring task
    /// whose handler (``handleCapabilityChange``) only logs — it never
    /// starts work. Keeping it alive means callers do not need to
    /// re-start the observer after thermal/battery recovery, which
    /// eliminates a class of lifecycle coupling bugs between
    /// ``BackgroundProcessingService`` and this coordinator.
    func stop() async {
        stopRequested = true
        await cancelPipeline()
        activeSessionId = nil
        activeAssetId = nil
        activeEpisodeId = nil
        activePodcastId = nil
        shardConsumerTask?.cancel()
        shardConsumerTask = nil
        activeShards = nil
        activeAudioURL = nil
        latestSnapshot = nil
        logger.info("AnalysisCoordinator stopped")
    }

    // MARK: - Background Backfill

    /// Drain any pending pre-analysis work during a BGProcessingTask window.
    ///
    /// The actual job execution happens in `AnalysisWorkScheduler`, which runs
    /// its own loop in the background process. This method exists so the
    /// background task handler in `BackgroundProcessingService` has a real
    /// piece of work to await: it polls the analysis_jobs table for queued/
    /// running jobs and yields the actor between checks, keeping the
    /// BGProcessingTask alive while the scheduler loop drains the queue.
    ///
    /// Returns when no more work is pending, or when `Task.isCancelled`
    /// becomes true (i.e. iOS expired the background window).
    func runPendingBackfill() async {
        // Clear any prior stop request. stop() sets this flag to break the
        // polling loop in a previous backfill run, but a NEW backfill invocation
        // (from a fresh BGProcessingTask) must be allowed to run. Without this
        // reset, a thermal-critical stop() would permanently disable backfill
        // until the next app restart.
        stopRequested = false
        logger.info("runPendingBackfill: draining pending analysis jobs")

        // Maximum lifetime cap so we never spin forever inside one BG window
        // even if the scheduler keeps producing new tier jobs.
        let deadline = ContinuousClock.now + .seconds(25 * 60)
        // Poll interval — 1s gives the scheduler loop time to make progress
        // without burning the actor on tight queries.
        let pollInterval: Duration = .seconds(1)

        await Self.runBackfillPollingLoop(
            deadline: deadline,
            pollInterval: pollInterval,
            isStopRequested: { [weak self] in
                guard let self else { return true }
                return await self.stopRequested
            },
            fetchPendingCount: { [weak self] in
                guard let self else { return 0 }
                return await self.fetchPendingJobCount()
            },
            sleep: { duration in
                try await Task.sleep(for: duration)
            },
            logger: logger
        )
    }

    /// Read the current pending job count across queued/running/paused.
    /// Extracted so `runPendingBackfill` can delegate to a closure-driven
    /// polling loop that is unit-testable without a full coordinator.
    private func fetchPendingJobCount() async -> Int {
        let queued = (try? await store.fetchJobsByState("queued")) ?? []
        let running = (try? await store.fetchJobsByState("running")) ?? []
        let paused = (try? await store.fetchJobsByState("paused")) ?? []
        return queued.count + running.count + paused.count
    }

    /// The polling loop used by `runPendingBackfill`. Exposed as a static
    /// helper so regression tests can drive it with injected closures without
    /// standing up a full `AnalysisCoordinator` (which transitively requires
    /// AudioService / FeatureService / TranscriptEngine / AdDetection / etc.).
    ///
    /// Behaviour notes covered by regression tests:
    ///   - H2: the loop exits promptly when `isStopRequested` returns true,
    ///     not just on `Task.isCancelled` / deadline.
    ///   - H3: requires two consecutive zero-pending polls before declaring
    ///     the queue drained. Single-job queues with tier advancement expose
    ///     a transient zero window between AnalysisWorkScheduler completing
    ///     one job and picking up a tier-advanced next job; returning on the
    ///     first zero was causing BG time to be surrendered prematurely.
    static func runBackfillPollingLoop(
        deadline: ContinuousClock.Instant,
        pollInterval: Duration,
        zeroPollThreshold: Int = 2,
        isStopRequested: @Sendable () async -> Bool,
        fetchPendingCount: @Sendable () async -> Int,
        sleep: @Sendable (Duration) async throws -> Void,
        now: @Sendable () -> ContinuousClock.Instant = { ContinuousClock.now },
        logger: Logger
    ) async {
        var processedAny = false
        var consecutiveZeroCount = 0

        while !Task.isCancelled, now() < deadline {
            if await isStopRequested() {
                logger.info("runPendingBackfill: coordinator stop requested, exiting loop")
                return
            }

            let pendingCount = await fetchPendingCount()

            if pendingCount == 0 {
                consecutiveZeroCount += 1
                if consecutiveZeroCount >= zeroPollThreshold {
                    logger.info("runPendingBackfill: queue drained after \(zeroPollThreshold) consecutive empty polls (processed=\(processedAny))")
                    return
                }
                logger.debug("runPendingBackfill: zero pending (\(consecutiveZeroCount)/\(zeroPollThreshold)) — waiting for tier advancement")
            } else {
                consecutiveZeroCount = 0
                processedAny = true
                logger.debug("runPendingBackfill: \(pendingCount) jobs pending")
            }

            // Sleep returns CancellationError when the task is cancelled,
            // which propagates expiration through the loop guard above.
            do {
                try await sleep(pollInterval)
            } catch {
                logger.info("runPendingBackfill: cancelled by expiration")
                return
            }
        }

        if Task.isCancelled {
            logger.info("runPendingBackfill: cancelled")
        } else {
            logger.info("runPendingBackfill: deadline reached after draining")
        }
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
            await handleStopped()
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
        await cancelPipeline()

        activeEpisodeId = episodeId
        activePodcastId = podcastId
        let startSnapshot = PlaybackSnapshot(playheadTime: time, playbackRate: Double(rate), isPlaying: true)
        latestSnapshot = startSnapshot
        pipelineStartSnapshot = startSnapshot

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

    private func handleStopped() async {
        await cancelPipeline()
        activeEpisodeId = nil
        activePodcastId = nil
        activeSessionId = nil
        activeAssetId = nil
        activeShards = nil
        activeAudioURL = nil
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
        activeAudioURL = audioURL

        // Start streaming decode — feeds download bytes directly into
        // the decoder, emitting shards as audio arrives.
        await startStreamingDecode(
            episodeId: episodeId,
            assetId: assetId
        )

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

        // Use the snapshot from when the pipeline started, not the latest
        // one — timeUpdate events race with pipeline setup and would cause
        // shard 0 to be deprioritized if the playhead has drifted.
        if let shards = activeShards, let snapshot = pipelineStartSnapshot ?? latestSnapshot {
            logger.info("FeaturesReady: starting transcription (\(shards.count) shards, playhead at \(String(format: "%.1f", snapshot.playheadTime))s)")
            startObservingTranscriptEvents(sessionId: sessionId, assetId: assetId)
            await transcriptEngine.startTranscription(
                shards: shards,
                analysisAssetId: assetId,
                snapshot: snapshot,
                podcastId: activePodcastId
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
        if capabilities.thermalState == .critical {
            logger.info("Critical thermal active, deferring backfill")
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
                // For late-stage states (backfill, complete), verify that
                // transcript data actually exists. A prior crash can leave
                // the state machine advanced but the data empty.
                if state == .backfill || state == .complete {
                    let chunks = try await store.fetchTranscriptChunks(assetId: assetId)
                    if chunks.isEmpty {
                        logger.info("Session \(session.id) in \(state.rawValue) but 0 chunks — resetting to queued")
                        try await store.updateSessionState(
                            id: session.id,
                            state: SessionState.queued.rawValue,
                            failureReason: nil
                        )
                        try await store.updateAssetState(id: assetId, state: SessionState.queued.rawValue)
                        // Reset coverage watermarks so no shards are skipped.
                        try await store.updateFastTranscriptCoverage(id: assetId, endTime: 0)
                        return (session.id, assetId, .queued)
                    }
                }
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
        if snapshot.thermalState == .critical {
            logger.warning("Critical thermal: pausing analysis pipeline")
            // Don't cancel -- just let the current stage finish and
            // the next stage check will defer.
        } else if snapshot.thermalState == .serious {
            logger.warning("Serious thermal: reducing analysis aggressiveness")
        }
    }

    // MARK: - Streaming Decode from Download

    /// Active streaming decoder for the current episode.
    private var activeDecoder: StreamingAudioDecoder?

    /// Debug counters for TestFlight diagnostics (written to UserDefaults).
#if DEBUG
    private var streamingShardsEmitted: Int = 0 {
        didSet { UserDefaults.standard.set(streamingShardsEmitted, forKey: "debug_streamingShards") }
    }
    private var streamingSeededBytes: Int = 0 {
        didSet { UserDefaults.standard.set(streamingSeededBytes, forKey: "debug_streamingSeeded") }
    }
    private var streamingChunksReceived: Int = 0 {
        didSet { UserDefaults.standard.set(streamingChunksReceived, forKey: "debug_streamingChunks") }
    }
#else
    private var streamingShardsEmitted: Int = 0
    private var streamingSeededBytes: Int = 0
    private var streamingChunksReceived: Int = 0
#endif

    private func startStreamingDecode(
        episodeId: String,
        assetId: String,
        contentType: String = "mp3"
    ) async {
        streamingDecodeTask?.cancel()
        guard let dm = downloadManager else {
            logger.warning("No download manager — skipping streaming decode")
            return
        }

        streamingShardsEmitted = 0
        streamingSeededBytes = 0
        streamingChunksReceived = 0
        let decoder = StreamingAudioDecoder(
            episodeID: episodeId,
            shardDuration: 30.0,
            contentType: contentType
        )
        activeDecoder = decoder

        // Create the shard stream BEFORE feeding data to avoid the race where
        // feedData emits shards before anyone is listening (continuation would be nil).
        let shardStream = await decoder.shards()

        // Task 1: Subscribe to live stream first, then seed from disk.
        // Subscribe BEFORE reading the file so we don't miss chunks that
        // arrive between the file read and the subscription.
        streamingDecodeTask = Task {
            let dataStream = await dm.audioDataUpdates()

            // Seed with the file already on disk. This captures all bytes
            // downloaded before we subscribed, including the case where the
            // download has already completed.
            if let url = self.activeAudioURL?.url,
               let existingData = try? Data(contentsOf: url) {
                self.streamingSeededBytes = existingData.count
                self.logger.info("Streaming decode: seeding \(existingData.count) bytes from disk")
                await decoder.feedData(existingData)
            } else {
                self.logger.warning("Streaming decode: no file to seed from (activeAudioURL=\(String(describing: self.activeAudioURL)))")
            }

            // Consume live bytes. If the download already completed before
            // we subscribed, this loop receives zero chunks and exits when
            // we break below via timeout.
            let seededBytes = Int64(self.streamingSeededBytes)
            var receivedAny = false
            for await chunk in dataStream {
                guard !Task.isCancelled else { break }
                guard chunk.episodeId == episodeId else { continue }
                receivedAny = true
                // Skip chunks that overlap bytes already seeded from disk.
                guard chunk.totalBytesWritten > seededBytes else { continue }
                self.streamingChunksReceived += 1
                await decoder.feedData(chunk.data)
            }

            // If we never received live chunks, the download completed
            // before we subscribed. Re-read the file to get everything.
            if !receivedAny, let url = self.activeAudioURL?.url,
               let fullData = try? Data(contentsOf: url) {
                let alreadySeeded = self.streamingSeededBytes
                if fullData.count > alreadySeeded {
                    let newBytes = fullData.subdata(in: alreadySeeded..<fullData.count)
                    self.logger.info("Streaming decode: late read \(newBytes.count) new bytes (total \(fullData.count))")
                    await decoder.feedData(newBytes)
                }
            }

            await decoder.finish()
            self.logger.info("Streaming decode complete, decoder finished")
        }

        // Task 2: Consume emitted shards and feed to transcript + features.
        let initialCoverageEnd = activeShards?.last.map { $0.startTime + $0.duration } ?? 0
        shardConsumerTask = Task {

            for await shard in shardStream {
                guard !Task.isCancelled else { break }

                // Skip shards that overlap with the initial decode at all.
                // A shard starting at 2:00 with the initial covering 0:00-2:23
                // would produce duplicate transcript for the 2:00-2:23 range.
                if shard.startTime < initialCoverageEnd {
                    continue
                }

                self.streamingShardsEmitted += 1

                // Append to activeShards for the coordinator's state.
                if self.activeShards != nil {
                    self.activeShards?.append(shard)
                } else {
                    self.activeShards = [shard]
                }

                // Feed each shard to the transcript engine immediately.
                let snapshot = self.latestSnapshot ?? PlaybackSnapshot(
                    playheadTime: 0, playbackRate: 1.0, isPlaying: true
                )
                await self.transcriptEngine.appendShards(
                    [shard],
                    analysisAssetId: assetId,
                    snapshot: snapshot
                )

                // Extract features for this shard.
                do {
                    try await self.featureService.extractAndPersist(
                        shards: [shard],
                        analysisAssetId: assetId,
                        existingCoverage: shard.startTime
                    )
                } catch is CancellationError {
                    break
                } catch {
                    self.logger.error("Feature extraction failed for streaming shard \(shard.id): \(error)")
                }
            }

            await decoder.cleanup()
            self.activeDecoder = nil
            self.logger.info("Streaming decode complete: \(self.streamingShardsEmitted) shards emitted")
        }
    }

    // MARK: - Pipeline Control

    private func cancelPipeline() async {
        pipelineTask?.cancel()
        pipelineTask = nil
        transcriptEventTask?.cancel()
        transcriptEventTask = nil
        streamingDecodeTask?.cancel()
        streamingDecodeTask = nil
        shardConsumerTask?.cancel()
        shardConsumerTask = nil
        await transcriptEngine.stop()
        if let decoder = activeDecoder {
            activeDecoder = nil
            await decoder.cleanup()
        }
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
            let persistedChunks = try await store.fetchTranscriptChunks(assetId: assetId)
            let hotPathChunks = await adDetectionService.hotPathReplayContextChunks(
                from: persistedChunks,
                around: fastChunks
            )
            guard !hotPathChunks.isEmpty else { return }

            let result = try await adDetectionService.runHotPathResult(
                chunks: hotPathChunks,
                analysisAssetId: assetId,
                episodeDuration: currentEpisodeDuration(),
                retireUnmatchedReplayCandidates: true
            )
            if !result.retiredWindowIDs.isEmpty {
                await skipOrchestrator.retireAdWindows(ids: result.retiredWindowIDs)
            }
            let windows = result.windows
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
            // Cycle 4 H5: thread the sessionId through so the shadow FM
            // phase can stamp `needsShadowRetry` on this exact session if
            // the FM capability is unavailable. Captures the sessionId at
            // dispatch time, before any concurrent reprocessing on the
            // same asset could create a newer session and race the
            // marker. See `AdDetectionService.runShadowFMPhase` for the
            // override-only resolution rule.
            try await adDetectionService.runBackfill(
                chunks: allChunks,
                analysisAssetId: assetId,
                podcastId: podcastId,
                episodeDuration: currentEpisodeDuration(),
                sessionId: sessionId
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
