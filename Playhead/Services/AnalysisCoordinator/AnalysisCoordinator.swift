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
///
/// playhead-gtt9.8: expanded the monolithic `.complete` into three
/// distinguishable completion terminals (`completeFull`,
/// `completeFeatureOnly`, `completeTranscriptPartial`) plus richer
/// failure terminals (`failedTranscript`, `failedFeature`,
/// `cancelledBudget`) and a non-terminal `waitingForBackfill` that
/// replaces the prior "stay in hotPathReady under thermal pressure"
/// behavior. The new terminals let the harness compute
/// `scoredCoverageRatio` directly from `analysisState` without having
/// to reverse-engineer it from coverage watermarks. See
/// `docs/narl/2026-04-23-expert-response.md` §1.
enum SessionState: String, Sendable, CaseIterable {
    /// Audio identified, waiting for cached audio to become available.
    case queued
    /// Audio caching in progress, decode starting.
    case spooling
    /// Feature windows extracted for the hot zone around the playhead.
    case featuresReady
    /// Hot-path ad detection complete, skip cues available.
    case hotPathReady
    /// playhead-gtt9.8: non-terminal holding state between the hot-path
    /// and the backfill drain. Replaces the prior implicit "park on
    /// hotPathReady under thermal pressure" behavior so the reducer can
    /// tell a thermal-paused session apart from one that simply hasn't
    /// advanced yet.
    case waitingForBackfill
    /// Final-pass ASR and metadata extraction running.
    case backfill
    /// Deprecated monolithic completion (pre-gtt9.8). Kept only so
    /// legacy persisted rows decode on migrate. `finalizeBackfill` no
    /// longer writes this case — new sessions always resolve to one
    /// of the three richer `complete*` terminals below.
    case complete
    /// playhead-gtt9.8: feature scoring covered the intended audio
    /// range, decision windows were emitted over it, AND transcript
    /// covered at or above the finalize threshold (i.e. `completeFull`
    /// implies the coverage invariant passes).
    case completeFull
    /// playhead-gtt9.8: feature covered but transcript never advanced
    /// beyond preview. Eligible for FM-shadow runs once transcript is
    /// backfilled; not promotion-eligible for transcript-anchored
    /// decisions.
    case completeFeatureOnly
    /// playhead-gtt9.8: transcript advanced but fell short of the
    /// finalize coverage ratio. Scoring ran on the partial range; the
    /// unscored tail is preserved in telemetry for retry.
    case completeTranscriptPartial
    /// Deprecated monolithic failure (pre-gtt9.8). Kept only so legacy
    /// persisted rows decode on migrate. Stays as a catch-all for
    /// unclassified failures routed from legacy paths.
    case failed
    /// playhead-gtt9.8: transcript pipeline failed (SpeechAnalyzer
    /// refusal, ASR timeout, decode error on transcript chunks).
    case failedTranscript
    /// playhead-gtt9.8: feature-extraction pipeline failed (audio
    /// decode, feature-extractor crash).
    case failedFeature
    /// playhead-gtt9.8: session was halted because it exceeded the
    /// budget for its analysis class (transcript-budget-exceeded,
    /// app-backgrounded beyond the grace window, etc). Distinct from
    /// `.failed` so the harness doesn't double-count as a detector
    /// miss — see `docs/narl/2026-04-23-expert-response.md` §10.
    case cancelledBudget

    /// Valid successor states from each state.
    var validTransitions: Set<SessionState> {
        switch self {
        case .queued:
            return [.spooling, .failed, .failedTranscript, .failedFeature, .cancelledBudget]
        case .spooling:
            return [.featuresReady, .failed, .failedFeature, .cancelledBudget]
        case .featuresReady:
            return [.hotPathReady, .failed, .failedFeature, .cancelledBudget]
        case .hotPathReady:
            return [.waitingForBackfill, .backfill, .failed, .failedTranscript, .failedFeature, .cancelledBudget]
        case .waitingForBackfill:
            return [.backfill, .cancelledBudget, .failed, .failedTranscript, .failedFeature]
        case .backfill:
            return [.completeFull, .completeFeatureOnly, .completeTranscriptPartial,
                    .failedTranscript, .failedFeature, .failed, .cancelledBudget]
        case .complete, .completeFull, .completeFeatureOnly, .completeTranscriptPartial:
            return [.queued] // recovery: re-run if no data
        case .failed, .failedTranscript, .failedFeature, .cancelledBudget:
            return [.queued] // retry
        }
    }

    func canTransition(to next: SessionState) -> Bool {
        validTransitions.contains(next)
    }

    /// playhead-gtt9.8: convenience classifier — the three new
    /// completion terminals plus the legacy `.complete`. Used by the
    /// reducer, invariant layer, and coverage-guard recovery sweep to
    /// decide "is this session definitely done?" without enumerating
    /// cases at every site.
    var isTerminalCompletion: Bool {
        switch self {
        case .complete, .completeFull, .completeFeatureOnly, .completeTranscriptPartial:
            return true
        case .queued, .spooling, .featuresReady, .hotPathReady,
             .waitingForBackfill, .backfill,
             .failed, .failedTranscript, .failedFeature, .cancelledBudget:
            return false
        }
    }

    /// playhead-gtt9.8: convenience classifier — any failure or
    /// cancellation terminal.
    var isTerminalFailure: Bool {
        switch self {
        case .failed, .failedTranscript, .failedFeature, .cancelledBudget:
            return true
        case .queued, .spooling, .featuresReady, .hotPathReady,
             .waitingForBackfill, .backfill,
             .complete, .completeFull, .completeFeatureOnly, .completeTranscriptPartial:
            return false
        }
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

    /// playhead-o45p: optional observer that routes session-state
    /// transitions through `EpisodeSurfaceStatusReducer` so
    /// `ready_entered` events fire on the analysis-completion edge. Held
    /// nullably so existing tests that construct the coordinator
    /// directly (without the observer) keep working unchanged.
    private let surfaceStatusObserver: EpisodeSurfaceStatusObserver?

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
    /// playhead-gtt9.1.1: cached `analysis_assets.episodeDurationSec`
    /// for the current session. Loaded from the store on spool entry
    /// and on resume-from-persisted-`.backfill`; mirrored into the
    /// store at spool time via ``AnalysisStore/updateEpisodeDuration``.
    ///
    /// Used by ``currentEpisodeDuration()`` when `activeShards` is
    /// unavailable (which is the norm on resume paths: `activeShards`
    /// is only populated inside `runFromSpooling` and is never
    /// rehydrated when the pipeline restarts directly into backfill).
    /// See ``resolveEpisodeDuration(activeShards:persistedDuration:)``
    /// for the precedence rules.
    private var cachedPersistedEpisodeDuration: Double?
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

    /// Minimum fraction of `episodeDuration` that the transcript must
    /// cover before `finalizeBackfill` may transition the session to
    /// `.complete`. If coverage falls short we transition to `.failed`
    /// so the scheduler re-queues the job; better to retry on a future
    /// session than stamp `analysisState=complete` on a partial result.
    ///
    /// Tuning note: 0.95 was picked to tolerate the few-seconds tail
    /// that a decoder can chop off the very end of an episode (trailing
    /// silence, unfinished frames) while still catching the order-of-
    /// magnitude shortfalls we saw in production (e.g. 689s of
    /// coverage on a 3600s episode, ratio 0.19).
    static let finalizeBackfillMinCoverageRatio: Double = 0.95

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
        downloadManager: DownloadManager? = nil,
        surfaceStatusObserver: EpisodeSurfaceStatusObserver? = nil
    ) {
        self.store = store
        self.audioService = audioService
        self.featureService = featureService
        self.transcriptEngine = transcriptEngine
        self.capabilitiesService = capabilitiesService
        self.adDetectionService = adDetectionService
        self.skipOrchestrator = skipOrchestrator
        self.downloadManager = downloadManager
        self.surfaceStatusObserver = surfaceStatusObserver
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
        cachedPersistedEpisodeDuration = nil
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

    // MARK: - Foreground-Assist Hand-off (playhead-44h1)

    /// In-memory record of an active foreground-assist hand-off
    /// request. The coordinator flips `paused` true when
    /// `pauseAtNextCheckpoint(episodeId:cause:)` fires; the running
    /// continuation work observes the flag at its next
    /// `renewLease`-style safe point and returns.
    ///
    /// Tests can read `foregroundAssistPauseRequest(for:)` to assert
    /// the cause propagated without inspecting private actor state.
    struct ForegroundAssistPauseRequest: Sendable, Equatable {
        let cause: InternalMissCause
        let requestedAt: Date
    }

    /// Map of episode → most recent pause request. Bounded by the set
    /// of live continuation requests; entries are cleared when the
    /// corresponding `continueForegroundAssist` returns.
    private var foregroundAssistPauseRequests: [String: ForegroundAssistPauseRequest] = [:]

    /// Read-accessor for tests and diagnostics. Returns the most recent
    /// pause request for `episodeId`, or nil when none is pending.
    func foregroundAssistPauseRequest(for episodeId: String) -> ForegroundAssistPauseRequest? {
        foregroundAssistPauseRequests[episodeId]
    }

    /// playhead-44h1: entry point invoked by
    /// `BackgroundProcessingService.handleContinuedProcessingTask` after
    /// the app has backgrounded with an in-flight foreground-assist
    /// transfer + analysis. The caller hands control here so the
    /// coordinator can complete the remaining transfer under the
    /// `BGContinuedProcessingTask`'s 15–30 min window.
    ///
    /// Phase 1 scope boundary: the scheduler + download-manager wiring
    /// that drives this call lands in playhead-iwiy ("integration in
    /// playhead-iwiy — out of scope for this bead"). This
    /// implementation is the API shape used by
    /// `BackgroundProcessingService` and tests; it polls its own
    /// pause-request map and exits early when a
    /// `pauseAtNextCheckpoint` arrives, which pins the expiration
    /// contract without duplicating scheduler internals.
    ///
    /// Throws on any pipeline failure. The caller (BPS) maps the throw
    /// to `setTaskCompleted(success: false)`.
    ///
    /// - Parameters:
    ///   - episodeId: Episode whose lease to continue. Parsed from the
    ///     `BGContinuedProcessingTask.identifier` wildcard suffix.
    ///   - deadline: Wall-clock deadline the coordinator must respect.
    ///     Derived from `BackgroundProcessingService`'s budget constant,
    ///     not from the OS (`BGContinuedProcessingTask` has no
    ///     `expirationDate`). The `expirationHandler` still bounds the
    ///     actual runtime; the deadline is the soft gate.
    func continueForegroundAssist(episodeId: String, deadline: Date) async throws {
        logger.info("continueForegroundAssist: episode=\(episodeId, privacy: .public) deadline=\(deadline)")

        // Clear any stale pause request from a prior hand-off so the
        // poll below does not short-circuit on an old cause.
        foregroundAssistPauseRequests.removeValue(forKey: episodeId)

        defer {
            foregroundAssistPauseRequests.removeValue(forKey: episodeId)
        }

        // Integration wiring lands in playhead-iwiy. For now the
        // method polls its own pause-request map so the
        // `expirationHandler` contract (pause→fail) is exercisable
        // end-to-end by tests: BPS invokes
        // `pauseAtNextCheckpoint(episodeId:cause:)`, the flag flips,
        // the next iteration of this loop exits, BPS calls
        // `setTaskCompleted(success: false)`. Production will replace
        // the poll with the `AnalysisWorkScheduler`'s real drive
        // loop.
        while !Task.isCancelled {
            if foregroundAssistPauseRequests[episodeId] != nil {
                logger.info("continueForegroundAssist: pause observed for \(episodeId, privacy: .public)")
                return
            }
            if Date() >= deadline {
                logger.info("continueForegroundAssist: deadline reached for \(episodeId, privacy: .public)")
                return
            }
            do {
                try await Task.sleep(for: .milliseconds(50))
            } catch {
                // Cancellation raised by BPS on expiration — fall
                // through to the loop guard and exit cleanly.
                return
            }
        }
    }

    /// playhead-44h1: request the running worker for `episodeId` pause
    /// at its next safe checkpoint. Called from the continued-processing
    /// task's `expirationHandler` so the worker's shard / chunk loop
    /// flushes state, releases the lease with `event=.preempted`, and
    /// exits before iOS forcibly terminates the window.
    ///
    /// Phase 1 scope boundary: the LanePreemptionCoordinator hook
    /// sites are wired by the scheduler, not this coordinator. This
    /// implementation records the request in-memory so
    /// `continueForegroundAssist` can exit promptly; the full
    /// WorkJournal `preempted` append lands in playhead-iwiy where
    /// the running lease is actually accessible.
    ///
    /// The `cause` parameter is threaded through so downstream code
    /// (and tests) can verify the correct `InternalMissCause`
    /// (`.taskExpired` for OS expirations, `.userPreempted` for user
    /// preemptions) was recorded.
    func pauseAtNextCheckpoint(episodeId: String, cause: InternalMissCause) async {
        logger.info(
            "pauseAtNextCheckpoint: episode=\(episodeId, privacy: .public) cause=\(cause.rawValue, privacy: .public)"
        )
        foregroundAssistPauseRequests[episodeId] = ForegroundAssistPauseRequest(
            cause: cause,
            requestedAt: Date()
        )
    }

    /// playhead-44h1 (fix): append a terminal WorkJournal row for the
    /// foreground-assist hand-off. See the protocol doc-comment on
    /// ``AnalysisCoordinating/recordForegroundAssistOutcome`` for
    /// semantics and the two BPS call-sites.
    ///
    /// Resolution:
    ///   - Look up the episode's most-recently-updated `analysis_jobs`
    ///     row to capture the live `{generationID, schedulerEpoch}`.
    ///   - When no row is found, log a warning and SKIP the append.
    ///     Rationale (review fix): writing a row with a freshly-minted
    ///     UUID that never joined to a real job row produces an orphan
    ///     WorkJournal entry that nothing downstream can reconcile —
    ///     worse than no row at all for support triage. The miss path
    ///     is already anomalous (a continued-processing task fired
    ///     for an episode with no job) and deserves a warning, not a
    ///     ghost row. If a future spec revision needs a durable miss
    ///     record, extend `SliceMetadata` with a `wasOrphan` flag
    ///     rather than re-enabling the UUID regeneration here.
    ///
    /// Errors from the store are logged and swallowed: the caller is
    /// the BG task expiration / completion path, which has no
    /// recourse. A logged warning is strictly less bad than a raised
    /// error here, which would otherwise have to propagate into
    /// `setTaskCompleted(success:)`.
    func recordForegroundAssistOutcome(
        episodeId: String,
        eventType: WorkJournalEntry.EventType,
        cause: InternalMissCause?
    ) async {
        do {
            guard let job = try await store.fetchLatestJobForEpisode(episodeId) else {
                logger.warning(
                    "recordForegroundAssistOutcome: no analysis_jobs row for episode=\(episodeId, privacy: .public) event=\(eventType.rawValue, privacy: .public); skipping journal append to avoid orphan UUID"
                )
                return
            }
            guard let generationUUID = UUID(uuidString: job.generationID) else {
                logger.warning(
                    "recordForegroundAssistOutcome: non-UUID generationID=\(job.generationID, privacy: .public) for episode=\(episodeId, privacy: .public); skipping journal append to avoid orphan UUID"
                )
                return
            }
            let entry = WorkJournalEntry(
                id: UUID().uuidString,
                episodeId: episodeId,
                generationID: generationUUID,
                schedulerEpoch: job.schedulerEpoch,
                timestamp: Date().timeIntervalSince1970,
                eventType: eventType,
                cause: cause,
                metadata: "{}",
                artifactClass: .scratch
            )
            try await store.appendWorkJournalEntry(entry)
            logger.info(
                "recordForegroundAssistOutcome: episode=\(episodeId, privacy: .public) event=\(eventType.rawValue, privacy: .public) cause=\(cause?.rawValue ?? "nil", privacy: .public)"
            )
        } catch {
            logger.error(
                "recordForegroundAssistOutcome failed for episode \(episodeId, privacy: .public): \(String(describing: error), privacy: .public)"
            )
        }
    }

    // MARK: - Episode Execution Lease (playhead-uzdq)

    /// Default lease TTL. Long enough that a cooperatively-scheduled
    /// owner can take a full scheduling slice before the next renewal
    /// cycle, short enough that a crashed owner's lease releases within
    /// the BG task's lifetime.
    private static let defaultLeaseTTLSeconds: Double = 30

    /// Acquires a single-writer lease on `episodeId`. See
    /// ``EpisodeExecutionLease`` for semantics and the bead spec for
    /// the contract summary.
    ///
    /// On success the returned lease carries a freshly-minted
    /// `generationID` (UUID). The owner passes this lease back on every
    /// renew / release / finalize call so the store can reject late
    /// callbacks (generationMismatch) or stale workers (staleEpoch).
    func acquireLease(
        episodeId: String,
        ownerWorkerId: String,
        schedulerEpoch: Int,
        ttlSeconds: Double = AnalysisCoordinator.defaultLeaseTTLSeconds,
        now: Double = Date().timeIntervalSince1970
    ) async throws -> EpisodeExecutionLease {
        let generationID = UUID()
        let descriptor = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: ownerWorkerId,
            generationID: generationID.uuidString,
            schedulerEpoch: schedulerEpoch,
            now: now,
            ttlSeconds: ttlSeconds
        )
        return EpisodeExecutionLease(
            episodeId: descriptor.episodeId,
            ownerWorkerId: descriptor.ownerWorkerId,
            generationID: generationID,
            schedulerEpoch: descriptor.schedulerEpoch,
            acquiredAt: descriptor.acquiredAt,
            expiresAt: descriptor.expiresAt,
            currentCheckpoint: nil,
            preemptionRequested: false
        )
    }

    /// Extends the lease TTL. Caller-visible effect is only to push
    /// `expiresAt` forward; the persisted row's `leaseExpiresAt` moves
    /// with it. Rejected with `LeaseError.staleEpoch` when the caller's
    /// epoch is older than the store's current epoch, and with
    /// `LeaseError.generationMismatch` when the row's generation has
    /// rotated underneath the caller (e.g. orphan recovery requeued).
    func renewLease(
        _ lease: EpisodeExecutionLease,
        ttlSeconds: Double = AnalysisCoordinator.defaultLeaseTTLSeconds,
        now: Double = Date().timeIntervalSince1970
    ) async throws {
        try await store.renewEpisodeLease(
            episodeId: lease.episodeId,
            generationID: lease.generationID.uuidString,
            schedulerEpoch: lease.schedulerEpoch,
            newExpiresAt: now + ttlSeconds,
            now: now
        )
    }

    /// Releases the lease and records a terminal event in the
    /// WorkJournal. Idempotent for `.finalized` and `.failed`:
    /// a second call on the same {episodeId, generationID, event}
    /// tuple is a no-op (no duplicate journal row, no error).
    func releaseLease(
        _ lease: EpisodeExecutionLease,
        event: WorkJournalEntry.EventType,
        cause: InternalMissCause? = nil,
        now: Double = Date().timeIntervalSince1970
    ) async throws {
        try await store.releaseEpisodeLease(
            episodeId: lease.episodeId,
            generationID: lease.generationID.uuidString,
            schedulerEpoch: lease.schedulerEpoch,
            eventType: event,
            cause: cause,
            now: now
        )
    }

    /// Cold-launch orphan recovery. Scans `analysis_jobs` for held
    /// leases whose `expiresAt` is more than the grace window in the
    /// past and, for each, inspects the last `work_journal` entry to
    /// decide how to reconcile:
    ///
    /// - terminal (`finalized` / `failed`) → clear lease slot, no
    ///   requeue.
    /// - `checkpointed` (or `acquired` with no progress) → requeue
    ///   with a fresh generationID, bumped epoch, attemptCount=0, and
    ///   the lane-preserved priority. Now-lane rows stale for >60 s
    ///   demote to Soon (priority 10).
    ///
    /// Returns the list of freshly-rebuilt leases for the caller to
    /// log / reschedule.
    ///
    /// The bead's "work_journal with scheduler_epoch > _meta.scheduler_epoch
    /// is corruption" rule is a reconciliation invariant: we log the
    /// condition and drop the offending entry's effect on the decision
    /// (treat as "no last event found", which routes to the
    /// no-progress / requeue path).
    @discardableResult
    func recoverOrphans(
        now: Double = Date().timeIntervalSince1970,
        graceSeconds: Double = 10
    ) async throws -> [EpisodeExecutionLease] {
        let stale = try await store.fetchEpisodesWithExpiredLeases(
            now: now,
            graceSeconds: graceSeconds
        )
        guard !stale.isEmpty else { return [] }

        var rebuilt: [EpisodeExecutionLease] = []
        let currentEpoch = try await store.fetchSchedulerEpoch() ?? 0

        for job in stale {
            let lastEvent: WorkJournalEntry?
            do {
                lastEvent = try await store.fetchLastWorkJournalEntry(
                    episodeId: job.episodeId,
                    generationID: job.generationID
                )
            } catch {
                // A single corrupt journal row must not abort the
                // entire orphan sweep; log and skip this episode so
                // the rest still recover.
                logger.error("orphan_recover.fetchJournal_failed episode=\(job.episodeId) error=\(String(describing: error))")
                continue
            }

            // Spec invariant: journal rows with scheduler_epoch >
            // _meta.scheduler_epoch are corruption. The previous
            // implementation dropped the effect and fell into the
            // resume branch — which would *redo* finalized work if the
            // corrupted row was `.finalized`. Conservative recovery:
            // skip the episode entirely so a human can investigate.
            // The lease slot stays orphaned; the next sweep will retry.
            if let last = lastEvent, last.schedulerEpoch > currentEpoch {
                logger.error("work_journal.schedulerEpoch=\(last.schedulerEpoch) > _meta=\(currentEpoch) for episode \(job.episodeId); skipping recovery to avoid redoing terminal work")
                continue
            }
            let decisionEvent = lastEvent?.eventType

            do {
                switch decisionEvent {
                case .finalized, .failed:
                    // Already terminal — clear lease slot and move on.
                    try await store.clearOrphanedLeaseNoRequeue(
                        jobId: job.jobId,
                        now: now
                    )

                case .checkpointed, .acquired, .preempted, .none:
                    // Resume path: requeue with fresh identity under a
                    // freshly-bumped scheduler epoch.
                    if decisionEvent == .acquired || decisionEvent == nil {
                        logger.notice("orphan_recovered_no_progress episode=\(job.episodeId)")
                    }
                    let newEpoch = try await store.incrementSchedulerEpoch()
                    let newGenerationID = UUID()

                    // Lane preservation: Now-lane (priority >= 10) rows
                    // whose lease expired >60 s ago demote to Soon
                    // (priority = 10 floor). All others keep their band.
                    let staleSeconds = max(0.0, now - (job.leaseExpiresAt ?? now))
                    let newPriority = Self.laneAfterOrphan(
                        currentPriority: job.priority,
                        staleSeconds: staleSeconds
                    )

                    try await store.requeueOrphanedLease(
                        jobId: job.jobId,
                        newGenerationID: newGenerationID.uuidString,
                        newSchedulerEpoch: newEpoch,
                        newPriority: newPriority,
                        now: now
                    )
                    rebuilt.append(
                        EpisodeExecutionLease(
                            episodeId: job.episodeId,
                            ownerWorkerId: "",
                            generationID: newGenerationID,
                            schedulerEpoch: newEpoch,
                            acquiredAt: now,
                            expiresAt: now,
                            currentCheckpoint: nil,
                            preemptionRequested: false
                        )
                    )
                }
            } catch {
                // One job's DB error must not abort the sweep — the
                // orphan stays orphaned (next sweep will retry) but
                // other recoverable orphans still recover.
                logger.error("orphan_recover.perJob_failed episode=\(job.episodeId) error=\(String(describing: error))")
                continue
            }
        }
        return rebuilt
    }

    /// Priority value for a requeued orphan that was demoted out of
    /// the Now lane. Matches the bead's "priority → 10" spec and lines
    /// up with the explicit-download priority used by
    /// ``AnalysisWorkScheduler/enqueue``.
    static let soonLaneOrphanPriority: Int = 10

    /// Cutoff below which a Now-lane row still belongs in the Now lane
    /// and above which orphan recovery demotes it to Soon.
    ///
    /// The threshold is exclusive ("> 60 s ago") so a lease that
    /// expired exactly 60 s ago stays in its Now lane; anything older
    /// demotes.
    static let nowLaneOrphanDemotionSeconds: Double = 60

    /// Lane-preservation rule from the bead's orphan requeue policy:
    /// same priority band, EXCEPT Now lane (priority strictly greater
    /// than Soon=10) leases stale for more than
    /// ``nowLaneOrphanDemotionSeconds`` demote to Soon.
    ///
    /// "Now lane" is defined here as priority > `soonLaneOrphanPriority`
    /// (= 10). `AnalysisWorkScheduler.enqueue` currently assigns
    /// explicit downloads priority=10 (Soon) and auto-downloads
    /// priority=0 (Background). Now-lane priorities (e.g. >= 20) are
    /// reserved for future playback / hot-path escalations and are the
    /// only band this rule demotes.
    static func laneAfterOrphan(currentPriority: Int, staleSeconds: Double) -> Int {
        if currentPriority > soonLaneOrphanPriority
           && staleSeconds > nowLaneOrphanDemotionSeconds {
            return soonLaneOrphanPriority
        }
        return currentPriority
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
            case .waitingForBackfill:
                // playhead-gtt9.8: resume parks the session at the same
                // stage as `hotPathReady` did pre-gtt9.8. The coordinator
                // transitions into backfill once thermal/budget signals
                // allow; the entry point is the same drain loop.
                try await runFromHotPathReady(sessionId: sessionId, assetId: assetId)
            case .backfill:
                try await runFromBackfill(sessionId: sessionId, assetId: assetId)
            case .complete, .completeFull, .completeFeatureOnly, .completeTranscriptPartial:
                // playhead-gtt9.8: any terminal completion state verifies
                // the session actually has transcript data. A crash
                // during backfill can leave the session as "complete"
                // with no chunks. Restart from queued to re-decode audio
                // (shards aren't in memory across app launches).
                let chunks = try await store.fetchTranscriptChunks(assetId: assetId)
                if chunks.isEmpty {
                    logger.info("Session \(sessionId) marked \(resumeState.rawValue) but has 0 chunks — restarting from queued")
                    try await transition(sessionId: sessionId, assetId: assetId, to: .queued)
                    try await runFromQueued(sessionId: sessionId, assetId: assetId, episodeId: episodeId, audioURL: audioURL)
                } else {
                    logger.info("Session \(sessionId) already \(resumeState.rawValue) (\(chunks.count) chunks)")
                }
            case .failed, .failedTranscript, .failedFeature, .cancelledBudget:
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
        cachedPersistedEpisodeDuration = nil
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

        // playhead-gtt9.1.1: persist the shard-sum duration onto the
        // `analysis_assets` row so the coverage guard has a durable
        // denominator if the pipeline is restarted directly into
        // `.backfill` (where `activeShards` is never rehydrated). We
        // also cache it locally so `currentEpisodeDuration()` has a
        // fallback when `activeShards` is later cleared.
        if totalAudio > 0 {
            do {
                try await store.updateEpisodeDuration(
                    id: assetId,
                    episodeDurationSec: totalAudio
                )
                cachedPersistedEpisodeDuration = totalAudio
            } catch {
                // A failed duration write is not fatal to the pipeline
                // — `activeShards` is still authoritative for the
                // in-memory guard. Log and continue; the only cost is
                // that if the process is killed before another write,
                // the resume guard will fail safe (gtt9.1.1 intent).
                logger.warning("Failed to persist episodeDurationSec=\(totalAudio) for asset \(assetId): \(error)")
            }
        }

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
            // playhead-gtt9.1.1: hydrate the persisted duration cache
            // before consulting `currentEpisodeDuration()`. On this
            // path `activeShards` is never populated (the spool that
            // set it ran in a previous process), so the resolver must
            // fall back to the row's `episodeDurationSec`. Legacy
            // rows predating the column return nil and fall through
            // to the fail-safe `.restart` shortcut.
            if cachedPersistedEpisodeDuration == nil {
                do {
                    if let asset = try await store.fetchAsset(id: assetId) {
                        cachedPersistedEpisodeDuration = asset.episodeDurationSec
                    }
                } catch {
                    logger.warning("Failed to load persisted episodeDurationSec for asset \(assetId): \(error)")
                }
            }
            let chunks = try await store.fetchTranscriptChunks(assetId: assetId)
            let episodeDuration = currentEpisodeDuration()
            switch Self.resumeBackfillDecision(chunks: chunks, episodeDuration: episodeDuration) {
            case .restart:
                let coverageEnd = chunks.map(\.endTime).max() ?? 0
                logger.info(
                    "Backfill resumed with partial coverage \(String(format: "%.1f", coverageEnd))/\(String(format: "%.1f", episodeDuration))s — requesting full restart"
                )
                throw AnalysisCoordinatorError.noAudioAvailable(episodeId: activeEpisodeId ?? "unknown")
            case .finalize:
                try await finalizeBackfill(sessionId: sessionId, assetId: assetId)
                return
            }
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

        // playhead-o45p: on the analysis-completion edge, route the
        // episode through the surface-status observer so `ready_entered`
        // fires once (analysisCompleted trigger) for the false_ready_rate
        // dogfood metric's numerator/denominator. Best-effort —
        // failures inside the observer are logged there and swallowed;
        // the analysis pipeline's correctness never depends on this.
        if newState == .complete,
           let observer = surfaceStatusObserver,
           let episodeId = activeEpisodeId
        {
            await observer.observeAnalysisSessionComplete(episodeId: episodeId)
        }
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

            // The decoder closed its shard stream, so no more shards
            // will arrive from this streaming session. Tell the
            // transcript engine so it can drain remaining backlog and
            // emit `.completed`. Without this the engine would park
            // forever on its append-waiter and `.completed` would never
            // fire, leaving `finalizeBackfill` unreachable — or (before
            // the fix) the engine would race to `.completed` on a
            // momentarily empty queue mid-stream.
            await self.transcriptEngine.finishAppending(analysisAssetId: assetId)

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

        case .hotPathReady, .waitingForBackfill:
            // playhead-gtt9.8: `.waitingForBackfill` is the explicit
            // "thermal-paused between hot-path and backfill" state; it
            // advances to `.backfill` the same way `.hotPathReady` does
            // once the pipeline is allowed to drain.
            try await transition(sessionId: sessionId, assetId: assetId, to: .backfill)

        case .backfill:
            break

        case .complete, .completeFull, .completeFeatureOnly, .completeTranscriptPartial,
             .failed, .failedTranscript, .failedFeature, .cancelledBudget,
             .queued, .spooling:
            return
        }

        // Coverage guard: refuse to mark `.complete` when the transcript
        // covers less than `finalizeBackfillMinCoverageRatio` of the
        // episode. This defends against upstream races (e.g. streaming
        // decoder finishing before the engine has transcribed every
        // shard) that would otherwise stamp `analysisState=complete` on
        // a heavily partial result. When the guard fires we transition
        // to `.failed` with a descriptive reason; the scheduler will
        // re-queue the job on a future session.
        let episodeDuration = currentEpisodeDuration()
        let verdict = Self.finalizeBackfillVerdict(
            chunks: allChunks,
            episodeDuration: episodeDuration
        )
        switch verdict {
        case .blockComplete(let coverageEnd, let duration, let ratio):
            let reason = String(
                format: "transcript coverage %.1f/%.1fs (ratio %.3f < %.3f)",
                coverageEnd, duration, ratio, Self.finalizeBackfillMinCoverageRatio
            )
            logger.warning(
                "Coverage guard BLOCKED .complete for asset \(assetId): \(reason) — transitioning to .failed for retry"
            )
            try await transition(
                sessionId: sessionId,
                assetId: assetId,
                to: .failed,
                failureReason: reason
            )
            return
        case .allowComplete:
            break
        }

        try await transition(sessionId: sessionId, assetId: assetId, to: .complete)
        logger.info("Analysis complete for asset \(assetId)")
    }

    /// Decision returned by ``finalizeBackfillVerdict(chunks:episodeDuration:)``.
    /// Static enum so it can be referenced from tests without depending on
    /// the coordinator's actor isolation.
    enum FinalizeBackfillVerdict: Equatable {
        /// Coverage is sufficient — proceed to transition `.complete`.
        case allowComplete
        /// Coverage is below the required ratio. The backfill finalizer
        /// transitions to `.failed` instead of `.complete` with the
        /// attached diagnostic numbers.
        case blockComplete(coverageEnd: Double, episodeDuration: Double, ratio: Double)
    }

    /// Pure helper that decides whether `finalizeBackfill` may mark the
    /// session `.complete` given the persisted transcript chunks and the
    /// resolved episode duration. Extracted as a static function so it
    /// can be exercised by unit tests without instantiating the full
    /// coordinator graph (audio, feature, ad-detection, etc.).
    ///
    /// - When `episodeDuration <= 0` the helper returns `.blockComplete`
    ///   with the recorded coverage and zero denominators. We cannot
    ///   prove the transcript meets the ratio floor without a reliable
    ///   denominator, so the guard fails safe and blocks rather than
    ///   permissively allowing. See `playhead-gtt9.1.1` — the pre-fix
    ///   `.allowComplete` shortcut was the root cause of four real
    ///   episodes being stamped `analysisState='complete'` with only
    ///   90s of fast-pass transcript covering 1800-7000s of audio, because
    ///   `activeShards` was never rehydrated on resume paths.
    /// - Empty `chunks` with a known duration evaluates to
    ///   `coverageEnd == 0` and therefore `.blockComplete`.
    static func finalizeBackfillVerdict(
        chunks: [TranscriptChunk],
        episodeDuration: Double
    ) -> FinalizeBackfillVerdict {
        let coverageEnd = chunks.map(\.endTime).max() ?? 0
        guard episodeDuration > 0 else {
            // playhead-gtt9.1.1: fail-safe. We preserve `coverageEnd` so
            // the persisted `failureReason` still carries the observed
            // transcript extent for the coverage-guard recovery sweep,
            // even though the denominator is unknown (encoded as 0).
            return .blockComplete(
                coverageEnd: coverageEnd,
                episodeDuration: 0,
                ratio: 0
            )
        }

        let ratio = coverageEnd / episodeDuration
        if ratio + 1e-9 < finalizeBackfillMinCoverageRatio {
            return .blockComplete(
                coverageEnd: coverageEnd,
                episodeDuration: episodeDuration,
                ratio: ratio
            )
        }
        return .allowComplete
    }

    /// playhead-gtt9.1.1: resolver consulted by the coverage guards.
    /// Prefers the live in-memory `activeShards` (authoritative when
    /// the spool path has just decoded) and falls back to the value
    /// persisted on `analysis_assets.episodeDurationSec` — cached into
    /// ``cachedPersistedEpisodeDuration`` on resume so the guard does
    /// not hit the store synchronously.
    ///
    /// Returns 0 when both sources are missing: the guards treat 0 as
    /// "unknown denominator" and route to their fail-safe shortcuts.
    private func currentEpisodeDuration() -> Double {
        Self.resolveEpisodeDuration(
            activeShards: activeShards,
            persistedDuration: cachedPersistedEpisodeDuration
        )
    }

    /// playhead-gtt9.1.1: pure resolver for the coverage-guard
    /// denominator. Extracted as a static helper so unit tests can
    /// cover the ordering rules without constructing a coordinator.
    ///
    /// Rules:
    /// 1. If `activeShards` is non-nil AND non-empty, return the sum
    ///    of `startTime + duration` maxima (matches the pre-gtt9.1.1
    ///    behaviour of `currentEpisodeDuration()`).
    /// 2. Else if `persistedDuration` is > 0, return it.
    /// 3. Else return 0 ("unknown").
    ///
    /// Rule 1 wins over rule 2 even when the persisted value is
    /// present: during an active spool the shard sum is the freshest
    /// authority and the persisted row may be momentarily stale (the
    /// spool-time write happens just after decode).
    static func resolveEpisodeDuration(
        activeShards: [AnalysisShard]?,
        persistedDuration: Double?
    ) -> Double {
        if let shards = activeShards, !shards.isEmpty {
            return shards.reduce(0) { partial, shard in
                max(partial, shard.startTime + shard.duration)
            }
        }
        if let persistedDuration, persistedDuration > 0 {
            return persistedDuration
        }
        return 0
    }

    /// Decision returned by ``resumeBackfillDecision(chunks:episodeDuration:)``.
    enum ResumeBackfillDecision: Equatable {
        /// Persisted chunks cover enough of the episode — finalize normally.
        case finalize
        /// No chunks, or coverage is short of the finalize guard threshold.
        /// Callers should throw so the pipeline re-decodes from scratch.
        case restart
    }

    /// Pure helper for the resume-from-crash branch of `runFromBackfill`.
    /// Reuses the same ``finalizeBackfillMinCoverageRatio`` floor the
    /// finalize coverage guard uses, so an asset that would later be
    /// blocked by the finalize guard gets caught on resume and rebuilt
    /// from scratch instead of being stamped `.failed` while a parallel
    /// transcript backlog is still draining.
    ///
    /// - Empty chunks always return `.restart` — there is nothing to
    ///   finalize. This preserves the original `chunks.isEmpty` behaviour
    ///   even when `episodeDuration` is unknown.
    /// - Non-empty chunks with unknown `episodeDuration` (<= 0) return
    ///   `.restart` (playhead-gtt9.1.1 fail-safe). Pre-fix behaviour
    ///   returned `.finalize`, which then hit the finalize-time guard
    ///   and — before the guard was also hardened — routed to
    ///   `.allowComplete`, stamping assets complete with 90s of fast-pass
    ///   transcript. The safe fallback is "restart the pipeline to
    ///   re-decode audio so `activeShards` (and the cached persisted
    ///   duration) become available on the next pass."
    static func resumeBackfillDecision(
        chunks: [TranscriptChunk],
        episodeDuration: Double
    ) -> ResumeBackfillDecision {
        if chunks.isEmpty {
            return .restart
        }
        guard episodeDuration > 0 else {
            return .restart
        }
        let coverageEnd = chunks.map(\.endTime).max() ?? 0
        let ratio = coverageEnd / episodeDuration
        if ratio + 1e-9 < finalizeBackfillMinCoverageRatio {
            return .restart
        }
        return .finalize
    }

    // MARK: - Coverage-Guard Recovery Sweep

    /// Prefix used by the coverage guard when it transitions a session to
    /// `.failed`. The sweep below selects `.failed` sessions whose
    /// `failureReason` starts with this string and re-evaluates them
    /// against the latest transcript coverage.
    static let coverageGuardFailureReasonPrefix = "transcript coverage "

    /// Verdict returned by ``coverageGuardRecoveryVerdict(failureReason:coverageEnd:)``.
    /// Extracted as a pure enum so unit tests can drive the decision without
    /// touching the store.
    enum CoverageGuardRecoveryVerdict: Equatable {
        /// Recover the session: transcript coverage now meets the threshold.
        case recover(coverageEnd: Double, episodeDuration: Double, ratio: Double)
        /// Leave the session alone: coverage is still below threshold.
        case skipBelowThreshold(coverageEnd: Double, episodeDuration: Double, ratio: Double)
        /// Leave the session alone: the failure reason is not a coverage-guard
        /// failure (different prefix, or the encoded duration was unparseable).
        case skipUnrelated
    }

    /// Decide whether a session whose last-known `failureReason` came from
    /// the coverage guard should be requeued, based on the current
    /// transcript coverage.
    ///
    /// The episode duration comes out of the preserved failure-reason
    /// string (e.g. `"transcript coverage 689.8/3600.0s (ratio 0.192 <
    /// 0.950)"` → `3600.0`). We re-use that denominator rather than
    /// re-deriving it from shards because the sweep runs outside of an
    /// active pipeline where `activeShards` is unavailable.
    ///
    /// - Parameters:
    ///   - failureReason: The persisted `analysis_sessions.failureReason`.
    ///     Any string that does not start with
    ///     ``coverageGuardFailureReasonPrefix`` returns `.skipUnrelated`.
    ///   - coverageEnd: Highest `endTime` across the asset's transcript
    ///     chunks at the moment of the sweep.
    static func coverageGuardRecoveryVerdict(
        failureReason: String?,
        coverageEnd: Double
    ) -> CoverageGuardRecoveryVerdict {
        guard let failureReason,
              failureReason.hasPrefix(coverageGuardFailureReasonPrefix) else {
            return .skipUnrelated
        }
        guard let episodeDuration = parseCoverageGuardEpisodeDuration(from: failureReason),
              episodeDuration > 0 else {
            return .skipUnrelated
        }
        let ratio = coverageEnd / episodeDuration
        if ratio + 1e-9 >= finalizeBackfillMinCoverageRatio {
            return .recover(
                coverageEnd: coverageEnd,
                episodeDuration: episodeDuration,
                ratio: ratio
            )
        }
        return .skipBelowThreshold(
            coverageEnd: coverageEnd,
            episodeDuration: episodeDuration,
            ratio: ratio
        )
    }

    /// Parse the episode duration out of a coverage-guard failure reason.
    /// Returns `nil` when the string is not in the expected shape — the
    /// caller then leaves the session alone. Exposed internal so tests
    /// can exercise the parser independently.
    static func parseCoverageGuardEpisodeDuration(from failureReason: String) -> Double? {
        // Expected layout: "transcript coverage <cov>/<duration>s (...)".
        // Locate the `/` after the prefix, then the first `s` after that.
        guard failureReason.hasPrefix(coverageGuardFailureReasonPrefix) else { return nil }
        let body = failureReason.dropFirst(coverageGuardFailureReasonPrefix.count)
        guard let slashIdx = body.firstIndex(of: "/") else { return nil }
        let afterSlash = body[body.index(after: slashIdx)...]
        guard let sIdx = afterSlash.firstIndex(of: "s") else { return nil }
        return Double(afterSlash[..<sIdx])
    }

    /// Summary of a single recovery sweep. Returned so callers (and
    /// tests) can log or assert the outcome without inspecting store
    /// state directly.
    struct CoverageGuardRecoverySummary: Sendable, Equatable {
        /// Sessions that were flipped from `.failed` back to `.backfill`.
        var recoveredSessionIds: [String] = []
        /// Sessions inspected but left alone because coverage is still
        /// short of the threshold. Useful for diagnostics.
        var stillBelowThreshold: Int = 0
        /// Sessions skipped because their failure reason could not be
        /// parsed as a coverage-guard failure.
        var skippedUnrelated: Int = 0

        var recoveredCount: Int { recoveredSessionIds.count }
    }

    /// Scan the session table for rows stranded by the coverage guard and
    /// flip any whose transcript has since caught up back to `.backfill`
    /// so the next pipeline run can re-call ``finalizeBackfill``.
    ///
    /// The sweep intentionally bypasses the normal `transition(...)`
    /// validator because the persisted state machine does not permit a
    /// direct `.failed → .backfill` edge — `.failed` rows only flow back
    /// through `.queued`, which forces a redundant re-decode / re-
    /// transcription. Bypassing the validator here is safe because:
    ///
    ///   * The sweep only ever writes `.backfill` into rows it has
    ///     itself selected as `.failed` with the coverage-guard prefix.
    ///   * `runFromBackfill` handles the empty-chunks case by throwing,
    ///     which promotes the session back to `.failed` through the
    ///     normal error path — so a spurious recovery is self-healing.
    ///   * Ad windows (including user-marked `boundaryState = "userMarked"`
    ///     rows) live in a separate table and are never touched here.
    ///
    /// Returns a summary of what was touched. Errors from the store are
    /// logged and swallowed per-session so one bad row cannot sink the
    /// whole sweep.
    @discardableResult
    func recoverCoverageGuardFailures() async -> CoverageGuardRecoverySummary {
        var summary = CoverageGuardRecoverySummary()

        let candidates: [AnalysisSession]
        do {
            candidates = try await store.fetchFailedSessions(
                withFailureReasonPrefix: Self.coverageGuardFailureReasonPrefix
            )
        } catch {
            logger.warning(
                "Coverage-guard sweep: failed to enumerate candidate sessions: \(String(describing: error), privacy: .public)"
            )
            return summary
        }

        guard !candidates.isEmpty else {
            return summary
        }

        logger.info("Coverage-guard sweep: inspecting \(candidates.count) candidate session(s)")

        for session in candidates {
            let assetId = session.analysisAssetId
            let chunks: [TranscriptChunk]
            do {
                chunks = try await store.fetchTranscriptChunks(assetId: assetId)
            } catch {
                logger.warning(
                    "Coverage-guard sweep: chunk fetch failed for asset \(assetId, privacy: .public): \(String(describing: error), privacy: .public)"
                )
                continue
            }

            let coverageEnd = chunks.map(\.endTime).max() ?? 0
            let verdict = Self.coverageGuardRecoveryVerdict(
                failureReason: session.failureReason,
                coverageEnd: coverageEnd
            )

            switch verdict {
            case .recover(let cov, let dur, let ratio):
                do {
                    // Flip the session back to `.backfill` and clear the
                    // stored failure reason so the next pipeline run
                    // enters `runFromBackfill` and re-invokes
                    // `finalizeBackfill`.
                    try await store.updateSessionState(
                        id: session.id,
                        state: SessionState.backfill.rawValue,
                        failureReason: nil
                    )
                    try await store.updateAssetState(
                        id: assetId,
                        state: SessionState.backfill.rawValue
                    )
                    summary.recoveredSessionIds.append(session.id)
                    logger.info(
                        "Coverage-guard sweep: recovered session \(session.id, privacy: .public) (asset \(assetId, privacy: .public), coverage \(String(format: "%.1f", cov))/\(String(format: "%.1f", dur))s, ratio \(String(format: "%.3f", ratio)))"
                    )
                } catch {
                    logger.warning(
                        "Coverage-guard sweep: failed to requeue session \(session.id, privacy: .public): \(String(describing: error), privacy: .public)"
                    )
                }

            case .skipBelowThreshold:
                summary.stillBelowThreshold += 1

            case .skipUnrelated:
                summary.skippedUnrelated += 1
            }
        }

        if summary.recoveredCount > 0 {
            logger.info(
                "Coverage-guard sweep: recovered \(summary.recoveredCount) session(s); \(summary.stillBelowThreshold) still below threshold"
            )
        }

        return summary
    }

    // MARK: - Test seams (playhead-gtt9.1.1)

    #if DEBUG
    /// Test seam that mirrors the production resume-from-persisted-`.backfill`
    /// path without requiring audio, a download manager, or a
    /// playback snapshot.
    ///
    /// Production entry for this branch is `runPipeline` with
    /// `resumeState: .backfill`; that in turn invokes the private
    /// ``runFromBackfill`` and, when `transcriptEventTask == nil`,
    /// hits the resume decision we are trying to cover. Invoking
    /// `runPipeline` directly would require constructing a
    /// `LocalAudioURL`, which has no bearing on the guard under test
    /// (the resume branch never reaches audio-dependent code). This
    /// seam is the smallest reproduction of that path.
    ///
    /// Behaviour:
    /// - Sets the session-scoped state the way `runPipeline` does
    ///   (`activeSessionId`, `activeAssetId`, `activeEpisodeId`),
    ///   explicitly leaves `activeShards == nil` and
    ///   `transcriptEventTask == nil`.
    /// - Calls ``runFromBackfill`` inside the same do/catch the
    ///   production pipeline uses, so a thrown
    ///   `AnalysisCoordinatorError.noAudioAvailable` transitions
    ///   the session to `.failed` via ``transition``.
    ///
    /// Only compiled in DEBUG — not a public production API.
    func resumeBackfillForTesting(
        sessionId: String,
        assetId: String,
        episodeId: String
    ) async {
        activeSessionId = sessionId
        activeAssetId = assetId
        activeEpisodeId = episodeId
        // Intentionally do NOT populate `activeShards` or
        // `transcriptEventTask`: the point of this seam is to
        // exercise the resume branch of `runFromBackfill`.
        do {
            try await runFromBackfill(sessionId: sessionId, assetId: assetId)
        } catch is CancellationError {
            logger.info("resumeBackfillForTesting: cancelled for episode \(episodeId)")
        } catch {
            logger.info("resumeBackfillForTesting: pipeline failed: \(error)")
            try? await transition(
                sessionId: sessionId,
                assetId: assetId,
                to: .failed,
                failureReason: String(describing: error)
            )
        }
    }
    #endif
}
