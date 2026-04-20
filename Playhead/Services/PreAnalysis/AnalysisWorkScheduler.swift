// AnalysisWorkScheduler.swift
// Eligibility-aware job scheduler for pre-analysis work.
// Selects the highest-priority eligible job under the shared deferred-work
// admission policy and delegates execution to AnalysisJobRunner.

import Foundation
import OSLog

/// Hook the scheduler calls when admitting a `SchedulerLane.now` job so that a
/// later bead (playhead-01t8) can implement preemption of active Soon and
/// Background jobs at the next safe checkpoint boundary. This bead does not
/// implement the handler; it only defines the protocol surface so downstream
/// work can plug in without re-opening `AnalysisWorkScheduler`.
///
/// Lane vocabulary leaks across module boundaries here because preemption is
/// inherently a scheduler-internal concern; consumers that implement this
/// protocol are expected to live inside `Playhead/Services/` alongside the
/// scheduler itself. The UI-lint test (`SchedulerLaneUILintTests`) enforces
/// that restriction.
protocol LanePreemptionHandler: Sendable {
    /// Called when the scheduler is about to admit a job in the given lane.
    /// Implementations should demote (pause) active jobs in strictly lower
    /// lanes at the next safe checkpoint. No-op is a valid default.
    func preemptLowerLanes(for incoming: AnalysisWorkScheduler.SchedulerLane) async
}

// `TransportStatusProviding` + `WifiTransportStatusProvider` live in
// TransportStatusProviding.swift in this directory. A live
// `NWPathMonitor`-backed provider will land in playhead-ml96.

actor AnalysisWorkScheduler {
    private static let coverageProgressEpsilon = 0.001
    /// Back-off applied when the scheduler decides not to run the job at
    /// the top of the queue on this pass — either the Soon-vs-Background
    /// deferred filter skipped it, the config is disabled, or the
    /// multi-resource admission gate (playhead-bnrs) rejected it. Longer
    /// than `idlePollSeconds` because in each of those cases the same job
    /// would come straight back to the top of the queue; a short sleep
    /// would produce a hot log/poll loop. A capability change or an
    /// explicit `wake()` preempts the sleep.
    private static let rejectionBackoffSeconds: UInt64 = 30
    /// Default sleep between idle scheduler passes when there's nothing to
    /// admit but no explicit reason to back off harder. Wake() preempts.
    private static let idlePollSeconds: UInt64 = 5
    /// Lease lifetime applied at acquire and at each renewal CAS. Renewal
    /// happens every `leaseRenewalIntervalSeconds`, well inside this window.
    private static let leaseExpirySeconds: TimeInterval = 300
    /// Renewal cadence for in-flight job leases. Must be < leaseExpirySeconds
    /// with margin so a missed wakeup doesn't lose the lease.
    private static let leaseRenewalIntervalSeconds: UInt64 = 120

    /// Centralized exponential-backoff for failed/retrying jobs. Doubles per
    /// attempt, capped at 1 hour. `attempt` is 1-indexed (first retry is 2 min).
    private static func exponentialBackoffSeconds(attempt: Int) -> Double {
        min(pow(2.0, Double(attempt)) * 60, 3600)
    }

    private let store: AnalysisStore
    private let jobRunner: AnalysisJobRunner
    private let capabilitiesService: any CapabilitiesProviding
    private let downloadManager: any DownloadProviding
    private let batteryProvider: any BatteryStateProviding
    /// playhead-bnrs: transport-status provider consumed by the
    /// admission gate. Defaults to `WifiTransportStatusProvider` so
    /// production behavior is unchanged until a real
    /// `NWPathMonitor`-backed provider lands in playhead-ml96.
    private let transportStatusProvider: any TransportStatusProviding
    /// playhead-c3pi: advisory cascade that tracks the readiness anchor
    /// and candidate-window ordering per episode. When nil, the
    /// scheduler operates exactly as it did before c3pi — the
    /// cascade is a side channel consumed by the Phase 2 surfaces
    /// (CoverageSummary derivation, future per-slice execution in
    /// playhead-1iq1). A production runtime supplies a real instance
    /// via `PlayheadRuntime`; tests that don't care about candidate
    /// windows can omit it.
    private let candidateWindowCascade: CandidateWindowCascade?
    private let config: PreAnalysisConfig
    private let logger = Logger(subsystem: "com.playhead", category: "WorkScheduler")

    // MARK: - SchedulerLane (playhead-r835)

    /// Three-lane partition of the work queue. Derived from
    /// `AnalysisJob.priority` via the `schedulerLane` computed property on
    /// the job model. The partition is orthogonal to the existing T0/T1/T2
    /// coverage tiers — a T0 playback job can live in the Now lane, while a
    /// deep T2 backfill lives in Background.
    ///
    /// The variant names are deliberately scheduler-internal: they MUST NOT
    /// surface in UI copy, diagnostics text, or activity strings. The UI-lint
    /// test `SchedulerLaneUILintTests` enforces that prohibition by scanning
    /// every non-Services Swift source in the app target.
    ///
    /// Priority ranges (see also `AnalysisJob.schedulerLane`):
    /// - `.now` — priority >= 20 (user-initiated Play / Download promotions,
    ///   including playback T0)
    /// - `.soon` — priority 1..<20 (auto-download w/ proximity hints /
    ///   upcoming episodes)
    /// - `.background` — priority <= 0 (deferred auto-download, bulk backfill)
    enum SchedulerLane: Sendable, Equatable, CaseIterable {
        case now
        case soon
        case background
    }

    /// Per-lane concurrency caps. Bead spec:
    /// - Now:        <= 2 concurrent non-playback jobs
    /// - Soon:       <= 1 concurrent
    /// - Background: <= 1 concurrent
    /// - T0 playback jobs (`jobType == "playback"`) are EXEMPT from the Now
    ///   cap — they are always admitted when not globally paused.
    ///
    /// Concurrency accounting lives directly on the actor (`laneActive`,
    /// `canAdmit`, `didStart`, `didFinish`) rather than in a separate
    /// reference type. Actor isolation guarantees data-race safety for the
    /// mutable counter, so no `@unchecked Sendable` escape hatch is needed.
    /// The caps are compile-time constants; swap them here if the bead spec
    /// changes.
    static let nowCap = 2
    static let soonCap = 1
    static let backgroundCap = 1

    /// Admission decision the scheduler derives from the current QualityProfile
    /// and applies to every loop iteration. Consolidates thermal/battery/
    /// low-power gating into a single surface — see `QualityProfile.derive`.
    struct LaneAdmission: Sendable, Equatable {
        let qualityProfile: QualityProfile
        let policy: QualityProfile.SchedulerPolicy

        /// Whether any work at all may run. Mirrors `policy.pauseAllWork` for
        /// readability at call sites.
        var pauseAllWork: Bool { policy.pauseAllWork }

        /// Whether a deferred job of the given coverage depth is allowed
        /// under the current QualityProfile. T0 (playback) jobs are never
        /// gated here — the store selects them on the hot-path criteria;
        /// only `pauseAllWork` can stop them.
        ///
        /// A job is classified as Background when its desired coverage is at
        /// or above `t2Threshold`. Anything below that is Soon lane.
        func allowsDeferredJob(desiredCoverageSec: Double, t2Threshold: Double) -> Bool {
            if pauseAllWork { return false }
            let isBackgroundLane = desiredCoverageSec >= t2Threshold
            if isBackgroundLane {
                return policy.allowBackgroundLane
            } else {
                return policy.allowSoonLane
            }
        }

        /// Whether a job in the given `SchedulerLane` is admitted under the
        /// current QualityProfile. This is the priority-derived dual of
        /// `allowsDeferredJob(desiredCoverageSec:t2Threshold:)` — the latter
        /// gates by coverage depth, this one gates by lane.
        ///
        /// Semantics:
        /// - Any lane is blocked when `pauseAllWork` is true (critical).
        /// - `.now` is admitted unless `pauseAllWork`; it ignores the Soon
        ///   and Background gates because Now-lane jobs are user-initiated
        ///   (Play / explicit Download) and must drain promptly even in
        ///   serious thermal states.
        /// - `.soon` is admitted only when `policy.allowSoonLane` is true.
        /// - `.background` is admitted only when `policy.allowBackgroundLane`
        ///   is true.
        func allows(lane: SchedulerLane) -> Bool {
            if pauseAllWork { return false }
            switch lane {
            case .now:        return true
            case .soon:       return policy.allowSoonLane
            case .background: return policy.allowBackgroundLane
            }
        }
    }

    private var schedulerTask: Task<Void, Never>?
    private var currentRunningTask: Task<Void, Never>?
    private var currentJobId: String?
    private var currentEpisodeId: String?
    private var activePlaybackEpisodeId: String?
    private var shouldCancelCurrentJob = false
    /// Set to `true` by the lease-renewal task when its CAS finds no
    /// matching row — i.e. orphan recovery (or another scheduler
    /// instance) has reclaimed the lease and may already have re-queued
    /// or completed the job under a new owner. When set, the run loop
    /// must skip every store write in its cleanup paths (state revert,
    /// progress update, retry/backoff, releaseLease) because those
    /// writes would clobber the new owner's bookkeeping. Reset at the
    /// start of every job so the flag never bleeds across iterations.
    private var lostOwnership = false
    /// Cause to thread into WorkJournal when the current running job is
    /// cancelled. Set by `cancelCurrentJob(cause:)`; consumed on the
    /// cancellation branch of the run loop. Resets to `nil` after each
    /// job finishes (whether cancelled or not) so a subsequent job
    /// doesn't inherit a stale cause tag.
    private var pendingCancelCause: InternalMissCause?
    private var leaseRenewalTask: Task<Void, any Error>?
    /// Optional WorkJournal recorder. When non-nil the scheduler emits
    /// a `recordFailed(..., cause:, metadataJSON:)` row on the
    /// cancellation path so causes like `.taskExpired` and
    /// `.userCancelled` land in `work_journal.cause`. Nil (default) is
    /// fine for unit tests that don't exercise the journal — the
    /// emission is a best-effort tail call after the lease release.
    private var workJournalRecorder: WorkJournalRecording = NoopWorkJournalRecorder()
    private static let maxAttemptCount = 5

    /// Per-lane running-job counter. Enforces the Now/Soon/Background
    /// concurrency caps spelled out in playhead-r835. Today the scheduler
    /// runs at most one job at a time via `currentRunningTask`, so the
    /// counter's per-lane caps are not yet the binding constraint on real
    /// execution — they are the contract the admission path uses so that
    /// later beads can fan the scheduler out to honest multi-lane
    /// concurrency without re-opening admission policy.
    ///
    /// Stored inline on the actor for data-race safety by isolation — see
    /// the `nowCap` / `soonCap` / `backgroundCap` constants above.
    private var laneActive: [SchedulerLane: Int] = [
        .now: 0,
        .soon: 0,
        .background: 0,
    ]

    /// Hook installed by downstream beads (playhead-01t8) to implement
    /// preemption of active Soon / Background jobs when a Now-lane job is
    /// admitted. Nil means "no preemption" — which is the only behavior this
    /// bead ships.
    private var preemptionHandler: (any LanePreemptionHandler)?

    private var wakeContinuation: AsyncStream<Void>.Continuation?
    private var wakeStream: AsyncStream<Void>

    /// Tracks OSSignposter queue-wait intervals keyed by jobId.
    private var queueWaitStates: [String: OSSignpostIntervalState] = [:]

    init(
        store: AnalysisStore,
        jobRunner: AnalysisJobRunner,
        capabilitiesService: any CapabilitiesProviding,
        downloadManager: any DownloadProviding,
        batteryProvider: any BatteryStateProviding = UIDeviceBatteryProvider(),
        transportStatusProvider: any TransportStatusProviding = WifiTransportStatusProvider(),
        candidateWindowCascade: CandidateWindowCascade? = nil,
        config: PreAnalysisConfig = .load()
    ) {
        self.store = store
        self.jobRunner = jobRunner
        self.capabilitiesService = capabilitiesService
        self.downloadManager = downloadManager
        self.batteryProvider = batteryProvider
        self.transportStatusProvider = transportStatusProvider
        self.candidateWindowCascade = candidateWindowCascade
        self.config = config
        var continuation: AsyncStream<Void>.Continuation?
        self.wakeStream = AsyncStream<Void> { continuation = $0 }
        self.wakeContinuation = continuation
    }

    // MARK: - Public API

    /// Enqueue a new pre-analysis job for an episode.
    /// Explicit downloads get priority=10, auto-downloads get priority=0.
    func enqueue(
        episodeId: String,
        podcastId: String?,
        downloadId: String,
        sourceFingerprint: String,
        isExplicitDownload: Bool,
        desiredCoverage: Double? = nil
    ) async {
        let priority = isExplicitDownload ? 10 : 0
        let coverage = desiredCoverage ?? config.defaultT0DepthSeconds
        let workKey = AnalysisJob.computeWorkKey(
            fingerprint: sourceFingerprint,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            jobType: "preAnalysis"
        )
        let now = Date().timeIntervalSince1970
        let job = AnalysisJob(
            jobId: UUID().uuidString,
            jobType: "preAnalysis",
            episodeId: episodeId,
            podcastId: podcastId,
            analysisAssetId: nil,
            workKey: workKey,
            sourceFingerprint: sourceFingerprint,
            downloadId: downloadId,
            priority: priority,
            desiredCoverageSec: coverage,
            featureCoverageSec: 0,
            transcriptCoverageSec: 0,
            cueCoverageSec: 0,
            state: "queued",
            attemptCount: 0,
            nextEligibleAt: nil,
            leaseOwner: nil,
            leaseExpiresAt: nil,
            lastErrorCode: nil,
            createdAt: now,
            updatedAt: now
        )
        do {
            try await store.insertJob(job)
            queueWaitStates[job.jobId] = PreAnalysisInstrumentation.beginQueueWait(jobId: job.jobId)
            logger.info("Enqueued job \(job.jobId) for episode \(episodeId), priority=\(priority), coverage=\(coverage)s")
        } catch {
            logger.error("Failed to enqueue job: \(error)")
        }
        wakeSchedulerLoop()
    }

    /// playhead-c3pi: seed the candidate-window cascade for an episode.
    /// Call this after `enqueue(...)` once the metadata + chapter
    /// evidence have been parsed (typically from the download
    /// completion path). When no cascade was injected at construction
    /// the call is a no-op and returns an empty window list.
    ///
    /// - Returns: The ordered candidate windows the cascade now
    ///   associates with this episode (empty when no cascade is wired).
    @discardableResult
    func seedCandidateWindows(
        episodeId: String,
        episodeDuration: TimeInterval?,
        playbackAnchor: TimeInterval?,
        chapterEvidence: [ChapterEvidence]
    ) async -> [CandidateWindow] {
        guard let cascade = candidateWindowCascade else { return [] }
        return await cascade.seed(
            episodeId: episodeId,
            episodeDuration: episodeDuration,
            playbackAnchor: playbackAnchor,
            chapterEvidence: chapterEvidence
        )
    }

    /// playhead-c3pi: notify the scheduler of a committed playhead
    /// update for an episode. Invoked from the playback service /
    /// `PlayheadApp.persistPlaybackPosition` so the cascade can re-latch
    /// when the user seeks more than `seekRelatchThresholdSeconds` away
    /// from the prior anchor.
    ///
    /// playhead-swws: `chapterEvidence` is optional. The cascade caches
    /// the evidence captured at the most recent `seedCandidateWindows`
    /// call; commit-point callers (which don't carry chapter evidence
    /// in scope) should pass `nil` so cached sponsor-chapter windows
    /// survive the re-latch instead of being erased on every seek.
    /// Pass an explicit array only when fresh evidence is available
    /// (e.g. after a metadata reparse).
    ///
    /// - Returns: The new candidate-window order on a re-latch, or
    ///   `nil` when the delta did not exceed the threshold (no
    ///   re-latch). When no cascade was injected, always returns nil.
    @discardableResult
    func noteCommittedPlayhead(
        episodeId: String,
        newPosition: TimeInterval,
        episodeDuration: TimeInterval?,
        chapterEvidence: [ChapterEvidence]? = nil
    ) async -> [CandidateWindow]? {
        guard let cascade = candidateWindowCascade else { return nil }
        return await cascade.noteSeek(
            episodeId: episodeId,
            newPosition: newPosition,
            episodeDuration: episodeDuration,
            chapterEvidence: chapterEvidence
        )
    }

    /// playhead-c3pi: read-only accessor for the current candidate
    /// windows associated with an episode. Surfaces / SLI emitters use
    /// this to report the planned cascade order without standing up a
    /// fresh selector. Returns an empty array when no cascade is
    /// wired or the episode is unknown to the cascade.
    func currentCandidateWindows(for episodeId: String) async -> [CandidateWindow] {
        guard let cascade = candidateWindowCascade else { return [] }
        return await cascade.currentWindows(for: episodeId) ?? []
    }

    /// playhead-swws: peek the next slice the scheduler would dispatch
    /// on the next loop iteration WITHOUT mutating any state (no lease
    /// acquisition, no state transitions, no signposts). Returns the
    /// next eligible job paired with the cascade's first candidate
    /// window for that job's episode.
    ///
    /// Used by the swws ordering test to prove that the cascade's
    /// proximal-first order is the order the scheduler ACTUALLY
    /// dispatches in, not just what the cascade reports it would
    /// prefer. When the cascade has been seeded with a sponsor window
    /// at e.g. [10min, 11min] and the proximal window at [0, 20min],
    /// the cascade's first window is the sponsor — and the dispatched
    /// slice's `cascadeWindow` matches it. With no cascade seed for
    /// the episode, `cascadeWindow` is `nil` and the dispatched job is
    /// exactly what `fetchNextEligibleJob` would return on its own —
    /// preserving FIFO behavior for the long tail of episodes that
    /// have not been seeded.
    ///
    /// Returns `nil` when no eligible job exists OR when the
    /// admission policy currently bars all work (`pauseAllWork`); the
    /// loop's per-pass back-off behavior still applies, this accessor
    /// just reports the absence of a dispatchable slice without
    /// taking the standard sleep.
    func peekNextDispatchableSlice() async -> DispatchableSlice? {
        guard config.isEnabled else { return nil }
        let admission = await currentLaneAdmission()
        guard !admission.pauseAllWork else { return nil }

        let deferredWorkAllowed = admission.policy.allowSoonLane
            || admission.policy.allowBackgroundLane
        let now = Date().timeIntervalSince1970

        guard let job = try? await store.fetchNextEligibleJob(
            deferredWorkAllowed: deferredWorkAllowed,
            t0ThresholdSec: config.defaultT0DepthSeconds,
            now: now
        ) else {
            return nil
        }

        let cascadeWindow: CandidateWindow?
        if let cascade = candidateWindowCascade,
           let windows = await cascade.currentWindows(for: job.episodeId) {
            cascadeWindow = windows.first
        } else {
            cascadeWindow = nil
        }

        return DispatchableSlice(
            jobId: job.jobId,
            episodeId: job.episodeId,
            cascadeWindow: cascadeWindow
        )
    }

    /// Notify the scheduler that playback has started for an episode.
    /// Cancel any running pre-analysis work while the foreground hot path owns
    /// the shared analysis pipeline.
    func playbackStarted(episodeId: String) async {
        activePlaybackEpisodeId = episodeId
        if currentRunningTask != nil {
            shouldCancelCurrentJob = true
            currentRunningTask?.cancel()
            logger.info("Playback preempted pre-analysis while episode \(episodeId) is active")
        }
        wakeSchedulerLoop()
    }

    /// Notify the scheduler that foreground playback has stopped, allowing
    /// queued deferred work to resume.
    func playbackStopped() {
        activePlaybackEpisodeId = nil
        wakeSchedulerLoop()
    }

    /// Mark jobs for a deleted episode as superseded.
    func episodeDeleted(episodeId: String) async {
        if currentEpisodeId == episodeId {
            shouldCancelCurrentJob = true
            currentRunningTask?.cancel()
        }
        do {
            let states = ["queued", "paused", "running", "failed",
                          "blocked:missingFile", "blocked:modelUnavailable"]
            for state in states {
                let jobs = try await store.fetchJobsByState(state)
                for job in jobs where job.episodeId == episodeId {
                    try await store.updateJobState(jobId: job.jobId, state: "superseded")
                }
            }
        } catch {
            logger.error("Failed to supersede jobs for deleted episode \(episodeId): \(error)")
        }
        // playhead-c3pi: drop the cascade entry so a re-subscribe to
        // the same episode does not inherit a stale anchor.
        await candidateWindowCascade?.forget(episodeId: episodeId)
    }

    /// Cancel the currently executing analysis job.
    ///
    /// `cause` is the `InternalMissCause` that the scheduler will emit
    /// on the cancellation branch of the run loop (via the injected
    /// `WorkJournalRecording` recorder). The default is
    /// `.pipelineError` so existing callers that don't know a better
    /// cause still produce a typed cause tag rather than `nil`.
    /// `.taskExpired` is passed from `BackgroundProcessingService`'s
    /// expirationHandler; `.userCancelled` is the explicit-cancel
    /// entry point.
    func cancelCurrentJob(cause: InternalMissCause = .pipelineError) {
        shouldCancelCurrentJob = true
        // Concurrent cancels with different causes must not stomp each
        // other with last-writer-wins. Route through
        // `CauseAttributionPolicy.primaryCause` so precedence (e.g.
        // `.userCancelled` outranks `.taskExpired`) is honored regardless
        // of arrival order. Context values are conservative defaults
        // that keep the tier ranking of the causes the cancel path uses
        // (`.userCancelled`, `.userPreempted`, `.taskExpired`,
        // `.pipelineError`) stable — retryBudgetRemaining only matters
        // for `.taskExpired`, and both tiers (environmentalTransient /
        // resourceExhausted) still lose to `.userInitiated`.
        let resolved: InternalMissCause
        if let existing = pendingCancelCause, existing != cause {
            let context = CauseAttributionContext(
                modelAvailableNow: true,
                retryBudgetRemaining: 0
            )
            resolved = CauseAttributionPolicy.primaryCause(
                among: [existing, cause],
                context: context
            ) ?? cause
        } else {
            resolved = cause
        }
        pendingCancelCause = resolved
        currentRunningTask?.cancel()
    }

    #if DEBUG
    /// Test-only accessor for the `pendingCancelCause` field so unit
    /// tests can verify `cancelCurrentJob(cause:)` precedence without
    /// having to run the full scheduler loop. Do not wire into
    /// production code — the cause is consumed on the cancellation
    /// branch of `runOneIteration` and should not be observed
    /// externally.
    func pendingCancelCauseForTesting() -> InternalMissCause? {
        pendingCancelCause
    }
    #endif

    /// Install the WorkJournal recorder the scheduler uses on the
    /// cancellation branch. Optional — tests that don't need the
    /// journal can leave the default `NoopWorkJournalRecorder` in
    /// place. Called from `PlayheadRuntime` once the real recorder is
    /// available.
    func setWorkJournalRecorder(_ recorder: WorkJournalRecording) {
        self.workJournalRecorder = recorder
    }

    /// Wake the scheduler loop externally. Used by BackgroundProcessingService
    /// when a BGProcessingTask window opens so the loop immediately polls for
    /// eligible jobs instead of waiting out its current sleep interval.
    ///
    /// This is the explicit public handle for the private `wakeSchedulerLoop`
    /// signal and makes the BPS→WorkScheduler dependency visible at the call
    /// site.
    func wake() {
        wakeSchedulerLoop()
    }

    /// Install a lane-preemption handler. This bead (playhead-r835) only
    /// defines the protocol surface — it installs no default handler. A
    /// later bead (playhead-01t8) will wire an implementation that pauses
    /// active Soon / Background jobs at their next safe checkpoint when the
    /// scheduler admits a Now-lane job.
    func setLanePreemptionHandler(_ handler: (any LanePreemptionHandler)?) {
        self.preemptionHandler = handler
    }

    /// Start the scheduler loop. Call after reconciliation is complete.
    func startSchedulerLoop() {
        schedulerTask?.cancel()
        schedulerTask = Task { [weak self] in
            guard let self else { return }
            await self.runLoop()
        }
    }

    /// Stop the scheduler loop and any running job.
    func stop() {
        schedulerTask?.cancel()
        schedulerTask = nil
        currentRunningTask?.cancel()
        currentRunningTask = nil
        leaseRenewalTask?.cancel()
        leaseRenewalTask = nil
    }

    // MARK: - Scheduler Loop

    private func runLoop() async {
        while !Task.isCancelled {
            guard config.isEnabled else {
                await sleepOrWake(seconds: Self.rejectionBackoffSeconds)
                continue
            }

            // Foreground playback owns the shared transcript/backfill pipeline.
            // Do not start deferred pre-analysis work until playback stops.
            if activePlaybackEpisodeId != nil {
                await sleepOrWake(seconds: Self.idlePollSeconds)
                continue
            }

            let admission = await currentLaneAdmission()

            // Critical thermal (or equivalent) pauses every lane, including
            // T0 playback drains. Wait for the next wake/capability change.
            if admission.pauseAllWork {
                await sleepOrWake(seconds: Self.idlePollSeconds)
                continue
            }

            // `deferredWorkAllowed` gates the store's deferred (T1+) selection.
            // Critical cases where T2 is paused but Soon is allowed are handled
            // after fetch via `admission.allowsDeferredJob`, because the store
            // predicate only distinguishes T0 vs. deferred, not Soon vs.
            // Background.
            let deferredWorkAllowed = admission.policy.allowSoonLane
                || admission.policy.allowBackgroundLane
            let now = Date().timeIntervalSince1970

            guard let job = try? await store.fetchNextEligibleJob(
                deferredWorkAllowed: deferredWorkAllowed,
                t0ThresholdSec: config.defaultT0DepthSeconds,
                now: now
            ) else {
                await sleepOrWake(seconds: Self.idlePollSeconds)
                continue
            }

            // Secondary filter for the Soon-vs-Background lane split. Only
            // deferred jobs are subject to this; T0 playback jobs are always
            // admitted when not paused-all (checked above). We back off
            // longer here (30s) rather than the default 5s because the store
            // predicate can't express "Soon only," so the same Background
            // job will come back to the top of the queue on every re-fetch —
            // a short sleep would produce a hot log/poll loop. A capability
            // change or an explicit wake() will preempt the sleep.
            if job.jobType != "playback",
               !admission.allowsDeferredJob(
                    desiredCoverageSec: job.desiredCoverageSec,
                    t2Threshold: config.t2DepthSeconds
               ) {
                logger.info("Skipping job \(job.jobId) (depth=\(job.desiredCoverageSec)s) under QualityProfile \(admission.qualityProfile.rawValue, privacy: .public)")
                await sleepOrWake(seconds: Self.rejectionBackoffSeconds)
                continue
            }

            // Per-lane concurrency cap (playhead-r835). T0 playback jobs are
            // exempt from the Now cap; `canAdmit` encodes that rule. If the
            // cap is saturated, fall back to the standard sleep so we do not
            // re-fetch the same job in a tight loop.
            guard canAdmit(job: job) else {
                logger.info("Skipping job \(job.jobId) — lane \(String(describing: job.schedulerLane), privacy: .public) at capacity")
                await sleepOrWake(seconds: Self.idlePollSeconds)
                continue
            }

            // Multi-resource admission gate (playhead-bnrs). Consults
            // thermal / transport / storage / CPU axes; a hard rejection
            // here skips this pass with a logged cause. On the reject
            // path we do NOT mutate job state (no `updateJobState` /
            // WorkJournal write) — the scheduler will re-fetch the job
            // on the next pass once the failing axis clears (a
            // capability change wakes the loop). This preserves the
            // retry semantics that existed before the gate was wired.
            let gateDecision = await evaluateAdmissionGate(for: job)
            switch gateDecision {
            case .reject(let cause):
                logger.info("AdmissionGate rejected job \(job.jobId) episode=\(job.episodeId) lane=\(String(describing: job.schedulerLane), privacy: .public) cause=\(cause.rawValue, privacy: .public)")
                await sleepOrWake(seconds: Self.rejectionBackoffSeconds)
                continue
            case .admit(let sliceBytes):
                // Audit trail: record the gate's computed slice budget
                // so rejection-vs-admission traces have symmetric log
                // coverage. Nothing downstream consumes sliceBytes yet
                // because the execution unit is still the full job, not
                // a slice.
                // TODO(playhead-1iq1): plumb sliceBytes into AnalysisRangeRequest once per-slice execution is the scheduling unit
                logger.info("AdmissionGate admitted job \(job.jobId) episode=\(job.episodeId) lane=\(String(describing: job.schedulerLane), privacy: .public) sliceBytes=\(sliceBytes)")
            }

            // Now-lane admission demotes active Soon / Background jobs at
            // their next safe checkpoint. The protocol is owned by
            // playhead-01t8; this bead only wires the hook. Pass the job's
            // lane rather than a hardcoded `.now` so that if the guard above
            // ever widens (e.g. Soon-on-Background preemption), the call
            // site does not silently misreport the incoming lane.
            if job.schedulerLane == .now, let preempt = preemptionHandler {
                await preempt.preemptLowerLanes(for: job.schedulerLane)
            }

            didStart(job: job)
            await processJob(job)
            didFinish(job: job)
        }
    }

    // MARK: - Lane concurrency accounting (playhead-r835)

    /// Current running-job count in `lane`. Exposed for instrumentation and
    /// tests; the scheduler loop uses `canAdmit` / `didStart` / `didFinish`
    /// directly.
    func laneActiveCount(_ lane: SchedulerLane) -> Int {
        laneActive[lane] ?? 0
    }

    /// Whether `job` may be admitted under the current per-lane count. T0
    /// playback jobs (`jobType == "playback"`) bypass the Now cap
    /// unconditionally — the hot-path must always be able to drain.
    func canAdmit(job: AnalysisJob) -> Bool {
        let lane = job.schedulerLane
        if lane == .now && job.jobType == "playback" {
            return true
        }
        let cap: Int
        switch lane {
        case .now:        cap = Self.nowCap
        case .soon:       cap = Self.soonCap
        case .background: cap = Self.backgroundCap
        }
        return laneActiveCount(lane) < cap
    }

    /// Record that `job` has started running in its lane.
    func didStart(job: AnalysisJob) {
        let lane = job.schedulerLane
        laneActive[lane, default: 0] += 1
    }

    /// Record that `job` has finished running in its lane. Clamped at zero
    /// so a stray double-finish does not produce negative counts.
    func didFinish(job: AnalysisJob) {
        let lane = job.schedulerLane
        let current = laneActive[lane, default: 0]
        laneActive[lane] = max(0, current - 1)
    }

    /// Evaluate the full multi-resource admission gate for `job`. Returns
    /// the `GateAdmissionDecision` the scheduler will act on: `.admit` means
    /// the caller may proceed to `processJob(_:)`, `.reject(cause)` means
    /// the scheduler must skip this pass and log the cause.
    ///
    /// playhead-bnrs: this is the production consumer of
    /// `AdmissionGate.admit(...)`. It stitches together the four gate
    /// inputs:
    ///
    /// - `profile`: derived from the capabilities snapshot + live
    ///   battery, same source as `currentLaneAdmission()`.
    /// - `deviceClass` / `deviceProfile`: from the snapshot + the
    ///   playhead-dh9b hard-coded fallback table (the JSON manifest
    ///   loader is not plumbed here — slice-sizing uses the fallback row
    ///   until a loader is injected).
    /// - `transport`: synthesized from `transportStatusProvider`
    ///   (defaults to `WifiTransportStatusProvider`) and the job's lane.
    ///   Background-lane jobs map to `.maintenance` (Wi-Fi only); every
    ///   other lane maps to `.interactive`.
    /// - `storage`: plentiful-default snapshot — the per-class cap check
    ///   is still performed by `StorageBudget.admit` at write time; this
    ///   gate's role in the bnrs wire is to ensure the transport / CPU /
    ///   thermal axes are consulted at admission. Plumbing a live
    ///   `StorageBudget` snapshot into the scheduler is playhead-1iq1.
    func evaluateAdmissionGate(for job: AnalysisJob) async -> GateAdmissionDecision {
        let snapshot = await capabilitiesService.currentSnapshot
        let batteryState = await batteryProvider.currentBatteryState()
        let profile = snapshot.qualityProfile(
            batteryLevel: batteryState.level,
            isCharging: batteryState.isCharging
        )
        let deviceClass = snapshot.deviceClass
        let deviceProfile = DeviceClassProfile.fallback(for: deviceClass)

        let reachability = await transportStatusProvider.currentReachability()
        let allowsCellular = await transportStatusProvider.userAllowsCellular()
        // Background-lane jobs are maintenance transfers (auto-download
        // / bulk backfill). Everything else is interactive — user-
        // initiated Play / explicit Download (Now), or a proximate
        // upcoming-episode preload (Soon). This mirrors the
        // BackgroundSessionIdentifier split in closed bead playhead-24cm.
        let session: TransportSnapshot.Session = (job.schedulerLane == .background)
            ? .maintenance
            : .interactive
        let transport = TransportSnapshot(
            reachability: reachability,
            session: session,
            userAllowsCellular: allowsCellular
        )

        // Storage snapshot: plentiful by construction for this bead.
        // The per-class admission check is performed by
        // `StorageBudget.admit(class:sizeBytes:)` at the actual write
        // site; this gate's contribution is to keep transport/CPU/
        // thermal consulted at scheduling time. playhead-1iq1 will
        // inject a live StorageBudget reference and replace this with a
        // real snapshot.
        let storage = StorageSnapshot.plentiful

        let admissionJob = AdmissionJob(
            artifactClasses: [job.artifactClass],
            estimatedWriteBytes: max(0, job.estimatedWriteBytes)
        )

        return AdmissionGate.admit(
            job: admissionJob,
            profile: profile,
            deviceClass: deviceClass,
            deviceProfile: deviceProfile,
            storage: storage,
            transport: transport
        )
    }

    /// Evaluate the current `LaneAdmission` from the capabilities snapshot and
    /// a live battery reading. Exposed internally so tests (and integrators
    /// like BackgroundProcessingService) can ask what the scheduler would do
    /// right now without driving the full loop.
    ///
    /// All thermal/battery/low-power reads route through `QualityProfile` —
    /// there are no direct `ProcessInfo.thermalState` or `isLowPowerMode`
    /// reads in this actor. The thermal gate of the broader
    /// multi-resource admission policy (playhead-bnrs) is honored here
    /// via `policy.pauseAllWork`; the transport, storage, and CPU gates
    /// are consulted by `evaluateAdmissionGate(for:)` which the scheduler
    /// loop calls after `canAdmit(job:)` succeeds.
    func currentLaneAdmission() async -> LaneAdmission {
        let snapshot = await capabilitiesService.currentSnapshot
        let batteryState = await batteryProvider.currentBatteryState()
        // Route every thermal/battery/low-power read through the snapshot's
        // QualityProfile surface. The `isCharging:` overload is preferred
        // because the battery provider's charging signal is fresher than the
        // snapshot's (which only refreshes on `batteryStateDidChange`).
        let profile = snapshot.qualityProfile(
            batteryLevel: batteryState.level,
            isCharging: batteryState.isCharging
        )
        return LaneAdmission(qualityProfile: profile, policy: profile.schedulerPolicy)
    }

    private func sleepOrWake(seconds: UInt64) async {
        await withTaskGroup(of: Void.self) { group in
            group.addTask {
                try? await Task.sleep(for: .seconds(seconds))
            }
            group.addTask { [wakeStream] in
                var iterator = wakeStream.makeAsyncIterator()
                _ = await iterator.next()
            }
            _ = await group.next()
            group.cancelAll()
        }
    }

    private func wakeSchedulerLoop() {
        wakeContinuation?.yield()
    }

    // MARK: - Job Processing

    private func processJob(_ job: AnalysisJob) async {
        // Resolve audio URL from download cache.
        guard let fileURL = await downloadManager.cachedFileURL(for: job.episodeId) else {
            logger.warning("No cached audio for episode \(job.episodeId), blocking job \(job.jobId)")
            do {
                try await store.updateJobState(jobId: job.jobId, state: "blocked:missingFile")
            } catch {
                logger.error("Failed to update job state: \(error)")
            }
            return
        }

        guard let localAudioURL = LocalAudioURL(fileURL) else {
            logger.error("cachedFileURL returned non-file URL for episode \(job.episodeId)")
            do {
                try await store.updateJobState(jobId: job.jobId, state: "blocked:missingFile")
            } catch {
                logger.error("Failed to update job state: \(error)")
            }
            return
        }

        // Acquire lease.
        let leaseExpiry = Date().timeIntervalSince1970 + Self.leaseExpirySeconds
        let leaseAcquired = (try? await store.acquireLease(
            jobId: job.jobId,
            owner: "preAnalysis",
            expiresAt: leaseExpiry
        )) ?? false

        guard leaseAcquired else {
            logger.info("Failed to acquire lease for job \(job.jobId), skipping")
            return
        }

        currentJobId = job.jobId
        currentEpisodeId = job.episodeId
        shouldCancelCurrentJob = false
        lostOwnership = false

        // End queue-wait signpost interval.
        if let queueState = queueWaitStates.removeValue(forKey: job.jobId) {
            PreAnalysisInstrumentation.endQueueWait(queueState)
        }

        // Lease renewal task. If the CAS finds no matching row, orphan
        // recovery (or another scheduler instance) has reclaimed the lease
        // — set `lostOwnership` so the cleanup paths skip every store
        // write (state revert, progress update, backoff, releaseLease)
        // that would otherwise clobber the new owner's bookkeeping, then
        // cancel the running task so the run loop unwinds promptly.
        leaseRenewalTask = Task {
            while !Task.isCancelled {
                try await Task.sleep(for: .seconds(Self.leaseRenewalIntervalSeconds))
                let newExpiry = Date().timeIntervalSince1970 + Self.leaseExpirySeconds
                let stillOwned = (try? await self.store.renewLease(
                    jobId: job.jobId,
                    owner: "preAnalysis",
                    newExpiresAt: newExpiry
                )) ?? false
                if !stillOwned {
                    self.lostOwnership = true
                    self.currentRunningTask?.cancel()
                    break
                }
            }
        }

        defer {
            leaseRenewalTask?.cancel()
            leaseRenewalTask = nil
            currentJobId = nil
            currentEpisodeId = nil
        }

        // Build request and run.
        let assetId: String
        do {
            assetId = try await resolveAnalysisAssetId(for: job, localAudioURL: localAudioURL)
        } catch {
            logger.error("Failed to resolve analysis asset for job \(job.jobId): \(error)")
            guard !lostOwnership else {
                logger.warning("Skipping asset-resolution failure writes for job \(job.jobId): lease reclaimed by orphan recovery")
                return
            }
            let backoff = Self.exponentialBackoffSeconds(attempt: job.attemptCount + 1)
            await writeIfStillOwned("assetResolution.markFailed") {
                try await store.updateJobState(
                    jobId: job.jobId,
                    state: "failed",
                    nextEligibleAt: Date().timeIntervalSince1970 + backoff,
                    lastErrorCode: "assetResolution: \(error)"
                )
            }
            await writeIfStillOwned("assetResolution.releaseLease") {
                try await store.releaseLease(jobId: job.jobId)
            }
            return
        }
        // playhead-swws: consult the candidate-window cascade for
        // this episode and surface its first window on the request.
        // The runner (and downstream slice-execution work in
        // playhead-1iq1) will use this to prioritize the
        // proximal/sponsor window over the legacy "process [0,
        // desiredCoverageSec]" depth-first behavior. `nil` when the
        // cascade is unwired or the episode has not been seeded —
        // existing FIFO behavior is preserved in that case.
        let cascadeWindow: CandidateWindow?
        if let cascade = candidateWindowCascade,
           let windows = await cascade.currentWindows(for: job.episodeId) {
            cascadeWindow = windows.first
        } else {
            cascadeWindow = nil
        }
        let request = AnalysisRangeRequest(
            jobId: job.jobId,
            episodeId: job.episodeId,
            podcastId: job.podcastId ?? "",
            analysisAssetId: assetId,
            audioURL: localAudioURL,
            desiredCoverageSec: job.desiredCoverageSec,
            mode: .preRollWarmup,
            outputPolicy: .writeWindowsAndCues,
            priority: .medium,
            schedulerLane: job.schedulerLane,
            windowRange: cascadeWindow?.range
        )

        let jobSignpost = PreAnalysisInstrumentation.beginJobDuration(jobId: job.jobId)
        do {
            guard !shouldCancelCurrentJob else {
                PreAnalysisInstrumentation.endJobDuration(jobSignpost)
                // `acquireLease` set state='running' atomically; if we
                // skip without reverting, the job is stranded at
                // 'running' with leaseOwner=NULL — invisible to
                // `fetchNextEligibleJob` (queued|paused|failed only) and
                // to `recoverExpiredLease` (leaseOwner IS NOT NULL
                // only). Revert to 'queued' before releasing the lease.
                await writeIfStillOwned("cancelRace.revertQueued") {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                }
                await writeIfStillOwned("cancelRace.releaseLease") {
                    try await store.releaseLease(jobId: job.jobId)
                }
                // Clear cancel state so it doesn't leak into the next
                // job picked up by the loop. `pendingCancelCause` was
                // set by the racing canceller for the now-skipped job;
                // a stale value would mis-attribute the next job's
                // cancellation if one arrives.
                shouldCancelCurrentJob = false
                pendingCancelCause = nil
                return
            }

            let runTask = Task<AnalysisOutcome, Error> {
                try Task.checkCancellation()
                let result = await self.jobRunner.run(request)
                try Task.checkCancellation()
                return result
            }
            currentRunningTask = Task {
                await withTaskCancellationHandler {
                    _ = try? await runTask.value
                } onCancel: {
                    runTask.cancel()
                }
            }

            let outcome: AnalysisOutcome
            do {
                outcome = try await runTask.value
            } catch is CancellationError {
                PreAnalysisInstrumentation.endJobDuration(jobSignpost)
                if lostOwnership {
                    // Lease was reclaimed by orphan recovery. The new
                    // owner is the source of truth for state, retry
                    // count, and cause; any write here would clobber
                    // its bookkeeping. Drop the cancel cause to avoid
                    // bleeding it into the next job.
                    logger.warning("Skipping cancel cleanup writes for job \(job.jobId): lease reclaimed by orphan recovery")
                    pendingCancelCause = nil
                    return
                }
                await writeIfStillOwned("cancelCatch.revertQueued") {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                }
                await writeIfStillOwned("cancelCatch.releaseLease") {
                    try await store.releaseLease(jobId: job.jobId)
                }
                // playhead-1nl6: emit the cause that accompanied the
                // cancel into the WorkJournal via the injected recorder.
                // Default cause is `.pipelineError` so callers that
                // forgot to pass one still produce a typed tag; the
                // expirationHandler in BackgroundProcessingService
                // passes `.taskExpired`, the explicit user cancel path
                // passes `.userCancelled`.
                let cause = pendingCancelCause ?? .pipelineError
                pendingCancelCause = nil
                let recorder = workJournalRecorder
                let episodeId = job.episodeId
                let metadata = await SliceCompletionInstrumentation.recordPaused(
                    cause: cause,
                    deviceClass: DeviceClass.detect(),
                    sliceDurationMs: 0,
                    bytesProcessed: 0,
                    shardsCompleted: 0,
                    extras: [
                        "stage": "analysisWorkScheduler.cancelCurrentJob",
                        "job_id": job.jobId,
                    ]
                )
                await recorder.recordPreempted(
                    episodeId: episodeId,
                    cause: cause,
                    metadataJSON: metadata.encodeJSON()
                )
                return
            }
            currentRunningTask = nil
            // Non-cancel path: clear any stale cause so the next job
            // doesn't inherit it.
            pendingCancelCause = nil

            PreAnalysisInstrumentation.endJobDuration(jobSignpost)

            // Log outcome metric.
            PreAnalysisInstrumentation.logJobOutcome(
                jobId: job.jobId,
                stopReason: String(describing: outcome.stopReason),
                coverageSec: outcome.cueCoverageSec
            )

            // First check before the outcome write chain. Each
            // individual `store.X(...)` below is also wrapped in
            // `writeIfStillOwned` so the renewer flipping the flag at
            // any subsequent `await` suspension point does not slip a
            // late write through.
            if lostOwnership {
                logger.warning("Skipping outcome writes for job \(job.jobId): lease reclaimed by orphan recovery")
                return
            }

            // Update progress in the store.
            await writeIfStillOwned("updateJobProgress") {
                try await store.updateJobProgress(
                    jobId: job.jobId,
                    featureCoverageSec: outcome.featureCoverageSec,
                    transcriptCoverageSec: outcome.transcriptCoverageSec,
                    cueCoverageSec: outcome.cueCoverageSec
                )
            }

            // Handle outcome.
            switch outcome.stopReason {
            case .reachedTarget where outcome.cueCoverageSec >= job.desiredCoverageSec:
                if let nextCoverage = nextTierCoverage(current: job.desiredCoverageSec) {
                    let tierWorkKey = AnalysisJob.computeWorkKey(
                        fingerprint: job.sourceFingerprint,
                        analysisVersion: PreAnalysisConfig.analysisVersion,
                        jobType: "preAnalysis"
                    ) + ":\(Int(nextCoverage))"
                    let now = Date().timeIntervalSince1970
                    let nextJob = AnalysisJob(
                        jobId: UUID().uuidString,
                        jobType: "preAnalysis",
                        episodeId: job.episodeId,
                        podcastId: job.podcastId,
                        analysisAssetId: assetId,
                        workKey: tierWorkKey,
                        sourceFingerprint: job.sourceFingerprint,
                        downloadId: job.downloadId,
                        priority: 0,
                        desiredCoverageSec: nextCoverage,
                        featureCoverageSec: outcome.featureCoverageSec,
                        transcriptCoverageSec: outcome.transcriptCoverageSec,
                        cueCoverageSec: outcome.cueCoverageSec,
                        state: "paused",
                        attemptCount: 0,
                        nextEligibleAt: nil,
                        leaseOwner: nil,
                        leaseExpiresAt: nil,
                        lastErrorCode: nil,
                        createdAt: now,
                        updatedAt: now
                    )
                    await writeIfStillOwned("tierAdvance.insertNext") {
                        try await store.insertJob(nextJob)
                    }
                    await writeIfStillOwned("tierAdvance.markComplete") {
                        try await store.updateJobState(jobId: job.jobId, state: "complete")
                    }
                    PreAnalysisInstrumentation.logTierCompletion(tier: "\(Int(job.desiredCoverageSec))s", completed: true)
                    logger.info("Tier advancement: \(job.desiredCoverageSec)s -> \(nextCoverage)s for episode \(job.episodeId)")
                } else {
                    await writeIfStillOwned("allTiersDone.markComplete") {
                        try await store.updateJobState(jobId: job.jobId, state: "complete")
                    }
                    PreAnalysisInstrumentation.logTierCompletion(tier: "\(Int(job.desiredCoverageSec))s", completed: true)
                    logger.info("Job \(job.jobId) complete (all tiers done)")
                }

            case .reachedTarget:
                // Re-queue, but with a guard against infinite loops for short episodes
                // or episodes that can never reach the desired coverage.
                if !Self.shouldRetryCoverageInsufficient(job: job, outcome: outcome) {
                    await writeIfStillOwned("coverageInsufficient.noProgress") {
                        try await store.updateJobState(
                            jobId: job.jobId,
                            state: "complete",
                            lastErrorCode: "coverageInsufficient:noProgress"
                        )
                    }
                    logger.info("Job \(job.jobId) marked complete after no-progress pass (coverage insufficient)")
                } else {
                    await writeIfStillOwned("coverageInsufficient.increment") {
                        try await store.incrementAttemptCount(jobId: job.jobId)
                    }
                    let updatedJob = lostOwnership ? nil : (try? await store.fetchJob(byId: job.jobId))
                    if (updatedJob?.attemptCount ?? 0) >= Self.maxAttemptCount {
                        await writeIfStillOwned("coverageInsufficient.maxAttempts") {
                            try await store.updateJobState(
                                jobId: job.jobId,
                                state: "complete",
                                lastErrorCode: "maxAttemptsReached:coverageInsufficient"
                            )
                        }
                        logger.info("Job \(job.jobId) marked complete after max attempts (coverage insufficient)")
                    } else {
                        // Backoff before next attempt: without a
                        // gap, the scheduler loop wakes
                        // immediately, picks the same job, and
                        // burns the full decode pipeline N more
                        // times in a tight loop. Match the
                        // `.failed` exponential backoff so
                        // attempt-N waits min(2^N * 60, 3600) s.
                        let attemptIndex = Double((updatedJob?.attemptCount ?? 1))
                        let backoff = min(pow(2.0, attemptIndex) * 60, 3600)
                        await writeIfStillOwned("coverageInsufficient.requeue") {
                            try await store.updateJobState(
                                jobId: job.jobId,
                                state: "queued",
                                nextEligibleAt: Date().timeIntervalSince1970 + backoff
                            )
                        }
                    }
                }

            case .blockedByModel:
                let nextEligible = Date().timeIntervalSince1970 + 300
                await writeIfStillOwned("blockedByModel") {
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "blocked:modelUnavailable",
                        nextEligibleAt: nextEligible
                    )
                }
                logger.info("Job \(job.jobId) blocked: model unavailable, retry in 300s")

            case .pausedForThermal, .memoryPressure:
                let nextEligible = Date().timeIntervalSince1970 + 30
                await writeIfStillOwned("pausedThermalOrMemory") {
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "paused",
                        nextEligibleAt: nextEligible
                    )
                }
                logger.info("Job \(job.jobId) paused for thermal/memory, retry in 30s")

            case .failed(let reason):
                await writeIfStillOwned("failed.increment") {
                    try await store.incrementAttemptCount(jobId: job.jobId)
                }
                let updated = lostOwnership ? nil : (try? await store.fetchJob(byId: job.jobId))
                let attempts = updated?.attemptCount ?? job.attemptCount + 1
                if attempts >= Self.maxAttemptCount {
                    await writeIfStillOwned("failed.supersede") {
                        try await store.updateJobState(
                            jobId: job.jobId,
                            state: "superseded",
                            lastErrorCode: "maxAttemptsReached:\(reason)"
                        )
                    }
                    logger.warning("Job \(job.jobId) abandoned after \(attempts) attempts: \(reason)")
                } else {
                    let backoff = Self.exponentialBackoffSeconds(attempt: attempts)
                    let nextEligible = Date().timeIntervalSince1970 + backoff
                    await writeIfStillOwned("failed.requeue") {
                        try await store.updateJobState(
                            jobId: job.jobId,
                            state: "failed",
                            nextEligibleAt: nextEligible,
                            lastErrorCode: reason
                        )
                    }
                    logger.warning("Job \(job.jobId) failed: \(reason), attempt \(attempts), backoff \(backoff)s")
                }

            case .backgroundExpired:
                await writeIfStillOwned("backgroundExpired.requeue") {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                }
                logger.info("Job \(job.jobId) background expired, requeued")

            case .cancelledByPlayback:
                await writeIfStillOwned("cancelledByPlayback.requeue") {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                }
                logger.info("Job \(job.jobId) cancelled by playback, requeued")

            case .preempted:
                await writeIfStillOwned("preempted.requeue") {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                }
                logger.info("Job \(job.jobId) preempted by higher-lane work, requeued")
            }
        } catch {
            PreAnalysisInstrumentation.endJobDuration(jobSignpost)
            if lostOwnership {
                logger.warning("Skipping failure cleanup writes for job \(job.jobId): lease reclaimed by orphan recovery (error: \(error))")
                return
            }
            await writeIfStillOwned("outerCatch.increment") {
                try await store.incrementAttemptCount(jobId: job.jobId)
            }
            let updated = lostOwnership ? nil : (try? await store.fetchJob(byId: job.jobId))
            let attempts = updated?.attemptCount ?? job.attemptCount + 1
            if attempts >= Self.maxAttemptCount {
                await writeIfStillOwned("outerCatch.supersede") {
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "superseded",
                        lastErrorCode: "maxAttemptsReached:\(error.localizedDescription)"
                    )
                }
            } else {
                let backoff = Self.exponentialBackoffSeconds(attempt: attempts)
                let nextEligible = Date().timeIntervalSince1970 + backoff
                await writeIfStillOwned("outerCatch.requeue") {
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "failed",
                        nextEligibleAt: nextEligible,
                        lastErrorCode: error.localizedDescription
                    )
                }
            }
            logger.error("Job \(job.jobId) threw: \(error)")
        }

        // Final releaseLease is the load-bearing tail for the
        // happy-path arms (e.g. `.reachedTarget` with all tiers done)
        // that don't early-return. Gated like every other store write
        // because AnalysisStore.releaseLease is a blind UPDATE-by-jobId
        // (not owner-scoped); calling it after losing ownership
        // clears the new owner's lease.
        await writeIfStillOwned("releaseLease.tail") {
            try await store.releaseLease(jobId: job.jobId)
        }
    }

    // MARK: - Lease-aware write helper

    /// Performs `body` only if this scheduler still owns the job's
    /// lease. `lostOwnership` may be flipped to `true` by the renewal
    /// task at any actor suspension point — so a single early-return
    /// guard before a chain of `await store.X(...)` calls is not
    /// sufficient. Wrap every cleanup-path store call so the check is
    /// re-evaluated immediately before the write.
    ///
    /// `body` errors are caught and logged here (matches the prior
    /// `do { try } catch { logger.error(...) }` pattern) so callers
    /// don't need to wrap each call themselves.
    private func writeIfStillOwned(
        _ what: String,
        _ body: () async throws -> Void
    ) async {
        guard !lostOwnership else { return }
        do {
            try await body()
        } catch is CancellationError {
            logger.warning("Cleanup write [\(what)] cancelled (likely lease reclaim mid-write)")
        } catch {
            logger.error("Failed cleanup write [\(what)]: \(error)")
        }
    }

    // MARK: - Tier Definitions

    /// Returns the next tier's coverage target, or nil if all tiers are complete.
    private func nextTierCoverage(current: Double) -> Double? {
        // Ensure tiers are ascending; skip any that aren't.
        let tiers = [config.defaultT0DepthSeconds, config.t1DepthSeconds, config.t2DepthSeconds]
            .sorted()
        for tier in tiers where tier > current {
            return tier
        }
        return nil
    }

    private func resolveAnalysisAssetId(
        for job: AnalysisJob,
        localAudioURL: LocalAudioURL
    ) async throws -> String {
        if let analysisAssetId = job.analysisAssetId {
            return analysisAssetId
        }

        // Lease-aware writes: this method runs after `leaseRenewalTask` is
        // armed, so any `await` here is a suspension point where the renewer
        // can flip `lostOwnership`. Throw CancellationError on a flip so the
        // existing asset-resolution catch in processJob hits its
        // `guard !lostOwnership` and skips its own cleanup writes too.
        // Orphan recovery (or the new owner) will redo this work cleanly.
        if let existing = try await store.fetchAssetByEpisodeId(job.episodeId) {
            guard !lostOwnership else { throw CancellationError() }
            try await store.updateJobAnalysisAssetId(jobId: job.jobId, analysisAssetId: existing.id)
            return existing.id
        }

        let capabilityJSON: String?
        do {
            let snapshot = await capabilitiesService.currentSnapshot
            let data = try JSONEncoder().encode(snapshot)
            capabilityJSON = String(data: data, encoding: .utf8)
        } catch {
            capabilityJSON = nil
        }

        let assetId = UUID().uuidString
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: job.episodeId,
            assetFingerprint: job.sourceFingerprint,
            weakFingerprint: nil,
            sourceURL: localAudioURL.absoluteString,
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: PreAnalysisConfig.analysisVersion,
            capabilitySnapshot: capabilityJSON
        )
        guard !lostOwnership else { throw CancellationError() }
        try await store.insertAsset(asset)
        guard !lostOwnership else { throw CancellationError() }
        try await store.updateJobAnalysisAssetId(jobId: job.jobId, analysisAssetId: assetId)
        return assetId
    }

    static func shouldRetryCoverageInsufficient(job: AnalysisJob, outcome: AnalysisOutcome) -> Bool {
        let epsilon = coverageProgressEpsilon
        let featureAdvanced = outcome.featureCoverageSec > job.featureCoverageSec + epsilon
        let transcriptAdvanced = outcome.transcriptCoverageSec > job.transcriptCoverageSec + epsilon
        let cueAdvanced = outcome.cueCoverageSec > job.cueCoverageSec + epsilon
        let cuesCreated = outcome.newCueCount > 0

        return featureAdvanced || transcriptAdvanced || cueAdvanced || cuesCreated
    }
}

// MARK: - AnalysisJob → SchedulerLane derivation (playhead-r835)

extension AnalysisJob {
    /// Maps the job's `priority` into the scheduler's three-lane partition.
    ///
    /// The boundaries are:
    /// - `priority >= 20`       → `.now`
    /// - `priority 1..<20`      → `.soon`
    /// - `priority <= 0`        → `.background`
    ///
    /// These ranges are the ones spelled out in the playhead-r835 bead
    /// spec. Keep the ranges contiguous and non-overlapping — every integer
    /// priority must map to exactly one lane.
    var schedulerLane: AnalysisWorkScheduler.SchedulerLane {
        switch priority {
        case 20...:    return .now
        case 1..<20:   return .soon
        default:       return .background
        }
    }
}
