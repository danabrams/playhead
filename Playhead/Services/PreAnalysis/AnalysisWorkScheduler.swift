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

actor AnalysisWorkScheduler {
    private static let coverageProgressEpsilon = 0.001
    private let store: AnalysisStore
    private let jobRunner: AnalysisJobRunner
    private let capabilitiesService: any CapabilitiesProviding
    private let downloadManager: any DownloadProviding
    private let batteryProvider: any BatteryStateProviding
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
        config: PreAnalysisConfig = .load()
    ) {
        self.store = store
        self.jobRunner = jobRunner
        self.capabilitiesService = capabilitiesService
        self.downloadManager = downloadManager
        self.batteryProvider = batteryProvider
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
        pendingCancelCause = cause
        currentRunningTask?.cancel()
    }

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
                await sleepOrWake(seconds: 30)
                continue
            }

            // Foreground playback owns the shared transcript/backfill pipeline.
            // Do not start deferred pre-analysis work until playback stops.
            if activePlaybackEpisodeId != nil {
                await sleepOrWake(seconds: 5)
                continue
            }

            let admission = await currentLaneAdmission()

            // Critical thermal (or equivalent) pauses every lane, including
            // T0 playback drains. Wait for the next wake/capability change.
            if admission.pauseAllWork {
                await sleepOrWake(seconds: 5)
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
                await sleepOrWake(seconds: 5)
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
                await sleepOrWake(seconds: 30)
                continue
            }

            // Per-lane concurrency cap (playhead-r835). T0 playback jobs are
            // exempt from the Now cap; `canAdmit` encodes that rule. If the
            // cap is saturated, fall back to the standard sleep so we do not
            // re-fetch the same job in a tight loop.
            guard canAdmit(job: job) else {
                logger.info("Skipping job \(job.jobId) — lane \(String(describing: job.schedulerLane), privacy: .public) at capacity")
                await sleepOrWake(seconds: 5)
                continue
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

    /// Evaluate the current `LaneAdmission` from the capabilities snapshot and
    /// a live battery reading. Exposed internally so tests (and integrators
    /// like BackgroundProcessingService) can ask what the scheduler would do
    /// right now without driving the full loop.
    ///
    /// All thermal/battery/low-power reads route through `QualityProfile` —
    /// there are no direct `ProcessInfo.thermalState` or `isLowPowerMode`
    /// reads in this actor.
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
        let leaseExpiry = Date().timeIntervalSince1970 + 300
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

        // End queue-wait signpost interval.
        if let queueState = queueWaitStates.removeValue(forKey: job.jobId) {
            PreAnalysisInstrumentation.endQueueWait(queueState)
        }

        // Lease renewal task: renew every 120s.
        leaseRenewalTask = Task {
            while !Task.isCancelled {
                try await Task.sleep(for: .seconds(120))
                let newExpiry = Date().timeIntervalSince1970 + 300
                try? await self.store.renewLease(jobId: job.jobId, newExpiresAt: newExpiry)
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
            do {
                let backoff = min(pow(2.0, Double(job.attemptCount + 1)) * 60, 3600)
                try await store.updateJobState(
                    jobId: job.jobId,
                    state: "failed",
                    nextEligibleAt: Date().timeIntervalSince1970 + backoff,
                    lastErrorCode: "assetResolution: \(error)"
                )
            } catch {
                logger.error("Failed to update job state after asset resolution error: \(error)")
            }
            try? await store.releaseLease(jobId: job.jobId)
            return
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
            schedulerLane: job.schedulerLane
        )

        let jobSignpost = PreAnalysisInstrumentation.beginJobDuration(jobId: job.jobId)
        do {
            guard !shouldCancelCurrentJob else {
                PreAnalysisInstrumentation.endJobDuration(jobSignpost)
                try? await store.releaseLease(jobId: job.jobId)
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
                do {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                } catch {
                    logger.error("Failed to update job state: \(error)")
                }
                try? await store.releaseLease(jobId: job.jobId)
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

            // Update progress in the store.
            do {
                try await store.updateJobProgress(
                    jobId: job.jobId,
                    featureCoverageSec: outcome.featureCoverageSec,
                    transcriptCoverageSec: outcome.transcriptCoverageSec,
                    cueCoverageSec: outcome.cueCoverageSec
                )
            } catch {
                logger.error("Failed to update job progress: \(error)")
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
                    try await store.insertJob(nextJob)
                    try await store.updateJobState(jobId: job.jobId, state: "complete")
                    PreAnalysisInstrumentation.logTierCompletion(tier: "\(Int(job.desiredCoverageSec))s", completed: true)
                    logger.info("Tier advancement: \(job.desiredCoverageSec)s -> \(nextCoverage)s for episode \(job.episodeId)")
                } else {
                    try await store.updateJobState(jobId: job.jobId, state: "complete")
                    PreAnalysisInstrumentation.logTierCompletion(tier: "\(Int(job.desiredCoverageSec))s", completed: true)
                    logger.info("Job \(job.jobId) complete (all tiers done)")
                }

            case .reachedTarget:
                // Re-queue, but with a guard against infinite loops for short episodes
                // or episodes that can never reach the desired coverage.
                do {
                    if !Self.shouldRetryCoverageInsufficient(job: job, outcome: outcome) {
                        try await store.updateJobState(
                            jobId: job.jobId,
                            state: "complete",
                            lastErrorCode: "coverageInsufficient:noProgress"
                        )
                        logger.info("Job \(job.jobId) marked complete after no-progress pass (coverage insufficient)")
                    } else {
                        try await store.incrementAttemptCount(jobId: job.jobId)
                        let updatedJob = try await store.fetchJob(byId: job.jobId)
                        if (updatedJob?.attemptCount ?? 0) >= Self.maxAttemptCount {
                            try await store.updateJobState(
                                jobId: job.jobId,
                                state: "complete",
                                lastErrorCode: "maxAttemptsReached:coverageInsufficient"
                            )
                            logger.info("Job \(job.jobId) marked complete after max attempts (coverage insufficient)")
                        } else {
                            try await store.updateJobState(jobId: job.jobId, state: "queued")
                        }
                    }
                } catch {
                    logger.error("Failed to update job state: \(error)")
                }

            case .blockedByModel:
                let nextEligible = Date().timeIntervalSince1970 + 300
                do {
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "blocked:modelUnavailable",
                        nextEligibleAt: nextEligible
                    )
                } catch {
                    logger.error("Failed to update job state: \(error)")
                }
                logger.info("Job \(job.jobId) blocked: model unavailable, retry in 300s")

            case .pausedForThermal, .memoryPressure:
                let nextEligible = Date().timeIntervalSince1970 + 30
                do {
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "paused",
                        nextEligibleAt: nextEligible
                    )
                } catch {
                    logger.error("Failed to update job state: \(error)")
                }
                logger.info("Job \(job.jobId) paused for thermal/memory, retry in 30s")

            case .failed(let reason):
                do {
                    try await store.incrementAttemptCount(jobId: job.jobId)
                    let updated = try await store.fetchJob(byId: job.jobId)
                    let attempts = updated?.attemptCount ?? job.attemptCount + 1
                    if attempts >= Self.maxAttemptCount {
                        try await store.updateJobState(
                            jobId: job.jobId,
                            state: "superseded",
                            lastErrorCode: "maxAttemptsReached:\(reason)"
                        )
                        logger.warning("Job \(job.jobId) abandoned after \(attempts) attempts: \(reason)")
                    } else {
                        let backoff = min(pow(2.0, Double(attempts)) * 60, 3600)
                        let nextEligible = Date().timeIntervalSince1970 + backoff
                        try await store.updateJobState(
                            jobId: job.jobId,
                            state: "failed",
                            nextEligibleAt: nextEligible,
                            lastErrorCode: reason
                        )
                        logger.warning("Job \(job.jobId) failed: \(reason), attempt \(attempts), backoff \(backoff)s")
                    }
                } catch {
                    logger.error("Failed to update job state: \(error)")
                }

            case .backgroundExpired:
                do {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                } catch {
                    logger.error("Failed to update job state: \(error)")
                }
                logger.info("Job \(job.jobId) background expired, requeued")

            case .cancelledByPlayback:
                do {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                } catch {
                    logger.error("Failed to update job state: \(error)")
                }
                logger.info("Job \(job.jobId) cancelled by playback, requeued")

            case .preempted:
                do {
                    try await store.updateJobState(jobId: job.jobId, state: "queued")
                } catch {
                    logger.error("Failed to update job state: \(error)")
                }
                logger.info("Job \(job.jobId) preempted by higher-lane work, requeued")
            }
        } catch {
            PreAnalysisInstrumentation.endJobDuration(jobSignpost)
            do {
                try await store.incrementAttemptCount(jobId: job.jobId)
                let updated = try await store.fetchJob(byId: job.jobId)
                let attempts = updated?.attemptCount ?? job.attemptCount + 1
                if attempts >= Self.maxAttemptCount {
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "superseded",
                        lastErrorCode: "maxAttemptsReached:\(error.localizedDescription)"
                    )
                } else {
                    let backoff = min(pow(2.0, Double(attempts)) * 60, 3600)
                    let nextEligible = Date().timeIntervalSince1970 + backoff
                    try await store.updateJobState(
                        jobId: job.jobId,
                        state: "failed",
                        nextEligibleAt: nextEligible,
                        lastErrorCode: error.localizedDescription
                    )
                }
            } catch {
                logger.error("Failed to update job state after failure: \(error)")
            }
            logger.error("Job \(job.jobId) threw: \(error)")
        }

        try? await store.releaseLease(jobId: job.jobId)
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

        if let existing = try await store.fetchAssetByEpisodeId(job.episodeId) {
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
        try await store.insertAsset(asset)
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
