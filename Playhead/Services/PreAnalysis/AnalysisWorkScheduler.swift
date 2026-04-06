// AnalysisWorkScheduler.swift
// Eligibility-aware job scheduler for pre-analysis work.
// Selects the highest-priority eligible job under current device constraints
// (charging, thermal) and delegates execution to AnalysisJobRunner.

import Foundation
import OSLog

actor AnalysisWorkScheduler {
    private let store: AnalysisStore
    private let jobRunner: AnalysisJobRunner
    private let capabilitiesService: CapabilitiesService
    private let downloadManager: any DownloadProviding
    private let config: PreAnalysisConfig
    private let logger = Logger(subsystem: "com.playhead", category: "WorkScheduler")

    private var schedulerTask: Task<Void, Never>?
    private var currentRunningTask: Task<Void, Never>?
    private var currentJobId: String?
    private var currentEpisodeId: String?
    private var shouldCancelCurrentJob = false
    private var leaseRenewalTask: Task<Void, any Error>?
    private static let maxAttemptCount = 5

    private var wakeContinuation: AsyncStream<Void>.Continuation?
    private var wakeStream: AsyncStream<Void>

    /// Tracks OSSignposter queue-wait intervals keyed by jobId.
    private var queueWaitStates: [String: OSSignpostIntervalState] = [:]

    init(
        store: AnalysisStore,
        jobRunner: AnalysisJobRunner,
        capabilitiesService: CapabilitiesService,
        downloadManager: any DownloadProviding,
        config: PreAnalysisConfig = .load()
    ) {
        self.store = store
        self.jobRunner = jobRunner
        self.capabilitiesService = capabilitiesService
        self.downloadManager = downloadManager
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
    /// If the currently running job matches, cancel it to avoid contention.
    func playbackStarted(episodeId: String) async {
        if currentEpisodeId == episodeId {
            shouldCancelCurrentJob = true
            currentRunningTask?.cancel()
            logger.info("Playback preempted pre-analysis for episode \(episodeId)")
        }
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
    func cancelCurrentJob() {
        shouldCancelCurrentJob = true
        currentRunningTask?.cancel()
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

            let snapshot = await capabilitiesService.currentSnapshot
            let now = Date().timeIntervalSince1970

            guard let job = try? await store.fetchNextEligibleJob(
                isCharging: snapshot.isCharging,
                isThermalOk: !snapshot.shouldThrottleAnalysis,
                t0ThresholdSec: config.defaultT0DepthSeconds,
                now: now
            ) else {
                await sleepOrWake(seconds: 5)
                continue
            }

            await processJob(job)
        }
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
        let assetId = job.analysisAssetId ?? job.episodeId
        let request = AnalysisRangeRequest(
            jobId: job.jobId,
            episodeId: job.episodeId,
            podcastId: job.podcastId ?? "",
            analysisAssetId: assetId,
            audioURL: localAudioURL,
            desiredCoverageSec: job.desiredCoverageSec,
            mode: .preRollWarmup,
            outputPolicy: .writeWindowsAndCues,
            priority: .medium
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
                return
            }
            currentRunningTask = nil

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
                        analysisAssetId: job.analysisAssetId,
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
}
