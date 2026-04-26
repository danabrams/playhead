// AnalysisJobReconciler.swift
// Repairs stale, blocked, and broken job state in the analysis_jobs table.
// Runs at app launch and at periodic reconciliation points.

import Foundation
import OSLog

// MARK: - ReconciliationReport

struct ReconciliationReport: Sendable {
    let expiredLeasesRecovered: Int
    /// playhead-btwk: rows in active analysis-jobs states (`running`,
    /// `paused`, `backfill`) whose `schedulerEpoch` predates the current
    /// session were flipped back to `queued` so the scheduler can dispatch
    /// them. Counted distinctly from `expiredLeasesRecovered` because the
    /// stranding shape is different — those rows have no live lease at all
    /// (build replacement; cleanly-paused row from a dead process), and
    /// `recoverExpiredLeases` cannot see them.
    let recoveredStrandedSessionJobs: Int
    let missingFilesUnblocked: Int
    let missingFilesStillBlocked: Int
    let modelsUnblocked: Int
    let staleVersionsSuperseded: Int
    /// playhead-5uvz.8 (Gap-10): jobs re-enqueued at the current
    /// analysis version inside the same `reconcile()` pass that
    /// superseded their predecessors. Counted distinctly from
    /// `unEnqueuedDownloadsCreated` (step 7) so callers can tell
    /// version-bump churn apart from "new download discovered."
    let staleVersionsReenqueued: Int
    let completedJobsGarbageCollected: Int
    let failedJobsBackedOff: Int
    let unEnqueuedDownloadsCreated: Int
}

// MARK: - AnalysisJobReconciler

actor AnalysisJobReconciler {
    private let store: AnalysisStore
    private let downloadManager: any DownloadProviding
    private let capabilitiesService: any CapabilitiesProviding
    private let config: PreAnalysisConfig
    private let logger = Logger(subsystem: "com.playhead", category: "JobReconciler")
    private var isReconciling = false

    /// The current analysis version. Jobs whose workKey encodes a different
    /// version are considered stale and will be superseded.
    static var currentAnalysisVersion: Int { PreAnalysisConfig.analysisVersion }

    init(
        store: AnalysisStore,
        downloadManager: any DownloadProviding,
        capabilitiesService: any CapabilitiesProviding,
        config: PreAnalysisConfig = .load()
    ) {
        self.store = store
        self.downloadManager = downloadManager
        self.capabilitiesService = capabilitiesService
        self.config = config
    }

    // MARK: - Reconcile

    func reconcile() async throws -> ReconciliationReport {
        guard !isReconciling else {
            logger.info("Reconciliation already in progress, skipping")
            return ReconciliationReport(
                expiredLeasesRecovered: 0,
                recoveredStrandedSessionJobs: 0,
                missingFilesUnblocked: 0,
                missingFilesStillBlocked: 0, modelsUnblocked: 0,
                staleVersionsSuperseded: 0, staleVersionsReenqueued: 0,
                completedJobsGarbageCollected: 0,
                failedJobsBackedOff: 0, unEnqueuedDownloadsCreated: 0
            )
        }
        isReconciling = true
        defer { isReconciling = false }

        let step1 = try await recoverExpiredLeases()
        // playhead-btwk: must run AFTER `recoverExpiredLeases` so that the
        // expired-lease sweep gets first dibs on rows it can claim (and
        // increments `attemptCount` along the way) — and BEFORE
        // `unblockMissingFiles` so that `state='blocked:missingFile'` rows
        // continue to be handled by step 3 only. The new sweep operates on
        // disjoint state values (`running`/`paused`/`backfill`), so it
        // cannot accidentally collide with either neighbor.
        let stepStranded = try await recoverStrandedSessionJobs()
        let step2 = try await unblockMissingFiles()
        let step3 = try await unblockModelUnavailable()
        let step4 = try await supersedeStaleVersions()
        let step5 = try await garbageCollectOldJobs()
        let step6 = try await backoffFailedJobs()
        let step7 = try await discoverUnEnqueuedDownloads()

        let report = ReconciliationReport(
            expiredLeasesRecovered: step1,
            recoveredStrandedSessionJobs: stepStranded,
            missingFilesUnblocked: step2.unblocked,
            missingFilesStillBlocked: step2.stillBlocked,
            modelsUnblocked: step3,
            staleVersionsSuperseded: step4.superseded,
            staleVersionsReenqueued: step4.reenqueued,
            completedJobsGarbageCollected: step5,
            failedJobsBackedOff: step6,
            unEnqueuedDownloadsCreated: step7
        )

        logger.info("""
        Reconciliation complete: \
        expiredLeases=\(report.expiredLeasesRecovered), \
        strandedSessionJobs=\(report.recoveredStrandedSessionJobs), \
        missingFilesUnblocked=\(report.missingFilesUnblocked), \
        missingFilesStillBlocked=\(report.missingFilesStillBlocked), \
        modelsUnblocked=\(report.modelsUnblocked), \
        staleVersions=\(report.staleVersionsSuperseded), \
        staleReenqueued=\(report.staleVersionsReenqueued), \
        gc=\(report.completedJobsGarbageCollected), \
        backoff=\(report.failedJobsBackedOff), \
        newJobs=\(report.unEnqueuedDownloadsCreated)
        """)

        return report
    }

    // MARK: - Step 1: Recover expired leases

    /// Blind-sweep fallback for stranded leases.
    ///
    /// playhead-5uvz.2 (Gap-2): `PlayheadRuntime.startSchedulerLoop`
    /// now calls `AnalysisCoordinator.recoverOrphans` BEFORE
    /// `reconcile()`, so this step is no longer the primary
    /// cold-launch reaper. The journal-aware path claims every orphan
    /// whose `work_journal` row routes its decision (terminal → clear,
    /// else requeue with fresh epoch/generation + Now→Soon demotion).
    /// This sweep stays as cheap insurance for the residual classes
    /// the journal-aware path skips:
    ///   - rows whose journal row carries an epoch >
    ///     `_meta.scheduler_epoch` (corruption-skip branch in
    ///     `recoverOrphans`),
    ///   - rows whose per-orphan `try` body threw (the journal-aware
    ///     path swallows per-job errors and continues, leaving those
    ///     rows for this fallback),
    ///   - and pre-5uvz.1 rows that never wrote a journal trail
    ///     (`fetchLastWorkJournalEntry` returns nil → resume branch
    ///     requeues them too, but this sweep catches anything missed).
    /// The cost is a few extra UPDATEs against rows the journal-aware
    /// path already cleared; SQLite makes that essentially free.
    private func recoverExpiredLeases() async throws -> Int {
        let now = Date().timeIntervalSince1970
        let expired = try await store.fetchJobsWithExpiredLeases(before: now)
        // Recover any job with an expired lease — the lease proves it was being processed.
        let recoverable = expired.filter { $0.state == "running" || $0.state == "queued" || $0.state == "paused" }
        for job in recoverable {
            try await store.recoverExpiredLease(jobId: job.jobId)
        }
        if !recoverable.isEmpty {
            logger.info("Recovered \(recoverable.count) expired lease(s)")
        }
        return recoverable.count
    }

    // MARK: - Step 1.5: Recover stranded prior-session jobs (playhead-btwk)

    /// Sweeps rows that survived a prior process in an active analysis-jobs
    /// state (`running`, `paused`, or — defensively — `backfill`) without a
    /// live lease. Stranded rows are flipped back to `queued` so the
    /// scheduler's `fetchNextEligibleJob` can pick them up.
    ///
    /// Stranding shape this catches (and that the existing reconciler steps
    /// missed before this bead landed):
    ///   - **Build replacement.** A fresh build replaces the running app.
    ///     Rows that were `state='running'` in the prior process are still
    ///     `running` after launch, but their lease — if it was set at all —
    ///     is owned by a process that no longer exists. `recoverExpiredLeases`
    ///     only sees rows whose lease is set-but-expired; rows whose lease was
    ///     released cleanly during a graceful pause have `leaseOwner IS NULL`
    ///     and slip past it. `fetchNextEligibleJob` only dispatches
    ///     `queued`/`paused`/`failed`, so a `running` row stays invisible.
    ///   - **Cleanly-paused row from a dead session.** `paused` rows are
    ///     dispatch-eligible in principle, but the schedule loop has to
    ///     actually be running to pick them up. After a build replacement
    ///     the rows that were waiting on a tier advance never get touched —
    ///     this sweep flips them to `queued` so they re-enter the dispatch
    ///     queue with a clean slate.
    ///
    /// Why it sits between `recoverExpiredLeases` and `unblockMissingFiles`:
    ///   - **After step 1**: step 1 is the lease-aware path and increments
    ///     `attemptCount` to feed exponential backoff. Letting it run first
    ///     keeps the attempt-count semantics intact for rows whose lease was
    ///     genuinely held until the process died. The new sweep then picks
    ///     up only the remaining "no live lease" survivors, which is the
    ///     additive case that needs handling.
    ///   - **Before step 2**: `unblockMissingFiles` operates on
    ///     `state='blocked:missingFile'` only, so the order is structurally
    ///     non-interacting. We sit ahead of it so the report counters land
    ///     in launch order and any future per-row logging in step 2 sees a
    ///     row set already cleaned of stranded `running` outliers.
    ///
    /// Coverage progress (`featureCoverageSec`, `transcriptCoverageSec`,
    /// `cueCoverageSec`) and `attemptCount` are intentionally preserved — we
    /// resume from where the prior session left off rather than re-running
    /// already-completed work or penalizing the row for an outage.
    /// `lastErrorCode` is cleared because any error code from the prior
    /// session is no longer informative.
    ///
    /// Telemetry: one `logger.info` per recovered row plus a summary line.
    /// The reconciler uses OSLog throughout (matches `recoverExpiredLeases`,
    /// `unblockMissingFiles`, etc.), so callers diagnosing a stranded fleet
    /// can grep `JobReconciler` in Console for `stranded_session_recovered`
    /// markers.
    private func recoverStrandedSessionJobs() async throws -> Int {
        let now = Date().timeIntervalSince1970
        let currentEpoch = (try await store.fetchSchedulerEpoch()) ?? 0
        let stranded = try await store.fetchStrandedActiveJobs(
            now: now,
            currentEpoch: currentEpoch
        )
        guard !stranded.isEmpty else { return 0 }

        for job in stranded {
            try await store.recoverStrandedActiveJob(
                jobId: job.jobId,
                newSchedulerEpoch: currentEpoch,
                now: now
            )
            logger.info("""
            stranded_session_recovered \
            jobId=\(job.jobId) \
            episodeId=\(job.episodeId) \
            jobType=\(job.jobType) \
            fromState=\(job.state) \
            priorEpoch=\(job.schedulerEpoch) \
            currentEpoch=\(currentEpoch) \
            featureCoverageSec=\(job.featureCoverageSec) \
            transcriptCoverageSec=\(job.transcriptCoverageSec)
            """)
        }
        logger.info("Recovered \(stranded.count) stranded prior-session job(s)")
        return stranded.count
    }

    // MARK: - Step 2: Unblock missingFile jobs

    private func unblockMissingFiles() async throws -> (unblocked: Int, stillBlocked: Int) {
        let blocked = try await store.fetchJobsByState("blocked:missingFile")
        var unblocked = 0
        var stillBlocked = 0
        for job in blocked {
            let url = await downloadManager.cachedFileURL(for: job.episodeId)
            if url != nil {
                try await store.updateJobState(jobId: job.jobId, state: "queued")
                unblocked += 1
            } else {
                stillBlocked += 1
            }
        }
        if unblocked > 0 {
            logger.info("Unblocked \(unblocked) missingFile job(s)")
        }
        return (unblocked, stillBlocked)
    }

    // MARK: - Step 3: Unblock modelUnavailable jobs

    private func unblockModelUnavailable() async throws -> Int {
        let blocked = try await store.fetchJobsByState("blocked:modelUnavailable")
        guard !blocked.isEmpty else { return 0 }

        let snapshot = await capabilitiesService.currentSnapshot
        guard snapshot.canUseFoundationModels else { return 0 }

        let ids = blocked.map(\.jobId)
        try await store.batchUpdateJobState(jobIds: ids, state: "queued")
        logger.info("Unblocked \(ids.count) modelUnavailable job(s)")
        return ids.count
    }

    // MARK: - Step 4: Supersede stale versions

    /// Marks every non-terminal job whose `workKey` encodes a stale
    /// `analysisVersion` as `superseded`, then enqueues a fresh
    /// replacement at the current version in the same pass.
    ///
    /// playhead-5uvz.8 (Gap-10): without the in-pass re-enqueue,
    /// in-process analysis-version bumps (test harness, hot config) had
    /// to wait for the next `reconcile()` call before step 7
    /// (`discoverUnEnqueuedDownloads`) noticed the episode had no active
    /// job. Cold launch happened to work because step 7 runs in the
    /// same pass and `fetchActiveJobEpisodeIds` filters
    /// `superseded`/`complete` out of the active set — but the
    /// "same-pass re-enqueue" was incidental, not contractual. This
    /// method now explicitly mints a `queued` row at
    /// `currentAnalysisVersion` for every superseded predecessor whose
    /// download is still cached, preserving correlated fields
    /// (`analysisAssetId`, `podcastId`, `priority`, `downloadId`) and
    /// resetting attempt/error/lease state.
    ///
    /// Returns the count of superseded rows AND the count of fresh
    /// `queued` replacements minted in the same pass. The replacement
    /// count is reported separately on `ReconciliationReport`
    /// (`staleVersionsReenqueued`) to keep version-bump churn visible
    /// against step 7's "new download discovered" newJobs counter.
    private func supersedeStaleVersions() async throws -> (superseded: Int, reenqueued: Int) {
        // Fetch all non-terminal jobs and check their workKey version.
        let allStates = ["queued", "running", "paused",
                         "blocked:missingFile", "blocked:modelUnavailable", "failed"]
        var superseded = 0
        var reenqueued = 0
        // Track episodes already re-enqueued in this pass so multiple
        // stale rows for the same episode (same workKey is unique, but
        // different jobTypes can coexist) only produce one replacement.
        var reenqueuedEpisodes = Set<String>()
        for state in allStates {
            let jobs = try await store.fetchJobsByState(state)
            for job in jobs {
                let version = parseVersionFromWorkKey(job.workKey)
                guard let version, version != Self.currentAnalysisVersion else { continue }
                try await store.updateJobState(jobId: job.jobId, state: "superseded")
                superseded += 1
                if !reenqueuedEpisodes.contains(job.episodeId),
                   try await enqueueReplacement(for: job) {
                    reenqueuedEpisodes.insert(job.episodeId)
                    reenqueued += 1
                }
            }
        }
        if superseded > 0 {
            logger.info("""
            Superseded \(superseded) stale-version job(s); \
            re-enqueued \(reenqueued) at v\(Self.currentAnalysisVersion)
            """)
        }
        return (superseded, reenqueued)
    }

    /// Inserts a fresh `queued` replacement for a job we just
    /// superseded. Returns `true` if a new row was inserted, `false` if
    /// no replacement was needed (download no longer cached, or a row
    /// at the new workKey already exists — `INSERT OR IGNORE` semantics
    /// in `AnalysisStore.insertJob`). Caller has already marked the
    /// predecessor `superseded`.
    private func enqueueReplacement(for staleJob: AnalysisJob) async throws -> Bool {
        // Only replace pre-analysis jobs. Playback/backfill rows are
        // fanned out by the playback or backfill pipelines themselves;
        // re-creating one here would race with their own enqueue paths.
        guard staleJob.jobType == "preAnalysis" else { return false }
        // Don't mint a job for an episode whose download no longer
        // exists; step 7 has the same skip and we match its semantics.
        guard await downloadManager.cachedFileURL(for: staleJob.episodeId) != nil else {
            return false
        }
        let now = Date().timeIntervalSince1970
        let workKey = AnalysisJob.computeWorkKey(
            fingerprint: staleJob.sourceFingerprint,
            analysisVersion: Self.currentAnalysisVersion,
            jobType: staleJob.jobType
        )
        let replacement = AnalysisJob(
            jobId: UUID().uuidString,
            jobType: staleJob.jobType,
            episodeId: staleJob.episodeId,
            podcastId: staleJob.podcastId,
            // Reuse the prior asset row — feature/transcript artifacts
            // already attached to it remain accessible. The runner will
            // re-derive coverage from the asset on its next pass.
            analysisAssetId: staleJob.analysisAssetId,
            workKey: workKey,
            sourceFingerprint: staleJob.sourceFingerprint,
            downloadId: staleJob.downloadId,
            priority: staleJob.priority,
            desiredCoverageSec: staleJob.desiredCoverageSec,
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
        return try await store.insertJob(replacement)
    }

    // MARK: - Step 5: GC old completed/superseded jobs

    private func garbageCollectOldJobs() async throws -> Int {
        let sevenDaysAgo = Date().timeIntervalSince1970 - (7 * 24 * 3600)
        let deleted = try await store.deleteOldJobs(
            olderThan: sevenDaysAgo,
            inStates: ["complete", "superseded"]
        )
        if deleted > 0 {
            logger.info("Garbage collected \(deleted) old job(s)")
        }
        return deleted
    }

    // MARK: - Step 6: Exponential backoff for failed jobs

    private func backoffFailedJobs() async throws -> Int {
        let failed = try await store.fetchJobsByState("failed")
        let needsBackoff = failed.filter { $0.nextEligibleAt == nil }
        let now = Date().timeIntervalSince1970
        for job in needsBackoff {
            let delay = min(pow(2.0, Double(job.attemptCount)) * 60.0, 3600.0)
            let nextEligible = now + delay
            try await store.updateJobState(
                jobId: job.jobId,
                state: "failed",
                nextEligibleAt: nextEligible
            )
        }
        if !needsBackoff.isEmpty {
            logger.info("Applied backoff to \(needsBackoff.count) failed job(s)")
        }
        return needsBackoff.count
    }

    // MARK: - Step 7: Discover un-enqueued downloads

    private func discoverUnEnqueuedDownloads() async throws -> Int {
        let cachedIds = await downloadManager.allCachedEpisodeIds()
        let activeJobIds = try await store.fetchActiveJobEpisodeIds()

        let unEnqueued = cachedIds.subtracting(activeJobIds)
        for episodeId in unEnqueued {
            guard let fp = await downloadManager.fingerprint(for: episodeId) else { continue }
            let workKey = AnalysisJob.computeWorkKey(
                fingerprint: fp.strong ?? fp.weak,
                analysisVersion: Self.currentAnalysisVersion,
                jobType: "preAnalysis"
            )
            let job = AnalysisJob(
                jobId: UUID().uuidString,
                jobType: "preAnalysis",
                episodeId: episodeId,
                podcastId: nil,
                analysisAssetId: nil,
                workKey: workKey,
                sourceFingerprint: fp.strong ?? fp.weak,
                downloadId: episodeId,
                priority: 0,
                desiredCoverageSec: config.defaultT0DepthSeconds,
                featureCoverageSec: 0,
                transcriptCoverageSec: 0,
                cueCoverageSec: 0,
                state: "queued",
                attemptCount: 0,
                nextEligibleAt: nil,
                leaseOwner: nil,
                leaseExpiresAt: nil,
                lastErrorCode: nil,
                createdAt: Date().timeIntervalSince1970,
                updatedAt: Date().timeIntervalSince1970
            )
            try await store.insertJob(job)
        }
        if !unEnqueued.isEmpty {
            logger.info("Created \(unEnqueued.count) job(s) for un-enqueued downloads")
        }
        return unEnqueued.count
    }

    // MARK: - Helpers

    /// Extracts the analysis version from a workKey.
    /// Base format: "fingerprint:version:jobType"
    /// Tier-advanced format: "fingerprint:version:jobType:coverage"
    /// The version is always the component immediately before the jobType token.
    private func parseVersionFromWorkKey(_ workKey: String) -> Int? {
        let parts = workKey.split(separator: ":")
        // Find the jobType component (preAnalysis, playback, backfill)
        let jobTypes: Set<Substring> = ["preAnalysis", "playback", "backfill"]
        guard let jobTypeIndex = parts.firstIndex(where: { jobTypes.contains($0) }),
              jobTypeIndex > 0 else { return nil }
        return Int(parts[jobTypeIndex - 1])
    }
}
