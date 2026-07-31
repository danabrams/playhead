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
    /// stranded-backfill-reaper: rows in `backfill_jobs` that were stuck
    /// in `status='running'` from a prior process and have been flipped
    /// back to `queued` so the next `BackfillJobRunner.runPendingBackfill`
    /// call's M-5 idempotency check can re-enqueue them. Distinct from
    /// `recoveredStrandedSessionJobs` (which acts on the `analysis_jobs`
    /// table, not `backfill_jobs`).
    let strandedBackfillJobsReset: Int
    /// C1 follow-up to Bug 9: same shape as `strandedBackfillJobsReset` but
    /// against the `final_pass_jobs` sibling table. Bug 9 added the reaper
    /// helper but did not wire it; this counter records its yield so a
    /// stranded-final-pass-fleet event leaves a structured trail.
    let strandedFinalPassJobsReset: Int
    /// playhead-gy2s (RC-3): queued `analysis_jobs` rows that were enqueued
    /// under an older scheduler epoch and have been re-stamped to the current
    /// epoch so orphan-recovery routing keys on a consistent
    /// `{generationID, schedulerEpoch}`. Distinct from
    /// `recoveredStrandedSessionJobs` (which flips running/paused/backfill
    /// rows back to queued): these rows are ALREADY queued and dispatchable —
    /// only their routing metadata was stale. Purely a correctness bundle;
    /// dispatch eligibility never consulted `schedulerEpoch`.
    let queuedJobEpochsRestamped: Int
    /// playhead-dqfm: queued background-lane rows promoted into the Soon band
    /// by the scarcity-aware re-prioritization pass because the backlog
    /// exceeded one background window's drain capacity and the row's episode
    /// ranked as next-to-play. Zero whenever the backlog fit the window (not
    /// scarce) or no ranking provider is wired (plain FIFO preserved).
    let scarcityReprioritizedJobs: Int
    /// playhead-onn6: bounded ad-scan re-drive `analysis_jobs` rows minted for
    /// assets that still hold resumable `backfill_jobs` work but had no
    /// non-terminal analysis job to carry it. Zero once every stranded asset has
    /// spent its re-drive budget (`AnalysisWorkScheduler.maxAdScanRedrives`),
    /// which is what makes repeated launches safe.
    let adScanRedrivesMinted: Int

    /// playhead-onn6: how many rows this pass actually RECOVERED — the number the
    /// background-task ledger reports as `jobsCompleted` and uses to decide
    /// `.recoveredWork` vs `.noOp`.
    ///
    /// It lives here, on the type that owns the fields, because it was previously
    /// an inline sum in `BackgroundProcessingService` and a new counter was added
    /// without being added to it: a sweep whose only yield was minting ad-scan
    /// re-drives — real queued, dispatchable work — reported `.noOp` with
    /// `jobsCompleted: 0`, hiding the recovery from the ledger built to make it
    /// visible. A named property next to the fields makes the omission obvious.
    ///
    /// The membership is unchanged from the inline sum it replaces, plus
    /// `adScanRedrivesMinted`. Four counters stay OUT, and each for a reason:
    /// `missingFilesStillBlocked` is a diagnosis rather than a repair;
    /// `staleVersionsSuperseded` retires rows whose replacements are already
    /// counted by `staleVersionsReenqueued`; `completedJobsGarbageCollected` and
    /// `failedJobsBackedOff` remove or delay work rather than recovering it; and
    /// `queuedJobEpochsRestamped` / `scarcityReprioritizedJobs` act on rows that
    /// were already queued and dispatchable — only their metadata moved.
    var recoveredWorkCount: Int {
        expiredLeasesRecovered
            + recoveredStrandedSessionJobs
            + missingFilesUnblocked
            + modelsUnblocked
            + staleVersionsReenqueued
            + unEnqueuedDownloadsCreated
            + strandedBackfillJobsReset
            + strandedFinalPassJobsReset
            + adScanRedrivesMinted
    }
}

// MARK: - AnalysisJobReconciler

actor AnalysisJobReconciler {
    private let store: AnalysisStore
    private let downloadManager: any DownloadProviding
    private let capabilitiesService: any CapabilitiesProviding
    private let config: PreAnalysisConfig
    /// playhead-dqfm: scarcity-aware backfill re-prioritization inputs
    /// (one-window drain capacity + per-episode ranking signals). Injected
    /// post-init via `setBacklogScarcityRanking` once the SwiftData
    /// `ModelContainer` exists (same late-attach shape as the runtime's other
    /// model-container-dependent providers). `nil` = pass no-ops → plain FIFO.
    private var backlogScarcityRanking: (any BacklogScarcityRanking)?
    private let logger = Logger(subsystem: "com.playhead", category: "JobReconciler")
    private var isReconciling = false

    /// The current analysis version. Jobs whose workKey encodes a different
    /// version are considered stale and will be superseded.
    static var currentAnalysisVersion: Int { PreAnalysisConfig.analysisVersion }

    /// playhead-onn6: per-`reconcile()` ceiling on ad-scan re-drive inserts.
    ///
    /// A cap, not a quota: the stranded backlog is drained across launches
    /// rather than dumped into one queue, so a device that accumulated a large
    /// orphaned coverage lane does not wake up to dozens of FM passes competing
    /// with whatever the user actually wants analysed. 8 is roughly one
    /// background window's worth of work at the observed per-episode cost, and
    /// the oldest-first ordering in `fetchAssetIdsWithResumableBackfillJobs`
    /// drains the longest-stranded work first.
    ///
    /// This counts MINTS, not candidates — see
    /// ``maxAdScanRedriveCandidatesPerReconcile``.
    static let maxAdScanRedrivesPerReconcile = 8

    /// playhead-onn6: how many candidate assets one `reconcile()` will EXAMINE
    /// before stopping, regardless of how many it mints.
    ///
    /// Separate from the mint cap because a skipped candidate must not consume a
    /// mint slot. Some candidates are permanently unmintable AND sort to the
    /// FRONT of the oldest-first ordering: on the 2026-07-29 device pull three
    /// assets had every `analysis_jobs` row garbage-collected (they are covered by
    /// `discoverUnEnqueuedDownloads` instead), and they were the three oldest. A
    /// candidate budget would let those three silently consume 3 of the 8 slots on
    /// every launch, forever. Examining is a handful of indexed reads, so this
    /// bound keeps the sweep's cost fixed without letting skips starve it.
    static let maxAdScanRedriveCandidatesPerReconcile = 64

    init(
        store: AnalysisStore,
        downloadManager: any DownloadProviding,
        capabilitiesService: any CapabilitiesProviding,
        config: PreAnalysisConfig = .load(),
        backlogScarcityRanking: (any BacklogScarcityRanking)? = nil
    ) {
        self.store = store
        self.downloadManager = downloadManager
        self.capabilitiesService = capabilitiesService
        self.config = config
        self.backlogScarcityRanking = backlogScarcityRanking
    }

    /// playhead-dqfm: install the scarcity-ranking provider once the SwiftData
    /// `ModelContainer` is available. Idempotent — re-installing replaces the
    /// prior provider. A no-op-preserving default (`nil`) keeps the reconciler
    /// constructible without the provider (preview runtimes, unit tests).
    func setBacklogScarcityRanking(_ ranking: (any BacklogScarcityRanking)?) {
        self.backlogScarcityRanking = ranking
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
                failedJobsBackedOff: 0, unEnqueuedDownloadsCreated: 0,
                strandedBackfillJobsReset: 0,
                strandedFinalPassJobsReset: 0,
                queuedJobEpochsRestamped: 0,
                scarcityReprioritizedJobs: 0,
                adScanRedrivesMinted: 0
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
        // playhead-gy2s (RC-3): re-stamp queued rows minted under an older
        // epoch to the current one. Runs AFTER `recoverStrandedSessionJobs`
        // (which flips stranded running/paused/backfill rows back to queued
        // under the current epoch) so any row it just re-queued is already
        // current and this step is a no-op for it — this step only catches
        // rows that were queued-and-stale all along (the dogfood shape:
        // `_meta.scheduler_epoch=1`, queued jobs at epoch 0).
        let stepRestamped = try await restampQueuedJobEpochs()
        let step2 = try await unblockMissingFiles()
        let step3 = try await unblockModelUnavailable()
        let step4 = try await supersedeStaleVersions()
        let step5 = try await garbageCollectOldJobs()
        let step6 = try await backoffFailedJobs()
        let step7 = try await discoverUnEnqueuedDownloads()
        let stepBackfillReaper = try await reconcileStrandedBackfillJobs()
        let stepFinalPassReaper = try await reconcileStrandedFinalPassJobs()
        // playhead-onn6: AFTER the backfill reaper (rows it just rescued from
        // `running` are now resumable and must be counted) and after step 7 (an
        // episode receiving a fresh job is already excluded as active).
        let stepAdScanRedrive = await mintAdScanRedrives()
        // playhead-dqfm: LAST — runs after `discoverUnEnqueuedDownloads`
        // (step 7) so freshly-minted priority-0 background rows are part of
        // the backlog it ranks, and after the reapers (disjoint tables) so it
        // sees a fully-reconciled `analysis_jobs` set at window entry.
        let stepScarcity = await reprioritizeScarceBacklog()

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
            unEnqueuedDownloadsCreated: step7,
            strandedBackfillJobsReset: stepBackfillReaper,
            strandedFinalPassJobsReset: stepFinalPassReaper,
            queuedJobEpochsRestamped: stepRestamped,
            scarcityReprioritizedJobs: stepScarcity,
            adScanRedrivesMinted: stepAdScanRedrive
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
        newJobs=\(report.unEnqueuedDownloadsCreated), \
        strandedBackfillJobs=\(report.strandedBackfillJobsReset), \
        strandedFinalPassJobs=\(report.strandedFinalPassJobsReset), \
        queuedEpochsRestamped=\(report.queuedJobEpochsRestamped), \
        scarcityReprioritized=\(report.scarcityReprioritizedJobs), \
        adScanRedrivesMinted=\(report.adScanRedrivesMinted)
        """)

        return report
    }

    // MARK: - Step: Re-stamp stale queued-row epochs (playhead-gy2s RC-3)

    /// Re-stamp queued `analysis_jobs` rows whose `schedulerEpoch` predates
    /// the current session so orphan-recovery routing keys on a consistent
    /// `{generationID, schedulerEpoch}`. These rows are already queued and
    /// dispatchable — only their routing metadata was stale (e.g. rows minted
    /// before an epoch bump, or legacy rows enqueued at the epoch-0 sentinel).
    /// Reads `_meta.scheduler_epoch` once at entry, matching the sequencing
    /// contract documented on `recoverStrandedSessionJobs`.
    private func restampQueuedJobEpochs() async throws -> Int {
        let now = Date().timeIntervalSince1970
        let currentEpoch = (try await store.fetchSchedulerEpoch()) ?? 0
        let count = try await store.restampQueuedJobEpochs(to: currentEpoch, now: now)
        if count > 0 {
            logger.info("queued_epoch_restamped count=\(count) currentEpoch=\(currentEpoch)")
        }
        return count
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
    ///
    /// `lastErrorCode` and `nextEligibleAt` are also preserved
    /// (review-followup csp / H2). An earlier revision cleared
    /// `lastErrorCode` on the reasoning that "any error code from the
    /// prior session is no longer informative" — that reasoning was
    /// reversed: the prior session's terminal error is the single most
    /// informative diagnostic we have about why the row stranded, and
    /// pairing it with `nextEligibleAt` keeps the legitimately-earned
    /// exponential-backoff window intact across cold launch. Clearing
    /// either field on recovery would let a row that crashed the
    /// process — exactly the row that should respect cooldown most —
    /// dispatch immediately. See the AnalysisStore-side docstring on
    /// ``AnalysisStore.recoverStrandedActiveJob`` for the persistence
    /// contract these two columns enforce together.
    ///
    /// Telemetry: one `logger.info` per recovered row plus a summary line.
    /// The reconciler uses OSLog throughout (matches `recoverExpiredLeases`,
    /// `unblockMissingFiles`, etc.), so callers diagnosing a stranded fleet
    /// can grep `JobReconciler` in Console for `stranded_session_recovered`
    /// markers.
    private func recoverStrandedSessionJobs() async throws -> Int {
        // Sequential-ordering invariant (review-followup csp / L3):
        // this step reads `_meta.scheduler_epoch` once at entry. Any
        // upstream call that mutates the epoch (today: only the
        // `incrementSchedulerEpoch` that happens before
        // `PlayheadRuntime.startSchedulerLoop` boots the reconciler)
        // MUST have completed before we are invoked. The reconciler
        // sequencing in `reconcile()` above honors this — step 1
        // (`recoverExpiredLeases`) does not touch the epoch, and the
        // bead-btwk wiring guarantees we run after the runtime's
        // launch-time epoch bump. If a future caller drives this
        // function from a path that lets the epoch race the read,
        // the stranded-detection predicate
        // (`schedulerEpoch < currentEpoch`) will momentarily
        // misclassify rows minted in the new epoch as stranded.
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

    /// playhead-8bp2: returns jobs that were ACTUALLY inserted, not candidates
    /// considered.
    ///
    /// `fetchActiveJobEpisodeIds` excludes `complete` and `superseded`, so an
    /// episode whose only job reached either terminal is reported here as
    /// "un-enqueued" on every single pass — but its `workKey` row still exists,
    /// `workKey` is UNIQUE, and `insertJob` is `INSERT OR IGNORE`, so the insert
    /// is silently swallowed forever. This method counted the CANDIDATES and
    /// called them `unEnqueuedDownloadsCreated`, which
    /// ``ReconciliationReport/recoveredWorkCount`` sums and
    /// `BackgroundProcessingService` reports to the background-task ledger as
    /// `jobsCompleted` / `.recoveredWork`. On the 2026-07-30 device pull that
    /// produced a flat `jobsCompleted: 1` on 70 of the 287 `preanalysis_recovery`
    /// runs — 25 of the 33 half-hourly sweeps on 2026-07-28 alone — while 19
    /// assets sat un-progressing. The ledger built to make a stalled fleet
    /// visible was the thing reporting it healthy, which is a large part of why
    /// this read as platform starvation for as long as it did.
    ///
    /// Reporting the true count does not by itself un-stick those episodes —
    /// re-requesting a swallowed `workKey` is playhead-y8f3's forward-looking
    /// half and is deliberately not duplicated here. It stops the swallow from
    /// being invisible, and it stops a sweep whose only "yield" is a swallowed
    /// insert from logging as `.recoveredWork`.
    private func discoverUnEnqueuedDownloads() async throws -> Int {
        let cachedIds = await downloadManager.allCachedEpisodeIds()
        let activeJobIds = try await store.fetchActiveJobEpisodeIds()

        let unEnqueued = cachedIds.subtracting(activeJobIds)
        var created = 0
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
            if try await store.insertJob(job) {
                created += 1
            }
        }
        if created > 0 {
            logger.info("Created \(created) job(s) for un-enqueued downloads")
        }
        let swallowed = unEnqueued.count - created
        if swallowed > 0 {
            logger.info(
                "\(swallowed) cached episode(s) had no active job but an existing workKey swallowed the re-enqueue (see playhead-y8f3)"
            )
        }
        return created
    }

    // MARK: - Step 8: Reap stranded backfill jobs (stranded-backfill-reaper)

    /// Flips every `backfill_jobs` row stuck at `status='running'` back to
    /// `'queued'` so the next `BackfillJobRunner.runPendingBackfill`
    /// invocation can re-enqueue them through the M-5 idempotency check
    /// at `BackfillJobRunner.swift:386-403`.
    ///
    /// **Why a process-restart-implies-strand reaper is sufficient.**
    /// `BackfillJobRunner` writes `status='running'` immediately before
    /// dispatching FM and only clears it on a terminal transition
    /// (`markBackfillJobComplete` / `markBackfillJobFailed` /
    /// `markBackfillJobDeferred`). If a `running` row survives a process
    /// boundary, the prior process necessarily failed to reach any
    /// terminal — there is no in-memory dispatch state that could re-arm
    /// the row, so it stays stuck forever (the captured xcappdata case
    /// pinned `fm-8cbf80fee0a24b99` for 4.8 days). The reconciler runs
    /// exactly once at app launch from `PlayheadRuntime.startSchedulerLoop`
    /// (and once per BGProcessingTask handler invocation), so by the time
    /// this step executes the only living writer is the current process —
    /// which has not yet started a backfill run. A blanket
    /// `UPDATE backfill_jobs SET status='queued' WHERE status='running'`
    /// is therefore safe without an `updatedAt` / `leaseExpiresAt` filter.
    ///
    /// **skeptical-review-cycle-1 H1 / cycle-4 M1 reconciliation.** The
    /// implementation in `AnalysisStore.resetStrandedBackfillJobs` DOES
    /// apply an `updatedAt < strftime('%s','now') - 600` floor. The
    /// "blanket update is safe" reasoning above is the design
    /// invariant; the freshness filter is defence-in-depth against the
    /// edge case where a same-process BG-task reconciler fires while
    /// the foreground runner still holds a lease. The filter strictly
    /// strengthens the invariant — every row the implementation reaps
    /// is also one this design comment authorises reaping; the
    /// implementation just additionally protects rows that are too
    /// fresh to have been crashed-and-abandoned.
    ///
    /// **Re-driving safety.** The M-5 idempotency check accepts any
    /// non-`.complete` row including `.running` (today the
    /// `markBackfillJobRunning` guard at
    /// `AnalysisStore.swift` accepts `IN ('queued', 'deferred', 'running')`),
    /// so even if a row is somehow observed in `running` between this
    /// reset and the next runner invocation, the runner will not throw.
    /// Resetting to `queued` is strictly more conservative — it forces
    /// the runner to enqueue through the admission controller again,
    /// preserving `progressCursor` / `retryCount` / `deferReason` (the
    /// `markBackfillJobRunning` SET clause does not touch those columns).
    ///
    /// Position in `reconcile()`: runs last because it operates on a
    /// disjoint table (`backfill_jobs` rather than `analysis_jobs`) and
    /// has no ordering interaction with steps 1–7. Placing it at the end
    /// keeps the existing step ordering pinned and makes the new counter
    /// the trailing field in the structured log line.
    private func reconcileStrandedBackfillJobs() async throws -> Int {
        let count = try await store.resetStrandedBackfillJobs()
        if count > 0 {
            logger.info("stranded_backfill_reset count=\(count)")
        }
        return count
    }

    /// C1: sibling reaper for `final_pass_jobs.status='running'` rows
    /// stranded across a process death. Bug 9 introduced the `final_pass_jobs`
    /// table and the `resetStrandedFinalPassJobs` helper but never wired
    /// the helper into `reconcile()`, leaving stranded final-pass rows in
    /// `running` forever. Same H1 freshness-filter semantics as the
    /// backfill sibling — only rows whose `updatedAt` predates
    /// `AnalysisStore.strandedJobFreshnessSeconds` are reset, so a
    /// foreground runner mid-drain is not racy with a same-process
    /// BGProcessingTask reentry.
    private func reconcileStrandedFinalPassJobs() async throws -> Int {
        let count = try await store.resetStrandedFinalPassJobs()
        if count > 0 {
            logger.info("stranded_final_pass_reset count=\(count)")
        }
        return count
    }

    // MARK: - Step: Ad-scan re-drive for the orphaned coverage lane (playhead-onn6)

    /// Mint a bounded ad-scan re-drive for each asset that still holds resumable
    /// `backfill_jobs` work but has no non-terminal `analysis_jobs` row to carry
    /// it. Returns the number of `analysis_jobs` rows inserted.
    ///
    /// **The stranding this repairs.** `backfill_jobs` rows are minted inside
    /// `BackfillJobRunner.runPendingBackfill` and re-driven only by a LATER
    /// invocation of the same function, which only happens when the analysis lane
    /// dispatches a job for that asset. Nothing selected pending rows, so a row
    /// that outlived its invocation was orphaned: on the 2026-07-29 device pull 35
    /// rows sat `queued` across 12 assets — 9 of them assets whose every
    /// `analysis_jobs` row was already terminal, so no pass would ever come. The
    /// terminal-arm mint in `AnalysisWorkScheduler` fixes this going forward; this
    /// step is what reaches the episodes already stranded.
    ///
    /// **Bounds, and why this cannot become a drain loop.** It runs only inside
    /// `reconcile()` — at launch and at explicit reconciliation points, never on a
    /// timer and never on a playback tick — so it cannot busy-spin under the write
    /// lock the way playhead-bbut's `drainEligible` did. Within one pass it is
    /// capped at ``maxAdScanRedrivesPerReconcile`` inserts. Across passes the
    /// budget is the ordinal in the UNIQUE `workKey`
    /// (``AnalysisWorkScheduler/maxAdScanRedrives``), so relaunching the app
    /// repeatedly cannot manufacture more work: once the ordinal is spent,
    /// `nextAdScanRedriveWorkKey` returns `nil` and this step is a no-op.
    ///
    /// **The budget is per ANALYSIS GENERATION, not per episode-for-all-time, and
    /// that is deliberate.** The ledger is the `workKey` string on `analysis_jobs`
    /// rows, and `garbageCollectOldJobs` deletes `complete`/`superseded` rows older
    /// than seven days. Once an episode's whole job history ages out,
    /// `fetchLatestJobForEpisode` returns `nil` and this step goes permanently
    /// quiet for it — UNLESS `discoverUnEnqueuedDownloads` re-mints a base job
    /// because the audio is still downloaded, which is a full fresh analysis of the
    /// episode that would have happened with or without this bead. A new analysis
    /// that AGAIN leaves the scan short should get re-drives; that is the whole
    /// point. So the worst case is a full re-analysis plus at most
    /// ``AnalysisWorkScheduler/maxAdScanRedrives`` extra passes per episode per
    /// seven days, dominated by the re-analysis itself — bounded per unit time, and
    /// gated on the episode still being downloaded AND still measuring short.
    ///
    /// Position in `reconcile()`: after `reconcileStrandedBackfillJobs` so rows
    /// this process just rescued from `running` are counted as resumable, and
    /// after `discoverUnEnqueuedDownloads` so an episode that is getting a fresh
    /// job anyway is already excluded by the active-episode check below.
    ///
    /// **Best-effort by contract**, like `reprioritizeScarceBacklog`: `async`, not
    /// `async throws`, and every store error is logged and swallowed. This is
    /// opportunistic repair. Letting a coverage read fail `reconcile()` would mask
    /// lease recovery — a critical step — and would make a `BGProcessingTask`
    /// report `.failed` for a hiccup in a step whose only job is to top up
    /// coverage. Minting nothing is always safe; the next reconcile retries.
    private func mintAdScanRedrives() async -> Int {
        do {
            return try await mintAdScanRedrivesThrowing()
        } catch {
            logger.warning("ad_scan_redrive sweep failed (skipped): \(error)")
            return 0
        }
    }

    private func mintAdScanRedrivesThrowing() async throws -> Int {
        let assetIds = try await store.fetchAssetIdsWithResumableBackfillJobs(
            limit: Self.maxAdScanRedriveCandidatesPerReconcile
        )
        guard !assetIds.isEmpty else { return 0 }

        // Episodes that already have a non-terminal `analysis_jobs` row will get
        // a pass without our help, and the runner's M-5 branch will resume their
        // coverage-lane rows when it lands. Minting for them would only add a
        // redundant pass.
        let episodesWithPendingWork = try await store.fetchActiveJobEpisodeIds()

        var minted = 0
        for assetId in assetIds {
            guard minted < Self.maxAdScanRedrivesPerReconcile else { break }
            guard let candidate = try await adScanRedriveCandidate(
                assetId: assetId,
                episodesWithPendingWork: episodesWithPendingWork
            ) else {
                continue
            }
            if try await store.insertJob(candidate.job) {
                minted += 1
                logger.info(
                    """
                    ad_scan_redrive_minted asset=\(assetId, privacy: .public) \
                    resumableCoverageJobs=\(candidate.resumableCount) \
                    ordinal=\(candidate.ordinal)
                    """
                )
            }
        }
        if minted > 0 {
            logger.info("ad_scan_redrive_minted total=\(minted)")
        }
        return minted
    }

    private struct AdScanRedriveCandidate {
        let job: AnalysisJob
        let resumableCount: Int
        let ordinal: Int
    }

    /// The per-asset half of the sweep: every reason NOT to mint, in
    /// cheapest-first order, then the row to insert.
    private func adScanRedriveCandidate(
        assetId: String,
        episodesWithPendingWork: Set<String>
    ) async throws -> AdScanRedriveCandidate? {
        guard let asset = try await store.fetchAsset(id: assetId) else { return nil }
        guard !episodesWithPendingWork.contains(asset.episodeId) else { return nil }
        // The re-drive inherits the episode's fingerprint / download / tier from
        // its most recent job row. No row means we cannot name the audio this
        // asset came from, so there is nothing safe to mint. (Those episodes are
        // not abandoned: `discoverUnEnqueuedDownloads` re-mints a full analysis
        // job for them when their download is still cached.)
        guard let latest = try await store.fetchLatestJobForEpisode(asset.episodeId) else {
            return nil
        }
        // The latest job is looked up by EPISODE, and an episode can carry several
        // assets — a re-download mints a new asset while the old one keeps its
        // coverage-lane rows. Minting from a job that belongs to a different asset
        // would stamp the row with the wrong `sourceFingerprint` and `downloadId`
        // while pointing `analysisAssetId` at this one: the stale-fingerprint
        // detector would compare the OTHER asset's fingerprint against the cached
        // audio and never fire, and the re-drive ordinal would be charged to the
        // other asset's work key, silently spending ITS budget. Require the
        // identities to agree.
        guard latest.analysisAssetId == assetId else { return nil }
        guard let workKey = AnalysisWorkScheduler.nextAdScanRedriveWorkKey(for: latest) else {
            return nil
        }
        // Same guard `enqueueReplacement` uses: never mint a pass for an episode
        // whose audio is gone. Without it, a stale coverage-lane row whose media
        // was deleted would mint work that can only fail.
        guard await downloadManager.cachedFileURL(for: asset.episodeId) != nil else {
            return nil
        }

        let adScanFraction = try await store
            .fetchCoverageSummariesByAssetIds([assetId])[assetId]?
            .adScanFraction
        let resumableCount = try await store.countResumableBackfillJobs(assetId: assetId)
        guard AnalysisWorkScheduler.shouldMintAdScanRedrive(
            adScanFraction: adScanFraction,
            resumableCoverageJobCount: resumableCount
        ) else {
            return nil
        }

        let now = Date().timeIntervalSince1970
        let redrive = AnalysisJob(
            jobId: UUID().uuidString,
            jobType: latest.jobType,
            episodeId: asset.episodeId,
            podcastId: latest.podcastId,
            analysisAssetId: assetId,
            workKey: workKey,
            sourceFingerprint: latest.sourceFingerprint,
            downloadId: latest.downloadId,
            // Background lane: repair work never preempts what the user is
            // waiting on.
            priority: 0,
            desiredCoverageSec: latest.desiredCoverageSec,
            // Carry the predecessor's coverage so a no-op pass reads as
            // no-progress rather than as fresh advancement.
            featureCoverageSec: latest.featureCoverageSec,
            transcriptCoverageSec: latest.transcriptCoverageSec,
            cueCoverageSec: latest.cueCoverageSec,
            state: "queued",
            attemptCount: 0,
            nextEligibleAt: nil,
            leaseOwner: nil,
            leaseExpiresAt: nil,
            lastErrorCode: nil,
            createdAt: now,
            updatedAt: now
        )
        return AdScanRedriveCandidate(
            job: redrive,
            resumableCount: resumableCount,
            ordinal: AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: workKey) ?? 0
        )
    }

    // MARK: - Step: Scarcity-aware backfill re-prioritization (playhead-dqfm)

    /// When the queued Background-lane backlog exceeds one background
    /// window's drain capacity, bump the next-to-play episodes out of the
    /// plain-FIFO Background band and into a low Soon sub-range so a scarce
    /// grant covers what the user will actually play. When the backlog fits
    /// the window (not scarce) — or no ranking provider is wired — this is a
    /// pure no-op and the queue stays plain FIFO.
    ///
    /// Best-effort by contract: it is `async` (not `async throws`) and
    /// swallows every provider/store error, because a re-ranking hiccup must
    /// never fail `reconcile()` or block the drain that follows it at window
    /// entry. Falling back to plain FIFO is always safe.
    private func reprioritizeScarceBacklog() async -> Int {
        guard let ranking = backlogScarcityRanking else { return 0 }
        do {
            guard let capacity = await ranking.currentWindowDrainCapacity(),
                  capacity >= 0 else { return 0 }
            // The backlog that competes for a fresh grant is the QUEUED
            // Background-lane rows (priority <= 0). Running/paused rows are
            // already in-flight or gated and are not what selection picks up
            // first, so they neither count toward scarcity nor get re-ranked.
            let queued = try await store.fetchJobsByState("queued")
            let backlog = queued.filter { $0.schedulerLane == .background }
            guard backlog.count > capacity else { return 0 } // not scarce → unchanged FIFO

            let signals = await ranking.rankingSignals(forEpisodeIds: backlog.map(\.episodeId))
            let candidates = backlog.map { job in
                ScarcityReprioritizer.Candidate(
                    jobId: job.jobId,
                    episodeId: job.episodeId,
                    priority: job.priority,
                    createdAt: job.createdAt,
                    signals: signals[job.episodeId] ?? BacklogRankingSignals()
                )
            }
            let bumps = ScarcityReprioritizer.plan(candidates: candidates, drainCapacity: capacity)
            var applied = 0
            for bump in bumps {
                try await store.updateJobPriority(jobId: bump.jobId, priority: bump.newPriority)
                applied += 1
            }
            if applied > 0 {
                logger.info("scarcity_reprioritized applied=\(applied) backlog=\(backlog.count) capacity=\(capacity)")
            }
            return applied
        } catch {
            logger.warning("scarcity reprioritization skipped: \(error)")
            return 0
        }
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
