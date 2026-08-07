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
    /// playhead-y8f3: bounded retries minted for episodes whose only
    /// `analysis_jobs` row is an ATTEMPT-CAP terminal (`state = 'superseded'`
    /// with a `maxAttemptsReached:*` cause). Those rows own a UNIQUE `workKey`
    /// forever, so every re-enqueue for the episode was swallowed and the
    /// episode was un-analysable until the 7-day GC removed the row. Zero once
    /// each episode has spent ``AnalysisWorkScheduler/maxCapOutRetries``, which
    /// is what makes repeated launches safe.
    let capOutRetriesMinted: Int
    /// playhead-y8f3: re-enqueues that `INSERT OR IGNORE` swallowed and that
    /// this pass did NOT convert into a retry — the episode has cached audio and
    /// no active job, but the row holding its `workKey` is not re-requestable
    /// (still cooling, budget spent, fully transcribed, or a genuine
    /// supersession rather than a cap-out).
    ///
    /// A DIAGNOSIS, not a repair, so it is deliberately absent from
    /// ``recoveredWorkCount`` — same reasoning as `missingFilesStillBlocked`.
    /// It is here because "`INSERT OR IGNORE` reported success while doing
    /// nothing" is a defect shape this project keeps hitting, and a count that
    /// only ever appears in a log line is one nobody can assert on.
    let reEnqueuesSwallowed: Int
    /// playhead-fil5: durable `backfill_jobs` scan CLAIMS minted for assets that
    /// reached the transcript finalize floor with a short ad scan and NO
    /// coverage-lane row of any kind. Those assets are invisible to
    /// `adScanRedrivesMinted`'s candidate query by construction (it selects on
    /// the state of rows that do not exist), which is why 4 of the 12 episodes
    /// on the 2026-08-03 device pull had no path to a scan at all — 48E903D7,
    /// FCDDB309, 4FF3A238 and 2C5C3699, the set
    /// ``AnalysisStore/fetchAssetIdsMissingCoverageLaneJobs(limit:offset:)``
    /// returns verbatim against that pull. The bead's own text says three; it
    /// counts only the assets it judged transcribed, and the count corrected in
    /// R4 is four. Two of them clear the transcript floor and are minted
    /// (FCDDB309, 4FF3A238); the other two are refused here and stay
    /// playhead-9y9e's problem.
    ///
    /// Deliberately OUT of ``recoveredWorkCount``. A claim is a request, not a
    /// dispatchable row — and because this step runs immediately before the
    /// re-drive sweep in the same pass, the work it unblocks is already counted
    /// as `adScanRedrivesMinted`. Adding it would double-count the same repair.
    let semanticScanClaimsMinted: Int

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
    /// playhead-y8f3 adds `capOutRetriesMinted` (a queued, dispatchable row that
    /// did not exist before) and keeps `reEnqueuesSwallowed` out, for the same
    /// reason `missingFilesStillBlocked` is out: it reports work that did NOT
    /// happen.
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
            + capOutRetriesMinted
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
    /// playhead-fil5: rotating read cursor for the semantic-scan-claim sweep's
    /// candidate window. See ``nextSemanticScanClaimCandidates()`` for why a
    /// fixed `LIMIT` on an oldest-first query whose rejects never leave is a
    /// wall rather than a rate limit.
    private var semanticScanClaimSweepOffset = 0

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

    /// playhead-fil5: per-`reconcile()` ceiling on scan-CLAIM inserts.
    ///
    /// Deliberately the same 8 as ``maxAdScanRedrivesPerReconcile``, because a
    /// claim minted here is what the very next step turns into a re-drive:
    /// minting more claims than that step can act on would only pull work
    /// forward into a queue it cannot drain, and the claims would still be there
    /// on the next pass.
    static let maxSemanticScanClaimsPerReconcile = 8

    /// playhead-fil5: how many zero-row assets one `reconcile()` will EXAMINE.
    ///
    /// Lower than ``maxAdScanRedriveCandidatesPerReconcile`` (64) because a
    /// candidate here is more expensive to reject — a rejected re-drive
    /// candidate costs indexed reads, while a candidate that clears the
    /// transcript and coverage floors here costs a full
    /// `fetchTranscriptChunks` so the claim can name the transcript version the
    /// runner would derive. 24 covers six times the largest zero-row population
    /// yet measured in the field (4 of 12 assets, 2026-08-03).
    ///
    /// **It is a window into the candidate list, not a prefix of it.** Rejects
    /// write nothing and so stay candidates forever, which on a fixed
    /// oldest-first `LIMIT` would let a full window of never-claimable assets
    /// starve everything behind them permanently. ``nextSemanticScanClaimCandidates()``
    /// rotates the read cursor for exactly that reason; without it "a larger
    /// backlog drains across launches" would be a claim the mechanism cannot
    /// make.
    static let maxSemanticScanClaimCandidatesPerReconcile = 24

    /// playhead-y8f3: time source for the cap-out retry cooldown and the
    /// timestamps on the rows step 7 mints. Injected because
    /// ``AnalysisWorkScheduler/capOutRetryCooldownSeconds`` is an hour, and a
    /// test that proves the retry chain TERMINATES has to cross several of them
    /// — sleeping through that, or asserting only the first cycle, would be a
    /// weaker claim than the one this bead needs to make.
    ///
    /// Deliberately narrow: the other steps still read the wall clock directly.
    /// Converting all eleven `Date()` reads in this file would be churn in code
    /// this bead does not otherwise touch, and every one of them is already
    /// covered by tests that seed timestamps relative to `Date()`.
    private let clock: @Sendable () -> Date

    init(
        store: AnalysisStore,
        downloadManager: any DownloadProviding,
        capabilitiesService: any CapabilitiesProviding,
        config: PreAnalysisConfig = .load(),
        backlogScarcityRanking: (any BacklogScarcityRanking)? = nil,
        clock: @escaping @Sendable () -> Date = { Date() }
    ) {
        self.store = store
        self.downloadManager = downloadManager
        self.capabilitiesService = capabilitiesService
        self.config = config
        self.backlogScarcityRanking = backlogScarcityRanking
        self.clock = clock
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
                adScanRedrivesMinted: 0,
                capOutRetriesMinted: 0,
                reEnqueuesSwallowed: 0,
                semanticScanClaimsMinted: 0
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
        // playhead-fil5: IMMEDIATELY BEFORE the re-drive sweep, and that
        // ordering is the whole design. This step gives a zero-row asset the
        // durable claim it never had; the very next step is the sweep that
        // selects on exactly those rows. Run after, and the claim would sit
        // until the next launch before anything acted on it.
        let stepScanClaims = await mintSemanticScanClaims()
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
            unEnqueuedDownloadsCreated: step7.created,
            strandedBackfillJobsReset: stepBackfillReaper,
            strandedFinalPassJobsReset: stepFinalPassReaper,
            queuedJobEpochsRestamped: stepRestamped,
            scarcityReprioritizedJobs: stepScarcity,
            adScanRedrivesMinted: stepAdScanRedrive,
            capOutRetriesMinted: step7.capOutRetriesMinted,
            reEnqueuesSwallowed: step7.swallowed,
            semanticScanClaimsMinted: stepScanClaims
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
        adScanRedrivesMinted=\(report.adScanRedrivesMinted), \
        capOutRetriesMinted=\(report.capOutRetriesMinted), \
        reEnqueuesSwallowed=\(report.reEnqueuesSwallowed), \
        semanticScanClaimsMinted=\(report.semanticScanClaimsMinted)
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
    /// is silently swallowed until step 5's 7-day GC deletes that terminal row
    /// (`garbageCollectOldJobs`), at which point this method's insert finally
    /// takes and the episode restarts the tier ladder from
    /// `defaultT0DepthSeconds`. So the swallow is weekly, not forever — every
    /// sweep in between reports a discovery it did not make. This method counted
    /// the CANDIDATES and
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
    /// playhead-y8f3 is what now un-sticks the subset of those episodes whose
    /// terminal is an ATTEMPT-CAP dead end: on a swallow, this method asks
    /// ``capOutRetry(episodeId:baseWorkKey:fingerprint:)`` for a bounded retry at
    /// a fresh `workKey` ordinal. The swallowed row itself is never touched —
    /// no state reset, no attempt reset, no migration.
    private func discoverUnEnqueuedDownloads() async throws -> DiscoveryOutcome {
        let cachedIds = await downloadManager.allCachedEpisodeIds()
        let activeJobIds = try await store.fetchActiveJobEpisodeIds()

        let unEnqueued = cachedIds.subtracting(activeJobIds)
        var created = 0
        var attempted = 0
        var capOutRetriesMinted = 0
        for episodeId in unEnqueued {
            // No fingerprint = no workKey = no insert was ever attempted. These
            // must not be counted as swallowed below: a swallow is specifically
            // an insert that collided with an existing `workKey`, and reporting
            // an un-fingerprinted download as one would point the next reader at
            // the wrong bug.
            guard let fp = await downloadManager.fingerprint(for: episodeId) else { continue }
            attempted += 1
            let fingerprint = fp.strong ?? fp.weak
            let workKey = AnalysisJob.computeWorkKey(
                fingerprint: fingerprint,
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
                sourceFingerprint: fingerprint,
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
            } else if await capOutRetry(
                episodeId: episodeId,
                baseWorkKey: workKey,
                fingerprint: fingerprint
            ) {
                capOutRetriesMinted += 1
            }
        }
        if created > 0 {
            logger.info("Created \(created) job(s) for un-enqueued downloads")
        }
        // A swallow that this pass converted into a cap-out retry is still a
        // swallow — the re-enqueue at the base key did nothing — so it is
        // counted here as well as in `capOutRetriesMinted`. The two answer
        // different questions: "did INSERT OR IGNORE silently do nothing" and
        // "did the episode get a dispatchable row anyway".
        let swallowed = attempted - created
        if swallowed > 0 {
            logger.info(
                """
                \(swallowed) cached episode(s) had no active job but an existing \
                workKey swallowed the re-enqueue; \(capOutRetriesMinted) converted \
                into a bounded cap-out retry (playhead-y8f3)
                """
            )
        }
        return DiscoveryOutcome(
            created: created,
            capOutRetriesMinted: capOutRetriesMinted,
            swallowed: swallowed
        )
    }

    /// playhead-8bp2 / playhead-y8f3: what one step-7 sweep did. `created` and
    /// `capOutRetriesMinted` are disjoint (a base-key insert either took or it
    /// did not); `swallowed` counts every insert that did not take, including
    /// the ones a retry rescued.
    private struct DiscoveryOutcome {
        let created: Int
        let capOutRetriesMinted: Int
        let swallowed: Int
    }

    // MARK: - Step 7b: bounded cap-out retry (playhead-y8f3)

    /// Mint one bounded retry for an episode whose base `workKey` is held by a
    /// terminal this rescue owns — an ATTEMPT-CAP terminal (playhead-y8f3) or a
    /// NO-PROGRESS terminal (playhead-dl9k). Returns `true` when a dispatchable
    /// row was inserted.
    ///
    /// **playhead-dl9k: the second road to the same dead end.** The trap below
    /// is described in terms of the attempt cap, but nothing in it is specific
    /// to attempts — it is a property of ANY terminal that leaves
    /// `nextEligibleAt = nil` on the row holding a UNIQUE `workKey`. The
    /// `coverageInsufficient.noProgress` arm does exactly that with
    /// `state = 'complete'`, and reaches it with `attemptCount = 0`, so y8f3's
    /// `superseded` state check excluded it. Measured on the 2026-08-03 device
    /// pull: three assets — `2C5C3699` (transcript watermark 900.0 s of a
    /// 6,925.5 s episode, 13.0 %), `44F076BB` (1,620.0 / 1,977.0, 81.9 %) and
    /// `48E903D7` (2,010.0 / 2,113.1, 95.1 %) — every `analysis_jobs` row
    /// terminal, every one with a `coverageInsufficient:noProgress` tail, and no
    /// path back to a dispatchable row short of the 7-day GC.
    ///
    /// **Why the retry is more productive than it was — stated as the CONDITION
    /// it actually is, because R1 review measured it and it does not hold
    /// everywhere.** A no-progress terminal means a pass ran and moved none of
    /// the four coverage measures, so re-requesting the same target used to be a
    /// guaranteed repeat: the run re-read the covered prefix first and spent the
    /// whole 300 s stage cap before reaching anything new. playhead-mptr (#335)
    /// re-ordered that pass (`TranscriptCoverageIndex.orderingUncoveredFirst`)
    /// so unread audio goes first.
    ///
    /// **"Unread" there used to mean "no `pass = 'fast'` chunk overlaps it",
    /// which is not the same as "never transcribed" — playhead-6r4z fixed that,
    /// and the condition below is what it was measured against.** The index read
    /// `AnalysisStore.fetchFastTranscriptCoveredRanges`, `pass = 'fast'` alone,
    /// so a region a FINAL pass covers — whether because the two passes ran over
    /// disjoint spans or because `TranscriptChunkCanonicalizer` dropped the fast
    /// chunks a final chunk fully contains — had no fast artifact to point at and
    /// sorted as UNREAD. The partition is stable over playhead-proximity order,
    /// so that already-read audio floated to the FRONT, ahead of the audio the
    /// retry was minted for. It was the same fast-only-under-report
    /// playhead-9y9e fixed for the ad-scan bound and did not fix for this index.
    ///
    /// Measured on the 2026-08-03 pull, at 30 s shards, over the three assets
    /// this rescue admits — shards below the watermark that nonetheless sorted
    /// UNREAD, against the genuinely-new audio behind them, BEFORE playhead-6r4z:
    ///
    /// | asset | phantom-unread below watermark | new audio | premise |
    /// |---|---|---|---|
    /// | `44F076BB` | 0 of 54 shards (0 s) | 357 s | held anyway |
    /// | `2C5C3699` | 20 of 30 shards (600 s) | 6,025 s | weakened |
    /// | `48E903D7` | 41 of 67 shards (1,230 s) | 103 s | inverted, 12:1 |
    ///
    /// The honest claim WAS: mptr made the retry productive on assets whose fast
    /// artifacts survive, and left it re-reading a prefix on assets that have had
    /// a final pass — and the rescue was still strictly better than the status
    /// quo, the alternative for all three being stranded until the 7-day GC, with
    /// the cost of being wrong bounded by
    /// ``AnalysisWorkScheduler/maxCapOutRetries`` passes rather than an unbounded
    /// loop. playhead-6r4z widened the index to the canonical union of both
    /// passes, so all three columns now read 0 phantom-unread shards and the
    /// premise holds unconditionally on the assets this rescue admits.
    ///
    /// What this bead still owns unconditionally is the VEHICLE: "uncovered audio
    /// exists" — the condition
    /// ``AnalysisWorkScheduler/outstandingTranscriptTarget(transcriptCoverageSec:tiers:episodeDurationSec:)``
    /// already tests — is what gates the mint, and before this change no pass was
    /// minted at all for these rows however productive it would have been.
    ///
    /// **The trap this opens.** A job that exhausts ``AnalysisWorkScheduler``'s
    /// five attempts is written `state = 'superseded'` with `nextEligibleAt =
    /// nil`. `analysis_jobs.workKey` is UNIQUE, `insertJob` is `INSERT OR
    /// IGNORE`, and `AnalysisJob.computeWorkKey` is stable across launches, so
    /// from that moment every enqueue for the episode is a silent no-op. The one
    /// attempt-reset in the codebase, `AnalysisStore.requeueOrphanedLease`,
    /// rewrites `state = CASE WHEN state = 'running' THEN 'queued' ELSE state
    /// END` — it preserves a superseded row on purpose, because `superseded` is
    /// also how a GENUINE supersession is recorded and those must stay retired.
    /// The episode is therefore un-analysable until step 5's 7-day GC deletes
    /// the row, after which the ladder restarts from scratch at
    /// `defaultT0DepthSeconds`.
    ///
    /// Measured on the 2026-07-31 device pull: 8 rows at this terminal, 6 of
    /// them on episodes still under 95% transcribed, against a queue holding
    /// exactly one dispatchable job.
    ///
    /// **What this does NOT do.** It never touches the swallowed row: no state
    /// change, no `attemptCount` reset, no `workKey` rewrite, no migration. The
    /// terminal stays exactly as it was, and stays the ledger. Everything this
    /// method can do is insert ONE new row at a key nothing owns.
    ///
    /// **Why the bound holds**, in three independent layers:
    ///  1. The ordinal lives in the UNIQUE `workKey`
    ///     (``AnalysisWorkScheduler/maxCapOutRetries``), so the chain cannot
    ///     exceed its cap even if every other guard is wrong, and a duplicated
    ///     or racing sweep is idempotent — the second insert collides and
    ///     returns `false`.
    ///  2. The retry is only minted while the episode has NO active job (this is
    ///     inside the `cachedIds.subtracting(activeJobIds)` loop), so a live or
    ///     queued row can never be double-dispatched, and the budget cannot be
    ///     spent faster than jobs actually reach the cap.
    ///  3. ``AnalysisWorkScheduler/capOutRetryDecision(baseWorkKey:chainTail:nextOrdinal:transcriptCoverageSec:episodeDurationSec:adScanFraction:tiers:now:)``
    ///     declines with a NAMED reason on a cooling terminal, on an episode
    ///     with neither transcript nor ad scan outstanding, and on a genuine
    ///     supersession.
    ///
    /// **Best-effort by contract**, like `mintAdScanRedrives`: a store failure
    /// here is logged and swallowed rather than failing `reconcile()`, whose
    /// critical steps (lease recovery) must not be masked by a hiccup in an
    /// opportunistic top-up. Minting nothing is always safe; the next sweep
    /// retries.
    private func capOutRetry(
        episodeId: String,
        baseWorkKey: String,
        fingerprint: String
    ) async -> Bool {
        do {
            return try await capOutRetryThrowing(
                episodeId: episodeId,
                baseWorkKey: baseWorkKey,
                fingerprint: fingerprint
            )
        } catch {
            logger.warning("cap_out_retry skipped for episode \(episodeId, privacy: .public): \(error)")
            return false
        }
    }

    private func capOutRetryThrowing(
        episodeId: String,
        baseWorkKey: String,
        fingerprint: String
    ) async throws -> Bool {
        // THE TERMINAL THIS DECISION IS ABOUT is the episode's most recent row,
        // not the row that happens to hold the base key. The base key is what
        // was swallowed, but the cap-out can be on a TIER SUCCESSOR: the
        // tier-advance arm mints `<base>:<depth>` keys, so an episode that
        // cleared 90 s and then exhausted its attempts at 300 s leaves the base
        // row `complete` and the real terminal one row further along. Anchoring
        // on the base row alone would read that as "not a cap-out" and strand it
        // exactly as before.
        //
        // The fingerprint guard is playhead-onn6's asset-identity trap: an
        // episode can carry rows for several fingerprints (a re-download mints a
        // new asset), and `fetchLatestJobForEpisode` would happily return one
        // describing different bytes than the audio on disk. When it does, fall
        // back to the row occupying the base key, which by construction hashes
        // to the cached audio.
        let latest = try await store.fetchLatestJobForEpisode(episodeId)
        let tail: AnalysisJob
        if let latest, latest.sourceFingerprint == fingerprint {
            tail = latest
        } else if let base = try await store.fetchJob(byWorkKey: baseWorkKey) {
            tail = base
        } else {
            return false
        }

        // Cheapest guard first, and deliberately BEFORE the ordinal walk and the
        // asset read. Every clean `complete` terminal reaches this method on
        // every sweep — 17 of them on the 2026-07-31 device pull — and paying
        // three store round-trips and an `info` line apiece, forever, to
        // re-derive "not ours" would make an opportunistic top-up the most
        // expensive step in the sweep.
        //
        // playhead-dl9k widened the predicate, NOT the cost model: a clean
        // `complete` (no `lastErrorCode`) still declines here on one string
        // comparison, which is the population that dominates the sweep.
        guard AnalysisWorkScheduler.isRescuableTerminal(tail) else {
            logger.debug(
                """
                cap_out_retry_declined episode=\(episodeId, privacy: .public) \
                reason=\(AnalysisWorkScheduler.CapOutRetryDeclineReason.notACapOutTerminal.rawValue, privacy: .public) \
                tailState=\(tail.state, privacy: .public)
                """
            )
            return false
        }

        // The budget ledger: the lowest `capRetry:<n>` key not already on disk,
        // or none when the budget is spent. Derived from the KEYS rather than
        // from a counter, so it survives a process death mid-chain and a
        // duplicated sweep is idempotent. `stride` rather than `1...max` so a
        // cap of 0 disables the feature instead of trapping on an invalid range.
        var nextOrdinal: Int?
        for ordinal in stride(from: 1, through: AnalysisWorkScheduler.maxCapOutRetries, by: 1) {
            let key = AnalysisWorkScheduler.capOutRetryWorkKey(
                baseWorkKey: baseWorkKey,
                ordinal: ordinal
            )
            if try await store.fetchJob(byWorkKey: key) == nil {
                nextOrdinal = ordinal
                break
            }
        }

        // The asset behind the tail supplies the two coverage inputs. A tail
        // with no asset row (the job never got far enough to resolve one) reads
        // as "nothing transcribed, duration unknown", which lands the retry on
        // the ladder's first rung — the same cheapest-probe default the base
        // insert above would have used.
        var asset: AnalysisAsset?
        if let assetId = tail.analysisAssetId {
            asset = try await store.fetchAsset(id: assetId)
        }
        let decidedAt = clock().timeIntervalSince1970
        // playhead-9y9e: the third coverage input, and the most expensive thing
        // this method can do — a full coverage-summary read (four prepared
        // statements, plus every transcript chunk of BOTH passes for the asset;
        // 5,167 rows for AD5F3A0A on the 2026-08-03 pull).
        //
        // GATED ON THE ARMS THAT WOULD DISCARD IT, which is the same
        // cheapest-first discipline the `isAttemptCapTerminal` guard above is
        // written for (R1 review). A spent budget and a cooling terminal are the
        // steady state — a capped episode reaches this method on every sweep for
        // the rest of the episode's life — so reading coverage before those two
        // arms would make an opportunistic top-up the most expensive step in the
        // sweep, permanently. `nil` from a skipped read is never seen by a mint:
        // `capOutRetryDecision` re-checks both conditions and returns
        // `.declined` before `adScanFraction` is consulted, and the cooldown
        // predicate is literally the same expression on both sides.
        //
        // An unresolved asset is also skipped: nothing to measure, and `nil`
        // already reads as owed.
        var adScanFraction: ReachRatio?
        if nextOrdinal != nil,
           AnalysisWorkScheduler.capOutRetryCooldownElapsed(chainTail: tail, now: decidedAt),
           let assetId = tail.analysisAssetId {
            adScanFraction = try await store
                .fetchCoverageSummariesByAssetIds([assetId])[assetId]?
                .adScanFraction
        }
        let decision = AnalysisWorkScheduler.capOutRetryDecision(
            baseWorkKey: baseWorkKey,
            chainTail: tail,
            nextOrdinal: nextOrdinal,
            transcriptCoverageSec: asset?.fastTranscriptCoverageEndTime ?? 0,
            episodeDurationSec: asset?.episodeDurationSec,
            adScanFraction: adScanFraction,
            tiers: [config.defaultT0DepthSeconds, config.t1DepthSeconds, config.t2DepthSeconds],
            now: decidedAt
        )

        let plan: AnalysisWorkScheduler.CapOutRetryPlan
        switch decision {
        case .mint(let minted):
            plan = minted
        case .declined(let reason):
            logger.info(
                """
                cap_out_retry_declined episode=\(episodeId, privacy: .public) \
                reason=\(reason.rawValue, privacy: .public) \
                tailState=\(tail.state, privacy: .public)
                """
            )
            return false
        }

        let now = clock().timeIntervalSince1970
        // The fingerprint is the CALLER's — the one the cached audio actually
        // hashes to — not `tail.sourceFingerprint`. Both tail branches above
        // already guarantee the two agree (one tests it, the other selects the
        // row by a key built from it), so this is not a behaviour change; it
        // states which of the two is the source of truth, so a future edit to
        // the tail selection cannot silently stamp the row with a fingerprint
        // describing different bytes than the file on disk.
        let retry = AnalysisJob(
            jobId: UUID().uuidString,
            jobType: "preAnalysis",
            episodeId: episodeId,
            podcastId: tail.podcastId,
            analysisAssetId: tail.analysisAssetId,
            workKey: plan.workKey,
            sourceFingerprint: fingerprint,
            downloadId: tail.downloadId,
            priority: 0,
            desiredCoverageSec: plan.desiredCoverageSec,
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
        guard try await store.insertJob(retry) else { return false }
        logger.info(
            """
            cap_out_retry_minted episode=\(episodeId, privacy: .public) \
            ordinal=\(plan.ordinal) target=\(plan.desiredCoverageSec) \
            tailCause=\(tail.lastErrorCode ?? "-", privacy: .public)
            """
        )
        return true
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

    /// playhead-qk44: run the stranded-backfill reaper DURING a foregrounded
    /// session instead of only at launch and on a BGProcessingTask wake.
    ///
    /// **The gap this closes.** Before this bead `resetStrandedBackfillJobs`
    /// was reachable from exactly two places — `reconcile()` at launch
    /// (`PlayheadRuntime`) and the same `reconcile()` from
    /// `BackgroundProcessingService`. Neither runs while the user is holding
    /// the phone. The 2026-07-31 device pull shows what that costs: a
    /// `fullEpisodeScan` sat at `running` for 23 minutes with zero scan rows
    /// while `Documents/bg-task-log.jsonl` records the app continuously
    /// `active` for 22 of them. The row cleared the reaper's 10-minute
    /// freshness floor after 10 of those minutes and nothing asked.
    ///
    /// **Why it is only safe to call this now.** The reaper's predicate is
    /// `updatedAt < now - strandedJobFreshnessSeconds`, and until playhead-qk44
    /// `updatedAt` did not advance during a coarse pass at all — every
    /// heartbeat site in `BackfillJobRunner.runJob` is downstream of
    /// `coarsePassA` returning, and a coarse pass runs 12–45 minutes on device.
    /// Adding a foreground sweep on top of THAT would have reaped healthy jobs
    /// mid-pass, discarding the exact FM work the reaper exists to protect.
    /// The per-window lease touch added in the same bead is what makes a fresh
    /// `updatedAt` mean "work advanced", and therefore what makes this sweep
    /// correct rather than destructive. Do not port this call anywhere without
    /// that guarantee.
    ///
    /// **This is the belt, not the braces.** The primary bound on a wedged
    /// pass is `FMNoProgressWatchdog`, which ends it in-process after
    /// 3 x 180 s and writes a named terminal — deliberately sooner than the
    /// 600 s floor here, so this sweep only ever sees rows whose owning runner
    /// is genuinely gone. Scene activation, not a timer, is the trigger: a
    /// periodic foreground timer would add a long-lived Task for a case the
    /// watchdog already covers.
    ///
    /// Best-effort. A failure leaves the row for the next activation or the
    /// next launch; it must never propagate into the scene-phase handler.
    @discardableResult
    func sweepStrandedBackfillJobsInSession() async -> Int {
        do {
            let count = try await store.resetStrandedBackfillJobs()
            if count > 0 {
                logger.info("stranded_backfill_reset_foreground count=\(count)")
            }
            return count
        } catch {
            logger.warning(
                "Foreground stranded-backfill sweep failed: \(error.localizedDescription, privacy: .public)"
            )
            return 0
        }
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

    // MARK: - Step: Semantic-scan claims for zero-row assets (playhead-fil5)

    /// Give every transcribed-but-unscanned asset that owns NO coverage-lane row
    /// the durable claim that makes it visible to the re-drive sweep.
    ///
    /// **The hole this fills.** ``mintAdScanRedrives`` starts from
    /// ``AnalysisStore/fetchAssetIdsWithResumableBackfillJobs(limit:)`` — it
    /// selects assets by the STATE of their `backfill_jobs` rows. An asset with
    /// zero rows therefore cannot be a candidate no matter how short its scan
    /// is, and "zero rows" is not a rare shape: on the 2026-08-03 device pull it
    /// was 4 of 12 assets, two of which this sweep can act on today (FCDDB309
    /// and 4FF3A238, both transcribed past the finalize floor). Every
    /// path that mints a coverage-lane row does so INSIDE
    /// `BackfillJobRunner.runPendingBackfill`, i.e. downstream of four
    /// `runShadowFMPhase` gates, so a single closed gate leaves the asset with
    /// no row, no re-drive candidacy, and no way back — permanently, because
    /// once its `analysis_jobs` rows go terminal nothing calls `runBackfill`
    /// again either.
    ///
    /// **Why this cannot loop.** The candidate query requires ZERO rows, so the
    /// claim this step writes removes the asset from its own candidate set on
    /// the first pass. From then on the asset is the re-drive sweep's problem,
    /// under that sweep's existing budget
    /// (``AnalysisWorkScheduler/maxAdScanRedrives``).
    ///
    /// **One claim per asset, EVER — not per transcript version.** Two bounds
    /// now agree on this where they used to differ.
    /// ``SemanticScanClaim/record(gate:analysisAssetId:podcastId:store:clock:logger:)``
    /// is keyed per ASSET since playhead-wxsv (it was per
    /// `(asset, transcriptVersion)`, which is how a growing transcript minted a
    /// second claim for work the first one already named), and this sweep's
    /// candidate query independently excludes an asset that owns a row of ANY
    /// kind. So once the first claim lands the asset never returns here
    /// regardless of what its transcript does afterwards. That is deliberate: a
    /// re-transcription is not a new strand, and an asset whose transcript
    /// version moves is by definition being worked on by a lane that will reach
    /// the gates — and the gates record for themselves.
    ///
    /// **Best-effort by contract**, exactly like ``mintAdScanRedrives``: `async`,
    /// not `async throws`, every store error logged and swallowed. A failure to
    /// record a claim must never fail `reconcile()` and mask lease recovery.
    private func mintSemanticScanClaims() async -> Int {
        do {
            return try await mintSemanticScanClaimsThrowing()
        } catch {
            logger.warning("semantic_scan_claim sweep failed (skipped): \(error)")
            return 0
        }
    }

    /// The next window of zero-row candidates, ROTATING so a candidate that can
    /// never be claimed cannot hold the front of the queue forever.
    ///
    /// **Why a fixed `LIMIT` alone is not a rate limit but a wall.** The
    /// candidate query is oldest-first and a rejected candidate writes nothing,
    /// so it is still a candidate — in the same position — on the next pass.
    /// Every reject the sweep can never accept is therefore a PERMANENT
    /// occupant of the window: an asset whose transcript stalled below the
    /// finalize floor and whose analysis jobs are terminal will never clear it
    /// and will never leave. On the 2026-08-03 pull two of the four zero-row
    /// assets are exactly that shape (48E903D7 at 36.9 % of its duration
    /// transcribed, 2C5C3699 at 4.3 %, both with `complete` analysis jobs), so a
    /// library only needs ``maxSemanticScanClaimCandidatesPerReconcile`` of them
    /// ahead of a claimable asset for the sweep to be silently dead — the exact
    /// failure this bead exists to remove, reintroduced one layer up.
    ///
    /// The cursor makes the per-pass bound a rate limit again: it advances by a
    /// full window whenever the query filled one and resets when it did not, so
    /// the sweep walks the whole candidate population across passes and wraps.
    /// A population that fits in one window (every device pull so far: 4
    /// candidates against a window of 24) never leaves offset 0, so this is
    /// inert until it is needed.
    ///
    /// In-memory rather than persisted on purpose. The cursor is an ordering
    /// fairness device, not state anything depends on being correct: a fresh
    /// process restarts at the oldest candidate, which is the right place to
    /// start and exactly what a persisted cursor would have to special-case.
    private func nextSemanticScanClaimCandidates() async throws -> [String] {
        let window = Self.maxSemanticScanClaimCandidatesPerReconcile
        var candidates = try await store.fetchAssetIdsMissingCoverageLaneJobs(
            limit: window,
            offset: semanticScanClaimSweepOffset
        )
        // The population shrank past the cursor (claims landed, assets were
        // deleted). Wrap NOW rather than returning empty and skipping a whole
        // pass — an empty read at a non-zero offset is not evidence there is
        // nothing to do.
        if candidates.isEmpty, semanticScanClaimSweepOffset > 0 {
            semanticScanClaimSweepOffset = 0
            candidates = try await store.fetchAssetIdsMissingCoverageLaneJobs(
                limit: window, offset: 0
            )
        }
        semanticScanClaimSweepOffset =
            candidates.count < window ? 0 : semanticScanClaimSweepOffset + window
        return candidates
    }

    private func mintSemanticScanClaimsThrowing() async throws -> Int {
        let assetIds = try await nextSemanticScanClaimCandidates()
        guard !assetIds.isEmpty else { return 0 }

        // playhead-fil5 R2: the SAME guard ``mintAdScanRedrives`` applies one
        // step later, and it belongs here for a sharper reason than symmetry.
        //
        // The candidate query names "assets with no coverage-lane row", and this
        // step reads that as "assets nobody will call `runBackfill` for again".
        // Those are not the same set. `runPendingBackfill` is step 6 of a pass,
        // so EVERY episode currently being analysed is a zero-row asset from the
        // moment its transcript crosses the finalize floor until the shadow
        // phase runs — and `reconcile()` runs on a BGProcessingTask wake, which
        // `playhead-qk44` documents as happening inside the live app process
        // while the foreground runner is mid-drain. Without this line a launch
        // that overlaps analysis spends the mint budget on episodes that were
        // about to request a scan on their own, ahead of the stranded ones that
        // never will, and stamps them with a gate reason asserting nothing ever
        // asked.
        let episodesWithPendingWork = try await store.fetchActiveJobEpisodeIds()
        var minted = 0
        for assetId in assetIds {
            guard minted < Self.maxSemanticScanClaimsPerReconcile else { break }

            // Resolved FIRST: the asset row names the episode and carries the
            // duration every measure below is a fraction of, and the in-flight
            // question is answered from an already-batched set, so a candidate
            // whose pass is mid-drain is rejected before it costs a read. A
            // candidate whose asset row cannot be read is skipped rather than
            // claimed with a nil podcast — without it we cannot tell whether a
            // pass is already in flight, and claiming then is a guess.
            guard let asset = try await store.fetchAsset(id: assetId) else { continue }
            guard !episodesWithPendingWork.contains(asset.episodeId) else { continue }

            // The transcript has to have reached the finalize floor first. A
            // half-transcribed asset is the transcript lane's work, and minting
            // here would spend an ad-scan re-drive on a pass whose real job is
            // transcription.
            //
            // The numerator is an AREA with sub-ad-width gaps bridged, and both
            // halves of that are load-bearing in opposite directions. Against
            // the `max(endTime)` WATERMARK `AnalysisCoordinator.finalizeBackfillVerdict`
            // divides, a gappy transcript reads 100 % over audio nobody
            // transcribed (the playhead-sd71 antipattern) — a hazard the
            // 2026-08-03 pull STILL exhibits with the area spanning both passes,
            // on NINE of twelve assets — and R6 review re-derived that nine and
            // NAMED it, because the pull carries three different nines-of-twelve
            // and this sentence identified none of them. THIS nine is "chunk-max
            // watermark strictly above the bridged two-pass area": 44F076BB,
            // 4FF3A238, 53FC53E3, 58882C47, 83592353, AD5F3A0A, D9B513CD,
            // DE0784D8, FCDDB309. It is NOT the nine at
            // `AnalysisStore.fetchCoverageSummariesByAssetIds` ("fast-only
            // ceiling below the 0.98 sufficiency floor"), with which it shares
            // only six members, and it is NOT the nine with no coverage-lane row
            // at all (`adScanFraction` ABSENT, not zero). Quoting a count without
            // its population is instances 15/16/17 of this bead's own catalogue.
            //
            // Only ONE member of this nine actually flips the 0.95 floor, and it
            // is the one worth naming: D9B513CD reads 100.0 % by chunk-max
            // watermark against an 88.3 % two-pass area — an 11.7 pp gap, where
            // the other eight are 0.4–2.5 pp and land the same side of it. See
            // ``SemanticScanClaim/transcriptClearsFinalizeFloor(coveredSec:episodeDurationSec:)``
            // for the table, for why the watermark is `chunks.map(\.endTime).max()`
            // and not the fast-pass COLUMN, and for the four successive drafts of
            // this claim that were wrong. Against the RAW chunk
            // union, a fully transcribed episode reads ~87 %, because a chunk
            // spans first-word to last-word and every breath is a hole: on the
            // 2026-08-03 pull the raw union cleared 0.95 for **zero of twelve**
            // assets, which is a gate that mints nothing rather than a strict
            // one. See ``SemanticScanClaim/bridgedTranscriptCoveredSec(region:)``.
            //
            // playhead-9y9e: the ranges span BOTH transcript passes. Reading the
            // fast pass alone made this gate refuse 48E903D7 at 36.9 % when its
            // transcript covers 95.1 %, and 0C2FC22E at 55.4 % when its two
            // passes tile the episode end to end between them.
            //
            // playhead-x0lb R6: both parameters carry types. They were `Double?`
            // and `Double?`, so probe PJ5 exchanged the numerator and the
            // denominator here and it COMPILED — R4's PB1/PB2 reciprocal shape,
            // which R4 closed for the Activity bars and which was still writable
            // at this gate. Rail TY37.
            guard SemanticScanClaim.transcriptClearsFinalizeFloor(
                coveredSec: SemanticScanClaim.bridgedTranscriptCoveredSec(
                    region: try await store.fetchTranscribedRegion(assetId: assetId)
                ),
                episodeDurationSec: asset.episodeDurationSec.map { EpisodeSeconds($0) }
            ) else { continue }
            // Whether a scan is OWED is deliberately not re-asked here.
            // ``SemanticScanClaim/record(gate:analysisAssetId:podcastId:store:clock:logger:)``
            // reads the same coverage summary and refuses with `.notOwed`, and a
            // copy of that floor in this file would be a second policy that
            // drifts — and, being behaviourally identical while it agreed, one
            // no test could ever kill. The transcript floor above is different:
            // it is this sweep's own judgement about which assets are still the
            // transcript lane's problem, and nothing downstream makes it.
            //
            // The cost of that choice, stated so nobody has to re-derive it:
            // a candidate that clears the transcript floor but turns out not to
            // be owed a scan pays a whole-transcript read and two more store
            // round-trips before `record` says `.notOwed`, and it
            // pays them again on every `reconcile()` because no row is written
            // and it stays a candidate. That is affordable because the state is
            // near-unreachable in production rather than merely rare: reaching
            // it needs `semantic_scan_results` rows covering >= 98 % of the
            // episode with ZERO `backfill_jobs` rows. playhead-wxsv MADE THAT
            // REACHABLE and this paragraph used to say it could not be: the v44
            // rung runs `DELETE FROM backfill_jobs` once, to drop rows minted
            // under an id derivation nothing re-derives. So exactly one launch,
            // on exactly one upgrade, an asset can hold scan rows and no job
            // row — and that is the state this sweep exists to recover, which
            // is why the break is affordable. Every other removal is still the
            // asset's own FK CASCADE, which takes the scan rows with it.

            let chunks = try await store.fetchTranscriptChunks(assetId: assetId)
            guard !chunks.isEmpty else { continue }

            // The podcast id the analysis lane recorded for this asset's
            // episode. It is USUALLY ABSENT: on the 2026-08-03 pull
            // `analysis_jobs.podcastId` is SQL NULL on 43 of 44 rows, including
            // every row on all four zero-row episodes, and `readJob` decodes it
            // through `optionalText`, so what this line actually yields for the
            // assets this sweep reaches is `nil` — not `""`.
            //
            // Handed on verbatim anyway, because the value is a pass-through and
            // ``SemanticScanClaim/claimRow(analysisAssetId:podcastId:gate:createdAt:)``
            // is the ONE place that decides an empty id is an ABSENCE.
            // Normalizing here as well is the shape that let the SC09 mutant
            // survive in R2 — a duplicate policy that no test can kill while it
            // agrees, because the constructor it guards never sees the value it
            // exists to catch. Note where that value DOES come from, since it is
            // not here: `AdDetectionService`'s `.podcastIdMissing` gate takes a
            // non-optional `String` and fires exactly when it `.isEmpty`, so
            // that call site hands `claimRow` a literal `""` every time it runs.
            let podcastId = try await store.fetchLatestJobForEpisode(asset.episodeId)?.podcastId

            let outcome = await SemanticScanClaim.record(
                gate: .noCoverageLaneRow,
                analysisAssetId: assetId,
                podcastId: podcastId,
                store: store,
                clock: { [clock] in clock().timeIntervalSince1970 },
                logger: logger
            )
            if outcome == .minted {
                minted += 1
            }
        }
        if minted > 0 {
            logger.info("semantic_scan_claim_minted total=\(minted)")
        }
        return minted
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
