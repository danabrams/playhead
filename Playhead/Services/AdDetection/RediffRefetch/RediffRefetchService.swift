// RediffRefetchService.swift
// playhead-xsdz.28: the WiFi+charging BGTask that drives the rediff re-fetch
// policy. DEFAULT OFF — with the flag off nothing is scheduled and no network
// or filesystem is touched, so the app is byte-identical (mirrors the flag-OFF
// posture of the other rediff pieces: xsdz.27 capture, xsdz.29 ownership).
//
// WHAT IT DOES (only when enabled + granted a BGProcessingTask window):
//   1. Reschedule the next fire first (so a crash still leaves a pending task).
//   2. Enumerate downloaded episodes as `Sendable` candidates.
//   3. For each due candidate (`RediffRefetchPolicy.eligibility` — the ≥24h
//      gate + backoff + retry budget):
//        a. Ranged-GET head/tail sample of the CURRENT enclosure (NO HEAD/ETag).
//        b. Compare to the played copy's local head/tail sample.
//        c. IDENTICAL → non-rotator → SKIP the full fetch, back off.
//        d. DIFFERENT → full re-fetch → fingerprint the B-side OFF the hot
//           actor → DELETE the B-copy (never persisted) → mark resolved.
//      Bandwidth is accounted per candidate and summed for the sweep.
//   4. Complete the BGTask exactly once (success unless expired).
//
// BGTASK POLICY (spike §5): `BGProcessingTaskRequest` with
// `requiresExternalPower = true` (charging) + `requiresNetworkConnectivity =
// true` (network present); WiFi is pinned by the WiFi-only URLSession in the
// production sampler/fetcher. ~54 MB/episode, ~1.1 GB/library-week — acceptable
// on WiFi overnight, unacceptable on cellular.
//
// OFF THE HOT ACTOR (xsdz.29 R5 residual): this is a DEDICATED actor, not the
// playback / AdDetection actor. The B-side resample+fingerprint runs through
// the `RediffBSideFingerprinting` seam whose production conformer is a plain
// value type — its work lands on the generic executor, never stalling a serial
// hot actor with a full-episode resample.

import BackgroundTasks
import Foundation
import os
import OSLog

/// Drives the WiFi+charging `BGProcessingTask` that re-fetches episode audio for
/// the rediff width oracle. Default OFF.
actor RediffRefetchService {

    // MARK: - Identity & flag

    /// BGTaskScheduler identifier. Declared in `Info.plist`'s
    /// `BGTaskSchedulerPermittedIdentifiers` + `project.yml` (kept in sync by
    /// the pbxproj/Info.plist regen), under the shared `com.playhead.app.`
    /// namespace. Permitted-but-unregistered until activation (xsdz.36) wires
    /// the handler — registering an identifier absent from the plist crashes,
    /// so the entry ships now for flip-readiness.
    static let taskIdentifier = "com.playhead.app.rediff-refetch"

    /// MASTER default-OFF flag. `false` ⇒ no scheduling, no network, no fetch —
    /// the app is byte-identical. This DEFAULT stays `false` (pinned by
    /// `defaultFlagIsOff`); activation (xsdz.36) does not flip it — instead
    /// `PlayheadRuntime` constructs the production instance with
    /// `enabled: RediffActivation.isEnabledByDefault`-gated wiring
    /// (`enabled: true` only when the single activation switch is on).
    /// Tests likewise construct `enabled: true` to exercise the sweep.
    static let isEnabledByDefault = false

    /// Soft floor iOS should wait before the next fire. iOS defers further on
    /// its own heuristics; the per-episode ≥24h/backoff gates are the real
    /// cadence, so this only bounds how often the app is woken to CHECK.
    static let minimumRefetchInterval: TimeInterval = 6 * 60 * 60

    /// playhead-xsdz.36 (R1 hygiene): minimum age before a `rediff-bcopy-*`
    /// tmp file counts as an orphan for the per-fire sweep. A LIVE B-copy
    /// exists only inside `processCandidate`'s scope on this actor (minutes at
    /// most — a BGProcessingTask window), so one hour is unambiguous, and the
    /// ≥6h fire spacing means a real orphan is well past it by the next fire.
    static let orphanedBCopyMinimumAge: TimeInterval = 60 * 60

    // MARK: - Dependencies

    private nonisolated let enabled: Bool
    private let config: RediffRefetchPolicy.Configuration
    private let enumerator: any RediffRefetchEnumerating
    private let rangedSampler: any RangedAudioSampling
    private let localSampler: any LocalAudioSampling
    private let fullFetcher: any FullEpisodeFetching
    private let bsideFingerprinter: any RediffBSideFingerprinting
    private let recorder: any RediffRefetchRecording
    private let fileRemover: any RediffTempFileRemoving
    private let taskScheduler: any BackgroundTaskScheduling
    /// playhead-xsdz.36 ACTIVATION: optional handoff that routes a freshly
    /// fetched, rotated B-copy into the rediff slot pass (stage → revalidate →
    /// unstage) BEFORE the copy is deleted. `nil` (the default, and every
    /// pre-activation caller) preserves the xsdz.28 behavior byte-for-byte:
    /// standalone B-side fingerprint, then delete.
    private let bsideConsumer: (any RediffBSideConsuming)?
    /// playhead-xsdz.36.4 DAY-0: optional byte-exact mint path for the play-time
    /// (day-0) trigger. `nil` (default, and every lagged-only caller) ⇒ the day-0
    /// path is a no-op even when the trigger fires. When wired,
    /// `runDayZeroRefetch` routes the k-way B-copies through it — byte-align vs
    /// the pinned A-side → ≥2-persona-robust byte-EXACT slots → mark-only banners
    /// — INSTEAD of the `bsideConsumer`/`revalidateFromFeatures` route (which
    /// needs persisted analysis that a true first listen does not have).
    private let dayZeroMinter: (any RediffDayZeroMinting)?
    /// playhead-xsdz.36: optional durable run ledger (same surface the other
    /// BG tasks use, `background_task_runs`). `nil` (default) records nothing.
    private let runLedger: (any BackgroundTaskRunLedger)?
    /// Injectable clock so eligibility is deterministic in tests.
    private let now: @Sendable () -> Double
    private let logger = Logger(subsystem: "com.playhead", category: "RediffRefetch")

    // MARK: - Per-fire state

    /// Flipped by the CURRENT fire's expiration handler so the sweep bails at
    /// the next candidate boundary (no `Task.cancel` — the enclosing task is
    /// not ours). Guarded by `currentTaskID` so a LATE expiration from an
    /// already-completed fire cannot poison a successor fire's sweep.
    private var expired = false
    /// Identity of the fire whose sweep is in flight, nil between fires.
    /// Doubles as the reentry guard: an overlapping fire (double-fire, or a
    /// sweep outliving its window into the next grant) completes immediately
    /// instead of resetting the in-flight fire's state.
    private var currentTaskID: ObjectIdentifier?
    /// Tasks that already had `setTaskCompleted` called (iOS terminates on a
    /// 2nd call). PER-TASK, not a shared bool, mirroring
    /// `BackgroundProcessingService.completedTaskIDs`: with a shared bool an
    /// overlapping handler's reset lets a stale completion path double-call
    /// `setTaskCompleted` on one task while silently dropping the other's.
    /// Grows one entry per fire for the process lifetime — same accepted
    /// bound as the sibling service.
    private var completedTaskIDs = Set<ObjectIdentifier>()

    // MARK: - Init

    init(
        enabled: Bool = RediffRefetchService.isEnabledByDefault,
        config: RediffRefetchPolicy.Configuration = .default,
        enumerator: any RediffRefetchEnumerating,
        rangedSampler: any RangedAudioSampling,
        localSampler: any LocalAudioSampling,
        fullFetcher: any FullEpisodeFetching,
        bsideFingerprinter: any RediffBSideFingerprinting,
        recorder: any RediffRefetchRecording = LoggingRediffRefetchRecorder(),
        fileRemover: any RediffTempFileRemoving = FileManagerTempFileRemover(),
        taskScheduler: any BackgroundTaskScheduling = BGTaskScheduler.shared,
        bsideConsumer: (any RediffBSideConsuming)? = nil,
        dayZeroMinter: (any RediffDayZeroMinting)? = nil,
        runLedger: (any BackgroundTaskRunLedger)? = nil,
        now: @escaping @Sendable () -> Double = { Date().timeIntervalSince1970 }
    ) {
        self.enabled = enabled
        self.config = config
        self.enumerator = enumerator
        self.rangedSampler = rangedSampler
        self.localSampler = localSampler
        self.fullFetcher = fullFetcher
        self.bsideFingerprinter = bsideFingerprinter
        self.recorder = recorder
        self.fileRemover = fileRemover
        self.taskScheduler = taskScheduler
        self.bsideConsumer = bsideConsumer
        self.dayZeroMinter = dayZeroMinter
        self.runLedger = runLedger
        self.now = now
    }

    /// Whether re-fetch is enabled for this instance. Exposed for tests.
    func isEnabled() -> Bool { enabled }

    // MARK: - Scheduling

    /// Submit the next `BGProcessingTaskRequest` (WiFi+charging). A no-op when
    /// disabled — the default-OFF byte-identity contract (nothing scheduled).
    func scheduleNextRefetch() {
        guard enabled else {
            logger.debug("rediff re-fetch disabled — not scheduling")
            return
        }
        let request = BGProcessingTaskRequest(identifier: Self.taskIdentifier)
        request.requiresNetworkConnectivity = true   // network present…
        request.requiresExternalPower = true          // …and charging (WiFi pinned by the URLSession)
        request.earliestBeginDate = Date(timeIntervalSinceNow: Self.minimumRefetchInterval)
        do {
            try taskScheduler.submit(request)
            logger.info("Scheduled rediff re-fetch in \(Self.minimumRefetchInterval, privacy: .public)s")
        } catch {
            logger.error("Failed to schedule rediff re-fetch: \(String(describing: error), privacy: .public)")
        }
    }

    // MARK: - Handler

    /// Drive one BGProcessingTask fire: reschedule, install expiration, run the
    /// sweep, complete once. Defensive no-op-but-complete when disabled (the OS
    /// can only fire a task that was scheduled, which never happens when OFF).
    func handleRefetchTask(_ task: any BackgroundProcessingTaskProtocol) async {
        guard enabled else {
            task.setTaskCompleted(success: true)
            return
        }
        // Reentry guard: if a fire's sweep is already in flight, complete the
        // overlapping task immediately — do NOT reset the in-flight fire's
        // state (that reset is how a shared-flag design double-completes the
        // first task and orphans the second).
        guard currentTaskID == nil else {
            logger.warning("rediff re-fetch fired while a sweep is in flight — completing the overlapping task immediately")
            completeTaskOnce(task, success: false)
            return
        }
        currentTaskID = ObjectIdentifier(task as AnyObject)
        defer { currentTaskID = nil }
        expired = false

        // Install the expiration handler FIRST — before the reschedule and
        // especially before the synchronous per-fire orphan sweep below,
        // which can spend real time deleting a stranded multi-hundred-MB
        // shard-cache directory. The OS reclaim window opens as soon as the
        // launch handler is invoked; an expiration that lands before the
        // handler is installed cannot be observed, so the fire would neither
        // bail nor complete cleanly (the same failure mode
        // `BackgroundProcessingService.handleBackfillTask` documents and
        // defends against). `currentTaskID` is already claimed above, so an
        // early expiration correctly flips `expired` for THIS fire.
        task.expirationHandler = { [weak self] in
            let box = _UncheckedSendableBox(task)
            Task { await self?.markExpiredAndComplete(box.value) }
        }

        // Reschedule next so iOS always has a pending request even if the
        // sweep crashes mid-fire.
        scheduleNextRefetch()

        // playhead-xsdz.36 (R1 hygiene): clean up any B-copy a previous
        // process abandoned between download and deletion (jetsam/expiration
        // mid-consume). Runs before the sweep so the fire's own transient
        // B-copy (always younger than the age floor) is never touched.
        fileRemover.removeOrphanedBCopies(olderThan: Self.orphanedBCopyMinimumAge)

        // playhead-xsdz.36: durable run row (same `background_task_runs`
        // surface the backfill/recovery BG tasks use) so overnight dogfood
        // diagnostics can classify rediff fires and read the bandwidth spent
        // without JSONL grep. Best-effort — a nil ledger records nothing.
        let runId = await runLedger?.startRun(
            entryPoint: .rediffRefetch,
            taskIdentifier: Self.taskIdentifier,
            taskInstanceID: nil,
            scenePhase: nil
        )

        let summary = await runRefetchSweep()

        if let runLedger, let runId {
            let outcome: BackgroundTaskRunOutcome = expired
                ? .expired
                : (summary.eligibleProcessed > 0 ? .admittedWork : .noEligibleWork)
            await runLedger.finishRun(runId: runId, update: BackgroundTaskRunOutcomeUpdate(
                outcome: outcome,
                // Bandwidth accounting rides the free-form annotation column —
                // the closed counter columns have no bytes axis.
                deferReason: "precheckBytes=\(summary.precheckBytes) fullFetchBytes=\(summary.fullFetchBytes)",
                jobsSeen: summary.candidateCount,
                jobsAdmitted: summary.eligibleProcessed,
                jobsCompleted: summary.rotatedCount,
                expiration: expired
            ))
        }

        completeTaskOnce(task, success: !expired)
    }

    /// Aggregate outcome of one sweep — the handler's ledger row and the
    /// summary os_log line read from this.
    struct SweepSummary: Sendable, Equatable {
        var candidateCount = 0
        var eligibleProcessed = 0
        var rotatedCount = 0
        var failedCount = 0
        var precheckBytes = 0
        var fullFetchBytes = 0
        var totalBytes: Int { precheckBytes + fullFetchBytes }
    }

    /// The core sweep. Also callable directly by tests without a BGTask.
    @discardableResult
    func runRefetchSweep() async -> SweepSummary {
        var summary = SweepSummary()
        guard enabled else { return summary }
        let candidates = await enumerator.candidates()
        summary.candidateCount = candidates.count
        let sweepNow = now()

        for candidate in candidates {
            if expired { break }
            let eligibility = RediffRefetchPolicy.eligibility(
                now: sweepNow,
                downloadedAt: candidate.downloadedAt,
                state: candidate.attemptState,
                config: config
            )
            guard case .eligible = eligibility else {
                await recorder.recordOutcome(.skippedIneligible(assetId: candidate.assetId, reason: eligibility))
                continue
            }
            summary.eligibleProcessed += 1
            let result = await processCandidate(candidate, at: sweepNow)
            summary.precheckBytes += result.cost.precheckBytes
            summary.fullFetchBytes += result.cost.fullFetchBytes
            if result.rotated { summary.rotatedCount += 1 }
            if result.failed { summary.failedCount += 1 }
        }

        logger.info(
            "rediff re-fetch sweep: \(summary.candidateCount, privacy: .public) candidates, \(summary.rotatedCount, privacy: .public) rotated, \(summary.failedCount, privacy: .public) failed, \(summary.totalBytes, privacy: .public) bytes"
        )
        return summary
    }

    /// Per-candidate result: bandwidth spent + which terminal arm it took.
    private struct CandidateResult {
        let cost: RediffRefetchPolicy.BandwidthCost
        let rotated: Bool
        let failed: Bool
    }

    /// Pre-check one candidate; full-fetch + fingerprint/consume + DELETE only
    /// on a rotation. Errors are swallowed per candidate (recorded as
    /// `.failed` WITH the advanced R2 failure state) so one bad episode cannot
    /// abort the sweep — matching the feed-refresh per-feed-swallow contract.
    private func processCandidate(
        _ candidate: RediffRefetchCandidate,
        at sweepNow: Double
    ) async -> CandidateResult {
        var precheckBytes = 0
        // Stage marker for a PRE-CHECK failure classification: an unknown error
        // BEFORE the ~54 MB fetch retries cheaply (transient). The fetch/consume
        // stages are classified inside `fetchConsumeAndRecord`.
        do {
            // (a) Ranged head/tail sample of the CURRENT enclosure (NO HEAD).
            let remote = try await rangedSampler.sample(
                url: candidate.enclosureURL,
                headBytes: config.headSampleBytes,
                tailBytes: config.tailSampleBytes
            )
            precheckBytes = remote.bytesTransferred

            // (b) Local played-copy sample.
            let local = try localSampler.sample(
                fileURL: candidate.localAudioURL,
                headBytes: config.headSampleBytes,
                tailBytes: config.tailSampleBytes
            )

            // (c) Non-rotator → SKIP the full fetch, back off.
            if !RediffRefetchPolicy.isRotated(local: local, remote: remote.fingerprint) {
                let newState = RediffRefetchPolicy.advanceUnchanged(candidate.attemptState, at: sweepNow)
                let cost = RediffRefetchPolicy.BandwidthCost(precheckBytes: precheckBytes, fullFetchBytes: 0)
                await recorder.recordOutcome(.unchanged(assetId: candidate.assetId, cost: cost, newState: newState))
                return CandidateResult(cost: cost, rotated: false, failed: false)
            }
        } catch {
            // A pre-check-stage failure: no full fetch was spent, so the cost is
            // whatever the ranged sample transferred (0 if it was the throw).
            let failureClass = RediffRefetchPolicy.classifyFailure(error, stage: .precheck)
            let newState = RediffRefetchPolicy.advanceFailed(
                candidate.attemptState,
                failureClass: failureClass,
                at: sweepNow
            )
            if RediffRefetchPolicy.isParked(newState, config: config) {
                logger.error(
                    "rediff re-fetch PARKED assetId=\(candidate.assetId, privacy: .public) class=\(failureClass.rawValue, privacy: .public) streak=\(newState.sameClassFailureStreak, privacy: .public)"
                )
            }
            let cost = RediffRefetchPolicy.BandwidthCost(precheckBytes: precheckBytes, fullFetchBytes: 0)
            await recorder.recordOutcome(.failed(
                assetId: candidate.assetId,
                cost: cost,
                failureClass: failureClass,
                newState: newState,
                error: String(describing: error)
            ))
            return CandidateResult(cost: cost, rotated: false, failed: true)
        }

        // (d) Rotator → the shared full-fetch → consume → delete → record arm.
        return await fetchConsumeAndRecord(
            candidate,
            at: sweepNow,
            precheckBytes: precheckBytes,
            kWayFetchCount: config.kWayFetchCount
        )
    }

    /// The rotator arm, shared by the lagged sweep (after its rotation
    /// pre-check) and the day-0 play-time trigger (which skips the pre-check):
    /// full re-fetch K distinct-persona B-copies → fingerprint/consume off the
    /// hot actor → DELETE every copy on every exit → record the terminal
    /// outcome + bandwidth. `precheckBytes` is the bandwidth already spent
    /// before this arm (the lagged pre-check's ~128 KB ranged sample; `0` for
    /// day-0, which does no pre-check). `kWayFetchCount` is the config value for
    /// the lagged sweep and the day-0 constant for the trigger.
    ///
    /// playhead-xsdz.36.2 (k-way): fetch K B-copies, each under a DISTINCT
    /// persona drawn from the curated bank in the divergence-reliable order
    /// (iPhone → Mac → Overcast → empty), each with its own unique cache-buster.
    /// K=1 fetches exactly ONE B-copy under the default persona. The consumer
    /// stages ALL K so `computeByteAlignedPlayedSlots` unions the pairwise
    /// diffs; a pod one fetch-pair misses is recovered from another persona's
    /// stitch.
    private func fetchConsumeAndRecord(
        _ candidate: RediffRefetchCandidate,
        at sweepNow: Double,
        precheckBytes: Int,
        kWayFetchCount: Int
    ) async -> CandidateResult {
        var fullFetchBytes = 0
        // Stage marker for failure classification: an unknown error AFTER the
        // ~54 MB fetch is decode-class so a deterministic loop cannot re-spend
        // the fetch every sweep (xsdz.28 R2).
        var stage = RediffRefetchPolicy.FailureStage.fetch
        do {
            let personas = RediffFetchPersona.kWayPersonas(count: kWayFetchCount)
            var fetchedFileURLs: [URL] = []
            // NEVER persist the B-copies: delete EVERY fetched copy on EVERY exit
            // from this scope — a mid-batch fetch throw, or a throw out of the
            // fingerprint/consume step below. Captures the growing array by
            // reference, so it deletes exactly what was fetched.
            defer { for url in fetchedFileURLs { fileRemover.remove(url) } }
            for persona in personas {
                let full = try await fullFetcher.download(url: candidate.enclosureURL, persona: persona)
                fetchedFileURLs.append(full.fileURL)
                fullFetchBytes += full.byteCount
            }

            stage = .postDownload
            let fingerprintCount: Int
            if let bsideConsumer {
                // ACTIVATION path (xsdz.36): hand the K B-copies to the rediff
                // slot pass (stage all → revalidate once → unstage) while the
                // files still exist. The pass fingerprints/aligns internally, so
                // the standalone fingerprint step would be a redundant
                // full-episode decode — skipped. A consume throw is a FAILURE
                // (no resolve) so a later sweep retries under the R2 policy.
                try await bsideConsumer.consumeRotatedBSides(
                    assetId: candidate.assetId,
                    fileURLs: fetchedFileURLs
                )
                fingerprintCount = 0
            } else {
                // Pre-activation xsdz.28 path, byte-identical at K=1: standalone
                // B-side fingerprint validation of the primary (first-persona)
                // copy. An EMPTY stream is a fingerprint-mismatch-class failure
                // rather than a silent "resolved with 0 fingerprints" terminal.
                let fingerprints = try await bsideFingerprinter.fingerprint(fileURL: fetchedFileURLs[0])
                guard !fingerprints.isEmpty else { throw RediffBSideEmptyStreamError() }
                fingerprintCount = fingerprints.count
            }

            let newState = RediffRefetchPolicy.markResolved(candidate.attemptState, at: sweepNow)
            let cost = RediffRefetchPolicy.BandwidthCost(precheckBytes: precheckBytes, fullFetchBytes: fullFetchBytes)
            await recorder.recordOutcome(.rotated(
                assetId: candidate.assetId,
                cost: cost,
                fingerprintCount: fingerprintCount,
                newState: newState
            ))
            return CandidateResult(cost: cost, rotated: true, failed: false)
        } catch {
            let failureClass = RediffRefetchPolicy.classifyFailure(error, stage: stage)
            let newState = RediffRefetchPolicy.advanceFailed(
                candidate.attemptState,
                failureClass: failureClass,
                at: sweepNow
            )
            if RediffRefetchPolicy.isParked(newState, config: config) {
                logger.error(
                    "rediff re-fetch PARKED assetId=\(candidate.assetId, privacy: .public) class=\(failureClass.rawValue, privacy: .public) streak=\(newState.sameClassFailureStreak, privacy: .public)"
                )
            }
            let cost = RediffRefetchPolicy.BandwidthCost(precheckBytes: precheckBytes, fullFetchBytes: fullFetchBytes)
            await recorder.recordOutcome(.failed(
                assetId: candidate.assetId,
                cost: cost,
                failureClass: failureClass,
                newState: newState,
                error: String(describing: error)
            ))
            return CandidateResult(cost: cost, rotated: false, failed: true)
        }
    }

    // MARK: - Day-0 (immediate, play-time) rediff (playhead-xsdz.36.4)

    /// Run an IMMEDIATE (day-0) rediff for a SINGLE just-started episode — the
    /// capstone of the rediff-activation series. Unlike the lagged sweep this
    /// deliberately BYPASSES the ≥24h eligibility gate AND the rotation
    /// PRE-CHECK: the pre-check is a single-persona head/tail probe that
    /// false-negatives on client-PINNED DAI (a same-context re-fetch returns a
    /// byte-identical body), the very category day-0's k-way probe targets.
    ///
    /// FIRST-LISTEN MARKING (playhead-xsdz.36.4 rework): day-0 does NOT route
    /// through the lagged `bsideConsumer`/`revalidateFromFeatures` arm. That arm
    /// re-runs the slot pass over PERSISTED transcript/analysis, which on a true
    /// first listen does NOT yet exist (transcription takes minutes; the k-way
    /// fetch finishes in seconds) — so it produced ZERO marks AND poisoned the
    /// shared lagged state. Instead day-0 uses `fetchMintAndRecord`: k-way full
    /// fetch → hand the B-copies to `dayZeroMinter`, which byte-aligns each
    /// against the pinned A-side and MINTS mark-only banners for byte-EXACT,
    /// ≥2-persona-robust divergent slots (a byte-exact divergent region is its
    /// OWN deterministic ad-presence core — no persisted analysis required) →
    /// DELETE every B-copy → account bandwidth. Marks flow to banners as the
    /// SAME mark-only surface (auto-skip stays held).
    ///
    /// SAFETY (wrj8): the A-side of the byte diff is the pinned played file
    /// (read-only, resolved from the asset row's `sourceURL` inside the
    /// minter); the B-side(s) are separate never-played temp copies deleted on
    /// every exit. Day-0 NEVER writes/rotates the pinned playback audio.
    ///
    /// POISONING FIX: day-0 is ADDITIVE toward `rediff_refetch_state`. It writes
    /// `resolved` ONLY when it actually produced marks; an empty/failed run
    /// accounts bandwidth but advances NO attempt state, so the lagged sweep
    /// still recovers those ads later (see `Outcome.dayZeroUnmarked`).
    ///
    /// Enablement/power/network GATING is the CALLER's responsibility
    /// (`DayZeroRediffTrigger`, gated on `RediffActivation.dayZeroEnabledByDefault`
    /// + WiFi + charging/deep-scan). The service-level `enabled` flag still
    /// governs whether ANY network/filesystem is touched — a disabled service is
    /// a no-op even here (the OFF byte-identity contract). A `nil` `dayZeroMinter`
    /// is likewise a no-op (no fetch, no marks): day-0 has no marking path.
    @discardableResult
    func runDayZeroRefetch(
        for candidate: RediffRefetchCandidate,
        kWayFetchCount: Int = RediffActivation.dayZeroKWayFetchCount
    ) async -> SweepSummary {
        var summary = SweepSummary()
        guard enabled, dayZeroMinter != nil else { return summary }
        summary.candidateCount = 1
        summary.eligibleProcessed = 1
        let result = await fetchMintAndRecord(
            candidate,
            at: now(),
            kWayFetchCount: kWayFetchCount
        )
        summary.precheckBytes += result.cost.precheckBytes
        summary.fullFetchBytes += result.cost.fullFetchBytes
        if result.rotated { summary.rotatedCount += 1 }
        if result.failed { summary.failedCount += 1 }
        logger.info(
            "rediff DAY-0 refetch asset=\(candidate.assetId, privacy: .public) marked=\(summary.rotatedCount, privacy: .public) failed=\(summary.failedCount, privacy: .public) bytes=\(summary.totalBytes, privacy: .public)"
        )
        return summary
    }

    /// The DAY-0 mint arm (playhead-xsdz.36.4): fetch K distinct-persona B-copies
    /// → DELETE every copy on every exit → hand them to the `dayZeroMinter` for
    /// the byte-EXACT ≥2-persona-robust mark mint → record the poisoning-safe
    /// outcome + bandwidth. Mirrors `fetchConsumeAndRecord`'s fetch + never-
    /// persist-B `defer`, but replaces the persisted-analysis consume with the
    /// first-listen byte-exact mint and the always-resolve `.rotated` outcome
    /// with the additive `.dayZeroMarked` / `.dayZeroUnmarked` split.
    private func fetchMintAndRecord(
        _ candidate: RediffRefetchCandidate,
        at sweepNow: Double,
        kWayFetchCount: Int
    ) async -> CandidateResult {
        // Guarded by `runDayZeroRefetch`; re-asserted so this arm is self-safe.
        guard let dayZeroMinter else {
            return CandidateResult(cost: .zero, rotated: false, failed: false)
        }
        var fullFetchBytes = 0
        do {
            let personas = RediffFetchPersona.kWayPersonas(count: kWayFetchCount)
            var fetchedFileURLs: [URL] = []
            // NEVER persist the B-copies: delete EVERY fetched copy on EVERY exit
            // (a mid-batch fetch throw, or a throw/return out of the mint below).
            defer { for url in fetchedFileURLs { fileRemover.remove(url) } }
            for persona in personas {
                let full = try await fullFetcher.download(url: candidate.enclosureURL, persona: persona)
                fetchedFileURLs.append(full.fileURL)
                fullFetchBytes += full.byteCount
            }

            let markCount = await dayZeroMinter.mintByteExactDayZeroMarks(
                assetId: candidate.assetId,
                bSideURLs: fetchedFileURLs
            )
            let cost = RediffRefetchPolicy.BandwidthCost(precheckBytes: 0, fullFetchBytes: fullFetchBytes)
            if markCount > 0 {
                // A day-0 MARK resolves the shared state (terminal, no lagged
                // re-fetch) — a deliberate bandwidth tradeoff. Day-0's K=3 fetch
                // breadth exceeds the lagged K=1 sweep, but day-0 also applies a
                // STRICTER ≥2-persona quorum, so it is not a strict superset of
                // lagged RESULTS: a pod that diverges on ONLY the lagged default
                // persona would be dropped by day-0's quorum yet marked by the
                // lagged K=1 sweep — resolving forecloses that (rare) recovery.
                // Accepted because such marks are mark-only (a missed banner, never
                // eaten content) and the saved re-fetch bandwidth is the priority.
                let newState = RediffRefetchPolicy.markResolved(candidate.attemptState, at: sweepNow)
                await recorder.recordOutcome(.dayZeroMarked(
                    assetId: candidate.assetId, cost: cost, markCount: markCount, newState: newState))
                return CandidateResult(cost: cost, rotated: true, failed: false)
            } else {
                // POISONING FIX: no marks ⇒ bandwidth accounted, NO state advance
                // (the asset stays a lagged candidate — nothing resolved).
                await recorder.recordOutcome(.dayZeroUnmarked(
                    assetId: candidate.assetId, cost: cost, error: nil))
                return CandidateResult(cost: cost, rotated: false, failed: false)
            }
        } catch {
            // POISONING FIX: a day-0 fetch error also advances NO lagged state —
            // bytes spent are accounted; the lagged sweep runs later unimpeded.
            let cost = RediffRefetchPolicy.BandwidthCost(precheckBytes: 0, fullFetchBytes: fullFetchBytes)
            await recorder.recordOutcome(.dayZeroUnmarked(
                assetId: candidate.assetId, cost: cost, error: String(describing: error)))
            return CandidateResult(cost: cost, rotated: false, failed: true)
        }
    }

    // MARK: - BGTask registration (playhead-xsdz.36 activation)

    /// Process-wide once-guard: `BGTaskScheduler.register` crashes on a second
    /// registration of the same identifier (mirrors
    /// `BackgroundProcessingService.registerOnce`).
    private static let registrationClaimed = OSAllocatedUnfairLock(initialState: false)

    private nonisolated static func claimRegistration() -> Bool {
        registrationClaimed.withLock { claimed in
            if claimed { return false }
            claimed = true
            return true
        }
    }

    /// Test-only reset is deliberately absent — registration is process-wide
    /// by BGTaskScheduler's own semantics.

    /// Register the launch handler for `Self.taskIdentifier`. Must be called
    /// before app launch ends (BGTaskScheduler requirement). A no-op when the
    /// service is disabled (the OFF byte-identity contract: nothing is
    /// registered, nothing is scheduled) or when another instance already
    /// registered in this process.
    nonisolated func registerBackgroundTaskHandler() {
        guard enabled else { return }
        guard Self.claimRegistration() else { return }
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: Self.taskIdentifier,
            using: nil
        ) { [weak self] task in
            guard let self, let processingTask = task as? BGProcessingTask else {
                task.setTaskCompleted(success: false)
                return
            }
            let box = _UncheckedSendableBox(processingTask)
            Task { await self.handleRefetchTask(box.value) }
        }
    }

    // MARK: - Completion

    /// Expiration hop: flip `expired` so the sweep bails, and complete once.
    /// Only the CURRENT fire's expiration may abort the sweep — a late
    /// expiration hop from an already-completed earlier fire must not poison
    /// a successor fire's in-flight sweep.
    private func markExpiredAndComplete(_ task: any BackgroundProcessingTaskProtocol) {
        if currentTaskID == ObjectIdentifier(task as AnyObject) {
            expired = true
            logger.info("rediff re-fetch task expired — bailing at next boundary")
        }
        completeTaskOnce(task, success: false)
    }

    /// Idempotent `setTaskCompleted`, PER TASK (first caller for a given task
    /// wins; other tasks' completions are unaffected).
    private func completeTaskOnce(_ task: any BackgroundProcessingTaskProtocol, success: Bool) {
        let id = ObjectIdentifier(task as AnyObject)
        guard !completedTaskIDs.contains(id) else { return }
        completedTaskIDs.insert(id)
        task.setTaskCompleted(success: success)
    }
}

// MARK: - UncheckedSendable helper

/// Asserts the wrapped BG task crosses the expiration-callback isolation
/// boundary exactly once and is touched on only one actor thereafter (mirrors
/// the same-named helper in the sibling BGTask services).
private struct _UncheckedSendableBox<T>: @unchecked Sendable {
    let value: T
    init(_ value: T) { self.value = value }
}
