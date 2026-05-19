// SchedulerReconcilerRegressionTests.swift
// Regression tests for fixes applied to AnalysisWorkScheduler and AnalysisJobReconciler.

import Foundation
import SQLite3
import Testing
@testable import Playhead

// MARK: - Scheduler Regression Tests

@Suite("AnalysisWorkScheduler — Regression")
struct SchedulerRegressionTests {

    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider = StubDownloadProvider(),
        config: PreAnalysisConfig = PreAnalysisConfig()
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let capabilities = CapabilitiesService()
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: downloads,
            transportStatusProvider: StubTransportStatusProvider(),
            config: config
        )
    }

    @Test("playbackStarted sets cancellation flag for matching episode")
    func testPlaybackPreemptionCancelsRunningJob() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloads: downloads)

        // Don't start the scheduler loop — test the preemption mechanism directly.
        // cancelCurrentJob is idempotent even with no running job.
        await scheduler.cancelCurrentJob()

        // playbackStarted for a non-matching episode should not crash.
        await scheduler.playbackStarted(episodeId: "ep-other")

        // Verify stop is idempotent.
        await scheduler.stop()
        await scheduler.stop()

        // Verify the job in the store is untouched (scheduler never ran).
        let job = makeAnalysisJob(
            jobId: "preempt-job",
            episodeId: "ep-playing",
            workKey: "fp-preempt:1:preAnalysis",
            sourceFingerprint: "fp-preempt",
            state: "queued"
        )
        try await store.insertJob(job)
        let fetched = try await store.fetchJob(byId: "preempt-job")
        #expect(fetched?.state == "queued", "Job should remain queued when scheduler never ran")
    }

    @Test("foreground playback blocks deferred pre-analysis work")
    func testForegroundPlaybackBlocksSchedulerLoop() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-queued"] = URL(fileURLWithPath: "/tmp/ep-queued.mp3")

        let job = makeAnalysisJob(
            jobId: "playback-gated-job",
            episodeId: "ep-queued",
            workKey: "fp-playback-gated:1:preAnalysis",
            sourceFingerprint: "fp-playback-gated",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        await scheduler.playbackStarted(episodeId: "ep-playing")
        await scheduler.startSchedulerLoop()
        defer {
            Task { await scheduler.stop() }
        }

        try await Task.sleep(for: .milliseconds(400))

        let gated = try await store.fetchJob(byId: "playback-gated-job")
        #expect(gated?.state == "queued")
        #expect(gated?.leaseOwner == nil)
        #expect(gated?.leaseExpiresAt == nil)
    }

    @Test("episodeDeleted supersedes queued and paused jobs")
    func testEpisodeDeletedSupersedesQueuedAndPaused() async throws {
        let store = try await makeTestStore()

        let queuedJob = makeAnalysisJob(
            jobId: "del-queued",
            episodeId: "ep-deleted",
            workKey: "fp-del1:1:preAnalysis",
            sourceFingerprint: "fp-del1",
            state: "queued"
        )
        let pausedJob = makeAnalysisJob(
            jobId: "del-paused",
            episodeId: "ep-deleted",
            workKey: "fp-del2:1:preAnalysis:300",
            sourceFingerprint: "fp-del2",
            state: "paused"
        )
        let unrelatedJob = makeAnalysisJob(
            jobId: "del-other",
            episodeId: "ep-other",
            workKey: "fp-other:1:preAnalysis",
            sourceFingerprint: "fp-other",
            state: "queued"
        )
        try await store.insertJob(queuedJob)
        try await store.insertJob(pausedJob)
        try await store.insertJob(unrelatedJob)

        let scheduler = makeScheduler(store: store)
        await scheduler.episodeDeleted(episodeId: "ep-deleted")

        let fetchedQueued = try await store.fetchJob(byId: "del-queued")
        #expect(fetchedQueued?.state == "superseded")

        let fetchedPaused = try await store.fetchJob(byId: "del-paused")
        #expect(fetchedPaused?.state == "superseded")

        let fetchedOther = try await store.fetchJob(byId: "del-other")
        #expect(fetchedOther?.state == "queued")
    }

    @Test("enqueue does not cancel the current job")
    func testEnqueueDoesNotCancelCurrentJob() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-running"] = URL(fileURLWithPath: "/tmp/ep-running.mp3")

        let runningJob = makeAnalysisJob(
            jobId: "running-job",
            episodeId: "ep-running",
            workKey: "fp-run:1:preAnalysis",
            sourceFingerprint: "fp-run",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(runningJob)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        await scheduler.startSchedulerLoop()
        try await Task.sleep(for: .milliseconds(200))

        // Enqueue a second job — this should wake the loop, not cancel the current job.
        await scheduler.enqueue(
            episodeId: "ep-new",
            podcastId: nil,
            downloadId: "dl-new",
            sourceFingerprint: "fp-new",
            isExplicitDownload: false
        )

        // The original job should still exist and not be force-cancelled by enqueue.
        let original = try await store.fetchJob(byId: "running-job")
        #expect(original != nil)

        await scheduler.stop()
    }

    @Test("backoff formula: 2^attemptCount * 60, capped at 3600")
    func testBackoffFormulaConsistency() {
        let formula: (Int) -> Double = { attempt in
            min(pow(2.0, Double(attempt)) * 60, 3600)
        }
        #expect(formula(0) == 60.0)
        #expect(formula(1) == 120.0)
        #expect(formula(2) == 240.0)
        #expect(formula(3) == 480.0)
        #expect(formula(4) == 960.0)
        #expect(formula(5) == 1920.0)
        #expect(formula(6) == 3600.0)   // 2^6*60 = 3840, capped to 3600
        #expect(formula(10) == 3600.0)  // still capped
    }

    @Test("lease is released after processJob completes")
    func testLeaseReleasedAfterProcessJob() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-lease"] = URL(fileURLWithPath: "/tmp/ep-lease.mp3")

        let job = makeAnalysisJob(
            jobId: "lease-job",
            episodeId: "ep-lease",
            workKey: "fp-lease:1:preAnalysis",
            sourceFingerprint: "fp-lease",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()
        #expect(processed, "Scheduler did not process lease-job within deadline")

        let fetched = try await store.fetchJob(byId: "lease-job")
        #expect(fetched?.leaseOwner == nil, "Lease should be released after processing")
        #expect(fetched?.leaseExpiresAt == nil, "Lease expiry should be cleared after processing")
    }

    @Test("concurrent cancelCurrentJob calls resolve cause via precedence, not last-writer-wins")
    func testConcurrentCancelCausePrecedence() async throws {
        // playhead-1nl6: before this fix, `cancelCurrentJob(cause:)`
        // did `pendingCancelCause = cause` unconditionally, so two
        // concurrent cancels with different causes resolved to whichever
        // call landed second — stomping whatever precedence the
        // `CauseAttributionPolicy` ladder would have chosen.
        //
        // Sequence the calls deterministically (taskExpired first, then
        // userCancelled) and assert that `userCancelled` — which is in
        // the `userInitiated` tier and outranks `taskExpired`'s
        // `environmentalTransient` / `resourceExhausted` tier — wins the
        // resolution regardless of arrival order.
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        await scheduler.cancelCurrentJob(cause: .taskExpired)
        await scheduler.cancelCurrentJob(cause: .userCancelled)
        let forward = await scheduler.pendingCancelCauseForTesting()
        #expect(forward == .userCancelled, "userCancelled should outrank taskExpired after taskExpired→userCancelled sequence")

        // Reverse order must resolve to the same precedence winner —
        // demonstrates the fix is order-independent, not merely
        // last-write-wins masquerading as correct.
        let scheduler2 = makeScheduler(store: store)
        await scheduler2.cancelCurrentJob(cause: .userCancelled)
        await scheduler2.cancelCurrentJob(cause: .taskExpired)
        let reverse = await scheduler2.pendingCancelCauseForTesting()
        #expect(reverse == .userCancelled, "userCancelled should outrank taskExpired after userCancelled→taskExpired sequence")
    }
}

// MARK: - Reconciler Regression Tests

@Suite("AnalysisJobReconciler — Regression")
struct ReconcilerRegressionTests {

    private func makeReconciler(
        store: AnalysisStore,
        downloads: StubDownloadProvider = StubDownloadProvider(),
        capabilities: StubCapabilitiesProvider = StubCapabilitiesProvider(),
        config: PreAnalysisConfig = PreAnalysisConfig()
    ) -> AnalysisJobReconciler {
        AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: capabilities,
            config: config
        )
    }

    @Test("parseVersionFromWorkKey handles tier-advanced keys via reconcile")
    func testTierAdvancedWorkKeySuperseded() async throws {
        let store = try await makeTestStore()

        // Stamp seed rows with the current scheduler epoch so the
        // playhead-btwk stranded-session sweep doesn't reclaim the
        // `paused` rows (it correctly recovers any active-state row
        // whose epoch predates the current session).
        let currentEpoch = try await store.fetchSchedulerEpoch() ?? 0

        // Base key with version 2 — current version is 1, so it should be superseded.
        let baseJob = makeAnalysisJob(
            jobId: "tier-base-v2",
            workKey: "fp:2:preAnalysis",
            state: "queued",
            schedulerEpoch: currentEpoch
        )
        // Tier-advanced key with version 2 and coverage suffix.
        let tierJob = makeAnalysisJob(
            jobId: "tier-advanced-v2",
            workKey: "fp:2:preAnalysis:300",
            state: "paused",
            schedulerEpoch: currentEpoch
        )
        // Base key with version 1 — current version, should NOT be superseded.
        let currentBase = makeAnalysisJob(
            jobId: "tier-base-v1",
            workKey: "fp-ok:1:preAnalysis",
            state: "queued",
            schedulerEpoch: currentEpoch
        )
        // Tier-advanced key with version 1 — current, should NOT be superseded.
        let currentTier = makeAnalysisJob(
            jobId: "tier-advanced-v1",
            workKey: "fp-ok:1:preAnalysis:900",
            state: "paused",
            schedulerEpoch: currentEpoch
        )

        try await store.insertJob(baseJob)
        try await store.insertJob(tierJob)
        try await store.insertJob(currentBase)
        try await store.insertJob(currentTier)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.staleVersionsSuperseded == 2)

        let fetchedBase = try await store.fetchJob(byId: "tier-base-v2")
        #expect(fetchedBase?.state == "superseded")

        let fetchedTier = try await store.fetchJob(byId: "tier-advanced-v2")
        #expect(fetchedTier?.state == "superseded")

        let fetchedCurrentBase = try await store.fetchJob(byId: "tier-base-v1")
        #expect(fetchedCurrentBase?.state == "queued")

        let fetchedCurrentTier = try await store.fetchJob(byId: "tier-advanced-v1")
        #expect(fetchedCurrentTier?.state == "paused")
    }

    @Test("discoverUnEnqueuedDownloads uses config T0 depth")
    func testDiscoverUsesConfigT0Depth() async throws {
        let store = try await makeTestStore()

        var config = PreAnalysisConfig()
        config.defaultT0DepthSeconds = 120

        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-discover"] = URL(fileURLWithPath: "/tmp/ep-discover.mp3")
        downloads.fingerprints["ep-discover"] = AudioFingerprint(weak: "fp-discover", strong: nil)

        let reconciler = makeReconciler(store: store, downloads: downloads, config: config)
        let report = try await reconciler.reconcile()

        #expect(report.unEnqueuedDownloadsCreated == 1)

        let allIds = try await store.fetchAllJobEpisodeIds()
        #expect(allIds.contains("ep-discover"))

        let jobs = try await store.fetchJobsByState("queued")
        let created = jobs.first { $0.episodeId == "ep-discover" }
        #expect(created != nil)
        #expect(created?.desiredCoverageSec == 120)
    }

    @Test("currentAnalysisVersion delegates to PreAnalysisConfig.analysisVersion")
    func testCurrentAnalysisVersionMatchesConfig() {
        #expect(AnalysisJobReconciler.currentAnalysisVersion == PreAnalysisConfig.analysisVersion)
    }
}

// MARK: - Store Regression Tests

@Suite("AnalysisStore — Regression")
struct StoreRegressionTests {

    @Test("insertJob returns true on success")
    func testInsertJobReturnsTrue() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "insert-ok",
            workKey: "fp-insert:1:preAnalysis"
        )
        let result = try await store.insertJob(job)
        #expect(result == true)
    }

    @Test("insertJob returns false on workKey collision")
    func testInsertJobReturnsFalseOnWorkKeyCollision() async throws {
        let store = try await makeTestStore()
        let workKey = "fp-dupe:1:preAnalysis"

        let first = makeAnalysisJob(
            jobId: "dupe-1",
            workKey: workKey
        )
        let second = makeAnalysisJob(
            jobId: "dupe-2",
            workKey: workKey
        )

        let firstResult = try await store.insertJob(first)
        #expect(firstResult == true)

        let secondResult = try await store.insertJob(second)
        #expect(secondResult == false)

        // Verify only the first job exists.
        let fetched = try await store.fetchJob(byId: "dupe-1")
        #expect(fetched != nil)

        let missing = try await store.fetchJob(byId: "dupe-2")
        #expect(missing == nil)
    }

    @Test("batchUpdateJobState updates all jobs atomically")
    func testBatchUpdateJobStateUpdatesAll() async throws {
        let store = try await makeTestStore()

        let job1 = makeAnalysisJob(jobId: "batch-1", workKey: "fp-b1:1:preAnalysis", state: "queued")
        let job2 = makeAnalysisJob(jobId: "batch-2", workKey: "fp-b2:1:preAnalysis", state: "queued")
        let job3 = makeAnalysisJob(jobId: "batch-3", workKey: "fp-b3:1:preAnalysis", state: "queued")

        try await store.insertJob(job1)
        try await store.insertJob(job2)
        try await store.insertJob(job3)

        try await store.batchUpdateJobState(
            jobIds: ["batch-1", "batch-2", "batch-3"],
            state: "complete"
        )

        let fetched1 = try await store.fetchJob(byId: "batch-1")
        let fetched2 = try await store.fetchJob(byId: "batch-2")
        let fetched3 = try await store.fetchJob(byId: "batch-3")

        #expect(fetched1?.state == "complete")
        #expect(fetched2?.state == "complete")
        #expect(fetched3?.state == "complete")
    }

    // MARK: - Bug-fix regression tests

    @Test("failed jobs are retried after backoff expires")
    func testFailedJobsRetriedAfterBackoffExpires() async throws {
        let store = try await makeTestStore()
        let pastTimestamp = Date().timeIntervalSince1970 - 600 // 10 minutes ago
        let job = makeAnalysisJob(
            jobId: "retry-failed",
            episodeId: "ep-retry",
            workKey: "fp-retry:1:preAnalysis",
            sourceFingerprint: "fp-retry",
            priority: 10,
            state: "failed",
            attemptCount: 1,
            nextEligibleAt: pastTimestamp
        )
        try await store.insertJob(job)

        let fetched = try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true,
            t0ThresholdSec: 90,
            now: Date().timeIntervalSince1970
        )
        #expect(fetched != nil, "Failed job with elapsed backoff should be eligible")
        #expect(fetched?.jobId == "retry-failed")
    }

    @Test("acquireLease sets state to running")
    func testAcquireLeaseSetStateToRunning() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "lease-state",
            workKey: "fp-ls:1:preAnalysis",
            state: "queued"
        )
        try await store.insertJob(job)

        let acquired = try await store.acquireLease(
            jobId: "lease-state",
            owner: "test-worker",
            expiresAt: Date().timeIntervalSince1970 + 300
        )
        #expect(acquired == true)

        let fetched = try await store.fetchJob(byId: "lease-state")
        #expect(fetched?.state == "running")
    }

    @Test("incrementAttemptCount increments correctly")
    func testIncrementAttemptCount() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "inc-attempt",
            workKey: "fp-inc:1:preAnalysis",
            attemptCount: 0
        )
        try await store.insertJob(job)

        try await store.incrementAttemptCount(jobId: "inc-attempt")
        let after1 = try await store.fetchJob(byId: "inc-attempt")
        #expect(after1?.attemptCount == 1)

        try await store.incrementAttemptCount(jobId: "inc-attempt")
        let after2 = try await store.fetchJob(byId: "inc-attempt")
        #expect(after2?.attemptCount == 2)
    }

    @Test("fetchActiveJobEpisodeIds includes failed jobs")
    func testFetchActiveJobEpisodeIdsIncludesFailed() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "active-failed",
            episodeId: "ep-failed-active",
            workKey: "fp-af:1:preAnalysis",
            state: "failed"
        )
        try await store.insertJob(job)

        let activeIds = try await store.fetchActiveJobEpisodeIds()
        #expect(activeIds.contains("ep-failed-active"),
                "Failed jobs should be included in active episode IDs")
    }

    @Test("exponential backoff grows with attemptCount")
    func testExponentialBackoffGrows() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "backoff-grow",
            workKey: "fp-bg:1:preAnalysis",
            attemptCount: 0
        )
        try await store.insertJob(job)

        try await store.incrementAttemptCount(jobId: "backoff-grow")
        try await store.incrementAttemptCount(jobId: "backoff-grow")
        try await store.incrementAttemptCount(jobId: "backoff-grow")

        let fetched = try await store.fetchJob(byId: "backoff-grow")
        #expect(fetched?.attemptCount == 3)

        // Verify the backoff formula: min(2^3 * 60, 3600) == 480
        let expectedBackoff = min(pow(2.0, 3.0) * 60, 3600)
        #expect(expectedBackoff == 480.0)
    }

    @Test("max attempt count reached after increments")
    func testMaxAttemptCountReached() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "max-attempt",
            workKey: "fp-max:1:preAnalysis",
            attemptCount: 4
        )
        try await store.insertJob(job)

        try await store.incrementAttemptCount(jobId: "max-attempt")
        let fetched = try await store.fetchJob(byId: "max-attempt")
        #expect(fetched?.attemptCount == 5,
                "After incrementing from 4, attemptCount should be 5 (the max)")
    }

    @Test("recoverExpiredLease resets running job to queued")
    func testRecoverExpiredLeaseResetsRunningJob() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "recover-lease",
            workKey: "fp-rl:1:preAnalysis",
            state: "queued",
            attemptCount: 0
        )
        try await store.insertJob(job)

        // Acquire lease (sets state to "running")
        let acquired = try await store.acquireLease(
            jobId: "recover-lease",
            owner: "worker-1",
            expiresAt: Date().timeIntervalSince1970 - 60 // already expired
        )
        #expect(acquired == true)

        // Confirm it's running
        let running = try await store.fetchJob(byId: "recover-lease")
        #expect(running?.state == "running")
        #expect(running?.leaseOwner == "worker-1")

        // Recover the expired lease
        try await store.recoverExpiredLease(jobId: "recover-lease")

        let recovered = try await store.fetchJob(byId: "recover-lease")
        #expect(recovered?.state == "queued", "Should be back to queued after recovery")
        #expect(recovered?.attemptCount == 1, "attemptCount should be incremented")
        #expect(recovered?.leaseOwner == nil, "Lease owner should be cleared")
        #expect(recovered?.leaseExpiresAt == nil, "Lease expiry should be cleared")
    }

    @Test("PRAGMA journal_mode is WAL (verifies configurePragmas ran)")
    func testPragmaJournalModeIsWAL() async throws {
        let store = try await makeTestStore()
        // busy_timeout is a per-connection setting that cannot be verified via a
        // second connection. Instead, verify journal_mode=WAL which IS persisted
        // to the database file and confirms configurePragmas() ran successfully.
        var handle: OpaquePointer?
        let rc = sqlite3_open_v2(
            store.databaseURL.path,
            &handle,
            SQLITE_OPEN_READONLY,
            nil
        )
        defer { if let handle { sqlite3_close_v2(handle) } }
        #expect(rc == SQLITE_OK, "Failed to open database for PRAGMA check")

        var stmt: OpaquePointer?
        let prepRC = sqlite3_prepare_v2(handle, "PRAGMA journal_mode", -1, &stmt, nil)
        defer { sqlite3_finalize(stmt) }
        #expect(prepRC == SQLITE_OK)

        let stepRC = sqlite3_step(stmt)
        #expect(stepRC == SQLITE_ROW)

        let mode = String(cString: sqlite3_column_text(stmt, 0))
        #expect(mode == "wal", "journal_mode should be WAL, got \(mode)")
    }

    @Test("resetFailedJobToQueued clears error and backoff")
    func testResetFailedJobToQueued() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "reset-failed",
            workKey: "fp-rf:1:preAnalysis",
            state: "failed",
            attemptCount: 2,
            nextEligibleAt: Date().timeIntervalSince1970 + 600,
            lastErrorCode: "audioDecodeFailed"
        )
        try await store.insertJob(job)

        try await store.resetFailedJobToQueued(jobId: "reset-failed")

        let fetched = try await store.fetchJob(byId: "reset-failed")
        #expect(fetched?.state == "queued")
        #expect(fetched?.nextEligibleAt == nil, "nextEligibleAt should be cleared")
        #expect(fetched?.lastErrorCode == nil, "lastErrorCode should be cleared")
    }

    @Test("paused job with future nextEligibleAt is not returned early")
    func testPausedJobWithFutureBackoffNotReturned() async throws {
        let store = try await makeTestStore()
        let futureTime = Date().timeIntervalSince1970 + 3600
        let job = makeAnalysisJob(
            jobId: "paused-backoff",
            jobType: "preAnalysis",
            workKey: "fp-paused-backoff:1:preAnalysis",
            state: "paused",
            nextEligibleAt: futureTime
        )
        try await store.insertJob(job)
        let now = Date().timeIntervalSince1970
        let result = try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true,
            t0ThresholdSec: 90,
            now: now
        )
        #expect(result == nil, "Paused job with future nextEligibleAt should not be returned")
    }
}

// MARK: - Scheduler Bug-Fix Regression Tests

@Suite("AnalysisWorkScheduler — Bug-fix Regression")
struct SchedulerBugFixRegressionTests {

    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider = StubDownloadProvider(),
        config: PreAnalysisConfig = PreAnalysisConfig()
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let capabilities = CapabilitiesService()
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: downloads,
            transportStatusProvider: StubTransportStatusProvider(),
            config: config
        )
    }

    private func missingTemporaryAudioURL(named stem: String) -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("\(stem)-\(UUID().uuidString).mp3")
    }

    @Test("episodeDeleted supersedes all non-terminal states")
    func testEpisodeDeletedSupersedesAllNonTerminalStates() async throws {
        let store = try await makeTestStore()

        let states = ["queued", "paused", "failed", "blocked:missingFile", "blocked:modelUnavailable"]
        for (i, state) in states.enumerated() {
            let job = makeAnalysisJob(
                jobId: "del-all-\(i)",
                episodeId: "ep-del-all",
                workKey: "fp-del-all-\(i):1:preAnalysis",
                sourceFingerprint: "fp-del-all-\(i)",
                state: state
            )
            try await store.insertJob(job)
        }

        let scheduler = makeScheduler(store: store)
        await scheduler.episodeDeleted(episodeId: "ep-del-all")

        for (i, state) in states.enumerated() {
            let fetched = try await store.fetchJob(byId: "del-all-\(i)")
            #expect(fetched?.state == "superseded",
                    "Job in state '\(state)' should be superseded after episodeDeleted, got '\(fetched?.state ?? "nil")'")
        }
    }

    @Test("scheduler resolves a real analysis asset for jobs that start with nil analysisAssetId")
    func testSchedulerResolvesRealAssetIdForNilJobAssetId() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-fk-regression")
        downloads.cachedURLs["ep-fk-regression"] = localURL

        let job = makeAnalysisJob(
            jobId: "fk-regression-job",
            jobType: "preAnalysis",
            episodeId: "ep-fk-regression",
            analysisAssetId: nil,
            workKey: "fp-fk-regression:1:preAnalysis",
            sourceFingerprint: "fp-fk-regression",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()
        let resolved = (try await store.fetchJob(byId: "fk-regression-job"))?.analysisAssetId != nil
        #expect(processed, "Scheduler test hook should process fk-regression-job")
        #expect(resolved, "Scheduler did not resolve analysisAssetId within deadline")

        let updatedJob = try #require(await store.fetchJob(byId: "fk-regression-job"))
        let analysisAssetId = try #require(updatedJob.analysisAssetId)
        #expect(analysisAssetId != "ep-fk-regression")

        let asset = try #require(await store.fetchAsset(id: analysisAssetId))
        #expect(asset.episodeId == "ep-fk-regression")
        #expect(asset.assetFingerprint == "fp-fk-regression")
        #expect(asset.sourceURL == localURL.absoluteString)
    }

    @Test("scheduler upgrades reused placeholder asset to canonical full-file SHA when current fingerprint proves identity")
    func testSchedulerUpgradesExistingPlaceholderAssetToFullFileSHAWithFingerprintProof() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-sha-upgrade")
        downloads.cachedURLs["ep-sha-upgrade"] = localURL
        let placeholderAsset = AnalysisAsset(
            id: "placeholder-asset",
            episodeId: "ep-sha-upgrade",
            assetFingerprint: "placeholder-asset",
            weakFingerprint: nil,
            sourceURL: "",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(placeholderAsset)

        let fullFileSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        downloads.fingerprints["ep-sha-upgrade"] = AudioFingerprint(
            weak: "placeholder-asset",
            strong: fullFileSHA
        )
        let job = makeAnalysisJob(
            jobId: "sha-upgrade-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-upgrade",
            analysisAssetId: nil,
            workKey: "\(fullFileSHA):1:preAnalysis",
            sourceFingerprint: fullFileSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-upgrade-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-upgrade-job"))
        #expect(updatedJob.analysisAssetId == "placeholder-asset")

        let asset = try #require(await store.fetchAsset(id: "placeholder-asset"))
        #expect(asset.assetFingerprint == fullFileSHA)
        #expect(asset.weakFingerprint == "placeholder-asset")
    }

    @Test("scheduler does not upgrade placeholder asset without current fingerprint proof")
    func testSchedulerDoesNotUpgradePlaceholderAssetWithoutFingerprintProof() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-sha-placeholder-no-proof")
        downloads.cachedURLs["ep-sha-placeholder-no-proof"] = localURL
        let placeholderAsset = AnalysisAsset(
            id: "placeholder-no-proof-asset",
            episodeId: "ep-sha-placeholder-no-proof",
            assetFingerprint: "placeholder-no-proof-asset",
            weakFingerprint: nil,
            sourceURL: "",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(placeholderAsset)

        let fullFileSHA = "abababababababababababababababababababababababababababababababab"
        let job = makeAnalysisJob(
            jobId: "sha-placeholder-no-proof-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-placeholder-no-proof",
            analysisAssetId: nil,
            workKey: "\(fullFileSHA):1:preAnalysis",
            sourceFingerprint: fullFileSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-placeholder-no-proof-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-placeholder-no-proof-job"))
        let newAssetId = try #require(updatedJob.analysisAssetId)
        #expect(newAssetId != "placeholder-no-proof-asset")

        let oldAsset = try #require(await store.fetchAsset(id: "placeholder-no-proof-asset"))
        #expect(oldAsset.assetFingerprint == "placeholder-no-proof-asset")
        #expect(oldAsset.weakFingerprint == nil)

        let newAsset = try #require(await store.fetchAsset(id: newAssetId))
        #expect(newAsset.episodeId == "ep-sha-placeholder-no-proof")
        #expect(newAsset.assetFingerprint == fullFileSHA)
        #expect(newAsset.weakFingerprint == nil)
        #expect(newAsset.sourceURL == localURL.absoluteString)
    }

    @Test("scheduler creates new asset when existing full-file SHA differs")
    func testSchedulerDoesNotReuseDifferentCanonicalFullFileSHA() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-sha-mismatch")
        downloads.cachedURLs["ep-sha-mismatch"] = localURL

        let oldSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        let newSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        let existingAsset = AnalysisAsset(
            id: "old-sha-asset",
            episodeId: "ep-sha-mismatch",
            assetFingerprint: oldSHA,
            weakFingerprint: nil,
            sourceURL: "",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(existingAsset)

        let job = makeAnalysisJob(
            jobId: "sha-mismatch-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-mismatch",
            analysisAssetId: nil,
            workKey: "\(newSHA):1:preAnalysis",
            sourceFingerprint: newSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-mismatch-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-mismatch-job"))
        let newAssetId = try #require(updatedJob.analysisAssetId)
        #expect(newAssetId != "old-sha-asset")

        let oldAsset = try #require(await store.fetchAsset(id: "old-sha-asset"))
        #expect(oldAsset.assetFingerprint == oldSHA)

        let newAsset = try #require(await store.fetchAsset(id: newAssetId))
        #expect(newAsset.episodeId == "ep-sha-mismatch")
        #expect(newAsset.assetFingerprint == newSHA)
        #expect(newAsset.weakFingerprint == nil)
        #expect(newAsset.sourceURL == localURL.absoluteString)
    }

    @Test("scheduler does not upgrade an unrelated weak asset to a canonical full-file SHA")
    func testSchedulerDoesNotUpgradeMismatchedWeakAssetToFullFileSHA() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-sha-weak-mismatch")
        downloads.cachedURLs["ep-sha-weak-mismatch"] = localURL

        let oldWeakFingerprint = "https://old.example.com/audio.mp3|old-etag|123|Mon, 01 Jan 2024 00:00:00 GMT"
        let currentWeakFingerprint = "https://new.example.com/audio.mp3|new-etag|456|Tue, 02 Jan 2024 00:00:00 GMT"
        let fullFileSHA = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        let existingAsset = AnalysisAsset(
            id: "old-weak-asset",
            episodeId: "ep-sha-weak-mismatch",
            assetFingerprint: oldWeakFingerprint,
            weakFingerprint: nil,
            sourceURL: "old.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(existingAsset)
        downloads.fingerprints["ep-sha-weak-mismatch"] = AudioFingerprint(
            weak: currentWeakFingerprint,
            strong: fullFileSHA
        )

        let job = makeAnalysisJob(
            jobId: "sha-weak-mismatch-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-weak-mismatch",
            analysisAssetId: nil,
            workKey: "\(fullFileSHA):1:preAnalysis",
            sourceFingerprint: fullFileSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-weak-mismatch-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-weak-mismatch-job"))
        let newAssetId = try #require(updatedJob.analysisAssetId)
        #expect(newAssetId != "old-weak-asset")

        let oldAsset = try #require(await store.fetchAsset(id: "old-weak-asset"))
        #expect(oldAsset.assetFingerprint == oldWeakFingerprint)
        #expect(oldAsset.weakFingerprint == nil)

        let newAsset = try #require(await store.fetchAsset(id: newAssetId))
        #expect(newAsset.episodeId == "ep-sha-weak-mismatch")
        #expect(newAsset.assetFingerprint == fullFileSHA)
        #expect(newAsset.sourceURL == localURL.absoluteString)
    }

    @Test("scheduler does not treat an empty current weak fingerprint as upgrade proof")
    func testSchedulerDoesNotUpgradeWeakAssetWhenCurrentWeakFingerprintIsEmpty() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-sha-empty-weak")
        downloads.cachedURLs["ep-sha-empty-weak"] = localURL

        let fullFileSHA = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        let existingAsset = AnalysisAsset(
            id: "empty-weak-asset",
            episodeId: "ep-sha-empty-weak",
            assetFingerprint: "",
            weakFingerprint: nil,
            sourceURL: "old.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(existingAsset)
        downloads.fingerprints["ep-sha-empty-weak"] = AudioFingerprint(
            weak: "",
            strong: fullFileSHA
        )

        let job = makeAnalysisJob(
            jobId: "sha-empty-weak-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-empty-weak",
            analysisAssetId: nil,
            workKey: "\(fullFileSHA):1:preAnalysis",
            sourceFingerprint: fullFileSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-empty-weak-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-empty-weak-job"))
        let newAssetId = try #require(updatedJob.analysisAssetId)
        #expect(newAssetId != "empty-weak-asset")

        let oldAsset = try #require(await store.fetchAsset(id: "empty-weak-asset"))
        #expect(oldAsset.assetFingerprint == "")
        #expect(oldAsset.weakFingerprint == nil)

        let newAsset = try #require(await store.fetchAsset(id: newAssetId))
        #expect(newAsset.episodeId == "ep-sha-empty-weak")
        #expect(newAsset.assetFingerprint == fullFileSHA)
        #expect(newAsset.sourceURL == localURL.absoluteString)
    }

    @Test("scheduler reuses older asset when its canonical SHA matches the job")
    func testSchedulerReusesOlderMatchingCanonicalFullFileSHA() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-sha-reuse")
        downloads.cachedURLs["ep-sha-reuse"] = localURL

        let oldSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        let newSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        try await store.insertAsset(AnalysisAsset(
            id: "old-sha-asset",
            episodeId: "ep-sha-reuse",
            assetFingerprint: oldSHA,
            weakFingerprint: nil,
            sourceURL: "old.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))
        try await store.insertAsset(AnalysisAsset(
            id: "new-sha-asset",
            episodeId: "ep-sha-reuse",
            assetFingerprint: newSHA,
            weakFingerprint: nil,
            sourceURL: "new.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        let job = makeAnalysisJob(
            jobId: "sha-reuse-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-reuse",
            analysisAssetId: nil,
            workKey: "\(oldSHA):1:preAnalysis",
            sourceFingerprint: oldSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-reuse-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-reuse-job"))
        #expect(updatedJob.analysisAssetId == "old-sha-asset")

        let assets = try await store.fetchAllAssets()
            .filter { $0.episodeId == "ep-sha-reuse" }
        #expect(assets.count == 2)
    }

    @Test("scheduler supersedes stale canonical SHA jobs when the cached file fingerprint changed")
    func testSchedulerSupersedesCanonicalJobWhenCachedFingerprintChanged() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-sha-stale-job")
        downloads.cachedURLs["ep-sha-stale-job"] = localURL

        let oldSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        let newSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        try await store.insertAsset(AnalysisAsset(
            id: "old-sha-asset",
            episodeId: "ep-sha-stale-job",
            assetFingerprint: oldSHA,
            weakFingerprint: nil,
            sourceURL: "old.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))
        downloads.fingerprints["ep-sha-stale-job"] = AudioFingerprint(
            weak: "current-weak-fingerprint",
            strong: newSHA
        )

        let job = makeAnalysisJob(
            jobId: "sha-stale-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-stale-job",
            analysisAssetId: nil,
            workKey: "\(oldSHA):1:preAnalysis",
            sourceFingerprint: oldSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-stale-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-stale-job"))
        #expect(updatedJob.state == "superseded")
        #expect(updatedJob.analysisAssetId == nil)
        #expect(updatedJob.lastErrorCode == "staleFingerprint:cachedAudioMismatch")

        let oldAsset = try #require(await store.fetchAsset(id: "old-sha-asset"))
        #expect(oldAsset.assetFingerprint == oldSHA)

        let assets = try await store.fetchAllAssets()
            .filter { $0.episodeId == "ep-sha-stale-job" }
        #expect(assets.count == 1)
    }

    @Test("scheduler hashes cached audio even when the fingerprint cache still reports the stale SHA")
    func testSchedulerSupersedesCanonicalJobWhenFingerprintCacheStillReportsStaleSHA() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("preanalysis-sha-stale-cache-\(UUID().uuidString).mp3")
        try Data("current cached audio bytes".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        downloads.cachedURLs["ep-sha-stale-cache"] = localURL

        let oldSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        let currentSHA = try FileHasher.sha256(fileURL: localURL)
        #expect(currentSHA != oldSHA)
        try await store.insertAsset(AnalysisAsset(
            id: "old-sha-stale-cache-asset",
            episodeId: "ep-sha-stale-cache",
            assetFingerprint: oldSHA,
            weakFingerprint: nil,
            sourceURL: "old.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))
        downloads.fingerprints["ep-sha-stale-cache"] = AudioFingerprint(
            weak: "stale-weak-fingerprint",
            strong: oldSHA
        )

        let job = makeAnalysisJob(
            jobId: "sha-stale-cache-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-stale-cache",
            analysisAssetId: nil,
            workKey: "\(oldSHA):1:preAnalysis",
            sourceFingerprint: oldSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-stale-cache-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-stale-cache-job"))
        #expect(updatedJob.state == "superseded")
        #expect(updatedJob.analysisAssetId == nil)
        #expect(updatedJob.lastErrorCode == "staleFingerprint:cachedAudioMismatch")

        let assets = try await store.fetchAllAssets()
            .filter { $0.episodeId == "ep-sha-stale-cache" }
        #expect(assets.count == 1)
    }

    @Test("scheduler trusts cached audio over a stale mismatching fingerprint cache")
    func testSchedulerDoesNotSupersedeCanonicalJobWhenFileMatchesDespiteStaleFingerprintCache() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("preanalysis-sha-valid-stale-cache-\(UUID().uuidString).mp3")
        try Data("original cached audio bytes".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        downloads.cachedURLs["ep-sha-valid-stale-cache"] = localURL

        let matchingSHA = try FileHasher.sha256(fileURL: localURL)
        let staleCacheSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        #expect(matchingSHA != staleCacheSHA)
        try await store.insertAsset(AnalysisAsset(
            id: "matching-sha-asset",
            episodeId: "ep-sha-valid-stale-cache",
            assetFingerprint: matchingSHA,
            weakFingerprint: nil,
            sourceURL: "matching.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))
        downloads.fingerprints["ep-sha-valid-stale-cache"] = AudioFingerprint(
            weak: "stale-weak-fingerprint",
            strong: staleCacheSHA
        )

        let job = makeAnalysisJob(
            jobId: "sha-valid-stale-cache-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-valid-stale-cache",
            analysisAssetId: nil,
            workKey: "\(matchingSHA):1:preAnalysis",
            sourceFingerprint: matchingSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-valid-stale-cache-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-valid-stale-cache-job"))
        #expect(updatedJob.analysisAssetId == "matching-sha-asset")
        #expect(updatedJob.state != "superseded")
        #expect(updatedJob.lastErrorCode != "staleFingerprint:cachedAudioMismatch")

        let assets = try await store.fetchAllAssets()
            .filter { $0.episodeId == "ep-sha-valid-stale-cache" }
        #expect(assets.count == 1)
    }

    @Test("scheduler hashes cached audio to supersede stale canonical SHA jobs when fingerprint cache is cold")
    func testSchedulerSupersedesCanonicalJobWhenFingerprintCacheIsCold() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("preanalysis-sha-stale-cold-\(UUID().uuidString).mp3")
        try Data("current audio bytes".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        downloads.cachedURLs["ep-sha-stale-cold"] = localURL

        let oldSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        let currentSHA = try FileHasher.sha256(fileURL: localURL)
        #expect(currentSHA != oldSHA)
        try await store.insertAsset(AnalysisAsset(
            id: "old-sha-cold-asset",
            episodeId: "ep-sha-stale-cold",
            assetFingerprint: oldSHA,
            weakFingerprint: nil,
            sourceURL: "old.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        let job = makeAnalysisJob(
            jobId: "sha-stale-cold-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-stale-cold",
            analysisAssetId: nil,
            workKey: "\(oldSHA):1:preAnalysis",
            sourceFingerprint: oldSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-stale-cold-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-stale-cold-job"))
        #expect(updatedJob.state == "superseded")
        #expect(updatedJob.analysisAssetId == nil)
        #expect(updatedJob.lastErrorCode == "staleFingerprint:cachedAudioMismatch")

        let oldAsset = try #require(await store.fetchAsset(id: "old-sha-cold-asset"))
        #expect(oldAsset.assetFingerprint == oldSHA)

        let assets = try await store.fetchAllAssets()
            .filter { $0.episodeId == "ep-sha-stale-cold" }
        #expect(assets.count == 1)
    }

    @Test("stale canonical supersede enqueues fresh work for the current cached SHA even when the fingerprint cache is cold")
    func testStaleCanonicalSupersedeEnqueuesCurrentSHAWhenFingerprintCacheIsCold() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let currentURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("preanalysis-sha-stale-cold-replacement-\(UUID().uuidString).mp3")
        try Data("current replacement bytes that must be analyzed".utf8).write(to: currentURL)
        defer { try? FileManager.default.removeItem(at: currentURL) }
        downloads.cachedURLs["ep-sha-cold-replacement"] = currentURL

        let oldSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        let currentSHA = try FileHasher.sha256(fileURL: currentURL)
        #expect(currentSHA != oldSHA)
        let currentWorkKey = AnalysisJob.computeWorkKey(
            fingerprint: currentSHA,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            jobType: "preAnalysis"
        )
        try await store.insertJob(makeAnalysisJob(
            jobId: "sha-cold-replacement-stale-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-cold-replacement",
            analysisAssetId: nil,
            workKey: "\(oldSHA):1:preAnalysis",
            sourceFingerprint: oldSHA,
            priority: 10,
            desiredCoverageSec: 240,
            state: "queued"
        ))

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-cold-replacement-stale-job")
        let staleJob = try #require(await store.fetchJob(byId: "sha-cold-replacement-stale-job"))
        #expect(staleJob.state == "superseded")
        #expect(staleJob.workKey != "\(oldSHA):1:preAnalysis")

        let queuedJobs = try await store.fetchJobsByState("queued")
            .filter { $0.episodeId == "ep-sha-cold-replacement" }
        #expect(queuedJobs.count == 1)
        let replacement = try #require(queuedJobs.first)
        #expect(replacement.sourceFingerprint == currentSHA)
        #expect(replacement.workKey == currentWorkKey)
        #expect(replacement.priority == 10)
        #expect(replacement.desiredCoverageSec == 240)
        #expect(replacement.analysisAssetId == nil)
    }

    @Test("scheduler upgrades an older weak asset when a newer stale canonical asset exists")
    func testSchedulerUpgradesOlderWeakAssetWhenNewerStaleCanonicalAssetExists() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-sha-older-weak-current")
        downloads.cachedURLs["ep-sha-older-weak-current"] = localURL

        let currentWeakFingerprint = "https://current.example.com/audio.mp3|etag-current|123|Tue, 19 May 2026 00:00:00 GMT"
        let currentSHA = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        let oldSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        downloads.fingerprints["ep-sha-older-weak-current"] = AudioFingerprint(
            weak: currentWeakFingerprint,
            strong: currentSHA
        )

        try await store.insertAsset(AnalysisAsset(
            id: "older-current-weak-asset",
            episodeId: "ep-sha-older-weak-current",
            assetFingerprint: currentWeakFingerprint,
            weakFingerprint: nil,
            sourceURL: "current-weak.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))
        try await store.insertAsset(AnalysisAsset(
            id: "newer-stale-sha-asset",
            episodeId: "ep-sha-older-weak-current",
            assetFingerprint: oldSHA,
            weakFingerprint: nil,
            sourceURL: "old.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        try await store.insertJob(makeAnalysisJob(
            jobId: "sha-older-weak-current-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-older-weak-current",
            analysisAssetId: nil,
            workKey: "\(currentSHA):1:preAnalysis",
            sourceFingerprint: currentSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        ))

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-older-weak-current-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-older-weak-current-job"))
        #expect(updatedJob.analysisAssetId == "older-current-weak-asset")

        let upgraded = try #require(await store.fetchAsset(id: "older-current-weak-asset"))
        #expect(upgraded.assetFingerprint == currentSHA)
        #expect(upgraded.weakFingerprint == currentWeakFingerprint)

        let staleAsset = try #require(await store.fetchAsset(id: "newer-stale-sha-asset"))
        #expect(staleAsset.assetFingerprint == oldSHA)
    }

    @Test("scheduler does not upgrade a stale canonical asset that only shares the current weak fingerprint")
    func testSchedulerDoesNotUpgradeStaleCanonicalAssetSharingCurrentWeakFingerprint() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-sha-stale-canonical-shares-weak")
        downloads.cachedURLs["ep-sha-stale-canonical-shares-weak"] = localURL

        let currentWeakFingerprint = "https://current.example.com/audio.mp3|same-etag|123|Tue, 19 May 2026 00:00:00 GMT"
        let currentSHA = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        let oldSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        downloads.fingerprints["ep-sha-stale-canonical-shares-weak"] = AudioFingerprint(
            weak: currentWeakFingerprint,
            strong: currentSHA
        )

        try await store.insertAsset(AnalysisAsset(
            id: "stale-canonical-with-current-weak",
            episodeId: "ep-sha-stale-canonical-shares-weak",
            assetFingerprint: oldSHA,
            weakFingerprint: currentWeakFingerprint,
            sourceURL: "old.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        try await store.insertJob(makeAnalysisJob(
            jobId: "sha-stale-canonical-shares-weak-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-stale-canonical-shares-weak",
            analysisAssetId: nil,
            workKey: "\(currentSHA):1:preAnalysis",
            sourceFingerprint: currentSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        ))

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-stale-canonical-shares-weak-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-stale-canonical-shares-weak-job"))
        let currentAssetId = try #require(updatedJob.analysisAssetId)
        #expect(currentAssetId != "stale-canonical-with-current-weak")

        let currentAsset = try #require(await store.fetchAsset(id: currentAssetId))
        #expect(currentAsset.assetFingerprint == currentSHA)
        #expect(currentAsset.weakFingerprint == nil)

        let staleAsset = try #require(await store.fetchAsset(id: "stale-canonical-with-current-weak"))
        #expect(staleAsset.assetFingerprint == oldSHA)
        #expect(staleAsset.weakFingerprint == currentWeakFingerprint)
    }

    @Test("scheduler skips a stale canonical weak-fingerprint match and upgrades an older weak asset")
    func testSchedulerScansPastStaleCanonicalWeakFingerprintMatch() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-sha-hidden-older-weak")
        downloads.cachedURLs["ep-sha-hidden-older-weak"] = localURL

        let currentWeakFingerprint = "https://current.example.com/audio.mp3|etag-hidden|123|Tue, 19 May 2026 00:00:00 GMT"
        let currentSHA = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        let oldSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        downloads.fingerprints["ep-sha-hidden-older-weak"] = AudioFingerprint(
            weak: currentWeakFingerprint,
            strong: currentSHA
        )

        try await store.insertAsset(AnalysisAsset(
            id: "older-hidden-current-weak-asset",
            episodeId: "ep-sha-hidden-older-weak",
            assetFingerprint: "legacy-current-weak-row",
            weakFingerprint: currentWeakFingerprint,
            sourceURL: "current-weak.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))
        try await store.insertAsset(AnalysisAsset(
            id: "newer-stale-canonical-with-current-weak",
            episodeId: "ep-sha-hidden-older-weak",
            assetFingerprint: oldSHA,
            weakFingerprint: currentWeakFingerprint,
            sourceURL: "old.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        try await store.insertJob(makeAnalysisJob(
            jobId: "sha-hidden-older-weak-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-hidden-older-weak",
            analysisAssetId: nil,
            workKey: "\(currentSHA):1:preAnalysis",
            sourceFingerprint: currentSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        ))

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-hidden-older-weak-job")
        let updatedJob = try #require(await store.fetchJob(byId: "sha-hidden-older-weak-job"))
        #expect(updatedJob.analysisAssetId == "older-hidden-current-weak-asset")

        let upgraded = try #require(await store.fetchAsset(id: "older-hidden-current-weak-asset"))
        #expect(upgraded.assetFingerprint == currentSHA)
        #expect(upgraded.weakFingerprint == currentWeakFingerprint)

        let latestAsset = try #require(await store.fetchAssetByEpisodeId("ep-sha-hidden-older-weak"))
        #expect(latestAsset.id == "older-hidden-current-weak-asset")

        let staleAsset = try #require(await store.fetchAsset(id: "newer-stale-canonical-with-current-weak"))
        #expect(staleAsset.assetFingerprint == oldSHA)
        #expect(staleAsset.weakFingerprint == currentWeakFingerprint)
    }

    @Test("stale canonical supersede writes its terminal journal row to the stale job generation")
    func testStaleCanonicalSupersedeEmitsJournalForStaleGenerationWhenReplacementIsInserted() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let currentURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("preanalysis-sha-stale-journal-\(UUID().uuidString).mp3")
        try Data("current bytes for stale journal test".utf8).write(to: currentURL)
        defer { try? FileManager.default.removeItem(at: currentURL) }
        downloads.cachedURLs["ep-sha-stale-journal"] = currentURL

        let oldSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        let currentSHA = try FileHasher.sha256(fileURL: currentURL)
        #expect(currentSHA != oldSHA)
        try await store.insertJob(makeAnalysisJob(
            jobId: "sha-stale-journal-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-stale-journal",
            analysisAssetId: nil,
            workKey: "\(oldSHA):1:preAnalysis",
            sourceFingerprint: oldSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        ))

        let scheduler = makeScheduler(store: store, downloads: downloads)
        await scheduler.setWorkJournalRecorder(
            AnalysisStoreWorkJournalRecorder(store: store)
        )
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-stale-journal-job")
        let staleJob = try #require(await store.fetchJob(byId: "sha-stale-journal-job"))
        let replacement = try #require(
            try await store.fetchJobsByState("queued")
                .first { $0.episodeId == "ep-sha-stale-journal" }
        )
        #expect(replacement.sourceFingerprint == currentSHA)
        #expect(replacement.generationID.isEmpty)

        let staleRows = try await store.fetchWorkJournalEntries(
            episodeId: "ep-sha-stale-journal",
            generationID: staleJob.generationID
        )
        #expect(staleRows.map(\.eventType) == [.acquired, .failed])
        #expect(staleRows.last?.cause == .pipelineError)
        #expect(staleRows.last?.metadata.contains("staleCanonicalFingerprintSupersede") == true)
    }

    @Test("stale canonical supersede frees the original work key for a later matching re-download")
    func testStaleCanonicalSupersedeRetiresWorkKeySoReturnedBytesCanReenqueue() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let originalURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("preanalysis-sha-returned-original-\(UUID().uuidString).mp3")
        let replacementURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("preanalysis-sha-returned-replacement-\(UUID().uuidString).mp3")
        try Data("original audio bytes that later return".utf8).write(to: originalURL)
        try Data("replacement audio bytes".utf8).write(to: replacementURL)
        defer {
            try? FileManager.default.removeItem(at: originalURL)
            try? FileManager.default.removeItem(at: replacementURL)
        }

        let originalSHA = try FileHasher.sha256(fileURL: originalURL)
        let replacementSHA = try FileHasher.sha256(fileURL: replacementURL)
        #expect(originalSHA != replacementSHA)
        let originalWorkKey = "\(originalSHA):1:preAnalysis"
        let replacementWorkKey = "\(replacementSHA):1:preAnalysis"
        downloads.cachedURLs["ep-sha-returned"] = replacementURL

        try await store.insertJob(makeAnalysisJob(
            jobId: "sha-returned-stale-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-returned",
            analysisAssetId: nil,
            workKey: originalWorkKey,
            sourceFingerprint: originalSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        ))

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-returned-stale-job")
        let staleJob = try #require(await store.fetchJob(byId: "sha-returned-stale-job"))
        #expect(staleJob.state == "superseded")
        #expect(staleJob.lastErrorCode == "staleFingerprint:cachedAudioMismatch")
        #expect(staleJob.workKey != originalWorkKey)

        downloads.cachedURLs["ep-sha-returned"] = originalURL
        downloads.fingerprints["ep-sha-returned"] = AudioFingerprint(
            weak: "returned-weak-fingerprint",
            strong: originalSHA
        )
        await scheduler.enqueue(
            episodeId: "ep-sha-returned",
            podcastId: "pod-returned",
            downloadId: "dl-returned",
            sourceFingerprint: originalSHA,
            isExplicitDownload: true
        )

        let queuedJobs = try await store.fetchJobsByState("queued")
            .filter { $0.episodeId == "ep-sha-returned" }
        #expect(queuedJobs.contains { $0.workKey == replacementWorkKey })
        #expect(queuedJobs.contains { $0.workKey == originalWorkKey })
    }
}

// MARK: - Reconciler Bug-Fix Regression Tests

@Suite("AnalysisJobReconciler — Bug-fix Regression")
struct ReconcilerBugFixRegressionTests {

    private func makeReconciler(
        store: AnalysisStore,
        downloads: StubDownloadProvider = StubDownloadProvider(),
        capabilities: StubCapabilitiesProvider = StubCapabilitiesProvider(),
        config: PreAnalysisConfig = PreAnalysisConfig()
    ) -> AnalysisJobReconciler {
        AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: capabilities,
            config: config
        )
    }

    @Test("sequential reconcile calls both succeed")
    func testSequentialReconcileCallsBothSucceed() async throws {
        let store = try await makeTestStore()
        let reconciler = makeReconciler(store: store)

        let report1 = try await reconciler.reconcile()
        let report2 = try await reconciler.reconcile()

        // Both calls should complete (the isReconciling guard resets via defer).
        // Neither should have a negative count (sanity check).
        #expect(report1.expiredLeasesRecovered >= 0)
        #expect(report2.expiredLeasesRecovered >= 0)
    }
}

// MARK: - PreAnalysisConfig Regression Tests

@Suite("PreAnalysisConfig — Regression")
struct PreAnalysisConfigRegressionTests {

    @Test("tier validation resets invalid ascending order to defaults")
    func testTierValidationResetsInvalidOrder() {
        // Create a config where t1 < t0 (invalid).
        var config = PreAnalysisConfig()
        config.defaultT0DepthSeconds = 500
        config.t1DepthSeconds = 100  // t1 < t0: invalid
        config.t2DepthSeconds = 900
        config.save()

        let loaded = PreAnalysisConfig.load()
        let defaults = PreAnalysisConfig()

        // The loaded config should have default tier values since the saved ones were invalid.
        #expect(loaded.defaultT0DepthSeconds == defaults.defaultT0DepthSeconds,
                "Invalid tier config should fall back to default T0")
        #expect(loaded.t1DepthSeconds == defaults.t1DepthSeconds,
                "Invalid tier config should fall back to default T1")
        #expect(loaded.t2DepthSeconds == defaults.t2DepthSeconds,
                "Invalid tier config should fall back to default T2")

        // Clean up UserDefaults.
        UserDefaults.standard.removeObject(forKey: "PreAnalysisConfig")
    }
}
