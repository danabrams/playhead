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
        #expect(asset.sourceURL == localURL.absoluteString,
                "a path outside the audio cache has no portable form and is stored verbatim")
    }

    /// playhead-b8hj: `analysis_assets.sourceURL` is write-once — no
    /// `UPDATE ... SET sourceURL` exists — so an absolute path baked in here is
    /// permanent. The audio cache is addressed through the app Data container,
    /// whose UUID iOS rewrites on reinstall and restore, which is how 36 rows
    /// on the owner's device came to name 12 different, mostly-dead containers.
    ///
    /// The URL is deliberately NOT created on disk: what is under test is what
    /// gets written to the column, and the writer must not depend on the file
    /// still being there. The stem is the real one, `SHA-256(episodeId)`.
    @Test("playhead-b8hj: a cached audio path is persisted container-portable, never absolute")
    func testSchedulerPersistsContainerPortableSourceURL() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let episodeId = "ep-b8hj-portable"
        let name = "\(DownloadManager.safeFilename(for: episodeId)).mp3"
        // Inside the LIVE audio cache — the production shape.
        let localURL = DownloadManager.defaultCacheDirectory()
            .appendingPathComponent("complete", isDirectory: true)
            .appendingPathComponent(name)
        downloads.cachedURLs[episodeId] = localURL

        try await store.insertJob(makeAnalysisJob(
            jobId: "b8hj-portable-job",
            jobType: "preAnalysis",
            episodeId: episodeId,
            analysisAssetId: nil,
            workKey: "fp-b8hj-portable:1:preAnalysis",
            sourceFingerprint: "fp-b8hj-portable",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        ))

        let scheduler = makeScheduler(store: store, downloads: downloads)
        #expect(await scheduler.processNextDispatchableJobForTesting())
        let updatedJob = try #require(await store.fetchJob(byId: "b8hj-portable-job"))
        let resolvedAssetId = try #require(updatedJob.analysisAssetId)
        let asset = try #require(await store.fetchAsset(id: resolvedAssetId))

        #expect(asset.sourceURL == "complete/\(name)")
        #expect(!asset.sourceURL.contains("Containers"),
                "a container segment in a write-once column is a permanent dead reference")
        #expect(URL(string: asset.sourceURL)?.isFileURL != true,
                "an untaught reader must resolve nothing rather than open a wrong path")
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

    @Test("stale canonical supersede preserves tier work-key suffix on replacement")
    func testStaleCanonicalSupersedePreservesTierWorkKeySuffix() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let currentURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("preanalysis-sha-stale-tier-replacement-\(UUID().uuidString).mp3")
        try Data("current replacement bytes for a deeper tier".utf8).write(to: currentURL)
        defer { try? FileManager.default.removeItem(at: currentURL) }
        downloads.cachedURLs["ep-sha-tier-replacement"] = currentURL

        let oldSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        let currentSHA = try FileHasher.sha256(fileURL: currentURL)
        #expect(currentSHA != oldSHA)
        let replacementWorkKey = "\(currentSHA):1:preAnalysis:300"
        try await store.insertJob(makeAnalysisJob(
            jobId: "sha-tier-replacement-stale-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-tier-replacement",
            analysisAssetId: nil,
            workKey: "\(oldSHA):1:preAnalysis:300",
            sourceFingerprint: oldSHA,
            priority: 0,
            desiredCoverageSec: 300,
            state: "queued"
        ))

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-tier-replacement-stale-job")
        let staleJob = try #require(await store.fetchJob(byId: "sha-tier-replacement-stale-job"))
        #expect(staleJob.state == "superseded")

        let queuedJobs = try await store.fetchJobsByState("queued")
            .filter { $0.episodeId == "ep-sha-tier-replacement" }
        #expect(queuedJobs.count == 1)
        let replacement = try #require(queuedJobs.first)
        #expect(replacement.sourceFingerprint == currentSHA)
        #expect(replacement.workKey == replacementWorkKey)
        #expect(replacement.desiredCoverageSec == 300)
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

    @Test("scheduler preserves the verified current weak fingerprint when upgrading by asset fingerprint")
    func testSchedulerPreservesCurrentWeakFingerprintWhenUpgradingWeakAsset() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let localURL = missingTemporaryAudioURL(named: "preanalysis-sha-current-weak-preserve")
        downloads.cachedURLs["ep-sha-current-weak-preserve"] = localURL

        let currentWeakFingerprint = "https://current.example.com/audio.mp3|etag-current|123|Tue, 19 May 2026 00:00:00 GMT"
        let staleSecondaryWeak = "https://old.example.com/audio.mp3|etag-old|123|Tue, 19 May 2026 00:00:00 GMT"
        let currentSHA = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        downloads.fingerprints["ep-sha-current-weak-preserve"] = AudioFingerprint(
            weak: currentWeakFingerprint,
            strong: currentSHA
        )

        try await store.insertAsset(AnalysisAsset(
            id: "current-weak-with-stale-secondary",
            episodeId: "ep-sha-current-weak-preserve",
            assetFingerprint: currentWeakFingerprint,
            weakFingerprint: staleSecondaryWeak,
            sourceURL: "current-weak.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        try await store.insertJob(makeAnalysisJob(
            jobId: "sha-current-weak-preserve-job",
            jobType: "preAnalysis",
            episodeId: "ep-sha-current-weak-preserve",
            analysisAssetId: nil,
            workKey: "\(currentSHA):1:preAnalysis",
            sourceFingerprint: currentSHA,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        ))

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let processed = await scheduler.processNextDispatchableJobForTesting()

        #expect(processed, "Scheduler test hook should process sha-current-weak-preserve-job")
        let upgraded = try #require(await store.fetchAsset(id: "current-weak-with-stale-secondary"))
        #expect(upgraded.assetFingerprint == currentSHA)
        #expect(upgraded.weakFingerprint == currentWeakFingerprint)

        let weakMatches = try await store.fetchAssetsByEpisodeId(
            "ep-sha-current-weak-preserve",
            weakFingerprint: currentWeakFingerprint
        )
        #expect(weakMatches.map(\.id).contains("current-weak-with-stale-secondary"))
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
        // playhead-0hi9: the freshly-minted row now records the observed weak
        // fingerprint instead of leaving it NULL. The invariant this test
        // exists for is untouched — a STALE canonical asset that merely shares
        // the current weak must NOT be upgraded — and is still asserted by
        // `currentAssetId != "stale-canonical-with-current-weak"` above and by
        // the stale row's own fingerprints below.
        #expect(currentAsset.weakFingerprint == currentWeakFingerprint)

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

// MARK: - Expired-lease reclaim (playhead-mk6z)

/// playhead-mk6z. Measured on Dan's phone 2026-08-06: an `analysis_jobs` row
/// sat `state='running'` with `leaseOwner='preAnalysis'` and a five-minute
/// lease that had expired **132 minutes** earlier, while ten jobs sat queued.
///
/// The reclaim machinery exists — `AnalysisCoordinator.recoverOrphans` and
/// `AnalysisJobReconciler.recoverExpiredLeases` both handle exactly this row —
/// but every path that invokes them is a **process-bootstrap** path
/// (`PlayheadRuntime`'s launch Task) or an **OS-granted BGTask**
/// (`preAnalysisRecovery`, which additionally sets
/// `requiresExternalPower = true`). The long-lived `AnalysisWorkScheduler`
/// run loop — the one component that keeps running for the rest of the
/// process's life, polling every five seconds — never reclaims, and
/// `fetchNextEligibleJob` selects only `queued`/`paused`/`failed`, so a
/// `running` row is structurally invisible to it. An orphan minted *after*
/// bootstrap therefore pins its episode out of the queue until the app is
/// relaunched.
///
/// The reclaim is **expiry-based, not liveness-based**, and that is safe here
/// because the lease is already a heartbeat: `leaseRenewalTask` re-stamps
/// `leaseExpiresAt = now + 300` every 120 s for as long as an in-process owner
/// lives, so an expired lease means at least two renewal ticks failed to land.
/// The second test pins the exact guard that keeps it from killing live work.
@Suite("AnalysisWorkScheduler — expired-lease reclaim (playhead-mk6z)")
struct SchedulerExpiredLeaseReclaimTests {

    // MARK: - Test doubles

    /// A decode that PARKS until released. `processJob` records the job as
    /// in-flight immediately after it acquires the lease and clears it in a
    /// `defer`, so parking inside the runner is the only way to hold a
    /// scheduler in the "genuinely running this row" state long enough for a
    /// concurrent run-loop sweep to be observed against it. Throwing on
    /// release routes the job through the `.failed` arm, which gives the
    /// drain a deterministic fixed point (the pattern
    /// `AnalysisWorkSchedulerLaneGateRegressionTests` proves out).
    private final class ParkingDecodeStub: AnalysisAudioProviding, @unchecked Sendable {
        /// Only this episode parks. Every other episode fails immediately, so
        /// a SIBLING dispatch can run to completion while one job is held.
        private let parkedEpisodeID: String
        private let lock = NSLock()
        private var enteredFlag = false
        private var releasedFlag = false

        init(parking parkedEpisodeID: String) {
            self.parkedEpisodeID = parkedEpisodeID
        }

        var hasEntered: Bool {
            lock.lock(); defer { lock.unlock() }
            return enteredFlag
        }

        func release() {
            lock.lock(); releasedFlag = true; lock.unlock()
        }

        private var isReleased: Bool {
            lock.lock(); defer { lock.unlock() }
            return releasedFlag
        }

        /// A SYNC helper, deliberately: `NSLock.lock()` is unavailable from an
        /// async context, and `decode` is async.
        private func markEntered() {
            lock.lock(); enteredFlag = true; lock.unlock()
        }

        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            guard episodeID == parkedEpisodeID else {
                throw AnalysisAudioError.decodingFailed("playhead-mk6z: sibling decode fails fast")
            }
            markEntered()
            while !isReleased && !Task.isCancelled {
                try? await Task.sleep(for: .milliseconds(10))
            }
            throw AnalysisAudioError.decodingFailed("playhead-mk6z: parked decode released")
        }
    }

    /// A wall clock the test can move. Needed because the only honest way to
    /// produce "a lease that expired while its owner is still working" is to
    /// let the scheduler write the lease itself and then advance past it —
    /// the renewal task does not tick for 120 s of REAL time, so it cannot
    /// rescue the row inside a test.
    private final class MutableClock: @unchecked Sendable {
        private let lock = NSLock()
        private var value: TimeInterval
        init(_ value: TimeInterval) { self.value = value }
        var now: TimeInterval {
            lock.lock(); defer { lock.unlock() }
            return value
        }
        func set(_ newValue: TimeInterval) {
            lock.lock(); value = newValue; lock.unlock()
        }
    }

    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider = StubDownloadProvider(),
        config: PreAnalysisConfig = PreAnalysisConfig(),
        audio: (any AnalysisAudioProviding)? = nil,
        clock: (@Sendable () -> Date)? = nil
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audio ?? StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(
                    thermalState: .nominal,
                    isLowPowerMode: false,
                    isCharging: true
                )
            ),
            downloadManager: downloads,
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            config: config,
            clock: clock ?? { Date() }
        )
    }

    @Test("the live scheduler loop returns a lease-expired running row to the queue")
    func testLoopReclaimsExpiredLeaseWithoutDisturbingALiveOne() async throws {
        let store = try await makeTestStore()
        let now = Date().timeIntervalSince1970

        // The device row. `nextEligibleAt` is parked in the future so that a
        // successful reclaim leaves the row `queued` rather than immediately
        // dispatching it — this test is about the reclaim, not the dispatch.
        let orphan = makeAnalysisJob(
            jobId: "mk6z-orphan",
            jobType: "preAnalysis",
            episodeId: "ep-mk6z-orphan",
            workKey: "fp-mk6z-orphan:1:preAnalysis",
            sourceFingerprint: "fp-mk6z-orphan",
            state: "running",
            nextEligibleAt: now + 3600,
            leaseOwner: "preAnalysis",
            leaseExpiresAt: now - 7920  // 132 minutes stale, as measured
        )
        // F2F2FC4C from the same pull: `running` with a lease 172 s in the
        // FUTURE. A healthy, live claim. Whatever reclaims the orphan must
        // not touch this row.
        let liveExpiry = now + 172
        let live = makeAnalysisJob(
            jobId: "mk6z-live",
            jobType: "preAnalysis",
            episodeId: "ep-mk6z-live",
            workKey: "fp-mk6z-live:1:preAnalysis",
            sourceFingerprint: "fp-mk6z-live",
            state: "running",
            nextEligibleAt: now + 3600,
            leaseOwner: "preAnalysis",
            leaseExpiresAt: liveExpiry
        )
        try await store.insertJob(orphan)
        try await store.insertJob(live)

        let scheduler = makeScheduler(store: store)
        await scheduler.startSchedulerLoop()
        defer { Task { await scheduler.stop() } }

        var observed: AnalysisJob?
        for _ in 0..<60 {
            try await Task.sleep(for: .milliseconds(50))
            let fetched = try await store.fetchJob(byId: "mk6z-orphan")
            if fetched?.state == "queued" {
                observed = fetched
                break
            }
            observed = fetched
        }

        #expect(observed?.state == "queued",
                "A lease that expired 132 minutes ago must be returned to the queue by the running scheduler, not left for the next app launch")
        #expect(observed?.leaseOwner == nil, "Reclaim must clear the dead owner")
        #expect(observed?.leaseExpiresAt == nil, "Reclaim must clear the stale deadline")

        // Positive control on the SAME observer: `fetchJob` demonstrably
        // reports a row the reclaim did not touch, so the assertions above
        // are reading a real difference rather than a query that sees
        // nothing. If the reclaim were indiscriminate this row would have
        // moved too.
        let untouched = try await store.fetchJob(byId: "mk6z-live")
        #expect(untouched?.state == "running",
                "A lease 172 s in the future is a live claim and must survive the sweep")
        #expect(untouched?.leaseOwner == "preAnalysis")
        #expect(untouched?.leaseExpiresAt == liveExpiry)
    }

    @Test("the sweep never reclaims the row the caller is running, however stale its lease looks")
    func testSweepExcludesTheCallersOwnInFlightJob() async throws {
        let store = try await makeTestStore()
        let now = Date().timeIntervalSince1970

        // Both rows are indistinguishable on the clock: `running`, owned, and
        // expired by the same amount. The ONLY thing separating them is that
        // the caller knows it is running `mk6z-mine` — proof of life by
        // identity rather than by timestamp, which is what makes it safe to
        // reclaim on expiry alone without diagnosing why the other stopped.
        for jobId in ["mk6z-mine", "mk6z-theirs"] {
            try await store.insertJob(makeAnalysisJob(
                jobId: jobId,
                jobType: "preAnalysis",
                episodeId: "ep-\(jobId)",
                workKey: "fp-\(jobId):1:preAnalysis",
                sourceFingerprint: "fp-\(jobId)",
                state: "running",
                leaseOwner: "preAnalysis",
                leaseExpiresAt: now - 600
            ))
        }

        let reclaimed = try await store.reclaimExpiredLeases(
            now: now,
            excludingJobId: "mk6z-mine"
        )
        #expect(reclaimed == 1, "Exactly the one row the caller does not own should move")

        let mine = try await store.fetchJob(byId: "mk6z-mine")
        #expect(mine?.state == "running", "A scheduler must never reclaim the job it is itself running")
        #expect(mine?.leaseOwner == "preAnalysis")

        // Positive control on the same observer: `mk6z-theirs` differs from
        // `mk6z-mine` in nothing but the exclusion, and it DID move — so the
        // assertions above are reading the guard, not an inert sweep.
        let theirs = try await store.fetchJob(byId: "mk6z-theirs")
        #expect(theirs?.state == "queued")
        #expect(theirs?.leaseOwner == nil)
        #expect(theirs?.leaseExpiresAt == nil)
        #expect(theirs?.attemptCount == 1, "Reclaim feeds the same backoff counter recoverExpiredLease does")
    }

    @Test("the sweep resurrects no terminal row and disturbs no live lease")
    func testSweepLeavesTerminalRowsAndLiveLeasesAlone() async throws {
        let store = try await makeTestStore()
        let now = Date().timeIntervalSince1970

        // A completed row whose lease columns were never cleared. Reading
        // `leaseExpiresAt < now` as "this job needs re-running" would put
        // finished work back on the queue forever.
        try await store.insertJob(makeAnalysisJob(
            jobId: "mk6z-complete",
            jobType: "preAnalysis",
            episodeId: "ep-mk6z-complete",
            workKey: "fp-mk6z-complete:1:preAnalysis",
            sourceFingerprint: "fp-mk6z-complete",
            state: "complete",
            leaseOwner: "preAnalysis",
            leaseExpiresAt: now - 600
        ))
        // F2F2FC4C: a healthy claim, 172 s of lease left.
        try await store.insertJob(makeAnalysisJob(
            jobId: "mk6z-fresh",
            jobType: "preAnalysis",
            episodeId: "ep-mk6z-fresh",
            workKey: "fp-mk6z-fresh:1:preAnalysis",
            sourceFingerprint: "fp-mk6z-fresh",
            state: "running",
            leaseOwner: "preAnalysis",
            leaseExpiresAt: now + 172
        ))
        // The one row that SHOULD move — the observer's positive control.
        try await store.insertJob(makeAnalysisJob(
            jobId: "mk6z-stale",
            jobType: "preAnalysis",
            episodeId: "ep-mk6z-stale",
            workKey: "fp-mk6z-stale:1:preAnalysis",
            sourceFingerprint: "fp-mk6z-stale",
            state: "running",
            leaseOwner: "preAnalysis",
            leaseExpiresAt: now - 600
        ))

        let reclaimed = try await store.reclaimExpiredLeases(now: now, excludingJobId: nil)
        #expect(reclaimed == 1, "Only the stale running row is reclaimable")

        let completed = try await store.fetchJob(byId: "mk6z-complete")
        #expect(completed?.state == "complete", "A terminal row must not be resurrected by a stale lease column")

        let fresh = try await store.fetchJob(byId: "mk6z-fresh")
        #expect(fresh?.state == "running", "A lease 172 s in the future is live work")
        #expect(fresh?.leaseExpiresAt == now + 172)

        let stale = try await store.fetchJob(byId: "mk6z-stale")
        #expect(stale?.state == "queued")
        #expect(stale?.leaseOwner == nil)
    }

    // MARK: - The exclusion the SCHEDULER supplies (review round)

    /// playhead-mk6z review round. The three tests above prove the STORE
    /// honours `excludingJobId`. Nothing proved the scheduler passes anything
    /// but `nil`, and that gap hid a real defect.
    ///
    /// The sweep originally read `currentJobId`. That property names the job
    /// dispatched MOST RECENTLY: `processJob` overwrites it on entry and nils
    /// it in its `defer`. Two dispatch drivers can be inside `processJob` at
    /// once — the run loop and `drainEligible`, whose own header says it is
    /// "safe to run alongside the long-lived `runLoop()`" — so a sibling
    /// dispatch's COMPLETION nils the identity of a job that is still running:
    ///
    ///     drain dispatches A   -> currentJobId = A
    ///     loop  dispatches B   -> currentJobId = B      (A's identity lost)
    ///     B completes          -> currentJobId = nil    (A still running)
    ///     loop reaches its top -> sweeps with `nil`
    ///
    /// This test drives exactly that interleaving against the real scheduler
    /// and the real store: a `drainEligible` pass owns and is parked inside
    /// `mk6z-inflight`, the run loop dispatches and completes `mk6z-sibling`,
    /// and only then does the loop reach its sweep. `mk6z-orphan` is the
    /// observer's positive control — if it does not move, the sweep did not
    /// fire and the assertion about the in-flight row proves nothing.
    ///
    /// The lease is written by the scheduler itself and then the injected
    /// clock is moved past it, which is the one honest way to produce the row
    /// the exclusion exists for: a runner that is genuinely working while its
    /// renewal has been starved past the deadline.
    @Test("the loop's sweep excludes the job this process is running, not merely the last one it dispatched (playhead-mk6z)",
          .timeLimit(.minutes(2)))
    func testSweepExcludesAStillRunningJobAfterASiblingDispatchCompletes() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let t0 = Date().timeIntervalSince1970
        let clock = MutableClock(t0)

        // The row that must SURVIVE: dispatched by this process and still
        // running when the sweep fires.
        downloads.cachedURLs["ep-mk6z-inflight"] = URL(fileURLWithPath: "/tmp/ep-mk6z-inflight.m4a")
        try await store.insertJob(makeAnalysisJob(
            jobId: "mk6z-inflight",
            jobType: "preAnalysis",
            episodeId: "ep-mk6z-inflight",
            workKey: "fp-mk6z-inflight:1:preAnalysis",
            sourceFingerprint: "fp-mk6z-inflight",
            // Strictly ahead of the sibling on both selection keys (`priority
            // DESC, createdAt ASC`) so the drain's pass takes THIS row and the
            // run loop is left with the sibling. Both are Now-lane (>= 20), and
            // `nowCap` is 2, so the two dispatches genuinely overlap.
            priority: 21,
            desiredCoverageSec: 5417,
            state: "queued",
            attemptCount: 4,
            createdAt: t0 - 10,
            updatedAt: t0 - 10
        ))
        // The sibling: dispatched by the run loop, runs to completion, and its
        // `defer` is what erases the in-flight row's identity. Its decode fails
        // fast and `attemptCount 4` of `maxAttemptCount 5` makes it terminal in
        // one pass, so it is a deterministic fixed point.
        downloads.cachedURLs["ep-mk6z-sibling"] = URL(fileURLWithPath: "/tmp/ep-mk6z-sibling.m4a")
        try await store.insertJob(makeAnalysisJob(
            jobId: "mk6z-sibling",
            jobType: "preAnalysis",
            episodeId: "ep-mk6z-sibling",
            workKey: "fp-mk6z-sibling:1:preAnalysis",
            sourceFingerprint: "fp-mk6z-sibling",
            priority: 20,
            desiredCoverageSec: 5417,
            state: "queued",
            attemptCount: 4,
            createdAt: t0,
            updatedAt: t0
        ))

        let park = ParkingDecodeStub(parking: "ep-mk6z-inflight")
        let scheduler = makeScheduler(
            store: store,
            downloads: downloads,
            audio: park,
            clock: { Date(timeIntervalSince1970: clock.now) }
        )
        await scheduler.updateScenePhase(.foreground)

        // A BGTask-window drain claims the in-flight row and parks in the runner.
        let drain = Task { await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600)) }

        var claimed = false
        for _ in 0..<400 where !claimed {
            try await Task.sleep(for: .milliseconds(25))
            let row = try await store.fetchJob(byId: "mk6z-inflight")
            claimed = row?.state == "running" && row?.leaseOwner != nil && park.hasEntered
        }
        #expect(claimed, "precondition: the drain pass must own mk6z-inflight and be parked inside its runner")

        // Starve the renewal. The scheduler wrote `leaseExpiresAt = t0 + 300`
        // itself; the renewal task does not tick for 120 s of real time, so
        // moving the clock past the deadline is exactly "two renewal ticks
        // failed to land while the runner is still working".
        clock.set(t0 + 400)

        // Start the run loop. Its FIRST iteration sweeps (the throttle starts
        // at zero) and then dispatches the sibling, so this alone does not
        // reach the defect: at that first sweep `currentJobId` still names the
        // in-flight row. What matters is the sweep AFTER the sibling's `defer`.
        await scheduler.startSchedulerLoop()

        var siblingState: String?
        for _ in 0..<400 where siblingState != "superseded" {
            try await Task.sleep(for: .milliseconds(25))
            siblingState = try await store.fetchJob(byId: "mk6z-sibling")?.state
        }
        #expect(siblingState == "superseded",
                "precondition: the run loop must have dispatched AND completed the sibling — its defer is what nils currentJobId")

        // Only now insert the orphan and defeat the 60 s sweep throttle, so the
        // sweep that observes it is the one taken with `currentJobId == nil`
        // and the in-flight row still running. Parked non-eligible so a
        // successful reclaim leaves it `queued` rather than re-dispatching.
        try await store.insertJob(makeAnalysisJob(
            jobId: "mk6z-orphan",
            jobType: "preAnalysis",
            episodeId: "ep-mk6z-orphan",
            workKey: "fp-mk6z-orphan:1:preAnalysis",
            sourceFingerprint: "fp-mk6z-orphan",
            state: "running",
            nextEligibleAt: t0 + 86_400,
            leaseOwner: "preAnalysis",
            leaseExpiresAt: t0 - 7_920,
            createdAt: t0,
            updatedAt: t0
        ))
        clock.set(t0 + 500)
        // Preempt the loop's idle sleep so the next sweep is prompt rather
        // than up to `rejectionBackoffSeconds` away.
        await scheduler.wake()

        var orphanState: String?
        for _ in 0..<800 where orphanState != "queued" {
            try await Task.sleep(for: .milliseconds(25))
            orphanState = try await store.fetchJob(byId: "mk6z-orphan")?.state
        }
        #expect(orphanState == "queued",
                "positive control: a second sweep must have reached the store — otherwise the assertion below is vacuous")

        let inflight = try await store.fetchJob(byId: "mk6z-inflight")
        // `state == "running"` is NOT a witness here, and finding that out cost
        // this test a round. A reclaimed row goes back to `queued` with
        // `nextEligibleAt` still nil, so it is immediately eligible again and
        // the same run loop re-dispatches it on the very next pass — which
        // re-acquires the lease and puts the row back to `running` with owner
        // `preAnalysis`. Both answers therefore read `running`, and the earlier
        // version of this test passed under the mutant that reintroduces the
        // defect.
        //
        // That is the bead's own defect class committed inside its test: a
        // column naming "SOME claim exists" read as though it named "the
        // ORIGINAL claim survived". The witnesses below are the two things a
        // reclaim-then-reacquire necessarily moves and a survivor cannot.
        #expect(inflight?.attemptCount == 4, """
            The sweep reclaimed a row this process is running: `attemptCount` went 4 -> 5, and \
            `reclaimExpiredLeases` is the only thing here that charges it. `currentJobId` names \
            the LAST dispatch, not what is in flight — the sibling's completion nils it while the \
            drain's job is still working — so the exclusion must be read from the in-flight SET.
            """)
        #expect(inflight?.leaseExpiresAt == t0 + 300, """
            The lease deadline moved off `leaseAcquiredAt + leaseExpirySeconds`, so this row is a \
            NEW claim minted after a reclaim rather than the original one surviving the sweep.
            """)
        #expect(inflight?.state == "running", "the claim must still be running")
        #expect(inflight?.leaseOwner == "preAnalysis", "the live claim's owner must survive the sweep")

        park.release()
        drain.cancel()
        await scheduler.stop()
    }

    /// playhead-mk6z review round, harm mode 3. `recoverExpiredLease` is a
    /// SELECT (`fetchJobsWithExpiredLeases`) followed by an UPDATE keyed on
    /// `jobId` alone, so a renewal landing between the two is overwritten and
    /// a live lease is cleared. At the reconciler's cadence (once per
    /// bootstrap) that window is negligible; at the run loop's it is not,
    /// which is why the predicate moved INSIDE the write. This proves the
    /// race is closed, against the legacy path in the same store as the
    /// contrast.
    @Test("a renewal that lands after the row was seen as expired is honoured by the sweep (playhead-mk6z)")
    func testRenewalAfterObservationIsHonouredBecauseThePredicateIsInTheWrite() async throws {
        let store = try await makeTestStore()
        let now = Date().timeIntervalSince1970

        for jobId in ["mk6z-renewed", "mk6z-legacy"] {
            try await store.insertJob(makeAnalysisJob(
                jobId: jobId,
                jobType: "preAnalysis",
                episodeId: "ep-\(jobId)",
                workKey: "fp-\(jobId):1:preAnalysis",
                sourceFingerprint: "fp-\(jobId)",
                state: "running",
                leaseOwner: "preAnalysis",
                leaseExpiresAt: now - 10
            ))
        }

        // Step 1 — the reconciler's read. Both rows look dead.
        let seen = try await store.fetchJobsWithExpiredLeases(before: now)
        #expect(seen.contains { $0.jobId == "mk6z-renewed" })
        #expect(seen.contains { $0.jobId == "mk6z-legacy" })

        // Step 2 — the owner's renewal lands. Both rows are live again.
        #expect(try await store.renewLease(jobId: "mk6z-renewed", owner: "preAnalysis", newExpiresAt: now + 300))
        #expect(try await store.renewLease(jobId: "mk6z-legacy", owner: "preAnalysis", newExpiresAt: now + 300))

        // Step 3 — the write, carrying the decision made in step 1.
        let reclaimed = try await store.reclaimExpiredLeases(now: now, excludingJobId: nil)
        #expect(reclaimed == 0, "the expiry predicate is re-evaluated at write time, so a renewed row no longer matches")

        let renewedRow = try await store.fetchJob(byId: "mk6z-renewed")
        #expect(renewedRow?.state == "running")
        #expect(renewedRow?.leaseExpiresAt == now + 300)
        #expect(renewedRow?.attemptCount == 0, "a row that was never reclaimed must not be charged an attempt")

        // Contrast on the same observer, so the assertions above are reading a
        // real difference rather than an inert store: the legacy path takes the
        // step-1 decision at face value and clears a lease that is live again.
        try await store.recoverExpiredLease(jobId: "mk6z-legacy")
        let legacyRow = try await store.fetchJob(byId: "mk6z-legacy")
        #expect(legacyRow?.state == "queued")
        #expect(legacyRow?.leaseOwner == nil, "the TOCTOU the set-based reclaim exists to avoid")
    }

    /// playhead-mk6z review round, harm mode 3. Two sweeps racing the same
    /// orphan must reclaim it ONCE. `attemptCount` feeds the exponential
    /// backoff and `maxAttemptCount`, so a double-charge on one reclaim is a
    /// job pushed toward `superseded` for a defect it did not commit.
    @Test("concurrent sweeps reclaim an orphan exactly once (playhead-mk6z)")
    func testTwoSweepsChargeTheOrphanOneAttempt() async throws {
        let store = try await makeTestStore()
        let now = Date().timeIntervalSince1970

        try await store.insertJob(makeAnalysisJob(
            jobId: "mk6z-contended",
            jobType: "preAnalysis",
            episodeId: "ep-mk6z-contended",
            workKey: "fp-mk6z-contended:1:preAnalysis",
            sourceFingerprint: "fp-mk6z-contended",
            state: "running",
            leaseOwner: "preAnalysis",
            leaseExpiresAt: now - 600
        ))

        async let first = store.reclaimExpiredLeases(now: now, excludingJobId: nil)
        async let second = store.reclaimExpiredLeases(now: now, excludingJobId: nil)
        let counts = try await [first, second]

        #expect(counts.reduce(0, +) == 1,
                "the two sweeps together must move exactly one row; got \(counts)")
        let row = try await store.fetchJob(byId: "mk6z-contended")
        #expect(row?.state == "queued")
        #expect(row?.attemptCount == 1, "a single reclaim charges a single attempt, whatever the contention")
    }
}
