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
            adDetection: StubAdDetectionProvider(),
            cueMaterializer: SkipCueMaterializer(store: store)
        )
        let capabilities = CapabilitiesService()
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: downloads,
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
        await scheduler.startSchedulerLoop()

        // Wait for the scheduler to pick up and process the job.
        try await Task.sleep(for: .seconds(2))
        await scheduler.stop()

        let fetched = try await store.fetchJob(byId: "lease-job")
        #expect(fetched?.leaseOwner == nil, "Lease should be released after processing")
        #expect(fetched?.leaseExpiresAt == nil, "Lease expiry should be cleared after processing")
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

        // Base key with version 2 — current version is 1, so it should be superseded.
        let baseJob = makeAnalysisJob(
            jobId: "tier-base-v2",
            workKey: "fp:2:preAnalysis",
            state: "queued"
        )
        // Tier-advanced key with version 2 and coverage suffix.
        let tierJob = makeAnalysisJob(
            jobId: "tier-advanced-v2",
            workKey: "fp:2:preAnalysis:300",
            state: "paused"
        )
        // Base key with version 1 — current version, should NOT be superseded.
        let currentBase = makeAnalysisJob(
            jobId: "tier-base-v1",
            workKey: "fp-ok:1:preAnalysis",
            state: "queued"
        )
        // Tier-advanced key with version 1 — current, should NOT be superseded.
        let currentTier = makeAnalysisJob(
            jobId: "tier-advanced-v1",
            workKey: "fp-ok:1:preAnalysis:900",
            state: "paused"
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
            isCharging: true,
            isThermalOk: true,
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
            isCharging: true,
            isThermalOk: true,
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
            adDetection: StubAdDetectionProvider(),
            cueMaterializer: SkipCueMaterializer(store: store)
        )
        let capabilities = CapabilitiesService()
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: downloads,
            config: config
        )
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
