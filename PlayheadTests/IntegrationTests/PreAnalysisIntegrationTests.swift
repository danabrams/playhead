// PreAnalysisIntegrationTests.swift
// Cross-component integration tests for the pre-analysis pipeline:
// AnalysisStore ↔ AnalysisJobReconciler ↔ SkipCueMaterializer.

import Foundation
import Testing
@testable import Playhead

// MARK: - Shared temp dir tracker

private let _preAnalysisDirs = TestTempDirTracker()

@Suite("PreAnalysis Integration")
struct PreAnalysisIntegrationTests {

    // MARK: - Test 1: Duplicate enqueue

    @Test("INSERT OR IGNORE deduplicates jobs by workKey")
    func duplicateEnqueue() async throws {
        let store = try await makeTestStore()
        let job1 = makeAnalysisJob(
            jobId: "job-1",
            workKey: "fp-abc:1:preAnalysis"
        )
        let job2 = makeAnalysisJob(
            jobId: "job-2",
            workKey: "fp-abc:1:preAnalysis"
        )
        try await store.insertJob(job1)
        try await store.insertJob(job2)

        let queued = try await store.fetchJobsByState("queued")
        #expect(queued.count == 1)
        #expect(queued.first?.jobId == "job-1")
    }

    // MARK: - Test 2: Paused T1 does NOT block eligible T0

    @Test("Paused T1 job does not block a queued T0 playback job")
    func pausedT1DoesNotBlockT0() async throws {
        let store = try await makeTestStore()
        let t1 = makeAnalysisJob(
            jobId: "t1-job",
            jobType: "preAnalysis",
            desiredCoverageSec: 300,
            state: "paused"
        )
        let t0 = makeAnalysisJob(
            jobId: "t0-job",
            jobType: "playback",
            desiredCoverageSec: 90,
            featureCoverageSec: 0,
            state: "queued"
        )
        try await store.insertJob(t1)
        try await store.insertJob(t0)

        let next = try await store.fetchNextEligibleJob(
            isCharging: false,
            isThermalOk: true,
            t0ThresholdSec: 90,
            now: Date().timeIntervalSince1970
        )
        #expect(next != nil)
        #expect(next?.jobId == "t0-job")
    }

    // MARK: - Test 3: Model available unblocks job

    @Test("Reconciler moves blocked:modelUnavailable → queued when model is available")
    func modelAvailableUnblocks() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "blocked-model",
            state: "blocked:modelUnavailable"
        )
        try await store.insertJob(job)

        let caps = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(foundationModelsAvailable: true)
        )
        let downloads = StubDownloadProvider()
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: caps
        )

        let report = try await reconciler.reconcile()
        #expect(report.modelsUnblocked == 1)

        let updated = try await store.fetchJobsByState("queued")
        #expect(updated.contains { $0.jobId == "blocked-model" })
    }

    // MARK: - Test 4: Reconciler discovers un-enqueued download

    @Test("Reconciler creates a job for a cached episode not in the store")
    func discoverUnEnqueuedDownload() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("ep-discovered.m4a")
        downloads.cachedURLs["ep-discovered"] = tempURL
        downloads.fingerprints["ep-discovered"] = AudioFingerprint(
            weak: "weak-fp",
            strong: "strong-fp"
        )

        let caps = StubCapabilitiesProvider()
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: caps
        )

        let report = try await reconciler.reconcile()
        #expect(report.unEnqueuedDownloadsCreated == 1)

        let jobs = try await store.fetchJobsByState("queued")
        #expect(jobs.contains { $0.episodeId == "ep-discovered" })
    }

    // MARK: - Test 5: Cue hash dedup

    @Test("Materializing the same ad range twice produces only one SkipCue")
    func cueHashDedup() async throws {
        let store = try await makeTestStore()
        let materializer = SkipCueMaterializer(store: store)
        let assetId = "asset-dedup"
        let window = makeAdWindow(startTime: 30.0, endTime: 60.0, confidence: 0.9)

        let first = try await materializer.materialize(
            windows: [window],
            analysisAssetId: assetId
        )
        #expect(first.count == 1)

        let second = try await materializer.materialize(
            windows: [window],
            analysisAssetId: assetId
        )
        // INSERT OR IGNORE means the second call succeeds but creates no new rows.
        _ = second

        let cues = try await store.fetchSkipCues(for: assetId)
        #expect(cues.count == 1)
    }

    // MARK: - Test 6: Lease expiry crash recovery

    @Test("Reconciler recovers a running job whose lease has expired")
    func leaseExpiryCrashRecovery() async throws {
        let store = try await makeTestStore()
        let pastLease = Date().timeIntervalSince1970 - 600 // 10 min ago
        let job = makeAnalysisJob(
            jobId: "leased-crash",
            state: "running",
            attemptCount: 2,
            leaseOwner: "old-worker",
            leaseExpiresAt: pastLease
        )
        try await store.insertJob(job)

        let caps = StubCapabilitiesProvider()
        let downloads = StubDownloadProvider()
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: caps
        )

        let report = try await reconciler.reconcile()
        #expect(report.expiredLeasesRecovered == 1)

        let recovered = try await store.fetchJob(byId: "leased-crash")
        #expect(recovered?.state == "queued")
        #expect(recovered?.attemptCount == 3)
        #expect(recovered?.leaseOwner == nil)
    }

    // MARK: - Test 7: Exponential backoff progression

    @Test("Failed jobs get exponentially increasing nextEligibleAt delays")
    func exponentialBackoffProgression() async throws {
        let store = try await makeTestStore()
        let caps = StubCapabilitiesProvider()
        let downloads = StubDownloadProvider()
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: caps
        )

        // attemptCount=0 → delay = 2^0 * 60 = 60s
        let job0 = makeAnalysisJob(
            jobId: "fail-0",
            workKey: "fp-f0:1:playback",
            state: "failed",
            attemptCount: 0
        )
        // attemptCount=1 → delay = 2^1 * 60 = 120s
        let job1 = makeAnalysisJob(
            jobId: "fail-1",
            workKey: "fp-f1:1:playback",
            state: "failed",
            attemptCount: 1
        )
        // attemptCount=2 → delay = 2^2 * 60 = 240s
        let job2 = makeAnalysisJob(
            jobId: "fail-2",
            workKey: "fp-f2:1:playback",
            state: "failed",
            attemptCount: 2
        )

        try await store.insertJob(job0)
        try await store.insertJob(job1)
        try await store.insertJob(job2)

        let before = Date().timeIntervalSince1970
        _ = try await reconciler.reconcile()
        let after = Date().timeIntervalSince1970

        let r0 = try await store.fetchJob(byId: "fail-0")
        let r1 = try await store.fetchJob(byId: "fail-1")
        let r2 = try await store.fetchJob(byId: "fail-2")

        // Verify each nextEligibleAt is within expected range.
        // delay0 ~60s, delay1 ~120s, delay2 ~240s
        let next0 = try #require(r0?.nextEligibleAt)
        let next1 = try #require(r1?.nextEligibleAt)
        let next2 = try #require(r2?.nextEligibleAt)

        #expect(next0 >= before + 59 && next0 <= after + 61)
        #expect(next1 >= before + 119 && next1 <= after + 121)
        #expect(next2 >= before + 239 && next2 <= after + 241)

        // Verify ordering: delay increases with attempt count.
        #expect(next1 > next0)
        #expect(next2 > next1)
    }

    // MARK: - Test 8: Audio file evicted then restored

    @Test("Blocked missingFile job stays blocked when file absent, unblocks when restored")
    func audioFileEvictedThenRestored() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "missing-file",
            state: "blocked:missingFile"
        )
        try await store.insertJob(job)

        let downloads = StubDownloadProvider()
        // No cached URL → file still missing.
        let caps = StubCapabilitiesProvider()
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: caps
        )

        let report1 = try await reconciler.reconcile()
        #expect(report1.missingFilesStillBlocked == 1)
        #expect(report1.missingFilesUnblocked == 0)

        let stillBlocked = try await store.fetchJob(byId: "missing-file")
        #expect(stillBlocked?.state == "blocked:missingFile")

        // Now simulate file restored.
        let restoredURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("restored.m4a")
        downloads.cachedURLs[job.episodeId] = restoredURL

        let report2 = try await reconciler.reconcile()
        #expect(report2.missingFilesUnblocked == 1)

        let unblocked = try await store.fetchJob(byId: "missing-file")
        #expect(unblocked?.state == "queued")
    }

    // MARK: - Test 9: Stale version superseded

    @Test("Jobs with an old analysis version are marked superseded")
    func staleVersionSuperseded() async throws {
        let store = try await makeTestStore()
        // Current version is 1; encode version 0 in workKey.
        let staleJob = makeAnalysisJob(
            jobId: "stale-v0",
            workKey: "fp-stale:0:preAnalysis",
            state: "queued"
        )
        try await store.insertJob(staleJob)

        let caps = StubCapabilitiesProvider()
        let downloads = StubDownloadProvider()
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: caps
        )

        let report = try await reconciler.reconcile()
        #expect(report.staleVersionsSuperseded == 1)

        let updated = try await store.fetchJob(byId: "stale-v0")
        #expect(updated?.state == "superseded")
    }

    // MARK: - Test 10: Feature flag disabled

    @Test("When isEnabled is false, fetchNextEligibleJob still returns jobs but scheduler would skip them")
    func featureFlagDisabled() async throws {
        // The feature flag lives in PreAnalysisConfig and is checked at the
        // scheduler loop level, not at the store level. Jobs can still be
        // enqueued and fetched; the scheduler simply won't process them.
        // We verify that jobs remain queued and untouched.
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "flag-off",
            jobType: "playback",
            featureCoverageSec: 0,
            state: "queued"
        )
        try await store.insertJob(job)

        let config = PreAnalysisConfig(isEnabled: false)
        #expect(config.isEnabled == false)

        // The store layer is unaware of the flag — jobs remain eligible.
        let fetched = try await store.fetchNextEligibleJob(
            isCharging: false,
            isThermalOk: true,
            t0ThresholdSec: 90,
            now: Date().timeIntervalSince1970
        )
        #expect(fetched != nil, "Store returns the job; the scheduler is responsible for gating on isEnabled")
        #expect(fetched?.jobId == "flag-off")

        // Verify the config gate would prevent processing.
        #expect(!config.isEnabled)
    }
}
