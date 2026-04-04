// AnalysisJobReconcilerTests.swift
// Tests for AnalysisJobReconciler: blocked state recovery, backoff, GC, and discovery.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisJobReconciler")
struct AnalysisJobReconcilerTests {

    // MARK: - Helpers

    private func makeReconciler(
        store: AnalysisStore,
        downloads: StubDownloadProvider = StubDownloadProvider(),
        capabilities: StubCapabilitiesProvider = StubCapabilitiesProvider()
    ) -> AnalysisJobReconciler {
        AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: capabilities
        )
    }

    // MARK: - Step 1: Recover expired leases

    @Test("Recovers expired lease on running job")
    func testRecoverExpiredLease() async throws {
        let store = try await makeTestStore()
        let pastLease = Date().timeIntervalSince1970 - 600 // 10 min ago
        let job = makeAnalysisJob(
            jobId: "expired-1",
            state: "running",
            leaseOwner: "worker-1",
            leaseExpiresAt: pastLease
        )
        try await store.insertJob(job)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.expiredLeasesRecovered == 1)

        let recovered = try await store.fetchJob(byId: "expired-1")
        #expect(recovered?.state == "queued")
        #expect(recovered?.leaseOwner == nil)
        #expect(recovered?.leaseExpiresAt == nil)
        #expect(recovered?.attemptCount == 1)
    }

    // MARK: - Step 2: Unblock missingFile

    @Test("Unblocks missingFile job when file re-cached")
    func testUnblockMissingFileWhenRecached() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "blocked-file-1",
            episodeId: "ep-recached",
            state: "blocked:missingFile"
        )
        try await store.insertJob(job)

        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-recached"] = URL(fileURLWithPath: "/tmp/ep-recached.mp3")

        let reconciler = makeReconciler(store: store, downloads: downloads)
        let report = try await reconciler.reconcile()

        #expect(report.missingFilesUnblocked == 1)
        #expect(report.missingFilesStillBlocked == 0)

        let unblocked = try await store.fetchJob(byId: "blocked-file-1")
        #expect(unblocked?.state == "queued")
    }

    @Test("Missing file stays blocked when still missing")
    func testMissingFileStaysBlockedWhenStillMissing() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "blocked-file-2",
            episodeId: "ep-gone",
            state: "blocked:missingFile"
        )
        try await store.insertJob(job)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.missingFilesUnblocked == 0)
        #expect(report.missingFilesStillBlocked == 1)

        let still = try await store.fetchJob(byId: "blocked-file-2")
        #expect(still?.state == "blocked:missingFile")
    }

    // MARK: - Step 4: Supersede stale versions

    @Test("Supersedes jobs with stale analysis version")
    func testSupersedeStaleVersion() async throws {
        let store = try await makeTestStore()
        // Create a job with version 0 in the workKey (stale).
        let job = makeAnalysisJob(
            jobId: "stale-1",
            workKey: "fp-test:0:playback",
            state: "queued"
        )
        try await store.insertJob(job)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.staleVersionsSuperseded == 1)

        let superseded = try await store.fetchJob(byId: "stale-1")
        #expect(superseded?.state == "superseded")
    }

    // MARK: - Step 5: GC old completed jobs

    @Test("Garbage collects old completed jobs")
    func testGarbageCollectOldCompleted() async throws {
        let store = try await makeTestStore()
        let eightDaysAgo = Date().timeIntervalSince1970 - (8 * 24 * 3600)
        let job = makeAnalysisJob(
            jobId: "old-complete",
            state: "complete",
            updatedAt: eightDaysAgo
        )
        try await store.insertJob(job)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.completedJobsGarbageCollected == 1)

        let gone = try await store.fetchJob(byId: "old-complete")
        #expect(gone == nil)
    }

    // MARK: - Step 6: Exponential backoff

    @Test("Applies exponential backoff to failed jobs without nextEligibleAt")
    func testExponentialBackoff() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "failed-1",
            state: "failed",
            attemptCount: 2,
            nextEligibleAt: nil
        )
        try await store.insertJob(job)

        let beforeReconcile = Date().timeIntervalSince1970
        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.failedJobsBackedOff == 1)

        let updated = try await store.fetchJob(byId: "failed-1")
        #expect(updated?.nextEligibleAt != nil)
        // 2^2 * 60 = 240 seconds
        let expectedDelay = 240.0
        let nextEligible = updated!.nextEligibleAt!
        #expect(nextEligible >= beforeReconcile + expectedDelay - 1)
        #expect(nextEligible <= beforeReconcile + expectedDelay + 5)
    }

    @Test("Backoff capped at 3600 seconds")
    func testBackoffCappedAt3600() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "failed-capped",
            workKey: "fp-cap:1:playback",
            state: "failed",
            attemptCount: 10, // 2^10 * 60 = 61440, capped to 3600
            nextEligibleAt: nil
        )
        try await store.insertJob(job)

        let beforeReconcile = Date().timeIntervalSince1970
        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.failedJobsBackedOff == 1)

        let updated = try await store.fetchJob(byId: "failed-capped")
        let nextEligible = updated!.nextEligibleAt!
        #expect(nextEligible >= beforeReconcile + 3599)
        #expect(nextEligible <= beforeReconcile + 3605)
    }

    // MARK: - Report

    @Test("Reconciliation report contains all fields")
    func testReconciliationReport() async throws {
        let store = try await makeTestStore()
        // Insert one of each category.
        let now = Date().timeIntervalSince1970

        // Expired lease
        try await store.insertJob(makeAnalysisJob(
            jobId: "r-expired",
            workKey: "fp-r1:1:playback",
            state: "running",
            leaseOwner: "w",
            leaseExpiresAt: now - 600
        ))

        // Blocked missingFile (still missing — no download stub)
        try await store.insertJob(makeAnalysisJob(
            jobId: "r-blocked",
            episodeId: "ep-missing",
            workKey: "fp-r2:1:playback",
            state: "blocked:missingFile"
        ))

        // Old complete
        try await store.insertJob(makeAnalysisJob(
            jobId: "r-old",
            workKey: "fp-r3:1:playback",
            state: "complete",
            updatedAt: now - 8 * 24 * 3600
        ))

        // Failed needing backoff
        try await store.insertJob(makeAnalysisJob(
            jobId: "r-failed",
            workKey: "fp-r4:1:playback",
            state: "failed",
            attemptCount: 1,
            nextEligibleAt: nil
        ))

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.expiredLeasesRecovered == 1)
        #expect(report.missingFilesStillBlocked == 1)
        #expect(report.completedJobsGarbageCollected == 1)
        #expect(report.failedJobsBackedOff == 1)
    }

    // MARK: - Idempotent

    @Test("Second reconcile pass produces all zeros")
    func testIdempotent() async throws {
        let store = try await makeTestStore()
        let now = Date().timeIntervalSince1970

        // Expired lease
        try await store.insertJob(makeAnalysisJob(
            jobId: "idem-expired",
            workKey: "fp-i1:1:playback",
            state: "running",
            leaseOwner: "w",
            leaseExpiresAt: now - 600
        ))

        // Old complete
        try await store.insertJob(makeAnalysisJob(
            jobId: "idem-old",
            workKey: "fp-i2:1:playback",
            state: "complete",
            updatedAt: now - 8 * 24 * 3600
        ))

        let reconciler = makeReconciler(store: store)

        // First pass should do work.
        let first = try await reconciler.reconcile()
        #expect(first.expiredLeasesRecovered == 1)
        #expect(first.completedJobsGarbageCollected == 1)

        // Second pass should be all zeros.
        let second = try await reconciler.reconcile()
        #expect(second.expiredLeasesRecovered == 0)
        #expect(second.missingFilesUnblocked == 0)
        #expect(second.missingFilesStillBlocked == 0)
        #expect(second.modelsUnblocked == 0)
        #expect(second.staleVersionsSuperseded == 0)
        #expect(second.completedJobsGarbageCollected == 0)
        #expect(second.failedJobsBackedOff == 0)
        #expect(second.unEnqueuedDownloadsCreated == 0)
    }
}
