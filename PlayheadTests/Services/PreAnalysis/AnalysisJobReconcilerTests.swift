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

    // MARK: - Step 3: Unblock modelUnavailable

    @Test("Does not unblock blocked:modelUnavailable when runtime probe says unusable")
    func testModelUnavailableStaysBlockedWithoutUsabilityProbe() async throws {
        let store = try await makeTestStore()
        try await store.insertJob(makeAnalysisJob(
            jobId: "blocked-model-still",
            state: "blocked:modelUnavailable"
        ))

        let capabilities = StubCapabilitiesProvider(snapshot: CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: false,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: false,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        ))

        let reconciler = makeReconciler(store: store, capabilities: capabilities)
        let report = try await reconciler.reconcile()

        #expect(report.modelsUnblocked == 0)
        let stillBlocked = try await store.fetchJob(byId: "blocked-model-still")
        #expect(stillBlocked?.state == "blocked:modelUnavailable")
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
        #expect(second.staleVersionsReenqueued == 0)
        #expect(second.completedJobsGarbageCollected == 0)
        #expect(second.failedJobsBackedOff == 0)
        #expect(second.unEnqueuedDownloadsCreated == 0)
    }

    // MARK: - playhead-5uvz.8 (Gap-10): same-pass re-enqueue

    @Test("Bumping analysis version produces fresh queued rows in the same reconcile pass")
    func testStaleVersionReenqueuesInSamePass() async throws {
        let store = try await makeTestStore()

        // Simulate the post-bump state: existing queued jobs at v0 for
        // episodes whose downloads are still cached. The bead's
        // motivating scenario is "in-process analysis-version bump"
        // (test harness, hot config). `PreAnalysisConfig.analysisVersion`
        // is a `static let`, so we encode the same end state by
        // inserting jobs with a workKey that's NOT at the current
        // version — semantically identical to "the version was bumped
        // since these jobs were created."
        let cachedFingerprints: [(episode: String, fingerprint: String)] = [
            ("ep-bumped-1", "fp-bumped-1"),
            ("ep-bumped-2", "fp-bumped-2"),
        ]
        let downloads = StubDownloadProvider()
        for entry in cachedFingerprints {
            downloads.cachedURLs[entry.episode] = URL(
                fileURLWithPath: "/tmp/\(entry.episode).mp3"
            )
            downloads.fingerprints[entry.episode] = AudioFingerprint(
                weak: entry.fingerprint, strong: nil
            )
            try await store.insertJob(makeAnalysisJob(
                jobId: "stale-\(entry.episode)",
                jobType: "preAnalysis",
                episodeId: entry.episode,
                workKey: "\(entry.fingerprint):0:preAnalysis", // stale (current=1)
                sourceFingerprint: entry.fingerprint,
                priority: 5,
                desiredCoverageSec: 240,
                state: "queued"
            ))
        }

        let reconciler = makeReconciler(store: store, downloads: downloads)
        let report = try await reconciler.reconcile()

        // Both old jobs got marked superseded.
        #expect(report.staleVersionsSuperseded == 2)
        // AND both got fresh queued replacements in the SAME pass —
        // not on the next launch's reconciler pass.
        #expect(report.staleVersionsReenqueued == 2)
        // The newJobs counter from step 7 stays at zero — every
        // episode here already had an active row by the time step 7
        // ran, so nothing was "discovered."
        #expect(report.unEnqueuedDownloadsCreated == 0)

        for entry in cachedFingerprints {
            let stale = try await store.fetchJob(byId: "stale-\(entry.episode)")
            #expect(stale?.state == "superseded",
                    "Predecessor must be marked superseded")

            // Find the freshly enqueued v1 job for this episode.
            let queuedJobs = try await store.fetchJobsByState("queued")
            let replacements = queuedJobs.filter { $0.episodeId == entry.episode }
            #expect(replacements.count == 1,
                    "Exactly one replacement per superseded episode")
            guard let replacement = replacements.first else { continue }
            #expect(replacement.workKey == "\(entry.fingerprint):1:preAnalysis",
                    "Replacement workKey encodes the current analysis version")
            #expect(replacement.attemptCount == 0,
                    "Replacement starts with a fresh attempt counter")
            #expect(replacement.lastErrorCode == nil)
            #expect(replacement.leaseOwner == nil)
            #expect(replacement.leaseExpiresAt == nil)
            // Correlated fields propagate from the predecessor.
            #expect(replacement.priority == 5)
            #expect(replacement.desiredCoverageSec == 240)
            #expect(replacement.sourceFingerprint == entry.fingerprint)
        }

        // Re-enqueue is idempotent: a second pass finds nothing stale
        // (predecessors are already `superseded`, replacements are at
        // the current version) so produces zero on both counters.
        let second = try await reconciler.reconcile()
        #expect(second.staleVersionsSuperseded == 0)
        #expect(second.staleVersionsReenqueued == 0)
        #expect(second.unEnqueuedDownloadsCreated == 0)
    }

    @Test("Stale-version replacement is skipped when the download is no longer cached")
    func testStaleVersionNoReenqueueWithoutDownload() async throws {
        let store = try await makeTestStore()

        // A stale-version job whose download has been deleted from the
        // cache. We must NOT mint a replacement: step 7's contract is
        // "no download → no enqueue" and we match it.
        try await store.insertJob(makeAnalysisJob(
            jobId: "stale-evicted",
            jobType: "preAnalysis",
            episodeId: "ep-evicted",
            workKey: "fp-evicted:0:preAnalysis",
            sourceFingerprint: "fp-evicted",
            state: "queued"
        ))

        let downloads = StubDownloadProvider()
        // No cachedURLs entry for ep-evicted.

        let reconciler = makeReconciler(store: store, downloads: downloads)
        let report = try await reconciler.reconcile()

        #expect(report.staleVersionsSuperseded == 1)
        #expect(report.staleVersionsReenqueued == 0,
                "No replacement should be minted for an evicted download")

        // Predecessor is still marked superseded — that part is
        // unconditional.
        let predecessor = try await store.fetchJob(byId: "stale-evicted")
        #expect(predecessor?.state == "superseded")

        // No queued v1 job materialized.
        let allEpisodeIds = try await store.fetchAllJobEpisodeIds()
        let queued = try await store.fetchJobsByState("queued")
        #expect(queued.first(where: { $0.episodeId == "ep-evicted" }) == nil)
        #expect(allEpisodeIds.contains("ep-evicted"),
                "The superseded predecessor row still exists for GC's later sweep")
    }
}
