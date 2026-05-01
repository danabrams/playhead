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
        #expect(second.recoveredStrandedSessionJobs == 0)
        #expect(second.missingFilesUnblocked == 0)
        #expect(second.missingFilesStillBlocked == 0)
        #expect(second.modelsUnblocked == 0)
        #expect(second.staleVersionsSuperseded == 0)
        #expect(second.staleVersionsReenqueued == 0)
        #expect(second.completedJobsGarbageCollected == 0)
        #expect(second.failedJobsBackedOff == 0)
        #expect(second.unEnqueuedDownloadsCreated == 0)
        // stranded-backfill-reaper: with no `backfill_jobs` rows seeded the
        // reaper has nothing to flip, so the count is 0 on every pass.
        #expect(second.strandedBackfillJobsReset == 0)
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

    // MARK: - playhead-btwk: recover stranded prior-session jobs
    //
    // Background: when a fresh build replaces an older one, jobs that were
    // mid-flight in the prior process stay in `running` / `paused` state with
    // either no lease or an unexpired lease that the prior process can never
    // release. `recoverExpiredLeases` only catches expired-lease rows;
    // `fetchNextEligibleJob` only picks `queued`/`paused`/`failed` for
    // dispatch; `discoverUnEnqueuedDownloads` skips episodes that already have
    // a non-terminal row. The combination means stranded `running` jobs are
    // simultaneously "already covered" and "not pickable" — invisible.
    //
    // The new `recoverStrandedSessionJobs` step finds jobs that are in an
    // active state (`running`, `paused`, `backfill`) but whose
    // `schedulerEpoch` predates the current session, and flips them back to
    // `queued` so the scheduler can pick them up. Coverage progress is
    // preserved (we resume, we don't restart from zero).

    @Test("recoverStrandedSessionJobs_resetsBackfillJobFromPriorSession_toQueued_preservingCoverage")
    func testRecoverStrandedBackfillJobPreservesCoverage() async throws {
        let store = try await makeTestStore()
        let currentEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let priorEpoch = max(0, currentEpoch - 1)

        let job = makeAnalysisJob(
            jobId: "stranded-backfill",
            episodeId: "ep-stranded-backfill",
            featureCoverageSec: 120.5,
            transcriptCoverageSec: 89.0,
            cueCoverageSec: 30.0,
            state: "backfill",
            attemptCount: 2,
            schedulerEpoch: priorEpoch
        )
        try await store.insertJob(job)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.recoveredStrandedSessionJobs == 1)

        let recovered = try await store.fetchJob(byId: "stranded-backfill")
        #expect(recovered?.state == "queued",
                "Stranded backfill row must be flipped to queued so the scheduler can pick it up")
        #expect(recovered?.featureCoverageSec == 120.5,
                "Feature coverage must survive the recovery so we resume, not restart")
        #expect(recovered?.transcriptCoverageSec == 89.0,
                "Transcript coverage must survive the recovery")
        #expect(recovered?.cueCoverageSec == 30.0,
                "Cue coverage must survive the recovery")
        #expect(recovered?.leaseOwner == nil,
                "Any residual lease must be cleared so a new worker can claim the row")
        #expect(recovered?.leaseExpiresAt == nil)
    }

    @Test("recoverStrandedSessionJobs_resetsRunningJobFromPriorSession_toQueued")
    func testRecoverStrandedRunningJob() async throws {
        let store = try await makeTestStore()
        let currentEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let priorEpoch = max(0, currentEpoch - 1)

        // No live lease — represents a row whose worker died with the prior
        // process before releasing. The reconciler's lease-expiry sweep
        // would not see this because there is no `leaseOwner` set.
        let job = makeAnalysisJob(
            jobId: "stranded-running",
            episodeId: "ep-stranded-running",
            state: "running",
            schedulerEpoch: priorEpoch
        )
        try await store.insertJob(job)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.recoveredStrandedSessionJobs == 1)

        let recovered = try await store.fetchJob(byId: "stranded-running")
        #expect(recovered?.state == "queued")
    }

    @Test("recoverStrandedSessionJobs_resetsPausedJobFromPriorSession_toQueued")
    func testRecoverStrandedPausedJob() async throws {
        let store = try await makeTestStore()
        let currentEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let priorEpoch = max(0, currentEpoch - 1)

        let job = makeAnalysisJob(
            jobId: "stranded-paused",
            episodeId: "ep-stranded-paused",
            state: "paused",
            schedulerEpoch: priorEpoch
        )
        try await store.insertJob(job)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.recoveredStrandedSessionJobs == 1)

        let recovered = try await store.fetchJob(byId: "stranded-paused")
        #expect(recovered?.state == "queued")
    }

    @Test("recoverStrandedSessionJobs_preservesNextEligibleAtAndLastErrorCode")
    func testRecoverStrandedJobPreservesBackoffAndError() async throws {
        // Review-followup (csp / H2): pin the contract that the
        // stranded-recovery sweep preserves both `nextEligibleAt` and
        // `lastErrorCode` on the recovered row. The earlier
        // implementation cleared both; the H2 fix flips that.
        //
        // What this test pins: single-row column preservation. Insert
        // a stranded `running` row that carries a future
        // `nextEligibleAt` and a non-nil `lastErrorCode`, run the
        // reconcile sweep, and assert the recovered row still carries
        // both values verbatim alongside the expected state flip.
        //
        // On cross-coupling with the H1 cancel-mid-decode pacing fix:
        // the typical `running` row in production has
        // `nextEligibleAt = NULL` (a row that successfully acquired a
        // lease was already past its eligibility window), so the
        // H1/H2 interaction is defensive — the cancel-mid-decode
        // requeue path could in principle stamp a future
        // `nextEligibleAt` onto a row that then strands, and we don't
        // want that backoff window erased. The cross-coupling shapes
        // the "why preserve" rationale but is NOT the load-bearing
        // reason for this test; the contract this test enforces is
        // the simpler one — preservation of the two columns on
        // recovery, full stop.
        let store = try await makeTestStore()
        let currentEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let priorEpoch = max(0, currentEpoch - 1)

        let now = Date().timeIntervalSince1970
        let futureEligible = now + 600 // 10 minutes of earned backoff
        let job = makeAnalysisJob(
            jobId: "stranded-with-backoff",
            episodeId: "ep-stranded-with-backoff",
            state: "running",
            attemptCount: 3,
            nextEligibleAt: futureEligible,
            lastErrorCode: "decodingFailed:OperationInterrupted",
            schedulerEpoch: priorEpoch
        )
        try await store.insertJob(job)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.recoveredStrandedSessionJobs == 1)

        let recovered = try await store.fetchJob(byId: "stranded-with-backoff")
        #expect(recovered?.state == "queued",
                "Stranded row must still flip to queued for dispatcher visibility")
        #expect(recovered?.leaseOwner == nil,
                "Residual lease must still be cleared")
        #expect(recovered?.leaseExpiresAt == nil)
        // Core invariant the followup adds: backoff + last-error-code
        // survive the cold-launch recovery.
        #expect(recovered?.nextEligibleAt == futureEligible,
                "nextEligibleAt must survive — it represents earned exponential backoff")
        #expect(recovered?.lastErrorCode == "decodingFailed:OperationInterrupted",
                "lastErrorCode must survive — it's the most informative diagnostic on a stranded row")
        #expect(recovered?.attemptCount == 3,
                "attemptCount preservation is a sibling invariant; sanity-check it didn't regress")
    }

    @Test("recoverStrandedSessionJobs_doesNotTouchRunningJobFromCurrentSession")
    func testDoesNotTouchRunningJobFromCurrentSession() async throws {
        let store = try await makeTestStore()
        // Bump epoch so we have a deterministic "current session" value
        // greater than zero (the default for `makeAnalysisJob`).
        let currentEpoch = try await store.incrementSchedulerEpoch()

        let now = Date().timeIntervalSince1970
        // A live worker in the current process would have:
        //   * `schedulerEpoch == currentEpoch` (acquired via the live lease
        //     code-path which stamps the current epoch),
        //   * a non-NULL `leaseOwner` and an unexpired `leaseExpiresAt`.
        // The new sweep MUST NOT yank such rows: they are legitimate
        // in-flight work.
        let job = makeAnalysisJob(
            jobId: "live-running",
            episodeId: "ep-live-running",
            state: "running",
            leaseOwner: "live-worker",
            leaseExpiresAt: now + 600,
            schedulerEpoch: currentEpoch
        )
        try await store.insertJob(job)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.recoveredStrandedSessionJobs == 0,
                "A running row whose schedulerEpoch matches the current session must not be touched")

        let preserved = try await store.fetchJob(byId: "live-running")
        #expect(preserved?.state == "running")
        #expect(preserved?.leaseOwner == "live-worker")
        #expect(preserved?.leaseExpiresAt != nil)
    }

    @Test("recoverStrandedSessionJobs_doesNotTouchCompleteJob")
    func testDoesNotTouchCompleteJob() async throws {
        let store = try await makeTestStore()
        let currentEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let priorEpoch = max(0, currentEpoch - 1)

        // Even with a stale schedulerEpoch, a `complete` row is terminal
        // and must remain so. The sweep is purely additive against
        // active-state rows.
        let job = makeAnalysisJob(
            jobId: "terminal-complete",
            episodeId: "ep-complete",
            state: "complete",
            schedulerEpoch: priorEpoch
        )
        try await store.insertJob(job)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.recoveredStrandedSessionJobs == 0)
        let preserved = try await store.fetchJob(byId: "terminal-complete")
        #expect(preserved?.state == "complete",
                "A complete row from any prior session must remain complete")
    }

    @Test("recoverStrandedSessionJobs_doesNotTouchSupersededJob")
    func testDoesNotTouchSupersededJob() async throws {
        let store = try await makeTestStore()
        let currentEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let priorEpoch = max(0, currentEpoch - 1)

        let job = makeAnalysisJob(
            jobId: "terminal-superseded",
            episodeId: "ep-superseded",
            state: "superseded",
            schedulerEpoch: priorEpoch
        )
        try await store.insertJob(job)

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.recoveredStrandedSessionJobs == 0)
        let preserved = try await store.fetchJob(byId: "terminal-superseded")
        #expect(preserved?.state == "superseded",
                "A superseded row must remain superseded — replacement minted by step 4")
    }

    @Test("reconcile_runsRecoverStrandedSessionJobs_afterRecoverExpiredLeases_beforeUnblockMissingFiles")
    func testStepOrdering() async throws {
        let store = try await makeTestStore()
        let currentEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let priorEpoch = max(0, currentEpoch - 1)
        let now = Date().timeIntervalSince1970

        // Job A: classic expired-lease row. Caught by step 1
        // (`recoverExpiredLeases`). After step 1 it is `queued` with
        // `attemptCount == 1`. The new sweep, running second, must NOT
        // see it as stranded because step 1 already requeued it.
        try await store.insertJob(makeAnalysisJob(
            jobId: "expired-lease",
            episodeId: "ep-expired-lease",
            workKey: "fp-A:1:playback",
            state: "running",
            attemptCount: 0,
            leaseOwner: "dead-worker",
            leaseExpiresAt: now - 600,
            schedulerEpoch: priorEpoch
        ))

        // Job B: stranded prior-session row (no lease). Caught only by the
        // new sweep.
        try await store.insertJob(makeAnalysisJob(
            jobId: "stranded-no-lease",
            episodeId: "ep-stranded-no-lease",
            workKey: "fp-B:1:playback",
            state: "running",
            schedulerEpoch: priorEpoch
        ))

        // Job C: blocked-on-missing-file row whose download just landed.
        // Caught by step 3 (`unblockMissingFiles`). The new sweep must
        // NOT preempt step 3 — the new sweep only touches active states,
        // never `blocked:missingFile`.
        try await store.insertJob(makeAnalysisJob(
            jobId: "blocked-missing",
            episodeId: "ep-blocked-missing",
            workKey: "fp-C:1:playback",
            state: "blocked:missingFile",
            schedulerEpoch: priorEpoch
        ))
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-blocked-missing"] = URL(
            fileURLWithPath: "/tmp/ep-blocked-missing.mp3"
        )

        let reconciler = makeReconciler(store: store, downloads: downloads)
        let report = try await reconciler.reconcile()

        // Step 1 caught Job A.
        #expect(report.expiredLeasesRecovered == 1,
                "recoverExpiredLeases must run BEFORE recoverStrandedSessionJobs and claim the expired-lease row")
        // The new sweep saw Job B (Job A was already queued by step 1).
        #expect(report.recoveredStrandedSessionJobs == 1,
                "recoverStrandedSessionJobs must run AFTER recoverExpiredLeases — and only Job B remains stranded")
        // Step 2 saw Job C (the new sweep does not touch blocked rows).
        #expect(report.missingFilesUnblocked == 1,
                "unblockMissingFiles must run AFTER recoverStrandedSessionJobs and pick up the recached row")

        // Verify the per-row outcomes line up with the ordering claim.
        let a = try await store.fetchJob(byId: "expired-lease")
        #expect(a?.state == "queued")
        #expect(a?.attemptCount == 1,
                "recoverExpiredLeases increments attemptCount; the new sweep does not — so the count proves which step ran")
        #expect(a?.leaseOwner == nil)

        let b = try await store.fetchJob(byId: "stranded-no-lease")
        #expect(b?.state == "queued")

        let c = try await store.fetchJob(byId: "blocked-missing")
        #expect(c?.state == "queued")
    }

    // MARK: - stranded-backfill-reaper

    @Test("Reconciler resets stranded backfill_jobs row from running to queued, and the M-5 path can re-drive it without throwing")
    func testStrandedBackfillJobReset() async throws {
        let store = try await makeTestStore()
        // backfill_jobs has FK CASCADE on analysis_assets.id, so seed the
        // parent asset first.
        try await store.insertAsset(makeTestAsset(id: "asset-stranded-bf"))

        // Seed a `running` backfill row that survived a prior process.
        let strandedJobId = "fm-stranded-running"
        let strandedJob = makeBackfillJob(
            jobId: strandedJobId,
            analysisAssetId: "asset-stranded-bf",
            phase: .scanLikelyAdSlots,
            coveragePolicy: .targetedWithAudit,
            progressCursor: BackfillProgressCursor(
                processedPhaseCount: 1,
                lastProcessedUpperBoundSec: 30
            ),
            retryCount: 1,
            deferReason: "prior-defer-reason",
            // Insert as queued, then force to running. `insertBackfillJob`
            // accepts any status, but using the explicit force helper
            // keeps intent obvious — we are simulating what the prior
            // process left behind, NOT what the runner would write today.
            status: .queued
        )
        try await store.insertBackfillJob(strandedJob)
        try await store.forceBackfillJobStateForTesting(
            jobId: strandedJobId,
            status: .running,
            progressCursor: BackfillProgressCursor(
                processedPhaseCount: 1,
                lastProcessedUpperBoundSec: 30
            ),
            retryCount: 1,
            deferReason: "prior-defer-reason"
        )

        // Also seed a `complete` row to prove the reaper is not overzealous.
        let completeJobId = "fm-already-complete"
        try await store.insertBackfillJob(makeBackfillJob(
            jobId: completeJobId,
            analysisAssetId: "asset-stranded-bf",
            phase: .fullEpisodeScan,
            coveragePolicy: .fullCoverage,
            status: .queued
        ))
        try await store.forceBackfillJobStateForTesting(
            jobId: completeJobId,
            status: .complete,
            progressCursor: BackfillProgressCursor(processedPhaseCount: 1)
        )

        // Run the reconciler.
        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        // The stranded `running` row was reset.
        #expect(report.strandedBackfillJobsReset == 1,
                "reconcileStrandedBackfillJobs must report exactly the running row")
        let resurrected = try await store.fetchBackfillJob(byId: strandedJobId)
        #expect(resurrected?.status == .queued,
                "stranded `.running` row must be flipped back to `.queued`")
        // Audit trail (progressCursor / retryCount / deferReason) is
        // preserved — the reaper only touches `status`.
        #expect(resurrected?.progressCursor?.processedPhaseCount == 1)
        #expect(resurrected?.progressCursor?.lastProcessedUpperBoundSec == 30)
        #expect(resurrected?.retryCount == 1)
        #expect(resurrected?.deferReason == "prior-defer-reason")

        // The `.complete` row must NOT be touched.
        let stillComplete = try await store.fetchBackfillJob(byId: completeJobId)
        #expect(stillComplete?.status == .complete,
                "reaper must only flip running rows; complete rows are terminal")

        // Re-driving the now-queued row through the M-5 path must NOT
        // throw. M-5 (`BackfillJobRunner.swift:386-403`) re-enqueues the
        // existing row through admission and the runner then calls
        // `markBackfillJobRunning` as the first DB write of the drain
        // loop. We exercise that exact write here so the test proves
        // end-to-end recovery: status reset → admission → running.
        try await store.markBackfillJobRunning(jobId: strandedJobId)
        let nowRunning = try await store.fetchBackfillJob(byId: strandedJobId)
        #expect(nowRunning?.status == .running,
                "M-5 path re-drives the row by calling markBackfillJobRunning, which must succeed against a queued row")
        // markBackfillJobRunning is documented to preserve
        // progressCursor / retryCount / deferReason. Re-assert here so a
        // future change that clobbers them on running will fail this test.
        #expect(nowRunning?.progressCursor?.processedPhaseCount == 1)
        #expect(nowRunning?.retryCount == 1)
        #expect(nowRunning?.deferReason == "prior-defer-reason")

        // Idempotency: a second reconcile pass after the row has legitimately
        // been re-flipped to running by the live runner would reset it again.
        // That is the correct shape for the launch-time reaper (each new
        // process is a fresh strand candidate); but a second pass within
        // the SAME process invocation must NOT reset rows that were
        // legitimately just re-driven within this process. Simulate by
        // running reconcile() a second time AFTER the markBackfillJobRunning
        // above. The reaper WILL flip it again — and that is fine in the
        // production flow because reconcile() runs at most once per launch
        // (PlayheadRuntime) and once per BGProcessingTask handler. We
        // verify the count is 1 (the one running row we just promoted)
        // rather than 0, documenting the launch-only contract.
        let secondReport = try await reconciler.reconcile()
        #expect(secondReport.strandedBackfillJobsReset == 1,
                "reaper is intentionally process-restart-implies-strand; documented launch-only invocation")
        let postSecond = try await store.fetchBackfillJob(byId: strandedJobId)
        #expect(postSecond?.status == .queued)
    }

    @Test("Reaper only flips `running`; `queued`/`deferred`/`failed`/`complete` rows are untouched")
    func testStrandedBackfillReaperOnlyTouchesRunningRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-reaper-discriminator"))

        // Seed one row per non-running status so a future change that
        // widens the WHERE clause has a regression rail. status='running'
        // is exercised by `testStrandedBackfillJobReset`; here we
        // explicitly pin the negative space.
        let cases: [(jobId: String, status: BackfillJobStatus)] = [
            ("fm-stays-queued", .queued),
            ("fm-stays-deferred", .deferred),
            ("fm-stays-failed", .failed),
            ("fm-stays-complete", .complete),
        ]
        for (offset, c) in cases.enumerated() {
            try await store.insertBackfillJob(makeBackfillJob(
                jobId: c.jobId,
                analysisAssetId: "asset-reaper-discriminator",
                phase: .fullEpisodeScan,
                coveragePolicy: .fullCoverage,
                status: .queued,
                createdAt: Date().timeIntervalSince1970 + Double(offset) * 0.0001
            ))
            // `.queued` is the insert default; for the others, force.
            if c.status != .queued {
                try await store.forceBackfillJobStateForTesting(
                    jobId: c.jobId,
                    status: c.status,
                    progressCursor: BackfillProgressCursor(processedPhaseCount: 0)
                )
            }
        }

        let reconciler = makeReconciler(store: store)
        let report = try await reconciler.reconcile()

        #expect(report.strandedBackfillJobsReset == 0,
                "reaper must not touch any non-running row")
        for c in cases {
            let row = try await store.fetchBackfillJob(byId: c.jobId)
            #expect(row?.status == c.status,
                    "row \(c.jobId) must remain in status .\(c.status); reaper SQL must filter exactly on status='running'")
        }
    }
}
