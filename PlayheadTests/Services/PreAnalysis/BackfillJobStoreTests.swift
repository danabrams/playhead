// BackfillJobStoreTests.swift
// Tests for the dedicated backfill_jobs persistence and resume semantics.

import Foundation
import Testing

@testable import Playhead

private func insertParentAsset(
    _ store: AnalysisStore,
    id: String = "asset-1"
) async throws {
    try await store.insertAsset(
        AnalysisAsset(
            id: id,
            episodeId: "episode-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    )
}

@Suite("BackfillJob Store")
struct BackfillJobStoreTests {

    @Test("BackfillJob round-trips through SQLite with cohort JSON")
    func testBackfillJobRoundTrip() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let scanCohort = ScanCohort(
            promptLabel: "phase3",
            promptHash: "prompt-hash",
            schemaHash: "schema-hash",
            scanPlanHash: "plan-hash",
            normalizationHash: "norm-hash",
            osBuild: "26.0",
            locale: "en_US",
            appBuild: "123"
        )
        let encodedScanCohort = String(
            decoding: try JSONEncoder().encode(scanCohort),
            as: UTF8.self
        )
        let job = makeBackfillJob(
            jobId: "backfill-roundtrip",
            phase: .scanHarvesterProposals,
            coveragePolicy: .targetedWithAudit,
            priority: 10,
            status: .running,
            scanCohortJSON: encodedScanCohort
        )

        try await store.insertBackfillJob(job)
        let fetched = try await store.fetchBackfillJob(byId: job.jobId)

        #expect(fetched == job)
    }

    @Test("checkpoint survives interruption and resumes from stored cursor")
    func testCheckpointAndResume() async throws {
        let dir = try makeTempDir(prefix: "BackfillResume")
        let store = try await AnalysisStore.open(directory: dir)
        try await insertParentAsset(store)
        let job = makeBackfillJob(
            jobId: "resume-job",
            phase: .scanLikelyAdSlots,
            coveragePolicy: .targetedWithAudit
        )

        try await store.insertBackfillJob(job)
        try await store.forceBackfillJobStateForTesting(
            jobId: job.jobId,
            status: .running,
            progressCursor: BackfillProgressCursor(
                processedPhaseCount: 2,
                lastProcessedUpperBoundSec: 90
            )
        )

        let reopened = try await AnalysisStore.open(directory: dir)
        let resumed = try #require(await reopened.fetchBackfillJob(byId: job.jobId))

        #expect(resumed.phase == .scanLikelyAdSlots)
        #expect(resumed.progressCursor == BackfillProgressCursor(
            processedPhaseCount: 2,
            lastProcessedUpperBoundSec: 90
        ))
        #expect(Array(resumed.remainingUnitRange(totalUnits: 5)) == [2, 3, 4])
    }

    @Test("phase transition is atomic and clears progress cursor")
    func testPhaseTransitionClearsCursorAtomically() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(
            jobId: "phase-advance",
            phase: .scanHarvesterProposals,
            coveragePolicy: .targetedWithAudit,
            progressCursor: BackfillProgressCursor(processedPhaseCount: 3),
            status: .running
        )

        try await store.insertBackfillJob(job)
        let advanced = try await store.advanceBackfillJobPhase(
            jobId: job.jobId,
            expecting: .scanHarvesterProposals,
            to: .scanRandomAuditWindows,
            status: .queued
        )
        let fetched = try await store.fetchBackfillJob(byId: job.jobId)
        let staleAdvance = try await store.advanceBackfillJobPhase(
            jobId: job.jobId,
            expecting: .scanHarvesterProposals,
            to: .fullEpisodeScan,
            status: .queued
        )

        #expect(advanced == true)
        #expect(fetched?.phase == .scanRandomAuditWindows)
        #expect(fetched?.progressCursor == nil)
        #expect(fetched?.status == .queued)
        #expect(staleAdvance == false)
    }

    // MARK: - New behaviour from review fixes

    @Test("H7: re-inserting an existing job throws duplicateJobId")
    func testInsertBackfillJobDuplicateThrows() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "dup-job")

        try await store.insertBackfillJob(job)
        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertBackfillJob(job)
        }
    }

    @Test("H5: progress checkpoint preserves existing deferReason")
    func testProgressCheckpointPreservesDeferReason() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(
            jobId: "preserve-defer",
            phase: .scanLikelyAdSlots,
            coveragePolicy: .targetedWithAudit,
            deferReason: "thermal"
        )

        try await store.insertBackfillJob(job)
        try await store.checkpointBackfillJobProgress(
            jobId: job.jobId,
            progressCursor: BackfillProgressCursor(processedPhaseCount: 4)
        )

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.deferReason == "thermal")
        #expect(fetched.progressCursor?.processedPhaseCount == 4)
    }

    @Test("H5: markBackfillJobDeferred preserves existing progressCursor")
    func testMarkDeferredPreservesProgressCursor() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(
            jobId: "preserve-cursor",
            phase: .scanLikelyAdSlots,
            coveragePolicy: .targetedWithAudit,
            progressCursor: BackfillProgressCursor(
                processedPhaseCount: 7,
                lastProcessedUpperBoundSec: 123.0
            )
        )

        try await store.insertBackfillJob(job)
        try await store.markBackfillJobDeferred(jobId: job.jobId, reason: "battery")

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .deferred)
        #expect(fetched.deferReason == "battery")
        #expect(fetched.progressCursor == BackfillProgressCursor(
            processedPhaseCount: 7,
            lastProcessedUpperBoundSec: 123.0
        ))
    }

    @Test("M4: foreign_keys is ON for every connection on the same path")
    func testPragmasAppliedToAllConnections() async throws {
        let dir = try makeTempDir(prefix: "PragmaCache")
        let first = try await AnalysisStore.open(directory: dir)
        try await insertParentAsset(first, id: "fk-test")

        // Second store on the same path; the previous implementation
        // short-circuited migrate() and skipped configurePragmas().
        let second = try await AnalysisStore.open(directory: dir)

        // FK enforcement is observable: inserting a backfill job for a
        // non-existent asset must fail with a constraint error.
        let orphan = makeBackfillJob(
            jobId: "orphan-\(UUID().uuidString)",
            analysisAssetId: "no-such-asset"
        )
        await #expect(throws: AnalysisStoreError.self) {
            try await second.insertBackfillJob(orphan)
        }

        // Sanity: the legitimate insert through the second connection works.
        let good = makeBackfillJob(
            jobId: "good-\(UUID().uuidString)",
            analysisAssetId: "fk-test"
        )
        try await second.insertBackfillJob(good)
    }

    @Test("M5: migrate is idempotent across multiple opens")
    func testMigrateIsIdempotent() async throws {
        let dir = try makeTempDir(prefix: "MigrateIdempotent")
        let first = try await AnalysisStore.open(directory: dir)
        // Force a second migrate() on a fresh actor to exercise the
        // ALTER TABLE / column-existence path against an already-migrated db.
        let second = try AnalysisStore(directory: dir)
        try await second.migrate()
        try await second.migrate()

        // Both stores should be usable.
        try await insertParentAsset(first, id: "idem-1")
        let job = makeBackfillJob(jobId: "idem-1", analysisAssetId: "idem-1")
        try await second.insertBackfillJob(job)
    }

    @Test("M7: schema_version is recorded on first migration")
    func testSchemaVersionRecorded() async throws {
        let store = try await makeTestStore()
        let version = try await store.schemaVersion()
        // Migration always climbs the full ladder to the current head; assert
        // against the production constant rather than a literal (hardcoding the
        // integer has been a recurring source of stale-assertion flakes on
        // every schema bump — see AnalysisStore.currentSchemaVersion's doc).
        #expect(version == AnalysisStore.currentSchemaVersion)
    }

    @Test("M8: deleting an asset cascades to its backfill_jobs rows")
    func testFKCascadeOnAssetDelete() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store, id: "cascade-asset")
        let job = makeBackfillJob(
            jobId: "cascade-job",
            analysisAssetId: "cascade-asset"
        )
        try await store.insertBackfillJob(job)

        try await store.deleteAsset(id: "cascade-asset")

        let fetched = try await store.fetchBackfillJob(byId: "cascade-job")
        #expect(fetched == nil)
    }

    // MARK: - C-2: split lifecycle methods

    @Test("C-2: markBackfillJobRunning preserves existing deferReason for audit trail")
    func markBackfillJobRunning_preservesDeferReason() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(
            jobId: "running-preserves-defer",
            phase: .scanLikelyAdSlots,
            coveragePolicy: .targetedWithAudit
        )
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobDeferred(jobId: job.jobId, reason: "thermal")

        try await store.markBackfillJobRunning(jobId: job.jobId)

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .running)
        #expect(fetched.deferReason == "thermal")
    }

    @Test("C-2: markBackfillJobRunning preserves progressCursor and retryCount")
    func markBackfillJobRunning_preservesCursorAndRetry() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(
            jobId: "running-preserves-all",
            phase: .scanLikelyAdSlots,
            coveragePolicy: .targetedWithAudit,
            progressCursor: BackfillProgressCursor(
                processedPhaseCount: 3,
                lastProcessedUpperBoundSec: 45
            ),
            retryCount: 2
        )
        try await store.insertBackfillJob(job)

        try await store.markBackfillJobRunning(jobId: job.jobId)

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .running)
        #expect(fetched.progressCursor?.processedPhaseCount == 3)
        #expect(fetched.retryCount == 2)
    }

    @Test("C-2: markBackfillJobComplete writes final cursor and preserves deferReason")
    func markBackfillJobComplete_writesFinalCursor() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(
            jobId: "complete-writes-cursor",
            phase: .scanLikelyAdSlots,
            coveragePolicy: .targetedWithAudit,
            deferReason: "prior-defer"
        )
        try await store.insertBackfillJob(job)
        try await store.checkpointBackfillJobProgress(
            jobId: job.jobId,
            progressCursor: BackfillProgressCursor(processedPhaseCount: 3)
        )

        try await store.markBackfillJobComplete(
            jobId: job.jobId,
            progressCursor: BackfillProgressCursor(
                processedPhaseCount: 5,
                lastProcessedUpperBoundSec: 120
            )
        )

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .complete)
        #expect(fetched.progressCursor == BackfillProgressCursor(
            processedPhaseCount: 5,
            lastProcessedUpperBoundSec: 120
        ))
        // Audit trail preserved.
        #expect(fetched.deferReason == "prior-defer")
    }

    @Test("C-2: markBackfillJobFailed writes reason and retryCount")
    func markBackfillJobFailed_writesReason() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(
            jobId: "failed-writes-reason",
            phase: .scanLikelyAdSlots,
            coveragePolicy: .targetedWithAudit
        )
        try await store.insertBackfillJob(job)

        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "classifier threw",
            retryCount: 2
        )

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .failed)
        #expect(fetched.deferReason == "classifier threw")
        #expect(fetched.retryCount == 2)
    }

    // MARK: - C-1: migratedPaths cache must not skip migration when the file is gone

    @Test("C-1: reopening after the db file is deleted re-runs migration (stale cache bug)")
    func migratedPathsCacheInvalidatedWhenFileDisappears() async throws {
        let dir = try makeTempDir(prefix: "StaleMigrateCache")

        // First open: primes the static migratedPaths cache for this path.
        do {
            let first = try await AnalysisStore.open(directory: dir)
            try await insertParentAsset(first, id: "pre-delete")
            _ = first
        }

        // Nuke the directory so the sqlite file no longer exists, but the
        // static cache still remembers the path. Recreate the directory so
        // the second open() succeeds at the FS layer.
        try FileManager.default.removeItem(at: dir)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)

        // Second open: previously short-circuited on the stale cache entry,
        // returning a store with no tables. The insert below would fail with
        // "no such table: analysis_assets".
        let second = try await AnalysisStore.open(directory: dir)
        try await insertParentAsset(second, id: "post-recreate")

        let fetched = try await second.fetchBackfillJob(byId: "nonexistent")
        #expect(fetched == nil) // schema is present; query returns nothing without throwing
    }

    // MARK: - C-2: terminal rows must not be silently resurrected

    @Test("C-2: markBackfillJobRunning throws on a complete row")
    func markBackfillJobRunning_throwsOnCompleteRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "terminal-complete")
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobComplete(
            jobId: job.jobId,
            progressCursor: BackfillProgressCursor(processedPhaseCount: 1)
        )

        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobRunning(jobId: job.jobId)
        }

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .complete, "row must not be resurrected to running")
    }

    /// playhead-wxsv SPEC 1: a row nothing can START must not BLOCK.
    ///
    /// **This test asserted the opposite until playhead-wxsv, and the
    /// behaviour it pinned was a live production defect.** `BackfillJobRunner`
    /// re-drives a `failed` row whose `retryCount` is under
    /// `AdmissionController.maxRetries` — that is what a retry budget means —
    /// but this transition refused it, so every retry threw
    /// `invalidStateTransition` and no attempt ever ran. Meanwhile
    /// `countResumableBackfillJobs` counts exactly those rows as resumable, so
    /// the asset reads as "work pending" while nothing can start it: permanent
    /// suppression. On the 2026-08-07 pull all 5 `failed` rows sat at
    /// `retryCount = 1` against a budget of 3, across 3 assets, every one of
    /// them a transient FM daemon fault.
    ///
    /// A broken implementation that still passes this: one that accepts a
    /// `failed` row at ANY retryCount. That is why the exhausted half is a
    /// separate test and not an afterthought here.
    @Test("playhead-wxsv: markBackfillJobRunning restarts a failed row under the retry budget")
    func markBackfillJobRunning_restartsFailedRowUnderBudget() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "retryable-failed")
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "FMInferenceTimeoutError(deadline: 30.0 seconds)",
            retryCount: 1
        )

        try await store.markBackfillJobRunning(jobId: job.jobId)

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .running, "a failure under budget is a retry, not a retirement")
        #expect(fetched.retryCount == 1, "the budget spent so far must be preserved")
        #expect(fetched.deferReason == "FMInferenceTimeoutError(deadline: 30.0 seconds)",
                "the audit trail of why the last attempt lost must survive the restart")
    }

    /// The other side of spec 1: the budget is still a budget. A row that has
    /// spent it stays refused, so "a failed row can restart" cannot be read as
    /// "a failing job loops forever".
    @Test("playhead-wxsv: markBackfillJobRunning still refuses a failed row at the retry budget")
    func markBackfillJobRunning_refusesFailedRowAtBudget() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "exhausted-failed")
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "boom",
            retryCount: AdmissionController.maxRetries
        )

        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobRunning(jobId: job.jobId)
        }

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .failed, "a spent budget is still a refusal")
    }

    @Test("C-2: markBackfillJobRunning succeeds on a deferred row")
    func markBackfillJobRunning_succeedsOnDeferredRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "from-deferred")
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobDeferred(jobId: job.jobId, reason: "thermal")

        try await store.markBackfillJobRunning(jobId: job.jobId)

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .running)
        #expect(fetched.deferReason == "thermal")
    }

    @Test("C-2: markBackfillJobRunning throws on a nonexistent row")
    func markBackfillJobRunning_throwsOnMissingRow() async throws {
        let store = try await makeTestStore()
        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobRunning(jobId: "nope")
        }
    }

    // MARK: - HIGH-R6-1: markBackfillJobRunning must be idempotent on
    // `.running` rows so a crash between markRunning and the terminal
    // transition cannot create a zombie that the runner loops on forever.

    @Test("HIGH-R6-1: markBackfillJobRunning is idempotent on an already-running row")
    func markBackfillJobRunning_idempotentOnRunningRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(
            jobId: "running-idempotent",
            phase: .scanLikelyAdSlots,
            coveragePolicy: .targetedWithAudit,
            progressCursor: BackfillProgressCursor(
                processedPhaseCount: 7,
                lastProcessedUpperBoundSec: 210
            ),
            retryCount: 3,
            deferReason: "earlier-defer"
        )
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobRunning(jobId: job.jobId)

        // Second call must not throw and must not clobber audit fields.
        try await store.markBackfillJobRunning(jobId: job.jobId)

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .running)
        #expect(fetched.progressCursor == BackfillProgressCursor(
            processedPhaseCount: 7,
            lastProcessedUpperBoundSec: 210
        ))
        #expect(fetched.retryCount == 3)
        #expect(fetched.deferReason == "earlier-defer")
    }

    @Test("HIGH-R6-1: markBackfillJobRunning recovers from crash-left zombie row")
    func markBackfillJobRunning_recoversFromCrashLeftZombieRow() async throws {
        // Simulate a process crash between markBackfillJobRunning and the
        // terminal transition: the row is already `.running` when the next
        // drain cycle re-enqueues and calls markBackfillJobRunning again.
        // Pre-HIGH-R6-1 this threw invalidStateTransition, the runner's catch
        // arm logged "already in terminal state" without bumping retryCount,
        // and the job looped forever as a zombie.
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(
            jobId: "zombie-running",
            phase: .scanLikelyAdSlots,
            coveragePolicy: .targetedWithAudit,
            progressCursor: BackfillProgressCursor(processedPhaseCount: 2),
            retryCount: 1
        )
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobRunning(jobId: job.jobId)

        // Second run after "crash" — must not throw.
        try await store.markBackfillJobRunning(jobId: job.jobId)

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .running)
        #expect(fetched.progressCursor?.processedPhaseCount == 2)
        #expect(fetched.retryCount == 1)
    }

    // MARK: - C3-2: terminal transitions on markBackfillJobComplete/Failed

    @Test("C3-2: markBackfillJobComplete throws on a failed row")
    func markBackfillJobComplete_throwsOnFailedRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "c32-complete-on-failed")
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "boom",
            retryCount: 1
        )

        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobComplete(
                jobId: job.jobId,
                progressCursor: BackfillProgressCursor(processedPhaseCount: 1)
            )
        }

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .failed, "failed row must not be resurrected to complete")
    }

    @Test("C3-2: markBackfillJobComplete is idempotent on an already-complete row")
    func markBackfillJobComplete_idempotentOnCompleteRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "c32-complete-idempotent")
        try await store.insertBackfillJob(job)
        let cursor = BackfillProgressCursor(processedPhaseCount: 2)
        try await store.markBackfillJobComplete(jobId: job.jobId, progressCursor: cursor)

        // Second call must not throw.
        try await store.markBackfillJobComplete(jobId: job.jobId, progressCursor: cursor)

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .complete)
    }

    @Test("C3-2: markBackfillJobComplete throws on a nonexistent row")
    func markBackfillJobComplete_throwsOnMissingRow() async throws {
        let store = try await makeTestStore()
        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobComplete(
                jobId: "c32-missing",
                progressCursor: nil
            )
        }
    }

    @Test("C3-2: markBackfillJobFailed throws on a complete row")
    func markBackfillJobFailed_throwsOnCompleteRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "c32-failed-on-complete")
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobComplete(
            jobId: job.jobId,
            progressCursor: BackfillProgressCursor(processedPhaseCount: 1)
        )

        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobFailed(
                jobId: job.jobId,
                reason: "too late",
                retryCount: 1
            )
        }

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .complete, "complete row must not be demoted to failed")
    }

    @Test("C3-2: markBackfillJobFailed is idempotent on an already-failed row")
    func markBackfillJobFailed_idempotentOnFailedRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "c32-failed-idempotent")
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "first",
            retryCount: 1
        )

        // Second call on an already-failed row must NOT throw (idempotent)
        // and must NOT bump retryCount or mutate deferReason.
        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "second",
            retryCount: 99
        )

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .failed)
        #expect(fetched.retryCount == 1, "idempotent path must not bump retryCount")
        #expect(fetched.deferReason == "first", "idempotent path must not overwrite deferReason")
    }

    @Test("C3-2: markBackfillJobFailed throws on a nonexistent row")
    func markBackfillJobFailed_throwsOnMissingRow() async throws {
        let store = try await makeTestStore()
        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobFailed(
                jobId: "c32-missing",
                reason: "nope",
                retryCount: 1
            )
        }
    }

    // MARK: - C-R3-1: markBackfillJobDeferred status guard

    @Test("C-R3-1: markBackfillJobDeferred throws on a failed row")
    func markBackfillJobDeferred_throwsOnFailedRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "cr31-deferred-on-failed")
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "boom",
            retryCount: 2
        )

        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobDeferred(jobId: job.jobId, reason: "thermal")
        }

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .failed, "failed row must not be demoted to deferred")
        #expect(fetched.deferReason == "boom", "failure reason must be preserved")
        #expect(fetched.retryCount == 2, "retryCount must not be altered")
    }

    @Test("C-R3-1: markBackfillJobDeferred throws on a complete row")
    func markBackfillJobDeferred_throwsOnCompleteRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "cr31-deferred-on-complete")
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobComplete(
            jobId: job.jobId,
            progressCursor: BackfillProgressCursor(processedPhaseCount: 1)
        )

        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobDeferred(jobId: job.jobId, reason: "thermal")
        }

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .complete, "complete row must not be demoted to deferred")
    }

    @Test("C-R3-1: markBackfillJobDeferred is idempotent on deferred rows and updates the reason")
    func markBackfillJobDeferred_idempotentOnDeferredRow_updatesReason() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "cr31-deferred-idempotent")
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobDeferred(jobId: job.jobId, reason: "thermal")

        // Second call on an already-deferred row must not throw, and should
        // update the reason so operators see the most recent defer cause.
        try await store.markBackfillJobDeferred(jobId: job.jobId, reason: "batteryTooLow")

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .deferred)
        #expect(fetched.deferReason == "batteryTooLow",
                "idempotent defer must refresh the reason to the newer value")
    }

    @Test("C-R3-1: markBackfillJobDeferred throws on a nonexistent row")
    func markBackfillJobDeferred_throwsOnMissingRow() async throws {
        let store = try await makeTestStore()
        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobDeferred(jobId: "cr31-missing", reason: "nope")
        }
    }

    // MARK: - M-4: markBackfillJobFailed overwrites deferReason (documented, pinned)

    @Test("M-4: markBackfillJobFailed overwrites any prior deferReason by design")
    func markBackfillJobFailed_overwritesDeferReason() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(
            jobId: "failed-overwrites-defer",
            phase: .scanLikelyAdSlots,
            coveragePolicy: .targetedWithAudit
        )
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobDeferred(jobId: job.jobId, reason: "thermal")

        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "classifier threw",
            retryCount: 3
        )

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .failed)
        #expect(fetched.deferReason == "classifier threw",
                "failure reason replaces the audit-trail defer reason; this behavior is intentional and must be pinned")
        #expect(fetched.retryCount == 3)
    }

    @Test("WAL durability: insertBackfillJob survives a store reopen")
    func testInsertBackfillJobIsDurable() async throws {
        let dir = try makeTempDir(prefix: "WALDurability")
        let writer = try await AnalysisStore.open(directory: dir)
        try await insertParentAsset(writer, id: "wal-asset")
        let job = makeBackfillJob(
            jobId: "wal-job",
            analysisAssetId: "wal-asset",
            phase: .scanHarvesterProposals,
            coveragePolicy: .targetedWithAudit
        )
        try await writer.insertBackfillJob(job)
        // Drop the writer reference so the actor and its connection are
        // released before reopening.
        _ = writer

        let reader = try await AnalysisStore.open(directory: dir)
        let fetched = try #require(await reader.fetchBackfillJob(byId: "wal-job"))
        #expect(fetched.jobId == "wal-job")
        #expect(fetched.phase == .scanHarvesterProposals)
    }

    // MARK: - Fix #6: invalidStateTransition carries prior status

    /// playhead-wxsv: the row is driven to the budget rather than to
    /// `retryCount = 1`, because a failure under budget is now a restart (see
    /// `markBackfillJobRunning_restartsFailedRowUnderBudget`) and only an
    /// EXHAUSTED failure still refuses. The error payload — which this test is
    /// about — is unchanged.
    @Test("markBackfillJobRunning on a failed row carries fromStatus='failed'")
    func markBackfillJobRunning_throwsOnFailedRow_carriesPriorStatus() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "prior-failed-job", status: .queued)
        try await store.insertBackfillJob(job)

        // Move the row into the `.failed` terminal state, with the budget spent.
        try await store.markBackfillJobRunning(jobId: job.jobId)
        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "boom",
            retryCount: AdmissionController.maxRetries
        )

        // Trying to re-run the failed row must raise invalidStateTransition
        // with the job id, the prior `.failed` status, and the requested
        // target status.
        do {
            try await store.markBackfillJobRunning(jobId: job.jobId)
            Issue.record("expected invalidStateTransition, but no error was thrown")
        } catch let error as AnalysisStoreError {
            guard case .invalidStateTransition(let id, let fromStatus, let toStatus) = error else {
                Issue.record("unexpected error: \(error)")
                return
            }
            #expect(id == job.jobId)
            #expect(fromStatus == "failed")
            #expect(toStatus == "running")
        }
    }

    // MARK: - playhead-wxsv: the transcript version is a column, not an identity

    /// The stamp is a record of an ATTEMPT, so a row that has had none carries
    /// `nil` — not `""`. An empty string is a value that names an absence, and
    /// the reopen guard would then read it as a version that differs from every
    /// real one.
    @Test("playhead-wxsv: an inserted row has no attempt version, and nil round-trips")
    func insertedRowHasNoAttemptVersion() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "fresh-row")
        try await store.insertBackfillJob(job)

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.attemptTranscriptVersion == nil)

        try await store.noteBackfillJobAttempt(jobId: job.jobId, transcriptVersion: "tx-a")
        let stamped = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(stamped.attemptTranscriptVersion == "tx-a")
    }

    /// playhead-wxsv SPEC 2 + THE PRIZE, at the store layer.
    ///
    /// A row that completed at 900 s because that is where the transcript ended
    /// must resume at 900 s when the transcript reaches further — not restart,
    /// and not be abandoned for a new row with a nil cursor, which is what the
    /// pre-wxsv identity did once per transcription session.
    ///
    /// What a broken implementation would still pass if this only checked
    /// `status`: an `INSERT OR REPLACE` re-open. It is the obvious spelling, it
    /// flips the status correctly, and it silently drops both the cursor and
    /// `createdAt`.
    @Test("playhead-wxsv: re-opening a completed row keeps its cursor and its createdAt")
    func reopenKeepsCursorAndCreatedAt() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "grew", createdAt: 1_000)
        try await store.insertBackfillJob(job)
        try await store.noteBackfillJobAttempt(jobId: job.jobId, transcriptVersion: "tx-v1")
        try await store.markBackfillJobRunning(jobId: job.jobId)
        try await store.markBackfillJobComplete(
            jobId: job.jobId,
            progressCursor: BackfillProgressCursor(
                processedPhaseCount: 1,
                lastProcessedUpperBoundSec: EpisodeSeconds(900)
            )
        )

        #expect(try await store.reopenBackfillJob(jobId: job.jobId, forTranscriptVersion: "tx-v2"))

        let reopened = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(reopened.status == .queued)
        #expect(reopened.progressCursor?.lastProcessedUpperBoundSec == EpisodeSeconds(900),
                "the scan must carry on from where it stopped, not start again")
        #expect(reopened.createdAt == 1_000, "createdAt names when the work was first requested")
        #expect(reopened.attemptTranscriptVersion == "tx-v1",
                "the stamp names the version the row's progress was made against — not the one on disk now")
    }

    /// playhead-wxsv SPEC 2: a completed row is not resurrected.
    ///
    /// The only door out of `complete` is a version that demonstrably moved.
    /// `markBackfillJobRunning` refuses `complete` outright, so this guard plus
    /// that one is the whole perimeter.
    @Test("playhead-wxsv: re-open refuses a completed row at the same version")
    func reopenRefusesTheSameVersion() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "already-done")
        try await store.insertBackfillJob(job)
        try await store.noteBackfillJobAttempt(jobId: job.jobId, transcriptVersion: "tx-v1")
        try await store.markBackfillJobRunning(jobId: job.jobId)
        try await store.markBackfillJobComplete(jobId: job.jobId, progressCursor: nil)

        #expect(try await store.reopenBackfillJob(jobId: job.jobId, forTranscriptVersion: "tx-v1") == false)
        let row = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(row.status == .complete)

        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobRunning(jobId: job.jobId)
        }
    }

    /// playhead-wxsv: an ABSENCE of evidence is not evidence.
    ///
    /// A terminal row with no recorded attempt cannot be shown to be stale, and
    /// re-opening on that would be unbounded — every invocation re-opens, runs,
    /// and re-opens again. Production cannot produce this state; a fixture and a
    /// future repair path can, which is exactly why the guard is positive.
    @Test("playhead-wxsv: re-open refuses a terminal row that was never stamped")
    func reopenRefusesAnUnstampedRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "unstamped", status: .queued)
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobRunning(jobId: job.jobId)
        try await store.markBackfillJobComplete(jobId: job.jobId, progressCursor: nil)

        #expect(try await store.reopenBackfillJob(jobId: job.jobId, forTranscriptVersion: "tx-anything") == false)
        #expect(try await store.fetchBackfillJob(byId: job.jobId)?.status == .complete)
    }

    /// playhead-wxsv SPEC 4: no value may carry a premise that can expire.
    ///
    /// `retryCount` at the budget asserts "this work is hopeless". That premise
    /// is scoped to a transcript, and the pull's evidence is that the failures
    /// are not about the episode at all — all 5 `failed` rows are transient FM
    /// daemon faults. Before this bead the premise expired by ACCIDENT: a new
    /// version minted a new id, so an exhausted row was escaped rather than
    /// retried, and its cursor was thrown away with it. Removing the accident
    /// without replacing it would have turned one bad afternoon into permanent
    /// retirement for the episode.
    ///
    /// A broken implementation that would still pass a status-only check: one
    /// that re-opens to `queued` while leaving `retryCount` at the budget. The
    /// row would look startable and be refused by `markBackfillJobRunning`
    /// forever — a row nothing can start, blocking, which is spec 1 again.
    @Test("playhead-wxsv: re-opening an exhausted row clears the retry budget it spent elsewhere")
    func reopenClearsTheRetryBudget() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "exhausted-elsewhere", createdAt: 2_000)
        try await store.insertBackfillJob(job)
        try await store.noteBackfillJobAttempt(jobId: job.jobId, transcriptVersion: "tx-v1")
        try await store.markBackfillJobRunning(jobId: job.jobId)
        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "Request has been rate limited. Please try again later.",
            retryCount: AdmissionController.maxRetries
        )
        #expect(try await store.countResumableBackfillJobs(assetId: "asset-1") == 0,
                "an exhausted row is not resumable while its premise holds")

        // The premise does NOT expire on its own: at the SAME version the row
        // stays retired, and nothing can start it.
        #expect(try await store.reopenBackfillJob(jobId: job.jobId, forTranscriptVersion: "tx-v1") == false)
        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobRunning(jobId: job.jobId)
        }

        // It expires when the thing it was scoped to changes.
        #expect(try await store.reopenBackfillJob(jobId: job.jobId, forTranscriptVersion: "tx-v2"))

        let reopened = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(reopened.retryCount == 0, "a different transcript is different work")
        #expect(reopened.status == .queued)
        #expect(reopened.createdAt == 2_000, "re-opening must not move createdAt — see spec item 3")
        // …and it is genuinely startable now, not merely relabelled.
        try await store.markBackfillJobRunning(jobId: job.jobId)
        #expect(try await store.fetchBackfillJob(byId: job.jobId)?.status == .running)
    }

    /// A live carrier owns its row. Re-opening under it would hand the same work
    /// to two drivers; the stranded-row reaper is the path for a carrier that
    /// died, and it runs across a process boundary where that is knowable.
    @Test("playhead-wxsv: re-open refuses a running row")
    func reopenRefusesARunningRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "in-flight")
        try await store.insertBackfillJob(job)
        try await store.noteBackfillJobAttempt(jobId: job.jobId, transcriptVersion: "tx-v1")
        try await store.markBackfillJobRunning(jobId: job.jobId)

        #expect(try await store.reopenBackfillJob(jobId: job.jobId, forTranscriptVersion: "tx-v2") == false)
        #expect(try await store.fetchBackfillJob(byId: job.jobId)?.status == .running)
    }
}
