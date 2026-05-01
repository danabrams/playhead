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
        // Current schema is v19 after Bug 5 (skip-cues-deletion)
        // dropped the vestigial `skip_cues` table.
        #expect(version == 19)
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

    @Test("C-2: markBackfillJobRunning throws on a failed row")
    func markBackfillJobRunning_throwsOnFailedRow() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "terminal-failed")
        try await store.insertBackfillJob(job)
        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "boom",
            retryCount: 1
        )

        await #expect(throws: AnalysisStoreError.self) {
            try await store.markBackfillJobRunning(jobId: job.jobId)
        }

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .failed, "row must not be resurrected to running")
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

    @Test("markBackfillJobRunning on a failed row carries fromStatus='failed'")
    func markBackfillJobRunning_throwsOnFailedRow_carriesPriorStatus() async throws {
        let store = try await makeTestStore()
        try await insertParentAsset(store)
        let job = makeBackfillJob(jobId: "prior-failed-job", status: .queued)
        try await store.insertBackfillJob(job)

        // Move the row into the `.failed` terminal state.
        try await store.markBackfillJobRunning(jobId: job.jobId)
        try await store.markBackfillJobFailed(
            jobId: job.jobId,
            reason: "boom",
            retryCount: 1
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
}
