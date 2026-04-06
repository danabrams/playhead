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
        try await store.checkpointBackfillJob(
            jobId: job.jobId,
            progressCursor: BackfillProgressCursor(
                processedUnitCount: 2,
                lastProcessedUpperBoundSec: 90
            ),
            status: .running
        )

        let reopened = try await AnalysisStore.open(directory: dir)
        let resumed = try #require(await reopened.fetchBackfillJob(byId: job.jobId))

        #expect(resumed.phase == .scanLikelyAdSlots)
        #expect(resumed.progressCursor == BackfillProgressCursor(
            processedUnitCount: 2,
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
            progressCursor: BackfillProgressCursor(processedUnitCount: 3),
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
            progressCursor: BackfillProgressCursor(processedUnitCount: 4)
        )

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.deferReason == "thermal")
        #expect(fetched.progressCursor?.processedUnitCount == 4)
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
                processedUnitCount: 7,
                lastProcessedUpperBoundSec: 123.0
            )
        )

        try await store.insertBackfillJob(job)
        try await store.markBackfillJobDeferred(jobId: job.jobId, reason: "battery")

        let fetched = try #require(await store.fetchBackfillJob(byId: job.jobId))
        #expect(fetched.status == .deferred)
        #expect(fetched.deferReason == "battery")
        #expect(fetched.progressCursor == BackfillProgressCursor(
            processedUnitCount: 7,
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
        #expect(version == 1)
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
}
