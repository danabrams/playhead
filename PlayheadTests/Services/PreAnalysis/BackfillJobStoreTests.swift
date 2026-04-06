// BackfillJobStoreTests.swift
// Tests for the dedicated backfill_jobs persistence and resume semantics.

import Foundation
import Testing

@testable import Playhead

@Suite("BackfillJob Store")
struct BackfillJobStoreTests {

    @Test("BackfillJob round-trips through SQLite with cohort JSON")
    func testBackfillJobRoundTrip() async throws {
        let store = try await makeTestStore()
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
            scanCohortJSON: encodedScanCohort,
            decisionCohortJSON: #"{"schema":"decision-v1"}"#
        )

        try await store.insertBackfillJob(job)
        let fetched = try await store.fetchBackfillJob(byId: job.jobId)

        #expect(fetched == job)
    }

    @Test("checkpoint survives interruption and resumes from stored cursor")
    func testCheckpointAndResume() async throws {
        let dir = try makeTempDir(prefix: "BackfillResume")
        let store = try await AnalysisStore.open(directory: dir)
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
}
