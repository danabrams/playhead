// AnalysisJobStoreTests.swift
// Tests for the analysis_jobs table CRUD operations in AnalysisStore.

import Foundation
import Testing
@testable import Playhead

// MARK: - Test Helpers

private let _testStoreDirs = TestTempDirTracker()

private func makeTestStore() async throws -> AnalysisStore {
    let dir = try makeTempDir(prefix: "AnalysisJobTests")
    _testStoreDirs.track(dir)
    let store = try AnalysisStore(directory: dir)
    try await store.migrate()
    return store
}

// MARK: - Tests

@Suite("AnalysisJob CRUD")
struct AnalysisJobStoreTests {

    @Test func testInsertAndFetch() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(jobId: "j1", workKey: "wk-1")
        try await store.insertJob(job)

        let fetched = try await store.fetchJob(byId: "j1")
        #expect(fetched != nil)
        #expect(fetched?.jobId == "j1")
        #expect(fetched?.workKey == "wk-1")
        #expect(fetched?.state == "queued")
    }

    @Test func testWorkKeyDedup() async throws {
        let store = try await makeTestStore()
        let job1 = makeAnalysisJob(jobId: "j1", workKey: "wk-dup")
        let job2 = makeAnalysisJob(jobId: "j2", workKey: "wk-dup")
        try await store.insertJob(job1)
        try await store.insertJob(job2) // should be silently ignored

        let fetched = try await store.fetchJob(byId: "j1")
        #expect(fetched != nil)
        let missing = try await store.fetchJob(byId: "j2")
        #expect(missing == nil)
    }

    @Test func testFetchNextEligibleJobT0() async throws {
        let store = try await makeTestStore()
        // A playback job with zero coverage should be eligible regardless of charging/thermal
        let job = makeAnalysisJob(
            jobId: "t0",
            jobType: "playback",
            workKey: "wk-t0",
            featureCoverageSec: 0
        )
        try await store.insertJob(job)

        let now = Date().timeIntervalSince1970
        let eligible = try await store.fetchNextEligibleJob(
            isCharging: false, isThermalOk: false, t0ThresholdSec: 1.0, now: now
        )
        #expect(eligible != nil)
        #expect(eligible?.jobId == "t0")
    }

    @Test func testFetchNextEligibleJobDeferredRequiresCharging() async throws {
        let store = try await makeTestStore()
        // A backfill job should only be eligible when charging + thermal ok
        let job = makeAnalysisJob(
            jobId: "bf1",
            jobType: "backfill",
            workKey: "wk-bf1",
            featureCoverageSec: 100
        )
        try await store.insertJob(job)

        let now = Date().timeIntervalSince1970

        // Not charging => not eligible
        let notEligible = try await store.fetchNextEligibleJob(
            isCharging: false, isThermalOk: true, t0ThresholdSec: 1.0, now: now
        )
        #expect(notEligible == nil)

        // Charging + thermal ok => eligible
        let eligible = try await store.fetchNextEligibleJob(
            isCharging: true, isThermalOk: true, t0ThresholdSec: 1.0, now: now
        )
        #expect(eligible != nil)
        #expect(eligible?.jobId == "bf1")
    }

    @Test func testPriorityOrdering() async throws {
        let store = try await makeTestStore()
        let now = Date().timeIntervalSince1970
        let low = makeAnalysisJob(
            jobId: "low", jobType: "backfill", workKey: "wk-low",
            priority: 1, featureCoverageSec: 100, createdAt: now, updatedAt: now
        )
        let high = makeAnalysisJob(
            jobId: "high", jobType: "backfill", workKey: "wk-high",
            priority: 10, featureCoverageSec: 100, createdAt: now, updatedAt: now
        )
        try await store.insertJob(low)
        try await store.insertJob(high)

        let eligible = try await store.fetchNextEligibleJob(
            isCharging: true, isThermalOk: true, t0ThresholdSec: 1.0, now: now
        )
        #expect(eligible?.jobId == "high")
    }

    @Test func testNextEligibleAtRespected() async throws {
        let store = try await makeTestStore()
        let now = Date().timeIntervalSince1970
        let future = now + 3600
        let job = makeAnalysisJob(
            jobId: "deferred",
            jobType: "backfill",
            workKey: "wk-deferred",
            featureCoverageSec: 100,
            nextEligibleAt: future
        )
        try await store.insertJob(job)

        // Not eligible yet because nextEligibleAt is in the future
        let notYet = try await store.fetchNextEligibleJob(
            isCharging: true, isThermalOk: true, t0ThresholdSec: 1.0, now: now
        )
        #expect(notYet == nil)

        // Eligible once "now" passes nextEligibleAt
        let later = try await store.fetchNextEligibleJob(
            isCharging: true, isThermalOk: true, t0ThresholdSec: 1.0, now: future + 1
        )
        #expect(later?.jobId == "deferred")
    }

    @Test func testLeaseAcquireRelease() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(jobId: "lease-test", workKey: "wk-lease")
        try await store.insertJob(job)

        let future = Date().timeIntervalSince1970 + 300
        let acquired = try await store.acquireLease(jobId: "lease-test", owner: "worker-1", expiresAt: future)
        #expect(acquired == true)

        let fetched = try await store.fetchJob(byId: "lease-test")
        #expect(fetched?.leaseOwner == "worker-1")
        #expect(fetched?.leaseExpiresAt == future)

        try await store.releaseLease(jobId: "lease-test")
        let released = try await store.fetchJob(byId: "lease-test")
        #expect(released?.leaseOwner == nil)
        #expect(released?.leaseExpiresAt == nil)
    }

    @Test func testLeaseConflict() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(jobId: "conflict", workKey: "wk-conflict")
        try await store.insertJob(job)

        let future = Date().timeIntervalSince1970 + 300
        let first = try await store.acquireLease(jobId: "conflict", owner: "worker-1", expiresAt: future)
        #expect(first == true)

        // Second acquire should fail because lease hasn't expired
        let second = try await store.acquireLease(jobId: "conflict", owner: "worker-2", expiresAt: future)
        #expect(second == false)

        // Verify the original owner still holds it
        let fetched = try await store.fetchJob(byId: "conflict")
        #expect(fetched?.leaseOwner == "worker-1")
    }

    @Test func testUpdateProgress() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(jobId: "prog", workKey: "wk-prog")
        try await store.insertJob(job)

        try await store.updateJobProgress(
            jobId: "prog",
            featureCoverageSec: 120,
            transcriptCoverageSec: 90,
            cueCoverageSec: 60
        )

        let fetched = try await store.fetchJob(byId: "prog")
        #expect(fetched?.featureCoverageSec == 120)
        #expect(fetched?.transcriptCoverageSec == 90)
        #expect(fetched?.cueCoverageSec == 60)
    }

    @Test func testUpdateState() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(jobId: "state", workKey: "wk-state")
        try await store.insertJob(job)

        try await store.updateJobState(
            jobId: "state",
            state: "failed",
            nextEligibleAt: 999999,
            lastErrorCode: "timeout"
        )

        let fetched = try await store.fetchJob(byId: "state")
        #expect(fetched?.state == "failed")
        #expect(fetched?.nextEligibleAt == 999999)
        #expect(fetched?.lastErrorCode == "timeout")
    }

    @Test func testMigrationCreatesTable() async throws {
        // Verify that a fresh store has the analysis_jobs table by inserting/fetching
        let store = try await makeTestStore()
        let job = makeAnalysisJob(jobId: "migration-check", workKey: "wk-migration")
        try await store.insertJob(job)
        let fetched = try await store.fetchJob(byId: "migration-check")
        #expect(fetched != nil)
    }
}
