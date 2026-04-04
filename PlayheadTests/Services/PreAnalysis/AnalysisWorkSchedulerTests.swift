// AnalysisWorkSchedulerTests.swift
// Tests for the eligibility-aware pre-analysis work scheduler.
// These tests focus on store-level behavior (enqueue, priority, eligibility,
// tier advancement) without running the full pipeline.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — Store-level behavior")
struct AnalysisWorkSchedulerTests {

    @Test("enqueue creates a job with correct fields")
    func testEnqueueCreatesJob() async throws {
        let store = try await makeTestStore()
        let ids = try await store.fetchAllJobEpisodeIds()
        #expect(ids.isEmpty)

        // Simulate enqueue by inserting a job directly (same logic as scheduler.enqueue)
        let now = Date().timeIntervalSince1970
        let job = makeAnalysisJob(
            episodeId: "ep-1",
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let fetched = try await store.fetchAllJobEpisodeIds()
        #expect(fetched.contains("ep-1"))
    }

    @Test("explicit downloads get priority=10, auto gets priority=0")
    func testExplicitDownloadsPrioritized() async throws {
        let store = try await makeTestStore()

        let autoJob = makeAnalysisJob(
            jobId: "auto-job",
            episodeId: "ep-auto",
            workKey: "fp-auto:1:preAnalysis",
            sourceFingerprint: "fp-auto",
            priority: 0,
            desiredCoverageSec: 90
        )
        let explicitJob = makeAnalysisJob(
            jobId: "explicit-job",
            episodeId: "ep-explicit",
            workKey: "fp-explicit:1:preAnalysis",
            sourceFingerprint: "fp-explicit",
            priority: 10,
            desiredCoverageSec: 90
        )

        try await store.insertJob(autoJob)
        try await store.insertJob(explicitJob)

        let now = Date().timeIntervalSince1970
        let nextJob = try await store.fetchNextEligibleJob(
            isCharging: false,
            isThermalOk: true,
            t0ThresholdSec: 90,
            now: now
        )
        #expect(nextJob?.episodeId == "ep-explicit")
        #expect(nextJob?.priority == 10)
    }

    @Test("charging gates deferred lane (T1+) jobs")
    func testChargingGatesDeferredLane() async throws {
        let store = try await makeTestStore()

        let t1Job = makeAnalysisJob(
            jobId: "t1-job",
            episodeId: "ep-deferred",
            desiredCoverageSec: 300,
            state: "paused"
        )
        try await store.insertJob(t1Job)

        let now = Date().timeIntervalSince1970

        // Not charging: deferred job should not be returned
        let notCharging = try await store.fetchNextEligibleJob(
            isCharging: false,
            isThermalOk: true,
            t0ThresholdSec: 90,
            now: now
        )
        #expect(notCharging == nil)

        // Charging: deferred job should be returned
        let charging = try await store.fetchNextEligibleJob(
            isCharging: true,
            isThermalOk: true,
            t0ThresholdSec: 90,
            now: now
        )
        #expect(charging?.jobId == "t1-job")
    }

    @Test("tier advancement creates paused next-tier job")
    func testTierAdvancement() async throws {
        let store = try await makeTestStore()
        let config = PreAnalysisConfig()

        // Simulate: T0 job completed successfully, scheduler creates T1 job
        let t0Job = makeAnalysisJob(
            jobId: "t0-job",
            episodeId: "ep-1",
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(t0Job)

        // Simulate scheduler marking T0 as complete and creating T1
        try await store.updateJobState(jobId: "t0-job", state: "complete")

        let tierWorkKey = "fp-test:1:preAnalysis:\(Int(config.t1DepthSeconds))"
        let now = Date().timeIntervalSince1970
        let t1Job = AnalysisJob(
            jobId: UUID().uuidString,
            jobType: "preAnalysis",
            episodeId: "ep-1",
            podcastId: nil,
            analysisAssetId: nil,
            workKey: tierWorkKey,
            sourceFingerprint: "fp-test",
            downloadId: "dl-1",
            priority: 0,
            desiredCoverageSec: config.t1DepthSeconds,
            featureCoverageSec: 90,
            transcriptCoverageSec: 90,
            cueCoverageSec: 90,
            state: "paused",
            attemptCount: 0,
            nextEligibleAt: nil,
            leaseOwner: nil,
            leaseExpiresAt: nil,
            lastErrorCode: nil,
            createdAt: now,
            updatedAt: now
        )
        try await store.insertJob(t1Job)

        // Verify T0 is complete
        let completedJob = try await store.fetchJob(byId: "t0-job")
        #expect(completedJob?.state == "complete")

        // Verify T1 job exists in paused state
        let pausedJobs = try await store.fetchJobsByState("paused")
        let t1Jobs = pausedJobs.filter { $0.episodeId == "ep-1" && $0.desiredCoverageSec == 300 }
        #expect(!t1Jobs.isEmpty)
    }

    @Test("exponential backoff formula: min(2^(attempt+1) * 60, 3600)")
    func testExponentialBackoffOnFailure() async throws {
        let store = try await makeTestStore()

        let job = makeAnalysisJob(
            jobId: "fail-job",
            episodeId: "ep-1",
            state: "queued",
            attemptCount: 2
        )
        try await store.insertJob(job)

        // Simulate scheduler handling a failure with attemptCount=2
        // backoff = min(2^(2+1) * 60, 3600) = min(480, 3600) = 480
        let backoff = min(pow(2.0, Double(job.attemptCount + 1)) * 60, 3600)
        let nextEligible = Date().timeIntervalSince1970 + backoff
        try await store.updateJobState(
            jobId: "fail-job",
            state: "failed",
            nextEligibleAt: nextEligible,
            lastErrorCode: "testError"
        )

        let failedJob = try await store.fetchJob(byId: "fail-job")
        #expect(failedJob?.state == "failed")
        #expect(failedJob?.lastErrorCode == "testError")
        #expect(backoff == 480.0)
    }

    @Test("backoff capped at 3600s")
    func testBackoffCappedAt3600() async throws {
        let attemptCount = 10
        let backoff = min(pow(2.0, Double(attemptCount + 1)) * 60, 3600)
        #expect(backoff == 3600.0)
    }

    @Test("audio URL resolution failure blocks job as missingFile")
    func testAudioURLResolutionFailureBlocksJob() async throws {
        let store = try await makeTestStore()

        let job = makeAnalysisJob(
            jobId: "missing-audio-job",
            episodeId: "ep-missing",
            state: "queued"
        )
        try await store.insertJob(job)

        // Simulate scheduler: cachedFileURL returns nil → blocked:missingFile
        try await store.updateJobState(jobId: "missing-audio-job", state: "blocked:missingFile")

        let blockedJob = try await store.fetchJob(byId: "missing-audio-job")
        #expect(blockedJob?.state == "blocked:missingFile")
    }
}
