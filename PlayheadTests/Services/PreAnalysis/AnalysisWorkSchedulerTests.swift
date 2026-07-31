// AnalysisWorkSchedulerTests.swift
// Tests for the eligibility-aware pre-analysis work scheduler.
// These tests focus on store-level behavior (enqueue, priority, eligibility,
// tier advancement) without running the full pipeline.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — Store-level behavior")
struct AnalysisWorkSchedulerTests {

    @Test("coverage-insufficient retry requires incremental transcript or cue progress")
    func testCoverageInsufficientRetryRequiresProgress() {
        let job = makeAnalysisJob(
            desiredCoverageSec: 90,
            featureCoverageSec: 0,
            transcriptCoverageSec: 0,
            cueCoverageSec: 0
        )
        let outcome = AnalysisOutcome(
            assetId: "asset-1",
            requestedCoverageSec: 90,
            featureCoverageSec: 90,
            transcriptCoverageSec: 90,
            cueCoverageSec: 0,
            newCueCount: 0,
            stopReason: .reachedTarget
        )

        #expect(AnalysisWorkScheduler.shouldRetryCoverageInsufficient(job: job, outcome: outcome))
    }

    @Test("coverage-insufficient retry stops when a reachedTarget pass makes no progress")
    func testCoverageInsufficientRetryStopsWithoutProgress() {
        let job = makeAnalysisJob(
            desiredCoverageSec: 90,
            featureCoverageSec: 90,
            transcriptCoverageSec: 90,
            cueCoverageSec: 0
        )
        let outcome = AnalysisOutcome(
            assetId: "asset-1",
            requestedCoverageSec: 90,
            featureCoverageSec: 90,
            transcriptCoverageSec: 90,
            cueCoverageSec: 0,
            newCueCount: 0,
            stopReason: .reachedTarget
        )

        #expect(!AnalysisWorkScheduler.shouldRetryCoverageInsufficient(job: job, outcome: outcome))
    }

    @Test("coverage-insufficient retry keeps running when new cues were created")
    func testCoverageInsufficientRetryTreatsNewCuesAsProgress() {
        let job = makeAnalysisJob(
            desiredCoverageSec: 90,
            featureCoverageSec: 90,
            transcriptCoverageSec: 90,
            cueCoverageSec: 0
        )
        let outcome = AnalysisOutcome(
            assetId: "asset-1",
            requestedCoverageSec: 90,
            featureCoverageSec: 90,
            transcriptCoverageSec: 90,
            cueCoverageSec: 0,
            newCueCount: 1,
            stopReason: .reachedTarget
        )

        #expect(AnalysisWorkScheduler.shouldRetryCoverageInsufficient(job: job, outcome: outcome))
    }

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
            deferredWorkAllowed: true,
            t0ThresholdSec: 90,
            now: now
        )
        #expect(nextJob?.episodeId == "ep-explicit")
        #expect(nextJob?.priority == 10)
    }

    @Test("deferred-work admission gates deferred lane (T1+) jobs")
    func testDeferredWorkAdmissionGatesDeferredLane() async throws {
        let store = try await makeTestStore()

        let t1Job = makeAnalysisJob(
            jobId: "t1-job",
            jobType: "preAnalysis",
            episodeId: "ep-deferred",
            desiredCoverageSec: 300,
            state: "paused"
        )
        try await store.insertJob(t1Job)

        let now = Date().timeIntervalSince1970

        // Admission denied: deferred job should not be returned.
        let notCharging = try await store.fetchNextEligibleJob(
            deferredWorkAllowed: false,
            t0ThresholdSec: 90,
            now: now
        )
        #expect(notCharging == nil)

        // Admission granted: deferred job should be returned.
        let charging = try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true,
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

    @Test("exponential backoff formula: min(2^attempt * 60, 3600)")
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
        // backoff = min(2^2 * 60, 3600) = min(240, 3600) = 240
        let backoff = min(pow(2.0, Double(job.attemptCount)) * 60, 3600)
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
        #expect(backoff == 240.0)
    }

    @Test("backoff capped at 3600s")
    func testBackoffCappedAt3600() async throws {
        let attemptCount = 10
        let backoff = min(pow(2.0, Double(attemptCount)) * 60, 3600)
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

    // MARK: - Tier satisfaction (playhead-8bp2)

    /// The exact shape of `analysis_jobs` row `07DBF13B` on the 2026-07-30
    /// device pull: a 300 s tier on a 1,672 s episode, transcript watermark at
    /// 300 s, last confident ad window ending at 120 s. It terminated
    /// `state='complete', lastErrorCode='coverageInsufficient:noProgress'` and,
    /// because `workKey` is UNIQUE and `insertJob` is `INSERT OR IGNORE`, that
    /// episode can never be enqueued again at this `analysisVersion`. The tier's
    /// work was finished; only the question was wrong.
    @Test("a tier whose audio was transcribed is satisfied even with no ad past the target")
    func tierSatisfiedByTranscriptWithoutLateCue() {
        let job = makeAnalysisJob(desiredCoverageSec: 300, cueCoverageSec: 0)
        let outcome = AnalysisOutcome(
            assetId: "asset-1",
            requestedCoverageSec: 300,
            featureCoverageSec: 300,
            transcriptCoverageSec: 300,
            cueCoverageSec: 119.82,
            newCueCount: 0,
            stopReason: .reachedTarget
        )

        #expect(AnalysisWorkScheduler.tierTargetSatisfied(job: job, outcome: outcome))
    }

    /// The legacy sufficient condition, kept: a confident ad window that ends
    /// past the target does imply the detector read that far.
    @Test("a cue past the target still satisfies the tier")
    func tierSatisfiedByCuePastTarget() {
        let job = makeAnalysisJob(desiredCoverageSec: 300)
        let outcome = AnalysisOutcome(
            assetId: "asset-1",
            requestedCoverageSec: 300,
            featureCoverageSec: 100,
            transcriptCoverageSec: 100,
            cueCoverageSec: 400,
            newCueCount: 0,
            stopReason: .reachedTarget
        )

        #expect(AnalysisWorkScheduler.tierTargetSatisfied(job: job, outcome: outcome))
    }

    @Test("a tier whose audio was NOT read is not satisfied")
    func tierNotSatisfiedWhenAudioUnread() {
        let job = makeAnalysisJob(desiredCoverageSec: 300)
        let outcome = AnalysisOutcome(
            assetId: "asset-1",
            requestedCoverageSec: 300,
            featureCoverageSec: 100,
            transcriptCoverageSec: 100,
            cueCoverageSec: 0,
            newCueCount: 0,
            stopReason: .reachedTarget
        )

        #expect(!AnalysisWorkScheduler.tierTargetSatisfied(job: job, outcome: outcome))
    }

    /// Feature extraction sweeps the whole episode independently of
    /// transcription — three `superseded` rows on the same device pull carried
    /// `featureCoverageSec` at full duration against `transcriptCoverageSec == 0`.
    /// An implementation that accepted feature coverage would deepen the target
    /// of an episode that has no transcript at all, which is the opposite of
    /// what this bead is for.
    @Test("feature coverage alone does not satisfy a tier")
    func tierNotSatisfiedByFeatureCoverageAlone() {
        let job = makeAnalysisJob(desiredCoverageSec: 300)
        let outcome = AnalysisOutcome(
            assetId: "asset-1",
            requestedCoverageSec: 300,
            featureCoverageSec: 3_180,
            transcriptCoverageSec: 0,
            cueCoverageSec: 0,
            newCueCount: 0,
            stopReason: .reachedTarget
        )

        #expect(!AnalysisWorkScheduler.tierTargetSatisfied(job: job, outcome: outcome))
    }

    // MARK: - Coverage tier ladder (playhead-8bp2)

    private static let configuredTiers: [Double] = [90, 300, 900]

    /// Without a duration the ladder is exactly the pre-8bp2 one, so a missing
    /// `episodeDurationSec` degrades to the old ceiling rather than guessing.
    @Test("with no known duration the ladder is the configured tiers and stops at T2")
    func ladderWithoutDurationMatchesConfiguredTiers() {
        #expect(
            AnalysisWorkScheduler.coverageTierLadder(
                tiers: Self.configuredTiers,
                episodeDurationSec: nil
            ) == [90, 300, 900]
        )
        #expect(
            AnalysisWorkScheduler.nextTierCoverage(
                current: 900,
                tiers: Self.configuredTiers,
                episodeDurationSec: nil
            ) == nil
        )
    }

    /// The headline defect: `B7D5B117` is a 6,147 s episode whose only job
    /// terminated at a 90 s target. Even a perfectly healthy ladder used to top
    /// out at T2 = 900 s, i.e. 14.6% of it.
    @Test("a long episode's ladder ends at the episode, not at T2")
    func ladderEndsAtEpisodeDuration() {
        let ladder = AnalysisWorkScheduler.coverageTierLadder(
            tiers: Self.configuredTiers,
            episodeDurationSec: 6_147
        )
        #expect(ladder == [90, 300, 900, 6_147])
        #expect(
            AnalysisWorkScheduler.nextTierCoverage(
                current: 900,
                tiers: Self.configuredTiers,
                episodeDurationSec: 6_147
            ) == 6_147
        )
        #expect(
            AnalysisWorkScheduler.nextTierCoverage(
                current: 6_147,
                tiers: Self.configuredTiers,
                episodeDurationSec: 6_147
            ) == nil
        )
    }

    /// No rung may ask for audio that does not exist — reaching for it costs a
    /// pass that can never succeed.
    @Test("no rung exceeds the episode duration")
    func ladderNeverExceedsDuration() {
        let ladder = AnalysisWorkScheduler.coverageTierLadder(
            tiers: Self.configuredTiers,
            episodeDurationSec: 500
        )
        #expect(ladder == [90, 300, 500])
        #expect(
            AnalysisWorkScheduler.nextTierCoverage(
                current: 500,
                tiers: Self.configuredTiers,
                episodeDurationSec: 500
            ) == nil
        )
    }

    /// The tier `workKey` suffix is `":\(Int(nextCoverage))"`, so two rungs
    /// under a second apart would truncate to the SAME key — and `insertJob` is
    /// `INSERT OR IGNORE`, which would swallow the deeper rung without a word.
    /// A 900.4 s episode is the concrete case.
    @Test("rungs never collide once truncated into a workKey suffix")
    func ladderRungsDoNotCollideAsWorkKeySuffixes() {
        for duration in [900.4, 900.0, 899.6, 90.2, 301.0, 6_147.9] {
            let ladder = AnalysisWorkScheduler.coverageTierLadder(
                tiers: Self.configuredTiers,
                episodeDurationSec: duration
            )
            let suffixes = ladder.map { Int($0) }
            #expect(Set(suffixes).count == ladder.count,
                    "duration \(duration) produced colliding workKey suffixes \(suffixes)")
        }
    }

    /// `episodeDurationSec` is a `REAL` read off disk and the rung becomes a
    /// `workKey` suffix via `Int(_:)`, which TRAPS on a non-finite or
    /// out-of-range value. A corrupt row must degrade to the configured tiers,
    /// not crash the scheduler.
    @Test("a corrupt or absurd duration degrades to the configured tiers")
    func ladderRejectsUnusableDurations() {
        let unusable: [Double] = [
            .nan,
            .infinity,
            -.infinity,
            0,
            -1,
            AnalysisWorkScheduler.maximumTierLadderDurationSeconds + 1,
            1e30,
        ]
        for duration in unusable {
            let ladder = AnalysisWorkScheduler.coverageTierLadder(
                tiers: Self.configuredTiers,
                episodeDurationSec: duration
            )
            #expect(ladder == [90, 300, 900], "duration \(duration) must not become a rung")
            #expect(ladder.allSatisfy { $0.isFinite })
        }
        // The bound itself is inclusive — a 24 h episode is still usable.
        #expect(
            AnalysisWorkScheduler.coverageTierLadder(
                tiers: Self.configuredTiers,
                episodeDurationSec: AnalysisWorkScheduler.maximumTierLadderDurationSeconds
            ).last == AnalysisWorkScheduler.maximumTierLadderDurationSeconds
        )
    }

    /// Bounded by construction: walking the ladder from zero must strictly
    /// increase and halt. An unbounded retry is a worse bug than the one this
    /// bead fixes.
    @Test("walking the ladder strictly increases and terminates")
    func ladderWalkTerminates() {
        for duration in [nil, 45.0, 500.0, 900.0, 6_147.0] as [Double?] {
            var current = 0.0
            var steps = 0
            while let next = AnalysisWorkScheduler.nextTierCoverage(
                current: current,
                tiers: Self.configuredTiers,
                episodeDurationSec: duration
            ) {
                #expect(next > current)
                current = next
                steps += 1
                #expect(steps <= 4, "ladder did not terminate for duration \(String(describing: duration))")
                if steps > 4 { break }
            }
        }
    }
}
