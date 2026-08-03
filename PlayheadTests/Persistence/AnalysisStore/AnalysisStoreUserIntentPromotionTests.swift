// AnalysisStoreUserIntentPromotionTests.swift
// playhead-kanf: `promoteQueuedJobToUserIntentLane` is the write that makes a
// "Download & Analyze" tap mean something for an episode the auto-pipeline
// already queued. Its guard — `state = 'queued' AND leaseOwner IS NULL` — is
// the correctness boundary the bead draws, so both halves are tested
// independently, and every refusal is paired with a positive witness proving
// the promotion path is live in that same configuration (playhead-le02: a
// negative assertion needs a positive witness).

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisStore — user-intent lane promotion (playhead-kanf)")
struct AnalysisStoreUserIntentPromotionTests {

    private static let nowFloor = AnalysisWorkScheduler.nowLanePriorityFloor

    /// A `queued`, unleased pre-analysis row at background priority — exactly
    /// what the auto-pipeline leaves behind and what the tap must promote.
    private func makeJob(
        jobId: String = "job-1",
        episodeId: String = "ep-1",
        workKey: String = "wk-1",
        priority: Int = 0,
        state: String = "queued",
        leaseOwner: String? = nil,
        leaseExpiresAt: Double? = nil,
        createdAt: Double = 1_000,
        updatedAt: Double = 1_000
    ) -> AnalysisJob {
        AnalysisJob(
            jobId: jobId,
            jobType: "preAnalysis",
            episodeId: episodeId,
            podcastId: "pod",
            analysisAssetId: nil,
            workKey: workKey,
            sourceFingerprint: "fp-\(episodeId)",
            downloadId: episodeId,
            priority: priority,
            desiredCoverageSec: 120,
            featureCoverageSec: 0,
            transcriptCoverageSec: 0,
            cueCoverageSec: 0,
            state: state,
            attemptCount: 0,
            nextEligibleAt: nil,
            leaseOwner: leaseOwner,
            leaseExpiresAt: leaseExpiresAt,
            lastErrorCode: nil,
            createdAt: createdAt,
            updatedAt: updatedAt,
            generationID: "gen-original",
            schedulerEpoch: 7
        )
    }

    // MARK: - The promotion itself

    @Test("a queued, unleased row is promoted to the .now floor")
    func testQueuedRowIsPromoted() async throws {
        let store = try await makeTestStore()
        try await store.insertJob(makeJob())

        let outcome = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-1",
            priority: Self.nowFloor,
            now: 2_000
        )

        #expect(outcome == .promoted(
            jobId: "job-1",
            fromPriority: 0,
            toPriority: Self.nowFloor
        ))
        let row = try #require(try await store.fetchJob(byWorkKey: "wk-1"))
        #expect(row.priority == Self.nowFloor)
        #expect(row.schedulerLane == .now)
    }

    @Test("promotion touches priority and updatedAt only — never the lifecycle columns")
    func testPromotionIsASelectionNudgeNotATransition() async throws {
        let store = try await makeTestStore()
        try await store.insertJob(makeJob())

        _ = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-1",
            priority: Self.nowFloor,
            now: 2_000
        )

        let row = try #require(try await store.fetchJob(byWorkKey: "wk-1"))
        #expect(row.priority == Self.nowFloor)
        #expect(row.updatedAt == 2_000)
        // Everything a lifecycle transition WOULD move, and doesn't here. The
        // FIFO tiebreak (`createdAt`) and the orphan-recovery routing pair
        // (`generationID` / `schedulerEpoch`) are the two that would corrupt
        // real behaviour if this were mistaken for a transition.
        #expect(row.createdAt == 1_000)
        #expect(row.state == "queued")
        #expect(row.leaseOwner == nil)
        #expect(row.leaseExpiresAt == nil)
        #expect(row.generationID == "gen-original")
        #expect(row.schedulerEpoch == 7)
        #expect(row.desiredCoverageSec == 120)
        #expect(row.attemptCount == 0)
    }

    @Test("an explicit-download row at priority 10 is still promoted to the .now floor")
    func testSoonLaneRowIsPromoted() async throws {
        let store = try await makeTestStore()
        try await store.insertJob(makeJob(priority: 10))

        let outcome = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-1",
            priority: Self.nowFloor,
            now: 2_000
        )

        #expect(outcome == .promoted(
            jobId: "job-1",
            fromPriority: 10,
            toPriority: Self.nowFloor
        ))
        let row = try #require(try await store.fetchJob(byWorkKey: "wk-1"))
        #expect(row.schedulerLane == .now)
    }

    // MARK: - The refusals (each with a positive witness)

    @Test("a LEASED row is not promoted, while an unleased twin in the same store is")
    func testLeasedRowIsNotPromoted() async throws {
        let store = try await makeTestStore()
        // The leased row: state left at 'queued' deliberately, so the ONLY
        // thing refusing the promotion is `leaseOwner IS NULL`. `queued` +
        // stale lease is a state the schema really produces — see
        // `fetchNextEligibleJob`, which selects `state IN ('queued','paused')
        // AND (leaseOwner IS NULL OR leaseExpiresAt < ?)`.
        try await store.insertJob(makeJob(
            jobId: "job-leased",
            episodeId: "ep-leased",
            workKey: "wk-leased",
            leaseOwner: "worker-A",
            leaseExpiresAt: 9_999
        ))
        // The witness: identical in every respect except the lease.
        try await store.insertJob(makeJob(
            jobId: "job-free",
            episodeId: "ep-free",
            workKey: "wk-free"
        ))

        let refused = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-leased",
            priority: Self.nowFloor,
            now: 2_000
        )
        let served = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-free",
            priority: Self.nowFloor,
            now: 2_000
        )

        #expect(refused == .notPromotable(
            jobId: "job-leased",
            state: "queued",
            priority: 0,
            leased: true
        ))
        #expect(served == .promoted(
            jobId: "job-free",
            fromPriority: 0,
            toPriority: Self.nowFloor
        ))

        let leasedRow = try #require(try await store.fetchJob(byWorkKey: "wk-leased"))
        #expect(leasedRow.priority == 0)
        #expect(leasedRow.schedulerLane == .background)
        #expect(leasedRow.leaseOwner == "worker-A")
        #expect(leasedRow.updatedAt == 1_000, "a refused promotion writes nothing at all")
        let freeRow = try #require(try await store.fetchJob(byWorkKey: "wk-free"))
        #expect(freeRow.priority == Self.nowFloor)
    }

    @Test("a RUNNING row is not promoted, while a queued twin in the same store is")
    func testRunningRowIsNotPromoted() async throws {
        let store = try await makeTestStore()
        // No lease owner: the ONLY thing refusing this promotion is
        // `state = 'queued'`, isolating that half of the guard.
        try await store.insertJob(makeJob(
            jobId: "job-running",
            episodeId: "ep-running",
            workKey: "wk-running",
            state: "running"
        ))
        try await store.insertJob(makeJob(
            jobId: "job-queued",
            episodeId: "ep-queued",
            workKey: "wk-queued"
        ))

        let refused = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-running",
            priority: Self.nowFloor,
            now: 2_000
        )
        let served = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-queued",
            priority: Self.nowFloor,
            now: 2_000
        )

        #expect(refused == .notPromotable(
            jobId: "job-running",
            state: "running",
            priority: 0,
            leased: false
        ))
        #expect(served == .promoted(
            jobId: "job-queued",
            fromPriority: 0,
            toPriority: Self.nowFloor
        ))
        let runningRow = try #require(try await store.fetchJob(byWorkKey: "wk-running"))
        #expect(runningRow.priority == 0)
        #expect(runningRow.updatedAt == 1_000)
    }

    @Test("a row leased through acquireLease — the production shape — is not promoted")
    func testProductionLeaseShapeIsNotPromoted() async throws {
        let store = try await makeTestStore()
        try await store.insertJob(makeJob(jobId: "job-p", workKey: "wk-p"))
        // The witness that the row WAS promotable a moment ago is the lease
        // itself: `acquireLease` only succeeds on an unleased row.
        let acquired = try await store.acquireLease(
            jobId: "job-p",
            owner: "worker-live",
            expiresAt: Date().timeIntervalSince1970 + 600
        )
        #expect(acquired, "precondition: the row was claimable")

        let outcome = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-p",
            priority: Self.nowFloor,
            now: 2_000
        )

        #expect(outcome == .notPromotable(
            jobId: "job-p",
            state: "running",
            priority: 0,
            leased: true
        ))
        let row = try #require(try await store.fetchJob(byWorkKey: "wk-p"))
        #expect(row.priority == 0)
        #expect(row.state == "running")
        #expect(row.leaseOwner == "worker-live")
    }

    @Test("a paused row is not promoted (one promotable state, deliberately)")
    func testPausedRowIsNotPromoted() async throws {
        let store = try await makeTestStore()
        try await store.insertJob(makeJob(state: "paused"))

        let outcome = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-1",
            priority: Self.nowFloor,
            now: 2_000
        )

        #expect(outcome == .notPromotable(
            jobId: "job-1",
            state: "paused",
            priority: 0,
            leased: false
        ))
        let row = try #require(try await store.fetchJob(byWorkKey: "wk-1"))
        #expect(row.priority == 0)
    }

    @Test("a complete row is not promoted")
    func testTerminalRowIsNotPromoted() async throws {
        let store = try await makeTestStore()
        try await store.insertJob(makeJob(state: "complete"))

        let outcome = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-1",
            priority: Self.nowFloor,
            now: 2_000
        )

        #expect(outcome == .notPromotable(
            jobId: "job-1",
            state: "complete",
            priority: 0,
            leased: false
        ))
    }

    // MARK: - Already served, and nothing at all

    @Test("a row already at the floor reports alreadyPromoted and is left untouched")
    func testAlreadyAtFloor() async throws {
        let store = try await makeTestStore()
        try await store.insertJob(makeJob(priority: Self.nowFloor))

        let outcome = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-1",
            priority: Self.nowFloor,
            now: 2_000
        )

        #expect(outcome == .alreadyPromoted(jobId: "job-1", priority: Self.nowFloor))
        let row = try #require(try await store.fetchJob(byWorkKey: "wk-1"))
        #expect(row.updatedAt == 1_000, "no write, so no updatedAt bump")
    }

    @Test("a RUNNING row already at the floor reports alreadyPromoted, not notPromotable")
    func testRunningRowAtFloorIsAlreadyServed() async throws {
        // The distinction the four-case outcome exists for: this row cannot be
        // written to, but the user's intent IS satisfied — it is `.now`-lane
        // work that is actually executing. Reporting it as a refusal would
        // strand the one-shot flag forever.
        let store = try await makeTestStore()
        try await store.insertJob(makeJob(
            priority: Self.nowFloor,
            state: "running",
            leaseOwner: "worker-A",
            leaseExpiresAt: 9_999
        ))

        let outcome = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-1",
            priority: Self.nowFloor,
            now: 2_000
        )

        #expect(outcome == .alreadyPromoted(jobId: "job-1", priority: Self.nowFloor))
    }

    @Test("an unoccupied work key reports noRow and creates nothing")
    func testNoRow() async throws {
        let store = try await makeTestStore()

        let outcome = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-absent",
            priority: Self.nowFloor,
            now: 2_000
        )

        #expect(outcome == .noRow)
        #expect(try await store.fetchJob(byWorkKey: "wk-absent") == nil)
        #expect(try await store.fetchJobsByState("queued").isEmpty)
    }

    @Test("promotion is keyed on workKey alone and moves no other row")
    func testPromotionIsScopedToTheKey() async throws {
        let store = try await makeTestStore()
        try await store.insertJob(makeJob(
            jobId: "job-target", episodeId: "ep-target", workKey: "wk-target"
        ))
        try await store.insertJob(makeJob(
            jobId: "job-other", episodeId: "ep-other", workKey: "wk-other"
        ))

        _ = try await store.promoteQueuedJobToUserIntentLane(
            workKey: "wk-target",
            priority: Self.nowFloor,
            now: 2_000
        )

        let target = try #require(try await store.fetchJob(byWorkKey: "wk-target"))
        let other = try #require(try await store.fetchJob(byWorkKey: "wk-other"))
        #expect(target.priority == Self.nowFloor)
        #expect(other.priority == 0)
        #expect(other.updatedAt == 1_000)
    }
}
