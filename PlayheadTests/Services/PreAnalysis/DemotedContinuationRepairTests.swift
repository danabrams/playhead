// DemotedContinuationRepairTests.swift
// playhead-w8db: the rows already on disk.
//
// The two mint corrections in this bead (the tier successor in
// `AnalysisWorkScheduler`, the cap-out retry in `AnalysisJobReconciler`) fix
// what gets WRITTEN from now on. They cannot fix what is already written:
// `insertJob` is `INSERT OR IGNORE` on the UNIQUE `workKey`, so no re-enqueue
// can rewrite a demoted row, and the lane is a stored column that
// `fetchNextEligibleJob` reads directly rather than deriving at dispatch time.
//
// THE MEASUREMENT (`scratchpad/db-pull11/analysis.sqlite`, pulled 2026-08-15).
// Fifteen priority-0 rows, and ALL FIFTEEN descend from a first generation at
// priority 10 or 20 — there is not one genuine background backfill in the
// table. Seven were still queued, four of them tier successors carrying
// 92.2 s, 168.7 s, 190.8 s and 215.9 s of untranscribed tail, at waits of
// 22.3, 15.0, 18.3 and 22.3 hours against first generations at 20, 10, 20 and
// 20.
//
// And the mechanism is not queue order. Every `lastRejectReason` beginning
// `laneGate:` on that device — `laneGate:fair` x4 and `laneGate:serious` x1 —
// is on a priority-0 row, and none is on a 10 or a 20, because
// `LaneAdmission.allows(lane:)` opens `.background` only at `.nominal`. The
// demotion put the tail of an episode behind a thermal gate its own head never
// had to pass. playhead-by07 measures that tail at roughly five times the
// body's ad density.

import Foundation
import Testing
@testable import Playhead

@Suite("Demoted continuations are repaired to their predecessor's lane (playhead-w8db)")
struct DemotedContinuationRepairTests {

    private static let fingerprint = "fp-w8db"
    private static let episodeId = "ep-w8db"

    private static var baseKey: String {
        AnalysisJob.computeWorkKey(
            fingerprint: fingerprint,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            jobType: "preAnalysis"
        )
    }

    /// A reconciler with nothing else wired: no cached downloads (so step 7
    /// discovers nothing), no scarcity ranking (so dqfm is a pure no-op). What
    /// this pass does to the seeded rows is therefore attributable to the
    /// repair step alone.
    private func reconciler(_ store: AnalysisStore) -> AnalysisJobReconciler {
        AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider(),
            config: PreAnalysisConfig()
        )
    }

    /// The first generation — the row that carries what the user asked for.
    private func seedBase(_ store: AnalysisStore, priority: Int, state: String = "complete") async throws {
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-base",
            jobType: "preAnalysis",
            episodeId: Self.episodeId,
            workKey: Self.baseKey,
            sourceFingerprint: Self.fingerprint,
            priority: priority,
            desiredCoverageSec: 90,
            state: state
        ))
    }

    private func seedContinuation(
        _ store: AnalysisStore,
        jobId: String,
        keySuffix: String,
        priority: Int = 0,
        state: String = "queued",
        leaseOwner: String? = nil,
        leaseExpiresAt: Double? = nil
    ) async throws {
        try await store.insertJob(makeAnalysisJob(
            jobId: jobId,
            jobType: "preAnalysis",
            episodeId: Self.episodeId,
            workKey: "\(Self.baseKey):\(keySuffix)",
            sourceFingerprint: Self.fingerprint,
            priority: priority,
            desiredCoverageSec: 1_930.8,
            state: state,
            leaseOwner: leaseOwner,
            leaseExpiresAt: leaseExpiresAt
        ))
    }

    // MARK: - The repair

    /// The device's shape, reduced: a user-lane first generation and the tier
    /// successor that carries its tail, demoted.
    @Test("a queued tier successor below its own first generation is raised to it")
    func demotedTierSuccessorIsRaised() async throws {
        let store = try await makeTestStore()
        try await seedBase(store, priority: 20)
        try await seedContinuation(store, jobId: "job-tail", keySuffix: "1930")

        let report = try await reconciler(store).reconcile()

        #expect(report.demotedContinuationsRepaired == 1)
        let tail = try await store.fetchJob(byId: "job-tail")
        #expect(tail?.priority == 20)
        #expect(tail?.schedulerLane == .now)
        // The predecessor is not touched: the repair reads it, it does not
        // write it.
        #expect(try await store.fetchJob(byId: "job-base")?.priority == 20)
    }

    /// The same for an explicit download, whose lane is `.soon` rather than
    /// `.now` — so the repair is proved to copy a VALUE rather than to promote
    /// everything it touches to one floor.
    @Test("an explicit download's continuation is raised to 10, not to 20")
    func explicitDownloadContinuationIsRaisedToTen() async throws {
        let store = try await makeTestStore()
        try await seedBase(store, priority: 10)
        try await seedContinuation(store, jobId: "job-tail", keySuffix: "1930")

        #expect(try await reconciler(store).reconcile().demotedContinuationsRepaired == 1)
        let tail = try await store.fetchJob(byId: "job-tail")
        #expect(tail?.priority == 10)
        #expect(tail?.schedulerLane == .soon)
    }

    /// A cap-out retry is a continuation too, and its key is the other shape
    /// the parser has to recognise.
    @Test("a queued cap-out retry is raised to the generation it retries")
    func demotedCapOutRetryIsRaised() async throws {
        let store = try await makeTestStore()
        try await seedBase(store, priority: 20, state: "superseded")
        try await seedContinuation(store, jobId: "job-retry", keySuffix: "capRetry:1")

        #expect(try await reconciler(store).reconcile().demotedContinuationsRepaired == 1)
        #expect(try await store.fetchJob(byId: "job-retry")?.priority == 20)
    }

    /// Every rung of a ladder, not just the shallowest — the deepest rung is
    /// the one the bead is about.
    @Test("every queued rung of one ladder is repaired, in one pass")
    func everyRungOfTheLadderIsRepaired() async throws {
        let store = try await makeTestStore()
        try await seedBase(store, priority: 20)
        try await seedContinuation(store, jobId: "job-t1", keySuffix: "300")
        try await seedContinuation(store, jobId: "job-t2", keySuffix: "900")
        try await seedContinuation(store, jobId: "job-dur", keySuffix: "1930")

        #expect(try await reconciler(store).reconcile().demotedContinuationsRepaired == 3)
        for id in ["job-t1", "job-t2", "job-dur"] {
            #expect(try await store.fetchJob(byId: id)?.priority == 20, "\(id)")
        }
    }

    // MARK: - What the repair must NOT do

    /// THE CONTROL THAT DECIDES WHETHER THIS IS A CORRECTION OR A POLICY
    /// CHANGE. An ad-scan re-drive's `0` is deliberate and argued at both of
    /// its mint sites — "repair work never preempts what the user is waiting
    /// on" — and its work key descends from the same base as a tier
    /// successor's. Promoting it would be a scheduling decision wearing a
    /// correction's clothes, and it is Dan's to make, not this bead's.
    ///
    /// This is the test a naive "raise anything that descends from a 10/20
    /// row" implementation fails. On db-pull11 three of the seven queued
    /// priority-0 rows are re-drives, so it is the majority of the population,
    /// not an edge case.
    @Test("an ad-scan re-drive is never raised, however high its base sits")
    func adScanRedriveIsNeverRaised() async throws {
        let store = try await makeTestStore()
        try await seedBase(store, priority: 20)
        try await seedContinuation(
            store,
            jobId: "job-redrive",
            keySuffix: "\(AnalysisWorkScheduler.adScanRedriveWorkKeyMarker):1"
        )

        #expect(try await reconciler(store).reconcile().demotedContinuationsRepaired == 0)
        #expect(try await store.fetchJob(byId: "job-redrive")?.priority == 0)
    }

    /// The vacuity control for the whole step: the value is COPIED from the
    /// predecessor, never chosen. An auto-download's ladder is genuine
    /// background work and must stay in the background lane.
    @Test("a continuation whose base is already 0 is left alone")
    func backgroundLadderIsLeftAlone() async throws {
        let store = try await makeTestStore()
        try await seedBase(store, priority: 0)
        try await seedContinuation(store, jobId: "job-tail", keySuffix: "1930")

        #expect(try await reconciler(store).reconcile().demotedContinuationsRepaired == 0)
        #expect(try await store.fetchJob(byId: "job-tail")?.priority == 0)
    }

    /// The write is a RAISE. A continuation already above its base — one dqfm
    /// bumped into the Soon band, or one a `kanf` tap promoted — must not be
    /// dragged back down.
    @Test("a continuation above its base is never lowered")
    func continuationAboveItsBaseIsNotLowered() async throws {
        let store = try await makeTestStore()
        try await seedBase(store, priority: 0)
        try await seedContinuation(store, jobId: "job-tail", keySuffix: "1930", priority: 3)

        #expect(try await reconciler(store).reconcile().demotedContinuationsRepaired == 0)
        #expect(try await store.fetchJob(byId: "job-tail")?.priority == 3)
    }

    /// Re-ranking work a worker holds is a lease/epoch decision made elsewhere
    /// — the same refusal `promoteQueuedJobToUserIntentLane` makes, for the
    /// same reason. `queued` with a lease is a state the schema tolerates
    /// (`fetchNextEligibleJob` selects `leaseOwner IS NULL OR leaseExpiresAt <
    /// now`), so the SQL `state = 'queued'` guard alone would not catch it.
    @Test("a leased row is not re-ranked, even though it is still 'queued'")
    func leasedRowIsNotReRanked() async throws {
        let store = try await makeTestStore()
        try await seedBase(store, priority: 20)
        try await seedContinuation(
            store,
            jobId: "job-tail",
            keySuffix: "1930",
            leaseOwner: "worker-1",
            // A LIVE lease, not an expired one — `recoverExpiredLeases` (step 1)
            // would otherwise clear it before this step ever sees the row, and
            // the test would pass for the wrong reason.
            leaseExpiresAt: Date().timeIntervalSince1970 + 3_600
        )

        #expect(try await reconciler(store).reconcile().demotedContinuationsRepaired == 0)
        #expect(try await store.fetchJob(byId: "job-tail")?.priority == 0)
    }

    /// A continuation whose base the 7-day GC has removed has no evidence of
    /// what was asked for. Declining is the only honest answer; guessing "20"
    /// would invent user intent from an absence.
    @Test("a continuation with no surviving base row is left alone")
    func orphanedContinuationIsLeftAlone() async throws {
        let store = try await makeTestStore()
        try await seedContinuation(store, jobId: "job-tail", keySuffix: "1930")

        #expect(try await reconciler(store).reconcile().demotedContinuationsRepaired == 0)
        #expect(try await store.fetchJob(byId: "job-tail")?.priority == 0)
    }

    /// The base row itself continues nothing. A step that treated it as its own
    /// predecessor would report a repair on every pass forever.
    @Test("the first generation is not a continuation of itself")
    func baseRowIsNotItsOwnContinuation() async throws {
        let store = try await makeTestStore()
        try await seedBase(store, priority: 0, state: "queued")

        #expect(try await reconciler(store).reconcile().demotedContinuationsRepaired == 0)
    }

    /// Self-draining: the repaired row equals its base on the next pass, so the
    /// steady state is zero. A counter that kept firing would mean the write
    /// was not landing, and every background window would pay for it.
    @Test("the repair is idempotent — the second pass finds nothing")
    func repairIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await seedBase(store, priority: 20)
        try await seedContinuation(store, jobId: "job-tail", keySuffix: "1930")

        let recon = reconciler(store)
        #expect(try await recon.reconcile().demotedContinuationsRepaired == 1)
        for _ in 0..<3 {
            #expect(try await recon.reconcile().demotedContinuationsRepaired == 0)
        }
        #expect(try await store.fetchJob(byId: "job-tail")?.priority == 20)
    }

    /// The repair moves a row into a lane; the point of the lane is that
    /// something downstream reads it. Asserted against the REAL selector rather
    /// than against the column, because a fix that wrote the number without
    /// changing what dispatches next would pass every test above.
    @Test("the repaired tail is what the real selector picks next")
    func repairedTailIsSelectedFirst() async throws {
        let store = try await makeTestStore()
        try await seedBase(store, priority: 20)
        try await seedContinuation(store, jobId: "job-tail", keySuffix: "1930")
        // An unrelated background job, created EARLIER, so plain FIFO inside one
        // lane would return it.
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-backfill",
            jobType: "preAnalysis",
            episodeId: "ep-other",
            sourceFingerprint: "fp-other",
            priority: 0,
            state: "queued",
            createdAt: 1
        ))

        // RED baseline: before the repair the tail is background work behind an
        // older background row...
        #expect(try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true, t0ThresholdSec: 0, now: 10_000
        )?.jobId == "job-backfill")
        // ...and with deferred work CLOSED — the `.fair` / `.serious` device
        // that produced every `laneGate:*` reject on db-pull11 — it is not
        // reachable AT ALL. This is the half that is not about ordering.
        #expect(try await store.fetchNextEligibleJob(
            deferredWorkAllowed: false,
            nowLanePriorityFloor: AnalysisWorkScheduler.nowLanePriorityFloor,
            t0ThresholdSec: 0,
            now: 10_000
        ) == nil)

        #expect(try await reconciler(store).reconcile().demotedContinuationsRepaired == 1)

        #expect(try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true, t0ThresholdSec: 0, now: 10_000
        )?.jobId == "job-tail")
        #expect(try await store.fetchNextEligibleJob(
            deferredWorkAllowed: false,
            nowLanePriorityFloor: AnalysisWorkScheduler.nowLanePriorityFloor,
            t0ThresholdSec: 0,
            now: 10_000
        )?.jobId == "job-tail")
    }

    // MARK: - The parser, directly

    @Test("continuationBaseWorkKey recognises exactly the continuation shapes")
    func continuationParserShapes() {
        func job(_ workKey: String, jobType: String = "preAnalysis") -> AnalysisJob {
            makeAnalysisJob(
                jobType: jobType,
                workKey: workKey,
                sourceFingerprint: Self.fingerprint
            )
        }
        let base = Self.baseKey

        // Tier successor and cap-out retry are continuations.
        #expect(AnalysisWorkScheduler.continuationBaseWorkKey(for: job("\(base):300")) == base)
        #expect(AnalysisWorkScheduler.continuationBaseWorkKey(for: job("\(base):1930")) == base)
        #expect(AnalysisWorkScheduler.continuationBaseWorkKey(for: job("\(base):capRetry:1")) == base)
        #expect(AnalysisWorkScheduler.continuationBaseWorkKey(for: job("\(base):capRetry:2")) == base)

        // An ad-scan re-drive is not, and its key is a `base:` prefix too — so
        // this is the ordering that matters inside the function, not a
        // coincidence of shape.
        let marker = AnalysisWorkScheduler.adScanRedriveWorkKeyMarker
        #expect(AnalysisWorkScheduler.continuationBaseWorkKey(for: job("\(base):\(marker):1")) == nil)
        #expect(AnalysisWorkScheduler.continuationBaseWorkKey(for: job("\(base):\(marker):2")) == nil)

        // The base row continues nothing.
        #expect(AnalysisWorkScheduler.continuationBaseWorkKey(for: job(base)) == nil)

        // A row at another analysis version does not descend from the key this
        // function builds, which is always at the CURRENT version. Reading a
        // predecessor off it would be a lookup against an unrelated row.
        let otherVersion = AnalysisJob.computeWorkKey(
            fingerprint: Self.fingerprint,
            analysisVersion: PreAnalysisConfig.analysisVersion + 1,
            jobType: "preAnalysis"
        )
        #expect(AnalysisWorkScheduler.continuationBaseWorkKey(for: job("\(otherVersion):300")) == nil)

        // Only the pre-analysis lane has generations at all.
        #expect(AnalysisWorkScheduler.continuationBaseWorkKey(
            for: job("\(Self.fingerprint):\(PreAnalysisConfig.analysisVersion):playback:300",
                     jobType: "playback")
        ) == nil)
    }
}
