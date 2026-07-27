// ScarcityReprioritizerTests.swift
// playhead-dqfm — scarcity-aware backfill queue ordering.
//
// Two layers:
//   * Pure `ScarcityReprioritizer` tests — the total ranking order, the
//     scarcity gate, capacity derivation, and determinism, with no store.
//   * Reconciler integration tests — proving that, against the REAL FIFO
//     selector (`AnalysisStore.fetchNextEligibleJob`), a scarce backlog
//     promotes the next-to-play episode ahead of an older-but-not-next
//     one (RED under plain FIFO / GREEN after), and that a non-scarce
//     backlog is left exactly as plain FIFO.

import Foundation
import Testing
@testable import Playhead

// MARK: - Stub provider

private final class StubBacklogScarcityRanking: BacklogScarcityRanking, @unchecked Sendable {
    let capacity: Int?
    let signals: [String: BacklogRankingSignals]

    init(capacity: Int?, signals: [String: BacklogRankingSignals]) {
        self.capacity = capacity
        self.signals = signals
    }

    func currentWindowDrainCapacity() async -> Int? { capacity }
    func rankingSignals(forEpisodeIds ids: [String]) async -> [String: BacklogRankingSignals] { signals }
}

// MARK: - Pure ranking / gate

@Suite("ScarcityReprioritizer — pure ranking + scarcity gate")
struct ScarcityReprioritizerPureTests {

    private func candidate(
        _ jobId: String,
        episodeId: String? = nil,
        priority: Int = 0,
        createdAt: Double = 0,
        signals: BacklogRankingSignals = BacklogRankingSignals()
    ) -> ScarcityReprioritizer.Candidate {
        ScarcityReprioritizer.Candidate(
            jobId: jobId,
            episodeId: episodeId ?? jobId,
            priority: priority,
            createdAt: createdAt,
            signals: signals
        )
    }

    @Test("Not scarce (backlog fits window) → no bumps, FIFO untouched")
    func notScarceIsNoOp() {
        let candidates = [
            candidate("a", createdAt: 1, signals: BacklogRankingSignals(queuePosition: 0)),
            candidate("b", createdAt: 2, signals: BacklogRankingSignals(queuePosition: 1)),
        ]
        // capacity 2, backlog 2 → not scarce.
        #expect(ScarcityReprioritizer.plan(candidates: candidates, drainCapacity: 2).isEmpty)
        // capacity 5 (> backlog) → not scarce.
        #expect(ScarcityReprioritizer.plan(candidates: candidates, drainCapacity: 5).isEmpty)
    }

    @Test("Scarce → next-to-play (later createdAt) promoted over older unranked")
    func scarcePromotesNextToPlay() {
        // Older (createdAt 1) has NO signal; newer (createdAt 2) is queued.
        // Plain FIFO would pick the older; promotion must flip it.
        let candidates = [
            candidate("older", createdAt: 1),
            candidate("next", createdAt: 2, signals: BacklogRankingSignals(queuePosition: 0)),
        ]
        let bumps = ScarcityReprioritizer.plan(candidates: candidates, drainCapacity: 1)
        #expect(bumps.count == 1)
        #expect(bumps.first?.jobId == "next")
        #expect((bumps.first?.newPriority ?? 0) > 0)          // out of the background band
        #expect((bumps.first?.newPriority ?? 99) < 10)         // still below explicit-download
    }

    @Test("Signal order: playback-queue beats most-listened beats user-queued")
    func tierOrderIsRespected() {
        let candidates = [
            candidate("t3", createdAt: 1, signals: BacklogRankingSignals(userQueuePosition: 0)),
            candidate("t2", createdAt: 1, signals: BacklogRankingSignals(showListenRank: 0, publishedAt: 100)),
            candidate("t1", createdAt: 1, signals: BacklogRankingSignals(queuePosition: 5)),
        ]
        // Capacity 3, backlog 3 is NOT scarce — add a 4th unranked so it IS.
        let all = candidates + [candidate("pad", createdAt: 0)]
        let bumps = ScarcityReprioritizer.plan(candidates: all, drainCapacity: 3)
        // Highest priority to tier 1, then tier 2, then tier 3.
        let byJob = Dictionary(uniqueKeysWithValues: bumps.map { ($0.jobId, $0.newPriority) })
        #expect(byJob["t1"] != nil && byJob["t2"] != nil && byJob["t3"] != nil)
        #expect(byJob["pad"] == nil) // unranked never promoted
        #expect((byJob["t1"] ?? 0) > (byJob["t2"] ?? 0))
        #expect((byJob["t2"] ?? 0) > (byJob["t3"] ?? 0))
    }

    @Test("Within most-listened tier: lower show rank then newer episode wins")
    func tier2OrdersByShowRankThenRecency() {
        let candidates = [
            candidate("show1-old", createdAt: 1, signals: BacklogRankingSignals(showListenRank: 0, publishedAt: 10)),
            candidate("show1-new", createdAt: 1, signals: BacklogRankingSignals(showListenRank: 0, publishedAt: 20)),
            candidate("show2", createdAt: 1, signals: BacklogRankingSignals(showListenRank: 1, publishedAt: 99)),
            candidate("pad-a", createdAt: 0),
            candidate("pad-b", createdAt: 0),
        ]
        let bumps = ScarcityReprioritizer.plan(candidates: candidates, drainCapacity: 3)
        let order = bumps.map(\.jobId)
        #expect(order == ["show1-new", "show1-old", "show2"])
    }

    @Test("Only background-lane rows are promoted; already-Soon left untouched")
    func alreadySoonNotTouched() {
        let candidates = [
            candidate("soon", priority: 10, createdAt: 1, signals: BacklogRankingSignals(queuePosition: 0)),
            candidate("bg", priority: 0, createdAt: 2, signals: BacklogRankingSignals(queuePosition: 1)),
            candidate("pad", priority: 0, createdAt: 3),
        ]
        let bumps = ScarcityReprioritizer.plan(candidates: candidates, drainCapacity: 1)
        #expect(bumps.map(\.jobId) == ["bg"]) // the already-Soon "soon" is never re-ranked
    }

    @Test("Deterministic total order: equal signals fall back to createdAt then jobId")
    func deterministicTieBreak() {
        // Two queued at the SAME position + same createdAt → jobId decides.
        let candidates = [
            candidate("b", createdAt: 5, signals: BacklogRankingSignals(queuePosition: 0)),
            candidate("a", createdAt: 5, signals: BacklogRankingSignals(queuePosition: 0)),
            candidate("pad", createdAt: 0),
        ]
        let first = ScarcityReprioritizer.plan(candidates: candidates, drainCapacity: 1)
        let second = ScarcityReprioritizer.plan(candidates: candidates.reversed(), drainCapacity: 1)
        #expect(first.map(\.jobId) == ["a"])           // jobId asc
        #expect(first.map(\.jobId) == second.map(\.jobId)) // input order does not matter
    }

    @Test("Unranked backlog is never promoted even when scarce")
    func unrankedNeverPromoted() {
        let candidates = [
            candidate("x", createdAt: 1),
            candidate("y", createdAt: 2),
            candidate("z", createdAt: 3),
        ]
        #expect(ScarcityReprioritizer.plan(candidates: candidates, drainCapacity: 1).isEmpty)
    }

    @Test("Window drain capacity derives from grant window + shard cost (no magic number)")
    func capacityDerivation() {
        let config = PreAnalysisConfig() // depth 90s, shard 20s → 5 shards/job
        // iPhone17: grantWindowMedianSeconds 40, avgShardDurationMs 2800.
        // perJob = 5 * 2.8 = 14s; floor(40 / 14) = 2.
        let cap = ScarcityReprioritizer.windowDrainCapacity(
            profile: DeviceClassProfile.fallback(for: .iPhone17),
            config: config
        )
        #expect(cap == 2)
        // Always at least one job's worth, for every device class.
        for bucket in DeviceClass.allCases {
            let c = ScarcityReprioritizer.windowDrainCapacity(
                profile: DeviceClassProfile.fallback(for: bucket),
                config: config
            )
            #expect(c >= 1)
        }
    }
}

// MARK: - Reconciler integration (against the real FIFO selector)

@Suite("AnalysisJobReconciler — scarcity re-prioritization (playhead-dqfm)")
struct ScarcityReprioritizerReconcilerTests {

    private func backgroundJob(_ jobId: String, episodeId: String, createdAt: Double) -> AnalysisJob {
        makeAnalysisJob(
            jobId: jobId,
            jobType: "preAnalysis",
            episodeId: episodeId,
            // Distinct fingerprint per job → distinct workKey. The store's
            // insert is INSERT-OR-IGNORE on the unique workKey, so a shared
            // fingerprint would silently drop the second insert.
            sourceFingerprint: "fp-\(episodeId)",
            priority: 0,            // Background lane
            state: "queued",
            createdAt: createdAt
        )
    }

    private func reconciler(
        store: AnalysisStore,
        ranking: (any BacklogScarcityRanking)?
    ) -> AnalysisJobReconciler {
        AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider(),
            backlogScarcityRanking: ranking
        )
    }

    /// The FIFO selector the drain uses. Background pre-analysis jobs are
    /// eligible when deferred work is allowed.
    private func selected(_ store: AnalysisStore) async throws -> AnalysisJob? {
        try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true,
            t0ThresholdSec: 0,
            now: 10_000
        )
    }

    @Test("Scarce → next-to-play promoted ahead of older FIFO item (RED→GREEN)")
    func scarcePromotesNextToPlayInStore() async throws {
        let store = try await makeTestStore()
        // Older item is NOT next-to-play; newer item IS (queued). Under plain
        // FIFO the older (earlier createdAt) wins — this is the RED baseline.
        try await store.insertJob(backgroundJob("job-old", episodeId: "ep-old", createdAt: 1_000))
        try await store.insertJob(backgroundJob("job-next", episodeId: "ep-next", createdAt: 2_000))

        // capacity 1 < backlog 2 → scarce. Only ep-next carries a signal.
        let ranking = StubBacklogScarcityRanking(
            capacity: 1,
            signals: ["ep-next": BacklogRankingSignals(queuePosition: 0)]
        )
        let report = try await reconciler(store: store, ranking: ranking).reconcile()

        #expect(report.scarcityReprioritizedJobs == 1)
        let next = try await store.fetchJob(byId: "job-next")
        let old = try await store.fetchJob(byId: "job-old")
        #expect((next?.priority ?? 0) > 0)   // promoted into Soon
        #expect(old?.priority == 0)          // older untouched
        // The real FIFO selector now returns the next-to-play item (GREEN).
        #expect(try await selected(store)?.jobId == "job-next")
    }

    @Test("Not scarce → ordering unchanged from plain FIFO")
    func notScarceLeavesFIFOUnchanged() async throws {
        let store = try await makeTestStore()
        try await store.insertJob(backgroundJob("job-old", episodeId: "ep-old", createdAt: 1_000))
        try await store.insertJob(backgroundJob("job-next", episodeId: "ep-next", createdAt: 2_000))

        // capacity 5 >= backlog 2 → NOT scarce, even though ep-next is queued.
        let ranking = StubBacklogScarcityRanking(
            capacity: 5,
            signals: ["ep-next": BacklogRankingSignals(queuePosition: 0)]
        )
        let report = try await reconciler(store: store, ranking: ranking).reconcile()

        #expect(report.scarcityReprioritizedJobs == 0)
        #expect(try await store.fetchJob(byId: "job-next")?.priority == 0)
        #expect(try await store.fetchJob(byId: "job-old")?.priority == 0)
        // Plain FIFO: the older item is still selected.
        #expect(try await selected(store)?.jobId == "job-old")
    }

    @Test("No ranking provider wired → plain FIFO preserved")
    func noProviderPreservesFIFO() async throws {
        let store = try await makeTestStore()
        try await store.insertJob(backgroundJob("job-old", episodeId: "ep-old", createdAt: 1_000))
        try await store.insertJob(backgroundJob("job-next", episodeId: "ep-next", createdAt: 2_000))

        let report = try await reconciler(store: store, ranking: nil).reconcile()

        #expect(report.scarcityReprioritizedJobs == 0)
        #expect(try await store.fetchJob(byId: "job-next")?.priority == 0)
        #expect(try await selected(store)?.jobId == "job-old")
    }
}
