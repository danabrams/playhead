// ScarcityReprioritizer.swift
// playhead-dqfm — scarcity-aware backfill queue ordering.
//
// Within-lane backfill selection is plain FIFO
// (`AnalysisStore.fetchNextEligibleJob`: `ORDER BY priority DESC,
// createdAt ASC`). When the queued **background-lane** backlog exceeds
// what ONE background analysis window can drain, that FIFO can spend a
// scarce grant on episodes the user won't play next, leaving the episode
// they DO press play on tomorrow un-analyzed.
//
// This file adds the pure, deterministic ranking + promotion logic. The
// reconciler (`AnalysisJobReconciler`) runs it as a re-prioritization
// pass at window entry (launch + every BGProcessingTask handler
// invocation): when the backlog is scarce it BUMPS next-to-play episodes
// out of the Background band (`priority <= 0`) and into a low sub-range
// of the Soon band so the existing `ORDER BY priority DESC` selection
// covers what the user will actually play. When NOT scarce it is a pure
// no-op and the queue stays plain FIFO.
//
// Nothing here reads SwiftData or the device profile directly — those are
// supplied through `BacklogScarcityRanking` so the ranking + gate + bump
// logic is unit-testable without a `ModelContainer`.

import Foundation

// MARK: - Ranking signals

/// The per-episode signals the reprioritizer ranks a scarce backlog by.
/// Every field is optional; an episode with no signal at all is never
/// promoted (it stays in the plain-FIFO background band).
///
/// Signal order is the bead-sanctioned default (playhead-dqfm):
///   1. `queuePosition`     — playback-queue position (next-to-play). Lower = sooner.
///   2. `showListenRank` + `publishedAt` — most-recent episode of the
///      most-listened shows. Lower rank = more-listened; newer `publishedAt` wins ties.
///   3. `userQueuePosition` — user-curated "Up Next" ordering. Lower = sooner.
struct BacklogRankingSignals: Sendable, Equatable {
    var queuePosition: Int?
    var showListenRank: Int?
    var publishedAt: Double?
    var userQueuePosition: Int?

    init(
        queuePosition: Int? = nil,
        showListenRank: Int? = nil,
        publishedAt: Double? = nil,
        userQueuePosition: Int? = nil
    ) {
        self.queuePosition = queuePosition
        self.showListenRank = showListenRank
        self.publishedAt = publishedAt
        self.userQueuePosition = userQueuePosition
    }

    /// True when the episode belongs to at least one ranking tier and is
    /// therefore promotable. `publishedAt` is deliberately excluded: on its
    /// own it is NOT a next-to-play signal (nearly every episode has a
    /// publish date) — it only breaks recency ties WITHIN the most-listened
    /// shows tier. An episode with only a publish date stays in plain-FIFO
    /// age order.
    var isRanked: Bool {
        queuePosition != nil || showListenRank != nil || userQueuePosition != nil
    }
}

// MARK: - Environment provider

/// Supplies the two environment inputs the reconciler cannot compute
/// itself: the current one-window drain capacity (device-profile derived)
/// and the ranking signals for the backlog episodes (SwiftData derived).
///
/// Injected into `AnalysisJobReconciler`. `nil` (the default) makes the
/// re-prioritization step a complete no-op, so the queue stays plain FIFO
/// and every existing behavior/test is byte-unchanged. Production wires a
/// SwiftData-backed adapter (`ProductionBacklogScarcityRanking`).
protocol BacklogScarcityRanking: Sendable {
    /// Estimated number of background-lane pre-analysis jobs one
    /// background analysis window can drain, derived from the device
    /// grant-window profile. When the queued background backlog exceeds
    /// this the window is "scarce". Return `nil` when capacity is unknown
    /// — the reconciler then skips re-prioritization (FIFO preserved).
    func currentWindowDrainCapacity() async -> Int?

    /// Ranking signals for the given backlog episodes, keyed by
    /// `episodeId` (== `Episode.canonicalEpisodeKey`). Episodes absent
    /// from the result (or with an all-`nil` entry) are never promoted.
    func rankingSignals(forEpisodeIds ids: [String]) async -> [String: BacklogRankingSignals]
}

// MARK: - Pure ranking + promotion

/// Pure, deterministic ranking + promotion logic. No I/O, no clock, no
/// SwiftData — every input is passed in so the total order and the
/// scarcity gate are exhaustively unit-testable.
enum ScarcityReprioritizer {

    /// Promoted jobs land in `[1, soonPromotionCeiling]` — a low sub-range
    /// of the Soon band (`priority 1..<20`, see `AnalysisJob.schedulerLane`).
    /// Deliberately kept strictly BELOW explicit-download (priority 10) and
    /// Now (priority 20), so promoting a next-to-play auto-download does not
    /// leapfrog a job the user explicitly asked for; and strictly ABOVE the
    /// Background band (`<= 0`) so the scarce window's `ORDER BY priority
    /// DESC` selection covers the promoted episodes first.
    static let soonPromotionCeiling = 9

    /// One background job's coverage target (`defaultT0DepthSeconds`) sliced
    /// into shards of `nominalShardDurationSec`, each costing
    /// `avgShardDurationMs` of wall-clock, must fit inside the device's
    /// median grant window. This reuses the very same seed table + depth
    /// config the scheduler's slice sizing consults — it invents no magic
    /// number. Result is clamped to >= 1 (a window always drains at least
    /// one job's worth before expiring).
    static func windowDrainCapacity(
        profile: DeviceClassProfile,
        config: PreAnalysisConfig
    ) -> Int {
        let depth = max(1, config.defaultT0DepthSeconds)
        let shardSeconds = max(1, config.nominalShardDurationSec)
        let shardsPerJob = max(1, Int((depth / shardSeconds).rounded(.up)))
        let perJobSeconds = Double(shardsPerJob) * (Double(profile.avgShardDurationMs) / 1000.0)
        guard perJobSeconds > 0 else { return 1 }
        let capacity = Double(profile.grantWindowMedianSeconds) / perJobSeconds
        guard capacity.isFinite else { return 1 }
        return max(1, Int(capacity.rounded(.down)))
    }

    /// One backlog row plus its ranking signals.
    struct Candidate: Sendable, Equatable {
        let jobId: String
        let episodeId: String
        let priority: Int
        let createdAt: Double
        let signals: BacklogRankingSignals
    }

    /// A single priority mutation to apply.
    struct Bump: Sendable, Equatable {
        let jobId: String
        let newPriority: Int
    }

    /// Compute the priority bumps for a (possibly scarce) backlog.
    ///
    /// - When `candidates.count <= drainCapacity` the window can drain the
    ///   whole backlog: returns `[]` (NO perturbation — plain FIFO).
    /// - When scarce, ranks the promotable candidates by the bead's signal
    ///   order (below), takes the top `drainCapacity` — the ones the window
    ///   can actually cover — and assigns them a descending Soon sub-range
    ///   priority so selection order matches rank order.
    ///
    /// Only Background-band rows (`priority <= 0`) that carry at least one
    /// signal are promotable — a row already in Soon/Now was placed there
    /// deliberately and is left untouched, and an unranked backlog row keeps
    /// its plain-FIFO age order. A candidate already at its target priority
    /// yields no bump (idempotent across repeated scarce windows).
    ///
    /// The ordering is TOTAL and STABLE: after the tiered signals it falls
    /// back to `createdAt` ascending (the store's own FIFO tiebreak) then
    /// `jobId` ascending, so there is never a flaky tie.
    static func plan(candidates: [Candidate], drainCapacity: Int) -> [Bump] {
        guard drainCapacity >= 0, candidates.count > drainCapacity else { return [] }

        let promotable = candidates.filter { $0.priority <= 0 && $0.signals.isRanked }
        guard !promotable.isEmpty else { return [] }

        let ordered = promotable.sorted(by: rankLess)
        let count = min(ordered.count, drainCapacity)
        guard count > 0 else { return [] }

        var bumps: [Bump] = []
        bumps.reserveCapacity(count)
        for (index, candidate) in ordered.prefix(count).enumerated() {
            let newPriority = max(1, soonPromotionCeiling - index)
            if newPriority != candidate.priority {
                bumps.append(Bump(jobId: candidate.jobId, newPriority: newPriority))
            }
        }
        return bumps
    }

    // MARK: - Total order

    /// Strict-weak `<` over the bead's default signal order. Tier 1 beats
    /// tier 2 beats tier 3; within a tier the tier's own key orders; the
    /// final tiebreak (`createdAt` then `jobId`) makes the order total and
    /// deterministic.
    private static func rankLess(_ a: Candidate, _ b: Candidate) -> Bool {
        let ta = tier(a.signals)
        let tb = tier(b.signals)
        if ta != tb { return ta < tb }

        switch ta {
        case 1:
            // Playback-queue position — lower is sooner.
            let pa = a.signals.queuePosition ?? Int.max
            let pb = b.signals.queuePosition ?? Int.max
            if pa != pb { return pa < pb }
        case 2:
            // Most-listened show first, then most-recent episode.
            let ra = a.signals.showListenRank ?? Int.max
            let rb = b.signals.showListenRank ?? Int.max
            if ra != rb { return ra < rb }
            let da = a.signals.publishedAt ?? -.greatestFiniteMagnitude
            let db = b.signals.publishedAt ?? -.greatestFiniteMagnitude
            if da != db { return da > db } // newer wins
        case 3:
            // User-curated "Up Next" — lower is sooner.
            let qa = a.signals.userQueuePosition ?? Int.max
            let qb = b.signals.userQueuePosition ?? Int.max
            if qa != qb { return qa < qb }
        default:
            break
        }

        // Total, stable tiebreak: oldest-first (matches the store's FIFO
        // `createdAt ASC`), then jobId for full determinism.
        if a.createdAt != b.createdAt { return a.createdAt < b.createdAt }
        return a.jobId < b.jobId
    }

    /// Ranking tier for a signal set: 1 = playback queue, 2 = most-listened
    /// shows (recency-tiebroken), 3 = user-curated Up Next, 4 = none (never
    /// reached — callers filter on `isRanked`). `publishedAt` never triggers
    /// a tier on its own; it only orders episodes within tier 2.
    private static func tier(_ s: BacklogRankingSignals) -> Int {
        if s.queuePosition != nil { return 1 }
        if s.showListenRank != nil { return 2 }
        if s.userQueuePosition != nil { return 3 }
        return 4
    }
}
