// ProductionBacklogScarcityRanking.swift
// playhead-dqfm — SwiftData-backed `BacklogScarcityRanking` for production.
//
// Supplies the reconciler's scarcity-aware re-prioritization pass with:
//   * the current one-window drain capacity, derived from the device's seed
//     grant-window profile + the pre-analysis depth config (the same inputs
//     the scheduler's slice sizing consults — no magic number), and
//   * the per-episode ranking signals, read from SwiftData on the MainActor.
//
// Wired via `PlayheadRuntime.attachBacklogScarcityRanking(modelContainer:)`
// once the `ModelContainer` exists — the same late-attach pattern as the
// runtime's other model-container-dependent providers. Until then the
// reconciler's provider is `nil` and the queue stays plain FIFO.

import Foundation
import SwiftData

struct ProductionBacklogScarcityRanking: BacklogScarcityRanking {
    let modelContainer: ModelContainer
    let capabilitiesService: any CapabilitiesProviding
    let config: PreAnalysisConfig

    func currentWindowDrainCapacity() async -> Int? {
        let snapshot = await capabilitiesService.currentSnapshot
        let profile = DeviceClassProfile.fallback(for: snapshot.deviceClass)
        return ScarcityReprioritizer.windowDrainCapacity(profile: profile, config: config)
    }

    func rankingSignals(forEpisodeIds ids: [String]) async -> [String: BacklogRankingSignals] {
        guard !ids.isEmpty else { return [:] }
        let idSet = Set(ids)
        let idArray = Array(idSet)
        return await MainActor.run {
            let context = modelContainer.mainContext

            // Tier 1 (+ "user-queued"): the playback queue — the episodes the
            // user will actually play next, in order.
            let queueEntries = (try? context.fetch(FetchDescriptor<QueueEntry>())) ?? []
            var queuePositionByKey: [String: Int] = [:]
            for entry in queueEntries where idSet.contains(entry.episodeKey) {
                if let existing = queuePositionByKey[entry.episodeKey] {
                    queuePositionByKey[entry.episodeKey] = min(existing, entry.position)
                } else {
                    queuePositionByKey[entry.episodeKey] = entry.position
                }
            }

            // Tier 2 basis: rank shows by how much the user has listened to
            // them (count of played episodes per show), most-listened first.
            // Only shows with at least one played episode get a rank; episodes
            // from never-played shows are not tier-2-eligible.
            let playedDescriptor = FetchDescriptor<Episode>(
                predicate: #Predicate { $0.isPlayed == true }
            )
            let playedEpisodes = (try? context.fetch(playedDescriptor)) ?? []
            var playedCountByShow: [String: Int] = [:]
            for episode in playedEpisodes {
                if let feed = episode.podcast?.feedURL.absoluteString {
                    playedCountByShow[feed, default: 0] += 1
                }
            }
            // Sort count DESC, feed ASC as a stable tiebreak so the rank
            // assignment is deterministic across runs.
            let rankedShows = playedCountByShow
                .sorted { lhs, rhs in
                    lhs.value != rhs.value ? lhs.value > rhs.value : lhs.key < rhs.key
                }
                .map(\.key)
            var showRankByFeed: [String: Int] = [:]
            for (index, feed) in rankedShows.enumerated() { showRankByFeed[feed] = index }

            // The backlog episodes themselves (tier-2 recency + tier-3 order).
            let backlogDescriptor = FetchDescriptor<Episode>(
                predicate: #Predicate { idArray.contains($0.canonicalEpisodeKey) }
            )
            let backlogEpisodes = (try? context.fetch(backlogDescriptor)) ?? []

            var result: [String: BacklogRankingSignals] = [:]
            result.reserveCapacity(backlogEpisodes.count)
            for episode in backlogEpisodes {
                let key = episode.canonicalEpisodeKey
                let showRank = (episode.podcast?.feedURL.absoluteString).flatMap { showRankByFeed[$0] }
                result[key] = BacklogRankingSignals(
                    queuePosition: queuePositionByKey[key],
                    showListenRank: showRank,
                    publishedAt: episode.publishedAt?.timeIntervalSince1970,
                    userQueuePosition: episode.queuePosition
                )
            }
            // Backlog episodes present only in the playback queue (Episode row
            // briefly absent during a feed refresh) still carry tier-1.
            for (key, position) in queuePositionByKey where result[key] == nil {
                result[key] = BacklogRankingSignals(queuePosition: position)
            }
            return result
        }
    }
}
