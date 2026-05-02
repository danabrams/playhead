// QueueViewModel.swift
// `@Observable @MainActor` projection of `PlaybackQueueService` rows
// joined to their matching `Episode` SwiftData rows. Drives `QueueView`.
//
// The view model is intentionally thin: it pulls the queue rows
// (Sendable DTOs) from the service, fetches the matching episodes
// from the SwiftData container in one go, and projects them into
// `QueueDisplayRow` values for the view to render.
//
// Tombstones: a queue entry whose `episodeKey` no longer resolves to a
// live Episode (e.g. the row was dropped on a feed refresh that's not
// yet repopulated) is silently dropped from the projection. The
// underlying queue row stays put — when the Episode reappears on the
// next refresh, the projection regains the row. We do NOT proactively
// `remove()` such tombstones because a transient absence (e.g. the
// feed-refresh delete-then-insert window) would otherwise erase the
// user's queue.

import Foundation
import Observation
import SwiftData

/// One row's worth of display data. Pure value type so SwiftUI can
/// diff cheaply.
struct QueueDisplayRow: Sendable, Hashable, Identifiable {
    let episodeKey: String
    let title: String
    let podcastTitle: String?
    let duration: TimeInterval?

    var id: String { episodeKey }
}

@MainActor
@Observable
final class QueueViewModel {

    private(set) var rows: [QueueDisplayRow] = []

    private let queueService: PlaybackQueueService
    private let modelContainer: ModelContainer

    init(
        queueService: PlaybackQueueService,
        modelContainer: ModelContainer
    ) {
        self.queueService = queueService
        self.modelContainer = modelContainer
    }

    /// Pull the latest queue ordering, join to episodes, publish.
    func refresh() async {
        let entries: [PlaybackQueueRow]
        do {
            entries = try await queueService.allEntries()
        } catch {
            entries = []
        }

        let episodeMap = fetchEpisodeMap(keys: entries.map(\.episodeKey))

        rows = entries.compactMap { entry in
            guard let episode = episodeMap[entry.episodeKey] else { return nil }
            return QueueDisplayRow(
                episodeKey: entry.episodeKey,
                title: episode.title,
                podcastTitle: episode.podcast?.title,
                duration: episode.duration
            )
        }
    }

    func remove(episodeKey: String) async throws {
        try await queueService.remove(episodeKey: episodeKey)
    }

    func clear() async throws {
        try await queueService.clear()
    }

    func move(fromOffsets source: IndexSet, toOffset destination: Int) async throws {
        try await queueService.move(fromOffsets: source, toOffset: destination)
    }

    // MARK: - Helpers

    private func fetchEpisodeMap(keys: [String]) -> [String: Episode] {
        guard !keys.isEmpty else { return [:] }
        let context = ModelContext(modelContainer)
        // skeptical-review-cycle-8 M3: `keys` MUST stay typed as
        // `[String]` (Array). SwiftData's `#Predicate` translator
        // handles `Array.contains` reliably across iOS / macOS Catalyst
        // builds, but `Set.contains` can fall back to a linear scan or
        // fail to translate at all on some toolchain combinations
        // (witnessed during cycle-7 H1 work in
        // `ActivitySnapshotProvider.swift:163`, fixed at
        // `PlayheadApp.swift:159-169`). Do not "tighten" this signature
        // to `Set<String>` — the Queue list silently empties if the
        // predicate stops matching.
        let descriptor = FetchDescriptor<Episode>(
            predicate: #Predicate { keys.contains($0.canonicalEpisodeKey) }
        )
        guard let episodes = try? context.fetch(descriptor) else { return [:] }
        var map: [String: Episode] = [:]
        for episode in episodes {
            map[episode.canonicalEpisodeKey] = episode
        }
        return map
    }
}
