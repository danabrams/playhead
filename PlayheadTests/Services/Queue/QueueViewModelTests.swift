// QueueViewModelTests.swift
// Verifies the projection layer that turns persisted `QueueEntry` rows
// plus the matching `Episode` SwiftData rows into a list of display
// rows the UI renders. Decoupled from any SwiftUI view so it can be
// asserted with Swift Testing alone.

import Foundation
import SwiftData
import Testing
@testable import Playhead

@MainActor
@Suite("QueueViewModel — episode projection")
struct QueueViewModelTests {

    private func makeContainer() throws -> ModelContainer {
        let schema = Schema([
            Podcast.self,
            Episode.self,
            QueueEntry.self,
        ])
        let config = ModelConfiguration(
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        return try ModelContainer(for: schema, configurations: [config])
    }

    private func seedPodcastAndEpisodes(
        in context: ModelContext,
        episodes: [(guid: String, title: String, duration: TimeInterval?)]
    ) throws -> [String] {
        let feedURL = URL(string: "https://example.com/feed.xml")!
        let podcast = Podcast(
            feedURL: feedURL,
            title: "Sample Podcast",
            author: "Author Name"
        )
        context.insert(podcast)
        var keys: [String] = []
        for ep in episodes {
            let episode = Episode(
                feedItemGUID: ep.guid,
                feedURL: feedURL,
                podcast: podcast,
                title: ep.title,
                audioURL: URL(string: "https://example.com/\(ep.guid).mp3")!,
                duration: ep.duration
            )
            context.insert(episode)
            keys.append(episode.canonicalEpisodeKey)
        }
        try context.save()
        return keys
    }

    @Test("empty queue projects to no rows")
    func emptyProjection() async throws {
        let container = try makeContainer()
        let queueService = PlaybackQueueService(modelContainer: container)
        let viewModel = QueueViewModel(
            queueService: queueService,
            modelContainer: container
        )
        await viewModel.refresh()
        #expect(viewModel.rows.isEmpty)
    }

    @Test("queue projects rows joined to episode metadata")
    func projectsEpisodeMetadata() async throws {
        let container = try makeContainer()
        let context = ModelContext(container)
        let keys = try seedPodcastAndEpisodes(
            in: context,
            episodes: [
                (guid: "g1", title: "Episode One", duration: 1800),
                (guid: "g2", title: "Episode Two", duration: 3600),
            ]
        )

        let queueService = PlaybackQueueService(modelContainer: container)
        try await queueService.addLast(episodeKey: keys[0])
        try await queueService.addLast(episodeKey: keys[1])

        let viewModel = QueueViewModel(
            queueService: queueService,
            modelContainer: container
        )
        await viewModel.refresh()

        #expect(viewModel.rows.count == 2)
        #expect(viewModel.rows.map(\.title) == ["Episode One", "Episode Two"])
        #expect(viewModel.rows.map(\.podcastTitle) == ["Sample Podcast", "Sample Podcast"])
        #expect(viewModel.rows.map(\.duration) == [1800, 3600])
    }

    @Test("queue projection omits rows whose episodeKey no longer resolves to an Episode")
    func tombstonesAreDropped() async throws {
        let container = try makeContainer()
        let context = ModelContext(container)
        let keys = try seedPodcastAndEpisodes(
            in: context,
            episodes: [(guid: "g1", title: "Resolves", duration: 100)]
        )

        let queueService = PlaybackQueueService(modelContainer: container)
        // One key that resolves, one that does not.
        try await queueService.addLast(episodeKey: keys[0])
        try await queueService.addLast(episodeKey: "does-not-exist")

        let viewModel = QueueViewModel(
            queueService: queueService,
            modelContainer: container
        )
        await viewModel.refresh()

        // Only the resolved row appears in the projection.
        #expect(viewModel.rows.count == 1)
        #expect(viewModel.rows.map(\.title) == ["Resolves"])
    }

    @Test("remove() on the view-model removes the row from queue and projection")
    func removeRow() async throws {
        let container = try makeContainer()
        let context = ModelContext(container)
        let keys = try seedPodcastAndEpisodes(
            in: context,
            episodes: [
                (guid: "g1", title: "Keep", duration: 100),
                (guid: "g2", title: "Drop", duration: 100),
            ]
        )

        let queueService = PlaybackQueueService(modelContainer: container)
        try await queueService.addLast(episodeKey: keys[0])
        try await queueService.addLast(episodeKey: keys[1])

        let viewModel = QueueViewModel(
            queueService: queueService,
            modelContainer: container
        )
        await viewModel.refresh()

        try await viewModel.remove(episodeKey: keys[1])
        await viewModel.refresh()

        #expect(viewModel.rows.map(\.title) == ["Keep"])
    }

    @Test("clear() removes all rows")
    func clearRemovesAll() async throws {
        let container = try makeContainer()
        let context = ModelContext(container)
        let keys = try seedPodcastAndEpisodes(
            in: context,
            episodes: [
                (guid: "g1", title: "A", duration: 100),
                (guid: "g2", title: "B", duration: 100),
            ]
        )

        let queueService = PlaybackQueueService(modelContainer: container)
        try await queueService.addLast(episodeKey: keys[0])
        try await queueService.addLast(episodeKey: keys[1])

        let viewModel = QueueViewModel(
            queueService: queueService,
            modelContainer: container
        )
        try await viewModel.clear()
        await viewModel.refresh()

        #expect(viewModel.rows.isEmpty)
    }
}
