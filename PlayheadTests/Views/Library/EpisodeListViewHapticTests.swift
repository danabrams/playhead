// EpisodeListViewHapticTests.swift
// Verifies that the EpisodeListView queue-swipe action routes haptic
// feedback through the injected `HapticPlaying` seam instead of calling
// `HapticManager.notification(.success)` directly.

import XCTest
import SwiftData
@testable import Playhead

@MainActor
final class EpisodeListViewHapticTests: XCTestCase {

    private func makeInMemoryContainer() throws -> ModelContainer {
        let config = ModelConfiguration(isStoredInMemoryOnly: true)
        return try ModelContainer(
            for: Podcast.self, Episode.self,
            configurations: config
        )
    }

    func testQueueTapEmitsSaveHaptic() throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let podcast = Podcast(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "Test Podcast",
            author: "Test Author"
        )
        context.insert(podcast)
        let episode = Episode(
            feedItemGUID: "guid-1",
            feedURL: podcast.feedURL,
            podcast: podcast,
            title: "Test Episode",
            audioURL: URL(string: "https://example.com/ep.mp3")!
        )
        context.insert(episode)

        let recorder = RecordingHapticPlayer()
        let view = EpisodeListView(podcast: podcast, hapticPlayer: recorder)

        view.queueEpisode(episode)

        XCTAssertEqual(recorder.played, [.save],
            "Queue-swipe must emit exactly one .save haptic event via the injected player")
    }

    func testDefaultHapticPlayerIsSystemPlayer() throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let podcast = Podcast(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "Test Podcast",
            author: "Test Author"
        )
        context.insert(podcast)

        let view = EpisodeListView(podcast: podcast)
        XCTAssertTrue(view.hapticPlayer is SystemHapticPlayer,
            "EpisodeListView default hapticPlayer should be SystemHapticPlayer")
    }
}
