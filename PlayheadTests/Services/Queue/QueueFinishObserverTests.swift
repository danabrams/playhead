// QueueFinishObserverTests.swift
// Verifies the small bridge that subscribes to
// `playbackDidFinishEpisode` and calls `advance()` on the advancer for
// each event. Decoupled from `PlaybackQueueAutoAdvancer` so the
// advancer stays testable in isolation.

import Foundation
import SwiftData
import Testing
@testable import Playhead

@MainActor
@Suite("PlaybackQueueFinishObserver — notification bridge")
struct QueueFinishObserverTests {

    private func makeContainer() throws -> ModelContainer {
        let schema = Schema([QueueEntry.self])
        let config = ModelConfiguration(
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        return try ModelContainer(for: schema, configurations: [config])
    }

    private actor PlayRecorder {
        private(set) var played: [String] = []
        func record(_ key: String) { played.append(key) }
    }

    @Test("posts on the center cause the advancer to fire")
    func observerFiresAdvance() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)
        try await service.addLast(episodeKey: "ep-A")

        let recorder = PlayRecorder()
        let advancer = PlaybackQueueAutoAdvancer(
            queue: service,
            countdown: .zero,
            playHandler: { key in await recorder.record(key) }
        )

        let center = NotificationCenter()
        let observer = PlaybackQueueFinishObserver(
            center: center,
            advancer: advancer
        )
        observer.start()
        defer { observer.stop() }

        // The observer registers a synchronous addObserver block, so
        // by the time `start()` returns, the post below is guaranteed
        // to fire the block. The block dispatches into an unstructured
        // Task whose advance() runs on the actor; await that exact
        // scheduled task rather than polling recorder side effects.
        center.post(name: .playbackDidFinishEpisode, object: nil)

        let completed = await observer.waitForLastAdvanceForTesting()
        #expect(completed, "Observer-scheduled advance task must complete")
        let played = await recorder.played
        #expect(played == ["ep-A"])
    }

    @Test("stop unsubscribes — subsequent notifications do not fire advance")
    func stopUnsubscribes() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)
        try await service.addLast(episodeKey: "ep-A")

        let recorder = PlayRecorder()
        let advancer = PlaybackQueueAutoAdvancer(
            queue: service,
            countdown: .zero,
            playHandler: { key in await recorder.record(key) }
        )

        let center = NotificationCenter()
        let observer = PlaybackQueueFinishObserver(
            center: center,
            advancer: advancer
        )
        observer.start()
        observer.stop()

        center.post(name: .playbackDidFinishEpisode, object: nil)

        #expect(observer.recordedAdvanceCountForTesting() == 0)
        let played = await recorder.played
        #expect(played.isEmpty)
    }
}
