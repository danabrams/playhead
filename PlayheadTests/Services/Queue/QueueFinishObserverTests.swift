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
        // Task whose advance() runs on the actor — poll the recorder
        // up to a generous wall-clock deadline so parallel-test
        // scheduler pressure (PlayheadFastTests runs ~5000 tests in
        // ~700 suites) can't wedge us. The advance Task may queue
        // behind many cooperative-pool workers under that load.
        center.post(name: .playbackDidFinishEpisode, object: nil)

        let deadline = Date().addingTimeInterval(8.0)
        while Date() < deadline {
            let played = await recorder.played
            if played == ["ep-A"] { return }
            try await Task.sleep(for: .milliseconds(20))
        }
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

        try await Task.sleep(for: .milliseconds(10))
        center.post(name: .playbackDidFinishEpisode, object: nil)
        try await Task.sleep(for: .milliseconds(100))

        let played = await recorder.played
        #expect(played.isEmpty)
    }
}
