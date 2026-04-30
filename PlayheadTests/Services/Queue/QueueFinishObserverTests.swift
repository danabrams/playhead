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
            isStoredInMemoryOnly: true
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

        // Yield generously so the observer task is consuming on the
        // simulator's heavily-loaded scheduler. 10ms was enough in
        // isolation but flaked under PlayheadFastTests parallel load.
        try await Task.sleep(for: .milliseconds(50))

        center.post(name: .playbackDidFinishEpisode, object: nil)

        // Poll up to 2 s for the handler to fire rather than relying
        // on a fixed sleep — keeps the test deterministic under load.
        let deadline = Date().addingTimeInterval(2.0)
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
