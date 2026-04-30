// QueueAutoAdvanceTests.swift
// Verifies the auto-advance path: when an episode finishes, the
// `PlaybackQueueAutoAdvancer` waits a configurable countdown, pops the
// next entry, and asks the runtime to play it. Cancellation during the
// countdown halts the start.

import Foundation
import SwiftData
import Testing
@testable import Playhead

@MainActor
@Suite("PlaybackQueueAutoAdvancer — auto-advance on finish")
struct QueueAutoAdvanceTests {

    private func makeContainer() throws -> ModelContainer {
        let schema = Schema([QueueEntry.self])
        let config = ModelConfiguration(
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        return try ModelContainer(for: schema, configurations: [config])
    }

    /// Test recorder that captures the episodeKeys the advancer asks to
    /// play, so tests can assert which episode auto-advance picked.
    private actor PlayRecorder {
        private(set) var played: [String] = []

        func record(_ key: String) {
            played.append(key)
        }
    }

    @Test("advance pops the next entry and asks the runtime to play it")
    func advanceFiresPlay() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)
        try await service.addLast(episodeKey: "ep-A")
        try await service.addLast(episodeKey: "ep-B")

        let recorder = PlayRecorder()
        let advancer = PlaybackQueueAutoAdvancer(
            queue: service,
            countdown: .zero,
            playHandler: { key in await recorder.record(key) }
        )

        await advancer.advance()
        let played = await recorder.played
        #expect(played == ["ep-A"])

        // Queue should now hold only ep-B.
        let entries = try await service.allEntries()
        #expect(entries.map(\.episodeKey) == ["ep-B"])
    }

    @Test("advance on empty queue does NOT call the play handler")
    func advanceOnEmptyDoesNothing() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)

        let recorder = PlayRecorder()
        let advancer = PlaybackQueueAutoAdvancer(
            queue: service,
            countdown: .zero,
            playHandler: { key in await recorder.record(key) }
        )

        await advancer.advance()
        let played = await recorder.played
        #expect(played.isEmpty)
    }

    @Test("cancel during countdown prevents the next episode from starting")
    func cancelDuringCountdown() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)
        try await service.addLast(episodeKey: "ep-A")

        let recorder = PlayRecorder()
        let advancer = PlaybackQueueAutoAdvancer(
            queue: service,
            countdown: .seconds(60), // long enough that we cancel before fire
            playHandler: { key in await recorder.record(key) }
        )

        // Kick off the countdown and immediately cancel it.
        async let advanceTask: Void = advancer.advance()
        // Wait briefly so the advance() Task starts the countdown.
        try await Task.sleep(for: .milliseconds(20))
        await advancer.cancel()
        await advanceTask

        let played = await recorder.played
        #expect(played.isEmpty, "Cancellation should prevent playback")

        // The queue should still hold ep-A — popNext only fires after the
        // countdown completes.
        let entries = try await service.allEntries()
        #expect(entries.map(\.episodeKey) == ["ep-A"])
    }

    @Test("countdown delay is observed before play handler fires")
    func countdownIsObserved() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)
        try await service.addLast(episodeKey: "ep-A")

        let recorder = PlayRecorder()
        let advancer = PlaybackQueueAutoAdvancer(
            queue: service,
            countdown: .milliseconds(150),
            playHandler: { key in await recorder.record(key) }
        )

        let start = Date()
        await advancer.advance()
        let elapsed = Date().timeIntervalSince(start)
        #expect(elapsed >= 0.1, "Countdown of 150ms should have elapsed")

        let played = await recorder.played
        #expect(played == ["ep-A"])
    }

    @Test("a second advance() while one is in flight does NOT start a duplicate")
    func reentrantAdvanceIsGuarded() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)
        try await service.addLast(episodeKey: "ep-A")
        try await service.addLast(episodeKey: "ep-B")

        let recorder = PlayRecorder()
        let advancer = PlaybackQueueAutoAdvancer(
            queue: service,
            countdown: .milliseconds(100),
            playHandler: { key in await recorder.record(key) }
        )

        async let one: Void = advancer.advance()
        async let two: Void = advancer.advance()
        _ = await (one, two)

        let played = await recorder.played
        // Exactly one advance should have fired — even though two were
        // requested, the in-flight guard collapses the second.
        #expect(played == ["ep-A"], "Only one advance should fire even with two concurrent calls")
        let entries = try await service.allEntries()
        #expect(entries.map(\.episodeKey) == ["ep-B"])
    }
}
