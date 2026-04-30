// QueueServiceTests.swift
// Verifies `PlaybackQueueService`'s public API: addNext / addLast /
// remove / move / clear / peek / popNext / count, all backed by
// SwiftData. Persistence semantics: every public mutation calls
// `save()` so the rows survive a restart; tests assert that with a
// fresh `ModelContext` against the same container.

import Foundation
import SwiftData
import Testing
@testable import Playhead

@MainActor
@Suite("PlaybackQueueService — public API")
struct QueueServiceTests {

    private func makeContainer() throws -> ModelContainer {
        let schema = Schema([QueueEntry.self])
        let config = ModelConfiguration(
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        return try ModelContainer(for: schema, configurations: [config])
    }

    @Test("empty service returns nil peek and count 0")
    func emptyService() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)

        let count = await service.count
        let peek = await service.peek()
        #expect(count == 0)
        #expect(peek == nil)
    }

    @Test("addLast persists ordered entries")
    func addLastPersists() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)

        try await service.addLast(episodeKey: "ep-A")
        try await service.addLast(episodeKey: "ep-B")
        try await service.addLast(episodeKey: "ep-C")

        let entries = try await service.allEntries()
        #expect(entries.map(\.episodeKey) == ["ep-A", "ep-B", "ep-C"])
        #expect(entries.map(\.position) == [0, 1, 2])
    }

    @Test("addNext inserts at front and shifts existing entries")
    func addNextShifts() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)

        try await service.addLast(episodeKey: "ep-A")
        try await service.addLast(episodeKey: "ep-B")
        try await service.addNext(episodeKey: "ep-X")

        let entries = try await service.allEntries()
        #expect(entries.map(\.episodeKey) == ["ep-X", "ep-A", "ep-B"])
        #expect(entries.map(\.position) == [0, 1, 2])
    }

    @Test("popNext removes and returns the head; subsequent peek shows the new head")
    func popNextRemoves() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)

        try await service.addLast(episodeKey: "ep-A")
        try await service.addLast(episodeKey: "ep-B")

        let popped = try await service.popNext()
        #expect(popped?.episodeKey == "ep-A")

        let peek = await service.peek()
        #expect(peek?.episodeKey == "ep-B")

        let count = await service.count
        #expect(count == 1)
    }

    @Test("popNext on empty service returns nil")
    func popNextEmpty() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)
        let popped = try await service.popNext()
        #expect(popped == nil)
    }

    @Test("remove deletes a specific row and compacts positions")
    func removeCompacts() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)

        try await service.addLast(episodeKey: "ep-A")
        try await service.addLast(episodeKey: "ep-B")
        try await service.addLast(episodeKey: "ep-C")
        try await service.remove(episodeKey: "ep-B")

        let entries = try await service.allEntries()
        #expect(entries.map(\.episodeKey) == ["ep-A", "ep-C"])
        #expect(entries.map(\.position) == [0, 1])
    }

    @Test("clear removes all entries")
    func clearAll() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)

        try await service.addLast(episodeKey: "ep-A")
        try await service.addLast(episodeKey: "ep-B")
        try await service.clear()

        let count = await service.count
        #expect(count == 0)
    }

    @Test("move reorders rows")
    func moveReorders() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)

        try await service.addLast(episodeKey: "ep-A")
        try await service.addLast(episodeKey: "ep-B")
        try await service.addLast(episodeKey: "ep-C")
        try await service.move(fromOffsets: IndexSet(integer: 0), toOffset: 3)

        let entries = try await service.allEntries()
        #expect(entries.map(\.episodeKey) == ["ep-B", "ep-C", "ep-A"])
        #expect(entries.map(\.position) == [0, 1, 2])
    }

    @Test("addLast on duplicate key moves to tail (no duplicate row)")
    func addLastDeduplicates() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)

        try await service.addLast(episodeKey: "ep-A")
        try await service.addLast(episodeKey: "ep-B")
        try await service.addLast(episodeKey: "ep-A")

        let entries = try await service.allEntries()
        #expect(entries.map(\.episodeKey) == ["ep-B", "ep-A"])
        #expect(entries.count == 2)
    }

    @Test("addNext on duplicate key moves to head (no duplicate row)")
    func addNextDeduplicates() async throws {
        let container = try makeContainer()
        let service = PlaybackQueueService(modelContainer: container)

        try await service.addLast(episodeKey: "ep-A")
        try await service.addLast(episodeKey: "ep-B")
        try await service.addNext(episodeKey: "ep-B")

        let entries = try await service.allEntries()
        #expect(entries.map(\.episodeKey) == ["ep-B", "ep-A"])
        #expect(entries.count == 2)
    }

    @Test("queue persists across a fresh service instance on the same container (simulates relaunch)")
    func persistsAcrossRelaunch() async throws {
        let container = try makeContainer()

        // Session 1: enqueue.
        let session1 = PlaybackQueueService(modelContainer: container)
        try await session1.addLast(episodeKey: "ep-A")
        try await session1.addLast(episodeKey: "ep-B")

        // Session 2: fresh service against the same container.
        let session2 = PlaybackQueueService(modelContainer: container)
        let entries = try await session2.allEntries()
        #expect(entries.map(\.episodeKey) == ["ep-A", "ep-B"])
        #expect(entries.map(\.position) == [0, 1])
    }
}
