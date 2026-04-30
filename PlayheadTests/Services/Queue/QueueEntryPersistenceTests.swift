// QueueEntryPersistenceTests.swift
// Verifies the SwiftData `QueueEntry` model: schema registration,
// insert/fetch round-trip, and ordering by position. The Persistence
// layer is exercised in isolation here — the higher-level service
// behavior lives in `QueueServiceTests.swift`.

import Foundation
import SwiftData
import Testing
@testable import Playhead

@MainActor
@Suite("QueueEntry — SwiftData persistence")
struct QueueEntryPersistenceTests {

    private func makeContainer() throws -> ModelContainer {
        let schema = Schema([QueueEntry.self])
        let config = ModelConfiguration(
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        return try ModelContainer(for: schema, configurations: [config])
    }

    @Test("QueueEntry insert and fetch round-trips fields")
    func roundTrips() throws {
        let container = try makeContainer()
        let context = ModelContext(container)

        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let entry = QueueEntry(
            episodeKey: "ep-A",
            position: 0,
            addedAt: now
        )
        context.insert(entry)
        try context.save()

        let descriptor = FetchDescriptor<QueueEntry>()
        let rows = try context.fetch(descriptor)
        #expect(rows.count == 1)
        #expect(rows.first?.episodeKey == "ep-A")
        #expect(rows.first?.position == 0)
        #expect(rows.first?.addedAt == now)
    }

    @Test("QueueEntry rows can be fetched ordered by position ascending")
    func fetchOrdered() throws {
        let container = try makeContainer()
        let context = ModelContext(container)

        // Insert out of order; fetch must come back ordered.
        context.insert(QueueEntry(episodeKey: "ep-C", position: 2))
        context.insert(QueueEntry(episodeKey: "ep-A", position: 0))
        context.insert(QueueEntry(episodeKey: "ep-B", position: 1))
        try context.save()

        var descriptor = FetchDescriptor<QueueEntry>()
        descriptor.sortBy = [SortDescriptor(\QueueEntry.position, order: .forward)]
        let rows = try context.fetch(descriptor)

        #expect(rows.map(\.episodeKey) == ["ep-A", "ep-B", "ep-C"])
    }

    @Test("QueueEntry survives a fresh ModelContext (simulated app restart)")
    func survivesNewContext() throws {
        let container = try makeContainer()

        // Write through one context.
        let writer = ModelContext(container)
        writer.insert(QueueEntry(episodeKey: "ep-A", position: 0))
        writer.insert(QueueEntry(episodeKey: "ep-B", position: 1))
        try writer.save()

        // Read back through a fresh context — same container (simulating
        // an app cold-start re-opening the persistent store).
        let reader = ModelContext(container)
        var descriptor = FetchDescriptor<QueueEntry>()
        descriptor.sortBy = [SortDescriptor(\QueueEntry.position, order: .forward)]
        let rows = try reader.fetch(descriptor)
        #expect(rows.map(\.episodeKey) == ["ep-A", "ep-B"])
    }
}
