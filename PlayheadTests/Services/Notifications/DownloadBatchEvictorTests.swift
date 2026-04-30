// DownloadBatchEvictorTests.swift
// 7-day eviction policy tests for `DownloadBatchEvictor`. playhead-zp0x.

import Foundation
import SwiftData
import Testing

@testable import Playhead

@MainActor
private func makeInMemoryContainer() throws -> ModelContainer {
    let schema = Schema([DownloadBatch.self])
    let config = ModelConfiguration(
        "DownloadBatchEvictorTests",
        schema: schema,
        isStoredInMemoryOnly: true,
        cloudKitDatabase: .none
    )
    return try ModelContainer(for: schema, configurations: [config])
}

@Suite("DownloadBatchEvictor — 7-day retention (playhead-zp0x)")
@MainActor
struct DownloadBatchEvictorTests {

    private static let t0 = Date(timeIntervalSince1970: 1_700_000_000)
    private static let oneDay: TimeInterval = 24 * 60 * 60

    @Test("Closed > 7 days → deleted")
    func closedOver7DaysDeleted() throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext

        let stale = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["a"],
            closedAt: Self.t0.addingTimeInterval(-8 * Self.oneDay) // 8 days ago
        )
        context.insert(stale)
        try context.save()

        let deleted = DownloadBatchEvictor.evict(modelContext: context, now: Self.t0)
        #expect(deleted == 1)

        let remaining = try context.fetch(FetchDescriptor<DownloadBatch>())
        #expect(remaining.isEmpty)
    }

    @Test("Closed exactly 7 days ago → deleted (boundary inclusive)")
    func closedExactly7DaysDeleted() throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext

        let boundary = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["a"],
            closedAt: Self.t0.addingTimeInterval(-7 * Self.oneDay)
        )
        context.insert(boundary)
        try context.save()

        let deleted = DownloadBatchEvictor.evict(modelContext: context, now: Self.t0)
        #expect(deleted == 1)
    }

    @Test("Closed < 7 days → kept")
    func closedUnder7DaysKept() throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext

        let recent = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["a"],
            closedAt: Self.t0.addingTimeInterval(-6 * Self.oneDay) // 6 days ago
        )
        context.insert(recent)
        try context.save()

        let deleted = DownloadBatchEvictor.evict(modelContext: context, now: Self.t0)
        #expect(deleted == 0)

        let remaining = try context.fetch(FetchDescriptor<DownloadBatch>())
        #expect(remaining.count == 1)
    }

    @Test("Open batch (closedAt nil) → kept regardless of age")
    func openBatchAlwaysKept() throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext

        let veryOldOpen = DownloadBatch(
            id: UUID(),
            createdAt: Self.t0.addingTimeInterval(-365 * Self.oneDay), // 1 year old
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["a"]
            // closedAt nil
        )
        context.insert(veryOldOpen)
        try context.save()

        let deleted = DownloadBatchEvictor.evict(modelContext: context, now: Self.t0)
        #expect(deleted == 0)

        let remaining = try context.fetch(FetchDescriptor<DownloadBatch>())
        #expect(remaining.count == 1)
    }

    @Test("Mixed open + closed → only stale-closed deleted")
    func mixedSetEvictsStaleClosedOnly() throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext

        let openBatch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["open"]
        )
        let recentClosed = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["recent"],
            closedAt: Self.t0.addingTimeInterval(-3 * Self.oneDay)
        )
        let staleClosed = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["stale"],
            closedAt: Self.t0.addingTimeInterval(-30 * Self.oneDay)
        )
        context.insert(openBatch)
        context.insert(recentClosed)
        context.insert(staleClosed)
        try context.save()

        let deleted = DownloadBatchEvictor.evict(modelContext: context, now: Self.t0)
        #expect(deleted == 1)

        let remaining = try context.fetch(FetchDescriptor<DownloadBatch>())
        #expect(remaining.count == 2)
        // The stale row should be the one missing.
        #expect(remaining.allSatisfy { $0.episodeKeys != ["stale"] })
    }
}
