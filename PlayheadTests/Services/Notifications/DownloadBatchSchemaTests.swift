// DownloadBatchSchemaTests.swift
// Verifies `DownloadBatch` round-trips through SwiftData with the
// production schema and that `[String]` storage works for
// `episodeKeys`. playhead-zp0x.
//
// Loads via the production schema (`SwiftDataStore.schema`) so a
// migration regression that drops `DownloadBatch.self` from the model
// list fails this test.

import Foundation
import SwiftData
import Testing

@testable import Playhead

@Suite("DownloadBatch — SwiftData schema round-trip (playhead-zp0x)")
@MainActor
struct DownloadBatchSchemaTests {

    @Test("Insert + fetch round-trips all fields through production schema")
    func roundTripThroughProductionSchema() throws {
        // Use the production schema directly to confirm DownloadBatch.self
        // is registered. Failure to register would yield a "no such
        // entity" runtime error here.
        let config = ModelConfiguration(
            "DownloadBatchSchemaTests",
            schema: SwiftDataStore.schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        let container = try ModelContainer(
            for: SwiftDataStore.schema,
            configurations: [config]
        )
        let context = container.mainContext

        let id = UUID()
        let createdAt = Date(timeIntervalSince1970: 1_700_000_000)
        let firstBlockedAt = Date(timeIntervalSince1970: 1_700_001_000)
        let closedAt = Date(timeIntervalSince1970: 1_700_002_000)

        let batch = DownloadBatch(
            id: id,
            createdAt: createdAt,
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["ep-a", "ep-b", "ep-c"],
            tripReadyNotified: true,
            actionRequiredNotified: false,
            consecutiveBlockedPasses: 3,
            firstBlockedAt: firstBlockedAt,
            lastEligibility: BatchNotificationEligibility.blockedStorage.rawValue,
            closedAt: closedAt
        )
        context.insert(batch)
        try context.save()

        // Fetch back and verify every field round-trips.
        let descriptor = FetchDescriptor<DownloadBatch>(
            predicate: #Predicate<DownloadBatch> { $0.id == id }
        )
        let fetched = try context.fetch(descriptor)
        #expect(fetched.count == 1)

        let fb = try #require(fetched.first)
        #expect(fb.id == id)
        #expect(fb.createdAt == createdAt)
        #expect(fb.tripContextRaw == DownloadTripContext.flight.rawValue)
        #expect(fb.episodeKeys == ["ep-a", "ep-b", "ep-c"])
        #expect(fb.tripReadyNotified == true)
        #expect(fb.actionRequiredNotified == false)
        #expect(fb.consecutiveBlockedPasses == 3)
        #expect(fb.firstBlockedAt == firstBlockedAt)
        #expect(fb.lastEligibility == "blockedStorage")
        #expect(fb.closedAt == closedAt)
    }

    @Test("Empty episodeKeys array round-trips")
    func emptyEpisodeKeysArrayRoundTrips() throws {
        let config = ModelConfiguration(
            "DownloadBatchSchemaTests-empty",
            schema: SwiftDataStore.schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        let container = try ModelContainer(
            for: SwiftDataStore.schema,
            configurations: [config]
        )
        let context = container.mainContext

        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.commute.rawValue,
            episodeKeys: []
        )
        context.insert(batch)
        try context.save()

        let fetched = try context.fetch(FetchDescriptor<DownloadBatch>())
        #expect(fetched.count == 1)
        #expect(fetched.first?.episodeKeys == [])
    }

    @Test("Migration plan still loads with DownloadBatch added to V1")
    func migrationPlanStillLoads() throws {
        // Going through the production `makeContainer` path exercises
        // the migration plan (zero stages today). A failure here would
        // indicate adding DownloadBatch to PlayheadSchemaV1.models has
        // somehow broken the migration plan loading itself.
        let config = ModelConfiguration(
            "DownloadBatchSchemaTests-migration",
            schema: SwiftDataStore.schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        let container = try ModelContainer(
            for: SwiftDataStore.schema,
            migrationPlan: PlayheadMigrationPlan.self,
            configurations: [config]
        )
        // If we got here, the migration plan loaded successfully.
        // Smoke-test a write to confirm the container is healthy.
        let context = container.mainContext
        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.workout.rawValue,
            episodeKeys: ["a"]
        )
        context.insert(batch)
        try context.save()

        let fetched = try context.fetch(FetchDescriptor<DownloadBatch>())
        #expect(fetched.count == 1)
    }
}
