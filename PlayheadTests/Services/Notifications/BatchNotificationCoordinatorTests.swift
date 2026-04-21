// BatchNotificationCoordinatorTests.swift
// Full-pipeline tests for `BatchNotificationCoordinator` against an
// in-memory SwiftData ModelContainer. playhead-zp0x.
//
// Verifies cap enforcement (tripReady fires once, actionRequired fires
// once), generic-context skip, and closed-batch skip.

import Foundation
import SwiftData
import Testing
import UserNotifications

@testable import Playhead

@MainActor
private final class RecordingScheduler: BatchNotificationService.Scheduler {
    private(set) var requests: [UNNotificationRequest] = []
    func add(_ request: UNNotificationRequest) async throws {
        requests.append(request)
    }
    func snapshot() -> [UNNotificationRequest] { requests }
}

@MainActor
private func makeInMemoryContainer() throws -> ModelContainer {
    let schema = Schema([DownloadBatch.self])
    let config = ModelConfiguration(
        "BatchNotificationCoordinatorTests",
        schema: schema,
        isStoredInMemoryOnly: true
    )
    return try ModelContainer(for: schema, configurations: [config])
}

@Suite("BatchNotificationCoordinator — cap + generic + closed (playhead-zp0x)")
@MainActor
struct BatchNotificationCoordinatorTests {

    private static let t0 = Date(timeIntervalSince1970: 1_700_000_000)

    /// Builder that returns "all-ready" for every key (drives tripReady).
    private static let allReadyBuilder: @Sendable ([String]) async -> [BatchChildSurfaceSummary] = { keys in
        keys.map { key in
            BatchChildSurfaceSummary(
                canonicalEpisodeKey: key,
                disposition: .queued,
                reason: .waitingForTime,
                analysisUnavailableReason: nil,
                isReady: true,
                userFixable: false
            )
        }
    }

    /// Builder that returns "storage-blocker fixable" for every key.
    private static let allStorageBlockedBuilder: @Sendable ([String]) async -> [BatchChildSurfaceSummary] = { keys in
        keys.map { key in
            BatchChildSurfaceSummary(
                canonicalEpisodeKey: key,
                disposition: .paused,
                reason: .storageFull,
                analysisUnavailableReason: nil,
                isReady: false,
                userFixable: true
            )
        }
    }

    // MARK: - tripReady cap

    @Test("tripReady fires exactly once even across multiple passes")
    func tripReadyFiresOnce() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)

        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["ep-a", "ep-b"]
        )
        context.insert(batch)
        try context.save()

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: Self.allReadyBuilder
        )

        // Three passes — only the first should emit tripReady.
        await coordinator.runOncePass(now: Self.t0)
        await coordinator.runOncePass(now: Self.t0.addingTimeInterval(60))
        await coordinator.runOncePass(now: Self.t0.addingTimeInterval(120))

        let requests = await scheduler.snapshot()
        let tripReadyCount = requests.filter {
            ($0.content.userInfo["trigger"] as? String) == "tripReady"
        }.count
        #expect(tripReadyCount == 1)
        #expect(batch.tripReadyNotified == true)
    }

    // MARK: - actionRequired cap

    @Test("actionRequired fires exactly once even across multiple passes")
    func actionRequiredFiresOnce() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)

        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["ep-a"]
        )
        context.insert(batch)
        try context.save()

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: Self.allStorageBlockedBuilder
        )

        // Pass 1: first blocked pass (counter & anchor establish).
        // Pass 2 (≥ 30 min later): both bars cleared → fire actionRequired.
        // Pass 3, 4: should NOT re-fire.
        let now1 = Self.t0
        let now2 = Self.t0.addingTimeInterval(35 * 60)
        let now3 = Self.t0.addingTimeInterval(70 * 60)
        let now4 = Self.t0.addingTimeInterval(105 * 60)

        await coordinator.runOncePass(now: now1)
        await coordinator.runOncePass(now: now2)
        await coordinator.runOncePass(now: now3)
        await coordinator.runOncePass(now: now4)

        let requests = await scheduler.snapshot()
        let storageCount = requests.filter {
            ($0.content.userInfo["trigger"] as? String) == "blockedStorage"
        }.count
        #expect(storageCount == 1)
        #expect(batch.actionRequiredNotified == true)
    }

    // MARK: - Generic context never fires

    @Test("Generic batch never fires anything (no permission, no notification)")
    func genericBatchNeverFires() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)

        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.generic.rawValue,
            episodeKeys: ["ep-a", "ep-b"]
        )
        context.insert(batch)
        try context.save()

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: Self.allReadyBuilder
        )

        await coordinator.runOncePass(now: Self.t0)
        await coordinator.runOncePass(now: Self.t0.addingTimeInterval(60))

        let requests = await scheduler.snapshot()
        #expect(requests.isEmpty)
        #expect(batch.tripReadyNotified == false)
    }

    // MARK: - Closed batch is skipped

    @Test("Closed batch (closedAt set) is skipped in subsequent passes")
    func closedBatchSkipped() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)

        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["ep-a"],
            closedAt: Self.t0.addingTimeInterval(-3600)
        )
        context.insert(batch)
        try context.save()

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            // Even with all-ready summaries, the closed batch is not
            // fetched by `runOncePass` (predicate filters on closedAt).
            summaryBuilder: Self.allReadyBuilder
        )

        await coordinator.runOncePass(now: Self.t0)

        let requests = await scheduler.snapshot()
        #expect(requests.isEmpty)
        #expect(batch.tripReadyNotified == false)
    }

    // MARK: - Persistence-rule AND-gate (integration)

    @Test("≥2 passes within <30 min does NOT fire actionRequired (wall-clock bar)")
    func passesWithoutWallClockDoesNotFire() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)

        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["ep-a"]
        )
        context.insert(batch)
        try context.save()

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: Self.allStorageBlockedBuilder
        )

        // Two passes 5 min apart — pass-count bar cleared, wall-clock not.
        await coordinator.runOncePass(now: Self.t0)
        await coordinator.runOncePass(now: Self.t0.addingTimeInterval(5 * 60))

        let requests = await scheduler.snapshot()
        #expect(requests.isEmpty)
        #expect(batch.actionRequiredNotified == false)
        #expect(batch.consecutiveBlockedPasses == 2)
    }

    @Test("1 pass at ≥30 min wall-clock does NOT fire actionRequired (count bar)")
    func wallClockWithoutPassesDoesNotFire() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)

        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["ep-a"]
        )
        context.insert(batch)
        try context.save()

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: Self.allStorageBlockedBuilder
        )

        // Single pass 60 min after t0. Wall-clock cleared on its own
        // pass would not be possible without two ticks, so simulate by
        // running just one pass — `consecutiveBlockedPasses` reaches 1.
        await coordinator.runOncePass(now: Self.t0.addingTimeInterval(60 * 60))

        let requests = await scheduler.snapshot()
        #expect(requests.isEmpty)
        #expect(batch.actionRequiredNotified == false)
        #expect(batch.consecutiveBlockedPasses == 1)
    }

    // MARK: - Trip-ready closes the batch when all children terminal

    @Test("All-ready pass marks batch closed (closedAt set)")
    func allReadyClosesBatch() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)

        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: ["ep-a", "ep-b"]
        )
        context.insert(batch)
        try context.save()

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: Self.allReadyBuilder
        )

        await coordinator.runOncePass(now: Self.t0)

        #expect(batch.closedAt != nil)
    }

    /// Regression guard: a batch with zero children (either constructed
    /// empty, or left with all episodes deleted from SwiftData) must
    /// close on the next pass. Prior to this guard, the coordinator
    /// required `summaries.isEmpty == false` before setting `closedAt`,
    /// which meant an empty batch stayed open forever — the evictor's
    /// 7-day TTL keys off `closedAt`, so the row would leak indefinitely.
    @Test("Empty batch closes on first pass (no children to wait on)")
    func emptyBatchClosesImmediately() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)

        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: []
        )
        context.insert(batch)
        try context.save()

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: Self.allReadyBuilder
        )

        await coordinator.runOncePass(now: Self.t0)

        #expect(batch.closedAt != nil)
        let requests = await scheduler.snapshot()
        #expect(requests.isEmpty)
    }
}
