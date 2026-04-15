// ImplicitFeedbackStoreTests.swift
// Phase ef2 (playhead-ef2.3.4): Tests for ImplicitFeedbackSignal,
// ImplicitFeedbackEvent, ImplicitFeedbackStore, and the V12 schema migration.

import XCTest
import SQLite3
@testable import Playhead

final class ImplicitFeedbackStoreTests: XCTestCase {

    // MARK: - ImplicitFeedbackSignal

    func testAllSignalRawValuesAreUnique() {
        let rawValues = ImplicitFeedbackSignal.allCases.map(\.rawValue)
        XCTAssertEqual(rawValues.count, Set(rawValues).count,
            "All signal raw values must be unique")
    }

    func testSignalRoundTripsViaRawValue() {
        for signal in ImplicitFeedbackSignal.allCases {
            let restored = ImplicitFeedbackSignal(rawValue: signal.rawValue)
            XCTAssertEqual(restored, signal)
        }
    }

    func testExpectedSignalCount() {
        XCTAssertEqual(ImplicitFeedbackSignal.allCases.count, 5,
            "Spec requires exactly 5 signal types")
    }

    // MARK: - ImplicitFeedbackEvent

    func testEventWeightIsAlways0Point3() {
        let event = ImplicitFeedbackEvent(
            signal: .immediateUnskip,
            analysisAssetId: "asset-1"
        )
        XCTAssertEqual(event.weight, 0.3, accuracy: 1e-9)
    }

    func testEventWeightCannotBeOverridden() {
        // The stored-weight initializer still enforces 0.3.
        let event = ImplicitFeedbackEvent(
            id: "test-id",
            signal: .seekBackIntoSkipped,
            analysisAssetId: "asset-1",
            podcastId: nil,
            spanId: nil,
            timestamp: 1000.0,
            storedWeight: 0.9  // attempt to override
        )
        XCTAssertEqual(event.weight, 0.3, accuracy: 1e-9,
            "Weight must be 0.3 regardless of stored value")
    }

    func testEventEquality() {
        let a = ImplicitFeedbackEvent(
            id: "same-id",
            signal: .immediateUnskip,
            analysisAssetId: "asset-1",
            podcastId: "pod-1",
            spanId: "span-1",
            timestamp: 1000.0
        )
        let b = ImplicitFeedbackEvent(
            id: "same-id",
            signal: .immediateUnskip,
            analysisAssetId: "asset-1",
            podcastId: "pod-1",
            spanId: "span-1",
            timestamp: 1000.0
        )
        XCTAssertEqual(a, b)
    }

    func testEventOptionalFieldsDefaultToNil() {
        let event = ImplicitFeedbackEvent(
            signal: .showAutoSkipDisabled,
            analysisAssetId: "asset-2"
        )
        XCTAssertNil(event.podcastId)
        XCTAssertNil(event.spanId)
        XCTAssertFalse(event.id.isEmpty)
    }

    // MARK: - Schema V12 Migration

    func testSchemaV12MigrationCreatesImplicitFeedbackTable() async throws {
        let dir = try makeTempDir(prefix: "ImplicitFeedbackStoreTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let version = try await store.schemaVersion()
        XCTAssertGreaterThanOrEqual(version ?? 0, 12)
        XCTAssertTrue(try probeTableExists(in: dir, table: "implicit_feedback_events"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "implicit_feedback_events", column: "signal"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "implicit_feedback_events", column: "analysisAssetId"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "implicit_feedback_events", column: "podcastId"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "implicit_feedback_events", column: "spanId"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "implicit_feedback_events", column: "timestamp"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "implicit_feedback_events", column: "weight"))
    }

    func testFreshDatabaseReachesAtLeastV12() async throws {
        let dir = try makeTempDir(prefix: "ImplicitFeedbackStoreTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let version = try await store.schemaVersion()
        XCTAssertGreaterThanOrEqual(version ?? 0, 12)
        XCTAssertTrue(try probeTableExists(in: dir, table: "implicit_feedback_events"))
    }

    // MARK: - Record and Load

    func testRecordAndLoadByAssetId() async throws {
        let analysisStore = try await makeTestStore()
        let feedbackStore = ImplicitFeedbackStore(store: analysisStore)

        try await analysisStore.insertAsset(makeTestAsset(id: "asset-fb"))

        let event = ImplicitFeedbackEvent(
            signal: .immediateUnskip,
            analysisAssetId: "asset-fb",
            podcastId: "pod-1",
            spanId: "span-abc"
        )
        try await feedbackStore.record(event)

        let weight = await feedbackStore.feedbackWeight(for: "asset-fb")
        XCTAssertEqual(weight, 0.3, accuracy: 1e-9)
    }

    func testMultipleEventsAggregateWeight() async throws {
        let analysisStore = try await makeTestStore()
        let feedbackStore = ImplicitFeedbackStore(store: analysisStore)

        try await analysisStore.insertAsset(makeTestAsset(id: "asset-multi"))

        let t0 = Date().timeIntervalSince1970
        for i in 0..<4 {
            let event = ImplicitFeedbackEvent(
                signal: .immediateUnskip,
                analysisAssetId: "asset-multi",
                timestamp: t0 + Double(i)
            )
            try await feedbackStore.record(event)
        }

        let weight = await feedbackStore.feedbackWeight(for: "asset-multi")
        // 4 events × 0.3 = 1.2
        XCTAssertEqual(weight, 1.2, accuracy: 1e-9)
    }

    func testFeedbackWeightForUnknownAssetReturnsZero() async throws {
        let analysisStore = try await makeTestStore()
        let feedbackStore = ImplicitFeedbackStore(store: analysisStore)

        let weight = await feedbackStore.feedbackWeight(for: "nonexistent")
        XCTAssertEqual(weight, 0.0, accuracy: 1e-9)
    }

    // MARK: - Signal Counts

    func testSignalCountsPerType() async throws {
        let analysisStore = try await makeTestStore()
        let feedbackStore = ImplicitFeedbackStore(store: analysisStore)

        try await analysisStore.insertAsset(makeTestAsset(id: "asset-counts"))

        let t0 = Date().timeIntervalSince1970
        let signals: [ImplicitFeedbackSignal] = [
            .immediateUnskip,
            .immediateUnskip,
            .seekBackIntoSkipped,
            .rapidRewindAfterSkip,
            .immediateUnskip,
        ]
        for (i, signal) in signals.enumerated() {
            let event = ImplicitFeedbackEvent(
                signal: signal,
                analysisAssetId: "asset-counts",
                timestamp: t0 + Double(i)
            )
            try await feedbackStore.record(event)
        }

        let counts = await feedbackStore.signalCounts(for: "asset-counts")
        XCTAssertEqual(counts[.immediateUnskip], 3)
        XCTAssertEqual(counts[.seekBackIntoSkipped], 1)
        XCTAssertEqual(counts[.rapidRewindAfterSkip], 1)
        XCTAssertNil(counts[.repeatedManualSkipForward])
        XCTAssertNil(counts[.showAutoSkipDisabled])
    }

    func testSignalCountsForUnknownAssetReturnsEmpty() async throws {
        let analysisStore = try await makeTestStore()
        let feedbackStore = ImplicitFeedbackStore(store: analysisStore)

        let counts = await feedbackStore.signalCounts(for: "nonexistent")
        XCTAssertTrue(counts.isEmpty)
    }

    // MARK: - Recent Signals by Podcast

    func testRecentSignalsForShowReturnsDescendingOrder() async throws {
        let analysisStore = try await makeTestStore()
        let feedbackStore = ImplicitFeedbackStore(store: analysisStore)

        try await analysisStore.insertAsset(makeTestAsset(id: "asset-recent"))

        let t0 = Date().timeIntervalSince1970
        for i in 0..<5 {
            let event = ImplicitFeedbackEvent(
                signal: .seekBackIntoSkipped,
                analysisAssetId: "asset-recent",
                podcastId: "pod-recent",
                timestamp: t0 + Double(i)
            )
            try await feedbackStore.record(event)
        }

        let recent = await feedbackStore.recentSignals(forShow: "pod-recent", limit: 3)
        XCTAssertEqual(recent.count, 3)
        // Descending order: most recent first.
        XCTAssertGreaterThanOrEqual(recent[0].timestamp, recent[1].timestamp)
        XCTAssertGreaterThanOrEqual(recent[1].timestamp, recent[2].timestamp)
    }

    func testRecentSignalsForShowFiltersOnPodcastId() async throws {
        let analysisStore = try await makeTestStore()
        let feedbackStore = ImplicitFeedbackStore(store: analysisStore)

        try await analysisStore.insertAsset(makeTestAsset(id: "asset-filter-a"))
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-filter-b"))

        try await feedbackStore.record(ImplicitFeedbackEvent(
            signal: .immediateUnskip,
            analysisAssetId: "asset-filter-a",
            podcastId: "pod-a"
        ))
        try await feedbackStore.record(ImplicitFeedbackEvent(
            signal: .seekBackIntoSkipped,
            analysisAssetId: "asset-filter-b",
            podcastId: "pod-b"
        ))

        let recentA = await feedbackStore.recentSignals(forShow: "pod-a", limit: 10)
        XCTAssertEqual(recentA.count, 1)
        XCTAssertEqual(recentA[0].signal, .immediateUnskip)

        let recentB = await feedbackStore.recentSignals(forShow: "pod-b", limit: 10)
        XCTAssertEqual(recentB.count, 1)
        XCTAssertEqual(recentB[0].signal, .seekBackIntoSkipped)
    }

    func testRecentSignalsForUnknownShowReturnsEmpty() async throws {
        let analysisStore = try await makeTestStore()
        let feedbackStore = ImplicitFeedbackStore(store: analysisStore)

        let recent = await feedbackStore.recentSignals(forShow: "nonexistent", limit: 10)
        XCTAssertTrue(recent.isEmpty)
    }

    // MARK: - Idempotent Insert

    func testDuplicateEventIdIsIgnored() async throws {
        let analysisStore = try await makeTestStore()
        let feedbackStore = ImplicitFeedbackStore(store: analysisStore)

        try await analysisStore.insertAsset(makeTestAsset(id: "asset-idem"))

        let event = ImplicitFeedbackEvent(
            id: "fixed-feedback-id",
            signal: .rapidRewindAfterSkip,
            analysisAssetId: "asset-idem"
        )
        try await feedbackStore.record(event)
        // Second insert with the same id must be silently ignored.
        try await feedbackStore.record(event)

        let weight = await feedbackStore.feedbackWeight(for: "asset-idem")
        XCTAssertEqual(weight, 0.3, accuracy: 1e-9,
            "Duplicate insert must not double-count")
    }

    // MARK: - All Signal Types Round-Trip

    func testAllSignalTypesRoundTripThroughDatabase() async throws {
        let analysisStore = try await makeTestStore()
        let feedbackStore = ImplicitFeedbackStore(store: analysisStore)

        try await analysisStore.insertAsset(makeTestAsset(id: "asset-all-signals"))

        let t0 = Date().timeIntervalSince1970
        for (i, signal) in ImplicitFeedbackSignal.allCases.enumerated() {
            let event = ImplicitFeedbackEvent(
                signal: signal,
                analysisAssetId: "asset-all-signals",
                podcastId: "pod-rt",
                spanId: "span-\(i)",
                timestamp: t0 + Double(i)
            )
            try await feedbackStore.record(event)
        }

        let counts = await feedbackStore.signalCounts(for: "asset-all-signals")
        for signal in ImplicitFeedbackSignal.allCases {
            XCTAssertEqual(counts[signal], 1,
                "\(signal.rawValue) must round-trip through database")
        }
    }

    // MARK: - Null Optional Fields

    func testNullOptionalFieldsRoundTrip() async throws {
        let analysisStore = try await makeTestStore()
        let feedbackStore = ImplicitFeedbackStore(store: analysisStore)

        try await analysisStore.insertAsset(makeTestAsset(id: "asset-nulls"))

        let event = ImplicitFeedbackEvent(
            signal: .showAutoSkipDisabled,
            analysisAssetId: "asset-nulls",
            podcastId: nil,
            spanId: nil
        )
        try await feedbackStore.record(event)

        // Load via the raw store to verify nulls persisted.
        let loaded = try await analysisStore.loadImplicitFeedbackEvents(analysisAssetId: "asset-nulls")
        XCTAssertEqual(loaded.count, 1)
        XCTAssertNil(loaded[0].podcastId)
        XCTAssertNil(loaded[0].spanId)
        XCTAssertEqual(loaded[0].weight, 0.3, accuracy: 1e-9)
    }
}
