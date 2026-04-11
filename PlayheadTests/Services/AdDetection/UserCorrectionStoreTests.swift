// UserCorrectionStoreTests.swift
// Phase 7 (playhead-4my.7.1): Tests for CorrectionScope, correctionDecayWeight,
// PersistentUserCorrectionStore, and the v6 schema migration.

import XCTest
import SQLite3
@testable import Playhead

final class UserCorrectionStoreTests: XCTestCase {

    // MARK: - CorrectionScope Serialization Round-Trip

    func testExactSpanSerializationRoundTrip() {
        let scope = CorrectionScope.exactSpan(assetId: "asset-abc", ordinalRange: 3...17)
        let serialized = scope.serialized
        let deserialized = CorrectionScope.deserialize(serialized)
        XCTAssertEqual(deserialized, scope, "exactSpan must round-trip through serialization")
    }

    func testSponsorOnShowSerializationRoundTrip() {
        let scope = CorrectionScope.sponsorOnShow(podcastId: "pod-123", sponsor: "BrandName")
        let serialized = scope.serialized
        let deserialized = CorrectionScope.deserialize(serialized)
        XCTAssertEqual(deserialized, scope, "sponsorOnShow must round-trip through serialization")
    }

    func testPhraseOnShowSerializationRoundTrip() {
        let scope = CorrectionScope.phraseOnShow(podcastId: "pod-xyz", phrase: "go to slash promo")
        let serialized = scope.serialized
        let deserialized = CorrectionScope.deserialize(serialized)
        XCTAssertEqual(deserialized, scope, "phraseOnShow must round-trip through serialization")
    }

    func testCampaignOnShowSerializationRoundTrip() {
        let scope = CorrectionScope.campaignOnShow(podcastId: "pod-q7r", campaign: "spring-sale-2026")
        let serialized = scope.serialized
        let deserialized = CorrectionScope.deserialize(serialized)
        XCTAssertEqual(deserialized, scope, "campaignOnShow must round-trip through serialization")
    }

    func testDeserializeMalformedStringReturnsNil() {
        XCTAssertNil(CorrectionScope.deserialize(""))
        XCTAssertNil(CorrectionScope.deserialize("unknownType:foo:bar"))
        XCTAssertNil(CorrectionScope.deserialize("exactSpan:assetOnly"))
        XCTAssertNil(CorrectionScope.deserialize("exactSpan:asset:notAnInt:10"))
    }

    func testEmptyAssetIdRoundTrips() {
        // Empty assetId serializes to "exactSpan::0:5". The split with
        // omittingEmptySubsequences: false preserves the empty string,
        // so this round-trips correctly.
        let original = CorrectionScope.exactSpan(assetId: "", ordinalRange: 0...5)
        let deserialized = CorrectionScope.deserialize(original.serialized)
        XCTAssertEqual(deserialized, original,
            "Empty assetId should round-trip through serialization")
    }

    func testExactSpanSerializedFormat() {
        let scope = CorrectionScope.exactSpan(assetId: "my-asset", ordinalRange: 0...5)
        XCTAssertEqual(scope.serialized, "exactSpan:my-asset:0:5")
    }

    // MARK: - CorrectionScope Equatable

    func testScopeEquality() {
        let a = CorrectionScope.exactSpan(assetId: "a", ordinalRange: 1...10)
        let b = CorrectionScope.exactSpan(assetId: "a", ordinalRange: 1...10)
        let c = CorrectionScope.exactSpan(assetId: "a", ordinalRange: 1...11)
        XCTAssertEqual(a, b)
        XCTAssertNotEqual(a, c)
    }

    // MARK: - correctionDecayWeight

    func testDecayWeightAtZeroDays() {
        let weight = correctionDecayWeight(ageDays: 0)
        XCTAssertEqual(weight, 1.0, accuracy: 1e-9)
    }

    func testDecayWeightAt45Days() {
        let weight = correctionDecayWeight(ageDays: 45)
        // 1.0 - (45/180) = 1.0 - 0.25 = 0.75
        XCTAssertEqual(weight, 0.75, accuracy: 1e-9)
    }

    func testDecayWeightAt90Days() {
        let weight = correctionDecayWeight(ageDays: 90)
        // 1.0 - (90/180) = 0.5
        XCTAssertEqual(weight, 0.5, accuracy: 1e-9)
    }

    func testDecayWeightAt180Days() {
        let weight = correctionDecayWeight(ageDays: 180)
        // 1.0 - (180/180) = 0.0, clamped to 0.1
        XCTAssertEqual(weight, 0.1, accuracy: 1e-9)
    }

    func testDecayWeightAt365Days() {
        let weight = correctionDecayWeight(ageDays: 365)
        // 1.0 - (365/180) < 0, clamped to 0.1
        XCTAssertEqual(weight, 0.1, accuracy: 1e-9)
    }

    func testDecayWeightNeverDropsBelowMinimum() {
        let weight = correctionDecayWeight(ageDays: 10_000)
        XCTAssertGreaterThanOrEqual(weight, 0.1)
    }

    func testDecayWeightNegativeAgeDaysClampedToOne() {
        // Negative ageDays (clock skew: correction has future createdAt) is
        // clamped to 1.0 at the source to prevent downstream over-weighting.
        let weight = correctionDecayWeight(ageDays: -30)
        XCTAssertEqual(weight, 1.0, accuracy: 0.001,
            "Negative ageDays must be clamped to 1.0, not exceed it")
    }

    func testCorrectionPassthroughFactorClampsFutureDatedCorrection() async throws {
        // A correction with a future createdAt (clock skew) should yield
        // passthrough factor = 0.0 (full suppression), not a negative value.
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-future"))

        let futureCreatedAt = Date().addingTimeInterval(30 * 86400) // 30 days in future
        let event = CorrectionEvent(
            analysisAssetId: "asset-future",
            scope: CorrectionScope.exactSpan(assetId: "asset-future", ordinalRange: 0...5).serialized,
            createdAt: futureCreatedAt.timeIntervalSince1970,
            source: .manualVeto  // must be false positive to affect passthrough
        )
        try await correctionStore.record(event)

        let factor = await correctionStore.correctionPassthroughFactor(for: "asset-future")
        XCTAssertGreaterThanOrEqual(factor, 0.0, "Factor must not go negative even with future-dated corrections")
        XCTAssertEqual(factor, 0.0, accuracy: 0.001, "Future-dated correction should yield full suppression (0.0)")
    }

    // MARK: - NoOpUserCorrectionStore

    func testNoOpRecordVetoDoesNotThrow() async {
        let noop = NoOpUserCorrectionStore()
        let span = makeTestSpan()
        // Must not throw or crash.
        await noop.recordVeto(span: span)
    }

    func testNoOpRecordDoesNotThrow() async throws {
        let noop = NoOpUserCorrectionStore()
        let event = CorrectionEvent(
            analysisAssetId: "asset-noop",
            scope: CorrectionScope.exactSpan(assetId: "asset-noop", ordinalRange: 0...5).serialized
        )
        try await noop.record(event)  // must not throw
    }

    // MARK: - PersistentUserCorrectionStore: record and retrieve

    func testRecordAndLoadByAssetId() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)

        // Insert a parent asset first (FK constraint).
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-persist"))

        let event = CorrectionEvent(
            analysisAssetId: "asset-persist",
            scope: CorrectionScope.exactSpan(assetId: "asset-persist", ordinalRange: 5...15).serialized,
            source: .manualVeto,
            podcastId: "pod-abc"
        )
        try await correctionStore.record(event)

        let loaded = try await correctionStore.activeCorrections(for: "asset-persist")
        XCTAssertEqual(loaded.count, 1)
        let loaded0 = try XCTUnwrap(loaded.first)
        XCTAssertEqual(loaded0.id, event.id)
        XCTAssertEqual(loaded0.scope, event.scope)
        XCTAssertEqual(loaded0.source, .manualVeto)
        XCTAssertEqual(loaded0.podcastId, "pod-abc")
    }

    func testRecordMultipleEventsLoadedInChronologicalOrder() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-order"))

        let t0 = Date().timeIntervalSince1970
        let event1 = CorrectionEvent(
            analysisAssetId: "asset-order",
            scope: CorrectionScope.exactSpan(assetId: "asset-order", ordinalRange: 0...5).serialized,
            createdAt: t0
        )
        let event2 = CorrectionEvent(
            analysisAssetId: "asset-order",
            scope: CorrectionScope.exactSpan(assetId: "asset-order", ordinalRange: 10...20).serialized,
            createdAt: t0 + 1
        )
        try await correctionStore.record(event1)
        try await correctionStore.record(event2)

        let loaded = try await correctionStore.activeCorrections(for: "asset-order")
        XCTAssertEqual(loaded.count, 2)
        XCTAssertLessThanOrEqual(loaded[0].createdAt, loaded[1].createdAt)
    }

    func testActiveCorrectionsForDifferentAssetReturnsEmpty() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-a"))
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-b"))

        let event = CorrectionEvent(
            analysisAssetId: "asset-a",
            scope: CorrectionScope.exactSpan(assetId: "asset-a", ordinalRange: 0...3).serialized
        )
        try await correctionStore.record(event)

        let loadedB = try await correctionStore.activeCorrections(for: "asset-b")
        XCTAssertTrue(loadedB.isEmpty)
    }

    // MARK: - weightedCorrections

    func testWeightedCorrectionsFreshEventHasWeightOne() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-fresh"))

        let now = Date()
        let event = CorrectionEvent(
            analysisAssetId: "asset-fresh",
            scope: CorrectionScope.exactSpan(assetId: "asset-fresh", ordinalRange: 0...5).serialized,
            createdAt: now.timeIntervalSince1970
        )
        try await correctionStore.record(event)

        let weighted = try await correctionStore.weightedCorrections(for: "asset-fresh", at: now)
        XCTAssertEqual(weighted.count, 1)
        let weight = weighted[0].1
        XCTAssertEqual(weight, 1.0, accuracy: 0.001)
    }

    func testWeightedCorrectionsAgedEventHasReducedWeight() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-aged"))

        // Pin both sides of the age calculation to avoid non-determinism from calling Date() twice.
        let createdAt = Date(timeIntervalSinceReferenceDate: 0)
        let queryDate = createdAt.addingTimeInterval(90 * 86400)

        let event = CorrectionEvent(
            analysisAssetId: "asset-aged",
            scope: CorrectionScope.exactSpan(assetId: "asset-aged", ordinalRange: 0...5).serialized,
            createdAt: createdAt.timeIntervalSince1970
        )
        try await correctionStore.record(event)

        let weighted = try await correctionStore.weightedCorrections(for: "asset-aged", at: queryDate)
        XCTAssertEqual(weighted.count, 1)
        let weight = weighted[0].1
        // 90 days: expected weight = max(0.1, 1.0 - 90/180) = 0.5
        XCTAssertEqual(weight, 0.5, accuracy: 1e-9)
    }

    // MARK: - hasActiveCorrection

    func testHasActiveCorrectionReturnsFalseOnMiss() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)

        let scope = CorrectionScope.exactSpan(assetId: "no-asset", ordinalRange: 0...5)
        let missResult = try await correctionStore.hasActiveCorrection(scope: scope)
        XCTAssertFalse(missResult)
    }

    func testHasActiveCorrectionReturnsTrueOnHit() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-hit"))

        let scope = CorrectionScope.exactSpan(assetId: "asset-hit", ordinalRange: 0...10)
        let event = CorrectionEvent(
            analysisAssetId: "asset-hit",
            scope: scope.serialized
        )
        try await correctionStore.record(event)

        let hitResult = try await correctionStore.hasActiveCorrection(scope: scope)
        XCTAssertTrue(hitResult)
    }

    func testHasActiveCorrectionDistinguishesDifferentScopes() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-scope"))

        let exactScope = CorrectionScope.exactSpan(assetId: "asset-scope", ordinalRange: 0...5)
        let event = CorrectionEvent(
            analysisAssetId: "asset-scope",
            scope: exactScope.serialized
        )
        try await correctionStore.record(event)

        // Exact scope hit.
        let hitResult = try await correctionStore.hasActiveCorrection(scope: exactScope)
        XCTAssertTrue(hitResult)

        // Different scope — should miss.
        let otherScope = CorrectionScope.sponsorOnShow(podcastId: "pod-x", sponsor: "Brand")
        let missResult = try await correctionStore.hasActiveCorrection(scope: otherScope)
        XCTAssertFalse(missResult)
    }

    // MARK: - Schema v6 migration: new columns exist

    func testSchemaV6MigrationAddsCorrectionEventsTable() async throws {
        let dir = try makeTempDir(prefix: "UserCorrectionStoreTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let version = try await store.schemaVersion()
        XCTAssertEqual(version, 6)
        XCTAssertTrue(try probeTableExists(in: dir, table: "correction_events"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "source"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "podcastId"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "scope"))
    }

    func testSchemaV5DatabaseUpgradesToV6() async throws {
        let dir = try makeTempDir(prefix: "UserCorrectionStoreTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        // Seed at v5.
        try seedSchemaVersion(5, in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let version = try await store.schemaVersion()
        XCTAssertEqual(version, 6)
        XCTAssertTrue(try probeTableExists(in: dir, table: "correction_events"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "source"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "podcastId"))
    }

    /// Exercises the real v5→v6 upgrade path where the old-schema correction_events
    /// table (with correctionScope, atomOrdinalRange, evidenceJSON columns) already
    /// exists on disk. Verifies that the migration rebuilds the table, migrates data
    /// from correctionScope → scope, and that CRUD works on the new schema.
    func testSchemaV5WithOldCorrectionEventsTableUpgradesToV6() async throws {
        let dir = try makeTempDir(prefix: "UserCorrectionStoreTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        // Seed v5 schema version and the old-schema correction_events table.
        try seedSchemaVersion(5, in: dir)
        try seedOldCorrectionEventsTable(in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Schema should be at v6.
        let version = try await store.schemaVersion()
        XCTAssertEqual(version, 6)

        // The new `scope` column must exist; the old `correctionScope` must not.
        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "scope"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "source"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "podcastId"))
        XCTAssertFalse(try probeColumnExists(in: dir, table: "correction_events", column: "correctionScope"),
                       "Old correctionScope column must be removed after table rebuild")
        XCTAssertFalse(try probeColumnExists(in: dir, table: "correction_events", column: "atomOrdinalRange"),
                       "Old atomOrdinalRange column must be removed after table rebuild")
        XCTAssertFalse(try probeColumnExists(in: dir, table: "correction_events", column: "evidenceJSON"),
                       "Old evidenceJSON column must be removed after table rebuild")

        // Verify the pre-existing row's data was migrated (correctionScope → scope).
        let loaded = try await store.loadCorrectionEvents(analysisAssetId: "asset-old-v5")
        XCTAssertEqual(loaded.count, 1)
        XCTAssertEqual(loaded[0].id, "old-event-1")
        XCTAssertEqual(loaded[0].scope, "exactSpan:asset-old-v5:10:25")
        // source and podcastId were not in v5, so they should be nil.
        XCTAssertNil(loaded[0].source)
        XCTAssertNil(loaded[0].podcastId)
    }

    /// Verifies CRUD round-trips on a database upgraded from old-schema correction_events.
    func testCRUDWorksAfterOldSchemaUpgrade() async throws {
        let dir = try makeTempDir(prefix: "UserCorrectionStoreTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        try seedSchemaVersion(5, in: dir)
        try seedOldCorrectionEventsTable(in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Insert a parent asset so the FK holds.
        try await store.insertAsset(makeTestAsset(id: "asset-crud-upgrade"))

        // Write a new event using the v6 API.
        let event = CorrectionEvent(
            analysisAssetId: "asset-crud-upgrade",
            scope: CorrectionScope.exactSpan(assetId: "asset-crud-upgrade", ordinalRange: 0...5).serialized,
            source: .manualVeto,
            podcastId: "pod-upgrade"
        )
        try await store.appendCorrectionEvent(event)

        // Read it back.
        let loaded = try await store.loadCorrectionEvents(analysisAssetId: "asset-crud-upgrade")
        XCTAssertEqual(loaded.count, 1)
        XCTAssertEqual(loaded[0].scope, "exactSpan:asset-crud-upgrade:0:5")
        XCTAssertEqual(loaded[0].source, .manualVeto)
        XCTAssertEqual(loaded[0].podcastId, "pod-upgrade")

        // hasAnyCorrectionEvent should find it.
        let found = try await store.hasAnyCorrectionEvent(
            withScope: CorrectionScope.exactSpan(assetId: "asset-crud-upgrade", ordinalRange: 0...5).serialized
        )
        XCTAssertTrue(found)
    }

    /// Verifies the v5→v6 migration succeeds when old correction_events rows
    /// reference an analysisAssetId that no longer exists in analysis_assets.
    /// Orphaned rows must be silently discarded (not cause an FK violation crash).
    func testSchemaV5UpgradeDiscardsOrphanedCorrectionEvents() async throws {
        let dir = try makeTempDir(prefix: "UserCorrectionStoreTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        try seedSchemaVersion(5, in: dir)
        try seedOldCorrectionEventsTableWithOrphan(in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        // This must not crash with an FK constraint violation.
        try await store.migrate()

        let version = try await store.schemaVersion()
        XCTAssertEqual(version, 6)

        // The valid row (referencing "asset-valid") should survive.
        let valid = try await store.loadCorrectionEvents(analysisAssetId: "asset-valid")
        XCTAssertEqual(valid.count, 1, "Valid correction event must survive migration")
        XCTAssertEqual(valid[0].id, "valid-event")

        // The orphaned row (referencing "asset-deleted") should be discarded.
        let orphaned = try await store.loadCorrectionEvents(analysisAssetId: "asset-deleted")
        XCTAssertEqual(orphaned.count, 0, "Orphaned correction event must be discarded during migration")
    }

    func testFreshDatabaseReachesV6() async throws {
        let dir = try makeTempDir(prefix: "UserCorrectionStoreTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let version = try await store.schemaVersion()
        XCTAssertEqual(version, 6)
    }

    // MARK: - CorrectionEvent source and podcastId round-trip

    func testCorrectionEventSourceRoundTrip() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-source"))

        let event = CorrectionEvent(
            analysisAssetId: "asset-source",
            scope: CorrectionScope.exactSpan(assetId: "asset-source", ordinalRange: 0...5).serialized,
            source: .listenRevert,
            podcastId: "pod-roundtrip"
        )
        try await correctionStore.record(event)

        let loaded = try await correctionStore.activeCorrections(for: "asset-source")
        XCTAssertEqual(loaded.count, 1)
        XCTAssertEqual(loaded[0].source, .listenRevert)
        XCTAssertEqual(loaded[0].podcastId, "pod-roundtrip")
    }

    func testCorrectionEventNilSourceAndPodcastId() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-nil-fields"))

        let event = CorrectionEvent(
            analysisAssetId: "asset-nil-fields",
            scope: CorrectionScope.phraseOnShow(podcastId: "p", phrase: "save at slash").serialized,
            source: nil,
            podcastId: nil
        )
        try await correctionStore.record(event)

        let loaded = try await correctionStore.activeCorrections(for: "asset-nil-fields")
        XCTAssertEqual(loaded.count, 1)
        XCTAssertNil(loaded[0].source)
        XCTAssertNil(loaded[0].podcastId)
    }

    // MARK: - recordVeto integration

    func testRecordVetoWritesExactSpanEvent() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-veto"))

        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-veto", firstAtomOrdinal: 2, lastAtomOrdinal: 8),
            assetId: "asset-veto",
            firstAtomOrdinal: 2,
            lastAtomOrdinal: 8,
            startTime: 10.0,
            endTime: 40.0,
            anchorProvenance: []
        )
        await correctionStore.recordVeto(span: span)

        let events = try await correctionStore.activeCorrections(for: "asset-veto")
        XCTAssertEqual(events.count, 1)
        let event = try XCTUnwrap(events.first)
        XCTAssertEqual(event.scope, "exactSpan:asset-veto:2:8")
        XCTAssertEqual(event.source, .manualVeto)
    }

    // MARK: - CorrectionScope: colon-in-value round-trip

    func testExactSpanWithColonInAssetIdRoundTrip() {
        let scope = CorrectionScope.exactSpan(assetId: "asset:with:colons", ordinalRange: 10...25)
        let deserialized = CorrectionScope.deserialize(scope.serialized)
        XCTAssertEqual(deserialized, scope, "exactSpan with colons in assetId must round-trip")
    }

    func testSponsorWithColonSerializationRoundTrip() {
        let scope = CorrectionScope.sponsorOnShow(podcastId: "pod-123", sponsor: "Squarespace: Build It")
        let deserialized = CorrectionScope.deserialize(scope.serialized)
        XCTAssertEqual(deserialized, scope, "sponsorOnShow with colon in sponsor must round-trip")
    }

    func testPhraseWithColonSerializationRoundTrip() {
        let scope = CorrectionScope.phraseOnShow(podcastId: "pod-456", phrase: "go to https://brand.com/promo")
        let deserialized = CorrectionScope.deserialize(scope.serialized)
        XCTAssertEqual(deserialized, scope, "phraseOnShow with colon in phrase must round-trip")
    }

    func testCampaignWithColonSerializationRoundTrip() {
        let scope = CorrectionScope.campaignOnShow(podcastId: "pod-789", campaign: "spring:sale:2026")
        let deserialized = CorrectionScope.deserialize(scope.serialized)
        XCTAssertEqual(deserialized, scope, "campaignOnShow with multiple colons in campaign must round-trip")
    }

    // MARK: - correctionPassthroughFactor

    func testCorrectionPassthroughFactorFreshCorrectionReturnsZero() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-fresh-factor"))

        let event = CorrectionEvent(
            analysisAssetId: "asset-fresh-factor",
            scope: CorrectionScope.exactSpan(assetId: "asset-fresh-factor", ordinalRange: 0...5).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        let factor = await correctionStore.correctionPassthroughFactor(for: "asset-fresh-factor")
        // Fresh correction: decayWeight ~ 1.0, so factor = 1.0 - 1.0 = 0.0 (full suppression).
        XCTAssertEqual(factor, 0.0, accuracy: 0.01)
    }

    func testCorrectionPassthroughFactor180DayOldReturnsApproximatelyPointNine() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-old-factor"))

        let createdAt = Date().addingTimeInterval(-180 * 86400)
        let event = CorrectionEvent(
            analysisAssetId: "asset-old-factor",
            scope: CorrectionScope.exactSpan(assetId: "asset-old-factor", ordinalRange: 0...5).serialized,
            createdAt: createdAt.timeIntervalSince1970,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        let factor = await correctionStore.correctionPassthroughFactor(for: "asset-old-factor")
        // 180-day-old correction: decayWeight = max(0.1, 1.0 - 180/180) = 0.1, factor = 1.0 - 0.1 = 0.9.
        XCTAssertEqual(factor, 0.9, accuracy: 0.01)
    }

    func testCorrectionPassthroughFactorNoCorrectionReturnsOne() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)

        let factor = await correctionStore.correctionPassthroughFactor(for: "nonexistent-asset")
        // No corrections → 1.0 (no suppression).
        XCTAssertEqual(factor, 1.0, accuracy: 1e-9)
    }

    // MARK: - recordVeto: brandSpan evidence writes sponsorOnShow

    func testRecordVetoWithBrandSpanWritesTwoEvents() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-brand"))

        let brandEntry = EvidenceEntry(
            evidenceRef: 0,
            category: .brandSpan,
            matchedText: "Squarespace",
            normalizedText: "squarespace",
            atomOrdinal: 5,
            startTime: 10.0,
            endTime: 12.0
        )
        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-brand", firstAtomOrdinal: 3, lastAtomOrdinal: 9),
            assetId: "asset-brand",
            firstAtomOrdinal: 3,
            lastAtomOrdinal: 9,
            startTime: 10.0,
            endTime: 40.0,
            anchorProvenance: [.evidenceCatalog(entry: brandEntry)]
        )
        await correctionStore.recordVeto(span: span)

        let events = try await correctionStore.activeCorrections(for: "asset-brand")
        XCTAssertEqual(events.count, 2, "recordVeto must write exactSpan + sponsorOnShow when brandSpan evidence is present")

        let scopes = events.map { $0.scope }
        XCTAssertTrue(scopes.contains("exactSpan:asset-brand:3:9"), "exactSpan event must be written")
        XCTAssertTrue(
            scopes.contains(where: { $0.hasPrefix("sponsorOnShow:") && $0.hasSuffix(":squarespace") }),
            "sponsorOnShow event using normalizedText must be written"
        )
    }

    // MARK: - Idempotent insert (INSERT OR IGNORE)

    func testDuplicateEventIdIsIgnored() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-idem"))

        let event = CorrectionEvent(
            id: "fixed-id",
            analysisAssetId: "asset-idem",
            scope: CorrectionScope.exactSpan(assetId: "asset-idem", ordinalRange: 0...5).serialized
        )
        try await correctionStore.record(event)
        // Second insert with the same id must be silently ignored.
        try await correctionStore.record(event)

        let loaded = try await correctionStore.activeCorrections(for: "asset-idem")
        XCTAssertEqual(loaded.count, 1)
    }

    // MARK: - CASCADE delete

    func testDeletingAssetCascadesCorrections() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-cascade"))

        let event = CorrectionEvent(
            analysisAssetId: "asset-cascade",
            scope: CorrectionScope.exactSpan(assetId: "asset-cascade", ordinalRange: 0...5).serialized,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        // Verify the event exists.
        let before = try await correctionStore.activeCorrections(for: "asset-cascade")
        XCTAssertEqual(before.count, 1)

        // Delete the parent asset.
        try await analysisStore.deleteAsset(id: "asset-cascade")

        // Correction events should be cascade-deleted.
        let after = try await correctionStore.activeCorrections(for: "asset-cascade")
        XCTAssertEqual(after.count, 0, "Correction events must be cascade-deleted when the parent asset is removed")
    }
}

// MARK: - Test Helpers

// makeTestAsset(id:) is defined in TestHelpers.swift

/// Seeds the old-schema correction_events table (as shipped in 0.6) with a sample row.
/// Used to test the v5→v6 migration path where the table already exists with
/// the `correctionScope`, `atomOrdinalRange`, and `evidenceJSON` columns.
private func seedOldCorrectionEventsTable(in directory: URL) throws {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEvents", code: 1)
    }
    defer { sqlite3_close_v2(db) }

    // Create the parent table so FKs can be satisfied later.
    let createAssets = """
        CREATE TABLE IF NOT EXISTS analysis_assets (
            id TEXT PRIMARY KEY,
            episodeId TEXT NOT NULL,
            assetFingerprint TEXT NOT NULL,
            weakFingerprint TEXT,
            sourceURL TEXT NOT NULL,
            featureCoverageEndTime REAL,
            fastTranscriptCoverageEndTime REAL,
            confirmedAdCoverageEndTime REAL,
            analysisState TEXT NOT NULL,
            analysisVersion INTEGER NOT NULL,
            capabilitySnapshot TEXT
        )
        """
    guard sqlite3_exec(db, createAssets, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEvents", code: 2)
    }

    // Insert a parent asset row.
    let insertAsset = """
        INSERT OR IGNORE INTO analysis_assets
        (id, episodeId, assetFingerprint, sourceURL, analysisState, analysisVersion)
        VALUES ('asset-old-v5', 'ep-old-v5', 'fp-old-v5', 'file:///tmp/old.m4a', 'new', 1)
        """
    guard sqlite3_exec(db, insertAsset, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEvents", code: 3)
    }

    // Create the OLD-schema correction_events table (matching 0.6 release).
    let createOldTable = """
        CREATE TABLE IF NOT EXISTS correction_events (
            id                  TEXT PRIMARY KEY,
            analysisAssetId     TEXT NOT NULL,
            correctionScope     TEXT NOT NULL,
            atomOrdinalRange    TEXT NOT NULL,
            evidenceJSON        TEXT NOT NULL,
            createdAt           REAL NOT NULL
        )
        """
    guard sqlite3_exec(db, createOldTable, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEvents", code: 4)
    }
    guard sqlite3_exec(db, "CREATE INDEX IF NOT EXISTS idx_ce_asset ON correction_events(analysisAssetId)", nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEvents", code: 5)
    }

    // Insert a sample row in the old format.
    let insertOldRow = """
        INSERT INTO correction_events
        (id, analysisAssetId, correctionScope, atomOrdinalRange, evidenceJSON, createdAt)
        VALUES ('old-event-1', 'asset-old-v5', 'exactSpan:asset-old-v5:10:25', '[10, 25]', '{"reason":"not_an_ad"}', 1712700000.0)
        """
    guard sqlite3_exec(db, insertOldRow, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEvents", code: 6)
    }
}

/// Seeds an old-schema correction_events table with one valid row and one orphaned row
/// (whose analysisAssetId does not exist in analysis_assets).
private func seedOldCorrectionEventsTableWithOrphan(in directory: URL) throws {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEventsOrphan", code: 1)
    }
    defer { sqlite3_close_v2(db) }

    // Create parent table and insert only one asset ("asset-valid").
    guard sqlite3_exec(db, """
        CREATE TABLE IF NOT EXISTS analysis_assets (
            id TEXT PRIMARY KEY,
            episodeId TEXT NOT NULL,
            assetFingerprint TEXT NOT NULL,
            weakFingerprint TEXT,
            sourceURL TEXT NOT NULL,
            featureCoverageEndTime REAL,
            fastTranscriptCoverageEndTime REAL,
            confirmedAdCoverageEndTime REAL,
            analysisState TEXT NOT NULL,
            analysisVersion INTEGER NOT NULL,
            capabilitySnapshot TEXT
        )
        """, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEventsOrphan", code: 2)
    }
    guard sqlite3_exec(db, """
        INSERT OR IGNORE INTO analysis_assets
        (id, episodeId, assetFingerprint, sourceURL, analysisState, analysisVersion)
        VALUES ('asset-valid', 'ep-valid', 'fp-valid', 'file:///tmp/valid.m4a', 'new', 1)
        """, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEventsOrphan", code: 3)
    }

    // Create old-schema correction_events (no FK constraint, matching 0.6).
    guard sqlite3_exec(db, """
        CREATE TABLE IF NOT EXISTS correction_events (
            id                  TEXT PRIMARY KEY,
            analysisAssetId     TEXT NOT NULL,
            correctionScope     TEXT NOT NULL,
            atomOrdinalRange    TEXT NOT NULL,
            evidenceJSON        TEXT NOT NULL,
            createdAt           REAL NOT NULL
        )
        """, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEventsOrphan", code: 4)
    }

    // Valid row: references "asset-valid" which exists.
    guard sqlite3_exec(db, """
        INSERT INTO correction_events
        (id, analysisAssetId, correctionScope, atomOrdinalRange, evidenceJSON, createdAt)
        VALUES ('valid-event', 'asset-valid', 'exactSpan:asset-valid:5:15', '[5, 15]', '{}', 1712700000.0)
        """, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEventsOrphan", code: 5)
    }

    // Orphaned row: references "asset-deleted" which does NOT exist in analysis_assets.
    guard sqlite3_exec(db, """
        INSERT INTO correction_events
        (id, analysisAssetId, correctionScope, atomOrdinalRange, evidenceJSON, createdAt)
        VALUES ('orphan-event', 'asset-deleted', 'exactSpan:asset-deleted:0:10', '[0, 10]', '{}', 1712700000.0)
        """, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedOldCorrectionEventsOrphan", code: 6)
    }
}

private func makeTestSpan(
    assetId: String = "asset-span",
    first: Int = 0,
    last: Int = 10
) -> DecodedSpan {
    DecodedSpan(
        id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: first, lastAtomOrdinal: last),
        assetId: assetId,
        firstAtomOrdinal: first,
        lastAtomOrdinal: last,
        startTime: 0,
        endTime: 30,
        anchorProvenance: []
    )
}
