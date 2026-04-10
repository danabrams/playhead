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

    // MARK: - NoOpUserCorrectionStore

    func testNoOpRecordVetoDoesNotThrow() async {
        let noop = NoOpUserCorrectionStore()
        let span = makeTestSpan()
        // Must not throw or crash.
        await noop.recordVeto(span: span, timeRange: 0.0...30.0)
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
        await correctionStore.recordVeto(span: span, timeRange: 10.0...40.0)

        let events = try await correctionStore.activeCorrections(for: "asset-veto")
        XCTAssertEqual(events.count, 1)
        let event = try XCTUnwrap(events.first)
        XCTAssertEqual(event.scope, "exactSpan:asset-veto:2:8")
        XCTAssertEqual(event.source, .manualVeto)
    }

    // MARK: - CorrectionScope: colon-in-value round-trip

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
        await correctionStore.recordVeto(span: span, timeRange: 10.0...40.0)

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
}

// MARK: - Test Helpers

private func makeTestAsset(id: String) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: "ep-\(id)",
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///tmp/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
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
