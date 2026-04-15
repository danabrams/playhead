// SyntheticAnchorTests.swift
// playhead-ef2.3.2: Tests for synthetic anchor creation from false-negative corrections.

import XCTest
@testable import Playhead

final class SyntheticAnchorTests: XCTestCase {

    // MARK: - AnchorRef.userCorrection basics

    func testUserCorrectionAnchorRefEquality() {
        let a = AnchorRef.userCorrection(correctionId: "c1", reportedTime: 120.0)
        let b = AnchorRef.userCorrection(correctionId: "c1", reportedTime: 120.0)
        let c = AnchorRef.userCorrection(correctionId: "c2", reportedTime: 120.0)
        XCTAssertEqual(a, b)
        XCTAssertNotEqual(a, c)
    }

    func testUserCorrectionAnchorRefCodableRoundTrip() throws {
        let original = AnchorRef.userCorrection(correctionId: "corr-abc", reportedTime: 95.5)
        let encoder = JSONEncoder()
        let data = try encoder.encode(original)
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(AnchorRef.self, from: data)
        XCTAssertEqual(decoded, original)
    }

    func testUserCorrectionAnchorRefInArrayCodableRoundTrip() throws {
        let refs: [AnchorRef] = [
            .fmConsensus(regionId: "r1", consensusStrength: 0.8),
            .userCorrection(correctionId: "c1", reportedTime: 60.0),
        ]
        let encoder = JSONEncoder()
        let data = try encoder.encode(refs)
        let decoder = JSONDecoder()
        let decoded = try decoder.decode([AnchorRef].self, from: data)
        XCTAssertEqual(decoded, refs)
    }

    // MARK: - DecodedSpan with userCorrection provenance persists and fetches

    func testDecodedSpanWithUserCorrectionProvenancePersistsAndFetches() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-synth"))

        let correctionId = "corr-fn-1"
        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-synth", firstAtomOrdinal: -1, lastAtomOrdinal: -1),
            assetId: "asset-synth",
            firstAtomOrdinal: -1,
            lastAtomOrdinal: -1,
            startTime: 100.0,
            endTime: 130.0,
            anchorProvenance: [.userCorrection(correctionId: correctionId, reportedTime: 115.0)]
        )

        try await store.upsertDecodedSpans([span])
        let fetched = try await store.fetchDecodedSpans(assetId: "asset-synth")
        XCTAssertEqual(fetched.count, 1)
        let fetchedSpan = try XCTUnwrap(fetched.first)
        XCTAssertEqual(fetchedSpan.anchorProvenance.count, 1)
        if case .userCorrection(let fetchedId, let fetchedTime) = fetchedSpan.anchorProvenance[0] {
            XCTAssertEqual(fetchedId, correctionId)
            XCTAssertEqual(fetchedTime, 115.0, accuracy: 0.001)
        } else {
            XCTFail("Expected .userCorrection provenance")
        }
    }

    // MARK: - Synthetic span creation from false-negative correction

    func testRecordFalseNegativeCreatesSyntheticSpan() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-fn"))

        // Record a false-negative correction at 120s
        try await correctionStore.recordFalseNegative(
            assetId: "asset-fn",
            reportedTime: 120.0
        )

        // A synthetic span should now be queryable
        let spans = try await analysisStore.fetchDecodedSpans(assetId: "asset-fn")
        XCTAssertEqual(spans.count, 1, "False-negative correction must create exactly one synthetic span")

        let span = try XCTUnwrap(spans.first)
        // Span should cover the reported time
        XCTAssertLessThanOrEqual(span.startTime, 120.0)
        XCTAssertGreaterThanOrEqual(span.endTime, 120.0)
        // Duration should be the fallback ±15s = 30s
        XCTAssertEqual(span.endTime - span.startTime, 30.0, accuracy: 0.001)

        // Provenance must be .userCorrection
        XCTAssertEqual(span.anchorProvenance.count, 1)
        guard case .userCorrection(_, let reportedTime) = span.anchorProvenance[0] else {
            XCTFail("Expected .userCorrection provenance on synthetic span")
            return
        }
        XCTAssertEqual(reportedTime, 120.0, accuracy: 0.001)
    }

    func testRecordFalseNegativeAlsoWritesCorrectionEvent() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-fn-event"))

        try await correctionStore.recordFalseNegative(
            assetId: "asset-fn-event",
            reportedTime: 200.0
        )

        let events = try await correctionStore.activeCorrections(for: "asset-fn-event")
        XCTAssertGreaterThanOrEqual(events.count, 1, "Must write a correction event")
        let fnEvent = events.first { $0.source == .falseNegative }
        XCTAssertNotNil(fnEvent, "Correction event must have source .falseNegative")

        // Verify the correction scope references the actual synthetic ordinals (not hardcoded).
        let spans = try await analysisStore.fetchDecodedSpans(assetId: "asset-fn-event")
        let span = try XCTUnwrap(spans.first)
        let scopeStr = try XCTUnwrap(fnEvent?.scope)
        let parsedScope = CorrectionScope.deserialize(scopeStr)
        if case .exactSpan(_, let ordinalRange) = parsedScope {
            XCTAssertEqual(ordinalRange.lowerBound, span.firstAtomOrdinal,
                           "Correction scope ordinals must match synthetic span ordinals")
            XCTAssertEqual(ordinalRange.upperBound, span.lastAtomOrdinal,
                           "Correction scope ordinals must match synthetic span ordinals")
        } else {
            XCTFail("Expected exactSpan scope, got: \(scopeStr)")
        }
    }

    func testSyntheticSpanIsEpisodeLocal() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-fn-local"))
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-other"))

        try await correctionStore.recordFalseNegative(
            assetId: "asset-fn-local",
            reportedTime: 60.0
        )

        // Other episode must have no spans
        let otherSpans = try await analysisStore.fetchDecodedSpans(assetId: "asset-other")
        XCTAssertTrue(otherSpans.isEmpty, "Synthetic span must not leak to other episodes")
    }

    func testSyntheticSpanUsesNegativeOrdinalsToAvoidClash() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-fn-ordinals"))

        try await correctionStore.recordFalseNegative(
            assetId: "asset-fn-ordinals",
            reportedTime: 300.0
        )

        let spans = try await analysisStore.fetchDecodedSpans(assetId: "asset-fn-ordinals")
        let span = try XCTUnwrap(spans.first)
        // Synthetic spans use negative ordinals so they can never collide with real atom ordinals
        XCTAssertLessThan(span.firstAtomOrdinal, 0, "Synthetic span must use negative ordinals")
        XCTAssertLessThan(span.lastAtomOrdinal, 0, "Synthetic span must use negative ordinals")
    }

    func testRecordFalseNegativeAtTimeBelowFifteenClampsStartToZero() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-fn-clamp"))

        try await correctionStore.recordFalseNegative(
            assetId: "asset-fn-clamp",
            reportedTime: 5.0
        )

        let spans = try await analysisStore.fetchDecodedSpans(assetId: "asset-fn-clamp")
        let span = try XCTUnwrap(spans.first)
        XCTAssertGreaterThanOrEqual(span.startTime, 0.0, "Start time must not go negative")
        XCTAssertLessThanOrEqual(span.startTime, 5.0)
        XCTAssertGreaterThanOrEqual(span.endTime, 5.0)
    }
}
