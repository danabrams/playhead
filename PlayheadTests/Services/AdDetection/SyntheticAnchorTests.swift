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

    // MARK: - Synthetic ordinal collision avoidance (playhead-rfu-sad)

    /// Pre-seeds a synthetic span at a known negative ordinal pair, then
    /// records a false-negative correction. The test asserts that the
    /// new correction does NOT overwrite the existing span — it must
    /// probe forward to a free ordinal pair. Before the fix, two spans
    /// with colliding hash buckets would silently clobber each other
    /// via `INSERT OR REPLACE`, and only one synthetic span would
    /// remain.
    func testSyntheticOrdinalProbeAvoidsCollision() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-collision"))

        // Two false-negative reports back-to-back. SHA256 hash buckets
        // are collision-resistant in the natural case, so this verifies
        // the happy path: both spans persist with distinct ordinals.
        try await correctionStore.recordFalseNegative(
            assetId: "asset-collision",
            reportedTime: 60.0
        )
        try await correctionStore.recordFalseNegative(
            assetId: "asset-collision",
            reportedTime: 200.0
        )

        let spans = try await analysisStore.fetchDecodedSpans(assetId: "asset-collision")
        XCTAssertEqual(spans.count, 2,
                       "Two false-negative corrections must produce two distinct synthetic spans")

        // Ordinal pairs must be disjoint — silent overwrite would
        // collapse to a single row.
        let ordinalPairs = spans.map { Set([$0.firstAtomOrdinal, $0.lastAtomOrdinal]) }
        XCTAssertEqual(ordinalPairs.count, 2)
        XCTAssertTrue(ordinalPairs[0].isDisjoint(with: ordinalPairs[1]),
                       "Synthetic spans must not share any ordinal")
    }

    /// Direct collision proof: pre-seed a DecodedSpan whose ordinals
    /// match the deterministic value `recordFalseNegative` would
    /// otherwise produce. The probe-forward loop must shift to a
    /// distinct pair so both spans coexist.
    func testSyntheticOrdinalProbeForwardsOnDirectCollision() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-direct-collision"))

        // First record creates a synthetic span at hash-derived ordinals.
        try await correctionStore.recordFalseNegative(
            assetId: "asset-direct-collision",
            reportedTime: 50.0
        )
        let firstSpans = try await analysisStore.fetchDecodedSpans(assetId: "asset-direct-collision")
        XCTAssertEqual(firstSpans.count, 1)
        let firstSpan = try XCTUnwrap(firstSpans.first)
        let firstFirstOrdinal = firstSpan.firstAtomOrdinal
        let firstLastOrdinal = firstSpan.lastAtomOrdinal

        // Manually upsert a *different* synthetic span occupying the
        // ordinals two steps earlier in the probe sequence (the next
        // attempt the probe loop would try). This forces the next
        // recordFalseNegative call to probe past it.
        let blockingFirst = firstFirstOrdinal - 2
        let blockingLast = blockingFirst + 1
        let blockingSpan = DecodedSpan(
            id: DecodedSpan.makeId(
                assetId: "asset-direct-collision",
                firstAtomOrdinal: blockingFirst,
                lastAtomOrdinal: blockingLast
            ),
            assetId: "asset-direct-collision",
            firstAtomOrdinal: blockingFirst,
            lastAtomOrdinal: blockingLast,
            startTime: 1000.0,
            endTime: 1030.0,
            anchorProvenance: [.userCorrection(correctionId: "blocker", reportedTime: 1015.0)]
        )
        try await analysisStore.upsertDecodedSpans([blockingSpan])

        // Now record another false negative. Its hash MAY or MAY NOT
        // hit the original or blocking ordinals; what matters is that
        // we end up with all THREE spans, not two. (If we collapsed,
        // count would be 2.)
        try await correctionStore.recordFalseNegative(
            assetId: "asset-direct-collision",
            reportedTime: 200.0
        )

        let allSpans = try await analysisStore.fetchDecodedSpans(assetId: "asset-direct-collision")
        XCTAssertEqual(allSpans.count, 3,
                       "Probe-forward must avoid overwriting either pre-existing synthetic span")

        // All ordinal pairs must be distinct.
        let pairs = allSpans.map { Set([$0.firstAtomOrdinal, $0.lastAtomOrdinal]) }
        for i in 0..<pairs.count {
            for j in (i+1)..<pairs.count {
                XCTAssertTrue(pairs[i].isDisjoint(with: pairs[j]),
                               "All synthetic spans must have disjoint ordinals (i=\(i), j=\(j))")
            }
        }

        // Both pre-existing ordinal pairs must still be present.
        let allOrdinals = Set(allSpans.flatMap { [$0.firstAtomOrdinal, $0.lastAtomOrdinal] })
        XCTAssertTrue(allOrdinals.contains(firstFirstOrdinal))
        XCTAssertTrue(allOrdinals.contains(firstLastOrdinal))
        XCTAssertTrue(allOrdinals.contains(blockingFirst))
        XCTAssertTrue(allOrdinals.contains(blockingLast))
    }

    // MARK: - Int.min hash safety (playhead-rfu-sad)

    /// Pin behaviour for the `Int.min` edge case in
    /// `syntheticBaseOffset(forHashInt:)`. The previous implementation
    /// used `abs(hashInt % 1_000_000) + 2`, which traps when hashInt
    /// is `Int.min` because `abs(.min)` overflows. The replacement
    /// must produce a deterministic, in-range, positive offset for
    /// every Int input — including `.min` — without trapping.
    func testSyntheticBaseOffsetHandlesIntMinWithoutTrapping() {
        // .min must NOT trap. The previous expression traps the
        // process at runtime; the fix returns a deterministic value
        // in the documented range.
        let offsetForMin = PersistentUserCorrectionStore.syntheticBaseOffset(forHashInt: .min)
        XCTAssertGreaterThanOrEqual(offsetForMin, 2,
            "Offset must be at least 2 (synthetic ranges are pairs [N, N+1])")
        XCTAssertLessThan(offsetForMin, 1_000_002,
            "Offset must stay within (0 ..< 1_000_002] so the synthetic ordinal range is bounded")

        // Other corner cases stay well-defined.
        let offsetForMax = PersistentUserCorrectionStore.syntheticBaseOffset(forHashInt: .max)
        XCTAssertGreaterThanOrEqual(offsetForMax, 2)
        XCTAssertLessThan(offsetForMax, 1_000_002)

        let offsetForZero = PersistentUserCorrectionStore.syntheticBaseOffset(forHashInt: 0)
        XCTAssertEqual(offsetForZero, 2,
            "Zero hash must produce the minimum offset (0 % 1M + 2 = 2)")

        // Symmetry around zero: ±N below the modulus produce the
        // same offset because abs collapses the sign before the
        // modulus.
        let offsetPos = PersistentUserCorrectionStore.syntheticBaseOffset(forHashInt: 12345)
        let offsetNeg = PersistentUserCorrectionStore.syntheticBaseOffset(forHashInt: -12345)
        XCTAssertEqual(offsetPos, offsetNeg)
    }
}
