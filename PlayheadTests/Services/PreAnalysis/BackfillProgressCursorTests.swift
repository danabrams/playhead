// BackfillProgressCursorTests.swift
// Unit tests for BackfillProgressCursor clamping and monotonic merge.

import Foundation
import Testing

@testable import Playhead

@Suite("BackfillProgressCursor")
struct BackfillProgressCursorTests {

    @Test("processedPhaseCount is clamped to non-negative")
    func testProcessedPhaseCountClamp() {
        let cursor = BackfillProgressCursor(processedPhaseCount: -5)
        #expect(cursor.processedPhaseCount == 0)
    }

    @Test("lastProcessedUpperBoundSec is clamped to non-negative")
    func testLastProcessedUpperBoundClampNegative() {
        let cursor = BackfillProgressCursor(
            processedPhaseCount: 1,
            lastProcessedUpperBoundSec: -1
        )
        #expect(cursor.lastProcessedUpperBoundSec == 0)
    }

    @Test("nil lastProcessedUpperBoundSec stays nil")
    func testLastProcessedUpperBoundNilPassthrough() {
        let cursor = BackfillProgressCursor(
            processedPhaseCount: 1,
            lastProcessedUpperBoundSec: nil
        )
        #expect(cursor.lastProcessedUpperBoundSec == nil)
    }

    @Test("monotonic(from:) picks the larger of each field")
    func testMonotonicMergePicksLargerFields() {
        let older = BackfillProgressCursor(
            processedPhaseCount: 5,
            lastProcessedUpperBoundSec: 60
        )
        let newer = BackfillProgressCursor(
            processedPhaseCount: 8,
            lastProcessedUpperBoundSec: 90
        )

        #expect(newer.monotonic(from: older) == newer)
        #expect(older.monotonic(from: newer) == newer)
    }

    @Test("monotonic(from:) rejects regression on individual fields")
    func testMonotonicMergeMixedFields() {
        // Mixed: lhs has higher count, rhs has higher upperBound.
        let lhs = BackfillProgressCursor(
            processedPhaseCount: 10,
            lastProcessedUpperBoundSec: 30
        )
        let rhs = BackfillProgressCursor(
            processedPhaseCount: 4,
            lastProcessedUpperBoundSec: 90
        )

        let merged = lhs.monotonic(from: rhs)
        #expect(merged.processedPhaseCount == 10)
        #expect(merged.lastProcessedUpperBoundSec == 90)
    }

    @Test("monotonic(from:) treats nil lastProcessedUpperBoundSec as the smaller value")
    func testMonotonicMergeWithNil() {
        let withTime = BackfillProgressCursor(
            processedPhaseCount: 1,
            lastProcessedUpperBoundSec: 42
        )
        let withoutTime = BackfillProgressCursor(
            processedPhaseCount: 1,
            lastProcessedUpperBoundSec: nil
        )

        #expect(withTime.monotonic(from: withoutTime).lastProcessedUpperBoundSec == 42)
        #expect(withoutTime.monotonic(from: withTime).lastProcessedUpperBoundSec == 42)
    }

    // MARK: - JSON persistence compatibility (rename safety)

    /// The Swift property was renamed from `processedUnitCount` to
    /// `processedPhaseCount` to reflect how it is actually written (0 or 1
    /// per phase completion). The on-disk JSON key stays `processedUnitCount`
    /// so existing `backfill_jobs.progressCursorJSON` rows remain readable
    /// without a database migration. See BackfillJob.swift CodingKeys.
    @Test("encode preserves legacy processedUnitCount JSON key")
    func testEncodeUsesLegacyJSONKey() throws {
        let cursor = BackfillProgressCursor(
            processedPhaseCount: 1,
            lastProcessedUpperBoundSec: 689.82
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(cursor)
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("\"processedUnitCount\":1"))
        #expect(!json.contains("processedPhaseCount"))
    }

    @Test("decode reads legacy processedUnitCount JSON key into processedPhaseCount")
    func testDecodeReadsLegacyJSONKey() throws {
        let json = #"{"processedUnitCount":1,"lastProcessedUpperBoundSec":689.82}"#
        let cursor = try JSONDecoder().decode(
            BackfillProgressCursor.self,
            from: Data(json.utf8)
        )
        #expect(cursor.processedPhaseCount == 1)
        #expect(cursor.lastProcessedUpperBoundSec == 689.82)
    }
}
