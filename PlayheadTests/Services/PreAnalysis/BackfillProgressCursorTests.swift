// BackfillProgressCursorTests.swift
// Unit tests for BackfillProgressCursor clamping and monotonic merge.

import Testing

@testable import Playhead

@Suite("BackfillProgressCursor")
struct BackfillProgressCursorTests {

    @Test("processedUnitCount is clamped to non-negative")
    func testProcessedUnitCountClamp() {
        let cursor = BackfillProgressCursor(processedUnitCount: -5)
        #expect(cursor.processedUnitCount == 0)
    }

    @Test("lastProcessedUpperBoundSec is clamped to non-negative")
    func testLastProcessedUpperBoundClampNegative() {
        let cursor = BackfillProgressCursor(
            processedUnitCount: 1,
            lastProcessedUpperBoundSec: -1
        )
        #expect(cursor.lastProcessedUpperBoundSec == 0)
    }

    @Test("nil lastProcessedUpperBoundSec stays nil")
    func testLastProcessedUpperBoundNilPassthrough() {
        let cursor = BackfillProgressCursor(
            processedUnitCount: 1,
            lastProcessedUpperBoundSec: nil
        )
        #expect(cursor.lastProcessedUpperBoundSec == nil)
    }

    @Test("monotonic(from:) picks the larger of each field")
    func testMonotonicMergePicksLargerFields() {
        let older = BackfillProgressCursor(
            processedUnitCount: 5,
            lastProcessedUpperBoundSec: 60
        )
        let newer = BackfillProgressCursor(
            processedUnitCount: 8,
            lastProcessedUpperBoundSec: 90
        )

        #expect(newer.monotonic(from: older) == newer)
        #expect(older.monotonic(from: newer) == newer)
    }

    @Test("monotonic(from:) rejects regression on individual fields")
    func testMonotonicMergeMixedFields() {
        // Mixed: lhs has higher count, rhs has higher upperBound.
        let lhs = BackfillProgressCursor(
            processedUnitCount: 10,
            lastProcessedUpperBoundSec: 30
        )
        let rhs = BackfillProgressCursor(
            processedUnitCount: 4,
            lastProcessedUpperBoundSec: 90
        )

        let merged = lhs.monotonic(from: rhs)
        #expect(merged.processedUnitCount == 10)
        #expect(merged.lastProcessedUpperBoundSec == 90)
    }

    @Test("monotonic(from:) treats nil lastProcessedUpperBoundSec as the smaller value")
    func testMonotonicMergeWithNil() {
        let withTime = BackfillProgressCursor(
            processedUnitCount: 1,
            lastProcessedUpperBoundSec: 42
        )
        let withoutTime = BackfillProgressCursor(
            processedUnitCount: 1,
            lastProcessedUpperBoundSec: nil
        )

        #expect(withTime.monotonic(from: withoutTime).lastProcessedUpperBoundSec == 42)
        #expect(withoutTime.monotonic(from: withTime).lastProcessedUpperBoundSec == 42)
    }
}
