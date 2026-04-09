// DesignTokenSpacingTests.swift
// Verifies the 4pt spacing scale matches the "Quiet Instrument" spec.

import XCTest
@testable import Playhead

final class DesignTokenSpacingTests: XCTestCase {

    func testSpacingScaleIsExact4ptGrid() {
        // Spec: spacing scale = {4, 8, 12, 16, 24, 32, 48}
        let expected: [CGFloat] = [4, 8, 12, 16, 24, 32, 48]
        let actual: [CGFloat] = [
            Spacing.xxs,
            Spacing.xs,
            Spacing.sm,
            Spacing.md,
            Spacing.lg,
            Spacing.xl,
            Spacing.xxl
        ]
        XCTAssertEqual(actual, expected, "Spacing scale must match the 4pt grid exactly")
    }

    func testSpacingValuesAreAllPositive() {
        let all: [CGFloat] = [
            Spacing.xxs, Spacing.xs, Spacing.sm, Spacing.md,
            Spacing.lg, Spacing.xl, Spacing.xxl
        ]
        for value in all {
            XCTAssertGreaterThan(value, 0)
        }
    }

    func testSpacingScaleIsMonotonicallyIncreasing() {
        let scale: [CGFloat] = [
            Spacing.xxs, Spacing.xs, Spacing.sm, Spacing.md,
            Spacing.lg, Spacing.xl, Spacing.xxl
        ]
        for i in 1..<scale.count {
            XCTAssertGreaterThan(scale[i], scale[i - 1])
        }
    }
}
