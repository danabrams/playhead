// DesignTokenCornerRadiusTests.swift
// Verifies the CornerRadius tokens match the bead spec: small=4, medium=8, large=12.

import XCTest
@testable import Playhead

final class DesignTokenCornerRadiusTests: XCTestCase {

    func testSemanticCornerRadiiMatchSpec() {
        XCTAssertEqual(CornerRadius.small, 4)
        XCTAssertEqual(CornerRadius.medium, 8)
        XCTAssertEqual(CornerRadius.large, 12)
    }

    func testLegacyAliasesResolveToSemanticValues() {
        XCTAssertEqual(CornerRadius.sm, CornerRadius.small)
        XCTAssertEqual(CornerRadius.md, CornerRadius.medium)
        XCTAssertEqual(CornerRadius.lg, CornerRadius.large)
    }
}
