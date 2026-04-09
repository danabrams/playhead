// DesignTokenForbiddenPatternsTests.swift
// Enforces "Quiet Instrument" prohibitions via the inventory enum exposed
// by the Design module. No purple, gradient, shimmer, sparkle, spring, or
// bounce anywhere in the token set.

import XCTest
@testable import Playhead

final class DesignTokenForbiddenPatternsTests: XCTestCase {

    private static let forbidden: [String] = [
        "purple", "gradient", "shimmer", "sparkle",
        "spring", "bounce", "bouncy"
    ]

    func testInventoryContainsNoForbiddenNames() {
        for entry in DesignTokenInventory.all {
            let lower = entry.name.lowercased()
            for word in Self.forbidden {
                XCTAssertFalse(
                    lower.contains(word),
                    "Design token '\(entry.name)' (category \(entry.category)) contains forbidden keyword '\(word)'"
                )
            }
        }
    }

    func testInventoryIsNonEmpty() {
        XCTAssertFalse(DesignTokenInventory.all.isEmpty,
                       "DesignTokenInventory.all must enumerate every token")
    }

    func testInventoryIncludesAllCategories() {
        let categories = Set(DesignTokenInventory.all.map(\.category))
        XCTAssertTrue(categories.contains(.color))
        XCTAssertTrue(categories.contains(.spacing))
        XCTAssertTrue(categories.contains(.cornerRadius))
        XCTAssertTrue(categories.contains(.motion))
        XCTAssertTrue(categories.contains(.typography))
        XCTAssertTrue(categories.contains(.haptic))
    }
}
