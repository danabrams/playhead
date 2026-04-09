// DesignTokenTypographyTests.swift
// Verifies the semantic typography roles, Dynamic Type scaling wiring,
// and fallback behaviour when the preferred font family is unavailable.

import XCTest
import SwiftUI
@testable import Playhead

final class DesignTokenTypographyTests: XCTestCase {

    // MARK: - Descriptors (testable surface)

    func testTitleDescriptor() {
        let d = AppTypography.descriptor(for: .title)
        XCTAssertEqual(d.baseSize, 28)
        XCTAssertEqual(d.family, .sans)
        XCTAssertEqual(d.textStyle, .title)
    }

    func testHeadlineDescriptor() {
        let d = AppTypography.descriptor(for: .headline)
        XCTAssertEqual(d.baseSize, 20)
        XCTAssertEqual(d.family, .sans)
        XCTAssertEqual(d.textStyle, .title3)
    }

    func testBodyDescriptor() {
        let d = AppTypography.descriptor(for: .body)
        XCTAssertEqual(d.baseSize, 17, "Body must be 17pt per spec, not 16pt")
        XCTAssertEqual(d.family, .sans)
        XCTAssertEqual(d.textStyle, .body)
    }

    func testCaptionDescriptor() {
        let d = AppTypography.descriptor(for: .caption)
        XCTAssertEqual(d.baseSize, 13)
        XCTAssertEqual(d.family, .sans)
        XCTAssertEqual(d.textStyle, .caption)
    }

    func testTimestampDescriptor() {
        let d = AppTypography.descriptor(for: .timestamp)
        XCTAssertEqual(d.baseSize, 13)
        XCTAssertEqual(d.family, .mono)
        XCTAssertEqual(d.textStyle, .caption)
    }

    func testTranscriptDescriptor() {
        let d = AppTypography.descriptor(for: .transcript)
        XCTAssertEqual(d.baseSize, 17)
        XCTAssertEqual(d.family, .serif)
        XCTAssertEqual(d.textStyle, .body)
    }

    func testTranscriptCaptionDescriptor() {
        let d = AppTypography.descriptor(for: .transcriptCaption)
        XCTAssertEqual(d.baseSize, 13)
        XCTAssertEqual(d.family, .serif)
        XCTAssertEqual(d.textStyle, .caption)
    }

    // MARK: - Dynamic Type wiring

    func testAllRolesUseDynamicTypeRelativeTextStyle() {
        for role in TypographyRole.allCases {
            let d = AppTypography.descriptor(for: role)
            // textStyle must be a real UIFont.TextStyle so SwiftUI scales it.
            XCTAssertTrue(Font.TextStyle.allCases.contains(d.textStyle),
                          "\(role) is not bound to a Dynamic Type text style")
        }
    }

    // MARK: - Fallback resolver

    func testResolverPrefersFirstAvailable() {
        let picked = FontFamilyResolver.resolve(
            candidates: ["Nonexistent Font", "Helvetica", "Zapfino"],
            availableFamilies: ["Helvetica", "Zapfino"]
        )
        XCTAssertEqual(picked, "Helvetica")
    }

    func testResolverFallsThroughWhenNoneAvailable() {
        let picked = FontFamilyResolver.resolve(
            candidates: ["Instrument Sans", "Inter Tight"],
            availableFamilies: []
        )
        XCTAssertNil(picked, "Resolver must return nil so caller can pick system font")
    }

    func testResolverSkipsUnavailableFirstChoice() {
        let picked = FontFamilyResolver.resolve(
            candidates: ["Instrument Sans", "Inter Tight", "Helvetica"],
            availableFamilies: ["Inter Tight", "Helvetica"]
        )
        XCTAssertEqual(picked, "Inter Tight")
    }
}
