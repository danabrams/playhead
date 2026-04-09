// DesignTokenHapticTests.swift
// Verifies the HapticEvent enum maps to the correct underlying
// UIFeedbackGenerator kinds via the HapticPlaying protocol seam.

import XCTest
@testable import Playhead

@MainActor
final class DesignTokenHapticTests: XCTestCase {

    func testHapticEventCasesExist() {
        // Drive off `allCases` so a newly added case forces this count to
        // be updated alongside the per-mapping assertions below.
        XCTAssertEqual(HapticEvent.allCases.count, 5)
    }

    func testSkipMapsToMediumImpact() {
        XCTAssertEqual(HapticEvent.skip.mapping, .impact(.medium))
    }

    func testControlMapsToLightImpact() {
        XCTAssertEqual(HapticEvent.control.mapping, .impact(.light))
    }

    func testSaveMapsToSuccessNotification() {
        XCTAssertEqual(HapticEvent.save.mapping, .notification(.success))
    }

    func testMenuOpenMapsToMediumImpact() {
        XCTAssertEqual(HapticEvent.menuOpen.mapping, .impact(.medium))
    }

    func testNoticeMapsToSoftImpact() {
        XCTAssertEqual(HapticEvent.notice.mapping, .impact(.soft))
    }

    func testRecordingPlayerCapturesEvents() {
        let player = RecordingHapticPlayer()
        player.play(.skip)
        player.play(.control)
        player.play(.save)
        XCTAssertEqual(player.played, [.skip, .control, .save])
    }
}
