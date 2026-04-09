// DesignTokenHapticTests.swift
// Verifies the HapticEvent enum maps to the correct underlying
// UIFeedbackGenerator kinds via the HapticPlaying protocol seam.

import XCTest
import UIKit
@testable import Playhead

@MainActor
final class RecordingHapticPlayer: HapticPlaying {
    var played: [HapticEvent] = []
    func play(_ event: HapticEvent) { played.append(event) }
}

@MainActor
final class DesignTokenHapticTests: XCTestCase {

    func testHapticEventCasesExist() {
        let all: [HapticEvent] = [.skip, .control, .save]
        XCTAssertEqual(all.count, 3)
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

    func testRecordingPlayerCapturesEvents() {
        let player = RecordingHapticPlayer()
        player.play(.skip)
        player.play(.control)
        player.play(.save)
        XCTAssertEqual(player.played, [.skip, .control, .save])
    }
}
