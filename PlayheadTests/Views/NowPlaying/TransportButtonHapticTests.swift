// TransportButtonHapticTests.swift
// Verifies that the NowPlayingView TransportButton routes haptic feedback
// through the injected `HapticPlaying` seam instead of calling
// `HapticManager.light()` directly.

import XCTest
@testable import Playhead

@MainActor
final class TransportButtonHapticTests: XCTestCase {

    func testTapEmitsControlHapticAndInvokesAction() {
        let recorder = RecordingHapticPlayer()
        var actionCount = 0
        let button = TransportButton(
            systemName: "play.fill",
            size: 42,
            accessibilityText: "Play",
            hapticPlayer: recorder
        ) {
            actionCount += 1
        }

        button.handleTap()

        XCTAssertEqual(recorder.played, [.control],
            "TransportButton tap must emit exactly one .control haptic event via the injected player")
        XCTAssertEqual(actionCount, 1,
            "TransportButton tap must still invoke the action callback exactly once")
    }

    func testDefaultHapticPlayerIsSystemPlayer() {
        let button = TransportButton(
            systemName: "play.fill",
            size: 42,
            accessibilityText: "Play"
        ) {}
        XCTAssertTrue(button.hapticPlayer is SystemHapticPlayer,
            "TransportButton default hapticPlayer should be SystemHapticPlayer")
    }
}
