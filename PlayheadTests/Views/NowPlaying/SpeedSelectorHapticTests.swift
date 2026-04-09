// SpeedSelectorHapticTests.swift
// Verifies that SpeedSelectorView routes haptic feedback through the
// injected `HapticPlaying` seam instead of calling `HapticManager` directly.
//
// Two distinct tap actions emit haptics:
//   - tap-to-cycle         -> .control  (light)
//   - long-press-to-picker -> .menuOpen (medium)
// Each is driven through a factored handler so the test does not need a
// live SwiftUI view hierarchy.

import XCTest
@testable import Playhead

@MainActor
final class SpeedSelectorHapticTests: XCTestCase {

    func testCycleTapEmitsControlHaptic() {
        let recorder = RecordingHapticPlayer()
        var chosen: Float?
        let view = SpeedSelectorView(
            currentSpeed: 1.0,
            onSpeedChanged: { chosen = $0 },
            hapticPlayer: recorder
        )

        view.handleCycleTap()

        XCTAssertEqual(recorder.played, [.control],
            "Cycle tap must emit exactly one .control haptic event via the injected player")
        XCTAssertEqual(chosen, 1.25,
            "Cycle tap must still invoke the onSpeedChanged callback with the next preset")
    }

    func testLongPressEmitsMenuOpenHaptic() {
        let recorder = RecordingHapticPlayer()
        let view = SpeedSelectorView(
            currentSpeed: 1.0,
            onSpeedChanged: { _ in },
            hapticPlayer: recorder
        )

        view.handleLongPress()

        XCTAssertEqual(recorder.played, [.menuOpen],
            "Long-press must emit exactly one .menuOpen haptic event via the injected player")
    }

    func testDefaultHapticPlayerIsSystemPlayer() {
        let view = SpeedSelectorView(
            currentSpeed: 1.0,
            onSpeedChanged: { _ in }
        )
        XCTAssertTrue(view.hapticPlayer is SystemHapticPlayer,
            "SpeedSelectorView default hapticPlayer should be SystemHapticPlayer")
    }
}
