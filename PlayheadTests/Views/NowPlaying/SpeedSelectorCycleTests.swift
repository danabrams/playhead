// SpeedSelectorCycleTests.swift
// Verifies the speed cycling logic covers all 9 preset rates in order
// and wraps around correctly. (Acceptance criterion #3 of playhead-b9i.)

import XCTest
@testable import Playhead

@MainActor
final class SpeedSelectorCycleTests: XCTestCase {

    /// The expected cycle order when starting from below the minimum preset.
    /// SpeedSelectorView.presets sorted: [0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0]
    private let expectedOrder: [Float] = [0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0]

    // MARK: - Full Cycle

    func testCycleThroughAllRatesInOrder() {
        var currentSpeed: Float = 0.5
        var speeds: [Float] = []

        // Cycle 9 times starting from 0.5 — should visit every rate then wrap.
        for _ in 0..<9 {
            let recorder = RecordingHapticPlayer()
            var chosen: Float?
            let view = SpeedSelectorView(
                currentSpeed: currentSpeed,
                onSpeedChanged: { chosen = $0 },
                hapticPlayer: recorder
            )
            view.handleCycleTap()
            guard let next = chosen else {
                XCTFail("onSpeedChanged not called at speed \(currentSpeed)")
                return
            }
            speeds.append(next)
            currentSpeed = next
        }

        // The first cycle from 0.5 should go to 0.75, then 1.0, ..., 3.0, then wrap to 0.5
        let expected: [Float] = [0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 0.5]
        XCTAssertEqual(speeds, expected,
            "Speed selector must cycle through all presets in ascending order then wrap to 0.5")
    }

    // MARK: - Individual Transitions

    func testCycleFromEachPresetReachesNextPreset() {
        let transitions: [(from: Float, expected: Float)] = [
            (0.5,  0.75),
            (0.75, 1.0),
            (1.0,  1.25),
            (1.25, 1.5),
            (1.5,  1.75),
            (1.75, 2.0),
            (2.0,  2.5),
            (2.5,  3.0),
            (3.0,  0.5),  // wrap-around
        ]

        for (from, expected) in transitions {
            var chosen: Float?
            let view = SpeedSelectorView(
                currentSpeed: from,
                onSpeedChanged: { chosen = $0 },
                hapticPlayer: RecordingHapticPlayer()
            )
            view.handleCycleTap()
            XCTAssertEqual(chosen, expected,
                "Cycling from \(from)x should select \(expected)x")
        }
    }

}
