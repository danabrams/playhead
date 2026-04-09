// TimelineRailHapticTests.swift
// Verifies that TimelineRailView routes scrub-start and scrub-end haptic
// feedback through the injected `HapticPlaying` seam instead of calling
// `HapticManager` directly.
//
// The drag gesture fires two distinct .control haptics:
//   - scrub begin -> .control (light)
//   - scrub end   -> .control (light)
// Each is driven through a factored handler so the test does not need a
// live SwiftUI view hierarchy or a real DragGesture.

import XCTest
@testable import Playhead

@MainActor
final class TimelineRailHapticTests: XCTestCase {

    func testScrubBeginEmitsControlHaptic() {
        let recorder = RecordingHapticPlayer()
        let view = TimelineRailView(
            progress: 0.25,
            adSegments: [],
            onSeek: { _ in },
            hapticPlayer: recorder
        )

        view.handleScrubBegin()

        XCTAssertEqual(recorder.played, [.control],
            "Scrub begin must emit exactly one .control haptic event via the injected player")
    }

    func testScrubEndEmitsControlHapticAndSeeks() {
        let recorder = RecordingHapticPlayer()
        var seekedTo: Double?
        let view = TimelineRailView(
            progress: 0.25,
            adSegments: [],
            onSeek: { seekedTo = $0 },
            hapticPlayer: recorder
        )

        view.handleScrubEnd(fraction: 0.42)

        XCTAssertEqual(recorder.played, [.control],
            "Scrub end must emit exactly one .control haptic event via the injected player")
        XCTAssertEqual(seekedTo, 0.42,
            "Scrub end must still invoke onSeek with the final fraction")
    }

    func testFullScrubSequenceEmitsTwoControlEvents() {
        let recorder = RecordingHapticPlayer()
        let view = TimelineRailView(
            progress: 0.0,
            adSegments: [],
            onSeek: { _ in },
            hapticPlayer: recorder
        )

        view.handleScrubBegin()
        view.handleScrubEnd(fraction: 0.9)

        XCTAssertEqual(recorder.played, [.control, .control],
            "Begin+end of a scrub must emit exactly two .control events in order")
    }

    func testDefaultHapticPlayerIsSystemPlayer() {
        let view = TimelineRailView(
            progress: 0.0,
            adSegments: [],
            onSeek: { _ in }
        )
        XCTAssertTrue(view.hapticPlayer is SystemHapticPlayer,
            "TimelineRailView default hapticPlayer should be SystemHapticPlayer")
    }
}
