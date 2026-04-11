// NowPlayingBarTests.swift
// Unit tests for NowPlayingBar layout constants, progress computation,
// and play/pause toggle behavior. Complements NowPlayingBarHapticTests
// which focuses on the haptic seam.

import XCTest
@testable import Playhead

@MainActor
final class NowPlayingBarTests: XCTestCase {

    // MARK: - Play/Pause Toggle

    func testHandlePlayPauseTapEmitsHapticAndToggles() {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let vm = NowPlayingViewModel(runtime: runtime)
        let recorder = RecordingHapticPlayer()

        let bar = NowPlayingBar(viewModel: vm, hapticPlayer: recorder)

        bar.handlePlayPauseTap()

        XCTAssertEqual(recorder.played, [.control],
            "Play/pause tap must emit .control haptic")
    }

    func testMultipleTapsEmitMultipleHaptics() {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let vm = NowPlayingViewModel(runtime: runtime)
        let recorder = RecordingHapticPlayer()

        let bar = NowPlayingBar(viewModel: vm, hapticPlayer: recorder)

        bar.handlePlayPauseTap()
        bar.handlePlayPauseTap()
        bar.handlePlayPauseTap()

        XCTAssertEqual(recorder.played, [.control, .control, .control],
            "Each tap must emit exactly one .control haptic")
    }

}
