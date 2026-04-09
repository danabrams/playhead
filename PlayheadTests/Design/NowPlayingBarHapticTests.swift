// NowPlayingBarHapticTests.swift
// Verifies that the NowPlayingBar play/pause button routes through the
// injected `HapticPlaying` seam instead of calling `HapticManager.light()`
// directly. This is the first real consumer of the protocol and proves
// the seam is wired end-to-end, not orphaned.

import XCTest
@testable import Playhead

@MainActor
final class NowPlayingBarHapticTests: XCTestCase {

    func testPlayPauseTapRecordsControlEvent() {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let viewModel = NowPlayingViewModel(runtime: runtime)
        let recorder = RecordingHapticPlayer()

        let bar = NowPlayingBar(viewModel: viewModel, hapticPlayer: recorder)
        bar.handlePlayPauseTap()

        XCTAssertEqual(recorder.played, [.control],
            "Play/pause tap must emit exactly one .control haptic event via the injected player")
    }

    func testDefaultHapticPlayerIsSystemPlayer() {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let viewModel = NowPlayingViewModel(runtime: runtime)
        let bar = NowPlayingBar(viewModel: viewModel)
        // Type check: the default should be SystemHapticPlayer.
        XCTAssertTrue(bar.hapticPlayer is SystemHapticPlayer,
            "NowPlayingBar default hapticPlayer should be SystemHapticPlayer")
    }
}
