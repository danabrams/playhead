// NowPlayingViewModelTests.swift
// Tests for NowPlayingViewModel derived state: progress, elapsed/remaining
// formatting, and edge cases. (Acceptance criteria #7 and #3 of playhead-b9i.)

import XCTest
@testable import Playhead

@MainActor
final class NowPlayingViewModelTests: XCTestCase {

    // MARK: - Progress

    func testProgressIsZeroWhenDurationIsZero() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        vm.duration = 0
        vm.currentTime = 0
        XCTAssertEqual(vm.progress, 0)
    }

    func testProgressComputesCorrectly() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        vm.duration = 200
        vm.currentTime = 50
        XCTAssertEqual(vm.progress, 0.25, accuracy: 0.001)
    }

    func testProgressAtEnd() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        vm.duration = 100
        vm.currentTime = 100
        XCTAssertEqual(vm.progress, 1.0, accuracy: 0.001)
    }

    // MARK: - Elapsed Formatting

    func testElapsedFormattedShowsMinutesAndSeconds() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        vm.currentTime = 65 // 1:05
        XCTAssertEqual(vm.elapsedFormatted, "1:05")
    }

    func testElapsedFormattedShowsHoursWhenOverAnHour() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        vm.currentTime = 3661 // 1:01:01
        XCTAssertEqual(vm.elapsedFormatted, "1:01:01")
    }

    func testElapsedFormattedShowsZeroAtStart() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        vm.currentTime = 0
        XCTAssertEqual(vm.elapsedFormatted, "0:00")
    }

    // MARK: - Remaining Formatting

    func testRemainingFormattedShowsNegativePrefix() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        vm.duration = 300
        vm.currentTime = 60
        // Remaining = 240s = 4:00
        XCTAssertEqual(vm.remainingFormatted, "-4:00")
    }

    func testRemainingFormattedAtEndShowsZero() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        vm.duration = 100
        vm.currentTime = 100
        XCTAssertEqual(vm.remainingFormatted, "-0:00")
    }

    func testRemainingFormattedNeverShowsNegativeTime() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        vm.duration = 100
        vm.currentTime = 105 // past end
        // max(duration - currentTime, 0) should clamp to 0
        XCTAssertEqual(vm.remainingFormatted, "-0:00")
    }

    // MARK: - Default State

    func testDefaultEpisodeTitleWhenNoEpisode() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        XCTAssertEqual(vm.episodeTitle, "No Episode Selected")
    }

    func testDefaultPlaybackSpeed() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        XCTAssertEqual(vm.playbackSpeed, 1.0)
    }

    func testIsPlayingDefaultsFalse() {
        let vm = NowPlayingViewModel(runtime: PlayheadRuntime(isPreviewRuntime: true))
        XCTAssertFalse(vm.isPlaying)
    }
}
