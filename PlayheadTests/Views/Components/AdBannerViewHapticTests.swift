// AdBannerViewHapticTests.swift
// Verifies that the AdBannerView banner-appear haptic routes through the
// injected `HapticPlaying` seam instead of calling `HapticManager.soft()`
// directly.

import XCTest
@testable import Playhead

@MainActor
final class AdBannerViewHapticTests: XCTestCase {

    func testBannerAppearEmitsNoticeHaptic() {
        let recorder = RecordingHapticPlayer()
        let queue = AdBannerQueue()
        let view = AdBannerView(
            queue: queue,
            onListen: nil,
            hapticPlayer: recorder
        )

        view.handleBannerAppear()

        XCTAssertEqual(recorder.played, [.notice],
            "Banner appear must emit exactly one .notice haptic event via the injected player")
    }

    func testDefaultHapticPlayerIsSystemPlayer() {
        let queue = AdBannerQueue()
        let view = AdBannerView(queue: queue)
        XCTAssertTrue(view.hapticPlayer is SystemHapticPlayer,
            "AdBannerView default hapticPlayer should be SystemHapticPlayer")
    }
}
