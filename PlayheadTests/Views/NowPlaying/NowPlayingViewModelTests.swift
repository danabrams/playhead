// NowPlayingViewModelTests.swift
// Tests for NowPlayingViewModel derived state: progress, elapsed/remaining
// formatting, and edge cases. (Acceptance criteria #7 and #3 of playhead-b9i.)

import XCTest
@testable import Playhead

@MainActor
final class NowPlayingViewModelTests: XCTestCase {

    // MARK: - Progress

    func testProgressIsZeroWhenDurationIsZero() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.duration = 0
            vm.currentTime = 0
            XCTAssertEqual(vm.progress, 0)
        }
    }

    func testProgressComputesCorrectly() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.duration = 200
            vm.currentTime = 50
            XCTAssertEqual(vm.progress, 0.25, accuracy: 0.001)
        }
    }

    func testProgressAtEnd() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.duration = 100
            vm.currentTime = 100
            XCTAssertEqual(vm.progress, 1.0, accuracy: 0.001)
        }
    }

    // MARK: - Elapsed Formatting

    func testElapsedFormattedShowsMinutesAndSeconds() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.currentTime = 65 // 1:05
            XCTAssertEqual(vm.elapsedFormatted, "1:05")
        }
    }

    func testElapsedFormattedShowsHoursWhenOverAnHour() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.currentTime = 3661 // 1:01:01
            XCTAssertEqual(vm.elapsedFormatted, "1:01:01")
        }
    }

    func testElapsedFormattedShowsZeroAtStart() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.currentTime = 0
            XCTAssertEqual(vm.elapsedFormatted, "0:00")
        }
    }

    // MARK: - Remaining Formatting

    func testRemainingFormattedShowsNegativePrefix() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.duration = 300
            vm.currentTime = 60
            // Remaining = 240s = 4:00
            XCTAssertEqual(vm.remainingFormatted, "-4:00")
        }
    }

    func testRemainingFormattedAtEndShowsZero() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.duration = 100
            vm.currentTime = 100
            XCTAssertEqual(vm.remainingFormatted, "-0:00")
        }
    }

    func testRemainingFormattedNeverShowsNegativeTime() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.duration = 100
            vm.currentTime = 105 // past end
            // max(duration - currentTime, 0) should clamp to 0
            XCTAssertEqual(vm.remainingFormatted, "-0:00")
        }
    }

    // MARK: - Default State

    func testDefaultEpisodeTitleWhenNoEpisode() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            XCTAssertEqual(vm.episodeTitle, "No Episode Selected")
        }
    }

    func testDefaultPlaybackSpeed() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            XCTAssertEqual(vm.playbackSpeed, 1.0)
        }
    }

    func testIsPlayingDefaultsFalse() async {
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            XCTAssertFalse(vm.isPlaying)
        }
    }

    // MARK: - reportHearingAd Guard

    /// playhead-t9vyi: a tap that dies at the guard is a ROW, not a silence.
    ///
    /// THIS TEST USED TO ASSERT NOTHING. Its whole body was two calls and a
    /// comment saying "should not throw or crash" — so it named the exact
    /// branch that failed Dan in the field on 2026-09-08 ("the 'I'm hearing an
    /// ad' button absolutely did not work either time") and could only ever
    /// have told us the app survived it. A test whose claim is "no crash" over
    /// a branch whose defect is SILENCE is the standing defect class wearing a
    /// green check.
    func testReportHearingAdRecordsARefusalWithoutAnalysisAssetId() async {
        // Preview runtime has nil currentAnalysisAssetId.
        await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            XCTAssertNil(runtime.currentAnalysisAssetId, "premise: this runtime has no asset")
            XCTAssertTrue(
                runtime.userCorrectionAuditsForTesting.isEmpty,
                "premise: nothing has been audited yet, so what follows is this tap's doing"
            )

            vm.reportHearingAd()

            let refusal = UserCorrectionOutcomeAudit(
                gesture: .hearingAd, outcome: .refusedIdentity, analysisAssetId: nil, windowId: nil
            )
            XCTAssertEqual(
                runtime.userCorrectionAuditsForTesting[refusal], 1,
                """
                A tap with no current asset recorded NOTHING. That is the field defect: no \
                correction, no row, and no answer to the listener. Audits seen: \
                \(runtime.userCorrectionAuditsForTesting)
                """
            )

            // A second tap is a second row. The debounce cannot swallow it,
            // because the debounce is below this guard — and if it ever moves
            // above it, this assertion is what says so.
            vm.reportHearingAd()
            XCTAssertEqual(runtime.userCorrectionAuditsForTesting[refusal], 2)
        }
    }
}
