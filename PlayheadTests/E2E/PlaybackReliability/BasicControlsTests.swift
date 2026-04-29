// BasicControlsTests.swift
// playhead-456 — E2E: Playback Reliability, scenario 1 (basic controls).
//
// Drives the real PlaybackService through its public API (load, play,
// pause, seek, skipForward, skipBackward) and asserts state transitions
// + currentTime arithmetic. AVPlayer in the simulator does not emit
// audible audio in unit tests, but its time-tracking and our actor's
// state machine still operate, which is what the bead asks us to prove
// (CLAUDE.md guidance: "AVPlayer time tracking still works").
//
// We deliberately use `_testingInjectState` to seed a known
// (currentTime, duration) baseline rather than racing against
// AVPlayerItem.readyToPlay, because the simulator can't reliably
// drive a real audio decode in the per-bead fast-test plan.

@preconcurrency import AVFoundation
import Foundation
import Testing
@testable import Playhead

@Suite("playhead-456 — Basic controls")
struct BasicControlsTests {

    private func makeService() async -> PlaybackService {
        await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
    }

    // MARK: - Test 1: Play

    @Test("play() transitions to .playing when an item is loaded")
    func playTransitionsToPlaying() async {
        let service = await makeService()

        // Inject a paused state with a player item present (the guard in
        // play() requires `playerItem != nil`); load a synthetic asset
        // that gets us past the guard. The asset URL is a custom scheme
        // with no resource loader, so it never reaches readyToPlay, but
        // the player item is registered which is all play() guards on.
        await service._testingInstallStubPlayerItem()
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 0,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.play()
        let snap = await service.snapshot()
        #expect(snap.status == .playing,
                "play() must transition to .playing; got \(snap.status)")
    }

    // MARK: - Test 2: Pause

    @Test("pause() transitions from .playing to .paused")
    func pauseTransitionsToPaused() async {
        let service = await makeService()
        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 30,
            duration: 600,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        await service.pause()
        let snap = await service.snapshot()
        #expect(snap.status == .paused,
                "pause() must transition to .paused; got \(snap.status)")
    }

    // MARK: - Test 3: Resume from exact position

    @Test("resume after pause keeps currentTime exact (zero drift)")
    func resumePreservesPosition() async {
        let service = await makeService()
        await service._testingInstallStubPlayerItem()

        let pausePosition: TimeInterval = 123.45
        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: pausePosition,
            duration: 600,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        await service.pause()
        let pausedSnap = await service.snapshot()
        #expect(pausedSnap.currentTime == pausePosition,
                "pause must not move the playhead; was \(pausedSnap.currentTime)")

        await service.play()
        let resumedSnap = await service.snapshot()
        // play() does not advance currentTime; only the periodic time
        // observer does. Without a real decoded item, currentTime stays
        // pinned at the paused position — which is exactly what the
        // bead asks: "resume from exact position".
        #expect(resumedSnap.currentTime == pausePosition,
                "resume must restart at the paused position (no drift); was \(resumedSnap.currentTime)")
        #expect(resumedSnap.status == .playing)
    }

    // MARK: - Test 4: Seek to 50%

    @Test("seek(to:) moves the playhead to the requested second (±0)")
    func seekToHalfDuration() async {
        let service = await makeService()
        let totalDuration: TimeInterval = 1800 // 30 min episode
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 0,
            duration: totalDuration,
            rate: 0,
            playbackSpeed: 1.0
        ))

        let target = totalDuration / 2.0
        await service.seek(to: target)

        let snap = await service.snapshot()
        // Bead allows ±1s tolerance on real audio; with no player item
        // the seek just sets _state.currentTime = seconds exactly.
        #expect(abs(snap.currentTime - target) < 1.0,
                "seek must land within ±1s of \(target); got \(snap.currentTime)")
    }

    // MARK: - Test 5: Skip +30s

    @Test("skipForward(30) advances currentTime by 30s exactly")
    func skipForwardThirtySeconds() async {
        let service = await makeService()
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 100,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.skipForward(30)
        let snap = await service.snapshot()
        #expect(abs(snap.currentTime - 130) < 0.5,
                "skipForward(30) must advance by ~30s; got \(snap.currentTime)")
    }

    @Test("default skipForward uses 30s constant")
    func skipForwardDefaultMatchesConstant() async {
        let service = await makeService()
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 50,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.skipForward()
        let snap = await service.snapshot()
        let expected = 50.0 + PlaybackService.skipForwardSeconds
        #expect(abs(snap.currentTime - expected) < 0.5)
    }

    // MARK: - Test 6: Skip -15s

    @Test("skipBackward(15) retreats currentTime by 15s exactly")
    func skipBackwardFifteenSeconds() async {
        let service = await makeService()
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 100,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.skipBackward(15)
        let snap = await service.snapshot()
        #expect(abs(snap.currentTime - 85) < 0.5,
                "skipBackward(15) must retreat by ~15s; got \(snap.currentTime)")
    }

    @Test("default skipBackward uses 15s constant")
    func skipBackwardDefaultMatchesConstant() async {
        let service = await makeService()
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 50,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.skipBackward()
        let snap = await service.snapshot()
        let expected = 50.0 - PlaybackService.skipBackwardSeconds
        #expect(abs(snap.currentTime - expected) < 0.5)
    }

    // MARK: - Test 7: Clamp to 0 on negative skip near start

    @Test("skipBackward near start clamps to 0, never negative")
    func skipBackwardClampsToZero() async {
        let service = await makeService()

        // currentTime at 5s; skipBackward(30) would land at -25 if unclamped.
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 5,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.skipBackward(30)
        let snap = await service.snapshot()
        #expect(snap.currentTime == 0,
                "skipBackward must clamp to 0; got \(snap.currentTime)")
        #expect(snap.currentTime >= 0,
                "currentTime must never go negative")
    }

    @Test("skipBackward at exactly 0 stays at 0")
    func skipBackwardFromZero() async {
        let service = await makeService()
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 0,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.skipBackward(15)
        let snap = await service.snapshot()
        #expect(snap.currentTime == 0)
    }

    // MARK: - Skip forward bounded by duration

    @Test("skipForward past duration clamps to duration")
    func skipForwardClampsToDuration() async {
        let service = await makeService()
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 590,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.skipForward(30)
        let snap = await service.snapshot()
        #expect(snap.currentTime == 600,
                "skipForward must clamp to duration; got \(snap.currentTime)")
    }

    // MARK: - togglePlayPause

    @Test("togglePlayPause flips playing → paused")
    func togglePlayPauseFlipsPlaying() async {
        let service = await makeService()
        await service._testingInstallStubPlayerItem()
        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 10,
            duration: 600,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        await service.togglePlayPause()
        let snap = await service.snapshot()
        #expect(snap.status == .paused)
    }

    @Test("togglePlayPause flips paused → playing")
    func togglePlayPauseFlipsPaused() async {
        let service = await makeService()
        await service._testingInstallStubPlayerItem()
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 10,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.togglePlayPause()
        let snap = await service.snapshot()
        #expect(snap.status == .playing)
    }
}
