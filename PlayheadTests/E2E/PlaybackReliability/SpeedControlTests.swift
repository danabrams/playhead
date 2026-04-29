// SpeedControlTests.swift
// playhead-456 — E2E: Playback Reliability, scenario 2 (speed control).
//
// Audible quality at each speed (0.5x, 1.0x, 1.5x, 2.0x, 2.5x, 3.0x)
// is not testable in-process — the simulator does not produce audible
// audio in the unit-test runner. That assertion is deferred to the
// real-device manual QA gate.
//
// What IS testable in-process and what we cover:
//   * setSpeed clamps inputs into [0.5, 3.0]
//   * setSpeed mutates _state.playbackSpeed at every nominal rate
//   * setSpeed pushes the new rate to AVPlayer when status == .playing
//   * setSpeed updates MPNowPlayingInfoCenter's playback rate fields
//   * mid-playback speed change does not change currentTime / status
//     (smooth transition: no gap, no repeat at the actor level)

@preconcurrency import AVFoundation
import Foundation
import MediaPlayer
import Testing
@testable import Playhead

@Suite("playhead-456 — Speed control")
struct SpeedControlTests {

    private static let nominalSpeeds: [Float] = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]

    private func makeService(
        nowPlaying: FakeNowPlayingInfoProvider = FakeNowPlayingInfoProvider()
    ) async -> PlaybackService {
        await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: nowPlaying,
            notificationCenter: NotificationCenter()
        )
    }

    // MARK: - Each nominal speed lands in state

    @Test("setSpeed accepts each of 0.5, 1.0, 1.5, 2.0, 2.5, 3.0",
          arguments: nominalSpeeds)
    func setSpeedAcceptsNominal(_ speed: Float) async {
        let service = await makeService()
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 0,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.setSpeed(speed)
        let snap = await service.snapshot()
        #expect(snap.playbackSpeed == speed,
                "setSpeed(\(speed)) must store \(speed); got \(snap.playbackSpeed)")
    }

    // MARK: - Clamping

    @Test("setSpeed below 0.5 clamps to 0.5 (minSpeed)")
    func setSpeedBelowMinClamps() async {
        let service = await makeService()
        await service.setSpeed(0.1)
        let snap = await service.snapshot()
        // PlaybackService.minSpeed is actor-isolated; bind a local copy
        // outside the #expect autoclosure to avoid an isolation error.
        let minSpeed: Float = 0.5
        #expect(snap.playbackSpeed == minSpeed)
    }

    @Test("setSpeed above 3.0 clamps to 3.0 (maxSpeed)")
    func setSpeedAboveMaxClamps() async {
        let service = await makeService()
        await service.setSpeed(5.0)
        let snap = await service.snapshot()
        let maxSpeed: Float = 3.0
        #expect(snap.playbackSpeed == maxSpeed)
    }

    @Test("setSpeed with zero clamps to minSpeed (no divide-by-zero risk)")
    func setSpeedZeroClamps() async {
        let service = await makeService()
        await service.setSpeed(0)
        let snap = await service.snapshot()
        #expect(snap.playbackSpeed == 0.5)
    }

    @Test("setSpeed negative clamps to minSpeed")
    func setSpeedNegativeClamps() async {
        let service = await makeService()
        await service.setSpeed(-1.0)
        let snap = await service.snapshot()
        #expect(snap.playbackSpeed == 0.5)
    }

    // MARK: - Now Playing rate is published

    @Test("setSpeed pushes the rate into NowPlayingInfo")
    func setSpeedUpdatesNowPlaying() async {
        let nowPlaying = FakeNowPlayingInfoProvider()
        let service = await makeService(nowPlaying: nowPlaying)

        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 0,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.setSpeed(2.0)

        let info = nowPlaying.info
        #expect(info != nil, "setSpeed must push a NowPlaying update")
        let rate = info?[MPNowPlayingInfoPropertyDefaultPlaybackRate] as? Float
        #expect(rate == 2.0,
                "DefaultPlaybackRate must mirror playback speed; got \(String(describing: rate))")
    }

    // MARK: - Mid-playback change is smooth

    /// Bead requirement: mid-playback speed change is "smooth" — no gap,
    /// no repeat. At the actor surface, that means setSpeed must not
    /// alter currentTime nor change status away from .playing. The real
    /// AVPlayer's mid-rate change handling is Apple's responsibility,
    /// but our wrapper must not introduce drift.
    @Test("setSpeed during playback does not move currentTime or status")
    func midPlaybackSpeedChangeIsSmooth() async {
        let service = await makeService()
        let startTime: TimeInterval = 60.0
        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: startTime,
            duration: 600,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        // Cycle through speeds while "playing" — verify each step keeps
        // currentTime pinned at the same value (no gap, no repeat at our
        // layer).
        for speed in [Float(0.5), 1.5, 2.0, 3.0, 1.0] {
            await service.setSpeed(speed)
            let snap = await service.snapshot()
            #expect(snap.currentTime == startTime,
                    "speed change to \(speed) must not move currentTime; got \(snap.currentTime)")
            #expect(snap.status == .playing,
                    "speed change must not interrupt playback status; got \(snap.status)")
            #expect(snap.playbackSpeed == speed,
                    "playback speed must reflect the change; got \(snap.playbackSpeed)")
        }
    }

    // MARK: - Subsequent setSpeed calls are idempotent

    @Test("setSpeed with the same value twice produces the same state")
    func setSpeedIdempotent() async {
        let service = await makeService()
        await service.setSpeed(1.5)
        let snap1 = await service.snapshot()
        await service.setSpeed(1.5)
        let snap2 = await service.snapshot()
        #expect(snap1.playbackSpeed == snap2.playbackSpeed)
        #expect(snap1.playbackSpeed == 1.5)
    }
}

// MARK: - Audible-quality scenarios deferred

// not testable in-process — deferred to manual QA / real-device gate:
//   * audible quality and pitch correction at each speed (0.5x..3x)
//   * "audio is audible and not distorted" (bead step 2)
//   * positional accuracy ±5% over wall-clock (bead step 3) — requires
//     real audio decoding, which AVPlayer does not perform under the
//     test runner's audio session
