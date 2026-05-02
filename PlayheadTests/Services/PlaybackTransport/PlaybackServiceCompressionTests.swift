// PlaybackServiceCompressionTests.swift
// playhead-epii — Tests for the rate-override surface added to
// `PlaybackService`. Focused on the invariants the skeptical-review
// callout flagged:
//   - manual setSpeed mid-compression clears the multiplier
//   - endCompression restores the user's CURRENT base speed (not the
//     pre-compression base, in case the user changed speed during)
//   - timePitchAlgorithm is set on the active item

import AVFoundation
@preconcurrency import Foundation
import Testing

@testable import Playhead

@Suite("PlaybackService silence-compression surface (playhead-epii)")
struct PlaybackServiceCompressionTests {

    /// Builds a PlaybackService with the test seams. Returns the
    /// service and a teardown closure to call before the test exits.
    private func makeService() -> PlaybackService {
        // Use a private NotificationCenter so parallel test instances
        // don't see each other's interruption / route notifications.
        PlaybackService(
            audioSession: NoOpAudioSession(),
            nowPlayingInfo: InMemoryNowPlayingInfo(),
            notificationCenter: NotificationCenter()
        )
    }

    @Test("beginCompression updates currentCompressionMultiplier")
    func beginCompressionUpdatesMultiplier() async {
        let service = makeService()
        await service.beginCompression(multiplier: 2.5, algorithm: .varispeed)
        let mult = await service.currentCompressionMultiplier
        let algo = await service.currentTimePitchAlgorithmName
        #expect(mult == 2.5)
        #expect(algo == .varispeed)
        await service.tearDown()
    }

    @Test("endCompression restores 1.0× multiplier and .spectral")
    func endCompressionRestoresDefaults() async {
        let service = makeService()
        await service.beginCompression(multiplier: 2.0, algorithm: .varispeed)
        await service.endCompression()
        let mult = await service.currentCompressionMultiplier
        let algo = await service.currentTimePitchAlgorithmName
        #expect(mult == 1.0)
        #expect(algo == .spectral)
        await service.tearDown()
    }

    @Test("setSpeed mid-compression clears the override multiplier")
    func setSpeedClearsOverride() async {
        let service = makeService()
        await service.beginCompression(multiplier: 2.0, algorithm: .varispeed)
        await service.setSpeed(1.25)
        let mult = await service.currentCompressionMultiplier
        let algo = await service.currentTimePitchAlgorithmName
        #expect(mult == 1.0)
        #expect(algo == .spectral, "Manual speed change should also reset algorithm")
        await service.tearDown()
    }

    @Test(
        "endCompression after a base-speed change restores to NEW base, not pre-compression base"
    )
    func endCompressionRespectsLatestBase() async {
        // Critical for the reviewer's restore-on-exit correctness
        // concern: the multiplier is not a "saved base" sentinel,
        // it's strictly a multiplicative override. The user's base
        // speed lives in `_state.playbackSpeed` and is mutated only
        // by `setSpeed`. If the user flips 1.0×→1.25× WHILE we hold
        // a multiplier of 2.0, `setSpeed` clears the multiplier (per
        // the test above). When the compressor later calls
        // `endCompression`, the effective rate must read the current
        // base (1.25), never the pre-compression value.
        let service = makeService()
        await service.setSpeed(1.0)
        await service.beginCompression(multiplier: 2.0, algorithm: .varispeed)
        await service.setSpeed(1.25)
        // setSpeed already cleared the multiplier and rate. A
        // subsequent endCompression must remain a no-op (no
        // surprise restore to 1.0).
        await service.endCompression()
        let mult = await service.currentCompressionMultiplier
        let snapshot = await service.snapshot()
        #expect(mult == 1.0)
        #expect(snapshot.playbackSpeed == 1.25)
        await service.tearDown()
    }

    @Test("Idempotent: calling beginCompression twice with same args is a no-op")
    func beginCompressionIdempotent() async {
        let service = makeService()
        await service.beginCompression(multiplier: 2.0, algorithm: .spectral)
        await service.beginCompression(multiplier: 2.0, algorithm: .spectral)
        let mult = await service.currentCompressionMultiplier
        #expect(mult == 2.0)
        await service.tearDown()
    }

    @Test("Multiplier below 1.0 is clamped to 1.0 (never slows playback)")
    func multiplierNeverSlows() async {
        let service = makeService()
        await service.beginCompression(multiplier: 0.5, algorithm: .spectral)
        let mult = await service.currentCompressionMultiplier
        #expect(mult == 1.0)
        await service.tearDown()
    }
}

// MARK: - Test seams (no-op providers)

/// No-op AudioSessionProviding for tests that don't care about the
/// session lifecycle.
struct NoOpAudioSession: AudioSessionProviding {
    func setCategory(
        _: AVAudioSession.Category,
        mode _: AVAudioSession.Mode,
        policy _: AVAudioSession.RouteSharingPolicy
    ) throws {}
    func setActive(_: Bool) throws {}
}

/// In-memory NowPlayingInfoProviding so parallel tests don't clobber
/// the global MPNowPlayingInfoCenter.
final class InMemoryNowPlayingInfo: NowPlayingInfoProviding, @unchecked Sendable {
    private let lock = NSLock()
    private var info: [String: Any]?

    func getNowPlayingInfo() -> [String: Any]? {
        lock.lock(); defer { lock.unlock() }
        return info
    }

    func setNowPlayingInfo(_ info: [String: Any]?) {
        lock.lock(); defer { lock.unlock() }
        self.info = info
    }
}
