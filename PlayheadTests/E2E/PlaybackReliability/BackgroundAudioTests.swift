// BackgroundAudioTests.swift
// playhead-456 — E2E: Playback Reliability, scenario 5 (background audio).
//
// "Send the app to background" is itself a real-device lifecycle
// transition that the unit-test process cannot perform. What matters
// for the bead is the OBSERVABLE state from the system's view while we
// are backgrounded:
//
//   1. Audio category is .playback with .longFormAudio policy — that is
//      what UIBackgroundMode `audio` requires for the OS to keep us
//      running in the background. Verified at configureAudioSession.
//   2. Now Playing info center is populated with title / artist / artwork
//      / playback rate / elapsed time — that's how the lock screen and
//      Control Center render the now-playing card.
//   3. State transitions (play, pause, seek, setSpeed) keep the Now
//      Playing dictionary in sync — so the lock-screen UI never lies.
//   4. The remote command center's command handlers, when invoked,
//      drive the same PlaybackService methods that the foreground UI
//      uses — so lock-screen taps and CarPlay taps produce identical
//      state transitions to the in-app controls.
//
// All four are testable in-process via the seam fakes (audio session,
// now-playing). Real lock-screen taps are deferred to manual QA.

@preconcurrency import AVFoundation
import Foundation
import MediaPlayer
import Testing
import UIKit
@testable import Playhead

@Suite("playhead-456 — Background audio")
struct BackgroundAudioTests {

    /// 1×1 image used as a stand-in for podcast artwork.
    private static var testImage: UIImage {
        UIGraphicsImageRenderer(size: CGSize(width: 1, height: 1))
            .image { $0.fill(CGRect(x: 0, y: 0, width: 1, height: 1)) }
    }

    // MARK: - Audio session configured for long-form background playback

    @Test("Audio session is configured for long-form playback (UIBackgroundMode audio)")
    func audioSessionConfiguredForBackground() async {
        let audio = FakeAudioSessionProvider()
        let service = await PlaybackService(
            audioSession: audio,
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        // Hop the actor to flush the init Task that configures the session.
        _ = await service.snapshot()

        let calls = audio.categoryCalls
        #expect(calls.count >= 1)
        let last = calls.last!
        #expect(last.category == AVAudioSession.Category.playback.rawValue,
                "Background audio requires .playback category; got \(last.category)")
        #expect(last.mode == AVAudioSession.Mode.spokenAudio.rawValue,
                "Spoken-audio mode (skip-silence eligible); got \(last.mode)")
        #expect(last.policy == AVAudioSession.RouteSharingPolicy.longFormAudio.rawValue,
                "Podcasts use longFormAudio policy; got \(last.policy)")

        #expect(audio.setActiveCalls == [true],
                "Session must be activated; got \(audio.setActiveCalls)")
    }

    // MARK: - Now Playing metadata populates the lock-screen card

    @Test("setNowPlayingMetadata populates title/artist/album/artwork")
    func nowPlayingMetadataPopulated() async {
        let nowPlaying = FakeNowPlayingInfoProvider()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: nowPlaying,
            notificationCenter: NotificationCenter()
        )

        await service.setNowPlayingMetadata(
            title: "Episode 42 — Test Episode",
            artist: "Test Host",
            albumTitle: "Test Podcast",
            artworkImage: Self.testImage
        )

        let info = nowPlaying.info
        #expect(info != nil, "metadata must be written")
        #expect(info?[MPMediaItemPropertyTitle] as? String == "Episode 42 — Test Episode")
        #expect(info?[MPMediaItemPropertyArtist] as? String == "Test Host")
        #expect(info?[MPMediaItemPropertyAlbumTitle] as? String == "Test Podcast")
        #expect(info?[MPMediaItemPropertyArtwork] is MPMediaItemArtwork,
                "Artwork must be an MPMediaItemArtwork instance")
    }

    @Test("Now Playing info exposes elapsed time, duration, and rate")
    func nowPlayingTimeFieldsPopulated() async {
        let nowPlaying = FakeNowPlayingInfoProvider()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: nowPlaying,
            notificationCenter: NotificationCenter()
        )
        await service._testingInstallStubPlayerItem()

        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 73.5,
            duration: 1800,
            rate: 1.0,
            playbackSpeed: 1.5
        ))
        await service.setNowPlayingMetadata(title: "Time Fields Test")

        let info = nowPlaying.info
        let duration = info?[MPMediaItemPropertyPlaybackDuration] as? TimeInterval
        let elapsed = info?[MPNowPlayingInfoPropertyElapsedPlaybackTime] as? TimeInterval
        let rate = info?[MPNowPlayingInfoPropertyPlaybackRate] as? Float

        #expect(duration == 1800)
        #expect(elapsed == 73.5)
        #expect(rate == 1.0)
    }

    // MARK: - State transitions keep Now Playing in sync

    @Test("play() updates Now Playing rate to current playback speed")
    func playUpdatesNowPlayingRate() async {
        let nowPlaying = FakeNowPlayingInfoProvider()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: nowPlaying,
            notificationCenter: NotificationCenter()
        )
        await service._testingInstallStubPlayerItem()
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 10,
            duration: 600,
            rate: 0,
            playbackSpeed: 2.0
        ))

        await service.play()
        // play() pushes nowPlaying immediately; the rate field reflects
        // _state.rate (0 here because no real player), but
        // DefaultPlaybackRate reflects the playback speed.
        let info = nowPlaying.info
        let defaultRate = info?[MPNowPlayingInfoPropertyDefaultPlaybackRate] as? Float
        #expect(defaultRate == 2.0,
                "DefaultPlaybackRate must mirror playback speed; got \(String(describing: defaultRate))")
    }

    @Test("seek() updates Now Playing elapsed time")
    func seekUpdatesNowPlayingElapsed() async {
        let nowPlaying = FakeNowPlayingInfoProvider()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: nowPlaying,
            notificationCenter: NotificationCenter()
        )

        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 0,
            duration: 1200,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await service.seek(to: 600)
        let info = nowPlaying.info
        let elapsed = info?[MPNowPlayingInfoPropertyElapsedPlaybackTime] as? TimeInterval
        #expect(elapsed == 600)
    }

    @Test("Sequential transport ops keep Now Playing dictionary sync'd")
    func sequentialOpsKeepNowPlayingSyncd() async {
        let nowPlaying = FakeNowPlayingInfoProvider()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: nowPlaying,
            notificationCenter: NotificationCenter()
        )
        await service._testingInstallStubPlayerItem()

        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 0,
            duration: 1800,
            rate: 0,
            playbackSpeed: 1.0
        ))
        await service.setNowPlayingMetadata(title: "Sequential Ops")

        await service.seek(to: 100)
        await service.setSpeed(2.0)
        await service.play()
        await service.skipForward(30)

        let info = nowPlaying.info
        // After skipForward(30) from 100, currentTime should be 130.
        let elapsed = info?[MPNowPlayingInfoPropertyElapsedPlaybackTime] as? TimeInterval
        #expect(elapsed == 130,
                "Now Playing elapsed must reflect last seek/skip; got \(String(describing: elapsed))")
        let rate = info?[MPNowPlayingInfoPropertyDefaultPlaybackRate] as? Float
        #expect(rate == 2.0,
                "Now Playing default rate must reflect setSpeed; got \(String(describing: rate))")
        let title = info?[MPMediaItemPropertyTitle] as? String
        #expect(title == "Sequential Ops",
                "Title must persist through state changes")
    }

    // MARK: - Foreground UI state matches playback state

    @Test("State observer immediately yields current snapshot to a late subscriber")
    func lateSubscriberReceivesCurrentState() async {
        // Returning to the foreground after a background session means
        // the UI re-mounts and re-subscribes. The bead spec: "Return to
        // app → verify UI state matches playback state". We verify the
        // contract that backs that: a fresh observer immediately yields
        // the current snapshot, no defaults visible.
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )

        // Drive the service through a few transitions while no one is
        // listening (i.e. the UI was backgrounded).
        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 250,
            duration: 1800,
            rate: 1.0,
            playbackSpeed: 1.5
        ))

        // Foreground: a fresh subscriber appears.
        let stream = await service.observeStates()
        var firstYield: PlaybackState?
        for await state in stream {
            firstYield = state
            break
        }

        #expect(firstYield != nil)
        #expect(firstYield?.currentTime == 250,
                "Late subscriber must see real currentTime, not 0")
        #expect(firstYield?.status == .playing,
                "Late subscriber must see .playing, not .idle")
        #expect(firstYield?.playbackSpeed == 1.5)
    }
}

// MARK: - Real-device-only scenarios deferred

// not testable in-process — deferred to manual QA / real-device gate:
//   * actual app backgrounding (UIScene transitions, the OS deciding
//     whether to keep us alive)
//   * lock-screen tap rendering (MediaPlayer's own UI)
//   * Control Center / CarPlay rendering
