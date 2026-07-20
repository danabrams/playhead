// InterruptionHandlingTests.swift
// Regression test for the Siri crash: Swift 6 actor isolation assertion
// when Combine's .sink accessed @PlaybackServiceActor-isolated self from
// the main queue during AVAudioSession interruption notifications.
//
// The fix replaced Combine observers with async notification sequences
// that run entirely on PlaybackServiceActor. These tests verify the
// notification is handled on the actor without triggering an assertion.

@preconcurrency import AVFoundation
import Foundation
import MediaPlayer
import Testing
@testable import Playhead

// MARK: - Helpers

/// Drain the given state stream ON THE TEST'S OWN TASK until a status
/// matching `predicate` arrives, then return everything observed.
///
/// playhead-vsot round 2: the previous version drained in a child task
/// while the test polled a shared box under a 2 s wall-clock deadline —
/// under the full parallel plan the actor's notification handler (or
/// the drain task itself) was not scheduled inside 2 s and the tests
/// failed with `observed=[]` at ~95 s wall (2026-07-20 gate run 2).
/// The stream IS the actor-handled signal: the handler's `pause()` runs
/// on PlaybackServiceActor and yields the `.paused` state to every
/// observer, and `observeStates()` yields the current snapshot
/// immediately on subscribe, so subscribing before the notification
/// post cannot miss the transition. No deadline — if production stops
/// pausing, the drain parks forever and the test's `.timeLimit` trait
/// fails deterministically instead of load-dependently.
private func awaitStatus(
    in stream: AsyncStream<PlaybackState>,
    matching predicate: @Sendable @escaping (PlaybackState.Status) -> Bool
) async -> [PlaybackState.Status] {
    var observed: [PlaybackState.Status] = []
    for await state in stream {
        observed.append(state.status)
        if predicate(state.status) { break }
    }
    return observed
}

// MARK: - Interruption Handling

@Suite("PlaybackService – Audio Session Interruptions")
struct InterruptionHandlingTests {

    /// Post a fake interruption-began notification and verify PlaybackService
    /// handles it without crashing. This is the exact scenario that triggered
    /// the "Incorrect actor executor assumption" crash with Combine observers.
    @Test("Interruption notification handled on actor without crash",
          .timeLimit(.minutes(1)))
    func interruptionBegan() async throws {
        // playhead-86s: use a private NotificationCenter + fake seams so
        // parallel test instances can't clobber each other via the process
        // global AVAudioSession / MPNowPlayingInfoCenter.
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        // Put the service into a playing state so pause() has an effect.
        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 42.0,
            duration: 1800.0,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        // Subscribe to state changes before posting the notification.
        let stream = await service.observeStates()

        // Post the interruption notification on the main queue — this is
        // exactly how AVAudioSession delivers it when Siri activates.
        // With the old Combine .sink, this would crash here.
        await MainActor.run {
            center.post(
                name: AVAudioSession.interruptionNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionInterruptionTypeKey: AVAudioSession.InterruptionType.began.rawValue
                ]
            )
        }

        // Drain the state stream until we observe the pause transition.
        // This replaces a 100ms sleep that races with the actor's async
        // notification handler on slow/contended simulators.
        let observed = await awaitStatus(in: stream) { $0 == .paused }
        #expect(observed.contains(.paused),
                "Service should pause on interruption began; observed: \(observed)")
    }

    @Test("Interruption ended with shouldResume resumes playback")
    func interruptionEndedResumes() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        // Start in paused state (as if interruption already began).
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 42.0,
            duration: 1800.0,
            rate: 0,
            playbackSpeed: 1.0
        ))

        // Post interruption ended with shouldResume.
        await MainActor.run {
            center.post(
                name: AVAudioSession.interruptionNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionInterruptionTypeKey: AVAudioSession.InterruptionType.ended.rawValue,
                    AVAudioSessionInterruptionOptionKey: AVAudioSession.InterruptionOptions.shouldResume.rawValue
                ]
            )
        }
        _ = service

        try await Task.sleep(for: .milliseconds(100))

        // Without a loaded player item, play() won't change status to .playing,
        // but the important thing is we didn't crash.
        // The actor isolation assertion would have fired before reaching here.
    }

    @Test("Route change notification handled on actor without crash",
          .timeLimit(.minutes(1)))
    func routeChange() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 10.0,
            duration: 600.0,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        // Subscribe to state changes BEFORE posting the notification so we
        // never miss the pause transition the handler will emit.
        let stream = await service.observeStates()

        // Post route change (headphones unplugged) on main queue.
        await MainActor.run {
            center.post(
                name: AVAudioSession.routeChangeNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionRouteChangeReasonKey: AVAudioSession.RouteChangeReason.oldDeviceUnavailable.rawValue
                ]
            )
        }

        // Drain the state stream until we observe the pause transition.
        // This replaces a 100ms sleep that races with the actor's async
        // notification handler on slow/contended simulators.
        let observed = await awaitStatus(in: stream) { $0 == .paused }
        #expect(observed.contains(.paused),
                "Service should pause when headphones disconnect; observed: \(observed)")
    }

    /// Rapid-fire notifications simulating the storm that Siri causes.
    /// The old Combine pattern would crash on the first or second notification.
    @Test("Rapid interruption notifications don't crash")
    func rapidInterruptions() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 0,
            duration: 100,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        // Fire 20 notifications in quick succession from the main queue.
        await MainActor.run {
            for i in 0..<20 {
                let type: AVAudioSession.InterruptionType = i % 2 == 0 ? .began : .ended
                center.post(
                    name: AVAudioSession.interruptionNotification,
                    object: nil,
                    userInfo: [
                        AVAudioSessionInterruptionTypeKey: type.rawValue
                    ]
                )
            }
        }

        // If we survive 100ms without crashing, the async pattern works.
        try await Task.sleep(for: .milliseconds(100))

        // Just verify we're still alive and the service is responsive.
        let snapshot = await service.snapshot()
        #expect(snapshot.duration == 100)
    }
}

// MARK: - Seam Injection (playhead-86s)

@Suite("PlaybackService – Injected System Seams")
struct PlaybackServiceSeamInjectionTests {

    /// With fakes injected, PlaybackService should talk to them instead of
    /// AVAudioSession.sharedInstance() / MPNowPlayingInfoCenter.default().
    /// This is the regression test for the parallel-test coupling that forced
    /// parallelizable: false in project.yml.
    @Test("Injected seams capture all audio-session + now-playing writes")
    func injectedSeamsReceiveCalls() async throws {
        let audio = FakeAudioSessionProvider()
        let nowPlaying = FakeNowPlayingInfoProvider()
        let center = NotificationCenter()

        let service = await PlaybackService(
            audioSession: audio,
            nowPlayingInfo: nowPlaying,
            notificationCenter: center
        )

        // Drive a code path that writes to the now-playing info seam.
        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 5,
            duration: 100,
            rate: 0,
            playbackSpeed: 1.0
        ))
        await service.setSpeed(1.5)

        // Give the actor's init-time configureAudioSession Task time to run.
        // The init hops onto PlaybackServiceActor and calls setCategory/setActive
        // from a detached Task, so we need a checkpoint on the actor before
        // asserting. Taking a snapshot serializes behind any pending work.
        _ = await service.snapshot()

        #expect(audio.categoryCalls.count == 1,
                "configureAudioSession should call setCategory exactly once")
        #expect(audio.setActiveCalls == [true],
                "configureAudioSession should activate the session")

        let info = nowPlaying.info
        #expect(info != nil, "setSpeed should push a now-playing update")
        #expect(info?[MPNowPlayingInfoPropertyDefaultPlaybackRate] as? Float == 1.5)

        await service.tearDown()
    }
}
