// RouteChangeTests.swift
// playhead-456 — E2E: Playback Reliability, scenario 4 (route changes).
//
// Real route changes (a Bluetooth headset disconnect, a wired-headset
// unplug) cannot be triggered from a unit-test process. Per CLAUDE.md
// guidance, we drive the OBSERVER side of the production code via the
// same notification iOS posts (`AVAudioSession.routeChangeNotification`)
// and assert PlaybackTransport's handler does the right thing:
// pause immediately on `oldDeviceUnavailable` (Apple policy), and
// require an explicit user tap to resume.

@preconcurrency import AVFoundation
import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private actor StatusBox {
    var statuses: [PlaybackState.Status] = []
    func append(_ s: PlaybackState.Status) { statuses.append(s) }
    func contains(_ s: PlaybackState.Status) -> Bool { statuses.contains(s) }
    var snapshot: [PlaybackState.Status] { statuses }
}

private func awaitStatus(
    in stream: AsyncStream<PlaybackState>,
    matching expected: PlaybackState.Status,
    deadline: Duration = .seconds(2)
) async -> [PlaybackState.Status] {
    let box = StatusBox()
    let drain = Task {
        for await state in stream {
            await box.append(state.status)
            if await box.contains(expected) { return }
        }
    }
    let clock = ContinuousClock()
    let end = clock.now.advanced(by: deadline)
    while clock.now < end {
        if await box.contains(expected) { break }
        try? await Task.sleep(for: .milliseconds(5))
    }
    drain.cancel()
    return await box.snapshot
}

@Suite("playhead-456 — Route changes")
struct RouteChangeTests {

    // MARK: - Disconnect

    @Test("Headphone disconnect (oldDeviceUnavailable) pauses immediately")
    func headphoneDisconnectPauses() async throws {
        // Apple App Store policy: when the user yanks headphones, audio
        // must pause. Production handles this via the
        // `.routeChangeNotification` async observer.
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 200,
            duration: 1200,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        let stream = await service.observeStates()

        await MainActor.run {
            center.post(
                name: AVAudioSession.routeChangeNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionRouteChangeReasonKey:
                        AVAudioSession.RouteChangeReason.oldDeviceUnavailable.rawValue
                ]
            )
        }

        let observed = await awaitStatus(in: stream, matching: .paused)
        #expect(observed.contains(.paused),
                "Headphone unplug must pause playback (Apple policy); observed: \(observed)")
    }

    @Test("Headphone disconnect preserves currentTime")
    func headphoneDisconnectPreservesPosition() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        let position: TimeInterval = 333.7
        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: position,
            duration: 1200,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        // Subscribe BEFORE posting so we never miss the pause transition.
        let stream = await service.observeStates()

        await MainActor.run {
            center.post(
                name: AVAudioSession.routeChangeNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionRouteChangeReasonKey:
                        AVAudioSession.RouteChangeReason.oldDeviceUnavailable.rawValue
                ]
            )
        }

        // Wait for the actor's notification handler to flip status.
        let observed = await awaitStatus(in: stream, matching: .paused)
        #expect(observed.contains(.paused),
                "Disconnect must pause; observed: \(observed)")

        let snap = await service.snapshot()
        #expect(snap.status == .paused)
        #expect(abs(snap.currentTime - position) < 0.1,
                "Position must not move on route change; got \(snap.currentTime)")
    }

    // MARK: - Other route-change reasons must NOT pause

    @Test("New device available (reconnect) does not auto-pause")
    func newDeviceAvailableDoesNotPause() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 200,
            duration: 1200,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        // Plugging headphones in mid-playback should NOT cause our
        // handler to pause — production handles only oldDeviceUnavailable.
        await MainActor.run {
            center.post(
                name: AVAudioSession.routeChangeNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionRouteChangeReasonKey:
                        AVAudioSession.RouteChangeReason.newDeviceAvailable.rawValue
                ]
            )
        }

        try await Task.sleep(for: .milliseconds(50))
        let snap = await service.snapshot()
        #expect(snap.status == .playing,
                "newDeviceAvailable must not pause; got \(snap.status)")
    }

    @Test("Category change does not pause")
    func categoryChangeDoesNotPause() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 50,
            duration: 1200,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        await MainActor.run {
            center.post(
                name: AVAudioSession.routeChangeNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionRouteChangeReasonKey:
                        AVAudioSession.RouteChangeReason.categoryChange.rawValue
                ]
            )
        }

        try await Task.sleep(for: .milliseconds(50))
        let snap = await service.snapshot()
        #expect(snap.status == .playing)
    }

    // MARK: - Reconnect + tap-to-play

    @Test("Reconnect headphones + user tap play resumes from same position")
    func reconnectAndTapPlayResumes() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )
        await service._testingInstallStubPlayerItem()

        // Step 1: playing on headphones.
        let position: TimeInterval = 100.0
        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: position,
            duration: 1200,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        // Subscribe to states before disconnect so we never miss the
        // pause transition (the actor's notification handler runs
        // asynchronously off our main-queue post).
        let stream = await service.observeStates()

        // Step 2: disconnect — service auto-pauses.
        await MainActor.run {
            center.post(
                name: AVAudioSession.routeChangeNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionRouteChangeReasonKey:
                        AVAudioSession.RouteChangeReason.oldDeviceUnavailable.rawValue
                ]
            )
        }
        let pausedObserved = await awaitStatus(in: stream, matching: .paused)
        #expect(pausedObserved.contains(.paused),
                "Disconnect must pause; observed: \(pausedObserved)")
        let pausedSnap = await service.snapshot()
        #expect(pausedSnap.status == .paused)
        #expect(abs(pausedSnap.currentTime - position) < 0.1)

        // Step 3: reconnect notification — does not auto-resume.
        await MainActor.run {
            center.post(
                name: AVAudioSession.routeChangeNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionRouteChangeReasonKey:
                        AVAudioSession.RouteChangeReason.newDeviceAvailable.rawValue
                ]
            )
        }
        try await Task.sleep(for: .milliseconds(50))
        let stillPausedSnap = await service.snapshot()
        #expect(stillPausedSnap.status == .paused,
                "Reconnect alone must not auto-resume; got \(stillPausedSnap.status)")

        // Step 4: user taps play — resumes from preserved position.
        await service.play()
        let resumedSnap = await service.snapshot()
        #expect(resumedSnap.status == .playing)
        #expect(abs(resumedSnap.currentTime - position) < 0.1,
                "Tap-to-play must resume from the disconnect position; got \(resumedSnap.currentTime)")
    }
}

// MARK: - Real-device-only scenarios deferred

// not testable in-process — deferred to manual QA / real-device gate:
//   * actual headphone unplug (real route hardware)
//   * Bluetooth disconnect mid-playback
//   * AirPlay route changes
