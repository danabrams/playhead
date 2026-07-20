// InterruptionHandlingTests.swift
// playhead-456 — E2E: Playback Reliability, scenario 3 (interruptions).
//
// Real iOS lifecycle events (incoming phone call, Siri activation) cannot
// be triggered from a unit-test process. Per CLAUDE.md guidance, we drive
// the OBSERVER side of the production code via the same notifications
// iOS would post (`AVAudioSession.interruptionNotification`) and assert
// the production handlers in PlaybackTransport.swift do the right thing.
//
// Coverage gap vs the existing
// `PlayheadTests/Services/PlaybackTransport/InterruptionHandlingTests.swift`:
// that file is a Swift-6-isolation regression suite (the Siri crash from
// playhead-86s era). The tests here cover the behavioral spec of the
// bead — pause-then-resume, with-and-without shouldResume, ducking
// behavior — using the same notification-injection seam.

@preconcurrency import AVFoundation
import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

/// Drain the state stream ON THE TEST'S OWN TASK until `predicate` is
/// satisfied by the accumulated status sequence, then return everything
/// observed (so the test body can assert ordering, e.g. playing →
/// paused → playing).
///
/// playhead-vsot round 3: the previous version drained in a child task
/// while the test polled a shared box under a 2 s wall-clock deadline —
/// under the full parallel plan the actor's notification handler (or
/// the drain task itself) was not scheduled inside 2 s and the tests
/// failed with `observed=[]` at ~119 s wall (2026-07-20 double-parallel
/// gate: "Siri activation interruption pauses playback" / "Phone call
/// interruption pauses playback"). This is the SAME shape already fixed
/// in InterruptionHandlingTests + RouteChangeTests (round 2). The stream
/// IS the actor-handled signal — the handler's `pause()`/`play()` runs
/// on PlaybackServiceActor and yields the new state to every observer,
/// and `observeStates()` yields the current snapshot immediately on
/// subscribe, so subscribing before the notification post cannot miss a
/// transition. No deadline: if production stops pausing/resuming the
/// drain parks forever and the test's `.timeLimit` trait fails
/// deterministically instead of load-dependently.
private func collectStatuses(
    from stream: AsyncStream<PlaybackState>,
    until predicate: @Sendable @escaping ([PlaybackState.Status]) -> Bool
) async -> [PlaybackState.Status] {
    var observed: [PlaybackState.Status] = []
    for await state in stream {
        observed.append(state.status)
        if predicate(observed) { break }
    }
    return observed
}

@Suite("playhead-456 — Interruption handling")
struct PlaybackReliabilityInterruptionTests {

    // MARK: - Phone call interruption

    @Test("Phone call interruption pauses playback", .timeLimit(.minutes(1)))
    func phoneCallInterruptionPauses() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 60,
            duration: 1800,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        let stream = await service.observeStates()

        await MainActor.run {
            center.post(
                name: AVAudioSession.interruptionNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionInterruptionTypeKey:
                        AVAudioSession.InterruptionType.began.rawValue
                ]
            )
        }

        let observed = await collectStatuses(from: stream) {
            $0.contains(.paused)
        }
        #expect(observed.contains(.paused),
                "Phone call interruption must pause playback; observed: \(observed)")
    }

    @Test("Phone call ends with shouldResume → playback resumes", .timeLimit(.minutes(1)))
    func phoneCallEndsResumes() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )
        // Service must hold a player item for play() to succeed.
        await service._testingInstallStubPlayerItem()

        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 60,
            duration: 1800,
            rate: 0,
            playbackSpeed: 1.0
        ))

        let stream = await service.observeStates()

        await MainActor.run {
            center.post(
                name: AVAudioSession.interruptionNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionInterruptionTypeKey:
                        AVAudioSession.InterruptionType.ended.rawValue,
                    AVAudioSessionInterruptionOptionKey:
                        AVAudioSession.InterruptionOptions.shouldResume.rawValue
                ]
            )
        }

        let observed = await collectStatuses(from: stream) {
            $0.contains(.playing)
        }
        #expect(observed.contains(.playing),
                "Interruption-ended with shouldResume must resume playback; observed: \(observed)")
    }

    @Test("Phone call ends without shouldResume → playback stays paused")
    func phoneCallEndsWithoutShouldResumeStaysPaused() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 60,
            duration: 1800,
            rate: 0,
            playbackSpeed: 1.0
        ))

        await MainActor.run {
            center.post(
                name: AVAudioSession.interruptionNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionInterruptionTypeKey:
                        AVAudioSession.InterruptionType.ended.rawValue
                    // No shouldResume in options.
                ]
            )
        }

        // Give the actor a moment to process the no-op notification.
        try await Task.sleep(for: .milliseconds(50))

        let snap = await service.snapshot()
        #expect(snap.status == .paused,
                "Interruption-ended without shouldResume must not auto-play; got \(snap.status)")
    }

    // MARK: - Siri activation

    @Test("Siri activation interruption pauses playback", .timeLimit(.minutes(1)))
    func siriActivationPauses() async throws {
        // iOS posts the same `.interruptionNotification` for Siri as for
        // a phone call, distinguished only by the .began type. The bead
        // also allows "ducks OR pauses"; production chose pause, which
        // is what we verify here.
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 5,
            duration: 600,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        let stream = await service.observeStates()

        await MainActor.run {
            center.post(
                name: AVAudioSession.interruptionNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionInterruptionTypeKey:
                        AVAudioSession.InterruptionType.began.rawValue
                ]
            )
        }

        let observed = await collectStatuses(from: stream) {
            $0.contains(.paused)
        }
        #expect(observed.contains(.paused),
                "Siri activation must duck/pause; observed: \(observed)")
    }

    @Test("Siri dismissal with shouldResume restores playback", .timeLimit(.minutes(1)))
    func siriDismissResumes() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )
        await service._testingInstallStubPlayerItem()

        await service._testingInjectState(PlaybackState(
            status: .paused,
            currentTime: 5,
            duration: 600,
            rate: 0,
            playbackSpeed: 1.0
        ))

        let stream = await service.observeStates()

        await MainActor.run {
            center.post(
                name: AVAudioSession.interruptionNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionInterruptionTypeKey:
                        AVAudioSession.InterruptionType.ended.rawValue,
                    AVAudioSessionInterruptionOptionKey:
                        AVAudioSession.InterruptionOptions.shouldResume.rawValue
                ]
            )
        }

        let observed = await collectStatuses(from: stream) {
            $0.contains(.playing)
        }
        #expect(observed.contains(.playing),
                "Siri dismissal must resume; observed: \(observed)")
    }

    // MARK: - Full pause→resume cycle

    @Test("Full interruption cycle: playing → paused → playing", .timeLimit(.minutes(1)))
    func fullInterruptionCycle() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )
        await service._testingInstallStubPlayerItem()

        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 12.5,
            duration: 600,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        let stream = await service.observeStates()

        await MainActor.run {
            center.post(
                name: AVAudioSession.interruptionNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionInterruptionTypeKey:
                        AVAudioSession.InterruptionType.began.rawValue
                ]
            )
            center.post(
                name: AVAudioSession.interruptionNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionInterruptionTypeKey:
                        AVAudioSession.InterruptionType.ended.rawValue,
                    AVAudioSessionInterruptionOptionKey:
                        AVAudioSession.InterruptionOptions.shouldResume.rawValue
                ]
            )
        }

        let observed = await collectStatuses(from: stream) { statuses in
            // We need both transitions visible.
            statuses.contains(.paused) && statuses.contains(.playing)
        }
        #expect(observed.contains(.paused),
                "Cycle must include pause; observed: \(observed)")
        #expect(observed.contains(.playing),
                "Cycle must include resume; observed: \(observed)")

        // Position must not have moved across the cycle.
        let snap = await service.snapshot()
        #expect(abs(snap.currentTime - 12.5) < 0.1,
                "Interruption cycle must not move the playhead; got \(snap.currentTime)")
    }
}

// MARK: - Real-device-only scenarios deferred

// not testable in-process — deferred to manual QA / real-device gate:
//   * actual incoming phone call (CallKit/system UI)
//   * actual Siri activation via "Hey Siri" or button hold
//   * audio ducking ramps (CoreAudio level)
