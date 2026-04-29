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

/// Drain a state stream to a list of statuses up to a wall-clock
/// deadline. Returns the observed status sequence so the test body
/// can assert ordering when needed (e.g. playing → paused → playing).
private func collectStatuses(
    from stream: AsyncStream<PlaybackState>,
    until predicate: @Sendable @escaping ([PlaybackState.Status]) -> Bool,
    deadline: Duration = .seconds(2)
) async -> [PlaybackState.Status] {
    let box = StatusBox()
    let drain = Task {
        for await state in stream {
            await box.append(state.status)
            if await box.matches(predicate) { return }
        }
    }
    let clock = ContinuousClock()
    let end = clock.now.advanced(by: deadline)
    while clock.now < end {
        if await box.matches(predicate) { break }
        try? await Task.sleep(for: .milliseconds(5))
    }
    drain.cancel()
    return await box.statuses
}

private actor StatusBox {
    var statuses: [PlaybackState.Status] = []
    func append(_ s: PlaybackState.Status) { statuses.append(s) }
    func matches(_ predicate: ([PlaybackState.Status]) -> Bool) -> Bool {
        predicate(statuses)
    }
}

@Suite("playhead-456 — Interruption handling")
struct PlaybackReliabilityInterruptionTests {

    // MARK: - Phone call interruption

    @Test("Phone call interruption pauses playback")
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

    @Test("Phone call ends with shouldResume → playback resumes")
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

    @Test("Siri activation interruption pauses playback")
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

    @Test("Siri dismissal with shouldResume restores playback")
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

    @Test("Full interruption cycle: playing → paused → playing")
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
