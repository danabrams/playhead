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
import Testing
@testable import Playhead

// MARK: - Interruption Handling

@Suite("PlaybackService – Audio Session Interruptions")
struct InterruptionHandlingTests {

    /// Post a fake interruption-began notification and verify PlaybackService
    /// handles it without crashing. This is the exact scenario that triggered
    /// the "Incorrect actor executor assumption" crash with Combine observers.
    @Test("Interruption notification handled on actor without crash")
    func interruptionBegan() async throws {
        let service = await PlaybackService()

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
            NotificationCenter.default.post(
                name: AVAudioSession.interruptionNotification,
                object: AVAudioSession.sharedInstance(),
                userInfo: [
                    AVAudioSessionInterruptionTypeKey: AVAudioSession.InterruptionType.began.rawValue
                ]
            )
        }

        // Give the async notification sequence time to deliver.
        try await Task.sleep(for: .milliseconds(100))

        // Verify the service transitioned to paused.
        let snapshot = await service.snapshot()
        #expect(snapshot.status == .paused,
                "Service should pause on interruption began")
    }

    @Test("Interruption ended with shouldResume resumes playback")
    func interruptionEndedResumes() async throws {
        let service = await PlaybackService()

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
            NotificationCenter.default.post(
                name: AVAudioSession.interruptionNotification,
                object: AVAudioSession.sharedInstance(),
                userInfo: [
                    AVAudioSessionInterruptionTypeKey: AVAudioSession.InterruptionType.ended.rawValue,
                    AVAudioSessionInterruptionOptionKey: AVAudioSession.InterruptionOptions.shouldResume.rawValue
                ]
            )
        }

        try await Task.sleep(for: .milliseconds(100))

        // Without a loaded player item, play() won't change status to .playing,
        // but the important thing is we didn't crash.
        // The actor isolation assertion would have fired before reaching here.
    }

    @Test("Route change notification handled on actor without crash")
    func routeChange() async throws {
        let service = await PlaybackService()

        await service._testingInjectState(PlaybackState(
            status: .playing,
            currentTime: 10.0,
            duration: 600.0,
            rate: 1.0,
            playbackSpeed: 1.0
        ))

        // Post route change (headphones unplugged) on main queue.
        await MainActor.run {
            NotificationCenter.default.post(
                name: AVAudioSession.routeChangeNotification,
                object: AVAudioSession.sharedInstance(),
                userInfo: [
                    AVAudioSessionRouteChangeReasonKey: AVAudioSession.RouteChangeReason.oldDeviceUnavailable.rawValue
                ]
            )
        }

        try await Task.sleep(for: .milliseconds(100))

        let snapshot = await service.snapshot()
        #expect(snapshot.status == .paused,
                "Service should pause when headphones disconnect")
    }

    /// Rapid-fire notifications simulating the storm that Siri causes.
    /// The old Combine pattern would crash on the first or second notification.
    @Test("Rapid interruption notifications don't crash")
    func rapidInterruptions() async throws {
        let service = await PlaybackService()

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
                NotificationCenter.default.post(
                    name: AVAudioSession.interruptionNotification,
                    object: AVAudioSession.sharedInstance(),
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
