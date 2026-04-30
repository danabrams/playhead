// PlaybackFinishNotificationTests.swift
// Verifies that `PlaybackService` posts a `PlaybackDidFinishEpisode`
// notification when its underlying AVPlayerItem reaches the end. The
// queue's auto-advancer subscribes to this notification — without it,
// auto-advance can't fire.

import AVFoundation
import Foundation
import Testing
@testable import Playhead

@Suite("PlaybackService — finish notification")
struct PlaybackFinishNotificationTests {

    @Test("PlaybackService posts PlaybackDidFinishEpisode when AVPlayerItem reaches end")
    func postsFinishNotification() async throws {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        // Subscribe BEFORE we synthesize the AVPlayerItem-end notification
        // so the async sequence captures the post deterministically.
        let received = Task { () -> Bool in
            for await _ in center.notifications(named: .playbackDidFinishEpisode) {
                return true
            }
            return false
        }

        // Yield once so the service's notification observer task has a
        // chance to start consuming the stream.
        try await Task.sleep(for: .milliseconds(20))

        // Synthesize the AVPlayerItem-end-time notification on the
        // injected center; the service's observer should re-broadcast
        // PlaybackDidFinishEpisode.
        center.post(
            name: AVPlayerItem.didPlayToEndTimeNotification,
            object: nil
        )

        // Wait up to ~1s for the re-broadcast.
        let timeoutTask = Task {
            try? await Task.sleep(for: .seconds(1))
            received.cancel()
            return false
        }

        let outcome = await received.value
        timeoutTask.cancel()
        #expect(outcome, "Expected PlaybackDidFinishEpisode notification to be posted")
        _ = service
    }
}
