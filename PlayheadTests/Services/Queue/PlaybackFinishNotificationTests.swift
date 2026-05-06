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

        // PlaybackService.init now installs the player-item-finish
        // observer SYNCHRONOUSLY (block-based `addObserver`) before
        // returning, so by the time the `await PlaybackService(...)`
        // call lands the observer is live on `center`. The previous
        // `for await` async-sequence path needed a poll-with-deadline
        // workaround because its subscription registered on a child
        // Task — that race is gone, so a single post suffices.
        let receivedFlag = NotificationFlag()
        nonisolated(unsafe) let token = center.addObserver(
            forName: .playbackDidFinishEpisode,
            object: nil,
            queue: nil
        ) { _ in
            receivedFlag.markFired()
        }
        defer { center.removeObserver(token) }

        // `addObserver(forName:object:queue:nil ...)` delivers blocks
        // synchronously on the posting thread, so this single post
        // walks: center.post → production observer block → secondary
        // post → test observer block → receivedFlag.markFired()
        // before the call returns.
        center.post(
            name: AVPlayerItem.didPlayToEndTimeNotification,
            object: nil
        )

        #expect(receivedFlag.didFire(),
                "Expected PlaybackDidFinishEpisode notification to be posted")
        _ = service
    }
}

/// Tiny thread-safe flag for the notification-arrival test. The
/// observer block runs on a delivery thread chosen by NotificationCenter
/// (we passed `queue: nil`), so the flag's set/read pair must be
/// concurrency-safe. A `Mutex` from Swift's stdlib would be cleaner
/// but isn't available pre-iOS 18; an OSAllocatedUnfairLock works
/// without raising the Swift Testing target's deployment floor.
private final class NotificationFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var fired = false

    func markFired() {
        lock.lock()
        fired = true
        lock.unlock()
    }

    func didFire() -> Bool {
        lock.lock()
        let result = fired
        lock.unlock()
        return result
    }
}
