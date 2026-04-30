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

        // Force a hop through PlaybackServiceActor BEFORE posting. The
        // service's init enqueues a `Task { @PlaybackServiceActor in
        // observePlayerItemFinishAsync() }` which itself spawns a
        // child task that performs the `for await` subscription.
        // `observeStates()` re-enters the same actor and serializes
        // after the init-time work — by the time it returns, the
        // observer task chain has been scheduled. Mirrors the pattern
        // that keeps `InterruptionHandlingTests` deterministic under
        // PlayheadFastTests parallel load.
        _ = await service.observeStates()

        // Subscribe via the synchronous addObserver API. By the time
        // addObserver returns, the observer is registered — no race
        // against the consumer task starting iteration.
        let receivedFlag = NotificationFlag()
        nonisolated(unsafe) let token = center.addObserver(
            forName: .playbackDidFinishEpisode,
            object: nil,
            queue: nil
        ) { _ in
            receivedFlag.markFired()
        }
        defer { center.removeObserver(token) }

        // The service's observer still uses the async-sequence path,
        // so its `for await` may take a beat to start consuming.
        // Repeat the trigger post on a short cadence; re-broadcast is
        // idempotent so any retries after the first reception are
        // harmless.
        let deadline = Date().addingTimeInterval(3.0)
        while Date() < deadline {
            if receivedFlag.didFire() { break }
            center.post(
                name: AVPlayerItem.didPlayToEndTimeNotification,
                object: nil
            )
            try await Task.sleep(for: .milliseconds(50))
        }

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
