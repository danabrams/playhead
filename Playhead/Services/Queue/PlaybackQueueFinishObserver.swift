// PlaybackQueueFinishObserver.swift
// Bridge between the `playbackDidFinishEpisode` notification and the
// queue's `PlaybackQueueAutoAdvancer`. Registers a synchronous block
// observer with NotificationCenter; the block kicks off an unstructured
// Task that calls `advance()` on the advancer actor.
//
// Why a callback observer rather than `for await center.notifications(...)`:
// the async-sequence path only begins receiving posts once the consumer
// task has actively entered its `for await` loop. Under heavy parallel
// test load (PlayheadFastTests runs ~5000 tests across ~700 suites), a
// notification posted shortly after `start()` can race the consumer
// task and be silently dropped. `addObserver(forName:object:queue:using:)`
// is fully registered by the time the call returns, so no race exists.
//
// Lives outside any actor — its sole responsibility is wiring; the
// real serialization happens inside the advancer (which is itself an
// actor).

import Foundation

final class PlaybackQueueFinishObserver: @unchecked Sendable {

    private let center: NotificationCenter
    private let advancer: PlaybackQueueAutoAdvancer
    private var token: NSObjectProtocol?

    init(center: NotificationCenter, advancer: PlaybackQueueAutoAdvancer) {
        self.center = center
        self.advancer = advancer
    }

    /// Begin observing `playbackDidFinishEpisode`. Idempotent — calling
    /// `start()` while already started is a no-op.
    func start() {
        guard token == nil else { return }
        let advancer = self.advancer
        // `queue: nil` delivers the block synchronously on the posting
        // thread. We dispatch the actual advance into an unstructured
        // Task so the (possibly main-thread) post call returns
        // immediately and the actor-isolated work runs on its own
        // executor.
        token = center.addObserver(
            forName: .playbackDidFinishEpisode,
            object: nil,
            queue: nil
        ) { _ in
            Task { await advancer.advance() }
        }
    }

    /// Stop observing. After `stop()`, no further notifications drive
    /// the advancer; an in-flight `advance()` is allowed to complete
    /// (we do not call `advancer.cancel()` here because the user may
    /// have other reasons for an advance to be in progress, e.g. they
    /// just tapped "Play next" manually).
    func stop() {
        if let token { center.removeObserver(token) }
        token = nil
    }

    deinit {
        if let token { center.removeObserver(token) }
    }
}
