// PlaybackQueueFinishObserver.swift
// Bridge between the `playbackDidFinishEpisode` notification and the
// queue's `PlaybackQueueAutoAdvancer`. Holds a long-running Task that
// awaits each notification and calls `advance()`.
//
// Lives outside any actor — its sole responsibility is wiring; the
// real serialization happens inside the advancer (which is itself an
// actor).

import Foundation

final class PlaybackQueueFinishObserver: @unchecked Sendable {

    private let center: NotificationCenter
    private let advancer: PlaybackQueueAutoAdvancer
    private var task: Task<Void, Never>?

    init(center: NotificationCenter, advancer: PlaybackQueueAutoAdvancer) {
        self.center = center
        self.advancer = advancer
    }

    /// Begin observing `playbackDidFinishEpisode`. Idempotent — calling
    /// `start()` while already started is a no-op.
    func start() {
        guard task == nil else { return }
        task = Task { [center, advancer] in
            let notifications = center.notifications(named: .playbackDidFinishEpisode)
            for await _ in notifications {
                if Task.isCancelled { return }
                await advancer.advance()
            }
        }
    }

    /// Stop observing. After `stop()`, no further notifications drive
    /// the advancer; an in-flight `advance()` is allowed to complete
    /// (we do not call `advancer.cancel()` here because the user may
    /// have other reasons for an advance to be in progress, e.g. they
    /// just tapped "Play next" manually).
    func stop() {
        task?.cancel()
        task = nil
    }

    deinit {
        task?.cancel()
    }
}
