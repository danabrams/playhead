// PlaybackQueueEnvironment.swift
// SwiftUI environment plumbing for the playback queue service. Views
// (Library swipe actions, NowPlaying "Up Next" button, QueueView)
// pull `PlaybackQueueService` and an `@MainActor` enqueue closure
// from this environment.
//
// We expose two pieces:
//
//   * `\.playbackQueueService` — the actor itself, for views that
//     want to `await` directly (e.g. QueueViewModel).
//   * `\.enqueueLast` / `\.enqueueNext` — `@MainActor` closures that
//     wrap the actor calls in a `Task` and translate failures into a
//     no-op + log. View code that just wants "tap to queue this
//     episode" doesn't need to think about actor boundaries.
//
// `\.playbackQueueService` defaults to `nil` so unit tests / previews
// that never set the environment compile cleanly without a service.

import SwiftUI

private struct PlaybackQueueServiceKey: EnvironmentKey {
    static let defaultValue: PlaybackQueueService? = nil
}

extension EnvironmentValues {
    var playbackQueueService: PlaybackQueueService? {
        get { self[PlaybackQueueServiceKey.self] }
        set { self[PlaybackQueueServiceKey.self] = newValue }
    }
}

/// Convenience for views that just want a fire-and-forget
/// `enqueueLast(episodeKey)` shape rather than reaching into the
/// actor directly.
@MainActor
extension View {
    /// Returns a closure that enqueues an episode at the tail of the
    /// playback queue. No-op when no service is in the environment
    /// (preview / test contexts).
    func makeEnqueueLast(
        _ service: PlaybackQueueService?
    ) -> @MainActor (String) -> Void {
        return { key in
            guard let service else { return }
            Task { try? await service.addLast(episodeKey: key) }
        }
    }

    /// Returns a closure that enqueues an episode at the head of the
    /// playback queue.
    func makeEnqueueNext(
        _ service: PlaybackQueueService?
    ) -> @MainActor (String) -> Void {
        return { key in
            guard let service else { return }
            Task { try? await service.addNext(episodeKey: key) }
        }
    }
}
