// PlaybackQueueAutoAdvancer.swift
// Bridges "the current episode finished" → "play the next queued
// episode after a brief, cancellable countdown".
//
// Why a separate type (vs. baking this into `PlaybackQueueService`):
//   * The advancer holds a *task handle* across calls so it can be
//     cancelled mid-countdown. Owning that handle on the queue service
//     would conflate persistence concerns with playback orchestration.
//   * Tests can drive the advancer with a fake `playHandler` and
//     `countdown: .zero` without touching `PlaybackService` at all,
//     keeping the unit tests fast and deterministic.
//
// Threading: the advancer is an actor; concurrent `advance()` calls
// are guarded by an "in-flight" flag so a re-entrant call (e.g. a
// duplicate `didPlayToEndTime` notification, which AVFoundation does
// emit in some interruption scenarios) cannot pop two entries.
//
// Cancellation: `cancel()` sets a flag and cancels the active sleep
// `Task`. After cancellation the in-flight call returns without
// invoking the play handler and without popping the queue.

import Foundation

actor PlaybackQueueAutoAdvancer {

    private let queue: PlaybackQueueService
    private let countdown: Duration
    /// Closure the advancer invokes once the countdown completes and the
    /// next entry is popped. The runtime wires this to `playEpisode(_:)`.
    /// Sendable so it can cross the actor boundary.
    private let playHandler: @Sendable (String) async -> Void

    /// Active sleep task. Set when `advance()` enters its countdown,
    /// cleared on completion or cancellation.
    private var activeTask: Task<Void, Never>?
    /// `true` while `advance()` is mid-flight. Guards against re-entrant
    /// calls producing two pops for one finish event.
    private var isAdvancing: Bool = false
    /// `true` when `cancel()` has been called for the active advance.
    /// Prevents the play handler from firing if cancellation happens
    /// during the countdown sleep.
    private var isCancelled: Bool = false

    init(
        queue: PlaybackQueueService,
        countdown: Duration,
        playHandler: @escaping @Sendable (String) async -> Void
    ) {
        self.queue = queue
        self.countdown = countdown
        self.playHandler = playHandler
    }

    /// Drive one auto-advance cycle: wait the countdown, pop the next
    /// queued entry, ask the play handler to start it. If cancelled
    /// during the countdown, bails without popping.
    func advance() async {
        // Re-entrancy guard: a duplicate finish notification cannot
        // trigger a second pop.
        guard !isAdvancing else { return }
        isAdvancing = true
        isCancelled = false
        defer {
            isAdvancing = false
            activeTask = nil
        }

        // Sleep the countdown inside a child Task so `cancel()` can
        // halt it via `Task.cancel()`. We do not propagate the
        // cancellation as an error — instead we observe `isCancelled`.
        let sleep = Task<Void, Never> { [countdown] in
            try? await Task.sleep(for: countdown)
        }
        activeTask = sleep
        await sleep.value

        guard !isCancelled else { return }

        // Pop the head and ask the runtime to play it. Errors from
        // popNext are swallowed — the queue is best-effort here, and a
        // failed pop should not crash the runtime mid-playback.
        guard let row = try? await queue.popNext() else { return }
        await playHandler(row.episodeKey)
    }

    /// Cancel the in-flight `advance()` call. The play handler will not
    /// fire; the queue is not popped. No-op when no advance is active.
    func cancel() {
        isCancelled = true
        activeTask?.cancel()
    }

    /// Whether an advance is currently mid-countdown. Mostly useful for
    /// UI ("show the countdown overlay") and diagnostics.
    var isInFlight: Bool {
        isAdvancing
    }
}
