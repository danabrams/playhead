// ListeningTimeAccumulator.swift
// playhead-jw63.3 — turns the playback status stream into wall-clock
// listening seconds, which is the north-star denominator.
//
// Why a pure value type driven from outside: the accumulator must be
// exercisable without a player, a simulator, or a clock that actually
// advances. Everything about it — pause, interruption, route unplug,
// stall, episode end, backgrounding, a device clock that jumps — is a
// sequence of `(status, instant)` pairs, so it is tested as one.
//
// Rules it encodes:
//   * Only `.playing` accrues. `.paused` covers user pause, phone-call
//     interruption, headphone unplug, buffering stalls and end-of-episode,
//     because `PlaybackService` funnels all of those through `pause()` or
//     through the rate-KVO's `.playing where rate == 0` branch.
//   * Backgrounding does **not** stop the clock. Playhead has the `audio`
//     background mode; audio in your ears while the screen is off is
//     listening. The app instead *commits* the partial interval on the
//     `.background` scene-phase transition so an OS kill loses at most the
//     seconds since the last commit rather than the whole session.
//   * A backwards or absurd clock reading contributes zero rather than a
//     negative or wild number. `ContinuousClock` is monotonic, so this can
//     only mean a programming error, and analytics is not a subsystem that
//     gets to trust its inputs.

import Foundation

/// Accumulates wall-clock listening time from playback status transitions.
///
/// Not thread-safe by design — it is driven from a single consumer of
/// `PlaybackService.observeStates()`.
struct ListeningTimeAccumulator {
    /// Longest single interval that will be credited, in seconds. A gap
    /// larger than this means the process was suspended or the consumer
    /// stalled, not that someone listened for six hours without a single
    /// status transition; crediting it would silently inflate the
    /// denominator and make the north-star ratio look better than it is.
    static let maximumCreditedIntervalSeconds: TimeInterval = 4 * 60 * 60

    private var playingSince: ContinuousClock.Instant?
    private var isPlaying = false

    init() {}

    /// Whether the accumulator currently believes audio is playing.
    var isAccruing: Bool { isPlaying }

    /// Feeds a status observation. Returns whole seconds to credit, if any.
    ///
    /// Fractional remainders are carried forward rather than discarded, so
    /// a long session of short intervals does not systematically round the
    /// denominator down.
    mutating func observe(
        isPlaying nowPlaying: Bool,
        at instant: ContinuousClock.Instant
    ) -> TimeInterval {
        var credited: TimeInterval = 0
        if isPlaying {
            credited = elapsed(until: instant)
        }
        isPlaying = nowPlaying
        playingSince = nowPlaying ? instant : nil
        return credited
    }

    /// Commits the interval accrued so far without changing play state.
    /// Called on the `.background` scene-phase transition and before
    /// process teardown.
    mutating func commit(at instant: ContinuousClock.Instant) -> TimeInterval {
        guard isPlaying else { return 0 }
        let credited = elapsed(until: instant)
        playingSince = instant
        return credited
    }

    private mutating func elapsed(until instant: ContinuousClock.Instant) -> TimeInterval {
        guard let start = playingSince else { return 0 }
        let duration = start.duration(to: instant)
        let seconds = TimeInterval(duration.components.seconds)
            + TimeInterval(duration.components.attoseconds) / 1e18
        guard seconds > 0 else { return 0 }
        return min(seconds, Self.maximumCreditedIntervalSeconds)
    }
}

/// Converts credited fractional seconds into whole seconds without losing
/// the remainder across calls. Kept separate from the accumulator so the
/// rounding policy is testable on its own.
struct ListeningSecondsQuantizer {
    private var carry: TimeInterval = 0

    init() {}

    /// Adds `seconds` to the carry and returns whole seconds to record.
    mutating func take(_ seconds: TimeInterval) -> Int {
        guard seconds.isFinite, seconds > 0 else { return 0 }
        carry += seconds
        let whole = carry.rounded(.down)
        guard whole >= 1 else { return 0 }
        carry -= whole
        return Int(min(whole, TimeInterval(Int.max)))
    }
}
