// FMClockPair.swift
// playhead-rkfp / playhead-ezmv: one start instant, two clocks — so a latency
// can say how much of itself the process actually lived through.
//
// THE DEFECT THIS CLOSES is the standing class "a value that names one thing,
// read as though it named another". `semantic_scan_results.latencyMs` is a
// ContinuousClock span around a window attempt, and it was read as "FM cost".
// The 2026-08-06 device pull's worst row read 1,955.6 s that way; grant
// records prove 1,504 s of it (76.9%) was the app frozen between two
// background grants (08:20:39 → 08:45:43), when no inference and no waiting
// happened at all. One number cannot carry both populations, so every passA
// row now carries two, measured over the SAME span from the SAME start
// instant:
//
//   latencyMs            ContinuousClock — elapsed wall-clock, freezes and
//                        device sleep included. Unchanged meaning, unchanged
//                        history.
//   suspendingLatencyMs  SuspendingClock — the same span excluding device
//                        sleep. latencyMs − suspendingLatencyMs is the
//                        device-asleep share of the row.
//
// STATED LIMIT, same as the deadline's: `SuspendingClock` pauses during
// DEVICE sleep, not during app suspension with the device awake, so a
// frozen-but-awake stretch still lands in both numbers. The split is a lower
// bound on "time nobody was inferring", never an upper one.

import Foundation

/// A paired reading of `ContinuousClock` and `SuspendingClock` taken at one
/// instant. Take one with ``now()`` when a measured span begins; ask it for
/// ``FMClockPair/elapsed(continuousNow:suspendingNow:)`` when the span ends.
struct FMClockPair: Sendable {
    let continuousStart: ContinuousClock.Instant
    let suspendingStart: SuspendingClock.Instant

    /// Both clocks, read back to back. The reads are non-atomic; the skew is
    /// sub-microsecond and shared by both ends of a span, so it cancels.
    static func now() -> FMClockPair {
        FMClockPair(
            continuousStart: ContinuousClock().now,
            suspendingStart: SuspendingClock().now
        )
    }

    /// Milliseconds elapsed on each clock since this pair was taken.
    ///
    /// `continuousMs` is the population `latencyMs` has always recorded;
    /// `suspendingMs` is the same span excluding device sleep. On an awake
    /// device the two are equal to within scheduler noise; `suspendingMs`
    /// can never meaningfully exceed `continuousMs`.
    ///
    /// The `now` parameters exist so a test can hand in synthetic instants
    /// and prove which clock feeds which number — with live clocks the two
    /// values agree and a crossed wire would be invisible.
    func elapsed(
        continuousNow: ContinuousClock.Instant = ContinuousClock().now,
        suspendingNow: SuspendingClock.Instant = SuspendingClock().now
    ) -> FMClockPairReading {
        FMClockPairReading(
            continuousMs: Self.milliseconds(of: continuousNow - continuousStart),
            suspendingMs: Self.milliseconds(of: suspendingNow - suspendingStart)
        )
    }

    private static func milliseconds(of duration: Duration) -> Double {
        Double(duration.components.seconds) * 1_000
            + Double(duration.components.attoseconds) / 1e15
    }
}

/// One span, two clocks. See ``FMClockPair`` for which number names which
/// population.
struct FMClockPairReading: Sendable, Equatable {
    /// Elapsed on the continuous clock — wall-clock, sleep included.
    let continuousMs: Double
    /// Elapsed on the suspending clock — the same span, device sleep excluded.
    let suspendingMs: Double
}
