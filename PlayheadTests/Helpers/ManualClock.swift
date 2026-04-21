// ManualClock.swift
// playhead-e2vw: deterministic, monotonic test clock used by the
// cascade-attributed proximal-readiness SLI test
// (`CandidateWindowCascadeProximalReadinessSLITest`) to drive the
// real cascade + scheduler selection path off synthetic time.
//
// What this is
// ------------
// A thread-safe, advance-by-call wall-clock substitute. The clock
// holds a `Date` value; `now()` returns the current value (monotonic
// — `advance(by:)` only ever moves forward, never back). The
// production actors that accept `clock: @Sendable () -> Date` invoke
// `now()` whenever they would have called `Date()` directly, so a
// test can drive their `createdAt` / `nextEligibleAt` /
// `leaseExpiresAt` timestamps off a fully synthetic timeline.
//
// What this is NOT
// ----------------
// Not a Swift `Clock` conformance — production code uses
// `() -> Date` closures (matching the existing `BackfillJobRunner`
// pattern), so this helper exposes a closure form via the
// `dateProvider` accessor rather than a `Clock`-protocol surface.
// Tests that need actor-friendly time advance use `advance(by:)`
// rather than `Task.sleep`, because the actors under test never
// suspend on the clock — they sample it inline.

import Foundation

/// Monotonic synthetic clock for tests. Thread-safe via internal
/// lock. Provides a closure (`dateProvider`) suitable for the
/// `clock: @escaping @Sendable () -> Date` init parameter on
/// `AnalysisWorkScheduler`, `AnalysisJobRunner`, and
/// `CandidateWindowCascade`.
///
/// Usage:
///
/// ```swift
/// let clock = ManualClock(start: Date(timeIntervalSince1970: 1_000_000))
/// let scheduler = AnalysisWorkScheduler(
///     // ... other deps ...
///     clock: clock.dateProvider
/// )
/// // ... drive the scheduler ...
/// clock.advance(by: 60)  // 60 seconds forward
/// // ... continue driving — subsequent timestamps reflect the new now ...
/// ```
final class ManualClock: @unchecked Sendable {
    private let lock = NSLock()
    private var current: Date

    /// Initialise at an arbitrary epoch-anchored start so timestamps
    /// in tests look like real Unix times (round-numbered, far from
    /// the 1970 origin). The default value is deliberately
    /// arbitrary — pick a different start if your test asserts on
    /// absolute timestamps.
    init(start: Date = Date(timeIntervalSince1970: 1_700_000_000)) {
        self.current = start
    }

    /// Current synthetic time. Safe to call from any thread.
    func now() -> Date {
        lock.lock()
        defer { lock.unlock() }
        return current
    }

    /// Advance the clock forward by `seconds`. Negative values are
    /// rejected via precondition — the clock is monotonic by design
    /// because the production code stores timestamps that only ever
    /// increase (e.g. `nextEligibleAt`, `leaseExpiresAt`, `createdAt`),
    /// and a backwards jump would let the test create state that
    /// production could never produce.
    func advance(by seconds: TimeInterval) {
        precondition(seconds >= 0, "ManualClock.advance is monotonic; got negative \(seconds)")
        lock.lock()
        defer { lock.unlock() }
        current = current.addingTimeInterval(seconds)
    }

    /// Closure-form accessor matching the actors' `clock` init
    /// parameter signature. Capturing `self` is safe because the
    /// closure carries the same internal lock the direct calls use.
    var dateProvider: @Sendable () -> Date {
        { [weak self] in
            self?.now() ?? Date()
        }
    }
}
