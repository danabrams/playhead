// BackgroundGrantBudget.swift
// playhead-lmrx: how much work a BGTask handler may ATTEMPT inside the window
// the OS actually grants it.
//
// WHY THIS EXISTS. `handleBackfillTask` used to hand both of its work drivers a
// deadline of `ContinuousClock.now + .seconds(25 * 60)`. Nothing in the app or
// the framework grants 25 minutes. Measured on the 2026-08-06 device pull
// (`scratchpad/db-new-t2h/analysis.sqlite`), over the 203 `background_task_runs`
// rows with `taskIdentifier = 'com.playhead.app.analysis.backfill'` and
// `outcome = 'expired'`, `finishedAt - startedAt` is:
//
//     p10   36.4 s
//     p25   73.2 s
//     p50  294.0 s
//     p75  295.2 s
//     p90  295.9 s
//     p95  296.0 s
//     max  321.4 s
//
// 1500 s is **5.07x the p90 grant**. The consequence is in the same ledger and
// is not subtle: 203 of 254 backfill wakes (80.0 %) ended `expired`, and only 4
// of those 203 wrote a single `semantic_scan_results` row inside their own
// start/finish window. A handler told to poll for 25 minutes inside a 295 s
// window can never reach its own terminal write, which is also why
// `jobsSeen`/`jobsAdmitted`/`jobsCompleted` are NULL on every expired row.
//
// THE DISTRIBUTION IS BIMODAL, AND ONLY ONE MODE IS A BUDGET QUESTION. 57 % of
// the expiries land at >= 290 s — the OS running the task to its own limit —
// while 24 % end under 60 s, which is iOS reclaiming early for thermal/power
// reasons. A budget cannot rescue the second mode and must not be sized for it:
// an early reclaim is what `expirationHandler` -> `Task.cancel()` is for. So the
// design grant below is derived from the FIRST mode only, and the second mode is
// deliberately left to cancellation.
//
// NOT A KILL SWITCH. Every consumer of these values checks them BEFORE starting
// a unit of work, never during one. A deadline that has passed stops the next
// pass; it does not interrupt the pass in flight. Interruption is the
// expiration handler's job (it cancels the work task AND tells
// `AnalysisWorkScheduler` its grant is over, so the in-flight job checkpoints
// and requeues rather than being abandoned mid-flight).

import Foundation

/// A measured description of one BGTask grant: how long the OS actually gives
/// this task class, how much of that must be held back so the handler can write
/// its own terminal row, and the smallest unit of work worth starting.
///
/// Every field is derived from the device ledger — see the file header for the
/// numerator and denominator of each. A value picked because it "looks about
/// right" is the defect this type exists to remove, so a new instance must come
/// with its own measurement.
struct BackgroundGrantBudget: Sendable, Equatable {
    /// The grant length to design against, in seconds of wall clock.
    ///
    /// Deliberately NOT the mean and NOT the p50 of all expiries. See
    /// ``backfillProcessing`` for the derivation of the shipped value.
    let designGrant: Duration

    /// Wall clock held back from ``designGrant`` so the handler can finish:
    /// resolve its outcome, write the `background_task_runs` UPDATE, and call
    /// `setTaskCompleted`. A handler that spends its whole grant on work and
    /// then gets reclaimed mid-teardown leaves exactly the `expired` row with
    /// NULL counters this bead is about.
    let teardownReserve: Duration

    /// The smallest amount of remaining budget worth starting a dispatch pass
    /// with. Below this, a pass cannot reach even one durable checkpoint, so
    /// starting it converts remaining grant into nothing.
    let minimumUnitBudget: Duration

    init(designGrant: Duration, teardownReserve: Duration, minimumUnitBudget: Duration) {
        self.designGrant = designGrant
        self.teardownReserve = teardownReserve
        self.minimumUnitBudget = minimumUnitBudget
    }

    /// How much of the grant may be spent attempting work.
    ///
    /// Clamped at zero: a reserve larger than the grant means there is no room
    /// to attempt anything, which is a coherent (if useless) budget, whereas a
    /// negative `Duration` would make ``workDeadline(from:)`` return an instant
    /// in the past and read as "already expired" for the wrong reason.
    var workBudget: Duration {
        let remainder = designGrant - teardownReserve
        return remainder > .zero ? remainder : .zero
    }

    /// The instant after which no further unit of work may be STARTED, given
    /// the instant the grant opened.
    func workDeadline(from grantStart: ContinuousClock.Instant) -> ContinuousClock.Instant {
        grantStart + workBudget
    }

    /// Is there enough grant left to start another unit of work?
    ///
    /// The comparison is `>=` so a budget exactly equal to the floor still
    /// admits a pass — the floor is "the cost of one unit", and a unit that
    /// costs exactly its own cost fits.
    func canStartUnit(remaining: Duration) -> Bool {
        remaining >= minimumUnitBudget
    }

    /// Convenience over ``canStartUnit(remaining:)`` for callers holding a
    /// deadline rather than a remaining duration. A deadline already in the
    /// past yields a negative remainder, which is correctly below any
    /// non-negative floor.
    func canStartUnit(before deadline: ContinuousClock.Instant, now: ContinuousClock.Instant) -> Bool {
        canStartUnit(remaining: now.duration(to: deadline))
    }

    // MARK: - Measured instances

    /// `com.playhead.app.analysis.backfill` (and its `.charged` sibling, which
    /// shares `handleBackfillTask`).
    ///
    /// - `designGrant` = **255 s**. Denominator: the 132 of 203 expired backfill
    ///   runs that reached at least 200 s, i.e. the ones iOS ran to its OWN
    ///   limit rather than reclaiming early. Their p05 is 255.7 s (p50 295.0,
    ///   p90 296.0, max 321.4), so designing against 255 s means at least 95 %
    ///   of full-length grants can hold the whole budget plus the reserve.
    ///   Rounding is DOWN, because over-estimating the grant is the direction
    ///   that reproduces this bead.
    /// - `teardownReserve` = **36 s**. Denominator: the 30 backfill runs whose
    ///   outcome required no work at all (`no_eligible_work` +
    ///   `deferred_capability`) — each is a full handler entry, a pending-count
    ///   query, a ledger `finishRun` and a `setTaskCompleted`. p50 1.4 s,
    ///   p90 17.3 s, p95 19.4 s, **max 36.0 s**. The observed MAX is taken, not
    ///   a percentile: a handler that cannot reach its terminal write produces
    ///   precisely the unreadable `expired` row this bead exists to fix, so the
    ///   reserve is sized to the worst teardown actually seen.
    /// - `minimumUnitBudget` = **60 s**. Denominator: the 142 `latencyMs` values
    ///   in `semantic_scan_results`, one per FM coarse window — the smallest
    ///   unit playhead-26od makes durable. p50 6.0 s, p75 20.3 s, p90 49.0 s,
    ///   **p95 57.5 s**, rounded up to 60. A dispatch pass started with less
    ///   than this remaining cannot bank even one window.
    ///
    /// Work budget is therefore 255 − 36 = **219 s**, against the 1500 s the
    /// handler used to assume.
    static let backfillProcessing = BackgroundGrantBudget(
        designGrant: .seconds(255),
        teardownReserve: .seconds(36),
        minimumUnitBudget: .seconds(60)
    )

    /// `com.playhead.app.preanalysis.recovery`.
    ///
    /// Same shape, same denominators, different population: the 5 expired
    /// recovery runs average 159.0 s, and the 92 that finished normally average
    /// 5.1 s (86 `recovered_work` at 5.1 s, 6 `no_op` at 0.7 s). Recovery is the
    /// HEALTHY sibling — it is not what this bead measured — so its grant is
    /// assumed to be the same OS class as backfill (both are `BGProcessingTask`
    /// on the same device) and the same 255/36 split applies. It carried a
    /// 5-minute (300 s) drain deadline, already within 1.2x of the measured
    /// grant rather than 5.07x, so this changes it only marginally; the point is
    /// that both handlers now derive the number from the same measurement
    /// instead of two unrelated constants.
    static let preAnalysisRecovery = BackgroundGrantBudget(
        designGrant: .seconds(255),
        teardownReserve: .seconds(36),
        minimumUnitBudget: .seconds(60)
    )
}
