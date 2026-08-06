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

    /// Where a budget's numbers came from.
    ///
    /// Carried as DATA rather than left to the doc comment because the whole
    /// point of this type is that a value nobody measured must not be able to
    /// pass as one that was. A budget that says `.measured` is checked against
    /// the ledger by `BackgroundGrantBudgetMeasurementTests`; a budget that says
    /// `.assumed` is exempt from that check and is, by construction, not
    /// evidence of anything.
    enum Provenance: Sendable, Equatable {
        /// Derived from device observations — one denominator PER FIELD,
        /// because the three fields were not measured over the same rows and a
        /// single `sampleSize` could only be right about one of them.
        ///
        /// The first draft of this type carried exactly that single number, and
        /// it read `203` — the count of expired backfill runs. Ask the
        /// diagnostic question: what would `203` read if nobody had derived the
        /// design grant at all? The same, because it is just how many rows the
        /// table holds for that outcome; none of the three shipped values has
        /// 203 as its denominator (they are 132, 30 and 142). A provenance that
        /// survives its own values not being measured is not provenance, which
        /// is the defect this type exists to remove, committed one level up.
        ///
        /// - Parameters:
        ///   - grantObservations: rows behind ``designGrant``.
        ///   - teardownObservations: rows behind ``teardownReserve``.
        ///   - checkpointObservations: rows behind ``minimumCheckpointBudget``.
        case measured(
            grantObservations: Int,
            teardownObservations: Int,
            checkpointObservations: Int
        )
        /// No observations exist for this task class. The value preserves prior
        /// behaviour so the change is not a regression, and it must never be
        /// quoted as evidence about that class.
        case assumed
    }

    /// The grant length to design against, in seconds of wall clock.
    ///
    /// Deliberately NOT the mean and NOT the p50 of all expiries. See
    /// ``backfillProcessing`` for the derivation of the shipped value.
    let designGrant: Duration

    /// Wall clock held back from ``designGrant`` so the handler can finish:
    /// resolve its outcome, read the counters it is about to persist, write the
    /// `background_task_runs` UPDATE, wait for the interrupted analysis job to
    /// commit its resume point, and call `setTaskCompleted`. A handler that
    /// spends its whole grant on work and then gets reclaimed mid-teardown
    /// leaves exactly the `expired` row with NULL counters this bead is about.
    ///
    /// It is a budget for ALL of that together, not one each — see
    /// ``remainingTeardownReserve(since:now:)``, which is how the expiration
    /// handler spends it.
    let teardownReserve: Duration

    /// The smallest remaining grant worth starting a dispatch pass with.
    ///
    /// **This is the cost of the smallest DURABLE ARTIFACT the pipeline writes,
    /// NOT an estimate of what a pass costs, and the distinction is the whole
    /// reason it is named this way.** A dispatch pass is an entire
    /// `AnalysisJobRunner` job — decode, transcript, features, ad detection, the
    /// FM phase — and the device ledger says a pass routinely outlives the whole
    /// grant, so no honest floor would ever admit one. What the floor bounds is
    /// different and weaker: below it, a pass that starts cannot reach even ONE
    /// checkpoint before the window closes, so starting it converts the tail of
    /// a grant into nothing at all. Above it, a pass may still not finish — but
    /// it can bank something.
    ///
    /// **Scope, stated because the first draft of this comment overstated it.**
    /// `drainEligible` consults this only BETWEEN passes. Of the 203 expired
    /// backfill grants measured, 108 contained exactly one dispatch and 33
    /// contained none, so roughly 30 windows ever reach a second pass and this
    /// gate is asked about those. It is a genuine bound on wasted tail-of-grant
    /// dispatch; it is NOT the fix for the 199-of-203 barren windows, and
    /// nothing here should be read as claiming otherwise.
    let minimumCheckpointBudget: Duration

    /// Whether the three durations above are measurements or assumptions.
    let provenance: Provenance

    init(
        designGrant: Duration,
        teardownReserve: Duration,
        minimumCheckpointBudget: Duration,
        provenance: Provenance
    ) {
        self.designGrant = designGrant
        self.teardownReserve = teardownReserve
        self.minimumCheckpointBudget = minimumCheckpointBudget
        self.provenance = provenance
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

    /// How much of ``teardownReserve`` is left, given when teardown began.
    ///
    /// The reserve is a budget for the WHOLE teardown — resolve the outcome,
    /// write the `background_task_runs` UPDATE, wait for the interrupted job to
    /// commit its resume point, call `setTaskCompleted` — not a per-step
    /// allowance each step may spend in full. The first draft handed the entire
    /// reserve to the settle wait alone, which is the same over-spend the
    /// handler's 25-minute deadline was, at a smaller scale: an expiration
    /// handler that has already spent its reserve and is still waiting is one
    /// iOS may kill for never completing its task.
    ///
    /// Clamped at zero for the same reason ``workBudget`` is: a negative
    /// remainder would read as a deadline in the past for the wrong reason.
    func remainingTeardownReserve(
        since teardownStart: ContinuousClock.Instant,
        now: ContinuousClock.Instant = .now
    ) -> Duration {
        let remainder = teardownReserve - teardownStart.duration(to: now)
        return remainder > .zero ? remainder : .zero
    }

    /// Is there enough grant left for a pass that starts now to reach a
    /// checkpoint?
    ///
    /// The comparison is `>=` so a budget exactly equal to the floor still
    /// admits a pass — the floor is the cost of one durable artifact, and an
    /// artifact that costs exactly its own cost fits.
    func canReachCheckpoint(remaining: Duration) -> Bool {
        remaining >= minimumCheckpointBudget
    }

    /// Convenience over ``canReachCheckpoint(remaining:)`` for callers holding a
    /// deadline rather than a remaining duration. A deadline already in the
    /// past yields a negative remainder, which is correctly below any
    /// non-negative floor.
    func canReachCheckpoint(before deadline: ContinuousClock.Instant, now: ContinuousClock.Instant) -> Bool {
        canReachCheckpoint(remaining: now.duration(to: deadline))
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
    /// - `minimumCheckpointBudget` = **60 s**. Denominator: the 142 `latencyMs`
    ///   values in `semantic_scan_results`, one per FM coarse window — the
    ///   smallest artifact playhead-26od makes durable. p50 6.0 s, p75 20.3 s,
    ///   p90 49.0 s, **p95 57.5 s**, rounded up to 60. Read the field's own doc
    ///   comment before quoting this: it is the cost of an ARTIFACT, not of a
    ///   pass.
    ///
    /// Work budget is therefore 255 − 36 = **219 s**, against the 1500 s the
    /// handler used to assume.
    ///
    /// **One exclusion, named because it is not obvious.** The nine `failed`
    /// backfill rows average 1581 s and reach 2299 s, which looks like evidence
    /// of much longer grants. They are all `lastErrorCode = 'orphan_at_launch'`
    /// — rows reaped by a LATER launch, so their `finishedAt` is the reaping,
    /// not the end of a grant. They are excluded from every percentile above.
    static let backfillProcessing = BackgroundGrantBudget(
        designGrant: .seconds(255),
        teardownReserve: .seconds(36),
        minimumCheckpointBudget: .seconds(60),
        provenance: .measured(
            grantObservations: 132,
            teardownObservations: 30,
            checkpointObservations: 142
        )
    )

    /// `com.playhead.app.analysis.backfill.charged` — the playhead-i6oi
    /// charger-maintenance sibling, which shares `handleBackfillTask`.
    ///
    /// **ASSUMED, NOT MEASURED, AND DELIBERATELY DIFFERENT.** The 2026-08-06
    /// pull contains **zero** rows for this identifier — `SELECT DISTINCT
    /// taskIdentifier FROM background_task_runs` returns only the plain
    /// backfill, recovery and rediff ids — so there is no observation of what
    /// iOS grants a charger-class window. Applying the plain identifier's
    /// measured 219 s here would be the exact error this type exists to prevent:
    /// a number measured on one population spent on another. It would also be a
    /// live regression risk, because the charger class is expected to grant
    /// LONGER windows (that is the whole point of playhead-i6oi) and a 219 s cap
    /// would surrender the rest of an overnight grant.
    ///
    /// So this keeps the 30-minute horizon the handler assumed before
    /// playhead-lmrx — no behaviour change for the class nobody has measured —
    /// and says out loud that it is an assumption. The next pull can settle it:
    /// `background_task_runs` already records `taskIdentifier`, and as of this
    /// bead it also records what each window achieved.
    static let backfillProcessingCharged = BackgroundGrantBudget(
        designGrant: .seconds(1800),
        teardownReserve: .seconds(36),
        minimumCheckpointBudget: .seconds(60),
        provenance: .assumed
    )

    /// `com.playhead.app.preanalysis.recovery`.
    ///
    /// Same shape, same denominators, different population: the 5 expired
    /// recovery runs average 159.0 s, and the 92 that finished normally average
    /// 5.1 s (86 `recovered_work` at 5.1 s, 6 `no_op` at 0.7 s), with the
    /// longest single run at 164.4 s. Recovery is the HEALTHY sibling — it is
    /// not what this bead measured — so its grant is taken to be the same OS
    /// class as backfill (both are `BGProcessingTask` on the same device) and
    /// the same 255/36 split applies. Shortening its drain from 300 s to 219 s
    /// is safe against that 164.4 s maximum, and marked `.assumed` because the
    /// grant length itself was borrowed rather than observed on this identifier.
    static let preAnalysisRecovery = BackgroundGrantBudget(
        designGrant: .seconds(255),
        teardownReserve: .seconds(36),
        minimumCheckpointBudget: .seconds(60),
        provenance: .assumed
    )
}
