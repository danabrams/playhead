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
// NOT A KILL SWITCH. `designGrant`, `workBudget` and `minimumCheckpointBudget`
// are checked BEFORE starting a unit of work, never during one. A deadline that
// has passed stops the next pass; it does not interrupt the pass in flight.
// Interruption is the handler's job — at the work deadline on the normal path
// and in the expiration handler when the OS gets there first, and both of them
// cancel the work task AND tell `AnalysisWorkScheduler` the grant is over, so
// the in-flight job requeues rather than being abandoned mid-flight.
//
// `teardownReserve` is the one exception and is deliberately different: it is
// spent DURING teardown, as a live timeout on how long the handler may wait for
// that requeue to commit before it must call `setTaskCompleted` anyway. See
// `remainingTeardownReserve(since:now:)`.
//
// AND IT IS ONLY THE RIGHT BOUND WHILE THE GRANT IS STILL OPEN. `teardownReserve`
// is wall clock carved OUT of `designGrant`; once the OS has fired
// `expirationHandler` there is no grant left for it to be carved out of, so
// spending it there is asking the diagnostic question the wrong way round —
// `teardownReserve - (now - teardownStart)` reads the same 36 s whether the
// window ended at t+50 s or ran to its full t+295 s limit. What governs after a
// reclaim is `expirationSettleGrace`, which is a property of iOS rather than of
// a task class. See `expirationSettleBudget(since:now:)`.

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
        /// THREE fields, not all of them. ``expirationSettleGrace`` is outside
        /// this claim by construction and always will be: it bounds what may
        /// happen AFTER the OS reclaims a window, and this ledger's clock stops
        /// at the reclaim. Naming a denominator for it would be inventing one.
        /// ``minimumDrainCheckpointBudget`` (playhead-13kf) is outside it for
        /// the sibling reason: its backfill value is derived from the ABSENCE
        /// of a measurable artifact cost, and an absence has no denominator
        /// either.
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

    /// How long the EXPIRATION handler may wait for the interrupted job to
    /// commit its resume point, once the OS has already reclaimed the window.
    ///
    /// **NOT A SLICE OF ``designGrant``, AND NOT MEASURED — it cannot be, from
    /// anything in this repo's ledger.** ``teardownReserve`` is wall clock held
    /// back FROM the grant, so it is the right bound on the handler's own
    /// work-deadline return, which happens while the grant is still open. It is
    /// the wrong bound after `expirationHandler` fires: the grant is over, and
    /// `teardownReserve - (now - teardownStart)` would hand back the full
    /// reserve no matter how much of the window had already been consumed.
    /// Asked the diagnostic way: what would that expression read if the OS had
    /// run this task to its own 295 s limit before reclaiming it? The same 36 s
    /// it reads for a task reclaimed at t+5 s. A quantity that survives the
    /// thing it claims to bound not existing is not a bound.
    ///
    /// **WHY IT IS SMALL.** iOS terminates an app that does not call
    /// `setTaskCompleted` promptly after its expiration handler runs, and
    /// penalises its future scheduling — which is this bead's own currency. The
    /// repo already writes that grace down once, in
    /// `BGTaskScheduler.pendingTaskRequestsTimeout`: "a timeout longer than the
    /// caller's own budget protects nothing", bounded at 2 s against the ~5 s
    /// iOS allows on the `.background` scenePhase transition. This follows the
    /// same shape — comfortably inside that grace, with room left to emit
    /// telemetry and complete the task.
    ///
    /// **WHAT IT GIVES UP, SAID PLAINLY.** A job that will not reach a
    /// checkpoint inside this window is not rescued. That is the honest
    /// division: the cancelled job's unwind is one SQLite transaction once it
    /// observes cancellation, so this grace covers the jobs the wait can
    /// actually win, and the ones it cannot are not winnable at 36 s either —
    /// ``minimumCheckpointBudget`` records that one FM coarse window alone has a
    /// p95 of 57.5 s. The `expired` ledger row is durable BEFORE this wait
    /// begins, so overrunning buys nothing and risks the whole process.
    let expirationSettleGrace: Duration

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
    ///
    /// **Who consumes it changed in playhead-13kf, and the meaning is why.**
    /// The value prices ONE FM COARSE WINDOW, so it gates the phase that banks
    /// coarse windows: `AnalysisCoordinator.runPendingCoarseScans`, the
    /// FM-first phase that now runs at the front of every backfill grant. The
    /// transcription drain behind it takes ``minimumDrainCheckpointBudget``
    /// instead — see that field for why the 60 s does not transfer.
    /// `handlePreAnalysisRecovery` is NOT reordered, and its drain remains the
    /// only road to Stage-4 FM work in its window; its instance therefore sets
    /// the drain field to the same 60 s (see ``preAnalysisRecovery``).
    let minimumCheckpointBudget: Duration

    /// The smallest remaining grant worth starting a TRANSCRIPTION-DRAIN pass
    /// with, after playhead-13kf put the FM phase in front of the drain.
    ///
    /// **This is the playhead-lmrx F6 question, resolved here by the bead that
    /// inherited it.** Under the old order the drain was the only road to the
    /// FM phase, so its floor was honestly ``minimumCheckpointBudget`` — the
    /// measured cost of one coarse window — and the compound bound it created
    /// (no pass may start after grantStart + 159 s of a 294 s median grant,
    /// ~46 % of the median grant closed to starting work) was the price of
    /// never converting a grant tail into a pass that could bank nothing.
    ///
    /// **After the reorder that derivation no longer names the drain's
    /// artifact.** The coarse windows are banked FIRST, directly; what the
    /// drain starts is transcription-headed, and its smallest durable artifact
    /// is a persisted transcript-chunk batch. The ledger cannot price that
    /// artifact: `transcript_chunks` carries no timestamps, and of the 115
    /// measured transcription attempts in the 2026-08-06 pull
    /// (`work_journal.metadata.stage = 'analysisJobRunner.run.transcriptionTimeout'`),
    /// 96 persisted ZERO chunks at ANY length (playhead-i2am) — a population
    /// in which no floor value separates "banked something" from "banked
    /// nothing". By this file's own rule a value nobody measured must not pass
    /// as one that was, so the only non-arbitrary drain floor is ZERO:
    /// admission unrestricted, exactly the pre-lmrx condition for non-grant
    /// callers.
    ///
    /// **What zero admits, and why that is now safe (the lmrx R3 argument,
    /// recorded when the question was deferred to this bead).** A pass started
    /// with seconds left is cancelled at the work deadline and pays: the
    /// teardown door closes dispatch for the reserve, the cancel is aimed by
    /// identity, the requeue commits a flat-floor `nextEligibleAt` WITHOUT
    /// spending an attempt, and whatever chunks it wrote are already durable.
    /// The tail-of-grant stranding the 60 s floor guarded against is closed by
    /// that machinery, not by refusing to start; refusing to start only
    /// converts drain tail into idle. What zero gives up, said plainly: a
    /// start that cannot reach its first chunk burns its setup for nothing —
    /// bounded, flat, and unpriceable from this ledger, which is the point.
    ///
    /// **NOT part of the `.measured` provenance claim**, for the same
    /// structural reason ``expirationSettleGrace`` is not: its backfill value
    /// is derived from the ABSENCE of a measurable artifact cost, and a
    /// denominator for an absence would be an invention. The recovery
    /// instance's 60 s is different in kind — see ``preAnalysisRecovery``.
    let minimumDrainCheckpointBudget: Duration

    /// Whether the three durations above are measurements or assumptions.
    let provenance: Provenance

    init(
        designGrant: Duration,
        teardownReserve: Duration,
        minimumCheckpointBudget: Duration,
        minimumDrainCheckpointBudget: Duration,
        expirationSettleGrace: Duration,
        provenance: Provenance
    ) {
        self.designGrant = designGrant
        self.teardownReserve = teardownReserve
        self.minimumCheckpointBudget = minimumCheckpointBudget
        self.minimumDrainCheckpointBudget = minimumDrainCheckpointBudget
        self.expirationSettleGrace = expirationSettleGrace
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

    /// How long an EXPIRATION handler may still wait, given when its teardown
    /// began.
    ///
    /// The `min` is the whole point, and the two operands answer different
    /// questions. ``remainingTeardownReserve(since:now:)`` asks "how much of the
    /// reserve has this teardown already spent?", which stays meaningful for
    /// ordering the steps within a teardown. ``expirationSettleGrace`` asks "how
    /// long may anything at all run after the OS has reclaimed the window?",
    /// which is the question that actually binds here — and only the second one
    /// knows the grant is gone. Taking the reserve alone was the defect: it
    /// hands back its full length at the moment the OS reclaims, however much
    /// of the window was consumed first.
    func expirationSettleBudget(
        since teardownStart: ContinuousClock.Instant,
        now: ContinuousClock.Instant = .now
    ) -> Duration {
        min(remainingTeardownReserve(since: teardownStart, now: now), expirationSettleGrace)
    }

    // THE CHECKPOINT PREDICATE USED TO LIVE HERE, AND IT WAS NEVER CALLED
    // (playhead-lmrx review round 7).
    //
    // `canReachCheckpoint(remaining:)` and its `(before:now:)` convenience were
    // shipped with ZERO production callers — `grep -rn canReachCheckpoint
    // Playhead/` returned only their own two definitions — while the gate they
    // describe is spelled out inline in `AnalysisWorkScheduler.drainEligible`
    // as `remaining >= minimumCheckpointBudget`. Two definitions of one gate,
    // free to drift, and rail LX08 pinned the one nobody ran. That is the
    // `playhead-y3ya` shape (`FMBackfillMode.canProposeNewRegions`, likewise
    // shipped with no consumers), and it is worse than plain dead code because
    // the rail reads as coverage of a gate it never touches.
    //
    // Wiring `drainEligible` to consult a `BackgroundGrantBudget` instead of
    // the `Duration` it is handed would mean pushing this type across a service
    // boundary, which is an architectural change and not a review one. So the
    // dead copy is deleted and the shipped gate keeps its single spelling,
    // pinned behaviourally by LX04/LX33.
    //
    // WHAT WENT WITH IT, named rather than quietly lost: LX08 flipped `>=` to
    // `>`, and no rail replaces it — because at the real gate that boundary is
    // not observable. `drainEligible` computes `remaining` from
    // `ContinuousClock.now` against a caller's deadline, so `remaining` is
    // never exactly `minimumCheckpointBudget`; a test can straddle the floor
    // but cannot land on it. LX08 was pinning an equality case that only the
    // uncalled copy could ever see.

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
    ///
    ///   **WHAT THOSE 30 ROWS ACTUALLY MEASURE, said plainly because the number
    ///   is load-bearing twice over (lmrx review round).** They are
    ///   `finishedAt - startedAt` for the WHOLE handler — entry,
    ///   `scheduleBackfillIfNeeded`'s out-of-process pending query,
    ///   `updateBatteryState`, the capabilities snapshot, the pending-count
    ///   read, `finishRun`, `setTaskCompleted`. Ask the diagnostic question: if
    ///   teardown itself cost nothing, 36.0 s would read the same. So it is an
    ///   UPPER BOUND on a superset, not a measurement of teardown — the
    ///   conservative direction for subtracting from the grant, and a BORROWED
    ///   bound where the handlers spend it as a settle timeout. That population
    ///   contains zero observations of a settle wait BY CONSTRUCTION: no work
    ///   was admitted, so nothing was ever in flight to unwind.
    ///   `teardownObservations: 30` names those 30 rows and claims nothing
    ///   more. A terminal-segment measurement would settle it, and this bead's
    ///   own counters are what make one possible.
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
    /// - `expirationSettleGrace` = **3 s**, and it is NOT part of the measured
    ///   claim below — see the field's own doc comment. It bounds the post-
    ///   reclaim wait, where no grant remains to be carved from; 3 s sits inside
    ///   the ~5 s the repo already records as iOS's tightest suspension grace
    ///   (`BGTaskScheduler.pendingTaskRequestsTimeout`) with room left to
    ///   complete the task.
    /// - `minimumDrainCheckpointBudget` = **0** (playhead-13kf). The 60 s
    ///   floor priced one FM coarse window, and the FM phase now runs FIRST
    ///   and directly — the drain behind it is transcription-headed, and this
    ///   ledger cannot price a transcription pass's smallest durable artifact
    ///   (no chunk timestamps; 96 of 115 measured attempts persisted zero
    ///   chunks at any length, playhead-i2am). Zero is the only value that is
    ///   not an invention; the tail-dispatch churn it admits is the case the
    ///   teardown door, identity cancel and no-attempt requeue were built for.
    ///   See the field's own doc comment for the full derivation.
    static let backfillProcessing = BackgroundGrantBudget(
        designGrant: .seconds(255),
        teardownReserve: .seconds(36),
        minimumCheckpointBudget: .seconds(60),
        minimumDrainCheckpointBudget: .zero,
        expirationSettleGrace: .seconds(3),
        provenance: .measured(
            grantObservations: 132,
            teardownObservations: 30,
            checkpointObservations: 142
        )
    )

    /// `com.playhead.app.analysis.backfill.charged` — the playhead-i6oi
    /// charger-maintenance sibling, which shares `handleBackfillTask`.
    ///
    /// **ASSUMED, NOT MEASURED, AND DELIBERATELY DIFFERENT.** There is no
    /// observation of what iOS grants a charger-class window. Applying the
    /// plain identifier's measured 219 s here would be the exact error this
    /// type exists to prevent: a number measured on one population spent on
    /// another. It would also be a live regression risk, because the charger
    /// class is expected to grant LONGER windows (that is the whole point of
    /// playhead-i6oi) and a 219 s cap would surrender the rest of an overnight
    /// grant.
    ///
    /// **WHY "NO OBSERVATION" IS NOT THE SAME AS "IT NEVER RAN", corrected in
    /// the lmrx review round.** The first draft justified this with `SELECT
    /// DISTINCT taskIdentifier FROM background_task_runs` returning only the
    /// plain backfill, recovery and rediff ids. That query could not have
    /// returned anything else: `handleBackfillTask` serves BOTH identifiers and
    /// hardcoded the plain one on every ledger and telemetry write, so
    /// `taskIdentifier` named which HANDLER ran and was being read as which
    /// CLASS the OS granted. Absence produced by the instrumentation is not
    /// evidence about the world — the same defect this type exists to remove,
    /// one layer up. It also means any charger-class window that did run is
    /// inside the 203 expired rows ``backfillProcessing`` was derived from;
    /// nothing in that population exceeds 321.4 s, so no long charger-class
    /// grant is visibly in it, but that is an argument from the data rather
    /// than from the recording.
    ///
    /// So this keeps the 30-minute horizon the handler assumed before
    /// playhead-lmrx — no behaviour change for the class nobody has measured —
    /// and says out loud that it is an assumption. The handler now threads its
    /// real identifier, so the NEXT pull genuinely can settle this.
    ///
    /// `expirationSettleGrace` is the ONE field this sibling does not differ on,
    /// and deliberately: it is a property of iOS's reclaim behaviour, not of how
    /// long a class's window is, so a longer grant does not buy a longer grace.
    ///
    /// `minimumDrainCheckpointBudget` is 0 here for the same playhead-13kf
    /// reason as ``backfillProcessing``: the charged sibling shares
    /// `handleBackfillTask`, so its drain also runs behind the FM-first phase
    /// and the 60 s coarse-window price does not name its artifact either.
    static let backfillProcessingCharged = BackgroundGrantBudget(
        designGrant: .seconds(1800),
        teardownReserve: .seconds(36),
        minimumCheckpointBudget: .seconds(60),
        minimumDrainCheckpointBudget: .zero,
        expirationSettleGrace: .seconds(3),
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
    ///
    /// **THE BORROW HAS SUPPORTING EVIDENCE, added in the lmrx review round
    /// because "same OS class" was asserted rather than shown.** The 5 expired
    /// recovery runs are 41.6 / 42.5 / 126.4 / 289.7 / **294.9 s** — the top of
    /// that range sits on the same ~295 s ceiling the 132 full-length backfill
    /// expiries do, which is what a shared OS class looks like. It stays
    /// `.assumed` because five observations of a ceiling is not a percentile.
    ///
    /// **What the change costs recovery, stated so it is not discovered later.**
    /// The old bound was `now + 300 s` read AT THE DRAIN CALL, i.e. after
    /// `reconcile()`; the new one is `grantStart + 219 s`, and no pass may start
    /// after `grantStart + 159 s`. Against a ~295 s grant the old bound was
    /// never the binding constraint — the OS was — so the drain loses roughly
    /// the reserve plus the floor. That binds on 5 of the 97 recorded runs, and
    /// all five of those ended in expiry, which is the outcome the reserve
    /// exists to convert into a recorded one.
    ///
    /// `minimumDrainCheckpointBudget` **keeps the full 60 s here, and the
    /// asymmetry with ``backfillProcessing`` is the playhead-13kf decision
    /// working as designed**: recovery was NOT reordered, so its drain remains
    /// the only road to Stage-4 FM work inside its window — the pass it admits
    /// may still head straight into a coarse scan, and the artifact the floor
    /// prices (one FM coarse window, p95 57.5 s over 142 rows) is still the
    /// artifact at stake. Relaxing it here would be spending playhead-13kf's
    /// derivation on a handler the reorder never touched.
    static let preAnalysisRecovery = BackgroundGrantBudget(
        designGrant: .seconds(255),
        teardownReserve: .seconds(36),
        minimumCheckpointBudget: .seconds(60),
        minimumDrainCheckpointBudget: .seconds(60),
        expirationSettleGrace: .seconds(3),
        provenance: .assumed
    )
}
