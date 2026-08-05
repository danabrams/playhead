// FMDaemonRefusal.swift
// playhead-e75l: the FoundationModels daemon not serving a job is a fact about
// the DAEMON. playhead-kvs8 established that for one such condition; this file
// names the class it belongs to and admits the second member.
//
// THE FIELD ROW. The 2026-08-03 device pull, `backfill_jobs`, asset 0C2FC22E,
// phase `fullEpisodeScan`:
//
//     status      = failed
//     retryCount  = 1
//     deferReason = "FMInferenceTimeoutError(deadline: 30.0 seconds)"
//     created     = 2026-08-03 15:21:38
//     failed      = 2026-08-03 15:22:31      (52.2 s later)
//
// with zero `semantic_scan_results` rows for the asset. Fifty-two seconds is
// the prologue — a healthy coarse pass runs 12–45 minutes — so the episode was
// never screened at all. It is the kvs8 row with a different error in the same
// column, and it arrived by the same route.
//
// WHAT 30 SECONDS IS. `FMInferenceDeadline.metadata`, the bound on
// `SystemLanguageModel.tokenCount(for:)` and the `@Generable` schema sizings.
// Those are XPC round trips that perform no sampling and no generation;
// inference is bounded separately by `FMInferenceDeadline.standard` (300 s).
// So the row records the daemon failing to answer a TOKENIZER question inside
// thirty seconds — two orders of magnitude above any honest cost. That is
// evidence the daemon is wedged or starved, and it is evidence about nothing
// else: not this episode, not this transcript, not this model window.
//
// **The deadline is correct and must stay 30 s.** The defect is the
// disposition. Raising the bound would trade a mislabelled row for a wedge that
// takes longer to notice, which is the trade playhead-qk44 already refused.
//
// WHY IT REACHED THE GENERIC CATCH-ALL. Precisely kvs8's escape. `promptBudget()`
// awaits `coarseSchemaTokenCount()`, and `planPassA` awaits `tokenCount(_:)`
// once per candidate window — all before the first window is planned, and all
// of them `throws`. The throw escapes `coarsePassA`, escapes `runJob`, and
// lands in the arm whose job is to record genuine failures:
//
//     markBackfillJobFailed(reason: String(describing: error), retryCount: job.retryCount + 1)
//
// WHY THAT COSTS REACH. `AdmissionController.maxRetries` is 3, and
// `runBackfill` refuses to re-enqueue a row at or above it. Three wedged-daemon
// moments, unrelated to each other and to the episode, disqualify that episode
// from ad scanning. Both terminal `failed` rows in the 2026-08-03 pull are
// daemon conditions — one throttle, one metadata stall — and neither is the
// job's own merits.
//
// R4 CORRECTED THE SCOPE OF THAT COST, and the field pull quoted above is its
// own counterexample. The budget is spent per JOB ROW, and a row's identity is
// `hash(asset | transcriptVersion | phase | offset)` (`makeJobIdForTesting`),
// so an episode whose transcript is still growing gets a FRESH row at
// `retryCount = 0` rather than being re-driven: 0C2FC22E's failed row
// `fm-443333542600941c` is followed 2 h 04 m later by `fm-673a3b2b167fc4c7` —
// queued, `retryCount = 0`, same asset, same phase. So "for the life of the
// install", which this paragraph used to say, is false while the transcript is
// still moving. It becomes true the moment the transcript stops moving, which
// is exactly when a full-episode ad scan is worth running and is nearly all of
// an episode's life. The disposition is wrong either way — a daemon condition
// is not evidence about the job at any scope — but the cost is "this episode is
// disqualified against its FINAL transcript", not "forever from the first
// failure".
//
// WHY A SHARED TYPE RATHER THAN A SECOND CATCH ARM. kvs8's own doc states the
// principle in general terms — a daemon condition "says nothing about the job" —
// and then had exactly one instance to apply it to. A second instance arriving
// by the same route, needing the same three corrections (defer not fail,
// preserve `retryCount`, name the cause) and the same consecutive-stop rule, is
// the moment to make the class explicit. The alternative — copying the arm —
// gives the drain two counters that can disagree about whether the daemon is
// serving us, which is the shape of bug this file exists to remove.
//
// WHAT THIS DELIBERATELY DOES NOT ABSORB. A `FMInferenceDeadline.standard`
// timeout. Same Swift type, different budget, different meaning: the model was
// asked and did not answer. Three reasons it stays a failure:
//
//   * It is evidence about the MODEL, and `FoundationModelClassifier
//     .consecutiveInferenceTimeoutAbortThreshold` already escalates on it — in
//     window, where the pass can bank what it screened first.
//   * It cannot reach here anyway on today's code. `.inferenceTimeout` has
//     `failureScope == .window`, so `coarsePassA` and `refinePassB` fold a
//     window timeout into `failedWindowStatuses` and RETURN rather than throw.
//     A standard-deadline throw reaching the drain loop would be a NEW path,
//     and a new path must not be silently granted an unbounded retry.
//   * The cost bound differs by an order of magnitude, and the bound is what
//     licenses kvs8's "no terminal cause". A refused metadata round trip costs
//     at most 30 s of daemon time per drain, so retrying forever is cheap. A
//     deferred inference timeout would cost up to 300 s per window per drain.
//
// So the discriminator is WHICH BOUND ELAPSED, not which type was thrown.
// `FMInferenceTimeoutError.deadline` carries that value for exactly this
// purpose — its own doc says it is there "so a log line or a test can assert
// WHICH budget was exceeded rather than just that something was".

import Foundation

/// The drain loop's notion of "the FoundationModels daemon did not serve this
/// job", of which there are two instances and no others.
///
/// One type, deliberately: the runner must not be able to hold two opinions
/// about whether the daemon is currently serving us. `BackfillJobRunner`'s
/// consecutive-refusal counter is incremented from a single site and read from
/// a single site because of this enum.
enum FMDaemonRefusal: Sendable, Equatable, CaseIterable {

    /// playhead-kvs8: the daemon explicitly declined — a rate limit, or
    /// `concurrentRequests`, which is the same refusal by another name.
    case throttle

    /// playhead-e75l: the daemon did not complete a tokenizer / schema-size XPC
    /// round trip inside ``FMInferenceDeadline/metadata``.
    case metadataStall

    /// Which refusal, if any, this error is.
    ///
    /// `nil` for everything else, and that is the important half: a refusal, a
    /// guardrail block, a context overflow, a cancellation and a store error
    /// all have their own dispositions, and folding any of them in here would
    /// give a real, durable failure an "it will heal on its own" reading.
    ///
    /// The throttle test runs first only because it is the cheaper predicate;
    /// the two are disjoint (`SemanticScanStatus.from(error:)` maps
    /// `FMInferenceTimeoutError` to `.inferenceTimeout`, never `.rateLimited`),
    /// so the order carries no meaning.
    static func classify(_ error: Error) -> FMDaemonRefusal? {
        if FMDaemonThrottle.isThrottle(error) {
            return .throttle
        }
        if isMetadataStall(error) {
            return .metadataStall
        }
        return nil
    }

    /// Did a tokenizer round trip outlive ``FMInferenceDeadline/metadata``?
    ///
    /// Compared against the production constant rather than a literal `30`, so
    /// the predicate follows the bound if the bound ever moves. A literal here
    /// would keep compiling, keep passing, and silently stop matching the thing
    /// it names.
    static func isMetadataStall(_ error: Error) -> Bool {
        guard let timeout = error as? FMInferenceTimeoutError else {
            return false
        }
        return timeout.deadline == FMInferenceDeadline.metadata
    }

    /// Written verbatim to `backfill_jobs.deferReason` when THIS job's own run
    /// was refused by the daemon.
    ///
    /// NAMED FOR THE CONDITION, NOT FOR A POSITION IN THE PASS, and the
    /// distinction is not pedantic: the throw does not only come from the
    /// prologue. `BackfillJobRunner.runJob` also awaits `planAdaptiveZoom`
    /// (which makes its own `tokenCount` round trips) AFTER `coarsePassA` has
    /// screened and checkpointed windows, so a refusal can arrive with a real
    /// cursor and real scan rows already banked. The property name is
    /// historical — kvs8's `rateLimited-prologue` token is preserved
    /// byte-for-byte because device pulls grep for it — but the reading an
    /// operator should take is "this job's run was refused", never "nothing had
    /// been scanned yet". See playhead-x8ck, and the suffix note at the bottom
    /// of this comment for why the metadata token does not repeat the claim.
    ///
    /// playhead-v7q6: `deferReason` is the durable audit trail a device pull
    /// actually reads, so every stop-short cause in this runner is a named,
    /// greppable token — never `String(describing:)` of a framework error,
    /// which cannot be grouped, cannot be counted, and changes shape with the
    /// OS.
    ///
    /// The two tokens share no prefix, and that is deliberate. An operator
    /// counting `rateLimited-` in a pull is counting throttles; a metadata
    /// stall answering to that prefix would inflate the count with an event the
    /// daemon never described that way.
    ///
    /// R2-Fix1: **and the family it does join must be its own.** The first
    /// spelling of this token was `inferenceTimeout-metadata`, which put a
    /// wedged tokenizer round trip into the SAME greppable family as
    /// playhead-8d5r's `inferenceTimeout-noProgress` — the cause this runner
    /// writes when the coarse pass aborts on a run of 300 s inference
    /// timeouts, and whose documented operator reading is "the model is not
    /// answering on this device". That is the rule two paragraphs up, one
    /// family over: `grep -c 'inferenceTimeout-'` would have counted daemon
    /// stalls as evidence about the model, which is the single distinction
    /// this whole file exists to hold. The prefix is the CONDITION, always.
    ///
    /// The suffix names the ROLE, not a position in the pass — `refused` is
    /// "the daemon refused this job's own run", which is true from
    /// `promptBudget` and equally true from `planAdaptiveZoom` after
    /// `coarsePassA` has already banked windows. kvs8's `-prologue` suffix
    /// makes the stronger claim and playhead-x8ck is filed against it; this
    /// token deliberately does not repeat it.
    var passPrologueCause: String {
        switch self {
        case .throttle:
            FMDaemonThrottle.DeferCause.passPrologue.rawValue
        case .metadataStall:
            "metadataStall-refused"
        }
    }

    /// Written to the siblings the drain sweeps when it stops asking.
    ///
    /// Nothing about those jobs was refused — they never reached the daemon —
    /// so they carry their own token, and it names the condition that actually
    /// stopped the drain. Deferring them as `rateLimited-batchSibling` after a
    /// metadata stall would put a rate limit in the record that never happened.
    ///
    /// R2-Fix1: same family correction as ``passPrologueCause`` — this was
    /// `inferenceTimeout-batchSibling`, which answered to playhead-8d5r's
    /// prefix.
    var batchSiblingCause: String {
        switch self {
        case .throttle:
            FMDaemonThrottle.DeferCause.batchSibling.rawValue
        case .metadataStall:
            "metadataStall-batchSibling"
        }
    }

    /// The log event name for a job refused in its prologue.
    ///
    /// Split per kind rather than emitting one generalized event, because
    /// `fm.backfill.job_throttled` predates this file and is what an existing
    /// support-bundle grep matches. Widening it to cover a second condition
    /// would silently change what that grep counts.
    var logEvent: String {
        switch self {
        case .throttle:
            "fm.backfill.job_throttled"
        case .metadataStall:
            "fm.backfill.job_daemon_metadata_stalled"
        }
    }

    /// The log event name for the drain STOPPING because the daemon is not
    /// serving this batch.
    ///
    /// Split per kind for exactly the reason ``logEvent`` is, and the omission
    /// was a real defect: `fm.backfill.drain_stopped_by_throttle` predates this
    /// file and is what a support-bundle grep counts, so a drain stopped by two
    /// wedged tokenizer round trips emitting it would inflate the very number an
    /// operator reads to decide whether the device is being rate-limited. A
    /// `cause=` field on the line does not fix that — an event NAME is the unit
    /// a log grep counts, and this one would have answered to a population it
    /// does not belong to.
    ///
    /// kvs8's spelling is preserved byte-for-byte for the throttle.
    var drainStoppedEvent: String {
        switch self {
        case .throttle:
            "fm.backfill.drain_stopped_by_throttle"
        case .metadataStall:
            "fm.backfill.drain_stopped_by_daemon_metadata_stall"
        }
    }

    /// Should the drain stop dispatching further jobs to the daemon?
    ///
    /// The SAME rule and the SAME number as playhead-kvs8's, delegated rather
    /// than duplicated: one refusal is an event on a daemon shared with the
    /// rest of the OS, two back-to-back with nothing succeeding in between is a
    /// fact about its current disposition, and the only useful response to that
    /// is to stop asking. Consecutive, never lifetime.
    ///
    /// A separate threshold per kind was considered and rejected: there is no
    /// evidence distinguishing "two throttles" from "a throttle and a stall" —
    /// both mean the daemon is not serving this drain — and a second constant
    /// is a second thing to keep in sync.
    ///
    /// Stopping IS the backoff. In a background grant, "later" means the next
    /// grant, not a `Task.sleep` that burns the window we were given
    /// (playhead-bkhc).
    static func shouldStopDraining(consecutiveRefusals: Int) -> Bool {
        FMDaemonThrottle.shouldStopDraining(consecutiveThrottles: consecutiveRefusals)
    }
}
