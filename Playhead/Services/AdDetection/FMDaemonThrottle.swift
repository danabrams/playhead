// FMDaemonThrottle.swift
// playhead-kvs8: the FoundationModels daemon throttling us is not the model
// failing us, and the two must never share a disposition.
//
// THE FIELD ROW. Dan's 2026-08-01 03:37 device pull, `backfill_jobs`, episode
// DE0784D8, phase `fullEpisodeScan`, updated 2026-07-31 20:42:34:
//
//     status      = failed
//     retryCount  = 1
//     deferReason = "Request has been rate limited. Please try again later.
//                    If you are using streaming responses in a background
//                    request, consider using non-streaming requests in
//                    background activities to reduce the likelihood of rate
//                    limiting."
//
// The FM ad-scan lane produced ZERO new `semantic_scan_results` in the 2.7 h
// since that build was installed, on a charging device that was thermally fine.
//
// WHAT THE DAEMON'S ADVICE IS WORTH HERE — NOTHING, AND THAT IS THE POINT.
// The message recommends non-streaming requests in background activities. That
// recommendation was already satisfied, and had been since playhead-pmp9: there
// is no `streamResponse` / `ResponseStream` / `PartiallyGenerated` call site
// anywhere in the app, the `playhead-pmp9` DOC-GUARD in
// `FoundationModelClassifier.swift` forbids adding one, and
// `FMDaemonThrottleCanaryTests` now pins that structurally. Apple's daemon
// emits the same generic advice whether or not the caller streams. So the
// throttle is real, the explanation offered for it does not apply to us, and
// the repair is entirely in how we RESPOND to being throttled.
//
// WHERE IT ESCAPED, AND WHY NOTHING CAUGHT IT. `coarsePassA` is careful with
// per-WINDOW failures: `coarseResponses` runs pmp9's capped-exponential backoff
// and a survivor becomes a `.rateLimited` entry in `failedWindowStatuses`, which
// the runner turns into a resumable `rateLimited-backoff` defer. Nothing about a
// window throws.
//
// The pass PROLOGUE is a different animal. `promptBudget()` awaits
// `coarseSchemaTokenCount()` and `planPassA` awaits `tokenCount(_:)` once per
// candidate window — each one an XPC round trip to the SAME daemon `respond`
// goes to (see the playhead-qk44 note on `liveRuntime`), all of them made
// BEFORE the first window is planned, and all of them `throws`. A throttle
// there escapes `coarsePassA`, escapes `runJob`, and lands in
// `BackfillJobRunner`'s generic catch-all, whose job is to record genuine
// failures:
//
//     try await store.markBackfillJobFailed(
//         jobId: job.jobId,
//         reason: String(describing: error),
//         retryCount: job.retryCount + 1
//     )
//
// which is the field row, field for field.
//
// WHY EACH OF THE THREE FIELDS IS ITS OWN DEFECT.
//
//   * `status = failed` is the permanent-coverage-hole shape playhead-pmp9
//     removed one level down. A throttle is temporary — the daemon says so in
//     its own text — so the only honest terminal state for it is no terminal
//     state at all. **There is deliberately no terminal cause in this file.**
//     Giving a throttle a give-up would re-introduce, at pass granularity,
//     exactly the "complete/failed with audio nobody ever scanned" bug the
//     whole rate-limit arc exists to kill. The bound on retrying is that a
//     throttled job makes no progress and costs one metadata round trip per
//     drain, and that drains are scheduled by the OS hours apart — not that we
//     eventually give up on the episode.
//
//   * `retryCount = 1` spent one of `AdmissionController.maxRetries` LIFETIME
//     attempts. That budget exists to disqualify jobs that keep failing on
//     their own merits; a throttle says nothing about the job. Three unlucky
//     throttles and an episode is disqualified forever having never been
//     scanned once. A throttle defer preserves `retryCount`.
//
//   * The raw framework string is unattributable. Every other stop-short cause
//     in this runner is a NAMED token — `rateLimited-backoff`,
//     `cancelled-during-<phase>`, `expiredWithoutProgress-<phase>` — precisely
//     so an operator can group and count causes from a database pull with no
//     device attached. `String(describing:)` of a framework error cannot be
//     grouped, cannot be counted, and changes shape with the OS.
//
// WHAT MUST NOT HAPPEN DOWNSTREAM. A throttle is not evidence about the model,
// so it must not be spent anywhere that judges the model:
//
//   * NOT toward `FoundationModelClassifier.consecutiveInferenceTimeoutAbortThreshold`
//     — that counter is evidence "the model has stopped answering", and a
//     throttle is evidence the daemon declined to ask it. (The window loop
//     already only counts `.inferenceTimeout`; a throttle RESETS it, which is
//     correct.)
//   * NOT as a permissive refusal or decode failure. `.permissiveRefusal`'s
//     retry policy is `.persistFailure`, so mislabelling a throttle there makes
//     a permanent hole out of a momentary one — and charges Apple's safety
//     layer for a call it never saw.
//   * NOT as a usability verdict. `FoundationModelsUsabilityProbe` gates the
//     WHOLE lane and caches a `false` for `falseCacheTTL`; caching a throttle
//     converts one momentary refusal into 15 minutes with no scanning at all.
//
// Coverage is the one place a throttle DOES show honestly: `.rateLimited` has
// `didExamineWindow == false`, so throttled audio counts as unscanned, which is
// true. Honesty about coverage is not a penalty.

import Foundation

/// The single definition of "the FoundationModels daemon throttled this call",
/// plus the named causes and the consecutive-stop rule that follow from it.
///
/// One definition, deliberately: the runner's defer branch, the permissive
/// lane's relabelling guard and the readiness probe's cache guard must be
/// unable to disagree about what a throttle is. That is the same
/// single-predicate discipline `SkipModeResolution.isLookupFailure`
/// (playhead-djl0) and `RediffDayZeroKickoffOutcome.isGiveUp` (playhead-4dqe)
/// apply to their own causes.
enum FMDaemonThrottle {

    /// Is this error the daemon declining to serve us, as opposed to the model
    /// answering badly or not at all?
    ///
    /// Routed through `SemanticScanStatus.from(error:)` rather than pattern
    /// matching the framework enums here, so the OS-version fan-out
    /// (`LanguageModelSession.GenerationError` on iOS 26,
    /// `LanguageModelError` / `LanguageModelSession.Error` on iOS 27) is
    /// maintained in exactly one place. `.concurrentRequests` maps to
    /// `.rateLimited` there and is a throttle for the same reason an explicit
    /// rate limit is: the daemon refused the load, the model was never asked.
    static func isThrottle(_ error: Error) -> Bool {
        SemanticScanStatus.from(error: error) == .rateLimited
    }

    /// WHY a backfill job deferred to a throttle.
    ///
    /// Written verbatim to `backfill_jobs.deferReason`, which — per
    /// playhead-v7q6 — is the durable audit trail a device pull actually reads.
    /// Distinct cases where the operator's reading differs, merged nowhere.
    enum DeferCause: String, Sendable, Equatable, Codable, CaseIterable {
        /// playhead-pmp9's cause, unchanged: a coarse WINDOW exhausted the
        /// capped-exponential backoff budget and the episode is not fully
        /// covered. Some audio WAS scanned; the cursor is honest and non-zero.
        ///
        /// The literal is preserved byte-for-byte because device pulls and
        /// support bundles already grep for it.
        case window = "rateLimited-backoff"

        /// playhead-kvs8: the coarse pass PROLOGUE was throttled — a
        /// `tokenCount` / schema-size round trip threw before a single window
        /// was planned. NOTHING was scanned, so there is no cursor to advance
        /// and no scan row to write. Its own cause because the operator reading
        /// is completely different from `.window`: `.window` means "the daemon
        /// is intermittent and we banked what we could", this means "the daemon
        /// refused us outright and the episode is untouched".
        case passPrologue = "rateLimited-prologue"

        /// playhead-kvs8: this job never reached the daemon at all — a sibling
        /// in the same drain was throttled often enough that the drain stopped
        /// asking. Distinct from the two above because nothing about THIS job
        /// was throttled; re-driving it is unconditionally worth doing.
        case batchSibling = "rateLimited-batchSibling"
    }

    /// How many CONSECUTIVE daemon-refused jobs end the drain.
    ///
    /// **playhead-e75l broadened the population this number governs**, and the
    /// argument below survives the broadening but not unchanged. It was written
    /// for a throttle, where the daemon SAID something ("try again later"), and
    /// half of what now reaches it is a `FMDaemonRefusal.metadataStall`, where a
    /// tokenizer round trip outlived thirty seconds and the daemon said nothing
    /// at all. What carries over is the part about the DEVICE rather than the
    /// message: one refusal is an event on a daemon shared with the rest of the
    /// OS, two back-to-back with nothing succeeding in between is a fact about
    /// its current disposition, and stopping is the only useful response either
    /// way. What does NOT carry over is the wording below about what the daemon
    /// said — a wedged tokenizer says nothing, so "the daemon said try again
    /// later" is now true of only one of the two conditions counted here.
    ///
    /// Consecutive, not lifetime, and for the same reason
    /// `consecutiveInferenceTimeoutAbortThreshold` is: one throttle is an
    /// event — the daemon is shared with the rest of the OS and momentary
    /// contention is normal — while two back-to-back with nothing succeeding
    /// in between is a fact about the daemon's current disposition, and the
    /// only useful response to that is to stop asking. A lifetime tally would
    /// instead punish a device that has been running for weeks.
    ///
    /// Stopping is the backoff. The daemon said "try again later"; in a
    /// background grant, "later" means the next grant, not a `Task.sleep` that
    /// burns the window we were given. Sleeping here would spend the scarcest
    /// resource the lane has (see playhead-bkhc) waiting on something no amount
    /// of waiting inside this window is likely to resolve.
    static let consecutiveDeferStopThreshold = 2

    /// Should the drain stop dispatching further jobs to the daemon?
    ///
    /// Pure, so the rule is assertable without a fixture that has to be lucky
    /// enough to enqueue several FM-reaching jobs.
    ///
    /// playhead-e75l R4: the label was `consecutiveThrottles:`, and after this
    /// bead the value passed to it is `consecutiveDaemonRefusals` — throttles
    /// AND metadata stalls. The runner's own counters were renamed at R1 for
    /// exactly that reason; the shared rule they delegate to kept the narrow
    /// name, so the delegation site read as though a refusal count were a
    /// throttle count. A parameter label is not a durable token and nothing
    /// greps it, so unlike the four `DeferCause` strings it is safe to correct.
    static func shouldStopDraining(consecutiveDaemonRefusals: Int) -> Bool {
        consecutiveDaemonRefusals >= consecutiveDeferStopThreshold
    }
}
