// FMInferenceDeadline.swift
// playhead-8d5r: a hard per-call deadline for on-device Foundation Models
// inference.
//
// WHY THIS EXISTS. Before this file every `LanguageModelSession.respond(...)`
// on the ad-detection path was a bare `try await` with no bound of any kind.
// The device pull (2026-07-30, `semantic_scan_results`) contains a single
// coarse window whose one FM call ran for **1,664.9 s** — 27.7 minutes — and a
// second at 1,648.6 s, both on episodes whose other windows completed in
// 10–13 s. It also contains a SUCCESSFUL passB call at 1,122.4 s on a 96-second
// window, against a p50 of 5.3 s. Nothing in the framework, the session, or
// this app stopped any of them.
//
// That is a throughput bug, not merely a battery one: `BackfillJobRunner` is an
// actor that drains jobs in a sequential `for` loop, and `coarsePassA` iterates
// its windows sequentially, so a single hung call blocks every other scan for
// as long as it hangs.
//
// HANG, NOT GRIND — and the difference decides whether a deadline is even the
// right tool. The bead's premise was that `refusal` averages 174 s and
// `permissive_decoding_failure` 312 s, which would mean the model produces
// verdicts slowly and a deadline would merely truncate slow answers into no
// answers. THAT PREMISE DOES NOT REPRODUCE. Those averages are an artifact of
// `semantic_scan_results.latencyMs` meaning different things per row: success
// rows carry ONE window's latency, failure rows carried the WHOLE PASS's, and
// N failure rows from one pass each carried the same pass total. The 20
// `refusal` rows come from 6 distinct passes; deduplicated they are 6 pass
// durations that also contain those passes' successful windows. There is no
// per-call refusal latency in the data at all, so nothing supports "a refusal
// takes three minutes".
//
// What the data DOES support is hang-shaped, per-call, and unambiguous. On
// asset 4E4730D8 the coarse pass banked window 1 in 12.6 s and then spent the
// remaining ~1,652 s inside a single window's call before throwing. On
// 752926FF the first window alone consumed ~1,648 s. Both episodes have
// sibling windows finishing in 3-13 s. And the 1,122.4 s SUCCESS is the same
// shape from the other side: a 96-second window, against a p50 of 5.3 s, that
// eventually did answer. Occasional calls take three orders of magnitude
// longer than the median with no relationship to the work requested. That is a
// hang, and a hang is what a deadline is for.
//
// WHY THIS IS NOT `withThrowingTaskGroup`. `ChapterLabelingService` races an FM
// call against a sleep inside a throwing task group. That is the idiomatic
// shape, but it only bounds work that honors cancellation: when the body of a
// task group throws, the group cancels its children AND AWAITS THEM before the
// error propagates. If Apple's `respond` does not return promptly on
// cancellation — which is exactly the failure mode a 27-minute call suggests —
// the structured race reports a timeout only after the call it was supposed to
// bound has finished, which is no bound at all.
//
// So the operation runs in an UNSTRUCTURED `Task` that the caller stops
// awaiting once the deadline elapses. The in-flight call is cancelled
// best-effort and then abandoned; the lane is freed deterministically. The cost
// is a possible orphaned in-flight request. That is bounded and already
// handled: every window mints its own session (bd-34e Fix B v5), and a
// concurrent request surfaces as `GenerationError.concurrentRequests` →
// `.rateLimited` → the existing capped-exponential backoff.

import Foundation
import os

/// Thrown by ``FMInferenceDeadline/run(_:operation:)`` when one Foundation
/// Models inference call outlived its deadline.
///
/// This is deliberately a DISTINCT type from `CancellationError`. The two mean
/// different things and must not be conflated:
///
/// - `CancellationError` means the enclosing background window expired. The
///   work was interrupted by the OS, the device is fine, and the correct
///   response is to checkpoint and resume (`.resumeFromCheckpoint`).
/// - `FMInferenceTimeoutError` means the model itself did not answer in time.
///   Nothing external interrupted anything, and re-running the same window in
///   the same pass would just spend another full deadline.
///
/// `SemanticScanStatus.from(error:)` maps this to
/// `SemanticScanStatus.inferenceTimeout` so the persisted row names the
/// outcome instead of folding it into `.failedTransient`, where it would be
/// indistinguishable from a genuine unrecognized model error.
struct FMInferenceTimeoutError: Error, Sendable, Equatable {
    /// The deadline that elapsed. Carried so a log line or a test can assert
    /// WHICH budget was exceeded rather than just that something was.
    let deadline: Duration
}

enum FMInferenceDeadline {
    /// Per-call deadline for a single Foundation Models inference request.
    ///
    /// THIS IS A HANG-BREAKER, NOT A SCHEDULER. It is not here to make a call
    /// fit inside a granted background window: playhead-bkhc already made an
    /// expiry cost only the in-flight window rather than the whole pass, so
    /// nothing downstream needs a call to be short. Its only job is to stop a
    /// call that is genuinely stuck and would never return. That framing is
    /// what licenses a generous value — and the asymmetry demands one. Killing
    /// a slow SUCCESS destroys coverage that would have existed; letting a slow
    /// FAILURE run wastes battery and lane time. Those costs are not equal.
    /// **When in doubt, err long.**
    ///
    /// CHOSEN AGAINST THE MEASURED SUCCESS TAIL. Across the 200 successful
    /// scans on the 2026-07-30 device pull:
    ///
    ///     p50      5.3 s
    ///     p90     33.2 s
    ///     p95     56.0 s
    ///     p99     86.7 s
    ///     max  1,122.4 s   <- one call, 12.7x the next-slowest success
    ///
    /// The distribution has a hard gap: the second-slowest success is 88.6 s
    /// and the slowest is 1,122.4 s, with NOTHING in between. Counting
    /// successes that a candidate deadline would have killed:
    ///
    ///      60 s -> 6 of 200   (3% of real coverage, silently — REJECTED)
    ///      90 s -> 1
    ///     120 s -> 1
    ///     300 s -> 1
    ///     600 s -> 1
    ///
    /// Every value from 90 s to 600 s costs exactly the same single call, so
    /// inside that band headroom is free and the choice should be made for
    /// safety. 300 s is 3.4x the slowest legitimate observed success — room for
    /// a device materially slower than the one measured (thermal throttling can
    /// plausibly multiply per-call latency) — while still bounding a wedged
    /// call to five minutes instead of the 27.7 minutes actually observed.
    ///
    /// CONSTANT, NOT CONTEXTUAL, and that is a decision rather than an
    /// omission. A foreground call could in principle afford longer than a
    /// background one, but that argument is about how much time is AVAILABLE,
    /// and bkhc removed the need to reason about availability here. What this
    /// value encodes is "no plausible real inference takes this long", which is
    /// a property of the model and the prompt, not of who is asking. A
    /// per-caller value would have to be justified against a per-caller tail
    /// that does not exist in the data — every measured call comes from the
    /// same backfill lane.
    ///
    /// NOT SCALED BY WINDOW SIZE either, for the same reason plus a measured
    /// one: latency does not track window duration on this data. Mean success
    /// latency by window-duration bucket is 9.2 s (<30 s windows), 13.8 s
    /// (30-60 s), 19.9 s (60-120 s) and 11.5 s (>=120 s) — flat, and the
    /// widest bucket is FASTER than the 60-120 s one. The three widest windows
    /// in the corpus (806 s of audio) completed in 3.5 s, 4.2 s and 12.5 s. A
    /// size-scaled deadline would hand the most headroom to the windows that
    /// need it least.
    static let standard: Duration = .seconds(300)

    /// playhead-qk44: the bound for the NON-generative model calls on the same
    /// path — `SystemLanguageModel.tokenCount(for:)` for a prompt and for each
    /// `@Generable` schema.
    ///
    /// These were unbounded, and that is not a detail. `planPassA` issues one
    /// `tokenCount` XPC round trip per candidate window, so a 92-minute episode
    /// makes tens of them BEFORE the first `respond` — which means a wedge in
    /// the framework at that point never reaches a deadline-guarded call at
    /// all. That is the exact shape of the 23-minute silent `running` row this
    /// bead was filed for: `status='running'`, `progressCursor` empty, zero
    /// scan rows, `updatedAt` frozen at the instant the row flipped.
    ///
    /// 30 s rather than `standard`, because these calls are not inference.
    /// There is no sampling and no generation — a tokenizer round trip is
    /// linear in prompt length and the prompts here are bounded by the model's
    /// own context window. Two orders of magnitude above any honest cost.
    ///
    /// The asymmetry that licenses the shorter value runs the OPPOSITE way to
    /// `standard`'s. Killing a slow inference destroys coverage that would have
    /// existed; killing a slow token count does not, because six of the eight
    /// `runtime.tokenCount` call sites already wrap it in `try?` with a
    /// documented character-based fallback estimate. A bound that fires there
    /// costs planning PRECISION, not coverage. Where it does propagate
    /// (`planPassA`), the outcome is a named `inference_timeout` row and a
    /// terminal job — strictly better than a row that says `running` forever.
    static let metadata: Duration = .seconds(30)

    /// Run `operation` under a hard wall-clock deadline.
    ///
    /// Returns whatever `operation` returns, rethrows whatever it throws, and
    /// throws ``FMInferenceTimeoutError`` if `deadline` elapses first. A
    /// non-positive `deadline` disables the bound entirely and calls
    /// `operation` inline, so a caller can opt out without a second code path.
    ///
    /// Cancellation of the CALLING task propagates as `CancellationError`,
    /// never as a timeout — see ``FMInferenceTimeoutError`` for why that
    /// distinction is load-bearing.
    static func run<T: Sendable>(
        _ deadline: Duration,
        operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        guard deadline > .zero else {
            return try await operation()
        }

        let race = AbandonableRace<T>()
        let work = Task {
            do {
                await race.settle(.success(try await operation()))
            } catch {
                await race.settle(.failure(error))
            }
        }
        let timer = Task {
            do {
                try await Task.sleep(for: deadline)
            } catch {
                // The timer lost the race and was cancelled. Settling here
                // would be a spurious timeout.
                return
            }
            await race.settle(.failure(FMInferenceTimeoutError(deadline: deadline)))
        }

        let outcome = await withTaskCancellationHandler {
            await race.awaitOutcome()
        } onCancel: {
            // FORWARD the cancellation; do NOT decide the race on it.
            //
            // `Task { }` inherits priority, actor context and task-locals from
            // its creator but NOT cancellation, so without this a BG-window
            // expiry would never reach the in-flight call at all. Before this
            // file the operation ran inline in the caller's task and observed
            // cancellation directly; this restores that exactly.
            //
            // What this deliberately does NOT do is settle the race as
            // cancelled. That was the first version and it broke playhead-bkhc:
            // an operation that does not observe cancellation used to RUN TO
            // COMPLETION when the grant expired, and the coarse pass banked
            // that window before returning partial results. Resolving the race
            // on the cancellation signal instead abandoned a window that was
            // about to finish — re-introducing the discard bkhc had just fixed
            // (caught by `expiryDuringCoarsePassCheckpointsAudioAndResumes`,
            // which measures banked audio in seconds and dropped 20s -> 10s).
            //
            // So cancellation is delivered and then the operation decides: a
            // cancellation-aware call throws `CancellationError` promptly, and
            // one that is not still finishes and is banked — now with the
            // deadline as its backstop instead of nothing.
            //
            // Nothing can hang here. `timer` is unstructured too, so it does
            // NOT inherit the caller's cancellation; it keeps running and
            // settles the race even if the operation never returns. The cost of
            // that guarantee is worth stating plainly: after a BG-window expiry
            // a cancellation-IGNORING call keeps this waiter parked for up to
            // the remaining deadline, which can outlast the OS grace period.
            // The alternative — resolving the race on the cancellation signal —
            // is what abandoned a nearly-finished window and broke
            // playhead-bkhc's banking, so this is the deliberate trade.
            work.cancel()
        }

        timer.cancel()
        // Best-effort stop for the losing call. We deliberately do NOT await
        // it — see the file header for why waiting would defeat the deadline.
        work.cancel()
        return try outcome.get()
    }
}

/// First-writer-wins handoff between an operation, its bound, and the caller's
/// cancellation handler. An actor rather than a lock because all three writers
/// are already in async context, and because `awaitOutcome` needs to park a
/// continuation under the same isolation that settles it.
///
/// playhead-qk44 lifted this out of `FMInferenceDeadline` so
/// ``FMNoProgressWatchdog`` can share it. Both bounds have to ABANDON the
/// operation rather than await it (see the file header), and one correct
/// implementation of that handoff is worth more than two.
private actor AbandonableRace<T: Sendable> {
    private var settled: Result<T, Error>?
    private var waiter: CheckedContinuation<Result<T, Error>, Never>?

    func settle(_ result: Result<T, Error>) {
        guard settled == nil else { return }
        settled = result
        if let waiter {
            self.waiter = nil
            waiter.resume(returning: result)
        }
    }

    /// Call at most once. Suspends until the first `settle`.
    func awaitOutcome() async -> Result<T, Error> {
        if let settled {
            return settled
        }
        // Enforced, not merely documented: a second caller would overwrite
        // the first's continuation, which never resumes — a permanent hang
        // that would be near-impossible to diagnose from a stuck backfill.
        precondition(waiter == nil, "AbandonableRace supports a single waiter")
        return await withCheckedContinuation { continuation in
            // No suspension point since the `settled` check above, so this
            // cannot miss a settle that happened in between.
            waiter = continuation
        }
    }
}

// MARK: - playhead-qk44: the NO-PROGRESS bound

/// A monotonic count of units of work an operation has finished, shared
/// between that operation and the watchdog that is timing it.
///
/// The counter is deliberately opaque and monotonic: the watchdog compares
/// successive readings and never interprets the value, so any definition of
/// "a unit of work" the operation likes is admissible as long as it only ever
/// goes up and only ever goes up because something real happened.
final class FMProgressTicker: Sendable {
    private let state = OSAllocatedUnfairLock(initialState: 0)

    init() {}

    /// Record that one unit of work completed.
    func note() {
        state.withLock { $0 += 1 }
    }

    /// The number of units recorded so far.
    var observed: Int {
        state.withLock { $0 }
    }
}

/// Thrown by ``FMNoProgressWatchdog/run(interval:consecutiveIntervalLimit:ticker:sleep:operation:)``
/// when an operation completed no units of work for `consecutiveIntervals`
/// consecutive sampling intervals.
///
/// A DISTINCT type from ``FMInferenceTimeoutError`` because it answers a
/// different question. `FMInferenceTimeoutError` means "this one call to the
/// model did not answer in time" — the pass around it may be perfectly
/// healthy, banking window after window. `FMNoProgressError` means "the pass
/// as a whole stopped producing anything", which is the only signal that can
/// catch a wedge in the parts of the path no per-call deadline covers:
/// availability probing, tokenisation, window planning, or a hang inside the
/// framework that never reaches a bounded `respond`.
struct FMNoProgressError: Error, Sendable, Equatable {
    /// The sampling interval that elapsed without work.
    let interval: Duration
    /// How many CONSECUTIVE intervals passed with no unit of work completed.
    let consecutiveIntervals: Int
}

/// A wall-clock bound on SILENCE rather than on duration.
///
/// WHY THIS EXISTS (playhead-qk44). On 2026-07-31 a `fullEpisodeScan` sat at
/// `status='running'` for 23 minutes with an empty `progressCursor`, zero
/// `semantic_scan_results` rows, no error and no terminal — while the app was
/// foregrounded and active the entire time. Nothing was wrong with the job
/// row; nothing was watching the WORK.
///
/// Three facts make that state undiagnosable without this bound:
///
/// 1. `FMInferenceDeadline` bounds `respond` and nothing else. The coarse pass
///    reaches the on-device model through several other awaits first —
///    `FoundationModelsUsabilityProbe` issues a bare `respond`, and
///    `SystemLanguageModel.tokenCount(for:)` is an XPC round trip called once
///    per candidate window inside `planPassA`. A hang in any of those never
///    reaches a deadline-guarded call at all.
/// 2. `coarsePassA` persisted NOTHING until it returned. Every scan row was
///    written by `BackfillJobRunner.runJob` from the returned windows, so a
///    pass in flight was invisible in the database by construction.
///    **No longer true as of playhead-26od**, which checkpoints each screened
///    window and an honest resume cursor while the pass runs — so a live pass
///    now leaves a trail. That STRENGTHENS the separation this bound draws
///    rather than replacing it: rows arriving is more evidence of work, and a
///    wedge in the pre-window stretch (probe, tokenisation, `planPassA`) still
///    produces no row to observe, because there is no screened window yet.
/// 3. A healthy coarse pass takes 12–45 minutes per episode on device. So the
///    observable state of a healthy pass and a wedged one were, before this
///    bead, byte-identical: `running`, no cursor, no rows.
///
/// A DURATION bound cannot separate those two — any value long enough to
/// spare a healthy 45-minute pass is too long to be a bound. A SILENCE bound
/// separates them exactly: a healthy pass banks a window every few seconds to
/// a few minutes, and a wedged one banks nothing, ever.
///
/// CONSECUTIVE, NEVER CUMULATIVE. The same discipline `playhead-bkhc` applies
/// to barren background windows and `playhead-8d5r` applies to inference
/// timeouts. A pass that answers slowly is still converging and must not be
/// killed for the sum of its slow stretches; only an unbroken run of silence
/// is evidence that nothing is coming. Any completed unit of work resets the
/// count to zero.
///
/// ABANDONS, DOES NOT AWAIT. Same reasoning as `FMInferenceDeadline` — see
/// this file's header. A structured race would report the stall only after
/// the thing it was supposed to bound finished, which is no bound at all.
enum FMNoProgressWatchdog {
    /// How long a coarse pass may produce nothing before that counts as one
    /// strike.
    ///
    /// 180 s is chosen against the ONE bound that already exists on this path:
    /// `FMInferenceDeadline.standard` is 300 s, so a single legitimate window
    /// can occupy at most 300 s before it either answers or is abandoned with
    /// a recorded failure — and both of those are units of work that reset the
    /// count. With a 180 s interval the slowest legitimate window scores at
    /// most two strikes (at 180 s and 360 s, with the window resolving by
    /// 300 s), which is why the limit below is three and not two.
    static let standardInterval: Duration = .seconds(180)

    /// How many consecutive silent intervals end the pass.
    ///
    /// Three, for the reason above: two is reachable by a single slow-but-
    /// legitimate window, three is not. 3 x 180 s = 540 s, so the 23-minute
    /// silence that produced this bead becomes a named failure in nine
    /// minutes. That is deliberately BELOW `AnalysisStore
    /// .strandedJobFreshnessSeconds` (600 s), so the in-process watchdog
    /// always reaches a wedged job before the database reaper would — the
    /// reaper is left to do the only job it can do correctly, which is
    /// cleaning up after a process that died.
    static let standardConsecutiveIntervalLimit = 3

    /// Run `operation` under a no-progress bound.
    ///
    /// Returns whatever `operation` returns and rethrows whatever it throws.
    /// Throws ``FMNoProgressError`` when `ticker` records no new work for
    /// `consecutiveIntervalLimit` consecutive `interval`s.
    ///
    /// A non-positive `interval` or a non-positive `consecutiveIntervalLimit`
    /// disables the bound entirely and calls `operation` inline, so a caller
    /// can opt out without a second code path.
    ///
    /// `sleep` is injectable so a test can drive the sampling loop without
    /// wall-clock. Production uses `Task.sleep`.
    static func run<T: Sendable>(
        interval: Duration,
        consecutiveIntervalLimit: Int,
        ticker: FMProgressTicker,
        sleep: @escaping @Sendable (Duration) async throws -> Void = { try await Task.sleep(for: $0) },
        operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        guard interval > .zero, consecutiveIntervalLimit > 0 else {
            return try await operation()
        }

        let race = AbandonableRace<T>()
        let work = Task {
            do {
                await race.settle(.success(try await operation()))
            } catch {
                await race.settle(.failure(error))
            }
        }
        let watchdog = Task {
            var lastObserved = ticker.observed
            var consecutiveSilentIntervals = 0
            while consecutiveSilentIntervals < consecutiveIntervalLimit {
                do {
                    try await sleep(interval)
                } catch {
                    // The watchdog lost the race and was cancelled. Settling
                    // here would be a spurious stall.
                    return
                }
                let observed = ticker.observed
                if observed == lastObserved {
                    consecutiveSilentIntervals += 1
                } else {
                    lastObserved = observed
                    consecutiveSilentIntervals = 0
                }
            }
            await race.settle(
                .failure(
                    FMNoProgressError(
                        interval: interval,
                        consecutiveIntervals: consecutiveIntervalLimit
                    )
                )
            )
        }

        let outcome = await withTaskCancellationHandler {
            await race.awaitOutcome()
        } onCancel: {
            // FORWARD the cancellation exactly as `FMInferenceDeadline` does,
            // and for the same reason: an unstructured `Task` does not inherit
            // it, so without this a background-window expiry would never reach
            // the in-flight pass. Do NOT decide the race here — an operation
            // that is about to bank a finished window must be allowed to.
            work.cancel()
        }

        watchdog.cancel()
        // Best-effort stop for the losing operation. Deliberately NOT awaited
        // — see the file header for why waiting would defeat the bound.
        work.cancel()
        return try outcome.get()
    }
}
