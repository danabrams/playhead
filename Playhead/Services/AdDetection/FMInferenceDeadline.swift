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

        let race = Race<T>()
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

    /// First-writer-wins handoff between the operation, the timer, and the
    /// caller's cancellation handler. An actor rather than a lock because all
    /// three writers are already in async context, and because `awaitOutcome`
    /// needs to park a continuation under the same isolation that settles it.
    private actor Race<T: Sendable> {
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
            precondition(waiter == nil, "FMInferenceDeadline.Race supports a single waiter")
            return await withCheckedContinuation { continuation in
                // No suspension point since the `settled` check above, so this
                // cannot miss a settle that happened in between.
                waiter = continuation
            }
        }
    }
}
