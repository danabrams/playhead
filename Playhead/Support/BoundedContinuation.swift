// BoundedContinuation.swift
// playhead-c25o: a cancellable, hard-timeout-bounded bridge from a
// completion-handler API into async/await.
//
// WHY THIS EXISTS
// ───────────────
// A bare `withCheckedContinuation` that is resumed only from someone
// else's callback is an *unbounded, uncancellable* suspension. If the
// callback never fires — an out-of-process daemon that never replies, a
// framework that drops its completion on an internal error — the awaiting
// task parks for the lifetime of the process. `Task.cancel()` cannot
// unwind it, and neither can a `.timeLimit` trait or a `withTaskGroup`
// deadline, because a parked continuation has no suspension point left
// to throw at (this is the failure class identified in playhead-xc6b).
//
// The shipping instance that motivated this file is
// `BGTaskScheduler.getPendingTaskRequests`, awaited from inside a
// BGTask handler *before* that handler arms its `expirationHandler`. A
// single lost XPC reply meant: no `setTaskCompleted`, no armed
// expiration handler, so iOS terminated the process and penalised
// future scheduling — and, because the caller's `defer` never ran, its
// reentrancy latch stayed set and killed all further rescheduling for
// the process lifetime.
//
// FAIL OPEN, NOT CLOSED
// ─────────────────────
// This helper resolves with a caller-supplied `fallback` rather than
// throwing. That is deliberate for advisory queries: the caller gets a
// value, never an error and never a hang, and the cost of a wrong-but-
// safe answer (e.g. "nothing is pending" → one redundant re-submit) is
// far cheaper than the failure it prevents. Choose a `fallback` whose
// worst case is a redundant retry; if a call site genuinely cannot
// tolerate a wrong answer, it should not use this helper.
//
// ADOPTION NOTE (sibling beads — do NOT refactor them from here)
// ──────────────────────────────────────────────────────────────
// The same defect shape lives at `TranscriptEngine`/`TranscriptEngineService`
// (playhead-8m2w) and `PlayheadRuntime` (playhead-2gka). Both are filed
// separately. This helper is written to be adoptable by them as-is: it is
// generic over the bridged value, takes the timeout and the fallback from
// the call site, and reports *why* it fell back through `onFallback` so a
// call site can log or count it.

import Foundation

/// Why a bounded continuation resolved with its caller-supplied
/// `fallback` instead of the value the callback would have produced.
enum BoundedContinuationFallback: String {
    /// The hard timeout elapsed before the callback fired.
    case timedOut
    /// The surrounding task was cancelled before the callback fired.
    case cancelled
}

/// Bridges a completion-handler API into `async`/`await` such that the
/// awaiting task can never park indefinitely.
///
/// Two bounds are added on top of `withCheckedContinuation`:
///
/// 1. **Hard timeout.** If `body`'s resume closure has not been called
///    within `timeout`, the continuation resumes with `fallback`.
/// 2. **Cancellation.** If the surrounding task is cancelled, the
///    continuation resumes with `fallback` immediately.
///
/// Both are *fail-open*: this function is non-throwing and always
/// returns a value.
///
/// ### The once guard
///
/// Resuming a `CheckedContinuation` twice is a runtime trap, and the
/// timeout, the cancellation handler and the callback genuinely race —
/// nothing orders a daemon reply against a sleeping timer. Every
/// resolution therefore goes through a single lock-protected state
/// machine (`BoundedContinuationGate`) that transitions out of its
/// resumable phase *under the lock* before it resumes. The first
/// resolution to win the lock owns the continuation; every later one is
/// dropped on the floor. The lock is released before the continuation is
/// resumed, so a synchronous callback cannot re-enter the lock.
///
/// The gate also handles resolution *before* the continuation exists:
/// `withTaskCancellationHandler` invokes `onCancel` immediately when the
/// task is already cancelled on entry, which can happen before the
/// continuation is created. That resolution is parked and replayed the
/// moment the continuation attaches.
///
/// - Parameters:
///   - timeout: Hard ceiling on how long to wait for the callback. Pick
///     a value that is short relative to whatever budget the *caller*
///     is running inside — a timeout longer than the caller's own budget
///     protects nothing.
///   - fallback: Value to resume with on timeout or cancellation.
///   - onFallback: Optional hook invoked (off the caller's isolation)
///     exactly once when, and only when, the fallback is used. Intended
///     for logging or telemetry; it must not block.
///   - body: Starts the underlying work, handing it a resume closure.
///     The resume closure is safe to call any number of times from any
///     thread; only the first call is honoured. `body` is always
///     invoked, even if the result is already decided, so a call site
///     never has to reason about whether its side effect ran.
/// - Returns: The first value handed to the resume closure, or
///   `fallback` on timeout/cancellation.
func withBoundedCheckedContinuation<T: Sendable>(
    timeout: Duration,
    fallback: T,
    onFallback: (@Sendable (BoundedContinuationFallback) -> Void)? = nil,
    _ body: (@escaping @Sendable (T) -> Void) -> Void
) async -> T {
    let gate = BoundedContinuationGate(fallback: fallback, onFallback: onFallback)
    return await withTaskCancellationHandler {
        await withCheckedContinuation { (continuation: CheckedContinuation<T, Never>) in
            gate.attach(continuation)
            gate.armTimeout(timeout)
            body { value in gate.settle(with: value) }
        }
    } onCancel: {
        gate.settleWithFallback(.cancelled)
    }
}

// MARK: - Gate

/// Serialises every possible resolution of one bounded continuation so
/// that exactly one of them resumes it.
///
/// `@unchecked Sendable` is load-bearing and audited: all mutable state
/// (`phase`, `timeoutTask`) is touched only while `lock` is held, and
/// the `CheckedContinuation` is handed out of the lock exactly once.
private final class BoundedContinuationGate<T: Sendable>: @unchecked Sendable {

    /// Resolution state. The transitions out of `waiting`/`suspended`
    /// happen under the lock, which is what makes double-resume
    /// impossible.
    private enum Phase {
        /// No continuation yet and no resolution yet.
        case waiting
        /// Continuation attached, awaiting a resolution.
        case suspended(CheckedContinuation<T, Never>)
        /// Resolved before the continuation attached; value is parked
        /// until `attach` can replay it.
        case settledEarly(T)
        /// Continuation has been resumed. Terminal.
        case done
    }

    private let lock = NSLock()
    private var phase: Phase = .waiting
    private var timeoutTask: Task<Void, Never>?

    private let fallback: T
    private let onFallback: (@Sendable (BoundedContinuationFallback) -> Void)?

    init(fallback: T, onFallback: (@Sendable (BoundedContinuationFallback) -> Void)?) {
        self.fallback = fallback
        self.onFallback = onFallback
    }

    /// Hand the gate its continuation. Called exactly once, before any
    /// callback can be registered.
    func attach(_ continuation: CheckedContinuation<T, Never>) {
        lock.lock()
        switch phase {
        case .waiting:
            phase = .suspended(continuation)
            lock.unlock()
        case .settledEarly(let value):
            // Cancelled before the continuation existed — replay now.
            phase = .done
            let staleTimeout = takeTimeoutTaskLocked()
            lock.unlock()
            staleTimeout?.cancel()
            continuation.resume(returning: value)
        case .suspended, .done:
            lock.unlock()
            // Unreachable: `attach` is called once per gate. Resume with
            // the fallback anyway so a future misuse degrades into a
            // wrong-but-prompt answer rather than a permanent hang.
            assertionFailure("BoundedContinuationGate.attach called more than once")
            continuation.resume(returning: fallback)
        }
    }

    /// Start the hard-timeout timer. No-op once the result is decided.
    func armTimeout(_ timeout: Duration) {
        // Build the task outside the lock; `Task` bodies can start on
        // another thread immediately and would otherwise contend on a
        // lock this call still holds.
        let task = Task { [self] in
            try? await Task.sleep(for: timeout)
            settleWithFallback(.timedOut)
        }
        lock.lock()
        switch phase {
        case .waiting, .suspended:
            timeoutTask = task
            lock.unlock()
        case .settledEarly, .done:
            lock.unlock()
            task.cancel()
        }
    }

    /// Resolve with `value`. The FIRST caller wins; every later call —
    /// a daemon reply that lands after the timeout, a timeout that fires
    /// while the reply is in flight, a callback invoked twice by a buggy
    /// framework — returns without touching the continuation.
    func settle(with value: T) {
        _ = resolve(with: value)
    }

    /// Resolve with the caller-supplied fallback. `onFallback` is
    /// reported by — and only by — the call that actually won the race,
    /// so a timeout losing to a cancellation (or to the real reply)
    /// stays silent and the hook fires at most once per gate.
    func settleWithFallback(_ reason: BoundedContinuationFallback) {
        guard resolve(with: fallback) else { return }
        onFallback?(reason)
    }

    /// The single mutating entry point. Returns `true` iff this call is
    /// the one that decided the outcome.
    private func resolve(with value: T) -> Bool {
        lock.lock()
        switch phase {
        case .waiting:
            phase = .settledEarly(value)
            let staleTimeout = takeTimeoutTaskLocked()
            lock.unlock()
            staleTimeout?.cancel()
            return true
        case .suspended(let continuation):
            phase = .done
            let staleTimeout = takeTimeoutTaskLocked()
            lock.unlock()
            staleTimeout?.cancel()
            // Resumed OUTSIDE the lock: a continuation resume can run
            // the awaiting task's next step, which must not be able to
            // re-enter this non-reentrant lock.
            continuation.resume(returning: value)
            return true
        case .settledEarly, .done:
            // The once guard.
            lock.unlock()
            return false
        }
    }

    /// Must be called with `lock` held.
    private func takeTimeoutTaskLocked() -> Task<Void, Never>? {
        let task = timeoutTask
        timeoutTask = nil
        return task
    }
}
