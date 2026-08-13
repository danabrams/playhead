// BackgroundSessionIO.swift
// playhead-nsjn: runs every call that crosses into a background
// `URLSession`'s XPC channel OFF the Swift Concurrency cooperative pool,
// and BOUNDS it.
//
// ─────────────────────────────────────────────────────────────────────────
// WHY THIS EXISTS — the measurement, not the theory
// ─────────────────────────────────────────────────────────────────────────
// `URLSession.downloadTask(with:)` and `downloadTask(withResumeData:)` look
// like cheap object construction. On a session created with
// `URLSessionConfiguration.background(withIdentifier:)` they are not. A
// `sample` of the test host during a wedged full-plan run showed the real
// shape:
//
//     DownloadManager.backgroundDownload(episodeId:from:context:)
//       -[__NSURLBackgroundSession _downloadTaskWithTaskForClass:]
//         -[__NSURLBackgroundSession performBlockOnQueueAndRethrowExceptions:]
//           _dispatch_lane_barrier_sync_invoke_and_complete          <- barrier
//             _NSXPCDistantObjectSimpleMessageSend1
//               __NSXPCCONNECTION_IS_WAITING_FOR_A_SYNCHRONOUS_REPLY__
//                 xpc_connection_send_message_with_reply_sync
//                   mach_msg2_trap                                   <- parked
//
// So the call is: take a barrier on the session's serial work queue, then
// block the calling thread in a SYNCHRONOUS XPC round-trip to
// `nsurlsessiond`. Every other caller of that session then piles up behind
// the barrier in `_dispatch_event_loop_wait_for_ownership`.
//
// `DownloadManager` is an `actor`, so its methods run on the Swift
// Concurrency COOPERATIVE thread pool. That pool is exactly
// `activeProcessorCount` threads wide and NEVER grows — the runtime has no
// blocked-thread detection. In the sampled run all 10 of 10 cooperative
// threads on this box were parked in that call chain: one holding the
// barrier inside the XPC wait, nine waiting for the barrier. The process
// had zero runnable concurrency left. 10,022 Swift Testing tests had
// started and not one finished.
//
// That is also why no time limit fired. A `.timeLimit` trait is a race
// between the test body and a `Task.sleep`; when the sleep expires its
// continuation must be scheduled on a cooperative thread to run, and there
// were none. And even had it run, `Task` cancellation only sets a flag that
// some suspension point must observe — there is no suspension point between
// entering and returning from `downloadTask(with:)`, so cancellation can
// never reach a thread sitting in `mach_msg2_trap`.
//
// This is a PRODUCTION defect, not a test artifact. `nsurlsessiond` is a
// shared system daemon; a force-quit leaves half-suspended transfers in it,
// and `ForceQuitResumeScan` calls back into it on the next cold launch. A
// daemon that is slow or wedged converts into an app-wide Swift Concurrency
// deadlock with no spinner, no timeout, and no crash report.
//
// ─────────────────────────────────────────────────────────────────────────
// WHAT THIS DOES ABOUT IT
// ─────────────────────────────────────────────────────────────────────────
//   1. The blocking call runs on a dedicated, NON-cooperative serial queue.
//      A stalled daemon then costs one ordinary thread instead of a slice
//      of the fixed-width cooperative pool, so the rest of the process —
//      the UI, other actors, and in tests every other suite plus the
//      time-limit machinery — keeps running.
//   2. The caller waits with a DEADLINE and suspends cooperatively while it
//      waits, so an unanswered call surfaces as a named `nil` outcome the
//      caller can log and recover from rather than an invisible hang.
//   3. A result that arrives after the deadline is handed to
//      `discardingLateResult` instead of being dropped. For a URLSession
//      task that means `cancel()` — otherwise a daemon that answers late
//      would leave a live transfer nobody in this process is tracking.
//
// Note what is deliberately NOT claimed: a wedged daemon still strands the
// one queue thread that is inside the XPC call, and work submitted behind
// it waits its own full deadline before reporting `nil`. Bounding the
// caller is the containment; unwedging `nsurlsessiond` is not something a
// client process can do.

import Foundation
import OSLog
import os

/// Executes blocking background-`URLSession` calls off the Swift
/// Concurrency cooperative pool, under a wall-clock bound.
///
/// Injected into ``DownloadManager`` so tests can substitute
/// ``Behavior/neverAnswers`` and exercise the recovery branches without
/// needing a genuinely wedged `nsurlsessiond`.
struct BackgroundSessionIO: Sendable {

    /// How a submitted body is executed.
    enum Behavior: Sendable {
        /// Production: run the body on the dedicated non-cooperative queue.
        case dedicatedThread

        #if DEBUG
        /// Test seam: model a daemon that never answers. The body is NEVER
        /// run and `perform` reports `nil` immediately — the deadline path
        /// itself is covered by `BackgroundSessionIOTests`, so tests of the
        /// *recovery* branches do not have to pay a real timeout.
        case neverAnswers

        /// Test seam: refuse only the calls whose label contains `marker`
        /// and run every other call normally.
        ///
        /// `neverAnswers` refuses the FIRST call, so it can never reach a
        /// branch that lives BEHIND a successful one — and the
        /// resume-timeout path in `backgroundDownload` is exactly that:
        /// creation has to succeed for there to be a task to abandon.
        /// Refusing everything is also not a safe way to get there, because
        /// the cleanup `cancel()` would be refused too and a real transfer
        /// would stay registered with the simulator's `nsurlsessiond` — the
        /// residue class this bead exists to stop producing.
        case refusesCallsLabelled(String)

        /// Test seam: refuse the calls whose label contains `marker` for
        /// exactly as long as `whileRefusing()` answers `true`, and run
        /// every other call normally.
        ///
        /// playhead-gpdb. `refusesCallsLabelled` refuses for the life of the
        /// instance, so it can prove a refusal is REPORTED but never that it
        /// was not CACHED — the second attempt is refused by the seam
        /// whatever the code under test did with the first. A daemon that
        /// refuses once and then answers is the only shape that separates
        /// "the memo stayed empty, so the next call retried" from "the
        /// failure was memoized and the subsystem is dead for the process",
        /// and those two are byte-identical under every other seam here.
        case intermittentlyRefusesCallsLabelled(
            String, whileRefusing: @Sendable () -> Bool
        )
        #endif
    }

    /// Wall-clock bound on a single background-session call.
    ///
    /// A healthy `nsurlsessiond` answers `downloadTask(with:)` in
    /// milliseconds, so this is not a latency budget — it is the line
    /// between "slow" and "never", set wide enough that no healthy device
    /// crosses it and narrow enough that a user is not left holding a dead
    /// download queue.
    static let defaultTimeout: TimeInterval = 10

    /// Default label of the dedicated queue.
    static let defaultQueueLabel = "com.playhead.downloads.session-xpc"

    let behavior: Behavior
    let timeout: TimeInterval

    /// Label of this instance's dedicated queue. Every instance owns its
    /// own queue so that a test which deliberately blocks one for seconds
    /// cannot stall production `.shared` traffic — or another test — in the
    /// same process.
    let queueLabel: String

    /// Serial on purpose. The session's own work queue already serializes
    /// these calls, so concurrency buys nothing here, and an unbounded
    /// concurrent queue would answer a wedged daemon with a thread
    /// explosion. Serial means a wedge strands exactly one thread.
    private let queue: DispatchQueue

    /// Deadlines fire on their own queue: the work queue is precisely the
    /// thing that may be blocked, so a timer scheduled there could never
    /// run when it is needed most.
    private let deadlineQueue: DispatchQueue

    /// The binding every production `DownloadManager` uses.
    static let shared = BackgroundSessionIO(
        behavior: .dedicatedThread,
        timeout: defaultTimeout
    )

    private static let logger = Logger(
        subsystem: "com.playhead", category: "BackgroundSessionIO"
    )

    /// Stamped onto each instance's work queue so a caller can ask, from
    /// inside a body, which queue it is actually running on. This is the
    /// witness for the property the whole file exists to provide: the
    /// blocking call left the cooperative pool.
    private static let queueLabelKey = DispatchSpecificKey<String>()

    /// The dedicated-queue label when the current code is running on one of
    /// these queues, and `nil` anywhere else — including on a Swift
    /// Concurrency cooperative thread.
    static var currentQueueLabel: String? {
        DispatchQueue.getSpecific(key: queueLabelKey)
    }

    init(
        behavior: Behavior,
        timeout: TimeInterval,
        queueLabel: String = BackgroundSessionIO.defaultQueueLabel
    ) {
        self.behavior = behavior
        self.timeout = timeout
        self.queueLabel = queueLabel
        self.queue = DispatchQueue(label: queueLabel, qos: .userInitiated)
        self.deadlineQueue = DispatchQueue(
            label: "\(queueLabel).deadline", qos: .userInitiated
        )
        self.queue.setSpecific(key: Self.queueLabelKey, value: queueLabel)
    }

    /// A sibling carrying the same behaviour and the same bound on its OWN
    /// serial queue.
    ///
    /// playhead-rouw. The work queue is serial, so two kinds of call that
    /// share an instance share a stall: a submission sitting on a silent
    /// daemon delays every later one, and each of those is then released by
    /// its own deadline having never run. That is right for calls of the same
    /// kind — they are competing for one session's barrier anyway — and wrong
    /// across kinds, where it lets an enumeration nobody is waiting on starve
    /// the download path. A caller that wants the bound WITHOUT the shared
    /// queue asks for one of these, and keeps the injected behaviour so a
    /// test's `.neverAnswers` still reaches it.
    func onItsOwnQueue(labelled label: String) -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: behavior, timeout: timeout, queueLabel: label
        )
    }

    /// Runs `body` off the cooperative pool and returns its result, or
    /// `nil` when the deadline passed first.
    ///
    /// - Parameters:
    ///   - label: what is being asked of the daemon, for the failure log.
    ///   - discardingLateResult: disposes of a result produced after the
    ///     caller gave up. Pass `{ $0.cancel() }` for a URLSession task.
    ///   - body: the blocking call. Runs at most once, and NOT at all if
    ///     the deadline passed before it reached the front of the queue —
    ///     starting a transfer nobody is tracking is worse than not
    ///     starting one.
    func perform<T: Sendable>(
        label: String,
        discardingLateResult discard: (@Sendable (T) -> Void)? = nil,
        running body: @escaping @Sendable () -> T
    ) async -> T? {
        #if DEBUG
        switch behavior {
        case .neverAnswers:
            Self.logger.error(
                "\(label, privacy: .public): neverAnswers test seam — reporting the daemon as unavailable"
            )
            return nil
        case .refusesCallsLabelled(let marker) where label.contains(marker):
            Self.logger.error(
                "\(label, privacy: .public): refusesCallsLabelled(\(marker, privacy: .public)) test seam — reporting the daemon as unavailable"
            )
            return nil
        case .intermittentlyRefusesCallsLabelled(let marker, let refusing)
            where label.contains(marker) && refusing():
            Self.logger.error(
                "\(label, privacy: .public): intermittentlyRefusesCallsLabelled(\(marker, privacy: .public)) test seam — reporting the daemon as unavailable"
            )
            return nil
        default:
            break
        }
        #endif

        let waiter = Waiter<T>()
        let bound = timeout
        let queue = self.queue
        let deadlineQueue = self.deadlineQueue
        return await withCheckedContinuation { continuation in
            waiter.arm(continuation)
            queue.async {
                guard waiter.mayRunBody() else {
                    Self.logger.error(
                        "\(label, privacy: .public): reached the daemon queue after its caller had already given up — not started"
                    )
                    return
                }
                let value = body()
                if !waiter.deliver(value) {
                    discard?(value)
                }
            }
            deadlineQueue.asyncAfter(deadline: .now() + bound) {
                if waiter.expire() {
                    Self.logger.error(
                        "\(label, privacy: .public): the background transfer daemon did not answer within \(bound, privacy: .public)s"
                    )
                }
            }
        }
    }
}

// MARK: - Waiter

/// One-shot handoff between the dedicated queue and the suspended caller.
///
/// Split out rather than inlined because three parties race for the same
/// continuation — the body, the deadline, and (through `mayRunBody`) the
/// decision not to start work at all — and every one of them must be able
/// to lose without resuming a continuation twice or leaking it.
private final class Waiter<T: Sendable>: Sendable {

    private struct State: Sendable {
        var continuation: CheckedContinuation<T?, Never>?
        var expired = false
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    func arm(_ continuation: CheckedContinuation<T?, Never>) {
        state.withLock { $0.continuation = continuation }
    }

    /// `true` while the caller is still waiting. `false` once the deadline
    /// has fired, which is the signal NOT to perform the side effect.
    func mayRunBody() -> Bool {
        state.withLock { !$0.expired }
    }

    /// Hands `value` to the caller. `false` when the caller already gave
    /// up, in which case the value is the late arrival the caller must
    /// dispose of.
    func deliver(_ value: T) -> Bool {
        let continuation = state.withLock { current -> CheckedContinuation<T?, Never>? in
            let pending = current.continuation
            current.continuation = nil
            return pending
        }
        continuation?.resume(returning: value)
        return continuation != nil
    }

    /// Resumes the caller with `nil`. `true` when the deadline actually
    /// won the race; `false` when the body had already delivered.
    func expire() -> Bool {
        let continuation = state.withLock { current -> CheckedContinuation<T?, Never>? in
            guard let pending = current.continuation else { return nil }
            current.continuation = nil
            current.expired = true
            return pending
        }
        continuation?.resume(returning: nil)
        return continuation != nil
    }
}
