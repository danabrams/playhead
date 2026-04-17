// LanePreemptionCoordinator.swift
// playhead-01t8: hard user preemption at checkpoint boundaries.
//
// Owns the runtime registry of active analysis jobs per scheduler lane and
// implements the `LanePreemptionHandler` hook wired by
// `AnalysisWorkScheduler.setLanePreemptionHandler(_:)`.
//
// Protocol (see bead spec):
//   1. A running job calls `register(jobId:lane:lease:)` at start-of-work and
//      receives a `PreemptionSignal` it polls at every safe checkpoint
//      boundary (post-shard in FeatureExtraction,
//      FeatureExtractionCheckpoint persist, post-TranscriptChunk).
//   2. When `AnalysisWorkScheduler` is about to admit a Now-lane job, the
//      scheduler invokes `preemptLowerLanes(for:)`. The coordinator flips
//      `preemptionRequested` on every signal in a strictly lower lane.
//   3. The running job observes `signal.isPreemptionRequested()` at its next
//      safe point, finalizes the current unit (checkpoint write), releases
//      the lease via `AnalysisCoordinator.releaseLease(event: .preempted,
//      cause: .userPreempted)`, and calls `acknowledge(jobId:)` on the
//      coordinator so the scheduler's `awaitLowerLaneAck` wait resolves.
//   4. The scheduler observes the ack (or the 5 s deadline), then admits
//      the Now-lane job.
//
// Acceptance constants live alongside the implementation so tests and
// production read from the same source of truth:
//   - Promotion latency (user-tap-to-Now-admission-request): 100 ms budget.
//   - Preemption latency (admission-request-to-lower-lane-paused): 5 s
//     HARD GATE per the bead spec.
//
// Concurrency: `LanePreemptionCoordinator` is an actor to serialize flag
// flips and registration mutations. `PreemptionSignal` is backed by its own
// tiny actor so the running job can poll its flag from any executor
// (`FeatureExtractionService`, `TranscriptEngineService`) without
// re-entering this coordinator on every shard boundary.

import Foundation
import OSLog

// MARK: - PreemptionContext

/// Capability bundle handed to downstream services so they can poll for
/// preemption at their safe-point boundaries and acknowledge promptly.
///
/// The runner registers with the coordinator, receives a
/// `PreemptionSignal`, and passes this context into
/// `FeatureExtractionService.extractAndPersist(...)` and
/// `TranscriptEngineService.startTranscription(...)`. Those services
/// poll `signal.isPreemptionRequested()` at their enumerated safe
/// points; when the flag flips they call
/// `coordinator.acknowledge(jobId:)` and return early.
///
/// Passed as a value so it crosses actor boundaries cleanly. The
/// referenced actors are inherently `Sendable`.
struct PreemptionContext: Sendable {
    let jobId: String
    let signal: PreemptionSignal
    let coordinator: LanePreemptionCoordinator

    /// Polls the signal. Wrapper for readability at call sites.
    func isPreemptionRequested() async -> Bool {
        await signal.isPreemptionRequested()
    }

    /// Mark the running job as paused at a safe point.
    /// Downstream services call this *once* immediately before
    /// returning early from their shard/chunk loop.
    func acknowledge() async {
        await coordinator.acknowledge(jobId: jobId)
    }
}

// MARK: - PreemptionSignal

/// Thread-safe signal polled by a running analysis job at its safe
/// checkpoint boundaries.
///
/// The signal is owned by `LanePreemptionCoordinator`; jobs receive one on
/// `register(jobId:lane:lease:)` and poll it from whichever executor they
/// happen to be on (feature-extractor actor, transcript-engine actor, etc.).
///
/// Reads are cheap (single atomic flag read inside the actor) and the
/// actor is a dedicated one-variable actor to avoid queueing behind other
/// work on the coordinator.
actor PreemptionSignal {
    /// Whether a preemption has been requested. Flipped by the
    /// coordinator on `preemptLowerLanes(for:)`; never flipped back to
    /// false — a fresh signal is minted for each new job registration.
    private(set) var preemptionRequested: Bool = false

    /// Timestamp (monotonic) at which the signal was flipped. Exposed
    /// for latency instrumentation. Nil until the signal is flipped.
    private(set) var requestedAt: ContinuousClock.Instant?

    /// Opportunistic cause tag. Always `.userPreempted` for Phase 1 —
    /// the hook site is exclusively user-tapped Play / Download
    /// promotions. Reserved for later phases that may preempt for
    /// other reasons (thermal-triggered demotion, background-window
    /// close).
    private(set) var cause: InternalMissCause = .userPreempted

    /// Poll entry point for safe checkpoint boundaries. Cheap — an
    /// actor hop but no I/O. Safe to call at every shard / chunk /
    /// checkpoint-persist boundary without affecting steady-state
    /// throughput.
    func isPreemptionRequested() -> Bool {
        preemptionRequested
    }

    /// Flip the flag. Called only by `LanePreemptionCoordinator`. The
    /// `cause` parameter is carried forward to the WorkJournal entry
    /// emitted by the running job when it releases its lease.
    func request(cause: InternalMissCause = .userPreempted,
                 at instant: ContinuousClock.Instant = .now) {
        // Idempotent: a second request leaves the earlier timestamp
        // in place so latency measurements reflect the first demand.
        guard !preemptionRequested else { return }
        preemptionRequested = true
        requestedAt = instant
        self.cause = cause
    }
}

// MARK: - AckWaiter

/// A one-shot ack resolver used inside `LanePreemptionCoordinator.awaitAck`.
///
/// Wraps a `CheckedContinuation<Bool, Never>` with an idempotent
/// `resolve(_:)`. Both the ack path (`resumeAckWaiters`) and the
/// timeout path (`resolveWaiterOnTimeout`) run on the coordinator
/// actor, so serialization is provided by actor isolation — we only
/// need the `resolved` flag to prevent the timeout Task from
/// double-resuming a waiter the ack already resolved.
///
/// Identified by object identity (`===`) so the timeout path can
/// locate the specific waiter to remove from `ackWaiters` without
/// disturbing other waiters on the same jobId.
///
/// The class must be non-`Sendable` because `CheckedContinuation` is
/// not `Sendable` (it carries captured continuation state). The
/// `unchecked` conformance below is safe because every resolve call
/// happens on the coordinator actor (the timeout Task's resolution
/// goes through `resolveWaiterOnTimeout`, which is actor-isolated).
final class AckWaiter: @unchecked Sendable {
    private let continuation: CheckedContinuation<Bool, Never>
    private var resolved: Bool = false

    init(continuation: CheckedContinuation<Bool, Never>) {
        self.continuation = continuation
    }

    /// Resolve the underlying continuation exactly once. Subsequent
    /// calls are no-ops. MUST be called from within the coordinator
    /// actor's isolation domain.
    func resolve(_ value: Bool) {
        guard !resolved else { return }
        resolved = true
        continuation.resume(returning: value)
    }
}

// MARK: - Registration

/// A single active registration held by the coordinator. Carries the
/// `EpisodeExecutionLease` so the coordinator can match running jobs to
/// their WorkJournal generation during diagnostics.
struct LanePreemptionRegistration: Sendable, Equatable {
    let jobId: String
    let lane: AnalysisWorkScheduler.SchedulerLane
    let lease: EpisodeExecutionLease
    let signal: PreemptionSignal
    /// Wall-clock time at which the job registered. Used for FIFO
    /// ordering within a lane (see `activeJobs(in:)`).
    let registeredAt: ContinuousClock.Instant

    static func == (lhs: LanePreemptionRegistration, rhs: LanePreemptionRegistration) -> Bool {
        lhs.jobId == rhs.jobId
    }
}

// MARK: - LanePreemptionCoordinator

/// Runtime registry + dispatcher for hard user preemption at
/// checkpoint boundaries.
///
/// The coordinator is installed on `AnalysisWorkScheduler` via
/// `setLanePreemptionHandler(_:)`. Each running analysis job registers
/// with its lane and its lease and receives a `PreemptionSignal`. When
/// the scheduler is about to admit a Now-lane job, it calls
/// `preemptLowerLanes(for: .now)` on the coordinator, which flips the
/// signals on every registered Soon/Background job. Jobs observe the
/// flag at their next safe checkpoint, finalize unit state, release
/// the lease with `event=.preempted, cause=.userPreempted`, and call
/// `acknowledge(jobId:)`. The scheduler (or a test) can then call
/// `awaitLowerLaneAck(after:within:)` to gate on all preempted jobs
/// having pushed through the safe-point → release sequence.
actor LanePreemptionCoordinator: LanePreemptionHandler {
    /// Hard upper bound on the time between a Now-lane admission
    /// request and every running Soon/Background job reaching its
    /// next safe point and releasing its lease.
    ///
    /// The bead's HARD GATE. Enforced by
    /// `LanePreemptionCoordinatorTests.preemptionLatencyHardGate`.
    static let preemptionLatencyBudget: Duration = .seconds(5)

    /// Soft upper bound on the time between a user tap (Play /
    /// Download) and the scheduler's admission request landing on
    /// the coordinator. This is the pre-admission promotion latency,
    /// independent of the preemption-latency budget above — they
    /// stack: 100 ms to admit + up to 5 s to pause lower lanes.
    static let promotionLatencyBudget: Duration = .milliseconds(100)

    private let logger = Logger(subsystem: "com.playhead", category: "LanePreemption")

    /// Live registry keyed by jobId. A job is registered from
    /// `didStart` (effectively) until it either runs to completion
    /// (`unregister`) or is preempted and releases its lease
    /// (`acknowledge` → `unregister` internally).
    private var registrations: [String: LanePreemptionRegistration] = [:]

    /// Continuations awaiting ack of preemption. Keyed by the jobId
    /// of the preempted job. Resolved from `acknowledge(jobId:)` (with
    /// `true`) or from the timeout task in `awaitAck` (with `false`).
    private var ackWaiters: [String: [AckWaiter]] = [:]

    /// Counts every successful `preemptLowerLanes(for: .now)` call.
    /// Exposed for observability / tests.
    private(set) var preemptionRequestCount: Int = 0

    init() {}

    // MARK: - Registration API

    /// Register a running job with the coordinator. Returns a signal
    /// the job polls at safe checkpoint boundaries.
    ///
    /// A re-registration of the same `jobId` (which should not happen
    /// under normal scheduler operation) replaces the previous entry
    /// and drops any pending ack waiters for that id.
    @discardableResult
    func register(
        jobId: String,
        lane: AnalysisWorkScheduler.SchedulerLane,
        lease: EpisodeExecutionLease,
        registeredAt: ContinuousClock.Instant = .now
    ) -> PreemptionSignal {
        let signal = PreemptionSignal()
        let registration = LanePreemptionRegistration(
            jobId: jobId,
            lane: lane,
            lease: lease,
            signal: signal,
            registeredAt: registeredAt
        )
        if registrations[jobId] != nil {
            logger.warning("re-registering jobId=\(jobId, privacy: .public) — dropping prior registration")
            resumeAckWaiters(jobId: jobId)
        }
        registrations[jobId] = registration
        return signal
    }

    /// Deregister a running job without acknowledging any pending
    /// preemption. Called on successful completion, cancellation, or
    /// any failure path that is NOT a user preemption.
    ///
    /// If a preemption was pending, callers must prefer
    /// `acknowledge(jobId:)` over `unregister(jobId:)` so that
    /// `awaitLowerLaneAck` wakes.
    func unregister(jobId: String) {
        guard registrations.removeValue(forKey: jobId) != nil else {
            return
        }
        // Intentional: do NOT resume ackWaiters here. A race where a
        // job finishes naturally after a preempt request flipped its
        // flag is legitimate (the job ran to its next safe point and
        // happened to complete on the same boundary). Callers should
        // always prefer `acknowledge` over `unregister` when a
        // preemption is outstanding; if they don't, the waiter times
        // out via `awaitLowerLaneAck`'s deadline.
    }

    /// Acknowledge that a running job has observed a preemption
    /// request at a safe point, released its lease, and is exiting.
    /// Resolves every outstanding `awaitLowerLaneAck` that was
    /// waiting on this job.
    ///
    /// This is the call that turns the scheduler's 5 s budget into a
    /// deterministic signal. Callers MUST invoke this after the lease
    /// release completes so the WorkJournal row is durable before the
    /// scheduler observes the ack.
    func acknowledge(jobId: String) {
        guard registrations.removeValue(forKey: jobId) != nil else {
            return
        }
        resumeAckWaiters(jobId: jobId)
    }

    // MARK: - LanePreemptionHandler

    /// Flip the preemption signal on every registered job in a
    /// strictly lower lane than `incoming`. Nominally this is called
    /// with `incoming = .now`, which preempts `.soon` and
    /// `.background`.
    ///
    /// This method returns as soon as every lower-lane signal is
    /// flipped — it does NOT wait for the targeted jobs to reach
    /// their next safe point. The scheduler gets back to admitting
    /// the Now-lane job immediately; the 5 s HARD GATE is enforced
    /// by `awaitLowerLaneAck` only in tests / long-lived background
    /// processes that need to bound the wait.
    nonisolated func preemptLowerLanes(for incoming: AnalysisWorkScheduler.SchedulerLane) async {
        await requestPreemption(for: incoming)
    }

    private func requestPreemption(
        for incoming: AnalysisWorkScheduler.SchedulerLane,
        cause: InternalMissCause = .userPreempted,
        at instant: ContinuousClock.Instant = .now
    ) async {
        preemptionRequestCount += 1
        let targets = registrations.values.filter { $0.lane.isStrictlyLower(than: incoming) }
        guard !targets.isEmpty else { return }
        logger.info("preempting \(targets.count) job(s) in lanes lower than \(String(describing: incoming), privacy: .public) for cause=\(cause.rawValue, privacy: .public)")
        for registration in targets {
            await registration.signal.request(cause: cause, at: instant)
        }
    }

    // MARK: - Observation

    /// Wait until every lane strictly lower than `incoming` is
    /// either empty or every registered job has acknowledged a
    /// preemption. Returns `true` when the drain completes within
    /// `timeout`, `false` otherwise.
    ///
    /// Used primarily by tests that assert the 5 s HARD GATE. The
    /// production scheduler does not block on this — it calls
    /// `preemptLowerLanes` and moves on; the owners release their
    /// leases asynchronously and the scheduler reacquires them
    /// lazily on its next loop iteration. See the bead spec's
    /// "latency is a bound, not a wait" note.
    func awaitLowerLaneAck(
        after incoming: AnalysisWorkScheduler.SchedulerLane,
        within timeout: Duration = LanePreemptionCoordinator.preemptionLatencyBudget
    ) async -> Bool {
        let lowerJobs = registrations.values
            .filter { $0.lane.isStrictlyLower(than: incoming) }
            .map(\.jobId)
        if lowerJobs.isEmpty { return true }

        return await withTaskGroup(of: Bool.self, returning: Bool.self) { group in
            for jobId in lowerJobs {
                group.addTask { [weak self] in
                    guard let self else { return false }
                    return await self.awaitAck(jobId: jobId, timeout: timeout)
                }
            }
            var all = true
            for await result in group {
                all = all && result
            }
            return all
        }
    }

    /// Wait for `acknowledge(jobId:)` to be called (or the timeout to
    /// elapse). Returns `true` on ack, `false` on timeout. If the
    /// job is already unregistered (e.g. raced to completion before
    /// this call lands) returns `true` immediately.
    ///
    /// Implementation note: we avoid `withTaskGroup` + race patterns
    /// because cancelling a child task that is suspended on a
    /// `CheckedContinuation` does not resume that continuation — the
    /// group would then hang forever waiting for the cancelled child.
    /// Instead we arm a separate `Task.sleep` to race against a
    /// cancellable continuation the acknowledger can resume.
    func awaitAck(jobId: String, timeout: Duration) async -> Bool {
        guard registrations[jobId] != nil else { return true }

        // Install a continuation that can be resumed by EITHER
        // `acknowledge(jobId:)` OR the timeout task. The `once`
        // wrapper guarantees exactly one resume regardless of which
        // path wins the race.
        return await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
            let waiter = AckWaiter(continuation: continuation)
            ackWaiters[jobId, default: []].append(waiter)

            // Arm the timeout. If the ack wins, `waiter.resolve(true)`
            // from `resumeAckWaiters` runs first and the timeout task's
            // attempt to resolve is a no-op.
            Task { [weak self, weak waiter] in
                try? await Task.sleep(for: timeout)
                guard let waiter else { return }
                await self?.resolveWaiterOnTimeout(waiter: waiter, jobId: jobId)
            }
        }
    }

    /// Invoked when the timeout task fires. If the waiter is still
    /// unresolved, remove it from `ackWaiters` and resume with `false`.
    private func resolveWaiterOnTimeout(waiter: AckWaiter, jobId: String) {
        guard var list = ackWaiters[jobId] else { return }
        if let idx = list.firstIndex(where: { $0 === waiter }) {
            list.remove(at: idx)
            if list.isEmpty {
                ackWaiters.removeValue(forKey: jobId)
            } else {
                ackWaiters[jobId] = list
            }
            waiter.resolve(false)
        }
    }

    private func resumeAckWaiters(jobId: String) {
        guard let waiters = ackWaiters.removeValue(forKey: jobId) else { return }
        for waiter in waiters {
            waiter.resolve(true)
        }
    }

    // MARK: - Introspection (tests + diagnostics only)

    /// Currently-registered job IDs in the given lane, ordered by
    /// registration time (FIFO). Used by tests to assert the
    /// lane-FIFO invariant across a preempt/resume cycle.
    func activeJobs(in lane: AnalysisWorkScheduler.SchedulerLane) -> [String] {
        registrations.values
            .filter { $0.lane == lane }
            .sorted { $0.registeredAt < $1.registeredAt }
            .map(\.jobId)
    }

    /// Total registered jobs across all lanes. Cheap snapshot for
    /// tests / diagnostics.
    func registeredCount() -> Int { registrations.count }
}

// MARK: - SchedulerLane ordering

extension AnalysisWorkScheduler.SchedulerLane {
    /// Strict total order for preemption: `.now > .soon > .background`.
    /// Used by `LanePreemptionCoordinator.preemptLowerLanes` to decide
    /// which lanes a newly-admitted `incoming` lane can demote.
    ///
    /// The ordering is hand-written rather than derived from a
    /// `Comparable` conformance because the lane enum is
    /// `CaseIterable` but not otherwise ordered, and keeping the
    /// ordering local to the preemption logic avoids accidentally
    /// leaking a lane ranking into UI copy (see the
    /// `SchedulerLaneUILintTests` prohibition).
    func isStrictlyLower(than other: AnalysisWorkScheduler.SchedulerLane) -> Bool {
        switch (self, other) {
        case (.background, .soon), (.background, .now), (.soon, .now):
            return true
        default:
            return false
        }
    }
}
