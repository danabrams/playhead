// ShadowRetryObserver.swift
// bd-3bz (Phase 4): watches `canUseFoundationModels` on the capability
// publisher for false→true transitions and, after a 60s-stable-true debounce,
// drains any `analysis_sessions` rows flagged with `needsShadowRetry = 1`.
//
// Design (locked by dabrams 2026-04-07):
//   • The debounce lives on the publisher, not per-episode timers. A single
//     observer task subscribes to `CapabilitiesProviding.capabilityUpdates()`
//     and keeps a rolling "last time canUseFoundationModels became true"
//     marker. Any transition back to false cancels the pending drain.
//   • The drain calls `AdDetectionService.retryShadowFMPhaseForSession` for
//     each flagged session. That entry point is re-entrant against a session
//     whose transcription and coarse phases are already complete — it only
//     re-runs the shadow FM phase and never touches `AnalysisCoordinator`
//     state.
//   • Never retroactively marks pre-existing `.complete` sessions. The
//     observer only drains sessions flagged by `runShadowFMPhase`'s bail
//     path, which stamps the flag at the moment of the bail.
//   • Errors inside the drain are swallowed: shadow telemetry is
//     observation-only and must never destabilize playback.
//
// Concurrency (H1 + H2 fix, cycle 2):
//   • The observer loop merges TWO sources via a `withTaskGroup`:
//       1. `capabilityUpdates()` — capability snapshots.
//       2. A local `wakeStream` AsyncStream<WakeReason> that the observer
//          owns. `wake()` yields `.work`, `stop()` yields `.shutdown`.
//   • On `.shutdown` the loop cancels the task group and returns. On
//     `.work` it triggers an immediate drain (the `BackfillJobRunner`
//     marker path can wake the observer when capability is already true,
//     fixing the cycle-2 bug where stable-true sessions never re-drained).
//   • A sentinel flag `loopDidExit` is set inside the loop's `defer` so
//     `withTestRuntime` callers can assert clean termination.
//   • `stop()` awaits `loopTask.value` so callers can rely on full teardown
//     having completed before they construct a new runtime.
//
// Cancellation gotcha (the cycle-2 bug):
//   • The previous implementation parked in `for await snapshot in stream`
//     and trusted `Task.isCancelled` to break the loop. AsyncStream
//     iteration does NOT auto-break on cancellation — it ignores it and
//     keeps awaiting the next yield. The owning task therefore leaked
//     across runtime teardown. The fix is the explicit `.shutdown` wake
//     reason: cancelling no longer matters because we tell the loop to
//     exit via the wake stream.

import Foundation
import OSLog

/// Async clock abstraction for the debounce timer. Production uses
/// `Task.sleep(nanoseconds:)`; tests inject a fake clock to advance virtual
/// time deterministically.
protocol ShadowRetryClock: Sendable {
    /// Sleep for `seconds` of wall-clock time, honoring task cancellation.
    func sleep(seconds: Double) async throws
}

struct SystemShadowRetryClock: ShadowRetryClock {
    func sleep(seconds: Double) async throws {
        let ns = UInt64(seconds * 1_000_000_000)
        try await Task.sleep(nanoseconds: ns)
    }
}

/// Minimal view of `AdDetectionService` needed by the observer, so tests can
/// stub the drain without constructing a full detection pipeline.
protocol ShadowRetryDraining: Sendable {
    @discardableResult
    func retryShadowFMPhaseForSession(sessionId: String) async -> Bool
}

extension AdDetectionService: ShadowRetryDraining {}

/// Minimal view of `AnalysisStore` needed by the observer. Named distinctly
/// from the store's own `fetchSessionsNeedingShadowRetry` so the actor can
/// satisfy the protocol without self-recursion.
protocol ShadowRetryStoreReader: Sendable {
    func loadSessionsNeedingShadowRetry() async throws -> [AnalysisSession]
}

extension AnalysisStore: ShadowRetryStoreReader {
    func loadSessionsNeedingShadowRetry() async throws -> [AnalysisSession] {
        try fetchSessionsNeedingShadowRetry()
    }
}

actor ShadowRetryObserver {
    private let logger = Logger(subsystem: "com.playhead", category: "ShadowRetryObserver")

    /// Wake-stream event types. `.work` means "another session was just
    /// flagged, drain again"; `.shutdown` means "stop the loop and return".
    enum WakeReason: Sendable {
        case work
        case shutdown
    }

    /// Required continuous true-duration before a drain is permitted.
    nonisolated let debounceSeconds: Double

    private let capabilities: CapabilitiesProviding
    private let store: ShadowRetryStoreReader
    private let drainer: ShadowRetryDraining
    private let clock: ShadowRetryClock

    private var loopTask: Task<Void, Never>?
    private var drainTask: Task<Void, Never>?

    /// H2: wake stream owned by the observer. Coalesces rapid `.work`
    /// signals via `.bufferingNewest(1)` so a burst of session-marker
    /// invocations turns into a single drain pass.
    private var wakeContinuation: AsyncStream<WakeReason>.Continuation?

    /// H1 sentinel: flipped `true` inside the loop's `defer` block when the
    /// observer task exits cleanly. Tests assert this after `stop()` to pin
    /// "the loop actually returned, not just got cancelled and parked".
    private var loopDidExit = false
    /// True once `stop()` has been called. Subsequent `wake()` / `start()`
    /// invocations no-op so callers can't accidentally restart a torn-down
    /// observer.
    private var didShutdown = false

    init(
        capabilities: CapabilitiesProviding,
        store: ShadowRetryStoreReader,
        drainer: ShadowRetryDraining,
        clock: ShadowRetryClock = SystemShadowRetryClock(),
        debounceSeconds: Double = 60
    ) {
        self.capabilities = capabilities
        self.store = store
        self.drainer = drainer
        self.clock = clock
        self.debounceSeconds = debounceSeconds
    }

    /// Test sentinel hook for C7. `withTestRuntime` asserts this is `true`
    /// after `runtime.shutdown()` returns to confirm the loop actually
    /// terminated rather than getting stranded behind an AsyncStream park.
    func testHasExitedLoop() -> Bool {
        loopDidExit
    }

    /// Cycle 4 H3: test sentinel for polling "has `start()` actually
    /// created the loop task yet?". Returns true iff `loopTask` is
    /// non-nil AND the loop hasn't exited. The runtime's observer
    /// startup chain is multi-hop async (migrate → startup task → loop
    /// task) so tests that want to assert a clean teardown must first
    /// wait for the loop to actually be running.
    func testIsLoopRunning() -> Bool {
        loopTask != nil && !loopDidExit
    }

    /// Starts the observer loop. Safe to call once; subsequent calls no-op.
    /// Calling `start()` after `stop()` is rejected (the wake stream has
    /// been finished and the sentinel is permanent).
    func start() {
        guard !didShutdown else {
            logger.debug("Shadow retry observer: start() ignored after shutdown")
            return
        }
        guard loopTask == nil else { return }
        let (wakeStream, wakeCont) = AsyncStream<WakeReason>.makeStream(
            bufferingPolicy: .bufferingNewest(1)
        )
        wakeContinuation = wakeCont
        loopTask = Task { [weak self] in
            await self?.runObserverLoop(wakeStream: wakeStream)
        }
    }

    /// Cancels the observer loop and any in-flight debounce/drain tasks.
    /// `stop()` is idempotent and safe to call from multiple tasks.
    /// Awaits the loop task to completion before returning so callers can
    /// rely on full teardown when this returns.
    func stop() async {
        if didShutdown {
            // Even on a re-entry, await the loop task to make sure
            // concurrent stop() callers all see termination.
            if let task = loopTask {
                _ = await task.value
            }
            return
        }
        didShutdown = true

        // Yield .shutdown so the loop's TaskGroup can break out of the
        // wakeStream branch immediately. The continuation may be nil if
        // start() was never called; that's fine — the loop never ran.
        wakeContinuation?.yield(.shutdown)
        wakeContinuation?.finish()
        wakeContinuation = nil

        // Cancel the drain timer (if any) and the loop task. Cancellation
        // is belt-and-suspenders; the .shutdown wake is the primary exit
        // path.
        drainTask?.cancel()
        drainTask = nil

        if let task = loopTask {
            task.cancel()
            _ = await task.value
        }
        loopTask = nil
    }

    /// H2: external wake-up. The shadow-skip marker closure in
    /// `PlayheadRuntime` calls this immediately after marking a session,
    /// so a stable-true capability still drains promptly without waiting
    /// for the next capability snapshot. Coalesced via the
    /// `.bufferingNewest(1)` policy: 50 rapid wakes turn into one drain.
    /// No-op after `stop()`.
    func wake() {
        guard !didShutdown else { return }
        wakeContinuation?.yield(.work)
    }

    // MARK: - Observer loop

    /// H1: the observer loop now merges capability updates and the wake
    /// stream into a single `AsyncStream<LoopEvent>`. Two child tasks
    /// inside a `TaskGroup` pump their respective sources into the merged
    /// stream's continuation. On `.shutdown` the loop cancels the group
    /// and returns, so the previous AsyncStream cancellation gotcha (where
    /// `for await` ignored `Task.isCancelled`) is impossible: the loop's
    /// exit condition is now an explicit event, not cancellation.
    private func runObserverLoop(wakeStream: AsyncStream<WakeReason>) async {
        defer { loopDidExit = true }

        let capabilityStream = await capabilities.capabilityUpdates()

        let (mergedStream, mergedCont) = AsyncStream<LoopEvent>.makeStream()

        await withTaskGroup(of: Void.self) { group in
            // Pump capability snapshots into the merged stream.
            group.addTask {
                for await snapshot in capabilityStream {
                    mergedCont.yield(.capability(snapshot))
                    if Task.isCancelled { break }
                }
            }
            // Pump wake reasons into the merged stream.
            group.addTask {
                for await reason in wakeStream {
                    mergedCont.yield(.wake(reason))
                    if Task.isCancelled { break }
                }
                // The wake stream finishing is also a terminal signal —
                // close the merged stream so the consumer loop below
                // exits even if the capability pump is still parked.
                mergedCont.finish()
            }
            // Consume merged events. This is the actor-isolated branch.
            group.addTask { [weak self] in
                guard let self else { return }
                await self.consumeMergedEvents(mergedStream)
                mergedCont.finish()
            }

            // Wait for the consumer to return (it returns on `.shutdown`,
            // wake-stream finish, or capability-stream finish), then
            // cancel the pump tasks so they unblock from their parked
            // `for await`.
            await group.next()
            group.cancelAll()
        }
    }

    /// Consumer half of the merged-event loop. Lives on the actor so it
    /// can mutate `drainTask` and call `scheduleDrain()` / `drainNow()`.
    private func consumeMergedEvents(_ stream: AsyncStream<LoopEvent>) async {
        var lastSeen: Bool? = nil
        for await event in stream {
            switch event {
            case .capability(let snapshot):
                let current = snapshot.canUseFoundationModels
                if current {
                    if lastSeen != true {
                        scheduleDrain()
                    }
                } else {
                    drainTask?.cancel()
                    drainTask = nil
                }
                lastSeen = current

            case .wake(let reason):
                switch reason {
                case .shutdown:
                    logger.debug("Shadow retry observer: shutdown wake received, exiting loop")
                    return
                case .work:
                    // bd-3bz H2: a session was just marked. If the
                    // capability is currently true, drain immediately
                    // (bypassing the debounce, since the
                    // false→true→stable transition has already happened).
                    // If false, no-op — the next false→true capability
                    // transition will schedule its own debounced drain.
                    let snap = await capabilities.currentSnapshot
                    if snap.canUseFoundationModels {
                        await drainNow()
                    }
                }
            }
        }
    }

    /// Internal merged-stream event type (used by the observer loop).
    private enum LoopEvent: Sendable {
        case capability(CapabilitySnapshot)
        case wake(WakeReason)
    }

    private func scheduleDrain() {
        drainTask?.cancel()
        let clock = self.clock
        let debounce = self.debounceSeconds
        drainTask = Task { [weak self] in
            do {
                try await clock.sleep(seconds: debounce)
            } catch {
                return  // cancelled
            }
            guard let self else { return }
            await self.drainNow()
        }
    }

    // MARK: - Drain

    /// Drains the retry queue immediately. Public for test harnesses that
    /// bypass the debounce; production callers go through the observer loop.
    func drainNow() async {
        if Task.isCancelled { return }
        // Re-check the capability at drain time: a flip back to false during
        // the debounce window that arrived via a race (or an eager test
        // call) should still bail cleanly. The per-session retry entry point
        // also re-checks, so this is belt-and-suspenders.
        let snapshot = await capabilities.currentSnapshot
        guard snapshot.canUseFoundationModels else {
            logger.debug("Shadow retry drain skipped: capability flipped false at drain time")
            return
        }

        let flagged: [AnalysisSession]
        do {
            flagged = try await store.loadSessionsNeedingShadowRetry()
        } catch {
            logger.warning("Shadow retry drain: failed to fetch flagged sessions: \(error.localizedDescription)")
            return
        }

        guard !flagged.isEmpty else {
            logger.debug("Shadow retry drain: no flagged sessions")
            return
        }

        logger.info("Shadow retry drain: \(flagged.count) flagged sessions")
        for session in flagged {
            // Rev1-M2: honor cooperative cancellation between sessions so
            // a `stop()` mid-drain interrupts the loop instead of running
            // every flagged session to completion.
            do {
                try Task.checkCancellation()
            } catch {
                logger.debug("Shadow retry drain: cancelled mid-loop")
                return
            }
            _ = await drainer.retryShadowFMPhaseForSession(sessionId: session.id)
        }
    }
}
