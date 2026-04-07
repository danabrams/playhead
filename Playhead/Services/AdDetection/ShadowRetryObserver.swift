// ShadowRetryObserver.swift
// bd-3bz (Phase 4): watches `canUseFoundationModels` on the capability
// publisher for false→true transitions and, after a 60s-stable-true debounce,
// drains any `analysis_sessions` rows flagged with `needs_shadow_retry = 1`.
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
// Concurrency:
//   • `ShadowRetryObserver` is an `actor` so the observer loop, drain
//     routine, and `stop()` cooperate without races. The observer loop runs
//     inside a `Task` owned by the actor itself.
//   • Cancelling the owning Task (e.g. during runtime teardown) cleanly
//     exits the AsyncStream iteration via `Task.isCancelled`.

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

    /// Required continuous true-duration before a drain is permitted.
    nonisolated let debounceSeconds: Double

    private let capabilities: CapabilitiesProviding
    private let store: ShadowRetryStoreReader
    private let drainer: ShadowRetryDraining
    private let clock: ShadowRetryClock

    private var loopTask: Task<Void, Never>?
    private var drainTask: Task<Void, Never>?

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

    /// Starts the observer loop. Safe to call once; subsequent calls no-op.
    func start() {
        guard loopTask == nil else { return }
        loopTask = Task { [weak self] in
            await self?.runObserverLoop()
        }
    }

    /// Cancels the observer loop and any in-flight debounce/drain tasks.
    func stop() {
        loopTask?.cancel()
        loopTask = nil
        drainTask?.cancel()
        drainTask = nil
    }

    // MARK: - Observer loop

    private func runObserverLoop() async {
        let stream = await capabilities.capabilityUpdates()
        var lastSeen: Bool? = nil
        for await snapshot in stream {
            if Task.isCancelled { break }
            let current = snapshot.canUseFoundationModels
            defer { lastSeen = current }

            // Only act on transitions (or the very first emission when
            // already `true` and sessions were flagged by a prior process
            // lifetime — we still want a drain, so treat "first true" as a
            // transition).
            if current {
                // Schedule a debounced drain. If one is already scheduled
                // from a prior transition, leave it alone — a true→true
                // emission (e.g. thermal change while FM stays usable)
                // should not reset the timer.
                if lastSeen != true {
                    scheduleDrain()
                }
            } else {
                // false (or transitioning to false): cancel any pending
                // drain so a false→true→false within the debounce window
                // does not fire a drain.
                drainTask?.cancel()
                drainTask = nil
            }
        }
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
            if Task.isCancelled { return }
            // Re-check between entries too: a large queue draining across
            // many seconds may straddle another capability flip. The inner
            // retry entry point re-checks its own guard and re-marks on
            // bail, so we don't need a bail-and-break here — just honor
            // cancellation.
            _ = await drainer.retryShadowFMPhaseForSession(sessionId: session.id)
        }
    }
}
