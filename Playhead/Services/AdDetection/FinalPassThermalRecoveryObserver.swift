// FinalPassThermalRecoveryObserver.swift
// playhead-l8dz: kicks the final-pass launch sweep when device conditions
// transition from "not runnable" to "runnable" (thermalState == .nominal &&
// isCharging && !isLowPowerMode). Without this observer, deferred
// final_pass_jobs only retry on the next cold launch; a phone that throttles
// once and recovers in-process can sit on a queue of `deferReason='thermalThrottled'`
// rows indefinitely (xcappdata 2026-05-02 reproduction).
//
// Design (mirrored intentionally from `ShadowRetryObserver`):
//   * Two streams merged via `withTaskGroup`: capability snapshots from
//     `CapabilitiesProviding.capabilityUpdates()` and a local `wakeStream`
//     that carries `.shutdown` from `stop()`. AsyncStream `for await` does
//     NOT auto-break on `Task.isCancelled`, so the explicit shutdown wake
//     is the loop's exit path; `.cancel()` is belt-and-suspenders.
//   * Runnable predicate matches `FinalPassRetranscriptionRunner.currentDeferReason`
//     EXACTLY (strict thermal == .nominal AND charging AND !LPM). This is
//     deliberately stricter than `CapabilitySnapshot.canRunDeferredWork`
//     (which allows .fair / .serious thresholds) — firing the sweep on a
//     state the runner will then refuse would just churn deferReason
//     stamps without making progress. The match is enforced by
//     `isRunnableNow()`, which re-probes via the SAME `chargeStateProvider`
//     and `capabilitySnapshotProvider` closures the runner reads from
//     (`PlayheadRuntime.swift` wires them through), closing the gap where
//     `CapabilitySnapshot.isCharging` would lag a fresh
//     `BatteryStateProviding` read by a few ms.
//   * Fires only on a confirmed false → true transition. The first
//     capability snapshot from the stream is treated as "establish prior",
//     not a transition; the launch sweep already runs once at startup.
//   * Cooldown (default 120s) suppresses thrash if the device oscillates
//     across the runnable boundary. `cooldownSeconds = 0` is supported
//     (and used by tests) to mean "no cooldown" — the outstanding-task
//     guard alone gates re-fires in that case. Comparison is strict `<`,
//     so an exact same-instant clock sample with cooldown 0 does NOT
//     skip; only the in-flight guard remains.
//   * The outstanding-task guard prevents overlapping sweeps if the
//     user happens to plug/unplug while a sweep is mid-drain.
//   * `kickSweep` is injected as a closure rather than calling the static
//     `runFinalPassBackfillForAllAssetsAtLaunch` directly. Tests can pass
//     a stub closure that just bumps a counter; production passes a
//     closure that calls into PlayheadRuntime.

import Foundation
import os
import OSLog

/// M1 (cycle 1 review): coordination flag shared between the launch
/// sweep dispatch in `PlayheadRuntime` and the recovery observer's
/// kick closure. The observer's internal `sweepTask != nil` guard
/// prevents the OBSERVER from overlapping itself, but cannot see the
/// launch sweep started by `init`. Without this shared flag, a thermal
/// recovery transition that lands while the launch sweep is still
/// mid-drain spawns a second concurrent sweep over the same asset set —
/// wasting transcribe slots and risking duplicate
/// `final_pass_jobs` work. Both call sites MUST `tryAcquire()` before
/// dispatching their sweep and `release()` in a `defer` after the
/// sweep returns.
///
/// **Suppression is by design**: when the observer's `kickSweep`
/// closure fails `tryAcquire()` because the launch sweep is mid-drain,
/// the kick returns silently. The observer has already stamped
/// `priorRunnable = true` before invoking the kick, so subsequent
/// steady-state runnable emissions don't re-fire — but no work is
/// dropped, because the launch sweep will process the same asset set
/// the observer would have. A subsequent throttled→runnable transition
/// on the observer side will dispatch normally. Don't treat suppressed
/// kicks as bugs to chase.
final class FinalPassSweepInFlightFlag: Sendable {
    private let inFlight = OSAllocatedUnfairLock<Bool>(initialState: false)

    init() {}

    /// Returns `true` if the caller acquired the flag (must release).
    /// Returns `false` if another sweep is already in flight (caller
    /// MUST NOT release; MUST NOT proceed with the sweep).
    func tryAcquire() -> Bool {
        inFlight.withLock { current in
            guard !current else { return false }
            current = true
            return true
        }
    }

    func release() {
        inFlight.withLock { $0 = false }
    }

    /// Test-only inspection. Not used in production code paths.
    func testIsInFlight() -> Bool {
        inFlight.withLock { $0 }
    }
}

/// **Lifecycle:** construct once via `PlayheadRuntime` at app launch; never
/// recreate within a process. The observer holds onto its `kickSweep`
/// closure (which captures the runtime's analysis services) and a
/// `loopTask` whose cleanup requires `stop()` to be awaited. Recreating
/// would orphan the prior loop unless the caller awaited `stop()` on the
/// previous instance first — which `PlayheadRuntime` does for shutdown
/// but not for any other path.
actor FinalPassThermalRecoveryObserver {
    private let logger = Logger(subsystem: "com.playhead", category: "FinalPassRecoveryObserver")

    /// Wake-stream event types. `.shutdown` is the only reason today;
    /// modeling as an enum keeps the structure parallel to
    /// `ShadowRetryObserver` and leaves room for future external pokes.
    enum WakeReason: Sendable {
        case shutdown
    }

    private let capabilities: CapabilitiesProviding
    /// Fresh-probe of the device's charge state, called from
    /// `isRunnableNow` before the observer commits to a transition.
    /// `CapabilitySnapshot.isCharging` only refreshes on
    /// `batteryStateDidChangeNotification`, while the runner reads a
    /// fresher reading via `BatteryStateProviding.currentBatteryState()`.
    /// To keep the observer's gate from firing on a state the runner
    /// would refuse, callers MUST inject the same probe the runner uses.
    private let chargeStateProvider: @Sendable () async -> Bool
    /// Fresh-probe of the device's capability snapshot. Same rationale
    /// as `chargeStateProvider`: the snapshot stream may be a few ms
    /// stale relative to a fresh probe via `CapabilitiesProviding.currentSnapshot`.
    private let capabilitySnapshotProvider: @Sendable () async -> CapabilitySnapshot
    private let kickSweep: @Sendable () async -> Void
    private let cooldownSeconds: Double
    private let clock: @Sendable () -> Date

    private var loopTask: Task<Void, Never>?
    private var sweepTask: Task<Void, Never>?
    private var wakeContinuation: AsyncStream<WakeReason>.Continuation?

    /// Runnable state on the previously-observed snapshot. `nil` until the
    /// first snapshot lands so the very first capability emission can't
    /// trigger a spurious sweep.
    private var priorRunnable: Bool?

    /// Wall-clock time of the last sweep dispatch. Used by the cooldown
    /// guard.
    private var lastSweepAt: Date?

    /// Test sentinel: flipped `true` inside the loop's `defer` when the
    /// observer task exits cleanly.
    private var loopDidExit = false
    /// True once `stop()` has been called. `start()` no-ops afterwards.
    private var didShutdown = false

    init(
        capabilities: CapabilitiesProviding,
        capabilitySnapshotProvider: (@Sendable () async -> CapabilitySnapshot)? = nil,
        chargeStateProvider: (@Sendable () async -> Bool)? = nil,
        cooldownSeconds: Double = 120,
        clock: @escaping @Sendable () -> Date = { .now },
        kickSweep: @escaping @Sendable () async -> Void
    ) {
        self.capabilities = capabilities
        // Defaults read from the same `capabilities` provider so unit
        // tests that already drive a `MockCapabilitiesProvider` don't
        // need to wire two separate sources. Production wiring MUST
        // override both with the same closures the runner uses.
        self.capabilitySnapshotProvider = capabilitySnapshotProvider
            ?? { await capabilities.currentSnapshot }
        self.chargeStateProvider = chargeStateProvider
            ?? { await capabilities.currentSnapshot.isCharging }
        self.cooldownSeconds = cooldownSeconds
        self.clock = clock
        self.kickSweep = kickSweep
    }

    // MARK: - Lifecycle

    /// Starts the observer loop. Safe to call once; subsequent calls no-op.
    /// Calling `start()` after `stop()` is rejected.
    func start() {
        guard !didShutdown else {
            logger.debug("Final-pass recovery observer: start() ignored after shutdown")
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

    /// Cancels the observer loop and any in-flight sweep task. Idempotent;
    /// awaits BOTH the loop and any kicked sweep to completion before
    /// returning, so callers (e.g. `PlayheadRuntime.shutdown`) can be
    /// confident no observer-spawned work outlives them. Cancellation is
    /// cooperative — the runner's per-asset `Task.isCancelled` checks
    /// bound the wait.
    ///
    /// **Reentrancy:** the early branch below is reachable when a second
    /// `stop()` lands while the first is still parked at one of the
    /// `await` points (actor functions yield their isolation across
    /// `await`, so a second invocation can interleave). The second
    /// caller witnesses `didShutdown == true` AND `loopTask` still
    /// non-nil (it gets cleared at the very end of the first call), so
    /// the inner `await task.value` makes the second caller wait for
    /// the first call's loop drain to complete — which is exactly the
    /// idempotent-stop contract `PlayheadRuntime.shutdown` relies on.
    func stop() async {
        if didShutdown {
            if let task = loopTask {
                _ = await task.value
            }
            return
        }
        didShutdown = true

        wakeContinuation?.yield(.shutdown)
        wakeContinuation?.finish()
        wakeContinuation = nil

        if let outstandingSweep = sweepTask {
            outstandingSweep.cancel()
            sweepTask = nil
            _ = await outstandingSweep.value
        }

        if let task = loopTask {
            task.cancel()
            _ = await task.value
        }
        loopTask = nil
    }

    // MARK: - Test Sentinels

    func testHasExitedLoop() -> Bool { loopDidExit }
    func testIsLoopRunning() -> Bool { loopTask != nil && !loopDidExit }
    func testLastSweepAt() -> Date? { lastSweepAt }
    func testPriorRunnable() -> Bool? { priorRunnable }

    // MARK: - Predicate

    /// Fresh-probe runnable check matching `FinalPassRetranscriptionRunner.currentDeferReason`
    /// EXACTLY (modulo the runner's defensive `qualityProfile.pauseAllWork`
    /// re-check, which the strict thermal/charge/LPM gates already subsume
    /// on nominal devices — if a future profile change demotes nominal
    /// devices, the runner refuses and the observer's kick is wasted, but
    /// the priorRunnable stamp won't desync because the next snapshot
    /// will re-evaluate). Re-probes via the same closures the runner uses
    /// so there's no observable gap where the observer fires and the
    /// runner refuses.
    private func isRunnableNow() async -> Bool {
        let snapshot = await capabilitySnapshotProvider()
        guard snapshot.thermalState == .nominal else { return false }
        guard !snapshot.isLowPowerMode else { return false }
        return await chargeStateProvider()
    }

    // MARK: - Observer Loop

    private enum LoopEvent: Sendable {
        case capability(CapabilitySnapshot)
        case wake(WakeReason)
    }

    private func runObserverLoop(wakeStream: AsyncStream<WakeReason>) async {
        defer { loopDidExit = true }

        let capabilityStream = await capabilities.capabilityUpdates()
        let (mergedStream, mergedCont) = AsyncStream<LoopEvent>.makeStream()

        await withTaskGroup(of: Void.self) { group in
            group.addTask {
                for await snapshot in capabilityStream {
                    mergedCont.yield(.capability(snapshot))
                    if Task.isCancelled { break }
                }
            }
            group.addTask {
                for await reason in wakeStream {
                    mergedCont.yield(.wake(reason))
                    if Task.isCancelled { break }
                }
                mergedCont.finish()
            }
            group.addTask { [weak self] in
                guard let self else { return }
                await self.consumeMergedEvents(mergedStream)
                mergedCont.finish()
            }

            await group.next()
            group.cancelAll()
        }
    }

    private func consumeMergedEvents(_ stream: AsyncStream<LoopEvent>) async {
        for await event in stream {
            switch event {
            case .capability:
                // The stream emission is the wake; the actual decision
                // uses `isRunnableNow()` which fresh-probes via the
                // same closures the runner uses. This closes the
                // staleness gap where `snapshot.isCharging` could lag
                // a fresh BatteryStateProviding read by a few ms.
                let nowRunnable = await self.isRunnableNow()
                let wasRunnable = priorRunnable
                priorRunnable = nowRunnable
                guard wasRunnable == false, nowRunnable else { continue }
                await maybeFireSweep()

            case .wake(let reason):
                switch reason {
                case .shutdown:
                    logger.debug("Final-pass recovery observer: shutdown wake received")
                    return
                }
            }
        }
    }

    // MARK: - Sweep dispatch

    /// Possibly kick a final-pass recovery sweep on a runnable transition.
    ///
    /// Cooldown semantics (cycle-1 M3): the `lastSweepAt` timestamp lives
    /// only on this actor instance — it is process-local and intentionally
    /// NOT persisted across launches. A re-launch resets the cooldown to
    /// "no prior sweep", so the very first runnable transition in a new
    /// process always fires a sweep regardless of how recently the
    /// previous process kicked one. This is deliberate: the Phase-1 launch
    /// sweep already runs once per cold-start, and pairing this observer
    /// with that launch sweep would over-kick if the cooldown carried
    /// forward. The downside (a thermal-trapped run that crashes and
    /// re-launches inside the cooldown window will sweep on its first
    /// runnable transition rather than waiting) is acceptable — a crash
    /// loop is a louder failure than a re-kicked sweep.
    private func maybeFireSweep() async {
        // cycle-2 H1: short-circuit if `stop()` has begun (didShutdown=true)
        // but the consumer is processing a capability event that was buffered
        // in the merged stream BEFORE the .shutdown wake landed. Without this
        // guard, the consumer can call `maybeFireSweep` after `stop()` has
        // already cleared `sweepTask = nil` and is awaiting `loopTask.value`,
        // and we'd then spawn a fresh unstructured `Task { ... }` whose
        // cancellation is NOT inherited from `loopTask`. That orphan sweep
        // outlives `stop()`'s return, violating the H2 contract.
        guard !didShutdown else {
            logger.debug("Final-pass recovery observer: shutdown in progress, skipping sweep dispatch")
            return
        }
        if let lastAt = lastSweepAt {
            let elapsed = clock().timeIntervalSince(lastAt)
            // cycle-2 L3: clock skew (NTP correction, manual change) can
            // make `elapsed` negative. Treat negative elapsed as "definitely
            // past cooldown" rather than suppressing — the alternative
            // (suppress until wall-clock catches back up) means a backwards
            // jump silently freezes the observer for the skew duration.
            if elapsed >= 0, elapsed < cooldownSeconds {
                logger.debug("Final-pass recovery observer: cooldown skip (\(elapsed, format: .fixed(precision: 1))s < \(self.cooldownSeconds, format: .fixed(precision: 1))s)")
                return
            }
            if elapsed < 0 {
                logger.debug("Final-pass recovery observer: clock skew detected (elapsed=\(elapsed, format: .fixed(precision: 1))s); ignoring cooldown")
            }
        }
        if sweepTask != nil {
            logger.debug("Final-pass recovery observer: sweep already in flight, skipping")
            return
        }
        lastSweepAt = clock()
        let kick = self.kickSweep
        logger.info("Final-pass recovery observer: kicking sweep on runnable transition")
        sweepTask = Task { [weak self] in
            await kick()
            await self?.clearSweepTask()
        }
    }

    private func clearSweepTask() {
        sweepTask = nil
    }
}
