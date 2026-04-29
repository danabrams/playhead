// BackgroundProcessingService.swift
// Manages background task scheduling and foreground hot-path analysis lifecycle.
//
// Strategy:
//   - Hot-path: runs in foreground whenever audio is playing. This is the
//     MVP reliability path -- background work only improves completeness.
//   - BGProcessingTask: registered for deferred backfill of episodes when the
//     system grants background time.
//   - BGContinuedProcessingTask: ONLY for user-initiated long-running work.
//
// Thermal + battery + power management (routed through QualityProfile, C1):
//   profile=.nominal/.fair -> full analysis (Background lane may pause at
//                             .fair via the lane-level scheduler)
//   profile=.serious       -> pause backfill (Soon + Background lanes);
//                             foreground hot-path remains open
//   profile=.critical      -> pause all analysis including hot-path
//
// QualityProfile demotes by ONE step from the raw thermal baseline when
// either Low Power Mode is on, OR battery is below 20% while unplugged.
// So plain LPM with nominal thermal lands on .fair (no BPS pause); LPM
// stacked on fair thermal lands on .serious (backfill pauses). The two-
// tier BPS gate is `pauseAllWork` for the hot-path and
// `pauseAllWork || !allowSoonLane` for backfill.
//
// Caveat: `hotPathLookaheadMultiplier()` below predates C1 and still reads
// `currentThermalState`/`isLowPowerMode` directly rather than routing
// through `QualityProfile.schedulerPolicy.sliceFraction`. Its outputs
// agree with QualityProfile on the raw thermal axis but diverge on:
//   (a) `.critical` thermal — multiplier 1.0 vs sliceFraction 0.0
//   (b) any nominal-thermal + LPM input — multiplier 0.5 vs sliceFraction
//       1.0 (profile demotes nominal→fair which still has sliceFraction 1.0)
//   (c) `.fair` thermal + unplugged-low-battery — multiplier 1.0 vs
//       sliceFraction 0.5 (profile demotes fair→serious which has 0.5)
// Tracked as tech debt — not blocking C1.

import BackgroundTasks
import Foundation
import os
import OSLog
import UIKit

/// Wraps a non-Sendable value for transfer across isolation boundaries
/// where the developer has verified safety (e.g., BGProcessingTask from
/// the system callback that is only accessed on one actor afterward).
private struct UncheckedSendableBox<T>: @unchecked Sendable {
    let value: T
    init(_ value: T) { self.value = value }
}

// MARK: - Protocols for Testability

/// Abstracts BGProcessingTask for testability.
@preconcurrency
protocol BackgroundProcessingTaskProtocol: AnyObject, Sendable {
    func setTaskCompleted(success: Bool)
    var expirationHandler: (() -> Void)? { get set }
}

extension BGProcessingTask: BackgroundProcessingTaskProtocol {}

/// playhead-44h1: additional accessors available on
/// `BGContinuedProcessingTask` that `handleContinuedProcessingTask`
/// needs to route the hand-off to `AnalysisCoordinator`.
///
/// iOS 26's `BGContinuedProcessingTask` does not expose a `userInfo`
/// dictionary, so the episode identifier is carried in the task's
/// `identifier` suffix (per `BGContinuedProcessingTaskRequest`'s
/// wildcard-identifier convention). This protocol's `episodeId`
/// parser extracts that suffix and is the only way production code
/// reads it, so tests can hand a `StubContinuedProcessingTask` with
/// a synthetic identifier.
@preconcurrency
protocol ContinuedProcessingTaskProtocol: BackgroundProcessingTaskProtocol {
    /// Full wildcard identifier submitted with the request.
    /// Production: `"com.playhead.app.analysis.continued.<episodeId>"`.
    var identifier: String { get }
}

extension BGContinuedProcessingTask: ContinuedProcessingTaskProtocol {}

/// Abstracts BGTaskScheduler for testability.
protocol BackgroundTaskScheduling: Sendable {
    func submit(_ taskRequest: BGTaskRequest) throws
}

extension BGTaskScheduler: BackgroundTaskScheduling {}

/// Abstracts the analysis coordinator for testability.
protocol AnalysisCoordinating: Sendable {
    /// Start the long-lived capability observer. Call once at launch.
    /// The observer survives ``stop()`` calls — see
    /// ``AnalysisCoordinator.startCapabilityObserver()`` for rationale.
    func startCapabilityObserver() async
    func stop() async
    /// Drain any pending analysis backfill work for the duration of a
    /// BGProcessingTask window. Must respect `Task.isCancelled` and return
    /// promptly on expiration.
    func runPendingBackfill() async

    /// playhead-44h1: continue a foreground-assist download + analysis
    /// inside a `BGContinuedProcessingTask` window.
    ///
    /// Called from `BackgroundProcessingService.handleContinuedProcessingTask`
    /// after the app has backgrounded with an in-flight Now-lane job. The
    /// coordinator takes ownership of the remaining transfer + post-download
    /// analysis and must return by `deadline` (propagated from the task's
    /// expiration callback) or sooner. Throws on failure; the caller maps
    /// the error to a WorkJournal `failed` entry.
    func continueForegroundAssist(episodeId: String, deadline: Date) async throws

    /// playhead-44h1: request the running worker for `episodeId` pause
    /// at its next safe checkpoint.
    ///
    /// Called from `BGContinuedProcessingTask.expirationHandler` so the
    /// worker flushes unit state, releases the lease with
    /// `event=.preempted`, and exits before iOS forcibly terminates the
    /// window. This is a best-effort request: if the worker has already
    /// finished or is not registered, the call is a no-op.
    func pauseAtNextCheckpoint(episodeId: String, cause: InternalMissCause) async

    /// playhead-44h1: append a terminal WorkJournal row for the
    /// foreground-assist hand-off. Called by
    /// ``BackgroundProcessingService.handleContinuedProcessingTask``
    /// from two sites:
    ///
    /// - Expiration handler (``recordForegroundAssistOutcome`` with
    ///   `.failed`, cause=`.taskExpired`) so the journal records the
    ///   OS-forced termination.
    /// - Success path after `continueForegroundAssist` returns without
    ///   throwing (``recordForegroundAssistOutcome`` with `.finalized`,
    ///   cause=nil).
    ///
    /// The coordinator resolves the episode's current `{generationID,
    /// schedulerEpoch}` from `analysis_jobs` and appends via the
    /// existing `AnalysisStore.appendWorkJournalEntry` API. Best-effort:
    /// a storage failure is logged and swallowed because the caller
    /// has no recourse — the BG task is about to complete either way.
    func recordForegroundAssistOutcome(
        episodeId: String,
        eventType: WorkJournalEntry.EventType,
        cause: InternalMissCause?
    ) async
}

extension AnalysisCoordinator: AnalysisCoordinating {}

/// Abstracts battery state for testability.
protocol BatteryStateProviding: Sendable {
    func currentBatteryState() async -> (level: Float, isCharging: Bool)
}

/// Production implementation using UIDevice.
struct UIDeviceBatteryProvider: BatteryStateProviding {
    func currentBatteryState() async -> (level: Float, isCharging: Bool) {
        await MainActor.run {
            let device = UIDevice.current
            device.isBatteryMonitoringEnabled = true
            let level = device.batteryLevel
            let charging = device.batteryState == .charging || device.batteryState == .full
            return (level, charging)
        }
    }
}

// MARK: - Task Identifiers

enum BackgroundTaskID {
    static let backfillProcessing = "com.playhead.app.analysis.backfill"
    static let continuedProcessing = "com.playhead.app.analysis.continued"
    static let preAnalysisRecovery = "com.playhead.app.preanalysis.recovery"
}

// MARK: - BackgroundProcessingService

/// Coordinates foreground hot-path analysis with background task scheduling.
/// Owns the lifecycle of BGProcessingTask and BGContinuedProcessingTask
/// registrations. Delegates actual analysis work to AnalysisCoordinator.
actor BackgroundProcessingService {

    /// Cycle 4 H3: process-wide guard against double BGTaskScheduler
    /// registration. Flips true on the first call to
    /// `registerBackgroundTasks()`. Subsequent calls no-op, so tests that
    /// construct a second `PlayheadRuntime` in the same process (after
    /// the test host's real `PlayheadApp` has already registered) don't
    /// crash the test runner.
    private static let registrationFlag = OSAllocatedUnfairLock<Bool>(initialState: false)

    nonisolated static func registerOnce() -> Bool {
        registrationFlag.withLock { flag in
            if flag { return false }
            flag = true
            return true
        }
    }

    private let logger = Logger(subsystem: "com.playhead", category: "BackgroundProcessing")

    // MARK: - Dependencies

    private let coordinator: any AnalysisCoordinating
    private let capabilitiesService: CapabilitiesService
    private let taskScheduler: any BackgroundTaskScheduling
    private let batteryProvider: any BatteryStateProviding
    /// playhead-shpy: BG-task lifecycle telemetry. Defaults to no-op so
    /// existing tests that construct a BPS without explicitly threading
    /// a logger keep working; production wiring in `PlayheadRuntime`
    /// supplies the live `BGTaskTelemetryLogger`.
    private let bgTelemetry: any BGTaskTelemetryLogging

    /// Pre-analysis services, injected after construction via
    /// ``setPreAnalysisServices(scheduler:reconciler:)``.
    private var analysisWorkScheduler: AnalysisWorkScheduler?
    private var analysisJobReconciler: AnalysisJobReconciler?

    /// playhead-8u3i: continuations parked by handlers that ran before
    /// pre-analysis services were injected. Drained and resumed from
    /// `setPreAnalysisServices`. List-typed because multiple BG handlers
    /// (e.g. concurrent preanalysis.recovery fires) can be waiting at
    /// the same time and all must wake together when injection lands.
    private var pendingInjectionWaiters: [WaiterEntry] = []

    /// playhead-8u3i: timeout (seconds) that
    /// `awaitPreAnalysisServicesInjected` will wait for a missing
    /// reconciler to be injected before falling through to the original
    /// fail path.
    ///
    /// Defaults to 15s (review-followup csp / L2). BGProcessingTask has
    /// roughly 30s before iOS reclaims it, and the reconcile work that
    /// runs AFTER injection completes still needs to land inside the
    /// same budget — recovering expired leases, sweeping stranded
    /// session jobs, and re-enqueuing missing-file rows can each take
    /// several hundred milliseconds and run sequentially. The earlier
    /// 20s default left the post-wake half of the budget at 10s,
    /// which is uncomfortably tight; 15s leaves a 15s margin for the
    /// actual work and is still well above the worst observed
    /// injection latency (sub-second under normal cold launch).
    /// Tests override this with a small value to keep wall time
    /// bounded.
    private var injectionWaitTimeoutSeconds: TimeInterval = 15

    /// playhead-hv73 test seam: fire the injection-wait timeout NOW for
    /// every currently parked waiter, returning the count resumed.
    ///
    /// Eliminates the wall-clock dependency in tests that pin the
    /// timeout-path behavior of `awaitPreAnalysisServicesInjected`:
    /// instead of setting a small `injectionWaitTimeoutSeconds` and
    /// sleeping the test thread until the timer fires, callers park
    /// the handler on a Task, observe via
    /// `pendingInjectionWaiterCountForTesting()` that the continuation
    /// is parked, then call this method to resume it synchronously
    /// with the timeout outcome (`false`).
    ///
    /// Production code never calls this — the only resume sources in
    /// production are `setPreAnalysisServices` (success) and the
    /// per-waiter timer Task spawned inside
    /// `awaitPreAnalysisServicesInjected` (timeout). Behavior under
    /// the live BPS is unchanged: the wall-clock timeout still fires
    /// after `injectionWaitTimeoutSeconds` in production, since
    /// nothing else calls this method.
    @discardableResult
    func triggerInjectionWaitTimeoutForTesting() -> Int {
        let waiters = pendingInjectionWaiters
        pendingInjectionWaiters.removeAll()
        var resumed = 0
        for entry in waiters {
            guard let continuation = entry.slot.continuation else { continue }
            entry.slot.continuation = nil
            continuation.resume(returning: false)
            resumed += 1
        }
        return resumed
    }

    /// playhead-hv73 test seam: count of waiters currently parked in
    /// `awaitPreAnalysisServicesInjected`. Lets tests deterministically
    /// observe that a handler has reached the suspend point before
    /// firing `triggerInjectionWaitTimeoutForTesting()` or
    /// `setPreAnalysisServices(...)`, eliminating the small `Task.sleep`
    /// previously needed to give the handler time to park.
    func pendingInjectionWaiterCountForTesting() -> Int {
        pendingInjectionWaiters.count
    }

    // MARK: - State

    /// Whether the hot-path is currently active (audio playing in foreground).
    private var hotPathActive = false

    /// Whether backfill is currently paused due to thermal/battery constraints.
    private var backfillPaused = false

    /// Whether all analysis is paused. Under C1, this gate is set only when
    /// `QualityProfile.schedulerPolicy.pauseAllWork` is true — today that is
    /// reached only at `.critical` (thermal-driven). Plain low-battery and
    /// plain LPM no longer set this flag; they demote the profile but leave
    /// the foreground hot-path running.
    private var allAnalysisPaused = false

    /// Task observing capability changes for thermal/battery management.
    private var capabilityObserverTask: Task<Void, Never>?

    /// Tasks for active background processing work, keyed by the identity of
    /// the BGProcessingTask they are servicing. Each handler inserts its own
    /// entry on entry and removes it on completion; `stop()` cancels every
    /// in-flight entry. Keyed per-task so overlapping handlers (e.g. the
    /// pre-analysis recovery task fires while the backfill task is suspended
    /// inside `runPendingBackfill`) do not orphan each other.
    private var activeBackgroundTasks: [ObjectIdentifier: Task<Void, Never>] = [:]

    /// Task identifiers that have already had `setTaskCompleted` called.
    /// Guards against double-completion of BGProcessingTask: both the work
    /// completion path and the expiration handler check this before calling
    /// setTaskCompleted. Per-task (rather than a single shared bool) so that
    /// an overlapping handler completing its own task does not silently drop
    /// another handler's completion, which would leave iOS holding an
    /// unreported BGProcessingTask.
    private var completedTaskIDs = Set<ObjectIdentifier>()

    /// playhead-shpy: BGTaskScheduler identifier per in-flight BG task,
    /// keyed by `ObjectIdentifier`. Populated by `emitStart` and read by
    /// `markComplete` / `handleExpiredProcessingTask` so the `complete`
    /// and `expire` telemetry rows carry the right identifier even when
    /// only the protocol type (`any BackgroundProcessingTaskProtocol`)
    /// is in scope. Cleared on terminal events so the dict bounds with
    /// the active-task lifetime.
    private var identifiersByTaskID: [ObjectIdentifier: String] = [:]

    /// Current thermal state, cached for decision-making.
    private var currentThermalState: ThermalState = .nominal

    /// Current battery level (0.0-1.0), cached.
    private var currentBatteryLevel: Float = 1.0

    /// Whether the device is currently charging.
    private var isCharging = false

    /// Whether Low Power Mode is active.
    private var isLowPowerMode = false

    // MARK: - Init

    init(
        coordinator: any AnalysisCoordinating,
        capabilitiesService: CapabilitiesService,
        taskScheduler: any BackgroundTaskScheduling = BGTaskScheduler.shared,
        batteryProvider: any BatteryStateProviding = UIDeviceBatteryProvider(),
        bgTelemetry: any BGTaskTelemetryLogging = NoOpBGTaskTelemetryLogger()
    ) {
        self.coordinator = coordinator
        self.capabilitiesService = capabilitiesService
        self.taskScheduler = taskScheduler
        self.batteryProvider = batteryProvider
        self.bgTelemetry = bgTelemetry
    }

    /// Inject pre-analysis services after construction. Called once the
    /// scheduler and reconciler are built during app setup.
    ///
    /// playhead-8u3i: also drains any continuations parked by BG-task
    /// handlers that fired before injection landed. Resuming inside the
    /// actor method is safe: the actor's reentrant scheduling will pick
    /// up the resumed handlers' continuations once we suspend or return.
    func setPreAnalysisServices(
        scheduler: AnalysisWorkScheduler,
        reconciler: AnalysisJobReconciler
    ) {
        self.analysisWorkScheduler = scheduler
        self.analysisJobReconciler = reconciler
        let waiters = self.pendingInjectionWaiters
        self.pendingInjectionWaiters.removeAll()
        var resumed = 0
        for entry in waiters {
            // The timeout path may have already won and cleared the slot.
            // Skip those — the timeout-side `removeAll` should have purged
            // them, but defensive nil-check is cheap insurance against a
            // future scheduling change.
            guard let continuation = entry.slot.continuation else { continue }
            entry.slot.continuation = nil
            continuation.resume(returning: true)
            resumed += 1
        }
        if resumed > 0 {
            logger.info("Pre-analysis services injected; resumed \(resumed, privacy: .public) waiting handler(s)")
        } else {
            logger.info("Pre-analysis services injected")
        }
    }

    /// playhead-8u3i: holder for a single waiter's continuation. Reference
    /// type so the timeout-side and the injection-side can race for
    /// ownership of the resume — the loser sees `continuation == nil` and
    /// becomes a no-op. Marked `@unchecked Sendable` because the actor
    /// owns all reads/writes and external touchers (the timeout `Task`)
    /// only mutate via actor-isolated methods.
    final class WaiterSlot: @unchecked Sendable {
        var continuation: CheckedContinuation<Bool, Never>?
    }

    /// Wrapper so `pendingInjectionWaiters` can hold reference-typed
    /// slots without forcing Optional<WaiterSlot> elsewhere.
    struct WaiterEntry {
        let slot: WaiterSlot
    }

    /// playhead-8u3i: suspend the caller until pre-analysis services are
    /// injected, or until `injectionWaitTimeoutSeconds` elapses. Returns
    /// `true` if injection landed, `false` on timeout. Returns `true`
    /// immediately when the reconciler is already non-nil.
    ///
    /// Belt-and-suspenders against a race that the Part 1 reorder in
    /// `PlayheadRuntime.swift` already closes — if a future refactor ever
    /// reintroduces work above the injection call, this buffer keeps the
    /// preanalysis.recovery handler from instant-failing. Multiple
    /// concurrent waiters share one wake when injection lands, with each
    /// caller running its own timeout race independently.
    private func awaitPreAnalysisServicesInjected() async -> Bool {
        if analysisJobReconciler != nil {
            return true
        }

        // Track this caller's slot in the waiters list so a timeout can
        // remove its own continuation without disturbing siblings. The
        // mutable holder is captured by both the timeout task and the
        // continuation-resume path so whichever fires first wins; the
        // loser becomes a no-op.
        let slot = WaiterSlot()

        let timeoutSeconds = injectionWaitTimeoutSeconds
        let timeoutNanos = UInt64(max(0, timeoutSeconds) * 1_000_000_000)

        let timedOutTask = Task { [weak self] in
            try? await Task.sleep(nanoseconds: timeoutNanos)
            await self?.timeoutInjectionWaiter(slot: slot)
        }

        let injected: Bool = await withCheckedContinuation { continuation in
            // Re-check under actor isolation: an injection that landed
            // between the early-return check at the top of this method
            // and our continuation creation must not strand us.
            if self.analysisJobReconciler != nil {
                continuation.resume(returning: true)
                return
            }
            slot.continuation = continuation
            self.pendingInjectionWaiters.append(WaiterEntry(slot: slot))
        }

        timedOutTask.cancel()
        return injected
    }

    /// playhead-8u3i: timeout-side resume for a parked waiter. If the
    /// slot still owns a live continuation, drop the entry from the
    /// pending list and resume it with `false`. If injection already
    /// fired, this is a no-op.
    private func timeoutInjectionWaiter(slot: WaiterSlot) {
        guard let continuation = slot.continuation else { return }
        slot.continuation = nil
        pendingInjectionWaiters.removeAll { entry in
            entry.slot === slot
        }
        continuation.resume(returning: false)
    }

    // MARK: - Registration

    /// Register background task identifiers with fallback handlers.
    /// This remains for compatibility, but the app should prefer
    /// ``registerBackgroundTasks()`` once the runtime has been built.
    static func registerTaskIdentifiers() {
        let logger = Logger(subsystem: "com.playhead", category: "BackgroundProcessing")

        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: BackgroundTaskID.backfillProcessing,
            using: nil
        ) { task in
            logger.warning("Backfill task fired before service initialized")
            task.setTaskCompleted(success: false)
        }

        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: BackgroundTaskID.continuedProcessing,
            using: nil
        ) { task in
            logger.warning("Continued processing task fired before service initialized")
            task.setTaskCompleted(success: false)
        }

        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: BackgroundTaskID.preAnalysisRecovery,
            using: nil
        ) { task in
            logger.warning("Pre-analysis recovery task fired before service initialized")
            task.setTaskCompleted(success: false)
        }
    }

    /// Re-register background task identifiers with real handlers.
    /// Call once the service is fully initialized with its dependencies.
    ///
    /// Cycle 4 H3: guarded against double registration within a single
    /// process. `BGTaskScheduler.register(forTaskWithIdentifier:)`
    /// crashes with an `NSInternalInconsistencyException` on the second
    /// call for the same identifier, which is fatal for any test that
    /// wants to construct a non-preview `PlayheadRuntime` in a process
    /// where the test host's real `PlayheadApp` already launched. The
    /// guard is process-wide because BGTaskScheduler itself is a
    /// singleton and has no deregister API.
    nonisolated func registerBackgroundTasks() {
        guard Self.registerOnce() else {
            // Already registered in this process — nothing to do. The
            // first registrar's handlers remain live.
            return
        }
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: BackgroundTaskID.backfillProcessing,
            using: nil
        ) { [weak self] task in
            guard let self, let processingTask = task as? BGProcessingTask else {
                task.setTaskCompleted(success: false)
                return
            }
            let sendableTask = UncheckedSendableBox(processingTask)
            Task { await self.handleBackfillTask(sendableTask.value) }
        }

        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: BackgroundTaskID.continuedProcessing,
            using: nil
        ) { [weak self] task in
            guard let self else {
                task.setTaskCompleted(success: false)
                return
            }
            // playhead-44h1: continued processing tasks under iOS 26 are
            // `BGContinuedProcessingTask` (wildcard identifier, long
            // user-initiated window). Older BGProcessingTask remains
            // accepted as a fallback for the previous no-op path so
            // scheduler re-registrations during tests do not crash.
            if let continuedTask = task as? BGContinuedProcessingTask {
                let sendableTask = UncheckedSendableBox(continuedTask)
                Task { await self.handleContinuedProcessingTask(sendableTask.value) }
                return
            }
            if task as? BGProcessingTask != nil {
                // Legacy identifier path: mark complete without work.
                // The new flow uses BGContinuedProcessingTask exclusively.
                task.setTaskCompleted(success: true)
                return
            }
            task.setTaskCompleted(success: false)
        }

        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: BackgroundTaskID.preAnalysisRecovery,
            using: nil
        ) { [weak self] task in
            guard let self, let processingTask = task as? BGProcessingTask else {
                task.setTaskCompleted(success: false)
                return
            }
            let sendableTask = UncheckedSendableBox(processingTask)
            Task { await self.handlePreAnalysisRecovery(sendableTask.value) }
        }
    }

    // MARK: - Lifecycle

    /// Start observing capability changes and battery state.
    /// Call once after registration.
    ///
    /// Starts the coordinator's capability observer once here at launch.
    /// The observer survives ``coordinator.stop()`` calls, so neither
    /// ``playbackDidStart()`` nor ``handleCapabilityUpdate(_:)`` need to
    /// re-start it — eliminating a class of lifecycle coupling bugs.
    func start() async {
        await updateBatteryState()

        // Start the coordinator's long-lived capability observer once.
        // It survives stop() calls and does not need to be re-started
        // on thermal/battery recovery.
        await coordinator.startCapabilityObserver()

        capabilityObserverTask?.cancel()
        capabilityObserverTask = Task { [weak self] in
            guard let self else { return }
            let updates = await self.capabilitiesService.capabilityUpdates()
            for await snapshot in updates {
                guard !Task.isCancelled else { break }
                await self.handleCapabilityUpdate(snapshot)
            }
        }

        logger.info("BackgroundProcessingService started")
        // Kick off the initial pre-analysis recovery schedule.
        schedulePreAnalysisRecovery()
    }

    /// Stop all observation and cancel pending work.
    func stop() {
        capabilityObserverTask?.cancel()
        capabilityObserverTask = nil
        for (_, task) in activeBackgroundTasks {
            task.cancel()
        }
        activeBackgroundTasks.removeAll()
        hotPathActive = false
        logger.info("BackgroundProcessingService stopped")
    }

    // MARK: - Hot-Path Control (Foreground Playback)

    /// Signal that audio playback has started. Activates hot-path analysis.
    func playbackDidStart() {
        guard !allAnalysisPaused else {
            logger.info("Hot-path activation deferred: all analysis paused")
            return
        }

        hotPathActive = true
        logger.info("Hot-path active")
    }

    /// Signal that audio playback has stopped. Deactivates hot-path analysis,
    /// and schedules background backfill if appropriate.
    func playbackDidStop() {
        hotPathActive = false
        logger.info("Hot-path inactive")

        scheduleBackfillIfNeeded()
    }

    /// playhead-fuo6: submit a backfill BGProcessingTask whenever the
    /// app enters the background.
    ///
    /// The 12-hour overnight blackout in capture
    /// `2026-04-25 07:43.49.095` reproduced because the only callers of
    /// `scheduleBackfillIfNeeded()` were `playbackDidStop()` and the
    /// backfill handler's own self-rearm. A user who queues episodes
    /// without ever pressing play never triggers either path, so iOS
    /// has no submitted BGProcessingTask to wake the app for. The
    /// in-memory `AnalysisWorkScheduler.runLoop` only gets CPU until
    /// iOS suspends the process (~30s after backgrounding).
    ///
    /// Wiring it on every `.background` scenePhase transition is safe
    /// because:
    ///   * `BGTaskScheduler.submit(_:)` deduplicates identical
    ///     identifiers; iOS coalesces duplicate submissions.
    ///   * The submitted request is identical to the one already used
    ///     by `playbackDidStop()` and the self-rearm path
    ///     (`requiresExternalPower=false`, `requiresNetworkConnectivity
    ///     =false`); there is no network/power policy regression.
    ///   * If the OS later runs the task, the existing handler
    ///     re-arms via `scheduleBackfillIfNeeded()` so periodic
    ///     submission continues without further app-side action.
    ///
    /// Called from `PlayheadApp.onChange(of: scenePhase)` on the main
    /// actor when the new phase is `.background`.
    func appDidEnterBackground() {
        logger.info("App entered background -- submitting backfill task")
        scheduleBackfillIfNeeded()
    }

    /// Returns the recommended hot-path lookahead multiplier based on
    /// thermal state and power mode. 1.0 = full, 0.5 = reduced.
    func hotPathLookaheadMultiplier() -> Double {
        if currentThermalState == .serious || isLowPowerMode {
            return 0.5
        }
        return 1.0
    }

    /// Whether the hot-path is currently running.
    func isHotPathActive() -> Bool {
        hotPathActive && !allAnalysisPaused
    }

    /// Whether backfill is currently paused. Exposed for tests so they can
    /// pin C1's two-tier gate (`pauseAllWork` for the foreground hot-path,
    /// `pauseAllWork || !allowSoonLane` for backfill) without reaching into
    /// private state.
    func isBackfillPaused() -> Bool {
        backfillPaused
    }

    // MARK: - Background Task Scheduling

    /// Schedule a BGProcessingTask for deferred backfill.
    func scheduleBackfillIfNeeded() {
        let request = BGProcessingTaskRequest(identifier: BackgroundTaskID.backfillProcessing)
        request.requiresNetworkConnectivity = false
        request.requiresExternalPower = false

        submitWithTelemetry(request, reason: nil)
    }

    /// playhead-shpy: shared `BGTaskScheduler.submit` wrapper that emits
    /// a `submit` telemetry row regardless of throw/non-throw. Logging
    /// is fire-and-forget so a logger failure cannot cascade into the
    /// scheduler call path. The `detail` field captures the
    /// `reason` annotation passed by the scheduling caller so
    /// dogfood logs distinguish intent without grepping by identifier.
    private func submitWithTelemetry(_ request: BGTaskRequest, reason: String?) {
        let earliestDelay = request.earliestBeginDate?.timeIntervalSinceNow
        do {
            try taskScheduler.submit(request)
            logger.info("BGTask submitted: \(request.identifier, privacy: .public)\(reason.map { " (\($0))" } ?? "", privacy: .public)")
            let identifier = request.identifier
            let bgTelemetry = self.bgTelemetry
            Task {
                let phase = await BGTaskTelemetryScenePhase.current()
                await bgTelemetry.record(
                    .submit(
                        identifier: identifier,
                        succeeded: true,
                        earliestBeginDelaySec: earliestDelay,
                        scenePhase: phase,
                        detail: reason
                    )
                )
            }
        } catch {
            logger.error("BGTask submit failed for \(request.identifier, privacy: .public): \(error)")
            let identifier = request.identifier
            let errString = String(describing: error)
            let bgTelemetry = self.bgTelemetry
            Task {
                let phase = await BGTaskTelemetryScenePhase.current()
                await bgTelemetry.record(
                    .submit(
                        identifier: identifier,
                        succeeded: false,
                        error: errString,
                        earliestBeginDelaySec: earliestDelay,
                        scenePhase: phase,
                        detail: reason
                    )
                )
            }
        }
    }

    // MARK: - Background Task Completion Guard

    /// Safely complete a BGProcessingTask, guarding against double-completion.
    /// Both the work path and expiration handler call this; only the first wins.
    /// Tracked per-task so overlapping handlers cannot silently drop each
    /// other's completions.
    ///
    /// Returns `true` when this call won the completion race (and therefore
    /// `setTaskCompleted` was invoked), `false` when a prior call already
    /// completed the task. Callers that emit a paired `WorkJournal`
    /// terminal-row should gate the emission on the return value to avoid
    /// double-writing audit rows when expiration races a normal return.
    @discardableResult
    func markComplete(_ task: any BackgroundProcessingTaskProtocol, success: Bool) -> Bool {
        let id = ObjectIdentifier(task as AnyObject)
        guard !completedTaskIDs.contains(id) else {
            logger.debug("BGTask completion already called, ignoring duplicate")
            return false
        }
        completedTaskIDs.insert(id)
        // Clean up the active-task entry for this BGProcessingTask, if any.
        activeBackgroundTasks.removeValue(forKey: id)
        task.setTaskCompleted(success: success)
        // playhead-shpy: emit a `complete` row exactly once per BG task
        // instance (gated by the same `completedTaskIDs` idempotence
        // guard above). The taskInstanceID is the same string the
        // matching `start` row used; the logger will fill
        // `timeInTaskSec` from its in-memory start map.
        let identifier = identifierForTask(task)
        identifiersByTaskID.removeValue(forKey: id)
        let instanceID = bgTaskInstanceID(for: task as AnyObject)
        let bgTelemetry = self.bgTelemetry
        Task {
            let phase = await BGTaskTelemetryScenePhase.current()
            await bgTelemetry.record(
                .complete(
                    identifier: identifier,
                    taskInstanceID: instanceID,
                    success: success,
                    timeInTaskSec: nil,
                    scenePhase: phase
                )
            )
        }
        return true
    }

    /// playhead-shpy: best-effort identifier resolution for a BG task
    /// instance, falling back to the per-instance map populated by
    /// `emitStart` if the protocol layer does not surface it.
    private func identifierForTask(_ task: any BackgroundProcessingTaskProtocol) -> String {
        if let continued = task as? any ContinuedProcessingTaskProtocol {
            return continued.identifier
        }
        let id = ObjectIdentifier(task as AnyObject)
        return identifiersByTaskID[id] ?? "unknown"
    }

    /// playhead-shpy: emit an `expire` telemetry row. Called from each
    /// `expirationHandler` closure before the handler runs the cleanup
    /// work, so the on-disk log records the expiration even if the
    /// subsequent cleanup throws or is preempted by the OS-forced
    /// termination timer.
    private func emitExpire(identifier: String, taskRef: AnyObject, detail: String?) async {
        let instanceID = bgTaskInstanceID(for: taskRef)
        let phase = await BGTaskTelemetryScenePhase.current()
        await bgTelemetry.record(
            .expire(
                identifier: identifier,
                taskInstanceID: instanceID,
                timeInTaskSec: nil, // logger fills from in-memory start map
                scenePhase: phase,
                detail: detail
            )
        )
    }

    /// playhead-shpy: emit a `start` telemetry row for the given task
    /// and record the identifier in the per-instance map so the eventual
    /// `complete`/`expire` rows can resolve it without the protocol
    /// surface needing an `identifier` accessor.
    private func emitStart(identifier: String, taskRef: AnyObject) {
        let id = ObjectIdentifier(taskRef)
        identifiersByTaskID[id] = identifier
        let instanceID = bgTaskInstanceID(for: taskRef)
        let bgTelemetry = self.bgTelemetry
        Task {
            let phase = await BGTaskTelemetryScenePhase.current()
            await bgTelemetry.record(
                .start(
                    identifier: identifier,
                    taskInstanceID: instanceID,
                    timeSinceSubmitSec: nil, // logger fills from in-memory submit map
                    scenePhase: phase
                )
            )
        }
    }

    // MARK: - Background Task Handlers

    /// Handle the backfill BGProcessingTask. Runs analysis on episodes that
    /// have incomplete transcription or ad detection.
    func handleBackfillTask(_ task: any BackgroundProcessingTaskProtocol) {
        logger.info("Backfill task started")

        let taskID = ObjectIdentifier(task as AnyObject)
        emitStart(identifier: BackgroundTaskID.backfillProcessing, taskRef: task as AnyObject)

        // Schedule the next occurrence.
        scheduleBackfillIfNeeded()

        let workTask = Task {
            // Check constraints before starting.
            await self.updateBatteryState()
            let snapshot = await self.capabilitiesService.currentSnapshot
            let profile = self.currentQualityProfile(for: snapshot)
            // C1 alignment: backfill-class work pauses when QualityProfile
            // closes the Soon lane — i.e. profile is `.serious` or `.critical`.
            // This is INTENTIONALLY broader than the BPS foreground gate
            // (which only fires on `.critical`) but narrower than the
            // historical DAP behavior: DAP also paused backfill on plain LPM
            // and plain unplugged-low-battery, which under QP only demote to
            // `.fair` — Soon lane stays open. The lane-level scheduler in
            // `AnalysisWorkScheduler` is the canonical place that throttles
            // `.fair` (it pauses Background lane), so leaving the BPS gate
            // open at `.fair` is the correct division of labor.
            let policy = profile.schedulerPolicy
            if policy.pauseAllWork || !policy.allowSoonLane {
                self.logger.info(
                    "Backfill skipped under QualityProfile \(profile.rawValue, privacy: .public)"
                )
                await self.markComplete(task, success: true)
                return
            }

            // Drive the analysis pipeline to drain pending backfill jobs.
            //
            // Two cooperating pieces are in play here:
            //  1. AnalysisWorkScheduler runs its own long-lived loop (started
            //     once in PlayheadRuntime) that actually executes jobs. We
            //     wake it explicitly so it does not wait out its sleep
            //     interval before polling for eligible work.
            //  2. runPendingBackfill polls the analysis_jobs table and yields
            //     between checks, keeping this BGProcessingTask alive while
            //     the scheduler loop drains the queue. It returns when the
            //     queue is empty or when the task is cancelled (expiration).
            //
            // Waking the scheduler here makes the BPS→WorkScheduler dependency
            // explicit rather than relying on the loop's idle sleep happening
            // to wake on its own timer.
            //
            // NOTE: this is the actual work payload — the previous version of
            // this handler called coordinator.startCapabilityObserver() (then
            // named start()), which is a fire-and-forget capability-observer
            // setup that returned in microseconds and let iOS reclaim the
            // granted background time without any analysis happening.
            // See git blame for context.
            await self.analysisWorkScheduler?.wake()
            await self.coordinator.runPendingBackfill()

            // If the task was cancelled (expiration fired), bail out without
            // marking success. The expiration handler will mark the task
            // completed with success=false via handleExpiredProcessingTask.
            // Without this guard, a coordinator that returns quickly after
            // cancellation would race the expiration handler in markComplete.
            guard !Task.isCancelled else {
                self.logger.info("Backfill work task cancelled before completion")
                return
            }

            // Let the coordinator run until the task expires or completes.
            await self.markComplete(task, success: true)
            self.logger.info("Backfill task completed")
        }

        activeBackgroundTasks[taskID] = workTask

        // Handle expiration: cancel work gracefully. The nonisolated callback
        // hops to the actor via Task to check the completion guard.
        task.expirationHandler = { [weak self] in
            workTask.cancel()
            Task { [weak self] in
                await self?.emitExpire(
                    identifier: BackgroundTaskID.backfillProcessing,
                    taskRef: task as AnyObject,
                    detail: "backfill-task-expired"
                )
                await self?.handleExpiredProcessingTask(task)
            }
        }
    }

    /// Default deadline budget for `continueForegroundAssist` when the
    /// caller does not know how long iOS will keep the task alive.
    /// iOS 26's `BGContinuedProcessingTask` does not surface an explicit
    /// `expirationDate`; the 15-minute floor matches what Apple documents
    /// as the minimum granted window for continued-processing requests.
    /// The `expirationHandler` still bounds the actual budget — this
    /// value only controls the `deadline` value the coordinator sees.
    static let continuedProcessingDeadlineBudget: TimeInterval = 15 * 60

    /// Prefix for continued-processing task identifiers under the
    /// `BGContinuedProcessingTaskRequest` wildcard-identifier convention.
    /// Production identifiers are of the form
    /// `"<prefix>.<episodeId>"`; the parser below splits on this prefix
    /// and returns the suffix (empty string → missing id, logged loud
    /// and the task fails fast so the worker does not spin on a
    /// nonexistent job).
    static let continuedProcessingIdentifierPrefix: String =
        BackgroundTaskID.continuedProcessing + "."

    /// playhead-44h1: hand-off a foreground-assist transfer + analysis
    /// from the expired foreground-assist work item to the OS-granted
    /// `BGContinuedProcessingTask` window.
    ///
    /// Flow:
    ///   1. Parse the episode id from the task identifier suffix (see
    ///      `continuedProcessingIdentifierPrefix`). A missing suffix is
    ///      a loud failure — the request was malformed, not the OS.
    ///   2. Invoke `AnalysisCoordinator.continueForegroundAssist(
    ///      episodeId:deadline:)` with a deadline derived from
    ///      `continuedProcessingDeadlineBudget`. The coordinator owns
    ///      the remaining transfer + post-download analysis and must
    ///      return before the deadline or the expiration handler
    ///      fires.
    ///   3. On success, call `setTaskCompleted(success: true)`. On
    ///      failure (any thrown error), call
    ///      `setTaskCompleted(success: false)`. The coordinator is
    ///      responsible for its own WorkJournal bookkeeping; this
    ///      handler does NOT double-write a `failed` entry here, which
    ///      would be a duplicate — `continueForegroundAssist` is the
    ///      canonical emission site for the `failed` / `finalized`
    ///      journal row.
    ///   4. The task's `expirationHandler` routes to
    ///      `pauseAtNextCheckpoint(episodeId:cause:.taskExpired)` so
    ///      the worker checkpoints and releases its lease with the
    ///      correct cause before the OS terminates the window.
    func handleContinuedProcessingTask(_ task: any ContinuedProcessingTaskProtocol) {
        logger.info("Continued processing task started: identifier=\(task.identifier, privacy: .public)")

        let taskID = ObjectIdentifier(task as AnyObject)
        emitStart(identifier: task.identifier, taskRef: task as AnyObject)

        guard let episodeId = Self.parseEpisodeId(from: task.identifier) else {
            logger.error("Continued processing task missing episode id suffix: \(task.identifier, privacy: .public)")
            markComplete(task, success: false)
            return
        }

        // Deadline propagated to the coordinator so its internal
        // polling loops can budget work against the same wall-clock
        // the OS will enforce via `expirationHandler`. The handler
        // itself is the hard gate; the deadline is the soft gate.
        let deadline = Date(timeIntervalSinceNow: Self.continuedProcessingDeadlineBudget)
        let coordinator = self.coordinator

        let workTask = Task { [weak self] in
            do {
                try await coordinator.continueForegroundAssist(
                    episodeId: episodeId,
                    deadline: deadline
                )
                // Spec state-machine step 5: on successful completion
                // append a `finalized` WorkJournal entry — but ONLY when
                // we win the markComplete race. The coordinator's polling
                // loop also returns normally when (a) the deadline was
                // reached cleanly OR (b) a pause-observed exit unwound
                // after the expiration handler already wrote `failed`.
                // In case (b), the expiration path's markComplete won
                // first; emitting `finalized` here would double-write a
                // contradictory terminal row for the same {episode,
                // generation} pair. Gating on the markComplete return
                // value keeps the audit trail single-row-per-outcome.
                //
                // Order matters: markComplete first, appendTerminal only
                // if we won. The opposite order in the expiration path is
                // intentional (durability of the failed row across a
                // mid-handler crash); here, losing the finalized row on a
                // crash between the two calls is preferable to writing
                // it ahead of an unknown race outcome.
                if let self {
                    let won = await self.markComplete(task, success: true)
                    if won {
                        await self.appendTerminal(
                            episodeId: episodeId,
                            eventType: .finalized,
                            cause: nil
                        )
                    }
                }
            } catch {
                self?.logger.error(
                    "Continued processing failed for episode \(episodeId, privacy: .public): \(String(describing: error), privacy: .public)"
                )
                // Same race-gating as the success branch: if expiration
                // already wrote `failed/taskExpired` and won markComplete,
                // a paired `failed/pipelineError` here would duplicate the
                // terminal row with a misattributed cause.
                if let self {
                    let won = await self.markComplete(task, success: false)
                    if won {
                        await self.appendTerminal(
                            episodeId: episodeId,
                            eventType: .failed,
                            cause: .pipelineError
                        )
                    }
                }
            }
        }

        activeBackgroundTasks[taskID] = workTask

        // Expiration handler: request a safe-point pause with
        // cause=.taskExpired, append a `failed` WorkJournal entry
        // (spec state-machine step 5), then mark the task failed and
        // cancel the in-flight work. The journal append happens
        // BEFORE markComplete so a crash between the two still
        // leaves a durable audit trail of the expiration. Marking
        // complete BEFORE cancellation wins the `completedTaskIDs`
        // idempotence race — if the work task's normal return
        // observes the pause and calls `markComplete(success: true)`
        // afterwards, the duplicate is dropped and the OS still
        // sees success=false, which is the correct reporting for an
        // expiration. The cancellation is the forcing function that
        // makes the work task return promptly so resources are
        // reclaimed.
        task.expirationHandler = { [weak self] in
            guard let self else { return }
            Task { [weak self] in
                guard let self else { return }
                await self.emitExpire(
                    identifier: task.identifier,
                    taskRef: task as AnyObject,
                    detail: "continued-processing-expired"
                )
                await self.coordinator.pauseAtNextCheckpoint(
                    episodeId: episodeId,
                    cause: .taskExpired
                )
                await self.appendTerminal(
                    episodeId: episodeId,
                    eventType: .failed,
                    cause: .taskExpired
                )
                await self.markComplete(task, success: false)
                workTask.cancel()
            }
        }
    }

    /// playhead-44h1 (fix): shared terminal-append helper for the
    /// three WorkJournal emission sites in `handleContinuedProcessingTask`
    /// (finalized / pipelineError / taskExpired). Keeps the call-sites
    /// DRY so a future change to the terminal-row contract (extra
    /// metadata, different routing, telemetry hooks) only edits one
    /// place. Behavior is byte-identical to the inline call it replaces:
    /// forwards to the coordinator's `recordForegroundAssistOutcome`.
    private func appendTerminal(
        episodeId: String,
        eventType: WorkJournalEntry.EventType,
        cause: InternalMissCause?
    ) async {
        await coordinator.recordForegroundAssistOutcome(
            episodeId: episodeId,
            eventType: eventType,
            cause: cause
        )
    }

    /// Parse the episode id from a wildcard-identifier continued-
    /// processing task. Returns nil when the identifier does not
    /// match the expected prefix or when the suffix is empty.
    ///
    /// Static so the parsing rule is unit-testable without standing
    /// up a service instance.
    static func parseEpisodeId(from identifier: String) -> String? {
        guard identifier.hasPrefix(continuedProcessingIdentifierPrefix) else {
            return nil
        }
        let suffix = String(identifier.dropFirst(continuedProcessingIdentifierPrefix.count))
        return suffix.isEmpty ? nil : suffix
    }

    // MARK: - Thermal & Battery Management

    /// React to capability snapshot changes.
    func handleCapabilityUpdate(_ snapshot: CapabilitySnapshot) async {
        let previousThermalState = currentThermalState
        currentThermalState = snapshot.thermalState
        isLowPowerMode = snapshot.isLowPowerMode

        await updateBatteryState()

        // C1 alignment: route every gate through `QualityProfile`. The
        // derived profile is the single source of truth shared with
        // `AdmissionController` and `AnalysisWorkScheduler` so this service
        // cannot drift on thresholds.
        //
        // - `shouldPauseAll` is BPS-specific service logic: pause every
        //   analysis path (including the foreground hot-path) when the device
        //   is in distress (`pauseAllWork` is set in `.critical`).
        // - `shouldPauseBackfill` mirrors the AdmissionController backfill
        //   gate: pause when the profile closes the Soon lane (`.serious` or
        //   `.critical`). This is INTENTIONALLY broader than the foreground
        //   gate but narrower than the historical DAP behavior — DAP also
        //   paused backfill on plain LPM and plain unplugged-low-battery,
        //   which under QP only demote to `.fair` (Soon lane stays open). The
        //   lane-level scheduler handles `.fair` directly by pausing the
        //   Background lane. See review-cycle 1 H-1.
        let profile = currentQualityProfile(for: snapshot)
        let policy = profile.schedulerPolicy
        let shouldPauseAll = policy.pauseAllWork
        let shouldPauseBackfill = policy.pauseAllWork || !policy.allowSoonLane

        // Pause all analysis.
        if shouldPauseAll && !allAnalysisPaused {
            allAnalysisPaused = true
            backfillPaused = true
            logger.warning("All analysis paused (profile=\(profile.rawValue, privacy: .public), thermal=\(snapshot.thermalState.rawValue), battery=\(self.currentBatteryLevel))")

            Task {
                await coordinator.stop()
            }
        }

        // Resume from full pause.
        if !shouldPauseAll && allAnalysisPaused {
            allAnalysisPaused = false
            logger.info("Analysis pause lifted (profile=\(profile.rawValue, privacy: .public), thermal=\(snapshot.thermalState.rawValue))")
            // The coordinator's capability observer survives stop() calls,
            // so no need to re-start it here. Hot-path work will resume
            // naturally on the next playback event or capability change
            // processed by the coordinator's own observer.
        }

        // Pause/resume backfill independently.
        if shouldPauseBackfill && !backfillPaused {
            backfillPaused = true
            logger.info("Backfill paused (profile=\(profile.rawValue, privacy: .public), thermal=\(snapshot.thermalState.rawValue), lowPower=\(snapshot.isLowPowerMode))")
        }

        if !shouldPauseBackfill && backfillPaused {
            backfillPaused = false
            logger.info("Backfill resumed (profile=\(profile.rawValue, privacy: .public))")
        }

        if previousThermalState != snapshot.thermalState {
            logger.info("Thermal state: \(previousThermalState.rawValue) -> \(snapshot.thermalState.rawValue)")
        }
    }

    /// Update cached battery state from the battery provider.
    private func updateBatteryState() async {
        let state = await batteryProvider.currentBatteryState()
        currentBatteryLevel = state.level
        isCharging = state.isCharging
    }

    /// Derive the current `QualityProfile` from the latest capability
    /// snapshot, overriding the snapshot's battery and charging fields with
    /// the BPS-cached values (refreshed from the battery provider on every
    /// capability update).
    ///
    /// All thermal/battery/low-power decisions in BPS route through this
    /// helper so the service and `AdmissionController` cannot drift — they
    /// share `QualityProfile.derive(...)` as the single source of truth.
    ///
    /// Field-by-field: `thermalState` and `isLowPowerMode` come from the
    /// `snapshot` argument (via `CapabilitySnapshot.qualityProfile(...)`).
    /// `batteryLevel` and `isCharging` are taken from BPS-cached values,
    /// which are refreshed from the battery provider on every capability
    /// update and so are at least as fresh as the snapshot's battery fields.
    /// In practice the snapshot is produced by the same capabilities service
    /// that drives BPS, so all four values agree under steady state. Outside
    /// steady state, the BPS-cached `isCharging` can lead the snapshot's by
    /// one capability tick (the snapshot only refreshes `isCharging` when a
    /// `batteryStateDidChange` notification fires); the override is the
    /// freshness fix for that brief window.
    private func currentQualityProfile(for snapshot: CapabilitySnapshot) -> QualityProfile {
        snapshot.qualityProfile(
            batteryLevel: currentBatteryLevel,
            isCharging: isCharging
        )
    }

    func handleExpiredProcessingTask(_ task: any BackgroundProcessingTaskProtocol) async {
        await coordinator.stop()
        await markComplete(task, success: false)
    }

    // MARK: - Pre-Analysis Recovery

    /// Handle the pre-analysis recovery BGProcessingTask. Runs reconciliation
    /// to find interrupted T0/T1+ jobs and resumes them.
    func handlePreAnalysisRecovery(_ task: any BackgroundProcessingTaskProtocol) async {
        logger.info("Pre-analysis recovery task started")

        let taskID = ObjectIdentifier(task as AnyObject)
        emitStart(identifier: BackgroundTaskID.preAnalysisRecovery, taskRef: task as AnyObject)

        // Schedule the next occurrence.
        schedulePreAnalysisRecovery()

        // playhead-8u3i: cold-launch wakes can fire this handler before
        // `PlayheadRuntime.init`'s deferred Task injects the reconciler.
        // Wait up to `injectionWaitTimeoutSeconds` for injection to land
        // before falling through to the original fail path. Returns true
        // immediately if the reconciler is already set (warm launch /
        // re-fire after start-up).
        let injected = await awaitPreAnalysisServicesInjected()
        guard injected, let reconciler = analysisJobReconciler else {
            logger.warning("Pre-analysis recovery: no reconciler available (timeout=\(!injected, privacy: .public))")
            markComplete(task, success: false)
            return
        }

        let workTask = Task {
            _ = try? await reconciler.reconcile()
            await self.markComplete(task, success: true)
            self.logger.info("Pre-analysis recovery task completed")
        }

        activeBackgroundTasks[taskID] = workTask

        task.expirationHandler = { [weak self] in
            workTask.cancel()
            Task { [weak self] in
                await self?.emitExpire(
                    identifier: BackgroundTaskID.preAnalysisRecovery,
                    taskRef: task as AnyObject,
                    detail: "preanalysis-recovery-expired"
                )
                // playhead-1nl6: surface the BGProcessingTask-reclaim
                // signal through the scheduler's cancel path as the
                // `.taskExpired` InternalMissCause so any live slice
                // emits a WorkJournal row with `cause = task_expired`
                // instead of a bare cancel.
                await self?.analysisWorkScheduler?.cancelCurrentJob(cause: .taskExpired)
                await self?.markComplete(task, success: false)
            }
        }
    }

    /// Schedule a BGProcessingTask for pre-analysis recovery.
    /// Requires external power; earliest begin 60s from now.
    func schedulePreAnalysisRecovery() {
        let request = BGProcessingTaskRequest(identifier: BackgroundTaskID.preAnalysisRecovery)
        request.requiresExternalPower = true
        request.earliestBeginDate = Date(timeIntervalSinceNow: 60)
        submitWithTelemetry(request, reason: "preanalysis-recovery")
    }
}
