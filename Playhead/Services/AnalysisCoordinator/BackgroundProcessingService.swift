// BackgroundProcessingService.swift
// Manages background task scheduling and foreground hot-path analysis lifecycle.
//
// Strategy:
//   - Hot-path: runs in foreground whenever audio is playing. This is the
//     MVP reliability path -- background work only improves completeness.
//   - BGProcessingTask: registered for deferred backfill of episodes when the
//     system grants background time.
//   - BGContinuedProcessingTask: ONLY for user-initiated long-running work
//     (e.g., initial model download via AssetProvider).
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

    /// Pre-analysis services, injected after construction via
    /// ``setPreAnalysisServices(scheduler:reconciler:)``.
    private var analysisWorkScheduler: AnalysisWorkScheduler?
    private var analysisJobReconciler: AnalysisJobReconciler?

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
        batteryProvider: any BatteryStateProviding = UIDeviceBatteryProvider()
    ) {
        self.coordinator = coordinator
        self.capabilitiesService = capabilitiesService
        self.taskScheduler = taskScheduler
        self.batteryProvider = batteryProvider
    }

    /// Inject pre-analysis services after construction. Called once the
    /// scheduler and reconciler are built during app setup.
    func setPreAnalysisServices(
        scheduler: AnalysisWorkScheduler,
        reconciler: AnalysisJobReconciler
    ) {
        self.analysisWorkScheduler = scheduler
        self.analysisJobReconciler = reconciler
        logger.info("Pre-analysis services injected")
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
            guard let self, let processingTask = task as? BGProcessingTask else {
                task.setTaskCompleted(success: false)
                return
            }
            let sendableTask = UncheckedSendableBox(processingTask)
            Task { await self.handleContinuedProcessingTask(sendableTask.value) }
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

        do {
            try taskScheduler.submit(request)
            logger.info("Backfill task scheduled")
        } catch {
            logger.error("Failed to schedule backfill task: \(error)")
        }
    }

    /// Schedule a BGContinuedProcessingTask for user-initiated long-running
    /// work (e.g., initial model download).
    func scheduleContinuedProcessing(reason: String) {
        let request = BGProcessingTaskRequest(identifier: BackgroundTaskID.continuedProcessing)
        request.requiresNetworkConnectivity = true
        request.requiresExternalPower = false

        do {
            try taskScheduler.submit(request)
            logger.info("Continued processing task scheduled: \(reason)")
        } catch {
            logger.error("Failed to schedule continued processing: \(error)")
        }
    }

    // MARK: - Background Task Completion Guard

    /// Safely complete a BGProcessingTask, guarding against double-completion.
    /// Both the work path and expiration handler call this; only the first wins.
    /// Tracked per-task so overlapping handlers cannot silently drop each
    /// other's completions.
    func markComplete(_ task: any BackgroundProcessingTaskProtocol, success: Bool) {
        let id = ObjectIdentifier(task as AnyObject)
        guard !completedTaskIDs.contains(id) else {
            logger.debug("BGTask completion already called, ignoring duplicate")
            return
        }
        completedTaskIDs.insert(id)
        // Clean up the active-task entry for this BGProcessingTask, if any.
        activeBackgroundTasks.removeValue(forKey: id)
        task.setTaskCompleted(success: success)
    }

    // MARK: - Background Task Handlers

    /// Handle the backfill BGProcessingTask. Runs analysis on episodes that
    /// have incomplete transcription or ad detection.
    func handleBackfillTask(_ task: any BackgroundProcessingTaskProtocol) {
        logger.info("Backfill task started")

        let taskID = ObjectIdentifier(task as AnyObject)

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
                await self?.handleExpiredProcessingTask(task)
            }
        }
    }

    /// Handle the continued processing BGProcessingTask.
    ///
    /// This identifier is reserved for user-initiated long-running work such
    /// as the initial Foundation Models asset download (see AssetProvider).
    /// It is intentionally NOT a backfill drain path — `handleBackfillTask`
    /// already covers deferred backfill via `runPendingBackfill`.
    ///
    /// Today there is no user-facing continuation work path that actually
    /// enqueues this task from the foreground, and no "model download future"
    /// is exposed by AssetProvider that we could await here. Rather than
    /// piggyback on `runPendingBackfill` (which duplicates the backfill path
    /// and misuses the continuation semantics), this handler is a deliberate
    /// no-op that marks the task complete. The task registration is kept
    /// alive so a future wiring of the real continuation work can slot in
    /// without touching registration.
    ///
    /// TODO: When AssetProvider exposes an awaitable model-download future,
    /// call it here and report success/failure from its result.
    /// See bead for design: BGContinuedProcessingTask rewiring.
    func handleContinuedProcessingTask(_ task: any BackgroundProcessingTaskProtocol) {
        logger.info("Continued processing task started (no-op: no continuation work wired)")

        // Honest no-op: there is no continuation work to perform today.
        // Mark complete so iOS does not treat this as a hang, and return
        // success=true because "nothing to do" is not a failure.
        markComplete(task, success: true)
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

        // Schedule the next occurrence.
        schedulePreAnalysisRecovery()

        guard let reconciler = analysisJobReconciler else {
            logger.warning("Pre-analysis recovery: no reconciler available")
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
        do {
            try taskScheduler.submit(request)
            logger.info("Scheduled pre-analysis recovery task")
        } catch {
            logger.error("Failed to schedule pre-analysis recovery: \(error)")
        }
    }
}
