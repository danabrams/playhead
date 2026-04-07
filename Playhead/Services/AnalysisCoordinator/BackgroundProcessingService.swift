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
// Thermal management:
//   .nominal/.fair  -> full analysis
//   .serious        -> reduce hot-path window, pause backfill
//   .critical       -> pause all analysis
//
// Battery management:
//   Below 20% and not charging -> pause all non-critical analysis
//   Low Power Mode             -> reduce hot-path lookahead, defer backfill

import BackgroundTasks
import Foundation
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
    func start() async
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

    /// Whether all analysis is paused (critical thermal or low battery).
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
    nonisolated func registerBackgroundTasks() {
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
    func start() async {
        await updateBatteryState()

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

        Task { await coordinator.start() }
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
            let decision = self.deviceAdmissionDecision(for: snapshot)
            guard decision == .admit else {
                let reason: String
                switch decision {
                case .admit:
                    reason = "admit"
                case .deferred(let deferReason):
                    reason = deferReason.rawValue
                }
                self.logger.info("Backfill skipped: \(reason, privacy: .public)")
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
            // this handler called coordinator.start(), which is a fire-and-
            // forget capability-observer setup that returned in microseconds
            // and let iOS reclaim the granted background time without any
            // analysis happening. See git blame for context.
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

        // `shouldPauseBackfill` mirrors the shared DeviceAdmissionPolicy gate
        // (thermal throttle, low battery while not charging, or Low Power
        // Mode), so this service and AdmissionController stay in sync.
        // `shouldPauseAll` is BPS-specific service logic layered on top: it
        // pauses every analysis path (including the foreground hot-path) when
        // the device is in distress.
        let shouldPauseAll = snapshot.thermalState == .critical || isBatteryTooLow()
        let shouldPauseBackfill = deviceAdmissionDecision(for: snapshot) != .admit

        // Pause all analysis.
        if shouldPauseAll && !allAnalysisPaused {
            allAnalysisPaused = true
            backfillPaused = true
            logger.warning("All analysis paused (thermal=\(snapshot.thermalState.rawValue), battery=\(self.currentBatteryLevel))")

            Task {
                await coordinator.stop()
            }
        }

        // Resume from full pause.
        if !shouldPauseAll && allAnalysisPaused {
            allAnalysisPaused = false
            logger.info("Analysis pause lifted (thermal=\(snapshot.thermalState.rawValue))")

            // Re-activate hot-path if playback is active.
            if hotPathActive {
                Task { await coordinator.start() }
            }
        }

        // Pause/resume backfill independently.
        if shouldPauseBackfill && !backfillPaused {
            backfillPaused = true
            logger.info("Backfill paused (thermal=\(snapshot.thermalState.rawValue), lowPower=\(snapshot.isLowPowerMode))")
        }

        if !shouldPauseBackfill && backfillPaused {
            backfillPaused = false
            logger.info("Backfill resumed")
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

    /// Whether battery is below threshold and device is not charging.
    /// The threshold is sourced from `DeviceAdmissionPolicy` so this service
    /// and `AdmissionController` cannot drift on the cutoff.
    private func isBatteryTooLow() -> Bool {
        currentBatteryLevel >= 0
            && currentBatteryLevel < DeviceAdmissionPolicy.lowBatteryThreshold
            && !isCharging
    }

    /// Evaluate the shared `DeviceAdmissionPolicy` against the latest
    /// capability snapshot, using the BPS-cached battery and charging state
    /// (which is refreshed from the battery provider on every capability
    /// update). The returned decision answers "may backfill-class work run
    /// right now?" — service-specific gates (e.g. `shouldPauseAll`) are
    /// layered on top by callers.
    private func deviceAdmissionDecision(for snapshot: CapabilitySnapshot) -> DeviceAdmissionPolicy.Decision {
        let effectiveSnapshot = CapabilitySnapshot(
            foundationModelsAvailable: snapshot.foundationModelsAvailable,
            foundationModelsUsable: snapshot.foundationModelsUsable,
            appleIntelligenceEnabled: snapshot.appleIntelligenceEnabled,
            foundationModelsLocaleSupported: snapshot.foundationModelsLocaleSupported,
            thermalState: snapshot.thermalState,
            isLowPowerMode: isLowPowerMode,
            isCharging: isCharging,
            backgroundProcessingSupported: snapshot.backgroundProcessingSupported,
            availableDiskSpaceBytes: snapshot.availableDiskSpaceBytes,
            capturedAt: snapshot.capturedAt
        )
        return DeviceAdmissionPolicy.evaluate(
            snapshot: effectiveSnapshot,
            batteryLevel: currentBatteryLevel
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
                await self?.analysisWorkScheduler?.cancelCurrentJob()
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
