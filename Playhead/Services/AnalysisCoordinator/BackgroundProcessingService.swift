// BackgroundProcessingService.swift
// Manages background task scheduling and foreground hot-path analysis lifecycle.
//
// Strategy:
//   - Hot-path: runs in foreground whenever audio is playing. This is the
//     MVP reliability path -- background work only improves completeness.
//   - BGProcessingTask: registered for idle/charging backfill of episodes.
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

    /// Task for the active background processing expiration handler.
    private var activeBackgroundTask: Task<Void, Never>?

    /// Guards against double-completion of BGProcessingTask. Both the work
    /// completion path and the expiration handler check this before calling
    /// setTaskCompleted.
    private var bgTaskCompleted = false

    /// Current thermal state, cached for decision-making.
    private var currentThermalState: ThermalState = .nominal

    /// Current battery level (0.0-1.0), cached.
    private var currentBatteryLevel: Float = 1.0

    /// Whether the device is currently charging.
    private var isCharging = false

    /// Whether Low Power Mode is active.
    private var isLowPowerMode = false

    // MARK: - Configuration

    /// Battery threshold below which non-critical analysis is paused.
    private static let lowBatteryThreshold: Float = 0.20

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
        activeBackgroundTask?.cancel()
        activeBackgroundTask = nil
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

    /// Schedule a BGProcessingTask for idle/charging backfill.
    func scheduleBackfillIfNeeded() {
        let request = BGProcessingTaskRequest(identifier: BackgroundTaskID.backfillProcessing)
        request.requiresNetworkConnectivity = false
        request.requiresExternalPower = true

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
    func markComplete(_ task: any BackgroundProcessingTaskProtocol, success: Bool) {
        guard !bgTaskCompleted else {
            logger.debug("BGTask completion already called, ignoring duplicate")
            return
        }
        bgTaskCompleted = true
        task.setTaskCompleted(success: success)
    }

    // MARK: - Background Task Handlers

    /// Handle the backfill BGProcessingTask. Runs analysis on episodes that
    /// have incomplete transcription or ad detection.
    func handleBackfillTask(_ task: any BackgroundProcessingTaskProtocol) {
        logger.info("Backfill task started")

        // Reset completion guard for this task invocation.
        bgTaskCompleted = false

        // Schedule the next occurrence.
        scheduleBackfillIfNeeded()

        let workTask = Task {
            // Check constraints before starting.
            let snapshot = await self.capabilitiesService.currentSnapshot
            guard !snapshot.shouldThrottleAnalysis else {
                self.logger.info("Backfill skipped: thermal throttle active")
                await self.markComplete(task, success: true)
                return
            }

            // Delegate backfill work to the coordinator.
            // The coordinator will checkpoint after every feature block and
            // transcript chunk, so expiration is safe at any point.
            await self.coordinator.start()

            // Let the coordinator run until the task expires or completes.
            await self.markComplete(task, success: true)
            self.logger.info("Backfill task completed")
        }

        activeBackgroundTask = workTask

        // Handle expiration: cancel work gracefully. The nonisolated callback
        // hops to the actor via Task to check the completion guard.
        task.expirationHandler = { [weak self] in
            workTask.cancel()
            Task { [weak self] in
                await self?.handleExpiredProcessingTask(task)
            }
        }
    }

    /// Handle the continued processing BGProcessingTask. Only for
    /// user-initiated long-running work.
    func handleContinuedProcessingTask(_ task: any BackgroundProcessingTaskProtocol) {
        logger.info("Continued processing task started")

        // Reset completion guard for this task invocation.
        bgTaskCompleted = false

        let workTask = Task {
            // Continued processing is for user-initiated work like model
            // downloads. The coordinator handles checkpoint/resume.
            await self.coordinator.start()

            await self.markComplete(task, success: true)
            self.logger.info("Continued processing task completed")
        }

        activeBackgroundTask = workTask

        task.expirationHandler = { [weak self] in
            workTask.cancel()
            Task { [weak self] in
                await self?.handleExpiredProcessingTask(task)
            }
        }
    }

    // MARK: - Thermal & Battery Management

    /// React to capability snapshot changes.
    func handleCapabilityUpdate(_ snapshot: CapabilitySnapshot) async {
        let previousThermalState = currentThermalState
        currentThermalState = snapshot.thermalState
        isLowPowerMode = snapshot.isLowPowerMode

        await updateBatteryState()

        let shouldPauseAll = snapshot.thermalState == .critical || isBatteryTooLow()
        let shouldPauseBackfill = snapshot.shouldThrottleAnalysis || isLowPowerMode || isBatteryTooLow()

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
    private func isBatteryTooLow() -> Bool {
        currentBatteryLevel >= 0 && currentBatteryLevel < Self.lowBatteryThreshold && !isCharging
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

        bgTaskCompleted = false

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

        activeBackgroundTask = workTask

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
