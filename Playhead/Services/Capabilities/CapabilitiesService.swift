// CapabilitiesService.swift
// Detects runtime capabilities and publishes changes via AsyncStream.
// Persists a CapabilitySnapshot with each analysis run so failures
// can be diagnosed after the fact.

import Foundation
import OSLog
import UIKit

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - CapabilitiesProviding

/// Protocol abstraction for capability queries, enabling test stubs.
protocol CapabilitiesProviding: Sendable {
    var currentSnapshot: CapabilitySnapshot { get async }
    func capabilityUpdates() async -> AsyncStream<CapabilitySnapshot>
}

struct FoundationModelsCapabilityState: Sendable, Equatable {
    let available: Bool
    let appleIntelligenceEnabled: Bool
    let localeSupported: Bool

    /// The on-device model's context window size in tokens.
    ///
    /// playhead-xx7m.2 (Phase B): threaded durably through the capability
    /// layer so a real-device run confirms the iOS 27 model reports the
    /// expected ~32k (vs iOS 26's 4096). The ad-classifier's per-window
    /// prompt budget scales linearly with this value, so it drives the
    /// boundary-undersizing retune. Holds the raw
    /// `SystemLanguageModel.default.contextSize`; `0` only when the OS predates
    /// iOS 26 or the compiler predates the API. It reflects the API's own value
    /// regardless of `availability`, so on the simulator it may read 0 or 4096
    /// (model unavailable / warming) — that 4096 is the API's base value, NOT
    /// the classifier's 4096 budget fallback (`fallbackFoundationModelContextSize`,
    /// which lives in the classifier's math, not here).
    let contextSize: Int

    init(
        available: Bool,
        appleIntelligenceEnabled: Bool,
        localeSupported: Bool,
        contextSize: Int = 0
    ) {
        self.available = available
        self.appleIntelligenceEnabled = appleIntelligenceEnabled
        self.localeSupported = localeSupported
        self.contextSize = contextSize
    }

    #if canImport(FoundationModels)
    @available(iOS 26.0, *)
    init(
        availability: SystemLanguageModel.Availability,
        localeSupported: Bool,
        contextSize: Int = 0
    ) {
        self.localeSupported = localeSupported
        self.contextSize = contextSize

        switch availability {
        case .available:
            self.available = true
            self.appleIntelligenceEnabled = true
        case .unavailable(.modelNotReady):
            self.available = false
            self.appleIntelligenceEnabled = true
        case .unavailable(.appleIntelligenceNotEnabled), .unavailable(.deviceNotEligible):
            self.available = false
            self.appleIntelligenceEnabled = false
        @unknown default:
            self.available = false
            self.appleIntelligenceEnabled = false
        }
    }
    #endif
}

// MARK: - CapabilitiesService

/// Actor that detects device capabilities and publishes changes.
///
/// Consumers subscribe to ``capabilityUpdates`` for reactive capability
/// changes (e.g., thermal throttling, Low Power Mode toggled).
actor CapabilitiesService {
    private let logger = Logger(subsystem: "com.playhead", category: "Capabilities")

    /// The most recent capability snapshot.
    private(set) var currentSnapshot: CapabilitySnapshot

    /// At most one readiness probe may run at a time.
    private var foundationModelsProbeTask: Task<Void, Never>?

    // AsyncStream plumbing
    private var continuations: [UUID: AsyncStream<CapabilitySnapshot>.Continuation] = [:]

    /// Notification observers kept alive for the actor's lifetime.
    private var observerTokens: [any NSObjectProtocol] = []

    init() {
        self.currentSnapshot = Self.captureSnapshot()
        Task { await self.refreshSnapshot() }
    }

    // MARK: - Public API

    /// Takes a fresh snapshot, logs it, and publishes to all subscribers.
    /// Call this at first launch and whenever a significant state change occurs.
    func refreshSnapshot() {
        let snapshot = Self.captureSnapshot()
        currentSnapshot = snapshot

        logger.info("""
        Capability snapshot captured: \
        foundationModels=\(snapshot.foundationModelsAvailable), \
        foundationModelsUsable=\(snapshot.foundationModelsUsable), \
        appleIntelligence=\(snapshot.appleIntelligenceEnabled), \
        localeSupported=\(snapshot.foundationModelsLocaleSupported), \
        contextSize=\(snapshot.foundationModelsContextSize), \
        thermal=\(snapshot.thermalState.description), \
        lowPower=\(snapshot.isLowPowerMode), \
        charging=\(snapshot.isCharging), \
        bgProcessing=\(snapshot.backgroundProcessingSupported), \
        diskSpace=\(snapshot.availableDiskSpaceBytes / (1024 * 1024))MB
        """)

        for (_, continuation) in continuations {
            continuation.yield(snapshot)
        }

        scheduleFoundationModelsProbeIfNeeded(for: snapshot)
    }

    /// Returns an AsyncStream that emits capability snapshots whenever
    /// device state changes (thermal, power mode, etc.).
    func capabilityUpdates() -> AsyncStream<CapabilitySnapshot> {
        let id = UUID()
        return AsyncStream { continuation in
            // Yield current state immediately so consumers don't wait.
            continuation.yield(currentSnapshot)

            continuation.onTermination = { [weak self] _ in
                guard let self else { return }
                Task { await self.removeContinuation(id: id) }
            }

            Task { await self.storeContinuation(id: id, continuation: continuation) }
        }
    }

    /// Starts observing system notifications for capability-relevant changes.
    /// Safe to call multiple times — removes old observers before adding new ones.
    func startObserving() {
        UIDevice.current.isBatteryMonitoringEnabled = true

        // Remove any previously registered observers to prevent leaks on double-call.
        removeObservers()

        let center = NotificationCenter.default

        let thermalToken = center.addObserver(
            forName: ProcessInfo.thermalStateDidChangeNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            guard let self else { return }
            Task { await self.refreshSnapshot() }
        }

        let powerToken = center.addObserver(
            forName: Notification.Name.NSProcessInfoPowerStateDidChange,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            guard let self else { return }
            Task { await self.refreshSnapshot() }
        }

        let batteryToken = center.addObserver(
            forName: UIDevice.batteryStateDidChangeNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            guard let self else { return }
            Task { await self.refreshSnapshot() }
        }

        observerTokens = [thermalToken, powerToken, batteryToken]
        refreshSnapshot()
    }

    // MARK: - Snapshot Capture

    /// Captures a point-in-time snapshot of all device capabilities.
    private static func captureSnapshot() -> CapabilitySnapshot {
        let processInfo = ProcessInfo.processInfo

        let modelState = checkFoundationModelsState()
        let thermalState = ThermalState(from: processInfo.thermalState)
        let isLowPowerMode = processInfo.isLowPowerModeEnabled
        let backgroundProcessingSupported = checkBackgroundProcessingSupported()
        let availableDiskSpace = queryAvailableDiskSpace()

        let batteryState = UIDevice.current.batteryState
        let isCharging = batteryState == .charging || batteryState == .full

        return CapabilitySnapshot(
            foundationModelsAvailable: modelState.available,
            foundationModelsUsable: FoundationModelsUsabilityProbe.cachedUsability() ?? false,
            appleIntelligenceEnabled: modelState.appleIntelligenceEnabled,
            foundationModelsLocaleSupported: modelState.localeSupported,
            foundationModelsContextSize: modelState.contextSize,
            thermalState: thermalState,
            isLowPowerMode: isLowPowerMode,
            isCharging: isCharging,
            backgroundProcessingSupported: backgroundProcessingSupported,
            availableDiskSpaceBytes: availableDiskSpace,
            capturedAt: .now
        )
    }

    // MARK: - Capability Checks

    private static func checkFoundationModelsState() -> FoundationModelsCapabilityState {
        #if canImport(FoundationModels)
        if #available(iOS 26.0, *) {
            let model = SystemLanguageModel.default
            // playhead-xx7m.2 (Phase B): read the on-device model's context
            // window so a real-device run confirms the iOS 27 model reports the
            // expected ~32k (vs iOS 26's 4096). The ad-classifier's window
            // budget scales linearly with this, so it drives the boundary-
            // undersizing retune. FM only runs on device, so this is the
            // measurement hook — one clean breadcrumb, then the value is
            // threaded durably into the capability state below. `contextSize`
            // is unavailable on compilers predating the API, in which case it
            // stays 0 (the classifier keeps its own 4096 budget fallback).
            #if compiler(>=6.3)
            let contextSize = model.contextSize
            Logger(subsystem: "com.playhead", category: "Capabilities")
                .notice("fm.capability.context_window contextSize=\(contextSize, privacy: .public)")
            #else
            let contextSize = 0
            #endif
            return FoundationModelsCapabilityState(
                availability: model.availability,
                localeSupported: model.supportsLocale(),
                contextSize: contextSize
            )
        }
        return FoundationModelsCapabilityState(
            available: false,
            appleIntelligenceEnabled: false,
            localeSupported: false
        )
        #else
        return FoundationModelsCapabilityState(
            available: false,
            appleIntelligenceEnabled: false,
            localeSupported: false
        )
        #endif
    }

    private static func checkBackgroundProcessingSupported() -> Bool {
        // BGTaskScheduler is always available on iOS 13+.
        // Registration success is the real gate, but we report platform support here.
        return true
    }

    private static func queryAvailableDiskSpace() -> Int64 {
        let fileManager = FileManager.default
        guard let homeURL = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first else {
            return 0
        }
        do {
            let values = try homeURL.resourceValues(forKeys: [.volumeAvailableCapacityForImportantUsageKey])
            return values.volumeAvailableCapacityForImportantUsage ?? 0
        } catch {
            return 0
        }
    }

    // MARK: - Internal Helpers

    private func storeContinuation(id: UUID, continuation: AsyncStream<CapabilitySnapshot>.Continuation) {
        continuations[id] = continuation
    }

    private func removeContinuation(id: UUID) {
        continuations.removeValue(forKey: id)
    }

    private func scheduleFoundationModelsProbeIfNeeded(for snapshot: CapabilitySnapshot) {
        guard snapshot.foundationModelsAvailable,
              snapshot.appleIntelligenceEnabled,
              snapshot.foundationModelsLocaleSupported,
              !snapshot.foundationModelsUsable,
              FoundationModelsUsabilityProbe.cachedUsability() == nil,
              foundationModelsProbeTask == nil else {
            return
        }

        foundationModelsProbeTask = Task { [weak self] in
            #if canImport(FoundationModels)
            if #available(iOS 26.0, *) {
                _ = await FoundationModelsUsabilityProbe.probeIfNeeded(logger: self?.logger ?? Logger(subsystem: "com.playhead", category: "Capabilities"))
            }
            #endif

            guard let self else { return }
            await self.finishFoundationModelsProbe()
        }
    }

    private func finishFoundationModelsProbe() {
        foundationModelsProbeTask = nil
        refreshSnapshot()
    }

    /// Remove all registered notification observers and clear the token list.
    private func removeObservers() {
        let center = NotificationCenter.default
        for token in observerTokens {
            center.removeObserver(token)
        }
        observerTokens.removeAll()
    }
}

// MARK: - CapabilitiesProviding Conformance

extension CapabilitiesService: CapabilitiesProviding {}
