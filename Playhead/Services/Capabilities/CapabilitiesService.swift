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

    init(
        available: Bool,
        appleIntelligenceEnabled: Bool,
        localeSupported: Bool
    ) {
        self.available = available
        self.appleIntelligenceEnabled = appleIntelligenceEnabled
        self.localeSupported = localeSupported
    }

    #if canImport(FoundationModels)
    @available(iOS 26.0, *)
    init(availability: SystemLanguageModel.Availability, localeSupported: Bool) {
        self.localeSupported = localeSupported

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
            return FoundationModelsCapabilityState(
                availability: model.availability,
                localeSupported: model.supportsLocale()
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
