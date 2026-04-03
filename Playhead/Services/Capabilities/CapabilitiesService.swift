// CapabilitiesService.swift
// Detects runtime capabilities and publishes changes via AsyncStream.
// Persists a CapabilitySnapshot with each analysis run so failures
// can be diagnosed after the fact.

import Foundation
import OSLog

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - CapabilitiesService

/// Actor that detects device capabilities and publishes changes.
///
/// Consumers subscribe to ``capabilityUpdates`` for reactive capability
/// changes (e.g., thermal throttling, Low Power Mode toggled).
actor CapabilitiesService {
    private let logger = Logger(subsystem: "com.playhead", category: "Capabilities")

    /// The most recent capability snapshot.
    private(set) var currentSnapshot: CapabilitySnapshot

    // AsyncStream plumbing
    private var continuations: [UUID: AsyncStream<CapabilitySnapshot>.Continuation] = [:]

    /// Notification observers kept alive for the actor's lifetime.
    private var observerTokens: [any NSObjectProtocol] = []

    init() {
        self.currentSnapshot = Self.captureSnapshot()
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
        appleIntelligence=\(snapshot.appleIntelligenceEnabled), \
        localeSupported=\(snapshot.foundationModelsLocaleSupported), \
        thermal=\(snapshot.thermalState.description), \
        lowPower=\(snapshot.isLowPowerMode), \
        bgProcessing=\(snapshot.backgroundProcessingSupported), \
        diskSpace=\(snapshot.availableDiskSpaceBytes / (1024 * 1024))MB
        """)

        for (_, continuation) in continuations {
            continuation.yield(snapshot)
        }
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

        observerTokens = [thermalToken, powerToken]
    }

    // MARK: - Snapshot Capture

    /// Captures a point-in-time snapshot of all device capabilities.
    private static func captureSnapshot() -> CapabilitySnapshot {
        let processInfo = ProcessInfo.processInfo

        let foundationModelsAvailable = checkFoundationModelsAvailable()
        let appleIntelligenceEnabled = checkAppleIntelligenceEnabled()
        let localeSupported = checkFoundationModelsLocaleSupported()
        let thermalState = ThermalState(from: processInfo.thermalState)
        let isLowPowerMode = processInfo.isLowPowerModeEnabled
        let backgroundProcessingSupported = checkBackgroundProcessingSupported()
        let availableDiskSpace = queryAvailableDiskSpace()

        return CapabilitySnapshot(
            foundationModelsAvailable: foundationModelsAvailable,
            appleIntelligenceEnabled: appleIntelligenceEnabled,
            foundationModelsLocaleSupported: localeSupported,
            thermalState: thermalState,
            isLowPowerMode: isLowPowerMode,
            backgroundProcessingSupported: backgroundProcessingSupported,
            availableDiskSpaceBytes: availableDiskSpace,
            capturedAt: .now
        )
    }

    // MARK: - Capability Checks

    private static func checkFoundationModelsAvailable() -> Bool {
        #if canImport(FoundationModels)
        if #available(iOS 26.0, *) {
            return SystemLanguageModel.default.isAvailable
        }
        return false
        #else
        return false
        #endif
    }

    private static func checkAppleIntelligenceEnabled() -> Bool {
        // Apple Intelligence availability is gated by the same check as
        // Foundation Models — if the model is available, AI is enabled.
        // There is no separate public API to query the AI toggle directly.
        return checkFoundationModelsAvailable()
    }

    private static func checkFoundationModelsLocaleSupported() -> Bool {
        #if canImport(FoundationModels)
        if #available(iOS 26.0, *) {
            // SystemLanguageModel.isAvailable already factors in locale.
            // If the model reports available, the current locale is supported.
            return SystemLanguageModel.default.isAvailable
        }
        return false
        #else
        return false
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


    /// Remove all registered notification observers and clear the token list.
    private func removeObservers() {
        let center = NotificationCenter.default
        for token in observerTokens {
            center.removeObserver(token)
        }
        observerTokens.removeAll()
    }
}
