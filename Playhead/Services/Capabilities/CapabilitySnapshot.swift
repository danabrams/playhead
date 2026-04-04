// CapabilitySnapshot.swift
// Codable snapshot of device capabilities at a point in time.
// Persisted as JSON in the analysis_assets table alongside each analysis run.

import Foundation

/// A point-in-time record of device capabilities relevant to analysis.
struct CapabilitySnapshot: Codable, Sendable, Equatable {
    /// Whether the Foundation Models framework is available on this device.
    let foundationModelsAvailable: Bool

    /// Whether Apple Intelligence is enabled in system settings.
    let appleIntelligenceEnabled: Bool

    /// Whether the current locale/language is supported by Foundation Models.
    let foundationModelsLocaleSupported: Bool

    /// Device thermal state at snapshot time.
    let thermalState: ThermalState

    /// Whether Low Power Mode is active.
    let isLowPowerMode: Bool

    /// Whether the device is currently charging (or full).
    let isCharging: Bool

    /// Whether background processing tasks are supported.
    let backgroundProcessingSupported: Bool

    /// Available disk space in bytes.
    let availableDiskSpaceBytes: Int64

    /// When this snapshot was taken.
    let capturedAt: Date

    /// Whether analysis should be throttled based on thermal state.
    var shouldThrottleAnalysis: Bool {
        thermalState == .serious || thermalState == .critical
    }

    /// Whether hot-path lookahead should be reduced (Low Power Mode).
    var shouldReduceHotPath: Bool {
        isLowPowerMode
    }

    /// Whether Foundation Models features (banner enrichment) are usable.
    var canUseFoundationModels: Bool {
        foundationModelsAvailable && appleIntelligenceEnabled && foundationModelsLocaleSupported
    }

    /// Whether deferred (T1+) analysis work can run: charging and not thermally throttled.
    var canRunDeferredWork: Bool {
        isCharging && !shouldThrottleAnalysis
    }

    // MARK: - Initializers

    init(
        foundationModelsAvailable: Bool,
        appleIntelligenceEnabled: Bool,
        foundationModelsLocaleSupported: Bool,
        thermalState: ThermalState,
        isLowPowerMode: Bool,
        isCharging: Bool,
        backgroundProcessingSupported: Bool,
        availableDiskSpaceBytes: Int64,
        capturedAt: Date
    ) {
        self.foundationModelsAvailable = foundationModelsAvailable
        self.appleIntelligenceEnabled = appleIntelligenceEnabled
        self.foundationModelsLocaleSupported = foundationModelsLocaleSupported
        self.thermalState = thermalState
        self.isLowPowerMode = isLowPowerMode
        self.isCharging = isCharging
        self.backgroundProcessingSupported = backgroundProcessingSupported
        self.availableDiskSpaceBytes = availableDiskSpaceBytes
        self.capturedAt = capturedAt
    }

    // MARK: - Backward-Compatible Decoding

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        foundationModelsAvailable = try container.decode(Bool.self, forKey: .foundationModelsAvailable)
        appleIntelligenceEnabled = try container.decode(Bool.self, forKey: .appleIntelligenceEnabled)
        foundationModelsLocaleSupported = try container.decode(Bool.self, forKey: .foundationModelsLocaleSupported)
        thermalState = try container.decode(ThermalState.self, forKey: .thermalState)
        isLowPowerMode = try container.decode(Bool.self, forKey: .isLowPowerMode)
        isCharging = try container.decodeIfPresent(Bool.self, forKey: .isCharging) ?? false
        backgroundProcessingSupported = try container.decode(Bool.self, forKey: .backgroundProcessingSupported)
        availableDiskSpaceBytes = try container.decode(Int64.self, forKey: .availableDiskSpaceBytes)
        capturedAt = try container.decode(Date.self, forKey: .capturedAt)
    }
}

// MARK: - ThermalState

/// Mirrors ProcessInfo.ThermalState as a Codable enum.
enum ThermalState: Int, Codable, Sendable, Equatable, CustomStringConvertible {
    case nominal
    case fair
    case serious
    case critical

    /// Human-readable name for logging (e.g. "nominal", "critical").
    var description: String {
        switch self {
        case .nominal: "nominal"
        case .fair: "fair"
        case .serious: "serious"
        case .critical: "critical"
        }
    }

    init(from processThermalState: ProcessInfo.ThermalState) {
        switch processThermalState {
        case .nominal: self = .nominal
        case .fair: self = .fair
        case .serious: self = .serious
        case .critical: self = .critical
        @unknown default: self = .nominal
        }
    }
}
