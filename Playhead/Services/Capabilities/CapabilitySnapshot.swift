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
}

// MARK: - ThermalState

/// Mirrors ProcessInfo.ThermalState as a Codable enum.
enum ThermalState: Int, Codable, Sendable, Equatable {
    case nominal
    case fair
    case serious
    case critical

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
