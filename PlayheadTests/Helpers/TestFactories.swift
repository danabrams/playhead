// TestFactories.swift
// Convenience factory methods for constructing test data with sensible defaults.

import Foundation
@testable import Playhead

// MARK: - CapabilitySnapshot

func makeCapabilitySnapshot(
    thermalState: ThermalState = .nominal,
    isLowPowerMode: Bool = false
) -> CapabilitySnapshot {
    CapabilitySnapshot(
        foundationModelsAvailable: false,
        appleIntelligenceEnabled: false,
        foundationModelsLocaleSupported: false,
        thermalState: thermalState,
        isLowPowerMode: isLowPowerMode,
        backgroundProcessingSupported: true,
        availableDiskSpaceBytes: 10 * 1024 * 1024 * 1024,
        capturedAt: .now
    )
}

// MARK: - AdWindow

func makeAdWindow(
    startTime: Double = 60.0,
    endTime: Double = 90.0,
    confidence: Double = 0.85
) -> AdWindow {
    AdWindow(
        id: UUID().uuidString,
        analysisAssetId: "test-asset",
        startTime: startTime,
        endTime: endTime,
        confidence: confidence,
        boundaryState: AdBoundaryState.acousticRefined.rawValue,
        decisionState: AdDecisionState.candidate.rawValue,
        detectorVersion: "detection-v1",
        advertiser: nil,
        product: nil,
        adDescription: nil,
        evidenceText: nil,
        evidenceStartTime: startTime,
        metadataSource: "none",
        metadataConfidence: nil,
        metadataPromptVersion: nil,
        wasSkipped: false,
        userDismissedBanner: false
    )
}

// MARK: - AnalysisJob (placeholder)
// makeAnalysisJob(overrides:) will be added when the AnalysisJob type
// is introduced by the AnalysisJobRunner bead.

// MARK: - AnalysisRangeRequest (placeholder)
// makeAnalysisRangeRequest(overrides:) will be added when the
// AnalysisRangeRequest type is introduced by the AnalysisWorkScheduler bead.
