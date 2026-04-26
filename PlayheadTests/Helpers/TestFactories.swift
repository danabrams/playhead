// TestFactories.swift
// Convenience factory methods for constructing test data with sensible defaults.

import Foundation
@testable import Playhead

// MARK: - CapabilitySnapshot

func makeCapabilitySnapshot(
    foundationModelsAvailable: Bool = false,
    foundationModelsUsable: Bool? = nil,
    appleIntelligenceEnabled: Bool? = nil,
    foundationModelsLocaleSupported: Bool? = nil,
    thermalState: ThermalState = .nominal,
    isLowPowerMode: Bool = false,
    isCharging: Bool = false
) -> CapabilitySnapshot {
    CapabilitySnapshot(
        foundationModelsAvailable: foundationModelsAvailable,
        foundationModelsUsable: foundationModelsUsable ?? foundationModelsAvailable,
        appleIntelligenceEnabled: appleIntelligenceEnabled ?? foundationModelsAvailable,
        foundationModelsLocaleSupported: foundationModelsLocaleSupported ?? foundationModelsAvailable,
        thermalState: thermalState,
        isLowPowerMode: isLowPowerMode,
        isCharging: isCharging,
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

// MARK: - AnalysisJob

func makeAnalysisJob(
    jobId: String = UUID().uuidString,
    jobType: String = "playback",
    episodeId: String = "ep-1",
    podcastId: String? = nil,
    analysisAssetId: String? = nil,
    workKey: String? = nil,
    sourceFingerprint: String = "fp-test",
    downloadId: String = "dl-1",
    priority: Int = 0,
    desiredCoverageSec: Double = 1800,
    featureCoverageSec: Double = 0,
    transcriptCoverageSec: Double = 0,
    cueCoverageSec: Double = 0,
    state: String = "queued",
    attemptCount: Int = 0,
    nextEligibleAt: Double? = nil,
    leaseOwner: String? = nil,
    leaseExpiresAt: Double? = nil,
    lastErrorCode: String? = nil,
    createdAt: Double = Date().timeIntervalSince1970,
    updatedAt: Double = Date().timeIntervalSince1970,
    generationID: String = "",
    schedulerEpoch: Int = 0
) -> AnalysisJob {
    let resolvedWorkKey = workKey ?? AnalysisJob.computeWorkKey(
        fingerprint: sourceFingerprint,
        analysisVersion: 1,
        jobType: jobType
    )
    return AnalysisJob(
        jobId: jobId,
        jobType: jobType,
        episodeId: episodeId,
        podcastId: podcastId,
        analysisAssetId: analysisAssetId,
        workKey: resolvedWorkKey,
        sourceFingerprint: sourceFingerprint,
        downloadId: downloadId,
        priority: priority,
        desiredCoverageSec: desiredCoverageSec,
        featureCoverageSec: featureCoverageSec,
        transcriptCoverageSec: transcriptCoverageSec,
        cueCoverageSec: cueCoverageSec,
        state: state,
        attemptCount: attemptCount,
        nextEligibleAt: nextEligibleAt,
        leaseOwner: leaseOwner,
        leaseExpiresAt: leaseExpiresAt,
        lastErrorCode: lastErrorCode,
        createdAt: createdAt,
        updatedAt: updatedAt,
        generationID: generationID,
        schedulerEpoch: schedulerEpoch
    )
}

// MARK: - BackfillJob

func makeBackfillJob(
    jobId: String = UUID().uuidString,
    analysisAssetId: String = "asset-1",
    podcastId: String? = "podcast-1",
    phase: BackfillJobPhase = .fullEpisodeScan,
    coveragePolicy: CoveragePolicy = .fullCoverage,
    priority: Int = 0,
    progressCursor: BackfillProgressCursor? = nil,
    retryCount: Int = 0,
    deferReason: String? = nil,
    status: BackfillJobStatus = .queued,
    scanCohortJSON: String? = nil,
    createdAt: Double = Date().timeIntervalSince1970
) -> BackfillJob {
    BackfillJob(
        jobId: jobId,
        analysisAssetId: analysisAssetId,
        podcastId: podcastId,
        phase: phase,
        coveragePolicy: coveragePolicy,
        priority: priority,
        progressCursor: progressCursor,
        retryCount: retryCount,
        deferReason: deferReason,
        status: status,
        scanCohortJSON: scanCohortJSON,
        createdAt: createdAt
    )
}
