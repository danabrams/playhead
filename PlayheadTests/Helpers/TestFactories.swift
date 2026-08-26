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
    createdAt: Double = Date().timeIntervalSince1970,
    attemptTranscriptVersion: String? = nil
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
        createdAt: createdAt,
        attemptTranscriptVersion: attemptTranscriptVersion
    )
}

// MARK: - playhead-bg2n: the row as the STORE writes it

extension SemanticScanResult {
    /// The value a whole-struct round-trip assertion should compare against
    /// after a FIRST write.
    ///
    /// Six fields are decided by ``AnalysisStore``, not by the producer —
    /// and TWO MORE are decided by nobody: `refusalExplanation` and
    /// `permissiveFallbackReason` have no column at all (playhead-iw7q's
    /// enumeration, filed as playhead-807i), so a fetched row always reads nil
    /// for them however they were handed in. This helper models that.
    ///
    /// `firstAttemptAt`, `lastAttemptAt` and `observedStatusesCSV`
    /// (playhead-bg2n), plus `latencyMsTotal`, `latencyMsMax` and
    /// `latencySampleCount` (playhead-6gcy) — because only the store can see
    /// the row already on disk. So `fetched == handedIn`
    /// compares a persisted row against a value that never existed on disk, and
    /// it started failing the moment the store learned to record attempt
    /// history. Using this helper is not a weakening: it PINS the first-write
    /// contract (both timestamps equal the row's own `createdAt`, the set holds
    /// exactly the row's status, and the latency record is ONE sample of the
    /// row's own `latencyMs` — or nothing at all when that is nil), which
    /// nothing asserted before.
    ///
    /// Deliberately FIRST-WRITE only. There is no `asStoredOnReplace` because a
    /// replace's history depends on what was already there, which is the thing
    /// `SemanticScanAttemptHistoryV55MigrationTests` exists to test directly
    /// rather than to encode in a helper both sides could get wrong together.
    func asStoredOnFirstWrite(storeClock: Double? = nil) -> SemanticScanResult {
        let stamped = createdAt ?? storeClock
        return SemanticScanResult(
            id: id,
            analysisAssetId: analysisAssetId,
            windowFirstAtomOrdinal: windowFirstAtomOrdinal,
            windowLastAtomOrdinal: windowLastAtomOrdinal,
            windowStartTime: windowStartTime,
            windowEndTime: windowEndTime,
            scanPass: scanPass,
            transcriptQuality: transcriptQuality,
            disposition: disposition,
            spansJSON: spansJSON,
            status: status,
            attemptCount: attemptCount,
            errorContext: errorContext,
            inputTokenCount: inputTokenCount,
            outputTokenCount: outputTokenCount,
            latencyMs: latencyMs,
            suspendingLatencyMs: suspendingLatencyMs,
            daemonPeersAtStart: daemonPeersAtStart,
            prewarmHit: prewarmHit,
            scanCohortJSON: scanCohortJSON,
            transcriptVersion: transcriptVersion,
            reuseScope: reuseScope,
            runMode: runMode,
            jobPhase: jobPhase,
            // playhead-iw7q: `nil`, NOT the handed-in value. Neither string has
            // a column, so the store drops both and a fetched row always reads
            // nil — modelling them as surviving is exactly the "it went in so
            // it must be on disk" reading this helper exists to prevent.
            // playhead-807i carries whether they should be persisted at all.
            refusalExplanation: nil,
            // …and `verdictProvenance` DOES survive since V61, so it is
            // forwarded.
            verdictProvenance: verdictProvenance,
            permissiveFallbackReason: nil,
            createdAt: stamped,
            scenePhase: scenePhase,
            backfillJobId: backfillJobId,
            firstAttemptAt: stamped,
            lastAttemptAt: stamped,
            observedStatusesCSV: SemanticScanResult.encodeObservedStatuses([status]),
            // playhead-6gcy: a first write is ONE sample. `latencyMs == nil`
            // yields all three nil rather than a zero total — the store must
            // not turn "nobody measured this" into "it was free", so neither
            // may the value this assertion compares against.
            latencyMsTotal: latencyMs,
            latencyMsMax: latencyMs,
            latencySampleCount: latencyMs == nil ? nil : 1
        )
    }
}
