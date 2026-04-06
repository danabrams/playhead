// BackfillJob.swift
// Persisted phase-3 FM backfill work with resumable checkpoints.

import Foundation

enum BackfillJobPhase: String, Codable, Sendable, Hashable, CaseIterable {
    case fullEpisodeScan
    case scanHarvesterProposals
    case scanLikelyAdSlots
    case scanRandomAuditWindows
}

enum BackfillJobStatus: String, Codable, Sendable, Hashable, CaseIterable {
    case queued
    case running
    case deferred
    case complete
    case failed
}

struct BackfillProgressCursor: Codable, Sendable, Equatable, Hashable {
    let processedUnitCount: Int
    let lastProcessedUpperBoundSec: Double?

    init(processedUnitCount: Int, lastProcessedUpperBoundSec: Double? = nil) {
        self.processedUnitCount = max(0, processedUnitCount)
        self.lastProcessedUpperBoundSec = lastProcessedUpperBoundSec
    }
}

struct BackfillJob: Sendable, Equatable {
    let jobId: String
    let analysisAssetId: String
    let podcastId: String
    let phase: BackfillJobPhase
    let coveragePolicy: CoveragePolicy
    let priority: Int
    let progressCursor: BackfillProgressCursor?
    let retryCount: Int
    let deferReason: String?
    let status: BackfillJobStatus
    let scanCohortJSON: String?
    let decisionCohortJSON: String?
    let createdAt: Double

    func remainingUnitRange(totalUnits: Int) -> Range<Int> {
        let boundedTotal = max(0, totalUnits)
        let start = min(progressCursor?.processedUnitCount ?? 0, boundedTotal)
        return start..<boundedTotal
    }
}
