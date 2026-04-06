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
        if let value = lastProcessedUpperBoundSec {
            self.lastProcessedUpperBoundSec = max(0, value)
        } else {
            self.lastProcessedUpperBoundSec = nil
        }
    }

    /// Returns a cursor whose fields are the field-wise maximum of `self` and
    /// `other`. Used to defend against backward checkpoint writes (e.g. a
    /// stale resume racing a fresh chunk completion). `nil` upper bounds are
    /// treated as the smaller value.
    func monotonic(from other: BackfillProgressCursor) -> BackfillProgressCursor {
        let mergedCount = max(processedUnitCount, other.processedUnitCount)
        let mergedUpper: Double?
        switch (lastProcessedUpperBoundSec, other.lastProcessedUpperBoundSec) {
        case let (lhs?, rhs?):
            mergedUpper = max(lhs, rhs)
        case let (lhs?, nil):
            mergedUpper = lhs
        case let (nil, rhs?):
            mergedUpper = rhs
        case (nil, nil):
            mergedUpper = nil
        }
        return BackfillProgressCursor(
            processedUnitCount: mergedCount,
            lastProcessedUpperBoundSec: mergedUpper
        )
    }
}

struct BackfillJob: Sendable, Equatable {
    let jobId: String
    let analysisAssetId: String
    let podcastId: String?
    let phase: BackfillJobPhase
    let coveragePolicy: CoveragePolicy
    let priority: Int
    let progressCursor: BackfillProgressCursor?
    let retryCount: Int
    let deferReason: String?
    let status: BackfillJobStatus
    let scanCohortJSON: String?
    let createdAt: Double

    func remainingUnitRange(totalUnits: Int) -> Range<Int> {
        let boundedTotal = max(0, totalUnits)
        let start = min(progressCursor?.processedUnitCount ?? 0, boundedTotal)
        return start..<boundedTotal
    }
}
