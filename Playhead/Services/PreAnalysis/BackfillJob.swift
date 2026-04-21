// BackfillJob.swift
// Persisted phase-3 FM backfill work with resumable checkpoints.

import Foundation

enum BackfillJobPhase: String, Codable, Sendable, Hashable, CaseIterable {
    case fullEpisodeScan
    case scanHarvesterProposals
    case scanLikelyAdSlots
    case scanRandomAuditWindows
    /// ef2.4.7: FM scheduling phase for regions where metadata suggests ad
    /// presence but no strong anchor exists yet. Gated by MetadataActivationConfig.
    case metadataSeededRegion
}

enum BackfillJobStatus: String, Codable, Sendable, Hashable, CaseIterable {
    case queued
    case running
    case deferred
    case complete
    case failed
}

struct BackfillProgressCursor: Codable, Sendable, Equatable, Hashable {
    /// Number of backfill phases that have completed for this job. In
    /// practice this is a 1-bit counter: each job enqueues exactly one
    /// phase, so the field is 0 before `runJob` succeeds and 1 afterward.
    /// The name used to be `processedUnitCount`, which suggested
    /// chunk/unit granularity and misled a production debugging pass.
    /// The on-disk JSON key remains `processedUnitCount` (see `CodingKeys`)
    /// so existing `backfill_jobs.progressCursorJSON` rows stay readable
    /// without a database migration.
    let processedPhaseCount: Int
    let lastProcessedUpperBoundSec: Double?

    private enum CodingKeys: String, CodingKey {
        // Preserve the legacy JSON key for backward compatibility with
        // rows written before the Swift rename. Do NOT change this string.
        case processedPhaseCount = "processedUnitCount"
        case lastProcessedUpperBoundSec
    }

    init(processedPhaseCount: Int, lastProcessedUpperBoundSec: Double? = nil) {
        self.processedPhaseCount = max(0, processedPhaseCount)
        if let value = lastProcessedUpperBoundSec {
            self.lastProcessedUpperBoundSec = max(0, value)
        } else {
            self.lastProcessedUpperBoundSec = nil
        }
    }

    /// Returns a cursor whose fields are the field-wise maximum of `self` and
    /// `other`. Used to defend against backward checkpoint writes (e.g. a
    /// stale resume racing a fresh phase completion). `nil` upper bounds are
    /// treated as the smaller value.
    func monotonic(from other: BackfillProgressCursor) -> BackfillProgressCursor {
        let mergedCount = max(processedPhaseCount, other.processedPhaseCount)
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
            processedPhaseCount: mergedCount,
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
        let start = min(progressCursor?.processedPhaseCount ?? 0, boundedTotal)
        return start..<boundedTotal
    }
}
