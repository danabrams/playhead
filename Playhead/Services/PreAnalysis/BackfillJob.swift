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
    /// playhead-b6jq PR 4 (Phase B2): background phase that runs the on-device
    /// distilled specialist over candidate windows and PERSISTS raw verdicts to
    /// `specialist_scan_results`. Acts on nothing (PR 5 consumes the rows).
    /// Two-key gated in `BackfillJobRunner` (`specialistScanEnabled` AND a
    /// non-nil `SpecialistAdClassifier.Runtime`), so with the shipped defaults
    /// this case is never enqueued and every FM path stays byte-identical. It is
    /// deliberately INERT in the FM recall union: `TargetedWindowNarrower.narrow`
    /// returns `.empty` for it so it contributes nothing to
    /// `predictedTargetedLineRefs`.
    case specialistHostReadScan
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
    /// playhead-x0lb: an ``EpisodeSeconds``, not a bare `Double`, because this
    /// field is the one the whole cursor family of the defect catalogue is
    /// about. Its meaning is an ASSERTION — `[0, x]` of the EPISODE is covered —
    /// and `BackfillJobRunner.narrowedForResume` acts on it by dropping every
    /// segment ending at or below `x` from the next attempt, permanently. A
    /// ``PlanListSeconds`` (the end of the list a run was handed) is the same
    /// number in the same timebase making a much weaker claim, and reading one
    /// as the other is what wrote 53FC53E3's cursor of 2,525.82 on a 2,528 s
    /// episode that had been ad-scanned for 1.4 % of its length.
    ///
    /// The on-disk JSON is UNCHANGED: ``EpisodeSeconds`` codes through a
    /// single-value container, so the field stays a bare number and existing
    /// `backfill_jobs.progressCursor` rows stay readable. `BackfillProgressCursorTests`
    /// pins that against literal JSON captured from the device.
    let lastProcessedUpperBoundSec: EpisodeSeconds?

    private enum CodingKeys: String, CodingKey {
        // Preserve the legacy JSON key for backward compatibility with
        // rows written before the Swift rename. Do NOT change this string.
        case processedPhaseCount = "processedUnitCount"
        case lastProcessedUpperBoundSec
    }

    init(processedPhaseCount: Int, lastProcessedUpperBoundSec: EpisodeSeconds? = nil) {
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
        let mergedUpper: EpisodeSeconds?
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
