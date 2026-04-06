// CoveragePlanner.swift
// Selects the phase-3 FM backfill scan policy for a show before scanning.

import Foundation

enum CoveragePolicy: String, Codable, Sendable, Hashable, CaseIterable {
    case fullCoverage
    case targetedWithAudit
    case periodicFullRescan
}

struct CoveragePlannerContext: Sendable, Equatable {
    let observedEpisodeCount: Int
    let stablePrecision: Bool
    let isFirstEpisodeAfterCohortInvalidation: Bool
    let recallDegrading: Bool
    let sponsorDriftDetected: Bool
    let auditMissDetected: Bool
    let episodesSinceLastFullRescan: Int
    let periodicFullRescanIntervalEpisodes: Int
}

struct CoveragePlan: Sendable, Equatable {
    let policy: CoveragePolicy
    let phases: [BackfillJobPhase]
    let auditWindowSampleRate: Double?
}

struct CoveragePlanner: Sendable {
    static let defaultColdStartEpisodeThreshold = 5
    static let defaultPeriodicFullRescanIntervalEpisodes = 10
    static let defaultAuditWindowSampleRate = 0.12

    let coldStartEpisodeThreshold: Int
    let periodicFullRescanIntervalEpisodes: Int
    let auditWindowSampleRate: Double

    /// Constructs a planner.
    ///
    /// - Parameter auditWindowSampleRate: Fraction of episode duration that
    ///   `targetedWithAudit` plans dedicate to random audit windows. The value
    ///   is **hard-clamped** to the inclusive range `[0.10, 0.15]`. The lower
    ///   bound preserves enough audit coverage to detect harvester drift; the
    ///   upper bound caps FM token spend per episode (~15% audit, ~85%
    ///   targeted). Values outside this range are silently clamped — callers
    ///   should treat anything outside `[0.10, 0.15]` as a configuration bug.
    init(
        coldStartEpisodeThreshold: Int = CoveragePlanner.defaultColdStartEpisodeThreshold,
        periodicFullRescanIntervalEpisodes: Int = CoveragePlanner.defaultPeriodicFullRescanIntervalEpisodes,
        auditWindowSampleRate: Double = CoveragePlanner.defaultAuditWindowSampleRate
    ) {
        self.coldStartEpisodeThreshold = coldStartEpisodeThreshold
        self.periodicFullRescanIntervalEpisodes = periodicFullRescanIntervalEpisodes
        self.auditWindowSampleRate = min(max(auditWindowSampleRate, 0.10), 0.15)
    }

    /// Acknowledges that a `periodicFullRescan` (or any full coverage scan)
    /// has been executed by returning a context with
    /// `episodesSinceLastFullRescan` zeroed.
    ///
    /// **Caller contract:** after executing a full rescan plan returned by
    /// ``plan(for:)``, the caller MUST record the rescan via this method
    /// before planning the next episode. Without it the planner will keep
    /// emitting `periodicFullRescan` on every subsequent call once the
    /// threshold has been crossed, deadlocking the policy loop.
    func reset(context: CoveragePlannerContext) -> CoveragePlannerContext {
        CoveragePlannerContext(
            observedEpisodeCount: context.observedEpisodeCount,
            stablePrecision: context.stablePrecision,
            isFirstEpisodeAfterCohortInvalidation: context.isFirstEpisodeAfterCohortInvalidation,
            recallDegrading: context.recallDegrading,
            sponsorDriftDetected: context.sponsorDriftDetected,
            auditMissDetected: context.auditMissDetected,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: context.periodicFullRescanIntervalEpisodes
        )
    }

    func plan(for context: CoveragePlannerContext) -> CoveragePlan {
        if shouldUseFullCoverage(context) {
            return CoveragePlan(
                policy: .fullCoverage,
                phases: [.fullEpisodeScan],
                auditWindowSampleRate: nil
            )
        }

        if shouldUsePeriodicFullRescan(context) {
            return CoveragePlan(
                policy: .periodicFullRescan,
                phases: [.fullEpisodeScan],
                auditWindowSampleRate: nil
            )
        }

        if context.observedEpisodeCount >= coldStartEpisodeThreshold && context.stablePrecision {
            return CoveragePlan(
                policy: .targetedWithAudit,
                phases: [
                    .scanHarvesterProposals,
                    .scanLikelyAdSlots,
                    .scanRandomAuditWindows,
                ],
                auditWindowSampleRate: auditWindowSampleRate
            )
        }

        return CoveragePlan(
            policy: .fullCoverage,
            phases: [.fullEpisodeScan],
            auditWindowSampleRate: nil
        )
    }

    private func shouldUseFullCoverage(_ context: CoveragePlannerContext) -> Bool {
        context.observedEpisodeCount < coldStartEpisodeThreshold ||
        context.isFirstEpisodeAfterCohortInvalidation ||
        context.recallDegrading ||
        context.auditMissDetected
    }

    private func shouldUsePeriodicFullRescan(_ context: CoveragePlannerContext) -> Bool {
        context.sponsorDriftDetected ||
        context.episodesSinceLastFullRescan >= max(
            1,
            context.periodicFullRescanIntervalEpisodes > 0
                ? context.periodicFullRescanIntervalEpisodes
                : periodicFullRescanIntervalEpisodes
        )
    }
}
