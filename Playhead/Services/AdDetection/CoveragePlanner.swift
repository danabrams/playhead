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

    init(
        coldStartEpisodeThreshold: Int = CoveragePlanner.defaultColdStartEpisodeThreshold,
        periodicFullRescanIntervalEpisodes: Int = CoveragePlanner.defaultPeriodicFullRescanIntervalEpisodes,
        auditWindowSampleRate: Double = CoveragePlanner.defaultAuditWindowSampleRate
    ) {
        self.coldStartEpisodeThreshold = coldStartEpisodeThreshold
        self.periodicFullRescanIntervalEpisodes = periodicFullRescanIntervalEpisodes
        self.auditWindowSampleRate = min(max(auditWindowSampleRate, 0.10), 0.15)
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
