// CoveragePlannerTests.swift
// Regression tests for phase 3 coverage-policy selection.

import Testing

@testable import Playhead

private func makeCoveragePlannerContext(
    observedEpisodeCount: Int = 6,
    stablePrecision: Bool = true,
    isFirstEpisodeAfterCohortInvalidation: Bool = false,
    recallDegrading: Bool = false,
    sponsorDriftDetected: Bool = false,
    auditMissDetected: Bool = false,
    episodesSinceLastFullRescan: Int = 1,
    periodicFullRescanIntervalEpisodes: Int = 10
) -> CoveragePlannerContext {
    CoveragePlannerContext(
        observedEpisodeCount: observedEpisodeCount,
        stablePrecision: stablePrecision,
        isFirstEpisodeAfterCohortInvalidation: isFirstEpisodeAfterCohortInvalidation,
        recallDegrading: recallDegrading,
        sponsorDriftDetected: sponsorDriftDetected,
        auditMissDetected: auditMissDetected,
        episodesSinceLastFullRescan: episodesSinceLastFullRescan,
        periodicFullRescanIntervalEpisodes: periodicFullRescanIntervalEpisodes
    )
}

@Suite("CoveragePlanner")
struct CoveragePlannerTests {

    @Test("fullCoverage selected for cold-start, invalidation, recall degradation, and audit misses")
    func testFullCoverageTriggers() {
        let planner = CoveragePlanner()
        let cases = [
            makeCoveragePlannerContext(observedEpisodeCount: 4),
            makeCoveragePlannerContext(isFirstEpisodeAfterCohortInvalidation: true),
            makeCoveragePlannerContext(recallDegrading: true),
            makeCoveragePlannerContext(auditMissDetected: true),
        ]

        for context in cases {
            let plan = planner.plan(for: context)
            #expect(plan.policy == .fullCoverage)
            #expect(plan.phases == [.fullEpisodeScan])
            #expect(plan.auditWindowSampleRate == nil)
        }
    }

    @Test("mature stable shows use targetedWithAudit with mandatory audit sampling")
    func testTargetedWithAuditForMatureStableShows() throws {
        let planner = CoveragePlanner()
        let plan = planner.plan(for: makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            stablePrecision: true,
            episodesSinceLastFullRescan: 3
        ))

        #expect(plan.policy == .targetedWithAudit)
        #expect(plan.phases == [
            .scanHarvesterProposals,
            .scanLikelyAdSlots,
            .scanRandomAuditWindows,
        ])
        let sampleRate = try #require(plan.auditWindowSampleRate)
        #expect(sampleRate == CoveragePlanner.defaultAuditWindowSampleRate)
        #expect(sampleRate >= 0.10 && sampleRate <= 0.15)
    }

    @Test("periodicFullRescan triggers on episode interval or sponsor drift")
    func testPeriodicFullRescanTriggers() {
        let planner = CoveragePlanner()
        let cases = [
            makeCoveragePlannerContext(episodesSinceLastFullRescan: 10, periodicFullRescanIntervalEpisodes: 10),
            makeCoveragePlannerContext(sponsorDriftDetected: true),
        ]

        for context in cases {
            let plan = planner.plan(for: context)
            #expect(plan.policy == .periodicFullRescan)
            #expect(plan.phases == [.fullEpisodeScan])
            #expect(plan.auditWindowSampleRate == nil)
        }
    }
}
