// CoveragePlannerTests.swift
// Regression tests for phase 3 coverage-policy selection.

import Testing

@testable import Playhead

private func makeCoveragePlannerContext(
    observedEpisodeCount: Int = 6,
    stableRecall: Bool = true,
    isFirstEpisodeAfterCohortInvalidation: Bool = false,
    recallDegrading: Bool = false,
    sponsorDriftDetected: Bool = false,
    auditMissDetected: Bool = false,
    episodesSinceLastFullRescan: Int = 1,
    periodicFullRescanIntervalEpisodes: Int = 10
) -> CoveragePlannerContext {
    CoveragePlannerContext(
        observedEpisodeCount: observedEpisodeCount,
        stableRecall: stableRecall,
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
            stableRecall: true,
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

    // M12: auditWindowSampleRate clamp documentation + behavior
    @Test("auditWindowSampleRate is clamped into [0.10, 0.15]")
    func testAuditWindowSampleRateClamp() {
        let high = CoveragePlanner(auditWindowSampleRate: 0.5)
        #expect(high.auditWindowSampleRate == 0.15)

        let low = CoveragePlanner(auditWindowSampleRate: 0.0)
        #expect(low.auditWindowSampleRate == 0.10)

        let inRange = CoveragePlanner(auditWindowSampleRate: 0.12)
        #expect(inRange.auditWindowSampleRate == 0.12)
    }

    // M11/H8: reset feedback loop for periodic rescan
    @Test("reset(context:) zeroes episodesSinceLastFullRescan")
    func testResetClearsEpisodesSinceLastFullRescan() {
        let planner = CoveragePlanner()
        let context = makeCoveragePlannerContext(
            episodesSinceLastFullRescan: 10,
            periodicFullRescanIntervalEpisodes: 10
        )
        let reset = planner.reset(context: context)

        #expect(reset.episodesSinceLastFullRescan == 0)
        // Other fields preserved.
        #expect(reset.observedEpisodeCount == context.observedEpisodeCount)
        #expect(reset.periodicFullRescanIntervalEpisodes == context.periodicFullRescanIntervalEpisodes)
    }

    @Test("only the threshold-crossing call returns periodicFullRescan when reset is honored")
    func testPeriodicRescanProgressionWithReset() {
        let planner = CoveragePlanner(periodicFullRescanIntervalEpisodes: 10)

        // interval - 1 → targeted (mature stable show)
        let preThreshold = makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 9,
            periodicFullRescanIntervalEpisodes: 10
        )
        #expect(planner.plan(for: preThreshold).policy == .targetedWithAudit)

        // interval → periodic
        let atThreshold = makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 10,
            periodicFullRescanIntervalEpisodes: 10
        )
        #expect(planner.plan(for: atThreshold).policy == .periodicFullRescan)

        // After consuming the periodic rescan, caller resets the counter.
        let afterReset = planner.reset(context: atThreshold)
        #expect(planner.plan(for: afterReset).policy == .targetedWithAudit)

        // Without reset, the next call would still return periodic (proves the
        // reset contract is what gates the loop).
        let withoutReset = makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 11,
            periodicFullRescanIntervalEpisodes: 10
        )
        #expect(planner.plan(for: withoutReset).policy == .periodicFullRescan)
    }

    // #11: precedence — cold-start beats periodic when both apply.
    @Test("cold-start full coverage wins over periodic full rescan when both trigger")
    func testColdStartWinsOverPeriodic() {
        let planner = CoveragePlanner()
        let context = makeCoveragePlannerContext(
            observedEpisodeCount: 1,
            episodesSinceLastFullRescan: 99,
            periodicFullRescanIntervalEpisodes: 10
        )

        let plan = planner.plan(for: context)
        #expect(plan.policy == .fullCoverage)
    }

    // #12: off-by-one and zero-config tests
    @Test("episodesSinceLastFullRescan == interval - 1 stays targeted")
    func testOffByOnePreThreshold() {
        let planner = CoveragePlanner()
        let plan = planner.plan(for: makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 9,
            periodicFullRescanIntervalEpisodes: 10
        ))
        #expect(plan.policy == .targetedWithAudit)
    }

    @Test("episodesSinceLastFullRescan == interval triggers periodic rescan")
    func testOffByOneAtThreshold() {
        let planner = CoveragePlanner()
        let plan = planner.plan(for: makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 10,
            periodicFullRescanIntervalEpisodes: 10
        ))
        #expect(plan.policy == .periodicFullRescan)
    }

    @Test("periodicFullRescanIntervalEpisodes == 0 falls back to planner default")
    func testPeriodicIntervalZeroFallsBackToDefault() {
        let planner = CoveragePlanner(periodicFullRescanIntervalEpisodes: 7)

        // Below the planner default (7) but above the bogus context-supplied 0.
        let below = makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 6,
            periodicFullRescanIntervalEpisodes: 0
        )
        #expect(planner.plan(for: below).policy == .targetedWithAudit)

        // At the planner default fallback.
        let atDefault = makeCoveragePlannerContext(
            observedEpisodeCount: 8,
            episodesSinceLastFullRescan: 7,
            periodicFullRescanIntervalEpisodes: 0
        )
        #expect(planner.plan(for: atDefault).policy == .periodicFullRescan)
    }

    @Test("coldStartEpisodeThreshold == 0 means cold-start never fires")
    func testColdStartThresholdZeroDisablesColdStart() {
        let planner = CoveragePlanner(coldStartEpisodeThreshold: 0)

        // observedEpisodeCount: 0 — would be cold-start under the default
        // threshold of 5, but with threshold 0 the cold-start branch must
        // never fire. Stable precision routes to targetedWithAudit.
        let plan = planner.plan(for: makeCoveragePlannerContext(
            observedEpisodeCount: 0,
            stableRecall: true,
            episodesSinceLastFullRescan: 1
        ))
        #expect(plan.policy == .targetedWithAudit)
    }
}
