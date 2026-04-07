// PodcastPlannerStateTests.swift
// bd-m8k: Tests for the podcast_planner_state CRUD on AnalysisStore and the
// integration with CoveragePlanner that lets a show transition out of the
// cold-start `fullCoverage` policy when (and only when) BOTH the observed-
// episode floor AND the precision ring threshold pass. Also pins the
// migration's clean DROP-and-recreate behavior so future schema work has a
// regression rail.

import Foundation
import Testing

@testable import Playhead

@Suite("PodcastPlannerState (bd-m8k)")
struct PodcastPlannerStateTests {

    // MARK: - Helpers

    /// Builds a `CoveragePlannerContext` from a persisted `PodcastPlannerState`
    /// using the same field mapping as `AdDetectionService.runShadowFMPhase`,
    /// so the planner-decision tests below exercise the exact production
    /// translation rather than a parallel one. The non-state fields default
    /// to "no failure detected" — this lets each test focus on whether the
    /// store-driven values alone are sufficient to flip the policy.
    private func contextFromState(
        _ state: PodcastPlannerState?,
        periodicFullRescanIntervalEpisodes: Int = 10
    ) -> CoveragePlannerContext {
        CoveragePlannerContext(
            observedEpisodeCount: state?.observedEpisodeCount ?? 0,
            stablePrecision: state?.stablePrecisionFlag ?? false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: state?.episodesSinceLastFullRescan ?? 0,
            periodicFullRescanIntervalEpisodes: periodicFullRescanIntervalEpisodes
        )
    }

    // MARK: - Migration / table lifecycle

    @Test("podcast_planner_state survives a DROP TABLE / migrate cycle cleanly")
    func dropAndReMigrateIsClean() async throws {
        let dir = try makeTempDir(prefix: "PlannerStateMigrate")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try await AnalysisStore.open(directory: dir)

        // Seed a row so we can prove the table really existed before we drop it.
        _ = try await store.recordPodcastEpisodeObservation(
            podcastId: "podcast-drop",
            wasFullRescan: true,
            fullRescanPrecisionSample: 0.9,
            now: 100
        )
        let beforeDrop = try await store.fetchPodcastPlannerState(podcastId: "podcast-drop")
        #expect(beforeDrop != nil)

        // Drop the table out from under the cached migration. The next
        // `migrate()` call must repair the schema rather than blow up.
        try await store.dropPodcastPlannerStateForTesting()
        AnalysisStore.resetMigratedPathsForTesting()

        // Re-open the store against the same directory. The v4 migration is
        // guarded by `schema_version`, so the cleanest re-migrate path is to
        // open a fresh connection. The `CREATE TABLE IF NOT EXISTS` in
        // `createTables()` runs unconditionally on every connection and
        // should rebuild the dropped table without throwing.
        let reopened = try await AnalysisStore.open(directory: dir)

        // Table should be back and empty.
        let afterReopen = try await reopened.fetchPodcastPlannerState(podcastId: "podcast-drop")
        #expect(afterReopen == nil)

        // And it must accept fresh writes.
        let written = try await reopened.recordPodcastEpisodeObservation(
            podcastId: "podcast-drop",
            wasFullRescan: false,
            now: 200
        )
        #expect(written.observedEpisodeCount == 1)
        #expect(written.episodesSinceLastFullRescan == 1)
    }

    // MARK: - Acceptance #4: 5 episodes + precision both required

    @Test("5 episodes alone do NOT flip stable_precision_flag without precision evidence")
    func fiveEpisodesWithoutPrecisionStaysFullCoverage() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-no-precision"
        let planner = CoveragePlanner()

        // Five non-full-rescan observations: counter advances, but the
        // precision ring is never populated. The flag must stay false and
        // the planner must keep returning fullCoverage.
        for tick in 1...5 {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: podcastId,
                wasFullRescan: false,
                now: Double(tick)
            )
        }
        let state = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))

        #expect(state.observedEpisodeCount == 5)
        #expect(state.stablePrecisionFlag == false)
        #expect(state.precisionSamples.isEmpty)

        let plan = planner.plan(for: contextFromState(state))
        #expect(plan.policy == .fullCoverage)
    }

    @Test("5 episodes + 3 precision samples >= 0.85 flips to targetedWithAudit")
    func fiveEpisodesPlusPrecisionFlipsToTargeted() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-flips"
        let planner = CoveragePlanner()

        // Drive five full-rescan observations with passing precision samples.
        // After three samples land in the ring AND the observed-episode
        // floor (5) is reached, the flag must flip true.
        let samples = [0.91, 0.88, 0.93, 0.90, 0.92]
        for (idx, sample) in samples.enumerated() {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: podcastId,
                wasFullRescan: true,
                fullRescanPrecisionSample: sample,
                now: Double(idx + 1)
            )
        }
        let state = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))

        #expect(state.observedEpisodeCount == 5)
        #expect(state.stablePrecisionFlag == true)
        // Ring keeps the most recent 3 samples, oldest first.
        #expect(state.precisionSamples == [0.93, 0.90, 0.92])
        // Full rescans must have reset the counter on every call.
        #expect(state.episodesSinceLastFullRescan == 0)

        // Bump the rescan counter to a non-zero value so the planner can't
        // route through the cold-start branch via `episodesSinceLastFullRescan == 0`
        // alone. We want the targeted branch reached on the merits of
        // `observedEpisodeCount + stablePrecision`, not as a side effect of
        // the cold-start guard.
        let bumpedContext = CoveragePlannerContext(
            observedEpisodeCount: state.observedEpisodeCount,
            stablePrecision: state.stablePrecisionFlag,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 3,
            periodicFullRescanIntervalEpisodes: 10
        )
        let plan = planner.plan(for: bumpedContext)
        #expect(plan.policy == .targetedWithAudit)
    }

    // MARK: - Acceptance #6: precision-fail keeps flag false at >= 5 episodes

    @Test("stable_precision_flag stays false when any sample is below 0.85")
    func precisionFailureKeepsFlagFalse() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-bad-precision"
        let planner = CoveragePlanner()

        // Three rescans where one sample is below the 0.85 threshold. Even
        // though the observed-episode count will eventually exceed 5, the
        // flag must remain false because the ring contains a failing sample.
        let samples = [0.91, 0.70 /* fail */, 0.88, 0.92, 0.90]
        for (idx, sample) in samples.enumerated() {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: podcastId,
                wasFullRescan: true,
                fullRescanPrecisionSample: sample,
                now: Double(idx + 1)
            )
        }
        let state = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))

        #expect(state.observedEpisodeCount == 5)
        // Ring should hold the most recent three: [0.88, 0.92, 0.90] — all
        // pass. So the FINAL state should actually flip true. To pin the
        // "any failing sample anywhere in the active ring keeps the flag
        // false" rule, examine the intermediate state after the 0.70 sample
        // landed (i.e. recompute via the pure helper).
        let intermediate = AnalysisStore.computePlannerStableFlag(
            observedEpisodeCount: 5,
            samples: [0.91, 0.70, 0.88]
        )
        #expect(intermediate == false)

        // Also test the live store path: if the ring still contains a
        // failing sample at >= 5 observations, the flag must be false. Set
        // up that exact state by feeding fresh samples on a different
        // podcast id and stopping early.
        let stickyId = "podcast-sticky-bad"
        for (idx, sample) in [0.92, 0.93, 0.50].enumerated() {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: stickyId,
                wasFullRescan: true,
                fullRescanPrecisionSample: sample,
                now: Double(idx + 1)
            )
        }
        // Two more non-rescan observations to push the count over 5 without
        // disturbing the ring.
        for tick in 4...5 {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: stickyId,
                wasFullRescan: false,
                now: Double(tick)
            )
        }
        let stickyState = try #require(await store.fetchPodcastPlannerState(podcastId: stickyId))
        #expect(stickyState.observedEpisodeCount == 5)
        #expect(stickyState.precisionSamples == [0.92, 0.93, 0.50])
        #expect(stickyState.stablePrecisionFlag == false)

        let plan = planner.plan(for: contextFromState(stickyState))
        #expect(plan.policy == .fullCoverage)

        // Final paranoia: with the planner still in cold-start by precision,
        // running it through the planner one more time should not somehow
        // produce targetedWithAudit even with elevated episodesSinceLastFullRescan.
        let elevated = CoveragePlannerContext(
            observedEpisodeCount: stickyState.observedEpisodeCount,
            stablePrecision: stickyState.stablePrecisionFlag,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 4,
            periodicFullRescanIntervalEpisodes: 10
        )
        #expect(planner.plan(for: elevated).policy == .fullCoverage)
    }

    // MARK: - Acceptance #5: 10 episodes since last full rescan triggers periodic

    @Test("10 episodes since last full rescan triggers periodicFullRescan regardless of stable_precision_flag")
    func tenEpisodesSinceFullRescanTriggersPeriodic() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-periodic"
        let planner = CoveragePlanner(periodicFullRescanIntervalEpisodes: 10)

        // First, drive the show to a "stable" state: 5 full rescans with
        // passing precision so the flag flips true. This is the case where
        // periodic rescan must STILL fire — the test name is "regardless
        // of stable_precision_flag".
        for (idx, sample) in [0.90, 0.91, 0.92, 0.93, 0.94].enumerated() {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: podcastId,
                wasFullRescan: true,
                fullRescanPrecisionSample: sample,
                now: Double(idx + 1)
            )
        }
        let stable = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(stable.stablePrecisionFlag == true)
        #expect(stable.episodesSinceLastFullRescan == 0)

        // Now record 10 non-full-rescan observations. After the 10th, the
        // counter must be 10 and the planner must return periodicFullRescan
        // even though stablePrecisionFlag is still true.
        for tick in 6...15 {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: podcastId,
                wasFullRescan: false,
                now: Double(tick)
            )
        }
        let drifted = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(drifted.observedEpisodeCount == 15)
        #expect(drifted.episodesSinceLastFullRescan == 10)
        #expect(drifted.stablePrecisionFlag == true)

        let plan = planner.plan(for: contextFromState(drifted, periodicFullRescanIntervalEpisodes: 10))
        #expect(plan.policy == .periodicFullRescan)

        // And once the caller acknowledges the rescan via a fresh
        // observation, the planner must drop back to targetedWithAudit on
        // the next plan call (regression rail for the reset path).
        _ = try await store.recordPodcastEpisodeObservation(
            podcastId: podcastId,
            wasFullRescan: true,
            fullRescanPrecisionSample: 0.91,
            now: 16
        )
        let resumed = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(resumed.episodesSinceLastFullRescan == 0)
        #expect(resumed.stablePrecisionFlag == true)
        let plan2 = planner.plan(for: contextFromState(resumed, periodicFullRescanIntervalEpisodes: 10))
        #expect(plan2.policy == .targetedWithAudit)
    }

    // MARK: - Lazy creation + cold-start defaults

    @Test("missing row maps to cold-start defaults and yields fullCoverage")
    func missingRowIsColdStart() async throws {
        let store = try await makeTestStore()
        let planner = CoveragePlanner()

        let state = try await store.fetchPodcastPlannerState(podcastId: "podcast-never-seen")
        #expect(state == nil)

        let plan = planner.plan(for: contextFromState(state))
        #expect(plan.policy == .fullCoverage)
    }
}
