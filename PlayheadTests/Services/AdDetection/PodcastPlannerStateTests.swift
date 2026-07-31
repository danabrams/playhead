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
            stableRecall: state?.stableRecallFlag ?? false,
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

    // MARK: - Acceptance #4: 5 episodes + recall both required

    @Test("5 episodes alone do NOT flip stable_recall_flag without recall evidence")
    func fiveEpisodesWithoutRecallStaysFullCoverage() async throws {
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
        #expect(state.stableRecallFlag == false)
        #expect(state.recallSamples.isEmpty)

        let plan = planner.plan(for: contextFromState(state))
        #expect(plan.policy == .fullCoverage)
    }

    @Test("5 episodes + 3 recall samples >= 0.85 flips to targetedWithAudit")
    func fiveEpisodesPlusRecallFlipsToTargeted() async throws {
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
        #expect(state.stableRecallFlag == true)
        // Ring keeps the most recent 3 samples, oldest first.
        #expect(state.recallSamples == [0.93, 0.90, 0.92])
        // Full rescans must have reset the counter on every call.
        #expect(state.episodesSinceLastFullRescan == 0)

        // Bump the rescan counter to a non-zero value so the planner can't
        // route through the cold-start branch via `episodesSinceLastFullRescan == 0`
        // alone. We want the targeted branch reached on the merits of
        // `observedEpisodeCount + stablePrecision`, not as a side effect of
        // the cold-start guard.
        let bumpedContext = CoveragePlannerContext(
            observedEpisodeCount: state.observedEpisodeCount,
            stableRecall: state.stableRecallFlag,
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

    // MARK: - Acceptance #6: recall-fail keeps flag false at >= 5 episodes

    @Test("stable_recall_flag stays false when any sample is below 0.85")
    func recallFailureKeepsFlagFalse() async throws {
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
        #expect(stickyState.recallSamples == [0.92, 0.93, 0.50])
        #expect(stickyState.stableRecallFlag == false)

        let plan = planner.plan(for: contextFromState(stickyState))
        #expect(plan.policy == .fullCoverage)

        // Final paranoia: with the planner still in cold-start by precision,
        // running it through the planner one more time should not somehow
        // produce targetedWithAudit even with elevated episodesSinceLastFullRescan.
        let elevated = CoveragePlannerContext(
            observedEpisodeCount: stickyState.observedEpisodeCount,
            stableRecall: stickyState.stableRecallFlag,
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

    @Test("10 episodes since last full rescan triggers periodicFullRescan regardless of stable_recall_flag")
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
        #expect(stable.stableRecallFlag == true)
        #expect(stable.episodesSinceLastFullRescan == 0)

        // Now record 10 non-full-rescan observations. After the 10th, the
        // counter must be 10 and the planner must return periodicFullRescan
        // even though stableRecallFlag is still true.
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
        #expect(drifted.stableRecallFlag == true)

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
        #expect(resumed.stableRecallFlag == true)
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

    // MARK: - playhead-hvk0: a rescan must have READ the episode to certify

    /// The shipped default is load-bearing, not cosmetic: with the gate OFF the
    /// whole bead is inert and a show can still be promoted by rescans that read
    /// 2–5% of their episodes. Pinned so a default flip is a deliberate,
    /// reviewed act rather than a silent regression.
    @Test("playhead-hvk0: the promotion gate ships ON")
    func promotionGateShipsOn() {
        #expect(AdDetectionConfig.default.plannerPromotionRequiresMeasuredCoverage == true)
    }

    /// The pure helper's contract, stated as a truth table so a future change
    /// to either argument's meaning fails here rather than in a live-store test
    /// where the ring bookkeeping could mask it.
    ///
    /// `nil` is the pre-hvk0 shape and MUST delegate to the two-argument helper
    /// verbatim — every caller that does not opt into the gate keeps its
    /// behaviour byte-for-byte.
    @Test("playhead-hvk0: read evidence gates the stable flag, nil delegates unchanged")
    func readEvidenceGatesStableFlag() {
        let passing = [0.90, 0.91, 0.92]
        let failing = [0.90, 0.10, 0.92]

        // nil ⇒ identical to the two-argument helper in BOTH directions.
        for samples in [passing, failing] {
            for count in [4, 5, 9] {
                #expect(
                    AnalysisStore.computePlannerStableFlag(
                        observedEpisodeCount: count,
                        samples: samples,
                        fullRescanReadEpisode: nil
                    ) == AnalysisStore.computePlannerStableFlag(
                        observedEpisodeCount: count,
                        samples: samples
                    )
                )
            }
        }

        // true ⇒ also identical: read evidence is NECESSARY, never sufficient.
        // A show whose ring holds a failing sample must stay false even when the
        // rescan read every second of its episode.
        #expect(
            AnalysisStore.computePlannerStableFlag(
                observedEpisodeCount: 5, samples: failing, fullRescanReadEpisode: true
            ) == false
        )
        #expect(
            AnalysisStore.computePlannerStableFlag(
                observedEpisodeCount: 5, samples: passing, fullRescanReadEpisode: true
            ) == true
        )
        // And it cannot manufacture a promotion from an unfilled ring.
        #expect(
            AnalysisStore.computePlannerStableFlag(
                observedEpisodeCount: 5, samples: [0.99], fullRescanReadEpisode: true
            ) == false
        )

        // false ⇒ forces false even when every other condition passes. This is
        // the whole bead: three 1.0 samples from rescans that read 2–5% of their
        // episodes must not promote a show.
        #expect(
            AnalysisStore.computePlannerStableFlag(
                observedEpisodeCount: 14, samples: [1.0, 1.0, 1.0], fullRescanReadEpisode: false
            ) == false
        )
    }

    /// The live-store path: the shape measured on the 2026-07-29 device pull.
    /// `feeds.simplecast.com/dHoohVNH` reached three 1.0 recall samples from
    /// `fullEpisodeScan` runs that each examined 82–167 s of a 1,503–4,379 s
    /// episode, and that promoted every subsequent episode of the show into
    /// `targetedWithAudit`.
    @Test("playhead-hvk0: short rescans cannot promote a show, and the ring is not poisoned")
    func shortRescansDoNotPromote() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-short-rescans"
        let planner = CoveragePlanner()

        // Five full rescans that would each have produced a perfect recall
        // sample under the pre-hvk0 rules, but none of which read its episode.
        for tick in 1...5 {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: podcastId,
                wasFullRescan: true,
                fullRescanPrecisionSample: nil,  // the runner withholds it
                fullRescanReadEpisode: false,
                now: Double(tick)
            )
        }

        let state = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(state.observedEpisodeCount == 5)
        // Withheld, not zeroed: a 0.0 would be an equally false claim and would
        // block the show for three further rescans after the scans got healthy.
        #expect(state.recallSamples.isEmpty)
        #expect(state.stableRecallFlag == false)
        #expect(planner.plan(for: contextFromState(state)).policy == .fullCoverage)

        // Now the scans get healthy. Three rescans that DID read their episodes
        // fill the ring honestly and the show is promoted — the gate withholds
        // certification, it does not withdraw the policy.
        for (idx, sample) in [0.90, 0.91, 0.92].enumerated() {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: podcastId,
                wasFullRescan: true,
                fullRescanPrecisionSample: sample,
                fullRescanReadEpisode: true,
                now: Double(6 + idx)
            )
        }
        let promoted = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(promoted.recallSamples == [0.90, 0.91, 0.92])
        #expect(promoted.stableRecallFlag == true)
        #expect(planner.plan(for: contextFromState(promoted)).policy == .targetedWithAudit)

        // And a single subsequent short rescan demotes it again, so the show's
        // next episode is planned `fullCoverage` rather than narrowed.
        _ = try await store.recordPodcastEpisodeObservation(
            podcastId: podcastId,
            wasFullRescan: true,
            fullRescanPrecisionSample: nil,
            fullRescanReadEpisode: false,
            now: 9
        )
        let demoted = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        // The ring is preserved across the demotion — the earned samples are
        // still true, they are simply no longer sufficient on their own.
        #expect(demoted.recallSamples == [0.90, 0.91, 0.92])
        #expect(demoted.stableRecallFlag == false)
        #expect(planner.plan(for: contextFromState(demoted)).policy == .fullCoverage)
    }

    /// A targeted observation carries no rescan, so it must pass `nil` through
    /// and leave the flag deriving from the ring. Without this, a show that
    /// earned the policy would be demoted by its own first targeted run — the
    /// flap the non-sticky design depends on being impossible.
    @Test("playhead-hvk0: a targeted observation never demotes an earned show")
    func targetedObservationDoesNotDemote() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-earned"
        let planner = CoveragePlanner()

        for (idx, sample) in [0.90, 0.91, 0.92, 0.93, 0.94].enumerated() {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: podcastId,
                wasFullRescan: true,
                fullRescanPrecisionSample: sample,
                fullRescanReadEpisode: true,
                now: Double(idx + 1)
            )
        }
        let earned = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(earned.stableRecallFlag == true)

        // The targeted runs that follow. The runner passes `nil` on this path
        // (there is no rescan to certify), and a hostile caller passing `false`
        // must ALSO be ignored — `recordPodcastEpisodeObservation` drops the
        // argument on non-rescan observations rather than trusting it.
        for tick in 6...10 {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: podcastId,
                wasFullRescan: false,
                fullRescanReadEpisode: tick.isMultiple(of: 2) ? false : nil,
                now: Double(tick)
            )
            let state = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
            #expect(state.stableRecallFlag == true, "targeted observation must not demote (tick \(tick))")
            #expect(planner.plan(for: contextFromState(state)).policy == .targetedWithAudit)
        }
    }

    /// The mandatory bounded / no-progress rail: many cycles against a show
    /// whose rescans never manage to read an episode.
    ///
    /// What a broken implementation would do here: flip the flag on some cycle
    /// (an off-by-one in the ring, a sticky "demoted" bit that decays, a
    /// `false` that is only consulted on the first observation), or poison the
    /// ring with synthetic 0.0 samples so the show could never recover.
    @Test("playhead-hvk0: 200 no-progress cycles never promote and never poison the ring")
    func noProgressCyclesNeverPromote() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-no-progress"
        let planner = CoveragePlanner()

        for tick in 1...200 {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: podcastId,
                wasFullRescan: true,
                fullRescanPrecisionSample: nil,
                fullRescanReadEpisode: false,
                now: Double(tick)
            )
            let state = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
            #expect(state.stableRecallFlag == false, "no-progress cycle \(tick) must not promote")
            #expect(state.recallSamples.isEmpty, "no-progress cycle \(tick) must not enter the ring")
            // Every cycle plans `fullCoverage` — the safe, more-audio-read
            // direction — and never `targetedWithAudit`.
            #expect(planner.plan(for: contextFromState(state)).policy == .fullCoverage)
        }

        let final = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(final.observedEpisodeCount == 200)
        // Bounded state: the ring is a fixed-size window and 200 withheld
        // samples leave it exactly as empty as one did.
        #expect(final.recallSamples.isEmpty)
        // A full rescan always resets this, so 200 of them leave it at 0 — the
        // counter cannot run away either.
        #expect(final.episodesSinceLastFullRescan == 0)
    }
}
