// PlannerObservedEpisodeCountV71MigrationTests.swift
// playhead-kfts: pin the V71 reset of `podcast_planner_state.observedEpisodeCount`.
//
// WHY THERE IS A MIGRATION AT ALL. The column named an EPISODE count and every
// consumer read it as one — `computePlannerStableFlag` gates it against
// `plannerStableObservedEpisodeFloor` (5) and `CoveragePlanner.plan` against
// `coldStartEpisodeThreshold` — but `recordPodcastEpisodeObservation`
// incremented it once per COMPLETED BACKFILL JOB, unconditionally, and one
// episode is backfilled many times. So every value on a pre-kfts device is a
// count of backfill RUNS: the same ~9x inflation V49 fixed for
// `podcast_profiles.observationCount` (21 and 6 for the same ~4 episodes on the
// 2026-08-12 pull), one column over. No per-row predicate can recover the true
// episode count — the planner kept no claim rows until this rung — so the reset
// to 0 is the same conservative "we do not know" answer V49/V58/V69 gave.
//
// The directions covered, because closing one leaves the others open:
//   1. THE RESET HAPPENS, driven through the real ladder from a rewound V70.
//   2. ONLY `observedEpisodeCount` MOVES — the recall ring,
//      `episodesSinceLastFullRescan` and `stablePrecisionFlag` survive, and the
//      demotion the reset causes is reached through the COUNT gate, not by
//      flipping the stored flag (the flag stays as it was; the planner still
//      returns fullCoverage because the live count is now 0).
//   3. THE LADDER STILL CLIMBS on a fixture that has no planner table.
//   4. IDEMPOTENT ACROSS LAUNCHES: a count earned under the NEW (claimed) unit
//      is not thrown away by a later launch.
//   5. THE STATED LIMIT: a deliberate stamp rewind DOES reset a real count,
//      because nothing on disk records which unit wrote the integer.

import Foundation
import Testing

@testable import Playhead

@Suite("observedEpisodeCount counts EPISODES, not backfills (playhead-kfts V71)")
struct PlannerObservedEpisodeCountV71MigrationTests {

    /// Rewind to the V70 version stamp. Pinned to the LITERAL 70: "pre-kfts" is
    /// a fixed historical fact, and `currentSchemaVersion - 1` stops meaning it
    /// the moment head moves.
    private func rewindToV70(_ store: AnalysisStore) async throws {
        try await store.setMetaValue(forKey: "schema_version", value: "70")
    }

    // MARK: - 1. The reset happens

    @Test("V71 resets an inflated observedEpisodeCount to 0")
    func migrationResetsTheInflatedCount() async throws {
        let dir = try makeTempDir(prefix: "PlannerCountV71Reset")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Seed the pre-kfts field state: nine backfills of one show with NO
        // asset id, so the increment is ungated and the count inflates exactly
        // as it did before this bead. This is the shape a device is in.
        for tick in 1...9 {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: "inflated-show",
                wasFullRescan: false,
                now: Double(tick)
            )
        }
        let before = try #require(await store.fetchPodcastPlannerState(podcastId: "inflated-show"))
        #expect(before.observedEpisodeCount == 9, "seed precondition: the pre-kfts unconditional increment")

        try await rewindToV70(store)
        #expect(try await store.schemaVersion() == 70)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)

        let after = try #require(await reopened.fetchPodcastPlannerState(podcastId: "inflated-show"))
        #expect(
            after.observedEpisodeCount == 0,
            "9 was a count of backfill RUNS; no predicate recovers the episode count, so the rung zeroes it. Got \(after.observedEpisodeCount)"
        )
    }

    // MARK: - 2. Only the episode count moves — the scope discipline

    @Test("V71 leaves every OTHER planner column alone, and demotes via the count gate")
    func migrationTouchesOnlyTheEpisodeCount() async throws {
        let dir = try makeTempDir(prefix: "PlannerCountV71Scope")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Six ungated full-rescan observations with passing samples: the count
        // inflates to 6, the ring fills, and `stablePrecisionFlag` is true — the
        // exact "promoted on backfill runs" state this bead exists to undo.
        for tick in 1...6 {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: "promoted-show",
                wasFullRescan: true,
                fullRescanPrecisionSample: 0.95,
                now: Double(tick)
            )
        }
        let before = try #require(await store.fetchPodcastPlannerState(podcastId: "promoted-show"))
        #expect(before.observedEpisodeCount == 6)
        #expect(before.stableRecallFlag == true)
        #expect(before.recallSamples.count == 3)

        try await rewindToV70(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let after = try #require(await reopened.fetchPodcastPlannerState(podcastId: "promoted-show"))
        #expect(after.observedEpisodeCount == 0, "the episode count is reset")
        // The other columns are NOT this bead's unit and must survive verbatim.
        #expect(after.recallSamples == before.recallSamples, "the recall ring is a scan-run record, untouched")
        #expect(
            after.stableRecallFlag == before.stableRecallFlag,
            "the stored flag is not flipped by the migration — same discipline as V58's `mode`"
        )

        // The demotion that IS the point is reached through the COUNT gate, not
        // by rewriting the flag: `CoveragePlanner` returns fullCoverage because
        // the live count is 0, even though the stored flag still reads true.
        let planner = CoveragePlanner()
        let context = CoveragePlannerContext(
            observedEpisodeCount: after.observedEpisodeCount,
            stableRecall: after.stableRecallFlag,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: after.episodesSinceLastFullRescan,
            periodicFullRescanIntervalEpisodes: 10
        )
        #expect(
            planner.plan(for: context).policy == .fullCoverage,
            "a reset count routes the show back to full coverage even with the stale flag — the safe direction"
        )
    }

    // MARK: - 3. The ladder still climbs

    @Test("a fixture with no podcast_planner_state still reaches head")
    func migrationSkipsMissingTable() async throws {
        let dir = try makeTempDir(prefix: "PlannerCountV71NoTable")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.execForTesting("DROP TABLE podcast_planner_state")
        try await rewindToV70(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
    }

    // MARK: - 4. Idempotent, for the right reason

    @Test("a later launch does not throw away a count earned under the NEW unit")
    func migrationIsIdempotentAcrossLaunches() async throws {
        let dir = try makeTempDir(prefix: "PlannerCountV71Idem")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // A real episode counted under the new unit: an asset id is supplied, so
        // the increment is claim-gated and honest.
        _ = try await store.recordPodcastEpisodeObservation(
            podcastId: "honest-show",
            wasFullRescan: false,
            analysisAssetId: "asset-1",
            now: 1
        )
        #expect(try #require(await store.fetchPodcastPlannerState(podcastId: "honest-show")).observedEpisodeCount == 1)

        // The next launch. No stamp rewind — a device that has reached 71 stays
        // there, so the ladder is what makes this a no-op.
        AnalysisStore.resetMigratedPathsForTesting()
        let second = try AnalysisStore(directory: dir)
        try await second.migrate()

        #expect(
            try #require(await second.fetchPodcastPlannerState(podcastId: "honest-show")).observedEpisodeCount == 1,
            "a later launch must not re-zero an episode counted honestly under the new unit"
        )
    }

    // MARK: - 5. The stated limit

    /// Like V49/V58, V71 has NO per-row predicate that can tell a legitimately
    /// counted 3 from an inflated 3 — the two are the same integer, and the fact
    /// that separates them (which unit wrote it) is exactly what was never
    /// recorded. So a deliberate stamp rewind after this rung DOES reset a real
    /// count. It is not a field state: nothing rewinds `schema_version`, and the
    /// production entry points climb monotonically.
    @Test("a deliberate stamp rewind DOES reset a real count — the stated limit [PlannerObservedEpisodeCountV71Migration]")
    func aStampRewindResetsARealCount() async throws {
        let dir = try makeTempDir(prefix: "PlannerCountV71Rewind")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Three honest episodes under the new unit (distinct claimed assets).
        for idx in 0..<3 {
            _ = try await store.recordPodcastEpisodeObservation(
                podcastId: "honest-show",
                wasFullRescan: false,
                analysisAssetId: "asset-\(idx)",
                now: Double(idx + 1)
            )
        }
        #expect(try #require(await store.fetchPodcastPlannerState(podcastId: "honest-show")).observedEpisodeCount == 3)

        try await rewindToV70(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(
            try #require(await reopened.fetchPodcastPlannerState(podcastId: "honest-show")).observedEpisodeCount == 0,
            "the rung cannot tell 3 episodes from 3 backfill runs, and does not pretend to"
        )
    }
}
