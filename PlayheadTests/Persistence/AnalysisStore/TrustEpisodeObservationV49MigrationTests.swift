// TrustEpisodeObservationV49MigrationTests.swift
// playhead-mn5e / playhead-2qz6 — V49.
//
// V49 does two things and BOTH are load-bearing:
//   1. creates `trust_episode_observations`, the per-episode claim ledger that
//      makes `observationCount` count episodes instead of backfills; and
//   2. RESETS every existing `podcast_profiles.observationCount` to 0, because
//      every row an older binary wrote is in the wrong unit (~9 backfills per
//      episode; 26 and 10 on the 2026-08-12 device pull, for 4 episodes).
//
// The reset is a data migration. It is tested here rather than argued for in a
// comment, and the things it must NOT touch — `mode`, `skipTrustScore`,
// `recentFalseSkipSignals` — are asserted alongside, because a migration that
// silently changed a show's mode would be a promotion nobody chose.
//
// THE FIXTURES CARRY A `detectorTrustJSON` NOW, AND THAT IS THE POINT
// (playhead-scc6). Until then `makeProfile` never set the column, so the whole
// suite ran against rows on which the defect scc6 filed CANNOT EXIST: V49's
// statement reaches the show scalar and nothing else, and the per-class mirror
// in `detectorTrustJSON` sat in the old unit on any device whose ledger had
// forked. The migration's blind spot and the test's blind spot were the same
// one — a migration test whose fixture cannot exhibit the defect is what let it
// ship. The fixtures below deliberately DISAGREE with their show scalar, in the
// direction and roughly the magnitude the 2026-08-18 device pulls show
// (`fusion` 16 against 8 claimed episodes).
//
// WHAT THIS SUITE CAN AND CANNOT SAY about which rung does which half. It drives
// the LADDER from a rewound 48, so V49 and V58 both run and it can only assert
// the end state: nothing inflated survives, in either representation. The claim
// that V58 is what repairs the mirror is pinned where it can be isolated —
// `DetectorTrustObservationCountV58MigrationTests` rewinds to 57, where V49
// never executes at all.

import Foundation
import Testing
@testable import Playhead

@Suite("observationCount counts episodes — V49 migration (playhead-2qz6)", .serialized)
struct TrustEpisodeObservationV49MigrationTests {

    private let scoreTolerance: Double = 1e-10

    private func makeProfile(
        podcastId: String,
        mode: SkipMode,
        trust: Double,
        observations: Int,
        falseSignals: Int,
        detectorTrustJSON: String? = nil
    ) -> PodcastProfile {
        PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: "acme",
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: trust,
            observationCount: observations,
            mode: mode.rawValue,
            recentFalseSkipSignals: falseSignals,
            detectorTrustJSON: detectorTrustJSON
        )
    }

    /// A FORKED ledger whose per-class counts disagree with the show scalar —
    /// the shape one attributed veto produces (`applyFalseSkipSignal`
    /// materializes EVERY class), and the shape the 2026-08-18 device pulls
    /// carry. Modes are spread across all three rungs so a repair that re-ran
    /// promotion, or rebuilt the ledger from the seed, is visible.
    private func forkedLedgerJSON(fusionCount: Int) -> String {
        """
        {"fusion":{"trustScore":1,"mode":"auto","falseSkipWeight":0.5,"observationCount":\(fusionCount)},\
        "segmentAggregated":{"trustScore":0.5,"mode":"manual","falseSkipWeight":0,"observationCount":\(fusionCount / 2)},\
        "userAsserted":{"trustScore":0.3,"mode":"shadow","falseSkipWeight":0.5,"observationCount":\(fusionCount / 4)},\
        "rediffByteExact":{"trustScore":0.5,"mode":"auto","falseSkipWeight":0,"observationCount":0}}
        """
    }

    private func storedLedger(
        _ store: AnalysisStore, _ podcastId: String
    ) async throws -> DetectorTrustLedger {
        try #require(await store.fetchProfile(podcastId: podcastId)).detectorTrustLedger
    }

    @Test("V49 resets the per-backfill counts and leaves mode, trust and the veto counter alone")
    func v49ResetsObservationCountOnly() async throws {
        let store = try await makeTestStore()

        // The device's two shows, verbatim, plus a demoted one so the
        // migration is shown not to disturb a show a user has vetoed. Each
        // carries a FORKED per-class ledger (playhead-scc6): before that, none
        // did, and the mirror the skip gate reads went untested and unrepaired.
        try await store.upsertProfile(
            makeProfile(podcastId: "simplecast", mode: .shadow, trust: 0.5,
                        observations: 26, falseSignals: 0,
                        detectorTrustJSON: forkedLedgerJSON(fusionCount: 16))
        )
        try await store.upsertProfile(
            makeProfile(podcastId: "flightcast", mode: .shadow, trust: 0.5,
                        observations: 10, falseSignals: 0,
                        detectorTrustJSON: forkedLedgerJSON(fusionCount: 10))
        )
        try await store.upsertProfile(
            makeProfile(podcastId: "vetoed", mode: .manual, trust: 0.3,
                        observations: 41, falseSignals: 3,
                        detectorTrustJSON: forkedLedgerJSON(fusionCount: 44))
        )
        // The fixtures really do exhibit the defect before the ladder runs — a
        // fixture that cannot is the thing this suite shipped with.
        #expect(try await storedLedger(store, "simplecast")
            .entry(for: .fusion, seededFrom: try #require(await store.fetchProfile(podcastId: "simplecast")))
            .observationCount == 16)

        // Wind the schema back so the V49 rung runs again over these rows.
        try await store.execForTesting(
            "UPDATE _meta SET value = '48' WHERE key = 'schema_version'"
        )
        try await store.execForTesting("DROP TABLE IF EXISTS trust_episode_observations")
        try await store.migrateOnlyForTesting()

        for id in ["simplecast", "flightcast", "vetoed"] {
            let profile = try #require(await store.fetchProfile(podcastId: id))
            #expect(
                profile.observationCount == 0,
                "\(id): a count written in backfills is not a count of episodes — it must not be carried into the new unit. Got \(profile.observationCount)"
            )
            // BOTH representations, because there are two and V49 reaches one.
            // Which rung does this half is pinned in
            // `DetectorTrustObservationCountV58MigrationTests`; here the claim
            // is only that nothing inflated survives the ladder.
            let ledger = profile.detectorTrustLedger
            #expect(ledger.entries.count == 4, "\(id): the ledger must survive, keys and all")
            for (cls, entry) in ledger.entries {
                #expect(
                    entry.observationCount == 0,
                    """
                    \(id)/\(cls): the PER-CLASS mirror is what \
                    `SkipOrchestrator.skipMode(for:)` consults, so leaving it in the old \
                    unit leaves the correction reaching nothing that decides anything. \
                    Got \(entry.observationCount)
                    """
                )
            }
        }

        // What the migration must NOT do.
        let vetoed = try #require(await store.fetchProfile(podcastId: "vetoed"))
        #expect(
            vetoed.mode == SkipMode.manual.rawValue,
            "the reset must not move a show's mode in either direction; got \(vetoed.mode)"
        )
        #expect(abs(vetoed.skipTrustScore - 0.3) < scoreTolerance)
        #expect(
            vetoed.recentFalseSkipSignals == 3,
            "a user's vetoes survive the migration — resetting them would relitigate a decision the listener made"
        )
        #expect(vetoed.sponsorLexicon == "acme", "learned priors are untouched")
        // …and the same contract one column down: a per-class mode is a posture
        // too, and the weight is the listener's own veto record.
        let vetoedLedger = vetoed.detectorTrustLedger
        #expect(vetoedLedger.entries["fusion"]?.mode == "auto")
        #expect(vetoedLedger.entries["segmentAggregated"]?.mode == "manual")
        #expect(vetoedLedger.entries["userAsserted"]?.mode == "shadow")
        #expect(vetoedLedger.entries["userAsserted"]?.falseSkipWeight == 0.5)
    }

    @Test("V49 creates the claim ledger, and the claim is idempotent per (show, episode)")
    func claimIsIdempotent() async throws {
        let store = try await makeTestStore()

        let first = try await store.claimEpisodeTrustObservation(
            podcastId: "show-a", analysisAssetId: "asset-1"
        )
        #expect(first, "the first claim on a pair must succeed")

        for _ in 0..<5 {
            let again = try await store.claimEpisodeTrustObservation(
                podcastId: "show-a", analysisAssetId: "asset-1"
            )
            #expect(!again, "every later claim on the SAME pair must fail — this is the dedupe")
        }
        #expect(try await store.episodeTrustObservationCount(podcastId: "show-a") == 1)

        // A different episode of the same show is a different claim.
        #expect(try await store.claimEpisodeTrustObservation(
            podcastId: "show-a", analysisAssetId: "asset-2"
        ))
        #expect(try await store.episodeTrustObservationCount(podcastId: "show-a") == 2)

        // The SAME episode under a different show is also a different claim —
        // the key is the pair, so a re-subscribed feed is not suppressed.
        #expect(try await store.claimEpisodeTrustObservation(
            podcastId: "show-b", analysisAssetId: "asset-1"
        ))
        #expect(try await store.episodeTrustObservationCount(podcastId: "show-b") == 1)
        #expect(
            try await store.episodeTrustObservationCount(podcastId: "show-a") == 2,
            "and it does not leak into the other show's count"
        )
    }

    @Test("An empty identifier is never claimable")
    func emptyIdentifiersAreRejected() async throws {
        let store = try await makeTestStore()
        #expect(try await store.claimEpisodeTrustObservation(
            podcastId: "", analysisAssetId: "asset-1"
        ) == false)
        #expect(try await store.claimEpisodeTrustObservation(
            podcastId: "show-a", analysisAssetId: ""
        ) == false)
        #expect(try await store.episodeTrustObservationCount(podcastId: "") == 0)
    }

    @Test("A freshly-created store is already at 49 with the claim table present")
    func freshStoreIsAt49() async throws {
        let store = try await makeTestStore()
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // The claim table exists without any migration having run, i.e. the
        // fresh `createTables()` path and the V49 rung agree. A table created
        // in only one of the two is the classic upgrade-only-or-install-only
        // schema split.
        #expect(try await store.claimEpisodeTrustObservation(
            podcastId: "fresh", analysisAssetId: "asset"
        ))
    }

    @Test("Winding the schema back to 48 and re-running the ladder climbs to 49")
    func ladderClimbsTo49() async throws {
        let store = try await makeTestStore()
        try await store.execForTesting(
            "UPDATE _meta SET value = '48' WHERE key = 'schema_version'"
        )
        try await store.execForTesting("DROP TABLE IF EXISTS trust_episode_observations")
        try await store.migrateOnlyForTesting()
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(
            try await store.claimEpisodeTrustObservation(
                podcastId: "upgraded", analysisAssetId: "asset"
            ),
            "the upgrade path must rebuild the claim table it just dropped"
        )
    }
}
