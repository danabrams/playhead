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
        falseSignals: Int
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
            recentFalseSkipSignals: falseSignals
        )
    }

    @Test("V49 resets the per-backfill counts and leaves mode, trust and the veto counter alone")
    func v49ResetsObservationCountOnly() async throws {
        let store = try await makeTestStore()

        // The device's two shows, verbatim, plus a demoted one so the
        // migration is shown not to disturb a show a user has vetoed.
        try await store.upsertProfile(
            makeProfile(podcastId: "simplecast", mode: .shadow, trust: 0.5,
                        observations: 26, falseSignals: 0)
        )
        try await store.upsertProfile(
            makeProfile(podcastId: "flightcast", mode: .shadow, trust: 0.5,
                        observations: 10, falseSignals: 0)
        )
        try await store.upsertProfile(
            makeProfile(podcastId: "vetoed", mode: .manual, trust: 0.3,
                        observations: 41, falseSignals: 3)
        )

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
