// TraitProfileEpisodeCountV57MigrationTests.swift
// playhead-g7ln: pin the V57 reset of `podcast_profiles.traitProfileJSON`'s
// `episodesObserved`.
//
// WHY THERE IS A MIGRATION AT ALL. `ShowTraitProfile.episodesObserved` counted
// BACKFILLS, not episodes; the forward fix puts the increment behind the
// `trust_episode_observations` claim, and every value already on disk was
// written in the old unit. playhead-2qz6's V49 enumerated four readers of
// `podcast_profiles.observationCount` and reset that column; it does not touch
// this one, which lives in a different column of the same table. Measured on
// the 2026-08-18 t3 pull: **104 and 56 against 8 and 7 distinct episodes**.
//
// WHY ZERO AND NOT THE LEDGER COUNT — the decision this suite exists to keep
// from being quietly undone. The `trust_episode_observations` ledger CAN answer
// "how many episodes contributed" (it reads exactly 8 and 7 on that pull), so
// unlike V49 a faithful-looking reconstruction was available and was declined.
// The readers are not asking that question. `isReliable` and `traitBlendWeight`
// are asking how much CROSS-EPISODE information is in the values, and the EMA
// ran ~13 times per episode at alpha 0.3 — `0.7^13 = 0.0097`, so 99 % of every
// earlier episode was overwritten inside the next one. Writing 8 would claim
// eight episodes of maturity for a vector carrying about one. Zero says "we do
// not know", which is true, and it is the only value that REBUILDS:
// `ShowTraitProfile.updated(from:)` takes its sentinel branch at 0 and replaces
// the vector instead of blending against it.
//
// The directions this file covers, because closing one leaves the others open:
//
//   1. THE RESET HAPPENS, on a store whose profiles were written in the old
//      unit, driven through the real ladder from a rewound V56.
//   2. THE VALUES SURVIVE. Only one key moves. A repair that blanked the
//      column, or rounded a Float, would pass a count-only assertion.
//   3. EVERY OTHER COLUMN SURVIVES — mode, trust, observationCount, the veto
//      counter, the learned priors. V49's contract, restated for this rung.
//   4. IDEMPOTENT ACROSS LAUNCHES, which is the property a device has — the
//      version ladder is the guard. The per-row `episodesObserved != 0`
//      predicate is a COST guard, not an already-migrated marker, and the
//      difference is asserted: a deliberate stamp rewind DOES reset a real
//      count, because no predicate can tell 4 episodes from 4 backfills.
//   5. THE POPULATION IS THE READABLE ONE. A blob the decoder rejects is
//      already read as `.unknown`, so it claims nothing and is left alone
//      rather than being destroyed by a repair it does not need.
//   6. A FIXTURE WITHOUT `podcast_profiles` still reaches v57.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("traitProfile.episodesObserved counts EPISODES (playhead-g7ln V57)")
struct TraitProfileEpisodeCountV57MigrationTests {

    // MARK: - Raw-column probe
    //
    // Deliberately NOT routed through `AnalysisStore.fetchProfile`, for the
    // sibling V55/V56 suites' reason: the claim is about what is ON DISK, and
    // asking the store would ask the same read whose correctness is under test.

    private func rawTraitJSON(in directory: URL, podcastId: String) throws -> String? {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "rawTraitJSON", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(
            db,
            "SELECT traitProfileJSON FROM podcast_profiles WHERE podcastId = ?",
            -1, &stmt, nil
        ) == SQLITE_OK else {
            throw NSError(domain: "rawTraitJSON", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, (podcastId as NSString).utf8String, -1, nil)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        guard sqlite3_column_type(stmt, 0) != SQLITE_NULL else { return nil }
        return String(cString: sqlite3_column_text(stmt, 0))
    }

    /// Rewind to the V56 version stamp. Unlike V52/V55/V56 there is no SHAPE to
    /// rewind: V57 adds no column and drops none, it rewrites a value. The
    /// fixture supplies the pre-V57 shape by writing an inflated count, which
    /// is the state a field device is in — so a rewind that only touches
    /// `_meta` is the honest one here rather than the weak one it would be
    /// for an additive rung.
    ///
    /// Pinned to the LITERAL 56: "pre-g7ln" is a fixed historical fact, and
    /// `currentSchemaVersion - 1` stops meaning it the moment head moves.
    private func rewindToV56(_ store: AnalysisStore) async throws {
        try await store.setMetaValue(forKey: "schema_version", value: "56")
    }

    private func makeProfile(
        podcastId: String,
        traitProfileJSON: String?,
        observationCount: Int = 8,
        mode: String = "manual",
        trust: Double = 0.1,
        falseSignals: Int = 2
    ) -> PodcastProfile {
        PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: "acme,brand",
            normalizedAdSlotPriors: "[0.1,0.5]",
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 4,
            skipTrustScore: trust,
            observationCount: observationCount,
            mode: mode,
            recentFalseSkipSignals: falseSignals,
            traitProfileJSON: traitProfileJSON,
            title: "A Show",
            adDurationStatsJSON: #"{"meanDuration":41.08485035056562,"sampleCount":488}"#,
            networkId: nil
        )
    }

    /// The simplecast profile as it stands on the 2026-08-18 t3 pull, verbatim.
    private static let devicePullTraitJSON = """
        {"insertionVolatility":0.1265696,"transcriptReliability":0.7646194,\
        "singleSpeakerDominance":0.5,"speakerTurnRate":0,"sponsorRecurrence":0,\
        "musicDensity":0.41769138,"episodesObserved":104,"structureRegularity":0.8734304}
        """

    private func decode(_ json: String) throws -> ShowTraitProfile {
        try JSONDecoder().decode(ShowTraitProfile.self, from: Data(json.utf8))
    }

    // MARK: - 1. The reset happens

    @Test("V57 resets an inflated episodesObserved to 0")
    func migrationResetsTheInflatedCount() async throws {
        let dir = try makeTempDir(prefix: "TraitEpisodeCountV57Reset")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.upsertProfile(
            makeProfile(podcastId: "show-a", traitProfileJSON: Self.devicePullTraitJSON)
        )
        try await rewindToV56(store)
        #expect(try await store.schemaVersion() == 56)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)

        let raw = try #require(try rawTraitJSON(in: dir, podcastId: "show-a"))
        let repaired = try decode(raw)
        #expect(repaired.episodesObserved == 0, "104 was a count of backfills, not episodes")
        #expect(repaired.isReliable == false, "the trait tier deactivates until real episodes accrue")
    }

    // MARK: - 2. Only the one key moves

    @Test("V57 preserves every trait VALUE and touches only the count")
    func migrationPreservesTheTraitVector() async throws {
        let dir = try makeTempDir(prefix: "TraitEpisodeCountV57Values")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.upsertProfile(
            makeProfile(podcastId: "show-a", traitProfileJSON: Self.devicePullTraitJSON)
        )
        try await rewindToV56(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let before = try decode(Self.devicePullTraitJSON)
        let after = try decode(try #require(try rawTraitJSON(in: dir, podcastId: "show-a")))
        // Bit-exact, not approximately: every field is a `Float` that decodes
        // and re-encodes to the same text, so a rounding repair is a defect.
        #expect(after.musicDensity == before.musicDensity)
        #expect(after.speakerTurnRate == before.speakerTurnRate)
        #expect(after.singleSpeakerDominance == before.singleSpeakerDominance)
        #expect(after.structureRegularity == before.structureRegularity)
        #expect(after.sponsorRecurrence == before.sponsorRecurrence)
        #expect(after.insertionVolatility == before.insertionVolatility)
        #expect(after.transcriptReliability == before.transcriptReliability)
        #expect(before.episodesObserved == 104)
        #expect(after.episodesObserved == 0)
    }

    // MARK: - 3. Every other column survives — V49's contract, this rung

    @Test("V57 changes no other column: mode, trust, vetoes and priors survive")
    func migrationTouchesNothingElse() async throws {
        let dir = try makeTempDir(prefix: "TraitEpisodeCountV57Untouched")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        // A DEMOTED profile, deliberately: the reading that matters most is
        // that a user's vetoes and the mode they produced survive a repair
        // aimed at a different quantity. Same fixture choice V49 made.
        try await store.upsertProfile(
            makeProfile(
                podcastId: "show-demoted",
                traitProfileJSON: Self.devicePullTraitJSON,
                observationCount: 8,
                mode: "shadow",
                trust: 0.1,
                falseSignals: 2
            )
        )
        try await rewindToV56(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let after = try #require(await reopened.fetchProfile(podcastId: "show-demoted"))
        #expect(after.mode == "shadow")
        #expect(after.skipTrustScore == 0.1)
        #expect(after.recentFalseSkipSignals == 2)
        #expect(after.implicitFalsePositiveCount == 4)
        #expect(after.observationCount == 8, "playhead-2qz6's column is already in EPISODES")
        #expect(after.sponsorLexicon == "acme,brand")
        #expect(after.normalizedAdSlotPriors == "[0.1,0.5]")
        #expect(after.adDurationStatsJSON == #"{"meanDuration":41.08485035056562,"sampleCount":488}"#)
        #expect(after.title == "A Show")
        #expect(after.traitProfile.episodesObserved == 0)
    }

    // MARK: - 4. Idempotent, for the right reason

    @Test("a second launch does not reset an episode counted under the NEW unit")
    func migrationIsIdempotentAcrossLaunches() async throws {
        let dir = try makeTempDir(prefix: "TraitEpisodeCountV57Idem")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.upsertProfile(
            makeProfile(podcastId: "show-a", traitProfileJSON: Self.devicePullTraitJSON)
        )
        try await rewindToV56(store)
        AnalysisStore.resetMigratedPathsForTesting()
        let first = try AnalysisStore(directory: dir)
        try await first.migrate()
        let afterFirst = try #require(try rawTraitJSON(in: dir, podcastId: "show-a"))
        #expect(try decode(afterFirst).episodesObserved == 0)

        // Now let a real, claimed episode accrue, as it would in the field.
        let advanced = try decode(afterFirst).updated(
            from: EpisodeTraitSnapshot(
                musicDensity: 0.2,
                speakerTurnRate: 1,
                singleSpeakerDominance: 0.4,
                structureRegularity: 0.6,
                sponsorRecurrence: 0.1,
                insertionVolatility: 0.4,
                transcriptReliability: 0.8
            )
        )
        #expect(advanced.episodesObserved == 1)
        try await first.upsertProfile(
            makeProfile(
                podcastId: "show-a",
                traitProfileJSON: String(decoding: try JSONEncoder().encode(advanced), as: UTF8.self)
            )
        )

        // The next launch. No stamp rewind — a device that has reached 57 stays
        // there, so the ladder is what makes this a no-op.
        AnalysisStore.resetMigratedPathsForTesting()
        let second = try AnalysisStore(directory: dir)
        try await second.migrate()

        let afterSecond = try decode(try #require(try rawTraitJSON(in: dir, podcastId: "show-a")))
        #expect(
            afterSecond.episodesObserved == 1,
            "a later launch must not throw away an episode counted under the new unit"
        )
    }

    /// THE LIMIT, stated rather than discovered later. Unlike V56's
    /// `latencySampleCount IS NULL`, V57 has NO per-row predicate that can tell
    /// a legitimately-counted 1 from an inflated 1 — the two are the same
    /// integer, and the fact that separates them (which unit wrote it) is
    /// exactly what was never recorded. So a deliberate stamp rewind to 56
    /// after this rung has run DOES reset a real count, and that is asserted
    /// here rather than left as a surprise.
    ///
    /// It is not a field state: nothing rewinds `schema_version`, and the two
    /// production entry points both climb monotonically. V49's reset has the
    /// identical property for the identical reason. The `episodesObserved != 0`
    /// predicate is a cost guard — it stops the rung rewriting rows it has
    /// nothing to say about — and is deliberately NOT dressed up as an
    /// already-migrated marker.
    @Test("a deliberate stamp rewind DOES reset a real count — the stated limit")
    func aStampRewindResetsARealCount() async throws {
        let dir = try makeTempDir(prefix: "TraitEpisodeCountV57Rewind")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let honest = ShowTraitProfile(
            musicDensity: 0.2,
            speakerTurnRate: 1,
            singleSpeakerDominance: 0.4,
            structureRegularity: 0.6,
            sponsorRecurrence: 0.1,
            insertionVolatility: 0.4,
            transcriptReliability: 0.8,
            episodesObserved: 4
        )
        try await store.upsertProfile(
            makeProfile(
                podcastId: "show-a",
                traitProfileJSON: String(decoding: try JSONEncoder().encode(honest), as: UTF8.self)
            )
        )
        try await rewindToV56(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(
            try decode(try #require(try rawTraitJSON(in: dir, podcastId: "show-a"))).episodesObserved == 0,
            "the rung cannot tell 4 episodes from 4 backfills, and does not pretend to"
        )
    }

    @Test("a profile already at 0 is not rewritten at all")
    func migrationLeavesAZeroCountAlone() async throws {
        let dir = try makeTempDir(prefix: "TraitEpisodeCountV57Zero")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        // Written with a DIFFERENT key order from `JSONEncoder`'s, so a
        // needless rewrite is visible as a byte difference. The predicate is
        // `episodesObserved != 0`, so nothing should touch this row.
        let zeroJSON = #"{"episodesObserved":0,"musicDensity":0.25,"speakerTurnRate":0,"singleSpeakerDominance":0.5,"structureRegularity":0.5,"sponsorRecurrence":0,"insertionVolatility":0.5,"transcriptReliability":0.5}"#
        try await store.upsertProfile(makeProfile(podcastId: "show-zero", traitProfileJSON: zeroJSON))
        try await rewindToV56(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try rawTraitJSON(in: dir, podcastId: "show-zero") == zeroJSON)
    }

    // MARK: - 5. The repaired population is the READABLE one

    @Test("a trait blob the decoder rejects is left exactly as it is")
    func migrationLeavesUndecodableBlobsAlone() async throws {
        let dir = try makeTempDir(prefix: "TraitEpisodeCountV57Corrupt")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        // Not JSON at all, and JSON that is missing required keys. Both are
        // ALREADY read as `ShowTraitProfile.unknown` by
        // `PodcastProfile.traitProfile`, so neither claims any maturity and
        // neither needs a repair; destroying them would be a repair inventing
        // work for itself.
        try await store.upsertProfile(makeProfile(podcastId: "show-garbage", traitProfileJSON: "{not json"))
        try await store.upsertProfile(
            makeProfile(podcastId: "show-partial", traitProfileJSON: #"{"episodesObserved":99}"#)
        )
        try await rewindToV56(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try rawTraitJSON(in: dir, podcastId: "show-garbage") == "{not json")
        #expect(try rawTraitJSON(in: dir, podcastId: "show-partial") == #"{"episodesObserved":99}"#)
        // …and the reason that is safe, asserted rather than asserted-about.
        let partial = try #require(await reopened.fetchProfile(podcastId: "show-partial"))
        #expect(partial.traitProfile.episodesObserved == 0)
        #expect(partial.traitProfile.isReliable == false)
    }

    @Test("a NULL traitProfileJSON is not given one")
    func migrationDoesNotFabricateATraitProfile() async throws {
        let dir = try makeTempDir(prefix: "TraitEpisodeCountV57Null")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.upsertProfile(makeProfile(podcastId: "show-null", traitProfileJSON: nil))
        try await rewindToV56(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try rawTraitJSON(in: dir, podcastId: "show-null") == nil)
    }

    // MARK: - 6. The ladder still climbs without the table

    @Test("a fixture with no podcast_profiles still reaches head")
    func migrationSkipsMissingTable() async throws {
        let dir = try makeTempDir(prefix: "TraitEpisodeCountV57NoTable")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.execForTesting("DROP TABLE podcast_profiles")
        try await rewindToV56(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
    }

    @Test("V57 does NOT step over a rolled-back V39")
    func migrationDoesNotStepOverARolledBackRung() async throws {
        let dir = try makeTempDir(prefix: "TraitEpisodeCountV57NoStepOver")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.upsertProfile(
            makeProfile(podcastId: "show-a", traitProfileJSON: Self.devicePullTraitJSON)
        )

        // The state a device is in when V39 rolled back, reproduced exactly as
        // `MergedChildRowDedupeV40MigrationTests` does: no unique asset-identity
        // index, a colliding pair V39 must delete, a trigger that makes the
        // delete ABORT, and `schema_version` at 38. Stamping 38 alone proves
        // nothing — V39 would simply succeed and the whole ladder would climb.
        try await store.execForTesting("DROP INDEX IF EXISTS idx_assets_episode_fingerprint")
        try await store.execForTesting("DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint")
        try await store.execForTesting("""
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES ('dupe-old', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 1.0)
            """)
        try await store.execForTesting("""
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES ('dupe-new', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 2.0)
            """)
        try await store.execForTesting("""
            CREATE TRIGGER g7ln_v39_guard BEFORE DELETE ON analysis_assets
            BEGIN SELECT RAISE(ABORT, 'v57 step-over fixture'); END
            """)
        try await store.setMetaValue(forKey: "schema_version", value: "38")

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 38, "the ladder must stay where V39 left it")
        #expect(
            try decode(try #require(try rawTraitJSON(in: dir, podcastId: "show-a"))).episodesObserved == 104,
            "V57 must not repair a store the ladder cannot legally climb"
        )

        // And the retry a later launch performs completes BOTH rungs.
        try await store.execForTesting("DROP TRIGGER g7ln_v39_guard")
        try await store.migrateOnlyForTesting()
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(
            try decode(try #require(try rawTraitJSON(in: dir, podcastId: "show-a"))).episodesObserved == 0
        )
    }
}
