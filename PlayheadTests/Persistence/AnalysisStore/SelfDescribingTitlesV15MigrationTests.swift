// SelfDescribingTitlesV15MigrationTests.swift
// playhead-i9dj: schema v15 lands two nullable TEXT columns so an exported
// `analysis.sqlite` is legible standalone:
//   * `analysis_assets.episodeTitle  TEXT`
//   * `podcast_profiles.title         TEXT`
//
// Coverage:
//   1. Fresh-migrate shape: both columns present after `migrate()` from
//      a brand-new DB; schema version reaches 15.
//   2. v14-seeded ladder: a DB rewound to v14 picks up the new columns
//      on the next `migrate()`; pre-existing rows decode with NULL for
//      the new columns (i.e. the legacy export path still parses).
//   3. Idempotence: a second `migrate()` is a no-op (schema stays at
//      15, columns are not duplicated).
//   4. Round-trip: writing and reading `episodeTitle` / `title` on the
//      respective rows preserves the value.
//   5. COALESCE-preserve: a follow-up `upsertProfile` whose `title` is
//      `nil` MUST NOT clobber a previously-written title — guards
//      trust-scoring rebuilds that don't have the title in scope.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("AnalysisStore self-describing titles migration v15 (playhead-i9dj)")
struct SelfDescribingTitlesV15MigrationTests {

    // MARK: - 1. Fresh-migrate shape

    @Test("Fresh DB: episodeTitle + title columns land at v15")
    func freshDbAddsTitleColumns() async throws {
        let dir = try makeTempDir(prefix: "TitlesV15-Fresh")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 17)
        #expect(try probeColumnExists(in: dir, table: "analysis_assets", column: "episodeTitle"))
        #expect(try probeColumnExists(in: dir, table: "podcast_profiles", column: "title"))
    }

    // MARK: - 2. v14-seeded ladder

    /// A v14-shaped DB (pre-i9dj) should pick up both new columns on the
    /// next `migrate()`. Pattern: bring the DB to v15 with a real
    /// migrate, then rewind `_meta.schema_version` to 14 and DROP both
    /// new columns. Re-migrate must re-add them.
    @Test("v14-seeded DB picks up titles at v15; legacy rows decode with nil")
    func v14ChainsToV15AndPreservesLegacyRows() async throws {
        let dir = try makeTempDir(prefix: "TitlesV15-FromV14")
        defer { try? FileManager.default.removeItem(at: dir) }

        // Phase 1: real migrate to current.
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        #expect(try await bootstrap.schemaVersion() == 17)

        // Insert one asset row (with title) so we have a pre-existing
        // row to rewind around.
        try await bootstrap.insertAsset(
            AnalysisAsset(
                id: "asset-i9dj-pre",
                episodeId: "ep-i9dj-pre",
                assetFingerprint: "fp-pre",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/pre.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "queued",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
        )

        // Phase 2: rewind to v14 — drop both new columns + reset _meta.
        // SQLite 3.35+ supports ALTER TABLE DROP COLUMN; iOS 26 sim has it.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            ALTER TABLE analysis_assets DROP COLUMN episodeTitle;
            ALTER TABLE podcast_profiles DROP COLUMN title;
            UPDATE _meta SET value = '14' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        // Sanity: the rewind actually removed the columns.
        #expect(!(try probeColumnExists(in: dir, table: "analysis_assets", column: "episodeTitle")))
        #expect(!(try probeColumnExists(in: dir, table: "podcast_profiles", column: "title")))

        // Phase 3: re-migrate. The v14 → v15 block must re-add both columns
        // and the pre-existing asset row must decode with episodeTitle == nil
        // (ADD COLUMN backfills NULL).
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 17)
        #expect(try probeColumnExists(in: dir, table: "analysis_assets", column: "episodeTitle"))
        #expect(try probeColumnExists(in: dir, table: "podcast_profiles", column: "title"))

        let preExisting = try await store.fetchAsset(id: "asset-i9dj-pre")
        #expect(preExisting?.id == "asset-i9dj-pre") // round-trip confirms row survived
        #expect(preExisting?.episodeTitle == nil)
    }

    // MARK: - 3. Idempotence

    @Test("Second migrate() is a no-op — version stays at 15, columns not duplicated")
    func secondMigrateIsNoOp() async throws {
        let dir = try makeTempDir(prefix: "TitlesV15-Idempotent")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()

        #expect(try await store.schemaVersion() == 17)
        #expect(try probeColumnExists(in: dir, table: "analysis_assets", column: "episodeTitle"))
        #expect(try probeColumnExists(in: dir, table: "podcast_profiles", column: "title"))
    }

    // MARK: - 4. Round-trip

    @Test("episodeTitle round-trips via insertAsset → fetchAsset")
    func episodeTitleRoundTrip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: "asset-i9dj-rt",
                episodeId: "ep-i9dj-rt",
                assetFingerprint: "fp-rt",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/rt.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "queued",
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeTitle: "How to escape burnout"
            )
        )

        let reloaded = try await store.fetchAsset(id: "asset-i9dj-rt")
        #expect(reloaded?.episodeTitle == "How to escape burnout")
    }

    @Test("updateAssetEpisodeTitle persists; nil-write is a no-op (preserves prior)")
    func updateEpisodeTitleNilIsNoOp() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: "asset-i9dj-noop",
                episodeId: "ep-i9dj-noop",
                assetFingerprint: "fp",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/n.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "queued",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
        )

        try await store.updateAssetEpisodeTitle(id: "asset-i9dj-noop", episodeTitle: "First Title")
        #expect(try await store.fetchAsset(id: "asset-i9dj-noop")?.episodeTitle == "First Title")

        // nil-write must NOT clobber the previously-recorded value.
        try await store.updateAssetEpisodeTitle(id: "asset-i9dj-noop", episodeTitle: nil)
        #expect(try await store.fetchAsset(id: "asset-i9dj-noop")?.episodeTitle == "First Title")

        // Non-nil write overwrites.
        try await store.updateAssetEpisodeTitle(id: "asset-i9dj-noop", episodeTitle: "Second Title")
        #expect(try await store.fetchAsset(id: "asset-i9dj-noop")?.episodeTitle == "Second Title")
    }

    @Test("title round-trips via upsertProfile → fetchProfile")
    func profileTitleRoundTrip() async throws {
        let store = try await makeTestStore()
        let profile = PodcastProfile(
            podcastId: "pod-i9dj",
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: 0,
            mode: "shadow",
            recentFalseSkipSignals: 0,
            traitProfileJSON: nil,
            title: "Diary of a CEO"
        )
        try await store.upsertProfile(profile)

        let reloaded = try await store.fetchProfile(podcastId: "pod-i9dj")
        #expect(reloaded?.title == "Diary of a CEO")
    }

    // MARK: - 5. COALESCE-preserve on upsert

    /// Trust-scoring fetches a profile, mutates one field, and re-upserts
    /// with `title: nil`. The COALESCE clause in `upsertProfile` must
    /// preserve the previously-recorded title rather than clobbering with
    /// NULL — this is the core safety guarantee for the lazy-backfill
    /// design.
    @Test("upsertProfile with nil title preserves previously-recorded title (COALESCE)")
    func upsertNilTitlePreservesExisting() async throws {
        let store = try await makeTestStore()
        try await store.upsertProfile(
            PodcastProfile(
                podcastId: "pod-coalesce",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: 0.5,
                observationCount: 1,
                mode: "shadow",
                recentFalseSkipSignals: 0,
                traitProfileJSON: nil,
                title: "Original Show Title"
            )
        )

        // Simulate trust-scoring rebuilding the profile without the title
        // in scope. `title` defaults to nil — the upsert must preserve
        // the existing title via COALESCE.
        try await store.upsertProfile(
            PodcastProfile(
                podcastId: "pod-coalesce",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 1,
                skipTrustScore: 0.7,
                observationCount: 2,
                mode: "shadow",
                recentFalseSkipSignals: 0
                // title omitted → defaults to nil
            )
        )

        let reloaded = try await store.fetchProfile(podcastId: "pod-coalesce")
        #expect(reloaded?.title == "Original Show Title")
        #expect(reloaded?.observationCount == 2) // confirms the upsert applied
    }

    @Test("updateProfileTitle is a no-op when no profile row exists")
    func updateProfileTitleNoOpWhenAbsent() async throws {
        let store = try await makeTestStore()
        try await store.updateProfileTitle(podcastId: "pod-absent", title: "Ghost Show")
        let reloaded = try await store.fetchProfile(podcastId: "pod-absent")
        #expect(reloaded?.podcastId == nil) // optional-chain nil-test: no row materialized
    }

    @Test("updateProfileTitle with nil leaves prior title untouched")
    func updateProfileTitleNilIsNoOp() async throws {
        let store = try await makeTestStore()
        try await store.upsertProfile(
            PodcastProfile(
                podcastId: "pod-update-nil",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: 0.5,
                observationCount: 1,
                mode: "shadow",
                recentFalseSkipSignals: 0,
                traitProfileJSON: nil,
                title: "Initial Title"
            )
        )

        try await store.updateProfileTitle(podcastId: "pod-update-nil", title: nil)
        #expect(try await store.fetchProfile(podcastId: "pod-update-nil")?.title == "Initial Title")

        try await store.updateProfileTitle(podcastId: "pod-update-nil", title: "Updated Title")
        #expect(try await store.fetchProfile(podcastId: "pod-update-nil")?.title == "Updated Title")
    }
}
