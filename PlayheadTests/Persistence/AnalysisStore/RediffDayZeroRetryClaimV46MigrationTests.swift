// RediffDayZeroRetryClaimV46MigrationTests.swift
// playhead-3oyz: pin the V46 migration that adds the day-0 same-session
// retry-claim columns (`retryClaimCount` / `lastRetryClaimAt`) to
// `rediff_day_zero_attempts`.
//
// WHY THIS FILE EXISTS SEPARATELY FROM `DayZeroSameSessionRetryTests`. That
// suite exercises the claim WRITER against a store built by `createTables()`,
// which creates the two columns unconditionally — so every one of its
// assertions would still pass with `migrateRediffDayZeroRetryClaimV46IfNeeded`
// deleted outright. Nothing there ever opens a database that predates the
// columns, and a device upgrading from v45 is precisely that database. The
// upgrade path is a different claim and needs its own evidence.
//
// Coverage targets, matching the V30/V43 sibling rungs:
//   1. Fresh-DB `migrate()` reaches head with both columns present, and head
//      is pinned to the LITERAL 46.
//   2. A v45-shaped `rediff_day_zero_attempts` (no claim columns) upgrades in
//      place: the columns are added, an existing attempt row survives with
//      EVERY other field intact — including `policyGeneration` (SELECT index
//      15) and `rescueAttemptCount` (16), the two reads immediately ahead of
//      the appended pair — and the new columns arrive at their DOCUMENTED
//      defaults (0 / NULL), which is the truth for every pre-v46 row: no build
//      before this one could claim a retry.
//   3. The migration is idempotent.
//   4. The ISOLATED ladder (`migrateOnlyForTesting`) reaches v46 and adds the
//      columns — `createTables()` would otherwise mask a rung that never runs.
//   5. The rung refuses to step over a rolled-back V39, the guard every rung
//      after V39 carries and which the ladder does not enforce structurally.

import Foundation
import Testing

@testable import Playhead

@Suite("rediff_day_zero_attempts retry-claim V46 migration (playhead-3oyz)")
struct RediffDayZeroRetryClaimV46MigrationTests {

    private static let table = "rediff_day_zero_attempts"
    private static let claimColumns = ["retryClaimCount", "lastRetryClaimAt"]

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "RediffDayZeroRetryClaimV46")
    }

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    /// Rewind an at-head database to the v45 shape: the two claim columns
    /// genuinely GONE, not merely the version stamped back. A rewind that only
    /// touches `_meta` proves nothing — the rung would run against a table that
    /// already has everything it adds.
    private func rewindToV45(_ store: AnalysisStore) async throws {
        try await store.execForTesting("""
            ALTER TABLE rediff_day_zero_attempts DROP COLUMN retryClaimCount;
            ALTER TABLE rediff_day_zero_attempts DROP COLUMN lastRetryClaimAt;
            """)
        // Pinned to the LITERAL 45, per the playhead-hx6n lesson recorded in
        // `DayZeroDownloadTimeStoreTests`: "pre-3oyz" is v45, a fixed
        // historical fact. Written as `currentSchemaVersion - 1` it would stop
        // meaning that the moment head moved past 46 — the rewind would land ON
        // 46, V46's `observed < 46` guard would decline, and the test would
        // assert the absence of columns it had just prevented from being added.
        try await store.setMetaValue(forKey: "schema_version", value: "45")
    }

    private func columnsPresent(in dir: URL) throws -> Bool {
        try Self.claimColumns.allSatisfy {
            try probeColumnExists(in: dir, table: Self.table, column: $0)
        }
    }

    private func columnsAbsent(in dir: URL) throws -> Bool {
        try Self.claimColumns.allSatisfy {
            !(try probeColumnExists(in: dir, table: Self.table, column: $0))
        }
    }

    // MARK: - Migration ladder

    @Test("fresh DB migrate() lands both retry-claim columns at head")
    func freshDbHasV46Columns() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // Drift guard, pinned to the LITERAL head. Whoever bumps the schema
        // next has to come here and read this rung. Written as
        // `== AnalysisStore.currentSchemaVersion` it would pass for every
        // possible value and police nothing.
        #expect(AnalysisStore.currentSchemaVersion == 48)
        #expect(try columnsPresent(in: dir))
    }

    @Test("v45-shaped rediff_day_zero_attempts upgrades in place: columns added, the pre-3oyz row survives at the documented defaults")
    func seededV45RowUpgradesWithoutDataLoss() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        try await bootstrap.insertAsset(makeAsset(id: "asset-v45"))

        // A row with a NON-DEFAULT value in every field a v45 binary could
        // write, so "survives" is a real claim rather than "all zeroes stayed
        // zero". `policyGeneration` / `rescueAttemptCount` are the load-bearing
        // pair: they are read at SELECT indices 15 / 16, immediately ahead of
        // the appended claim columns at 17 / 18, so they come back wrong if the
        // append shifted the positional reader.
        //
        // The claim pair is seeded non-zero DELIBERATELY. The rewind below
        // DROPS those columns, so these two values are destroyed with them —
        // which is what makes the post-migration read of 0 / nil evidence that
        // `DEFAULT 0` and the nullable column supplied them, not that a seeded
        // value merely survived.
        try await bootstrap.upsertRediffDayZeroAttempt(
            RediffDayZeroAttemptRecord(
                analysisAssetId: "asset-v45",
                attemptCount: 2,
                lastAttemptAt: 1_785_978_148,
                lastExit: .fetchFailed,
                lastMarkCount: 3,
                lastBSideCount: 4,
                lastBSidesAccepted: 5,
                lastBSidesGateRejected: 6,
                lastBSidesUnreadable: 7,
                lastDivergentSlotCount: 8,
                lastFullFetchBytes: 108_000_000,
                totalFullFetchBytes: 216_000_000,
                suppressedCount: 0,
                lastSuppressedAt: nil,
                lastDetail: "-1001",
                policyGeneration: 9,
                rescueAttemptCount: 1,
                retryClaimCount: 77,
                lastRetryClaimAt: 1_785_978_178
            )
        )

        try await rewindToV45(bootstrap)
        #expect(try columnsAbsent(in: dir), "the fixture must genuinely predate the columns")

        // Re-open and migrate: the v45→v46 step must add both columns.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try columnsPresent(in: dir))

        let row = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "asset-v45"))
        #expect(row.attemptCount == 2)
        #expect(row.lastAttemptAt == 1_785_978_148)
        #expect(row.lastExit == .fetchFailed)
        #expect(row.lastMarkCount == 3)
        #expect(row.lastBSideCount == 4)
        #expect(row.lastBSidesAccepted == 5)
        #expect(row.lastBSidesGateRejected == 6)
        #expect(row.lastBSidesUnreadable == 7)
        #expect(row.lastDivergentSlotCount == 8)
        #expect(row.lastFullFetchBytes == 108_000_000)
        #expect(row.totalFullFetchBytes == 216_000_000)
        #expect(row.lastDetail == "-1001")
        #expect(row.policyGeneration == 9,
                "policyGeneration (idx 15) must survive the appended claim columns — no positional shift")
        #expect(row.rescueAttemptCount == 1,
                "rescueAttemptCount (idx 16) must survive the appended claim columns — no positional shift")

        // V46 is ADDITIVE WITH NO BACKFILL. A pre-3oyz row never claimed a
        // retry, so the honest reading is "none, and none was ever recorded" —
        // never an invented timestamp for a claim nobody made.
        #expect(row.retryClaimCount == 0,
                "DEFAULT 0 is the truth for every pre-v46 row: no build before this one could claim a retry")
        #expect(row.lastRetryClaimAt == nil,
                "nullable with no backfill — inventing an instant would assert a claim that never happened")

        // …and the upgraded row is immediately usable by the claim writer, so
        // the migration produced a working column, not just a present one.
        try await store.noteRediffDayZeroRetryClaim(assetId: "asset-v45", at: 1_785_978_200)
        let claimed = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "asset-v45"))
        #expect(claimed.retryClaimCount == 1)
        #expect(claimed.lastRetryClaimAt == 1_785_978_200)
        #expect(claimed.attemptCount == 2, "a claim is not an attempt")
    }

    @Test("V46 migration is idempotent across resetMigratedPathsForTesting")
    func v46MigrationIsIdempotent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-idem"))
        try await store.noteRediffDayZeroRetryClaim(assetId: "asset-idem", at: 500)
        let v1 = try await store.schemaVersion()

        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()
        let v2 = try await store.schemaVersion()

        #expect(v1 == AnalysisStore.currentSchemaVersion)
        #expect(v2 == AnalysisStore.currentSchemaVersion)
        #expect(try columnsPresent(in: dir))
        let row = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "asset-idem"))
        #expect(row.retryClaimCount == 1, "a re-run must not reset the claim counter")
        #expect(row.lastRetryClaimAt == 500)
    }

    @Test("isolated ladder (migrateOnlyForTesting) reaches v46 and adds the columns")
    func isolatedLadderReachesV46() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        try await rewindToV45(store)
        #expect(try columnsAbsent(in: dir))

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try columnsPresent(in: dir),
                "the RUNG, not createTables(), is what adds the claim columns on an upgrade")
    }

    /// A v39 rollback leaves `_meta` at 38 so the next launch retries it. Every
    /// rung added after V39 has to refuse to step over that, and the ladder does
    /// not enforce it structurally — so this rung's `guard observed >= 45` needs
    /// its own witness, exactly as V40–V44 have.
    ///
    /// **A rewound `_meta` alone does NOT reproduce the condition.** On a
    /// healthy DB V39 simply succeeds and the ladder climbs straight back to
    /// head. The fixture has to make V39 genuinely ROLL BACK — the duplicate
    /// asset pair plus the ABORT trigger, the shape first paid for in
    /// `MergedChildRowDedupeV40MigrationTests`.
    @Test("playhead-3oyz: v46 does not step over a rolled-back v39")
    func v46DoesNotStepOverARolledBackV39() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // The claim columns gone, so "the rung did not run" is observable
        // rather than inferred.
        try await store.execForTesting("""
            ALTER TABLE rediff_day_zero_attempts DROP COLUMN retryClaimCount;
            ALTER TABLE rediff_day_zero_attempts DROP COLUMN lastRetryClaimAt;
            """)

        // Rewind to the state a device is in when V39 rolled back: no unique
        // asset-identity index, a duplicate pair for V39 to trip over, a
        // trigger that makes its delete ABORT, and `schema_version` at 38.
        try await store.execForTesting("""
            DROP INDEX IF EXISTS idx_assets_episode_fingerprint;
            DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint;
            INSERT INTO analysis_assets
              (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
              VALUES ('dupe-old', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 1.0);
            INSERT INTO analysis_assets
              (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
              VALUES ('dupe-new', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 2.0);
            CREATE TRIGGER oyz_v39_guard BEFORE DELETE ON analysis_assets
              BEGIN SELECT RAISE(ABORT, 'v46 step-over fixture'); END;
            UPDATE _meta SET value = '38' WHERE key = 'schema_version';
            """)

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 38,
                "a DB held at 38 by a rolled-back v39 must not be stamped 46")
        #expect(try columnsAbsent(in: dir),
                "…and the V46 rung must not have run against a database it had no business touching")
    }
}
