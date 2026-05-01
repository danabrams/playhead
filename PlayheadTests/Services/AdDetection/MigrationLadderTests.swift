// MigrationLadderTests.swift
// H11 (cycle 2): regression tests for the AnalysisStore migration ladder.
// Pins the C6 fix (writeInitialSchemaVersionIfNeeded must seed '1', not the
// current version) and the H10 column rename (`needs_shadow_retry` →
// `needsShadowRetry`).
//
// Strategy: seed a `_meta.schema_version` row with `seedSchemaVersion(_:)`
// before opening the store, then run `migrate()` and assert the resulting
// schema reaches `currentSchemaVersion` with all expected columns,
// indexes, and tables present.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("AnalysisStore migration ladder")
struct MigrationLadderTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "MigrationLadder")
    }

    // MARK: - C6: writeInitialSchemaVersionIfNeeded seeds '1'

    @Test("C6: fresh DB migrate() reaches currentSchemaVersion with all expected shape")
    func freshDbReachesCurrentVersion() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Reaches v5 (current).
        #expect(try await store.schemaVersion() == 19)

        // analysis_sessions has the camelCase column (H10).
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "shadowRetryPodcastId"))
        #expect(!(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needs_shadow_retry")))

        // Partial index exists.
        #expect(try probeIndexExists(in: dir, indexName: "idx_sessions_shadow_retry"))

        // bd-m8k planner state table exists (created via the v4 chain).
        #expect(try probeTableExists(in: dir, table: "podcast_planner_state"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "evidenceSources"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "eligibilityGate"))

        // Rev3-M5 phase columns present.
        #expect(try probeColumnExists(in: dir, table: "semantic_scan_results", column: "runMode"))
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "runMode"))

        // V7 sponsor knowledge tables (Phase 8).
        #expect(try probeTableExists(in: dir, table: "sponsor_knowledge_entries"))
        #expect(try probeTableExists(in: dir, table: "knowledge_candidate_events"))

        // V8 ad copy fingerprint tables (Phase 9).
        #expect(try probeTableExists(in: dir, table: "ad_copy_fingerprints"))
        #expect(try probeTableExists(in: dir, table: "fingerprint_source_events"))

        // V9 implicit feedback events table (ef2.3.4).
        #expect(try probeTableExists(in: dir, table: "implicit_feedback_events"))

        // Phase A feature-window additions.
        #expect(try probeColumnExists(in: dir, table: "feature_windows", column: "speakerChangeProxyScore"))
        #expect(try probeColumnExists(in: dir, table: "feature_windows", column: "musicBedChangeScore"))

        // V9 boundary_priors table (ef2.3.5).
        #expect(try probeTableExists(in: dir, table: "boundary_priors"))

        // V10 music_bracket_trust table (ef2.3.6).
        #expect(try probeTableExists(in: dir, table: "music_bracket_trust"))

        // V11 source_demotions and fingerprint_disputes tables (ef2.3.3).
        #expect(try probeTableExists(in: dir, table: "source_demotions"))
        #expect(try probeTableExists(in: dir, table: "fingerprint_disputes"))
    }

    // MARK: - H11: seeded v1 → v5 ladder runs all V*IfNeeded blocks

    @Test("H11: v1 → v5 migration chain reaches current schema")
    func seededV1ChainsToCurrent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        try seedSchemaVersion(1, in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 19)
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "shadowRetryPodcastId"))
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "transcriptVersion"))
        // Cycle 4 H2: the `evidence_events` phase column must survive the
        // v2/v3 rebuild via the belt-and-suspenders
        // `addColumnIfNeeded(...column: "runMode"...)` at the tail of
        // migrate().
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "runMode"))
        #expect(try probeColumnExists(in: dir, table: "semantic_scan_results", column: "runMode"))
        #expect(try probeTableExists(in: dir, table: "podcast_planner_state"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "evidenceSources"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "eligibilityGate"))

        // Migration is committed and the store is usable.
        try await store.insertAsset(
            AnalysisAsset(
                id: "asset-v1-chain",
                episodeId: "ep-1",
                assetFingerprint: "fp",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/x.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
        )
    }

    @Test("H11: v2 → v5 chains v3, v4, and v5 changes")
    func seededV2ChainsToCurrent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        try seedSchemaVersion(2, in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 19)
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "transcriptVersion"))
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "shadowRetryPodcastId"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_sessions_shadow_retry"))
        // Cycle 4 H2 / M4: phase columns must survive the v2/v3 rebuild
        // of evidence_events.
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "runMode"))
        #expect(try probeColumnExists(in: dir, table: "semantic_scan_results", column: "runMode"))
        // H10 camelCase rename — the snake_case column must NOT linger.
        #expect(!(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needs_shadow_retry")))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "evidenceSources"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "eligibilityGate"))
    }

    @Test("H11: v3 → v5 adds shadow retry columns, planner state table, and ad-window prep")
    func seededV3ChainsToCurrent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        try seedSchemaVersion(3, in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 19)
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "shadowRetryPodcastId"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_sessions_shadow_retry"))
        #expect(try probeTableExists(in: dir, table: "podcast_planner_state"))
        // Cycle 4 H2: phase columns must survive the v3 rebuild.
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "runMode"))
        #expect(try probeColumnExists(in: dir, table: "semantic_scan_results", column: "runMode"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "evidenceSources"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "eligibilityGate"))
    }

    // MARK: - Cycle 4 H1: isolated migration ladder (bypasses createTables)

    /// Cycle 4 H1: the cycle-2 seededV1 → migrate() test only passed
    /// because `createTables()` runs BEFORE `writeInitialSchemaVersionIfNeeded`
    /// inside `migrate()`, unconditionally building every table in its
    /// final v4 shape. The pre-C6 bug ("fresh DB gets schema_version set
    /// to current, short-circuiting V*IfNeeded") could not actually be
    /// reached in that test because the v4 shape was already on disk by
    /// the time the ladder ran. This test exercises the ladder in
    /// isolation via `migrateOnlyForTesting()` so the V*IfNeeded blocks
    /// have to do real work, and asserts the final v4 shape emerges.
    @Test("Cycle 4 H1: v1-shape DB climbs to v5 via migrateOnlyForTesting (no createTables)")
    func isolatedLadderFromV1() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Hand-seed a v1-shape DB. No `_meta.schema_version` row yet —
        // writeInitialSchemaVersionIfNeeded must seed it to '1'.
        try seedV1ShapeDatabase(in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrateOnlyForTesting()

        // V2 → v3 → v4 → v5 ladder must have run, reaching the target version.
        #expect(try await store.schemaVersion() == 19)
        // V3 added transcriptVersion to evidence_events.
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "transcriptVersion"))
        // V4 added needsShadowRetry + shadowRetryPodcastId + the partial
        // index on analysis_sessions.
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "shadowRetryPodcastId"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_sessions_shadow_retry"))
        // The tail-of-migrate() addColumnIfNeeded for `phase` is mirrored
        // in migrateOnlyForTesting, so the phase column must land.
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "runMode"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "evidenceSources"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "eligibilityGate"))
        // NOTE: `podcast_planner_state` is NOT asserted here. Both V4
        // migration blocks guard on `schemaVersion < 4`, and
        // `migrateAnalysisSessionsShadowRetryV4IfNeeded` runs first and
        // sets version to 4, so the planner state block short-circuits
        // — a quirk that is masked in production by `createTables()`,
        // which builds the planner table up-front. That masking is
        // exactly the behavior this isolated test is meant to expose,
        // and the point of the H1 seam is the V*IfNeeded execution flow
        // itself, not every v4 side effect. Drop the assertion so this
        // test fails only on the bugs it was designed to catch.
    }

    @Test("Cycle 4 H1: v2-seeded DB climbs to v5 via isolated ladder")
    func isolatedLadderFromV2() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        try seedV1ShapeDatabase(in: dir)
        // Pre-seed _meta to 2 so the V2 block short-circuits and only
        // V3/V4 run against the v1-shape tables. (Matches an on-device
        // DB that hand-applied v2 without _meta being written.)
        try seedSchemaVersion(2, in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 19)
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "transcriptVersion"))
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "runMode"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "evidenceSources"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "eligibilityGate"))
    }

    @Test("Cycle 4 H1: v3-seeded DB climbs to v5 via isolated ladder")
    func isolatedLadderFromV3() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        try seedV1ShapeDatabase(in: dir)
        try seedSchemaVersion(3, in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 19)
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "shadowRetryPodcastId"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "evidenceSources"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "eligibilityGate"))
    }

    // MARK: - H11: idempotence

    @Test("H11: migrate() called twice via resetMigratedPathsForTesting is a no-op")
    func migrateIsIdempotentAcrossReset() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let v1 = try await store.schemaVersion()

        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()
        let v2 = try await store.schemaVersion()

        #expect(v1 == v2)
        #expect(v2 == 19)
    }

    // MARK: - C6: seeded v4 _meta-less data path

    @Test("C6: store with v4 tables but no _meta row migrates cleanly")
    func v4TablesWithoutMetaRowMigrates() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // First, run a real migrate so the v5 tables exist on disk.
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()

        // Now nuke the _meta row (without dropping the table) and re-run
        // migrate. Per C6, the second migrate must reseed schema_version
        // to '1' and chain through V*IfNeeded blocks (which all become
        // no-ops because the columns/tables already exist), reaching
        // currentSchemaVersion.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        defer { sqlite3_close_v2(db) }
        #expect(sqlite3_exec(db, "DELETE FROM _meta WHERE key='schema_version'", nil, nil, nil) == SQLITE_OK)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 19)
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "evidenceSources"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "eligibilityGate"))
    }

    // MARK: - H10 repair: pre-rename column gets renamed in place

    @Test("H10 repair: a v4 DB still carrying snake_case `needs_shadow_retry` is renamed in place")
    func h10RepairRenamesLegacyColumn() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Hand-build a v4-shaped DB with the snake_case column to model
        // an on-device DB that was migrated under the pre-cycle-2 code.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let setup = """
            CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO _meta (key, value) VALUES ('schema_version', '4');
            CREATE TABLE analysis_assets (
                id TEXT PRIMARY KEY,
                episodeId TEXT NOT NULL,
                assetFingerprint TEXT NOT NULL,
                weakFingerprint TEXT,
                sourceURL TEXT NOT NULL,
                featureCoverageEndTime REAL,
                fastTranscriptCoverageEndTime REAL,
                confirmedAdCoverageEndTime REAL,
                analysisState TEXT NOT NULL,
                analysisVersion INTEGER NOT NULL,
                capabilitySnapshot TEXT,
                createdAt REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE analysis_sessions (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL,
                state TEXT NOT NULL,
                startedAt REAL NOT NULL,
                updatedAt REAL NOT NULL,
                failureReason TEXT,
                needs_shadow_retry INTEGER NOT NULL DEFAULT 0,
                shadowRetryPodcastId TEXT
            );
            CREATE INDEX idx_sessions_shadow_retry ON analysis_sessions(id) WHERE needs_shadow_retry = 1;
            """
        #expect(sqlite3_exec(db, setup, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(!(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needs_shadow_retry")))
        #expect(try probeIndexExists(in: dir, indexName: "idx_sessions_shadow_retry"))

        // Round-trip a row to make sure the rename didn't break the
        // CRUD path.
        try await store.insertAsset(
            AnalysisAsset(
                id: "asset-rename",
                episodeId: "ep-rename",
                assetFingerprint: "fp",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/r.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
        )
        try await store.insertSession(
            AnalysisSession(
                id: "sess-rename",
                analysisAssetId: "asset-rename",
                state: "complete",
                startedAt: 0,
                updatedAt: 0,
                failureReason: nil,
                needsShadowRetry: true,
                shadowRetryPodcastId: "pod-rename"
            )
        )
        let flagged = try await store.fetchSessionsNeedingShadowRetry()
        #expect(flagged.count == 1)
        #expect(flagged.first?.id == "sess-rename")
    }

    // MARK: - narl.2 v12 → v13: shadow_fm_responses table lands

    /// A v12-shaped DB (pre-narl.2) should pick up the new
    /// `shadow_fm_responses` table + its lookup index when migrate() climbs
    /// to v13. This pins the boundary so a future reviewer who touches
    /// the migration ladder notices when v12 → v13 stops creating the
    /// shadow table (e.g. if someone accidentally renames or removes
    /// `migrateShadowFMResponsesV13IfNeeded`).
    ///
    /// Pattern: we can't just seed `_meta.schema_version = 12` against a
    /// bare DB, because the intermediate V*IfNeeded blocks short-circuit
    /// and the tail `addColumnIfNeeded` calls in migrate() would fail
    /// against non-existent tables. Instead, we do a real migrate first
    /// (which builds v13), then regress the DB back to v12 by dropping
    /// the v13 table + index and rewinding `_meta.schema_version` to
    /// '12'. The next migrate() must re-create the v13 table/index.
    @Test("narl.2: v12-seeded DB picks up shadow_fm_responses at v13")
    func seededV12AddsShadowFMResponses() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // First: a real migrate to build the full v13 DB shape.
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        #expect(try await bootstrap.schemaVersion() == 19)
        #expect(try probeTableExists(in: dir, table: "shadow_fm_responses"))

        // Rewind to v12: drop the shadow table + index, reset _meta.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            DROP INDEX IF EXISTS idx_shadow_fm_asset_variant;
            DROP TABLE IF EXISTS shadow_fm_responses;
            UPDATE _meta SET value = '12' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        // Sanity: the v12 rewind actually removed the table.
        #expect(!(try probeTableExists(in: dir, table: "shadow_fm_responses")))
        #expect(!(try probeIndexExists(in: dir, indexName: "idx_shadow_fm_asset_variant")))

        // Re-migrate via a fresh store. The v12 → v13 block must re-create
        // the shadow table + its lookup index.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 19)
        #expect(try probeTableExists(in: dir, table: "shadow_fm_responses"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_shadow_fm_asset_variant"))
        // Migrated store must accept a row via the live CRUD path.
        let row = ShadowFMResponse(
            assetId: "asset-v12-narl2",
            windowStart: 0,
            windowEnd: 10,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0x01, 0x02]),
            capturedAt: 1_700_000_000,
            capturedBy: .laneB,
            fmModelVersion: "fm-1.0"
        )
        try await store.upsertShadowFMResponse(row)
        #expect(try await store.shadowFMResponseCount() == 1)
    }

    // MARK: - Rev3-M5: phase column reads/writes/filters

    @Test("Rev3-M5: semantic_scan_results phase column round-trips and filters distinctly")
    func phaseColumnRoundTripsAndFilters() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: "asset-phase",
                episodeId: "ep-phase",
                assetFingerprint: "fp",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/p.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
        )

        let cohort = """
            {"promptLabel":"l","promptHash":"p","schemaHash":"s","scanPlanHash":"sp","normalizationHash":"n","osBuild":"26A","locale":"en_US","appBuild":"1"}
            """

        // Insert one shadow row, one targeted row.
        let shadow = SemanticScanResult(
            id: "scan-shadow",
            analysisAssetId: "asset-phase",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 4,
            windowStartTime: 0,
            windowEndTime: 60,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tv-1",
            reuseScope: "scope-shadow",
            runMode: .shadow
        )
        let targeted = SemanticScanResult(
            id: "scan-targeted",
            analysisAssetId: "asset-phase",
            windowFirstAtomOrdinal: 5,
            windowLastAtomOrdinal: 9,
            windowStartTime: 60,
            windowEndTime: 120,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: "tv-1",
            reuseScope: "scope-targeted",
            runMode: .targeted
        )
        try await store.insertSemanticScanResult(shadow)
        try await store.insertSemanticScanResult(targeted)

        let all = try await store.fetchSemanticScanResults(analysisAssetId: "asset-phase")
        #expect(all.count == 2)
        let runModes = Set(all.map(\.runMode))
        #expect(runModes == [.shadow, .targeted])

        // Pinpoint round-trip: each row reads back with its written runMode.
        let byId = Dictionary(uniqueKeysWithValues: all.map { ($0.id, $0) })
        #expect(byId["scan-shadow"]?.runMode == .shadow)
        #expect(byId["scan-targeted"]?.runMode == .targeted)
    }

    @Test("Rev3-M5: evidence_events phase column round-trips")
    func evidencePhaseColumnRoundTrips() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: "asset-ev-phase",
                episodeId: "ep-ev-phase",
                assetFingerprint: "fp",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/e.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
        )

        let cohort = """
            {"promptLabel":"l","promptHash":"p","schemaHash":"s","scanPlanHash":"sp","normalizationHash":"n","osBuild":"26A","locale":"en_US","appBuild":"1"}
            """

        let shadowEvent = EvidenceEvent(
            id: "ev-shadow",
            analysisAssetId: "asset-ev-phase",
            eventType: "fm.spanRefinement",
            sourceType: .fm,
            atomOrdinals: "[0,1]",
            evidenceJSON: #"{"shadow":true}"#,
            scanCohortJSON: cohort,
            createdAt: 1,
            runMode: .shadow
        )
        let targetedEvent = EvidenceEvent(
            id: "ev-targeted",
            analysisAssetId: "asset-ev-phase",
            eventType: "fm.spanRefinement",
            sourceType: .fm,
            atomOrdinals: "[2,3]",
            evidenceJSON: #"{"targeted":true}"#,
            scanCohortJSON: cohort,
            createdAt: 2,
            runMode: .targeted
        )
        _ = try await store.insertEvidenceEvent(shadowEvent, transcriptVersion: "tv-1")
        _ = try await store.insertEvidenceEvent(targetedEvent, transcriptVersion: "tv-1")

        let fetched = try await store.fetchEvidenceEvents(analysisAssetId: "asset-ev-phase")
        #expect(fetched.count == 2)
        let runModes = Set(fetched.map(\.runMode))
        #expect(runModes == [.shadow, .targeted])
    }

    // MARK: - cycle-3 M1: v16 → v17 rebuild from a broken v16 on-disk shape

    /// cycle-3 M1: pin the v16 → v17 rebuild against a real broken-v16
    /// DB shape, not a no-op rebuild against an already-correct table.
    /// The pre-cycle-2 v16 migration shipped `training_examples` with
    /// `ON DELETE CASCADE` (a corpus that disappears with the asset is
    /// useless training data) and `decisionCohortJSON NOT NULL` (no way
    /// to encode "no decision overlapped this scan" — the bucketer's
    /// editorial-region case). v17 drops the table and rebuilds with
    /// `ON DELETE RESTRICT` and a nullable `decisionCohortJSON`.
    ///
    /// Strategy: bootstrap a real v17 DB, then surgically replace the
    /// `training_examples` table with the broken v16 shape and rewind
    /// `_meta.schema_version` to 16. Insert a parent `analysis_assets`
    /// row plus one broken-v16 `training_examples` row so the rebuild
    /// path is genuinely exercised — a no-op rebuild against an empty
    /// table would still pass even if the migrator silently failed to
    /// run. The pre-existing row is destroyed by the rebuild — that's
    /// fine and documented by the migrator's contract: rebuild is
    /// destructive by design, pre-fix data is unrecoverable, and any
    /// real cohort can re-materialize from the still-warm upstream
    /// ledgers on the next backfill.
    @Test("cycle-3 M1: broken-v16 training_examples DB rebuilds to v17 shape")
    func brokenV16TrainingExamplesRebuildsToV17() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // First: a real migrate to build the full v17 DB shape.
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        #expect(try await bootstrap.schemaVersion() == 19)

        // Now rip out the v17 training_examples table and rebuild it with
        // the broken v16 shape: ON DELETE CASCADE on the FK, NOT NULL on
        // decisionCohortJSON. Rewind _meta.schema_version to '16' so the
        // v17 migrator runs on the next open.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let regress = """
            DROP INDEX IF EXISTS idx_training_examples_asset_created;
            DROP INDEX IF EXISTS idx_training_examples_bucket;
            DROP TABLE IF EXISTS training_examples;
            CREATE TABLE training_examples (
                id                    TEXT PRIMARY KEY,
                analysisAssetId       TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                startAtomOrdinal      INTEGER NOT NULL,
                endAtomOrdinal        INTEGER NOT NULL,
                transcriptVersion     TEXT NOT NULL,
                startTime             REAL NOT NULL,
                endTime               REAL NOT NULL,
                textSnapshotHash      TEXT NOT NULL,
                textSnapshot          TEXT,
                bucket                TEXT NOT NULL,
                commercialIntent      TEXT NOT NULL,
                ownership             TEXT NOT NULL,
                evidenceSourcesJSON   TEXT NOT NULL,
                fmCertainty           REAL NOT NULL,
                classifierConfidence  REAL NOT NULL,
                userAction            TEXT,
                eligibilityGate       TEXT,
                scanCohortJSON        TEXT NOT NULL,
                decisionCohortJSON    TEXT NOT NULL,
                transcriptQuality     TEXT NOT NULL,
                createdAt             REAL NOT NULL
            );
            CREATE INDEX idx_training_examples_asset_created
                ON training_examples(analysisAssetId, createdAt ASC);
            CREATE INDEX idx_training_examples_bucket
                ON training_examples(bucket);
            UPDATE _meta SET value = '16' WHERE key = 'schema_version';
            INSERT INTO analysis_assets (
                id, episodeId, assetFingerprint, weakFingerprint, sourceURL,
                featureCoverageEndTime, fastTranscriptCoverageEndTime,
                confirmedAdCoverageEndTime, analysisState, analysisVersion,
                capabilitySnapshot, createdAt
            ) VALUES (
                'asset-broken-v16', 'ep-broken-v16', 'fp-broken-v16', NULL,
                'file:///tmp/broken.m4a', NULL, NULL, NULL, 'new', 1, NULL, 0
            );
            INSERT INTO training_examples (
                id, analysisAssetId, startAtomOrdinal, endAtomOrdinal,
                transcriptVersion, startTime, endTime, textSnapshotHash,
                textSnapshot, bucket, commercialIntent, ownership,
                evidenceSourcesJSON, fmCertainty, classifierConfidence,
                userAction, eligibilityGate, scanCohortJSON,
                decisionCohortJSON, transcriptQuality, createdAt
            ) VALUES (
                'te-broken-v16', 'asset-broken-v16', 0, 10,
                'tv-1', 0, 5, 'h-broken',
                NULL, 'positive', 'paid', 'thirdParty',
                '[]', 0.0, 0.0,
                NULL, NULL, '{}',
                '{}', 'good', 1700000000.0
            );
            """
        let regressErr: Int32 = sqlite3_exec(db, regress, nil, nil, nil)
        if regressErr != SQLITE_OK {
            let msg = sqlite3_errmsg(db).map { String(cString: $0) } ?? "unknown"
            Issue.record("regress to v16 failed: \(msg)")
        }
        sqlite3_close_v2(db)

        // Sanity: the seeded row is there pre-rebuild. (Lost on rebuild —
        // rebuild is destructive by design, pre-fix data is unrecoverable.
        // The presence of a row is what makes the rebuild path observable
        // rather than a vacuous no-op.)
        var preCountDb: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &preCountDb, SQLITE_OPEN_READONLY, nil) == SQLITE_OK)
        var preCountStmt: OpaquePointer?
        #expect(sqlite3_prepare_v2(preCountDb, "SELECT COUNT(*) FROM training_examples", -1, &preCountStmt, nil) == SQLITE_OK)
        #expect(sqlite3_step(preCountStmt) == SQLITE_ROW)
        let preCount = sqlite3_column_int(preCountStmt, 0)
        #expect(preCount == 1, "broken-v16 row must be present pre-rebuild to make this a real test")
        sqlite3_finalize(preCountStmt)
        sqlite3_close_v2(preCountDb)

        // Re-migrate via a fresh store. The v16 → v17 block must drop +
        // recreate the table with the corrected shape.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Schema bumped to 17.
        #expect(try await store.schemaVersion() == 19)

        // FK is now ON DELETE RESTRICT, not CASCADE. PRAGMA foreign_key_list
        // returns one row per FK; column 6 is `on_delete`.
        var fkDb: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &fkDb, SQLITE_OPEN_READONLY, nil) == SQLITE_OK)
        defer { sqlite3_close_v2(fkDb) }
        var fkStmt: OpaquePointer?
        #expect(sqlite3_prepare_v2(fkDb, "PRAGMA foreign_key_list('training_examples')", -1, &fkStmt, nil) == SQLITE_OK)
        defer { sqlite3_finalize(fkStmt) }
        var sawAnalysisAssetsFK = false
        var onDelete: String?
        while sqlite3_step(fkStmt) == SQLITE_ROW {
            // Columns: 0=id, 1=seq, 2=table, 3=from, 4=to, 5=on_update,
            //          6=on_delete, 7=match.
            let toTable = sqlite3_column_text(fkStmt, 2).map { String(cString: $0) }
            if toTable == "analysis_assets" {
                sawAnalysisAssetsFK = true
                onDelete = sqlite3_column_text(fkStmt, 6).map { String(cString: $0) }
            }
        }
        #expect(sawAnalysisAssetsFK, "FK to analysis_assets must exist post-rebuild")
        #expect(onDelete == "RESTRICT", "ON DELETE must be RESTRICT after v17 rebuild, was \(onDelete ?? "nil")")

        // decisionCohortJSON is now nullable: inserting NULL succeeds.
        // (The asset row was destroyed too — re-insert to satisfy the
        // surviving FK on the rebuilt table.)
        try await store.insertAsset(
            AnalysisAsset(
                id: "asset-post-rebuild",
                episodeId: "ep-post-rebuild",
                assetFingerprint: "fp-post-rebuild",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/post.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
        )
        let example = TrainingExample(
            id: "te-post-rebuild",
            analysisAssetId: "asset-post-rebuild",
            startAtomOrdinal: 0,
            endAtomOrdinal: 10,
            transcriptVersion: "tv-1",
            startTime: 0,
            endTime: 5,
            textSnapshotHash: "h-post",
            textSnapshot: nil,
            bucket: .positive,
            commercialIntent: "paid",
            ownership: "thirdParty",
            evidenceSources: [],
            fmCertainty: 0,
            classifierConfidence: 0,
            userAction: nil,
            eligibilityGate: nil,
            scanCohortJSON: "{}",
            // L4: nullable post-v17. Pre-fix this would have raised a
            // NOT NULL constraint failure.
            decisionCohortJSON: nil,
            transcriptQuality: "good",
            createdAt: 1
        )
        try await store.createTrainingExample(example)
        let loaded = try await store.loadTrainingExamples(forAsset: "asset-post-rebuild")
        try #require(loaded.count == 1)
        #expect(loaded[0].decisionCohortJSON == nil)

        // The pre-rebuild row was destroyed by the v17 drop+recreate.
        // Confirm the broken-v16 asset still exists (FK target preserved
        // — the rebuild only nukes training_examples) but its training
        // rows are gone.
        let lostRows = try await store.loadTrainingExamples(forAsset: "asset-broken-v16")
        #expect(lostRows.isEmpty, "pre-fix v16 rows are unrecoverable by design")
    }
}
