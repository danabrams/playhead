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

        // Reaches v4 (current).
        #expect(try await store.schemaVersion() == 4)

        // analysis_sessions has the camelCase column (H10).
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "shadowRetryPodcastId"))
        #expect(!(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needs_shadow_retry")))

        // Partial index exists.
        #expect(try probeIndexExists(in: dir, indexName: "idx_sessions_shadow_retry"))

        // bd-m8k planner state table exists (created via the v4 chain).
        #expect(try probeTableExists(in: dir, table: "podcast_planner_state"))

        // Rev3-M5 phase columns present.
        #expect(try probeColumnExists(in: dir, table: "semantic_scan_results", column: "phase"))
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "phase"))
    }

    // MARK: - H11: seeded v1 → v4 ladder runs all V*IfNeeded blocks

    @Test("H11: v1 → v4 migration chain reaches current schema")
    func seededV1ChainsToCurrent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        try seedSchemaVersion(1, in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 4)
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "transcriptVersion"))
        #expect(try probeTableExists(in: dir, table: "podcast_planner_state"))

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

    @Test("H11: v2 → v4 chains both v3 and v4 changes")
    func seededV2ChainsToCurrent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        try seedSchemaVersion(2, in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 4)
        #expect(try probeColumnExists(in: dir, table: "evidence_events", column: "transcriptVersion"))
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_sessions_shadow_retry"))
    }

    @Test("H11: v3 → v4 adds shadow retry columns and planner state table")
    func seededV3ChainsToCurrent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        try seedSchemaVersion(3, in: dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 4)
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "shadowRetryPodcastId"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_sessions_shadow_retry"))
        #expect(try probeTableExists(in: dir, table: "podcast_planner_state"))
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
        #expect(v2 == 4)
    }

    // MARK: - C6: seeded v4 _meta-less data path

    @Test("C6: store with v4 tables but no _meta row migrates cleanly")
    func v4TablesWithoutMetaRowMigrates() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // First, run a real migrate so the v4 tables exist on disk.
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

        #expect(try await store.schemaVersion() == 4)
        #expect(try probeColumnExists(in: dir, table: "analysis_sessions", column: "needsShadowRetry"))
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
            phase: .shadow
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
            phase: .targeted
        )
        try await store.insertSemanticScanResult(shadow)
        try await store.insertSemanticScanResult(targeted)

        let all = try await store.fetchSemanticScanResults(analysisAssetId: "asset-phase")
        #expect(all.count == 2)
        let phases = Set(all.map(\.phase))
        #expect(phases == [.shadow, .targeted])

        // Pinpoint round-trip: each row reads back with its written phase.
        let byId = Dictionary(uniqueKeysWithValues: all.map { ($0.id, $0) })
        #expect(byId["scan-shadow"]?.phase == .shadow)
        #expect(byId["scan-targeted"]?.phase == .targeted)
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
            phase: .shadow
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
            phase: .targeted
        )
        _ = try await store.insertEvidenceEvent(shadowEvent, transcriptVersion: "tv-1")
        _ = try await store.insertEvidenceEvent(targetedEvent, transcriptVersion: "tv-1")

        let fetched = try await store.fetchEvidenceEvents(analysisAssetId: "asset-ev-phase")
        #expect(fetched.count == 2)
        let phases = Set(fetched.map(\.phase))
        #expect(phases == [.shadow, .targeted])
    }
}
