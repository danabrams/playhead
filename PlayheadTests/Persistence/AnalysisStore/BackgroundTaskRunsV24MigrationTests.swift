// BackgroundTaskRunsV24MigrationTests.swift
// playhead-hygc.1.4: pin the V24 migration that introduces the
// `background_task_runs` table — the durable per-run outcome ledger
// for BGProcessingTask executions. Without it, dogfood overnight runs
// cannot be classified as admitted / no-eligible / deferred / expired
// without raw JSONL grep.
//
// Coverage targets:
//   1. Fresh-DB migrate() leaves the schema at v24 with the
//      `background_task_runs` table and three indexes present.
//   2. A v23-shaped DB climbs to v24 — pins the ladder boundary.
//   3. The migration is idempotent: running twice does not duplicate
//      indexes or fail.
//   4. The expected column shape (every column in the design hint)
//      is on the resulting table.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("BackgroundTaskRuns V24 migration (playhead-hygc.1.4)")
struct BackgroundTaskRunsV24MigrationTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "BackgroundTaskRunsV24")
    }

    @Test("fresh DB migrate() lands background_task_runs + indexes at v24")
    func freshDbHasV24Table() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 24)
        #expect(try probeTableExists(in: dir, table: "background_task_runs"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_background_task_runs_entry_started"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_background_task_runs_started"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_background_task_runs_asset_started"))
    }

    @Test("v23-seeded DB picks up background_task_runs at v24")
    func seededV23ChainsToV24() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        #expect(try await bootstrap.schemaVersion() == 24)

        // Rewind: drop the v24 table/indexes and reset _meta to '23' so
        // the v23 → v24 block runs on the next open.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            DROP INDEX IF EXISTS idx_background_task_runs_entry_started;
            DROP INDEX IF EXISTS idx_background_task_runs_started;
            DROP INDEX IF EXISTS idx_background_task_runs_asset_started;
            DROP TABLE IF EXISTS background_task_runs;
            UPDATE _meta SET value = '23' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        // Sanity: the rewind actually removed the table.
        #expect(!(try probeTableExists(in: dir, table: "background_task_runs")))

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 24)
        #expect(try probeTableExists(in: dir, table: "background_task_runs"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_background_task_runs_entry_started"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_background_task_runs_started"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_background_task_runs_asset_started"))
    }

    @Test("V24 migration is idempotent across resetMigratedPathsForTesting")
    func v24MigrationIsIdempotent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let v1 = try await store.schemaVersion()

        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()
        let v2 = try await store.schemaVersion()

        #expect(v1 == 24)
        #expect(v2 == 24)
    }

    @Test("background_task_runs has every column from the design hint")
    func tableHasAllExpectedColumns() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let expectedColumns = [
            "runId", "entryPoint", "taskIdentifier", "taskInstanceID",
            "startedAt", "finishedAt", "outcome", "deferReason", "cause",
            "jobsSeen", "jobsAdmitted", "jobsCompleted", "jobsDeferred",
            "coverageBefore", "coverageAfter", "assetId", "expiration",
            "lastErrorCode", "scenePhase",
        ]
        for column in expectedColumns {
            #expect(
                try probeColumnExists(in: dir, table: "background_task_runs", column: column),
                "Expected column `\(column)` to exist on background_task_runs"
            )
        }
    }
}
