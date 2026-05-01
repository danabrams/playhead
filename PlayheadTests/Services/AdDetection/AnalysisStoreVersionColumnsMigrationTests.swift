// AnalysisStoreVersionColumnsMigrationTests.swift
// playhead-7mq: foundation for B4 fast revalidation (playhead-zx6i).
//
// Asserts that `model_version`, `policy_version`, and
// `feature_schema_version` columns exist on the five tables whose row
// validity depends on model / policy / feature-schema versions, and
// that pre-existing rows are backfilled with the documented sentinels
// (`'pre-instrumentation'`, `0`, `0`).
//
// Coverage:
//   1. Fresh-migrate shape check (columns present on brand-new DB).
//   2. Prod-shape fixture: rows pre-exist from a version-agnostic DB
//      (columns absent), migration adds columns AND applies sentinel
//      backfill to every existing row across all five tables.
//   3. Idempotence: running `migrate()` a second time is a no-op —
//      columns don't duplicate, row values don't mutate.
//   4. SELECT *-tolerance: the two extant readers that use
//      `SELECT * FROM {transcript_chunks,ad_windows}` still
//      decode rows correctly after the new columns are appended.
//      (SQLite `ALTER TABLE ADD COLUMN` appends to the END of the
//      column list; the positional readers at fixed indices 0..N-1
//      remain correct because the new columns sit at index N, N+1, N+2.)
//
// Bug 5 (skip-cues-deletion): `skip_cues` was dropped from this
// suite when the table was deleted; the in-scope set is now five
// tables, not six.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("AnalysisStore version columns (playhead-7mq)")
struct AnalysisStoreVersionColumnsMigrationTests {

    private static let inScopeTables: [String] = [
        "analysis_sessions",
        "transcript_chunks",
        "feature_windows",
        "feature_extraction_state",
        "ad_windows",
    ]

    private static let versionColumns: [String] = [
        "model_version",
        "policy_version",
        "feature_schema_version",
    ]

    // MARK: - 1. Fresh-migrate shape

    @Test("Fresh DB: all three version columns present on all five in-scope tables")
    func freshDbAddsVersionColumnsToAllTables() async throws {
        let dir = try makeTempDir(prefix: "VersionCols-Fresh")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        for table in Self.inScopeTables {
            for column in Self.versionColumns {
                #expect(
                    try probeColumnExists(in: dir, table: table, column: column),
                    "Column \(column) missing from table \(table)"
                )
            }
        }
    }

    // MARK: - 2. Prod-shape fixture: pre-instrumentation rows get sentinel backfill

    /// Builds a "prod-shape" fixture: open a store, run migrate, write
    /// representative rows to all five in-scope tables, then drop the
    /// newly-added version columns to simulate a pre-instrumentation
    /// database. Re-opening the store must restore the columns and
    /// apply sentinel backfill to every pre-existing row.
    @Test("Prod-shape DB: pre-existing rows are backfilled with sentinel values on all five tables")
    func prodShapeBackfillAppliesSentinelsEverywhere() async throws {
        let dir = try makeTempDir(prefix: "VersionCols-ProdShape")
        defer { try? FileManager.default.removeItem(at: dir) }

        // Phase 1: bring the DB to current schema, then populate.
        AnalysisStore.resetMigratedPathsForTesting()
        let seedStore = try AnalysisStore(directory: dir)
        try await seedStore.migrate()
        try await seedRepresentativeRows(store: seedStore, assetId: "asset-7mq")

        // Phase 2: strip the version columns from each in-scope table
        // to simulate a DB that predates this bead. SQLite 3.35+ (iOS 15+)
        // supports ALTER TABLE DROP COLUMN; the test simulator on
        // iPhone 17 Pro / iOS 26 easily clears this bar.
        try stripVersionColumns(in: dir, tables: Self.inScopeTables)

        // Phase 3: open a new store, which triggers migrate() and should
        // re-add the columns with sentinel backfill.
        AnalysisStore.resetMigratedPathsForTesting()
        let reopenedStore = try AnalysisStore(directory: dir)
        try await reopenedStore.migrate()
        _ = reopenedStore  // keep-alive

        // Every row in every in-scope table must have the three sentinel
        // values. Count mismatches between row count and sentinel count
        // would indicate backfill missed rows.
        for table in Self.inScopeTables {
            try assertAllRowsBackfilledWithSentinels(in: dir, table: table)
        }
    }

    // MARK: - 3. Idempotence: migrate twice, DB is unchanged

    @Test("Idempotent: second migrate() call is a no-op — no duplicate columns, no mutated values")
    func secondMigrateIsNoOp() async throws {
        let dir = try makeTempDir(prefix: "VersionCols-Idempotent")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await seedRepresentativeRows(store: store, assetId: "asset-idem")

        // Run migrate() again against the same store; must not error,
        // must not change schema shape, must not mutate rows.
        let firstSnapshot = try captureSchemaAndRowSnapshot(in: dir, tables: Self.inScopeTables)
        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()
        let secondSnapshot = try captureSchemaAndRowSnapshot(in: dir, tables: Self.inScopeTables)

        #expect(firstSnapshot == secondSnapshot, "migrate() is not idempotent — snapshot diverged")

        // Explicit column-count check: each in-scope table must have
        // exactly one of each version column, not two or three.
        for table in Self.inScopeTables {
            for column in Self.versionColumns {
                let count = try countOccurrencesOfColumn(in: dir, table: table, column: column)
                #expect(count == 1, "Column \(column) appeared \(count) times in \(table) after double-migrate")
            }
        }
    }

    // MARK: - 4. SELECT * tolerance: existing readers still decode correctly

    /// Two call-sites use `SELECT * FROM ...` against in-scope tables:
    ///   - `fetchTranscriptChunks(assetId:)` / `fetchTranscriptChunk(...)`
    ///   - `fetchAdWindows(assetId:)`
    ///
    /// Their readers address columns positionally (0-based indices into
    /// the prepared statement). SQLite's `ALTER TABLE ADD COLUMN`
    /// appends new columns at the END of the column list, so
    /// pre-existing positional indices remain valid. This test asserts
    /// that post-migration round-trips through those call-sites still
    /// work — if anyone swaps the column order we will catch it here.
    ///
    /// Bug 5 (skip-cues-deletion): the third reader `fetchSkipCues`
    /// was deleted along with the `skip_cues` table.
    @Test("SELECT * tolerates new columns: round-trip via fetchTranscriptChunks / fetchAdWindows")
    func selectStarReadersTolerateNewColumns() async throws {
        let store = try await makeTestStore()
        try await seedRepresentativeRows(store: store, assetId: "asset-select-star")

        // transcript_chunks
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-select-star")
        #expect(chunks.count >= 1)
        #expect(chunks.first?.id == "chunk-0")
        #expect(chunks.first?.text == "hello world")

        // ad_windows
        let ads = try await store.fetchAdWindows(assetId: "asset-select-star")
        #expect(ads.count >= 1)
        #expect(ads.first?.id == "ad-0")
        #expect(ads.first?.boundaryState == "lexical")
    }

    // MARK: - Fixture: seed representative rows across all six tables

    private func seedRepresentativeRows(store: AnalysisStore, assetId: String) async throws {
        try await store.insertAsset(
            AnalysisAsset(
                id: assetId,
                episodeId: "ep-\(assetId)",
                assetFingerprint: "fp-\(assetId)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(assetId).m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
        )

        // analysis_sessions
        try await store.insertSession(
            AnalysisSession(
                id: "sess-\(assetId)",
                analysisAssetId: assetId,
                state: "complete",
                startedAt: 0,
                updatedAt: 0,
                failureReason: nil
            )
        )

        // transcript_chunks — two rows so we exercise multi-row backfill.
        for i in 0..<2 {
            try await store.insertTranscriptChunk(
                TranscriptChunk(
                    id: "chunk-\(i)",
                    analysisAssetId: assetId,
                    segmentFingerprint: "seg-\(i)",
                    chunkIndex: i,
                    startTime: Double(i) * 10,
                    endTime: Double(i + 1) * 10,
                    text: "hello world",
                    normalizedText: "hello world",
                    pass: "final",
                    modelVersion: "asr-test",
                    transcriptVersion: "tv-1",
                    atomOrdinal: i,
                    weakAnchorMetadata: nil,
                    speakerId: nil
                )
            )
        }

        // feature_windows — two rows
        try await store.insertFeatureWindows([
            FeatureWindow(
                analysisAssetId: assetId,
                startTime: 0,
                endTime: 1,
                rms: 0.1,
                spectralFlux: 0.2,
                musicProbability: 0.0,
                pauseProbability: 0.0,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 1
            ),
            FeatureWindow(
                analysisAssetId: assetId,
                startTime: 1,
                endTime: 2,
                rms: 0.1,
                spectralFlux: 0.2,
                musicProbability: 0.0,
                pauseProbability: 0.0,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 1
            ),
        ])

        // feature_extraction_state — one row (primary key is assetId)
        try await store.upsertFeatureExtractionCheckpoint(
            FeatureExtractionCheckpoint(
                analysisAssetId: assetId,
                lastWindowStartTime: 0,
                lastWindowEndTime: 2,
                lastRms: 0.1,
                lastMusicProbability: 0.0,
                lastRawSpeakerChangeProxyScore: 0.0,
                penultimateRawSpeakerChangeProxyScore: nil,
                lastMagnitudes: [0.1, 0.2, 0.3],
                featureVersion: 1
            )
        )

        // ad_windows — one row
        try await store.insertAdWindow(
            AdWindow(
                id: "ad-0",
                analysisAssetId: assetId,
                startTime: 60,
                endTime: 120,
                confidence: 0.85,
                boundaryState: "lexical",
                decisionState: "candidate",
                detectorVersion: "det-1",
                advertiser: nil,
                product: nil,
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: nil,
                metadataSource: "none",
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: nil,
                eligibilityGate: nil
            )
        )

        // skip_cues was deleted in Bug 5; no row to seed.
    }

    // MARK: - Fixture: strip version columns to simulate a pre-instrumentation DB

    private func stripVersionColumns(in directory: URL, tables: [String]) throws {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
            throw NSError(domain: "StripVersionColumns", code: 1)
        }
        defer { sqlite3_close_v2(db) }

        for table in tables {
            for column in Self.versionColumns {
                let sql = "ALTER TABLE \(table) DROP COLUMN \(column)"
                // DROP COLUMN silently returns SQLITE_OK if the column
                // exists; may fail if not. Allow both — what we assert
                // later is that columns are PRESENT post-migration.
                _ = sqlite3_exec(db, sql, nil, nil, nil)
            }
        }
    }

    // MARK: - Assertion: every row has sentinel values

    private func assertAllRowsBackfilledWithSentinels(in directory: URL, table: String) throws {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "Sentinels", code: 1)
        }
        defer { sqlite3_close_v2(db) }

        // Total rows.
        var totalStmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM \(table)", -1, &totalStmt, nil) == SQLITE_OK else {
            throw NSError(domain: "Sentinels", code: 2)
        }
        _ = sqlite3_step(totalStmt)
        let total = Int(sqlite3_column_int(totalStmt, 0))
        sqlite3_finalize(totalStmt)
        // At least one row was seeded; tests that don't seed a particular
        // table would return 0 here, which is a vacuous pass. We only
        // invoke this helper after `seedRepresentativeRows`, so every
        // in-scope table should contain ≥1 row.
        #expect(total >= 1, "Expected ≥1 row in \(table) after seeding; found \(total)")

        // Rows with all three sentinels.
        let sentinelPredicate = """
            SELECT COUNT(*) FROM \(table)
            WHERE model_version = 'pre-instrumentation'
              AND policy_version = 0
              AND feature_schema_version = 0
            """
        var sentinelStmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sentinelPredicate, -1, &sentinelStmt, nil) == SQLITE_OK else {
            throw NSError(domain: "Sentinels", code: 3)
        }
        _ = sqlite3_step(sentinelStmt)
        let backfilled = Int(sqlite3_column_int(sentinelStmt, 0))
        sqlite3_finalize(sentinelStmt)
        #expect(
            backfilled == total,
            "Table \(table): \(backfilled)/\(total) rows had sentinel values"
        )
    }

    // MARK: - Helper: schema + row snapshot for idempotence check

    private struct TableSnapshot: Equatable {
        let columns: [String]
        let rowCount: Int
        let firstRowValues: [String]  // stringified column values, for a single "canary" row
    }

    private func captureSchemaAndRowSnapshot(
        in directory: URL,
        tables: [String]
    ) throws -> [String: TableSnapshot] {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "Snapshot", code: 1)
        }
        defer { sqlite3_close_v2(db) }

        var snapshots: [String: TableSnapshot] = [:]
        for table in tables {
            // Columns (via PRAGMA table_info).
            var columns: [String] = []
            var infoStmt: OpaquePointer?
            guard sqlite3_prepare_v2(db, "PRAGMA table_info(\(table))", -1, &infoStmt, nil) == SQLITE_OK else {
                throw NSError(domain: "Snapshot", code: 2)
            }
            while sqlite3_step(infoStmt) == SQLITE_ROW {
                if let c = sqlite3_column_text(infoStmt, 1) {
                    columns.append(String(cString: c))
                }
            }
            sqlite3_finalize(infoStmt)

            // Row count.
            var countStmt: OpaquePointer?
            guard sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM \(table)", -1, &countStmt, nil) == SQLITE_OK else {
                throw NSError(domain: "Snapshot", code: 3)
            }
            _ = sqlite3_step(countStmt)
            let rowCount = Int(sqlite3_column_int(countStmt, 0))
            sqlite3_finalize(countStmt)

            // First row: stringify each column via SQLite's implicit
            // coercion so the snapshot is stable.
            let orderBy: String
            switch table {
            case "feature_windows":
                orderBy = "ORDER BY analysisAssetId, startTime"
            case "feature_extraction_state":
                orderBy = "ORDER BY analysisAssetId"
            case "transcript_chunks":
                orderBy = "ORDER BY id"
            default:
                orderBy = "ORDER BY rowid"
            }
            var firstRow: [String] = []
            if rowCount > 0 {
                var rowStmt: OpaquePointer?
                let sql = "SELECT * FROM \(table) \(orderBy) LIMIT 1"
                guard sqlite3_prepare_v2(db, sql, -1, &rowStmt, nil) == SQLITE_OK else {
                    throw NSError(domain: "Snapshot", code: 4)
                }
                if sqlite3_step(rowStmt) == SQLITE_ROW {
                    let colCount = sqlite3_column_count(rowStmt)
                    for i in 0..<colCount {
                        if let t = sqlite3_column_text(rowStmt, i) {
                            firstRow.append(String(cString: t))
                        } else {
                            firstRow.append("<NULL>")
                        }
                    }
                }
                sqlite3_finalize(rowStmt)
            }

            snapshots[table] = TableSnapshot(
                columns: columns,
                rowCount: rowCount,
                firstRowValues: firstRow
            )
        }
        return snapshots
    }

    // MARK: - Helper: count how many times a column name appears in table_info

    private func countOccurrencesOfColumn(
        in directory: URL,
        table: String,
        column: String
    ) throws -> Int {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "CountCol", code: 1)
        }
        defer { sqlite3_close_v2(db) }

        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "PRAGMA table_info(\(table))", -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "CountCol", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        var count = 0
        while sqlite3_step(stmt) == SQLITE_ROW {
            if let c = sqlite3_column_text(stmt, 1),
               String(cString: c) == column {
                count += 1
            }
        }
        return count
    }
}
