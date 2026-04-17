// ArtifactClassMigrationTests.swift
// playhead-h7r: the `analysis_assets.artifact_class` migration must be
// idempotent, safe to re-run, and must backfill every pre-existing row
// with the `'media'` sentinel on first run.
//
// Coverage:
//   1. Fresh-migrate: column is present with default 'media'.
//   2. Prod-shape fixture: drop the column from a seeded DB, re-migrate,
//      every pre-existing row is backfilled with `'media'`.
//   3. Idempotence: running `migrate()` a second time is a no-op —
//      the column does not duplicate, row values do not mutate.
//   4. readAsset round-trip: a seeded asset's `artifactClass` survives
//      persist → fetch after migration.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("AnalysisStore artifact_class migration (playhead-h7r)")
struct ArtifactClassMigrationTests {

    // MARK: - 1. Fresh-migrate shape

    @Test("Fresh DB: artifact_class column present on analysis_assets")
    func freshDbAddsArtifactClassColumn() async throws {
        let dir = try makeTempDir(prefix: "ArtifactClass-Fresh")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        _ = store  // keep-alive

        #expect(try probeColumnExists(in: dir, table: "analysis_assets", column: "artifact_class"))
    }

    // MARK: - 2. Prod-shape fixture: sentinel backfill

    @Test("Prod-shape DB: pre-existing rows get 'media' sentinel backfill")
    func prodShapeBackfillUsesMediaSentinel() async throws {
        let dir = try makeTempDir(prefix: "ArtifactClass-ProdShape")
        defer { try? FileManager.default.removeItem(at: dir) }

        // Phase 1: migrate, then seed MULTIPLE analysis_assets rows so
        // we exercise multi-row backfill (per spec: "multi-row
        // analysis_assets fixture").
        AnalysisStore.resetMigratedPathsForTesting()
        let seedStore = try AnalysisStore(directory: dir)
        try await seedStore.migrate()
        for i in 0..<5 {
            try await seedStore.insertAsset(AnalysisAsset(
                id: "asset-\(i)",
                episodeId: "ep-\(i)",
                assetFingerprint: "fp-\(i)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(i).m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil
            ))
        }

        // Phase 2: drop artifact_class to simulate a pre-h7r DB.
        try stripArtifactClassColumn(in: dir)

        // Phase 3: re-migrate. Column must come back, every row
        // must have the 'media' sentinel.
        AnalysisStore.resetMigratedPathsForTesting()
        let reopenedStore = try AnalysisStore(directory: dir)
        try await reopenedStore.migrate()
        _ = reopenedStore

        try assertAllRowsHaveArtifactClass(
            in: dir,
            expected: ArtifactClass.media.rawValue,
            minCount: 5
        )
    }

    // MARK: - 3. Idempotence

    @Test("Idempotent: second migrate() does not duplicate the column or mutate rows")
    func secondMigrateIsNoOp() async throws {
        let dir = try makeTempDir(prefix: "ArtifactClass-Idempotent")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Seed a mix of classes so we can verify row preservation.
        try await store.insertAsset(AnalysisAsset(
            id: "asset-media",
            episodeId: "ep-media",
            assetFingerprint: "fp-media",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/m.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            artifactClass: .media
        ))
        try await store.insertAsset(AnalysisAsset(
            id: "asset-warm",
            episodeId: "ep-warm",
            assetFingerprint: "fp-warm",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/w.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            artifactClass: .warmResumeBundle
        ))
        try await store.insertAsset(AnalysisAsset(
            id: "asset-scratch",
            episodeId: "ep-scratch",
            assetFingerprint: "fp-scratch",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/s.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            artifactClass: .scratch
        ))

        // Run migrate() a second time. Must succeed, must not change
        // row counts, and must not duplicate the artifact_class column.
        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()

        let count = try countOccurrencesOfColumn(
            in: dir,
            table: "analysis_assets",
            column: "artifact_class"
        )
        #expect(count == 1, "artifact_class appeared \(count) times after double-migrate")

        // Verify rows round-trip correctly through readAsset (which
        // reads artifact_class at positional index 12 post-migration).
        let counts = try await store.countAssetsByArtifactClass()
        #expect(counts[.media] == 1)
        #expect(counts[.warmResumeBundle] == 1)
        #expect(counts[.scratch] == 1)
    }

    // MARK: - 4. readAsset round-trip

    @Test("readAsset round-trips all three ArtifactClass variants")
    func readAssetRoundTripsEachClass() async throws {
        let store = try await makeTestStore()

        for cls in ArtifactClass.allCases {
            try await store.insertAsset(AnalysisAsset(
                id: "asset-\(cls.rawValue)",
                episodeId: "ep-\(cls.rawValue)",
                assetFingerprint: "fp-\(cls.rawValue)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(cls.rawValue).m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil,
                artifactClass: cls
            ))
        }

        for cls in ArtifactClass.allCases {
            let asset = try await store.fetchAsset(id: "asset-\(cls.rawValue)")
            #expect(asset != nil)
            #expect(asset?.artifactClass == cls, "readAsset lost class for \(cls)")
        }
    }

    // MARK: - Helpers

    private func stripArtifactClassColumn(in directory: URL) throws {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
            throw NSError(domain: "StripArtifactClass", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        // SQLite 3.35+ supports ALTER TABLE DROP COLUMN. iOS 15+ ships
        // a new-enough SQLite; the test simulator easily clears this.
        _ = sqlite3_exec(db, "ALTER TABLE analysis_assets DROP COLUMN artifact_class", nil, nil, nil)
    }

    private func assertAllRowsHaveArtifactClass(
        in directory: URL,
        expected: String,
        minCount: Int
    ) throws {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "AssertSentinel", code: 1)
        }
        defer { sqlite3_close_v2(db) }

        var totalStmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM analysis_assets", -1, &totalStmt, nil) == SQLITE_OK else {
            throw NSError(domain: "AssertSentinel", code: 2)
        }
        _ = sqlite3_step(totalStmt)
        let total = Int(sqlite3_column_int(totalStmt, 0))
        sqlite3_finalize(totalStmt)
        #expect(total >= minCount, "Expected ≥\(minCount) rows; found \(total)")

        var sentinelStmt: OpaquePointer?
        let sql = "SELECT COUNT(*) FROM analysis_assets WHERE artifact_class = ?"
        guard sqlite3_prepare_v2(db, sql, -1, &sentinelStmt, nil) == SQLITE_OK else {
            throw NSError(domain: "AssertSentinel", code: 3)
        }
        defer { sqlite3_finalize(sentinelStmt) }
        expected.withCString { ptr in
            sqlite3_bind_text(sentinelStmt, 1, ptr, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
        }
        _ = sqlite3_step(sentinelStmt)
        let backfilled = Int(sqlite3_column_int(sentinelStmt, 0))
        #expect(backfilled == total, "\(backfilled)/\(total) rows had sentinel '\(expected)'")
    }

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
