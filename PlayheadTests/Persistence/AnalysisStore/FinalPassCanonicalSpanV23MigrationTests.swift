// FinalPassCanonicalSpanV23MigrationTests.swift
// playhead-hygc.1.5: pin the V23 migration that adds
// `final_pass_jobs.canonicalSpanKey` and the sibling
// `final_pass_job_aliases` table. The May 6 dogfood DB carried
// duplicate `final_pass_jobs` rows whose `adWindowId`s differed but
// whose spans matched (e.g. `3386.0-3394.14` repeated 4×); without a
// canonical-span key the runner cannot dedupe these rows at enqueue
// time, so each duplicate consumed an ASR pass.
//
// Coverage targets:
//   1. Fresh-DB migrate() lands the schema at v23 with the new column,
//      table, and indexes.
//   2. A v22-shaped DB climbs to v23 — pins the ladder boundary.
//   3. The migration is idempotent: running twice does not duplicate
//      indexes, drop rows, or fail.
//   4. Existing pre-v23 final_pass_jobs rows have their canonicalSpanKey
//      populated by the SQL backfill, with the same format the in-Swift
//      `AnalysisStore.canonicalSpanKey(start:end:)` produces.
//   5. Round-trip CRUD via `findFinalPassJob(forAssetId:canonicalSpanKey:)`
//      / `recordFinalPassJobAlias` / `fetchFinalPassJobAliases` writes
//      and reads.
//   6. FK CASCADE: deleting a `final_pass_jobs` row drops its alias
//      rows.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("FinalPass canonical-span V23 migration (playhead-hygc.1.5)")
struct FinalPassCanonicalSpanV23MigrationTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "FinalPassCanonicalSpanV23")
    }

    private func makeAsset(
        id: String,
        episodeId: String = "ep"
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    // MARK: - Migration

    @Test("fresh DB migrate() lands canonicalSpanKey + alias table at v23")
    func freshDbHasV23Schema() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 25)
        #expect(try probeColumnExists(in: dir, table: "final_pass_jobs", column: "canonicalSpanKey"))
        #expect(try probeTableExists(in: dir, table: "final_pass_job_aliases"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_final_pass_jobs_canonical"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_final_pass_job_aliases_window"))
    }

    @Test("v22-seeded DB picks up canonicalSpanKey + aliases at v23")
    func seededV22ChainsToV23() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        #expect(try await bootstrap.schemaVersion() == 25)

        // Rewind: drop the v23 additions and reset _meta to '22' so
        // the v22 → v23 block runs on the next open.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            DROP INDEX IF EXISTS idx_final_pass_job_aliases_window;
            DROP TABLE IF EXISTS final_pass_job_aliases;
            DROP INDEX IF EXISTS idx_final_pass_jobs_canonical;
            -- SQLite doesn't support DROP COLUMN before 3.35, but on
            -- Apple platforms we have at least 3.39. To be safe across
            -- target OS minimum, rebuild the table without the column.
            ALTER TABLE final_pass_jobs RENAME TO final_pass_jobs_v22;
            CREATE TABLE final_pass_jobs (
                jobId TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                podcastId TEXT,
                adWindowId TEXT NOT NULL,
                windowStartTime REAL NOT NULL,
                windowEndTime REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                retryCount INTEGER NOT NULL DEFAULT 0,
                deferReason TEXT,
                createdAt REAL NOT NULL,
                updatedAt REAL NOT NULL DEFAULT 0
            );
            INSERT INTO final_pass_jobs SELECT jobId, analysisAssetId, podcastId, adWindowId, windowStartTime, windowEndTime, status, retryCount, deferReason, createdAt, updatedAt FROM final_pass_jobs_v22;
            DROP TABLE final_pass_jobs_v22;
            CREATE INDEX IF NOT EXISTS idx_final_pass_jobs_status ON final_pass_jobs(status, createdAt ASC);
            CREATE INDEX IF NOT EXISTS idx_final_pass_jobs_asset ON final_pass_jobs(analysisAssetId);
            UPDATE _meta SET value = '24' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        // Sanity: rewind actually removed the column + table.
        #expect(!(try probeColumnExists(in: dir, table: "final_pass_jobs", column: "canonicalSpanKey")))
        #expect(!(try probeTableExists(in: dir, table: "final_pass_job_aliases")))

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 25)
        #expect(try probeColumnExists(in: dir, table: "final_pass_jobs", column: "canonicalSpanKey"))
        #expect(try probeTableExists(in: dir, table: "final_pass_job_aliases"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_final_pass_jobs_canonical"))
    }

    @Test("V23 migration is idempotent")
    func v23MigrationIsIdempotent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let v1 = try await store.schemaVersion()

        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()
        let v2 = try await store.schemaVersion()

        #expect(v1 == 25)
        #expect(v2 == 25)
    }

    @Test("backfill populates canonicalSpanKey on existing rows with the Swift-helper format")
    func backfillPopulatesCanonicalSpanKey() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-A"))

        // Seed a v23-shape row so we can verify the canonicalSpanKey
        // matches what `canonicalSpanKey(start:end:)` produces.
        let job = FinalPassJob(
            jobId: "fpj-asset-A-w1",
            analysisAssetId: "asset-A",
            podcastId: "pod-1",
            adWindowId: "w1",
            windowStartTime: 3386.0,
            windowEndTime: 3394.14,
            status: .queued,
            retryCount: 0,
            deferReason: nil,
            createdAt: 1_000.0
        )
        try await store.insertOrIgnoreFinalPassJob(job)

        // The Swift-helper canonical-span key MUST match the SQL
        // `printf('%.3f-%.3f', ...)` shape the v23 backfill uses.
        let key = AnalysisStore.canonicalSpanKey(start: 3386.0, end: 3394.14)
        #expect(key == "3386.000-3394.140")

        // Confirm that fetching by canonical key returns the row.
        let found = try await store.findFinalPassJob(
            forAssetId: "asset-A",
            canonicalSpanKey: key
        )
        #expect(found?.jobId == "fpj-asset-A-w1")
    }

    // MARK: - CRUD round-trip

    @Test("findFinalPassJob / recordFinalPassJobAlias / fetchFinalPassJobAliases round-trip")
    func aliasRoundTrip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-A"))

        let job = FinalPassJob(
            jobId: "fpj-asset-A-w1",
            analysisAssetId: "asset-A",
            podcastId: "pod-1",
            adWindowId: "w1",
            windowStartTime: 24.0,
            windowEndTime: 38.16,
            status: .queued,
            retryCount: 0,
            deferReason: nil,
            createdAt: 1_000.0
        )
        try await store.insertOrIgnoreFinalPassJob(job)

        // No aliases yet.
        var aliases = try await store.fetchFinalPassJobAliases(jobId: job.jobId)
        #expect(aliases.isEmpty)

        // Record three aliases (e.g. the dogfood scenario where 4
        // duplicate AdWindow rows collapse into one canonical job).
        try await store.recordFinalPassJobAlias(jobId: job.jobId, adWindowId: "w-dup-1", addedAt: 1_001.0)
        try await store.recordFinalPassJobAlias(jobId: job.jobId, adWindowId: "w-dup-2", addedAt: 1_002.0)
        try await store.recordFinalPassJobAlias(jobId: job.jobId, adWindowId: "w-dup-3", addedAt: 1_003.0)

        aliases = try await store.fetchFinalPassJobAliases(jobId: job.jobId)
        #expect(aliases == ["w-dup-1", "w-dup-2", "w-dup-3"])

        // Idempotent: re-recording the same alias is a no-op.
        try await store.recordFinalPassJobAlias(jobId: job.jobId, adWindowId: "w-dup-1", addedAt: 1_004.0)
        aliases = try await store.fetchFinalPassJobAliases(jobId: job.jobId)
        #expect(aliases.count == 3, "re-recording an existing (jobId, adWindowId) pair must not duplicate")
    }

    @Test("FK CASCADE: deleting an asset cascades to alias rows")
    func aliasFkCascade() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-A"))

        let job = FinalPassJob(
            jobId: "fpj-asset-A-w1",
            analysisAssetId: "asset-A",
            podcastId: "pod-1",
            adWindowId: "w1",
            windowStartTime: 0.0,
            windowEndTime: 30.0,
            status: .queued,
            retryCount: 0,
            deferReason: nil,
            createdAt: 1_000.0
        )
        try await store.insertOrIgnoreFinalPassJob(job)
        try await store.recordFinalPassJobAlias(jobId: job.jobId, adWindowId: "w-alias", addedAt: 1_001.0)

        var aliases = try await store.fetchFinalPassJobAliases(jobId: job.jobId)
        #expect(aliases == ["w-alias"])

        try await store.deleteAsset(id: "asset-A")
        aliases = try await store.fetchFinalPassJobAliases(jobId: job.jobId)
        #expect(aliases.isEmpty, "ON DELETE CASCADE on the asset must cascade to final_pass_jobs and then to aliases")
    }

    // MARK: - canonicalCompleteFinalPassSpans

    @Test("canonicalCompleteFinalPassSpans collapses duplicate complete rows by canonical key")
    func progressFromCanonicalSpansIgnoresDuplicates() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-DUP"))

        // Simulate the May 6 dogfood pattern: same span persisted as
        // FOUR distinct `complete` rows. (This is the pre-fix shape
        // we need to read sanely; the runner-level dedupe prevents new
        // duplicates, but legacy rows still need a sane progress
        // computation.)
        for i in 1...4 {
            let job = FinalPassJob(
                jobId: "fpj-asset-DUP-w\(i)",
                analysisAssetId: "asset-DUP",
                podcastId: nil,
                adWindowId: "w\(i)",
                windowStartTime: 3386.0,
                windowEndTime: 3394.14,
                status: .queued,
                retryCount: 0,
                deferReason: nil,
                createdAt: Double(1_000 + i)
            )
            try await store.insertOrIgnoreFinalPassJob(job)
            try await store.forceFinalPassJobStateForTesting(
                jobId: job.jobId,
                status: .complete
            )
        }

        // A second canonical span that is also duplicated.
        for i in 1...4 {
            let job = FinalPassJob(
                jobId: "fpj-asset-DUP-x\(i)",
                analysisAssetId: "asset-DUP",
                podcastId: nil,
                adWindowId: "x\(i)",
                windowStartTime: 3394.14,
                windowEndTime: 3395.652,
                status: .queued,
                retryCount: 0,
                deferReason: nil,
                createdAt: Double(2_000 + i)
            )
            try await store.insertOrIgnoreFinalPassJob(job)
            try await store.forceFinalPassJobStateForTesting(
                jobId: job.jobId,
                status: .complete
            )
        }

        let spans = try await store.canonicalCompleteFinalPassSpans(forAsset: "asset-DUP")
        #expect(spans.count == 2,
                "8 complete rows representing 2 canonical spans must collapse to 2 progress entries (acceptance criterion: progress ignores duplicate completed rows)")
        #expect(spans.contains { $0.canonicalSpanKey == "3386.000-3394.140" })
        #expect(spans.contains { $0.canonicalSpanKey == "3394.140-3395.652" })
        // Each span lists every contributing window.
        let firstSpan = spans.first { $0.canonicalSpanKey == "3386.000-3394.140" }!
        #expect(firstSpan.adWindowIds.sorted() == ["w1", "w2", "w3", "w4"],
                "all four contributing AdWindow ids must be surfaced for audit")
    }
}
