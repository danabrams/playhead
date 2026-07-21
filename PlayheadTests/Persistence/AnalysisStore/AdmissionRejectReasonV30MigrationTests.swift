// AdmissionRejectReasonV30MigrationTests.swift
// playhead-gy2s: pin the V30 migration that adds the admission-reject
// advisory columns (`lastRejectReason` / `lastRejectAt`) to `analysis_jobs`,
// plus the record/fetch round-trip that carries them.
//
// This is the on-disk UPGRADE-PATH evidence for the highest-stakes part of
// the stall fix: a real v29 DB with existing `analysis_jobs` rows must open
// at v30, add the two nullable columns at trailing indices, and leave every
// positional reader (`readJob`, indices 0..22) correct — including the
// `generationID` (21) / `schedulerEpoch` (22) columns the uzdq lease work
// appended just before these.
//
// Coverage targets:
//   1. Fresh-DB migrate() reaches head (v30) with both columns present.
//   2. `currentSchemaVersion` is exactly 30 (drift guard).
//   3. A v29-shaped `analysis_jobs` (no reject columns) upgrades in place:
//      the columns are added, an existing pre-gy2s row survives with EVERY
//      field intact — crucially its NON-DEFAULT generationID/schedulerEpoch,
//      which only read back correctly if the appended columns did NOT shift
//      the positional reader — and the new columns backfill to NULL (no data
//      loss, no silent read corruption).
//   4. The migration is idempotent.
//   5. Round-trip: recordJobAdmissionReject → fetchJobAdmissionReject
//      preserves (reason, at) exactly; a fresh row reads back nil; and the
//      advisory write is UPDATE-in-place (does not mutate updatedAt / state).
//   6. Isolated ladder (migrateOnlyForTesting) reaches v30 and adds columns.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("analysis_jobs admission-reject V30 migration + round-trip (playhead-gy2s)")
struct AdmissionRejectReasonV30MigrationTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "AdmissionRejectReasonV30")
    }

    // MARK: - Migration ladder

    @Test("fresh DB migrate() lands both reject-advisory columns at head")
    func freshDbHasV30Columns() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // Drift guard: head is exactly 30 for this bead.
        #expect(AnalysisStore.currentSchemaVersion == 30)
        #expect(try probeColumnExists(in: dir, table: "analysis_jobs", column: "lastRejectReason"))
        #expect(try probeColumnExists(in: dir, table: "analysis_jobs", column: "lastRejectAt"))
    }

    @Test("v29-shaped analysis_jobs upgrades in place: columns added, pre-gy2s row survives with positional integrity + NULL reject fields")
    func seededV29RowUpgradesWithoutDataLoss() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Bootstrap the full head shape, then regress `analysis_jobs` to its
        // pre-gy2s (v29) shape and rewind `_meta.schema_version` to 29. A row
        // is seeded through the v29-shaped table so the upgrade path has real
        // data to preserve.
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        // Drop and recreate analysis_jobs in the EXACT physical column order a
        // real upgraded v29 DB carries: the 21 base columns, then the uzdq
        // lease columns generationID (index 21) / schedulerEpoch (index 22)
        // appended by `addEpisodeExecutionLeaseColumnsIfNeeded`. The V30 reject
        // columns must append at 23 / 24. Seeding NON-DEFAULT generationID and
        // schedulerEpoch is the whole point: those fields only read back
        // correctly if the appended reject columns did NOT shift `readJob`'s
        // fixed positional reads at 21 / 22.
        let regress = """
            DROP TABLE IF EXISTS analysis_jobs;
            CREATE TABLE analysis_jobs (
                jobId TEXT PRIMARY KEY,
                jobType TEXT NOT NULL,
                episodeId TEXT NOT NULL,
                podcastId TEXT,
                analysisAssetId TEXT,
                workKey TEXT NOT NULL UNIQUE,
                sourceFingerprint TEXT NOT NULL,
                downloadId TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                desiredCoverageSec REAL NOT NULL,
                featureCoverageSec REAL NOT NULL DEFAULT 0,
                transcriptCoverageSec REAL NOT NULL DEFAULT 0,
                cueCoverageSec REAL NOT NULL DEFAULT 0,
                state TEXT NOT NULL DEFAULT 'queued',
                attemptCount INTEGER NOT NULL DEFAULT 0,
                nextEligibleAt REAL,
                leaseOwner TEXT,
                leaseExpiresAt REAL,
                lastErrorCode TEXT,
                createdAt REAL NOT NULL,
                updatedAt REAL NOT NULL,
                generationID TEXT NOT NULL DEFAULT '',
                schedulerEpoch INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_state_priority ON analysis_jobs(state, priority DESC, createdAt ASC);
            CREATE INDEX IF NOT EXISTS idx_jobs_workkey ON analysis_jobs(workKey);
            CREATE INDEX IF NOT EXISTS idx_jobs_episode ON analysis_jobs(episodeId);
            INSERT INTO analysis_jobs
                (jobId, jobType, episodeId, podcastId, analysisAssetId, workKey,
                 sourceFingerprint, downloadId, priority, desiredCoverageSec,
                 featureCoverageSec, transcriptCoverageSec, cueCoverageSec,
                 state, attemptCount, nextEligibleAt, leaseOwner, leaseExpiresAt,
                 lastErrorCode, createdAt, updatedAt, generationID, schedulerEpoch)
            VALUES
                ('job-up', 'preAnalysis', 'ep-up', 'pod-up', NULL, 'wk-up',
                 'fp-up', 'dl-up', 7, 90.0,
                 0, 0, 0,
                 'queued', 3, NULL, NULL, NULL,
                 NULL, 100.0, 200.0, 'gen-nondefault-xyz', 5);
            UPDATE _meta SET value = '29' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, regress, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        // Sanity: the regressed table genuinely lacks the reject columns.
        #expect(!(try probeColumnExists(in: dir, table: "analysis_jobs", column: "lastRejectReason")))
        #expect(!(try probeColumnExists(in: dir, table: "analysis_jobs", column: "lastRejectAt")))

        // Re-open and migrate: the V29→V30 step must add both columns.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeColumnExists(in: dir, table: "analysis_jobs", column: "lastRejectReason"))
        #expect(try probeColumnExists(in: dir, table: "analysis_jobs", column: "lastRejectAt"))

        // The pre-gy2s row survives with EVERY field intact. `readJob` reads
        // positional indices 0..22; the reject columns landed at 23 / 24. If
        // the append had shifted the reader, generationID/schedulerEpoch (the
        // trailing 21 / 22 reads) would come back wrong — so these two
        // assertions are the load-bearing positional-integrity proof.
        let row = try #require(try await store.fetchJob(byId: "job-up"))
        #expect(row.jobType == "preAnalysis")
        #expect(row.episodeId == "ep-up")
        #expect(row.podcastId == "pod-up")
        #expect(row.workKey == "wk-up")
        #expect(row.priority == 7)
        #expect(row.desiredCoverageSec == 90.0)
        #expect(row.state == "queued")
        #expect(row.attemptCount == 3)
        #expect(row.createdAt == 100.0)
        #expect(row.updatedAt == 200.0)
        #expect(row.generationID == "gen-nondefault-xyz", "generationID (idx 21) must survive the appended reject columns — no positional shift")
        #expect(row.schedulerEpoch == 5, "schedulerEpoch (idx 22) must survive the appended reject columns — no positional shift")

        // New columns backfill to NULL: no advisory recorded for a pre-existing
        // row (fetch returns nil, not a garbage/zero row).
        #expect(try await store.fetchJobAdmissionReject(jobId: "job-up") == nil)
    }

    @Test("V30 migration is idempotent across resetMigratedPathsForTesting")
    func v30MigrationIsIdempotent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let v1 = try await store.schemaVersion()

        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()
        let v2 = try await store.schemaVersion()

        #expect(v1 == AnalysisStore.currentSchemaVersion)
        #expect(v2 == AnalysisStore.currentSchemaVersion)
        #expect(try probeColumnExists(in: dir, table: "analysis_jobs", column: "lastRejectReason"))
        #expect(try probeColumnExists(in: dir, table: "analysis_jobs", column: "lastRejectAt"))
    }

    @Test("isolated ladder (migrateOnlyForTesting) reaches v30 and adds the columns")
    func isolatedLadderReachesV30() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        // createTables() builds analysis_jobs; the ladder seam then runs the
        // V*IfNeeded chain (the reject columns already exist after the first
        // migrate(), so addColumnIfNeeded no-ops but setSchemaVersion(30) still
        // bumps).
        try await store.migrate()
        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeColumnExists(in: dir, table: "analysis_jobs", column: "lastRejectReason"))
        #expect(try probeColumnExists(in: dir, table: "analysis_jobs", column: "lastRejectAt"))
    }

    // MARK: - record / fetch round-trip

    @Test("recordJobAdmissionReject → fetchJobAdmissionReject round-trips (reason, at) and is UPDATE-in-place")
    func recordFetchRoundTrip() async throws {
        let store = try await makeTestStore()

        // A fresh queued row with a known updatedAt so we can prove the
        // advisory write does NOT disturb the lifecycle clock.
        let job = makeAnalysisJob(
            jobId: "job-rt",
            jobType: "preAnalysis",
            episodeId: "ep-rt",
            workKey: "wk-rt",
            sourceFingerprint: "fp-rt",
            state: "queued",
            updatedAt: 4242.0
        )
        try await store.insertJob(job)

        // No advisory before the first write.
        #expect(try await store.fetchJobAdmissionReject(jobId: "job-rt") == nil)

        try await store.recordJobAdmissionReject(jobId: "job-rt", reason: "media_cap", at: 1000.0)
        let first = try #require(try await store.fetchJobAdmissionReject(jobId: "job-rt"))
        #expect(first.reason == "media_cap")
        #expect(first.at == 1000.0)

        // A second reject UPDATES in place (no row spam) and refreshes the
        // fields; the job's own updatedAt is deliberately untouched.
        try await store.recordJobAdmissionReject(jobId: "job-rt", reason: "wifi_required", at: 2000.0)
        let second = try #require(try await store.fetchJobAdmissionReject(jobId: "job-rt"))
        #expect(second.reason == "wifi_required")
        #expect(second.at == 2000.0)

        // Still exactly one row for this jobId, and its lifecycle clock/state
        // are unchanged by the advisory writes.
        let after = try #require(try await store.fetchJob(byId: "job-rt"))
        #expect(after.state == "queued")
        #expect(after.updatedAt == 4242.0, "recordJobAdmissionReject must not bump updatedAt (advisory is orthogonal to the lease/lifecycle clock)")

        // A write to a non-existent jobId is a benign no-op (0 rows), not a throw.
        try await store.recordJobAdmissionReject(jobId: "job-does-not-exist", reason: "thermal", at: 3000.0)
        #expect(try await store.fetchJobAdmissionReject(jobId: "job-does-not-exist") == nil)
    }
}
