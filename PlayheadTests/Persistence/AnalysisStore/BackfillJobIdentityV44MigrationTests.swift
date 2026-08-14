// BackfillJobIdentityV44MigrationTests.swift
// playhead-wxsv — `backfill_jobs.jobId` stops naming a transcript, and the
// transcript version becomes a column (schema V44).
//
// THE MIGRATION IS DESTRUCTIVE ON PURPOSE, and that is what these tests are
// about. Every pre-v44 row's id was minted as
// `SHA256(assetId | transcriptVersion | phase | offset)`. No invocation on a
// v44 binary derives that preimage again, so the row is unreachable by
// construction — and the only way to give it an `attemptTranscriptVersion`
// would be to read the transcript that is on disk NOW and write it onto
// progress that was made against something else. That reading — a value that
// names the transcript being read as though it named the job's own history — is
// the exact defect this bead removes. Committing it inside the fix is the
// failure mode the drop exists to prevent.
//
// Dan approved the break: "I am ok with breaking a schema, I am still the only
// user."
//
// EVIDENCE NOTE: the field numbers quoted in the doc comments come from the
// 2026-08-07 device pull (`db-new-t2h`, 30 `backfill_jobs` rows: 16 queued,
// 7 deferred, 5 failed, 2 complete). Every FIXTURE below is built by this file.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("backfill_jobs identity V44 migration (playhead-wxsv)")
struct BackfillJobIdentityV44MigrationTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "BackfillJobIdentityV44")
    }

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    @Test("a fresh DB lands at v44 with the attempt-version column")
    func freshDbHasV44Column() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // Drift guard, pinned to the LITERAL head (49 → 50, playhead-e6d3's
        // re-judging of the coverage-lane rows the FLAT under-coverage budget
        // retired). Never `== currentSchemaVersion`: that passes for every value
        // and stops policing anything. V50 touches only `backfill_jobs.retryCount`
        // on already-`failed` rows, so the `attemptTranscriptVersion` probe below
        // is untouched — and this DB has no rows at all when it runs.
        #expect(AnalysisStore.currentSchemaVersion == 50)

        try await store.insertAsset(makeAsset(id: "asset-fresh"))
        try await store.insertBackfillJob(
            makeBackfillJob(jobId: "fresh", analysisAssetId: "asset-fresh")
        )
        #expect(try await store.fetchBackfillJob(byId: "fresh")?.attemptTranscriptVersion == nil)
    }

    /// The whole point of the rung: pre-v44 rows are DELETED.
    ///
    /// The fixture rewinds a head-shaped DB to v43 and seeds two rows through
    /// raw SQL — one `complete`, one `queued` with a real cursor — because those
    /// are the two shapes an author is most tempted to preserve. The `complete`
    /// one looks like finished work worth keeping; the cursor one looks like
    /// progress worth keeping. Neither can be addressed again, and neither
    /// carries the version its progress was made against, so keeping either
    /// means inventing one.
    ///
    /// A SIBLING ROW PROVES THE BLAST RADIUS IS THE TABLE. A migration that
    /// reached for `DELETE FROM` on the wrong table, or that cascaded through
    /// the asset, would pass a `backfill_jobs` count of zero just as happily.
    @Test("playhead-wxsv: the v43->v44 step deletes every pre-existing backfill row")
    func v44DropsLegacyRows() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        try await bootstrap.insertAsset(makeAsset(id: "asset-legacy"))

        // Regress to v43: drop the new column by rebuilding the table in its
        // pre-wxsv shape, seed two legacy rows, and rewind `_meta`.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            DROP TABLE IF EXISTS backfill_jobs;
            CREATE TABLE backfill_jobs (
                jobId TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                podcastId TEXT,
                phase TEXT NOT NULL,
                coveragePolicy TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                progressCursor TEXT,
                retryCount INTEGER NOT NULL DEFAULT 0,
                deferReason TEXT,
                status TEXT NOT NULL DEFAULT 'queued',
                scanCohortJSON TEXT,
                createdAt REAL NOT NULL,
                updatedAt REAL NOT NULL DEFAULT 0
            );
            INSERT INTO backfill_jobs
              (jobId, analysisAssetId, phase, coveragePolicy, status, createdAt, updatedAt)
              VALUES ('fm-legacy-complete', 'asset-legacy', 'fullEpisodeScan',
                      'fullCoverage', 'complete', 1000, 1000);
            INSERT INTO backfill_jobs
              (jobId, analysisAssetId, phase, coveragePolicy, status, progressCursor,
               createdAt, updatedAt)
              VALUES ('fm-legacy-queued', 'asset-legacy', 'fullEpisodeScan',
                      'fullCoverage', 'queued',
                      '{"processedUnitCount":0,"lastProcessedUpperBoundSec":683.58}',
                      1001, 1001);
            UPDATE _meta SET value = '43' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // A v43-seeded DB must climb the WHOLE remaining ladder, not just the
        // V44 rung under test — pinned to the literal head for the same reason
        // as the drift guard above.
        //
        // This one is NOT the same assertion as the drift guard: it reads the
        // version off the DATABASE, not off the constant, so it is the only pin
        // in the suite that would still fail if a new rung were added to the
        // constant but never reached from an old seed. That is exactly the risk
        // V50 carries — `migrateUnderCoverageRetryBudgetV50IfNeeded` refuses to
        // run below 49 (the deliberate don't-step-over-a-rolled-back-V39 rule),
        // so a v43 seed only reaches 50 if every rung from 44 up actually ran.
        // 50 here is therefore a real claim about the ladder, not a restatement.
        #expect(try await store.schemaVersion() == 50)
        #expect(try await store.fetchBackfillJob(byId: "fm-legacy-complete") == nil,
                "a completed row minted under the old preimage cannot be addressed again")
        #expect(try await store.fetchBackfillJob(byId: "fm-legacy-queued") == nil,
                "…and neither can a cursor nobody can prove the provenance of")
        #expect(try await store.countResumableBackfillJobs(assetId: "asset-legacy") == 0)

        // The blast radius is the table, not the asset.
        #expect(try await store.fetchAsset(id: "asset-legacy") != nil,
                "the parent asset must survive — only the coverage-lane rows are dropped")

        // And the table is usable at the new shape immediately afterwards.
        try await store.insertBackfillJob(
            makeBackfillJob(jobId: "post-v44", analysisAssetId: "asset-legacy")
        )
        try await store.markBackfillJobRunning(jobId: "post-v44", transcriptVersion: "tx-v1")
        #expect(try await store.fetchBackfillJob(byId: "post-v44")?.attemptTranscriptVersion == "tx-v1")
    }

    /// The rung must be reachable from the isolated ladder, not only from
    /// `migrate()`'s `createTables()` — which asserts the column defensively and
    /// would mask a rung that never runs. Rewinding to v43 with a row present is
    /// what tells the two apart: `createTables()` adds the column, but only the
    /// rung deletes.
    @Test("isolated ladder (migrateOnlyForTesting) includes v44")
    func isolatedLadderReachesV44() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-ladder"))
        try await store.insertBackfillJob(
            makeBackfillJob(jobId: "pre-ladder", analysisAssetId: "asset-ladder")
        )
        try await store.setMetaValue(forKey: "schema_version", value: "43")

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try await store.fetchBackfillJob(byId: "pre-ladder") == nil,
                "the rung, not createTables(), is what clears the pre-v44 rows")
    }

    /// A v39 rollback leaves `_meta` at 38 so the next launch retries it. Every
    /// rung added after V39 has to refuse to step over that, and the ladder does
    /// not enforce it structurally — so this rung's `guard observed >= 43` needs
    /// its own witness, exactly as V40–V43 have.
    ///
    /// **A rewound `_meta` alone does NOT reproduce the condition**, and the
    /// first cut of this test was green for that reason: on a healthy DB V39
    /// simply succeeds and the ladder climbs straight back to head. The fixture
    /// has to make V39 genuinely ROLL BACK, which is what the trigger below
    /// does — copied from `MergedChildRowDedupeV40MigrationTests`, which is
    /// where this shape was first paid for.
    ///
    /// The stakes are higher for this rung than for its siblings: V40–V43 add
    /// columns, so stepping over V39 merely strands an index. V44 DELETES, so a
    /// rung that ran on a database it had no business touching would destroy
    /// coverage-lane work on every launch of a device stuck at 38.
    @Test("playhead-wxsv: v44 does not step over a rolled-back v39")
    func v44DoesNotStepOverV39() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-v39"))
        try await store.insertBackfillJob(
            makeBackfillJob(jobId: "survives-v39-hold", analysisAssetId: "asset-v39")
        )

        // Rewind to the state a device is in when V39 rolled back: no unique
        // asset-identity index, a duplicate pair for V39 to trip over, a trigger
        // that makes its delete ABORT, and `schema_version` at 38.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            DROP INDEX IF EXISTS idx_assets_episode_fingerprint;
            DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint;
            INSERT INTO analysis_assets
              (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
              VALUES ('dupe-old', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 1.0);
            INSERT INTO analysis_assets
              (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
              VALUES ('dupe-new', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 2.0);
            CREATE TRIGGER wxsv_v39_guard BEFORE DELETE ON analysis_assets
              BEGIN SELECT RAISE(ABORT, 'v44 step-over fixture'); END;
            UPDATE _meta SET value = '38' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 38,
                "a DB held at 38 by a rolled-back v39 must not be stamped 44")
        #expect(try await store.fetchBackfillJob(byId: "survives-v39-hold") != nil,
                "…and must not have had its rows deleted by a rung that never legitimately ran")
    }
}

// MARK: - playhead-e6d3 (schema V50)

/// V50 gives ONE fresh budget to every coverage-lane row the FLAT under-coverage
/// rule retired — the eight-of-nine population on the 2026-08-14 device pull,
/// which had emptied the coarse phase's candidate set permanently.
///
/// It lives in this file rather than its own because it is a `backfill_jobs`
/// migration and this is the `backfill_jobs` migration suite; a new file would
/// oblige an `xcodegen generate` in a worktree where the gitignored specialist
/// model folder is absent, which is a bigger blast radius than the test is worth.
@Suite("backfill_jobs under-coverage retry budget V50 migration (playhead-e6d3)")
struct UnderCoverageRetryBudgetV50MigrationTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "UnderCoverageRetryBudgetV50")
    }

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    /// The five shapes the pull actually contains, seeded verbatim. Only the
    /// first is the population whose RULE changed.
    private func seed(_ store: AnalysisStore) async throws {
        try await store.insertAsset(makeAsset(id: "asset-e6d3"))
        let cap = AdmissionController.maxRetries
        for (jobId, status, retries, reason) in [
            // 561CEF5B on the pull: retired by the flat budget.
            ("fm-flat-casualty", BackfillJobStatus.failed, cap, "underCoverageBudgetSpent-fullEpisodeScan"),
            // bkhc's branch already reset on progress, so its exhaustion is
            // three genuinely CONSECUTIVE barren windows and stands.
            ("fm-expired", BackfillJobStatus.failed, cap, "expiredWithoutProgress-fullEpisodeScan"),
            // Already re-drivable; nothing to repair.
            ("fm-under-budget", BackfillJobStatus.failed, 1, "underCoverageBudgetSpent-fullEpisodeScan"),
            // B97B8779 on the pull: `complete` with the defer reason still on
            // the row from an earlier attempt. Completion is not exhaustion.
            ("fm-complete", BackfillJobStatus.complete, cap, "underCoverageBudgetSpent-fullEpisodeScan"),
            // A9F6DF05 on the pull: an FM daemon error, a different cause.
            ("fm-model-error", BackfillJobStatus.failed, cap,
             "Error Domain=FoundationModels.LanguageModelError Code=-1")
        ] {
            try await store.insertBackfillJob(makeBackfillJob(
                jobId: jobId,
                analysisAssetId: "asset-e6d3",
                retryCount: retries,
                deferReason: reason,
                status: status
            ))
        }
    }

    @Test("playhead-e6d3: v50 re-judges the flat-budget casualties and NOTHING else")
    func v50ResetsOnlyTheFlatBudgetCasualties() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await seed(store)
        try await store.setMetaValue(forKey: "schema_version", value: "49")

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        let cap = AdmissionController.maxRetries
        #expect(try await store.fetchBackfillJob(byId: "fm-flat-casualty")?.retryCount == 0,
                "the one row whose retirement rule changed")
        #expect(try await store.fetchBackfillJob(byId: "fm-flat-casualty")?.status == .failed,
                "the STATUS is deliberately untouched — `failed` under budget is already a candidate everywhere")
        #expect(try await store.fetchBackfillJob(byId: "fm-expired")?.retryCount == cap,
                "bkhc's branch already reset on progress; three barren windows is three barren windows")
        #expect(try await store.fetchBackfillJob(byId: "fm-under-budget")?.retryCount == 1,
                "already re-drivable — a repair that moved this would be inventing history")
        #expect(try await store.fetchBackfillJob(byId: "fm-complete")?.retryCount == cap,
                "a `complete` row is not exhausted and must not be re-opened")
        #expect(try await store.fetchBackfillJob(byId: "fm-model-error")?.retryCount == cap,
                "a different cause, whose rule this bead did not change")

        // The consequence: the asset is reachable again by the query the coarse
        // phase and the ad-scan re-drive both start from.
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 10).contains("asset-e6d3"))
    }

    @Test("playhead-e6d3: v50 is idempotent — a second ladder pass does not re-open a row the new rule retired")
    func v50DoesNotRunTwice() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await seed(store)
        try await store.setMetaValue(forKey: "schema_version", value: "49")
        try await store.migrateOnlyForTesting()

        #expect(try await store.fetchBackfillJob(byId: "fm-flat-casualty")?.retryCount == 0)

        // The row spends its new budget under the NEW rule and is retired again
        // with the same cause. A rung that re-fired would hand it yet another
        // budget on every launch — an unbounded loop dressed as a migration.
        //
        // Written in raw SQL rather than through `markBackfillJobFailed`,
        // because that transition is idempotent on a row that is ALREADY
        // `failed` (it preserves the existing count on purpose, so a
        // double-catch cannot double-bump), which would leave the fixture at 0
        // and make this test pass for the wrong reason.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        #expect(sqlite3_exec(
            db,
            "UPDATE backfill_jobs SET retryCount = \(AdmissionController.maxRetries) WHERE jobId = 'fm-flat-casualty';",
            nil, nil, nil
        ) == SQLITE_OK)
        sqlite3_close_v2(db)
        #expect(try await store.fetchBackfillJob(byId: "fm-flat-casualty")?.retryCount
                == AdmissionController.maxRetries, "fixture precondition")

        try await store.migrateOnlyForTesting()
        #expect(try await store.fetchBackfillJob(byId: "fm-flat-casualty")?.retryCount
                == AdmissionController.maxRetries,
                "the rung stamped its own version — a second ladder pass must not refill the budget")
    }

    /// Every rung added after V39 owes this witness: a device held at 38 by a
    /// rolled-back V39 must not have a later rung run on it. The fixture is
    /// `BackfillJobIdentityV44MigrationTests.v44DoesNotStepOverV39`'s, which is
    /// where the shape was first paid for.
    @Test("playhead-e6d3: v50 does not step over a rolled-back v39")
    func v50DoesNotStepOverV39() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await seed(store)

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            DROP INDEX IF EXISTS idx_assets_episode_fingerprint;
            DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint;
            INSERT INTO analysis_assets
              (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
              VALUES ('dupe-old', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 1.0);
            INSERT INTO analysis_assets
              (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
              VALUES ('dupe-new', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 2.0);
            CREATE TRIGGER e6d3_v39_guard BEFORE DELETE ON analysis_assets
              BEGIN SELECT RAISE(ABORT, 'v50 step-over fixture'); END;
            UPDATE _meta SET value = '38' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 38,
                "a DB held at 38 by a rolled-back v39 must not be stamped 50")
        #expect(try await store.fetchBackfillJob(byId: "fm-flat-casualty")?.retryCount
                == AdmissionController.maxRetries,
                "…and must not have had its budget refreshed by a rung that never legitimately ran")
    }
}
