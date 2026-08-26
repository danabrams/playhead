// SemanticScanRunAttributionTests.swift
// playhead-hx6n — `semantic_scan_results` learns when it was written, in which
// scene phase, and under which run (schema V42).
//
// The bead exists because two separate investigations ended in "cannot be
// determined from device data": playhead-kvs8 could not re-split the measured
// 2.4x slower-than-realtime FM figure by foreground versus background, and the
// 2026-07-31/08-01 stall timeline could only be dated through
// `backfill_jobs.updatedAt` because the scan rows themselves were undateable.
//
// So the tests below are not "does the column exist" tests. They are:
//   * the JOIN, run for real against a three-table fixture;
//   * the NO-BACKFILL property, proved by a row that survives the migration and
//     still reads NULL afterwards;
//   * and the NEGATIVE — the tests that go red the moment a `nil` scene phase is
//     silently read as a phase. That last one is the whole bead. A consumer that
//     defaults NULL to `.foreground` still produces numbers; they are simply
//     wrong in a way nothing downstream can detect.
//
// EVIDENCE NOTE: every number here comes from a FIXTURE built by this file. No
// device data was used. The newest device pull available on this box is
// 2026-07-22, which predates V42 by construction and therefore cannot contain a
// single attributed row.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("playhead-hx6n: semantic scan run attribution")
struct SemanticScanRunAttributionTests {

    // MARK: - Fixture helpers

    private static let cohort = """
        {"promptLabel":"l","promptHash":"p","schemaHash":"s","scanPlanHash":"sp",\
        "normalizationHash":"n","osBuild":"26A","locale":"en_US","appBuild":"1"}
        """

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

    // swiftlint:disable:next function_parameter_count
    private func makeScan(
        id: String,
        assetId: String,
        start: Double,
        end: Double,
        latencyMs: Double?,
        status: SemanticScanStatus = .success,
        errorContext: String? = nil,
        createdAt: Double? = nil,
        scenePhase: ScanScenePhase? = nil,
        backfillJobId: String? = nil,
        transcriptVersion: String = "tv-1"
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: Int(start),
            windowLastAtomOrdinal: Int(end),
            windowStartTime: start,
            windowEndTime: end,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: status,
            attemptCount: 1,
            errorContext: errorContext,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: latencyMs,
            prewarmHit: false,
            scanCohortJSON: Self.cohort,
            transcriptVersion: transcriptVersion,
            // Distinct per row so `UNIQUE(reuseKeyHash)` never collapses two
            // fixture rows into one and quietly halves a count.
            reuseScope: id,
            createdAt: createdAt,
            scenePhase: scenePhase,
            backfillJobId: backfillJobId
        )
    }

    private func makeJob(
        jobId: String,
        assetId: String,
        createdAt: Double
    ) -> BackfillJob {
        BackfillJob(
            jobId: jobId,
            analysisAssetId: assetId,
            podcastId: "pod-\(assetId)",
            phase: .fullEpisodeScan,
            coveragePolicy: .fullCoverage,
            priority: 0,
            progressCursor: nil,
            retryCount: 0,
            deferReason: nil,
            status: .complete,
            scanCohortJSON: Self.cohort,
            createdAt: createdAt
        )
    }

    /// Run an arbitrary read-only query and hand back the rows as strings.
    /// `NULL` arrives as `nil` so a test can tell "no match" from "empty
    /// string" — the distinction the whole bead turns on.
    private func query(
        _ sql: String,
        in directory: URL
    ) throws -> [[String?]] {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw AnalysisStoreError.queryFailed("open failed")
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            let message = sqlite3_errmsg(db).map { String(cString: $0) } ?? "unknown"
            throw AnalysisStoreError.queryFailed("prepare failed: \(message) — \(sql)")
        }
        defer { sqlite3_finalize(stmt) }
        var rows: [[String?]] = []
        let columnCount = Int(sqlite3_column_count(stmt))
        while sqlite3_step(stmt) == SQLITE_ROW {
            var row: [String?] = []
            for index in 0..<columnCount {
                if sqlite3_column_type(stmt, Int32(index)) == SQLITE_NULL {
                    row.append(nil)
                } else {
                    row.append(sqlite3_column_text(stmt, Int32(index)).map { String(cString: $0) })
                }
            }
            rows.append(row)
        }
        return rows
    }

    // MARK: - Migration: the shape lands, and nothing is invented

    @Test("V42: a fresh store carries the three attribution columns and their indexes")
    func freshStoreReachesV42WithAttributionShape() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // Drift guard, pinned to the LITERAL head (48 → 49, playhead-mn5e/2qz6's
        // `trust_episode_observations` ledger + the `observationCount` reset).
        // Never `== currentSchemaVersion`: that passes for every value and stops
        // policing anything.
        //
        // Read against THIS suite's real claim — "nothing is invented for a row
        // that never had one". V49's only data statement is
        // `UPDATE podcast_profiles SET observationCount = 0`; it writes no
        // `semantic_scan_results` column and back-fills nothing, so the
        // no-backfill proof below still has something to prove.
        // …and the same reading holds for 49 → 50 (playhead-e6d3), whose only
        // data statement is an UPDATE of `backfill_jobs.retryCount`: it writes
        // no `semantic_scan_results` column and back-fills nothing, so the
        // no-backfill proof below still has something to prove.
        // 50 → 51 read for this rung (playhead-wogi): V51 READS
        // `semantic_scan_results` (passA rows that examined their window) to
        // compute the prefix it lowers `backfill_jobs.progressCursor` to. It
        // writes nothing to this table, so every attribution column this rung
        // pins is untouched.
        // 52 → 53 read for this rung (playhead-jc42): V53 deletes duplicate
        // `transcript_chunks` rows and builds a UNIQUE index on that table. It
        // names no `semantic_scan_results` column and back-fills nothing, so
        // the no-backfill proof below still has something to prove.
        // 60 -> 61 read for this rung (playhead-iw7q): V61 is on THIS table —
        // it adds `semantic_scan_results.usedPermissiveFallback`. It is
        // additive only: `addColumnIfNeeded` and nothing else, no UPDATE and no
        // DEFAULT, so every existing column keeps its value and every row this
        // rung reads is byte-identical before and after. The deliberate absence
        // of a backfill is the point of that migration and is proved in its own
        // suite; here it is what makes this rung's claims survive it.
        // 61 -> 62 read for this rung (playhead-7dgx): V62 CREATES TWO NEW TABLES
        // — `background_download_drops` and its single-row arming companion — and
        // touches no existing table, column or row: no ALTER, no UPDATE, no DELETE
        // and no backfill (every drop before this build deleted its own evidence,
        // so there is nothing recoverable to seed). It names nothing this rung
        // asserts, so no assertion here moves.
        // 62 -> 63 read for this rung (playhead-4xmz): V63 CREATES TWO NEW TABLES —
        // `download_work_journal` and its single-row arming companion — and touches no
        // existing table, column or row: no ALTER, no UPDATE, no DELETE and no backfill
        // (every download event before this build went to a no-op recorder and left no
        // trace, so there is nothing recoverable to seed). It names nothing this rung
        // asserts, so no assertion here moves.
        // 63 -> 64 read for this rung (playhead-sdis): V64 ADDS FOUR NULLABLE
        // COLUMNS and only to the two playhead-7dgx tables — `launchId`,
        // `sessionCrossingId` and `launchArmingState` on
        // `background_download_drops`, `lastArmedLaunchId` on
        // `background_download_drop_arming`. No other table, no other column, no
        // UPDATE, no DELETE, no DEFAULT and no backfill: a pre-V64 row is left
        // NULL because every candidate default would turn an absence into a
        // launch count. It names nothing this rung asserts, so no assertion here
        // moves.
        // 64 -> 65 read for this rung (playhead-1gu0): V65 RENAMES ONE COLUMN —
        // `semantic_scan_results.runCorrelationId` becomes `backfillJobId`, and its
        // index moves with it. A pure `ALTER TABLE … RENAME COLUMN`: no row moves, no
        // value is written, nothing is backfilled and no other table is named. This rung's OWN
        // column is the one that moves: it is spelled `backfillJobId` everywhere
        // below, and the rail proving a database carrying the old spelling comes out
        // carrying the new one WITH ITS VALUES is `v64RowKeepsItsJobIdAcrossTheV65Rename`.
        #expect(AnalysisStore.currentSchemaVersion == 65)
        for column in ["createdAt", "scenePhase", "backfillJobId"] {
            #expect(
                try probeColumnExists(in: dir, table: "semantic_scan_results", column: column),
                "V42 must add `\(column)` to semantic_scan_results"
            )
        }
        #expect(try probeIndexExists(in: dir, indexName: "idx_semantic_scan_results_createdAt"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_semantic_scan_results_backfill_job"))
    }

    /// **The no-backfill proof.**
    ///
    /// A row that already existed when the migration ran must survive it and
    /// must still read `nil` for all three fields afterwards. If a future hand
    /// "helpfully" backfills `createdAt` from `rowid` order, or stamps
    /// `scenePhase = 'active'` because most scans historically ran in the
    /// foreground, this goes red — and it should, because inventing a value for
    /// a row that never had one is the exact defect this bead closes.
    @Test("V42: a v41 row survives the migration and stays unattributed forever")
    func v41RowSurvivesMigrationAndStaysUnattributed() async throws {
        let (bootstrap, dir) = try await makeTestStoreWithDirectory()
        try await bootstrap.insertAsset(makeAsset(id: "asset-v41"))

        // Regress the table to its v41 shape (three columns and two indexes
        // gone) and seed a row through raw SQL, exactly as a pre-V42 binary
        // would have written it.
        try await bootstrap.execForTesting("""
            DROP INDEX IF EXISTS idx_semantic_scan_results_createdAt;
            DROP INDEX IF EXISTS idx_semantic_scan_results_backfill_job;
            ALTER TABLE semantic_scan_results DROP COLUMN createdAt;
            ALTER TABLE semantic_scan_results DROP COLUMN scenePhase;
            ALTER TABLE semantic_scan_results DROP COLUMN backfillJobId;
            INSERT INTO semantic_scan_results
                (id, analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
                 windowStartTime, windowEndTime, scanPass, transcriptQuality,
                 disposition, spansJSON, status, attemptCount, errorContext,
                 inputTokenCount, outputTokenCount, latencyMs, prewarmHit,
                 scanCohortJSON, transcriptVersion, reuseKeyHash, runMode, jobPhase)
            VALUES
                ('scan-legacy', 'asset-v41', 0, 5, 0.0, 60.0, 'passA', 'good',
                 'noAds', '[]', 'success', 1, NULL, NULL, NULL, 30000.0, 0,
                 '\(Self.cohort)', 'tv-1', 'legacy-reuse-key', 'shadow', 'shadow');
            UPDATE _meta SET value = '41' WHERE key = 'schema_version';
            """)
        #expect(
            try !probeColumnExists(in: dir, table: "semantic_scan_results", column: "scenePhase"),
            "fixture precondition: the v41 regression must actually remove the column"
        )

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        for column in ["createdAt", "scenePhase", "backfillJobId"] {
            #expect(try probeColumnExists(in: dir, table: "semantic_scan_results", column: column))
        }

        // The row is still here (additive migration, no rebuild)…
        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-v41")
        try #require(rows.count == 1)
        let legacy = rows[0]
        #expect(legacy.id == "scan-legacy")
        #expect(legacy.latencyMs == 30000.0, "the migration must not disturb existing fields")

        // …and it is honestly unattributed.
        #expect(
            legacy.createdAt == nil,
            """
            A pre-V42 row read back a createdAt of \(String(describing: legacy.createdAt)). \
            It must be nil: nothing knows when that row was written, and a value \
            here — 0, the migration's own clock, anything — is a fabricated \
            timestamp that every stall-timeline reconstruction would then trust.
            """
        )
        #expect(
            legacy.scenePhase == nil,
            """
            A pre-V42 row read back scenePhase \(String(describing: legacy.scenePhase)). \
            It must be nil. This is the row class that made playhead-kvs8 \
            unanswerable, and it must keep saying so rather than pick a side.
            """
        )
        #expect(legacy.backfillJobId == nil)

        // And the split reports it as unattributed rather than as throughput.
        let split = SemanticScanThroughputSplit.compute(rows: rows)
        #expect(split.foreground.scanCount == 0)
        #expect(split.background.scanCount == 0)
        #expect(split.unattributed.scanCount == 1)
        #expect(split.attributedFraction == 0)
    }

    // MARK: - V65: the column is renamed and NOTHING is lost

    /// **The rename rail (playhead-1gu0).**
    ///
    /// A database written by a pre-V65 binary carries `runCorrelationId` with
    /// real job ids in it. After the rename it must carry `backfillJobId` with
    /// THE SAME job ids — a rename that silently drops its values is
    /// indistinguishable, on a fresh install, from one that worked.
    ///
    /// The fixture regresses a live store to the old spelling and re-opens it,
    /// which is the only way to exercise the branch: on a fresh install
    /// `createTables()` has already built the head shape, so the rename helper
    /// takes its no-op path and proves nothing.
    @Test("V65: a pre-V65 row keeps its job id across the runCorrelationId -> backfillJobId rename")
    func v64RowKeepsItsJobIdAcrossTheV65Rename() async throws {
        let (bootstrap, dir) = try await makeTestStoreWithDirectory()
        try await bootstrap.insertAsset(makeAsset(id: "asset-v64"))

        // Regress to the V64 spelling and seed two rows through it: one
        // attributed, one honestly unattributed.
        try await bootstrap.execForTesting("""
            DROP INDEX IF EXISTS idx_semantic_scan_results_backfill_job;
            ALTER TABLE semantic_scan_results RENAME COLUMN backfillJobId TO runCorrelationId;
            CREATE INDEX IF NOT EXISTS idx_semantic_scan_results_correlation
                ON semantic_scan_results(runCorrelationId);
            INSERT INTO semantic_scan_results
                (id, analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
                 windowStartTime, windowEndTime, scanPass, transcriptQuality,
                 disposition, spansJSON, status, attemptCount, errorContext,
                 inputTokenCount, outputTokenCount, latencyMs, prewarmHit,
                 scanCohortJSON, transcriptVersion, reuseKeyHash, runMode, jobPhase,
                 createdAt, scenePhase, runCorrelationId)
            VALUES
                ('scan-v64-attributed', 'asset-v64', 0, 5, 0.0, 60.0, 'passA', 'good',
                 'noAds', '[]', 'success', 1, NULL, NULL, NULL, 30000.0, 0,
                 '\(Self.cohort)', 'tv-1', 'v64-reuse-key-a', 'shadow', 'shadow',
                 1700000000.0, 'background', 'fm-9330e821aeb36a0d'),
                ('scan-v64-unattributed', 'asset-v64', 6, 11, 60.0, 120.0, 'passA', 'good',
                 'noAds', '[]', 'success', 1, NULL, NULL, NULL, 31000.0, 0,
                 '\(Self.cohort)', 'tv-1', 'v64-reuse-key-b', 'shadow', 'shadow',
                 1700000100.0, 'background', NULL);
            UPDATE _meta SET value = '64' WHERE key = 'schema_version';
            """)
        #expect(
            try probeColumnExists(in: dir, table: "semantic_scan_results", column: "runCorrelationId"),
            "fixture precondition: the V64 regression must actually restore the old spelling"
        )
        #expect(
            try !probeColumnExists(in: dir, table: "semantic_scan_results", column: "backfillJobId"),
            "fixture precondition: the new spelling must be gone, or the helper's no-op path is what runs"
        )

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeColumnExists(in: dir, table: "semantic_scan_results", column: "backfillJobId"))
        #expect(
            try !probeColumnExists(in: dir, table: "semantic_scan_results", column: "runCorrelationId"),
            """
            Both spellings are present after the migration. That is the failure \
            the ordering in `createTables()` exists to prevent: an empty \
            `backfillJobId` added alongside a populated `runCorrelationId`, with \
            every job id stranded in a column nothing reads.
            """
        )
        #expect(try probeIndexExists(in: dir, indexName: "idx_semantic_scan_results_backfill_job"))
        #expect(
            try !probeIndexExists(in: dir, indexName: "idx_semantic_scan_results_correlation"),
            "an index named for `correlation` over a column named `backfillJobId` is the same defect one layer down"
        )

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-v64")
        let byId = Dictionary(uniqueKeysWithValues: rows.map { ($0.id, $0) })
        try #require(rows.count == 2)
        #expect(
            byId["scan-v64-attributed"]?.backfillJobId == "fm-9330e821aeb36a0d",
            """
            The renamed column read back \
            \(String(describing: byId["scan-v64-attributed"]?.backfillJobId)). \
            A RENAME COLUMN moves no data, so the value written under the old \
            spelling must be readable under the new one — a nil here means the \
            rename dropped the column and re-added it, which loses every \
            attribution on the device.
            """
        )
        #expect(byId["scan-v64-attributed"]?.createdAt == 1_700_000_000)
        #expect(byId["scan-v64-attributed"]?.latencyMs == 30000.0, "the rename must not disturb neighbouring fields")
        #expect(
            byId["scan-v64-unattributed"]?.backfillJobId == nil,
            "a NULL stays NULL: nothing may invent a job for a row that never had one"
        )
    }

    /// A job id is per `(asset, phase, offset)`, so ONE value spans an asset's
    /// whole backfill history — which is what the V65 name now says out loud and
    /// what the old one denied. This is the property `SemanticSweepMarkComposer`
    /// was documented as relying on and does not: the id cannot separate two
    /// screenings of the same window, `transcriptVersion` can.
    ///
    /// THE FIXTURE BELOW IS THE 2026-08-19 DEVICE PULL'S SHAPE IN MINIATURE, AND
    /// IT QUOTES NO FIGURE FROM IT. ``SemanticSweepMarkComposer/corroboration(for:in:atTranscriptVersion:)``
    /// holds the measurement — every count, both pulls, each with the population
    /// it is over — and it is the ONLY place on this branch that does.
    ///
    /// That is deliberate and it is the branch's own finding. Six review rounds
    /// of playhead-1gu0 produced one defect class and almost nothing else: a
    /// figure restated in a second place and then corrected in only one of them.
    /// This doc was two of those — it carried the pull's counts under a
    /// population label that belonged to a different function, and the composer's
    /// copy was fixed while this one stood. A pointer cannot drift out of step
    /// with the thing it points at, so this is a pointer.
    // The name deliberately carries no semicolon: `mutation-battery.sh` SPLITS its
    // expectation field on ';', so a test whose display name contains one can
    // never be matched and every mutant naming it reports `expected test never
    // ran` instead of a verdict.
    @Test("V65: one backfill job id spans re-screenings, and transcriptVersion is what separates them")
    func oneJobIdSpansReScreeningsAndVersionsDoNot() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset(id: "asset-rescan"))

        for (index, version) in ["tv-1", "tv-2", "tv-3"].enumerated() {
            try await store.insertSemanticScanResult(
                makeScan(
                    id: "scan-rescan-\(index)",
                    assetId: "asset-rescan",
                    start: 600,
                    end: 690,
                    latencyMs: Double(3_000 + index * 1_000),
                    createdAt: 1_700_000_000 + Double(index) * 86_400,
                    scenePhase: .background,
                    backfillJobId: "fm-9330e821aeb36a0d",
                    transcriptVersion: version
                )
            )
        }

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-rescan")
        try #require(rows.count == 3)
        // THE SET IS TAKEN OVER NON-NIL IDS, AND "they are all present" IS ITS OWN
        // ASSERTION. `Set([nil, nil, nil]).count` is 1, so a bare distinct-count
        // reads a column that lost every value exactly like a column that shares
        // one — the standing defect class, inside the rail written to remove an
        // instance of it.
        //
        // NO MUTANT IN THE GU SERIES DEMONSTRATES IT, and an earlier version of
        // this note claimed one did ("GU02, an ADD where the RENAME belongs, is
        // precisely the mutation that empties this column"). Measured: GU02
        // leaves this rail GREEN, because the rail opens a store at the HEAD
        // shape, where `backfillJobId` already exists and the rename helper
        // returns before its first statement. GU02 can only empty a column on a
        // store carrying the OLD spelling, which is the sibling rail's fixture
        // and not this one. The assertion stays — a vacuous rail is a defect
        // whether or not a mutant in this series happens to reach it — but the
        // reach was asserted rather than measured, which is the thing this bead
        // is about.
        #expect(
            rows.allSatisfy { $0.backfillJobId != nil },
            """
            \(rows.filter { $0.backfillJobId == nil }.count) of \(rows.count) rows \
            came back with NO job id. An absent column and a shared one are \
            indistinguishable to a distinct-count, so this is asserted separately.
            """
        )
        #expect(
            Set(rows.compactMap(\.backfillJobId)) == ["fm-9330e821aeb36a0d"],
            """
            Three screenings of one window across three days produced \
            \(Set(rows.compactMap(\.backfillJobId)).sorted()) — it must be exactly \
            the one id they were written under: a backfill job is keyed on \
            (asset, phase, offset) and outlives every re-screening, which is \
            precisely why this column cannot be read as "a different FM call".
            """
        )
        #expect(Set(rows.map(\.transcriptVersion)).count == 3)
        #expect(Set(rows.map(\.latencyMs)).count == 3)
    }

    // MARK: - The join, demonstrated

    /// **The join, run for real.**
    ///
    /// Three tables, one fixture: a scan row carries `backfillJobId =
    /// backfill_jobs.jobId`, and its `createdAt` falls inside a
    /// `background_task_runs` window. The chain scan -> job -> BGTask run is
    /// resolved by SQLite, not asserted by inspection.
    ///
    /// The second row is the case that would be easy to get wrong: a scan that
    /// ran in the FOREGROUND belongs to no BGTask run at all. It must still
    /// appear, with a NULL run — "no run" is an answer, and an INNER JOIN that
    /// silently dropped it would make every foreground scan invisible to the
    /// very split this bead exists to enable.
    @Test("V42: a scan row joins to its backfill job and to the BGTask run that contained it")
    func scanRowJoinsToJobAndRun() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset(id: "asset-join"))

        let runStart = 1_700_000_000.0
        let runEnd = runStart + 300
        try await store.insertBackgroundTaskRun(
            BackgroundTaskRunRecord(
                runId: "run-bg-1",
                entryPoint: .backfill,
                taskIdentifier: "com.playhead.backfill",
                startedAt: runStart,
                finishedAt: runEnd,
                outcome: .admittedWork,
                assetId: "asset-join",
                scenePhase: ScanScenePhase.background.rawValue
            )
        )
        try await store.insertBackfillJob(
            makeJob(jobId: "job-bg", assetId: "asset-join", createdAt: runStart)
        )
        try await store.insertBackfillJob(
            makeJob(jobId: "job-fg", assetId: "asset-join", createdAt: runEnd + 1_000)
        )

        // One scan inside the BGTask window, one well outside it.
        try await store.insertSemanticScanResult(
            makeScan(
                id: "scan-in-run",
                assetId: "asset-join",
                start: 0,
                end: 60,
                latencyMs: 30_000,
                createdAt: runStart + 120,
                scenePhase: .background,
                backfillJobId: "job-bg"
            )
        )
        try await store.insertSemanticScanResult(
            makeScan(
                id: "scan-foreground",
                assetId: "asset-join",
                start: 60,
                end: 120,
                latencyMs: 20_000,
                createdAt: runEnd + 2_000,
                scenePhase: .active,
                backfillJobId: "job-fg"
            )
        )

        let joined = try query(
            """
            SELECT s.id, j.jobId, j.phase, r.runId, r.entryPoint, r.scenePhase
            FROM semantic_scan_results s
            JOIN backfill_jobs j
              ON j.jobId = s.backfillJobId
            LEFT JOIN background_task_runs r
              ON s.createdAt >= r.startedAt
             AND s.createdAt <= COALESCE(r.finishedAt, 9e18)
            WHERE s.backfillJobId IS NOT NULL
            ORDER BY s.createdAt ASC
            """,
            in: dir
        )

        try #require(
            joined.count == 2,
            """
            The scan -> job join resolved \(joined.count) row(s), not 2. Both \
            scans carry a backfillJobId that names a real backfill_jobs row, \
            so an INNER JOIN must return both — a missing row means the \
            job id does not actually join, which is the failure mode \
            this bead was filed against.
            """
        )

        #expect(joined[0][0] == "scan-in-run")
        #expect(joined[0][1] == "job-bg")
        #expect(joined[0][2] == BackfillJobPhase.fullEpisodeScan.rawValue)
        #expect(
            joined[0][3] == "run-bg-1",
            "the background scan must resolve to the BGTask run whose window contains it"
        )
        #expect(joined[0][4] == BackgroundTaskRunEntryPoint.backfill.rawValue)
        #expect(joined[0][5] == ScanScenePhase.background.rawValue)

        #expect(joined[1][0] == "scan-foreground")
        #expect(joined[1][1] == "job-fg")
        #expect(
            joined[1][3] == nil,
            """
            A foreground scan resolved to BGTask run \(joined[1][3] ?? "nil"). \
            It ran outside every run window and must LEFT-JOIN to NULL: \
            "no run" is the correct answer, and attributing it to one would \
            manufacture background execution that never happened.
            """
        )
    }

    // MARK: - The store stamps a clock; it never invents a phase

    @Test("V42: the store stamps createdAt when a writer supplies none — and stamps nothing else")
    func storeStampsClockButNeverInventsAPhase() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-stamp"))

        try await store.insertSemanticScanResult(
            makeScan(id: "scan-stamp", assetId: "asset-stamp", start: 0, end: 60, latencyMs: 1_000),
            now: 1_700_000_777
        )

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-stamp")
        try #require(rows.count == 1)
        #expect(
            rows[0].createdAt == 1_700_000_777,
            """
            A post-V42 write left createdAt as \(String(describing: rows[0].createdAt)). \
            The store's backstop clock exists so a NULL createdAt means exactly \
            one thing on disk — "written before V42" — and never "a writer \
            forgot". Without it the corpus quietly stops being attributable again.
            """
        )
        #expect(
            rows[0].scenePhase == nil,
            """
            The store invented a scene phase. It cannot know one — it is a \
            persistence actor and awaiting UIKit inside `BEGIN IMMEDIATE` is not \
            an option — so the only honest value for a caller that supplied \
            none is NULL.
            """
        )
        #expect(rows[0].backfillJobId == nil)
    }

    @Test("V42: a caller-supplied createdAt wins over the store's clock")
    func callerSuppliedCreatedAtWins() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-own-clock"))

        try await store.insertSemanticScanResult(
            makeScan(
                id: "scan-own-clock",
                assetId: "asset-own-clock",
                start: 0,
                end: 60,
                latencyMs: 1_000,
                createdAt: 1_699_000_000
            ),
            now: 1_700_000_777
        )

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-own-clock")
        try #require(rows.count == 1)
        #expect(rows[0].createdAt == 1_699_000_000)
    }

    @Test("V42: all four scene phases round-trip through SQLite verbatim")
    func everyScenePhaseRoundTrips() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-phases"))

        for (index, phase) in ScanScenePhase.allCases.enumerated() {
            try await store.insertSemanticScanResult(
                makeScan(
                    id: "scan-\(phase.rawValue)",
                    assetId: "asset-phases",
                    start: Double(index) * 60,
                    end: Double(index) * 60 + 60,
                    latencyMs: 1_000,
                    createdAt: 1_700_000_000 + Double(index),
                    scenePhase: phase,
                    backfillJobId: "job-\(phase.rawValue)"
                )
            )
        }

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-phases")
        let byId = Dictionary(uniqueKeysWithValues: rows.map { ($0.id, $0) })
        for phase in ScanScenePhase.allCases {
            #expect(byId["scan-\(phase.rawValue)"]?.scenePhase == phase)
        }
    }

    /// The vocabulary is borrowed, not invented (playhead-9v09). If someone
    /// renames a case or "tidies" a raw value, a split over
    /// `semantic_scan_results` stops being comparable with one over
    /// `background_task_runs` — silently, because both sides still produce
    /// strings and both still group.
    @Test("V42: the scene-phase vocabulary is byte-identical to BGTaskTelemetryScenePhase's")
    func vocabularyMatchesBackgroundTaskRuns() {
        #expect(ScanScenePhase.active.rawValue == "active")
        #expect(ScanScenePhase.inactive.rawValue == "inactive")
        #expect(ScanScenePhase.background.rawValue == "background")
        #expect(ScanScenePhase.unknown.rawValue == "unknown")
        #expect(ScanScenePhase.allCases.count == 4)
    }

    /// A phase string a newer binary invented decodes to `nil`, not to a throw
    /// and not to a phase. Unattributable is the safe reading: an unrecognised
    /// phase is by definition not attributable to foreground or background.
    @Test("V42: an unrecognised persisted phase reads as unattributed, not as a phase")
    func unrecognisedPhaseReadsAsUnattributed() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset(id: "asset-future"))
        try await store.execForTesting("""
            INSERT INTO semantic_scan_results
                (id, analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
                 windowStartTime, windowEndTime, scanPass, transcriptQuality,
                 disposition, spansJSON, status, attemptCount, errorContext,
                 inputTokenCount, outputTokenCount, latencyMs, prewarmHit,
                 scanCohortJSON, transcriptVersion, reuseKeyHash, runMode, jobPhase,
                 createdAt, scenePhase, backfillJobId)
            VALUES
                ('scan-future', 'asset-future', 0, 5, 0.0, 60.0, 'passA', 'good',
                 'noAds', '[]', 'success', 1, NULL, NULL, NULL, 30000.0, 0,
                 '\(Self.cohort)', 'tv-1', 'future-reuse-key', 'shadow', 'shadow',
                 1700000000.0, 'suspendedUnderReview', 'job-future');
            """)

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-future")
        try #require(rows.count == 1)
        #expect(rows[0].scenePhase == nil)
        // The rest of the row still decodes — one unrecognised cosmetic field
        // must never abort a whole-asset read.
        #expect(rows[0].createdAt == 1_700_000_000)
        #expect(rows[0].backfillJobId == "job-future")

        let split = SemanticScanThroughputSplit.compute(rows: rows)
        #expect(split.unattributed.scanCount == 1)
        #expect(split.foreground.scanCount == 0)
    }

    // MARK: - THE NEGATIVE: unknown stays unknown

    /// **The one-line rail.** `bucket(for: nil)` is the routing decision the
    /// entire bead reduces to. Mutate it to `.foreground` and this goes red.
    @Test("NEGATIVE: a nil scene phase buckets as unattributed, never as a phase")
    func nilScenePhaseIsUnattributed() {
        #expect(SemanticScanThroughputSplit.bucket(for: nil) == .unattributed)
        #expect(SemanticScanThroughputSplit.bucket(for: nil) != .foreground)
        #expect(SemanticScanThroughputSplit.bucket(for: nil) != .background)
    }

    /// `.unknown` is a RECORDED non-answer. The platform was asked and declined,
    /// so it is not foreground and not background either.
    @Test("NEGATIVE: a recorded `.unknown` phase buckets as unattributed")
    func unknownScenePhaseIsUnattributed() {
        #expect(SemanticScanThroughputSplit.bucket(for: .unknown) == .unattributed)
        #expect(ScanScenePhase.unknown.attributionBucket == .unattributed)
    }

    /// `.inactive` IS foreground — a documented judgment, not an accident. The
    /// raw phase stays visible in `byScenePhase` so the judgment is auditable.
    @Test("`.inactive` is foreground, and stays separately visible in byScenePhase")
    func inactiveIsForegroundAndStaysVisible() {
        #expect(ScanScenePhase.active.attributionBucket == .foreground)
        #expect(ScanScenePhase.inactive.attributionBucket == .foreground)
        #expect(ScanScenePhase.background.attributionBucket == .background)

        let split = SemanticScanThroughputSplit.compute(rows: [
            makeScan(id: "a", assetId: "x", start: 0, end: 60, latencyMs: 60_000, scenePhase: .active),
            makeScan(id: "b", assetId: "x", start: 60, end: 120, latencyMs: 120_000, scenePhase: .inactive)
        ])
        #expect(split.foreground.scanCount == 2)
        #expect(split.byScenePhase[.active]?.scanCount == 1)
        #expect(split.byScenePhase[.inactive]?.scanCount == 1)
        #expect(
            split.byScenePhase[.inactive]?.wallSeconds == 120,
            "the raw phase must survive the roll-up so `.inactive -> foreground` can be re-litigated"
        )
    }

    /// **THE NEGATIVE, at corpus scale.**
    ///
    /// A mixed corpus: two rows nobody can attribute, one foreground, one
    /// background. Every unattributed row is slow (600 s of wall clock over 60 s
    /// of audio, a 10x ratio) and the attributed ones are fast, so folding the
    /// unattributed rows into `.foreground` does not merely change a count — it
    /// drags the foreground ratio from 0.5 to 6.7 and produces a confident,
    /// completely fabricated finding of exactly the kind playhead-kvs8 was sent
    /// to produce and honourably refused to.
    @Test("NEGATIVE: unattributed rows are counted apart and never poison the foreground ratio")
    func unattributedRowsNeverBecomeForeground() {
        let rows = [
            makeScan(id: "u1", assetId: "x", start: 0, end: 60, latencyMs: 600_000),
            makeScan(id: "u2", assetId: "x", start: 60, end: 120, latencyMs: 600_000),
            makeScan(id: "f1", assetId: "x", start: 120, end: 180, latencyMs: 30_000, scenePhase: .active),
            makeScan(id: "b1", assetId: "x", start: 180, end: 240, latencyMs: 120_000, scenePhase: .background)
        ]

        let split = SemanticScanThroughputSplit.compute(rows: rows)

        #expect(
            split.foreground.scanCount == 1,
            """
            The foreground bucket holds \(split.foreground.scanCount) rows; only \
            ONE row in this fixture recorded a foreground phase. Anything higher \
            means unattributed rows were folded in, which is the defect: the \
            numbers keep coming out and are simply wrong.
            """
        )
        #expect(split.background.scanCount == 1)
        #expect(split.unattributed.scanCount == 2)
        #expect(split.totalScanCount == 4)
        #expect(split.attributedFraction == 0.5)

        #expect(split.foreground.realtimeRatio == 0.5)
        #expect(split.background.realtimeRatio == 2.0)
        #expect(split.unattributed.realtimeRatio == 10.0)
        #expect(
            split.foreground.realtimeRatio != 6.75,
            "6.75 is the ratio the foreground bucket reports if the two unattributed rows are folded in"
        )

        // The raw-phase view agrees and shows no phantom phases.
        #expect(split.byScenePhase[.active]?.scanCount == 1)
        #expect(split.byScenePhase[.background]?.scanCount == 1)
        #expect(
            split.byScenePhase.values.reduce(0) { $0 + $1.scanCount } == 2,
            "a row with no recorded phase must key to no phase at all"
        )
    }

    /// The degenerate case a reader is most likely to be handed: a device
    /// upgraded yesterday, whose entire corpus predates V42. The honest report
    /// is "I cannot answer", expressed as an empty foreground bucket with a
    /// **nil** ratio — not a `1.0` sitting where a missing number belongs.
    @Test("NEGATIVE: an all-unattributed corpus yields no foreground measurement at all")
    func allUnattributedCorpusYieldsNoForegroundMeasurement() {
        let rows = (0..<5).map { index in
            makeScan(
                id: "legacy-\(index)",
                assetId: "x",
                start: Double(index) * 60,
                end: Double(index) * 60 + 60,
                latencyMs: 144_000
            )
        }

        let split = SemanticScanThroughputSplit.compute(rows: rows)

        #expect(split.foreground.scanCount == 0)
        #expect(split.background.scanCount == 0)
        #expect(
            split.foreground.realtimeRatio == nil,
            """
            The foreground bucket produced a ratio of \
            \(String(describing: split.foreground.realtimeRatio)) from a corpus \
            in which no row recorded a phase. An absent denominator must yield an \
            absent answer — a number here is a fabricated finding.
            """
        )
        #expect(split.unattributed.scanCount == 5)
        #expect(split.unattributed.realtimeRatio == 2.4)
        #expect(
            split.attributedFraction == 0,
            "attributedFraction is what tells a reader the split cannot be believed"
        )
    }

    @Test("an empty corpus reports nil, not zero and not one")
    func emptyCorpusReportsNil() {
        let split = SemanticScanThroughputSplit.compute(rows: [])
        #expect(split.attributedFraction == nil)
        #expect(split.foreground.realtimeRatio == nil)
        #expect(split.unattributed.realtimeRatio == nil)
        #expect(split.totalScanCount == 0)
    }

    // MARK: - Eligibility

    /// A `noWork:` sentinel spans a range it never examined (playhead-pz32).
    /// Counting its ~zero latency against a whole-episode window would report a
    /// model of spectacular speed that never ran — the same shape as the
    /// "148 free ad-seconds" that counted show as ad.
    @Test("throughput excludes no-work sentinels, failures and zero-width windows")
    func ineligibleRowsAreExcluded() {
        let rows = [
            makeScan(
                id: "sentinel",
                assetId: "x",
                start: 0,
                end: 1_800,
                latencyMs: 0,
                errorContext: "\(SemanticScanResult.noWorkSentinelErrorContextPrefix)emptySegments",
                scenePhase: .background
            ),
            makeScan(
                id: "failed",
                assetId: "x",
                start: 0,
                end: 60,
                latencyMs: 5_000,
                status: .failedTransient,
                scenePhase: .background
            ),
            makeScan(id: "no-latency", assetId: "x", start: 60, end: 120, latencyMs: nil, scenePhase: .background),
            makeScan(id: "zero-width", assetId: "x", start: 120, end: 120, latencyMs: 5_000, scenePhase: .background),
            makeScan(id: "good", assetId: "x", start: 180, end: 240, latencyMs: 120_000, scenePhase: .background)
        ]

        let split = SemanticScanThroughputSplit.compute(rows: rows)
        #expect(split.totalScanCount == 1)
        #expect(split.background.scanCount == 1)
        #expect(split.background.realtimeRatio == 2.0)
    }

    // MARK: - The documented SQL and the shipped consumer agree

    /// `docs/investigations/playhead-hx6n-scan-attribution.md` publishes a query
    /// for hand-analysis of a device pull. This requires it to compute what the
    /// shipped consumer computes, on one shared fixture, so the doc cannot rot
    /// into a query that quietly answers a different question.
    @Test("the SQL split and the Swift split agree on one fixture")
    func sqlAndSwiftSplitsAgree() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-agree"))

        let rows = [
            makeScan(id: "s-fg", assetId: "asset-agree", start: 0, end: 60,
                     latencyMs: 30_000, scenePhase: .active, backfillJobId: "j1"),
            makeScan(id: "s-fg2", assetId: "asset-agree", start: 60, end: 120,
                     latencyMs: 45_000, scenePhase: .inactive, backfillJobId: "j1"),
            makeScan(id: "s-bg", assetId: "asset-agree", start: 120, end: 180,
                     latencyMs: 150_000, scenePhase: .background, backfillJobId: "j2"),
            makeScan(id: "s-unk", assetId: "asset-agree", start: 180, end: 240,
                     latencyMs: 90_000, scenePhase: .unknown, backfillJobId: "j2"),
            makeScan(id: "s-null", assetId: "asset-agree", start: 240, end: 300, latencyMs: 600_000),
            makeScan(id: "s-sentinel", assetId: "asset-agree", start: 0, end: 1_800, latencyMs: 0,
                     errorContext: "\(SemanticScanResult.noWorkSentinelErrorContextPrefix)x",
                     scenePhase: .background)
        ]
        for row in rows {
            try await store.insertSemanticScanResult(row, now: 1_700_000_000)
        }

        let fromSQL = try await store.fetchSemanticScanThroughputSplit()
        let stored = try await store.fetchSemanticScanResults(analysisAssetId: "asset-agree")
        let fromSwift = SemanticScanThroughputSplit.compute(rows: stored)

        #expect(
            fromSQL == fromSwift,
            """
            The documented SQL and the shipped consumer disagree.
            SQL:   \(fromSQL)
            Swift: \(fromSwift)
            """
        )
        // …and the shared answer is the right one, so "they agree" is not two
        // implementations being wrong together.
        #expect(fromSQL.foreground.scanCount == 2)
        #expect(fromSQL.background.scanCount == 1)
        #expect(
            fromSQL.unattributed.scanCount == 2,
            "the `.unknown` row and the NULL row both land here, and they must be SUMMED, not overwritten"
        )
        #expect(fromSQL.foreground.realtimeRatio == 0.625)
        #expect(fromSQL.background.realtimeRatio == 2.5)
        #expect(fromSQL.attributedFraction == 0.6)
    }

    // MARK: - playhead-8ljj: what one granted window banked

    /// The numerator a background window's ledger row now carries. It is a
    /// count over `createdAt`, which is why it lives in this suite: the whole
    /// reason a window can be attributed at all is the V42 column these tests
    /// exist to guard.
    ///
    /// Three properties, and the second and third are the ones that matter:
    ///   * the boundary is INCLUSIVE, so a row written in the same instant the
    ///     grant opened belongs to that grant;
    ///   * a row written BEFORE the grant is not this window's output — a count
    ///     over the whole table would report every window as productive the
    ///     moment any window ever was;
    ///   * a NULL `createdAt` — a pre-V42 row — counts toward NO window. It is
    ///     unattributable by construction, and folding it into the newest
    ///     window is the misattribution this bead exists to stop.
    @Test("the banked count is scoped to one grant, and NULL belongs to no grant")
    func bankedCountIsScopedAndExcludesUnattributableRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-banked"))

        let grantOpened: Double = 1_700_000_000
        try await store.insertSemanticScanResult(
            makeScan(id: "s-before", assetId: "asset-banked", start: 0, end: 60,
                     latencyMs: 1, createdAt: grantOpened - 1),
            now: grantOpened
        )
        try await store.insertSemanticScanResult(
            makeScan(id: "s-at", assetId: "asset-banked", start: 60, end: 120,
                     latencyMs: 1, createdAt: grantOpened),
            now: grantOpened
        )
        try await store.insertSemanticScanResult(
            makeScan(id: "s-after", assetId: "asset-banked", start: 120, end: 180,
                     latencyMs: 1, createdAt: grantOpened + 30),
            now: grantOpened
        )
        try await store.insertSemanticScanResult(
            makeScan(id: "s-null", assetId: "asset-banked", start: 180, end: 240,
                     latencyMs: 1, createdAt: nil),
            now: grantOpened
        )
        // A genuinely NULL `createdAt` cannot be produced through the insert —
        // it falls back to the store clock (`result.createdAt ?? now`), which is
        // the V42 contract and is correct. The rows that DO read NULL are the
        // pre-V42 ones the migration deliberately left alone, so the fixture
        // reproduces that state directly.
        //
        // playhead-bg2n: `lastAttemptAt` is NULLed too, and that is not a
        // convenience — it is what a real pre-V42 row looks like AFTER V55. The
        // V55 backfill is `SET lastAttemptAt = createdAt WHERE createdAt IS NOT
        // NULL`, so a row with no `createdAt` gains no `lastAttemptAt` either;
        // an unattributable row stays unattributable in both columns. Nulling
        // only `createdAt` would have reproduced half of that state and made
        // this rail assert about a row the schema cannot produce.
        try await store.execForTesting(
            "UPDATE semantic_scan_results SET createdAt = NULL, lastAttemptAt = NULL WHERE id = 's-null'"
        )

        #expect(try await store.countSemanticScanResults(lastAttemptAtOrAfter: grantOpened) == 2,
                "inclusive at the boundary, and the earlier row is a DIFFERENT window's output")
        #expect(try await store.countSemanticScanResults(lastAttemptAtOrAfter: grantOpened + 1) == 1)
        #expect(try await store.countSemanticScanResults(lastAttemptAtOrAfter: grantOpened + 1_000) == 0,
                "a window that banked nothing must read zero, not the table's size")
        // The NULL row is in the table and reachable by every other reader; it
        // is simply not evidence about any window.
        #expect(try await store.fetchSemanticScanResults(analysisAssetId: "asset-banked").count == 4)
    }

    /// A persisted REFUSAL is durable output. `playhead-26od` banks each coarse
    /// window the moment it lands, whatever it concluded, and a later window
    /// resumes from it — so a success-only count would report a window that
    /// examined the episode and refused as barren, which is the opposite of the
    /// truth.
    @Test("the banked count includes non-success rows — a persisted refusal is still output")
    func bankedCountIncludesNonSuccessRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-refusal"))
        let grantOpened: Double = 1_700_000_000

        try await store.insertSemanticScanResult(
            makeScan(id: "s-fail", assetId: "asset-refusal", start: 0, end: 60,
                     latencyMs: nil, status: .refusal, errorContext: "refused",
                     createdAt: grantOpened + 1),
            now: grantOpened
        )

        #expect(try await store.countSemanticScanResults(lastAttemptAtOrAfter: grantOpened) == 1)
    }
}
