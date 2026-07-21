// AdWindowEdgeAnchorsV29MigrationTests.swift
// playhead-hdgk: pin the V29 migration that adds the per-edge auto-skip
// anchor tier columns (`startEdgeAnchor` / `endEdgeAnchor`) to `ad_windows`,
// plus the AdWindow insert/fetch round-trip that carries them.
//
// Coverage targets:
//   1. Fresh-DB migrate() reaches head (v29) with both columns present.
//   2. `currentSchemaVersion` is exactly 29 (drift guard).
//   3. A v28-shaped `ad_windows` (no anchor columns) upgrades in place: the
//      columns are added, an existing pre-hdgk row survives with its other
//      fields intact and its anchors defaulted to 'unanchored' (no data loss).
//   4. The migration is idempotent.
//   5. CRUD round-trip: insertAdWindow → fetchAdWindow / fetchAdWindows
//      preserves a NON-default anchor pair exactly, and the struct default is
//      'unanchored' on both edges.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("ad_windows edge-anchor V29 migration + round-trip (playhead-hdgk)")
struct AdWindowEdgeAnchorsV29MigrationTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "AdWindowEdgeAnchorsV29")
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

    // MARK: - Migration ladder

    @Test("fresh DB migrate() lands both edge-anchor columns at head")
    func freshDbHasV29Columns() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // Drift guard: head moved 29 → 30 (playhead-gy2s analysis_jobs
        // reject-advisory columns) → 31 (playhead-b6jq specialist_scan_results);
        // the edge-anchor columns probed below are unchanged.
        #expect(AnalysisStore.currentSchemaVersion == 31)
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "startEdgeAnchor"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "endEdgeAnchor"))
    }

    @Test("v28-shaped ad_windows upgrades in place: columns added, pre-hdgk row survives defaulted to unanchored")
    func seededV28RowUpgradesWithoutDataLoss() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Bootstrap the full head shape, then regress `ad_windows` to its
        // pre-hdgk (v28) shape and rewind `_meta.schema_version` to 28. A row
        // is seeded through the v28-shaped table so the upgrade path has real
        // data to preserve.
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        try await bootstrap.insertAsset(makeAsset(id: "asset-up"))

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        // Drop and recreate ad_windows WITHOUT the anchor columns, in the
        // EXACT physical column order a real upgraded v28 DB carries: the
        // model/policy/feature-schema version columns (playhead-7mq) are
        // ALTER-appended in the ladder BEFORE the epfk catalog column, so on a
        // real device catalog sits at index 23 (not the fresh-CREATE index 20),
        // and the V29 anchor columns will append at 24/25 (not the fresh 21/22).
        // Modelling that here is the whole point: it forces the readers to
        // resolve catalog + anchors by NAME, not by fixed position — a fixed
        // positional read would silently pass a fresh-shaped fixture.
        let regress = """
            DROP TABLE IF EXISTS ad_windows;
            CREATE TABLE ad_windows (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                startTime           REAL NOT NULL,
                endTime             REAL NOT NULL,
                confidence          REAL NOT NULL,
                boundaryState       TEXT NOT NULL,
                decisionState       TEXT NOT NULL DEFAULT 'candidate',
                detectorVersion     TEXT NOT NULL,
                advertiser          TEXT,
                product             TEXT,
                adDescription       TEXT,
                evidenceText        TEXT,
                evidenceStartTime   REAL,
                metadataSource      TEXT NOT NULL DEFAULT 'none',
                metadataConfidence  REAL,
                metadataPromptVersion TEXT,
                wasSkipped          INTEGER NOT NULL DEFAULT 0,
                userDismissedBanner INTEGER NOT NULL DEFAULT 0,
                evidenceSources     TEXT,
                eligibilityGate     TEXT,
                model_version           TEXT NOT NULL DEFAULT 'pre-instrumentation',
                policy_version          INTEGER NOT NULL DEFAULT 0,
                feature_schema_version  INTEGER NOT NULL DEFAULT 0,
                catalogStoreMatchSimilarity REAL
            );
            CREATE INDEX IF NOT EXISTS idx_ad_asset ON ad_windows(analysisAssetId);
            INSERT INTO ad_windows
                (id, analysisAssetId, startTime, endTime, confidence,
                 boundaryState, decisionState, detectorVersion, metadataSource,
                 wasSkipped, userDismissedBanner, eligibilityGate,
                 catalogStoreMatchSimilarity)
            VALUES
                ('win-up', 'asset-up', 60.0, 120.0, 0.9, 'lexical', 'confirmed',
                 'v1', 'none', 0, 0, 'eligible', 0.42);
            UPDATE _meta SET value = '28' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, regress, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        // Sanity: the regressed table genuinely lacks the anchor columns.
        #expect(!(try probeColumnExists(in: dir, table: "ad_windows", column: "startEdgeAnchor")))
        #expect(!(try probeColumnExists(in: dir, table: "ad_windows", column: "endEdgeAnchor")))

        // Re-open and migrate: the V28→V29 step must add both columns.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "startEdgeAnchor"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "endEdgeAnchor"))

        // The pre-hdgk row survives with its data intact and its anchors
        // backfilled to the 'unanchored' default (no data loss). Because the
        // fixture places catalog + anchors at their real UPGRADED indices
        // (23 / 24 / 25, not the fresh 20 / 21 / 22), these assertions only
        // hold if the readers resolve those columns by NAME — a fixed
        // positional read would return the version-column values instead.
        let row = try await store.fetchAdWindow(id: "win-up")
        try #require(row != nil)
        #expect(row?.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(row?.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        // catalog read by name survives the divergent upgrade layout.
        #expect(row?.catalogStoreMatchSimilarity == 0.42)
        #expect(row?.startTime == 60.0)
        #expect(row?.endTime == 120.0)
        #expect(row?.confidence == 0.9)
        #expect(row?.eligibilityGate == "eligible")
        #expect(row?.decisionState == "confirmed")

        // Same row via the list reader (fetchAdWindows), which resolves the
        // by-name indices once before the row loop.
        let rows = try await store.fetchAdWindows(assetId: "asset-up")
        #expect(rows.first?.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(rows.first?.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(rows.first?.catalogStoreMatchSimilarity == 0.42)
    }

    @Test("V29 migration is idempotent across resetMigratedPathsForTesting")
    func v29MigrationIsIdempotent() async throws {
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
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "startEdgeAnchor"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "endEdgeAnchor"))
    }

    @Test("isolated ladder (migrateOnlyForTesting) reaches v29 and adds the columns")
    func isolatedLadderReachesV29() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        // createTables() builds ad_windows in its final v29 shape; then the
        // ladder seam runs the V*IfNeeded chain (the columns already exist, so
        // addColumnIfNeeded no-ops but setSchemaVersion(29) still bumps).
        try await store.migrate()
        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "startEdgeAnchor"))
        #expect(try probeColumnExists(in: dir, table: "ad_windows", column: "endEdgeAnchor"))
    }

    // MARK: - CRUD round-trip

    @Test("insert → fetchAdWindow round-trips a NON-default anchor pair exactly")
    func fetchAdWindowRoundTrip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-rt"))

        try await store.insertAdWindow(makeSkipTestAdWindow(
            id: "win-rt", assetId: "asset-rt",
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.stingerSnapped.rawValue
        ))

        let fetched = try await store.fetchAdWindow(id: "win-rt")
        #expect(fetched?.startEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue)
        #expect(fetched?.endEdgeAnchor == AutoSkipEdgeAnchor.stingerSnapped.rawValue)
    }

    @Test("insert → fetchAdWindows round-trips anchors; struct default is unanchored on both edges")
    func fetchAdWindowsRoundTripAndDefault() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-rt2"))

        // One row with an explicit mixed anchor pair, one with the defaults.
        try await store.insertAdWindow(makeSkipTestAdWindow(
            id: "win-anchored", assetId: "asset-rt2", startTime: 10, endTime: 40,
            startEdgeAnchor: AutoSkipEdgeAnchor.stingerSnapped.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        ))
        try await store.insertAdWindow(makeSkipTestAdWindow(
            id: "win-default", assetId: "asset-rt2", startTime: 50, endTime: 80
        ))

        let rows = try await store.fetchAdWindows(assetId: "asset-rt2")
        let byId = Dictionary(uniqueKeysWithValues: rows.map { ($0.id, $0) })
        #expect(byId["win-anchored"]?.startEdgeAnchor == AutoSkipEdgeAnchor.stingerSnapped.rawValue)
        #expect(byId["win-anchored"]?.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        // The struct default (used by non-fusion producers) is unanchored.
        #expect(byId["win-default"]?.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(byId["win-default"]?.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
    }
}
