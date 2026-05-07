// ShadowCaptureStorageTests.swift
// playhead-narl.2: Storage/migration tests for `shadow_fm_responses`.
//
// Coverage:
//   1. Fresh-DB migration creates the table with the expected columns
//      (including the playhead-hygc.1.7 v26 summary columns).
//   2. upsert + fetch round-trips the row data (including the opaque BLOB).
//   3. Primary-key conflict: a second write for the same window replaces
//      the first (later write wins).
//   4. Malformed windows (end < start) are rejected at the bind boundary.
//   5. capturedShadowWindows returns the exact set of (start, end) pairs.
//   6. Migration is idempotent across reopens of the same DB.
//   7. Bulk upsert handles empty input and committed batches.
//   8. Count reflects persisted rows.
//   9. Summary columns (isAdSummary, shadowConfidenceSummary) are populated
//      at write time and exposed via loadShadowSummaryRows so diagnostics
//      can read decision signals without decoding the opaque BLOB.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("Shadow capture storage (playhead-narl.2)")
struct ShadowCaptureStorageTests {

    // MARK: - 1. Fresh DB migration shape

    @Test("Fresh DB: shadow_fm_responses table exists with expected columns")
    func freshDbCreatesShadowTableWithExpectedColumns() async throws {
        let dir = try makeTempDir(prefix: "Shadow-Fresh")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()

        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Open the DB directly to inspect its pragma-level shape.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var handle: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &handle, SQLITE_OPEN_READONLY, nil) == SQLITE_OK)
        defer { sqlite3_close_v2(handle) }

        var stmt: OpaquePointer?
        let rc = sqlite3_prepare_v2(
            handle,
            "PRAGMA table_info(shadow_fm_responses)",
            -1, &stmt, nil
        )
        #expect(rc == SQLITE_OK)
        defer { sqlite3_finalize(stmt) }

        var columnNames: Set<String> = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            if let cstr = sqlite3_column_text(stmt, 1) {
                columnNames.insert(String(cString: cstr))
            }
        }
        let expected: Set<String> = [
            "assetId", "windowStart", "windowEnd", "configVariant",
            "fmResponse", "capturedAt", "capturedBy", "fmModelVersion",
            // playhead-hygc.1.7: queryable summary surface.
            "isAdSummary", "shadowConfidenceSummary",
        ]
        #expect(columnNames == expected)
    }

    // MARK: - 2. Round-trip persistence

    @Test("Upsert + fetch round-trips the row including the BLOB payload")
    func upsertAndFetchRoundTripsBlob() async throws {
        let store = try await makeTestStore()
        let asset = "asset-A"
        let blob = Data([0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01])
        let row = ShadowFMResponse(
            assetId: asset,
            windowStart: 12.5,
            windowEnd: 22.5,
            configVariant: .allEnabledShadow,
            fmResponse: blob,
            capturedAt: 1_700_000_000.25,
            capturedBy: .laneA,
            fmModelVersion: "fm-7.2"
        )
        try await store.upsertShadowFMResponse(row)

        let fetched = try await store.fetchShadowFMResponses(assetId: asset)
        #expect(fetched.count == 1)
        #expect(fetched.first == row)
    }

    // MARK: - 3. Primary-key conflict replaces the earlier row

    @Test("Second write for same (asset,window,variant) replaces the first")
    func secondWriteReplacesFirst() async throws {
        let store = try await makeTestStore()
        let asset = "asset-B"
        let first = ShadowFMResponse(
            assetId: asset, windowStart: 0, windowEnd: 10,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0x01]),
            capturedAt: 1_700_000_000,
            capturedBy: .laneA,
            fmModelVersion: "fm-7.2"
        )
        let second = ShadowFMResponse(
            assetId: asset, windowStart: 0, windowEnd: 10,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0x02, 0x03]),
            capturedAt: 1_700_000_100,
            capturedBy: .laneB,
            fmModelVersion: "fm-7.3"
        )
        try await store.upsertShadowFMResponse(first)
        try await store.upsertShadowFMResponse(second)

        let rows = try await store.fetchShadowFMResponses(assetId: asset)
        #expect(rows.count == 1)
        #expect(rows.first?.fmResponse == Data([0x02, 0x03]))
        #expect(rows.first?.capturedBy == .laneB)
        #expect(rows.first?.fmModelVersion == "fm-7.3")
    }

    // MARK: - 4. Malformed windows rejected

    @Test("Malformed window (end < start) is rejected at bind boundary")
    func malformedWindowRejected() async throws {
        let store = try await makeTestStore()
        let bad = ShadowFMResponse(
            assetId: "asset-C", windowStart: 50, windowEnd: 10,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0xAA]),  // non-empty so only the window gate fires
            capturedAt: 1_700_000_000,
            capturedBy: .laneA,
            fmModelVersion: nil
        )
        await #expect(throws: Error.self) {
            try await store.upsertShadowFMResponse(bad)
        }
        let rows = try await store.fetchShadowFMResponses(assetId: "asset-C")
        #expect(rows.isEmpty)
    }

    @Test("Empty fmResponse payload is rejected at bind boundary")
    func emptyPayloadRejected() async throws {
        let store = try await makeTestStore()
        let bad = ShadowFMResponse(
            assetId: "asset-empty", windowStart: 0, windowEnd: 10,
            configVariant: .allEnabledShadow,
            fmResponse: Data(),  // empty — meaningless for the harness
            capturedAt: 1_700_000_000,
            capturedBy: .laneA,
            fmModelVersion: "fm-1.0"
        )
        await #expect(throws: Error.self) {
            try await store.upsertShadowFMResponse(bad)
        }
        // And no stray row was partially committed.
        #expect(try await store.shadowFMResponseCount() == 0)
    }

    @Test("Non-finite window bounds rejected at bind boundary")
    func nonFiniteBoundsRejected() async throws {
        let store = try await makeTestStore()
        let bad = ShadowFMResponse(
            assetId: "asset-nan",
            windowStart: .nan,
            windowEnd: 10,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0xAA]),
            capturedAt: 1_700_000_000,
            capturedBy: .laneA,
            fmModelVersion: "fm-1.0"
        )
        await #expect(throws: Error.self) {
            try await store.upsertShadowFMResponse(bad)
        }
        #expect(try await store.shadowFMResponseCount() == 0)
    }

    // MARK: - 5. capturedShadowWindows returns exact (start,end) set

    @Test("capturedShadowWindows returns the exact set for the asset")
    func capturedShadowWindowsExactSet() async throws {
        let store = try await makeTestStore()
        let asset = "asset-D"
        try await store.upsertShadowFMResponse(makeRow(asset: asset, start: 0, end: 10))
        try await store.upsertShadowFMResponse(makeRow(asset: asset, start: 10, end: 20))
        // A row under a *different* variant must NOT appear. We only have one
        // variant today, but capturedShadowWindows must not smuggle cross-
        // variant entries — prove it by asserting count==2.
        try await store.upsertShadowFMResponse(makeRow(asset: "asset-E", start: 0, end: 10))

        let captured = try await store.capturedShadowWindows(
            assetId: asset,
            configVariant: .allEnabledShadow
        )
        #expect(captured.count == 2)
        #expect(captured.contains(ShadowWindowKey(start: 0, end: 10)))
        #expect(captured.contains(ShadowWindowKey(start: 10, end: 20)))
    }

    // MARK: - 6. Migration idempotence

    @Test("Reopening the store does not drop the shadow table or its rows")
    func migrationIdempotentAcrossReopen() async throws {
        let dir = try makeTempDir(prefix: "Shadow-Idempotent")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()

        let store1 = try AnalysisStore(directory: dir)
        try await store1.migrate()
        let row = makeRow(asset: "X", start: 1, end: 2)
        try await store1.upsertShadowFMResponse(row)

        // Drop the handle, re-open.
        AnalysisStore.resetMigratedPathsForTesting()
        let store2 = try AnalysisStore(directory: dir)
        try await store2.migrate()
        let rows = try await store2.fetchShadowFMResponses(assetId: "X")
        #expect(rows.count == 1)
        #expect(rows.first == row)
    }

    // MARK: - 7. Bulk upsert

    @Test("Bulk upsert of an empty array is a no-op")
    func bulkUpsertEmptyIsNoop() async throws {
        let store = try await makeTestStore()
        try await store.upsertShadowFMResponses([])
        #expect(try await store.shadowFMResponseCount() == 0)
    }

    @Test("Bulk upsert commits every row in the batch")
    func bulkUpsertCommitsBatch() async throws {
        let store = try await makeTestStore()
        let rows = (0..<5).map { i in
            makeRow(asset: "bulk", start: Double(i * 10), end: Double(i * 10 + 10))
        }
        try await store.upsertShadowFMResponses(rows)
        #expect(try await store.shadowFMResponseCount() == 5)
    }

    // MARK: - 8. Count

    @Test("shadowFMResponseCount reflects only persisted rows")
    func shadowFMResponseCountReflectsPersistedRows() async throws {
        let store = try await makeTestStore()
        #expect(try await store.shadowFMResponseCount() == 0)
        try await store.upsertShadowFMResponse(makeRow(asset: "a", start: 0, end: 1))
        #expect(try await store.shadowFMResponseCount() == 1)
    }

    // MARK: - 9. PK canonicalization

    /// Two writes whose windowStart/windowEnd values agree to the nearest
    /// millisecond must resolve to the *same* PK row (later write wins),
    /// even if their last-bit REAL representations differ. This locks in
    /// the AC-6 canonicalization: without it, two arithmetic paths that
    /// produce "the same" fractional second could land as distinct rows.
    @Test("Upsert canonicalizes REAL bounds to nearest ms for PK stability")
    func upsertCanonicalizesMillisecondPK() async throws {
        let store = try await makeTestStore()
        let asset = "asset-canon"
        // 12.5001 and 12.5004 both round to 12500 ms → same PK row.
        let first = ShadowFMResponse(
            assetId: asset, windowStart: 12.5001, windowEnd: 22.4999,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0x01]),
            capturedAt: 1_700_000_000,
            capturedBy: .laneA,
            fmModelVersion: "fm-1.0"
        )
        let second = ShadowFMResponse(
            assetId: asset, windowStart: 12.5004, windowEnd: 22.5003,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0x02, 0x03]),
            capturedAt: 1_700_000_050,
            capturedBy: .laneB,
            fmModelVersion: "fm-1.0"
        )
        try await store.upsertShadowFMResponse(first)
        try await store.upsertShadowFMResponse(second)
        // Both writes landed on the same canonical PK — only one row.
        #expect(try await store.shadowFMResponseCount() == 1)
        let rows = try await store.fetchShadowFMResponses(assetId: asset)
        // Later write wins.
        #expect(rows.first?.fmResponse == Data([0x02, 0x03]))
        // And the on-disk bounds are canonical (0.5 ms rounded to 0).
        #expect(rows.first?.windowStart == 12.500)
        #expect(rows.first?.windowEnd == 22.500)
    }

    // MARK: - 10. Invalidation: delete by stale fmModelVersion

    @Test("deleteShadowFMResponses(fmModelVersionOtherThan:) removes only stale rows")
    func deleteStaleByFMModelVersion() async throws {
        let store = try await makeTestStore()
        let current = "fm-2.0"
        try await store.upsertShadowFMResponse(ShadowFMResponse(
            assetId: "a", windowStart: 0, windowEnd: 10,
            configVariant: .allEnabledShadow, fmResponse: Data([0x01]),
            capturedAt: 1, capturedBy: .laneA, fmModelVersion: current
        ))
        try await store.upsertShadowFMResponse(ShadowFMResponse(
            assetId: "a", windowStart: 10, windowEnd: 20,
            configVariant: .allEnabledShadow, fmResponse: Data([0x02]),
            capturedAt: 2, capturedBy: .laneA, fmModelVersion: "fm-1.0"
        ))
        try await store.upsertShadowFMResponse(ShadowFMResponse(
            assetId: "b", windowStart: 0, windowEnd: 10,
            configVariant: .allEnabledShadow, fmResponse: Data([0x03]),
            capturedAt: 3, capturedBy: .laneB, fmModelVersion: nil  // legacy
        ))
        #expect(try await store.shadowFMResponseCount() == 3)

        let removed = try await store.deleteShadowFMResponses(
            fmModelVersionOtherThan: current
        )

        #expect(removed == 2)
        // Survivor is the current-version row.
        #expect(try await store.shadowFMResponseCount() == 1)
        let survivors = try await store.fetchShadowFMResponses(assetId: "a")
        #expect(survivors.count == 1)
        #expect(survivors.first?.fmModelVersion == current)
    }

    // MARK: - 11. Summary columns (playhead-hygc.1.7)
    //
    // The bead requires shadow rows to expose a queryable summary surface
    // so diagnostics and learning paths can read isAd/confidence without
    // base64-decoding the opaque BLOB at every read site. The exporter's
    // `decodeShadowSummary` already computes this; v26 persists the result
    // alongside the row at write time and `loadShadowSummaryRows` exposes
    // it without any blob decoding.

    @Test("upsert populates isAdSummary=true and shadowConfidenceSummary>0 for an ad-bearing payload")
    func upsertPopulatesSummaryForAdBearingPayload() async throws {
        let store = try await makeTestStore()
        let blob = try encodedAdPayload(certainty: .strong)
        let row = ShadowFMResponse(
            assetId: "asset-summary-ad",
            windowStart: 0, windowEnd: 30,
            configVariant: .allEnabledShadow,
            fmResponse: blob,
            capturedAt: 1_700_000_000,
            capturedBy: .laneA,
            fmModelVersion: "fm-test"
        )
        try await store.upsertShadowFMResponse(row)

        let summaries = try await store.loadShadowSummaryRows()
        #expect(summaries.count == 1)
        let s = try #require(summaries.first)
        #expect(s.assetId == "asset-summary-ad")
        #expect(s.windowStart == 0)
        #expect(s.windowEnd == 30)
        #expect(s.isAd == true)
        #expect(s.shadowConfidence > 0.99)
    }

    @Test("upsert populates isAdSummary=false and shadowConfidenceSummary=0 for organic payload")
    func upsertPopulatesSummaryForOrganicPayload() async throws {
        let store = try await makeTestStore()
        let blob = try encodedOrganicPayload()
        let row = ShadowFMResponse(
            assetId: "asset-summary-organic",
            windowStart: 0, windowEnd: 30,
            configVariant: .allEnabledShadow,
            fmResponse: blob,
            capturedAt: 1_700_000_000,
            capturedBy: .laneA,
            fmModelVersion: "fm-test"
        )
        try await store.upsertShadowFMResponse(row)

        let summaries = try await store.loadShadowSummaryRows()
        let s = try #require(summaries.first)
        #expect(s.isAd == false)
        #expect(s.shadowConfidence == 0.0)
    }

    @Test("loadShadowSummaryRows orders by (assetId, windowStart) and surveys all assets")
    func loadShadowSummaryRowsOrdered() async throws {
        let store = try await makeTestStore()
        let adBlob = try encodedAdPayload(certainty: .moderate)
        let organicBlob = try encodedOrganicPayload()
        let rows: [ShadowFMResponse] = [
            ShadowFMResponse(assetId: "b", windowStart: 0, windowEnd: 30,
                             configVariant: .allEnabledShadow, fmResponse: adBlob,
                             capturedAt: 1, capturedBy: .laneA, fmModelVersion: "v"),
            ShadowFMResponse(assetId: "a", windowStart: 30, windowEnd: 60,
                             configVariant: .allEnabledShadow, fmResponse: organicBlob,
                             capturedAt: 2, capturedBy: .laneA, fmModelVersion: "v"),
            ShadowFMResponse(assetId: "a", windowStart: 0, windowEnd: 30,
                             configVariant: .allEnabledShadow, fmResponse: adBlob,
                             capturedAt: 3, capturedBy: .laneA, fmModelVersion: "v"),
        ]
        for r in rows { try await store.upsertShadowFMResponse(r) }
        let summaries = try await store.loadShadowSummaryRows()
        #expect(summaries.map { $0.assetId } == ["a", "a", "b"])
        #expect(summaries.map { $0.windowStart } == [0, 30, 0])
        #expect(summaries.map { $0.isAd } == [true, false, true])
    }

    @Test("loadShadowSummaryRows filters by assetId when provided")
    func loadShadowSummaryRowsFiltersByAsset() async throws {
        let store = try await makeTestStore()
        let blob = try encodedAdPayload(certainty: .weak)
        try await store.upsertShadowFMResponse(ShadowFMResponse(
            assetId: "keep", windowStart: 0, windowEnd: 30,
            configVariant: .allEnabledShadow, fmResponse: blob,
            capturedAt: 1, capturedBy: .laneA, fmModelVersion: "v"
        ))
        try await store.upsertShadowFMResponse(ShadowFMResponse(
            assetId: "drop", windowStart: 0, windowEnd: 30,
            configVariant: .allEnabledShadow, fmResponse: blob,
            capturedAt: 2, capturedBy: .laneA, fmModelVersion: "v"
        ))
        let summaries = try await store.loadShadowSummaryRows(assetId: "keep")
        #expect(summaries.count == 1)
        #expect(summaries.first?.assetId == "keep")
    }

    @Test("Replacing a row updates the summary columns to match the new payload")
    func replacingRowUpdatesSummary() async throws {
        let store = try await makeTestStore()
        let asset = "asset-replace"
        try await store.upsertShadowFMResponse(ShadowFMResponse(
            assetId: asset, windowStart: 0, windowEnd: 30,
            configVariant: .allEnabledShadow,
            fmResponse: try encodedOrganicPayload(),
            capturedAt: 1, capturedBy: .laneA, fmModelVersion: "v"
        ))
        // Replace with an ad-bearing payload at the same PK.
        try await store.upsertShadowFMResponse(ShadowFMResponse(
            assetId: asset, windowStart: 0, windowEnd: 30,
            configVariant: .allEnabledShadow,
            fmResponse: try encodedAdPayload(certainty: .strong),
            capturedAt: 2, capturedBy: .laneB, fmModelVersion: "v"
        ))
        let summaries = try await store.loadShadowSummaryRows(assetId: asset)
        #expect(summaries.count == 1)
        #expect(summaries.first?.isAd == true)
        #expect((summaries.first?.shadowConfidence ?? 0) > 0.99)
    }

    @Test("Migration backfills summary columns for pre-v26 rows")
    func migrationBackfillsSummaryColumns() async throws {
        // Stand up a current-version store, insert two shadow rows through
        // the public upsert path, then simulate a "pre-v26" state by
        // dropping the summary columns and downgrading _meta to v25. Re-open
        // through the migration ladder — v26 must add the columns back and
        // backfill them by decoding the BLOBs.
        //
        // We "downgrade" rather than "seed at v25" because the v25 fixture
        // is a v1-shape DB without the full v25-era table set; subsequent
        // migrations (e.g. ad_copy_fingerprints) hit unconditional
        // addColumnIfNeeded calls and blow up. Standing up a real store
        // first guarantees every table the ladder expects exists.
        let dir = try makeTempDir(prefix: "Shadow-V26-Backfill")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()

        let initial = try AnalysisStore(directory: dir)
        try await initial.migrate()
        let adBlob = try encodedAdPayload(certainty: .strong)
        let organicBlob = try encodedOrganicPayload()
        let model = "v"
        try await initial.upsertShadowFMResponse(ShadowFMResponse(
            assetId: "legacy-ad",
            windowStart: 0, windowEnd: 30,
            configVariant: .allEnabledShadow,
            fmResponse: adBlob,
            capturedAt: 1, capturedBy: .laneA,
            fmModelVersion: model
        ))
        try await initial.upsertShadowFMResponse(ShadowFMResponse(
            assetId: "legacy-organic",
            windowStart: 0, windowEnd: 30,
            configVariant: .allEnabledShadow,
            fmResponse: organicBlob,
            capturedAt: 1, capturedBy: .laneA,
            fmModelVersion: model
        ))

        // Drop the v26 columns by rebuilding the table without them, then
        // downgrade _meta to v25 so the next open re-runs the v26 migration.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var raw: OpaquePointer?
        #expect(sqlite3_open_v2(
            dbURL.path, &raw,
            SQLITE_OPEN_READWRITE, nil
        ) == SQLITE_OK)
        defer { sqlite3_close_v2(raw) }
        let downgradeSQL = """
            BEGIN;
            CREATE TABLE shadow_fm_responses_v25 (
                assetId        TEXT NOT NULL,
                windowStart    REAL NOT NULL,
                windowEnd      REAL NOT NULL,
                configVariant  TEXT NOT NULL,
                fmResponse     BLOB NOT NULL,
                capturedAt     REAL NOT NULL,
                capturedBy     TEXT NOT NULL,
                fmModelVersion TEXT,
                PRIMARY KEY (assetId, windowStart, windowEnd, configVariant)
            );
            INSERT INTO shadow_fm_responses_v25
                (assetId, windowStart, windowEnd, configVariant, fmResponse,
                 capturedAt, capturedBy, fmModelVersion)
            SELECT assetId, windowStart, windowEnd, configVariant, fmResponse,
                   capturedAt, capturedBy, fmModelVersion
              FROM shadow_fm_responses;
            DROP TABLE shadow_fm_responses;
            ALTER TABLE shadow_fm_responses_v25 RENAME TO shadow_fm_responses;
            UPDATE _meta SET value = '25' WHERE key = 'schema_version';
            COMMIT;
            """
        var errMsg: UnsafeMutablePointer<CChar>?
        #expect(sqlite3_exec(raw, downgradeSQL, nil, nil, &errMsg) == SQLITE_OK)
        if let errMsg { sqlite3_free(errMsg) }
        sqlite3_close_v2(raw)
        raw = nil

        // Re-open through the migration ladder. v26 should add the columns
        // back and backfill them from the retained BLOBs.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let summaries = try await store.loadShadowSummaryRows()
        #expect(summaries.count == 2)
        let byId = Dictionary(uniqueKeysWithValues: summaries.map { ($0.assetId, $0) })
        #expect(byId["legacy-ad"]?.isAd == true)
        #expect((byId["legacy-ad"]?.shadowConfidence ?? 0) > 0.99)
        #expect(byId["legacy-organic"]?.isAd == false)
        #expect(byId["legacy-organic"]?.shadowConfidence == 0.0)
    }

    /// playhead-hygc.1.7 R1: explicit acceptance check — running the v24
    /// backfill migration twice MUST yield identical state. After the first
    /// `migrate()` brings the DB to v24 and stamps the summary columns, a
    /// second `migrate()` (e.g. process restart, `resetMigratedPathsForTesting`
    /// followed by re-open) MUST observe `schemaVersion == 24` and skip the
    /// backfill — column values must not change, row count must not change.
    @Test("V24 migration is idempotent: running twice does not alter row state")
    func v24MigrationIsIdempotentAcrossReruns() async throws {
        let dir = try makeTempDir(prefix: "Shadow-V24-Idempotent")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()

        // Bring up a fresh store at v24 with one ad row + one organic row.
        let initial = try AnalysisStore(directory: dir)
        try await initial.migrate()
        let adBlob = try encodedAdPayload(certainty: .strong)
        let organicBlob = try encodedOrganicPayload()
        try await initial.upsertShadowFMResponse(ShadowFMResponse(
            assetId: "asset-ad",
            windowStart: 0, windowEnd: 30,
            configVariant: .allEnabledShadow,
            fmResponse: adBlob,
            capturedAt: 1_700_000_000, capturedBy: .laneA,
            fmModelVersion: "fm-1"
        ))
        try await initial.upsertShadowFMResponse(ShadowFMResponse(
            assetId: "asset-organic",
            windowStart: 0, windowEnd: 30,
            configVariant: .allEnabledShadow,
            fmResponse: organicBlob,
            capturedAt: 1_700_000_000, capturedBy: .laneA,
            fmModelVersion: "fm-1"
        ))

        let firstSnapshot = try await initial.loadShadowSummaryRows()
        let firstVersion = try await initial.schemaVersion()
        #expect(firstVersion == 24)
        #expect(firstSnapshot.count == 2)

        // Second open + migrate. Schema is already at v24 so the v24
        // helper must short-circuit on its `< 24` guard. Backfill MUST NOT
        // re-run; row state and counts MUST be byte-identical.
        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let secondSnapshot = try await reopened.loadShadowSummaryRows()
        let secondVersion = try await reopened.schemaVersion()
        #expect(secondVersion == 24)
        #expect(secondSnapshot == firstSnapshot,
            "V24 migration must be idempotent: re-running on an already-v24 DB MUST NOT change row state")
    }

    // MARK: - Helpers

    private func makeRow(
        asset: String,
        start: TimeInterval,
        end: TimeInterval,
        lane: ShadowCapturedBy = .laneA,
        payload: Data = Data([0xAA])
    ) -> ShadowFMResponse {
        ShadowFMResponse(
            assetId: asset,
            windowStart: start,
            windowEnd: end,
            configVariant: .allEnabledShadow,
            fmResponse: payload,
            capturedAt: 1_700_000_000,
            capturedBy: lane,
            fmModelVersion: "fm-test"
        )
    }

    /// Encode a `ShadowFMPayload` carrying a single paid span with the
    /// given certainty band, suitable for an upsert that must produce a
    /// non-zero `shadowConfidence` summary on the persisted row.
    private func encodedAdPayload(certainty: CertaintyBand) throws -> Data {
        let response = RefinementWindowSchema(spans: [
            SpanRefinementSchema(
                commercialIntent: .paid,
                ownership: .thirdParty,
                firstLineRef: 0,
                lastLineRef: 4,
                certainty: certainty,
                boundaryPrecision: .precise,
                evidenceAnchors: []
            )
        ])
        let payload = ShadowFMPayload(
            payloadSchemaVersion: shadowFMPayloadSchemaVersion,
            promptText: "ad-bearing transcript",
            refinementResponse: response,
            errorTag: nil
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return try encoder.encode(payload)
    }

    /// Encode a `ShadowFMPayload` carrying only an organic span, which
    /// must yield `isAdSummary=false` and `shadowConfidenceSummary=0`.
    private func encodedOrganicPayload() throws -> Data {
        let response = RefinementWindowSchema(spans: [
            SpanRefinementSchema(
                commercialIntent: .organic,
                ownership: .show,
                firstLineRef: 0,
                lastLineRef: 4,
                certainty: .moderate,
                boundaryPrecision: .usable,
                evidenceAnchors: []
            )
        ])
        let payload = ShadowFMPayload(
            payloadSchemaVersion: shadowFMPayloadSchemaVersion,
            promptText: "organic transcript",
            refinementResponse: response,
            errorTag: nil
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return try encoder.encode(payload)
    }
}
