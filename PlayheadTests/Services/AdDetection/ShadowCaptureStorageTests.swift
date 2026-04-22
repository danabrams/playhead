// ShadowCaptureStorageTests.swift
// playhead-narl.2: Storage/migration tests for `shadow_fm_responses`.
//
// Coverage:
//   1. Fresh-DB migration creates the table with the expected columns.
//   2. upsert + fetch round-trips the row data (including the opaque BLOB).
//   3. Primary-key conflict: a second write for the same window replaces
//      the first (later write wins).
//   4. Malformed windows (end < start) are rejected at the bind boundary.
//   5. capturedShadowWindows returns the exact set of (start, end) pairs.
//   6. Migration is idempotent across reopens of the same DB.
//   7. Bulk upsert handles empty input and committed batches.
//   8. Count reflects persisted rows.

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
}
