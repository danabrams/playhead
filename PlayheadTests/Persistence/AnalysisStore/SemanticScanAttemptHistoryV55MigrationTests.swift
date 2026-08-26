// SemanticScanAttemptHistoryV55MigrationTests.swift
// playhead-bg2n: pin the V55 change that lets a `semantic_scan_results` row say
// THAT THERE WERE OTHER ATTEMPTS AND THEY DIFFERED.
//
// WHAT WENT WRONG, so the rails below read as answers to a question.
// Under `INSERT OR REPLACE`, `status` / `latencyMs` / `createdAt` were
// last-write-wins, so one row recorded only its most recent attempt while
// presenting a column named `createdAt` that looks like provenance. Measured
// across four capture generations of one device, the witness is
// `scan-24f9deacdb0e3ab6` (asset `AA6CD430`, head window `[0.0, 42.9]`), id
// stable in every capture:
//
//     status=decodingFailure        attempts=5   createdAt=08-14 04:57:50
//     status=decodingFailure        attempts=9   createdAt=08-15 04:47:13
//     status=exceededContextWindow  attempts=11  createdAt=08-15 06:17:14
//
// `createdAt` moved three times for a row created once. playhead-hzpa was filed
// on the last of those states stating "eleven attempts, every one
// `exceededContextWindow`" — the premise of a P1 was wrong in the direction the
// schema made wrong, and the 5th and 9th attempts ended in `decodingFailure`, a
// status with a DIFFERENT retry policy.
//
// The directions this file has to cover, because closing one leaves the defect
// alive in the others:
//
//   1. `createdAt` STOPS MOVING. A COALESCE in the bind. If it regresses, the
//      column is a last-written timestamp under a name that says creation, and
//      the longer a window has been stuck the newer it looks.
//   2. THE ROW REMEMBERS THAT THE ATTEMPTS DIFFERED. `observedStatuses` is a
//      monotone UNION. A row upserted twice with different statuses must not
//      read as two of the second one — the bead's own acceptance criterion.
//   3. `lastAttemptAt` CARRIES THE QUANTITY `createdAt` USED TO. Without it,
//      freezing `createdAt` silently under-counts what a background grant
//      banked, because `countSemanticScanResults` was reading it.
//   4. HISTORY DOES NOT LIE ABOUT ITS OWN COMPLETENESS. `firstAttemptAt` is
//      NULL on every pre-V55 row and stays NULL forever on that row, so
//      `attemptsDiffered` answers `nil` — "not established" — rather than
//      `false`. Reading `nil` as `false` reproduces this bead exactly.
//   5. THE SUCCESS PATH IS NOT EXEMPT. A `.success` replacing a failure used to
//      reset `attemptCount` to the caller's value, erasing every earlier
//      attempt. Reachable in production (a same-window retry hashes to the same
//      `reuseKeyHash`) though not observed on any preserved capture.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("semantic_scan_results remembers that its attempts differed (playhead-bg2n)")
struct SemanticScanAttemptHistoryV55MigrationTests {

    private static let cohort = """
        {"promptLabel":"l","promptHash":"p","schemaHash":"s","scanPlanHash":"sp",\
        "normalizationHash":"n","osBuild":"26A","locale":"en_US","appBuild":"1"}
        """

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "SemanticScanAttemptHistoryV55")
    }

    // MARK: - Raw-column probes
    //
    // Deliberately NOT routed through `AnalysisStore` for the disk claims. The
    // claim is about what is ON DISK; asking the store would ask the same read
    // whose correctness is under test, and a matched pair of bugs in the bind
    // and the read would agree with each other perfectly.

    private func withReadOnlyHandle<T>(in directory: URL, _ body: (OpaquePointer?) throws -> T) throws -> T {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "withReadOnlyHandle", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        return try body(db)
    }

    /// One row's raw `(createdAt, firstAttemptAt, lastAttemptAt, observedStatuses, status, attemptCount)`.
    /// Doubles are `.some(nil)` for SQL NULL.
    private struct RawRow {
        var createdAt: Double?
        var firstAttemptAt: Double?
        var lastAttemptAt: Double?
        var observedStatuses: String?
        var status: String
        var attemptCount: Int
    }

    private func rawRow(in directory: URL, rowId: String) throws -> RawRow? {
        try withReadOnlyHandle(in: directory) { db in
            var stmt: OpaquePointer?
            let sql = """
                SELECT createdAt, firstAttemptAt, lastAttemptAt, observedStatuses, status, attemptCount
                FROM semantic_scan_results WHERE id = ?
                """
            guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
                throw NSError(domain: "rawRow", code: 1)
            }
            defer { sqlite3_finalize(stmt) }
            sqlite3_bind_text(stmt, 1, rowId, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
            guard sqlite3_step(stmt) == SQLITE_ROW else { return RawRow?.none }
            func optDouble(_ index: Int32) -> Double? {
                sqlite3_column_type(stmt, index) == SQLITE_NULL ? nil : sqlite3_column_double(stmt, index)
            }
            func optText(_ index: Int32) -> String? {
                guard sqlite3_column_type(stmt, index) != SQLITE_NULL,
                      let raw = sqlite3_column_text(stmt, index) else { return nil }
                return String(cString: raw)
            }
            return RawRow(
                createdAt: optDouble(0),
                firstAttemptAt: optDouble(1),
                lastAttemptAt: optDouble(2),
                observedStatuses: optText(3),
                status: optText(4) ?? "",
                attemptCount: Int(sqlite3_column_int(stmt, 5))
            )
        }
    }

    /// Rewind a migrated store to the V54 SHAPE, not merely the V54 version
    /// stamp — the sibling V52 suite's rule, and for its reason: a rewind that
    /// only touches `_meta` proves nothing, because the rung would then run
    /// against columns that already exist and the work it is being tested for
    /// would be indistinguishable from a no-op.
    ///
    /// Preferred to hand-authoring a V54 `CREATE TABLE`: a hand-written fixture
    /// drifts from production's real shape the moment any other column moves,
    /// and a migration test that runs against a table production never had is a
    /// rail pinning a config production does not use.
    ///
    /// Pinned to the LITERAL 54 — "pre-bg2n" is a fixed historical fact, and
    /// `currentSchemaVersion - 1` would stop meaning it the moment head moves.
    private func rewindToV54(_ store: AnalysisStore) async throws {
        try await store.execForTesting("DROP INDEX IF EXISTS idx_semantic_scan_results_lastAttemptAt")
        for column in ["firstAttemptAt", "lastAttemptAt", "observedStatuses"] {
            try await store.execForTesting("ALTER TABLE semantic_scan_results DROP COLUMN \(column)")
        }
        try await store.setMetaValue(forKey: "schema_version", value: "54")
    }

    // MARK: - Fixtures

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    /// The witness's geometry, so every row below collides on `reuseKeyHash`
    /// exactly as the device's repeated attempts on `[0.0, 42.9]` did.
    private func attempt(
        id: String,
        assetId: String,
        status: SemanticScanStatus,
        attemptCount: Int = 1,
        latencyMs: Double? = nil,
        createdAt: Double? = nil
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 10,
            windowStartTime: 0,
            windowEndTime: 42.9,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: status,
            attemptCount: attemptCount,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: latencyMs,
            scanCohortJSON: Self.cohort,
            transcriptVersion: "tv-1",
            createdAt: createdAt
        )
    }

    // MARK: - 1. `createdAt` stops moving

    @Test("the witness shape: createdAt is FROZEN across three differing attempts")
    func createdAtDoesNotMoveAcrossUpserts() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-w"))

        // The device's own three states, in order. The ids differ exactly as
        // they would NOT on device — `id` is REPLACEd along with everything
        // else, so using distinct ids proves the collision is on
        // `reuseKeyHash` and not on the primary key.
        let t5 = 1_755_147_470.0   // 08-14 04:57:50
        let t9 = 1_755_233_233.0   // 08-15 04:47:13
        let t11 = 1_755_238_634.0  // 08-15 06:17:14
        try await store.insertSemanticScanResult(
            attempt(id: "scan-a5", assetId: "asset-w", status: .decodingFailure, attemptCount: 5, latencyMs: 6_747.4, createdAt: t5)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-a9", assetId: "asset-w", status: .decodingFailure, attemptCount: 9, latencyMs: 19_413.8, createdAt: t9)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-a11", assetId: "asset-w", status: .exceededContextWindow, attemptCount: 11, latencyMs: 8_213.7, createdAt: t11)
        )

        let raw = try #require(try rawRow(in: dir, rowId: "scan-a11"))
        // THE regression this file exists for. Before V55 this read `t11`.
        #expect(raw.createdAt == t5, "createdAt must stay at the FIRST write; it moved three times on device")
        #expect(raw.lastAttemptAt == t11, "lastAttemptAt carries the quantity createdAt used to")
        #expect(raw.firstAttemptAt == t5, "a row first written by this binary knows its own first attempt")
        // `attemptCount` is `max(existing + 1, incoming)`, so eleven stands.
        #expect(raw.attemptCount == 11)
    }

    @Test("a stuck window reports its true AGE, not the age of its newest attempt")
    func attemptSpanIsMeasuredFromTheFirstAttempt() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-age"))

        let first = 1_755_060_000.0
        let last = first + 127_200 // 35 h 20 m — the largest jump measured on device
        try await store.insertSemanticScanResult(
            attempt(id: "scan-age", assetId: "asset-age", status: .noAds, attemptCount: 3, createdAt: first)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-age", assetId: "asset-age", status: .noAds, attemptCount: 12, createdAt: last)
        )

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-age")
        let row = try #require(rows.first)
        // Before V55 both endpoints read `last` and the span was 0 — a window
        // stuck for a day and a half reporting as brand new.
        #expect(row.attemptSpanSeconds == 127_200)
        #expect(row.historyIsComplete)
    }

    // MARK: - 2. The row remembers that the attempts differed

    @Test("a row upserted twice with different statuses does not read as two of the second")
    func observedStatusesIsAMonotoneUnion() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-u"))

        try await store.insertSemanticScanResult(
            attempt(id: "scan-u", assetId: "asset-u", status: .decodingFailure, createdAt: 100)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-u", assetId: "asset-u", status: .exceededContextWindow, createdAt: 200)
        )

        let raw = try #require(try rawRow(in: dir, rowId: "scan-u"))
        #expect(raw.status == SemanticScanStatus.exceededContextWindow.rawValue, "status is still the LAST attempt")
        // Sorted, comma-joined, deduplicated — and CONTAINING the status that
        // hzpa's sentence said never happened.
        #expect(raw.observedStatuses == "decodingFailure,exceededContextWindow")

        let row = try #require(try await store.fetchSemanticScanResults(analysisAssetId: "asset-u").first)
        #expect(row.observedStatuses == [.decodingFailure, .exceededContextWindow])
        #expect(row.attemptsDiffered == true, "the sentence 'eleven attempts, every one exceededContextWindow' is now untypeable")
    }

    @Test("the set is SORTED on disk, so two pulls that saw the same statuses compare equal")
    func observedStatusesIsOrderIndependent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-s1"))
        try await store.insertAsset(makeAsset(id: "asset-s2"))

        // Same two statuses, opposite order.
        try await store.insertSemanticScanResult(attempt(id: "s1", assetId: "asset-s1", status: .cancelled, createdAt: 1))
        try await store.insertSemanticScanResult(attempt(id: "s1", assetId: "asset-s1", status: .noAds, createdAt: 2))
        try await store.insertSemanticScanResult(attempt(id: "s2", assetId: "asset-s2", status: .noAds, createdAt: 1))
        try await store.insertSemanticScanResult(attempt(id: "s2", assetId: "asset-s2", status: .cancelled, createdAt: 2))

        let a = try #require(try rawRow(in: dir, rowId: "s1")).observedStatuses
        let b = try #require(try rawRow(in: dir, rowId: "s2")).observedStatuses
        #expect(a == b, "an unsorted encoding would report churn between two pulls that saw the same thing")
        #expect(a == "cancelled,no_ads")
    }

    @Test("a repeat of the SAME status does not grow the set")
    func repeatedStatusDoesNotDuplicate() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-r"))

        for i in 1...5 {
            try await store.insertSemanticScanResult(
                attempt(id: "scan-r", assetId: "asset-r", status: .decodingFailure, createdAt: Double(i))
            )
        }
        let raw = try #require(try rawRow(in: dir, rowId: "scan-r"))
        #expect(raw.observedStatuses == "decodingFailure", "the column is bounded by the ENUM, not by the retry count")
        #expect(raw.attemptCount == 5)

        let row = try #require(try await store.fetchSemanticScanResults(analysisAssetId: "asset-r").first)
        #expect(row.attemptsDiffered == false, "five identical attempts DID happen and they were alike — that is a real 'no', not 'unknown'")
    }

    // MARK: - 3. `lastAttemptAt` carries what `createdAt` used to

    @Test("countSemanticScanResults counts WRITES, so a re-attempt in this grant is banked")
    func countIsOverLastAttemptNotCreation() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-c"))

        let dayOne = 1_000.0
        let grantOpened = 9_000.0
        try await store.insertSemanticScanResult(
            attempt(id: "scan-c", assetId: "asset-c", status: .decodingFailure, createdAt: dayOne)
        )
        #expect(try await store.countSemanticScanResults(lastAttemptAtOrAfter: grantOpened) == 0)

        // The same window, re-attempted inside this grant. It is work this grant
        // did; counting by CREATION would book it to a grant days earlier and
        // report `banked=0` for a window that was really written.
        try await store.insertSemanticScanResult(
            attempt(id: "scan-c", assetId: "asset-c", status: .exceededContextWindow, createdAt: grantOpened + 5)
        )
        #expect(try await store.countSemanticScanResults(lastAttemptAtOrAfter: grantOpened) == 1)
        // …and `createdAt` is still day one, which is the point of the split.
        let raw = try #require(try rawRow(in: dir, rowId: "scan-c"))
        #expect(raw.createdAt == dayOne)
    }

    // MARK: - 4. History does not lie about its own completeness

    @Test("V55 upgrade: lastAttemptAt is COPIED, observedStatuses is SEEDED, firstAttemptAt stays NULL")
    func upgradeClaimsOnlyWhatItCanProve() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let seeded = try AnalysisStore(directory: dir)
        try await seeded.migrate()
        try await seeded.insertAsset(makeAsset(id: "asset-up"))
        try await seeded.insertSemanticScanResult(
            attempt(id: "scan-up", assetId: "asset-up", status: .exceededContextWindow, attemptCount: 11, createdAt: 5_000)
        )
        try await rewindToV54(seeded)

        AnalysisStore.resetMigratedPathsForTesting()
        let upgraded = try AnalysisStore(directory: dir)
        try await upgraded.migrate()
        // playhead-6gcy: a store rewound to V54 climbs to HEAD, not to 55 —
        // the V56 rung runs in the same `migrate()`. What this test pins is
        // the V55 rung's EFFECTS, asserted on the raw columns below; the
        // stamp only has to prove the ladder ran to completion.
        #expect(try await upgraded.schemaVersion() == AnalysisStore.currentSchemaVersion)

        let raw = try #require(try rawRow(in: dir, rowId: "scan-up"))
        // A RENAME, not a claim: every stored `createdAt` IS the last write.
        #expect(raw.lastAttemptAt == 5_000)
        // A TRUE statement: that status was observed. A lower bound, not the history.
        #expect(raw.observedStatuses == "exceededContextWindow")
        // A REFUSAL. The first-attempt time was destroyed by the upserts, and
        // `attemptCount` cannot stand in for it — until this same change a
        // `.success` replacing a failure reset the counter to the caller's
        // value, so `attemptCount == 1` was never proof of a single write.
        #expect(raw.firstAttemptAt == nil, "history starts now; a backfilled value here would be manufactured")

        let row = try #require(try await upgraded.fetchSemanticScanResults(analysisAssetId: "asset-up").first)
        #expect(row.historyIsComplete == false)
        // THREE-VALUED. Reading this `nil` as `false` reproduces the bead.
        #expect(row.attemptsDiffered == nil, "one surviving status on a partial record is 'not established', never 'they were all alike'")
        #expect(row.attemptSpanSeconds == nil, "an unknown first attempt cannot yield a span")
    }

    @Test("a pre-V55 row's incompleteness SURVIVES later attempts")
    func partialHistoryStaysPartial() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let seeded = try AnalysisStore(directory: dir)
        try await seeded.migrate()
        try await seeded.insertAsset(makeAsset(id: "asset-pp"))
        try await seeded.insertSemanticScanResult(
            attempt(id: "scan-pp", assetId: "asset-pp", status: .decodingFailure, attemptCount: 9, createdAt: 5_000)
        )
        try await rewindToV54(seeded)

        AnalysisStore.resetMigratedPathsForTesting()
        let upgraded = try AnalysisStore(directory: dir)
        try await upgraded.migrate()
        // A twelfth attempt lands under the new binary.
        try await upgraded.insertSemanticScanResult(
            attempt(id: "scan-pp", assetId: "asset-pp", status: .exceededContextWindow, createdAt: 9_000)
        )

        let raw = try #require(try rawRow(in: dir, rowId: "scan-pp"))
        // The set GREW — the new attempt is recorded, and the seeded one is not
        // lost. This is the direction a naive "overwrite with the incoming
        // status" implementation gets wrong.
        #expect(raw.observedStatuses == "decodingFailure,exceededContextWindow")
        #expect(raw.lastAttemptAt == 9_000)
        #expect(raw.createdAt == 5_000, "the pre-V55 value is frozen where it stood; it is not re-stamped")
        // …and STILL null. A row cannot earn a complete history by being
        // attempted again; the attempts it lost are still lost.
        #expect(raw.firstAttemptAt == nil)

        let row = try #require(try await upgraded.fetchSemanticScanResults(analysisAssetId: "asset-pp").first)
        #expect(row.historyIsComplete == false)
        // Two statuses on the record: the answer is a definite YES even though
        // the record is partial. Partial evidence can still settle "they
        // differed"; it can never settle "they were alike".
        #expect(row.attemptsDiffered == true)
    }

    @Test("V55 is idempotent and does not re-stamp on a second migrate")
    func migrationIsIdempotent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let seeded = try AnalysisStore(directory: dir)
        try await seeded.migrate()
        try await seeded.insertAsset(makeAsset(id: "asset-idem"))
        try await seeded.insertSemanticScanResult(
            attempt(id: "scan-idem", assetId: "asset-idem", status: .noAds, createdAt: 7_777)
        )
        try await rewindToV54(seeded)

        AnalysisStore.resetMigratedPathsForTesting()
        let first = try AnalysisStore(directory: dir)
        try await first.migrate()
        let afterFirst = try #require(try rawRow(in: dir, rowId: "scan-idem"))

        AnalysisStore.resetMigratedPathsForTesting()
        let second = try AnalysisStore(directory: dir)
        try await second.migrate()
        let afterSecond = try #require(try rawRow(in: dir, rowId: "scan-idem"))

        // playhead-6gcy: HEAD, not 55 — see `upgradeClaimsOnlyWhatItCanProve`.
        #expect(try await second.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(afterFirst.createdAt == afterSecond.createdAt)
        #expect(afterFirst.lastAttemptAt == afterSecond.lastAttemptAt)
        #expect(afterFirst.observedStatuses == afterSecond.observedStatuses)
        #expect(afterSecond.firstAttemptAt == nil)
    }

    @Test("fresh DB: the three columns exist at head, and head is 56")
    func freshDatabaseCarriesTheColumns() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // Pinned to the LITERAL head so the next schema bump has to read this
        // rung, matching the convention of every sibling migration suite.
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
        #expect(AnalysisStore.currentSchemaVersion == 64)

        let columns = try withReadOnlyHandle(in: dir) { db -> Set<String> in
            var stmt: OpaquePointer?
            guard sqlite3_prepare_v2(db, "PRAGMA table_info(semantic_scan_results)", -1, &stmt, nil) == SQLITE_OK else {
                throw NSError(domain: "columns", code: 1)
            }
            defer { sqlite3_finalize(stmt) }
            var out: Set<String> = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                if let raw = sqlite3_column_text(stmt, 1) { out.insert(String(cString: raw)) }
            }
            return out
        }
        #expect(columns.isSuperset(of: ["firstAttemptAt", "lastAttemptAt", "observedStatuses"]))
    }

    // MARK: - 5. The success path is not exempt

    @Test("a SUCCESS replacing a failure keeps the attempt count and records both statuses")
    func successDoesNotEraseTheFailuresBeforeIt() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-sx"))

        try await store.insertSemanticScanResult(
            attempt(id: "scan-sx", assetId: "asset-sx", status: .decodingFailure, attemptCount: 4, createdAt: 100)
        )
        // The retry succeeds. Before V55 this wrote `attemptCount = 1,
        // status = success, createdAt = 200` — indistinguishable from a window
        // that succeeded on its first try.
        try await store.insertSemanticScanResult(
            attempt(id: "scan-sx", assetId: "asset-sx", status: .success, attemptCount: 1, createdAt: 200)
        )

        let raw = try #require(try rawRow(in: dir, rowId: "scan-sx"))
        #expect(raw.status == SemanticScanStatus.success.rawValue)
        #expect(raw.attemptCount == 5, "the four failures before the success are still counted")
        #expect(raw.observedStatuses == "decodingFailure,success")
        #expect(raw.createdAt == 100)
        #expect(raw.lastAttemptAt == 200)
    }

    /// Found by enumerating the EVENTS the write path can perform and asking
    /// which of them nothing observes (the playhead-o89d R5 method). There are
    /// six — new row, replace-with-failure, replace-with-success, H-1 SKIP, a
    /// validation throw, and this one: SUCCESS OVER SUCCESS, which the C5
    /// contract has always let fall through to REPLACE and which no rail
    /// touched.
    ///
    /// **The first version of this rail asserted `attemptCount == 2` and was
    /// wrong, and the bug it was blessing was mine.** Making the probe
    /// unconditional fixed a `.success` replacing a FAILURE (which used to reset
    /// the counter to 1) and broke `BackfillJobRunner.checkpointCoarseProgress`,
    /// which writes each screened window at the checkpoint AND again in the
    /// end-of-pass digest. Its own header states the constraint: "writing a
    /// success row here and again at end of pass leaves the counter at 1 both
    /// times, while writing a FAILURE row twice would silently inflate it and
    /// make a single failed window read as two." Incrementing unconditionally
    /// applied that inflation to every checkpointed success on every completed
    /// pass — one screened window reading as two attempts.
    ///
    /// So the ambiguity (an idempotent re-write and a real second success are the
    /// same two rows on disk) is resolved in favour of the writer that exists.
    @Test("a SUCCESS re-written over a SUCCESS is the SAME attempt — the checkpoint/digest double-write")
    func successOverSuccessDoesNotInflateTheCount() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-ss"))

        // The checkpoint write, then the end-of-pass digest write, of ONE
        // screened window — the shape `checkpointCoarseProgress` produces.
        try await store.insertSemanticScanResult(
            attempt(id: "scan-ss", assetId: "asset-ss", status: .success, latencyMs: 10, createdAt: 100)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-ss", assetId: "asset-ss", status: .success, latencyMs: 20, createdAt: 300)
        )

        let raw = try #require(try rawRow(in: dir, rowId: "scan-ss"))
        #expect(raw.attemptCount == 1, "one screened window must not read as two attempts")
        // …and the digest no longer drags `createdAt` forward to its own instant,
        // which is the loss `checkpointCoarseProgress`'s header records as
        // unavoidable ("a killed pass leaves the earlier, truer timestamp, and a
        // completed one does not"). Both keep it now.
        #expect(raw.createdAt == 100)
        #expect(raw.firstAttemptAt == 100)
        // `lastAttemptAt` DOES move, and must: the digest is a write.
        #expect(raw.lastAttemptAt == 300)
        #expect(raw.observedStatuses == "success")
        let row = try #require(try await store.fetchSemanticScanResults(analysisAssetId: "asset-ss").first)
        #expect(row.attemptsDiffered == false)
    }

    /// The other half of the same branch, and the reason it is a RANK test rather
    /// than a "did anything change" test: a success replacing a FAILURE is
    /// unambiguously a new attempt and must still increment. Without this, the
    /// R3 fix could be over-applied into "a success never increments", which is
    /// the reset playhead-bg2n set out to close.
    @Test("a SUCCESS over a FAILURE still increments — a rank change is not an idempotent re-write")
    func successOverFailureStillIncrements() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-sf"))

        try await store.insertSemanticScanResult(
            attempt(id: "scan-sf", assetId: "asset-sf", status: .decodingFailure, attemptCount: 2, createdAt: 100)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-sf", assetId: "asset-sf", status: .success, attemptCount: 1, createdAt: 200)
        )
        // …and the digest re-write of that very success must NOT add a fourth.
        try await store.insertSemanticScanResult(
            attempt(id: "scan-sf", assetId: "asset-sf", status: .success, attemptCount: 1, createdAt: 250)
        )

        let raw = try #require(try rawRow(in: dir, rowId: "scan-sf"))
        #expect(raw.attemptCount == 3, "two failures then a success is three attempts; the digest re-write is not a fourth")
        #expect(raw.observedStatuses == "decodingFailure,success")
        #expect(raw.createdAt == 100)
        #expect(raw.lastAttemptAt == 250)
    }

    @Test("H-1 still holds: a later failure cannot demote a cached success")
    func cachedSuccessIsStillProtected() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-h1"))

        try await store.insertSemanticScanResult(
            attempt(id: "scan-h1", assetId: "asset-h1", status: .success, latencyMs: 12, createdAt: 100)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-h1", assetId: "asset-h1", status: .refusal, createdAt: 200)
        )

        let raw = try #require(try rawRow(in: dir, rowId: "scan-h1"))
        #expect(raw.status == SemanticScanStatus.success.rawValue)
        // The write was SKIPPED entirely, so the refusal is not in the set and
        // `lastAttemptAt` did not move. That is correct and worth pinning: the
        // history records writes that HAPPENED, and this one did not.
        #expect(raw.observedStatuses == "success")
        #expect(raw.lastAttemptAt == 100)
        #expect(raw.attemptCount == 1)
    }

    // MARK: - Encoding

    @Test("a row whose observedStatuses column is NULL still contributes its status")
    func nullSetColumnStillContributesTheRowsStatus() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-nl"))
        try await store.insertSemanticScanResult(
            attempt(id: "scan-nl", assetId: "asset-nl", status: .decodingFailure, createdAt: 10)
        )
        // A V55-SHAPED row whose set column is NULL. Not hypothetical: the V55
        // rung is `guard observed >= 54`, exactly like every rung after V39, so
        // a database whose V39 rolled back gets the three columns from
        // `createTables()`'s `addColumnIfNeeded` and NEVER runs the seed. On such
        // a device every row is in this state, and the union has nothing but the
        // existing row's own `status` to fold in.
        try await store.execForTesting(
            "UPDATE semantic_scan_results SET observedStatuses = NULL WHERE id = 'scan-nl'"
        )

        try await store.insertSemanticScanResult(
            attempt(id: "scan-nl", assetId: "asset-nl", status: .exceededContextWindow, createdAt: 20)
        )
        let raw = try #require(try rawRow(in: dir, rowId: "scan-nl"))
        #expect(raw.observedStatuses == "decodingFailure,exceededContextWindow",
                "the status ON THE ROW is evidence even when the set column is not")
    }

    @Test("the encoder round-trips, and an unknown raw value is DROPPED rather than throwing")
    func encodingRoundTripsAndDecodesLeniently() {
        let set: Set<SemanticScanStatus> = [.exceededContextWindow, .decodingFailure, .noAds]
        let encoded = SemanticScanResult.encodeObservedStatuses(set)
        #expect(encoded == "decodingFailure,exceededContextWindow,no_ads")
        #expect(SemanticScanResult.decodeObservedStatuses(encoded) == set)

        // SORTEDNESS, asserted over ALL cases rather than over three. `Set`
        // iteration is deterministic within a process for a given element set,
        // so a three-element assertion would agree with an unsorted encoder
        // whenever the hash seed happened to order them correctly — a rail whose
        // verdict depends on the seed. With 20 cases that coincidence is 1/20!.
        let all = Set(SemanticScanStatus.allCases)
        #expect(
            SemanticScanResult.encodeObservedStatuses(all)
                == SemanticScanStatus.allCases.map(\.rawValue).sorted().joined(separator: ","),
            "an unsorted encoding makes two pulls that saw the same statuses compare unequal"
        )

        // Forward-compat: a status a newer binary invented must not abort the
        // read of a whole row. Dropping is the UNDER-claiming direction — it can
        // only shrink the set, i.e. answer `nil`/`false` where the truth is
        // `true`. It can never manufacture a difference that did not happen.
        #expect(SemanticScanResult.decodeObservedStatuses("decodingFailure,quantumRefusal") == [.decodingFailure])
        #expect(SemanticScanResult.decodeObservedStatuses(nil).isEmpty)
        #expect(SemanticScanResult.decodeObservedStatuses("").isEmpty)
    }

    /// Review round 2. The lenient decoder is the right call for the TYPED set —
    /// a status a newer binary invented must not abort a whole-row read — but
    /// counting that set to answer "did the attempts differ" turns a dropped
    /// token into a confident `false` on a COMPLETE row. That is
    /// playhead-hzpa's sentence, manufactured by a decoder, inside the API
    /// written to prevent it.
    @Test("an UNRECOGNISED status still proves the attempts differed — a dropped token is not a missing attempt")
    func unknownStatusTokenStillCountsTowardsDifference() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-fw"))
        try await store.insertSemanticScanResult(
            attempt(id: "scan-fw", assetId: "asset-fw", status: .decodingFailure, createdAt: 10)
        )
        // A row a NEWER binary wrote: two statuses on the record, one of which
        // this build cannot name. `firstAttemptAt` is left intact, so the row
        // claims a COMPLETE history — which is exactly the state in which a
        // wrong `false` is most convincing.
        try await store.execForTesting(
            "UPDATE semantic_scan_results SET observedStatuses = 'decodingFailure,quantumRefusal' WHERE id = 'scan-fw'"
        )

        let row = try #require(try await store.fetchSemanticScanResults(analysisAssetId: "asset-fw").first)
        #expect(row.historyIsComplete)
        // The TYPED set legitimately holds one case — that is the leniency, and
        // it stays.
        #expect(row.observedStatuses == [.decodingFailure])
        // The ANSWER must still be yes. Counting the typed set here reads
        // `false`: "these attempts were all alike".
        #expect(row.attemptsDiffered == true)
    }

    @Test("observedStatuses always contains the row's own status, even with no column")
    func observedStatusesFallsBackToTheRowsOwnStatus() {
        let row = attempt(id: "x", assetId: "a", status: .cancelled)
        #expect(row.observedStatusesCSV == nil)
        #expect(row.observedStatuses == [.cancelled])
        #expect(row.historyIsComplete == false)
        #expect(row.attemptsDiffered == nil)
    }
}
