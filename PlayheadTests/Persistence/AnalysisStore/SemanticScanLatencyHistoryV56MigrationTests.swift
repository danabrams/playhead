// SemanticScanLatencyHistoryV56MigrationTests.swift
// playhead-6gcy: pin the V56 change that lets a `semantic_scan_results` row say
// WHAT ITS ATTEMPTS COST, not just what the last one cost.
//
// WHAT WENT WRONG, so the rails below read as answers to a question.
// playhead-bg2n closed the attempt-IDENTITY half of one defect — `createdAt`
// stopped moving, `observedStatuses` became a monotone set — and named this half
// in its own filing: `status` AND `latencyMs` describe ONE attempt and are read
// as describing all of them. `latencyMs` stayed last-write-wins.
//
// THE WITNESS, re-measured for this bead over the preserved captures.
// `scan-24f9deacdb0e3ab6` (asset `AA6CD430`, head window `[0.0, 42.9]`, id
// stable in every capture) read:
//
//     status=decodingFailure        attempts=5   latencyMs=6747.4
//     status=decodingFailure        attempts=9   latencyMs=19413.8
//     status=exceededContextWindow  attempts=11  latencyMs=8213.7
//
// A 2.88x spread on one window, of which a pull sees only the last.
//
// AND THE DENOMINATOR, because "one row in 961" is the wrong reading of that.
// Only SIX rows are provably re-attempted between two preserved capture states
// — that is the whole population in which an overwrite could be OBSERVED — and
// all six had `latencyMs` overwritten. Five are `noWork:` sentinels whose cost
// is a structural 0.0, so they overwrote 0 with 0. On the 2026-08-15 pull the
// forward-looking population is 26 rows carrying 79 attempts beyond their
// first, and 22 of the 26 report a cost of exactly 0.0 today with nothing on
// disk to tell "every attempt was free" from "the last of twelve was".
//
// The directions this file has to cover, because closing one leaves the defect
// alive in the others:
//
//   1. THE ROW REMEMBERS WHAT ITS ATTEMPTS COST. `latencyMsTotal` /
//      `latencyMsMax` / `latencySampleCount` accumulate across upserts, and
//      `latencyMs` keeps meaning THIS attempt.
//   2. THE IDEMPOTENT DIGEST DOES NOT INFLATE THE TOTAL. This is the one that
//      is invisible from a device pull if it regresses:
//      `checkpointCoarseProgress` writes each screened window at the checkpoint
//      AND again at end of pass with the SAME `latencyMs`. Accumulating
//      unconditionally doubles the cost of every checkpointed success and
//      claims two timed attempts where there was one.
//   3. AN UNMEASURED ATTEMPT CONTRIBUTES NOTHING — and says so, by leaving
//      `latencySampleCount` short of `attemptCount`. A 0 here would be
//      playhead-ejr7's "unmeasured read as free" re-introduced.
//   4. THE HISTORY DOES NOT LIE ABOUT ITS OWN COMPLETENESS. The licence is
//      COMPOSITE — bg2n's `firstAttemptAt` AND `sampleCount == attemptCount` —
//      and neither half implies the other.
//   5. THE BACKFILL REPAIRS ONE SAMPLE AND CLAIMS NO MORE. A pre-V56 row gets
//      a true one-sample record; it does not gain a total of 0, and it does not
//      gain exhaustiveness.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("semantic_scan_results remembers what its attempts COST (playhead-6gcy)")
struct SemanticScanLatencyHistoryV56MigrationTests {

    private static let cohort = """
        {"promptLabel":"l","promptHash":"p","schemaHash":"s","scanPlanHash":"sp",\
        "normalizationHash":"n","osBuild":"26A","locale":"en_US","appBuild":"1"}
        """

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "SemanticScanLatencyHistoryV56")
    }

    // MARK: - Raw-column probes
    //
    // Deliberately NOT routed through `AnalysisStore` for the disk claims, for
    // the sibling V55 suite's reason: the claim is about what is ON DISK, and
    // asking the store would ask the same read whose correctness is under test.
    // A matched pair of bugs in the bind and the read would agree perfectly.

    private struct RawLatency {
        var latencyMs: Double?
        var total: Double?
        var max: Double?
        var sampleCount: Int?
        var attemptCount: Int
        var status: String
    }

    private func rawLatency(in directory: URL, rowId: String) throws -> RawLatency? {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "rawLatency", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        let sql = """
            SELECT latencyMs, latencyMsTotal, latencyMsMax, latencySampleCount,
                   attemptCount, status
            FROM semantic_scan_results WHERE id = ?
            """
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "rawLatency", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, (rowId as NSString).utf8String, -1, nil)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        func optDouble(_ index: Int32) -> Double? {
            sqlite3_column_type(stmt, index) == SQLITE_NULL ? nil : sqlite3_column_double(stmt, index)
        }
        func optInt(_ index: Int32) -> Int? {
            sqlite3_column_type(stmt, index) == SQLITE_NULL ? nil : Int(sqlite3_column_int(stmt, index))
        }
        return RawLatency(
            latencyMs: optDouble(0),
            total: optDouble(1),
            max: optDouble(2),
            sampleCount: optInt(3),
            attemptCount: Int(sqlite3_column_int(stmt, 4)),
            status: String(cString: sqlite3_column_text(stmt, 5))
        )
    }

    /// Rewind a migrated store to the V55 SHAPE, not merely the V55 version
    /// stamp — the sibling V52/V55 rule, and for its reason: a rewind that only
    /// touches `_meta` proves nothing, because the rung would then run against
    /// columns that already exist and the work under test would be
    /// indistinguishable from a no-op.
    ///
    /// Pinned to the LITERAL 55 — "pre-6gcy" is a fixed historical fact, and
    /// `currentSchemaVersion - 1` would stop meaning it the moment head moves.
    private func rewindToV55(_ store: AnalysisStore) async throws {
        for column in ["latencyMsTotal", "latencyMsMax", "latencySampleCount"] {
            try await store.execForTesting("ALTER TABLE semantic_scan_results DROP COLUMN \(column)")
        }
        try await store.setMetaValue(forKey: "schema_version", value: "55")
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

    // MARK: - 1. The row remembers what its attempts cost

    @Test("the witness shape: three differing costs accumulate, and latencyMs stays the LAST")
    func witnessSpreadIsRecoverable() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-w"))

        // The device's own three states, in order, with the device's own costs.
        try await store.insertSemanticScanResult(
            attempt(id: "scan-a5", assetId: "asset-w", status: .decodingFailure,
                    attemptCount: 5, latencyMs: 6_747.4, createdAt: 1_755_147_470.0)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-a9", assetId: "asset-w", status: .decodingFailure,
                    attemptCount: 9, latencyMs: 19_413.8, createdAt: 1_755_233_233.0)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-a11", assetId: "asset-w", status: .exceededContextWindow,
                    attemptCount: 11, latencyMs: 8_213.7, createdAt: 1_755_238_634.0)
        )

        let raw = try #require(try rawLatency(in: dir, rowId: "scan-a11"))
        // `latencyMs` is UNCHANGED in meaning: still THIS attempt's cost.
        #expect(raw.latencyMs == 8_213.7)
        // THE regression this file exists for. Before V56 the other two costs
        // were gone and 8213.7 was the only number a pull could see.
        #expect(raw.total == 6_747.4 + 19_413.8 + 8_213.7)
        #expect(raw.max == 19_413.8, "the worst attempt was the 9th, not the last")
        #expect(raw.sampleCount == 3)
    }

    @Test("the spread is legible through the read API, and the mean is over SAMPLES")
    func meanDividesBySamplesNotAttempts() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-m"))

        // Two timed attempts, but the row claims eleven — the device shape.
        try await store.insertSemanticScanResult(
            attempt(id: "scan-m", assetId: "asset-m", status: .decodingFailure,
                    attemptCount: 10, latencyMs: 6_747.4, createdAt: 1_755_147_470.0)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-m", assetId: "asset-m", status: .exceededContextWindow,
                    attemptCount: 11, latencyMs: 19_413.8, createdAt: 1_755_233_233.0)
        )

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-m")
        let row = try #require(rows.first)
        #expect(row.latencySampleCount == 2)
        #expect(row.attemptCount == 11)
        // The denominator is 2, not 11. Dividing by `attemptCount` would report
        // 2,378 ms for a window whose timed attempts averaged 13,080 ms.
        let mean = try #require(row.latencyMsMean)
        #expect(abs(mean - (6_747.4 + 19_413.8) / 2) < 0.0001)
        #expect(row.unmeasuredAttemptCount == 9)
    }

    // MARK: - 2. The idempotent digest does not inflate the total

    @Test("success re-persisted by the end-of-pass digest does NOT double the cost")
    func idempotentSuccessRewriteDoesNotAccumulate() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-d"))

        // `checkpointCoarseProgress` writes the screened window at the
        // checkpoint and again in the end-of-pass digest, same cost both times.
        for id in ["scan-checkpoint", "scan-digest"] {
            try await store.insertSemanticScanResult(
                attempt(id: id, assetId: "asset-d", status: .success,
                        attemptCount: 1, latencyMs: 4_200.0, createdAt: 1_755_147_470.0)
            )
        }

        let raw = try #require(try rawLatency(in: dir, rowId: "scan-digest"))
        // One window screened once. Accumulating unconditionally would read
        // 8,400 ms over 2 samples — a doubled cost that is structurally valid
        // and invisible from a device pull.
        #expect(raw.total == 4_200.0)
        #expect(raw.max == 4_200.0)
        #expect(raw.sampleCount == 1)
        #expect(raw.attemptCount == 1, "bg2n's rule for the same event, unchanged")
    }

    @Test("a RANK CHANGE is a real attempt: success replacing a failure accumulates")
    func successOverFailureIsANewSample() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-r"))

        try await store.insertSemanticScanResult(
            attempt(id: "scan-r", assetId: "asset-r", status: .decodingFailure,
                    attemptCount: 1, latencyMs: 12_000.0, createdAt: 1_755_147_470.0)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-r", assetId: "asset-r", status: .success,
                    attemptCount: 1, latencyMs: 3_000.0, createdAt: 1_755_233_233.0)
        )

        let raw = try #require(try rawLatency(in: dir, rowId: "scan-r"))
        #expect(raw.status == "success")
        #expect(raw.total == 15_000.0, "the failure's cost survives the success that replaced it")
        #expect(raw.max == 12_000.0)
        #expect(raw.sampleCount == 2)
    }

    @Test("a non-success arriving over a cached success is DROPPED and costs nothing")
    func h1EarlyReturnDoesNotAccumulate() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-h"))

        try await store.insertSemanticScanResult(
            attempt(id: "scan-h", assetId: "asset-h", status: .success,
                    attemptCount: 1, latencyMs: 5_000.0, createdAt: 1_755_147_470.0)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-h2", assetId: "asset-h", status: .refusal,
                    attemptCount: 1, latencyMs: 900_000.0, createdAt: 1_755_233_233.0)
        )

        let raw = try #require(try rawLatency(in: dir, rowId: "scan-h"))
        // The H-1 probe returns before the INSERT. No write happened, so no
        // sample happened — a dropped row must not bill its cost to the row it
        // failed to replace.
        #expect(raw.total == 5_000.0)
        #expect(raw.sampleCount == 1)
        #expect(try rawLatency(in: dir, rowId: "scan-h2") == nil)
    }

    // MARK: - 3. An unmeasured attempt contributes nothing, and says so

    @Test("a nil cost leaves all three NULL rather than claiming a free attempt")
    func unmeasuredFirstAttemptRecordsNothing() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-n"))

        try await store.insertSemanticScanResult(
            attempt(id: "scan-n", assetId: "asset-n", status: .cancelled,
                    attemptCount: 1, latencyMs: nil, createdAt: 1_755_147_470.0)
        )

        let raw = try #require(try rawLatency(in: dir, rowId: "scan-n"))
        // 0 asserts the attempt was free; NULL says nobody measured it. That is
        // the distinction playhead-ejr7 spent a bead establishing for
        // `latencyMs`, and it must not be undone one column over.
        #expect(raw.total == nil)
        #expect(raw.max == nil)
        #expect(raw.sampleCount == nil)
    }

    @Test("an unmeasured RETRY carries the history forward untouched")
    func unmeasuredRetryDoesNotDisturbTheRecord() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-u"))

        try await store.insertSemanticScanResult(
            attempt(id: "scan-u", assetId: "asset-u", status: .decodingFailure,
                    attemptCount: 1, latencyMs: 7_000.0, createdAt: 1_755_147_470.0)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-u", assetId: "asset-u", status: .cancelled,
                    attemptCount: 1, latencyMs: nil, createdAt: 1_755_233_233.0)
        )

        let raw = try #require(try rawLatency(in: dir, rowId: "scan-u"))
        #expect(raw.latencyMs == nil, "this attempt measured nothing and says so")
        #expect(raw.total == 7_000.0, "SUM skips NULL; so does this")
        #expect(raw.sampleCount == 1)
        #expect(raw.attemptCount == 2)
        // The gap is the point: two attempts, one of them timed.
        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-u")
        #expect(try #require(rows.first).unmeasuredAttemptCount == 1)
    }

    // MARK: - 4. The history does not lie about its own completeness

    @Test("the licence is COMPOSITE: both halves are required and neither implies the other")
    func completenessRequiresBothHalves() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-l"))

        // (a) every attempt timed AND the record reaches attempt 1 -> true
        try await store.insertSemanticScanResult(
            attempt(id: "scan-la", assetId: "asset-l", status: .decodingFailure,
                    attemptCount: 1, latencyMs: 100.0, createdAt: 1_755_147_470.0)
        )
        let complete = try #require(
            try await store.fetchSemanticScanResults(analysisAssetId: "asset-l").first
        )
        #expect(complete.latencyHistoryIsComplete == true)

        // (b) record reaches attempt 1, but one attempt measured nothing ->
        //     false. `historyIsComplete` alone would still say `true` here,
        //     which is exactly why it is not the licence for a COST.
        try await store.insertSemanticScanResult(
            attempt(id: "scan-lb", assetId: "asset-l", status: .cancelled,
                    attemptCount: 1, latencyMs: nil, createdAt: 1_755_233_233.0)
        )
        let partial = try #require(
            try await store.fetchSemanticScanResults(analysisAssetId: "asset-l").first
        )
        #expect(partial.historyIsComplete, "bg2n's licence still holds — the record reaches attempt 1")
        #expect(partial.latencyHistoryIsComplete == false, "…and it does not vouch for the COST")
    }

    @Test("a pre-V55 row cannot claim a complete cost however many samples it has")
    func missingFirstAttemptBlocksTheClaim() {
        // Constructed directly: this is the shape a migrated pre-V55 row has —
        // `firstAttemptAt` NULL forever, because the value was destroyed.
        let row = SemanticScanResult(
            id: "scan-old", analysisAssetId: "a", windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1, windowStartTime: 0, windowEndTime: 42.9,
            scanPass: "passA", transcriptQuality: .good, disposition: .noAds,
            spansJSON: "[]", status: .decodingFailure, attemptCount: 1,
            errorContext: nil, inputTokenCount: nil, outputTokenCount: nil,
            latencyMs: 8_213.7, scanCohortJSON: Self.cohort, transcriptVersion: "tv-1",
            firstAttemptAt: nil, lastAttemptAt: 1_755_238_634.0,
            latencyMsTotal: 8_213.7, latencyMsMax: 8_213.7, latencySampleCount: 1
        )
        // sampleCount == attemptCount, and it STILL must not read as complete:
        // bg2n established that a pre-V55 `attemptCount` of 1 is not proof of
        // one write, so the count comparison alone vouches for nothing.
        #expect(row.latencySampleCount == row.attemptCount)
        #expect(row.latencyHistoryIsComplete == false)
    }

    @Test("a row with no latency record at all answers nil, never false")
    func unrecordedAnswersNil() {
        let row = SemanticScanResult(
            id: "scan-none", analysisAssetId: "a", windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1, windowStartTime: 0, windowEndTime: 42.9,
            scanPass: "passA", transcriptQuality: .good, disposition: .noAds,
            spansJSON: "[]", status: .cancelled, attemptCount: 3,
            errorContext: nil, inputTokenCount: nil, outputTokenCount: nil,
            latencyMs: nil, scanCohortJSON: Self.cohort, transcriptVersion: "tv-1",
            firstAttemptAt: 1.0, lastAttemptAt: 2.0
        )
        // "not established" is a distinct answer from "no". Reading nil as
        // false here says "the total is a lower bound" about a row that has no
        // total at all — a claim about a quantity that does not exist.
        #expect(row.latencyHistoryIsComplete == nil)
        #expect(row.latencyMsMean == nil)
        #expect(row.unmeasuredAttemptCount == nil)
    }

    // MARK: - 5. The backfill repairs one sample and claims no more

    @Test("V56 seeds a TRUE one-sample record from the surviving latencyMs")
    func migrationSeedsOneSample() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-mig"))
        try await store.insertSemanticScanResult(
            attempt(id: "scan-mig", assetId: "asset-mig", status: .exceededContextWindow,
                    attemptCount: 11, latencyMs: 8_213.7, createdAt: 1_755_238_634.0)
        )
        try await rewindToV55(store)
        #expect(try await store.schemaVersion() == 55)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        #expect(try await reopened.schemaVersion() == 56)

        let raw = try #require(try rawLatency(in: dir, rowId: "scan-mig"))
        // Lossless for the one sample that survived — the same kind of statement
        // `lastAttemptAt = createdAt` was: a repair, not a claim.
        #expect(raw.total == 8_213.7)
        #expect(raw.max == 8_213.7)
        #expect(raw.sampleCount == 1)
        // …and it does NOT manufacture exhaustiveness. Eleven attempts, one
        // sample: the ten destroyed costs are visibly missing.
        let row = try #require(
            try await reopened.fetchSemanticScanResults(analysisAssetId: "asset-mig").first
        )
        #expect(row.latencyHistoryIsComplete == false)
        #expect(row.unmeasuredAttemptCount == 10)
    }

    @Test("V56 does NOT give an unmeasured row a total of zero")
    func migrationLeavesUnmeasuredRowsNull() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-null"))
        try await store.insertSemanticScanResult(
            attempt(id: "scan-null", assetId: "asset-null", status: .cancelled,
                    attemptCount: 4, latencyMs: nil, createdAt: 1_755_238_634.0)
        )
        try await rewindToV55(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let raw = try #require(try rawLatency(in: dir, rowId: "scan-null"))
        // A row that never measured itself must not arrive at V56 claiming it
        // cost nothing. That is the exact conflation ejr7 removed from the
        // writer; re-introducing it from the migration side undoes that bead.
        #expect(raw.total == nil)
        #expect(raw.max == nil)
        #expect(raw.sampleCount == nil)
    }

    @Test("the migration is idempotent and never resets an accumulated row")
    func migrationDoesNotResetAnAccumulatedRow() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-idem"))
        try await store.insertSemanticScanResult(
            attempt(id: "scan-i", assetId: "asset-idem", status: .decodingFailure,
                    attemptCount: 1, latencyMs: 1_000.0, createdAt: 1.0)
        )
        try await store.insertSemanticScanResult(
            attempt(id: "scan-i", assetId: "asset-idem", status: .decodingFailure,
                    attemptCount: 2, latencyMs: 3_000.0, createdAt: 2.0)
        )

        // Stamp back to 55 WITHOUT dropping the columns: the shape a store has
        // if the rung is re-entered. The guard is `latencySampleCount IS NULL`,
        // so an accumulated row must survive untouched.
        try await store.setMetaValue(forKey: "schema_version", value: "55")
        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let raw = try #require(try rawLatency(in: dir, rowId: "scan-i"))
        #expect(raw.total == 4_000.0, "a re-run must not overwrite the accumulated total with the last cost")
        #expect(raw.sampleCount == 2)
    }

    @Test("a fixture with no semantic_scan_results still reaches v56")
    func migrationSkipsMissingTable() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.execForTesting("DROP TABLE semantic_scan_results")
        try await store.setMetaValue(forKey: "schema_version", value: "55")

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        #expect(try await reopened.schemaVersion() == 56)
    }

    // MARK: - 6. The folding rule, driven directly
    //
    // A pure function can be driven where a branch inside a 60-line upsert can
    // only be reached through SQLite. These are the same claims as above, one
    // layer down, and they are what makes a mutant in `folding` land on a named
    // rail rather than on whichever integration test happens to notice.

    @Test("folding: the first measured attempt seeds one sample")
    func foldingSeedsFirstSample() {
        let history = SemanticScanLatencyHistory.first(attemptLatencyMs: 6_747.4)
        #expect(history.total == 6_747.4)
        #expect(history.max == 6_747.4)
        #expect(history.sampleCount == 1)
    }

    @Test("folding: an unmeasured first attempt records nothing")
    func foldingUnmeasuredFirstIsUnrecorded() {
        #expect(SemanticScanLatencyHistory.first(attemptLatencyMs: nil) == .unrecorded)
    }

    @Test("folding: a real retry accumulates total, max and count")
    func foldingAccumulates() {
        let history = SemanticScanLatencyHistory.first(attemptLatencyMs: 6_747.4)
            .folding(attemptLatencyMs: 19_413.8, isIdempotentRewrite: false)
            .folding(attemptLatencyMs: 8_213.7, isIdempotentRewrite: false)
        #expect(history.total == 6_747.4 + 19_413.8 + 8_213.7)
        #expect(history.max == 19_413.8)
        #expect(history.sampleCount == 3)
    }

    @Test("folding: an idempotent rewrite changes nothing")
    func foldingIdempotentRewriteIsInert() {
        let banked = SemanticScanLatencyHistory.first(attemptLatencyMs: 4_200.0)
        #expect(banked.folding(attemptLatencyMs: 4_200.0, isIdempotentRewrite: true) == banked)
    }

    @Test("folding: an idempotent rewrite SEEDS a row that has no record yet")
    func foldingIdempotentRewriteSeedsUnrecorded() {
        // A pre-V56 row whose only further writes are digests would otherwise
        // stay silent forever, so the one true sample is taken.
        let seeded = SemanticScanLatencyHistory.unrecorded
            .folding(attemptLatencyMs: 4_200.0, isIdempotentRewrite: true)
        #expect(seeded.sampleCount == 1)
        #expect(seeded.total == 4_200.0)
    }

    @Test("folding: an unmeasured retry is inert in both directions")
    func foldingUnmeasuredRetryIsInert() {
        let banked = SemanticScanLatencyHistory.first(attemptLatencyMs: 7_000.0)
        #expect(banked.folding(attemptLatencyMs: nil, isIdempotentRewrite: false) == banked)
        #expect(SemanticScanLatencyHistory.unrecorded
            .folding(attemptLatencyMs: nil, isIdempotentRewrite: false) == .unrecorded)
    }

    @Test("folding: max never falls, whatever order the attempts arrive in")
    func foldingMaxIsMonotone() {
        let descending = SemanticScanLatencyHistory.first(attemptLatencyMs: 19_413.8)
            .folding(attemptLatencyMs: 6_747.4, isIdempotentRewrite: false)
        #expect(descending.max == 19_413.8)
        #expect(descending.total == 19_413.8 + 6_747.4)
    }
}
