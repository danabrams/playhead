// UnearnedRecurrenceGradeV60MigrationTests.swift
// playhead-tktr / playhead-ph2d: pin the V60 downgrade of three
// `repeated_ad_cache` grades that a UI defect manufactured.
//
// READ THIS BEFORE THE BEAD TITLE. playhead-tktr was opened, authorised and
// BUILT as a RETRACTION — delete three `bannerAutoSkipConfirmed` receipts of
// 2026-08-21, because four taps landed inside 5.3 SECONDS of wall clock for
// windows spanning 71.3 MINUTES of episode time and only the first was audio
// the listener had reached. Dan then withdrew it: *"you can leave them if they
// were ads — for ph2d downgrade."* All four windows are
// `dayZeroRediffByteExact` at confidence 1.0, `wasSkipped = 1`, and his verdict
// on the session was that the skipping was perfect throughout. The taps
// recorded TRUE FACTS WITH A FALSE PROVENANCE, so the receipts stay and only
// the RANK they bought is corrected.
//
// WHAT THE RANK IS. `explicitConfirmation` scores 2 against `consumed`'s 1 in
// `upsertRepeatedAdCacheEntry`, which refuses any update ranking LOWER — so the
// grade is durably immune to being overwritten by weaker learning. It asserts
// "a human looked at this". Downgrading to `consumedAutoSkip`/`consumed` puts
// the row where it would have landed anyway: the listener played past all three
// skips, so `queueConsumedCatalogLearning` would have banked the same
// fingerprints at rank 1.
//
// The directions, because closing one leaves a different way to be wrong open:
//
//   1. THE THREE DROP, driven through the real ladder from a rewound V59.
//   2. BOTH COLUMNS MOVE. A lifecycle-only edit is worse than a delete: two
//      writers guard `learningSource.authoritativeLifecycle == learningLifecycle`,
//      so `confirmedAutoSkipBanner` + `consumed` is a pair no writer will
//      re-establish. This direction asserts the pair the enum calls
//      authoritative.
//   3. THE PRE-ROLL KEEPS ITS GRADE. Dan heard [0.000 - 86.831] and meant that
//      tap. It is the same asset, the same show, the same source and the same
//      second as the three — the ONLY thing separating it is that he was there.
//      **This is the direction Dan's decision creates**, and nothing else guards it.
//   4. THE RECEIPTS ARE NOT TOUCHED. All five `correction_events` rows on the
//      asset survive, `playheadTimeAtCorrection` still NULL. The retraction is
//      WITHDRAWN, and a suite that did not assert this could not tell the
//      shipped rung from the one that was replaced.
//   5. NOT SEVENTEEN. Every row in the table is `explicitConfirmation`; the
//      other thirteen were disclaimed by nobody.
//   6. NO TOMBSTONE IS WRITTEN. The revocation tables stay empty — the
//      product's own withdrawal path is permanent and is the wrong instrument.
//   7. IDEMPOTENT BECAUSE THE PREDICATE DISARMS ITSELF, and absent is not an
//      error.
//   8. A ROW THAT IS NOT THE ROW IS LEFT ALONE.
//   9. THE BLAST RADIUS IS ONE TABLE, and `ad_windows` in particular survives.
//  10. A LADDER THAT CANNOT READ THE TABLE DOES NOT STAMP v60.
//  11. THE POPULATION IS CLOSED AT THREE, checked as a VALUE.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("V60 downgrades exactly three unearned recurrence grades (playhead-tktr)")
struct UnearnedRecurrenceGradeV60MigrationTests {

    // MARK: - The device's own rows

    static let asset = "0FF7EFF3-CD54-4B14-98A7-148CD173AC42"
    static let feed = "https://feeds.simplecast.com/dHoohVNH"

    /// The pre-roll's cache row — the tap Dan actually meant. KEEPS rank 2.
    static let keptWindowId = "A5A6BD65-4FDC-45C5-8953-72EF9EC86666"
    static let keptFingerprint = "ccddffcedc444400"

    /// The three windows whose grade the disclaimed taps bought.
    static let downgradedWindowIds = [
        "2E3D978D-C83D-46D1-B4E1-8285C411162D",
        "B9366B42-D64B-4988-A8D5-E0927A16C163",
        "33AD12CE-C9C7-4443-AEED-12263202EFF7",
    ]

    /// One `repeated_ad_cache` row as it stands on the 2026-08-21 t6 pull.
    struct CacheRow {
        let showId: String
        let fingerprint: String
        let boundaryStart: Double
        let boundaryEnd: Double
        let learningSource: String
        let learningLifecycle: String
        let sourceAssetId: String
        let sourceWindowId: String
        let producerRevision: String
    }

    private static func deviceRow(
        _ fingerprint: String,
        _ start: Double,
        _ end: Double,
        _ windowId: String,
        _ producerRevision: String,
        asset assetId: String = asset,
        source: String = "confirmedAutoSkipBanner",
        lifecycle: String = "explicitConfirmation",
        show: String = feed
    ) -> CacheRow {
        CacheRow(
            showId: show,
            fingerprint: fingerprint,
            boundaryStart: start,
            boundaryEnd: end,
            learningSource: source,
            learningLifecycle: lifecycle,
            sourceAssetId: assetId,
            sourceWindowId: windowId,
            producerRevision: producerRevision
        )
    }

    /// The four rows on asset 0FF7EFF3, verbatim from the pull.
    static let assetCacheRows: [CacheRow] = [
        deviceRow(
            "ccdddeeedc544400", 1369.8092928093354, 1548.4865306106415,
            downgradedWindowIds[0], "A57A8B24-12DF-48B8-BB44-909E2B115AE8"
        ),
        deviceRow(
            "ccddddeedc544400", 3367.2622287430645, 3534.5763265257915,
            downgradedWindowIds[1], "52D27232-613D-487C-AACC-4A3597D80081"
        ),
        deviceRow(
            "eeddeeeecc444400", 4279.3015377025822, 4309.4204081571897,
            downgradedWindowIds[2], "D07853B9-7C46-4824-9141-8AE1E4D876D0"
        ),
        // THE PRE-ROLL. Same show, same asset, same source, same second — and
        // he was there for it.
        deviceRow(
            keptFingerprint, 0.0, 86.83102040816425,
            keptWindowId, "DD2EC730-988B-470A-A2A0-38135BF4F23E"
        ),
    ]

    /// The thirteen rows on OTHER assets. The device's table holds 17 and every
    /// one is `explicitConfirmation`; nobody disclaimed these.
    static let untouchedCacheRows: [CacheRow] = (0..<13).map { index in
        let otherAsset = String(format: "OTHERAST-0000-0000-0000-%012d", index)
        let start = Double(index + 1) * 100.0
        return deviceRow(
            String(format: "aabbccddeeff%04d", index),
            start, start + 40,
            String(format: "OTHERWIN-0000-0000-0000-%012d", index),
            String(format: "OTHERREV-0000-0000-0000-%012d", index),
            asset: otherAsset,
            source: index % 3 == 0 ? "userMarkedAd" : "confirmedAutoSkipBanner"
        )
    }

    // MARK: - Raw-disk probes
    //
    // Deliberately NOT routed through `AnalysisStore`'s own readers, for the
    // sibling V52/V55/V57 suites' reason: the claim is about what is ON DISK,
    // and the store's reader decodes the very columns under test, so a matched
    // pair of bugs in the write and the read would agree perfectly.

    /// Copy the bound string rather than borrowing it. `SQLITE_STATIC` (a nil
    /// destructor) would leave sqlite holding a pointer into a temporary whose
    /// lifetime ends before `sqlite3_step`.
    private static let sqliteTransient = unsafeBitCast(
        -1, to: sqlite3_destructor_type.self
    )

    private func openReadOnly(_ directory: URL) throws -> OpaquePointer {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK,
              let handle = db
        else { throw NSError(domain: "openReadOnly", code: 1) }
        return handle
    }

    /// `_meta.schema_version` as it stands on disk, without opening the store.
    private func rawSchemaVersion(in directory: URL) throws -> String? {
        let db = try openReadOnly(directory)
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(
            db, "SELECT value FROM _meta WHERE key = 'schema_version'", -1, &stmt, nil
        ) == SQLITE_OK else { throw NSError(domain: "rawSchemaVersion", code: 2) }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return String(cString: sqlite3_column_text(stmt, 0))
    }

    /// The grade a row carries, or nil when no row was learned from that window.
    private func rawGrade(
        in directory: URL,
        windowId: String
    ) throws -> (source: String?, lifecycle: String?)? {
        let db = try openReadOnly(directory)
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(
            db,
            "SELECT learningSource, learningLifecycle FROM repeated_ad_cache WHERE sourceWindowId = ?",
            -1, &stmt, nil
        ) == SQLITE_OK else { throw NSError(domain: "rawGrade", code: 2) }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, windowId, -1, Self.sqliteTransient)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        func optText(_ idx: Int32) -> String? {
            sqlite3_column_type(stmt, idx) == SQLITE_NULL
                ? nil : String(cString: sqlite3_column_text(stmt, idx))
        }
        return (source: optText(0), lifecycle: optText(1))
    }

    /// Every other column of one row, so a downgrade that also moved the
    /// geometry or the provenance is visible.
    private func rawRest(
        in directory: URL,
        windowId: String
    ) throws -> (fingerprint: String, start: Double, end: Double,
                 confidence: Double, lastSeenAt: Double,
                 assetId: String?, revision: String?)? {
        let db = try openReadOnly(directory)
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        let sql = """
            SELECT fingerprint, boundaryStart, boundaryEnd, confidence,
                   lastSeenAt, sourceAssetId, producerRevision
              FROM repeated_ad_cache WHERE sourceWindowId = ?
            """
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "rawRest", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, windowId, -1, Self.sqliteTransient)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        func optText(_ idx: Int32) -> String? {
            sqlite3_column_type(stmt, idx) == SQLITE_NULL
                ? nil : String(cString: sqlite3_column_text(stmt, idx))
        }
        return (
            fingerprint: String(cString: sqlite3_column_text(stmt, 0)),
            start: sqlite3_column_double(stmt, 1),
            end: sqlite3_column_double(stmt, 2),
            confidence: sqlite3_column_double(stmt, 3),
            lastSeenAt: sqlite3_column_double(stmt, 4),
            assetId: optText(5),
            revision: optText(6)
        )
    }

    private func rowCount(in directory: URL, table: String) throws -> Int {
        let db = try openReadOnly(directory)
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM \(table)", -1, &stmt, nil) == SQLITE_OK
        else { throw NSError(domain: "rowCount", code: 2) }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
        return Int(sqlite3_column_int(stmt, 0))
    }

    /// Every `correction_events.id` on disk, sorted. The retraction is
    /// WITHDRAWN, so this must not shrink.
    private func rawCorrectionIDs(in directory: URL) throws -> [String] {
        let db = try openReadOnly(directory)
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "SELECT id FROM correction_events", -1, &stmt, nil) == SQLITE_OK
        else { throw NSError(domain: "rawCorrectionIDs", code: 2) }
        defer { sqlite3_finalize(stmt) }
        var out: [String] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            out.append(String(cString: sqlite3_column_text(stmt, 0)))
        }
        return out.sorted()
    }

    // MARK: - Fixture

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

    /// A `dayZeroRediffByteExact` window shaped like the four the cache rows
    /// were learned from: detection's own record, minted by a byte diff the tap
    /// had nothing to do with — which is exactly why Dan let the receipts stand.
    private func makeWindow(id: String, start: Double) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: Self.asset,
            startTime: start,
            endTime: start + 30,
            confidence: 1.0,
            boundaryState: "dayZeroRediffByteExact",
            decisionState: "applied",
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: start,
            metadataSource: "rediffDayZeroByteExact",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: true,
            userDismissedBanner: false,
            eligibilityGate: "eligible"
        )
    }

    /// Writes rows straight into `repeated_ad_cache`, bypassing the service.
    /// Seeding through `recordConfirmedRecurrence` would REFUSE the very pair
    /// this fixture needs to reproduce — its own guard is
    /// `learningSource.authoritativeLifecycle == learningLifecycle` — and would
    /// also re-derive the fingerprints. The whole point is the device's bytes.
    private func seedCache(_ rows: [CacheRow], in directory: URL) throws {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK,
              let handle = db
        else { throw NSError(domain: "seedCache", code: 1) }
        defer { sqlite3_close_v2(handle) }
        let sql = """
            INSERT INTO repeated_ad_cache
            (showId, fingerprint, boundaryStart, boundaryEnd, confidence,
             lastSeenAt, learningSource, learningLifecycle, sourceAssetId,
             sourceWindowId, producerRevision)
            VALUES (?, ?, ?, ?, 1.0, 1787315499.161, ?, ?, ?, ?, ?)
            """
        for row in rows {
            var stmt: OpaquePointer?
            guard sqlite3_prepare_v2(handle, sql, -1, &stmt, nil) == SQLITE_OK else {
                throw NSError(domain: "seedCache", code: 2)
            }
            defer { sqlite3_finalize(stmt) }
            sqlite3_bind_text(stmt, 1, row.showId, -1, Self.sqliteTransient)
            sqlite3_bind_text(stmt, 2, row.fingerprint, -1, Self.sqliteTransient)
            sqlite3_bind_double(stmt, 3, row.boundaryStart)
            sqlite3_bind_double(stmt, 4, row.boundaryEnd)
            sqlite3_bind_text(stmt, 5, row.learningSource, -1, Self.sqliteTransient)
            sqlite3_bind_text(stmt, 6, row.learningLifecycle, -1, Self.sqliteTransient)
            sqlite3_bind_text(stmt, 7, row.sourceAssetId, -1, Self.sqliteTransient)
            sqlite3_bind_text(stmt, 8, row.sourceWindowId, -1, Self.sqliteTransient)
            sqlite3_bind_text(stmt, 9, row.producerRevision, -1, Self.sqliteTransient)
            guard sqlite3_step(stmt) == SQLITE_DONE else {
                throw NSError(
                    domain: "seedCache", code: 3,
                    userInfo: [NSLocalizedDescriptionKey: String(cString: sqlite3_errmsg(handle))]
                )
            }
        }
    }

    /// Rewind to the V59 stamp. There is no SHAPE to rewind — V60 adds no
    /// column and drops none, it rewrites two values — so touching `_meta` is
    /// the honest rewind here, exactly as V57's suite argues for its own rung.
    /// Pinned to the LITERAL 59: "pre-tktr" is a fixed historical fact, and
    /// `currentSchemaVersion - 1` stops meaning it the moment head moves.
    private func rewindToV59(_ store: AnalysisStore) async throws {
        try await store.setMetaValue(forKey: "schema_version", value: "59")
    }

    private func seededStore(
        prefix: String,
        rows: [CacheRow]
    ) async throws -> URL {
        let dir = try makeTempDir(prefix: prefix)
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        for assetId in Set(rows.map(\.sourceAssetId)) {
            try await store.insertAsset(makeAsset(id: assetId))
        }
        try seedCache(rows, in: dir)
        try await rewindToV59(store)
        #expect(try await store.schemaVersion() == 59)
        return dir
    }

    private func remigrate(_ dir: URL) async throws -> AnalysisStore {
        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        return reopened
    }

    // MARK: - 1 & 2. The three drop, and BOTH columns move

    @Test("V60 downgrades the three unearned grades to consumedAutoSkip/consumed")
    func migrationDowngradesTheThree() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60Downgrade",
            rows: Self.assetCacheRows + Self.untouchedCacheRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        let reopened = try await remigrate(dir)
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)

        for windowId in Self.downgradedWindowIds {
            let grade = try #require(try rawGrade(in: dir, windowId: windowId))
            // BOTH columns, not just the lifecycle. Two writers guard
            // `learningSource.authoritativeLifecycle == learningLifecycle`, so a
            // half-downgrade leaves a pair no writer will re-establish.
            #expect(grade.source == CatalogLearningSource.consumedAutoSkip.rawValue)
            #expect(grade.lifecycle == CatalogLearningLifecycle.consumed.rawValue)
            #expect(
                CatalogLearningSource(rawValue: grade.source ?? "")?.authoritativeLifecycle
                    == CatalogLearningLifecycle(rawValue: grade.lifecycle ?? ""),
                "the written pair must be the one the enum itself calls authoritative"
            )
        }
    }

    @Test("V60 moves the grade and NOTHING else on the row")
    func migrationMovesOnlyTheGrade() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60OnlyGrade",
            rows: Self.assetCacheRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        _ = try await remigrate(dir)

        for row in Self.assetCacheRows {
            let rest = try #require(try rawRest(in: dir, windowId: row.sourceWindowId))
            #expect(rest.fingerprint == row.fingerprint, "the audio identity is untouched")
            #expect(rest.start == row.boundaryStart)
            #expect(rest.end == row.boundaryEnd)
            #expect(rest.confidence == 1.0)
            #expect(rest.lastSeenAt == 1_787_315_499.161, "when it was last seen is a true fact")
            #expect(rest.assetId == row.sourceAssetId)
            #expect(rest.revision == row.producerRevision)
        }
        #expect(try rowCount(in: dir, table: "repeated_ad_cache") == 4, "a downgrade is not a delete")
    }

    // MARK: - 3. The pre-roll keeps its grade — the direction Dan's decision creates

    @Test("the PRE-ROLL grade survives: he heard that one and meant that tap")
    func migrationKeepsThePreRoll() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60PreRoll",
            rows: Self.assetCacheRows + Self.untouchedCacheRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        _ = try await remigrate(dir)

        let grade = try #require(try rawGrade(in: dir, windowId: Self.keptWindowId))
        #expect(
            grade.source == "confirmedAutoSkipBanner",
            "same asset, same show, same source, same second as the three — he was there for this one"
        )
        #expect(grade.lifecycle == "explicitConfirmation")
    }

    // MARK: - 4. The receipts are NOT touched — the retraction is WITHDRAWN

    @Test("V60 deletes no correction_events row: the spans were genuine ads")
    func migrationLeavesEveryReceiptStanding() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60Receipts",
            rows: Self.assetCacheRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        // The five receipts on the asset, including the three the WITHDRAWN
        // retraction would have deleted.
        let receiptIDs = [
            "11697881-92E4-4090-84EE-F7C4CA4AE650",
            "1C7996C6-87C2-45E8-B91D-0DBCFB627D6E",
            "6A5908CA-8E98-45A1-9840-A1C5C770E168",
            "A3273865-4BF4-475B-921A-37000B5A0B94",
            "4537D900-EC9D-40DF-8EA0-AD6AEE9A9F6E",
        ]
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        for (index, id) in receiptIDs.enumerated() {
            let sql = """
                INSERT INTO correction_events
                (id, analysisAssetId, scope, createdAt, source, podcastId,
                 correctionType, normalizedScopeKey, effectiveCorrectionType,
                 correctionIdentityKey, submissionCount)
                VALUES ('\(id)', '\(Self.asset)', 'exactTimeSpan:\(Self.asset):\(index).000:\(index + 1).000',
                        1787315496.0, 'bannerAutoSkipConfirmed', '\(Self.feed)',
                        'falseNegative', 'scope-\(index)', 'falseNegative', 'key-\(index)', 1)
                """
            #expect(sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK)
        }
        sqlite3_close_v2(db)

        AnalysisStore.resetMigratedPathsForTesting()
        let seeder = try AnalysisStore(directory: dir)
        try await seeder.setMetaValue(forKey: "schema_version", value: "59")

        _ = try await remigrate(dir)

        #expect(
            try rawCorrectionIDs(in: dir) == receiptIDs.sorted(),
            "the retraction is WITHDRAWN — every receipt stays, including the three that were disclaimed"
        )
    }

    // MARK: - 5. Not seventeen

    @Test("the other thirteen explicitConfirmation rows keep their grade")
    func migrationLeavesTheOtherThirteen() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60Thirteen",
            rows: Self.assetCacheRows + Self.untouchedCacheRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        _ = try await remigrate(dir)

        for row in Self.untouchedCacheRows {
            let grade = try #require(try rawGrade(in: dir, windowId: row.sourceWindowId))
            #expect(
                grade.lifecycle == "explicitConfirmation",
                "nobody disclaimed \(row.sourceWindowId); a predicate over learningSource or the show would have taken it"
            )
            #expect(grade.source == row.learningSource)
        }
        #expect(try rowCount(in: dir, table: "repeated_ad_cache") == 17)
    }

    // MARK: - 6. No tombstone is written

    @Test("V60 writes no revocation: a tombstone is permanent and is the wrong instrument")
    func migrationWritesNoTombstone() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60NoTombstone",
            rows: Self.assetCacheRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        _ = try await remigrate(dir)

        #expect(try rowCount(in: dir, table: "repeated_ad_cache_revocations") == 0)
        #expect(try rowCount(in: dir, table: "repeated_ad_cache_fingerprint_revocations") == 0)
    }

    // MARK: - 7. Idempotent, and absent is not an error

    @Test("a second run changes nothing, and a database without these rows migrates cleanly")
    func migrationIsIdempotent() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60Idem",
            rows: Self.assetCacheRows + Self.untouchedCacheRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        let first = try await remigrate(dir)
        let afterFirst = try Self.downgradedWindowIds.map { try rawGrade(in: dir, windowId: $0)?.lifecycle }
        #expect(afterFirst == ["consumed", "consumed", "consumed"])

        // Rewind the stamp by hand and run the rung again over rows that are
        // ALREADY downgraded. The predicate requires the OLD grade, so it
        // matches nothing — idempotence for the right reason.
        try await first.setMetaValue(forKey: "schema_version", value: "59")
        let second = try await remigrate(dir)
        #expect(try await second.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(
            try Self.downgradedWindowIds.map { try rawGrade(in: dir, windowId: $0)?.lifecycle }
                == afterFirst
        )
        #expect(try rawGrade(in: dir, windowId: Self.keptWindowId)?.lifecycle == "explicitConfirmation")

        // And a database that never held any of them — every device that is not
        // Dan's — reaches head untouched.
        let foreign = try await seededStore(
            prefix: "TktrV60Foreign",
            rows: Self.untouchedCacheRows
        )
        defer { try? FileManager.default.removeItem(at: foreign) }
        let reopened = try await remigrate(foreign)
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
        for row in Self.untouchedCacheRows {
            #expect(try rawGrade(in: foreign, windowId: row.sourceWindowId)?.lifecycle
                    == "explicitConfirmation")
        }
    }

    // MARK: - 8. A row that is not the row is left alone

    @Test(
        "a provenance pair carrying a grade the tap did not buy is left alone",
        arguments: ["source", "lifecycle"]
    )
    func migrationRefusesAMismatchedGrade(_ mutatedColumn: String) async throws {
        let target = Self.assetCacheRows[0]
        let imposter = CacheRow(
            showId: target.showId,
            fingerprint: target.fingerprint,
            boundaryStart: target.boundaryStart,
            boundaryEnd: target.boundaryEnd,
            learningSource: mutatedColumn == "source" ? "userMarkedAd" : target.learningSource,
            learningLifecycle: mutatedColumn == "lifecycle"
                ? "legacyUnconfirmed" : target.learningLifecycle,
            sourceAssetId: target.sourceAssetId,
            sourceWindowId: target.sourceWindowId,
            producerRevision: target.producerRevision
        )
        let dir = try await seededStore(prefix: "TktrV60Imposter", rows: [imposter])
        defer { try? FileManager.default.removeItem(at: dir) }

        let reopened = try await remigrate(dir)
        #expect(
            try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion,
            "a mismatch is reported, not thrown — the ladder must still reach head"
        )
        let grade = try #require(try rawGrade(in: dir, windowId: target.sourceWindowId))
        #expect(grade.source == imposter.learningSource)
        #expect(grade.lifecycle == imposter.learningLifecycle)
    }

    // MARK: - 9. The blast radius is one table

    @Test("the ad_windows the downgraded rows were learned from survive")
    func migrationTouchesNoOtherTable() async throws {
        let dir = try makeTempDir(prefix: "TktrV60Blast")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: Self.asset))
        let windowIDs = Self.downgradedWindowIds + [Self.keptWindowId]
        for (index, windowId) in windowIDs.enumerated() {
            try await store.insertAdWindow(makeWindow(id: windowId, start: Double(index + 1) * 1000))
        }
        try seedCache(Self.assetCacheRows, in: dir)
        try await rewindToV59(store)

        _ = try await remigrate(dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let reader = try AnalysisStore(directory: dir)
        for windowId in windowIDs {
            #expect(
                try await reader.fetchAdWindow(id: windowId) != nil,
                "the window \(windowId) is detection's own record and the grade change does not own it"
            )
        }
        #expect(try rowCount(in: dir, table: "ad_windows") == 4)
    }

    // MARK: - 10. A ladder that cannot read the table does not stamp v60

    @Test("a ladder that cannot read repeated_ad_cache does not stamp v60")
    func migrationDoesNotStampAVersionItCouldNotReach() async throws {
        let dir = try makeTempDir(prefix: "TktrV60NoTable")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await rewindToV59(store)

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        #expect(sqlite3_exec(db, "DROP TABLE repeated_ad_cache", nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        await #expect(throws: (any Error).self) {
            try await reopened.migrate()
        }
        // Read the stamp OFF DISK. `AnalysisStore.schemaVersion()` opens
        // lazily, so asking the store would re-enter the same failing ladder
        // and throw instead of answering.
        #expect(
            try rawSchemaVersion(in: dir) == "59",
            "a run that could not read the table must not claim to have downgraded anything"
        )
    }

    // MARK: - 11. The population is closed at three, as a value

    @Test("the downgrade table names the three unearned grades and not the pre-roll")
    func theTableIsClosedAtThree() {
        let table = AnalysisStore.unearnedRecurrenceGradesV60
        #expect(table.count == 3, "Dan's decision covers three cards; a fourth entry is a claim nobody made")
        #expect(Set(table.map(\.sourceWindowId)) == Set(Self.downgradedWindowIds))
        #expect(
            !table.contains { $0.sourceWindowId == Self.keptWindowId },
            "the pre-roll is the one he heard, and its grade is earned"
        )
        for entry in table {
            #expect(entry.sourceAssetId == Self.asset)
        }
        // The target grade is the one the enum calls authoritative for the
        // consumed path — the whole reason both columns move.
        #expect(CatalogLearningSource.consumedAutoSkip.authoritativeLifecycle == .consumed)
        #expect(
            CatalogLearningSource.confirmedAutoSkipBanner.authoritativeLifecycle
                == .explicitConfirmation
        )
    }
}
