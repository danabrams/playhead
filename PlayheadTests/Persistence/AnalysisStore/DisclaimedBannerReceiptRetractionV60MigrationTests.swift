// DisclaimedBannerReceiptRetractionV60MigrationTests.swift
// playhead-tktr: pin the V60 retraction of three `bannerAutoSkipConfirmed`
// receipts Dan disclaimed on 2026-08-21.
//
// WHY A MIGRATION RETRACTS ANYTHING AT ALL. Four banner confirmations landed on
// asset 0FF7EFF3 inside 5.3 SECONDS OF WALL CLOCK for windows spanning 71.3
// MINUTES of episode time, because playhead-bwxi's auto tier presented at
// DECISION time rather than on playhead entry. Only the first was audio the
// listener had reached. `ad_listen_rewinds` on that asset is zero, so no seek
// explains the other three, and Dan authorised their retraction in writing.
//
// WHAT MAKES THIS SUITE MORE THAN A COUNT. Every direction below exists because
// closing one leaves a different way to be wrong open, and three of them are
// ways a migration can look correct while being destructive:
//
//   1. THE THREE GO — driven through the real ladder from a rewound V59, on a
//      fixture carrying the device's own rows verbatim.
//   2. THE TWO KEPT ROWS SURVIVE, COLUMN FOR COLUMN. The pre-roll receipt is
//      what Dan actually meant, and the 10:07 denial is a genuine one. A
//      migration that took either would pass a "three fewer rows" assertion.
//   3. NOT FIFTEEN, AND NOT AN ASSET SWEEP. The device holds fifteen
//      `bannerAutoSkipConfirmed` rows and the other twelve are UNFALSIFIABLE,
//      not false — POSITION UNKNOWN IS NOT POSITION WRONG. This is the
//      direction a predicate over `source`, over the asset, or over a
//      `createdAt` window gets wrong, and every one of those predicates passes
//      direction 1.
//   4. IDEMPOTENT, AND FOR THE RIGHT REASON. The version ladder is the outer
//      guard; the inner one is that the statement addresses three primary keys,
//      so a hand-rewound stamp on an already-migrated device deletes nothing
//      and does not throw.
//   5. ABSENT IS NOT AN ERROR. Every device that is not Dan's holds none of
//      these rows, and that is the expected reading rather than a failure.
//   6. A ROW THAT IS NOT THE ROW IS LEFT ALONE. The delete carries the row's
//      own asset, source and scope, so a primary key that has come to name
//      something else takes nothing with it.
//   7. A LADDER THAT CANNOT READ THE TABLE DOES NOT STAMP v60. The stamp is
//      read as evidence the retraction ran, so a failed run must not write it.
//   8. THE BLAST RADIUS IS ONE TABLE. `ad_windows` — including the three
//      windows the retracted receipts pointed at — survives untouched.
//   9. THE POPULATION IS CLOSED AT THREE, checked as a VALUE. `disclaimed…V60`
//      is asserted to name the three ids Dan disclaimed and none of the two he
//      kept, so a swapped id is caught without a database at all.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("V60 retracts exactly three disclaimed banner receipts (playhead-tktr)")
struct DisclaimedBannerReceiptRetractionV60MigrationTests {

    // MARK: - The device's own rows

    static let asset = "0FF7EFF3-CD54-4B14-98A7-148CD173AC42"
    static let feed = "https://feeds.simplecast.com/dHoohVNH"

    /// The pre-roll receipt — the card Dan actually meant. KEPT.
    static let keptPreRoll = "11697881-92E4-4090-84EE-F7C4CA4AE650"
    /// The 10:07 `bannerSuggestionDenied` — a genuine denial. KEPT.
    static let keptDenial = "4537D900-EC9D-40DF-8EA0-AD6AEE9A9F6E"

    static let retractedIDs = [
        "1C7996C6-87C2-45E8-B91D-0DBCFB627D6E",
        "6A5908CA-8E98-45A1-9840-A1C5C770E168",
        "A3273865-4BF4-475B-921A-37000B5A0B94",
    ]

    /// One `correction_events` row as it stands on the 2026-08-21 t6 pull.
    struct DeviceRow {
        let id: String
        let assetId: String
        let scope: String
        let createdAt: Double
        let source: String
        let podcastId: String
        let correctionType: String
        let normalizedScopeKey: String
        let identityKey: String
    }

    /// All five rows on asset 0FF7EFF3, verbatim from the pull.
    static let assetRows: [DeviceRow] = [
        DeviceRow(
            id: keptPreRoll, assetId: asset,
            scope: "exactTimeSpan:\(asset):0.000:86.831",
            createdAt: 1_787_315_496.777266,
            source: "bannerAutoSkipConfirmed", podcastId: feed,
            correctionType: "falseNegative",
            normalizedScopeKey: "exactTimeSpan:\(asset):0.000:86.831",
            identityKey: """
                26:explicit-banner-receipt-v2|36:\(asset)|13:falseNegative\
                |23:bannerAutoSkipConfirmed|1:0|19:4635810606798657520\
                |36:A5A6BD65-4FDC-45C5-8953-72EF9EC86666
                """
        ),
        DeviceRow(
            id: retractedIDs[0], assetId: asset,
            scope: "exactTimeSpan:\(asset):1369.809:1548.487",
            createdAt: 1_787_315_499.151094,
            source: "bannerAutoSkipConfirmed", podcastId: feed,
            correctionType: "falseNegative",
            normalizedScopeKey: "exactTimeSpan:\(asset):1369.809:1548.487",
            identityKey: """
                26:explicit-banner-receipt-v2|36:\(asset)|13:falseNegative\
                |23:bannerAutoSkipConfirmed|19:4653739300427469807\
                |19:4654525131229795541|36:2E3D978D-C83D-46D1-B4E1-8285C411162D
                """
        ),
        DeviceRow(
            id: retractedIDs[1], assetId: asset,
            scope: "exactTimeSpan:\(asset):3367.262:3534.576",
            createdAt: 1_787_315_500.6917849,
            source: "bannerAutoSkipConfirmed", podcastId: feed,
            correctionType: "falseNegative",
            normalizedScopeKey: "exactTimeSpan:\(asset):3367.262:3534.576",
            identityKey: """
                26:explicit-banner-receipt-v2|36:\(asset)|13:falseNegative\
                |23:bannerAutoSkipConfirmed|19:4659623103022270225\
                |19:4659991030614276143|36:B9366B42-D64B-4988-A8D5-E0927A16C163
                """
        ),
        DeviceRow(
            id: retractedIDs[2], assetId: asset,
            scope: "exactTimeSpan:\(asset):4279.302:4309.420",
            createdAt: 1_787_315_502.062706,
            source: "bannerAutoSkipConfirmed", podcastId: feed,
            correctionType: "falseNegative",
            normalizedScopeKey: "exactTimeSpan:\(asset):4279.302:4309.420",
            identityKey: """
                26:explicit-banner-receipt-v2|36:\(asset)|13:falseNegative\
                |23:bannerAutoSkipConfirmed|19:4661427156500556570\
                |19:4661460272548836890|36:33AD12CE-C9C7-4443-AEED-12263202EFF7
                """
        ),
        DeviceRow(
            id: keptDenial, assetId: asset,
            scope: "exactTimeSpan:\(asset):4208.200:4306.040",
            createdAt: 1_787_321_235.6574969,
            source: "bannerSuggestionDenied", podcastId: feed,
            correctionType: "falsePositive",
            normalizedScopeKey: "exactTimeSpan:\(asset):4208.200:4306.040",
            identityKey: """
                26:explicit-banner-receipt-v2|36:\(asset)|13:falsePositive\
                |22:bannerSuggestionDenied|19:4661348979533099828\
                |19:4661456555750761431|23:fusion-b28418a113ae4ff4
                """
        ),
    ]

    /// The OTHER twelve `bannerAutoSkipConfirmed` rows the device holds, on
    /// eleven other assets. Their positions were never recorded either — that
    /// is the point. Nobody disclaimed them, so nothing may take them.
    static let unfalsifiableRows: [DeviceRow] = (0..<12).map { index in
        let other = String(format: "OTHERAST-0000-0000-0000-%012d", index)
        let start = Double(index) * 100.0
        let scope = "exactTimeSpan:\(other):\(String(format: "%.3f", start)):\(String(format: "%.3f", start + 30))"
        return DeviceRow(
            id: String(format: "UNFALSIF-0000-0000-0000-%012d", index),
            assetId: other,
            scope: scope,
            createdAt: 1_787_000_000.0 + Double(index),
            source: "bannerAutoSkipConfirmed",
            podcastId: feed,
            correctionType: "falseNegative",
            normalizedScopeKey: scope,
            identityKey: "26:explicit-banner-receipt-v2|36:\(other)|13:falseNegative|23:bannerAutoSkipConfirmed|\(index)"
        )
    }

    // MARK: - Raw-disk probes
    //
    // Deliberately NOT routed through `AnalysisStore.loadCorrectionEvents`, for
    // the sibling V52/V55/V57 suites' reason: the claim is about what is ON
    // DISK, and the store's own reader dedupes and decodes on the way out, so
    // it could agree with a broken write.

    /// Copy the bound string rather than borrowing it. `SQLITE_STATIC` (a nil
    /// destructor) would leave sqlite holding a pointer into a temporary whose
    /// lifetime ends before `sqlite3_step`, which is the shape that produces a
    /// fixture that is usually right.
    private static let sqliteTransient = unsafeBitCast(
        -1, to: sqlite3_destructor_type.self
    )

    private func openReadOnly(_ directory: URL) throws -> OpaquePointer {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK,
              let handle = db
        else {
            throw NSError(domain: "openReadOnly", code: 1)
        }
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

    /// Every `correction_events.id` on disk, sorted.
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

    /// One row's descriptive columns, or nil when the row is gone.
    /// `playheadTimeAtCorrection` comes back as `nil` for SQL NULL, which is
    /// the reading the two kept rows must keep: UNKNOWN IS NOT ZERO.
    private func rawRow(
        in directory: URL,
        id: String
    ) throws -> (asset: String, scope: String, createdAt: Double,
                 source: String?, podcastId: String?, correctionType: String?,
                 identityKey: String?, playhead: Double?)? {
        let db = try openReadOnly(directory)
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        let sql = """
            SELECT analysisAssetId, scope, createdAt, source, podcastId,
                   correctionType, correctionIdentityKey, playheadTimeAtCorrection
              FROM correction_events WHERE id = ?
            """
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "rawRow", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, id, -1, Self.sqliteTransient)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        func optText(_ idx: Int32) -> String? {
            sqlite3_column_type(stmt, idx) == SQLITE_NULL
                ? nil : String(cString: sqlite3_column_text(stmt, idx))
        }
        return (
            asset: String(cString: sqlite3_column_text(stmt, 0)),
            scope: String(cString: sqlite3_column_text(stmt, 1)),
            createdAt: sqlite3_column_double(stmt, 2),
            source: optText(3),
            podcastId: optText(4),
            correctionType: optText(5),
            identityKey: optText(6),
            playhead: sqlite3_column_type(stmt, 7) == SQLITE_NULL
                ? nil : sqlite3_column_double(stmt, 7)
        )
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

    /// A `dayZeroRediffByteExact` window shaped like the three the retracted
    /// receipts pointed at: detection's own record, minted by the byte diff
    /// rather than by the tap. Nothing in Dan's statement disclaims these.
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

    /// Writes rows straight into `correction_events` with a read-write handle,
    /// bypassing `appendCorrectionEvent`. Seeding through the writer would key
    /// the identity columns itself and mint fresh UUIDs; the whole point of
    /// this fixture is that the primary keys are the device's.
    private func seedRows(_ rows: [DeviceRow], in directory: URL) throws {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK,
              let handle = db
        else { throw NSError(domain: "seedRows", code: 1) }
        defer { sqlite3_close_v2(handle) }
        let sql = """
            INSERT INTO correction_events
            (id, analysisAssetId, scope, createdAt, source, podcastId,
             correctionType, normalizedScopeKey, effectiveCorrectionType,
             correctionIdentityKey, submissionCount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """
        for row in rows {
            var stmt: OpaquePointer?
            guard sqlite3_prepare_v2(handle, sql, -1, &stmt, nil) == SQLITE_OK else {
                throw NSError(domain: "seedRows", code: 2)
            }
            defer { sqlite3_finalize(stmt) }
            let values = [
                row.id, row.assetId, row.scope, "", row.source, row.podcastId,
                row.correctionType, row.normalizedScopeKey, row.correctionType,
                row.identityKey,
            ]
            for (offset, value) in values.enumerated() where offset != 3 {
                sqlite3_bind_text(stmt, Int32(offset + 1), value, -1, Self.sqliteTransient)
            }
            sqlite3_bind_double(stmt, 4, row.createdAt)
            guard sqlite3_step(stmt) == SQLITE_DONE else {
                throw NSError(
                    domain: "seedRows", code: 3,
                    userInfo: [NSLocalizedDescriptionKey: String(cString: sqlite3_errmsg(handle))]
                )
            }
        }
    }

    /// Rewind to the V59 stamp. There is no SHAPE to rewind — V60 adds no
    /// column and drops none, it removes three rows — so touching `_meta` is
    /// the honest rewind here, exactly as V57's suite argues for its own rung.
    /// Pinned to the LITERAL 59: "pre-tktr" is a fixed historical fact, and
    /// `currentSchemaVersion - 1` stops meaning it the moment head moves.
    private func rewindToV59(_ store: AnalysisStore) async throws {
        try await store.setMetaValue(forKey: "schema_version", value: "59")
    }

    /// A migrated store holding the device's five rows on 0FF7EFF3 plus the
    /// twelve unfalsifiable ones, rewound to V59 and ready to re-migrate.
    private func seededStore(
        prefix: String,
        rows: [DeviceRow]
    ) async throws -> URL {
        let dir = try makeTempDir(prefix: prefix)
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        for assetId in Set(rows.map(\.assetId)) {
            try await store.insertAsset(makeAsset(id: assetId))
        }
        try seedRows(rows, in: dir)
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

    // MARK: - 1. The three go

    @Test("V60 deletes exactly the three receipts Dan disclaimed")
    func migrationRetractsTheThree() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60Retracts",
            rows: Self.assetRows + Self.unfalsifiableRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        #expect(try rawCorrectionIDs(in: dir).count == 17)

        let reopened = try await remigrate(dir)
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)

        let remaining = try rawCorrectionIDs(in: dir)
        #expect(remaining.count == 14, "three rows go, and only three")
        for id in Self.retractedIDs {
            #expect(!remaining.contains(id), "\(id) was disclaimed and must be gone")
        }
    }

    // MARK: - 2. The two kept rows survive, column for column

    @Test("V60 leaves the pre-roll receipt and the 10:07 denial untouched")
    func migrationKeepsTheTwo() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60Keeps",
            rows: Self.assetRows + Self.unfalsifiableRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        _ = try await remigrate(dir)

        for kept in Self.assetRows where kept.id == Self.keptPreRoll || kept.id == Self.keptDenial {
            let row = try #require(
                try rawRow(in: dir, id: kept.id),
                "the kept receipt \(kept.id) must survive"
            )
            #expect(row.asset == kept.assetId)
            #expect(row.scope == kept.scope)
            #expect(row.createdAt == kept.createdAt)
            #expect(row.source == kept.source)
            #expect(row.podcastId == kept.podcastId)
            #expect(row.correctionType == kept.correctionType)
            #expect(row.identityKey == kept.identityKey)
            // UNKNOWN IS NOT ZERO. Both kept rows predate V59's column, so it
            // reads NULL — and a zero would say the listener was at the top of
            // the episode, which is exactly the claim nobody can make.
            #expect(row.playhead == nil, "a pre-V59 receipt has no recorded position, and must not be given one")
        }
    }

    // MARK: - 3. Not fifteen, and not an asset sweep

    @Test("the other twelve bannerAutoSkipConfirmed rows survive — unknown is not wrong")
    func migrationLeavesTheUnfalsifiableTwelve() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60Twelve",
            rows: Self.assetRows + Self.unfalsifiableRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        _ = try await remigrate(dir)
        let remaining = Set(try rawCorrectionIDs(in: dir))

        for row in Self.unfalsifiableRows {
            #expect(
                remaining.contains(row.id),
                "nobody disclaimed \(row.id); a predicate over `source` would have taken it"
            )
        }
        // …and the FOURTH tap on the very same asset, at the very same second,
        // which is what a predicate over the asset or a `createdAt` window
        // would have taken.
        #expect(remaining.contains(Self.keptPreRoll))
    }

    // MARK: - 4. Idempotent

    @Test("a second run over an already-retracted database deletes nothing and does not throw")
    func migrationIsIdempotent() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60Idem",
            rows: Self.assetRows + Self.unfalsifiableRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        let first = try await remigrate(dir)
        let afterFirst = try rawCorrectionIDs(in: dir)
        #expect(afterFirst.count == 14)

        // The device's own shape: the stamp says 60 and nothing re-runs.
        let second = try await remigrate(dir)
        #expect(try await second.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try rawCorrectionIDs(in: dir) == afterFirst)

        // And the inner guard, exercised directly: rewind the stamp by hand and
        // run the rung again over rows that are already gone. Three ABSENT is
        // the expected reading, not an error.
        try await first.setMetaValue(forKey: "schema_version", value: "59")
        let third = try await remigrate(dir)
        #expect(try await third.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try rawCorrectionIDs(in: dir) == afterFirst, "a re-run must not take a fourth row")
    }

    // MARK: - 5. Absent is not an error

    @Test("a database that never held these rows migrates to v60 untouched")
    func migrationOnAForeignDatabase() async throws {
        let dir = try await seededStore(
            prefix: "TktrV60Foreign",
            rows: Self.unfalsifiableRows
        )
        defer { try? FileManager.default.removeItem(at: dir) }

        let before = try rawCorrectionIDs(in: dir)
        let reopened = try await remigrate(dir)
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try rawCorrectionIDs(in: dir) == before, "no row on a foreign device is addressed by this rung")
    }

    // MARK: - 6. A row that is not the row is left alone

    @Test(
        "a primary key that has come to name something else takes nothing with it",
        arguments: ["scope", "source", "asset"]
    )
    func migrationRefusesAMismatchedRow(_ mutatedColumn: String) async throws {
        let target = Self.assetRows[1]
        let imposterAsset = "IMPOSTER-0000-0000-0000-000000000001"
        let imposter = DeviceRow(
            id: target.id,
            assetId: mutatedColumn == "asset" ? imposterAsset : target.assetId,
            scope: mutatedColumn == "scope"
                ? "exactTimeSpan:\(target.assetId):9999.000:9999.500"
                : target.scope,
            createdAt: target.createdAt,
            source: mutatedColumn == "source" ? "manualVeto" : target.source,
            podcastId: target.podcastId,
            correctionType: target.correctionType,
            normalizedScopeKey: target.normalizedScopeKey + "-imposter",
            identityKey: target.identityKey + "-imposter"
        )
        let dir = try await seededStore(prefix: "TktrV60Imposter", rows: [imposter])
        defer { try? FileManager.default.removeItem(at: dir) }

        let reopened = try await remigrate(dir)
        #expect(
            try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion,
            "a mismatch is reported, not thrown — the ladder must still reach head"
        )
        #expect(
            try rawCorrectionIDs(in: dir) == [target.id],
            "the row at this id is not the receipt that was disclaimed (\(mutatedColumn) differs), so it stays"
        )
    }

    // MARK: - 7. A database V60 cannot read is not stamped as migrated
    //
    // V60 carries `guard try tableExists("correction_events")`, mirroring V59.
    // That branch is UNREACHABLE THROUGH `migrate()` and this test is what says
    // so rather than leaving a reader to assume it was exercised: the ladder
    // runs `addColumnIfNeeded(table: "correction_events", …)` UNCONDITIONALLY,
    // hundreds of rungs earlier, so a database missing the table dies there.
    // The guard is defence in depth against a future reordering, and the
    // property that IS testable — and is the one that matters — is that a
    // ladder which fails does not leave the stamp at 60. A version stamp is
    // read as evidence the retraction ran; it must never be written by a run
    // that could not look.

    @Test("a ladder that cannot read correction_events does not stamp v60")
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
        #expect(sqlite3_exec(db, "DROP TABLE correction_events", nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        await #expect(throws: (any Error).self) {
            try await reopened.migrate()
        }
        // Read the stamp OFF DISK. `AnalysisStore.schemaVersion()` opens
        // lazily, so asking the store would re-enter the same failing ladder
        // and throw instead of answering — which is itself the reason this
        // probe exists rather than a convenience.
        #expect(
            try rawSchemaVersion(in: dir) == "59",
            "a run that could not read the table must not claim to have retracted anything"
        )
    }

    // MARK: - 8. The blast radius is one table

    @Test("the ad_windows the retracted receipts pointed at survive")
    func migrationTouchesNoOtherTable() async throws {
        let dir = try makeTempDir(prefix: "TktrV60Blast")
        defer { try? FileManager.default.removeItem(at: dir) }

        let windowIDs = [
            "2E3D978D-C83D-46D1-B4E1-8285C411162D",
            "B9366B42-D64B-4988-A8D5-E0927A16C163",
            "33AD12CE-C9C7-4443-AEED-12263202EFF7",
        ]

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: Self.asset))
        for (index, windowId) in windowIDs.enumerated() {
            try await store.insertAdWindow(
                makeWindow(id: windowId, start: Double(index + 1) * 1000)
            )
        }
        try seedRows(Self.assetRows, in: dir)
        try await rewindToV59(store)

        _ = try await remigrate(dir)

        AnalysisStore.resetMigratedPathsForTesting()
        let reader = try AnalysisStore(directory: dir)
        for windowId in windowIDs {
            #expect(
                try await reader.fetchAdWindow(id: windowId) != nil,
                "the window \(windowId) is detection\'s own record and no receipt owns it"
            )
        }
        #expect(try rawCorrectionIDs(in: dir).count == 2, "only the receipts move")
    }

    // MARK: - 9. The population is closed at three, as a value

    @Test("the retraction table names the three disclaimed ids and neither kept one")
    func theTableIsClosedAtThree() {
        let table = AnalysisStore.disclaimedBannerReceiptsV60
        #expect(table.count == 3, "Dan's statement covers three cards; a fourth entry is a claim nobody made")
        #expect(Set(table.map(\.id)) == Set(Self.retractedIDs))
        #expect(!table.contains { $0.id == Self.keptPreRoll }, "the pre-roll is what he actually meant")
        #expect(!table.contains { $0.id == Self.keptDenial }, "the 10:07 denial is genuine")
        for entry in table {
            #expect(entry.analysisAssetId == Self.asset)
            #expect(entry.source == "bannerAutoSkipConfirmed")
            #expect(
                entry.scope.hasPrefix("exactTimeSpan:\(Self.asset):"),
                "the delete carries the row's own scope, so the key alone cannot retarget it"
            )
        }
        // The scopes are the device's, span for span.
        #expect(Set(table.map(\.scope)) == Set([
            "exactTimeSpan:\(Self.asset):1369.809:1548.487",
            "exactTimeSpan:\(Self.asset):3367.262:3534.576",
            "exactTimeSpan:\(Self.asset):4279.302:4309.420",
        ]))
    }
}
