// MergedChildRowDedupeV40MigrationTests.swift
// playhead-6av0: the V40 rung that repairs — and then structurally prevents —
// the duplicate child rows `playhead-0hi9`'s duplicate-asset reconciliation
// imported onto every merge survivor.
//
// THE DEFECT. `repointChildRows` moves a loser asset's child rows onto the
// survivor with an unpredicated `UPDATE OR IGNORE ... SET <col> = ?`.
// `OR IGNORE` only skips on a CONSTRAINT conflict, and `transcript_chunks`
// had none to conflict on — its per-asset dedupe is a PRE-INSERT READ
// (`hasTranscriptChunk`), not an index. So every loser row landed on the
// survivor alongside the survivor's own copy of the same segment. The user
// sees it as "select one caption, the duplicate selects too", because
// `TranscriptChunkSelection` is keyed on `(startTime, endTime)` and collapses
// the pair into one selection element while both still draw.
//
// Coverage targets:
//   1. A duplicated `transcript_chunks` pair collapses to the LOWEST rowid —
//      the row `fetchTranscriptChunk`'s unordered `LIMIT 1` has been returning
//      all along.
//   2. `pass` is IN the dedupe key: a fast row and a final row that share a
//      fingerprint are twins, not duplicates, and BOTH survive.
//   3. The delete runs through SQL so the FTS `AFTER DELETE` trigger fires —
//      no ghost rowids left in `transcript_chunks_fts`.
//   4. The UNIQUE index exists afterwards, which is what finally makes
//      `repointChildRows`' `UPDATE OR IGNORE` do the right thing: a live
//      re-point of an already-covered segment discards instead of duplicating.
//   5. `decoded_spans`: an IMPORTED duplicate (id computed against the loser's
//      asset id) is dropped in favour of the id-consistent survivor row, and
//      `isWidthOwnership` provenance on the dropped row is carried over.
//   6. `decoded_spans` exception A — a group where NO row is id-consistent is
//      entirely imported and is KEPT.
//   7. `decoded_spans` exception B — NEGATIVE `firstAtomOrdinal` groups are
//      synthetic and minted per-asset, so identical ordinals do not imply
//      identical audio. Left alone.
//   8. `ad_windows` is deliberately NOT deduplicated (see the suite's
//      `adWindowDuplicatesSurvive` test for the measured reason).
//   9. The rung is idempotent and reaches head from a seeded v39.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("Merged child-row dedupe V40 migration (playhead-6av0)")
struct MergedChildRowDedupeV40MigrationTests {

    // MARK: - Raw sqlite helpers

    private func openRaw(_ directory: URL) throws -> OpaquePointer {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK,
              let handle = db
        else { throw NSError(domain: "MergedChildRowDedupeV40", code: 1) }
        return handle
    }

    private func exec(_ db: OpaquePointer, _ sql: String) throws {
        var message: UnsafeMutablePointer<CChar>?
        guard sqlite3_exec(db, sql, nil, nil, &message) == SQLITE_OK else {
            let text = message.map { String(cString: $0) } ?? "unknown"
            sqlite3_free(message)
            throw NSError(
                domain: "MergedChildRowDedupeV40",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "\(sql) -> \(text)"]
            )
        }
    }

    private func scalarInt(_ db: OpaquePointer, _ sql: String) throws -> Int64 {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(
                domain: "MergedChildRowDedupeV40",
                code: 3,
                userInfo: [NSLocalizedDescriptionKey: sql]
            )
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
        return sqlite3_column_int64(stmt, 0)
    }

    private func scalarText(_ db: OpaquePointer, _ sql: String) throws -> String? {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(
                domain: "MergedChildRowDedupeV40",
                code: 4,
                userInfo: [NSLocalizedDescriptionKey: sql]
            )
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW,
              let raw = sqlite3_column_text(stmt, 0)
        else { return nil }
        return String(cString: raw)
    }

    /// SQL-literal escaping for the seeded fixtures below.
    private func quoted(_ raw: String) -> String {
        "'\(raw.replacingOccurrences(of: "'", with: "''"))'"
    }

    // MARK: - Fixture

    /// Build a head-schema store, then rewind `_meta.schema_version` to 39 so
    /// the V40 rung is the only thing left to run. Returns the directory.
    ///
    /// The rewind is done AFTER `migrate()` so every table (including the FTS
    /// virtual table and its triggers) exists in its real, current shape —
    /// this is an UPGRADE-PATH fixture, not a hand-built schema.
    private func seededV39Directory(
        prefix: String,
        seed: (OpaquePointer) throws -> Void
    ) async throws -> URL {
        let dir = try makeTempDir(prefix: prefix)
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        // Drop the V40 artefacts so the seed can create violations, exactly as
        // a device that stopped at v39 would have.
        try exec(db, "DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint")
        try seed(db)
        try exec(db, "UPDATE _meta SET value = '39' WHERE key = 'schema_version'")
        return dir
    }

    private func insertAsset(_ db: OpaquePointer, id: String, episodeId: String, fingerprint: String) throws {
        try exec(db, """
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES (\(quoted(id)), \(quoted(episodeId)), \(quoted(fingerprint)),
                    'file:///tmp/\(fingerprint).mp3', 'pending', 1.0)
            """)
    }

    private func insertChunk(
        _ db: OpaquePointer,
        id: String,
        assetId: String,
        fingerprint: String,
        chunkIndex: Int,
        start: Double,
        end: Double,
        text: String,
        pass: String = "fast"
    ) throws {
        try exec(db, """
            INSERT INTO transcript_chunks
            (id, analysisAssetId, segmentFingerprint, chunkIndex, startTime, endTime,
             text, normalizedText, pass, modelVersion)
            VALUES (\(quoted(id)), \(quoted(assetId)), \(quoted(fingerprint)), \(chunkIndex),
                    \(start), \(end), \(quoted(text)), \(quoted(text.lowercased())),
                    \(quoted(pass)), 'test-model')
            """)
    }

    private func insertSpan(
        _ db: OpaquePointer,
        id: String,
        assetId: String,
        first: Int,
        last: Int,
        start: Double,
        end: Double,
        provenanceJSON: String = "[]"
    ) throws {
        try exec(db, """
            INSERT INTO decoded_spans
            (id, assetId, firstAtomOrdinal, lastAtomOrdinal, startTime, endTime, anchorProvenanceJSON)
            VALUES (\(quoted(id)), \(quoted(assetId)), \(first), \(last), \(start), \(end),
                    \(quoted(provenanceJSON)))
            """)
    }

    private func migrateAgain(_ dir: URL) async throws {
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
    }

    // MARK: - 1. transcript_chunks collapse

    @Test("a merge-imported transcript_chunks duplicate collapses to the LOWEST rowid")
    func duplicateTranscriptChunkCollapsesToLowestRowId() async throws {
        let dir = try await seededV39Directory(prefix: "V40Collapse") { db in
            try self.insertAsset(db, id: "SURVIVOR", episodeId: "ep-1", fingerprint: "sha-survivor")
            // The survivor's own copy — written first, so lowest rowid. This is
            // the row `fetchTranscriptChunk`'s unordered `LIMIT 1` returns.
            try self.insertChunk(
                db, id: "own", assetId: "SURVIVOR", fingerprint: "fp-A",
                chunkIndex: 12, start: 10, end: 11, text: "Hello there"
            )
            // The imported copy: identical content, its OWN run's chunkIndex.
            // 10,320 of the device's 10,455 real groups look exactly like this.
            try self.insertChunk(
                db, id: "imported", assetId: "SURVIVOR", fingerprint: "fp-A",
                chunkIndex: 3, start: 10, end: 11, text: "Hello there"
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 1)
        #expect(try scalarText(db, "SELECT id FROM transcript_chunks") == "own")
        // chunkIndex is left ALONE — `nextFinalChunkIndex` reads max()+1.
        #expect(try scalarInt(db, "SELECT chunkIndex FROM transcript_chunks") == 12)
    }

    // MARK: - 2. `pass` is in the key

    @Test("a fast row and a final row sharing a fingerprint are twins, not duplicates — BOTH survive")
    func fastAndFinalTwinsBothSurvive() async throws {
        let dir = try await seededV39Directory(prefix: "V40Twins") { db in
            try self.insertAsset(db, id: "SURVIVOR", episodeId: "ep-1", fingerprint: "sha-survivor")
            try self.insertChunk(
                db, id: "fast-row", assetId: "SURVIVOR", fingerprint: "fp-shared",
                chunkIndex: 1, start: 10, end: 11, text: "Same words", pass: "fast"
            )
            try self.insertChunk(
                db, id: "final-row", assetId: "SURVIVOR", fingerprint: "fp-shared",
                chunkIndex: 2, start: 10, end: 11, text: "Same words", pass: "final"
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 2)
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks WHERE pass = 'fast'") == 1)
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks WHERE pass = 'final'") == 1)
    }

    // MARK: - 3. FTS triggers fire

    @Test("the dedupe delete fires the FTS AFTER DELETE trigger — no ghost rowid in transcript_chunks_fts")
    func dedupeLeavesNoFTSGhost() async throws {
        let dir = try await seededV39Directory(prefix: "V40FTS") { db in
            try self.insertAsset(db, id: "SURVIVOR", episodeId: "ep-1", fingerprint: "sha-survivor")
            try self.insertChunk(
                db, id: "own", assetId: "SURVIVOR", fingerprint: "fp-A",
                chunkIndex: 12, start: 10, end: 11, text: "zorblat sponsorship"
            )
            try self.insertChunk(
                db, id: "imported", assetId: "SURVIVOR", fingerprint: "fp-A",
                chunkIndex: 3, start: 10, end: 11, text: "zorblat sponsorship"
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        // The content table has one row...
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 1)
        // ...and the FTS index agrees. A ghost entry makes this 2 (the deleted
        // rowid still matches) and `integrity-check` raises.
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks_fts WHERE transcript_chunks_fts MATCH 'zorblat'") == 1)
        try exec(db, "INSERT INTO transcript_chunks_fts(transcript_chunks_fts) VALUES('integrity-check')")
    }

    // MARK: - 4. The prevention half

    @Test("the UNIQUE index exists at head and makes a live re-point DISCARD instead of duplicate")
    func uniqueIndexMakesRepointDiscard() async throws {
        let dir = try makeTempDir(prefix: "V40Repoint")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try probeIndexExists(in: dir, indexName: "idx_chunks_asset_pass_fingerprint"))

        // Two rows for ONE episode: a self-keyed placeholder (`isPlaceholder`
        // is `assetFingerprint == id`) and a canonical-SHA survivor, the exact
        // `playhead-0hi9` shape. Same basename so the merge guard votes
        // `.merge`.
        let db = try openRaw(dir)
        try exec(db, """
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES ('LOSER', 'ep-1', 'LOSER', 'file:///tmp/same.mp3', 'pending', 1.0)
            """)
        try exec(db, """
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES ('WINNER', 'ep-1',
                    '\(String(repeating: "ab", count: 32))',
                    'file:///tmp/same.mp3', 'pending', 2.0)
            """)
        // Both rows already carry the SAME segment. Pre-V40 the re-point moved
        // the loser's copy on top of the winner's and the user got two.
        try insertChunk(
            db, id: "winner-own", assetId: "WINNER", fingerprint: "fp-A",
            chunkIndex: 0, start: 10, end: 11, text: "Overlapping segment"
        )
        try insertChunk(
            db, id: "loser-own", assetId: "LOSER", fingerprint: "fp-A",
            chunkIndex: 0, start: 10, end: 11, text: "Overlapping segment"
        )
        // A segment ONLY the loser has must still MOVE — the re-point is not a
        // delete, and where the survivor lacks coverage the imported row is
        // the only copy.
        try insertChunk(
            db, id: "loser-only", assetId: "LOSER", fingerprint: "fp-B",
            chunkIndex: 1, start: 20, end: 21, text: "Only the loser has this"
        )
        sqlite3_close_v2(db)

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 1)

        let after = try openRaw(dir)
        defer { sqlite3_close_v2(after) }
        #expect(try scalarInt(after, "SELECT count(*) FROM transcript_chunks WHERE analysisAssetId = 'WINNER'") == 2)
        #expect(try scalarInt(after, "SELECT count(*) FROM transcript_chunks WHERE segmentFingerprint = 'fp-A'") == 1)
        #expect(try scalarText(after, "SELECT id FROM transcript_chunks WHERE segmentFingerprint = 'fp-A'") == "winner-own")
        #expect(try scalarText(after, "SELECT analysisAssetId FROM transcript_chunks WHERE id = 'loser-only'") == "WINNER")
    }

    // MARK: - 5. decoded_spans: imported duplicate dropped, width provenance carried over

    @Test("decoded_spans: the IMPORTED row is dropped in favour of the id-consistent one, and isWidthOwnership provenance survives")
    func decodedSpanImportedDuplicateDropsAndPreservesWidthOwnership() async throws {
        let survivorId = "SURVIVOR"
        let loserId = "LOSER"
        let nativeSpanId = DecodedSpan.makeId(assetId: survivorId, firstAtomOrdinal: 58, lastAtomOrdinal: 67)
        let importedSpanId = DecodedSpan.makeId(assetId: loserId, firstAtomOrdinal: 58, lastAtomOrdinal: 67)

        let dir = try await seededV39Directory(prefix: "V40Spans") { db in
            try self.insertAsset(db, id: survivorId, episodeId: "ep-1", fingerprint: "sha-survivor")
            // Imported first (lower rowid), exactly as the device shows: its id
            // was computed against the LOSER's asset id, so after the re-point
            // it no longer matches its own `(assetId, first, last)`.
            // It carries the rediff width marker; the native row does not.
            try self.insertSpan(
                db, id: importedSpanId, assetId: survivorId, first: 58, last: 67,
                start: 71.88, end: 88.14,
                provenanceJSON: #"[{"type":"rediffSlot"}]"#
            )
            try self.insertSpan(
                db, id: nativeSpanId, assetId: survivorId, first: 58, last: 67,
                start: 71.88, end: 88.14,
                provenanceJSON: #"[{"type":"sustainedMusicOffset","regionId":"SURVIVOR#58-67","confidence":0.82}]"#
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(try scalarInt(db, "SELECT count(*) FROM decoded_spans") == 1)
        #expect(try scalarText(db, "SELECT id FROM decoded_spans") == nativeSpanId)

        // The dropped row's width-ownership anchor is not lost.
        let json = try #require(try scalarText(db, "SELECT anchorProvenanceJSON FROM decoded_spans"))
        let anchors = try JSONDecoder().decode([AnchorRef].self, from: Data(json.utf8))
        #expect(anchors.contains(where: { $0.isWidthOwnership }))
        #expect(anchors.contains(.rediffSlot))
        // ...and the keeper's own provenance is still there.
        #expect(anchors.count == 2)
    }

    // MARK: - 6. decoded_spans exception A — nothing id-consistent

    @Test("decoded_spans: a group where NO row is id-consistent is entirely imported and is KEPT")
    func decodedSpanFullyImportedGroupIsKept() async throws {
        let survivorId = "SURVIVOR"
        let idFromLoserA = DecodedSpan.makeId(assetId: "LOSER-A", firstAtomOrdinal: 100, lastAtomOrdinal: 110)
        let idFromLoserB = DecodedSpan.makeId(assetId: "LOSER-B", firstAtomOrdinal: 100, lastAtomOrdinal: 110)

        let dir = try await seededV39Directory(prefix: "V40SpansImported") { db in
            try self.insertAsset(db, id: survivorId, episodeId: "ep-1", fingerprint: "sha-survivor")
            try self.insertSpan(
                db, id: idFromLoserA, assetId: survivorId, first: 100, last: 110,
                start: 200, end: 210
            )
            try self.insertSpan(
                db, id: idFromLoserB, assetId: survivorId, first: 100, last: 110,
                start: 200, end: 210
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        // Neither row is the survivor's own; the survivor never decoded this
        // range itself, so both copies are all the coverage there is.
        #expect(try scalarInt(db, "SELECT count(*) FROM decoded_spans") == 2)
    }

    // MARK: - 7. decoded_spans exception B — synthetic negative ordinals

    @Test("decoded_spans: NEGATIVE firstAtomOrdinal groups are synthetic and per-asset — left alone")
    func decodedSpanNegativeOrdinalGroupIsLeftAlone() async throws {
        let survivorId = "SURVIVOR"
        let nativeId = DecodedSpan.makeId(assetId: survivorId, firstAtomOrdinal: -3, lastAtomOrdinal: -3)
        let importedId = DecodedSpan.makeId(assetId: "LOSER", firstAtomOrdinal: -3, lastAtomOrdinal: -3)

        let dir = try await seededV39Directory(prefix: "V40SpansNegative") { db in
            try self.insertAsset(db, id: survivorId, episodeId: "ep-1", fingerprint: "sha-survivor")
            try self.insertSpan(
                db, id: importedId, assetId: survivorId, first: -3, last: -3,
                start: 900, end: 930
            )
            try self.insertSpan(
                db, id: nativeId, assetId: survivorId, first: -3, last: -3,
                start: 100, end: 130
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        // A synthetic ordinal is minted per asset, so `-3` on the loser and
        // `-3` on the survivor name DIFFERENT audio — deleting either would
        // destroy a real span. Both survive.
        #expect(try scalarInt(db, "SELECT count(*) FROM decoded_spans") == 2)
    }

    // MARK: - 8. ad_windows is deliberately untouched

    @Test("ad_windows content duplicates are deliberately NOT deduplicated")
    func adWindowDuplicatesSurvive() async throws {
        let dir = try await seededV39Directory(prefix: "V40AdWindows") { db in
            try self.insertAsset(db, id: "SURVIVOR", episodeId: "ep-1", fingerprint: "sha-survivor")
            for id in ["w1", "w2"] {
                try self.exec(db, """
                    INSERT INTO ad_windows
                    (id, analysisAssetId, startTime, endTime, confidence, boundaryState,
                     decisionState, detectorVersion)
                    VALUES (\(self.quoted(id)), 'SURVIVOR', 60.0, 90.0, 0.9, 'exact',
                            'confirmed', 'detection-v1')
                    """)
            }
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        // `ad_windows.id` is a fresh UUID per row and is referenced by other
        // tables; content equality cannot separate a merge import from a
        // legitimate second detection. Measured on the device database: the
        // exact-content duplicate groups sit at ADJACENT rowids (same run) and
        // occur on assets that were never merged at all.
        #expect(try scalarInt(db, "SELECT count(*) FROM ad_windows") == 2)
    }

    // MARK: - 9. Ladder + idempotency

    @Test("the rung reaches head from a seeded v39 and is idempotent")
    func rungReachesHeadAndIsIdempotent() async throws {
        let dir = try await seededV39Directory(prefix: "V40Ladder") { db in
            try self.insertAsset(db, id: "SURVIVOR", episodeId: "ep-1", fingerprint: "sha-survivor")
            try self.insertChunk(
                db, id: "own", assetId: "SURVIVOR", fingerprint: "fp-A",
                chunkIndex: 12, start: 10, end: 11, text: "Hello there"
            )
            try self.insertChunk(
                db, id: "imported", assetId: "SURVIVOR", fingerprint: "fp-A",
                chunkIndex: 3, start: 10, end: 11, text: "Hello there"
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(AnalysisStore.currentSchemaVersion == 40)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 1)
    }

    @Test("the isolated ladder (migrateOnlyForTesting) also reaches v40 and creates the index")
    func isolatedLadderReachesV40() async throws {
        let dir = try makeTempDir(prefix: "V40IsolatedLadder")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 40)
        #expect(try probeIndexExists(in: dir, indexName: "idx_chunks_asset_pass_fingerprint"))
    }
}
