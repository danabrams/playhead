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
        //
        // playhead-jc42: and EVERY LATER INDEX ON THIS TABLE, for the same
        // reason — a database at v39 has none of them. `migrate()` above climbs
        // to head, so it also built V53's `idx_chunks_asset_pass_span_text`,
        // whose key is `(analysisAssetId, pass, startTime, endTime, text)`.
        // Every duplicate this suite seeds is byte-identical in content (that
        // is what makes it a duplicate), so leaving V53's index standing made
        // the SEED throw `UNIQUE constraint failed` — five tests failing before
        // the rung under test ever ran. Two things that read as one and are
        // not: "the fixture cannot be built" and "V40 did not repair it".
        try exec(db, "DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint")
        try exec(db, "DROP INDEX IF EXISTS idx_chunks_asset_pass_span_text")
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

    // MARK: - 7b. decoded_spans exception C — an unreadable keeper is not risked

    /// playhead-6av0 REVIEW R1 — NEW. `dedupeMergedDecodedSpans` skips a whole
    /// group when the KEEPER's `anchorProvenanceJSON` will not decode, and that
    /// branch had no test: replacing the `guard ... else { continue }` with a
    /// `?? []` fallback left the suite green.
    ///
    /// The branch is the difference between "we could not read the keeper's
    /// anchors" and "the keeper has no anchors". Under the `?? []` reading, a
    /// `.rediffSlot` / `.spliceSlot` marker on the row about to be DELETED is
    /// re-added to a keeper whose own anchor list was silently truncated to
    /// empty — so the group survives with LESS provenance than it started with,
    /// and a span that was exempt from the boundary refiners and the pre-roll
    /// clamp quietly stops being exempt. Keeping both rows is the recoverable
    /// outcome; a half-written keeper is not.
    @Test("decoded_spans: a group whose KEEPER's provenance will not decode is skipped, not half-merged")
    func decodedSpanGroupWithUndecodableKeeperProvenanceIsSkipped() async throws {
        let survivorId = "SURVIVOR"
        let nativeSpanId = DecodedSpan.makeId(assetId: survivorId, firstAtomOrdinal: 58, lastAtomOrdinal: 67)
        let importedSpanId = DecodedSpan.makeId(assetId: "LOSER", firstAtomOrdinal: 58, lastAtomOrdinal: 67)

        let dir = try await seededV39Directory(prefix: "V40SpansBadJSON") { db in
            try self.insertAsset(db, id: survivorId, episodeId: "ep-1", fingerprint: "sha-survivor")
            try self.insertSpan(
                db, id: importedSpanId, assetId: survivorId, first: 58, last: 67,
                start: 71.88, end: 88.14,
                provenanceJSON: #"[{"type":"rediffSlot"}]"#
            )
            // The keeper's provenance is unreadable — a truncated write, a
            // format this binary predates, anything.
            try self.insertSpan(
                db, id: nativeSpanId, assetId: survivorId, first: 58, last: 67,
                start: 71.88, end: 88.14,
                provenanceJSON: "{ this is not an AnchorRef array"
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(try scalarInt(db, "SELECT count(*) FROM decoded_spans") == 2,
                "the group is left ALONE — dropping the duplicate would drop its rediffSlot with it")
        #expect(try scalarText(db, "SELECT anchorProvenanceJSON FROM decoded_spans WHERE id = \(quoted(nativeSpanId))")
                == "{ this is not an AnchorRef array",
                "and the unreadable keeper is not rewritten with a truncated anchor list")
        // The rung still COMPLETES — one unreadable group is not a migration failure.
        #expect(try scalarText(db, "SELECT value FROM _meta WHERE key = 'schema_version'") == String(AnalysisStore.currentSchemaVersion))
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
        // 48 -> 49: playhead-mn5e/2qz6's `trust_episode_observations` ledger,
        // plus a RESET of every `podcast_profiles.observationCount`. Read
        // deliberately for this rung: what this test proves is that a v39 seed
        // climbs the WHOLE remaining ladder and that V40's dedupe left exactly
        // one `transcript_chunks` row. V49 creates a table and writes only to
        // `podcast_profiles`, which this fixture never populates — so the
        // dedupe assertion below is unchanged by it.
        // 49 -> 50 (playhead-e6d3): one fresh retry budget for the
        // coverage-lane rows the FLAT under-coverage rule retired. Its only
        // statement is an UPDATE of `backfill_jobs.retryCount` on already-
        // `failed` rows — a table this fixture never populates, so the dedupe
        // assertion below is unchanged by it too.
        // 50 → 51 read for this rung (playhead-wogi): V51 lowers
        // `backfill_jobs.progressCursor` to the prefix each asset's own
        // `semantic_scan_results` passA rows support, and touches no other
        // column and no other table. Nothing this rung asserts is named by it.
        // 52 → 53 read for this rung (playhead-jc42): V53 sweeps THE SAME TABLE
        // this suite is about, so the reading has to be a real one. It keys on
        // `(analysisAssetId, pass, startTime, endTime, text)` — CONTENT — where
        // V40 keys on `segmentFingerprint`, and it exists because
        // `segmentFingerprint` cannot see across the two hash namespaces the
        // two `pass='final'` producers use. The two rules are independent: V40's
        // `fastAndFinalTwinsBothSurvive` fixture puts one fast and one final row
        // over the same span, which V53 also keeps (its key carries `pass` for
        // the same reason V40's does), and V40's collapse fixtures share a
        // fingerprint AND their content, so V40 removes the row first and V53
        // finds nothing left to sweep. No assertion in this file changes.
        // 56 → 57 read for this rung (playhead-g7ln): V57 resets
        // `podcast_profiles.traitProfileJSON`'s `episodesObserved` key — the
        // same table V49 wrote to and the same table this fixture never
        // populates, so it changes nothing here either. Read rather than
        // assumed, which is the whole point of restating this per rung.
        // 57 → 58 read for this rung (playhead-scc6): V58 resets the PER-CLASS
        // `observationCount` inside `podcast_profiles.detectorTrustJSON` — the
        // mirror V49 reset the show scalar without reaching. Third rung in a row
        // on `podcast_profiles`, and the third time it is irrelevant here for the
        // same reason: this fixture never writes that table.
        // 60 -> 61 read for this rung (playhead-iw7q): V61 ADDS ONE NULLABLE
        // COLUMN, `semantic_scan_results.usedPermissiveFallback`, and writes
        // nothing to it — no UPDATE, no DEFAULT, no row touched. It names no
        // other table and no other column, so nothing this rung asserts moves.
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

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 1)
    }

    @Test("V40 does NOT step over a rolled-back V39 — a database left at 38 stays retryable")
    func v40DoesNotStepOverARolledBackV39() async throws {
        let dir = try makeTempDir(prefix: "V40NoStepOver")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Rewind to the state a device is in when V39 rolled back: no unique
        // asset-identity index, `schema_version` at 38.
        let db = try openRaw(dir)
        try exec(db, "DROP INDEX IF EXISTS idx_assets_episode_fingerprint")
        try exec(db, "DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint")
        // A trigger that makes V39's delete abort, exactly as the 0hi9
        // containment tests do — so V39 rolls back and stays at 38.
        try exec(db, """
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES ('dupe-old', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 1.0)
            """)
        try exec(db, """
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES ('dupe-new', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 2.0)
            """)
        try exec(db, """
            CREATE TRIGGER bd6av0_v39_guard BEFORE DELETE ON analysis_assets
            BEGIN SELECT RAISE(ABORT, 'v40 step-over fixture'); END
            """)
        try exec(db, "UPDATE _meta SET value = '38' WHERE key = 'schema_version'")
        sqlite3_close_v2(db)

        try await store.migrateOnlyForTesting()

        // V39 rolled back. V40 must NOT have stamped 40 over it: if it had, the
        // asset-identity index would be unreachable forever, because every
        // later launch short-circuits on `schemaVersion < 39`.
        #expect(try await store.schemaVersion() == 38)
        let after = try openRaw(dir)
        #expect(try scalarInt(after, "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_chunks_asset_pass_fingerprint'") == 0)
        // And the retry a later launch performs completes BOTH rungs.
        try exec(after, "DROP TRIGGER bd6av0_v39_guard")
        sqlite3_close_v2(after)
        try await store.migrateOnlyForTesting()
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeIndexExists(in: dir, indexName: "idx_chunks_asset_pass_fingerprint"))
    }

    @Test("a within-batch duplicate does not throw under the new UNIQUE index — the batch still lands")
    func withinBatchDuplicateDoesNotLoseTheBatch() async throws {
        let dir = try makeTempDir(prefix: "V40BatchDupe")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let db = try openRaw(dir)
        try exec(db, """
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES ('ASSET', 'ep-1', 'ffee', 'file:///tmp/x.mp3', 'pending', 1.0)
            """)
        sqlite3_close_v2(db)

        func chunk(id: String, fingerprint: String, index: Int) -> TranscriptChunk {
            TranscriptChunk(
                id: id,
                analysisAssetId: "ASSET",
                segmentFingerprint: fingerprint,
                chunkIndex: index,
                startTime: Double(index),
                endTime: Double(index) + 1,
                text: "words",
                normalizedText: "words",
                pass: "fast",
                modelVersion: "test-model",
                transcriptVersion: nil,
                atomOrdinal: nil,
                weakAnchorMetadata: nil,
                speakerId: nil,
                avgConfidence: nil
            )
        }

        // Both writers accumulate a batch and insert it in ONE transaction, so
        // their pre-insert read cannot see a duplicate that is only in the
        // batch. Under a bare INSERT the UNIQUE index turns that into a throw
        // that rolls back the WHOLE batch and loses the shard's transcript.
        let inserted = try await store.insertTranscriptChunks([
            chunk(id: "a", fingerprint: "fp-A", index: 0),
            chunk(id: "b", fingerprint: "fp-A", index: 1),
            chunk(id: "c", fingerprint: "fp-B", index: 2),
        ])
        // playhead-6av0 REVIEW R1: the batch size is NOT the row count any more.
        // `TranscriptEngineService` credits `ShardProgress.chunksInserted` from
        // this return value, and that counter is what decides whether a run with
        // shard failures "produced nothing"; returning 3 here would have it
        // claim a row the database does not hold.
        #expect(inserted == 2, "the store reports rows WRITTEN, not rows offered")

        let after = try openRaw(dir)
        defer { sqlite3_close_v2(after) }
        #expect(try scalarInt(after, "SELECT count(*) FROM transcript_chunks") == 2)
        #expect(try scalarInt(after, "SELECT count(*) FROM transcript_chunks WHERE id = 'a'") == 1)
        #expect(try scalarInt(after, "SELECT count(*) FROM transcript_chunks WHERE id = 'c'") == 1)
    }

    /// playhead-6av0 REVIEW R1 — REWRITTEN, because as first landed this test
    /// proved nothing. It built a fresh `AnalysisStore` and called
    /// `migrateOnlyForTesting()` directly; but that seam's first statement
    /// reaches SQL, every SQL surface routes through `ensureOpen()`, and
    /// `ensureOpen()` runs the WHOLE of `runSchemaMigration()` — `createTables()`
    /// plus the real ladder — before the seam's own first rung is consulted. The
    /// store was already at v40 with the index built by the time line 1 of the
    /// seam ran. Deleting the seam's `migrateDeduplicateMergedChildRowsV40IfNeeded()`
    /// call left the old test GREEN (measured).
    ///
    /// The fix is to open the store FIRST, so `didOpen` is set and
    /// `ensureOpen()` short-circuits, and only then rewind the database to a v39
    /// device and run the seam. Now the seam is the only thing that can move the
    /// schema, and the test fails if that call is removed.
    @Test("the isolated ladder seam (migrateOnlyForTesting) runs V40: it repairs AND builds the index")
    func isolatedLadderSeamRunsV40() async throws {
        let dir = try makeTempDir(prefix: "V40IsolatedLadder")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        // Opens the handle and flips `didOpen`, so nothing below re-enters
        // `runSchemaMigration()`.
        try await store.migrate()

        let db = try openRaw(dir)
        try exec(db, "DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint")
        // playhead-jc42: and V53's, for the same reason as `seededV39Directory`
        // — a v39 device has neither, and this seed is content-identical.
        try exec(db, "DROP INDEX IF EXISTS idx_chunks_asset_pass_span_text")
        try insertAsset(db, id: "SURVIVOR", episodeId: "ep-1", fingerprint: "sha-survivor")
        try insertChunk(
            db, id: "own", assetId: "SURVIVOR", fingerprint: "fp-A",
            chunkIndex: 12, start: 10, end: 11, text: "Hello there"
        )
        try insertChunk(
            db, id: "imported", assetId: "SURVIVOR", fingerprint: "fp-A",
            chunkIndex: 3, start: 10, end: 11, text: "Hello there"
        )
        try exec(db, "UPDATE _meta SET value = '39' WHERE key = 'schema_version'")
        sqlite3_close_v2(db)

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeIndexExists(in: dir, indexName: "idx_chunks_asset_pass_fingerprint"))
        let after = try openRaw(dir)
        defer { sqlite3_close_v2(after) }
        #expect(try scalarInt(after, "SELECT count(*) FROM transcript_chunks") == 1,
                "the seam ran the data repair too, not just the DDL")
        #expect(try scalarText(after, "SELECT id FROM transcript_chunks") == "own")
    }

    // MARK: - 10. The FTS rebuild is load-bearing, not defensive decoration

    /// playhead-6av0 REVIEW R1 — NEW. `dedupeMergedTranscriptChunks` issues an
    /// FTS `rebuild` before its DELETE, and NOTHING pinned it: removing that
    /// statement left the whole suite green (measured).
    ///
    /// It is not decoration. `transcript_chunks_fts` is EXTERNAL-CONTENT, and
    /// deleting a content row whose index entry is missing makes the
    /// `transcript_chunks_ad` trigger's `'delete'` command trip SQLite's
    /// corruption check — `SQLITE_CORRUPT`, "database disk image is malformed",
    /// verified directly against sqlite3. Inside V40 that throw is caught by the
    /// savepoint: the rung rolls back, `schema_version` stays at 39, and the
    /// owner's 7,339 duplicate rows survive every future launch with nothing but
    /// a fault line to show for it.
    ///
    /// Missing index entries are exactly the shape an old database has: rows
    /// written before `transcript_chunks_fts` existed. `'delete-all'` reproduces
    /// that state on the real schema without hand-building one.
    @Test("rows with NO FTS index entry (a pre-FTS database) are still deduped — the rebuild is required")
    func dedupeSurvivesRowsMissingFromTheFTSIndex() async throws {
        let dir = try await seededV39Directory(prefix: "V40FTSMissing") { db in
            try self.insertAsset(db, id: "SURVIVOR", episodeId: "ep-1", fingerprint: "sha-survivor")
            try self.insertChunk(
                db, id: "own", assetId: "SURVIVOR", fingerprint: "fp-A",
                chunkIndex: 12, start: 10, end: 11, text: "prehistoric sponsorship"
            )
            try self.insertChunk(
                db, id: "imported", assetId: "SURVIVOR", fingerprint: "fp-A",
                chunkIndex: 3, start: 10, end: 11, text: "prehistoric sponsorship"
            )
            // Strip the index entries the AFTER INSERT trigger just wrote, so the
            // rows look exactly like content that predates the FTS table.
            try self.exec(db, "INSERT INTO transcript_chunks_fts(transcript_chunks_fts) VALUES('delete-all')")
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        let db = try openRaw(dir)
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks_fts WHERE transcript_chunks_fts MATCH 'prehistoric'") == 0,
                "fixture precondition: the rows carry no FTS index entry")
        sqlite3_close_v2(db)

        try await migrateAgain(dir)

        let after = try openRaw(dir)
        defer { sqlite3_close_v2(after) }
        // Without the rebuild the DELETE raises SQLITE_CORRUPT, the savepoint
        // rolls back, and BOTH rows are still here at schema_version 39.
        #expect(try scalarText(after, "SELECT value FROM _meta WHERE key = 'schema_version'") == String(AnalysisStore.currentSchemaVersion),
                "the rung COMPLETED — a rollback here leaves the duplicates on disk forever")
        #expect(try scalarInt(after, "SELECT count(*) FROM transcript_chunks") == 1)
        #expect(try scalarText(after, "SELECT id FROM transcript_chunks") == "own")
        // ...and the rebuilt index agrees with the post-delete content table.
        #expect(try scalarInt(after, "SELECT count(*) FROM transcript_chunks_fts WHERE transcript_chunks_fts MATCH 'prehistoric'") == 1)
        try exec(after, "INSERT INTO transcript_chunks_fts(transcript_chunks_fts) VALUES('integrity-check')")
    }

    // MARK: - 11. The LAUNCH-SWEEP call site, which the index cannot cover

    /// playhead-6av0 REVIEW R1 — NEW. `reconcileDuplicatePlaceholderAssets`
    /// calls `dedupeMergedTranscriptChunks() + dedupeMergedDecodedSpans()` after
    /// a merge, and NOTHING pinned that call: deleting it left the whole suite
    /// green (measured). It is not redundant with the V40 rung — the rung is
    /// one-shot, and the sweep can merge assets on ANY later launch.
    ///
    /// `decoded_spans` is the half that only the sweep can fix. It carries no
    /// unique constraint and CANNOT carry one (an entirely-imported group has to
    /// be kept), so `UPDATE OR IGNORE` has nothing to conflict on and the
    /// re-point duplicates the row exactly the way `transcript_chunks` used to.
    @Test("the launch sweep collapses the decoded_spans duplicates its own re-point just created")
    func launchSweepDedupesDecodedSpansAfterMerge() async throws {
        let dir = try makeTempDir(prefix: "V40SweepSpans")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let winnerSpanId = DecodedSpan.makeId(assetId: "WINNER", firstAtomOrdinal: 58, lastAtomOrdinal: 67)
        let loserSpanId = DecodedSpan.makeId(assetId: "LOSER", firstAtomOrdinal: 58, lastAtomOrdinal: 67)

        let db = try openRaw(dir)
        // The playhead-0hi9 shape: a self-keyed placeholder and a canonical-SHA
        // survivor for one episode, same artifact basename so the guard merges.
        try exec(db, """
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES ('LOSER', 'ep-1', 'LOSER', 'file:///tmp/same.mp3', 'pending', 1.0)
            """)
        try exec(db, """
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES ('WINNER', 'ep-1', '\(String(repeating: "ab", count: 32))',
                    'file:///tmp/same.mp3', 'pending', 2.0)
            """)
        // Both decoded the SAME atom range. The loser's row id was minted
        // against the loser's asset id, so after the re-point it no longer
        // hashes from its own (assetId, first, last) — that is what identifies
        // it as imported. It carries the rediff width marker; the winner's does not.
        try insertSpan(
            db, id: loserSpanId, assetId: "LOSER", first: 58, last: 67,
            start: 71.88, end: 88.14, provenanceJSON: #"[{"type":"rediffSlot"}]"#
        )
        try insertSpan(
            db, id: winnerSpanId, assetId: "WINNER", first: 58, last: 67,
            start: 71.88, end: 88.14, provenanceJSON: "[]"
        )
        sqlite3_close_v2(db)

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 1)
        #expect(summary.duplicateChildRowsRemoved == 1,
                "the sweep reports the row it swept — the merge re-pointed a span the winner already had")

        let after = try openRaw(dir)
        defer { sqlite3_close_v2(after) }
        #expect(try scalarInt(after, "SELECT count(*) FROM decoded_spans") == 1,
                "without the post-merge sweep the re-point leaves TWO spans on the winner")
        #expect(try scalarText(after, "SELECT id FROM decoded_spans") == winnerSpanId)
        // The imported row's width-ownership anchor is carried onto the keeper
        // rather than deleted with it.
        let json = try #require(try scalarText(after, "SELECT anchorProvenanceJSON FROM decoded_spans"))
        let anchors = try JSONDecoder().decode([AnchorRef].self, from: Data(json.utf8))
        #expect(anchors.contains(.rediffSlot))
    }

    // MARK: - 12. The rung never stamps itself onto a NEWER database

    /// playhead-6av0 REVIEW R1 — NEW. The rung's upper bound (`observed < 40`)
    /// was unpinned: widening it to `< 41` left the suite green (measured).
    ///
    /// It is the guard that stops a data-repair rung from DOWNGRADING a
    /// database. `migrateDeduplicateMergedChildRowsV40IfNeeded` ends with an
    /// unconditional `setSchemaVersion(40)`, so if the ceiling ever slips, a
    /// database that a later binary carried to 41 gets stamped back to 40 by an
    /// older/rebuilt one — and every rung above 40 then re-runs against data
    /// that has already been through it. Same reasoning as the `>= 39` floor
    /// below it, in the other direction.
    @Test("a database stamped NEWER than this binary is left alone — V40 never writes its version backwards")
    func v40DoesNotDowngradeANewerDatabase() async throws {
        let dir = try makeTempDir(prefix: "V40NoDowngrade")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()

        let db = try openRaw(dir)
        // A hypothetical FUTURE device: the index is gone because a later rung
        // replaced it with something else. V40 must not resurrect it, and must
        // not re-stamp.
        //
        // playhead-hx6n: this is `currentSchemaVersion + 1`, not the literal 41
        // it used to be. The claim is "a database stamped NEWER THAN THIS
        // BINARY", so the fixture has to move with head — pinned at 41 it stopped
        // being a newer database the moment V42 landed, and the test then
        // reported that V40 had downgraded a database when what really happened
        // was that V42 legitimately climbed it.
        let newerVersion = AnalysisStore.currentSchemaVersion + 1
        try exec(db, "DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint")
        try exec(db, "UPDATE _meta SET value = '\(newerVersion)' WHERE key = 'schema_version'")
        sqlite3_close_v2(db)

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == newerVersion,
                "V40 stamped its own version over a NEWER database")
        #expect(!(try probeIndexExists(in: dir, indexName: "idx_chunks_asset_pass_fingerprint")),
                "V40 ran its body on a database that is past it")
    }
}
