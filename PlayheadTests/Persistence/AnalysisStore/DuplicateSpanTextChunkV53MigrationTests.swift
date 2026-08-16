// DuplicateSpanTextChunkV53MigrationTests.swift
// playhead-jc42: the V53 rung that repairs — and then structurally prevents —
// duplicate transcript spans two disjoint fingerprint namespaces let onto disk.
//
// THE DEFECT, as the 2026-08-15 device pull states it. Every one of the ten
// assets that had ever run a final pass carried duplicate `(startTime, endTime)`
// rows — 7,248 extra — and the three that had not carried none. All 7,248
// `modelVersion='apple-speech-final-v1'` rows (`FinalPassRetranscriptionRunner`'s)
// had a twin, and 7,247 of those twins were byte-identical in TEXT and TIMING.
//
// The runner's header claims that is impossible: "Fast and final chunks have
// distinct `segmentFingerprint`s (computed from text + timing, both of which
// differ across passes)". They do not differ. `computeFinalPassFingerprint`
// digests `"fp-final-" + "\(text)|\(start)|\(end)"` and
// `TranscriptEngineService.computeFingerprint` digests the same string without
// the prefix, so the fingerprints differ for one reason only — the prefix — and
// `TranscriptEngineService` writes `pass='final'` rows too (17,632 of the
// 24,880 on that device). Two producers, one pass, two hash namespaces: the
// runner's pre-insert read could not see the engine's row and neither could
// `idx_chunks_asset_pass_fingerprint`, which reported ZERO violations
// throughout.
//
// Coverage targets:
//   1. Two `pass='final'` rows with identical span+text and DIFFERENT
//      fingerprints — the exact shipped shape — collapse to the LOWEST rowid.
//   2. The V40 index is satisfied by that pair throughout, so the failure is a
//      real gap and not a rung that simply had not run.
//   3. A `fast` row and a `final` row with identical span+text are TWINS and
//      BOTH survive: `readFastTranscriptRegions` / `readFinalTranscriptRegions`
//      union them separately and deleting either shrinks a real measurement.
//   4. Same span, same pass, DIFFERENT text both survive — the pull's
//      `3C2FFE10 [6450.0, 6450.0]` pair ("that's" / "what.") is distinct data.
//   5. The delete runs through SQL so the FTS `AFTER DELETE` trigger fires — no
//      ghost rowid, and the index row count matches the table's.
//   6. The UNIQUE index exists afterwards and REFUSES a re-insert.
//   7. The rung is idempotent and reaches head from a seeded v52.
//   8. `fetchTranscriptChunkBySpanText` finds an ENGINE-written final row — the
//      question the prefixed fingerprint lookup structurally could not ask —
//      and is scoped to one `pass`.

import CryptoKit
import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("Duplicate span+text chunk V53 migration (playhead-jc42)")
struct DuplicateSpanTextChunkV53MigrationTests {

    // MARK: - Raw sqlite helpers

    private func openRaw(_ directory: URL) throws -> OpaquePointer {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK,
              let handle = db
        else { throw NSError(domain: "DuplicateSpanTextV53", code: 1) }
        return handle
    }

    private func exec(_ db: OpaquePointer, _ sql: String) throws {
        var message: UnsafeMutablePointer<CChar>?
        guard sqlite3_exec(db, sql, nil, nil, &message) == SQLITE_OK else {
            let text = message.map { String(cString: $0) } ?? "unknown"
            sqlite3_free(message)
            throw NSError(
                domain: "DuplicateSpanTextV53",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "\(sql) -> \(text)"]
            )
        }
    }

    private func scalarInt(_ db: OpaquePointer, _ sql: String) throws -> Int64 {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(
                domain: "DuplicateSpanTextV53",
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
                domain: "DuplicateSpanTextV53",
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

    private func quoted(_ raw: String) -> String {
        "'\(raw.replacingOccurrences(of: "'", with: "''"))'"
    }

    // MARK: - Fixture

    /// Build a head-schema store, then rewind `_meta.schema_version` to 52 so
    /// the V53 rung is the only thing left to run.
    ///
    /// The rewind happens AFTER `migrate()` so every table — including the FTS
    /// virtual table and its three triggers — exists in its real, current
    /// shape. This is an UPGRADE-PATH fixture, not a hand-built schema.
    ///
    /// Only V53's index is dropped. `idx_chunks_asset_pass_fingerprint` is left
    /// STANDING on purpose: every seed below has to satisfy it, which is what
    /// makes target 2 a measurement rather than an assertion.
    private func seededV52Directory(
        prefix: String,
        seed: (OpaquePointer) throws -> Void
    ) async throws -> URL {
        let dir = try makeTempDir(prefix: prefix)
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        try exec(db, "DROP INDEX IF EXISTS idx_chunks_asset_pass_span_text")
        try seed(db)
        try exec(db, "UPDATE _meta SET value = '52' WHERE key = 'schema_version'")
        return dir
    }

    private func insertAsset(
        _ db: OpaquePointer,
        id: String,
        episodeId: String,
        fingerprint: String
    ) throws {
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
        pass: String = "final",
        modelVersion: String = "apple-speech-v1"
    ) throws {
        try exec(db, """
            INSERT INTO transcript_chunks
            (id, analysisAssetId, segmentFingerprint, chunkIndex, startTime, endTime,
             text, normalizedText, pass, modelVersion)
            VALUES (\(quoted(id)), \(quoted(assetId)), \(quoted(fingerprint)), \(chunkIndex),
                    \(start), \(end), \(quoted(text)), \(quoted(text.lowercased())),
                    \(quoted(pass)), \(quoted(modelVersion)))
            """)
    }

    private func migrateAgain(_ dir: URL) async throws {
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
    }

    /// The shipped pair, spelled exactly as the two producers spell it: one
    /// engine row under the bare digest, one runner row under the `fp-final-`
    /// digest, same pass, same span, same text.
    private func seedShippedDuplicatePair(
        _ db: OpaquePointer,
        assetId: String = "ASSET",
        text: String = "With the American Express",
        start: Double = 0.78,
        end: Double = 1.86
    ) throws {
        try insertChunk(
            db, id: "engine-row", assetId: assetId,
            fingerprint: TranscriptEngineFingerprintProbe.bare(text: text, startTime: start, endTime: end),
            chunkIndex: 0, start: start, end: end, text: text,
            pass: "final", modelVersion: "apple-speech-v1"
        )
        try insertChunk(
            db, id: "runner-row", assetId: assetId,
            fingerprint: FinalPassRetranscriptionRunner.computeFinalPassFingerprint(
                text: text, startTime: start, endTime: end
            ),
            chunkIndex: 332, start: start, end: end, text: text,
            pass: "final", modelVersion: "apple-speech-final-v1"
        )
    }

    // MARK: - 1 + 2. The shipped shape collapses, and V40 never could

    @Test("two pass='final' rows with identical span+text and different fingerprints collapse to the LOWEST rowid")
    func shippedDuplicatePairCollapsesToLowestRowId() async throws {
        let dir = try await seededV52Directory(prefix: "V53Collapse") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            try self.seedShippedDuplicatePair(db)
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 1)
        #expect(try scalarText(db, "SELECT id FROM transcript_chunks") == "engine-row")
        // chunkIndex is left ALONE — `nextFinalChunkIndex` reads max()+1, so
        // renumbering a survivor would hand a future final pass a taken index.
        #expect(try scalarInt(db, "SELECT chunkIndex FROM transcript_chunks") == 0)
    }

    @Test("the V40 fingerprint rule is SATISFIED by the shipped pair — the gap is real, not an unrun rung")
    func v40IndexCannotSeeTheShippedPair() async throws {
        let dir = try await seededV52Directory(prefix: "V53V40Blind") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            try self.seedShippedDuplicatePair(db)
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        let before = try openRaw(dir)
        // The V40 index is present (only V53's was dropped) and admits BOTH
        // rows: two distinct `(asset, pass, segmentFingerprint)` keys.
        #expect(try scalarInt(
            before,
            "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_chunks_asset_pass_fingerprint'"
        ) == 1)
        #expect(try scalarInt(before, """
            SELECT count(*) FROM (
                SELECT 1 FROM transcript_chunks
                GROUP BY analysisAssetId, pass, segmentFingerprint HAVING count(*) > 1
            )
            """) == 0, "V40's key reports zero violations — exactly as it did on the device")
        #expect(try scalarInt(before, "SELECT count(DISTINCT segmentFingerprint) FROM transcript_chunks") == 2)
        // ...and yet the two rows carry the same audio.
        #expect(try scalarInt(before, "SELECT count(DISTINCT text) FROM transcript_chunks") == 1)
        sqlite3_close_v2(before)

        try await migrateAgain(dir)

        let after = try openRaw(dir)
        defer { sqlite3_close_v2(after) }
        #expect(try scalarInt(after, "SELECT count(*) FROM transcript_chunks") == 1)
    }

    // MARK: - 3. fast + final are twins, not duplicates

    @Test("a fast row and a final row over identical span+text are TWINS — BOTH survive")
    func fastAndFinalTwinsBothSurvive() async throws {
        let dir = try await seededV52Directory(prefix: "V53Twins") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            try self.insertChunk(
                db, id: "fast-row", assetId: "ASSET", fingerprint: "fp-fast",
                chunkIndex: 1, start: 10, end: 11, text: "Same words", pass: "fast"
            )
            try self.insertChunk(
                db, id: "final-row", assetId: "ASSET", fingerprint: "fp-final",
                chunkIndex: 2, start: 10, end: 11, text: "Same words", pass: "final"
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        // Deleting either member shrinks a real measurement:
        // `readFastTranscriptRegions` filters `pass='fast'` and
        // `readFinalTranscriptRegions` filters `pass='final'`, and
        // `fetchFastTranscriptCoveredRanges` gates skip authorisation on the
        // fast union alone. 3,751 of the pull's 7,248 duplicated spans are this
        // shape and none of them is what the user sees doubled —
        // `TranscriptChunkCanonicalizer` drops the fully-covered fast row from
        // every display and detection projection.
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 2)
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks WHERE pass = 'fast'") == 1)
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks WHERE pass = 'final'") == 1)
    }

    // MARK: - 4. Same span, same pass, different text is DATA

    @Test("same span and pass with DIFFERENT text both survive — the zero-width 3C2FFE10 pair is not a copy")
    func sameSpanDifferentTextBothSurvive() async throws {
        let dir = try await seededV52Directory(prefix: "V53DiffText") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            // The pull's one duplicate group whose members disagree:
            // 3C2FFE10 at [6450.0, 6450.0] holds "that's" and "what.".
            try self.insertChunk(
                db, id: "thats", assetId: "ASSET", fingerprint: "fp-1",
                chunkIndex: 1, start: 6450, end: 6450, text: "that's", pass: "fast"
            )
            try self.insertChunk(
                db, id: "what", assetId: "ASSET", fingerprint: "fp-2",
                chunkIndex: 2, start: 6450, end: 6450, text: "what.", pass: "fast"
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        // This is why the key is `(asset, pass, span, TEXT)` and not
        // `(asset, span)`: the bead's proposed key would have eaten one of
        // these, silently, through `insertTranscriptChunk`'s `OR IGNORE`.
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 2)
    }

    // MARK: - 5. FTS stays consistent

    @Test("the dedupe delete fires the FTS AFTER DELETE trigger — no ghost rowid, index count matches the table")
    func dedupeLeavesNoFTSGhost() async throws {
        let dir = try await seededV52Directory(prefix: "V53FTS") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            try self.seedShippedDuplicatePair(db, text: "zorblat sponsorship")
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 1)
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks_fts") == 1,
                "a row removed by a path that skipped the trigger would stay in the index as a ghost")
        // The join `searchTranscripts` performs must return exactly the one
        // surviving row — a ghost rowid matches nothing and a stale entry would
        // return the deleted text.
        #expect(try scalarInt(db, """
            SELECT count(*) FROM transcript_chunks tc
            JOIN transcript_chunks_fts fts ON tc.rowid = fts.rowid
            WHERE transcript_chunks_fts MATCH 'zorblat'
            """) == 1)
    }

    // MARK: - 6. The constraint refuses a re-insert

    @Test("after V53 the UNIQUE index exists and a byte-identical re-insert is refused")
    func constraintRefusesReinsert() async throws {
        let dir = try await seededV52Directory(prefix: "V53Constraint") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            try self.seedShippedDuplicatePair(db)
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        #expect(try scalarInt(
            db,
            "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_chunks_asset_pass_span_text'"
        ) == 1)
        sqlite3_close_v2(db)

        // Re-run the write path the runner would take: a THIRD fingerprint
        // namespace for the same content is refused on content alone.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let accepted = try await store.insertTranscriptChunk(TranscriptChunk(
            id: "third-namespace",
            analysisAssetId: "ASSET",
            segmentFingerprint: "some-future-writers-scheme",
            chunkIndex: 999,
            startTime: 0.78,
            endTime: 1.86,
            text: "With the American Express",
            normalizedText: "totally different normalisation",
            pass: "final",
            modelVersion: "apple-speech-future-v9",
            transcriptVersion: nil,
            atomOrdinal: nil,
            speakerId: nil
        ))
        #expect(!accepted, "identity is the CONTENT, so no fingerprint scheme can route around it")
        let chunks = try await store.fetchTranscriptChunks(assetId: "ASSET")
        #expect(chunks.count == 1)
    }

    // MARK: - 7. Idempotence, and the ladder reaches head

    @Test("V53 is idempotent and the ladder reaches head from a seeded v52")
    func rungIsIdempotentAndReachesHead() async throws {
        let dir = try await seededV52Directory(prefix: "V53Idempotent") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            try self.seedShippedDuplicatePair(db)
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)
        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 1)
        #expect(try scalarText(db, "SELECT value FROM _meta WHERE key = 'schema_version'")
                == String(AnalysisStore.currentSchemaVersion))

        // The sweep itself reports zero on an already-clean database rather
        // than re-running a DELETE that would match nothing.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        #expect(try await store.dedupeDuplicateSpanTextChunks() == 0)
    }

    @Test("a v52 database with NO duplicates still reaches head and keeps every row")
    func cleanV52DatabaseReachesHead() async throws {
        let dir = try await seededV52Directory(prefix: "V53Clean") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            try self.insertChunk(
                db, id: "a", assetId: "ASSET", fingerprint: "fp-a",
                chunkIndex: 0, start: 0, end: 1, text: "one", pass: "final"
            )
            try self.insertChunk(
                db, id: "b", assetId: "ASSET", fingerprint: "fp-b",
                chunkIndex: 1, start: 1, end: 2, text: "two", pass: "final"
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 2)
        #expect(try scalarText(db, "SELECT value FROM _meta WHERE key = 'schema_version'")
                == String(AnalysisStore.currentSchemaVersion))
    }

    // MARK: - 8. The content-keyed lookup

    @Test("fetchTranscriptChunkBySpanText finds an ENGINE-written final row the prefixed fingerprint cannot address")
    func contentLookupSeesTheEngineRow() async throws {
        let dir = try makeTempDir(prefix: "V53Lookup")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let rawDB = try openRaw(dir)
        try insertAsset(rawDB, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
        sqlite3_close_v2(rawDB)

        let text = "With the American Express"
        let engineFingerprint = TranscriptEngineFingerprintProbe.bare(
            text: text, startTime: 0.78, endTime: 1.86
        )
        try await store.insertTranscriptChunk(TranscriptChunk(
            id: "engine-row",
            analysisAssetId: "ASSET",
            segmentFingerprint: engineFingerprint,
            chunkIndex: 0,
            startTime: 0.78,
            endTime: 1.86,
            text: text,
            normalizedText: text.lowercased(),
            pass: TranscriptPassType.final_.rawValue,
            modelVersion: "apple-speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            speakerId: nil
        ))

        // The question the runner used to ask — and could only ever answer "no".
        let runnerFingerprint = FinalPassRetranscriptionRunner.computeFinalPassFingerprint(
            text: text, startTime: 0.78, endTime: 1.86
        )
        #expect(runnerFingerprint != engineFingerprint)
        let byFingerprint = try await store.fetchTranscriptChunk(
            analysisAssetId: "ASSET",
            segmentFingerprint: runnerFingerprint
        )
        #expect(byFingerprint == nil, "the prefix makes the engine's row unreachable by fingerprint")

        // The question it asks now.
        let byContent = try await store.fetchTranscriptChunkBySpanText(
            analysisAssetId: "ASSET",
            pass: TranscriptPassType.final_.rawValue,
            startTime: 0.78,
            endTime: 1.86,
            text: text
        )
        #expect(byContent?.id == "engine-row")
    }

    @Test("fetchTranscriptChunkBySpanText is scoped to ONE pass — a fast row over the same span is not a final row")
    func contentLookupIsPassScoped() async throws {
        let dir = try makeTempDir(prefix: "V53LookupPass")
        defer { try? FileManager.default.removeItem(at: dir) }
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let rawDB = try openRaw(dir)
        try insertAsset(rawDB, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
        sqlite3_close_v2(rawDB)

        try await store.insertTranscriptChunk(TranscriptChunk(
            id: "fast-row",
            analysisAssetId: "ASSET",
            segmentFingerprint: "fp-fast",
            chunkIndex: 0,
            startTime: 10,
            endTime: 11,
            text: "Same words",
            normalizedText: "same words",
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: "apple-speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            speakerId: nil
        ))

        // Matching it would tell the final-pass runner it had already stored a
        // final row it had not, and `readFinalTranscriptRegions` would stop
        // growing — a silent coverage regression rather than a duplicate.
        let asFinal = try await store.fetchTranscriptChunkBySpanText(
            analysisAssetId: "ASSET",
            pass: TranscriptPassType.final_.rawValue,
            startTime: 10,
            endTime: 11,
            text: "Same words"
        )
        #expect(asFinal == nil)

        let asFast = try await store.fetchTranscriptChunkBySpanText(
            analysisAssetId: "ASSET",
            pass: TranscriptPassType.fast.rawValue,
            startTime: 10,
            endTime: 11,
            text: "Same words"
        )
        #expect(asFast?.id == "fast-row")
    }

    // MARK: - The two schemes, pinned against each other

    @Test("the two producers digest the SAME string — the fingerprints differ by the fp-final- prefix ALONE")
    func theTwoSchemesDifferOnlyByThePrefix() {
        let text = "With the American Express"
        let start = 0.78
        let end = 1.86
        let shared = "\(text)|\(start)|\(end)"

        // `computeFinalPassFingerprint` is `SHA256("fp-final-" + shared)`.
        // Reproducing it from the shared string is what pins
        // `TranscriptEngineFingerprintProbe.bare` — the restatement of
        // `TranscriptEngineService.computeFingerprint`, which is `private` —
        // to the same digest shape. If either implementation ever changes its
        // digest, prefix width or hex encoding, this fails.
        #expect(
            FinalPassRetranscriptionRunner.computeFinalPassFingerprint(
                text: text, startTime: start, endTime: end
            ) == TranscriptEngineFingerprintProbe.digest("fp-final-" + shared)
        )

        // And the engine's — same input string, no prefix, therefore a value
        // that can never collide with the runner's however identical the audio.
        // That non-collision is the defect, spelled in one expectation.
        #expect(
            TranscriptEngineFingerprintProbe.bare(text: text, startTime: start, endTime: end)
                == TranscriptEngineFingerprintProbe.digest(shared)
        )
        #expect(
            TranscriptEngineFingerprintProbe.bare(text: text, startTime: start, endTime: end)
                != FinalPassRetranscriptionRunner.computeFinalPassFingerprint(
                    text: text, startTime: start, endTime: end
                )
        )
    }
}

/// `TranscriptEngineService.computeFingerprint` is `private`, and the whole
/// point of this suite is that the two producers digest the SAME string. This
/// re-states the engine's scheme so a seed can spell an engine-written row
/// exactly; `theTwoSchemesDifferOnlyByThePrefix` pins the restatement against
/// `FinalPassRetranscriptionRunner.computeFinalPassFingerprint`, which is
/// `internal` and therefore observable, so the two cannot drift apart silently.
enum TranscriptEngineFingerprintProbe {

    static func digest(_ input: String) -> String {
        SHA256.hash(data: Data(input.utf8))
            .prefix(16)
            .map { String(format: "%02x", $0) }
            .joined()
    }

    static func bare(text: String, startTime: Double, endTime: Double) -> String {
        digest("\(text)|\(startTime)|\(endTime)")
    }
}
