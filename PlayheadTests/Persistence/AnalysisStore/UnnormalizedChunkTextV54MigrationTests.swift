import Foundation
import SQLite3
import Testing

@testable import Playhead

// UnnormalizedChunkTextV54MigrationTests.swift
//
// playhead-gjxf — `transcript_chunks.normalizedText` must hold what its name
// says: `TranscriptEngineService.normalizeText(text)`.
//
// THE DEFECT. `FinalPassRetranscriptionRunner` built that column with
// `segment.text.lowercased()`. Lowercasing is the FIRST of the canonical
// normalizer's three steps — the other two, stripping non-alphanumeric scalars
// and collapsing on single spaces, never ran. So the runner's rows stored RAW
// text under a name that promises normalized text. Measured on the 2026-08-15
// pull: 3,825 of 55,005 rows, every one of them the runner's
// (`modelVersion='apple-speech-final-v1'`); all 30,125 `fast` rows and all
// 17,632 `pass='final'` rows written by `TranscriptEngineService` were correct.
//
// WHY IT COSTS DETECTION. `LexicalScanner.scanChunk` runs every built-in
// pattern group over `normalizedText`, and those patterns are written FOR
// normalized text (`go to \w+ com`). `TranscriptChunkCanonicalizer` retains the
// FINAL row of a fast/final twin and drops the fast one, so the row detection
// reads is precisely the broken one.
//
// ⚠️ EVERY FIXTURE HERE USES PUNCTUATED TEXT, AND THAT IS LOAD-BEARING.
// `normalizeText(s)` and `s.lowercased()` return the SAME STRING for any `s`
// with no punctuation, so a rail built on unpunctuated text cannot distinguish
// the broken writer from the fixed one and passes forever. That is not
// hypothetical: playhead-jc42's JC04 rail was born dead for exactly this
// reason, and it is why 21,055 of the 24,880 `pass='final'` rows on the pull
// looked "correct" while the writer that produced them was broken.
// `unpunctuatedTextCannotSeeTheDefect` pins the trap itself so the next author
// meets it as a test rather than as a survivor.
@Suite("Un-normalized chunk normalizedText V54 migration (playhead-gjxf)")
struct UnnormalizedChunkTextV54MigrationTests {

    // MARK: - Raw SQLite helpers

    private func openRaw(_ directory: URL) throws -> OpaquePointer {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK,
              let handle = db
        else { throw NSError(domain: "UnnormalizedChunkV54", code: 1) }
        return handle
    }

    private func exec(_ db: OpaquePointer, _ sql: String) throws {
        var message: UnsafeMutablePointer<CChar>?
        guard sqlite3_exec(db, sql, nil, nil, &message) == SQLITE_OK else {
            let text = message.map { String(cString: $0) } ?? "unknown"
            sqlite3_free(message)
            throw NSError(
                domain: "UnnormalizedChunkV54",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "\(sql) -> \(text)"]
            )
        }
    }

    private func scalarInt(_ db: OpaquePointer, _ sql: String) throws -> Int64 {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(
                domain: "UnnormalizedChunkV54",
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
                domain: "UnnormalizedChunkV54",
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

    /// The shipped shape, spelled exactly as the runner spelled it: `text`
    /// carrying punctuation, `normalizedText` carrying `text.lowercased()`.
    /// This is one of the real strings from the 2026-08-15 pull.
    private static let brokenText = "Go to Ketone.com, and use code DOAC."
    /// What the canonical normalizer produces from it — and what the built-in
    /// `go to \w+ com` / `use code \w+` patterns are written to match.
    private static let brokenTextNormalized = "go to ketone com and use code doac"

    /// Build a head-schema store, then rewind `_meta.schema_version` to 53 so
    /// the V54 rung is the only thing left to run.
    ///
    /// The rewind happens AFTER `migrate()` so every table — including the FTS
    /// virtual table and its three triggers — exists in its real, current
    /// shape. This is an UPGRADE-PATH fixture, not a hand-built schema.
    private func seededV53Directory(
        prefix: String,
        seed: (OpaquePointer) throws -> Void
    ) async throws -> URL {
        let dir = try makeTempDir(prefix: prefix)
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        try seed(db)
        try exec(db, "UPDATE _meta SET value = '53' WHERE key = 'schema_version'")
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

    /// `normalizedText` defaults to the RUNNER's broken spelling
    /// (`text.lowercased()`) because that is the population this rung exists to
    /// repair. Pass it explicitly to seed a row that is already correct.
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
        modelVersion: String = "apple-speech-final-v1",
        normalizedText: String? = nil
    ) throws {
        let normalized = normalizedText ?? text.lowercased()
        try exec(db, """
            INSERT INTO transcript_chunks
            (id, analysisAssetId, segmentFingerprint, chunkIndex, startTime, endTime,
             text, normalizedText, pass, modelVersion, transcriptVersion, atomOrdinal)
            VALUES (\(quoted(id)), \(quoted(assetId)), \(quoted(fingerprint)), \(chunkIndex),
                    \(start), \(end), \(quoted(text)), \(quoted(normalized)),
                    \(quoted(pass)), \(quoted(modelVersion)), NULL, NULL)
            """)
    }

    private func migrateAgain(_ dir: URL) async throws {
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
    }

    // MARK: - The repair

    @Test("a runner row storing RAW text is re-normalized to the canonical spelling")
    func runnerRowWithPunctuationIsRenormalized() async throws {
        let dir = try await seededV53Directory(prefix: "V54Broken") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            try self.insertChunk(
                db, id: "broken", assetId: "ASSET", fingerprint: "fp-final-broken",
                chunkIndex: 0, start: 0, end: 3, text: Self.brokenText
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        // Precondition — the fixture really does carry the defect. Without this
        // the test could pass on a fixture that was never broken.
        do {
            let db = try openRaw(dir)
            defer { sqlite3_close_v2(db) }
            #expect(
                try scalarText(db, "SELECT normalizedText FROM transcript_chunks WHERE id = 'broken'")
                    == Self.brokenText.lowercased()
            )
        }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(
            try scalarText(db, "SELECT normalizedText FROM transcript_chunks WHERE id = 'broken'")
                == Self.brokenTextNormalized
        )
        // `text` is the evidence and is never rewritten.
        #expect(
            try scalarText(db, "SELECT text FROM transcript_chunks WHERE id = 'broken'")
                == Self.brokenText
        )
        #expect(
            try scalarText(db, "SELECT value FROM _meta WHERE key = 'schema_version'")
                == String(AnalysisStore.currentSchemaVersion)
        )
    }

    @Test("the repaired value is EXACTLY TranscriptEngineService.normalizeText, on every punctuation shape")
    func repairMatchesTheCanonicalNormalizerExactly() async throws {
        // Each of these exercises a different way the two implementations
        // diverge: a dotted domain, a slashed path, doubled punctuation
        // producing an EMPTY component the normalizer must drop, leading and
        // trailing punctuation, and an em-dash between words.
        let texts = [
            "Go to Ketone.com, and use code DOAC.",
            "functionhealth.com/doac",
            "Wait -- what?!  Really...",
            "  ...leading and trailing!!  ",
            "It's a two—word thing.",
            "50% off, today only.",
        ]
        let dir = try await seededV53Directory(prefix: "V54Exact") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            for (index, text) in texts.enumerated() {
                try self.insertChunk(
                    db, id: "c\(index)", assetId: "ASSET", fingerprint: "fp-final-\(index)",
                    chunkIndex: index, start: Double(index), end: Double(index) + 1, text: text
                )
            }
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        for (index, text) in texts.enumerated() {
            let stored = try scalarText(
                db, "SELECT normalizedText FROM transcript_chunks WHERE id = 'c\(index)'"
            )
            #expect(
                stored == TranscriptEngineService.normalizeText(text),
                "row \(index) (\(text)) stored \(stored ?? "nil")"
            )
        }
    }

    @Test("a row that was ALREADY correct is left byte-identical and reported as no work")
    func alreadyCorrectRowIsNotTouched() async throws {
        let correct = TranscriptEngineService.normalizeText(Self.brokenText)
        let dir = try await seededV53Directory(prefix: "V54Clean") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            try self.insertChunk(
                db, id: "engine", assetId: "ASSET", fingerprint: "fp-engine",
                chunkIndex: 0, start: 0, end: 3, text: Self.brokenText,
                modelVersion: "apple-speech-v1", normalizedText: correct
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(
            try scalarText(db, "SELECT normalizedText FROM transcript_chunks WHERE id = 'engine'")
                == correct
        )

        // And the sweep itself reports zero rather than rewriting a row to the
        // value it already holds. This is the "does not touch correct rows"
        // property stated as a count, which an equality check alone cannot see:
        // an UPDATE to the identical value is invisible in the column and still
        // fires the FTS triggers.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        #expect(try await store.renormalizeTranscriptChunkNormalizedText() == 0)
    }

    @Test("the sweep repairs the broken row and leaves the correct one alone in the SAME table")
    func onlyTheBrokenRowIsRewritten() async throws {
        let correct = TranscriptEngineService.normalizeText(Self.brokenText)
        let dir = try await seededV53Directory(prefix: "V54Mixed") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            // The engine's row, already correct.
            try self.insertChunk(
                db, id: "engine", assetId: "ASSET", fingerprint: "fp-engine",
                chunkIndex: 0, start: 0, end: 3, text: Self.brokenText,
                modelVersion: "apple-speech-v1", normalizedText: correct
            )
            // The runner's row over DIFFERENT audio, broken.
            try self.insertChunk(
                db, id: "runner", assetId: "ASSET", fingerprint: "fp-final-runner",
                chunkIndex: 1, start: 4, end: 7, text: Self.brokenText
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        // The sweep reports EXACTLY ONE repair, not two — the count is what
        // distinguishes "repaired the broken row" from "rewrote the table".
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(
            try scalarText(db, "SELECT normalizedText FROM transcript_chunks WHERE id = 'runner'")
                == correct
        )
        #expect(
            try scalarText(db, "SELECT normalizedText FROM transcript_chunks WHERE id = 'engine'")
                == correct
        )
        // Nothing left to do: both rows now satisfy the invariant.
        #expect(try await store.renormalizeTranscriptChunkNormalizedText() == 0)
    }

    @Test("the rung is idempotent and a clean v53 database reaches head untouched")
    func rungIsIdempotentAndReachesHead() async throws {
        let dir = try await seededV53Directory(prefix: "V54Idem") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            try self.insertChunk(
                db, id: "broken", assetId: "ASSET", fingerprint: "fp-final-broken",
                chunkIndex: 0, start: 0, end: 3, text: Self.brokenText
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)
        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        #expect(
            try scalarText(db, "SELECT normalizedText FROM transcript_chunks WHERE id = 'broken'")
                == Self.brokenTextNormalized
        )
        #expect(try scalarInt(db, "SELECT count(*) FROM transcript_chunks") == 1)
        #expect(
            try scalarText(db, "SELECT value FROM _meta WHERE key = 'schema_version'")
                == String(AnalysisStore.currentSchemaVersion)
        )
    }

    @Test("the repair leaves no FTS ghost: the raw spelling stops matching and the normalized one starts")
    func repairLeavesNoFTSGhost() async throws {
        let dir = try await seededV53Directory(prefix: "V54FTS") { db in
            try self.insertAsset(db, id: "ASSET", episodeId: "ep-1", fingerprint: "sha-asset")
            try self.insertChunk(
                db, id: "broken", assetId: "ASSET", fingerprint: "fp-final-broken",
                chunkIndex: 0, start: 0, end: 3, text: Self.brokenText
            )
        }
        defer { try? FileManager.default.removeItem(at: dir) }

        try await migrateAgain(dir)

        let db = try openRaw(dir)
        defer { sqlite3_close_v2(db) }
        // The FTS index is external-content and synchronised by the
        // `transcript_chunks_au` trigger. If the trigger's `'delete'` half had
        // corrupted the index instead of clearing it, the OLD token stream
        // would still be searchable — a ghost the lexical scanner keeps
        // matching. `ketone` is a token only the repaired spelling produces
        // standing alone; the raw `ketone.com` tokenizes differently.
        #expect(
            try scalarInt(
                db,
                "SELECT count(*) FROM transcript_chunks_fts WHERE transcript_chunks_fts MATCH 'normalizedText : ketone'"
            ) == 1
        )
        // And the index is internally consistent — `integrity-check` is FTS5's
        // own verdict on whether the content table and the index agree.
        try exec(db, "INSERT INTO transcript_chunks_fts(transcript_chunks_fts, rank) VALUES('integrity-check', 0)")
    }

    // MARK: - The product effect

    @Test("LexicalScanner recovers a urlCTA hit the raw spelling could not produce")
    func lexicalScannerRecoversUrlCtaHit() async throws {
        func chunk(normalized: String) -> TranscriptChunk {
            TranscriptChunk(
                id: "c", analysisAssetId: "ASSET", segmentFingerprint: "fp",
                chunkIndex: 0, startTime: 0, endTime: 3,
                text: Self.brokenText, normalizedText: normalized,
                pass: TranscriptPassType.final_.rawValue,
                modelVersion: "apple-speech-final-v1",
                transcriptVersion: nil, atomOrdinal: nil,
                weakAnchorMetadata: nil, speakerId: nil
            )
        }
        let scanner = LexicalScanner()

        // BEFORE: the column holds raw text. `go to \w+ com` cannot match
        // `go to ketone.com,` because `.` is not a `\w`.
        let before = scanner.scanChunk(chunk(normalized: Self.brokenText.lowercased()))
        #expect(!before.contains { $0.matchedText == "go to ketone com" })

        // AFTER: the column holds what its name says.
        let after = scanner.scanChunk(
            chunk(normalized: TranscriptEngineService.normalizeText(Self.brokenText))
        )
        #expect(after.contains { $0.matchedText == "go to ketone com" })

        // WHICH PATTERNS THE DEFECT CAN REACH, pinned in the same fixture,
        // because the first version of this rail got it wrong and the run said
        // so. The bug costs a hit only when the match must SPAN a character
        // normalization would have stripped. `go to \w+ com` does — the raw
        // text has `ketone.com` where the pattern needs `ketone com`, and `.`
        // is not a `\w`. `use code \w+` does NOT: the phrase `use code doac`
        // carries no internal punctuation, so `\w+` simply stops at the
        // trailing `.` and the pattern matches BOTH spellings.
        //
        // This is why the corpus recovery is 3 hits and not hundreds: it is
        // bounded by the patterns that straddle punctuation, not by the 3,825
        // broken rows. Asserting it here stops a future reader from reading the
        // rail above as "the repair recovers every lexical hit".
        #expect(before.contains { $0.matchedText == "use code doac" })
        #expect(after.contains { $0.matchedText == "use code doac" })
    }

    // MARK: - Anti-vacuity

    @Test("UNPUNCTUATED text cannot see this defect — the trap that killed jc42's JC04")
    func unpunctuatedTextCannotSeeTheDefect() {
        // This is not a test of production behaviour. It is a test of the
        // FIXTURES above: it proves that the punctuation in `brokenText` is
        // what makes every other rail in this suite falsifiable, by showing
        // that without punctuation the broken writer and the correct one are
        // literally the same function.
        let unpunctuated = "with the american express"
        #expect(
            TranscriptEngineService.normalizeText(unpunctuated) == unpunctuated.lowercased(),
            "a rail built on unpunctuated text cannot distinguish the two normalizers"
        )
        // And the fixture this suite actually uses DOES distinguish them.
        #expect(
            TranscriptEngineService.normalizeText(Self.brokenText) != Self.brokenText.lowercased(),
            "brokenText must diverge under the two normalizers or every rail here is vacuous"
        )
        // Pinned literally, so a future edit to `brokenText` that quietly
        // removed the punctuation would fail here rather than silently
        // disarming the suite.
        #expect(Self.brokenTextNormalized == TranscriptEngineService.normalizeText(Self.brokenText))
    }
}
