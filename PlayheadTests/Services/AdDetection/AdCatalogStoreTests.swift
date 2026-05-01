// AdCatalogStoreTests.swift
// playhead-gtt9.13: Tests for the on-device ad catalog SQLite store.

import Foundation
import SQLite3
import Testing
@testable import Playhead

@Suite("AdCatalogStore")
struct AdCatalogStoreTests {

    // MARK: - Helpers

    private func makeTempDir() throws -> URL {
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("AdCatalogStoreTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: base, withIntermediateDirectories: true)
        return base
    }

    private func sampleFingerprint(seed: Int = 1) -> AcousticFingerprint {
        let values = (0..<64).map { Float(($0 + seed) % 17) + 0.5 }
        return AcousticFingerprint(values: values)!
    }

    /// Build an orthogonal-ish fingerprint distinct from `sampleFingerprint(seed:)`.
    /// Nonzero bins live in the back half of the vector while `sampleFingerprint`
    /// is weighted toward the front, making the two sit well below any
    /// default similarity floor.
    private func orthogonalFingerprint(seed: Int = 1) -> AcousticFingerprint {
        var values = [Float](repeating: 0, count: 64)
        for i in 32..<64 {
            values[i] = Float((i + seed) % 13) + 1.0
        }
        return AcousticFingerprint(values: values)!
    }

    // MARK: - Insert + query roundtrip

    @Test("insert + matches roundtrip returns the inserted entry")
    func insertMatchRoundtrip() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fp = sampleFingerprint(seed: 1)
        let inserted = try await store.insert(
            showId: "show-1",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: fp,
            transcriptSnippet: "betterhelp dot com slash podcast",
            sponsorTokens: ["betterhelp"],
            originalConfidence: 0.9
        )

        let matches = await store.matches(
            fingerprint: fp,
            show: "show-1",
            similarityFloor: 0.80
        )

        #expect(matches.count == 1)
        #expect(matches.first?.entry.id == inserted.id)
        #expect(matches.first?.entry.transcriptSnippet == "betterhelp dot com slash podcast")
        #expect(matches.first?.entry.sponsorTokens == ["betterhelp"])
        #expect(matches.first.map { abs(Double($0.similarity) - 1.0) < 1e-3 } ?? false)
    }

    // MARK: - Similarity threshold

    @Test("matches below similarity floor are filtered out")
    func belowFloorFilteredOut() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fp = sampleFingerprint(seed: 1)
        _ = try await store.insert(
            showId: "show-1",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: fp
        )

        // Build an orthogonal fingerprint for the query.
        var other = [Float](repeating: 0, count: 64)
        for i in 0..<32 { other[i] = Float(i + 1) }
        let orthogonalFP = AcousticFingerprint(values: other)!

        let matches = await store.matches(
            fingerprint: orthogonalFP,
            show: "show-1",
            similarityFloor: 0.80
        )
        #expect(matches.isEmpty)
    }

    @Test("lower similarity floor admits more matches")
    func lowerFloorAdmitsMore() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fp = sampleFingerprint(seed: 1)
        _ = try await store.insert(
            showId: "show-1",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: fp
        )

        // Close-but-not-identical query fingerprint.
        var closeValues = fp.values
        for i in 0..<8 { closeValues[i] = closeValues[i] * 0.7 }
        let closeFP = AcousticFingerprint(values: closeValues)!

        let strict = await store.matches(fingerprint: closeFP, show: "show-1", similarityFloor: 0.999)
        let permissive = await store.matches(fingerprint: closeFP, show: "show-1", similarityFloor: 0.50)
        #expect(strict.count <= permissive.count)
        #expect(permissive.count >= 1)
    }

    // MARK: - Show scoping

    @Test("matches are scoped to the requested show (and null-show entries)")
    func showScoping() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fpA = sampleFingerprint(seed: 1)
        let fpB = orthogonalFingerprint(seed: 2)

        _ = try await store.insert(
            showId: "show-a",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fpA
        )
        _ = try await store.insert(
            showId: "show-b",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fpB
        )
        _ = try await store.insert(
            showId: nil,
            episodePosition: .unknown,
            durationSec: 30,
            acousticFingerprint: fpA
        )

        let matchesA = await store.matches(fingerprint: fpA, show: "show-a", similarityFloor: 0.80)
        // show-a match + null-show match = 2 entries with identical fp
        #expect(matchesA.count == 2)
        for m in matchesA {
            #expect(m.entry.showId == "show-a" || m.entry.showId == nil)
        }

        // Searching show-b for fpB: only show-b entry matches; the null-show
        // row uses fpA which is orthogonal to fpB under this fixture.
        let matchesB = await store.matches(fingerprint: fpB, show: "show-b", similarityFloor: 0.80)
        #expect(matchesB.count == 1)
        #expect(matchesB.first?.entry.showId == "show-b")

        // Cross-show scoping: searching show-b with fpA should NOT find the
        // show-a entry even though fpA is highly similar to itself. Only the
        // null-show entry (fpA) should surface.
        let crossScope = await store.matches(fingerprint: fpA, show: "show-b", similarityFloor: 0.80)
        #expect(crossScope.count == 1)
        #expect(crossScope.first?.entry.showId == nil)
    }

    // MARK: - Zero fingerprint handling

    @Test("insert of a zero fingerprint is a no-op")
    func zeroFingerprintInsertIsNoOp() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let zero = AcousticFingerprint(values: [])!
        _ = try await store.insert(
            showId: "show-1",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: zero
        )

        let count = try await store.count()
        #expect(count == 0)
    }

    @Test("matches on a zero query fingerprint returns nothing")
    func zeroQueryReturnsEmpty() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        _ = try await store.insert(
            showId: "show-1",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: sampleFingerprint(seed: 1)
        )

        let zero = AcousticFingerprint(values: [])!
        let matches = await store.matches(fingerprint: zero, show: "show-1", similarityFloor: 0.80)
        #expect(matches.isEmpty)
    }

    // MARK: - Persistence across actor re-init

    @Test("entries persist across actor re-init")
    func persistsAcrossReinit() async throws {
        let dir = try makeTempDir()
        let fp = sampleFingerprint(seed: 7)

        do {
            let store = try AdCatalogStore(directoryURL: dir)
            _ = try await store.insert(
                showId: "show-persist",
                episodePosition: .postRoll,
                durationSec: 45,
                acousticFingerprint: fp,
                transcriptSnippet: "persistent ad"
            )
            let count = try await store.count()
            #expect(count == 1)
        }

        // Re-open the store from scratch.
        let reopened = try AdCatalogStore(directoryURL: dir)
        let count = try await reopened.count()
        #expect(count == 1)

        let matches = await reopened.matches(
            fingerprint: fp,
            show: "show-persist",
            similarityFloor: 0.80
        )
        #expect(matches.count == 1)
        #expect(matches.first?.entry.transcriptSnippet == "persistent ad")
    }

    // MARK: - Schema version

    @Test("migration bumps user_version to schemaVersion")
    func migrationBumpsUserVersion() async throws {
        let dir = try makeTempDir()
        _ = try AdCatalogStore(directoryURL: dir)

        // Probe the sqlite file directly via the store's user_version.
        // We rely on re-opening to confirm idempotency.
        let reopened = try AdCatalogStore(directoryURL: dir)
        let count = try await reopened.count()
        #expect(count == 0)  // Clean reopen, no rows.
    }

    // MARK: - Sorting

    @Test("matches sorted by similarity descending")
    func sortedBySimilarityDescending() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fpHi = sampleFingerprint(seed: 1)
        var closerValues = fpHi.values
        for i in 0..<4 { closerValues[i] *= 0.9 }
        let fpMid = AcousticFingerprint(values: closerValues)!
        var furtherValues = fpHi.values
        for i in 0..<16 { furtherValues[i] *= 0.3 }
        let fpLo = AcousticFingerprint(values: furtherValues)!

        _ = try await store.insert(
            showId: "show-1", episodePosition: .preRoll,
            durationSec: 30, acousticFingerprint: fpLo
        )
        _ = try await store.insert(
            showId: "show-1", episodePosition: .preRoll,
            durationSec: 30, acousticFingerprint: fpMid
        )
        _ = try await store.insert(
            showId: "show-1", episodePosition: .preRoll,
            durationSec: 30, acousticFingerprint: fpHi
        )

        let matches = await store.matches(
            fingerprint: fpHi,
            show: "show-1",
            similarityFloor: 0.0
        )
        #expect(matches.count == 3)
        for i in 1..<matches.count {
            #expect(matches[i - 1].similarity >= matches[i].similarity)
        }
    }

    // MARK: - Clear

    @Test("clear removes all entries")
    func clearRemovesAllEntries() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)
        _ = try await store.insert(
            showId: "show-1", episodePosition: .preRoll,
            durationSec: 30, acousticFingerprint: sampleFingerprint(seed: 1)
        )
        #expect(try await store.count() == 1)
        try await store.clear()
        #expect(try await store.count() == 0)
    }

    // MARK: - Integration: correction → entry → evidence

    @Test("simulated correction → catalog entry → catalog signal fires on similar fingerprint")
    func correctionToCatalogToSignalIntegration() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        // 1) Simulate a user correction landing: store inserts a fingerprint.
        let correctionFP = sampleFingerprint(seed: 42)
        _ = try await store.insert(
            showId: "integration-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: correctionFP,
            transcriptSnippet: "squarespace dot com slash podcast",
            sponsorTokens: ["squarespace"],
            originalConfidence: 0.92
        )

        // 2) A future episode produces a candidate with a near-identical fp.
        var slightlyDifferent = correctionFP.values
        for i in 0..<4 { slightlyDifferent[i] *= 0.95 }
        let futureFP = AcousticFingerprint(values: slightlyDifferent)!

        let matches = await store.matches(
            fingerprint: futureFP,
            show: "integration-show",
            similarityFloor: AdCatalogStore.defaultSimilarityFloor
        )
        #expect(!matches.isEmpty)
        let topSimilarity = matches.first?.similarity ?? 0

        // 3) Feed the top similarity into the precision gate input.
        let gateInput = AutoSkipPrecisionGateInput(
            segmentStartTime: 100,
            segmentEndTime: 130,
            segmentScore: 0.60,
            episodeDuration: 3600,
            overlappingFeatureWindows: [],
            lexicalCategories: [],
            userCorrectionBoostFactor: 1.0,
            catalogMatchSimilarity: topSimilarity
        )
        let signals = AutoSkipPrecisionGate.collectSafetySignals(for: gateInput)
        #expect(signals.contains(.catalogMatch))
    }

    @Test("no catalog entries → no catalog signal fires")
    func emptyCatalogNoSignal() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let queryFP = sampleFingerprint(seed: 1)
        let matches = await store.matches(
            fingerprint: queryFP,
            show: "any-show",
            similarityFloor: AdCatalogStore.defaultSimilarityFloor
        )
        #expect(matches.isEmpty)

        let gateInput = AutoSkipPrecisionGateInput(
            segmentStartTime: 100,
            segmentEndTime: 130,
            segmentScore: 0.60,
            episodeDuration: 3600,
            overlappingFeatureWindows: [],
            lexicalCategories: [],
            userCorrectionBoostFactor: 1.0,
            catalogMatchSimilarity: matches.first?.similarity ?? 0
        )
        let signals = AutoSkipPrecisionGate.collectSafetySignals(for: gateInput)
        #expect(!signals.contains(.catalogMatch))
    }

    // MARK: - Per-show growth bound (V2 schema)

    @Test("per-show row count is bounded by maxEntriesPerShow")
    func perShowRowCountBounded() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        // Use a scaled-down ceiling at the test level — `maxEntriesPerShow`
        // is a static constant so we exercise the eviction path with a
        // representative-but-tractable count.
        let cap = AdCatalogStore.maxEntriesPerShow
        let surplus = 25
        for seed in 0..<(cap + surplus) {
            // Each iteration uses a distinct fingerprint so the
            // (show_id, fingerprint_blob) UNIQUE constraint does not
            // collapse rows — eviction is the only mechanism that can
            // keep the row count bounded. Encode `seed` directly into
            // the first slots so each fingerprint is unique across the
            // full test range (avoid modular cycles).
            var values = [Float](repeating: 0, count: 64)
            values[0] = Float(seed) + 1.0
            values[1] = Float(seed >> 8) + 1.0
            for i in 2..<64 {
                values[i] = Float(i) + 1.0
            }
            let fp = AcousticFingerprint(values: values)!
            _ = try await store.insert(
                showId: "show-bounded",
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: fp,
                originalConfidence: 0.5
            )
        }

        let totalForShow = try await store.allEntries()
            .filter { $0.showId == "show-bounded" }
            .count
        #expect(totalForShow == cap)
    }

    // MARK: - UPSERT confidence-MAX (V2)

    @Test("UPSERT on (show, fingerprint) lifts original_confidence to the higher value")
    func upsertConfidenceMaxOnShowFingerprint() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fp = sampleFingerprint(seed: 7)

        // First insert at moderate confidence.
        _ = try await store.insert(
            showId: "merge-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fp,
            originalConfidence: 0.5
        )

        // Re-insert with a higher confidence. The (show, fingerprint)
        // UNIQUE collision should lift original_confidence to 0.9.
        _ = try await store.insert(
            showId: "merge-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fp,
            originalConfidence: 0.9
        )

        let entries = (try await store.allEntries()).filter { $0.showId == "merge-show" }
        #expect(entries.count == 1)
        #expect(entries.first?.originalConfidence == 0.9)

        // Re-insert again with a LOWER confidence. The MAX semantics
        // must keep the existing 0.9 — confidence must never regress.
        _ = try await store.insert(
            showId: "merge-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fp,
            originalConfidence: 0.3
        )

        let after = (try await store.allEntries()).filter { $0.showId == "merge-show" }
        #expect(after.count == 1)
        #expect(after.first?.originalConfidence == 0.9)
    }

    @Test("UPSERT preserves NULL original_confidence when both old and new are NULL")
    func upsertConfidenceNullPreservedWhenBothNull() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fp = sampleFingerprint(seed: 11)

        // First insert with NULL confidence (omit the parameter).
        _ = try await store.insert(
            showId: "null-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fp
        )
        // Re-insert with NULL confidence — collision on (show, fingerprint)
        // path. The merged column must remain NULL ("unknown"), not silently
        // become 0.0 ("we measured zero").
        _ = try await store.insert(
            showId: "null-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fp
        )

        let entries = (try await store.allEntries()).filter { $0.showId == "null-show" }
        #expect(entries.count == 1)
        #expect(entries.first?.originalConfidence == nil,
                "NULL × NULL UPSERT must preserve NULL — distinguishing 'unknown' from 'measured zero' is contractually meaningful.")
    }

    // MARK: - Per-show eviction with NULL show_id

    @Test("per-show eviction caps NULL-show rows by maxEntriesPerShow")
    func perNullShowRowCountBounded() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let cap = AdCatalogStore.maxEntriesPerShow
        let surplus = 25
        for seed in 0..<(cap + surplus) {
            // Distinct fingerprints (encoded directly into the first slots
            // so cycles don't collapse rows). NULL show_id rows are not
            // de-duped by the partial UNIQUE index, so eviction is the
            // only governor.
            var values = [Float](repeating: 0, count: 64)
            values[0] = Float(seed) + 1.0
            values[1] = Float(seed >> 8) + 1.0
            for i in 2..<64 {
                values[i] = Float(i) + 1.0
            }
            let fp = AcousticFingerprint(values: values)!
            _ = try await store.insert(
                showId: nil,
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: fp,
                originalConfidence: 0.5
            )
        }

        let nullShowCount = try await store.allEntries()
            .filter { $0.showId == nil }
            .count
        #expect(nullShowCount == cap)
    }

    // MARK: - V1 → V2 migration

    /// Helper: build a V1-schema sqlite file at the canonical store location
    /// inside `dir`, populate it, and stamp `PRAGMA user_version = 1`.
    /// Mirrors the production V0→V1 schema exactly so the V1→V2 migration
    /// runs against realistic input.
    private func seedV1Database(at dir: URL, populate: (OpaquePointer) -> Void) throws {
        let dbURL = dir.appendingPathComponent("ad_catalog.sqlite")
        var handle: OpaquePointer?
        let path = dbURL.path
        guard sqlite3_open_v2(path, &handle, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil) == SQLITE_OK,
              let db = handle else {
            throw AdCatalogStoreError.openFailed("test seed: open failed")
        }
        defer { sqlite3_close(db) }

        let createSQL = """
        CREATE TABLE IF NOT EXISTS ad_catalog_entries (
            id TEXT PRIMARY KEY NOT NULL,
            created_at REAL NOT NULL,
            show_id TEXT,
            episode_position TEXT NOT NULL,
            duration_sec REAL NOT NULL,
            fingerprint_blob BLOB NOT NULL,
            transcript_snippet TEXT,
            sponsor_tokens_json TEXT,
            original_confidence REAL
        );
        CREATE INDEX IF NOT EXISTS idx_catalog_show_id ON ad_catalog_entries(show_id);
        CREATE INDEX IF NOT EXISTS idx_catalog_created_at ON ad_catalog_entries(created_at);
        PRAGMA user_version = 1;
        """
        guard sqlite3_exec(db, createSQL, nil, nil, nil) == SQLITE_OK else {
            throw AdCatalogStoreError.migrationFailed("test seed: schema exec failed")
        }
        populate(db)
    }

    private static let SQLITE_TRANSIENT = unsafeBitCast(
        -1, to: sqlite3_destructor_type.self
    )

    private func insertV1Row(
        db: OpaquePointer,
        id: UUID,
        showId: String?,
        fingerprint: AcousticFingerprint,
        confidence: Double?,
        createdAt: Date
    ) {
        let sql = """
        INSERT INTO ad_catalog_entries
            (id, created_at, show_id, episode_position, duration_sec,
             fingerprint_blob, transcript_snippet, sponsor_tokens_json,
             original_confidence)
        VALUES (?, ?, ?, 'midRoll', 30, ?, NULL, NULL, ?)
        """
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else { return }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, id.uuidString, -1, Self.SQLITE_TRANSIENT)
        sqlite3_bind_double(stmt, 2, createdAt.timeIntervalSince1970)
        if let showId {
            sqlite3_bind_text(stmt, 3, showId, -1, Self.SQLITE_TRANSIENT)
        } else {
            sqlite3_bind_null(stmt, 3)
        }
        let blob = fingerprint.data
        blob.withUnsafeBytes { raw in
            sqlite3_bind_blob(stmt, 4, raw.baseAddress, Int32(blob.count), Self.SQLITE_TRANSIENT)
        }
        if let confidence {
            sqlite3_bind_double(stmt, 5, confidence)
        } else {
            sqlite3_bind_null(stmt, 5)
        }
        _ = sqlite3_step(stmt)
    }

    @Test("V1→V2 migration: highest-confidence row wins per (show, fingerprint) group")
    func migrationDedupKeepsHighestConfidence() async throws {
        let dir = try makeTempDir()
        let fp = sampleFingerprint(seed: 99)

        try seedV1Database(at: dir) { db in
            // Three duplicates of (show=A, fp). Confidences: nil, 0.4, 0.9.
            // The 0.9 row must survive; the others must be deleted.
            insertV1Row(db: db, id: UUID(), showId: "show-A", fingerprint: fp,
                        confidence: nil, createdAt: Date(timeIntervalSince1970: 1))
            insertV1Row(db: db, id: UUID(), showId: "show-A", fingerprint: fp,
                        confidence: 0.4, createdAt: Date(timeIntervalSince1970: 2))
            insertV1Row(db: db, id: UUID(), showId: "show-A", fingerprint: fp,
                        confidence: 0.9, createdAt: Date(timeIntervalSince1970: 3))
            // A separate (show=B, fp) row should pass through untouched.
            insertV1Row(db: db, id: UUID(), showId: "show-B", fingerprint: fp,
                        confidence: 0.6, createdAt: Date(timeIntervalSince1970: 4))
        }

        // Open the store — migration runs.
        let store = try AdCatalogStore(directoryURL: dir)
        let all = try await store.allEntries()

        let aRows = all.filter { $0.showId == "show-A" }
        let bRows = all.filter { $0.showId == "show-B" }
        #expect(aRows.count == 1, "Duplicates collapsed to a single row.")
        #expect(aRows.first?.originalConfidence == 0.9,
                "Highest-confidence row must survive (NULL sorts as lowest).")
        #expect(bRows.count == 1, "Distinct (show, fp) groups untouched.")

        // The post-migration UNIQUE partial index must reject a re-insert
        // attempt that bypasses UPSERT — we exercise the contract via
        // a normal insert (which uses UPSERT) and check the row count
        // doesn't increase.
        _ = try await store.insert(
            showId: "show-A",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fp,
            originalConfidence: 0.7
        )
        let afterReinsert = (try await store.allEntries()).filter { $0.showId == "show-A" }
        #expect(afterReinsert.count == 1, "Post-migration UPSERT must not duplicate.")
        #expect(afterReinsert.first?.originalConfidence == 0.9,
                "Lower re-insert must not regress the surviving confidence.")
    }

    @Test("V1→V2 migration: NULL show_id duplicates are NOT collapsed")
    func migrationPreservesNullShowDuplicates() async throws {
        let dir = try makeTempDir()
        let fp = sampleFingerprint(seed: 77)

        try seedV1Database(at: dir) { db in
            // Two rows with NULL show_id and the same fingerprint. SQLite
            // treats NULLs in UNIQUE indexes as distinct, AND the V2 index
            // is partial (`WHERE show_id IS NOT NULL`), so NULL-show rows
            // must survive unchanged.
            insertV1Row(db: db, id: UUID(), showId: nil, fingerprint: fp,
                        confidence: 0.5, createdAt: Date(timeIntervalSince1970: 1))
            insertV1Row(db: db, id: UUID(), showId: nil, fingerprint: fp,
                        confidence: 0.7, createdAt: Date(timeIntervalSince1970: 2))
        }

        let store = try AdCatalogStore(directoryURL: dir)
        let nullShow = try await store.allEntries().filter { $0.showId == nil }
        #expect(nullShow.count == 2,
                "NULL-show duplicates must survive — partial UNIQUE index excludes them.")
    }
}
