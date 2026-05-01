// AdCatalogStore.swift
// playhead-gtt9.13: On-device catalog of confirmed ad spans keyed by
// acoustic fingerprint, lexical snippet, and sponsor tokens. Used as a
// precision signal on future episodes — if a candidate window's
// fingerprint matches a stored entry above `similarityFloor`, the
// detector can lower its classifier threshold (gtt9.11 `catalogMatch`
// safety signal) and the fusion path emits `catalog` evidence.
//
// Ingress (when to insert)
// ------------------------
//   1. User corrects an ad span (UserCorrectionStore path).
//   2. SkipOrchestrator auto-skips a window AND the skip was consumed
//      (not suppressed). `markOnly` decisions from gtt9.11 do NOT
//      insert — those aren't confirmed ads yet.
//
// Egress (when to query)
// ----------------------
//   1. AdDetection fusion path looks up each candidate span via
//      `matches(fingerprint:show:similarityFloor:)`, feeding the top
//      similarity into the `catalog` evidence source.
//   2. AutoSkipPrecisionGate fires `catalogMatch` when any stored entry
//      for the same show scores above `similarityFloor`.
//
// Persistence
// -----------
// Self-contained SQLite file (`ad_catalog.sqlite`) in Application
// Support. Schema is versioned via `PRAGMA user_version`. The store is
// deliberately independent of `AnalysisStore` so a catalog-file
// corruption cannot take the main analysis DB down with it.
//
// On-device mandate (legal)
// -------------------------
// Fingerprints + transcript snippets + sponsor tokens NEVER leave the
// device. This store has no export path. Backup inclusion follows the
// file-protection attributes of its containing directory.

import Foundation
import OSLog
import SQLite3

// MARK: - Supporting types

/// Position of an ad within its episode — used as a coarse prior for
/// catalog-matching and for diagnostics.
enum CatalogEpisodePosition: String, Sendable, Hashable, Codable, CaseIterable {
    case preRoll
    case midRoll
    case postRoll
    case unknown
}

/// A single stored ad-catalog entry. Uniquely identified by `id`; the
/// `(showId, createdAt)` pair is a secondary lookup key.
struct CatalogEntry: Sendable, Hashable {
    let id: UUID
    let createdAt: Date
    /// Podcast / show identifier; `nil` when the originating span had no
    /// show attribution (rare, but we don't drop the entry — it still
    /// matches globally under `show == nil`).
    let showId: String?
    let episodePosition: CatalogEpisodePosition
    /// Duration of the fingerprinted span in seconds.
    let durationSec: Double
    let acousticFingerprint: AcousticFingerprint
    /// Short transcript excerpt (for replay / debugging). Nil when the
    /// transcript wasn't available at insert time.
    let transcriptSnippet: String?
    /// Normalized sponsor brand tokens extracted from the span, if any.
    let sponsorTokens: [String]?
    /// Classifier confidence at the moment the entry was inserted.
    let originalConfidence: Double?

    init(
        id: UUID = UUID(),
        createdAt: Date = Date(),
        showId: String?,
        episodePosition: CatalogEpisodePosition,
        durationSec: Double,
        acousticFingerprint: AcousticFingerprint,
        transcriptSnippet: String? = nil,
        sponsorTokens: [String]? = nil,
        originalConfidence: Double? = nil
    ) {
        self.id = id
        self.createdAt = createdAt
        self.showId = showId
        self.episodePosition = episodePosition
        self.durationSec = durationSec
        self.acousticFingerprint = acousticFingerprint
        self.transcriptSnippet = transcriptSnippet
        self.sponsorTokens = sponsorTokens
        self.originalConfidence = originalConfidence
    }
}

/// A match returned by `AdCatalogStore.matches(...)`. Similarity is a
/// cosine in `[0, 1]` — higher is closer.
struct CatalogMatch: Sendable, Hashable {
    let entry: CatalogEntry
    let similarity: Float
}

// MARK: - Errors

enum AdCatalogStoreError: Error, Equatable {
    case openFailed(String)
    case migrationFailed(String)
    case insertFailed(String)
    case queryFailed(String)
}

// MARK: - Store

/// Actor-backed catalog store. All reads and writes are serialized on the
/// store's private executor so SQLite's single-connection model is
/// honored without an explicit mutex.
actor AdCatalogStore {

    // MARK: Constants

    /// Schema version stamped into `PRAGMA user_version`. Bumped on any
    /// schema change; `migrate()` handles forward migrations.
    /// V1 → V2: dedup + UNIQUE index on (show_id, fingerprint_blob),
    /// per-show row cap with LRU eviction.
    static let schemaVersion: Int32 = 2

    /// Default similarity floor used when callers don't override it.
    /// 0.80 is deliberately conservative — the bead targets 15% firing
    /// rate on the 2026-04-23 corpus and false-positive catalog matches
    /// are expensive (they can promote a non-ad to auto-skip-eligible).
    static let defaultSimilarityFloor: Float = 0.80

    /// Maximum entries retained per (show_id) bucket. Rows beyond this
    /// are evicted by `created_at ASC` after each insert. Total catalog
    /// bound is therefore O(maxEntriesPerShow × distinct shows). 500 is
    /// chosen so a heavy listener with 50 shows still fits in <25k rows
    /// while preserving multiple ad fingerprints per show.
    static let maxEntriesPerShow: Int = 500

    // MARK: State

    /// Path to the SQLite database file. Nonisolated so deinit (and
    /// test scaffolding in the same module) can read it safely.
    nonisolated let dbURL: URL

    /// Raw SQLite handle. Marked `nonisolated(unsafe)` so deinit can
    /// close it without requiring actor isolation under Swift 6 strict
    /// concurrency. All real usage is funnelled through actor-isolated
    /// methods. Pattern matches `AnalysisStore`.
    nonisolated(unsafe) private var db: OpaquePointer?

    private let logger = Logger(subsystem: "com.playhead", category: "AdCatalogStore")

    // MARK: - Lifecycle

    /// Open or create the catalog database at `directoryURL`, creating
    /// the directory if needed. The file is named `ad_catalog.sqlite`.
    ///
    /// playhead-jndk: init is now lightweight — it stores `dbURL` and
    /// creates the parent directory but defers `sqlite3_open_v2`, PRAGMA
    /// setup, and schema migration to the first call into a public
    /// method. The previous synchronous-DDL-in-init path was on the main
    /// thread of `PlayheadRuntime.init`, extending the launch-storyboard
    /// window during cold launches with a stale WAL (424 KB observed on
    /// the 2026-04-25 22:42 snapshot, requiring full WAL replay before
    /// the first PRAGMA could complete). Mirrors the pattern in
    /// `AnalysisStore.init` + `AnalysisStore.migrate()`.
    ///
    /// Throwing is preserved on `createDirectory` failure so callers
    /// can detect a fundamentally broken filesystem at construction
    /// time. Open / pragma / migration errors surface on the first
    /// real operation through `AdCatalogStoreError`.
    init(directoryURL: URL) throws {
        try FileManager.default.createDirectory(
            at: directoryURL,
            withIntermediateDirectories: true
        )
        self.dbURL = directoryURL.appendingPathComponent("ad_catalog.sqlite")
    }

    /// playhead-jndk: lazy first-use bootstrap. Opens the database
    /// connection (`sqlite3_open_v2`), applies WAL/foreign-key/secure-
    /// delete PRAGMAs, and runs schema migration. Idempotent: subsequent
    /// calls observe the already-open handle and short-circuit. All
    /// public read/write methods call this before touching `db`, so the
    /// expensive setup happens off the main thread, inside the first
    /// caller's actor task.
    private func ensureOpen() throws {
        if db != nil { return }

        var handle: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        let rc = sqlite3_open_v2(dbURL.path, &handle, flags, nil)
        guard rc == SQLITE_OK, let handle else {
            let msg = handle.map { String(cString: sqlite3_errmsg($0)) } ?? "sqlite3_open_v2 rc=\(rc)"
            if let handle { sqlite3_close(handle) }
            throw AdCatalogStoreError.openFailed(msg)
        }

        do {
            // WAL for durable single-writer concurrency; FOREIGN_KEYS for
            // future joins; SECURE_DELETE so overwritten rows don't leave
            // fingerprint bytes lying around (legal: on-device-only mandate
            // implies we treat these bytes as sensitive).
            try Self.exec(handle, "PRAGMA journal_mode=WAL")
            try Self.exec(handle, "PRAGMA foreign_keys=ON")
            try Self.exec(handle, "PRAGMA secure_delete=ON")
            try Self.migrate(handle: handle, logger: logger)
        } catch {
            sqlite3_close(handle)
            throw error
        }

        self.db = handle
    }

    /// Public migration entry point. Idempotent. Production callers
    /// (`PlayheadRuntime`) `await` this once during deferred startup so
    /// the first hot-path query sees an already-bootstrapped store.
    /// Tests that exercise public methods directly may skip this — every
    /// public method calls `ensureOpen()` internally.
    func migrate() throws {
        try ensureOpen()
    }

    deinit {
        // The actor's isolation means we cannot touch `db` directly from
        // deinit under Swift 6 strict concurrency (`OpaquePointer?` is
        // not `Sendable`). SQLite connections own no thread affinity, so
        // closing via a raw `UnsafeMutablePointer<OpaquePointer?>` read
        // is safe — we're the last reference. Route through a local.
        if let handle = db {
            sqlite3_close(handle)
        }
    }

    /// Explicitly close the database. Idempotent. Call this when the
    /// store will be deallocated and you want a deterministic close
    /// before `deinit` (e.g., before deleting the directory in tests).
    func close() {
        if let handle = db {
            sqlite3_close(handle)
            db = nil
        }
    }

    /// Default store location inside Application Support. Used by the
    /// app container; tests inject a temp dir.
    static func defaultDirectory() throws -> URL {
        let fm = FileManager.default
        let base = try fm.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        return base.appendingPathComponent("AdCatalog", isDirectory: true)
    }

    // MARK: - Public API

    /// Insert a new catalog entry. Idempotent on `id` — if an entry with
    /// the same id is already present, the row is replaced (used by the
    /// rare "user re-marks the exact same span" path).
    func insert(entry: CatalogEntry) throws {
        try ensureOpen()
        guard let db else {
            throw AdCatalogStoreError.insertFailed("database closed")
        }
        if entry.acousticFingerprint.isZero {
            // Refuse to store fingerprints that will never match — this
            // keeps the table signal-dense and avoids polluting matches
            // with zero-similarity noise.
            logger.debug("insert: skipping zero fingerprint (id=\(entry.id, privacy: .public))")
            return
        }

        // ON CONFLICT(show_id, fingerprint_blob) DO UPDATE keeps a single
        // row per (show, fingerprint) and refreshes recency + lifts
        // confidence to the higher of the two values. The UNIQUE index
        // is created in V2 migration. A repeated insert of the same
        // fingerprint by the same show no longer wastes a slot; without
        // this, a binge listening session could fill the catalog with
        // duplicates of the same network ad.
        let sql = """
        INSERT INTO ad_catalog_entries
            (id, created_at, show_id, episode_position, duration_sec,
             fingerprint_blob, transcript_snippet, sponsor_tokens_json,
             original_confidence)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            created_at = excluded.created_at,
            episode_position = excluded.episode_position,
            duration_sec = excluded.duration_sec,
            transcript_snippet = excluded.transcript_snippet,
            sponsor_tokens_json = excluded.sponsor_tokens_json,
            original_confidence = CASE
                WHEN original_confidence IS NULL AND excluded.original_confidence IS NULL THEN NULL
                WHEN original_confidence IS NULL THEN excluded.original_confidence
                WHEN excluded.original_confidence IS NULL THEN original_confidence
                ELSE MAX(original_confidence, excluded.original_confidence)
            END
        ON CONFLICT(show_id, fingerprint_blob) WHERE show_id IS NOT NULL DO UPDATE SET
            created_at = excluded.created_at,
            original_confidence = CASE
                WHEN original_confidence IS NULL AND excluded.original_confidence IS NULL THEN NULL
                WHEN original_confidence IS NULL THEN excluded.original_confidence
                WHEN excluded.original_confidence IS NULL THEN original_confidence
                ELSE MAX(original_confidence, excluded.original_confidence)
            END
        """

        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw AdCatalogStoreError.insertFailed(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(stmt) }

        sqlite3_bind_text(stmt, 1, entry.id.uuidString, -1, Self.SQLITE_TRANSIENT)
        sqlite3_bind_double(stmt, 2, entry.createdAt.timeIntervalSince1970)
        if let show = entry.showId {
            sqlite3_bind_text(stmt, 3, show, -1, Self.SQLITE_TRANSIENT)
        } else {
            sqlite3_bind_null(stmt, 3)
        }
        sqlite3_bind_text(stmt, 4, entry.episodePosition.rawValue, -1, Self.SQLITE_TRANSIENT)
        sqlite3_bind_double(stmt, 5, entry.durationSec)

        let blob = entry.acousticFingerprint.data
        blob.withUnsafeBytes { raw in
            let base = raw.baseAddress
            sqlite3_bind_blob(stmt, 6, base, Int32(blob.count), Self.SQLITE_TRANSIENT)
        }

        if let snippet = entry.transcriptSnippet {
            sqlite3_bind_text(stmt, 7, snippet, -1, Self.SQLITE_TRANSIENT)
        } else {
            sqlite3_bind_null(stmt, 7)
        }

        if let tokens = entry.sponsorTokens,
           let tokensData = try? JSONEncoder().encode(tokens),
           let tokensJSON = String(data: tokensData, encoding: .utf8) {
            sqlite3_bind_text(stmt, 8, tokensJSON, -1, Self.SQLITE_TRANSIENT)
        } else {
            sqlite3_bind_null(stmt, 8)
        }

        if let conf = entry.originalConfidence {
            sqlite3_bind_double(stmt, 9, conf)
        } else {
            sqlite3_bind_null(stmt, 9)
        }

        guard sqlite3_step(stmt) == SQLITE_DONE else {
            throw AdCatalogStoreError.insertFailed(String(cString: sqlite3_errmsg(db)))
        }

        try evictOldestForShowIfNeeded(showId: entry.showId)
    }

    /// Cap the per-show row count at `maxEntriesPerShow` by deleting the
    /// oldest entries for that show. Called after each insert, so the
    /// table never exceeds the cap by more than one row at a time. Does
    /// nothing when the cap is not reached.
    private func evictOldestForShowIfNeeded(showId: String?) throws {
        guard let db else { return }
        let countSQL: String
        if showId != nil {
            countSQL = "SELECT COUNT(*) FROM ad_catalog_entries WHERE show_id = ?"
        } else {
            countSQL = "SELECT COUNT(*) FROM ad_catalog_entries WHERE show_id IS NULL"
        }
        var countStmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, countSQL, -1, &countStmt, nil) == SQLITE_OK else { return }
        defer { sqlite3_finalize(countStmt) }
        if let showId {
            sqlite3_bind_text(countStmt, 1, showId, -1, Self.SQLITE_TRANSIENT)
        }
        guard sqlite3_step(countStmt) == SQLITE_ROW else { return }
        let total = Int(sqlite3_column_int(countStmt, 0))
        guard total > Self.maxEntriesPerShow else { return }
        let toEvict = total - Self.maxEntriesPerShow

        let deleteSQL: String
        if showId != nil {
            deleteSQL = """
            DELETE FROM ad_catalog_entries
            WHERE id IN (
                SELECT id FROM ad_catalog_entries
                WHERE show_id = ?
                ORDER BY created_at ASC
                LIMIT \(toEvict)
            )
            """
        } else {
            deleteSQL = """
            DELETE FROM ad_catalog_entries
            WHERE id IN (
                SELECT id FROM ad_catalog_entries
                WHERE show_id IS NULL
                ORDER BY created_at ASC
                LIMIT \(toEvict)
            )
            """
        }
        var delStmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, deleteSQL, -1, &delStmt, nil) == SQLITE_OK else { return }
        defer { sqlite3_finalize(delStmt) }
        if let showId {
            sqlite3_bind_text(delStmt, 1, showId, -1, Self.SQLITE_TRANSIENT)
        }
        _ = sqlite3_step(delStmt)
    }

    /// Convenience: insert with individual fields.
    @discardableResult
    func insert(
        showId: String?,
        episodePosition: CatalogEpisodePosition,
        durationSec: Double,
        acousticFingerprint: AcousticFingerprint,
        transcriptSnippet: String? = nil,
        sponsorTokens: [String]? = nil,
        originalConfidence: Double? = nil
    ) throws -> CatalogEntry {
        let entry = CatalogEntry(
            showId: showId,
            episodePosition: episodePosition,
            durationSec: durationSec,
            acousticFingerprint: acousticFingerprint,
            transcriptSnippet: transcriptSnippet,
            sponsorTokens: sponsorTokens,
            originalConfidence: originalConfidence
        )
        try insert(entry: entry)
        return entry
    }

    /// Return all catalog entries (diagnostic / test use). No row cap —
    /// total rows are bounded by the per-show eviction policy.
    func allEntries() throws -> [CatalogEntry] {
        try ensureOpen()
        guard db != nil else { throw AdCatalogStoreError.queryFailed("database closed") }
        let sql = """
        SELECT id, created_at, show_id, episode_position, duration_sec,
               fingerprint_blob, transcript_snippet, sponsor_tokens_json,
               original_confidence
        FROM ad_catalog_entries
        ORDER BY created_at DESC
        """
        return try loadEntries(sql: sql, bind: nil)
    }

    /// Find matches above `similarityFloor`. When `show` is non-nil,
    /// only entries for that show (plus entries with `show_id = NULL`)
    /// are considered. Results are sorted by similarity descending.
    func matches(
        fingerprint: AcousticFingerprint,
        show: String?,
        similarityFloor: Float = AdCatalogStore.defaultSimilarityFloor
    ) -> [CatalogMatch] {
        // playhead-jndk: lazy first-use bootstrap. Failures here are
        // logged and swallowed so the hot-path detector keeps running
        // without catalog signal — same defensive posture the prior
        // `guard db != nil` had.
        do {
            try ensureOpen()
        } catch {
            logger.error("matches: ensureOpen failed: \(String(describing: error), privacy: .public)")
            return []
        }
        guard db != nil else { return [] }
        if fingerprint.isZero { return [] }

        // We scan per-show (+ null-show). For small catalogs a LIMIT + in-
        // memory similarity pass is cheaper than trying to build an ANN
        // index; gtt9.12 may later add a locality-sensitive shortcut.
        let sql: String
        if show != nil {
            sql = """
            SELECT id, created_at, show_id, episode_position, duration_sec,
                   fingerprint_blob, transcript_snippet, sponsor_tokens_json,
                   original_confidence
            FROM ad_catalog_entries
            WHERE show_id = ? OR show_id IS NULL
            ORDER BY created_at DESC
            """
        } else {
            sql = """
            SELECT id, created_at, show_id, episode_position, duration_sec,
                   fingerprint_blob, transcript_snippet, sponsor_tokens_json,
                   original_confidence
            FROM ad_catalog_entries
            ORDER BY created_at DESC
            """
        }

        let entries: [CatalogEntry]
        do {
            entries = try loadEntries(sql: sql, bind: { stmt in
                if let show {
                    sqlite3_bind_text(stmt, 1, show, -1, Self.SQLITE_TRANSIENT)
                }
            })
        } catch {
            logger.error("matches: query failed: \(String(describing: error), privacy: .public)")
            return []
        }

        var results: [CatalogMatch] = []
        results.reserveCapacity(entries.count)
        for e in entries {
            let s = AcousticFingerprint.similarity(fingerprint, e.acousticFingerprint)
            if s >= similarityFloor {
                results.append(CatalogMatch(entry: e, similarity: s))
            }
        }
        results.sort { $0.similarity > $1.similarity }
        return results
    }

    /// Number of rows currently in the catalog. Useful for telemetry /
    /// firing-rate diagnostics.
    func count() throws -> Int {
        try ensureOpen()
        guard let db else { throw AdCatalogStoreError.queryFailed("database closed") }
        let sql = "SELECT COUNT(*) FROM ad_catalog_entries"
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw AdCatalogStoreError.queryFailed(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
        return Int(sqlite3_column_int(stmt, 0))
    }

    /// Delete all entries. Test helper (exposed intentionally so the
    /// production app can also offer a "reset catalog" debug action).
    func clear() throws {
        try ensureOpen()
        guard let db else { throw AdCatalogStoreError.queryFailed("database closed") }
        try Self.exec(db, "DELETE FROM ad_catalog_entries")
    }

    // MARK: - Private: load helper

    private func loadEntries(
        sql: String,
        bind: ((OpaquePointer?) -> Void)?
    ) throws -> [CatalogEntry] {
        guard let db else { throw AdCatalogStoreError.queryFailed("database closed") }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw AdCatalogStoreError.queryFailed(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(stmt) }
        bind?(stmt)

        var out: [CatalogEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            guard let idCString = sqlite3_column_text(stmt, 0) else { continue }
            let idString = String(cString: idCString)
            guard let id = UUID(uuidString: idString) else { continue }
            let createdAt = Date(timeIntervalSince1970: sqlite3_column_double(stmt, 1))
            let showId: String? = {
                if let c = sqlite3_column_text(stmt, 2) { return String(cString: c) }
                return nil
            }()
            let positionRaw: String = {
                if let c = sqlite3_column_text(stmt, 3) { return String(cString: c) }
                return "unknown"
            }()
            let position = CatalogEpisodePosition(rawValue: positionRaw) ?? .unknown
            let duration = sqlite3_column_double(stmt, 4)

            // Fingerprint blob.
            let blobLength = Int(sqlite3_column_bytes(stmt, 5))
            var fingerprintData = Data()
            if blobLength > 0, let base = sqlite3_column_blob(stmt, 5) {
                fingerprintData = Data(bytes: base, count: blobLength)
            }
            guard let fingerprint = AcousticFingerprint(data: fingerprintData) else {
                logger.warning("loadEntries: skipped row with malformed fingerprint (id=\(idString, privacy: .public))")
                continue
            }

            let snippet: String? = {
                if let c = sqlite3_column_text(stmt, 6) { return String(cString: c) }
                return nil
            }()

            var tokens: [String]? = nil
            if let c = sqlite3_column_text(stmt, 7) {
                let json = String(cString: c)
                if let data = json.data(using: .utf8) {
                    tokens = try? JSONDecoder().decode([String].self, from: data)
                }
            }

            var originalConfidence: Double? = nil
            if sqlite3_column_type(stmt, 8) != SQLITE_NULL {
                originalConfidence = sqlite3_column_double(stmt, 8)
            }

            out.append(CatalogEntry(
                id: id,
                createdAt: createdAt,
                showId: showId,
                episodePosition: position,
                durationSec: duration,
                acousticFingerprint: fingerprint,
                transcriptSnippet: snippet,
                sponsorTokens: tokens,
                originalConfidence: originalConfidence
            ))
        }
        return out
    }

    // MARK: - Migration

    private static func migrate(handle: OpaquePointer, logger: Logger) throws {
        let current = try readUserVersion(handle)
        if current >= schemaVersion {
            return
        }

        // V0 → V1: create the base schema.
        if current < 1 {
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
            """
            try exec(handle, createSQL)
        }

        // V1 → V2: dedup existing rows by (show_id, fingerprint_blob),
        // then add a UNIQUE index so future inserts can use UPSERT to
        // refresh recency / confidence in place. Within each group the
        // surviving row is the one with the highest `original_confidence`
        // (NULL sorts as lowest); ties broken by highest rowid (most
        // recent insert). This matches the post-V2 UPSERT contract,
        // which keeps the higher-confidence value on collision —
        // the migration must not undo that property by dropping a
        // strong older row in favor of a weaker newer one. NULL show_id
        // rows are not collapsed against each other (SQLite treats
        // NULLs in UNIQUE indexes as distinct, so legacy NULL-show
        // duplicates remain — they are rare per the docstring contract).
        if current < 2 {
            let dedupSQL = """
            DELETE FROM ad_catalog_entries
            WHERE rowid NOT IN (
                SELECT rowid FROM (
                    SELECT rowid,
                           ROW_NUMBER() OVER (
                               PARTITION BY show_id, fingerprint_blob
                               ORDER BY IFNULL(original_confidence, -1.0) DESC,
                                        rowid DESC
                           ) AS rn
                    FROM ad_catalog_entries
                    WHERE show_id IS NOT NULL
                ) WHERE rn = 1
                UNION ALL
                SELECT rowid FROM ad_catalog_entries WHERE show_id IS NULL
            );
            """
            try exec(handle, dedupSQL)
            try exec(handle, "CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_show_fingerprint ON ad_catalog_entries(show_id, fingerprint_blob) WHERE show_id IS NOT NULL")
        }

        try exec(handle, "PRAGMA user_version = \(schemaVersion)")
        logger.info("AdCatalogStore migrated to schema v\(schemaVersion, privacy: .public)")
    }

    private static func readUserVersion(_ handle: OpaquePointer) throws -> Int32 {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(handle, "PRAGMA user_version", -1, &stmt, nil) == SQLITE_OK else {
            throw AdCatalogStoreError.migrationFailed(String(cString: sqlite3_errmsg(handle)))
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else {
            throw AdCatalogStoreError.migrationFailed("user_version step failed")
        }
        return sqlite3_column_int(stmt, 0)
    }

    // MARK: - SQLite helpers

    private static let SQLITE_TRANSIENT = unsafeBitCast(
        -1,
        to: sqlite3_destructor_type.self
    )

    private static func exec(_ handle: OpaquePointer, _ sql: String) throws {
        var err: UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(handle, sql, nil, nil, &err)
        if rc != SQLITE_OK {
            let msg = err.map { String(cString: $0) } ?? "sqlite3_exec rc=\(rc)"
            sqlite3_free(err)
            throw AdCatalogStoreError.migrationFailed(msg)
        }
    }
}
