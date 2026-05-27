// CrossShowSyndicationStore.swift
// playhead-xsdz.13: On-device CROSS-SHOW SYNDICATION observation store — the
// novel precision lever of the cross-show syndication signal.
//
// What it is
// ----------
// A device-local store of sponsor-ENTITY observations aggregated ACROSS the
// user's OWN subscribed shows. Ad campaigns are sold across show networks: a
// sponsor entity (BetterHelp, Squarespace, …) that recurs across MANY of the
// user's UNRELATED shows is overwhelming evidence of a paid NETWORK campaign,
// whereas an editorial brand mention is show-specific (it appears in one show
// because of that show's topic). This signal is invisible at the single-show
// level — `SponsorKnowledgeStore` is keyed PER-PODCAST and nothing aggregates
// entities across shows. This store fills exactly that gap.
//
// Keyed by NORMALIZED sponsor entity (reusing the same
// `EvidenceCatalogBuilder.normalize` / `SponsorKnowledgeEntry.normalizedValue`
// normalization so "BetterHelp" / "better help" / "betterhelp.com" collapse to
// one key), it tracks, per entity:
//   • the set of DISTINCT podcastIds it has appeared in (the spread numerator),
//   • a per-show observation count,
//   • the max confidence seen,
//   • first / last seen timestamps (the temporal-persistence signal).
//
// Persistence
// -----------
// Self-contained SQLite file (`cross_show_syndication.sqlite`) in Application
// Support, schema-versioned via `PRAGMA user_version`. Deliberately independent
// of `AnalysisStore` (a corruption here cannot take the analysis DB down) and
// modeled directly on `NegativeFingerprintBank` / `AdCatalogStore`: WAL,
// secure_delete, off-main lazy migration, and bounded growth via LRU eviction.
//
// On-device mandate (legal)
// -------------------------
// This aggregates ONLY the user's OWN local library — no network, no cross-user
// data. Observations NEVER leave the device. This store has no export path.

import Foundation
import OSLog
import SQLite3

// MARK: - Supporting types

/// A single cross-show syndication observation row: one normalized sponsor
/// entity observed on one show, with aggregate stats.
struct CrossShowSyndicationEntry: Sendable, Hashable {
    /// Normalized sponsor entity (the cross-show key).
    let normalizedEntity: String
    /// The podcast / show this observation belongs to.
    let podcastId: String
    /// Number of times this (entity, show) pair has been observed.
    let observationCount: Int
    /// Highest confidence seen for this (entity, show) pair.
    let maxConfidence: Double
    /// First time this (entity, show) pair was observed (Unix seconds).
    let firstSeenAt: Double
    /// Most recent time this (entity, show) pair was observed (Unix seconds).
    let lastSeenAt: Double
}

/// The cross-show spread profile for a single normalized entity — the read-path
/// payload the evaluator scores. Pure value type, deterministically computed
/// from the stored rows.
struct CrossShowSpreadProfile: Sendable, Hashable {
    /// The normalized entity this profile describes.
    let normalizedEntity: String
    /// Count of DISTINCT shows the entity has appeared in (the spread numerator).
    let distinctShowCount: Int
    /// Count of DISTINCT shows in the user's library that have ANY syndication
    /// observation (the spread denominator). Bounded below by `distinctShowCount`.
    let totalObservedShows: Int
    /// Earliest first-seen across all shows for this entity (Unix seconds).
    let earliestFirstSeenAt: Double
    /// Latest last-seen across all shows for this entity (Unix seconds).
    let latestLastSeenAt: Double

    /// Spread ratio in `[0, 1]`: fraction of the user's observed library this
    /// entity reaches. `0` when the library has no observed shows.
    var spreadRatio: Double {
        guard totalObservedShows > 0 else { return 0.0 }
        let r = Double(distinctShowCount) / Double(totalObservedShows)
        return Swift.max(0.0, Swift.min(1.0, r))
    }

    /// Temporal persistence in days: how long the entity has been present across
    /// the library (latest last-seen minus earliest first-seen). `0` for a
    /// single-moment burst.
    var persistenceDays: Double {
        guard latestLastSeenAt > earliestFirstSeenAt else { return 0.0 }
        return (latestLastSeenAt - earliestFirstSeenAt) / 86_400.0
    }
}

// MARK: - Errors

enum CrossShowSyndicationStoreError: Error, Equatable {
    case openFailed(String)
    case migrationFailed(String)
    case insertFailed(String)
    case queryFailed(String)
}

// MARK: - Store

/// Actor-backed cross-show syndication observation store. All reads/writes are
/// serialized on the actor so SQLite's single-connection model is honored
/// without an explicit mutex. Mirrors `NegativeFingerprintBank`'s lifecycle.
actor CrossShowSyndicationStore {

    // MARK: Constants / tunables

    /// Schema version stamped into `PRAGMA user_version`.
    static let schemaVersion: Int32 = 1

    /// Shortest normalized entity we will store. Below this a coincidental brand
    /// stem (e.g. a 2-char token) is too likely to be noise. Matches the
    /// `EvidenceCatalogBuilder` brand-span minimum (>= 3 chars).
    static let minEntityLength: Int = 3

    /// Maximum DISTINCT entities retained. Older rows beyond this are evicted by
    /// `last_seen_at ASC` (least-recently-observed) after each insert, bounding
    /// total rows at O(maxEntities × distinct shows per entity).
    static let maxEntities: Int = 2_000

    // MARK: State

    nonisolated let dbURL: URL
    nonisolated(unsafe) private var db: OpaquePointer?
    private let logger = Logger(subsystem: "com.playhead", category: "CrossShowSyndicationStore")

    // MARK: - Lifecycle

    /// Open or create the store at `directoryURL` (created if needed). File is
    /// `cross_show_syndication.sqlite`. Lazy first-use bootstrap mirrors
    /// `NegativeFingerprintBank` so the DDL stays off the launch path.
    init(directoryURL: URL) throws {
        try FileManager.default.createDirectory(
            at: directoryURL,
            withIntermediateDirectories: true
        )
        self.dbURL = directoryURL.appendingPathComponent("cross_show_syndication.sqlite")
    }

    private func ensureOpen() throws {
        if db != nil { return }
        var handle: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        let rc = sqlite3_open_v2(dbURL.path, &handle, flags, nil)
        guard rc == SQLITE_OK, let handle else {
            let msg = handle.map { String(cString: sqlite3_errmsg($0)) } ?? "sqlite3_open_v2 rc=\(rc)"
            if let handle { sqlite3_close(handle) }
            throw CrossShowSyndicationStoreError.openFailed(msg)
        }
        do {
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

    /// Public migration entry point (idempotent). Production callers await once
    /// during deferred startup; tests may skip — every public method calls
    /// `ensureOpen()` internally.
    func migrate() throws { try ensureOpen() }

    deinit {
        if let handle = db { sqlite3_close(handle) }
    }

    /// Explicit close (idempotent) for deterministic teardown in tests.
    func close() {
        if let handle = db {
            sqlite3_close(handle)
            db = nil
        }
    }

    /// Default store location inside Application Support.
    static func defaultDirectory() throws -> URL {
        let fm = FileManager.default
        let base = try fm.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        return base.appendingPathComponent("CrossShowSyndication", isDirectory: true)
    }

    // MARK: - Write

    /// Record a sponsor-entity observation for one show.
    ///
    /// Upserts the (normalizedEntity, podcastId) row: a new row is inserted with
    /// `observation_count = 1`, or the existing row's count is incremented, its
    /// `max_confidence` raised, and its `last_seen_at` refreshed. The caller is
    /// responsible for the min-confidence write gate (the write path in
    /// `AdDetectionService` only calls this above a minimum confidence); this
    /// method itself only rejects entities that are too short after
    /// normalization.
    ///
    /// - Returns: `true` when a row was inserted or updated; `false` when the
    ///   normalized entity was rejected (too short / empty) or the podcast id is
    ///   empty.
    @discardableResult
    func recordObservation(
        normalizedEntity: String,
        podcastId: String,
        confidence: Double,
        now: Double = Date().timeIntervalSince1970
    ) throws -> Bool {
        try ensureOpen()
        guard let db else { throw CrossShowSyndicationStoreError.insertFailed("database closed") }
        let entity = normalizedEntity.trimmingCharacters(in: .whitespacesAndNewlines)
        guard entity.count >= Self.minEntityLength, !podcastId.isEmpty else { return false }
        let clampedConfidence = Swift.max(0.0, Swift.min(1.0, confidence))

        // Upsert by the natural (entity, show) key. SQLite's ON CONFLICT clause
        // keeps the first-seen, raises the max confidence, increments the count,
        // and refreshes last-seen in one statement — deterministic and atomic.
        let sql = """
        INSERT INTO cross_show_syndication
            (normalized_entity, podcast_id, observation_count, max_confidence, first_seen_at, last_seen_at)
        VALUES (?, ?, 1, ?, ?, ?)
        ON CONFLICT(normalized_entity, podcast_id) DO UPDATE SET
            observation_count = observation_count + 1,
            max_confidence = MAX(max_confidence, excluded.max_confidence),
            last_seen_at = excluded.last_seen_at
        """
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw CrossShowSyndicationStoreError.insertFailed(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, entity, -1, Self.SQLITE_TRANSIENT)
        sqlite3_bind_text(stmt, 2, podcastId, -1, Self.SQLITE_TRANSIENT)
        sqlite3_bind_double(stmt, 3, clampedConfidence)
        sqlite3_bind_double(stmt, 4, now)
        sqlite3_bind_double(stmt, 5, now)
        guard sqlite3_step(stmt) == SQLITE_DONE else {
            throw CrossShowSyndicationStoreError.insertFailed(String(cString: sqlite3_errmsg(db)))
        }

        try evictIfNeeded()
        return true
    }

    // MARK: - Query (decision path)

    /// Total count of DISTINCT shows in the library that have ANY syndication
    /// observation — the spread-ratio denominator. Hoist this ONCE per backfill
    /// and reuse it across spans rather than re-querying per entity.
    func totalObservedShowCount() -> Int {
        do { try ensureOpen() } catch {
            logger.error("totalObservedShowCount: ensureOpen failed: \(String(describing: error), privacy: .public)")
            return 0
        }
        guard let db else { return 0 }
        var stmt: OpaquePointer?
        let sql = "SELECT COUNT(DISTINCT podcast_id) FROM cross_show_syndication"
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else { return 0 }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
        return Int(sqlite3_column_int(stmt, 0))
    }

    /// Build the spread profile for a single normalized entity, given the
    /// already-resolved library show count (`totalObservedShows`). Returns `nil`
    /// when the entity has no observations. Deterministic; bounded cost.
    func spreadProfile(
        forEntity normalizedEntity: String,
        totalObservedShows: Int
    ) -> CrossShowSpreadProfile? {
        do { try ensureOpen() } catch {
            logger.error("spreadProfile: ensureOpen failed: \(String(describing: error), privacy: .public)")
            return nil
        }
        guard let db else { return nil }
        let entity = normalizedEntity.trimmingCharacters(in: .whitespacesAndNewlines)
        guard entity.count >= Self.minEntityLength else { return nil }

        let sql = """
        SELECT COUNT(DISTINCT podcast_id), MIN(first_seen_at), MAX(last_seen_at)
        FROM cross_show_syndication
        WHERE normalized_entity = ?
        """
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else { return nil }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, entity, -1, Self.SQLITE_TRANSIENT)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        let distinct = Int(sqlite3_column_int(stmt, 0))
        guard distinct > 0 else { return nil }
        let earliest = sqlite3_column_double(stmt, 1)
        let latest = sqlite3_column_double(stmt, 2)
        // The denominator is bounded below by the entity's own distinct-show
        // count (an entity cannot reach more shows than the library observes).
        let denom = Swift.max(totalObservedShows, distinct)
        return CrossShowSpreadProfile(
            normalizedEntity: entity,
            distinctShowCount: distinct,
            totalObservedShows: denom,
            earliestFirstSeenAt: earliest,
            latestLastSeenAt: latest
        )
    }

    // MARK: - Diagnostics / maintenance

    /// Row count (telemetry / tests).
    func count() throws -> Int {
        try ensureOpen()
        guard let db else { throw CrossShowSyndicationStoreError.queryFailed("database closed") }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM cross_show_syndication", -1, &stmt, nil) == SQLITE_OK else {
            throw CrossShowSyndicationStoreError.queryFailed(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
        return Int(sqlite3_column_int(stmt, 0))
    }

    /// All rows for a given normalized entity (diagnostic / test).
    func entries(forEntity normalizedEntity: String) throws -> [CrossShowSyndicationEntry] {
        try ensureOpen()
        guard let db else { throw CrossShowSyndicationStoreError.queryFailed("database closed") }
        let entity = normalizedEntity.trimmingCharacters(in: .whitespacesAndNewlines)
        let sql = """
        SELECT normalized_entity, podcast_id, observation_count, max_confidence, first_seen_at, last_seen_at
        FROM cross_show_syndication
        WHERE normalized_entity = ?
        ORDER BY podcast_id ASC
        """
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw CrossShowSyndicationStoreError.queryFailed(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, entity, -1, Self.SQLITE_TRANSIENT)
        var out: [CrossShowSyndicationEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            let normalized = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
            let podcastId = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? ""
            let observationCount = Int(sqlite3_column_int(stmt, 2))
            let maxConfidence = sqlite3_column_double(stmt, 3)
            let firstSeenAt = sqlite3_column_double(stmt, 4)
            let lastSeenAt = sqlite3_column_double(stmt, 5)
            out.append(CrossShowSyndicationEntry(
                normalizedEntity: normalized,
                podcastId: podcastId,
                observationCount: observationCount,
                maxConfidence: maxConfidence,
                firstSeenAt: firstSeenAt,
                lastSeenAt: lastSeenAt
            ))
        }
        return out
    }

    /// Delete all rows (test helper / "reset" debug action).
    func clear() throws {
        try ensureOpen()
        guard let db else { throw CrossShowSyndicationStoreError.queryFailed("database closed") }
        try Self.exec(db, "DELETE FROM cross_show_syndication")
    }

    // MARK: - Private: LRU eviction

    /// Cap the DISTINCT-entity count at `maxEntities`, deleting all rows for the
    /// least-recently-observed entities first. Called after each insert. Bounds
    /// store growth deterministically.
    private func evictIfNeeded() throws {
        guard let db else { return }
        var countStmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "SELECT COUNT(DISTINCT normalized_entity) FROM cross_show_syndication", -1, &countStmt, nil) == SQLITE_OK else { return }
        let distinctEntities: Int
        if sqlite3_step(countStmt) == SQLITE_ROW {
            distinctEntities = Int(sqlite3_column_int(countStmt, 0))
        } else {
            distinctEntities = 0
        }
        sqlite3_finalize(countStmt)
        guard distinctEntities > Self.maxEntities else { return }
        let toEvict = distinctEntities - Self.maxEntities

        // Pick the `toEvict` entities with the oldest per-entity MAX(last_seen_at)
        // and delete ALL their rows. Deterministic tie-break on the entity key.
        let deleteSQL = """
        DELETE FROM cross_show_syndication WHERE normalized_entity IN (
            SELECT normalized_entity FROM cross_show_syndication
            GROUP BY normalized_entity
            ORDER BY MAX(last_seen_at) ASC, normalized_entity ASC
            LIMIT \(toEvict)
        )
        """
        try Self.exec(db, deleteSQL)
    }

    // MARK: - Migration

    private static func migrate(handle: OpaquePointer, logger: Logger) throws {
        let current = try readUserVersion(handle)
        if current >= schemaVersion { return }
        if current < 1 {
            let createSQL = """
            CREATE TABLE IF NOT EXISTS cross_show_syndication (
                normalized_entity TEXT NOT NULL,
                podcast_id TEXT NOT NULL,
                observation_count INTEGER NOT NULL DEFAULT 1,
                max_confidence REAL NOT NULL DEFAULT 0,
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL,
                PRIMARY KEY (normalized_entity, podcast_id)
            );
            CREATE INDEX IF NOT EXISTS idx_css_entity ON cross_show_syndication(normalized_entity);
            CREATE INDEX IF NOT EXISTS idx_css_last_seen ON cross_show_syndication(last_seen_at);
            """
            try exec(handle, createSQL)
        }
        try exec(handle, "PRAGMA user_version = \(schemaVersion)")
        logger.info("CrossShowSyndicationStore migrated to schema v\(schemaVersion, privacy: .public)")
    }

    private static func readUserVersion(_ handle: OpaquePointer) throws -> Int32 {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(handle, "PRAGMA user_version", -1, &stmt, nil) == SQLITE_OK else {
            throw CrossShowSyndicationStoreError.migrationFailed(String(cString: sqlite3_errmsg(handle)))
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else {
            throw CrossShowSyndicationStoreError.migrationFailed("user_version step failed")
        }
        return sqlite3_column_int(stmt, 0)
    }

    // MARK: - SQLite helpers

    private static let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

    private static func exec(_ handle: OpaquePointer, _ sql: String) throws {
        var err: UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(handle, sql, nil, nil, &err)
        if rc != SQLITE_OK {
            let msg = err.map { String(cString: $0) } ?? "sqlite3_exec rc=\(rc)"
            sqlite3_free(err)
            throw CrossShowSyndicationStoreError.migrationFailed(msg)
        }
    }
}
