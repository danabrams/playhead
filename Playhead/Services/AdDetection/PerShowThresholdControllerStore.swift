// PerShowThresholdControllerStore.swift
// playhead-xsdz.11: Per-show PI-controller STATE store for the user-feedback
// auto-skip threshold control.
//
// What it is
// ----------
// A device-local, per-show store of `PerShowThresholdControllerState`
// (`offset`, `integral`, `sampleCount`) keyed by `podcastId`. The write path
// (`SkipOrchestrator`) folds each correction signal into the show's state via
// `PerShowThresholdController.apply`; the read path (`AdDetectionService`
// `runBackfill`) reads the current offset once per backfill and applies it to
// the global auto-skip threshold at the gate.
//
// Persistence
// -----------
// Self-contained SQLite file (`per_show_threshold_control.sqlite`) in
// Application Support, schema-versioned via `PRAGMA user_version`. Deliberately
// INDEPENDENT of `AnalysisStore` — modeled directly on `CrossShowSyndicationStore`
// (xsdz.13) so:
//   • a corruption here cannot take the analysis DB down, and
//   • the store / its DB file / its migration exist ONLY when the off-by-default
//     `perShowThresholdControlEnabled` flag is on. With the flag off the runtime
//     constructs NO store, opens NO file, runs NO migration, and the gate uses
//     the unmodified global threshold — byte-identical to pre-xsdz.11.
//
// On-device mandate (legal)
// -------------------------
// This adapts ONLY to the user's OWN local corrections — no network, no
// cross-user data. State NEVER leaves the device. No export path.

import Foundation
import OSLog
import SQLite3

// MARK: - Errors

enum PerShowThresholdControllerStoreError: Error, Equatable {
    case openFailed(String)
    case migrationFailed(String)
    case writeFailed(String)
    case queryFailed(String)
}

// MARK: - Store

/// Actor-backed per-show controller-state store. All reads/writes are
/// serialized on the actor so SQLite's single-connection model is honored
/// without an explicit mutex. Mirrors `CrossShowSyndicationStore`'s lifecycle.
actor PerShowThresholdControllerStore {

    // MARK: Constants

    /// Schema version stamped into `PRAGMA user_version`.
    static let schemaVersion: Int32 = 1

    /// Maximum DISTINCT shows retained. Older rows beyond this are evicted by
    /// `updated_at ASC` (least-recently-updated) after each write, bounding
    /// total rows. A user with thousands of shows is implausible; this is a
    /// safety ceiling, not a tuning knob.
    static let maxShows: Int = 5_000

    // MARK: State

    nonisolated let dbURL: URL
    nonisolated(unsafe) private var db: OpaquePointer?
    private let logger = Logger(subsystem: "com.playhead", category: "PerShowThresholdControllerStore")
    private let parameters: PerShowThresholdControllerParameters

    // MARK: - Lifecycle

    /// Open or create the store at `directoryURL` (created if needed). File is
    /// `per_show_threshold_control.sqlite`. Lazy first-use bootstrap mirrors
    /// `CrossShowSyndicationStore` so the DDL stays off the launch path.
    ///
    /// `parameters` is captured so the WRITE path folds signals with the same
    /// gains/bounds the read path clamps against.
    init(
        directoryURL: URL,
        parameters: PerShowThresholdControllerParameters = .default
    ) throws {
        try FileManager.default.createDirectory(
            at: directoryURL,
            withIntermediateDirectories: true
        )
        self.dbURL = directoryURL.appendingPathComponent("per_show_threshold_control.sqlite")
        self.parameters = parameters
    }

    private func ensureOpen() throws {
        if db != nil { return }
        var handle: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        let rc = sqlite3_open_v2(dbURL.path, &handle, flags, nil)
        guard rc == SQLITE_OK, let handle else {
            let msg = handle.map { String(cString: sqlite3_errmsg($0)) } ?? "sqlite3_open_v2 rc=\(rc)"
            if let handle { sqlite3_close(handle) }
            throw PerShowThresholdControllerStoreError.openFailed(msg)
        }
        do {
            try Self.exec(handle, "PRAGMA journal_mode=WAL")
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
        return base.appendingPathComponent("PerShowThresholdControl", isDirectory: true)
    }

    // MARK: - Read (decision path)

    /// The current controller state for a show. Returns `.zero` (cold-start)
    /// when the show has no row — the read path then applies a zero offset, so
    /// a never-corrected show uses the unmodified global threshold.
    func state(forShow podcastId: String) -> PerShowThresholdControllerState {
        do { try ensureOpen() } catch {
            logger.error("state(forShow:): ensureOpen failed: \(String(describing: error), privacy: .public)")
            return .zero
        }
        guard let db, !podcastId.isEmpty else { return .zero }

        let sql = "SELECT offset, integral, sample_count FROM per_show_threshold_control WHERE podcast_id = ?"
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else { return .zero }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, podcastId, -1, Self.SQLITE_TRANSIENT)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return .zero }
        let offset = sqlite3_column_double(stmt, 0)
        let integral = Int(sqlite3_column_int64(stmt, 1))
        let sampleCount = Int(sqlite3_column_int64(stmt, 2))
        return PerShowThresholdControllerState(
            offset: offset,
            integral: integral,
            sampleCount: sampleCount
        )
    }

    /// The current per-show offset to add to the global threshold. Convenience
    /// over `state(forShow:).offset` for the read path.
    func offset(forShow podcastId: String) -> Double {
        state(forShow: podcastId).offset
    }

    // MARK: - Write (correction path)

    /// Fold ONE correction signal into the show's controller state and persist
    /// the result. Loads the prior state (cold-start `.zero` when absent),
    /// applies the pure PI update, and upserts the new `(offset, integral,
    /// sample_count)`. Returns the post-update state.
    ///
    /// `now` is captured only for the `updated_at` LRU bookkeeping column — it
    /// does NOT enter the controller math, so the update stays deterministic in
    /// the controller's own terms.
    @discardableResult
    func record(
        signal: ThresholdControlSignal,
        forShow podcastId: String,
        now: Double = Date().timeIntervalSince1970
    ) throws -> PerShowThresholdControllerState {
        try ensureOpen()
        guard let db else { throw PerShowThresholdControllerStoreError.writeFailed("database closed") }
        guard !podcastId.isEmpty else {
            throw PerShowThresholdControllerStoreError.writeFailed("empty podcastId")
        }

        let prior = state(forShow: podcastId)
        let next = PerShowThresholdController.apply(
            signal: signal,
            to: prior,
            parameters: parameters
        )

        let sql = """
        INSERT INTO per_show_threshold_control
            (podcast_id, offset, integral, sample_count, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(podcast_id) DO UPDATE SET
            offset = excluded.offset,
            integral = excluded.integral,
            sample_count = excluded.sample_count,
            updated_at = excluded.updated_at
        """
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw PerShowThresholdControllerStoreError.writeFailed(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, podcastId, -1, Self.SQLITE_TRANSIENT)
        sqlite3_bind_double(stmt, 2, next.offset)
        sqlite3_bind_int64(stmt, 3, Int64(next.integral))
        sqlite3_bind_int64(stmt, 4, Int64(next.sampleCount))
        sqlite3_bind_double(stmt, 5, now)
        guard sqlite3_step(stmt) == SQLITE_DONE else {
            throw PerShowThresholdControllerStoreError.writeFailed(String(cString: sqlite3_errmsg(db)))
        }

        try evictIfNeeded()
        return next
    }

    // MARK: - Diagnostics / maintenance

    /// Row count (telemetry / tests).
    func count() throws -> Int {
        try ensureOpen()
        guard let db else { throw PerShowThresholdControllerStoreError.queryFailed("database closed") }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM per_show_threshold_control", -1, &stmt, nil) == SQLITE_OK else {
            throw PerShowThresholdControllerStoreError.queryFailed(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
        return Int(sqlite3_column_int(stmt, 0))
    }

    /// Delete all rows (test helper / "reset" debug action).
    func clear() throws {
        try ensureOpen()
        guard let db else { throw PerShowThresholdControllerStoreError.queryFailed("database closed") }
        try Self.exec(db, "DELETE FROM per_show_threshold_control")
    }

    // MARK: - Private: LRU eviction

    /// Cap the row count at `maxShows`, deleting the least-recently-updated
    /// rows first. Called after each write. Bounds store growth deterministically.
    private func evictIfNeeded() throws {
        guard let db else { return }
        var countStmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM per_show_threshold_control", -1, &countStmt, nil) == SQLITE_OK else { return }
        let rows: Int
        if sqlite3_step(countStmt) == SQLITE_ROW {
            rows = Int(sqlite3_column_int(countStmt, 0))
        } else {
            rows = 0
        }
        sqlite3_finalize(countStmt)
        guard rows > Self.maxShows else { return }
        let toEvict = rows - Self.maxShows

        let deleteSQL = """
        DELETE FROM per_show_threshold_control WHERE podcast_id IN (
            SELECT podcast_id FROM per_show_threshold_control
            ORDER BY updated_at ASC, podcast_id ASC
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
            CREATE TABLE IF NOT EXISTS per_show_threshold_control (
                podcast_id   TEXT PRIMARY KEY,
                offset       REAL NOT NULL DEFAULT 0,
                integral     INTEGER NOT NULL DEFAULT 0,
                sample_count INTEGER NOT NULL DEFAULT 0,
                updated_at   REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_pstc_updated ON per_show_threshold_control(updated_at);
            """
            try exec(handle, createSQL)
        }
        try exec(handle, "PRAGMA user_version = \(schemaVersion)")
        logger.info("PerShowThresholdControllerStore migrated to schema v\(schemaVersion, privacy: .public)")
    }

    private static func readUserVersion(_ handle: OpaquePointer) throws -> Int32 {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(handle, "PRAGMA user_version", -1, &stmt, nil) == SQLITE_OK else {
            throw PerShowThresholdControllerStoreError.migrationFailed(String(cString: sqlite3_errmsg(handle)))
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else {
            throw PerShowThresholdControllerStoreError.migrationFailed("user_version step failed")
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
            throw PerShowThresholdControllerStoreError.migrationFailed(msg)
        }
    }
}
