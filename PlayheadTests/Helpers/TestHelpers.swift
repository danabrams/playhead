// TestHelpers.swift
// Shared test utilities used across PlayheadTests suites.

import Foundation
@testable import Playhead

/// Creates a uniquely-named temporary directory for test isolation.
/// Caller is responsible for cleanup (e.g., via `defer`, `addTeardownBlock`,
/// or `TestTempDirTracker`).
func makeTempDir(prefix: String = "PlayheadTests") throws -> URL {
    let url = FileManager.default.temporaryDirectory
        .appendingPathComponent("\(prefix)-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    return url
}

/// Thread-safe collector for temp directories that cleans up on deinit.
/// Use at file scope alongside `makeTestStore()`-style helpers to ensure
/// temp directories are removed when the test suite finishes.
final class TestTempDirTracker: @unchecked Sendable {
    private var dirs: [URL] = []
    private let lock = NSLock()

    func track(_ dir: URL) {
        lock.lock()
        dirs.append(dir)
        lock.unlock()
    }

    deinit {
        for dir in dirs {
            try? FileManager.default.removeItem(at: dir)
        }
    }
}

// MARK: - AnalysisStore Factory

/// Shared tracker for test store temp directories.
private let _sharedTestStoreDirs = TestTempDirTracker()

/// Creates an AnalysisStore backed by a temporary directory for isolated testing.
/// The directory is automatically cleaned up when the test process ends.
func makeTestStore() async throws -> AnalysisStore {
    let dir = try makeTempDir(prefix: "PlayheadTests")
    _sharedTestStoreDirs.track(dir)
    let store = try AnalysisStore(directory: dir)
    try await store.migrate()
    return store
}

// MARK: - Migration test helpers

import SQLite3

/// H11 (cycle 2): writes `_meta.schema_version = '<version>'` directly into a
/// sqlite file so a subsequent `AnalysisStore(directory:).migrate()` runs the
/// V*IfNeeded ladder from that starting point. Uses a raw sqlite handle so
/// the seed can run before any `AnalysisStore` actor opens the file.
///
/// The caller must ensure the directory exists. The function creates the
/// `_meta` table if necessary so this works on a brand-new directory.
func seedSchemaVersion(_ version: Int, in directory: URL) throws {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(
        dbURL.path,
        &db,
        SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE,
        nil
    ) == SQLITE_OK else {
        throw NSError(
            domain: "SeedSchemaVersion",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "open failed"]
        )
    }
    defer { sqlite3_close_v2(db) }

    let createMeta = "CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    guard sqlite3_exec(db, createMeta, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedSchemaVersion", code: 2)
    }
    let insertVersion = "INSERT OR REPLACE INTO _meta (key, value) VALUES ('schema_version', '\(version)')"
    guard sqlite3_exec(db, insertVersion, nil, nil, nil) == SQLITE_OK else {
        throw NSError(domain: "SeedSchemaVersion", code: 3)
    }
}

/// H11: probe a column's existence on a freshly-opened sqlite handle, used
/// by migration tests to assert that a particular column was added.
func probeColumnExists(in directory: URL, table: String, column: String) throws -> Bool {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeColumnExists", code: 1)
    }
    defer { sqlite3_close_v2(db) }

    let sql = "PRAGMA table_info(\(table))"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeColumnExists", code: 2)
    }
    defer { sqlite3_finalize(stmt) }
    while sqlite3_step(stmt) == SQLITE_ROW {
        if let cName = sqlite3_column_text(stmt, 1),
           String(cString: cName) == column {
            return true
        }
    }
    return false
}

/// H11: probe an index's existence by name.
func probeIndexExists(in directory: URL, indexName: String) throws -> Bool {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeIndexExists", code: 1)
    }
    defer { sqlite3_close_v2(db) }

    let sql = "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeIndexExists", code: 2)
    }
    defer { sqlite3_finalize(stmt) }
    sqlite3_bind_text(stmt, 1, indexName, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
    return sqlite3_step(stmt) == SQLITE_ROW
}

/// H11: probe a table's existence by name.
func probeTableExists(in directory: URL, table: String) throws -> Bool {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeTableExists", code: 1)
    }
    defer { sqlite3_close_v2(db) }

    let sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?"
    var stmt: OpaquePointer?
    guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
        throw NSError(domain: "ProbeTableExists", code: 2)
    }
    defer { sqlite3_finalize(stmt) }
    sqlite3_bind_text(stmt, 1, table, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
    return sqlite3_step(stmt) == SQLITE_ROW
}

// MARK: - AnalysisShard Factory

/// Creates a test AnalysisShard with sensible defaults. Silence samples are used
/// so the shard is lightweight yet passes any non-empty-sample checks.
func makeShard(
    id: Int = 0,
    episodeID: String = "test-ep",
    startTime: TimeInterval = 0,
    duration: TimeInterval = 30
) -> AnalysisShard {
    AnalysisShard(
        id: id,
        episodeID: episodeID,
        startTime: startTime,
        duration: duration,
        samples: [Float](repeating: 0, count: 16000 * Int(duration))
    )
}
