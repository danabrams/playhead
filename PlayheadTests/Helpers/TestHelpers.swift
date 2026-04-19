// TestHelpers.swift
// Shared test utilities used across PlayheadTests suites.

import Foundation
import SQLite3
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

// MARK: - Async polling helper

/// Polls an async predicate until it returns true or the deadline expires.
/// Returns `true` if the predicate became true within the deadline, `false`
/// on timeout. Used in place of fixed `Task.sleep` waits in scheduler tests
/// so they remain deterministic under heavy parallel-test CPU contention
/// (playhead-qtc). The poll interval is intentionally small so tests that
/// complete quickly aren't slowed down, and the default deadline is
/// generous enough to absorb simulator scheduling jitter when the whole
/// suite runs in parallel.
func pollUntil(
    timeout: Duration = .seconds(30),
    interval: Duration = .milliseconds(20),
    _ condition: @Sendable () async throws -> Bool
) async rethrows -> Bool {
    let clock = ContinuousClock()
    let deadline = clock.now.advanced(by: timeout)
    while clock.now < deadline {
        if try await condition() { return true }
        // `rethrows` constraint forces `try?` here (Task.sleep throws
        // CancellationError, not from the closure), so check
        // `Task.isCancelled` after the sleep to exit cleanly on
        // cancellation rather than spinning until the outer
        // `.timeLimit`.
        try? await Task.sleep(for: interval)
        if Task.isCancelled { return false }
    }
    return try await condition()
}

// MARK: - Migration test helpers

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

/// Cycle 4 H1: hand-builds a v1-shape SQLite database suitable for driving
/// `AnalysisStore.migrateOnlyForTesting()` in isolation from
/// `createTables()`. Only the tables that the V*IfNeeded ladder touches
/// are seeded, in their v1 shape: no `needsShadowRetry`, no
/// `transcriptVersion` on evidence_events, no `phase` columns, and the
/// pre-Phase-6 `ad_windows` shape without `evidenceSources` /
/// `eligibilityGate`. The caller seeds `_meta.schema_version`
/// explicitly via `seedSchemaVersion`.
func seedV1ShapeDatabase(in directory: URL) throws {
    let dbURL = directory.appendingPathComponent("analysis.sqlite")
    var db: OpaquePointer?
    guard sqlite3_open_v2(
        dbURL.path,
        &db,
        SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE,
        nil
    ) == SQLITE_OK else {
        throw NSError(domain: "SeedV1Shape", code: 1)
    }
    defer { sqlite3_close_v2(db) }

    let ddl = """
        CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS analysis_assets (
            id TEXT PRIMARY KEY,
            episodeId TEXT NOT NULL,
            assetFingerprint TEXT NOT NULL,
            weakFingerprint TEXT,
            sourceURL TEXT NOT NULL,
            featureCoverageEndTime REAL,
            fastTranscriptCoverageEndTime REAL,
            confirmedAdCoverageEndTime REAL,
            analysisState TEXT NOT NULL,
            analysisVersion INTEGER NOT NULL,
            capabilitySnapshot TEXT,
            createdAt REAL NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS analysis_sessions (
            id TEXT PRIMARY KEY,
            analysisAssetId TEXT NOT NULL,
            state TEXT NOT NULL,
            startedAt REAL NOT NULL,
            updatedAt REAL NOT NULL,
            failureReason TEXT
        );
        CREATE TABLE IF NOT EXISTS evidence_events (
            id TEXT PRIMARY KEY,
            analysisAssetId TEXT NOT NULL,
            eventType TEXT NOT NULL,
            sourceType TEXT NOT NULL,
            atomOrdinals TEXT NOT NULL,
            evidenceJSON TEXT NOT NULL,
            scanCohortJSON TEXT NOT NULL,
            createdAt REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ad_windows (
            id TEXT PRIMARY KEY,
            analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
            startTime REAL NOT NULL,
            endTime REAL NOT NULL,
            confidence REAL NOT NULL,
            boundaryState TEXT NOT NULL,
            decisionState TEXT NOT NULL DEFAULT 'candidate',
            detectorVersion TEXT NOT NULL,
            advertiser TEXT,
            product TEXT,
            adDescription TEXT,
            evidenceText TEXT,
            evidenceStartTime REAL,
            metadataSource TEXT NOT NULL DEFAULT 'none',
            metadataConfidence REAL,
            metadataPromptVersion TEXT,
            wasSkipped INTEGER NOT NULL DEFAULT 0,
            userDismissedBanner INTEGER NOT NULL DEFAULT 0
        );
        """
    guard sqlite3_exec(db, ddl, nil, nil, nil) == SQLITE_OK else {
        let msg = sqlite3_errmsg(db).map { String(cString: $0) } ?? "unknown"
        throw NSError(
            domain: "SeedV1Shape",
            code: 2,
            userInfo: [NSLocalizedDescriptionKey: msg]
        )
    }
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

// MARK: - Correction Test Helpers

/// Creates a minimal AnalysisAsset for correction store tests.
/// Shared across FalseNegativeCorrectionTests, UserCorrectionStoreTests,
/// and CorrectionSuppressionTests.
func makeTestAsset(id: String) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: "ep-\(id)",
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///tmp/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
}

// MARK: - Skip Orchestrator Test Helpers

/// Shared factory for SkipOrchestrator tests. Used by both
/// SkipOrchestratorCharacterizationTests and CorrectionSuppressionTests.
func makeSkipTestAnalysisAsset(
    id: String = "asset-1",
    episodeId: String = "ep-1"
) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: episodeId,
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///test/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
}

func makeSkipTestAdWindow(
    id: String = "ad-1",
    assetId: String = "asset-1",
    startTime: Double = 60,
    endTime: Double = 120,
    confidence: Double = 0.75,
    decisionState: String = "confirmed"
) -> AdWindow {
    AdWindow(
        id: id,
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        confidence: confidence,
        boundaryState: "lexical",
        decisionState: decisionState,
        detectorVersion: "detection-v1",
        advertiser: nil,
        product: nil,
        adDescription: nil,
        evidenceText: "brought to you by",
        evidenceStartTime: startTime,
        metadataSource: "none",
        metadataConfidence: nil,
        metadataPromptVersion: nil,
        wasSkipped: false,
        userDismissedBanner: false
    )
}

func makeSkipTestTrustService(
    mode: String,
    trustScore: Double,
    observations: Int,
    falseSignals: Int = 0
) async throws -> TrustScoringService {
    let trustStore = try await makeTestStore()
    try await trustStore.upsertProfile(
        PodcastProfile(
            podcastId: "podcast-1",
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: trustScore,
            observationCount: observations,
            mode: mode,
            recentFalseSkipSignals: falseSignals
        )
    )
    return TrustScoringService(store: trustStore)
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
