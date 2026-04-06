// AnalysisStore.swift
// SQLite/FTS5 persistence for analysis pipeline state: transcription chunks,
// feature windows, ad windows, podcast profiles, and preview budgets.
// Separated from SwiftData because this data is append-heavy, versioned,
// needs FTS5, and supports resumable processing with checkpointing.

import CryptoKit
import Foundation
import SQLite3

/// SQLite SQLITE_TRANSIENT destructor constant — tells sqlite3_bind_text to
/// immediately copy the provided string. Defined once to avoid repeated
/// unsafeBitCast calls at every bind site.
private let SQLITE_TRANSIENT_PTR = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

// MARK: - Row types

struct AnalysisAsset: Sendable {
    let id: String
    let episodeId: String
    let assetFingerprint: String
    let weakFingerprint: String?
    let sourceURL: String
    let featureCoverageEndTime: Double?
    let fastTranscriptCoverageEndTime: Double?
    let confirmedAdCoverageEndTime: Double?
    let analysisState: String
    let analysisVersion: Int
    let capabilitySnapshot: String?
}

struct AnalysisSession: Sendable {
    let id: String
    let analysisAssetId: String
    let state: String
    let startedAt: Double
    let updatedAt: Double
    let failureReason: String?
}

struct FeatureWindow: Sendable {
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let rms: Double
    let spectralFlux: Double
    let musicProbability: Double
    let pauseProbability: Double
    let speakerClusterId: Int?
    let jingleHash: String?
    let featureVersion: Int
}

struct TranscriptChunk: Sendable {
    let id: String
    let analysisAssetId: String
    let segmentFingerprint: String
    let chunkIndex: Int
    let startTime: Double
    let endTime: Double
    let text: String
    let normalizedText: String
    let pass: String // fast | final
    let modelVersion: String
    let transcriptVersion: String?   // nil for fast-pass chunks (version computed on final)
    let atomOrdinal: Int?            // nil for fast-pass chunks
}

struct AdWindow: Sendable {
    let id: String
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let confidence: Double
    let boundaryState: String
    let decisionState: String
    let detectorVersion: String
    let advertiser: String?
    let product: String?
    let adDescription: String?
    let evidenceText: String?
    let evidenceStartTime: Double?
    let metadataSource: String
    let metadataConfidence: Double?
    let metadataPromptVersion: String?
    let wasSkipped: Bool
    let userDismissedBanner: Bool
}

struct SkipCue: Sendable {
    let id: String
    let analysisAssetId: String
    let cueHash: String
    let startTime: Double
    let endTime: Double
    let confidence: Double
    let source: String      // "preAnalysis" | "live"
    let materializedAt: Double
    let wasSkipped: Bool
    let userDismissed: Bool
}

struct PodcastProfile: Sendable {
    let podcastId: String
    let sponsorLexicon: String?
    let normalizedAdSlotPriors: String?
    let repeatedCTAFragments: String?
    let jingleFingerprints: String?
    let implicitFalsePositiveCount: Int
    let skipTrustScore: Double
    let observationCount: Int
    let mode: String
    let recentFalseSkipSignals: Int
}

struct PreviewBudget: Sendable {
    let canonicalEpisodeKey: String
    let consumedAnalysisSeconds: Double
    let graceBreakWindow: Double
    let lastUpdated: Double
}

struct AnalysisJob: Sendable {
    let jobId: String
    let jobType: String         // "preAnalysis" | "playback" | "backfill"
    let episodeId: String
    let podcastId: String?
    let analysisAssetId: String?
    let workKey: String         // fingerprint + analysisVersion + jobType
    let sourceFingerprint: String
    let downloadId: String
    let priority: Int
    let desiredCoverageSec: Double
    let featureCoverageSec: Double
    let transcriptCoverageSec: Double
    let cueCoverageSec: Double
    let state: String
    let attemptCount: Int
    let nextEligibleAt: Double?
    let leaseOwner: String?
    let leaseExpiresAt: Double?
    let lastErrorCode: String?
    let createdAt: Double
    let updatedAt: Double

    static func computeWorkKey(fingerprint: String, analysisVersion: Int, jobType: String) -> String {
        "\(fingerprint):\(analysisVersion):\(jobType)"
    }
}

// MARK: - Store errors

enum AnalysisStoreError: Error, CustomStringConvertible, Equatable {
    case openFailed(code: Int32, message: String)
    case migrationFailed(String)
    case queryFailed(String)
    case insertFailed(String)
    case notFound
    case duplicateJobId(String)
    case invalidRow(column: Int)
    case invalidEvidenceEvent(String)
    case invalidScanCohortJSON(String)

    var description: String {
        switch self {
        case .openFailed(let code, let msg): "SQLite open failed (\(code)): \(msg)"
        case .migrationFailed(let msg): "Migration failed: \(msg)"
        case .queryFailed(let msg): "Query failed: \(msg)"
        case .insertFailed(let msg): "Insert failed: \(msg)"
        case .notFound: "Row not found"
        case .duplicateJobId(let id): "Duplicate backfill job id: \(id)"
        case .invalidRow(let col): "Unexpected NULL in non-null column \(col)"
        case .invalidEvidenceEvent(let msg): "Invalid evidence event: \(msg)"
        case .invalidScanCohortJSON(let msg): "Invalid scanCohortJSON: \(msg)"
        }
    }
}

// MARK: - AnalysisStore actor

actor AnalysisStore {

    /// The raw SQLite handle. Marked `nonisolated(unsafe)` so deinit can close
    /// it without requiring actor isolation (Swift 6 strict concurrency).
    /// All actual usage is funnelled through actor-isolated methods.
    nonisolated(unsafe) private var db: OpaquePointer?

    /// Path to the SQLite database file.
    nonisolated let databaseURL: URL

    // MARK: Lifecycle

    /// Open (or create) the analysis database. Call ``migrate()`` after init
    /// to set up tables. This two-step dance avoids calling actor-isolated
    /// methods from the nonisolated initialiser (Swift 6 requirement).
    init(directory: URL? = nil) throws {
        let dir = directory ?? Self.defaultDirectory()
        let fm = FileManager.default
        if !fm.fileExists(atPath: dir.path) {
            try fm.createDirectory(at: dir, withIntermediateDirectories: true)

            // Apply file protection to the directory.
            try fm.setAttributes(
                [.protectionKey: FileProtectionType.complete],
                ofItemAtPath: dir.path
            )
        }

        self.databaseURL = dir.appendingPathComponent("analysis.sqlite")

        var handle: OpaquePointer?
        // NOMUTEX: the enclosing actor already serializes all access, so the
        // full-mutex threading mode is redundant overhead.
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX
        let rc = sqlite3_open_v2(databaseURL.path, &handle, flags, nil)
        guard rc == SQLITE_OK, let handle else {
            let msg = handle.flatMap { String(cString: sqlite3_errmsg($0)) } ?? "unknown"
            throw AnalysisStoreError.openFailed(code: rc, message: msg)
        }
        self.db = handle
    }

    /// Convenience factory that opens the database and runs migrations in one
    /// call. Preferred entry point for production use.
    static func open(directory: URL? = nil) async throws -> AnalysisStore {
        let store = try AnalysisStore(directory: directory)
        try await store.migrate()
        return store
    }

    /// Tracks which database paths have already been migrated in this process
    /// to avoid redundant DDL work on repeated `open()` calls.
    private static let migratedLock = NSLock()
    nonisolated(unsafe) private static var migratedPaths: Set<String> = []

    /// Run pragmas and create all tables / indexes / FTS triggers. Safe to call
    /// more than once. Pragmas are always (re)applied since they live on the
    /// per-connection state, not on the database file. The schema DDL itself
    /// only runs once per database path per process — every DDL statement
    /// uses IF NOT EXISTS so re-running is correct, just unnecessary work.
    func migrate() throws {
        // M4 fix: pragmas must be applied to *every* connection. The previous
        // implementation short-circuited the entire migrate() call when the
        // path had already been seen, leaving second-instance connections with
        // foreign_keys=OFF, the default journal mode, and no busy_timeout.
        try configurePragmas()

        let path = databaseURL.path
        let alreadyDone = Self.migratedLock.withLock {
            !Self.migratedPaths.insert(path).inserted
        }
        guard !alreadyDone else { return }

        try createTables()
        try migrateTranscriptChunksPhase1()
        try writeInitialSchemaVersionIfNeeded()
    }

    private func migrateTranscriptChunksPhase1() throws {
        // Add columns for transcript identity. The old implementation parsed
        // the SQLite error string for "duplicate column name"; we now check
        // PRAGMA table_info first and only ALTER when the column is missing.
        try addColumnIfNeeded(table: "transcript_chunks", column: "transcriptVersion", definition: "TEXT")
        try addColumnIfNeeded(table: "transcript_chunks", column: "atomOrdinal", definition: "INTEGER")
    }

    private func columnExists(table: String, column: String) throws -> Bool {
        // PRAGMA table_info(...) cannot be parameterized via bind, so the
        // table name is interpolated. Both arguments are in-process constants
        // (no user input), so SQL injection is not in scope here.
        let sql = "PRAGMA table_info(\(table))"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        while sqlite3_step(stmt) == SQLITE_ROW {
            // Column 1 of table_info is the column name.
            let name = optionalText(stmt, 1) ?? ""
            if name == column { return true }
        }
        return false
    }

    private func addColumnIfNeeded(table: String, column: String, definition: String) throws {
        if try columnExists(table: table, column: column) {
            return
        }
        try exec("ALTER TABLE \(table) ADD COLUMN \(column) \(definition)")
    }

    /// Records `_meta('schema_version', '1')` on first migration. Future
    /// migrations should read this value, branch on it, and bump it inside the
    /// same transaction as the DDL change.
    private func writeInitialSchemaVersionIfNeeded() throws {
        let stmt = try prepare("INSERT OR IGNORE INTO _meta (key, value) VALUES ('schema_version', '1')")
        defer { sqlite3_finalize(stmt) }
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Reads the current schema version from `_meta`. Returns `nil` if the row
    /// is missing (only possible on a corrupted store, since `migrate()` writes
    /// it on first run).
    func schemaVersion() throws -> Int? {
        let stmt = try prepare("SELECT value FROM _meta WHERE key = 'schema_version'")
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        guard let raw = optionalText(stmt, 0) else { return nil }
        return Int(raw)
    }

    deinit {
        if let db { sqlite3_close_v2(db) }
    }

    static func defaultDirectory() -> URL {
        FileManager.default
            .urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("Playhead", isDirectory: true)
            .appendingPathComponent("AnalysisStore", isDirectory: true)
    }

    // MARK: Pragmas

    private func configurePragmas() throws {
        try exec("PRAGMA journal_mode = WAL")
        try exec("PRAGMA synchronous = NORMAL")
        try exec("PRAGMA foreign_keys = ON")
        try exec("PRAGMA busy_timeout = 3000")
    }

    // MARK: DDL

    private func createTables() throws {
        // _meta — anchor for schema version + future migration coordination.
        try exec("""
            CREATE TABLE IF NOT EXISTS _meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """)

        // analysis_assets
        try exec("""
            CREATE TABLE IF NOT EXISTS analysis_assets (
                id                          TEXT PRIMARY KEY,
                episodeId                   TEXT NOT NULL,
                assetFingerprint            TEXT NOT NULL,
                weakFingerprint             TEXT,
                sourceURL                   TEXT NOT NULL,
                featureCoverageEndTime      REAL,
                fastTranscriptCoverageEndTime REAL,
                confirmedAdCoverageEndTime  REAL,
                analysisState               TEXT NOT NULL DEFAULT 'new',
                analysisVersion             INTEGER NOT NULL DEFAULT 1,
                capabilitySnapshot          TEXT,
                createdAt                   REAL NOT NULL DEFAULT (strftime('%s', 'now'))
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_assets_episode ON analysis_assets(episodeId)")

        // analysis_sessions
        try exec("""
            CREATE TABLE IF NOT EXISTS analysis_sessions (
                id               TEXT PRIMARY KEY,
                analysisAssetId  TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                state            TEXT NOT NULL DEFAULT 'queued',
                startedAt        REAL NOT NULL,
                updatedAt        REAL NOT NULL,
                failureReason    TEXT
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_sessions_asset ON analysis_sessions(analysisAssetId)")

        // feature_windows
        try exec("""
            CREATE TABLE IF NOT EXISTS feature_windows (
                analysisAssetId   TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                startTime         REAL NOT NULL,
                endTime           REAL NOT NULL,
                rms               REAL NOT NULL,
                spectralFlux      REAL NOT NULL,
                musicProbability  REAL NOT NULL,
                pauseProbability  REAL NOT NULL,
                speakerClusterId  INTEGER,
                jingleHash        TEXT,
                featureVersion    INTEGER NOT NULL,
                PRIMARY KEY (analysisAssetId, startTime)
            )
            """)

        // transcript_chunks
        try exec("""
            CREATE TABLE IF NOT EXISTS transcript_chunks (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                segmentFingerprint  TEXT NOT NULL,
                chunkIndex          INTEGER NOT NULL,
                startTime           REAL NOT NULL,
                endTime             REAL NOT NULL,
                text                TEXT NOT NULL,
                normalizedText      TEXT NOT NULL,
                pass                TEXT NOT NULL DEFAULT 'fast',
                modelVersion        TEXT NOT NULL,
                transcriptVersion   TEXT,
                atomOrdinal         INTEGER
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_chunks_asset ON transcript_chunks(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_chunks_time ON transcript_chunks(analysisAssetId, startTime)")

        // ad_windows
        try exec("""
            CREATE TABLE IF NOT EXISTS ad_windows (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                startTime           REAL NOT NULL,
                endTime             REAL NOT NULL,
                confidence          REAL NOT NULL,
                boundaryState       TEXT NOT NULL,
                decisionState       TEXT NOT NULL DEFAULT 'candidate',
                detectorVersion     TEXT NOT NULL,
                advertiser          TEXT,
                product             TEXT,
                adDescription       TEXT,
                evidenceText        TEXT,
                evidenceStartTime   REAL,
                metadataSource      TEXT NOT NULL DEFAULT 'none',
                metadataConfidence  REAL,
                metadataPromptVersion TEXT,
                wasSkipped          INTEGER NOT NULL DEFAULT 0,
                userDismissedBanner INTEGER NOT NULL DEFAULT 0
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_ad_asset ON ad_windows(analysisAssetId)")

        // skip_cues
        try exec("""
            CREATE TABLE IF NOT EXISTS skip_cues (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL,
                cueHash TEXT NOT NULL,
                startTime REAL NOT NULL,
                endTime REAL NOT NULL,
                confidence REAL NOT NULL,
                source TEXT NOT NULL DEFAULT 'preAnalysis',
                materializedAt REAL NOT NULL,
                wasSkipped INTEGER NOT NULL DEFAULT 0,
                userDismissed INTEGER NOT NULL DEFAULT 0,
                UNIQUE(cueHash)
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_skip_cues_asset ON skip_cues(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_skip_cues_time ON skip_cues(analysisAssetId, startTime)")

        // podcast_profiles
        try exec("""
            CREATE TABLE IF NOT EXISTS podcast_profiles (
                podcastId                   TEXT PRIMARY KEY,
                sponsorLexicon              TEXT,
                normalizedAdSlotPriors      TEXT,
                repeatedCTAFragments        TEXT,
                jingleFingerprints          TEXT,
                implicitFalsePositiveCount  INTEGER NOT NULL DEFAULT 0,
                skipTrustScore              REAL NOT NULL DEFAULT 0.5,
                observationCount            INTEGER NOT NULL DEFAULT 0,
                mode                        TEXT NOT NULL DEFAULT 'shadow',
                recentFalseSkipSignals      INTEGER NOT NULL DEFAULT 0
            )
            """)

        // preview_budgets
        try exec("""
            CREATE TABLE IF NOT EXISTS preview_budgets (
                canonicalEpisodeKey      TEXT PRIMARY KEY,
                consumedAnalysisSeconds  REAL NOT NULL DEFAULT 0,
                graceBreakWindow         REAL NOT NULL DEFAULT 0,
                lastUpdated              REAL NOT NULL
            )
            """)

        // analysis_jobs
        try exec("""
            CREATE TABLE IF NOT EXISTS analysis_jobs (
                jobId TEXT PRIMARY KEY,
                jobType TEXT NOT NULL,
                episodeId TEXT NOT NULL,
                podcastId TEXT,
                analysisAssetId TEXT,
                workKey TEXT NOT NULL UNIQUE,
                sourceFingerprint TEXT NOT NULL,
                downloadId TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                desiredCoverageSec REAL NOT NULL,
                featureCoverageSec REAL NOT NULL DEFAULT 0,
                transcriptCoverageSec REAL NOT NULL DEFAULT 0,
                cueCoverageSec REAL NOT NULL DEFAULT 0,
                state TEXT NOT NULL DEFAULT 'queued',
                attemptCount INTEGER NOT NULL DEFAULT 0,
                nextEligibleAt REAL,
                leaseOwner TEXT,
                leaseExpiresAt REAL,
                lastErrorCode TEXT,
                createdAt REAL NOT NULL,
                updatedAt REAL NOT NULL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_jobs_state_priority ON analysis_jobs(state, priority DESC, createdAt ASC)")
        try exec("CREATE INDEX IF NOT EXISTS idx_jobs_workkey ON analysis_jobs(workKey)")
        try exec("CREATE INDEX IF NOT EXISTS idx_jobs_episode ON analysis_jobs(episodeId)")

        // backfill_jobs
        // M8: FK CASCADE so deleting an asset cleans up its backfill rows.
        // H16/M26: decisionCohortJSON removed (dead plumbing); podcastId
        // nullable because orphan/local episodes have no podcast.
        try exec("""
            CREATE TABLE IF NOT EXISTS backfill_jobs (
                jobId TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                podcastId TEXT,
                phase TEXT NOT NULL,
                coveragePolicy TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                progressCursor TEXT,
                retryCount INTEGER NOT NULL DEFAULT 0,
                deferReason TEXT,
                status TEXT NOT NULL DEFAULT 'queued',
                scanCohortJSON TEXT,
                createdAt REAL NOT NULL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_backfill_jobs_status_priority ON backfill_jobs(status, priority DESC, createdAt ASC)")
        try exec("CREATE INDEX IF NOT EXISTS idx_backfill_jobs_asset_phase ON backfill_jobs(analysisAssetId, phase)")

        // semantic_scan_results
        // C5: `reuseKeyHash` is a SHA-256 over the canonical concatenation of
        // (analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
        // scanPass, transcriptVersion, scanCohortJSON). UNIQUE on the hash
        // gives us bounded cache growth (one row per reuse key) without the
        // cost of indexing the long scanCohortJSON column directly. Insert
        // path uses INSERT OR REPLACE so the latest write wins.
        try exec("""
            CREATE TABLE IF NOT EXISTS semantic_scan_results (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                windowFirstAtomOrdinal INTEGER NOT NULL,
                windowLastAtomOrdinal INTEGER NOT NULL,
                windowStartTime REAL NOT NULL,
                windowEndTime REAL NOT NULL,
                scanPass TEXT NOT NULL,
                transcriptQuality TEXT NOT NULL,
                disposition TEXT NOT NULL,
                spansJSON TEXT NOT NULL,
                status TEXT NOT NULL,
                attemptCount INTEGER NOT NULL DEFAULT 0,
                errorContext TEXT,
                inputTokenCount INTEGER,
                outputTokenCount INTEGER,
                latencyMs REAL,
                prewarmHit INTEGER NOT NULL DEFAULT 0,
                scanCohortJSON TEXT NOT NULL,
                transcriptVersion TEXT NOT NULL,
                reuseKeyHash TEXT NOT NULL,
                UNIQUE(reuseKeyHash)
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_semantic_scan_results_asset_pass ON semantic_scan_results(analysisAssetId, scanPass)")
        // M1/L3: dropped `idx_semantic_scan_results_reuse` and
        // `idx_semantic_scan_results_reuse_cohort` — neither is used by the
        // primary reuse query (which now hits the UNIQUE(reuseKeyHash) index).
        // The asset_pass index above is sufficient for diagnostic listings.

        // evidence_events
        // H11: UNIQUE on (asset, eventType, sourceType, atomOrdinals, cohort).
        // Inserts use INSERT OR IGNORE for silent idempotent dedup.
        try exec("""
            CREATE TABLE IF NOT EXISTS evidence_events (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                eventType TEXT NOT NULL,
                sourceType TEXT NOT NULL,
                atomOrdinals TEXT NOT NULL,
                evidenceJSON TEXT NOT NULL,
                scanCohortJSON TEXT NOT NULL,
                createdAt REAL NOT NULL,
                UNIQUE(analysisAssetId, eventType, sourceType, atomOrdinals, scanCohortJSON)
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_evidence_events_asset_created ON evidence_events(analysisAssetId, createdAt ASC)")

        // FTS5 virtual table over transcript_chunks
        try exec("""
            CREATE VIRTUAL TABLE IF NOT EXISTS transcript_chunks_fts USING fts5(
                text,
                normalizedText,
                content='transcript_chunks',
                content_rowid='rowid'
            )
            """)

        // Content-sync triggers
        try exec("""
            CREATE TRIGGER IF NOT EXISTS transcript_chunks_ai AFTER INSERT ON transcript_chunks BEGIN
                INSERT INTO transcript_chunks_fts(rowid, text, normalizedText)
                VALUES (new.rowid, new.text, new.normalizedText);
            END
            """)
        try exec("""
            CREATE TRIGGER IF NOT EXISTS transcript_chunks_ad AFTER DELETE ON transcript_chunks BEGIN
                INSERT INTO transcript_chunks_fts(transcript_chunks_fts, rowid, text, normalizedText)
                VALUES ('delete', old.rowid, old.text, old.normalizedText);
            END
            """)
        try exec("""
            CREATE TRIGGER IF NOT EXISTS transcript_chunks_au AFTER UPDATE ON transcript_chunks BEGIN
                INSERT INTO transcript_chunks_fts(transcript_chunks_fts, rowid, text, normalizedText)
                VALUES ('delete', old.rowid, old.text, old.normalizedText);
                INSERT INTO transcript_chunks_fts(rowid, text, normalizedText)
                VALUES (new.rowid, new.text, new.normalizedText);
            END
            """)
    }

    // MARK: - CRUD: analysis_assets

    func insertAsset(_ asset: AnalysisAsset) throws {
        let sql = """
            INSERT INTO analysis_assets
            (id, episodeId, assetFingerprint, weakFingerprint, sourceURL,
             featureCoverageEndTime, fastTranscriptCoverageEndTime, confirmedAdCoverageEndTime,
             analysisState, analysisVersion, capabilitySnapshot)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, asset.id)
        bind(stmt, 2, asset.episodeId)
        bind(stmt, 3, asset.assetFingerprint)
        bind(stmt, 4, asset.weakFingerprint)
        bind(stmt, 5, asset.sourceURL)
        bind(stmt, 6, asset.featureCoverageEndTime)
        bind(stmt, 7, asset.fastTranscriptCoverageEndTime)
        bind(stmt, 8, asset.confirmedAdCoverageEndTime)
        bind(stmt, 9, asset.analysisState)
        bind(stmt, 10, asset.analysisVersion)
        bind(stmt, 11, asset.capabilitySnapshot)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchAsset(id: String) throws -> AnalysisAsset? {
        let sql = "SELECT * FROM analysis_assets WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readAsset(stmt)
    }

#if DEBUG
    /// Fetch every analysis asset in the store, ordered by creation time (newest first).
    ///
    /// DEBUG-ONLY: this method loads every `analysis_assets` row into memory in
    /// a single pass with no pagination, no `LIMIT`, and no streaming. It exists
    /// solely to back `DebugEpisodeExporter.buildLibraryExport`, which is itself
    /// `#if DEBUG`-gated. It is not safe for production callers — on a real
    /// listener's library this can OOM or stall the actor for seconds.
    ///
    /// If you find yourself wanting to call this from production code: STOP and
    /// add a paginated/streaming variant instead. Do not remove this `#if DEBUG`
    /// gate without first thinking through the scale implications.
    func fetchAllAssets() throws -> [AnalysisAsset] {
        let sql = "SELECT * FROM analysis_assets ORDER BY createdAt DESC, rowid DESC"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        var results: [AnalysisAsset] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readAsset(stmt))
        }
        return results
    }
#endif

    func updateAssetState(id: String, state: String) throws {
        let sql = "UPDATE analysis_assets SET analysisState = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, state)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchAssetByEpisodeId(_ episodeId: String) throws -> AnalysisAsset? {
        let sql = """
            SELECT *
            FROM analysis_assets
            WHERE episodeId = ?
            ORDER BY createdAt DESC, rowid DESC
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, episodeId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readAsset(stmt)
    }

    func deleteAsset(id: String) throws {
        let sql = "DELETE FROM analysis_assets WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    private func readAsset(_ stmt: OpaquePointer?) -> AnalysisAsset {
        AnalysisAsset(
            id: text(stmt, 0),
            episodeId: text(stmt, 1),
            assetFingerprint: text(stmt, 2),
            weakFingerprint: optionalText(stmt, 3),
            sourceURL: text(stmt, 4),
            featureCoverageEndTime: optionalDouble(stmt, 5),
            fastTranscriptCoverageEndTime: optionalDouble(stmt, 6),
            confirmedAdCoverageEndTime: optionalDouble(stmt, 7),
            analysisState: text(stmt, 8),
            analysisVersion: Int(sqlite3_column_int(stmt, 9)),
            capabilitySnapshot: optionalText(stmt, 10)
        )
    }

    // MARK: - CRUD: analysis_sessions

    func insertSession(_ session: AnalysisSession) throws {
        let sql = """
            INSERT INTO analysis_sessions (id, analysisAssetId, state, startedAt, updatedAt, failureReason)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, session.id)
        bind(stmt, 2, session.analysisAssetId)
        bind(stmt, 3, session.state)
        bind(stmt, 4, session.startedAt)
        bind(stmt, 5, session.updatedAt)
        bind(stmt, 6, session.failureReason)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchSession(id: String) throws -> AnalysisSession? {
        let sql = "SELECT * FROM analysis_sessions WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return AnalysisSession(
            id: text(stmt, 0),
            analysisAssetId: text(stmt, 1),
            state: text(stmt, 2),
            startedAt: sqlite3_column_double(stmt, 3),
            updatedAt: sqlite3_column_double(stmt, 4),
            failureReason: optionalText(stmt, 5)
        )
    }

    func fetchLatestSessionForAsset(assetId: String) throws -> AnalysisSession? {
        let sql = "SELECT * FROM analysis_sessions WHERE analysisAssetId = ? ORDER BY updatedAt DESC LIMIT 1"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return AnalysisSession(
            id: text(stmt, 0),
            analysisAssetId: text(stmt, 1),
            state: text(stmt, 2),
            startedAt: sqlite3_column_double(stmt, 3),
            updatedAt: sqlite3_column_double(stmt, 4),
            failureReason: optionalText(stmt, 5)
        )
    }

    func updateSessionState(id: String, state: String, failureReason: String? = nil) throws {
        let sql = "UPDATE analysis_sessions SET state = ?, updatedAt = ?, failureReason = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, state)
        bind(stmt, 2, Date().timeIntervalSince1970)
        bind(stmt, 3, failureReason)
        bind(stmt, 4, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func updateFeatureCoverage(id: String, endTime: Double) throws {
        let sql = "UPDATE analysis_assets SET featureCoverageEndTime = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, endTime)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    // MARK: - CRUD: feature_windows

    func insertFeatureWindows(_ windows: [FeatureWindow]) throws {
        guard !windows.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            for fw in windows {
                try insertFeatureWindow(fw)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func insertFeatureWindow(_ fw: FeatureWindow) throws {
        let sql = """
            INSERT INTO feature_windows
            (analysisAssetId, startTime, endTime, rms, spectralFlux,
             musicProbability, pauseProbability, speakerClusterId, jingleHash, featureVersion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, fw.analysisAssetId)
        bind(stmt, 2, fw.startTime)
        bind(stmt, 3, fw.endTime)
        bind(stmt, 4, fw.rms)
        bind(stmt, 5, fw.spectralFlux)
        bind(stmt, 6, fw.musicProbability)
        bind(stmt, 7, fw.pauseProbability)
        bind(stmt, 8, fw.speakerClusterId)
        bind(stmt, 9, fw.jingleHash)
        bind(stmt, 10, fw.featureVersion)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchFeatureWindows(assetId: String, from start: Double, to end: Double) throws -> [FeatureWindow] {
        let sql = """
            SELECT * FROM feature_windows
            WHERE analysisAssetId = ? AND startTime >= ? AND endTime <= ?
            ORDER BY startTime
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        bind(stmt, 2, start)
        bind(stmt, 3, end)
        var results: [FeatureWindow] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(FeatureWindow(
                analysisAssetId: text(stmt, 0),
                startTime: sqlite3_column_double(stmt, 1),
                endTime: sqlite3_column_double(stmt, 2),
                rms: sqlite3_column_double(stmt, 3),
                spectralFlux: sqlite3_column_double(stmt, 4),
                musicProbability: sqlite3_column_double(stmt, 5),
                pauseProbability: sqlite3_column_double(stmt, 6),
                speakerClusterId: optionalInt(stmt, 7),
                jingleHash: optionalText(stmt, 8),
                featureVersion: Int(sqlite3_column_int(stmt, 9))
            ))
        }
        return results
    }

    // MARK: - CRUD: transcript_chunks

    func insertTranscriptChunk(_ chunk: TranscriptChunk) throws {
        let sql = """
            INSERT INTO transcript_chunks
            (id, analysisAssetId, segmentFingerprint, chunkIndex, startTime, endTime,
             text, normalizedText, pass, modelVersion, transcriptVersion, atomOrdinal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, chunk.id)
        bind(stmt, 2, chunk.analysisAssetId)
        bind(stmt, 3, chunk.segmentFingerprint)
        bind(stmt, 4, chunk.chunkIndex)
        bind(stmt, 5, chunk.startTime)
        bind(stmt, 6, chunk.endTime)
        bind(stmt, 7, chunk.text)
        bind(stmt, 8, chunk.normalizedText)
        bind(stmt, 9, chunk.pass)
        bind(stmt, 10, chunk.modelVersion)
        bind(stmt, 11, chunk.transcriptVersion)
        bind(stmt, 12, chunk.atomOrdinal)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func insertTranscriptChunks(_ chunks: [TranscriptChunk]) throws {
        guard !chunks.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            for chunk in chunks {
                try insertTranscriptChunk(chunk)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func updateFastTranscriptCoverage(id: String, endTime: Double) throws {
        let sql = "UPDATE analysis_assets SET fastTranscriptCoverageEndTime = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, endTime)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func hasTranscriptChunk(analysisAssetId: String, segmentFingerprint: String) throws -> Bool {
        let sql = "SELECT 1 FROM transcript_chunks WHERE analysisAssetId = ? AND segmentFingerprint = ? LIMIT 1"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        bind(stmt, 2, segmentFingerprint)
        return sqlite3_step(stmt) == SQLITE_ROW
    }

    func fetchTranscriptChunks(assetId: String) throws -> [TranscriptChunk] {
        let sql = "SELECT * FROM transcript_chunks WHERE analysisAssetId = ? ORDER BY chunkIndex"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        var results: [TranscriptChunk] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readTranscriptChunk(stmt))
        }
        return results
    }

    func searchTranscripts(query: String) throws -> [TranscriptChunk] {
        // Sanitize the query for FTS5: strip double quotes, then wrap each
        // whitespace-separated token in double quotes so special characters
        // (*, AND, OR, NEAR, etc.) are treated as literal search terms.
        let sanitized = query
            .replacingOccurrences(of: "\"", with: "")
            .split(whereSeparator: \.isWhitespace)
            .map { "\"\($0)\"" }
            .joined(separator: " ")
        guard !sanitized.isEmpty else { return [] }

        let sql = """
            SELECT tc.* FROM transcript_chunks tc
            JOIN transcript_chunks_fts fts ON tc.rowid = fts.rowid
            WHERE transcript_chunks_fts MATCH ?
            ORDER BY rank
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, sanitized)
        var results: [TranscriptChunk] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readTranscriptChunk(stmt))
        }
        return results
    }

    private func readTranscriptChunk(_ stmt: OpaquePointer?) -> TranscriptChunk {
        TranscriptChunk(
            id: text(stmt, 0),
            analysisAssetId: text(stmt, 1),
            segmentFingerprint: text(stmt, 2),
            chunkIndex: Int(sqlite3_column_int(stmt, 3)),
            startTime: sqlite3_column_double(stmt, 4),
            endTime: sqlite3_column_double(stmt, 5),
            text: text(stmt, 6),
            normalizedText: text(stmt, 7),
            pass: text(stmt, 8),
            modelVersion: text(stmt, 9),
            transcriptVersion: optionalText(stmt, 10),
            atomOrdinal: optionalInt(stmt, 11)
        )
    }

    // MARK: - CRUD: ad_windows

    func insertAdWindow(_ ad: AdWindow) throws {
        let sql = """
            INSERT INTO ad_windows
            (id, analysisAssetId, startTime, endTime, confidence, boundaryState,
             decisionState, detectorVersion, advertiser, product, adDescription,
             evidenceText, evidenceStartTime, metadataSource, metadataConfidence,
             metadataPromptVersion, wasSkipped, userDismissedBanner)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, ad.id)
        bind(stmt, 2, ad.analysisAssetId)
        bind(stmt, 3, ad.startTime)
        bind(stmt, 4, ad.endTime)
        bind(stmt, 5, ad.confidence)
        bind(stmt, 6, ad.boundaryState)
        bind(stmt, 7, ad.decisionState)
        bind(stmt, 8, ad.detectorVersion)
        bind(stmt, 9, ad.advertiser)
        bind(stmt, 10, ad.product)
        bind(stmt, 11, ad.adDescription)
        bind(stmt, 12, ad.evidenceText)
        bind(stmt, 13, ad.evidenceStartTime)
        bind(stmt, 14, ad.metadataSource)
        bind(stmt, 15, ad.metadataConfidence)
        bind(stmt, 16, ad.metadataPromptVersion)
        bind(stmt, 17, ad.wasSkipped ? 1 : 0)
        bind(stmt, 18, ad.userDismissedBanner ? 1 : 0)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchAdWindows(assetId: String) throws -> [AdWindow] {
        let sql = "SELECT * FROM ad_windows WHERE analysisAssetId = ? ORDER BY startTime"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        var results: [AdWindow] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(AdWindow(
                id: text(stmt, 0),
                analysisAssetId: text(stmt, 1),
                startTime: sqlite3_column_double(stmt, 2),
                endTime: sqlite3_column_double(stmt, 3),
                confidence: sqlite3_column_double(stmt, 4),
                boundaryState: text(stmt, 5),
                decisionState: text(stmt, 6),
                detectorVersion: text(stmt, 7),
                advertiser: optionalText(stmt, 8),
                product: optionalText(stmt, 9),
                adDescription: optionalText(stmt, 10),
                evidenceText: optionalText(stmt, 11),
                evidenceStartTime: optionalDouble(stmt, 12),
                metadataSource: text(stmt, 13),
                metadataConfidence: optionalDouble(stmt, 14),
                metadataPromptVersion: optionalText(stmt, 15),
                wasSkipped: sqlite3_column_int(stmt, 16) != 0,
                userDismissedBanner: sqlite3_column_int(stmt, 17) != 0
            ))
        }
        return results
    }

    func updateAdWindowDecision(id: String, decisionState: String) throws {
        let sql = "UPDATE ad_windows SET decisionState = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, decisionState)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func insertAdWindows(_ windows: [AdWindow]) throws {
        guard !windows.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            for ad in windows {
                try insertAdWindow(ad)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func updateAdWindowMetadata(
        id: String,
        advertiser: String?,
        product: String?,
        evidenceText: String?,
        metadataSource: String,
        metadataConfidence: Double?,
        metadataPromptVersion: String?
    ) throws {
        let sql = """
            UPDATE ad_windows SET
                advertiser = ?, product = ?, evidenceText = ?,
                metadataSource = ?, metadataConfidence = ?, metadataPromptVersion = ?
            WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, advertiser)
        bind(stmt, 2, product)
        bind(stmt, 3, evidenceText)
        bind(stmt, 4, metadataSource)
        bind(stmt, 5, metadataConfidence)
        bind(stmt, 6, metadataPromptVersion)
        bind(stmt, 7, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func updateAdWindowWasSkipped(id: String, wasSkipped: Bool) throws {
        let sql = "UPDATE ad_windows SET wasSkipped = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, wasSkipped ? 1 : 0)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func updateConfirmedAdCoverage(id: String, endTime: Double) throws {
        let sql = "UPDATE analysis_assets SET confirmedAdCoverageEndTime = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, endTime)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchAllFeatureWindows(assetId: String) throws -> [FeatureWindow] {
        let sql = "SELECT * FROM feature_windows WHERE analysisAssetId = ? ORDER BY startTime"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        var results: [FeatureWindow] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(FeatureWindow(
                analysisAssetId: text(stmt, 0),
                startTime: sqlite3_column_double(stmt, 1),
                endTime: sqlite3_column_double(stmt, 2),
                rms: sqlite3_column_double(stmt, 3),
                spectralFlux: sqlite3_column_double(stmt, 4),
                musicProbability: sqlite3_column_double(stmt, 5),
                pauseProbability: sqlite3_column_double(stmt, 6),
                speakerClusterId: optionalInt(stmt, 7),
                jingleHash: optionalText(stmt, 8),
                featureVersion: Int(sqlite3_column_int(stmt, 9))
            ))
        }
        return results
    }

    // MARK: - CRUD: skip_cues

    func insertSkipCue(_ cue: SkipCue) throws {
        let sql = """
            INSERT OR IGNORE INTO skip_cues
            (id, analysisAssetId, cueHash, startTime, endTime, confidence,
             source, materializedAt, wasSkipped, userDismissed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, cue.id)
        bind(stmt, 2, cue.analysisAssetId)
        bind(stmt, 3, cue.cueHash)
        bind(stmt, 4, cue.startTime)
        bind(stmt, 5, cue.endTime)
        bind(stmt, 6, cue.confidence)
        bind(stmt, 7, cue.source)
        bind(stmt, 8, cue.materializedAt)
        bind(stmt, 9, cue.wasSkipped ? 1 : 0)
        bind(stmt, 10, cue.userDismissed ? 1 : 0)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func insertSkipCues(_ cues: [SkipCue]) throws {
        guard !cues.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            for cue in cues {
                try insertSkipCue(cue)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func fetchSkipCues(for analysisAssetId: String) throws -> [SkipCue] {
        let sql = "SELECT * FROM skip_cues WHERE analysisAssetId = ? ORDER BY startTime ASC"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        var results: [SkipCue] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readSkipCue(stmt))
        }
        return results
    }

    func markSkipCueSkipped(id: String) throws {
        let sql = "UPDATE skip_cues SET wasSkipped = 1 WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func markSkipCueDismissed(id: String) throws {
        let sql = "UPDATE skip_cues SET userDismissed = 1 WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    private func readSkipCue(_ stmt: OpaquePointer?) -> SkipCue {
        SkipCue(
            id: text(stmt, 0),
            analysisAssetId: text(stmt, 1),
            cueHash: text(stmt, 2),
            startTime: sqlite3_column_double(stmt, 3),
            endTime: sqlite3_column_double(stmt, 4),
            confidence: sqlite3_column_double(stmt, 5),
            source: text(stmt, 6),
            materializedAt: sqlite3_column_double(stmt, 7),
            wasSkipped: sqlite3_column_int(stmt, 8) != 0,
            userDismissed: sqlite3_column_int(stmt, 9) != 0
        )
    }

    // MARK: - CRUD: podcast_profiles

    func upsertProfile(_ profile: PodcastProfile) throws {
        let sql = """
            INSERT INTO podcast_profiles
            (podcastId, sponsorLexicon, normalizedAdSlotPriors, repeatedCTAFragments,
             jingleFingerprints, implicitFalsePositiveCount, skipTrustScore,
             observationCount, mode, recentFalseSkipSignals)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(podcastId) DO UPDATE SET
                sponsorLexicon = excluded.sponsorLexicon,
                normalizedAdSlotPriors = excluded.normalizedAdSlotPriors,
                repeatedCTAFragments = excluded.repeatedCTAFragments,
                jingleFingerprints = excluded.jingleFingerprints,
                implicitFalsePositiveCount = excluded.implicitFalsePositiveCount,
                skipTrustScore = excluded.skipTrustScore,
                observationCount = excluded.observationCount,
                mode = excluded.mode,
                recentFalseSkipSignals = excluded.recentFalseSkipSignals
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, profile.podcastId)
        bind(stmt, 2, profile.sponsorLexicon)
        bind(stmt, 3, profile.normalizedAdSlotPriors)
        bind(stmt, 4, profile.repeatedCTAFragments)
        bind(stmt, 5, profile.jingleFingerprints)
        bind(stmt, 6, profile.implicitFalsePositiveCount)
        bind(stmt, 7, profile.skipTrustScore)
        bind(stmt, 8, profile.observationCount)
        bind(stmt, 9, profile.mode)
        bind(stmt, 10, profile.recentFalseSkipSignals)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchProfile(podcastId: String) throws -> PodcastProfile? {
        let sql = "SELECT * FROM podcast_profiles WHERE podcastId = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return PodcastProfile(
            podcastId: text(stmt, 0),
            sponsorLexicon: optionalText(stmt, 1),
            normalizedAdSlotPriors: optionalText(stmt, 2),
            repeatedCTAFragments: optionalText(stmt, 3),
            jingleFingerprints: optionalText(stmt, 4),
            implicitFalsePositiveCount: Int(sqlite3_column_int(stmt, 5)),
            skipTrustScore: sqlite3_column_double(stmt, 6),
            observationCount: Int(sqlite3_column_int(stmt, 7)),
            mode: text(stmt, 8),
            recentFalseSkipSignals: Int(sqlite3_column_int(stmt, 9))
        )
    }

    // MARK: - CRUD: preview_budgets

    func upsertBudget(_ budget: PreviewBudget) throws {
        let sql = """
            INSERT INTO preview_budgets
            (canonicalEpisodeKey, consumedAnalysisSeconds, graceBreakWindow, lastUpdated)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(canonicalEpisodeKey) DO UPDATE SET
                consumedAnalysisSeconds = excluded.consumedAnalysisSeconds,
                graceBreakWindow = excluded.graceBreakWindow,
                lastUpdated = excluded.lastUpdated
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, budget.canonicalEpisodeKey)
        bind(stmt, 2, budget.consumedAnalysisSeconds)
        bind(stmt, 3, budget.graceBreakWindow)
        bind(stmt, 4, budget.lastUpdated)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchBudget(key: String) throws -> PreviewBudget? {
        let sql = "SELECT * FROM preview_budgets WHERE canonicalEpisodeKey = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, key)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return PreviewBudget(
            canonicalEpisodeKey: text(stmt, 0),
            consumedAnalysisSeconds: sqlite3_column_double(stmt, 1),
            graceBreakWindow: sqlite3_column_double(stmt, 2),
            lastUpdated: sqlite3_column_double(stmt, 3)
        )
    }

    // MARK: - CRUD: analysis_jobs

    @discardableResult
    func insertJob(_ job: AnalysisJob) throws -> Bool {
        let sql = """
            INSERT OR IGNORE INTO analysis_jobs
            (jobId, jobType, episodeId, podcastId, analysisAssetId, workKey,
             sourceFingerprint, downloadId, priority, desiredCoverageSec,
             featureCoverageSec, transcriptCoverageSec, cueCoverageSec,
             state, attemptCount, nextEligibleAt, leaseOwner, leaseExpiresAt,
             lastErrorCode, createdAt, updatedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, job.jobId)
        bind(stmt, 2, job.jobType)
        bind(stmt, 3, job.episodeId)
        bind(stmt, 4, job.podcastId)
        bind(stmt, 5, job.analysisAssetId)
        bind(stmt, 6, job.workKey)
        bind(stmt, 7, job.sourceFingerprint)
        bind(stmt, 8, job.downloadId)
        bind(stmt, 9, job.priority)
        bind(stmt, 10, job.desiredCoverageSec)
        bind(stmt, 11, job.featureCoverageSec)
        bind(stmt, 12, job.transcriptCoverageSec)
        bind(stmt, 13, job.cueCoverageSec)
        bind(stmt, 14, job.state)
        bind(stmt, 15, job.attemptCount)
        bind(stmt, 16, job.nextEligibleAt)
        bind(stmt, 17, job.leaseOwner)
        bind(stmt, 18, job.leaseExpiresAt)
        bind(stmt, 19, job.lastErrorCode)
        bind(stmt, 20, job.createdAt)
        bind(stmt, 21, job.updatedAt)
        try step(stmt, expecting: SQLITE_DONE)
        return sqlite3_changes(db) > 0
    }

    func fetchJob(byId jobId: String) throws -> AnalysisJob? {
        let sql = "SELECT * FROM analysis_jobs WHERE jobId = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, jobId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readJob(stmt)
    }

    func fetchNextEligibleJob(
        isCharging: Bool,
        isThermalOk: Bool,
        t0ThresholdSec: Double,
        now: TimeInterval
    ) throws -> AnalysisJob? {
        // T0 jobs: playback jobs that have zero coverage — always eligible.
        // Deferred jobs: backfill/preAnalysis require charging + thermal ok
        // and nextEligibleAt <= now (or NULL).
        let sql = """
            SELECT * FROM analysis_jobs
            WHERE (
                (state IN ('queued', 'paused')
                  AND (leaseOwner IS NULL OR leaseExpiresAt < ?)
                  AND (nextEligibleAt IS NULL OR nextEligibleAt <= ?))
                OR (state = 'failed' AND nextEligibleAt IS NOT NULL AND nextEligibleAt <= ?)
              )
              AND (
                (jobType = 'playback' AND featureCoverageSec < ?)
                OR (
                  ? = 1 AND ? = 1
                  AND (nextEligibleAt IS NULL OR nextEligibleAt <= ?)
                )
              )
            ORDER BY priority DESC, createdAt ASC
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, now)
        bind(stmt, 2, now)
        bind(stmt, 3, now)
        bind(stmt, 4, t0ThresholdSec)
        bind(stmt, 5, isCharging ? 1 : 0)
        bind(stmt, 6, isThermalOk ? 1 : 0)
        bind(stmt, 7, now)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readJob(stmt)
    }

    func updateJobProgress(
        jobId: String,
        featureCoverageSec: Double,
        transcriptCoverageSec: Double,
        cueCoverageSec: Double
    ) throws {
        let sql = """
            UPDATE analysis_jobs
            SET featureCoverageSec = ?, transcriptCoverageSec = ?, cueCoverageSec = ?,
                updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, featureCoverageSec)
        bind(stmt, 2, transcriptCoverageSec)
        bind(stmt, 3, cueCoverageSec)
        bind(stmt, 4, Date().timeIntervalSince1970)
        bind(stmt, 5, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func updateJobState(jobId: String, state: String, nextEligibleAt: Double? = nil, lastErrorCode: String? = nil) throws {
        let sql = """
            UPDATE analysis_jobs
            SET state = ?, nextEligibleAt = ?, lastErrorCode = ?, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, state)
        bind(stmt, 2, nextEligibleAt)
        bind(stmt, 3, lastErrorCode)
        bind(stmt, 4, Date().timeIntervalSince1970)
        bind(stmt, 5, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func acquireLease(jobId: String, owner: String, expiresAt: Double) throws -> Bool {
        let sql = """
            UPDATE analysis_jobs
            SET leaseOwner = ?, leaseExpiresAt = ?, state = 'running', updatedAt = ?
            WHERE jobId = ? AND (leaseOwner IS NULL OR leaseExpiresAt < ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        let now = Date().timeIntervalSince1970
        bind(stmt, 1, owner)
        bind(stmt, 2, expiresAt)
        bind(stmt, 3, now)
        bind(stmt, 4, jobId)
        bind(stmt, 5, now)
        try step(stmt, expecting: SQLITE_DONE)
        return sqlite3_changes(db) > 0
    }

    func releaseLease(jobId: String) throws {
        let sql = """
            UPDATE analysis_jobs
            SET leaseOwner = NULL, leaseExpiresAt = NULL, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, Date().timeIntervalSince1970)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func renewLease(jobId: String, newExpiresAt: Double) throws {
        let sql = """
            UPDATE analysis_jobs
            SET leaseExpiresAt = ?, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, newExpiresAt)
        bind(stmt, 2, Date().timeIntervalSince1970)
        bind(stmt, 3, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchJobsByState(_ state: String) throws -> [AnalysisJob] {
        let sql = "SELECT * FROM analysis_jobs WHERE state = ? ORDER BY priority DESC, createdAt ASC"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, state)
        var results: [AnalysisJob] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readJob(stmt))
        }
        return results
    }

    func fetchJobsWithExpiredLeases(before: TimeInterval) throws -> [AnalysisJob] {
        let sql = "SELECT * FROM analysis_jobs WHERE leaseOwner IS NOT NULL AND leaseExpiresAt < ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, before)
        var results: [AnalysisJob] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readJob(stmt))
        }
        return results
    }

    func deleteOldJobs(olderThan: TimeInterval, inStates: [String]) throws -> Int {
        guard !inStates.isEmpty else { return 0 }
        let placeholders = inStates.map { _ in "?" }.joined(separator: ", ")
        let sql = "DELETE FROM analysis_jobs WHERE updatedAt < ? AND state IN (\(placeholders))"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, olderThan)
        for (i, state) in inStates.enumerated() {
            bind(stmt, Int32(i + 2), state)
        }
        try step(stmt, expecting: SQLITE_DONE)
        return Int(sqlite3_changes(db))
    }

    func fetchAllJobEpisodeIds() throws -> Set<String> {
        let sql = "SELECT DISTINCT episodeId FROM analysis_jobs"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        var ids = Set<String>()
        while sqlite3_step(stmt) == SQLITE_ROW {
            ids.insert(text(stmt, 0))
        }
        return ids
    }

    /// Fetches episode IDs that have at least one active (non-terminal) job.
    func fetchActiveJobEpisodeIds() throws -> Set<String> {
        let sql = "SELECT DISTINCT episodeId FROM analysis_jobs WHERE state NOT IN ('complete', 'superseded')"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        var ids = Set<String>()
        while sqlite3_step(stmt) == SQLITE_ROW {
            ids.insert(text(stmt, 0))
        }
        return ids
    }

    /// Recovers an expired lease: sets state to queued, clears lease fields,
    /// and increments attemptCount.
    func recoverExpiredLease(jobId: String) throws {
        let sql = """
            UPDATE analysis_jobs
            SET state = 'queued', leaseOwner = NULL, leaseExpiresAt = NULL,
                attemptCount = attemptCount + 1, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, Date().timeIntervalSince1970)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Increments the attempt count for a job. Used after failures to drive exponential backoff.
    func incrementAttemptCount(jobId: String) throws {
        let sql = """
            UPDATE analysis_jobs
            SET attemptCount = attemptCount + 1, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, Date().timeIntervalSince1970)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Resets a failed job back to queued state, clearing the error and backoff.
    /// Used by reconciliation when a previously-failed episode's download is still present.
    func resetFailedJobToQueued(jobId: String) throws {
        let sql = """
            UPDATE analysis_jobs
            SET state = 'queued', nextEligibleAt = NULL, lastErrorCode = NULL, updatedAt = ?
            WHERE jobId = ? AND state = 'failed'
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, Date().timeIntervalSince1970)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Batch-updates the state (and optionally nextEligibleAt) for multiple jobs.
    func batchUpdateJobState(jobIds: [String], state: String, nextEligibleAt: Double? = nil) throws {
        guard !jobIds.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            let now = Date().timeIntervalSince1970
            let sql = """
                UPDATE analysis_jobs
                SET state = ?, nextEligibleAt = ?, updatedAt = ?
                WHERE jobId = ?
                """
            let stmt = try prepare(sql)
            defer { sqlite3_finalize(stmt) }
            for jobId in jobIds {
                sqlite3_reset(stmt)
                bind(stmt, 1, state)
                bind(stmt, 2, nextEligibleAt)
                bind(stmt, 3, now)
                bind(stmt, 4, jobId)
                try step(stmt, expecting: SQLITE_DONE)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    // MARK: - CRUD: backfill_jobs

    /// Inserts a new backfill job. Throws `AnalysisStoreError.duplicateJobId`
    /// if the row already exists — callers must explicitly choose between
    /// insert-new and update-existing semantics (H7).
    func insertBackfillJob(_ job: BackfillJob) throws {
        let sql = """
            INSERT INTO backfill_jobs
            (jobId, analysisAssetId, podcastId, phase, coveragePolicy, priority,
             progressCursor, retryCount, deferReason, status, scanCohortJSON,
             createdAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, job.jobId)
        bind(stmt, 2, job.analysisAssetId)
        bind(stmt, 3, job.podcastId)
        bind(stmt, 4, job.phase.rawValue)
        bind(stmt, 5, job.coveragePolicy.rawValue)
        bind(stmt, 6, job.priority)
        bind(stmt, 7, try encodeJSONString(job.progressCursor))
        bind(stmt, 8, job.retryCount)
        bind(stmt, 9, job.deferReason)
        bind(stmt, 10, job.status.rawValue)
        bind(stmt, 11, job.scanCohortJSON)
        bind(stmt, 12, job.createdAt)
        do {
            try step(stmt, expecting: SQLITE_DONE)
        } catch {
            // SQLite constraint errors come back as the primary code
            // SQLITE_CONSTRAINT (19); the extended subcodes are stable across
            // versions but are not exported as Swift symbols by the SQLite3
            // module. We hand-roll the literals here.
            //   SQLITE_CONSTRAINT_PRIMARYKEY = 19 | (6<<8) = 1555
            //   SQLITE_CONSTRAINT_UNIQUE     = 19 | (8<<8) = 2067
            let extended = sqlite3_extended_errcode(db)
            if extended == 1555 || extended == 2067 {
                throw AnalysisStoreError.duplicateJobId(job.jobId)
            }
            throw error
        }
    }

    func fetchBackfillJob(byId jobId: String) throws -> BackfillJob? {
        // Column order: jobId, analysisAssetId, podcastId, phase, coveragePolicy,
        // priority, progressCursor, retryCount, deferReason, status,
        // scanCohortJSON, createdAt.
        let sql = """
            SELECT jobId, analysisAssetId, podcastId, phase, coveragePolicy,
                   priority, progressCursor, retryCount, deferReason, status,
                   scanCohortJSON, createdAt
            FROM backfill_jobs WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, jobId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try readBackfillJob(stmt)
    }

    /// H5: progress-only checkpoint. Writes `progressCursor`, `retryCount`,
    /// and bumps no other fields. Use this for periodic in-flight progress
    /// updates so a concurrent or earlier `markBackfillJobDeferred` call is
    /// not silently overwritten.
    func checkpointBackfillJobProgress(
        jobId: String,
        progressCursor: BackfillProgressCursor?,
        retryCount: Int? = nil
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET progressCursor = ?, retryCount = COALESCE(?, retryCount)
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, try encodeJSONString(progressCursor))
        bind(stmt, 2, retryCount)
        bind(stmt, 3, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// H5: defer a job. Writes `status='deferred'` and the supplied reason
    /// while preserving the existing `progressCursor` so resumption from the
    /// last checkpoint still works.
    func markBackfillJobDeferred(
        jobId: String,
        reason: String
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'deferred', deferReason = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, reason)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// C-2: transition a job row to `status='running'` without clobbering
    /// `deferReason`, `progressCursor`, or `retryCount`. Preserving the
    /// `deferReason` on a running-after-defer transition keeps the audit
    /// trail intact: the row reflects that an earlier defer happened even
    /// as the next runner attempt starts executing.
    func markBackfillJobRunning(jobId: String) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'running'
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// C-2: terminal success transition. Writes the final `progressCursor`
    /// and flips `status='complete'` while preserving `deferReason` (audit
    /// trail) and `retryCount`.
    func markBackfillJobComplete(
        jobId: String,
        progressCursor: BackfillProgressCursor?
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'complete', progressCursor = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, try encodeJSONString(progressCursor))
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// C-2: terminal failure transition. The prior shim silently dropped
    /// `deferReason` on `.failed`; this method ensures the reason is
    /// written so operators can diagnose why a job failed without scraping
    /// logs.
    func markBackfillJobFailed(
        jobId: String,
        reason: String,
        retryCount: Int
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'failed', deferReason = ?, retryCount = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, reason)
        bind(stmt, 2, retryCount)
        bind(stmt, 3, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Legacy combined API. Now thin shim that delegates to the split methods.
    /// Production code should call `checkpointBackfillJobProgress`,
    /// `markBackfillJobDeferred`, `markBackfillJobRunning`,
    /// `markBackfillJobComplete`, or `markBackfillJobFailed` directly.
    @available(*, deprecated, message: "Use markBackfillJobRunning/Complete/Failed/Deferred or checkpointBackfillJobProgress")
    func checkpointBackfillJob(
        jobId: String,
        progressCursor: BackfillProgressCursor?,
        status: BackfillJobStatus = .running,
        retryCount: Int? = nil,
        deferReason: String? = nil
    ) throws {
        // Update only the running-status case to avoid clobbering deferReason.
        let sql: String
        if status == .deferred, let deferReason {
            sql = """
                UPDATE backfill_jobs
                SET progressCursor = ?, status = 'deferred',
                    retryCount = COALESCE(?, retryCount), deferReason = ?
                WHERE jobId = ?
                """
            let stmt = try prepare(sql)
            defer { sqlite3_finalize(stmt) }
            bind(stmt, 1, try encodeJSONString(progressCursor))
            bind(stmt, 2, retryCount)
            bind(stmt, 3, deferReason)
            bind(stmt, 4, jobId)
            try step(stmt, expecting: SQLITE_DONE)
        } else {
            sql = """
                UPDATE backfill_jobs
                SET progressCursor = ?, status = ?,
                    retryCount = COALESCE(?, retryCount)
                WHERE jobId = ?
                """
            let stmt = try prepare(sql)
            defer { sqlite3_finalize(stmt) }
            bind(stmt, 1, try encodeJSONString(progressCursor))
            bind(stmt, 2, status.rawValue)
            bind(stmt, 3, retryCount)
            bind(stmt, 4, jobId)
            try step(stmt, expecting: SQLITE_DONE)
        }
    }

    @discardableResult
    func advanceBackfillJobPhase(
        jobId: String,
        expecting currentPhase: BackfillJobPhase,
        to nextPhase: BackfillJobPhase,
        status: BackfillJobStatus = .queued
    ) throws -> Bool {
        let sql = """
            UPDATE backfill_jobs
            SET phase = ?, progressCursor = NULL, status = ?, deferReason = NULL
            WHERE jobId = ? AND phase = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, nextPhase.rawValue)
        bind(stmt, 2, status.rawValue)
        bind(stmt, 3, jobId)
        bind(stmt, 4, currentPhase.rawValue)
        try step(stmt, expecting: SQLITE_DONE)
        return sqlite3_changes(db) > 0
    }

    // MARK: - CRUD: semantic_scan_results

    /// Canonical column order shared by all `semantic_scan_results` readers:
    /// 0  id                         9  spansJSON           17 scanCohortJSON
    /// 1  analysisAssetId           10 status               18 transcriptVersion
    /// 2  windowFirstAtomOrdinal    11 attemptCount         19 reuseKeyHash
    /// 3  windowLastAtomOrdinal     12 errorContext
    /// 4  windowStartTime           13 inputTokenCount
    /// 5  windowEndTime             14 outputTokenCount
    /// 6  scanPass                  15 latencyMs
    /// 7  transcriptQuality         16 prewarmHit
    /// 8  disposition
    private static let semanticScanResultColumns = """
        id, analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
        windowStartTime, windowEndTime, scanPass, transcriptQuality,
        disposition, spansJSON, status, attemptCount, errorContext,
        inputTokenCount, outputTokenCount, latencyMs, prewarmHit,
        scanCohortJSON, transcriptVersion, reuseKeyHash
        """

    /// Computes the canonical reuse-key SHA-256 over the fields that govern
    /// FM scan reusability. The same field order is used in `fetchReusable…`,
    /// keeping inserts and lookups in lockstep.
    static func semanticScanReuseKeyHash(
        analysisAssetId: String,
        windowFirstAtomOrdinal: Int,
        windowLastAtomOrdinal: Int,
        scanPass: String,
        transcriptVersion: String,
        scanCohortJSON: String
    ) -> String {
        let canonical =
            "\(analysisAssetId)|\(windowFirstAtomOrdinal)|\(windowLastAtomOrdinal)|" +
            "\(scanPass)|\(transcriptVersion)|\(scanCohortJSON)"
        let digest = SHA256.hash(data: Data(canonical.utf8))
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    func insertSemanticScanResult(_ result: SemanticScanResult) throws {
        try validateScanCohortJSON(result.scanCohortJSON)
        let sql = """
            INSERT OR REPLACE INTO semantic_scan_results
            (id, analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
             windowStartTime, windowEndTime, scanPass, transcriptQuality,
             disposition, spansJSON, status, attemptCount, errorContext,
             inputTokenCount, outputTokenCount, latencyMs, prewarmHit,
             scanCohortJSON, transcriptVersion, reuseKeyHash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, result.id)
        bind(stmt, 2, result.analysisAssetId)
        bind(stmt, 3, result.windowFirstAtomOrdinal)
        bind(stmt, 4, result.windowLastAtomOrdinal)
        bind(stmt, 5, result.windowStartTime)
        bind(stmt, 6, result.windowEndTime)
        bind(stmt, 7, result.scanPass)
        bind(stmt, 8, result.transcriptQuality.rawValue)
        bind(stmt, 9, result.disposition.rawValue)
        bind(stmt, 10, result.spansJSON)
        bind(stmt, 11, result.status.rawValue)
        bind(stmt, 12, result.attemptCount)
        bind(stmt, 13, result.errorContext)
        bind(stmt, 14, result.inputTokenCount)
        bind(stmt, 15, result.outputTokenCount)
        bind(stmt, 16, result.latencyMs)
        bind(stmt, 17, result.prewarmHit ? 1 : 0)
        bind(stmt, 18, result.scanCohortJSON)
        bind(stmt, 19, result.transcriptVersion)
        bind(stmt, 20, Self.semanticScanReuseKeyHash(
            analysisAssetId: result.analysisAssetId,
            windowFirstAtomOrdinal: result.windowFirstAtomOrdinal,
            windowLastAtomOrdinal: result.windowLastAtomOrdinal,
            scanPass: result.scanPass,
            transcriptVersion: result.transcriptVersion,
            scanCohortJSON: result.scanCohortJSON
        ))
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// M2: atomic Pass-B write. Wraps a scan result and its evidence events in
    /// a single `BEGIN IMMEDIATE … COMMIT`. Any thrown error rolls back.
    func recordSemanticScanResult(
        _ result: SemanticScanResult,
        evidenceEvents: [EvidenceEvent]
    ) throws {
        try validateScanCohortJSON(result.scanCohortJSON)
        for event in evidenceEvents {
            try validateAtomOrdinalsJSON(event.atomOrdinals)
            try validateScanCohortJSON(event.scanCohortJSON)
        }
        try exec("BEGIN IMMEDIATE")
        do {
            try insertSemanticScanResult(result)
            for event in evidenceEvents {
                try insertEvidenceEvent(event)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func fetchSemanticScanResult(id: String) throws -> SemanticScanResult? {
        // Column order: see `semanticScanResultColumns` above.
        let sql = "SELECT \(Self.semanticScanResultColumns) FROM semantic_scan_results WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try readSemanticScanResult(stmt)
    }

    func fetchSemanticScanResults(
        analysisAssetId: String,
        scanPass: String? = nil
    ) throws -> [SemanticScanResult] {
        // Column order: see `semanticScanResultColumns` above.
        let sql: String
        if scanPass != nil {
            sql = """
                SELECT \(Self.semanticScanResultColumns) FROM semantic_scan_results
                WHERE analysisAssetId = ? AND scanPass = ?
                ORDER BY windowFirstAtomOrdinal ASC, rowid ASC
                """
        } else {
            sql = """
                SELECT \(Self.semanticScanResultColumns) FROM semantic_scan_results
                WHERE analysisAssetId = ?
                ORDER BY windowFirstAtomOrdinal ASC, rowid ASC
                """
        }
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        if let scanPass {
            bind(stmt, 2, scanPass)
        }
        var results: [SemanticScanResult] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(try readSemanticScanResult(stmt))
        }
        return results
    }

    /// C6: only consider successful scans for reuse, and prefer the newest one
    /// by rowid. The previous implementation ordered by attemptCount DESC,
    /// which silently surfaced many-attempt failures over later successes.
    func fetchReusableSemanticScanResult(
        analysisAssetId: String,
        windowFirstAtomOrdinal: Int,
        windowLastAtomOrdinal: Int,
        scanPass: String,
        scanCohortJSON: String,
        transcriptVersion: String
    ) throws -> SemanticScanResult? {
        let sql = """
            SELECT \(Self.semanticScanResultColumns) FROM semantic_scan_results
            WHERE analysisAssetId = ?
              AND windowFirstAtomOrdinal = ?
              AND windowLastAtomOrdinal = ?
              AND scanPass = ?
              AND transcriptVersion = ?
              AND status = 'success'
            ORDER BY rowid DESC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        bind(stmt, 2, windowFirstAtomOrdinal)
        bind(stmt, 3, windowLastAtomOrdinal)
        bind(stmt, 4, scanPass)
        bind(stmt, 5, transcriptVersion)

        while sqlite3_step(stmt) == SQLITE_ROW {
            let result = try readSemanticScanResult(stmt)
            if result.isReusable(
                scanCohortJSON: scanCohortJSON,
                transcriptVersion: transcriptVersion
            ) {
                return result
            }
        }
        return nil
    }

    // MARK: - CRUD: evidence_events

    /// Canonical column order for `evidence_events` readers:
    /// 0 id, 1 analysisAssetId, 2 eventType, 3 sourceType,
    /// 4 atomOrdinals, 5 evidenceJSON, 6 scanCohortJSON, 7 createdAt.
    private static let evidenceEventColumns = """
        id, analysisAssetId, eventType, sourceType,
        atomOrdinals, evidenceJSON, scanCohortJSON, createdAt
        """

    func insertEvidenceEvent(_ event: EvidenceEvent) throws {
        try validateAtomOrdinalsJSON(event.atomOrdinals)
        try validateScanCohortJSON(event.scanCohortJSON)
        // H11: silent dedup on (asset, eventType, sourceType, atomOrdinals,
        // scanCohortJSON). The PRIMARY KEY collision still throws so callers
        // that pass a stale UUID for *new* evidence get a loud failure rather
        // than silent overwrite.
        let dupSQL = """
            SELECT 1 FROM evidence_events
            WHERE analysisAssetId = ? AND eventType = ? AND sourceType = ?
              AND atomOrdinals = ? AND scanCohortJSON = ?
            LIMIT 1
            """
        let dupStmt = try prepare(dupSQL)
        defer { sqlite3_finalize(dupStmt) }
        bind(dupStmt, 1, event.analysisAssetId)
        bind(dupStmt, 2, event.eventType)
        bind(dupStmt, 3, event.sourceType.rawValue)
        bind(dupStmt, 4, event.atomOrdinals)
        bind(dupStmt, 5, event.scanCohortJSON)
        if sqlite3_step(dupStmt) == SQLITE_ROW {
            return // already present, silent dedup
        }

        let sql = """
            INSERT INTO evidence_events
            (id, analysisAssetId, eventType, sourceType, atomOrdinals,
             evidenceJSON, scanCohortJSON, createdAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, event.id)
        bind(stmt, 2, event.analysisAssetId)
        bind(stmt, 3, event.eventType)
        bind(stmt, 4, event.sourceType.rawValue)
        bind(stmt, 5, event.atomOrdinals)
        bind(stmt, 6, event.evidenceJSON)
        bind(stmt, 7, event.scanCohortJSON)
        bind(stmt, 8, event.createdAt)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchEvidenceEvents(analysisAssetId: String) throws -> [EvidenceEvent] {
        // Column order: see `evidenceEventColumns` above.
        let sql = """
            SELECT \(Self.evidenceEventColumns) FROM evidence_events
            WHERE analysisAssetId = ?
            ORDER BY createdAt ASC, rowid ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        var events: [EvidenceEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            events.append(try readEvidenceEvent(stmt))
        }
        return events
    }

    // MARK: - JSON validation helpers

    /// M25: parses `atomOrdinals` and verifies it's a JSON array of integers.
    private func validateAtomOrdinalsJSON(_ json: String) throws {
        guard let data = json.data(using: .utf8),
              let parsed = try? JSONSerialization.jsonObject(with: data),
              let array = parsed as? [Any] else {
            throw AnalysisStoreError.invalidEvidenceEvent("atomOrdinals must be a JSON array of integers, got: \(json.prefix(80))")
        }
        for element in array {
            if (element as? Int) == nil, (element as? NSNumber) == nil {
                throw AnalysisStoreError.invalidEvidenceEvent("atomOrdinals must be a JSON array of integers")
            }
        }
    }

    /// Validation hook for the persistence boundary. Allows malformed
    /// `scanCohortJSON` to be rejected early instead of silently failing the
    /// reuse comparison later.
    private func validateScanCohortJSON(_ json: String) throws {
        guard let data = json.data(using: .utf8),
              (try? JSONSerialization.jsonObject(with: data)) != nil else {
            throw AnalysisStoreError.invalidScanCohortJSON("not parseable as JSON")
        }
    }

    private func readJob(_ stmt: OpaquePointer?) -> AnalysisJob {
        AnalysisJob(
            jobId: text(stmt, 0),
            jobType: text(stmt, 1),
            episodeId: text(stmt, 2),
            podcastId: optionalText(stmt, 3),
            analysisAssetId: optionalText(stmt, 4),
            workKey: text(stmt, 5),
            sourceFingerprint: text(stmt, 6),
            downloadId: text(stmt, 7),
            priority: Int(sqlite3_column_int(stmt, 8)),
            desiredCoverageSec: sqlite3_column_double(stmt, 9),
            featureCoverageSec: sqlite3_column_double(stmt, 10),
            transcriptCoverageSec: sqlite3_column_double(stmt, 11),
            cueCoverageSec: sqlite3_column_double(stmt, 12),
            state: text(stmt, 13),
            attemptCount: Int(sqlite3_column_int(stmt, 14)),
            nextEligibleAt: optionalDouble(stmt, 15),
            leaseOwner: optionalText(stmt, 16),
            leaseExpiresAt: optionalDouble(stmt, 17),
            lastErrorCode: optionalText(stmt, 18),
            createdAt: sqlite3_column_double(stmt, 19),
            updatedAt: sqlite3_column_double(stmt, 20)
        )
    }

    private func readSemanticScanResult(_ stmt: OpaquePointer?) throws -> SemanticScanResult {
        let transcriptQualityRaw = try requireText(stmt, 7)
        guard let transcriptQuality = TranscriptQuality(rawValue: transcriptQualityRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown transcript quality '\(transcriptQualityRaw)'")
        }

        let dispositionRaw = try requireText(stmt, 8)
        guard let disposition = CoarseDisposition(rawValue: dispositionRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown coarse disposition '\(dispositionRaw)'")
        }

        let statusRaw = try requireText(stmt, 10)
        guard let status = SemanticScanStatus(rawValue: statusRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown semantic scan status '\(statusRaw)'")
        }

        return SemanticScanResult(
            id: try requireText(stmt, 0),
            analysisAssetId: try requireText(stmt, 1),
            windowFirstAtomOrdinal: Int(sqlite3_column_int(stmt, 2)),
            windowLastAtomOrdinal: Int(sqlite3_column_int(stmt, 3)),
            windowStartTime: sqlite3_column_double(stmt, 4),
            windowEndTime: sqlite3_column_double(stmt, 5),
            scanPass: try requireText(stmt, 6),
            transcriptQuality: transcriptQuality,
            disposition: disposition,
            spansJSON: try requireText(stmt, 9),
            status: status,
            attemptCount: Int(sqlite3_column_int(stmt, 11)),
            errorContext: optionalText(stmt, 12),
            inputTokenCount: optionalInt(stmt, 13),
            outputTokenCount: optionalInt(stmt, 14),
            latencyMs: optionalDouble(stmt, 15),
            prewarmHit: sqlite3_column_int(stmt, 16) != 0,
            scanCohortJSON: try requireText(stmt, 17),
            transcriptVersion: try requireText(stmt, 18)
        )
    }

    private func readEvidenceEvent(_ stmt: OpaquePointer?) throws -> EvidenceEvent {
        let sourceTypeRaw = try requireText(stmt, 3)
        guard let sourceType = EvidenceSourceType(rawValue: sourceTypeRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown evidence source type '\(sourceTypeRaw)'")
        }

        return EvidenceEvent(
            id: try requireText(stmt, 0),
            analysisAssetId: try requireText(stmt, 1),
            eventType: try requireText(stmt, 2),
            sourceType: sourceType,
            atomOrdinals: try requireText(stmt, 4),
            evidenceJSON: try requireText(stmt, 5),
            scanCohortJSON: try requireText(stmt, 6),
            createdAt: sqlite3_column_double(stmt, 7)
        )
    }

    private func readBackfillJob(_ stmt: OpaquePointer?) throws -> BackfillJob {
        let phaseRaw = try requireText(stmt, 3)
        guard let phase = BackfillJobPhase(rawValue: phaseRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown backfill phase '\(phaseRaw)'")
        }

        let coveragePolicyRaw = try requireText(stmt, 4)
        guard let coveragePolicy = CoveragePolicy(rawValue: coveragePolicyRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown coverage policy '\(coveragePolicyRaw)'")
        }

        let statusRaw = try requireText(stmt, 9)
        guard let status = BackfillJobStatus(rawValue: statusRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown backfill status '\(statusRaw)'")
        }

        return BackfillJob(
            jobId: try requireText(stmt, 0),
            analysisAssetId: try requireText(stmt, 1),
            podcastId: optionalText(stmt, 2),
            phase: phase,
            coveragePolicy: coveragePolicy,
            priority: Int(sqlite3_column_int(stmt, 5)),
            progressCursor: try decodeJSON(BackfillProgressCursor.self, from: optionalText(stmt, 6)),
            retryCount: Int(sqlite3_column_int(stmt, 7)),
            deferReason: optionalText(stmt, 8),
            status: status,
            scanCohortJSON: optionalText(stmt, 10),
            createdAt: sqlite3_column_double(stmt, 11)
        )
    }

    // MARK: - SQLite helpers

    private func exec(_ sql: String) throws {
        var errMsg: UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(db, sql, nil, nil, &errMsg)
        if rc != SQLITE_OK {
            let msg = errMsg.map { String(cString: $0) } ?? "unknown error"
            sqlite3_free(errMsg)
            throw AnalysisStoreError.migrationFailed("\(msg) (SQL: \(sql.prefix(120)))")
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer? {
        var stmt: OpaquePointer?
        let rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
        guard rc == SQLITE_OK else {
            // H9: SQLite may have allocated a partial statement before
            // failing — finalize unconditionally to avoid the leak.
            sqlite3_finalize(stmt)
            let msg = String(cString: sqlite3_errmsg(db))
            throw AnalysisStoreError.queryFailed("\(msg) (SQL: \(sql.prefix(120)))")
        }
        return stmt
    }

    private func step(_ stmt: OpaquePointer?, expecting expected: Int32) throws {
        let rc = sqlite3_step(stmt)
        guard rc == expected else {
            let msg = String(cString: sqlite3_errmsg(db))
            throw AnalysisStoreError.insertFailed(msg)
        }
    }

    // Bind helpers

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: String?) {
        if let value {
            // `withCString` guarantees the pointer is valid for the closure, and
            // `SQLITE_TRANSIENT` tells SQLite to copy the bytes immediately, so
            // no autoreleased NSString trampoline is needed per bind call.
            value.withCString { cstr in
                _ = sqlite3_bind_text(stmt, idx, cstr, -1, SQLITE_TRANSIENT_PTR)
            }
        } else {
            sqlite3_bind_null(stmt, idx)
        }
    }

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: Double?) {
        if let value {
            sqlite3_bind_double(stmt, idx, value)
        } else {
            sqlite3_bind_null(stmt, idx)
        }
    }

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: Int?) {
        if let value {
            sqlite3_bind_int(stmt, idx, Int32(value))
        } else {
            sqlite3_bind_null(stmt, idx)
        }
    }

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: Int) {
        sqlite3_bind_int(stmt, idx, Int32(value))
    }

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: Double) {
        sqlite3_bind_double(stmt, idx, value)
    }

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: String) {
        value.withCString { cstr in
            _ = sqlite3_bind_text(stmt, idx, cstr, -1, SQLITE_TRANSIENT_PTR)
        }
    }

    // Read helpers

    /// Read a NOT NULL text column. If the column is unexpectedly NULL, returns
    /// an empty string. Use ``optionalText(_:_:)`` for nullable columns.
    ///
    /// NOTE: This silent NULL → "" coercion is preserved for legacy readers
    /// (`readAsset`, `readSkipCue`, `readJob`, etc.) that are non-throwing.
    /// New code on the persistence boundary should call ``requireText(_:_:)``
    /// instead so an unexpected NULL throws `AnalysisStoreError.invalidRow`.
    private func text(_ stmt: OpaquePointer?, _ idx: Int32) -> String {
        sqlite3_column_text(stmt, idx).map { String(cString: $0) } ?? ""
    }

    /// M9: throwing variant of `text(_:_:)`. Throws
    /// `AnalysisStoreError.invalidRow` when a non-null column is unexpectedly
    /// NULL instead of masking the issue with an empty string.
    private func requireText(_ stmt: OpaquePointer?, _ idx: Int32) throws -> String {
        guard sqlite3_column_type(stmt, idx) != SQLITE_NULL,
              let cstr = sqlite3_column_text(stmt, idx) else {
            throw AnalysisStoreError.invalidRow(column: Int(idx))
        }
        return String(cString: cstr)
    }

    private func optionalText(_ stmt: OpaquePointer?, _ idx: Int32) -> String? {
        guard sqlite3_column_type(stmt, idx) != SQLITE_NULL else { return nil }
        return sqlite3_column_text(stmt, idx).map { String(cString: $0) }
    }

    private func optionalDouble(_ stmt: OpaquePointer?, _ idx: Int32) -> Double? {
        guard sqlite3_column_type(stmt, idx) != SQLITE_NULL else { return nil }
        return sqlite3_column_double(stmt, idx)
    }

    private func optionalInt(_ stmt: OpaquePointer?, _ idx: Int32) -> Int? {
        guard sqlite3_column_type(stmt, idx) != SQLITE_NULL else { return nil }
        return Int(sqlite3_column_int(stmt, idx))
    }

    private func encodeJSONString<T: Encodable>(_ value: T?) throws -> String? {
        guard let value else { return nil }
        let data = try JSONEncoder().encode(value)
        return String(decoding: data, as: UTF8.self)
    }

    private func decodeJSON<T: Decodable>(_ type: T.Type, from json: String?) throws -> T? {
        guard let json else { return nil }
        do {
            return try JSONDecoder().decode(T.self, from: Data(json.utf8))
        } catch {
            throw AnalysisStoreError.queryFailed("Failed to decode \(T.self): \(error)")
        }
    }
}
