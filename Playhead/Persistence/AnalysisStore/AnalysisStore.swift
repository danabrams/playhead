// AnalysisStore.swift
// SQLite/FTS5 persistence for analysis pipeline state: transcription chunks,
// feature windows, ad windows, podcast profiles, and preview budgets.
// Separated from SwiftData because this data is append-heavy, versioned,
// needs FTS5, and supports resumable processing with checkpointing.

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

enum AnalysisStoreError: Error, CustomStringConvertible {
    case openFailed(code: Int32, message: String)
    case migrationFailed(String)
    case queryFailed(String)
    case insertFailed(String)
    case notFound

    var description: String {
        switch self {
        case .openFailed(let code, let msg): "SQLite open failed (\(code)): \(msg)"
        case .migrationFailed(let msg): "Migration failed: \(msg)"
        case .queryFailed(let msg): "Query failed: \(msg)"
        case .insertFailed(let msg): "Insert failed: \(msg)"
        case .notFound: "Row not found"
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
    /// more than once (every DDL statement uses IF NOT EXISTS). Skips work if
    /// this database path has already been migrated in the current process.
    func migrate() throws {
        let path = databaseURL.path
        let alreadyDone = Self.migratedLock.withLock {
            !Self.migratedPaths.insert(path).inserted
        }
        guard !alreadyDone else { return }

        try configurePragmas()
        try createTables()
        try migrateTranscriptChunksPhase1()
    }

    private func migrateTranscriptChunksPhase1() throws {
        // Add columns for transcript identity. ALTER TABLE ADD COLUMN fails
        // if the column already exists — catch and ignore that specific case.
        try addColumnIfNeeded(table: "transcript_chunks", column: "transcriptVersion", definition: "TEXT")
        try addColumnIfNeeded(table: "transcript_chunks", column: "atomOrdinal", definition: "INTEGER")
    }

    private func addColumnIfNeeded(table: String, column: String, definition: String) throws {
        do {
            try exec("ALTER TABLE \(table) ADD COLUMN \(column) \(definition)")
        } catch let error as AnalysisStoreError {
            // SQLite reports "duplicate column name" when the column already exists.
            // Only swallow that specific case; re-throw anything else (disk full,
            // corruption, permissions, etc.).
            if case .migrationFailed(let msg) = error, msg.contains("duplicate column") {
                return
            }
            throw error
        }
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

    /// Fetch every analysis asset in the store, ordered by creation time (newest first).
    /// Used for library-wide exports and diagnostic reporting.
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
            sqlite3_bind_text(stmt, idx, (value as NSString).utf8String, -1, SQLITE_TRANSIENT_PTR)
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
        sqlite3_bind_text(stmt, idx, (value as NSString).utf8String, -1, SQLITE_TRANSIENT_PTR)
    }

    // Read helpers

    /// Read a NOT NULL text column. If the column is unexpectedly NULL, returns
    /// an empty string. Use ``optionalText(_:_:)`` for nullable columns.
    private func text(_ stmt: OpaquePointer?, _ idx: Int32) -> String {
        sqlite3_column_text(stmt, idx).map { String(cString: $0) } ?? ""
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
}
