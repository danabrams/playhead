// AnalysisStore.swift
// SQLite/FTS5 persistence for analysis pipeline state: transcription chunks,
// feature windows, ad windows, podcast profiles, and preview budgets.
// Separated from SwiftData because this data is append-heavy, versioned,
// needs FTS5, and supports resumable processing with checkpointing.

import Foundation
import SQLite3

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
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX
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

    /// Run pragmas and create all tables / indexes / FTS triggers. Safe to call
    /// more than once (every DDL statement uses IF NOT EXISTS).
    func migrate() throws {
        try configurePragmas()
        try createTables()
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
                capabilitySnapshot          TEXT
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_assets_episode ON analysis_assets(episodeId)")

        // analysis_sessions
        try exec("""
            CREATE TABLE IF NOT EXISTS analysis_sessions (
                id               TEXT PRIMARY KEY,
                analysisAssetId  TEXT NOT NULL REFERENCES analysis_assets(id),
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
                analysisAssetId   TEXT NOT NULL REFERENCES analysis_assets(id),
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
                analysisAssetId     TEXT NOT NULL REFERENCES analysis_assets(id),
                segmentFingerprint  TEXT NOT NULL,
                chunkIndex          INTEGER NOT NULL,
                startTime           REAL NOT NULL,
                endTime             REAL NOT NULL,
                text                TEXT NOT NULL,
                normalizedText      TEXT NOT NULL,
                pass                TEXT NOT NULL DEFAULT 'fast',
                modelVersion        TEXT NOT NULL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_chunks_asset ON transcript_chunks(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_chunks_time ON transcript_chunks(analysisAssetId, startTime)")

        // ad_windows
        try exec("""
            CREATE TABLE IF NOT EXISTS ad_windows (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL REFERENCES analysis_assets(id),
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

    func updateAssetState(id: String, state: String) throws {
        let sql = "UPDATE analysis_assets SET analysisState = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, state)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchAssetByEpisodeId(_ episodeId: String) throws -> AnalysisAsset? {
        let sql = "SELECT * FROM analysis_assets WHERE episodeId = ? ORDER BY rowid DESC LIMIT 1"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, episodeId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readAsset(stmt)
    }

    func deleteAsset(id: String) throws {
        try exec("DELETE FROM analysis_assets WHERE id = '\(id)'")
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
             text, normalizedText, pass, modelVersion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        let sql = """
            SELECT tc.* FROM transcript_chunks tc
            JOIN transcript_chunks_fts fts ON tc.rowid = fts.rowid
            WHERE transcript_chunks_fts MATCH ?
            ORDER BY rank
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, query)
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
            modelVersion: text(stmt, 9)
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
            sqlite3_bind_text(stmt, idx, (value as NSString).utf8String, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
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
        sqlite3_bind_text(stmt, idx, (value as NSString).utf8String, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
    }

    // Read helpers

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
