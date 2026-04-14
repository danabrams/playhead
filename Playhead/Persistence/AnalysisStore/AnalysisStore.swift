// AnalysisStore.swift
// SQLite/FTS5 persistence for analysis pipeline state: transcription chunks,
// feature windows, ad windows, podcast profiles, and preview budgets.
// Separated from SwiftData because this data is append-heavy, versioned,
// needs FTS5, and supports resumable processing with checkpointing.

import CryptoKit
import Foundation
import OSLog
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
    /// bd-3bz (Phase 4): flag set when the Foundation Models shadow phase
    /// bailed on `canUseFoundationModels == false`. A capability observer in
    /// `PlayheadRuntime` drains sessions with this flag after FM recovers.
    /// Defaults to `false` so pre-existing rows decode identically.
    let needsShadowRetry: Bool
    /// bd-3bz (Phase 4): the podcastId captured at the point the shadow
    /// phase bailed. Needed to reconstruct the shadow-phase inputs during a
    /// retry drain without reaching back into the coordinator. `nil` unless
    /// `needsShadowRetry == true`.
    let shadowRetryPodcastId: String?

    init(
        id: String,
        analysisAssetId: String,
        state: String,
        startedAt: Double,
        updatedAt: Double,
        failureReason: String?,
        needsShadowRetry: Bool = false,
        shadowRetryPodcastId: String? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.state = state
        self.startedAt = startedAt
        self.updatedAt = updatedAt
        self.failureReason = failureReason
        self.needsShadowRetry = needsShadowRetry
        self.shadowRetryPodcastId = shadowRetryPodcastId
    }
}

struct FeatureWindow: Sendable {
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let rms: Double
    let spectralFlux: Double
    let musicProbability: Double
    let speakerChangeProxyScore: Double
    let musicBedChangeScore: Double
    let musicBedOnsetScore: Double
    let musicBedOffsetScore: Double
    let musicBedLevel: MusicBedLevel
    let pauseProbability: Double
    let speakerClusterId: Int?
    let jingleHash: String?
    let featureVersion: Int

    init(
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        rms: Double,
        spectralFlux: Double,
        musicProbability: Double,
        speakerChangeProxyScore: Double = 0,
        musicBedChangeScore: Double = 0,
        musicBedOnsetScore: Double = 0,
        musicBedOffsetScore: Double = 0,
        musicBedLevel: MusicBedLevel = .none,
        pauseProbability: Double,
        speakerClusterId: Int?,
        jingleHash: String?,
        featureVersion: Int
    ) {
        self.analysisAssetId = analysisAssetId
        self.startTime = startTime
        self.endTime = endTime
        self.rms = rms
        self.spectralFlux = spectralFlux
        self.musicProbability = musicProbability
        self.speakerChangeProxyScore = speakerChangeProxyScore
        self.musicBedChangeScore = musicBedChangeScore
        self.musicBedOnsetScore = musicBedOnsetScore
        self.musicBedOffsetScore = musicBedOffsetScore
        self.musicBedLevel = musicBedLevel
        self.pauseProbability = pauseProbability
        self.speakerClusterId = speakerClusterId
        self.jingleHash = jingleHash
        self.featureVersion = featureVersion
    }
}

struct FeatureExtractionCheckpoint: Sendable, Equatable {
    let analysisAssetId: String
    let lastWindowStartTime: Double
    let lastWindowEndTime: Double
    let lastRms: Double
    let lastMusicProbability: Double
    let lastRawSpeakerChangeProxyScore: Double
    let penultimateRawSpeakerChangeProxyScore: Double?
    let lastMagnitudes: [Float]
    let featureVersion: Int
}

struct FeatureWindowSpeakerChangeProxyUpdate: Sendable, Equatable {
    let assetId: String
    let startTime: Double
    let endTime: Double
    let featureVersion: Int
    let speakerChangeProxyScore: Double
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
    let weakAnchorMetadata: TranscriptWeakAnchorMetadata?

    init(
        id: String,
        analysisAssetId: String,
        segmentFingerprint: String,
        chunkIndex: Int,
        startTime: Double,
        endTime: Double,
        text: String,
        normalizedText: String,
        pass: String,
        modelVersion: String,
        transcriptVersion: String?,
        atomOrdinal: Int?,
        weakAnchorMetadata: TranscriptWeakAnchorMetadata? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.segmentFingerprint = segmentFingerprint
        self.chunkIndex = chunkIndex
        self.startTime = startTime
        self.endTime = endTime
        self.text = text
        self.normalizedText = normalizedText
        self.pass = pass
        self.modelVersion = modelVersion
        self.transcriptVersion = transcriptVersion
        self.atomOrdinal = atomOrdinal
        self.weakAnchorMetadata = weakAnchorMetadata
    }
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
    let evidenceSources: String?
    let eligibilityGate: String?

    init(
        id: String,
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        confidence: Double,
        boundaryState: String,
        decisionState: String,
        detectorVersion: String,
        advertiser: String?,
        product: String?,
        adDescription: String?,
        evidenceText: String?,
        evidenceStartTime: Double?,
        metadataSource: String,
        metadataConfidence: Double?,
        metadataPromptVersion: String?,
        wasSkipped: Bool,
        userDismissedBanner: Bool,
        evidenceSources: String? = nil,
        eligibilityGate: String? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.startTime = startTime
        self.endTime = endTime
        self.confidence = confidence
        self.boundaryState = boundaryState
        self.decisionState = decisionState
        self.detectorVersion = detectorVersion
        self.advertiser = advertiser
        self.product = product
        self.adDescription = adDescription
        self.evidenceText = evidenceText
        self.evidenceStartTime = evidenceStartTime
        self.metadataSource = metadataSource
        self.metadataConfidence = metadataConfidence
        self.metadataPromptVersion = metadataPromptVersion
        self.wasSkipped = wasSkipped
        self.userDismissedBanner = userDismissedBanner
        self.evidenceSources = evidenceSources
        self.eligibilityGate = eligibilityGate
    }
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

/// bd-m8k: Per-podcast CoveragePlanner state. Sibling row to
/// `PodcastProfile`; persisted in the `podcast_planner_state` table that the
/// v4 migration creates. Rows are upserted lazily on first observation, never
/// backfilled. `recallSamples` is the most-recent-up-to-3 ring of full-
/// rescan **recall** measurements (oldest first); the cached
/// `stableRecallFlag` reflects the result of evaluating both the episode-
/// count floor and the recall threshold against this ring at the moment
/// of the last write.
///
/// Cycle 2 C4: the metric was historically misnamed "precision" — it is
/// actually recall (covered / actual ad line refs). The struct fields use
/// the corrected name; the persisted SQLite columns and JSON keys keep the
/// legacy `precision*` names so existing v4 rows decode without a
/// migration. Each storage boundary is annotated with
/// `// historical: stored as "precision"; semantically recall`.
struct PodcastPlannerState: Sendable, Equatable {
    let podcastId: String
    let observedEpisodeCount: Int
    let episodesSinceLastFullRescan: Int
    /// Cycle 2 C4: stored as `stablePrecisionFlag` in SQLite; semantically
    /// the stable-recall flag. See type-level doc.
    let stableRecallFlag: Bool
    let lastFullRescanAt: Double?
    /// Most recent up to `AnalysisStore.plannerRecallRingSize` full-rescan
    /// recall samples. Oldest first; new samples are appended and the
    /// oldest dropped on overflow.
    /// Cycle 2 C4: stored across `precisionSample1..3` columns; semantically
    /// recall. See type-level doc.
    let recallSamples: [Double]
    /// Cycle 4 B4: per-podcast running total of episodes observed that
    /// produced no recall sample (ad-free full rescans). Persisted on
    /// `podcast_planner_state` so the counter accrues across
    /// `BackfillJobRunner` instances and across process restarts — the
    /// runner-level counter of the same name is per-run only and was
    /// therefore always 0 or 1 when read. Legacy rows that predate this
    /// column decode as 0.
    let episodesObservedWithoutSampleCount: Int
    /// Cycle 4 B4: per-podcast running total of episodes where every
    /// non-fullEpisodeScan narrowing phase returned `wasEmpty == true`.
    /// Increments fire on BOTH full rescans and live targeted-with-audit
    /// runs, so the counter captures the cross-phase empty signal that
    /// individual `narrowing.empty.{phase}` cannot. Legacy rows decode
    /// as 0.
    let narrowingAllPhasesEmptyEpisodeCount: Int
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
    /// C-2: raised when a backfill-job state transition is attempted against
    /// a row whose current status does not permit it (e.g. transitioning a
    /// `.complete` or `.failed` row back into `.running`).
    ///
    /// Fix #6: `fromStatus` carries the row's prior status at the moment
    /// the transition was rejected. It is `nil` when no row existed for
    /// the `jobId`, so callers can distinguish "missing" from "in a
    /// specific terminal state" without re-querying.
    case invalidStateTransition(jobId: String, fromStatus: String?, toStatus: String)
    /// H-2: raised when `insertEvidenceEvent` encounters a PRIMARY KEY
    /// collision where the existing row's body (evidenceJSON/createdAt)
    /// differs from the incoming row. The M-4 INSERT OR IGNORE path was
    /// silently preserving the stored body; callers that truly collide on
    /// id but with different content now get a loud failure.
    case evidenceEventBodyMismatch(id: String)

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
        case .invalidStateTransition(let id, let from, let to):
            "Invalid backfill job state transition for \(id): \(from ?? "<missing>") -> \(to)"
        case .evidenceEventBodyMismatch(let id):
            "Evidence event id '\(id)' already persisted with a different body"
        }
    }
}

// MARK: - AnalysisStore actor

actor AnalysisStore {

    nonisolated private static let currentSchemaVersion = 6

    /// bd-m8k / Cycle 2 C4: Maximum number of recent full-rescan **recall**
    /// samples retained for the `stable_recall_flag` ring. Must match the
    /// column count in `podcast_planner_state` and the push/shift logic in
    /// `recordPodcastEpisodeObservation`. The persisted columns are still
    /// named `precisionSample{1,2,3}` / `precisionSampleCount`; the
    /// in-memory rename is code-only.
    nonisolated static let plannerRecallRingSize = 3

    /// bd-m8k / Cycle 2 C4: Minimum per-sample recall required for
    /// `stable_recall_flag` to flip true. All samples in the ring must
    /// clear this threshold. The persisted column is still named
    /// `stablePrecisionFlag`; semantically recall.
    nonisolated static let plannerRecallThreshold: Double = 0.85

    /// bd-m8k: Minimum `observed_episode_count` before
    /// `stable_precision_flag` is permitted to be true. Mirrors
    /// `CoveragePlanner.defaultColdStartEpisodeThreshold`.
    nonisolated static let plannerStableObservedEpisodeFloor = 5

    #if DEBUG
    enum FeatureBatchPersistenceFaultInjection: Equatable {
        case afterCoverageUpdateBeforeCommit
    }

    private var featureBatchPersistenceFaultInjection: FeatureBatchPersistenceFaultInjection?
    #endif

    /// bd-1tl: dedicated logger for store-level diagnostics that should
    /// reach Console.app on real devices without test scaffolding.
    private let logger = Logger(subsystem: "com.playhead", category: "AnalysisStore")

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

    /// Open a database at an explicit SQLite path, including `:memory:` for
    /// ephemeral in-memory databases (useful in unit tests). Call ``migrate()``
    /// after construction to create tables.
    init(path: String) throws {
        // `:memory:` is a special SQLite URI handled by the C API directly (line below).
        // databaseURL is metadata only; the DB opens via the raw path string.
        self.databaseURL = URL(fileURLWithPath: path)

        var handle: OpaquePointer?
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX
        let rc = sqlite3_open_v2(path, &handle, flags, nil)
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

        // C-1: hold the migration lock for the entire body, NOT just the
        // cache check. Two stores opening the same path concurrently used
        // to race: the loser skipped migration on a partially-built DB.
        //
        // C-1 (part 2): validate that the sqlite file on disk still exists
        // before trusting the cache. If the file was deleted (test teardown,
        // user clearing Library/Caches, etc.), drop the stale cache entry
        // and re-run migration against the fresh file. Previously the
        // cache short-circuited and returned a store whose tables did not
        // exist, blowing up on the first query.
        //
        // C-1 (part 3): mark `migratedPaths` only AFTER a successful COMMIT.
        // On any rollback path the path stays out of the cache so a retry
        // re-runs migration rather than silently accepting a half-built DB.
        let path = databaseURL.path
        Self.migratedLock.lock()
        defer { Self.migratedLock.unlock() }

        // The sqlite file may have been deleted out from under the static
        // cache since the last open on this path (test cleanup, user
        // clearing Library/Caches, etc.). `sqlite3_open_v2` with
        // `SQLITE_OPEN_CREATE` will have just recreated it as an empty
        // database, so we can't trust the filesystem presence check alone.
        // Instead, probe for the `_meta` table that `createTables()` always
        // builds: its absence means this connection is looking at a fresh
        // DB that still needs migration, regardless of the cache.
        if Self.migratedPaths.contains(path) {
            if try tableExists("_meta") {
                return
            }
            Self.migratedPaths.remove(path)
        }

        // H-5: wrap the whole migrate body in BEGIN IMMEDIATE … COMMIT so
        // a crash mid-migration cannot leave DDL applied without the
        // matching _meta schema_version row. SQLite supports transactional
        // DDL, so table creation, ALTER TABLE, and the version write all
        // roll back together on any thrown error.
        try exec("BEGIN IMMEDIATE")
        do {
            try createTables()
            // Ordering: transcript_chunks Phase 1 runs before the V*IfNeeded ladder because no later migration touches `transcript_chunks` or `transcript_chunks_fts`, so its FTS rebuild cannot be undone downstream; the backfill only depends on columns `createTables()` has already (re)asserted.
            try migrateTranscriptChunksPhase1()
            try writeInitialSchemaVersionIfNeeded()
            try migrateEvidenceEventsNaturalKeyV2IfNeeded()
            try migrateEvidenceEventsTranscriptVersionV3IfNeeded()
            try migrateAnalysisSessionsShadowRetryV4IfNeeded()
            try migratePodcastPlannerStateV4IfNeeded()
            try migrateAdWindowsPhase6PrepV5IfNeeded()
            try migrateCorrectionEventsV6IfNeeded()
            // Cycle 8 reconciliation: both C4 (Rev3-M5 shadow/targeted) and
            // B6 (Rev3-M6 BackfillJobPhase.rawValue) added a `phase` column
            // for different semantic dimensions. Keep C4's `phase` column
            // (will be renamed to `runMode` in a follow-up reconciliation
            // commit) and introduce a distinct `jobPhase` column for B6.
            //
            // V2 and V3 rebuild `evidence_events` from scratch (CREATE _vN,
            // copy, DROP, RENAME), and those rebuilds intentionally don't
            // carry either column. Re-apply both here once the table has
            // reached its final v3 shape.
            try addColumnIfNeeded(
                table: "evidence_events",
                column: "runMode",
                definition: "TEXT NOT NULL DEFAULT 'shadow'"
            )
            try addColumnIfNeeded(
                table: "evidence_events",
                column: "jobPhase",
                definition: "TEXT NOT NULL DEFAULT 'shadow'"
            )
            try addColumnIfNeeded(
                table: "semantic_scan_results",
                column: "jobPhase",
                definition: "TEXT NOT NULL DEFAULT 'shadow'"
            )
            try migrateSponsorKnowledgeV7IfNeeded()
            try migrateFingerprintStoreV8IfNeeded()
            try addColumnIfNeeded(
                table: "feature_windows",
                column: "speakerChangeProxyScore",
                definition: "REAL NOT NULL DEFAULT 0"
            )
            try addColumnIfNeeded(
                table: "feature_windows",
                column: "musicBedChangeScore",
                definition: "REAL NOT NULL DEFAULT 0"
            )
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }

        // Only mark as migrated after a successful COMMIT.
        Self.migratedPaths.insert(path)
    }

    #if DEBUG
    /// H-3: test-only helper that clears the process-global `migratedPaths`
    /// cache. Invoke from test setup when constructing temp-dir stores to
    /// prevent long test runs from accumulating stale entries. Not for
    /// production use — H3-2 gates this behind `#if DEBUG` so release
    /// builds cannot accidentally invalidate the migration cache.
    static func resetMigratedPathsForTesting() {
        migratedLock.withLock {
            migratedPaths.removeAll()
        }
    }

    /// Cycle 4 H1: runs ONLY the V*IfNeeded migration ladder against an
    /// already-opened store, bypassing `createTables()`. The cycle-2
    /// `MigrationLadderTests` seeded `_meta.schema_version` but still
    /// went through full `migrate()`, which calls `createTables()` first
    /// and builds every table in its current shape via
    /// `CREATE TABLE IF NOT EXISTS`. Tables-already-present short-circuits
    /// most of the ladder body, so the tests passed even against pre-C6
    /// code (the C6 bug could not actually be reached).
    ///
    /// This seam lets a test seed a v1-shape DB manually (via raw SQL)
    /// and then run the ladder without painting over. Failing under
    /// pre-C6 code proves the rail bites.
    ///
    /// Not transaction-wrapped — tests are expected to begin/commit
    /// themselves when they want to assert rollback semantics. The
    /// default behavior here mirrors what `migrate()` would do minus
    /// `createTables()`.
    func migrateOnlyForTesting() throws {
        try writeInitialSchemaVersionIfNeeded()
        if try tableExists("transcript_chunks") {
            try migrateTranscriptChunksPhase1()
        }
        try migrateEvidenceEventsNaturalKeyV2IfNeeded()
        try migrateEvidenceEventsTranscriptVersionV3IfNeeded()
        try migrateAnalysisSessionsShadowRetryV4IfNeeded()
        try migratePodcastPlannerStateV4IfNeeded()
        try migrateAdWindowsPhase6PrepV5IfNeeded()
        try migrateCorrectionEventsV6IfNeeded()
        try migrateSponsorKnowledgeV7IfNeeded()
        try migrateFingerprintStoreV8IfNeeded()
        try exec("""
            CREATE TABLE IF NOT EXISTS feature_windows (
                analysisAssetId   TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                startTime         REAL NOT NULL,
                endTime           REAL NOT NULL,
                rms               REAL NOT NULL,
                spectralFlux      REAL NOT NULL,
                musicProbability  REAL NOT NULL,
                speakerChangeProxyScore REAL NOT NULL DEFAULT 0,
                musicBedChangeScore REAL NOT NULL DEFAULT 0,
                musicBedOnsetScore REAL NOT NULL DEFAULT 0,
                musicBedOffsetScore REAL NOT NULL DEFAULT 0,
                musicBedLevelRaw  TEXT NOT NULL DEFAULT 'none',
                pauseProbability  REAL NOT NULL,
                speakerClusterId  INTEGER,
                jingleHash        TEXT,
                featureVersion    INTEGER NOT NULL,
                PRIMARY KEY (analysisAssetId, startTime)
            )
            """)
        try exec("""
            CREATE TABLE IF NOT EXISTS feature_extraction_state (
                analysisAssetId TEXT PRIMARY KEY REFERENCES analysis_assets(id) ON DELETE CASCADE,
                lastWindowStartTime REAL NOT NULL,
                lastWindowEndTime REAL NOT NULL,
                lastRms REAL NOT NULL,
                lastMusicProbability REAL NOT NULL,
                lastRawSpeakerChangeProxyScore REAL NOT NULL,
                penultimateRawSpeakerChangeProxyScore REAL,
                lastMagnitudesJSON TEXT NOT NULL,
                featureVersion INTEGER NOT NULL
            )
            """)
        try addColumnIfNeeded(
            table: "feature_windows",
            column: "speakerChangeProxyScore",
            definition: "REAL NOT NULL DEFAULT 0"
        )
        try addColumnIfNeeded(
            table: "feature_windows",
            column: "musicBedChangeScore",
            definition: "REAL NOT NULL DEFAULT 0"
        )
        try addColumnIfNeeded(
            table: "feature_windows",
            column: "musicBedOnsetScore",
            definition: "REAL NOT NULL DEFAULT 0"
        )
        try addColumnIfNeeded(
            table: "feature_windows",
            column: "musicBedOffsetScore",
            definition: "REAL NOT NULL DEFAULT 0"
        )
        try addColumnIfNeeded(
            table: "feature_windows",
            column: "musicBedLevelRaw",
            definition: "TEXT NOT NULL DEFAULT 'none'"
        )
        // Mirror the belt-and-suspenders phase/jobPhase column re-adds
        // that `migrate()` performs after the v2/v3 evidence_events
        // rebuild (cycle-8 reconciliation: both columns coexist).
        try addColumnIfNeeded(
            table: "evidence_events",
            column: "runMode",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
        try addColumnIfNeeded(
            table: "evidence_events",
            column: "jobPhase",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
    }
    #endif

    /// Probes `sqlite_master` for a table by name. Used by `migrate()` to
    /// detect a stale `migratedPaths` cache entry pointing at a file that
    /// has since been deleted and recreated empty.
    private func tableExists(_ table: String) throws -> Bool {
        let stmt = try prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '\(table)'")
        defer { sqlite3_finalize(stmt) }
        return sqlite3_step(stmt) == SQLITE_ROW
    }

    private func migrateTranscriptChunksPhase1() throws {
        // Add columns for transcript identity. The old implementation parsed
        // the SQLite error string for "duplicate column name"; we now check
        // PRAGMA table_info first and only ALTER when the column is missing.
        try addColumnIfNeeded(table: "transcript_chunks", column: "transcriptVersion", definition: "TEXT")
        try addColumnIfNeeded(table: "transcript_chunks", column: "atomOrdinal", definition: "INTEGER")
        try addColumnIfNeeded(table: "transcript_chunks", column: "weakAnchorMetadataJSON", definition: "TEXT")
        try backfillLegacyTranscriptChunksPhase1IfNeeded()
    }

    private func backfillLegacyTranscriptChunksPhase1IfNeeded() throws {
        // invariant: no nested transaction/savepoint is required here.
        // The production caller (`migrate()`) runs this inside an outer
        // BEGIN IMMEDIATE … COMMIT, and the test-only caller
        // (`migrateOnlyForTesting()`) is intentionally unwrapped. Even
        // without the outer transaction, re-running after a partial crash
        // is correct because (a) the SELECT predicate
        // `WHERE pass != 'fast' AND (transcriptVersion IS NULL OR
        // atomOrdinal IS NULL)` self-skips rows already backfilled, and
        // (b) `legacyTranscriptVersion` is a SHA256 over the chunks'
        // normalizedText — content-addressed, so the hash is stable
        // across partial states and a resumed run writes the same value
        // that a crashed run would have written.
        let assetStmt = try prepare("""
            SELECT DISTINCT analysisAssetId
            FROM transcript_chunks
            WHERE pass != 'fast'
              AND (transcriptVersion IS NULL OR atomOrdinal IS NULL)
            ORDER BY analysisAssetId
            """)
        defer { sqlite3_finalize(assetStmt) }

        let updateStmt = try prepare("""
            UPDATE transcript_chunks
            SET transcriptVersion = ?, atomOrdinal = ?
            WHERE id = ?
            """)
        defer { sqlite3_finalize(updateStmt) }

        var rebuiltFTS = false
        while sqlite3_step(assetStmt) == SQLITE_ROW {
            let assetId = text(assetStmt, 0)
            if !rebuiltFTS, try tableExists("transcript_chunks_fts") {
                // Old databases can contain transcript rows that predate the
                // external-content FTS table. Rebuild before mutating any of
                // those rows so the UPDATE trigger's delete/insert cycle sees
                // matching index entries instead of tripping SQLite corruption
                // checks on missing rowids.
                try exec("INSERT INTO transcript_chunks_fts(transcript_chunks_fts) VALUES('rebuild')")
                rebuiltFTS = true
            }
            let chunks = try fetchTranscriptChunks(assetId: assetId)
            let legacyChunks = chunks
                .filter { $0.pass != "fast" }
                .sorted(by: legacyTranscriptChunkSort)
            guard !legacyChunks.isEmpty else { continue }

            let version = legacyTranscriptVersion(for: legacyChunks)
            for (ordinal, chunk) in legacyChunks.enumerated() {
                sqlite3_reset(updateStmt)
                bind(updateStmt, 1, version)
                bind(updateStmt, 2, ordinal)
                bind(updateStmt, 3, chunk.id)
                try step(updateStmt, expecting: SQLITE_DONE)
            }
        }
    }

    private func legacyTranscriptChunkSort(_ lhs: TranscriptChunk, _ rhs: TranscriptChunk) -> Bool {
        if lhs.chunkIndex != rhs.chunkIndex {
            return lhs.chunkIndex < rhs.chunkIndex
        }
        return lhs.id < rhs.id
    }

    private func legacyTranscriptVersion(for chunks: [TranscriptChunk]) -> String {
        var hasher = SHA256()
        for chunk in chunks.sorted(by: legacyTranscriptChunkSort) {
            let textData = Data(chunk.normalizedText.utf8)
            withUnsafeBytes(of: UInt32(textData.count).bigEndian) { hasher.update(bufferPointer: $0) }
            hasher.update(data: textData)
        }
        return hasher.finalize().prefix(16).map { String(format: "%02x", $0) }.joined()
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

    /// Seeds `_meta.schema_version = '1'` on a brand-new database so the
    /// subsequent migration ladder (`migrateEvidenceEventsNaturalKeyV2IfNeeded`,
    /// `…V3IfNeeded`, `…V4IfNeeded`, `migratePodcastPlannerStateV4IfNeeded`,
    /// `migrateAdWindowsPhase6PrepV5IfNeeded`)
    /// climbs correctly to `currentSchemaVersion`.
    ///
    /// C6 fix (scope): this used to bind `String(currentSchemaVersion)`
    /// which left brand-new DBs at the latest version immediately,
    /// causing every V*IfNeeded migration's `guard schemaVersion < N` to
    /// short-circuit.
    ///
    /// Important caveat (cycle-4 L4): **in the production `migrate()`
    /// path, `createTables()` runs BEFORE this function** and already
    /// builds every table in its final v4 shape via
    /// `CREATE TABLE IF NOT EXISTS`, so the V*IfNeeded blocks are
    /// effectively prophylactic for production callers — the ladder they
    /// fix cannot be reached from `migrate()` alone, because the tables
    /// they would recreate already exist. The C6 fix matters for any
    /// future migration that does work `createTables()` cannot (e.g. a
    /// data backfill across existing rows, or a DDL change that requires
    /// inspecting `_meta.schema_version`). It also matters for
    /// `migrateOnlyForTesting()`, which bypasses `createTables()` so the
    /// ladder can be exercised against a hand-seeded v1/v2/v3 DB in
    /// isolation — that is the only path where the pre-C6 bug was
    /// actually reachable.
    ///
    /// `INSERT OR IGNORE` keeps this idempotent on re-migration: if a row
    /// already exists (any version) we leave it alone and the V*IfNeeded
    /// blocks read it via `schemaVersion()` and decide what to do.
    private func writeInitialSchemaVersionIfNeeded() throws {
        let stmt = try prepare("INSERT OR IGNORE INTO _meta (key, value) VALUES ('schema_version', '1')")
        defer { sqlite3_finalize(stmt) }
        try step(stmt, expecting: SQLITE_DONE)
    }

    private func setSchemaVersion(_ version: Int) throws {
        let stmt = try prepare("""
            INSERT INTO _meta (key, value) VALUES ('schema_version', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, String(version))
        try step(stmt, expecting: SQLITE_DONE)
    }

    private func migrateEvidenceEventsNaturalKeyV2IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 2 else { return }

        try exec("""
            CREATE TABLE evidence_events_v2 (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                eventType TEXT NOT NULL,
                sourceType TEXT NOT NULL,
                atomOrdinals TEXT NOT NULL,
                evidenceJSON TEXT NOT NULL,
                scanCohortJSON TEXT NOT NULL,
                createdAt REAL NOT NULL,
                UNIQUE(analysisAssetId, eventType, sourceType, atomOrdinals, evidenceJSON, scanCohortJSON)
            )
            """)
        try exec("""
            INSERT OR IGNORE INTO evidence_events_v2
            (id, analysisAssetId, eventType, sourceType, atomOrdinals, evidenceJSON, scanCohortJSON, createdAt)
            SELECT id, analysisAssetId, eventType, sourceType, atomOrdinals, evidenceJSON, scanCohortJSON, createdAt
            FROM evidence_events
            """)
        try exec("DROP TABLE evidence_events")
        try exec("ALTER TABLE evidence_events_v2 RENAME TO evidence_events")
        try exec("CREATE INDEX IF NOT EXISTS idx_evidence_events_asset_created ON evidence_events(analysisAssetId, createdAt ASC)")
        try setSchemaVersion(2)
    }

    private func migrateEvidenceEventsTranscriptVersionV3IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 3 else { return }

        try exec("""
            CREATE TABLE evidence_events_v3 (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                eventType TEXT NOT NULL,
                sourceType TEXT NOT NULL,
                atomOrdinals TEXT NOT NULL,
                evidenceJSON TEXT NOT NULL,
                scanCohortJSON TEXT NOT NULL,
                transcriptVersion TEXT NOT NULL DEFAULT '',
                createdAt REAL NOT NULL,
                UNIQUE(
                    analysisAssetId, eventType, sourceType, atomOrdinals,
                    evidenceJSON, scanCohortJSON, transcriptVersion
                )
            )
            """)
        try exec("""
            INSERT OR IGNORE INTO evidence_events_v3
            (id, analysisAssetId, eventType, sourceType, atomOrdinals,
             evidenceJSON, scanCohortJSON, transcriptVersion, createdAt)
            SELECT id, analysisAssetId, eventType, sourceType, atomOrdinals,
                   evidenceJSON, scanCohortJSON, '', createdAt
            FROM evidence_events
            """)
        try exec("DROP TABLE evidence_events")
        try exec("ALTER TABLE evidence_events_v3 RENAME TO evidence_events")
        try exec("CREATE INDEX IF NOT EXISTS idx_evidence_events_asset_created ON evidence_events(analysisAssetId, createdAt ASC)")
        try setSchemaVersion(3)
    }

    /// bd-3bz (Phase 4): add `needsShadowRetry` and `shadowRetryPodcastId`
    /// columns to `analysis_sessions`. The Foundation Models shadow phase
    /// stamps these when it bails on `canUseFoundationModels == false`; a
    /// capability observer in `PlayheadRuntime` drains the queue after FM
    /// recovers (see `runShadowFMPhase` and `retryShadowFMPhaseForSession`).
    ///
    /// Idempotent via `columnExists` checks so re-running the migration (or
    /// opening a schema-v3 DB that was manually upgraded) does not fail.
    /// NOT retroactively marked — sessions already in `.complete` stay as-is;
    /// only sessions whose shadow phase bails AFTER the migration set the
    /// flag.
    ///
    /// H10: column was originally `needs_shadow_retry` (snake_case,
    /// inconsistent with the rest of `analysis_sessions`). Renamed in place
    /// in the v4 migration block — single-user app, full DB wipe is
    /// acceptable so no v5 bump. Pre-existing on-device DBs that already
    /// applied v4 with the snake_case column are repaired by the
    /// `renameColumnIfNeeded` call below.
    private func migrateAnalysisSessionsShadowRetryV4IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 4 else {
            // Even on v4+ DBs, repair the H10 rename if a pre-rename column
            // is still present. Idempotent: no-op when the column already
            // has the new name.
            try renameSnakeCaseShadowRetryIfNeeded()
            return
        }
        try addColumnIfNeeded(
            table: "analysis_sessions",
            column: "needsShadowRetry",
            definition: "INTEGER NOT NULL DEFAULT 0"
        )
        try addColumnIfNeeded(
            table: "analysis_sessions",
            column: "shadowRetryPodcastId",
            definition: "TEXT"
        )
        // Partial index: cheap lookups for the retry drain. SQLite supports
        // WHERE clauses on indexes since 3.8.0.
        try exec("""
            CREATE INDEX IF NOT EXISTS idx_sessions_shadow_retry
            ON analysis_sessions(id)
            WHERE needsShadowRetry = 1
            """)
        try setSchemaVersion(4)
    }

    /// H10 repair: an earlier v4 migration created the column as
    /// `needs_shadow_retry`. If a pre-rename column is still present and the
    /// new camelCase column is not, rename in place via SQLite's
    /// `ALTER TABLE ... RENAME COLUMN` (3.25+). Idempotent.
    private func renameSnakeCaseShadowRetryIfNeeded() throws {
        let hasNew = try columnExists(table: "analysis_sessions", column: "needsShadowRetry")
        let hasOld = try columnExists(table: "analysis_sessions", column: "needs_shadow_retry")
        if hasNew { return }
        if hasOld {
            // Drop the old partial index first — its WHERE predicate
            // references the old column name and the rename would invalidate
            // the predicate.
            try exec("DROP INDEX IF EXISTS idx_sessions_shadow_retry")
            try exec("ALTER TABLE analysis_sessions RENAME COLUMN needs_shadow_retry TO needsShadowRetry")
            try exec("""
                CREATE INDEX IF NOT EXISTS idx_sessions_shadow_retry
                ON analysis_sessions(id)
                WHERE needsShadowRetry = 1
                """)
        }
    }

    /// bd-m8k: v4 creates `podcast_planner_state` for per-podcast
    /// CoveragePlanner state (observed episode count, episodes since last
    /// full rescan, precision ring, cached stable-precision flag). The table
    /// is created empty — we do NOT backfill rows for existing podcasts.
    /// Rows are upserted lazily the first time a podcast is observed.
    ///
    /// Idempotent: `CREATE TABLE IF NOT EXISTS` is a no-op when the table
    /// already exists (e.g. on a fresh DB that picked up the baseline DDL in
    /// `createTables()` before this migration ran). Guarded by the schema
    /// version so an upgraded DB still executes the DDL once and then never
    /// again.
    ///
    /// Coexists with `migrateAnalysisSessionsShadowRetryV4IfNeeded` (bd-3bz):
    /// both run during the v3→v4 step, both touch independent tables, both
    /// call setSchemaVersion(4) at the end (idempotent).
    private func migratePodcastPlannerStateV4IfNeeded() throws {
        // Cycle 4 B4: two new columns were added in place to the v4 schema
        // (`episodesObservedWithoutSampleCount`,
        // `narrowingAllPhasesEmptyEpisodeCount`). The `addColumnIfNeeded`
        // calls MUST run AFTER the `CREATE TABLE IF NOT EXISTS` below —
        // otherwise `migrateOnlyForTesting` (which skips `createTables()`)
        // hits an `ALTER TABLE` on a non-existent table when climbing the
        // ladder from a v1-shape DB. On a fresh DB both blocks are no-ops.

        let needsV4Upgrade = (try schemaVersion() ?? 1) < 4

        try exec("""
            CREATE TABLE IF NOT EXISTS podcast_planner_state (
                podcastId                                 TEXT PRIMARY KEY,
                observedEpisodeCount                      INTEGER NOT NULL DEFAULT 0,
                episodesSinceLastFullRescan               INTEGER NOT NULL DEFAULT 0,
                stablePrecisionFlag                       INTEGER NOT NULL DEFAULT 0,
                lastFullRescanAt                          REAL,
                precisionSample1                          REAL,
                precisionSample2                          REAL,
                precisionSample3                          REAL,
                precisionSampleCount                      INTEGER NOT NULL DEFAULT 0,
                episodesObservedWithoutSampleCount        INTEGER NOT NULL DEFAULT 0,
                narrowingAllPhasesEmptyEpisodeCount       INTEGER NOT NULL DEFAULT 0
            )
            """)

        // Idempotent column adds for pre-Cycle-4 v4 DBs. On a fresh DB both
        // are no-ops because the CREATE TABLE above already defined them.
        try addColumnIfNeeded(
            table: "podcast_planner_state",
            column: "episodesObservedWithoutSampleCount",
            definition: "INTEGER NOT NULL DEFAULT 0"
        )
        try addColumnIfNeeded(
            table: "podcast_planner_state",
            column: "narrowingAllPhasesEmptyEpisodeCount",
            definition: "INTEGER NOT NULL DEFAULT 0"
        )

        if needsV4Upgrade {
            try setSchemaVersion(4)
        }
    }

    /// Called from BOTH `migrate()` and `migrateOnlyForTesting()`. The `schemaVersion() < 5`
    /// guard makes it idempotent: the second call is a no-op. All DDL statements inside
    /// use `IF NOT EXISTS` / `addColumnIfNeeded` which are also idempotent. Any future
    /// non-idempotent step added here MUST be guarded by its own existence check.
    private func migrateAdWindowsPhase6PrepV5IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 5 else { return }

        try addColumnIfNeeded(
            table: "ad_windows",
            column: "evidenceSources",
            definition: "TEXT"
        )
        try addColumnIfNeeded(
            table: "ad_windows",
            column: "eligibilityGate",
            definition: "TEXT"
        )

        // Phase 6 decision tables (playhead-4my.6.3) — same v5 batch
        // UNIQUE on analysisAssetId so INSERT OR REPLACE enforces one active decision per asset.
        // A new cohort recomputes decisions → replaces the old row, preserving the last-writer-wins
        // contract without accumulating stale rows.
        try exec("""
            CREATE TABLE IF NOT EXISTS ad_decision_results (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL UNIQUE,
                decisionCohortJSON  TEXT NOT NULL,
                inputArtifactRefs   TEXT NOT NULL,
                decisionJSON        TEXT NOT NULL,
                createdAt           REAL NOT NULL
            )
            """)

        try exec("""
            CREATE TABLE IF NOT EXISTS decision_events (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL,
                eventType           TEXT NOT NULL,
                windowId            TEXT NOT NULL,
                proposalConfidence  REAL NOT NULL,
                skipConfidence      REAL NOT NULL,
                eligibilityGate     TEXT NOT NULL,
                policyAction        TEXT NOT NULL,
                decisionCohortJSON  TEXT NOT NULL,
                createdAt           REAL NOT NULL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_de_asset ON decision_events(analysisAssetId)")

        try setSchemaVersion(5)
    }

    /// Phase 7 (playhead-4my.7.1 / 7.3-fix): Migrate `correction_events` to v6 schema.
    ///
    /// Handles three upgrade paths:
    ///   1. **No table exists** (fresh DB or v4 that never reached v5 correction_events):
    ///      CREATE TABLE with the v6 schema directly.
    ///   2. **Old-schema table exists** (0.6 shipped v5 with `correctionScope`,
    ///      `atomOrdinalRange`, `evidenceJSON` columns): Rebuild the table to gain
    ///      the new `scope` column, FK constraint, and drop dead columns. Old row
    ///      data is migrated: `correctionScope` → `scope`.
    ///   3. **v6-schema table already exists** (test DBs or re-run): No-op via
    ///      `addColumnIfNeeded` guards.
    ///
    /// The table rebuild in path (2) is necessary because SQLite cannot add FK
    /// constraints or NOT NULL columns (without DEFAULT) via ALTER TABLE.
    private func migrateCorrectionEventsV6IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 6 else { return }

        // Detect whether the old-schema table exists by checking for the
        // `correctionScope` column (present in 0.6, absent in v6 schema).
        let hasOldSchema = try columnExists(table: "correction_events", column: "correctionScope")

        if hasOldSchema {
            // Path 2: Rebuild the table from old schema to v6.
            // Copy existing rows, mapping correctionScope → scope.
            // source and podcastId did not exist in v5 — they default to NULL.
            try exec("""
                CREATE TABLE correction_events_v6 (
                    id               TEXT PRIMARY KEY,
                    analysisAssetId  TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                    scope            TEXT NOT NULL,
                    createdAt        REAL NOT NULL,
                    source           TEXT,
                    podcastId        TEXT
                )
                """)
            // Filter to rows with a valid parent (FK is enforced via PRAGMA
            // foreign_keys = ON). Orphaned rows whose analysisAssetId was already
            // deleted are silently discarded — they are unreachable anyway.
            try exec("""
                INSERT INTO correction_events_v6 (id, analysisAssetId, scope, createdAt)
                SELECT id, analysisAssetId, correctionScope, createdAt
                FROM correction_events
                WHERE analysisAssetId IN (SELECT id FROM analysis_assets)
                """)
            try exec("DROP TABLE correction_events")
            try exec("ALTER TABLE correction_events_v6 RENAME TO correction_events")
            // Drop the old index (now orphaned by the table rebuild).
            try exec("DROP INDEX IF EXISTS idx_ce_asset")
        } else {
            // Path 1 or 3: Create the table if it doesn't exist yet.
            try exec("""
                CREATE TABLE IF NOT EXISTS correction_events (
                    id               TEXT PRIMARY KEY,
                    analysisAssetId  TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                    scope            TEXT NOT NULL,
                    createdAt        REAL NOT NULL,
                    source           TEXT,
                    podcastId        TEXT
                )
                """)
        }

        try exec("CREATE INDEX IF NOT EXISTS idx_correction_events_asset ON correction_events(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_correction_events_scope ON correction_events(scope)")

        // Belt-and-suspenders: ensure source/podcastId exist even if a test DB
        // hand-built the table without them.
        try addColumnIfNeeded(table: "correction_events", column: "source", definition: "TEXT")
        try addColumnIfNeeded(table: "correction_events", column: "podcastId", definition: "TEXT")

        try setSchemaVersion(6)
    }

    // MARK: - V7: Sponsor Knowledge Tables (Phase 8, playhead-4my.8.1)

    private func migrateSponsorKnowledgeV7IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 7 else { return }

        // Table 1: sponsor_knowledge_entries — lifecycle-managed sponsor entities.
        try exec("""
            CREATE TABLE IF NOT EXISTS sponsor_knowledge_entries (
                id                TEXT PRIMARY KEY,
                podcastId         TEXT NOT NULL,
                entityType        TEXT NOT NULL,
                entityValue       TEXT NOT NULL,
                normalizedValue   TEXT NOT NULL,
                state             TEXT NOT NULL DEFAULT 'candidate',
                confirmationCount INTEGER NOT NULL DEFAULT 0,
                rollbackCount     INTEGER NOT NULL DEFAULT 0,
                firstSeenAt       REAL NOT NULL,
                lastConfirmedAt   REAL,
                lastRollbackAt    REAL,
                decayedAt         REAL,
                blockedAt         REAL,
                aliases           TEXT,
                metadata          TEXT,
                UNIQUE(podcastId, entityType, normalizedValue)
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_ske_podcast ON sponsor_knowledge_entries(podcastId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_ske_state ON sponsor_knowledge_entries(state)")
        try exec("CREATE INDEX IF NOT EXISTS idx_ske_podcast_state ON sponsor_knowledge_entries(podcastId, state)")

        // Table 2: knowledge_candidate_events — append-only provenance log.
        try exec("""
            CREATE TABLE IF NOT EXISTS knowledge_candidate_events (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL,
                entityType          TEXT NOT NULL,
                entityValue         TEXT NOT NULL,
                sourceAtomOrdinals  TEXT NOT NULL,
                transcriptVersion   TEXT NOT NULL,
                confidence          REAL NOT NULL,
                scanCohortJSON      TEXT,
                createdAt           REAL NOT NULL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_kce_asset ON knowledge_candidate_events(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_kce_created ON knowledge_candidate_events(createdAt)")

        try setSchemaVersion(7)
    }

    // MARK: - V8: Ad Copy Fingerprint Tables (Phase 9, playhead-4my.9.1)

    private func migrateFingerprintStoreV8IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 8 else { return }

        // Table 1: ad_copy_fingerprints — lifecycle-managed fingerprint entries.
        try exec("""
            CREATE TABLE IF NOT EXISTS ad_copy_fingerprints (
                id                TEXT PRIMARY KEY,
                podcastId         TEXT NOT NULL,
                fingerprintHash   TEXT NOT NULL,
                normalizedText    TEXT NOT NULL,
                state             TEXT NOT NULL DEFAULT 'candidate',
                confirmationCount INTEGER NOT NULL DEFAULT 0,
                rollbackCount     INTEGER NOT NULL DEFAULT 0,
                firstSeenAt       REAL NOT NULL,
                lastConfirmedAt   REAL,
                lastRollbackAt    REAL,
                decayedAt         REAL,
                blockedAt         REAL,
                metadata          TEXT,
                UNIQUE(podcastId, fingerprintHash)
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_acf_podcast ON ad_copy_fingerprints(podcastId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_acf_state ON ad_copy_fingerprints(state)")
        try exec("CREATE INDEX IF NOT EXISTS idx_acf_podcast_state ON ad_copy_fingerprints(podcastId, state)")

        // Table 2: fingerprint_source_events — append-only provenance log.
        try exec("""
            CREATE TABLE IF NOT EXISTS fingerprint_source_events (
                id                TEXT PRIMARY KEY,
                analysisAssetId   TEXT NOT NULL,
                fingerprintHash   TEXT NOT NULL,
                sourceAdWindowId  TEXT NOT NULL,
                confidence        REAL NOT NULL,
                createdAt         REAL NOT NULL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_fse_asset ON fingerprint_source_events(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_fse_created ON fingerprint_source_events(createdAt)")

        try setSchemaVersion(8)
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
        // bd-3bz (Phase 4): `needsShadowRetry` + `shadowRetryPodcastId` are
        // created here for fresh databases. Existing DBs pick them up via
        // `migrateAnalysisSessionsShadowRetryV4IfNeeded`.
        // H10: column is `needsShadowRetry` (camelCase) to match the rest of
        // analysis_sessions; pre-rename DBs are repaired by
        // `renameSnakeCaseShadowRetryIfNeeded`.
        try exec("""
            CREATE TABLE IF NOT EXISTS analysis_sessions (
                id                    TEXT PRIMARY KEY,
                analysisAssetId       TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                state                 TEXT NOT NULL DEFAULT 'queued',
                startedAt             REAL NOT NULL,
                updatedAt             REAL NOT NULL,
                failureReason         TEXT,
                needsShadowRetry      INTEGER NOT NULL DEFAULT 0,
                shadowRetryPodcastId  TEXT
            )
            """)
        // bd-3bz on-device hotfix: when a pre-bd-3bz database exists at the
        // store's path, the `CREATE TABLE IF NOT EXISTS` above is a silent
        // no-op against the older table shape (no `needsShadowRetry`
        // column), and the partial index below would fail with
        // "no such column: needsShadowRetry" before the v4 migration ever
        // runs. Patch the column in defensively here so both fresh and
        // upgraded databases reach the index creation with the column
        // present.
        // H10 repair: an even-older shape may carry the snake_case
        // `needs_shadow_retry` column. Rename it in place before adding the
        // camelCase column so we don't end up with both.
        try renameSnakeCaseShadowRetryIfNeeded()
        try addColumnIfNeeded(
            table: "analysis_sessions",
            column: "needsShadowRetry",
            definition: "INTEGER NOT NULL DEFAULT 0"
        )
        try addColumnIfNeeded(
            table: "analysis_sessions",
            column: "shadowRetryPodcastId",
            definition: "TEXT"
        )
        try exec("CREATE INDEX IF NOT EXISTS idx_sessions_asset ON analysis_sessions(analysisAssetId)")
        try exec("""
            CREATE INDEX IF NOT EXISTS idx_sessions_shadow_retry
            ON analysis_sessions(id)
            WHERE needsShadowRetry = 1
            """)

        // feature_windows
        try exec("""
            CREATE TABLE IF NOT EXISTS feature_windows (
                analysisAssetId   TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                startTime         REAL NOT NULL,
                endTime           REAL NOT NULL,
                rms               REAL NOT NULL,
                spectralFlux      REAL NOT NULL,
                musicProbability  REAL NOT NULL,
                speakerChangeProxyScore REAL NOT NULL DEFAULT 0,
                musicBedChangeScore REAL NOT NULL DEFAULT 0,
                musicBedOnsetScore REAL NOT NULL DEFAULT 0,
                musicBedOffsetScore REAL NOT NULL DEFAULT 0,
                musicBedLevelRaw  TEXT NOT NULL DEFAULT 'none',
                pauseProbability  REAL NOT NULL,
                speakerClusterId  INTEGER,
                jingleHash        TEXT,
                featureVersion    INTEGER NOT NULL,
                PRIMARY KEY (analysisAssetId, startTime)
            )
            """)
        try exec("""
            CREATE TABLE IF NOT EXISTS feature_extraction_state (
                analysisAssetId TEXT PRIMARY KEY REFERENCES analysis_assets(id) ON DELETE CASCADE,
                lastWindowStartTime REAL NOT NULL,
                lastWindowEndTime REAL NOT NULL,
                lastRms REAL NOT NULL,
                lastMusicProbability REAL NOT NULL,
                lastRawSpeakerChangeProxyScore REAL NOT NULL,
                penultimateRawSpeakerChangeProxyScore REAL,
                lastMagnitudesJSON TEXT NOT NULL,
                featureVersion INTEGER NOT NULL
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
                atomOrdinal         INTEGER,
                weakAnchorMetadataJSON TEXT
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
                userDismissedBanner INTEGER NOT NULL DEFAULT 0,
                evidenceSources     TEXT,
                eligibilityGate     TEXT
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
        // Rev3-M5: `phase` is the LAST column on purpose — the column list
        // ordering is referenced by SELECT statements that read by index,
        // and keeping the new column at the bottom keeps post-merge
        // ordering predictable when sibling agents add their own fields.
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
                runMode TEXT NOT NULL DEFAULT 'shadow',
                jobPhase TEXT NOT NULL DEFAULT 'shadow',
                UNIQUE(reuseKeyHash)
            )
            """)
        // Cycle 8 reconciliation:
        //   * `runMode` (C4 Rev3-M5) — shadow vs targeted run-mode discriminator.
        //     Renamed from `phase` → `runMode` to disambiguate from B6's jobPhase.
        //   * `jobPhase` (B6 Rev3-M6) — BackfillJobPhase.rawValue, the originating
        //     backfill job phase (harvester/lexical/audit/fullEpisodeScan).
        // Both columns are defensively added here via `addColumnIfNeeded` so
        // pre-existing DBs pick them up without a schema-version bump.
        try addColumnIfNeeded(
            table: "semantic_scan_results",
            column: "runMode",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
        try addColumnIfNeeded(
            table: "semantic_scan_results",
            column: "jobPhase",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
        try exec("CREATE INDEX IF NOT EXISTS idx_semantic_scan_results_asset_pass ON semantic_scan_results(analysisAssetId, scanPass)")
        try exec("CREATE INDEX IF NOT EXISTS idx_semantic_scan_results_asset_runMode ON semantic_scan_results(analysisAssetId, runMode)")
        try exec("CREATE INDEX IF NOT EXISTS idx_semantic_scan_results_asset_jobPhase ON semantic_scan_results(analysisAssetId, jobPhase)")
        // M1/L3: dropped `idx_semantic_scan_results_reuse` and
        // `idx_semantic_scan_results_reuse_cohort` — neither is used by the
        // primary reuse query (which now hits the UNIQUE(reuseKeyHash) index).
        // The asset_pass index above is sufficient for diagnostic listings.

        // evidence_events
        // playhead-fn0: UNIQUE on (asset, eventType, sourceType, atomOrdinals,
        // evidenceJSON, cohort, transcriptVersion). This preserves distinct FM
        // refinement spans that cover the same atom range but differ
        // materially in payload while also keeping append-only audit across
        // transcript revisions. Exact reruns of the same transcript version
        // remain idempotent.
        // Inserts use INSERT OR IGNORE for silent idempotent dedup.
        //
        // H-2: evidence events are intentionally NOT FK-linked to
        // `semantic_scan_results`. They reference the asset directly via
        // `analysisAssetId` so that when an older scan row is replaced via
        // `reuseKeyHash` collision (INSERT OR REPLACE), the historical
        // evidence rows remain for audit purposes. Idempotency of re-runs
        // is handled by the UNIQUE(asset, eventType, sourceType,
        // atomOrdinals, evidenceJSON, scanCohortJSON, transcriptVersion)
        // constraint plus INSERT OR IGNORE: an exact rerun silently dedups,
        // while a new transcriptVersion, cohort, or materially different FM
        // span naturally appends.
        // Rev3-M5: `phase` is the LAST column on purpose, mirroring
        // `semantic_scan_results`. NOT included in the UNIQUE constraint:
        // the same logical span (asset, eventType, sourceType, atoms,
        // body, cohort, transcriptVersion) is the natural identity, and
        // the phase tag is an attribute of the row, not part of its key.
        try exec("""
            CREATE TABLE IF NOT EXISTS evidence_events (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                eventType TEXT NOT NULL,
                sourceType TEXT NOT NULL,
                atomOrdinals TEXT NOT NULL,
                evidenceJSON TEXT NOT NULL,
                scanCohortJSON TEXT NOT NULL,
                transcriptVersion TEXT NOT NULL DEFAULT '',
                createdAt REAL NOT NULL,
                runMode TEXT NOT NULL DEFAULT 'shadow',
                jobPhase TEXT NOT NULL DEFAULT 'shadow',
                UNIQUE(
                    analysisAssetId, eventType, sourceType, atomOrdinals,
                    evidenceJSON, scanCohortJSON, transcriptVersion
                )
            )
            """)
        // Cycle 8 reconciliation: defensively add both `runMode` (C4) and
        // `jobPhase` (B6) after the V2/V3 rebuilds that would have stripped
        // them. See semantic_scan_results above for the naming rationale.
        try addColumnIfNeeded(
            table: "evidence_events",
            column: "runMode",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
        try addColumnIfNeeded(
            table: "evidence_events",
            column: "jobPhase",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
        try exec("CREATE INDEX IF NOT EXISTS idx_evidence_events_asset_created ON evidence_events(analysisAssetId, createdAt ASC)")

        // bd-m8k: podcast_planner_state — per-podcast CoveragePlanner state
        // (observed episode count, episodes since last full rescan, recall
        // ring, cached stable-recall flag). Sibling table to
        // `podcast_profiles`; NOT backfilled on migration. Rows are created
        // lazily on first access. The recall ring stores the most recent
        // `plannerRecallRingSize` (3) full-rescan recall samples; the
        // flag is recomputed on every state mutation. Cycle 4 B4: two new
        // columns — `episodesObservedWithoutSampleCount` and
        // `narrowingAllPhasesEmptyEpisodeCount` — persist per-podcast
        // signals that previously lived only on the runner actor and were
        // therefore reset per `runPendingBackfill` call.
        try exec("""
            CREATE TABLE IF NOT EXISTS podcast_planner_state (
                podcastId                                 TEXT PRIMARY KEY,
                observedEpisodeCount                      INTEGER NOT NULL DEFAULT 0,
                episodesSinceLastFullRescan               INTEGER NOT NULL DEFAULT 0,
                stablePrecisionFlag                       INTEGER NOT NULL DEFAULT 0,
                lastFullRescanAt                          REAL,
                precisionSample1                          REAL,
                precisionSample2                          REAL,
                precisionSample3                          REAL,
                precisionSampleCount                      INTEGER NOT NULL DEFAULT 0,
                episodesObservedWithoutSampleCount        INTEGER NOT NULL DEFAULT 0,
                narrowingAllPhasesEmptyEpisodeCount       INTEGER NOT NULL DEFAULT 0
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

        // decoded_spans (Phase 5, playhead-4my.5.2)
        // New table — additive-only migration. Never extends ad_windows.
        // `anchorProvenanceJSON` is a JSON-encoded [AnchorRef] array.
        // INSERT OR REPLACE makes re-runs idempotent (same id → same row).
        try exec("""
            CREATE TABLE IF NOT EXISTS decoded_spans (
                id                  TEXT PRIMARY KEY,
                assetId             TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                firstAtomOrdinal    INTEGER NOT NULL,
                lastAtomOrdinal     INTEGER NOT NULL,
                startTime           REAL NOT NULL,
                endTime             REAL NOT NULL,
                anchorProvenanceJSON TEXT NOT NULL DEFAULT '[]'
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_decoded_spans_asset ON decoded_spans(assetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_decoded_spans_asset_time ON decoded_spans(assetId, startTime)")
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

    private func readFeatureExtractionCheckpoint(_ stmt: OpaquePointer?) throws -> FeatureExtractionCheckpoint {
        FeatureExtractionCheckpoint(
            analysisAssetId: text(stmt, 0),
            lastWindowStartTime: sqlite3_column_double(stmt, 1),
            lastWindowEndTime: sqlite3_column_double(stmt, 2),
            lastRms: sqlite3_column_double(stmt, 3),
            lastMusicProbability: sqlite3_column_double(stmt, 4),
            lastRawSpeakerChangeProxyScore: sqlite3_column_double(stmt, 5),
            penultimateRawSpeakerChangeProxyScore: optionalDouble(stmt, 6),
            lastMagnitudes: try decodeMagnitudesJSON(text(stmt, 7)),
            featureVersion: Int(sqlite3_column_int(stmt, 8))
        )
    }

    // MARK: - CRUD: analysis_sessions

    func insertSession(_ session: AnalysisSession) throws {
        let sql = """
            INSERT INTO analysis_sessions
                (id, analysisAssetId, state, startedAt, updatedAt, failureReason,
                 needsShadowRetry, shadowRetryPodcastId)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, session.id)
        bind(stmt, 2, session.analysisAssetId)
        bind(stmt, 3, session.state)
        bind(stmt, 4, session.startedAt)
        bind(stmt, 5, session.updatedAt)
        bind(stmt, 6, session.failureReason)
        bind(stmt, 7, session.needsShadowRetry ? 1 : 0)
        bind(stmt, 8, session.shadowRetryPodcastId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchSession(id: String) throws -> AnalysisSession? {
        let sql = """
            SELECT id, analysisAssetId, state, startedAt, updatedAt, failureReason,
                   needsShadowRetry, shadowRetryPodcastId
            FROM analysis_sessions WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readSession(stmt)
    }

    func fetchLatestSessionForAsset(assetId: String) throws -> AnalysisSession? {
        let sql = """
            SELECT id, analysisAssetId, state, startedAt, updatedAt, failureReason,
                   needsShadowRetry, shadowRetryPodcastId
            FROM analysis_sessions WHERE analysisAssetId = ?
            ORDER BY updatedAt DESC LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readSession(stmt)
    }

    private func readSession(_ stmt: OpaquePointer?) -> AnalysisSession {
        AnalysisSession(
            id: text(stmt, 0),
            analysisAssetId: text(stmt, 1),
            state: text(stmt, 2),
            startedAt: sqlite3_column_double(stmt, 3),
            updatedAt: sqlite3_column_double(stmt, 4),
            failureReason: optionalText(stmt, 5),
            needsShadowRetry: sqlite3_column_int(stmt, 6) != 0,
            shadowRetryPodcastId: optionalText(stmt, 7)
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

    /// bd-3bz (Phase 4): mark a session as needing a Foundation Models shadow
    /// retry and record the podcastId captured at bail time. Called by
    /// `AdDetectionService.runShadowFMPhase` when the `canUseFoundationModels`
    /// guard short-circuits. The `PlayheadRuntime` capability observer drains
    /// flagged sessions after a 60s-stable-true debounce.
    func markSessionNeedsShadowRetry(id: String, podcastId: String) throws {
        let sql = """
            UPDATE analysis_sessions
            SET needsShadowRetry = 1,
                shadowRetryPodcastId = ?,
                updatedAt = ?
            WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, Date().timeIntervalSince1970)
        bind(stmt, 3, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// bd-3bz (Phase 4): clear the shadow-retry flag after a successful
    /// `retryShadowFMPhaseForSession` drain.
    func clearSessionShadowRetry(id: String) throws {
        let sql = """
            UPDATE analysis_sessions
            SET needsShadowRetry = 0,
                shadowRetryPodcastId = NULL,
                updatedAt = ?
            WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, Date().timeIntervalSince1970)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// bd-3bz (Phase 4): fetch all sessions currently flagged for a shadow
    /// retry. Order is stable (by updatedAt ASC) so drains are deterministic
    /// in tests.
    func fetchSessionsNeedingShadowRetry() throws -> [AnalysisSession] {
        let sql = """
            SELECT id, analysisAssetId, state, startedAt, updatedAt, failureReason,
                   needsShadowRetry, shadowRetryPodcastId
            FROM analysis_sessions
            WHERE needsShadowRetry = 1
            ORDER BY updatedAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        var results: [AnalysisSession] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readSession(stmt))
        }
        return results
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
            INSERT OR REPLACE INTO feature_windows
            (analysisAssetId, startTime, endTime, rms, spectralFlux,
             musicProbability, speakerChangeProxyScore, musicBedChangeScore,
             musicBedOnsetScore, musicBedOffsetScore, musicBedLevelRaw,
             pauseProbability, speakerClusterId, jingleHash, featureVersion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, fw.analysisAssetId)
        bind(stmt, 2, fw.startTime)
        bind(stmt, 3, fw.endTime)
        bind(stmt, 4, fw.rms)
        bind(stmt, 5, fw.spectralFlux)
        bind(stmt, 6, fw.musicProbability)
        bind(stmt, 7, fw.speakerChangeProxyScore)
        bind(stmt, 8, fw.musicBedChangeScore)
        bind(stmt, 9, fw.musicBedOnsetScore)
        bind(stmt, 10, fw.musicBedOffsetScore)
        bind(stmt, 11, fw.musicBedLevel.rawValue)
        bind(stmt, 12, fw.pauseProbability)
        bind(stmt, 13, fw.speakerClusterId)
        bind(stmt, 14, fw.jingleHash)
        bind(stmt, 15, fw.featureVersion)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func updateFeatureWindowSpeakerChangeProxyScore(
        assetId: String,
        startTime: Double,
        endTime: Double,
        featureVersion: Int,
        speakerChangeProxyScore: Double
    ) throws {
        let sql = """
            UPDATE feature_windows
            SET speakerChangeProxyScore = ?
            WHERE analysisAssetId = ? AND startTime = ? AND endTime = ? AND featureVersion = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, speakerChangeProxyScore)
        bind(stmt, 2, assetId)
        bind(stmt, 3, startTime)
        bind(stmt, 4, endTime)
        bind(stmt, 5, featureVersion)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func persistFeatureExtractionBatch(
        assetId: String,
        windows: [FeatureWindow],
        priorWindowUpdate: FeatureWindowSpeakerChangeProxyUpdate?,
        checkpoint: FeatureExtractionCheckpoint?,
        coverageEndTime: Double?
    ) throws {
        guard priorWindowUpdate != nil || !windows.isEmpty || checkpoint != nil || coverageEndTime != nil else {
            return
        }

        try exec("BEGIN TRANSACTION")
        do {
            if let priorWindowUpdate {
                try updateFeatureWindowSpeakerChangeProxyScore(
                    assetId: priorWindowUpdate.assetId,
                    startTime: priorWindowUpdate.startTime,
                    endTime: priorWindowUpdate.endTime,
                    featureVersion: priorWindowUpdate.featureVersion,
                    speakerChangeProxyScore: priorWindowUpdate.speakerChangeProxyScore
                )
            }

            for window in windows {
                try insertFeatureWindow(window)
            }

            if let checkpoint {
                try upsertFeatureExtractionCheckpoint(checkpoint)
            }

            if let coverageEndTime {
                try updateFeatureCoverage(id: assetId, endTime: coverageEndTime)
                #if DEBUG
                try triggerFeatureBatchPersistenceFaultIfNeeded(.afterCoverageUpdateBeforeCommit)
                #endif
            }

            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func upsertFeatureExtractionCheckpoint(_ checkpoint: FeatureExtractionCheckpoint) throws {
        let sql = """
            INSERT INTO feature_extraction_state
            (analysisAssetId, lastWindowStartTime, lastWindowEndTime, lastRms,
             lastMusicProbability, lastRawSpeakerChangeProxyScore,
             penultimateRawSpeakerChangeProxyScore, lastMagnitudesJSON, featureVersion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(analysisAssetId) DO UPDATE SET
                lastWindowStartTime = excluded.lastWindowStartTime,
                lastWindowEndTime = excluded.lastWindowEndTime,
                lastRms = excluded.lastRms,
                lastMusicProbability = excluded.lastMusicProbability,
                lastRawSpeakerChangeProxyScore = excluded.lastRawSpeakerChangeProxyScore,
                penultimateRawSpeakerChangeProxyScore = excluded.penultimateRawSpeakerChangeProxyScore,
                lastMagnitudesJSON = excluded.lastMagnitudesJSON,
                featureVersion = excluded.featureVersion
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, checkpoint.analysisAssetId)
        bind(stmt, 2, checkpoint.lastWindowStartTime)
        bind(stmt, 3, checkpoint.lastWindowEndTime)
        bind(stmt, 4, checkpoint.lastRms)
        bind(stmt, 5, checkpoint.lastMusicProbability)
        bind(stmt, 6, checkpoint.lastRawSpeakerChangeProxyScore)
        bind(stmt, 7, checkpoint.penultimateRawSpeakerChangeProxyScore)
        bind(stmt, 8, try encodeMagnitudesJSON(checkpoint.lastMagnitudes))
        bind(stmt, 9, checkpoint.featureVersion)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchFeatureExtractionCheckpoint(
        assetId: String,
        featureVersion: Int,
        endingAt endTime: Double
    ) throws -> FeatureExtractionCheckpoint? {
        let sql = """
            SELECT analysisAssetId, lastWindowStartTime, lastWindowEndTime, lastRms,
                   lastMusicProbability, lastRawSpeakerChangeProxyScore,
                   penultimateRawSpeakerChangeProxyScore, lastMagnitudesJSON, featureVersion
            FROM feature_extraction_state
            WHERE analysisAssetId = ? AND featureVersion = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        bind(stmt, 2, featureVersion)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        let checkpoint = try readFeatureExtractionCheckpoint(stmt)
        guard abs(checkpoint.lastWindowEndTime - endTime) <= 1e-6 else { return nil }
        return checkpoint
    }

    #if DEBUG
    private func triggerFeatureBatchPersistenceFaultIfNeeded(
        _ injection: FeatureBatchPersistenceFaultInjection
    ) throws {
        guard featureBatchPersistenceFaultInjection == injection else { return }
        featureBatchPersistenceFaultInjection = nil
        throw AnalysisStoreError.insertFailed(
            "Injected feature extraction batch persistence failure at \(injection)"
        )
    }
    #endif

    #if DEBUG
    /// Test-only call log of `fetchFeatureWindows` invocations, captured as
    /// `(assetId, from, to)` tuples in call order. Used by
    /// `RegionShadowPhaseIntegrationTests` to pin that the Phase 4 shadow
    /// phase's full-episode fetch does NOT occur when no observer is
    /// injected. Never read in production code.
    var fetchFeatureWindowsCallLog: [(assetId: String, from: Double, to: Double)] = []
    #endif

    func earliestFeatureWindowStart(
        assetId: String,
        before end: Double,
        earlierThanFeatureVersion version: Int
    ) throws -> Double? {
        let sql = """
            SELECT MIN(startTime)
            FROM feature_windows
            WHERE analysisAssetId = ? AND endTime <= ? AND featureVersion < ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        bind(stmt, 2, end)
        bind(stmt, 3, version)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        guard sqlite3_column_type(stmt, 0) != SQLITE_NULL else { return nil }
        return sqlite3_column_double(stmt, 0)
    }

    func fetchFeatureWindows(
        assetId: String,
        from start: Double,
        to end: Double,
        minimumFeatureVersion: Int? = FeatureExtractionConfig.default.featureVersion
    ) throws -> [FeatureWindow] {
        #if DEBUG
        fetchFeatureWindowsCallLog.append((assetId: assetId, from: start, to: end))
        #endif
        let versionClause = minimumFeatureVersion == nil ? "" : "AND featureVersion >= ?"
        let sql = """
            SELECT analysisAssetId, startTime, endTime, rms, spectralFlux,
                   musicProbability, speakerChangeProxyScore, musicBedChangeScore,
                   musicBedOnsetScore, musicBedOffsetScore, musicBedLevelRaw,
                   pauseProbability, speakerClusterId, jingleHash, featureVersion
            FROM feature_windows
            WHERE analysisAssetId = ? AND startTime >= ? AND endTime <= ? \(versionClause)
            ORDER BY startTime
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        bind(stmt, 2, start)
        bind(stmt, 3, end)
        if let minimumFeatureVersion {
            bind(stmt, 4, minimumFeatureVersion)
        }
        var results: [FeatureWindow] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readFeatureWindow(stmt))
        }
        return results
    }

    // MARK: - CRUD: transcript_chunks

    func insertTranscriptChunk(_ chunk: TranscriptChunk) throws {
        let sql = """
            INSERT INTO transcript_chunks
            (id, analysisAssetId, segmentFingerprint, chunkIndex, startTime, endTime,
             text, normalizedText, pass, modelVersion, transcriptVersion, atomOrdinal, weakAnchorMetadataJSON)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        bind(stmt, 13, try encodeJSONString(chunk.weakAnchorMetadata))
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

    func fetchTranscriptChunk(
        analysisAssetId: String,
        segmentFingerprint: String
    ) throws -> TranscriptChunk? {
        let sql = """
            SELECT * FROM transcript_chunks
            WHERE analysisAssetId = ? AND segmentFingerprint = ?
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        bind(stmt, 2, segmentFingerprint)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readTranscriptChunk(stmt)
    }

    @discardableResult
    func updateTranscriptChunkWeakAnchorMetadata(
        analysisAssetId: String,
        segmentFingerprint: String,
        weakAnchorMetadata: TranscriptWeakAnchorMetadata?
    ) throws -> Bool {
        let sql = """
            UPDATE transcript_chunks
            SET weakAnchorMetadataJSON = ?
            WHERE analysisAssetId = ? AND segmentFingerprint = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, try encodeJSONString(weakAnchorMetadata))
        bind(stmt, 2, analysisAssetId)
        bind(stmt, 3, segmentFingerprint)
        try step(stmt, expecting: SQLITE_DONE)
        return sqlite3_changes(db) > 0
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
            atomOrdinal: optionalInt(stmt, 11),
            weakAnchorMetadata: try? decodeJSON(
                TranscriptWeakAnchorMetadata.self,
                from: optionalText(stmt, 12)
            )
        )
    }

    // MARK: - CRUD: ad_windows

    func insertAdWindow(_ ad: AdWindow) throws {
        // Column positions (1-indexed): id=1 analysisAssetId=2 startTime=3 endTime=4
        // confidence=5 boundaryState=6 decisionState=7 detectorVersion=8 advertiser=9
        // product=10 adDescription=11 evidenceText=12 evidenceStartTime=13
        // metadataSource=14 metadataConfidence=15 metadataPromptVersion=16 wasSkipped=17
        // userDismissedBanner=18 evidenceSources=19 eligibilityGate=20
        // Keep bind() call indices and this comment in sync when adding columns.
        let sql = """
            INSERT INTO ad_windows
            (id, analysisAssetId, startTime, endTime, confidence, boundaryState,
             decisionState, detectorVersion, advertiser, product, adDescription,
             evidenceText, evidenceStartTime, metadataSource, metadataConfidence,
             metadataPromptVersion, wasSkipped, userDismissedBanner,
             evidenceSources, eligibilityGate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        bind(stmt, 19, ad.evidenceSources)
        bind(stmt, 20, ad.eligibilityGate)
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
                userDismissedBanner: sqlite3_column_int(stmt, 17) != 0,
                evidenceSources: optionalText(stmt, 18),
                eligibilityGate: optionalText(stmt, 19)
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

    func updateAdWindowHotPathCandidate(_ ad: AdWindow) throws {
        let sql = """
            UPDATE ad_windows SET
                startTime = ?, endTime = ?, confidence = ?, boundaryState = ?,
                evidenceText = ?, evidenceStartTime = ?, evidenceSources = ?
            WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, ad.startTime)
        bind(stmt, 2, ad.endTime)
        bind(stmt, 3, ad.confidence)
        bind(stmt, 4, ad.boundaryState)
        bind(stmt, 5, ad.evidenceText)
        bind(stmt, 6, ad.evidenceStartTime)
        bind(stmt, 7, ad.evidenceSources)
        bind(stmt, 8, ad.id)
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

    func upsertHotPathAdWindows(
        _ windows: [AdWindow],
        existingIDs: Set<String>,
        retiredIDs: Set<String> = []
    ) throws {
        guard !windows.isEmpty || !retiredIDs.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            for ad in windows {
                if existingIDs.contains(ad.id) {
                    try updateAdWindowHotPathCandidate(ad)
                } else {
                    try insertAdWindow(ad)
                }
            }
            if !retiredIDs.isEmpty {
                try deleteAdWindows(ids: retiredIDs)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    private func deleteAdWindows(ids: Set<String>) throws {
        guard !ids.isEmpty else { return }
        let sql = "DELETE FROM ad_windows WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        for id in ids {
            sqlite3_reset(stmt)
            sqlite3_clear_bindings(stmt)
            bind(stmt, 1, id)
            try step(stmt, expecting: SQLITE_DONE)
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

    func fetchAllFeatureWindows(
        assetId: String,
        minimumFeatureVersion: Int? = FeatureExtractionConfig.default.featureVersion
    ) throws -> [FeatureWindow] {
        let versionClause = minimumFeatureVersion == nil ? "" : "AND featureVersion >= ?"
        let sql = """
            SELECT analysisAssetId, startTime, endTime, rms, spectralFlux,
                   musicProbability, speakerChangeProxyScore, musicBedChangeScore,
                   musicBedOnsetScore, musicBedOffsetScore, musicBedLevelRaw,
                   pauseProbability, speakerClusterId, jingleHash, featureVersion
            FROM feature_windows
            WHERE analysisAssetId = ? \(versionClause)
            ORDER BY startTime
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        if let minimumFeatureVersion {
            bind(stmt, 2, minimumFeatureVersion)
        }
        var results: [FeatureWindow] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readFeatureWindow(stmt))
        }
        return results
    }

    private func readFeatureWindow(_ stmt: OpaquePointer?) -> FeatureWindow {
        let levelRaw = optionalText(stmt, 10) ?? "none"
        let level = MusicBedLevel(rawValue: levelRaw) ?? .none
        return FeatureWindow(
            analysisAssetId: text(stmt, 0),
            startTime: sqlite3_column_double(stmt, 1),
            endTime: sqlite3_column_double(stmt, 2),
            rms: sqlite3_column_double(stmt, 3),
            spectralFlux: sqlite3_column_double(stmt, 4),
            musicProbability: sqlite3_column_double(stmt, 5),
            speakerChangeProxyScore: sqlite3_column_double(stmt, 6),
            musicBedChangeScore: sqlite3_column_double(stmt, 7),
            musicBedOnsetScore: sqlite3_column_double(stmt, 8),
            musicBedOffsetScore: sqlite3_column_double(stmt, 9),
            musicBedLevel: level,
            pauseProbability: sqlite3_column_double(stmt, 11),
            speakerClusterId: optionalInt(stmt, 12),
            jingleHash: optionalText(stmt, 13),
            featureVersion: Int(sqlite3_column_int(stmt, 14))
        )
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

    // MARK: - CRUD: podcast_planner_state (bd-m8k)

    /// bd-m8k: Returns the persisted `PodcastPlannerState` for `podcastId`, or
    /// `nil` if no row has been created for this podcast yet. Callers should
    /// treat `nil` as the conservative cold-start default
    /// (`observedEpisodeCount = 0`, `stablePrecisionFlag = false`,
    /// `episodesSinceLastFullRescan = 0`) — the migration deliberately leaves
    /// the table empty and rows are created lazily on first observation.
    func fetchPodcastPlannerState(podcastId: String) throws -> PodcastPlannerState? {
        // historical: stored as "precision*"; semantically recall
        // Cycle 4 B4: two new persisted counters appended at the end.
        let sql = """
            SELECT podcastId,
                   observedEpisodeCount,
                   episodesSinceLastFullRescan,
                   stablePrecisionFlag,
                   lastFullRescanAt,
                   precisionSample1,
                   precisionSample2,
                   precisionSample3,
                   precisionSampleCount,
                   episodesObservedWithoutSampleCount,
                   narrowingAllPhasesEmptyEpisodeCount
            FROM podcast_planner_state
            WHERE podcastId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }

        // Cycle 2 Rev4-M3: clamp the persisted sample count into the valid
        // range and log loudly when the clamp fires. A row that has
        // `precisionSampleCount` outside `[0, plannerRecallRingSize]` is
        // either a bug, a manual SQL edit, or a corrupted file — either way
        // operators should see it in Console.app instead of the store
        // silently rounding past it.
        let rawSampleCount = Int(sqlite3_column_int(stmt, 8))
        let sampleCount = max(0, min(Self.plannerRecallRingSize, rawSampleCount))
        if rawSampleCount != sampleCount {
            logger.error(
                "podcast_planner_state.precisionSampleCount=\(rawSampleCount, privacy: .public) out of range [0, \(Self.plannerRecallRingSize, privacy: .public)] for podcast=\(podcastId, privacy: .public); clamped to \(sampleCount, privacy: .public)"
            )
        }
        // Samples are stored oldest → newest in columns 5/6/7. We hand back
        // exactly `sampleCount` doubles so callers cannot accidentally treat
        // a NULL slot as a real measurement.
        var samples: [Double] = []
        samples.reserveCapacity(sampleCount)
        for offset in 0..<sampleCount {
            if let value = optionalDouble(stmt, Int32(5 + offset)) {
                samples.append(value)
            }
        }

        // Cycle 4 B4: columns 9/10 are the Cycle-4 additions. Legacy rows
        // default-decode to 0 thanks to `DEFAULT 0` on both columns —
        // SQLite hands back the column default for NULL-absent reads.
        let episodesObservedWithoutSampleCount = Int(sqlite3_column_int(stmt, 9))
        let narrowingAllPhasesEmptyEpisodeCount = Int(sqlite3_column_int(stmt, 10))

        return PodcastPlannerState(
            podcastId: text(stmt, 0),
            observedEpisodeCount: Int(sqlite3_column_int(stmt, 1)),
            episodesSinceLastFullRescan: Int(sqlite3_column_int(stmt, 2)),
            // historical: stored as "stablePrecisionFlag"; semantically recall
            stableRecallFlag: sqlite3_column_int(stmt, 3) != 0,
            lastFullRescanAt: optionalDouble(stmt, 4),
            // historical: stored as "precisionSamples"; semantically recall
            recallSamples: samples,
            episodesObservedWithoutSampleCount: episodesObservedWithoutSampleCount,
            narrowingAllPhasesEmptyEpisodeCount: narrowingAllPhasesEmptyEpisodeCount
        )
    }

    /// bd-m8k: Records that a backfill pass for `podcastId` has just completed
    /// and returns the updated state.
    ///
    /// **Lazy creation:** if no row exists for `podcastId`, one is inserted at
    /// cold-start defaults before the bookkeeping below is applied. This is
    /// the only path that materializes a row — there is no migration backfill
    /// and no separate `upsert` API.
    ///
    /// **Bookkeeping rules** (per the bd-m8k design field):
    /// - `observedEpisodeCount` is incremented by 1 on every call.
    /// - `wasFullRescan == true`: `episodesSinceLastFullRescan` resets to 0,
    ///   `lastFullRescanAt` is updated, and (when `fullRescanPrecisionSample`
    ///   is non-nil) the sample is appended to the recall ring with the
    ///   oldest entry dropped if the ring is already full.
    /// - `wasFullRescan == false`: `episodesSinceLastFullRescan` is
    ///   incremented; the recall ring is left untouched. A recall
    ///   sample passed alongside a non-full-rescan call is ignored (the
    ///   targeted-with-audit pass cannot measure recall against itself).
    /// - `stableRecallFlag` is recomputed from the post-update state on
    ///   every call: it is true iff
    ///   `observedEpisodeCount >= plannerStableObservedEpisodeFloor` AND the
    ///   ring is full (`plannerRecallRingSize` samples) AND every sample
    ///   in the ring is `>= plannerRecallThreshold`. If any condition
    ///   fails the flag is forced false, even if a previous write set it to
    ///   true (the ring shrinks back to false on regression).
    /// - Cycle 4 B4: `incrementEpisodesObservedWithoutSample` and
    ///   `incrementNarrowingAllPhasesEmpty` are independent per-podcast
    ///   counters. When true, the persisted counters are read-modify-written
    ///   under the same transaction as the rest of the bookkeeping. Both
    ///   flags are orthogonal — an ad-free full rescan passes
    ///   `incrementEpisodesObservedWithoutSample = true` and an all-phases-
    ///   empty targeted run passes `incrementNarrowingAllPhasesEmpty = true`.
    ///   A full rescan can pass both (ad-free episode where narrowing was
    ///   also empty).
    @discardableResult
    func recordPodcastEpisodeObservation(
        podcastId: String,
        wasFullRescan: Bool,
        fullRescanPrecisionSample: Double? = nil,
        incrementEpisodesObservedWithoutSample: Bool = false,
        incrementNarrowingAllPhasesEmpty: Bool = false,
        now: Double
    ) throws -> PodcastPlannerState {
        // Wrap the read-modify-write in a transaction so a concurrent
        // observation for the same podcast cannot interleave a stale read
        // with our write. SQLite's busy_timeout already serializes writers,
        // but BEGIN IMMEDIATE upgrades the lock immediately so two callers
        // hitting the same row see SQLITE_BUSY rather than racing on the
        // counter.
        try exec("BEGIN IMMEDIATE")
        do {
            let prior = try fetchPodcastPlannerState(podcastId: podcastId)
            // historical: stored as "precision*"; semantically recall
            let priorSamples = prior?.recallSamples ?? []

            let newObservedCount = (prior?.observedEpisodeCount ?? 0) + 1
            let newEpisodesSince: Int
            let newLastFullRescanAt: Double?
            var newSamples = priorSamples

            if wasFullRescan {
                newEpisodesSince = 0
                newLastFullRescanAt = now
                // Cycle 2 C4: parameter is named `fullRescanPrecisionSample`
                // for legacy compatibility but the value semantically is a
                // recall sample. Ad-free episodes pass nil and the ring is
                // intentionally NOT advanced (no fake 1.0).
                if let sample = fullRescanPrecisionSample {
                    newSamples.append(sample)
                    while newSamples.count > Self.plannerRecallRingSize {
                        newSamples.removeFirst()
                    }
                }
            } else {
                newEpisodesSince = (prior?.episodesSinceLastFullRescan ?? 0) + 1
                newLastFullRescanAt = prior?.lastFullRescanAt
                // Intentionally do NOT touch the recall ring on
                // non-full-rescan observations — see doc comment above.
            }

            let stableFlag = Self.computePlannerStableFlag(
                observedEpisodeCount: newObservedCount,
                samples: newSamples
            )

            // Cycle 4 B4: per-podcast counters. Read prior value (0 for
            // missing rows via the struct default above) and bump under
            // the same BEGIN IMMEDIATE that guards the rest of the
            // bookkeeping.
            let newEpisodesObservedWithoutSample =
                (prior?.episodesObservedWithoutSampleCount ?? 0)
                + (incrementEpisodesObservedWithoutSample ? 1 : 0)
            let newNarrowingAllPhasesEmptyEpisodes =
                (prior?.narrowingAllPhasesEmptyEpisodeCount ?? 0)
                + (incrementNarrowingAllPhasesEmpty ? 1 : 0)

            try writePodcastPlannerStateRow(
                podcastId: podcastId,
                observedEpisodeCount: newObservedCount,
                episodesSinceLastFullRescan: newEpisodesSince,
                stableRecallFlag: stableFlag,
                lastFullRescanAt: newLastFullRescanAt,
                samples: newSamples,
                episodesObservedWithoutSampleCount: newEpisodesObservedWithoutSample,
                narrowingAllPhasesEmptyEpisodeCount: newNarrowingAllPhasesEmptyEpisodes
            )

            try exec("COMMIT")

            return PodcastPlannerState(
                podcastId: podcastId,
                observedEpisodeCount: newObservedCount,
                episodesSinceLastFullRescan: newEpisodesSince,
                // historical: stored as "stablePrecisionFlag"; semantically recall
                stableRecallFlag: stableFlag,
                lastFullRescanAt: newLastFullRescanAt,
                // historical: stored as "precisionSamples"; semantically recall
                recallSamples: newSamples,
                episodesObservedWithoutSampleCount: newEpisodesObservedWithoutSample,
                narrowingAllPhasesEmptyEpisodeCount: newNarrowingAllPhasesEmptyEpisodes
            )
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    /// bd-m8k: pure helper exposed for tests. Computes the stable-recall
    /// flag from a post-update `(observedEpisodeCount, samples)` tuple. The
    /// flag is true iff:
    /// 1. `observedEpisodeCount >= plannerStableObservedEpisodeFloor` (5), AND
    /// 2. The recall ring contains exactly `plannerRecallRingSize` (3)
    ///    samples, AND
    /// 3. Every sample is `>= plannerRecallThreshold` (0.85).
    ///
    /// The "exactly 3 samples" requirement is deliberate: a freshly
    /// observed podcast with one stellar recall sample must not flip the
    /// flag — we want at least three full-rescan recall measurements
    /// before trusting the targeted-with-audit branch.
    nonisolated static func computePlannerStableFlag(
        observedEpisodeCount: Int,
        samples: [Double]
    ) -> Bool {
        guard observedEpisodeCount >= plannerStableObservedEpisodeFloor else { return false }
        guard samples.count >= plannerRecallRingSize else { return false }
        return samples.allSatisfy { $0 >= plannerRecallThreshold }
    }

    private func writePodcastPlannerStateRow(
        podcastId: String,
        observedEpisodeCount: Int,
        episodesSinceLastFullRescan: Int,
        // Cycle 6 B6 L: parameter name follows the "recall" semantic the
        // cycle-4 rename pass established. The underlying SQLite column is
        // still `stablePrecisionFlag` for backwards compatibility.
        stableRecallFlag: Bool,
        lastFullRescanAt: Double?,
        samples: [Double],
        episodesObservedWithoutSampleCount: Int,
        narrowingAllPhasesEmptyEpisodeCount: Int
    ) throws {
        // Pad the samples array out to the fixed-width ring slots so we can
        // unconditionally bind 3 columns regardless of how many samples we
        // have in hand.
        var ring: [Double?] = Array(repeating: nil, count: Self.plannerRecallRingSize)
        for (idx, value) in samples.enumerated()
        where idx < Self.plannerRecallRingSize {
            ring[idx] = value
        }

        // Cycle 4 B4: two new persisted counters appended.
        let sql = """
            INSERT INTO podcast_planner_state
            (podcastId, observedEpisodeCount, episodesSinceLastFullRescan,
             stablePrecisionFlag, lastFullRescanAt,
             precisionSample1, precisionSample2, precisionSample3,
             precisionSampleCount,
             episodesObservedWithoutSampleCount,
             narrowingAllPhasesEmptyEpisodeCount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(podcastId) DO UPDATE SET
                observedEpisodeCount                = excluded.observedEpisodeCount,
                episodesSinceLastFullRescan         = excluded.episodesSinceLastFullRescan,
                stablePrecisionFlag                 = excluded.stablePrecisionFlag,
                lastFullRescanAt                    = excluded.lastFullRescanAt,
                precisionSample1                    = excluded.precisionSample1,
                precisionSample2                    = excluded.precisionSample2,
                precisionSample3                    = excluded.precisionSample3,
                precisionSampleCount                = excluded.precisionSampleCount,
                episodesObservedWithoutSampleCount  = excluded.episodesObservedWithoutSampleCount,
                narrowingAllPhasesEmptyEpisodeCount = excluded.narrowingAllPhasesEmptyEpisodeCount
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, observedEpisodeCount)
        bind(stmt, 3, episodesSinceLastFullRescan)
        bind(stmt, 4, stableRecallFlag ? 1 : 0)
        bind(stmt, 5, lastFullRescanAt)
        bind(stmt, 6, ring[0])
        bind(stmt, 7, ring[1])
        bind(stmt, 8, ring[2])
        bind(stmt, 9, samples.count)
        bind(stmt, 10, episodesObservedWithoutSampleCount)
        bind(stmt, 11, narrowingAllPhasesEmptyEpisodeCount)
        try step(stmt, expecting: SQLITE_DONE)
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
        deferredWorkAllowed: Bool,
        t0ThresholdSec: Double,
        now: TimeInterval
    ) throws -> AnalysisJob? {
        // T0 jobs: playback jobs that have zero coverage — always eligible.
        // Deferred jobs: backfill/preAnalysis require the caller's shared
        // admission-policy gate to allow deferred work and nextEligibleAt <=
        // now (or NULL).
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
                  ? = 1
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
        bind(stmt, 5, deferredWorkAllowed ? 1 : 0)
        bind(stmt, 6, now)
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

    func updateJobAnalysisAssetId(jobId: String, analysisAssetId: String) throws {
        let sql = """
            UPDATE analysis_jobs
            SET analysisAssetId = ?, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        bind(stmt, 2, Date().timeIntervalSince1970)
        bind(stmt, 3, jobId)
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
    ///
    /// C-R3-1: guarded against silent terminal resurrection — the same pattern
    /// as C3-2's guards on `markBackfillJobComplete` / `markBackfillJobFailed`.
    /// The update is restricted to rows in `queued` or `running`; on zero-row
    /// updates we probe the current state and:
    ///   - return silently after refreshing `deferReason` when the row is
    ///     already `deferred` (idempotent retry path — an operator issuing a
    ///     new defer reason expects the row to reflect the most recent cause),
    ///   - throw `invalidStateTransition` on any other state (including a
    ///     missing row or a terminal `.complete` / `.failed` row) so the H-1
    ///     drain loop cannot silently demote a `.failed` row to `.deferred`
    ///     and lose the original failure reason.
    func markBackfillJobDeferred(
        jobId: String,
        reason: String
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'deferred', deferReason = ?
            WHERE jobId = ? AND status IN ('queued', 'running')
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, reason)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
        if sqlite3_changes(db) == 0 {
            let current = try probeBackfillJobStatus(jobId: jobId)
            if current == "deferred" {
                // Already deferred: refresh deferReason so the most recent
                // cause is visible to operators, but leave status untouched.
                let refreshSQL = """
                    UPDATE backfill_jobs
                    SET deferReason = ?
                    WHERE jobId = ? AND status = 'deferred'
                    """
                let refreshStmt = try prepare(refreshSQL)
                defer { sqlite3_finalize(refreshStmt) }
                bind(refreshStmt, 1, reason)
                bind(refreshStmt, 2, jobId)
                try step(refreshStmt, expecting: SQLITE_DONE)
                return
            }
            throw AnalysisStoreError.invalidStateTransition(
                jobId: jobId,
                fromStatus: current,
                toStatus: "deferred"
            )
        }
    }

    /// C-2: transition a job row to `status='running'` without clobbering
    /// `deferReason`, `progressCursor`, or `retryCount`. Preserving the
    /// `deferReason` on a running-after-defer transition keeps the audit
    /// trail intact: the row reflects that an earlier defer happened even
    /// as the next runner attempt starts executing.
    ///
    /// Round-2 fix: the unconditional UPDATE silently resurrected terminal
    /// rows (`.complete` / `.failed`) into `.running`, defeating the whole
    /// point of the H5 split. The status guard limits the transition to
    /// rows in `.queued` or `.deferred`; any other state (including a
    /// missing row) throws `AnalysisStoreError.invalidStateTransition` so
    /// callers learn about the programmer/race error instead of silently
    /// re-running a job that already finished.
    /// HIGH-R6-1: idempotent on an existing `.running` row. The prior
    /// implementation only accepted `queued`/`deferred` and threw
    /// `invalidStateTransition` on an already-running row. That asymmetry
    /// (the Complete/Failed/Deferred guards are all idempotent on their
    /// own terminal state) meant a process crash between
    /// `markBackfillJobRunning` and the subsequent terminal transition
    /// left the row stuck in `.running`. On the next drain the runner
    /// would re-enqueue via M-5 idempotency, call this method, hit the
    /// throw, and the runner's "already terminal" catch arm would
    /// `continue` without bumping `retryCount` — a zombie that loops
    /// forever. The `IN (..., 'running')` clause restores symmetry; the
    /// row is left untouched (no field clobbering) because the UPDATE is
    /// a no-op when the row is already `.running`.
    func markBackfillJobRunning(jobId: String) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'running'
            WHERE jobId = ? AND status IN ('queued', 'deferred', 'running')
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, jobId)
        try step(stmt, expecting: SQLITE_DONE)
        if sqlite3_changes(db) == 0 {
            // With 'running' included in the IN clause above, a zero-change
            // result means the row is either missing or in a terminal state
            // (`complete`/`failed`). Probe for defensive disambiguation: if
            // the row somehow reports `.running` (e.g. a future schema
            // change widens the set) treat it as idempotent success;
            // anything else is a real invalid transition.
            let current = try probeBackfillJobStatus(jobId: jobId)
            if current == "running" {
                return
            }
            throw AnalysisStoreError.invalidStateTransition(
                jobId: jobId,
                fromStatus: current,
                toStatus: "running"
            )
        }
    }

    /// C-2: terminal success transition. Writes the final `progressCursor`
    /// and flips `status='complete'` while preserving `deferReason` (audit
    /// trail) and `retryCount`.
    ///
    /// C3-2: guarded against silent terminal resurrection. The update is
    /// restricted to rows in `queued`, `deferred`, or `running`; if zero
    /// rows are affected we probe the current state and:
    ///   - return silently when the row is already `complete` (idempotent
    ///     retry after an earlier successful call),
    ///   - throw `invalidStateTransition` on any other state (including a
    ///     missing row or a `failed` row) so callers can never silently
    ///     promote a failed job to complete.
    func markBackfillJobComplete(
        jobId: String,
        progressCursor: BackfillProgressCursor?
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'complete', progressCursor = ?
            WHERE jobId = ? AND status IN ('queued', 'deferred', 'running')
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, try encodeJSONString(progressCursor))
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
        if sqlite3_changes(db) == 0 {
            let current = try probeBackfillJobStatus(jobId: jobId)
            if current == "complete" {
                // Already complete: idempotent retry path, silent success.
                return
            }
            throw AnalysisStoreError.invalidStateTransition(
                jobId: jobId,
                fromStatus: current,
                toStatus: "complete"
            )
        }
    }

    /// C-2: terminal failure transition. The prior shim silently dropped
    /// `deferReason` on `.failed`; this method ensures the reason is
    /// written so operators can diagnose why a job failed without scraping
    /// logs.
    ///
    /// M-4: note that this intentionally overwrites any prior
    /// `deferReason`. A job that was previously `.deferred` for thermal
    /// throttling and then failed the next attempt must record the newer
    /// *failure* cause, not the older defer reason. Operators diagnosing a
    /// failed job care about why it failed, not the cooldown that preceded
    /// it; the defer history is still recoverable from structured logs.
    /// This behavior is pinned by
    /// `markBackfillJobFailed_overwritesDeferReason`.
    ///
    /// C3-2: guarded against silent terminal resurrection. The update is
    /// restricted to rows in `queued`, `deferred`, or `running`; if zero
    /// rows are affected we probe the current state and:
    ///   - return silently when the row is already `failed` (idempotent
    ///     retry after an earlier failure was recorded; `retryCount` and
    ///     the original failure `deferReason` are preserved),
    ///   - throw `invalidStateTransition` on any other state (including a
    ///     missing row or a `complete` row) so a late exception cannot
    ///     silently demote a completed job.
    func markBackfillJobFailed(
        jobId: String,
        reason: String,
        retryCount: Int
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'failed', deferReason = ?, retryCount = ?
            WHERE jobId = ? AND status IN ('queued', 'deferred', 'running')
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, reason)
        bind(stmt, 2, retryCount)
        bind(stmt, 3, jobId)
        try step(stmt, expecting: SQLITE_DONE)
        if sqlite3_changes(db) == 0 {
            let current = try probeBackfillJobStatus(jobId: jobId)
            if current == "failed" {
                // Already failed: idempotent retry path, silent success.
                // retryCount and the original deferReason are preserved —
                // the caller's newer values are intentionally discarded so
                // a double-catch at a higher layer cannot double-bump the
                // retry counter.
                return
            }
            throw AnalysisStoreError.invalidStateTransition(
                jobId: jobId,
                fromStatus: current,
                toStatus: "failed"
            )
        }
    }

    /// C3-2: small helper used by the guarded terminal transitions to
    /// distinguish "no row" from "row present in a disallowed state". Returns
    /// `nil` when no row exists for `jobId`.
    private func probeBackfillJobStatus(jobId: String) throws -> String? {
        let stmt = try prepare("SELECT status FROM backfill_jobs WHERE jobId = ? LIMIT 1")
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, jobId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return optionalText(stmt, 0)
    }

    #if DEBUG
    /// bd-m8k test-only helper: drop the `podcast_planner_state` table from
    /// under the live connection so the next `migrate()` against this path
    /// has to recreate it. Used by the "DROP TABLE / re-migrate cycle is
    /// clean" regression test. Production code must never call this — the
    /// table is the planner's source of truth, and dropping it would erase
    /// every show's observed-episode counter and precision ring.
    func dropPodcastPlannerStateForTesting() throws {
        try exec("DROP TABLE IF EXISTS podcast_planner_state")
    }

    /// Cycle 2 Rev4-M3 test-only helper: run an arbitrary DDL/DML
    /// statement so tests can corrupt rows on purpose to exercise the
    /// fetchPodcastPlannerState clamp warning. Production code MUST NOT
    /// call this; it bypasses every validator the store enforces.
    func execForTesting(_ sql: String) throws {
        try exec(sql)
    }

    func setFeatureBatchPersistenceFaultInjectionForTesting(
        _ injection: FeatureBatchPersistenceFaultInjection?
    ) {
        featureBatchPersistenceFaultInjection = injection
    }
    #endif

    #if DEBUG
    /// Test-only: force a backfill row to a specific state without running
    /// the lifecycle guards. Used by tests that need to set up a pre-existing
    /// row in a specific configuration before exercising the runner (e.g.
    /// demoting a terminal row back to `.queued` to simulate an orphan
    /// recovery scenario). Production code MUST NOT call this — use
    /// `markBackfillJobRunning/Complete/Failed/Deferred` or
    /// `checkpointBackfillJobProgress` instead.
    ///
    /// `progressCursor` is written unconditionally: passing `nil` clears the
    /// column to NULL. `retryCount` and `deferReason` use COALESCE so nil
    /// leaves the existing row values untouched.
    func forceBackfillJobStateForTesting(
        jobId: String,
        status: BackfillJobStatus,
        progressCursor: BackfillProgressCursor?,
        retryCount: Int? = nil,
        deferReason: String? = nil
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = ?,
                progressCursor = ?,
                retryCount = COALESCE(?, retryCount),
                deferReason = COALESCE(?, deferReason)
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, status.rawValue)
        bind(stmt, 2, try encodeJSONString(progressCursor))
        bind(stmt, 3, retryCount)
        bind(stmt, 4, deferReason)
        bind(stmt, 5, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }
    #endif

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

    // MARK: - Cohort GC

    /// Fix #4: deletes every `semantic_scan_results` and `evidence_events`
    /// row whose `scanCohortJSON` does not canonicalize to the supplied
    /// current cohort. Intended to be called once per app launch from
    /// `PlayheadRuntime.init` so old rows persisted under prior cohort
    /// hashes (e.g. after an app upgrade bumps the prompt hash, or a user
    /// changes locale) are reaped instead of accumulating forever.
    ///
    /// Both DELETEs run inside a single `BEGIN IMMEDIATE … COMMIT`
    /// transaction so a crash mid-prune cannot leave the two tables in
    /// divergent cohort states. Returns the total number of rows deleted
    /// across both tables, as reported by `sqlite3_changes`.
    ///
    /// NOTE: this method is exposed but NOT called by `migrate()`
    /// automatically. Wiring the production call in `PlayheadRuntime.init`
    /// is intentionally out of scope here (architectural; runtime changes
    /// are owned by a sibling agent). Call it once at app launch from the
    /// runtime with `ScanCohort.productionJSON()` as input.
    @discardableResult
    func pruneOrphanedScansForCurrentCohort(currentScanCohortJSON: String) throws -> Int {
        let canonical = Self.canonicalizeCohortJSON(currentScanCohortJSON)

        try exec("BEGIN IMMEDIATE")
        var totalDeleted = 0
        do {
            // Delete semantic scan rows under a non-current cohort.
            let scanSQL = "DELETE FROM semantic_scan_results WHERE scanCohortJSON != ?"
            let scanStmt = try prepare(scanSQL)
            bind(scanStmt, 1, canonical)
            try step(scanStmt, expecting: SQLITE_DONE)
            totalDeleted += Int(sqlite3_changes(db))
            sqlite3_finalize(scanStmt)

            // Delete evidence events under a non-current cohort.
            let evSQL = "DELETE FROM evidence_events WHERE scanCohortJSON != ?"
            let evStmt = try prepare(evSQL)
            bind(evStmt, 1, canonical)
            try step(evStmt, expecting: SQLITE_DONE)
            totalDeleted += Int(sqlite3_changes(db))
            sqlite3_finalize(evStmt)

            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }

        return totalDeleted
    }

    // MARK: - CRUD: semantic_scan_results

    /// Canonical column order shared by all `semantic_scan_results` readers:
    /// 0  id                         9  spansJSON           17 scanCohortJSON
    /// 1  analysisAssetId           10 status               18 transcriptVersion
    /// 2  windowFirstAtomOrdinal    11 attemptCount         19 reuseKeyHash
    /// 3  windowLastAtomOrdinal     12 errorContext         20 runMode (Rev3-M5)
    /// 4  windowStartTime           13 inputTokenCount      21 jobPhase (Rev3-M6)
    /// 5  windowEndTime             14 outputTokenCount
    /// 6  scanPass                  15 latencyMs
    /// 7  transcriptQuality         16 prewarmHit
    /// 8  disposition
    private static let semanticScanResultColumns = """
        id, analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
        windowStartTime, windowEndTime, scanPass, transcriptQuality,
        disposition, spansJSON, status, attemptCount, errorContext,
        inputTokenCount, outputTokenCount, latencyMs, prewarmHit,
        scanCohortJSON, transcriptVersion, reuseKeyHash, runMode, jobPhase
        """

    /// H-1: canonicalize a `scanCohortJSON` before hashing so two
    /// semantically-equivalent cohorts with different key order or
    /// whitespace produce the same reuse key. Decodes to `ScanCohort` and
    /// re-encodes with `.sortedKeys`; if the decode fails, falls back to
    /// the raw string so the hash still diverges and the caller's malformed
    /// input cannot silently collide with valid rows.
    private static func canonicalizeCohortJSON(_ raw: String) -> String {
        guard let data = raw.data(using: .utf8),
              let decoded = try? JSONDecoder().decode(ScanCohort.self, from: data) else {
            return raw
        }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        guard let encoded = try? encoder.encode(decoded),
              let canonical = String(data: encoded, encoding: .utf8) else {
            return raw
        }
        return canonical
    }

    /// Computes the canonical reuse-key SHA-256 over the fields that govern
    /// FM scan reusability. The same field order is used in `fetchReusable…`,
    /// keeping inserts and lookups in lockstep. The `scanCohortJSON` field is
    /// canonicalized (sorted keys) before hashing so cohort-equivalent inputs
    /// collapse to the same hash regardless of upstream JSON formatting.
    ///
    /// H12 (cycle 2): `reuseScope` was added to the hash domain in bd-3vm
    /// to keep logically distinct jobs/phases (e.g. shadow vs. targeted)
    /// from collapsing each other when they share the same window bounds,
    /// scan pass, and transcript version. The string layout is
    ///   "<assetId>|<first>|<last>|<scanPass>|<transcriptVersion>|<canonicalCohort>|<scope>"
    /// where `scope` is `reuseScope ?? "default"`. **Pre-bd-3vm cached
    /// rows will not be reused** by post-bd-3vm callers because the hash
    /// domain expanded — those rows hash to the old layout (no scope
    /// segment) and never collide with the new lookups. Single user, full
    /// DB wipe on cohort change is acceptable, so we accept the cache
    /// miss instead of running a one-shot rehash migration.
    static func semanticScanReuseKeyHash(
        analysisAssetId: String,
        windowFirstAtomOrdinal: Int,
        windowLastAtomOrdinal: Int,
        scanPass: String,
        transcriptVersion: String,
        scanCohortJSON: String,
        reuseScope: String? = nil
    ) -> String {
        let canonicalCohort = canonicalizeCohortJSON(scanCohortJSON)
        let scope = reuseScope ?? "default"
        let canonical =
            "\(analysisAssetId)|\(windowFirstAtomOrdinal)|\(windowLastAtomOrdinal)|" +
            "\(scanPass)|\(transcriptVersion)|\(canonicalCohort)|\(scope)"
        let digest = SHA256.hash(data: Data(canonical.utf8))
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    func insertSemanticScanResult(_ result: SemanticScanResult) throws {
        try validateScanCohortJSON(result.scanCohortJSON)

        // Fix #9: length caps on the two free-form blob columns. A runaway
        // error blob (e.g. a malformed model response echoed back verbatim)
        // or a bloated spansJSON payload would otherwise accumulate on disk
        // without bound. Reject at insert time so operators get a loud
        // signal instead of a silently growing SQLite file.
        //
        // NOTE: `BackfillJobRunner.isPermanent` already classifies
        // `insertFailed("payloadTooLarge: ...")` as a permanent error via
        // a string-prefix match, so oversized payloads are short-circuited
        // out of the retry budget without needing a dedicated enum case.
        let maxBlobLength = 1_000_000 // 1MB
        if let ctx = result.errorContext, ctx.utf8.count > maxBlobLength {
            throw AnalysisStoreError.insertFailed(
                "payloadTooLarge: errorContext \(ctx.utf8.count) bytes (max \(maxBlobLength))"
            )
        }
        if result.spansJSON.utf8.count > maxBlobLength {
            throw AnalysisStoreError.insertFailed(
                "payloadTooLarge: spansJSON \(result.spansJSON.utf8.count) bytes (max \(maxBlobLength))"
            )
        }

        let reuseKeyHash = Self.semanticScanReuseKeyHash(
            analysisAssetId: result.analysisAssetId,
            windowFirstAtomOrdinal: result.windowFirstAtomOrdinal,
            windowLastAtomOrdinal: result.windowLastAtomOrdinal,
            scanPass: result.scanPass,
            transcriptVersion: result.transcriptVersion,
            scanCohortJSON: result.scanCohortJSON,
            reuseScope: result.reuseScope
        )

        // H-1: a cached `.success` row must never be overwritten by a
        // subsequent `.refusal` (or other non-success) retry with the same
        // reuseKeyHash. The previous `INSERT OR REPLACE` silently destroyed
        // the cached success. Probe the existing row under the actor's
        // serialization guarantee and bail out early if the incoming row
        // would demote a cached success.
        //
        // Rank: `.success` outranks everything else. Same-rank collisions
        // fall through to the REPLACE path (last write wins), matching the
        // existing C5 contract for success-vs-success retries.
        if result.status != .success {
            let probe = try prepare("SELECT status FROM semantic_scan_results WHERE reuseKeyHash = ? LIMIT 1")
            defer { sqlite3_finalize(probe) }
            bind(probe, 1, reuseKeyHash)
            if sqlite3_step(probe) == SQLITE_ROW,
               let existingStatus = optionalText(probe, 0),
               existingStatus == SemanticScanStatus.success.rawValue {
                // Silently skip: the cached success is the canonical answer
                // and a later refusal must not destroy it.
                return
            }
        }

        let sql = """
            INSERT OR REPLACE INTO semantic_scan_results
            (id, analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
             windowStartTime, windowEndTime, scanPass, transcriptQuality,
             disposition, spansJSON, status, attemptCount, errorContext,
             inputTokenCount, outputTokenCount, latencyMs, prewarmHit,
             scanCohortJSON, transcriptVersion, reuseKeyHash, runMode, jobPhase)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        bind(stmt, 20, reuseKeyHash)
        bind(stmt, 21, result.runMode.rawValue)
        bind(stmt, 22, result.jobPhase)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// M2: atomic Pass-B write. Wraps a scan result and its evidence events in
    /// a single `BEGIN IMMEDIATE … COMMIT`. Any thrown error rolls back.
    @discardableResult
    func recordSemanticScanResult(
        _ result: SemanticScanResult,
        evidenceEvents: [EvidenceEvent]
    ) throws -> [String] {
        try validateScanCohortJSON(result.scanCohortJSON)
        for event in evidenceEvents {
            try validateAtomOrdinalsJSON(event.atomOrdinals)
            try validateScanCohortJSON(event.scanCohortJSON)
        }

        try exec("BEGIN IMMEDIATE")
        do {
            // R4-Fix2: when an incoming non-success row would be silently
            // dropped by `insertSemanticScanResult`'s H-1 success-protection
            // probe, the surrounding transaction must NOT commit the evidence
            // events — otherwise they attach to a phantom scan that the store
            // never wrote. Run the same check *inside* BEGIN IMMEDIATE so a
            // second writer cannot sneak in a cached success between the
            // preflight and the insert path.
            if result.status != .success {
                let reuseKeyHash = Self.semanticScanReuseKeyHash(
                    analysisAssetId: result.analysisAssetId,
                    windowFirstAtomOrdinal: result.windowFirstAtomOrdinal,
                    windowLastAtomOrdinal: result.windowLastAtomOrdinal,
                    scanPass: result.scanPass,
                    transcriptVersion: result.transcriptVersion,
                    scanCohortJSON: result.scanCohortJSON,
                    reuseScope: result.reuseScope
                )
                let probe = try prepare("SELECT status FROM semantic_scan_results WHERE reuseKeyHash = ? LIMIT 1")
                defer { sqlite3_finalize(probe) }
                bind(probe, 1, reuseKeyHash)
                if sqlite3_step(probe) == SQLITE_ROW,
                   let existingStatus = optionalText(probe, 0),
                   existingStatus == SemanticScanStatus.success.rawValue {
                    try exec("COMMIT")
                    return []
                }
            }

            var persistedEvidenceEventIds: [String] = []
            var seenEvidenceEventIds: Set<String> = []
            try insertSemanticScanResult(result)
            for event in evidenceEvents {
                if let persistedId = try insertEvidenceEvent(
                    event,
                    transcriptVersion: result.transcriptVersion
                ),
                   seenEvidenceEventIds.insert(persistedId).inserted {
                    persistedEvidenceEventIds.append(persistedId)
                }
            }
            try exec("COMMIT")
            return persistedEvidenceEventIds
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

    /// C6/H-4: look up a reusable successful scan by computing the
    /// `reuseKeyHash` from the caller's tuple and hitting the
    /// `UNIQUE(reuseKeyHash)` index for a single O(log n) lookup. The
    /// previous implementation filtered on the (asset, ordinals, pass,
    /// transcriptVersion) tuple and iterated matching rows to compare
    /// cohort JSON in memory — correct but O(n) on the per-asset row set.
    ///
    /// Because inserts canonicalize cohort JSON before hashing (H-1) and
    /// this lookup does the same, cohort-equivalent strings always resolve
    /// to the same row regardless of upstream JSON formatting.
    func fetchReusableSemanticScanResult(
        analysisAssetId: String,
        windowFirstAtomOrdinal: Int,
        windowLastAtomOrdinal: Int,
        scanPass: String,
        scanCohortJSON: String,
        transcriptVersion: String,
        reuseScope: String? = nil
    ) throws -> SemanticScanResult? {
        let hash = Self.semanticScanReuseKeyHash(
            analysisAssetId: analysisAssetId,
            windowFirstAtomOrdinal: windowFirstAtomOrdinal,
            windowLastAtomOrdinal: windowLastAtomOrdinal,
            scanPass: scanPass,
            transcriptVersion: transcriptVersion,
            scanCohortJSON: scanCohortJSON,
            reuseScope: reuseScope
        )
        let sql = """
            SELECT \(Self.semanticScanResultColumns) FROM semantic_scan_results
            WHERE reuseKeyHash = ? AND status = 'success'
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, hash)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try readSemanticScanResult(stmt)
    }

    // MARK: - CRUD: evidence_events

    /// Canonical column order for `evidence_events` readers:
    /// 0 id, 1 analysisAssetId, 2 eventType, 3 sourceType,
    /// 4 atomOrdinals, 5 evidenceJSON, 6 scanCohortJSON, 7 createdAt,
    /// 8 runMode (Rev3-M5, shadow/targeted), 9 jobPhase (Rev3-M6,
    /// BackfillJobPhase.rawValue).
    private static let evidenceEventColumns = """
        id, analysisAssetId, eventType, sourceType,
        atomOrdinals, evidenceJSON, scanCohortJSON, createdAt, runMode, jobPhase
        """

    @discardableResult
    func insertEvidenceEvent(
        _ event: EvidenceEvent,
        transcriptVersion: String = ""
    ) throws -> String? {
        try validateAtomOrdinalsJSON(event.atomOrdinals)
        try validateScanCohortJSON(event.scanCohortJSON)
        // playhead-fn0: silent dedup on the exact persisted evidence identity:
        // (asset, eventType, sourceType, atomOrdinals, evidenceJSON,
        // scanCohortJSON, transcriptVersion). Distinct FM spans at the same
        // atom range now both persist when their bodies differ materially, and
        // append-only audit survives transcript-version churn.
        let sql = """
            INSERT OR IGNORE INTO evidence_events
            (id, analysisAssetId, eventType, sourceType, atomOrdinals,
             evidenceJSON, scanCohortJSON, transcriptVersion, createdAt, runMode, jobPhase)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        bind(stmt, 8, transcriptVersion)
        bind(stmt, 9, event.createdAt)
        bind(stmt, 10, event.runMode.rawValue)
        bind(stmt, 11, event.jobPhase)
        try step(stmt, expecting: SQLITE_DONE)
        if sqlite3_changes(db) > 0 {
            return event.id
        }

        // The row was ignored. Two legitimate cases remain:
        //   1. PRIMARY KEY collision with the *same* body — idempotent.
        //   2. Exact natural-key dedup where another row with the same
        //      persisted body already exists under the 6-column UNIQUE key.
        let probe = try prepare("""
            SELECT eventType, sourceType, atomOrdinals, evidenceJSON,
                   scanCohortJSON, transcriptVersion, createdAt, analysisAssetId
            FROM evidence_events
            WHERE id = ?
            LIMIT 1
            """)
        defer { sqlite3_finalize(probe) }
        bind(probe, 1, event.id)

        if sqlite3_step(probe) == SQLITE_ROW {
            let storedEventType = optionalText(probe, 0) ?? ""
            let storedSourceType = optionalText(probe, 1) ?? ""
            let storedAtomOrdinals = optionalText(probe, 2) ?? ""
            let storedEvidenceJSON = optionalText(probe, 3) ?? ""
            let storedScanCohortJSON = optionalText(probe, 4) ?? ""
            let storedTranscriptVersion = optionalText(probe, 5) ?? ""
            _ = sqlite3_column_double(probe, 6)
            let storedAnalysisAssetId = optionalText(probe, 7) ?? ""

            let bodyMatches =
                storedAnalysisAssetId == event.analysisAssetId &&
                storedEventType == event.eventType &&
                storedSourceType == event.sourceType.rawValue &&
                storedAtomOrdinals == event.atomOrdinals &&
                storedEvidenceJSON == event.evidenceJSON &&
                storedScanCohortJSON == event.scanCohortJSON &&
                storedTranscriptVersion == transcriptVersion

            if !bodyMatches {
                throw AnalysisStoreError.evidenceEventBodyMismatch(id: event.id)
            }
            return event.id
        }

        let naturalProbe = try prepare("""
            SELECT id
            FROM evidence_events
            WHERE analysisAssetId = ?
              AND eventType = ?
              AND sourceType = ?
              AND atomOrdinals = ?
              AND evidenceJSON = ?
              AND scanCohortJSON = ?
              AND transcriptVersion = ?
            LIMIT 1
            """)
        defer { sqlite3_finalize(naturalProbe) }
        bind(naturalProbe, 1, event.analysisAssetId)
        bind(naturalProbe, 2, event.eventType)
        bind(naturalProbe, 3, event.sourceType.rawValue)
        bind(naturalProbe, 4, event.atomOrdinals)
        bind(naturalProbe, 5, event.evidenceJSON)
        bind(naturalProbe, 6, event.scanCohortJSON)
        bind(naturalProbe, 7, transcriptVersion)

        if sqlite3_step(naturalProbe) == SQLITE_ROW {
            return optionalText(naturalProbe, 0) ?? event.id
        }

        logger.error(
            "evidence_events insert ignored without matching stored row: id=\(event.id, privacy: .public) eventType=\(event.eventType, privacy: .public)"
        )
        return nil
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

    /// M25/L-4: parses `atomOrdinals` and verifies it's a JSON array of
    /// integers. Uses `JSONDecoder.decode([Int].self, ...)` which rejects
    /// floats (`JSONSerialization` happily parses `1.5` as an `NSNumber`
    /// and a permissive numeric cast would let it through).
    private func validateAtomOrdinalsJSON(_ json: String) throws {
        guard let data = json.data(using: .utf8) else {
            throw AnalysisStoreError.invalidEvidenceEvent("atomOrdinals must be a JSON array of integers, got: \(json.prefix(80))")
        }
        do {
            _ = try JSONDecoder().decode([Int].self, from: data)
        } catch {
            throw AnalysisStoreError.invalidEvidenceEvent("atomOrdinals must be a JSON array of integers, got: \(json.prefix(80))")
        }
    }

    /// L-3: validates that `scanCohortJSON` decodes as a real `ScanCohort`
    /// object, not merely any parseable JSON value. The previous
    /// `JSONSerialization.jsonObject` check accepted top-level arrays,
    /// strings, or numbers, all of which are nonsensical cohorts and would
    /// silently defeat the reuse-key contract.
    private func validateScanCohortJSON(_ json: String) throws {
        guard let data = json.data(using: .utf8) else {
            throw AnalysisStoreError.invalidScanCohortJSON("not utf-8")
        }
        do {
            _ = try JSONDecoder().decode(ScanCohort.self, from: data)
        } catch {
            throw AnalysisStoreError.invalidScanCohortJSON("not a decodable ScanCohort: \(error)")
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

        // Rev3-M5: column 20 is `runMode`. Default to `.shadow` for any
        // legacy row that escaped the migration's NOT NULL DEFAULT.
        let runModeRaw = optionalText(stmt, 20) ?? SemanticScanPhase.shadow.rawValue
        let runMode = SemanticScanPhase(rawValue: runModeRaw) ?? .shadow

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
            transcriptVersion: try requireText(stmt, 18),
            // column 19 = reuseKeyHash (not persisted back onto the struct)
            runMode: runMode,
            jobPhase: optionalText(stmt, 21) ?? "shadow"
        )
    }

    private func readEvidenceEvent(_ stmt: OpaquePointer?) throws -> EvidenceEvent {
        let sourceTypeRaw = try requireText(stmt, 3)
        guard let sourceType = EvidenceSourceType(rawValue: sourceTypeRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown evidence source type '\(sourceTypeRaw)'")
        }

        // Rev3-M5: column 8 is `runMode`. Default to `.shadow` for any
        // legacy row that escaped the migration's NOT NULL DEFAULT.
        let runModeRaw = optionalText(stmt, 8) ?? SemanticScanPhase.shadow.rawValue
        let runMode = SemanticScanPhase(rawValue: runModeRaw) ?? .shadow

        return EvidenceEvent(
            id: try requireText(stmt, 0),
            analysisAssetId: try requireText(stmt, 1),
            eventType: try requireText(stmt, 2),
            sourceType: sourceType,
            atomOrdinals: try requireText(stmt, 4),
            evidenceJSON: try requireText(stmt, 5),
            scanCohortJSON: try requireText(stmt, 6),
            createdAt: sqlite3_column_double(stmt, 7),
            runMode: runMode,
            jobPhase: optionalText(stmt, 9) ?? "shadow"
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

    private func encodeMagnitudesJSON(_ magnitudes: [Float]) throws -> String {
        let data = try JSONEncoder().encode(magnitudes)
        guard let json = String(data: data, encoding: .utf8) else {
            throw AnalysisStoreError.insertFailed("Failed to encode feature extraction magnitudes as UTF-8")
        }
        return json
    }

    private func decodeMagnitudesJSON(_ json: String) throws -> [Float] {
        guard let data = json.data(using: .utf8) else {
            throw AnalysisStoreError.queryFailed("Feature extraction magnitudes JSON was not valid UTF-8")
        }
        do {
            return try JSONDecoder().decode([Float].self, from: data)
        } catch {
            throw AnalysisStoreError.queryFailed("Failed to decode feature extraction magnitudes JSON: \(error)")
        }
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

    // MARK: - CRUD: decoded_spans (Phase 5, playhead-4my.5.2)

    /// Persist decoded spans for an asset. Uses INSERT OR REPLACE for idempotency:
    /// re-running the decoder on the same input produces the same ids and overwrites
    /// existing rows without creating duplicates.
    func upsertDecodedSpans(_ spans: [DecodedSpan]) throws {
        guard !spans.isEmpty else { return }
        let encoder = JSONEncoder()
        try exec("BEGIN TRANSACTION")
        do {
            let sql = """
                INSERT OR REPLACE INTO decoded_spans
                (id, assetId, firstAtomOrdinal, lastAtomOrdinal, startTime, endTime, anchorProvenanceJSON)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
            let stmt = try prepare(sql)
            defer { sqlite3_finalize(stmt) }
            for span in spans {
                let provenanceData = (try? encoder.encode(span.anchorProvenance)) ?? Data()
                let provenanceJSON = String(decoding: provenanceData, as: UTF8.self)
                sqlite3_reset(stmt)
                bind(stmt, 1, span.id)
                bind(stmt, 2, span.assetId)
                bind(stmt, 3, span.firstAtomOrdinal)
                bind(stmt, 4, span.lastAtomOrdinal)
                bind(stmt, 5, span.startTime)
                bind(stmt, 6, span.endTime)
                bind(stmt, 7, provenanceJSON)
                try step(stmt, expecting: SQLITE_DONE)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    /// Fetch all decoded spans for an asset, ordered by startTime.
    func fetchDecodedSpans(assetId: String) throws -> [DecodedSpan] {
        let sql = """
            SELECT id, assetId, firstAtomOrdinal, lastAtomOrdinal,
                   startTime, endTime, anchorProvenanceJSON
            FROM decoded_spans
            WHERE assetId = ?
            ORDER BY startTime
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)

        let decoder = JSONDecoder()
        var results: [DecodedSpan] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            let id = text(stmt, 0)
            let aid = text(stmt, 1)
            let firstOrdinal = Int(sqlite3_column_int(stmt, 2))
            let lastOrdinal = Int(sqlite3_column_int(stmt, 3))
            let startTime = sqlite3_column_double(stmt, 4)
            let endTime = sqlite3_column_double(stmt, 5)
            let provenanceJSON = text(stmt, 6)

            let provenance: [AnchorRef]
            if provenanceJSON.isEmpty || provenanceJSON == "[]" {
                provenance = []
            } else if let data = provenanceJSON.data(using: .utf8),
                      let decoded = try? decoder.decode([AnchorRef].self, from: data) {
                provenance = decoded
            } else {
                logger.warning("fetchDecodedSpans: failed to decode anchorProvenanceJSON for span \(id, privacy: .public) asset \(aid, privacy: .public)")
                provenance = []
            }

            results.append(DecodedSpan(
                id: id,
                assetId: aid,
                firstAtomOrdinal: firstOrdinal,
                lastAtomOrdinal: lastOrdinal,
                startTime: startTime,
                endTime: endTime,
                anchorProvenance: provenance
            ))
        }
        return results
    }

    /// Delete all decoded spans for an asset. Used by tests and idempotent re-runs.
    func deleteDecodedSpans(assetId: String) throws {
        let sql = "DELETE FROM decoded_spans WHERE assetId = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    // MARK: - CRUD: ad_decision_results (Phase 6, playhead-4my.6.3)

    /// Upsert — a new cohort produces an updated decision for the same asset.
    ///
    /// The UNIQUE constraint on `analysisAssetId` means INSERT OR REPLACE overwrites the
    /// previous artifact row. Any `decision_events` rows written for the old artifact remain
    /// (append-only audit trail) and are now orphaned from the active artifact. This is
    /// intentional: callers querying events for a historical cohort can still find them by
    /// filtering on `decisionCohortJSON`. New-cohort callers should ignore old events.
    func saveDecisionResultArtifact(_ result: DecisionResultArtifact) throws {
        let sql = """
            INSERT OR REPLACE INTO ad_decision_results
            (id, analysisAssetId, decisionCohortJSON, inputArtifactRefs, decisionJSON, createdAt)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, result.id)
        bind(stmt, 2, result.analysisAssetId)
        bind(stmt, 3, result.decisionCohortJSON)
        bind(stmt, 4, result.inputArtifactRefs)
        bind(stmt, 5, result.decisionJSON)
        bind(stmt, 6, result.createdAt)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func loadDecisionResultArtifact(for analysisAssetId: String) throws -> DecisionResultArtifact? {
        // ORDER BY / LIMIT are defensive no-ops: the UNIQUE constraint on analysisAssetId
        // guarantees at most one row per asset. They are harmless and clarify intent.
        let sql = "SELECT id, analysisAssetId, decisionCohortJSON, inputArtifactRefs, decisionJSON, createdAt FROM ad_decision_results WHERE analysisAssetId = ? ORDER BY createdAt DESC LIMIT 1"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try DecisionResultArtifact(
            id: requireText(stmt, 0),
            analysisAssetId: requireText(stmt, 1),
            decisionCohortJSON: requireText(stmt, 2),
            inputArtifactRefs: requireText(stmt, 3),
            decisionJSON: requireText(stmt, 4),
            createdAt: sqlite3_column_double(stmt, 5)
        )
    }

    // MARK: - CRUD: decision_events (append-only)

    func appendDecisionEvent(_ event: DecisionEvent) throws {
        let sql = """
            INSERT INTO decision_events
            (id, analysisAssetId, eventType, windowId, proposalConfidence, skipConfidence,
             eligibilityGate, policyAction, decisionCohortJSON, createdAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, event.id)
        bind(stmt, 2, event.analysisAssetId)
        bind(stmt, 3, event.eventType)
        bind(stmt, 4, event.windowId)
        bind(stmt, 5, event.proposalConfidence)
        bind(stmt, 6, event.skipConfidence)
        bind(stmt, 7, event.eligibilityGate)
        bind(stmt, 8, event.policyAction)
        bind(stmt, 9, event.decisionCohortJSON)
        bind(stmt, 10, event.createdAt)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func loadDecisionEvents(for analysisAssetId: String) throws -> [DecisionEvent] {
        let sql = "SELECT id, analysisAssetId, eventType, windowId, proposalConfidence, skipConfidence, eligibilityGate, policyAction, decisionCohortJSON, createdAt FROM decision_events WHERE analysisAssetId = ? ORDER BY createdAt"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        var results: [DecisionEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(try DecisionEvent(
                id: requireText(stmt, 0),
                analysisAssetId: requireText(stmt, 1),
                eventType: requireText(stmt, 2),
                windowId: requireText(stmt, 3),
                proposalConfidence: sqlite3_column_double(stmt, 4),
                skipConfidence: sqlite3_column_double(stmt, 5),
                eligibilityGate: requireText(stmt, 6),
                policyAction: requireText(stmt, 7),
                decisionCohortJSON: requireText(stmt, 8),
                createdAt: sqlite3_column_double(stmt, 9)
            ))
        }
        return results
    }

    // MARK: - CRUD: correction_events (Phase 7, playhead-4my.7.1)

    /// Persist a user correction event.
    func appendCorrectionEvent(_ event: CorrectionEvent) throws {
        let sql = """
            INSERT OR IGNORE INTO correction_events
            (id, analysisAssetId, scope, createdAt, source, podcastId)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, event.id)
        bind(stmt, 2, event.analysisAssetId)
        bind(stmt, 3, event.scope)
        bind(stmt, 4, event.createdAt)
        bind(stmt, 5, event.source?.rawValue)
        bind(stmt, 6, event.podcastId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Load all correction events for an asset, ordered by createdAt ascending.
    func loadCorrectionEvents(analysisAssetId: String) throws -> [CorrectionEvent] {
        let sql = """
            SELECT id, analysisAssetId, scope, createdAt, source, podcastId
            FROM correction_events
            WHERE analysisAssetId = ?
            ORDER BY createdAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)

        var results: [CorrectionEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            let id = text(stmt, 0)
            let assetId = text(stmt, 1)
            let scope = text(stmt, 2)
            let createdAt = sqlite3_column_double(stmt, 3)
            let sourceRaw = optionalText(stmt, 4)
            let podcastId = optionalText(stmt, 5)
            let source = sourceRaw.flatMap { CorrectionSource(rawValue: $0) }
            results.append(CorrectionEvent(
                id: id,
                analysisAssetId: assetId,
                scope: scope,
                createdAt: createdAt,
                source: source,
                podcastId: podcastId
            ))
        }
        return results
    }

    /// Returns true if any correction event exists with the given scope string.
    func hasAnyCorrectionEvent(withScope scope: String) throws -> Bool {
        let sql = """
            SELECT EXISTS(
                SELECT 1 FROM correction_events WHERE scope = ?
            )
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, scope)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return false }
        return sqlite3_column_int(stmt, 0) != 0
    }

    /// Batch check: returns the set of scopes (from the input) that have at
    /// least one correction event. Single round-trip instead of N queries.
    func correctionScopesPresent(from scopes: [String]) throws -> Set<String> {
        guard !scopes.isEmpty else { return [] }
        let placeholders = scopes.map { _ in "?" }.joined(separator: ", ")
        let sql = "SELECT DISTINCT scope FROM correction_events WHERE scope IN (\(placeholders))"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        for (i, scope) in scopes.enumerated() {
            bind(stmt, Int32(i + 1), scope)
        }
        var result = Set<String>()
        while sqlite3_step(stmt) == SQLITE_ROW {
            result.insert(text(stmt, 0))
        }
        return result
    }

    // MARK: - CRUD: sponsor_knowledge_entries (Phase 8, playhead-4my.8.1)

    /// Upsert a sponsor knowledge entry. Uses INSERT OR REPLACE on the
    /// natural key (podcastId, entityType, normalizedValue).
    func upsertKnowledgeEntry(_ entry: SponsorKnowledgeEntry) throws {
        let sql = """
            INSERT INTO sponsor_knowledge_entries
            (id, podcastId, entityType, entityValue, normalizedValue, state,
             confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
             lastRollbackAt, decayedAt, blockedAt, aliases, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(podcastId, entityType, normalizedValue) DO UPDATE SET
                entityValue = excluded.entityValue,
                state = excluded.state,
                confirmationCount = excluded.confirmationCount,
                rollbackCount = excluded.rollbackCount,
                lastConfirmedAt = excluded.lastConfirmedAt,
                lastRollbackAt = excluded.lastRollbackAt,
                decayedAt = excluded.decayedAt,
                blockedAt = excluded.blockedAt,
                aliases = excluded.aliases,
                metadata = excluded.metadata
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, entry.id)
        bind(stmt, 2, entry.podcastId)
        bind(stmt, 3, entry.entityType.rawValue)
        bind(stmt, 4, entry.entityValue)
        bind(stmt, 5, entry.normalizedValue)
        bind(stmt, 6, entry.state.rawValue)
        bind(stmt, 7, entry.confirmationCount)
        bind(stmt, 8, entry.rollbackCount)
        bind(stmt, 9, entry.firstSeenAt)
        bind(stmt, 10, entry.lastConfirmedAt)
        bind(stmt, 11, entry.lastRollbackAt)
        bind(stmt, 12, entry.decayedAt)
        bind(stmt, 13, entry.blockedAt)
        let aliasesJSON = try encodeJSONString(entry.aliases)
        bind(stmt, 14, aliasesJSON)
        let metadataJSON = try encodeJSONString(entry.metadata)
        bind(stmt, 15, metadataJSON)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Load a single knowledge entry by its natural key.
    func loadKnowledgeEntry(
        podcastId: String,
        entityType: KnowledgeEntityType,
        normalizedValue: String
    ) throws -> SponsorKnowledgeEntry? {
        let sql = """
            SELECT id, podcastId, entityType, entityValue, normalizedValue, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, aliases, metadata
            FROM sponsor_knowledge_entries
            WHERE podcastId = ? AND entityType = ? AND normalizedValue = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, entityType.rawValue)
        bind(stmt, 3, normalizedValue)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try readKnowledgeEntry(stmt)
    }

    /// Load all knowledge entries for a podcast with a given state.
    /// Rows with unrecognized enum values are skipped (logged) rather than
    /// failing the entire batch, so one corrupt row doesn't break queries.
    func loadKnowledgeEntries(
        podcastId: String,
        state: KnowledgeState
    ) throws -> [SponsorKnowledgeEntry] {
        let sql = """
            SELECT id, podcastId, entityType, entityValue, normalizedValue, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, aliases, metadata
            FROM sponsor_knowledge_entries
            WHERE podcastId = ? AND state = ?
            ORDER BY firstSeenAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, state.rawValue)
        var results: [SponsorKnowledgeEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            do {
                results.append(try readKnowledgeEntry(stmt))
            } catch {
                logger.warning("Skipping corrupt knowledge entry: \(error.localizedDescription)")
            }
        }
        return results
    }

    /// Load all knowledge entries for a podcast regardless of state.
    /// Rows with unrecognized enum values are skipped (logged) rather than
    /// failing the entire batch.
    func loadAllKnowledgeEntries(podcastId: String) throws -> [SponsorKnowledgeEntry] {
        let sql = """
            SELECT id, podcastId, entityType, entityValue, normalizedValue, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, aliases, metadata
            FROM sponsor_knowledge_entries
            WHERE podcastId = ?
            ORDER BY firstSeenAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        var results: [SponsorKnowledgeEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            do {
                results.append(try readKnowledgeEntry(stmt))
            } catch {
                logger.warning("Skipping corrupt knowledge entry: \(error.localizedDescription)")
            }
        }
        return results
    }

    /// Read a SponsorKnowledgeEntry from the current row of a prepared statement.
    private func readKnowledgeEntry(_ stmt: OpaquePointer?) throws -> SponsorKnowledgeEntry {
        let id = text(stmt, 0)
        let podcastId = text(stmt, 1)
        let entityTypeRaw = text(stmt, 2)
        let entityValue = text(stmt, 3)
        let normalizedValue = text(stmt, 4)
        let stateRaw = text(stmt, 5)
        let confirmationCount = Int(sqlite3_column_int(stmt, 6))
        let rollbackCount = Int(sqlite3_column_int(stmt, 7))
        let firstSeenAt = sqlite3_column_double(stmt, 8)
        let lastConfirmedAt = optionalDouble(stmt, 9)
        let lastRollbackAt = optionalDouble(stmt, 10)
        let decayedAt = optionalDouble(stmt, 11)
        let blockedAt = optionalDouble(stmt, 12)
        let aliasesJSON = optionalText(stmt, 13)
        let metadataJSON = optionalText(stmt, 14)

        guard let entityType = KnowledgeEntityType(rawValue: entityTypeRaw) else {
            throw AnalysisStoreError.queryFailed("Invalid entityType: \(entityTypeRaw)")
        }
        guard let state = KnowledgeState(rawValue: stateRaw) else {
            throw AnalysisStoreError.queryFailed("Invalid KnowledgeState: \(stateRaw)")
        }

        let aliases: [String] = try decodeJSON([String].self, from: aliasesJSON) ?? []
        let metadata: [String: String]? = try decodeJSON([String: String].self, from: metadataJSON)

        return SponsorKnowledgeEntry(
            id: id,
            podcastId: podcastId,
            entityType: entityType,
            entityValue: entityValue,
            normalizedValue: normalizedValue,
            state: state,
            confirmationCount: confirmationCount,
            rollbackCount: rollbackCount,
            firstSeenAt: firstSeenAt,
            lastConfirmedAt: lastConfirmedAt,
            lastRollbackAt: lastRollbackAt,
            decayedAt: decayedAt,
            blockedAt: blockedAt,
            aliases: aliases,
            metadata: metadata
        )
    }

    // MARK: - CRUD: knowledge_candidate_events (Phase 8, playhead-4my.8.1)

    /// Append a knowledge candidate event (provenance log).
    func appendKnowledgeCandidateEvent(_ event: KnowledgeCandidateEvent) throws {
        let sql = """
            INSERT OR IGNORE INTO knowledge_candidate_events
            (id, analysisAssetId, entityType, entityValue, sourceAtomOrdinals,
             transcriptVersion, confidence, scanCohortJSON, createdAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, event.id)
        bind(stmt, 2, event.analysisAssetId)
        bind(stmt, 3, event.entityType.rawValue)
        bind(stmt, 4, event.entityValue)
        let ordinalsJSON = try encodeJSONString(event.sourceAtomOrdinals)
        bind(stmt, 5, ordinalsJSON)
        bind(stmt, 6, event.transcriptVersion)
        bind(stmt, 7, event.confidence)
        bind(stmt, 8, event.scanCohortJSON)
        bind(stmt, 9, event.createdAt)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Load all candidate events for a given analysis asset, ordered by createdAt.
    /// Rows with unrecognized enum values are skipped (logged) rather than
    /// failing the entire batch.
    func loadKnowledgeCandidateEvents(analysisAssetId: String) throws -> [KnowledgeCandidateEvent] {
        let sql = """
            SELECT id, analysisAssetId, entityType, entityValue, sourceAtomOrdinals,
                   transcriptVersion, confidence, scanCohortJSON, createdAt
            FROM knowledge_candidate_events
            WHERE analysisAssetId = ?
            ORDER BY createdAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        var results: [KnowledgeCandidateEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            let id = text(stmt, 0)
            let assetId = text(stmt, 1)
            let entityTypeRaw = text(stmt, 2)
            let entityValue = text(stmt, 3)
            let ordinalsJSON = text(stmt, 4)
            let transcriptVersion = text(stmt, 5)
            let confidence = sqlite3_column_double(stmt, 6)
            let scanCohortJSON = optionalText(stmt, 7)
            let createdAt = sqlite3_column_double(stmt, 8)

            guard let entityType = KnowledgeEntityType(rawValue: entityTypeRaw) else {
                logger.warning("Skipping candidate event with invalid entityType: \(entityTypeRaw)")
                continue
            }
            let ordinals: [Int] = (try? decodeJSON([Int].self, from: ordinalsJSON)) ?? []

            results.append(KnowledgeCandidateEvent(
                id: id,
                analysisAssetId: assetId,
                entityType: entityType,
                entityValue: entityValue,
                sourceAtomOrdinals: ordinals,
                transcriptVersion: transcriptVersion,
                confidence: confidence,
                scanCohortJSON: scanCohortJSON,
                createdAt: createdAt
            ))
        }
        return results
    }

    // MARK: - CRUD: ad_copy_fingerprints (Phase 9, playhead-4my.9.1)

    /// Upsert a fingerprint entry. Uses INSERT OR REPLACE on the
    /// natural key (podcastId, fingerprintHash).
    func upsertFingerprintEntry(_ entry: FingerprintEntry) throws {
        let sql = """
            INSERT INTO ad_copy_fingerprints
            (id, podcastId, fingerprintHash, normalizedText, state,
             confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
             lastRollbackAt, decayedAt, blockedAt, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(podcastId, fingerprintHash) DO UPDATE SET
                normalizedText = excluded.normalizedText,
                state = excluded.state,
                confirmationCount = excluded.confirmationCount,
                rollbackCount = excluded.rollbackCount,
                lastConfirmedAt = excluded.lastConfirmedAt,
                lastRollbackAt = excluded.lastRollbackAt,
                decayedAt = excluded.decayedAt,
                blockedAt = excluded.blockedAt,
                metadata = excluded.metadata
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, entry.id)
        bind(stmt, 2, entry.podcastId)
        bind(stmt, 3, entry.fingerprintHash)
        bind(stmt, 4, entry.normalizedText)
        bind(stmt, 5, entry.state.rawValue)
        bind(stmt, 6, entry.confirmationCount)
        bind(stmt, 7, entry.rollbackCount)
        bind(stmt, 8, entry.firstSeenAt)
        bind(stmt, 9, entry.lastConfirmedAt)
        bind(stmt, 10, entry.lastRollbackAt)
        bind(stmt, 11, entry.decayedAt)
        bind(stmt, 12, entry.blockedAt)
        let metadataJSON = try encodeJSONString(entry.metadata)
        bind(stmt, 13, metadataJSON)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Load a single fingerprint entry by its natural key.
    func loadFingerprintEntry(
        podcastId: String,
        fingerprintHash: String
    ) throws -> FingerprintEntry? {
        let sql = """
            SELECT id, podcastId, fingerprintHash, normalizedText, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, metadata
            FROM ad_copy_fingerprints
            WHERE podcastId = ? AND fingerprintHash = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, fingerprintHash)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try readFingerprintEntry(stmt)
    }

    /// Load all fingerprint entries for a podcast with a given state.
    /// Rows with unrecognized enum values are skipped (logged) rather than
    /// failing the entire batch.
    func loadFingerprintEntries(
        podcastId: String,
        state: KnowledgeState
    ) throws -> [FingerprintEntry] {
        let sql = """
            SELECT id, podcastId, fingerprintHash, normalizedText, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, metadata
            FROM ad_copy_fingerprints
            WHERE podcastId = ? AND state = ?
            ORDER BY firstSeenAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, state.rawValue)
        var results: [FingerprintEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            do {
                results.append(try readFingerprintEntry(stmt))
            } catch {
                logger.warning("Skipping corrupt fingerprint entry: \(error.localizedDescription)")
            }
        }
        return results
    }

    /// Load all fingerprint entries for a podcast regardless of state.
    /// Rows with unrecognized enum values are skipped (logged) rather than
    /// failing the entire batch.
    func loadAllFingerprintEntries(podcastId: String) throws -> [FingerprintEntry] {
        let sql = """
            SELECT id, podcastId, fingerprintHash, normalizedText, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, metadata
            FROM ad_copy_fingerprints
            WHERE podcastId = ?
            ORDER BY firstSeenAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        var results: [FingerprintEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            do {
                results.append(try readFingerprintEntry(stmt))
            } catch {
                logger.warning("Skipping corrupt fingerprint entry: \(error.localizedDescription)")
            }
        }
        return results
    }

    /// Read a FingerprintEntry from the current row of a prepared statement.
    private func readFingerprintEntry(_ stmt: OpaquePointer?) throws -> FingerprintEntry {
        let id = text(stmt, 0)
        let podcastId = text(stmt, 1)
        let fingerprintHash = text(stmt, 2)
        let normalizedText = text(stmt, 3)
        let stateRaw = text(stmt, 4)
        let confirmationCount = Int(sqlite3_column_int(stmt, 5))
        let rollbackCount = Int(sqlite3_column_int(stmt, 6))
        let firstSeenAt = sqlite3_column_double(stmt, 7)
        let lastConfirmedAt = optionalDouble(stmt, 8)
        let lastRollbackAt = optionalDouble(stmt, 9)
        let decayedAt = optionalDouble(stmt, 10)
        let blockedAt = optionalDouble(stmt, 11)
        let metadataJSON = optionalText(stmt, 12)

        guard let state = KnowledgeState(rawValue: stateRaw) else {
            throw AnalysisStoreError.queryFailed("Invalid KnowledgeState: \(stateRaw)")
        }

        let metadata: [String: String]? = try decodeJSON([String: String].self, from: metadataJSON)

        return FingerprintEntry(
            id: id,
            podcastId: podcastId,
            fingerprintHash: fingerprintHash,
            normalizedText: normalizedText,
            state: state,
            confirmationCount: confirmationCount,
            rollbackCount: rollbackCount,
            firstSeenAt: firstSeenAt,
            lastConfirmedAt: lastConfirmedAt,
            lastRollbackAt: lastRollbackAt,
            decayedAt: decayedAt,
            blockedAt: blockedAt,
            metadata: metadata
        )
    }

    // MARK: - Atomic confirm/rollback (Phase 9)

    /// Atomically load → increment confirmation → promote → upsert a fingerprint
    /// entry. Returns the resolved (podcastId, fingerprintHash) so the caller can
    /// log provenance against the correct stored hash. Because this runs inside a
    /// single actor-isolated call, no TOCTOU race is possible.
    func atomicConfirmFingerprint(
        podcastId: String,
        fingerprintHash: String,
        normalizedText: String,
        promote: (_ current: KnowledgeState, _ confirmations: Int, _ rollbacks: Int) -> KnowledgeState,
        nearDuplicateCheck: (_ newHash: String, _ existingHash: String) -> Bool
    ) throws -> (resolvedHash: String, entry: FingerprintEntry) {
        // 1. Exact match?
        if let existing = try loadFingerprintEntry(podcastId: podcastId, fingerprintHash: fingerprintHash) {
            // Blocked is truly terminal — return as-is to prevent the "new
            // entry" path from overwriting via UPSERT. Decayed entries CAN
            // recover through re-confirmation (by design).
            if existing.state == .blocked {
                return (fingerprintHash, existing)
            }
            let newCount = existing.confirmationCount + 1
            let now = Date().timeIntervalSince1970
            let newState = promote(existing.state, newCount, existing.rollbackCount)
            let updated = FingerprintEntry(
                id: existing.id,
                podcastId: existing.podcastId,
                fingerprintHash: existing.fingerprintHash,
                normalizedText: existing.normalizedText,
                state: newState,
                confirmationCount: newCount,
                rollbackCount: existing.rollbackCount,
                firstSeenAt: existing.firstSeenAt,
                lastConfirmedAt: now,
                lastRollbackAt: existing.lastRollbackAt,
                decayedAt: newState == .decayed ? now : existing.decayedAt,
                blockedAt: newState == .blocked ? now : existing.blockedAt,
                metadata: existing.metadata
            )
            try upsertFingerprintEntry(updated)
            return (fingerprintHash, updated)
        }

        // 2. Near-duplicate match? Skip blocked entries — blocked is truly
        //    terminal and should not accumulate confirmations. Decayed entries
        //    can recover through re-confirmation.
        let allEntries = try loadAllFingerprintEntries(podcastId: podcastId)
        for entry in allEntries where entry.state != .blocked {
            if nearDuplicateCheck(fingerprintHash, entry.fingerprintHash) {
                let newCount = entry.confirmationCount + 1
                let now = Date().timeIntervalSince1970
                let newState = promote(entry.state, newCount, entry.rollbackCount)
                let updated = FingerprintEntry(
                    id: entry.id,
                    podcastId: entry.podcastId,
                    fingerprintHash: entry.fingerprintHash,
                    normalizedText: entry.normalizedText,
                    state: newState,
                    confirmationCount: newCount,
                    rollbackCount: entry.rollbackCount,
                    firstSeenAt: entry.firstSeenAt,
                    lastConfirmedAt: now,
                    lastRollbackAt: entry.lastRollbackAt,
                    decayedAt: newState == .decayed ? now : entry.decayedAt,
                    blockedAt: newState == .blocked ? now : entry.blockedAt,
                    metadata: entry.metadata
                )
                try upsertFingerprintEntry(updated)
                return (entry.fingerprintHash, updated)
            }
        }

        // 3. New entry.
        let now = Date().timeIntervalSince1970
        let initialState = promote(.candidate, 1, 0)
        let newEntry = FingerprintEntry(
            podcastId: podcastId,
            fingerprintHash: fingerprintHash,
            normalizedText: normalizedText,
            state: initialState,
            confirmationCount: 1,
            firstSeenAt: now,
            lastConfirmedAt: now
        )
        try upsertFingerprintEntry(newEntry)
        return (fingerprintHash, newEntry)
    }

    /// Atomically load → increment rollback → demote → upsert a fingerprint entry.
    func atomicRollbackFingerprint(
        podcastId: String,
        fingerprintHash: String,
        demote: (_ current: KnowledgeState, _ confirmations: Int, _ rollbacks: Int) -> KnowledgeState
    ) throws {
        guard let existing = try loadFingerprintEntry(podcastId: podcastId, fingerprintHash: fingerprintHash) else {
            return
        }
        let now = Date().timeIntervalSince1970
        let newRollbackCount = existing.rollbackCount + 1
        let newState = demote(existing.state, existing.confirmationCount, newRollbackCount)
        let updated = FingerprintEntry(
            id: existing.id,
            podcastId: existing.podcastId,
            fingerprintHash: existing.fingerprintHash,
            normalizedText: existing.normalizedText,
            state: newState,
            confirmationCount: existing.confirmationCount,
            rollbackCount: newRollbackCount,
            firstSeenAt: existing.firstSeenAt,
            lastConfirmedAt: existing.lastConfirmedAt,
            lastRollbackAt: now,
            decayedAt: newState == .decayed ? now : existing.decayedAt,
            blockedAt: newState == .blocked ? now : existing.blockedAt,
            metadata: existing.metadata
        )
        try upsertFingerprintEntry(updated)
    }

    // MARK: - CRUD: fingerprint_source_events (Phase 9, playhead-4my.9.1)

    /// Append a fingerprint source event (provenance log).
    func appendFingerprintSourceEvent(_ event: FingerprintSourceEvent) throws {
        let sql = """
            INSERT OR IGNORE INTO fingerprint_source_events
            (id, analysisAssetId, fingerprintHash, sourceAdWindowId, confidence, createdAt)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, event.id)
        bind(stmt, 2, event.analysisAssetId)
        bind(stmt, 3, event.fingerprintHash)
        bind(stmt, 4, event.sourceAdWindowId)
        bind(stmt, 5, event.confidence)
        bind(stmt, 6, event.createdAt)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Load all source events for a given analysis asset, ordered by createdAt.
    func loadFingerprintSourceEvents(analysisAssetId: String) throws -> [FingerprintSourceEvent] {
        let sql = """
            SELECT id, analysisAssetId, fingerprintHash, sourceAdWindowId, confidence, createdAt
            FROM fingerprint_source_events
            WHERE analysisAssetId = ?
            ORDER BY createdAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        var results: [FingerprintSourceEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            let id = text(stmt, 0)
            let assetId = text(stmt, 1)
            let fpHash = text(stmt, 2)
            let windowId = text(stmt, 3)
            let confidence = sqlite3_column_double(stmt, 4)
            let createdAt = sqlite3_column_double(stmt, 5)

            results.append(FingerprintSourceEvent(
                id: id,
                analysisAssetId: assetId,
                fingerprintHash: fpHash,
                sourceAdWindowId: windowId,
                confidence: confidence,
                createdAt: createdAt
            ))
        }
        return results
    }
}
