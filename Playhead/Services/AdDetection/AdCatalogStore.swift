// AdCatalogStore.swift
// playhead-gtt9.13: On-device catalog of confirmed ad spans keyed by
// acoustic fingerprint, lexical snippet, and sponsor tokens. Used as a
// precision signal on future episodes — if a candidate window's
// fingerprint matches a stored entry above `similarityFloor`, the
// precision gate records a gtt9.11 `catalogMatch` safety signal and the
// fusion path emits diagnostic `catalog` evidence. Catalog evidence does
// not lower the classifier threshold and cannot independently authorize
// an automatic skip.
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

/// Authoritative event that admitted a fingerprint to the catalog.
enum CatalogLearningSource: String, Sendable, Hashable, Codable {
    case consumedAutoSkip
    case manualSkip
    case userMarkedAd
    case confirmedSuggestion
    case confirmedAutoSkipBanner
    /// Migration-only sentinel. New inserts reject this source.
    case legacyUnconfirmed

    var authoritativeLifecycle: CatalogLearningLifecycle? {
        switch self {
        case .consumedAutoSkip:
            return .consumed
        case .manualSkip,
             .userMarkedAd,
             .confirmedSuggestion,
             .confirmedAutoSkipBanner:
            return .explicitConfirmation
        case .legacyUnconfirmed:
            return nil
        }
    }
}

/// Confirmation level at the time a fingerprint was learned.
enum CatalogLearningLifecycle: String, Sendable, Hashable, Codable {
    case consumed
    case explicitConfirmation
    /// Migration-only sentinel. New inserts reject this lifecycle.
    case legacyUnconfirmed
}

/// Authoritative negative event that revoked learned catalog evidence.
enum CatalogRevocationSource: String, Sendable, Hashable, Codable {
    case listenRevert
    case manualVeto
    case bannerAutoSkipDenied
    case bannerSuggestionDenied
}

/// Stable identity for one exact source-window geometry.
///
/// The revocation tables predate geometry-aware same-ID replacement and key
/// their tombstones by `(asset, window)`. Encoding the exact bounds into the
/// tombstone's window component lets new corrections fence delayed learning
/// for the material the user actually rejected without changing the public
/// source-window provenance stored on catalog/cache entries. A legacy plain
/// window ID remains a wildcard tombstone so existing corrections stay
/// conservative after upgrade.
enum RecurrenceMaterialIdentity {
    private static let prefix = "material-v1"

    static func normalizedIdentifier(_ value: String?) -> String? {
        guard let value, !value.utf8.contains(0) else { return nil }
        let normalized = value.trimmingCharacters(
            in: .whitespacesAndNewlines
        )
        return normalized.isEmpty ? nil : normalized
    }

    /// Returns an identifier only when the caller supplied its exact canonical
    /// spelling. Recurrence evidence is show- and source-scoped, so silently
    /// trimming malformed identity would retarget evidence to a different
    /// persisted namespace.
    static func canonicalIdentifier(_ value: String?) -> String? {
        guard let value, normalizedIdentifier(value) == value else {
            return nil
        }
        return value
    }

    static func tombstoneWindowKey(
        sourceWindowId: String,
        sourceStartTime: Double?,
        sourceEndTime: Double?
    ) -> String? {
        guard let sourceWindowId = canonicalIdentifier(sourceWindowId) else {
            return nil
        }
        switch (sourceStartTime, sourceEndTime) {
        case (nil, nil):
            // Pre-geometry callers and migrated rows intentionally retain the
            // old wildcard meaning.
            return sourceWindowId
        case let (start?, end?):
            guard start.isFinite,
                  end.isFinite,
                  start >= 0,
                  end > start else {
                return nil
            }
            return [
                prefix,
                String(sourceWindowId.utf8.count),
                sourceWindowId,
                String(canonicalTimeBitPattern(start), radix: 16),
                String(canonicalTimeBitPattern(end), radix: 16),
            ].joined(separator: ":")
        default:
            return nil
        }
    }

    /// Compatibility alias for the pre-canonicalization encoding of a
    /// negative-zero start. New tombstones always use `+0.0`, but lookups also
    /// consult this key so a correction written by an older build remains
    /// terminal after migration/reopen.
    static func legacyNegativeZeroTombstoneWindowKey(
        sourceWindowId: String,
        sourceStartTime: Double?,
        sourceEndTime: Double?
    ) -> String? {
        guard let sourceWindowId = canonicalIdentifier(sourceWindowId),
              let start = sourceStartTime,
              let end = sourceEndTime,
              start == 0,
              start.isFinite,
              end.isFinite,
              end > start else {
            return nil
        }
        return [
            prefix,
            String(sourceWindowId.utf8.count),
            sourceWindowId,
            String((-0.0 as Double).bitPattern, radix: 16),
            String(end.bitPattern, radix: 16),
        ].joined(separator: ":")
    }

    /// SQLite numeric equality does not distinguish `-0.0` from `+0.0`.
    /// Material keys must use the same equivalence relation or a correction
    /// can delete the row while leaving a differently-signed tombstone that
    /// permits a delayed writer to resurrect it.
    static func canonicalTimeBitPattern(_ value: Double) -> UInt64 {
        value == 0 ? Double.zero.bitPattern : value.bitPattern
    }
}

/// A single stored ad-catalog entry. Uniquely identified by `id`; current rows
/// are deduplicated by `(showId, fingerprintVersion, fingerprintBlob)`.
struct CatalogEntry: Sendable, Hashable {
    let id: UUID
    let createdAt: Date
    /// Podcast / show identifier. `nil` can exist only on preserved legacy
    /// rows; new learning fails closed without a non-blank show identifier.
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
    let learningSource: CatalogLearningSource
    let learningLifecycle: CatalogLearningLifecycle
    let sourceAssetId: String?
    let sourceWindowId: String?
    let sourceStartTime: Double?
    let sourceEndTime: Double?
    let confirmedAt: Date?
    let revokedAt: Date?
    let revocationSource: CatalogRevocationSource?
    /// True only when a persisted non-null revocation source could not be
    /// decoded. Retaining the row keeps diagnostics auditable, while admission
    /// treats the malformed provenance as terminally untrusted.
    let persistedRevocationSourceIsMalformed: Bool

    init(
        id: UUID = UUID(),
        createdAt: Date = Date(),
        showId: String?,
        episodePosition: CatalogEpisodePosition,
        durationSec: Double,
        acousticFingerprint: AcousticFingerprint,
        transcriptSnippet: String? = nil,
        sponsorTokens: [String]? = nil,
        originalConfidence: Double? = nil,
        learningSource: CatalogLearningSource,
        learningLifecycle: CatalogLearningLifecycle,
        sourceAssetId: String?,
        sourceWindowId: String?,
        sourceStartTime: Double?,
        sourceEndTime: Double?,
        confirmedAt: Date?,
        revokedAt: Date? = nil,
        revocationSource: CatalogRevocationSource? = nil,
        persistedRevocationSourceIsMalformed: Bool = false
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
        self.learningSource = learningSource
        self.learningLifecycle = learningLifecycle
        self.sourceAssetId = sourceAssetId
        self.sourceWindowId = sourceWindowId
        self.sourceStartTime = sourceStartTime
        self.sourceEndTime = sourceEndTime
        self.confirmedAt = confirmedAt
        self.revokedAt = revokedAt
        self.revocationSource = revocationSource
        self.persistedRevocationSourceIsMalformed =
            persistedRevocationSourceIsMalformed
    }
}

/// A match returned by `AdCatalogStore.matches(...)`. Similarity uses the
/// fingerprint version's metric in `[0, 1]` — higher is closer.
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
    case invalidShowIdentity
    case invalidLearningProvenance
    case incompatibleFingerprintVersion(Int)
}

// MARK: - Store

/// Actor-backed catalog store. All reads and writes are serialized on the
/// store's private executor so SQLite's single-connection model is
/// honored without an explicit mutex.
actor AdCatalogStore {

    // MARK: Constants

    /// Schema version stamped into `PRAGMA user_version`. Bumped on any
    /// schema change; `migrate()` handles forward migrations.
    /// V1 → V2: dedup + UNIQUE index on (show_id, fingerprint_blob).
    /// V2 → V3: explicit fingerprint compatibility, learning provenance,
    /// soft revocation, and race-safe source-window revocation tombstones.
    static let schemaVersion: Int32 = 3

    /// Default similarity floor used when callers don't override it.
    /// Calibrated against the committed Catalyst fixture. Under the current
    /// relative-distance semantics the maximum of 1,225 real negative
    /// cross-pairs is 0.8653, while small real boundary perturbations score
    /// 0.9155 and 0.9413.
    static let defaultSimilarityFloor: Float = 0.90

    /// Maximum active entries retained per (show_id) bucket. Active rows
    /// beyond this are evicted by `created_at ASC` after each insert.
    /// Soft-revoked rows are durable audit/tombstone material and do not
    /// compete for this capacity; evicting one could let delayed consumption
    /// relearn the corrected creative.
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
        // not `Sendable`). `db` is explicitly `nonisolated(unsafe)` for this
        // last-reference cleanup; all access before deinit remains actor
        // isolated.
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

    /// Insert an authoritatively confirmed catalog entry.
    ///
    /// New learning is admitted only with a non-blank show, a current
    /// FeatureWindow fingerprint, and complete source-span provenance. A
    /// revocation tombstone for the same source window wins races against
    /// delayed learning. Returns the persisted row (including its stable ID)
    /// or `nil` when the insert is intentionally
    /// suppressed (zero fingerprint, tombstone, or a consumed observation
    /// attempting to resurrect a revoked row).
    @discardableResult
    func insert(entry: CatalogEntry) throws -> CatalogEntry? {
        try insert(entry: entry, onlyIfNewConsumedIdentity: false)
    }

    /// Insert a delayed consumed observation only when it creates a new
    /// creative identity. An existing row already supplies the diagnostic
    /// evidence; mutating its provenance would give a canceled/stale writer
    /// an update that it cannot safely retract without deleting that older
    /// evidence.
    @discardableResult
    func insertNewConsumedLearningIfAbsent(
        _ entry: CatalogEntry
    ) throws -> CatalogEntry? {
        guard entry.learningSource == .consumedAutoSkip,
              entry.learningLifecycle == .consumed else {
            throw AdCatalogStoreError.invalidLearningProvenance
        }
        return try insert(
            entry: entry,
            onlyIfNewConsumedIdentity: true
        )
    }

    private func insert(
        entry: CatalogEntry,
        onlyIfNewConsumedIdentity: Bool
    ) throws -> CatalogEntry? {
        try ensureOpen()
        guard let db else {
            throw AdCatalogStoreError.insertFailed("database closed")
        }
        guard let normalizedShow =
                RecurrenceMaterialIdentity.canonicalIdentifier(entry.showId)
        else {
            throw AdCatalogStoreError.invalidShowIdentity
        }
        guard entry.acousticFingerprint.version == .currentCatalog else {
            throw AdCatalogStoreError.incompatibleFingerprintVersion(
                entry.acousticFingerprint.version.rawValue
            )
        }
        guard entry.learningSource.authoritativeLifecycle
                == entry.learningLifecycle,
              let sourceAssetId =
                RecurrenceMaterialIdentity.canonicalIdentifier(
                    entry.sourceAssetId
                ),
              let sourceWindowId =
                RecurrenceMaterialIdentity.canonicalIdentifier(
                    entry.sourceWindowId
                ),
              let sourceStartTime = entry.sourceStartTime,
              let sourceEndTime = entry.sourceEndTime,
              let confirmedAt = entry.confirmedAt,
              entry.revokedAt == nil,
              entry.revocationSource == nil,
              !entry.persistedRevocationSourceIsMalformed,
              entry.createdAt.timeIntervalSince1970.isFinite,
              entry.createdAt.timeIntervalSince1970 >= 0,
              entry.durationSec.isFinite,
              entry.durationSec > 0,
              entry.originalConfidence.map({ $0.isFinite && (0...1).contains($0) })
                ?? true,
              confirmedAt.timeIntervalSince1970.isFinite,
              confirmedAt.timeIntervalSince1970 >= 0,
              sourceStartTime.isFinite,
              sourceEndTime.isFinite,
              sourceStartTime >= 0,
              abs(
                  entry.durationSec - (sourceEndTime - sourceStartTime)
              ) <= 0.01,
              sourceEndTime > sourceStartTime else {
            throw AdCatalogStoreError.invalidLearningProvenance
        }
        if entry.acousticFingerprint.isZero {
            // Refuse to store fingerprints that will never match — this
            // keeps the table signal-dense and avoids polluting matches
            // with zero-similarity noise.
            logger.debug("insert: skipping zero fingerprint (id=\(entry.id, privacy: .public))")
            return nil
        }

        // A duplicate current fingerprint refreshes provenance and recency
        // without downgrading an explicit confirmation to delayed consumption.
        // A revoked fingerprint row can be rehabilitated only by an explicit
        // positive from a different, untombstoned source. The exact source
        // tombstone above remains terminal for both consumed and explicit
        // delayed writers. An ID conflict is accepted only for that same
        // show/version/fingerprint identity; a caller cannot retarget a UUID
        // that may already be referenced by persisted decision provenance.
        let sql = """
        INSERT INTO ad_catalog_entries
            (id, created_at, show_id, episode_position, duration_sec,
             fingerprint_blob, transcript_snippet, sponsor_tokens_json,
             original_confidence, fingerprint_version, learning_source,
             learning_lifecycle, source_asset_id, source_window_id,
             source_start_time, source_end_time, confirmed_at,
             revoked_at, revocation_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
        ON CONFLICT(id) DO UPDATE SET
            created_at = MAX(ad_catalog_entries.created_at, excluded.created_at),
            show_id = excluded.show_id,
            episode_position = excluded.episode_position,
            duration_sec = excluded.duration_sec,
            fingerprint_blob = excluded.fingerprint_blob,
            fingerprint_version = excluded.fingerprint_version,
            transcript_snippet = excluded.transcript_snippet,
            sponsor_tokens_json = excluded.sponsor_tokens_json,
            learning_source = excluded.learning_source,
            learning_lifecycle = excluded.learning_lifecycle,
            source_asset_id = excluded.source_asset_id,
            source_window_id = excluded.source_window_id,
            source_start_time = excluded.source_start_time,
            source_end_time = excluded.source_end_time,
            confirmed_at = MAX(
                ad_catalog_entries.confirmed_at,
                excluded.confirmed_at
            ),
            revoked_at = CASE
                WHEN excluded.learning_lifecycle = 'explicitConfirmation' THEN NULL
                ELSE revoked_at
            END,
            revocation_source = CASE
                WHEN excluded.learning_lifecycle = 'explicitConfirmation' THEN NULL
                ELSE revocation_source
            END,
            original_confidence = CASE
                WHEN original_confidence IS NULL AND excluded.original_confidence IS NULL THEN NULL
                WHEN original_confidence IS NULL THEN excluded.original_confidence
                WHEN excluded.original_confidence IS NULL THEN original_confidence
                ELSE MAX(original_confidence, excluded.original_confidence)
            END
        WHERE ad_catalog_entries.show_id = excluded.show_id
          AND ad_catalog_entries.fingerprint_version
                = excluded.fingerprint_version
          AND ad_catalog_entries.fingerprint_blob
                = excluded.fingerprint_blob
          AND (
                (
                    ad_catalog_entries.revoked_at IS NULL
                    AND (
                        CASE ad_catalog_entries.learning_lifecycle
                            WHEN 'explicitConfirmation' THEN 2
                            WHEN 'consumed' THEN 1
                            ELSE 0
                        END
                        <
                        CASE excluded.learning_lifecycle
                            WHEN 'explicitConfirmation' THEN 2
                            WHEN 'consumed' THEN 1
                            ELSE 0
                        END
                        OR (
                            ad_catalog_entries.learning_lifecycle
                                = excluded.learning_lifecycle
                            AND excluded.confirmed_at
                                >= ad_catalog_entries.confirmed_at
                        )
                    )
                )
                OR (
                    ad_catalog_entries.revoked_at IS NOT NULL
                    AND excluded.learning_lifecycle
                        = 'explicitConfirmation'
                    AND excluded.confirmed_at
                        > ad_catalog_entries.revoked_at
                )
              )
        ON CONFLICT(show_id, fingerprint_version, fingerprint_blob)
            WHERE show_id IS NOT NULL DO UPDATE SET
            created_at = MAX(ad_catalog_entries.created_at, excluded.created_at),
            episode_position = excluded.episode_position,
            duration_sec = excluded.duration_sec,
            transcript_snippet = excluded.transcript_snippet,
            sponsor_tokens_json = excluded.sponsor_tokens_json,
            learning_source = excluded.learning_source,
            learning_lifecycle = excluded.learning_lifecycle,
            source_asset_id = excluded.source_asset_id,
            source_window_id = excluded.source_window_id,
            source_start_time = excluded.source_start_time,
            source_end_time = excluded.source_end_time,
            confirmed_at = MAX(
                ad_catalog_entries.confirmed_at,
                excluded.confirmed_at
            ),
            revoked_at = NULL,
            revocation_source = NULL,
            original_confidence = CASE
                WHEN original_confidence IS NULL AND excluded.original_confidence IS NULL THEN NULL
                WHEN original_confidence IS NULL THEN excluded.original_confidence
                WHEN excluded.original_confidence IS NULL THEN original_confidence
                ELSE MAX(original_confidence, excluded.original_confidence)
            END
        WHERE (
                (
                    ad_catalog_entries.revoked_at IS NULL
                    AND (
                        CASE ad_catalog_entries.learning_lifecycle
                            WHEN 'explicitConfirmation' THEN 2
                            WHEN 'consumed' THEN 1
                            ELSE 0
                        END
                        <
                        CASE excluded.learning_lifecycle
                            WHEN 'explicitConfirmation' THEN 2
                            WHEN 'consumed' THEN 1
                            ELSE 0
                        END
                        OR (
                            ad_catalog_entries.learning_lifecycle
                                = excluded.learning_lifecycle
                            AND excluded.confirmed_at
                                >= ad_catalog_entries.confirmed_at
                        )
                    )
                )
                OR (
                    ad_catalog_entries.revoked_at IS NOT NULL
                    AND excluded.learning_lifecycle
                        = 'explicitConfirmation'
                    AND excluded.confirmed_at
                        > ad_catalog_entries.revoked_at
                )
              )
        """

        try Self.exec(db, "BEGIN IMMEDIATE")
        do {
            // Check the exact-source tombstone under the same write lock as
            // the UPSERT. A second store connection cannot revoke this source
            // in the gap between the check and durable learning.
            if try revocationTombstone(
                sourceAssetId: sourceAssetId,
                sourceWindowId: sourceWindowId,
                sourceStartTime: sourceStartTime,
                sourceEndTime: sourceEndTime
            ) != nil {
                try Self.exec(db, "COMMIT")
                logger.info(
                    "insert: suppressed revoked source window \(sourceWindowId, privacy: .public)"
                )
                return nil
            }

            if onlyIfNewConsumedIdentity,
               try activeEntry(
                   showId: normalizedShow,
                   fingerprint: entry.acousticFingerprint
               ) != nil {
                try Self.exec(db, "COMMIT")
                return nil
            }

            var stmt: OpaquePointer?
            guard sqlite3_prepare_v2(
                db,
                sql,
                -1,
                &stmt,
                nil
            ) == SQLITE_OK else {
                throw AdCatalogStoreError.insertFailed(
                    String(cString: sqlite3_errmsg(db))
                )
            }
            defer { sqlite3_finalize(stmt) }

            sqlite3_bind_text(
                stmt,
                1,
                entry.id.uuidString,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_double(
                stmt,
                2,
                entry.createdAt.timeIntervalSince1970
            )
            sqlite3_bind_text(
                stmt,
                3,
                normalizedShow,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_text(
                stmt,
                4,
                entry.episodePosition.rawValue,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_double(stmt, 5, entry.durationSec)

            let blob = entry.acousticFingerprint.data
            blob.withUnsafeBytes { raw -> Void in
                sqlite3_bind_blob(
                    stmt,
                    6,
                    raw.baseAddress,
                    Int32(blob.count),
                    Self.SQLITE_TRANSIENT
                )
            }

            if let snippet = entry.transcriptSnippet {
                snippet.withCString { bytes in
                    _ = sqlite3_bind_text64(
                        stmt,
                        7,
                        bytes,
                        UInt64(snippet.utf8.count),
                        Self.SQLITE_TRANSIENT,
                        UInt8(SQLITE_UTF8)
                    )
                }
            } else {
                sqlite3_bind_null(stmt, 7)
            }

            if let tokens = entry.sponsorTokens,
               let tokensData = try? JSONEncoder().encode(tokens),
               let tokensJSON = String(data: tokensData, encoding: .utf8) {
                sqlite3_bind_text(
                    stmt,
                    8,
                    tokensJSON,
                    -1,
                    Self.SQLITE_TRANSIENT
                )
            } else {
                sqlite3_bind_null(stmt, 8)
            }

            if let conf = entry.originalConfidence {
                sqlite3_bind_double(stmt, 9, conf)
            } else {
                sqlite3_bind_null(stmt, 9)
            }
            sqlite3_bind_int(
                stmt,
                10,
                Int32(entry.acousticFingerprint.version.rawValue)
            )
            sqlite3_bind_text(
                stmt,
                11,
                entry.learningSource.rawValue,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_text(
                stmt,
                12,
                entry.learningLifecycle.rawValue,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_text(
                stmt,
                13,
                sourceAssetId,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_text(
                stmt,
                14,
                sourceWindowId,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_double(stmt, 15, sourceStartTime)
            sqlite3_bind_double(stmt, 16, sourceEndTime)
            sqlite3_bind_double(
                stmt,
                17,
                confirmedAt.timeIntervalSince1970
            )

            guard sqlite3_step(stmt) == SQLITE_DONE else {
                throw AdCatalogStoreError.insertFailed(
                    String(cString: sqlite3_errmsg(db))
                )
            }

            guard sqlite3_changes(db) > 0 else {
                try Self.exec(db, "COMMIT")
                return nil
            }
            try evictOldestForShowIfNeeded(showId: normalizedShow)
            let persisted = try activeEntry(
                showId: normalizedShow,
                fingerprint: entry.acousticFingerprint
            )
            try Self.exec(db, "COMMIT")
            return persisted
        } catch {
            try? Self.exec(db, "ROLLBACK")
            throw error
        }
    }

    /// Cap the per-show active row count at `maxEntriesPerShow` by deleting
    /// the oldest non-revoked entries for that show. Revoked rows remain as
    /// persistent creative tombstones and audit history.
    private func evictOldestForShowIfNeeded(showId: String?) throws {
        guard let db else { return }
        let countSQL: String
        if showId != nil {
            countSQL = """
            SELECT COUNT(*) FROM ad_catalog_entries
            WHERE show_id = ? AND revoked_at IS NULL
            """
        } else {
            countSQL = """
            SELECT COUNT(*) FROM ad_catalog_entries
            WHERE show_id IS NULL AND revoked_at IS NULL
            """
        }
        var countStmt: OpaquePointer?
        guard sqlite3_prepare_v2(
            db,
            countSQL,
            -1,
            &countStmt,
            nil
        ) == SQLITE_OK else {
            throw AdCatalogStoreError.queryFailed(
                String(cString: sqlite3_errmsg(db))
            )
        }
        defer { sqlite3_finalize(countStmt) }
        if let showId {
            sqlite3_bind_text(countStmt, 1, showId, -1, Self.SQLITE_TRANSIENT)
        }
        guard sqlite3_step(countStmt) == SQLITE_ROW else {
            throw AdCatalogStoreError.queryFailed(
                String(cString: sqlite3_errmsg(db))
            )
        }
        let total = Int(sqlite3_column_int(countStmt, 0))
        guard total > Self.maxEntriesPerShow else { return }
        let toEvict = total - Self.maxEntriesPerShow

        let deleteSQL: String
        if showId != nil {
            deleteSQL = """
            DELETE FROM ad_catalog_entries
            WHERE id IN (
                SELECT id FROM ad_catalog_entries
                WHERE show_id = ? AND revoked_at IS NULL
                ORDER BY created_at ASC, id ASC
                LIMIT \(toEvict)
            )
            """
        } else {
            deleteSQL = """
            DELETE FROM ad_catalog_entries
            WHERE id IN (
                SELECT id FROM ad_catalog_entries
                WHERE show_id IS NULL AND revoked_at IS NULL
                ORDER BY created_at ASC, id ASC
                LIMIT \(toEvict)
            )
            """
        }
        var delStmt: OpaquePointer?
        guard sqlite3_prepare_v2(
            db,
            deleteSQL,
            -1,
            &delStmt,
            nil
        ) == SQLITE_OK else {
            throw AdCatalogStoreError.queryFailed(
                String(cString: sqlite3_errmsg(db))
            )
        }
        defer { sqlite3_finalize(delStmt) }
        if let showId {
            sqlite3_bind_text(delStmt, 1, showId, -1, Self.SQLITE_TRANSIENT)
        }
        guard sqlite3_step(delStmt) == SQLITE_DONE else {
            throw AdCatalogStoreError.queryFailed(
                String(cString: sqlite3_errmsg(db))
            )
        }
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
        originalConfidence: Double? = nil,
        learningSource: CatalogLearningSource,
        learningLifecycle: CatalogLearningLifecycle,
        sourceAssetId: String,
        sourceWindowId: String,
        sourceStartTime: Double,
        sourceEndTime: Double,
        confirmedAt: Date = Date()
    ) throws -> CatalogEntry? {
        let entry = CatalogEntry(
            showId: showId,
            episodePosition: episodePosition,
            durationSec: durationSec,
            acousticFingerprint: acousticFingerprint,
            transcriptSnippet: transcriptSnippet,
            sponsorTokens: sponsorTokens,
            originalConfidence: originalConfidence,
            learningSource: learningSource,
            learningLifecycle: learningLifecycle,
            sourceAssetId: sourceAssetId,
            sourceWindowId: sourceWindowId,
            sourceStartTime: sourceStartTime,
            sourceEndTime: sourceEndTime,
            confirmedAt: confirmedAt
        )
        return try insert(entry: entry)
    }

    /// Retract a canceled delayed-consumption write only if this exact source
    /// revision still owns the active row. Explicit upgrades, corrections, and
    /// older duplicate observations are never removed.
    @discardableResult
    func deleteConsumedLearningIfCurrent(
        _ entry: CatalogEntry
    ) throws -> Bool {
        try ensureOpen()
        guard let db else {
            throw AdCatalogStoreError.queryFailed("database closed")
        }
        guard entry.learningLifecycle == .consumed,
              entry.learningSource.authoritativeLifecycle == .consumed,
              let showId = RecurrenceMaterialIdentity.canonicalIdentifier(
                  entry.showId
              ),
              let sourceAssetId =
                RecurrenceMaterialIdentity.canonicalIdentifier(
                  entry.sourceAssetId
                ),
              let sourceWindowId =
                RecurrenceMaterialIdentity.canonicalIdentifier(
                  entry.sourceWindowId
                ),
              let sourceStart = entry.sourceStartTime,
              let sourceEnd = entry.sourceEndTime,
              let confirmedAt = entry.confirmedAt else {
            return false
        }
        let sql = """
        DELETE FROM ad_catalog_entries
        WHERE id = ?
          AND show_id = ?
          AND fingerprint_version = ?
          AND fingerprint_blob = ?
          AND learning_source = ?
          AND learning_lifecycle = ?
          AND source_asset_id = ?
          AND source_window_id = ?
          AND source_start_time = ?
          AND source_end_time = ?
          AND confirmed_at = ?
          AND revoked_at IS NULL
        """
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw AdCatalogStoreError.queryFailed(
                String(cString: sqlite3_errmsg(db))
            )
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(
            stmt,
            1,
            entry.id.uuidString,
            -1,
            Self.SQLITE_TRANSIENT
        )
        sqlite3_bind_text(stmt, 2, showId, -1, Self.SQLITE_TRANSIENT)
        sqlite3_bind_int(
            stmt,
            3,
            Int32(entry.acousticFingerprint.version.rawValue)
        )
        let blob = entry.acousticFingerprint.data
        blob.withUnsafeBytes { raw -> Void in
            sqlite3_bind_blob(
                stmt,
                4,
                raw.baseAddress,
                Int32(blob.count),
                Self.SQLITE_TRANSIENT
            )
        }
        sqlite3_bind_text(
            stmt,
            5,
            entry.learningSource.rawValue,
            -1,
            Self.SQLITE_TRANSIENT
        )
        sqlite3_bind_text(
            stmt,
            6,
            entry.learningLifecycle.rawValue,
            -1,
            Self.SQLITE_TRANSIENT
        )
        sqlite3_bind_text(
            stmt,
            7,
            sourceAssetId,
            -1,
            Self.SQLITE_TRANSIENT
        )
        sqlite3_bind_text(
            stmt,
            8,
            sourceWindowId,
            -1,
            Self.SQLITE_TRANSIENT
        )
        sqlite3_bind_double(stmt, 9, sourceStart)
        sqlite3_bind_double(stmt, 10, sourceEnd)
        sqlite3_bind_double(
            stmt,
            11,
            confirmedAt.timeIntervalSince1970
        )
        guard sqlite3_step(stmt) == SQLITE_DONE else {
            throw AdCatalogStoreError.queryFailed(
                String(cString: sqlite3_errmsg(db))
            )
        }
        return sqlite3_changes(db) > 0
    }

    /// Soft-revoke catalog evidence after an authoritative false-positive
    /// correction. When the corrected fingerprint and show are available,
    /// every compatible row above the admission floor is revoked, not only the
    /// recorded top row; otherwise `matchedEntryId` is the exact-row fallback.
    /// The source-window tombstone is written in the same transaction as row
    /// revocation so a delayed consumed-skip task cannot reinsert the vetoed
    /// span after this method returns.
    @discardableResult
    func revoke(
        matchedEntryId: UUID?,
        sourceAssetId: String,
        sourceWindowId: String,
        sourceStartTime: Double? = nil,
        sourceEndTime: Double? = nil,
        source: CatalogRevocationSource,
        matchingFingerprint: AcousticFingerprint? = nil,
        showId: String? = nil,
        at revokedAt: Date = Date()
    ) throws -> Int {
        try ensureOpen()
        guard let db else {
            throw AdCatalogStoreError.queryFailed("database closed")
        }
        guard let normalizedAsset =
                RecurrenceMaterialIdentity.canonicalIdentifier(sourceAssetId),
              let normalizedWindow =
                RecurrenceMaterialIdentity.canonicalIdentifier(sourceWindowId),
              let tombstoneWindowKey =
                RecurrenceMaterialIdentity.tombstoneWindowKey(
                    sourceWindowId: normalizedWindow,
                    sourceStartTime: sourceStartTime,
                    sourceEndTime: sourceEndTime
                ),
              revokedAt.timeIntervalSince1970.isFinite,
              revokedAt.timeIntervalSince1970 >= 0
        else {
            throw AdCatalogStoreError.invalidLearningProvenance
        }

        let normalizedShow =
            RecurrenceMaterialIdentity.canonicalIdentifier(showId)

        try Self.exec(db, "BEGIN IMMEDIATE")
        do {
            // Resolve every row target under the same write snapshot as the
            // tombstone/update. A private row UUID is not globally scoped: it
            // is trusted only when the caller also supplies the canonical show
            // and that exact active row belongs to it. Missing, blank, stale,
            // malformed, or mismatched show identity therefore fails closed
            // while exact source-window revocation below still proceeds.
            var matchingEntryIds = Set<UUID>()
            if let matchedEntryId,
               let normalizedShow,
               let exact = try activeEntry(
                   id: matchedEntryId,
                   showId: normalizedShow
               ),
               Self.isTrustworthyActiveEntry(
                   exact,
                   expectedShowId: normalizedShow
               ) {
                matchingEntryIds.insert(matchedEntryId)
            }
            if let matchingFingerprint,
               let normalizedShow,
               matchingFingerprint.version == .currentCatalog,
               !matchingFingerprint.isZero {
                matchingEntryIds.formUnion(
                    try compatibleMatches(
                        fingerprint: matchingFingerprint,
                        normalizedShow: normalizedShow,
                        similarityFloor: Self.defaultSimilarityFloor
                    ).map(\.entry.id)
                )
            }
            let orderedEntryIds = matchingEntryIds
                .map(\.uuidString)
                .sorted()

            let tombstoneSQL = """
            INSERT INTO ad_catalog_revocations
                (source_asset_id, source_window_id, revoked_at, revocation_source)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_asset_id, source_window_id) DO UPDATE SET
                revoked_at = excluded.revoked_at,
                revocation_source = excluded.revocation_source
            WHERE excluded.revoked_at >= ad_catalog_revocations.revoked_at
            """
            var tombstone: OpaquePointer?
            guard sqlite3_prepare_v2(
                db,
                tombstoneSQL,
                -1,
                &tombstone,
                nil
            ) == SQLITE_OK else {
                throw AdCatalogStoreError.insertFailed(
                    String(cString: sqlite3_errmsg(db))
                )
            }
            defer { sqlite3_finalize(tombstone) }
            sqlite3_bind_text(
                tombstone,
                1,
                normalizedAsset,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_text(
                tombstone,
                2,
                tombstoneWindowKey,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_double(
                tombstone,
                3,
                revokedAt.timeIntervalSince1970
            )
            sqlite3_bind_text(
                tombstone,
                4,
                source.rawValue,
                -1,
                Self.SQLITE_TRANSIENT
            )
            guard sqlite3_step(tombstone) == SQLITE_DONE else {
                throw AdCatalogStoreError.insertFailed(
                    String(cString: sqlite3_errmsg(db))
                )
            }
            guard let effectiveTombstone = try revocationTombstone(
                sourceAssetId: normalizedAsset,
                sourceWindowId: normalizedWindow,
                sourceStartTime: sourceStartTime,
                sourceEndTime: sourceEndTime
            ) else {
                throw AdCatalogStoreError.queryFailed(
                    "revocation tombstone missing after upsert"
                )
            }

            let entryPredicate: String
            if orderedEntryIds.isEmpty {
                entryPredicate = "0"
            } else {
                entryPredicate = "id IN (\(orderedEntryIds.map { _ in "?" }.joined(separator: ", ")))"
            }
            let exactSourcePredicate: String
            if sourceStartTime != nil {
                exactSourcePredicate = """
                (
                    source_asset_id = ?
                    AND source_window_id = ?
                    AND source_start_time = ?
                    AND source_end_time = ?
                )
                """
            } else {
                exactSourcePredicate =
                    "(source_asset_id = ? AND source_window_id = ?)"
            }
            let updateSQL = """
            UPDATE ad_catalog_entries
            SET revoked_at = ?, revocation_source = ?
            WHERE revoked_at IS NULL
              AND (
                  (
                      (
                          confirmed_at IS NULL
                          OR confirmed_at <= ?
                      )
                      AND \(entryPredicate)
                  )
                  OR
                  \(exactSourcePredicate)
              )
            """
            var update: OpaquePointer?
            guard sqlite3_prepare_v2(
                db,
                updateSQL,
                -1,
                &update,
                nil
            ) == SQLITE_OK else {
                throw AdCatalogStoreError.queryFailed(
                    String(cString: sqlite3_errmsg(db))
                )
            }
            defer { sqlite3_finalize(update) }
            sqlite3_bind_double(
                update,
                1,
                effectiveTombstone.revokedAt.timeIntervalSince1970
            )
            sqlite3_bind_text(
                update,
                2,
                effectiveTombstone.source.rawValue,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_double(
                update,
                3,
                effectiveTombstone.revokedAt.timeIntervalSince1970
            )
            for (offset, entryId) in orderedEntryIds.enumerated() {
                sqlite3_bind_text(
                    update,
                    Int32(4 + offset),
                    entryId,
                    -1,
                    Self.SQLITE_TRANSIENT
                )
            }
            let exactSourceBinding = Int32(4 + orderedEntryIds.count)
            sqlite3_bind_text(
                update,
                exactSourceBinding,
                normalizedAsset,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_text(
                update,
                exactSourceBinding + 1,
                normalizedWindow,
                -1,
                Self.SQLITE_TRANSIENT
            )
            if let sourceStartTime, let sourceEndTime {
                sqlite3_bind_double(
                    update,
                    exactSourceBinding + 2,
                    sourceStartTime
                )
                sqlite3_bind_double(
                    update,
                    exactSourceBinding + 3,
                    sourceEndTime
                )
            }
            guard sqlite3_step(update) == SQLITE_DONE else {
                throw AdCatalogStoreError.queryFailed(
                    String(cString: sqlite3_errmsg(db))
                )
            }
            let revokedCount = Int(sqlite3_changes(db))
            try Self.exec(db, "COMMIT")
            return revokedCount
        } catch {
            try? Self.exec(db, "ROLLBACK")
            throw error
        }
    }

    private func revocationTombstone(
        sourceAssetId: String,
        sourceWindowId: String,
        sourceStartTime: Double?,
        sourceEndTime: Double?
    ) throws -> (revokedAt: Date, source: CatalogRevocationSource)? {
        guard let db else {
            throw AdCatalogStoreError.queryFailed("database closed")
        }
        guard let exactKey = RecurrenceMaterialIdentity.tombstoneWindowKey(
            sourceWindowId: sourceWindowId,
            sourceStartTime: sourceStartTime,
            sourceEndTime: sourceEndTime
        ), let legacyKey = RecurrenceMaterialIdentity.canonicalIdentifier(
            sourceWindowId
        ) else {
            throw AdCatalogStoreError.invalidLearningProvenance
        }
        let signedZeroLegacyKey =
            RecurrenceMaterialIdentity
            .legacyNegativeZeroTombstoneWindowKey(
                sourceWindowId: sourceWindowId,
                sourceStartTime: sourceStartTime,
                sourceEndTime: sourceEndTime
            )
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(
            db,
            """
            SELECT revoked_at, revocation_source
            FROM ad_catalog_revocations
            WHERE source_asset_id = ?
              AND source_window_id IN (?, ?, ?)
            ORDER BY revoked_at DESC, source_window_id ASC
            LIMIT 1
            """,
            -1,
            &stmt,
            nil
        ) == SQLITE_OK else {
            throw AdCatalogStoreError.queryFailed(
                String(cString: sqlite3_errmsg(db))
            )
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(
            stmt,
            1,
            sourceAssetId,
            -1,
            Self.SQLITE_TRANSIENT
        )
        sqlite3_bind_text(
            stmt,
            2,
            exactKey,
            -1,
            Self.SQLITE_TRANSIENT
        )
        if let signedZeroLegacyKey {
            sqlite3_bind_text(
                stmt,
                3,
                signedZeroLegacyKey,
                -1,
                Self.SQLITE_TRANSIENT
            )
        } else {
            sqlite3_bind_null(stmt, 3)
        }
        sqlite3_bind_text(
            stmt,
            4,
            legacyKey,
            -1,
            Self.SQLITE_TRANSIENT
        )
        let stepResult = sqlite3_step(stmt)
        if stepResult == SQLITE_DONE {
            return nil
        }
        guard stepResult == SQLITE_ROW else {
            throw AdCatalogStoreError.queryFailed(
                String(cString: sqlite3_errmsg(db))
            )
        }
        let timestamp = sqlite3_column_double(stmt, 0)
        guard timestamp.isFinite,
              timestamp >= 0,
              let rawSource = Self.decodedText(stmt, 1),
              let source = CatalogRevocationSource(
                  rawValue: rawSource
              ) else {
            throw AdCatalogStoreError.queryFailed(
                "invalid revocation tombstone"
            )
        }
        return (Date(timeIntervalSince1970: timestamp), source)
    }

    /// Return all catalog entries (diagnostic / test use). Active rows are
    /// capped per show; revoked audit/tombstone rows are intentionally retained.
    func allEntries() throws -> [CatalogEntry] {
        try ensureOpen()
        guard db != nil else { throw AdCatalogStoreError.queryFailed("database closed") }
        let sql = """
        SELECT id, created_at, show_id, episode_position, duration_sec,
               fingerprint_blob, transcript_snippet, sponsor_tokens_json,
               original_confidence, fingerprint_version, learning_source,
               learning_lifecycle, source_asset_id, source_window_id,
               source_start_time, source_end_time, confirmed_at,
               revoked_at, revocation_source
        FROM ad_catalog_entries
        ORDER BY created_at DESC, id ASC
        """
        return try loadEntries(sql: sql, bind: nil)
    }

    /// Find compatible, non-revoked matches for one exact show.
    ///
    /// Missing/blank show identity fails closed. Legacy/null-show rows remain
    /// queryable through `allEntries()` for audit but can never participate in
    /// production admission.
    func matches(
        fingerprint: AcousticFingerprint,
        show: String?,
        similarityFloor: Float = AdCatalogStore.defaultSimilarityFloor
    ) -> [CatalogMatch] {
        matchesIfAvailable(
            fingerprint: fingerprint,
            show: show,
            similarityFloor: similarityFloor
        ) ?? []
    }

    /// Return exact-show matches when the catalog could be evaluated.
    ///
    /// `nil` means the query was unavailable or invalid (including a missing
    /// canonical show, incompatible fingerprint, or SQLite failure). An empty
    /// array means a valid query completed and found no admitted match. This
    /// distinction prevents diagnostics and sharing from treating fail-closed
    /// catalog paths as observed negative evidence.
    func matchesIfAvailable(
        fingerprint: AcousticFingerprint,
        show: String?,
        similarityFloor: Float = AdCatalogStore.defaultSimilarityFloor
    ) -> [CatalogMatch]? {
        // playhead-jndk: lazy first-use bootstrap. Failures here are
        // logged and swallowed so the hot-path detector keeps running
        // without catalog signal — same defensive posture the prior
        // `guard db != nil` had.
        do {
            try ensureOpen()
        } catch {
            logger.error("matches: ensureOpen failed: \(String(describing: error), privacy: .public)")
            return nil
        }
        guard db != nil,
              let normalizedShow =
                RecurrenceMaterialIdentity.canonicalIdentifier(show),
              fingerprint.version == .currentCatalog,
              !fingerprint.isZero,
              similarityFloor.isFinite,
              (0...1).contains(similarityFloor) else {
            return nil
        }

        do {
            return try compatibleMatches(
                fingerprint: fingerprint,
                normalizedShow: normalizedShow,
                similarityFloor: similarityFloor
            )
        } catch {
            logger.error("matches: query failed: \(String(describing: error), privacy: .public)")
            return nil
        }
    }

    /// Validate that persisted match provenance still names the exact active,
    /// trustworthy row and that the current material still matches it above
    /// the calibrated floor. Automatic admission calls this immediately before
    /// installing a cue so a correction, geometry rewrite, reset, malformed
    /// database, or lifecycle upgrade that happened after detection fails
    /// closed.
    func isActiveMatch(
        id: UUID,
        showId: String,
        fingerprintVersion: CatalogFingerprintVersion,
        learningSource: CatalogLearningSource,
        learningLifecycle: CatalogLearningLifecycle,
        candidateFingerprint: AcousticFingerprint
    ) -> Bool {
        do {
            try ensureOpen()
            guard db != nil,
                  let normalizedShow = Self.normalizedIdentifier(showId),
                  normalizedShow == showId,
                  candidateFingerprint.version == fingerprintVersion,
                  !candidateFingerprint.isZero,
                  let entry = try activeEntry(
                      id: id,
                      showId: normalizedShow
                  ),
                  Self.isTrustworthyActiveEntry(
                      entry,
                      expectedShowId: normalizedShow
                  ),
                  let sourceAssetId = entry.sourceAssetId,
                  let sourceWindowId = entry.sourceWindowId,
                  entry.acousticFingerprint.version == fingerprintVersion,
                  entry.learningSource == learningSource,
                  entry.learningLifecycle == learningLifecycle,
                  try revocationTombstone(
                      sourceAssetId: sourceAssetId,
                      sourceWindowId: sourceWindowId,
                      sourceStartTime: entry.sourceStartTime,
                      sourceEndTime: entry.sourceEndTime
                  ) == nil else {
                return false
            }
            let similarity = AcousticFingerprint.similarity(
                candidateFingerprint,
                entry.acousticFingerprint
            )
            return similarity.isFinite
                && similarity >= Self.defaultSimilarityFloor
                && similarity <= 1
        } catch {
            logger.error(
                "isActiveMatch: validation failed: \(String(describing: error), privacy: .public)"
            )
            return false
        }
    }

    /// Throwing exact-show matcher used by mutation paths that must not turn a
    /// SQLite row-step error into an empty, partially applied revocation set.
    private func compatibleMatches(
        fingerprint: AcousticFingerprint,
        normalizedShow: String,
        similarityFloor: Float
    ) throws -> [CatalogMatch] {
        // We scan one exact show. For small catalogs a LIMIT + in-memory
        // similarity pass is cheaper than trying to build an ANN index.
        let sql = """
        SELECT id, created_at, show_id, episode_position, duration_sec,
               fingerprint_blob, transcript_snippet, sponsor_tokens_json,
               original_confidence, fingerprint_version, learning_source,
               learning_lifecycle, source_asset_id, source_window_id,
               source_start_time, source_end_time, confirmed_at,
               revoked_at, revocation_source
        FROM ad_catalog_entries
        WHERE show_id = ?
          AND fingerprint_version = ?
          AND revoked_at IS NULL
        ORDER BY created_at DESC, id ASC
        """

        let entries = try loadEntries(sql: sql, bind: { stmt in
            sqlite3_bind_text(
                stmt,
                1,
                normalizedShow,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_int(
                stmt,
                2,
                Int32(fingerprint.version.rawValue)
            )
        })

        var results: [CatalogMatch] = []
        results.reserveCapacity(entries.count)
        for e in entries {
            guard Self.isTrustworthyActiveEntry(
                e,
                expectedShowId: normalizedShow
            ) else {
                continue
            }
            let s = AcousticFingerprint.similarity(fingerprint, e.acousticFingerprint)
            if s.isFinite, (0...1).contains(s), s >= similarityFloor {
                guard let sourceAssetId = e.sourceAssetId,
                      let sourceWindowId = e.sourceWindowId else {
                    continue
                }
                if try revocationTombstone(
                    sourceAssetId: sourceAssetId,
                    sourceWindowId: sourceWindowId,
                    sourceStartTime: e.sourceStartTime,
                    sourceEndTime: e.sourceEndTime
                ) != nil {
                    continue
                }
                results.append(CatalogMatch(entry: e, similarity: s))
            }
        }
        results.sort {
            if $0.similarity != $1.similarity {
                return $0.similarity > $1.similarity
            }
            if $0.entry.createdAt != $1.entry.createdAt {
                return $0.entry.createdAt > $1.entry.createdAt
            }
            return $0.entry.id.uuidString < $1.entry.id.uuidString
        }
        return results
    }

    private static func isTrustworthyActiveEntry(
        _ entry: CatalogEntry,
        expectedShowId: String
    ) -> Bool {
        guard entry.showId == expectedShowId,
              normalizedIdentifier(entry.showId) == entry.showId,
              entry.createdAt.timeIntervalSince1970.isFinite,
              entry.createdAt.timeIntervalSince1970 >= 0,
              entry.durationSec.isFinite,
              entry.durationSec > 0,
              entry.acousticFingerprint.version == .currentCatalog,
              !entry.acousticFingerprint.isZero,
              entry.originalConfidence.map({
                  $0.isFinite && (0...1).contains($0)
              }) ?? true,
              entry.learningSource.authoritativeLifecycle
                == entry.learningLifecycle,
              let sourceAssetId = entry.sourceAssetId,
              normalizedIdentifier(sourceAssetId) == sourceAssetId,
              let sourceWindowId = entry.sourceWindowId,
              normalizedIdentifier(sourceWindowId) == sourceWindowId,
              let sourceStart = entry.sourceStartTime,
              let sourceEnd = entry.sourceEndTime,
              sourceStart.isFinite,
              sourceEnd.isFinite,
              sourceStart >= 0,
              sourceEnd > sourceStart,
              abs(entry.durationSec - (sourceEnd - sourceStart)) <= 0.01,
              let confirmedAt = entry.confirmedAt,
              confirmedAt.timeIntervalSince1970.isFinite,
              confirmedAt.timeIntervalSince1970 >= 0,
              entry.revokedAt == nil,
              entry.revocationSource == nil,
              !entry.persistedRevocationSourceIsMalformed else {
            return false
        }
        return true
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
        guard sqlite3_step(stmt) == SQLITE_ROW else {
            throw AdCatalogStoreError.queryFailed(
                String(cString: sqlite3_errmsg(db))
            )
        }
        return Int(sqlite3_column_int(stmt, 0))
    }

    /// Delete all entries. Test helper (exposed intentionally so the
    /// production app can also offer a "reset catalog" debug action).
    func clear() throws {
        try ensureOpen()
        guard let db else { throw AdCatalogStoreError.queryFailed("database closed") }
        try Self.exec(
            db,
            """
            DELETE FROM ad_catalog_entries;
            DELETE FROM ad_catalog_revocations;
            """
        )
    }

    // MARK: - Private: load helper

    /// Resolve the row that actually survived an UPSERT. The candidate UUID
    /// is not persisted when `(show, version, blob)` collides, so callers must
    /// receive this stable row identity for trustworthy match provenance.
    private func activeEntry(
        showId: String,
        fingerprint: AcousticFingerprint
    ) throws -> CatalogEntry? {
        let sql = """
        SELECT id, created_at, show_id, episode_position, duration_sec,
               fingerprint_blob, transcript_snippet, sponsor_tokens_json,
               original_confidence, fingerprint_version, learning_source,
               learning_lifecycle, source_asset_id, source_window_id,
               source_start_time, source_end_time, confirmed_at,
               revoked_at, revocation_source
        FROM ad_catalog_entries
        WHERE show_id = ?
          AND fingerprint_version = ?
          AND fingerprint_blob = ?
          AND revoked_at IS NULL
        LIMIT 1
        """
        let blob = fingerprint.data
        return try loadEntries(sql: sql, bind: { stmt in
            sqlite3_bind_text(
                stmt,
                1,
                showId,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_int(
                stmt,
                2,
                Int32(fingerprint.version.rawValue)
            )
            blob.withUnsafeBytes { raw -> Void in
                sqlite3_bind_blob(
                    stmt,
                    3,
                    raw.baseAddress,
                    Int32(blob.count),
                    Self.SQLITE_TRANSIENT
                )
            }
        }).first
    }

    private func activeEntry(
        id: UUID,
        showId: String
    ) throws -> CatalogEntry? {
        let sql = """
        SELECT id, created_at, show_id, episode_position, duration_sec,
               fingerprint_blob, transcript_snippet, sponsor_tokens_json,
               original_confidence, fingerprint_version, learning_source,
               learning_lifecycle, source_asset_id, source_window_id,
               source_start_time, source_end_time, confirmed_at,
               revoked_at, revocation_source
        FROM ad_catalog_entries
        WHERE id = ?
          AND show_id = ?
          AND revoked_at IS NULL
        LIMIT 1
        """
        return try loadEntries(sql: sql, bind: { stmt in
            sqlite3_bind_text(
                stmt,
                1,
                id.uuidString,
                -1,
                Self.SQLITE_TRANSIENT
            )
            sqlite3_bind_text(
                stmt,
                2,
                showId,
                -1,
                Self.SQLITE_TRANSIENT
            )
        }).first
    }

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
        while true {
            let stepResult = sqlite3_step(stmt)
            if stepResult == SQLITE_DONE {
                break
            }
            guard stepResult == SQLITE_ROW else {
                throw AdCatalogStoreError.queryFailed(
                    String(cString: sqlite3_errmsg(db))
                )
            }
            guard let idString = Self.decodedText(stmt, 0) else { continue }
            guard let id = UUID(uuidString: idString) else { continue }
            let createdAt = Date(timeIntervalSince1970: sqlite3_column_double(stmt, 1))
            let showId = Self.decodedText(stmt, 2)
            let positionRaw = Self.decodedText(stmt, 3) ?? "unknown"
            let position = CatalogEpisodePosition(rawValue: positionRaw) ?? .unknown
            let duration = sqlite3_column_double(stmt, 4)

            // Fingerprint blob.
            let blobLength = Int(sqlite3_column_bytes(stmt, 5))
            var fingerprintData = Data()
            if blobLength > 0, let base = sqlite3_column_blob(stmt, 5) {
                fingerprintData = Data(bytes: base, count: blobLength)
            }
            let versionRaw = Int(sqlite3_column_int(stmt, 9))
            guard let fingerprintVersion = CatalogFingerprintVersion(
                rawValue: versionRaw
            ),
            let fingerprint = AcousticFingerprint(
                data: fingerprintData,
                version: fingerprintVersion
            ) else {
                logger.warning("loadEntries: skipped row with malformed fingerprint (id=\(idString, privacy: .public))")
                continue
            }

            let snippet = Self.decodedText(stmt, 6)

            var tokens: [String]? = nil
            if let json = Self.decodedText(stmt, 7) {
                if let data = json.data(using: .utf8) {
                    tokens = try? JSONDecoder().decode([String].self, from: data)
                }
            }

            var originalConfidence: Double? = nil
            if sqlite3_column_type(stmt, 8) != SQLITE_NULL {
                originalConfidence = sqlite3_column_double(stmt, 8)
            }

            let learningSource = Self.decodedText(stmt, 10)
                .flatMap(CatalogLearningSource.init(rawValue:))
                ?? .legacyUnconfirmed
            let learningLifecycle = Self.decodedText(stmt, 11)
                .flatMap(CatalogLearningLifecycle.init(rawValue:))
                ?? .legacyUnconfirmed
            let sourceAssetId = Self.decodedText(stmt, 12)
            let sourceWindowId = Self.decodedText(stmt, 13)
            let sourceStartTime = sqlite3_column_type(stmt, 14) == SQLITE_NULL
                ? nil
                : sqlite3_column_double(stmt, 14)
            let sourceEndTime = sqlite3_column_type(stmt, 15) == SQLITE_NULL
                ? nil
                : sqlite3_column_double(stmt, 15)
            let confirmedAt = sqlite3_column_type(stmt, 16) == SQLITE_NULL
                ? nil
                : Date(
                    timeIntervalSince1970: sqlite3_column_double(stmt, 16)
                )
            let revokedAt = sqlite3_column_type(stmt, 17) == SQLITE_NULL
                ? nil
                : Date(
                    timeIntervalSince1970: sqlite3_column_double(stmt, 17)
                )
            let revocationSourceRaw = Self.decodedText(stmt, 18)
            let revocationSource = revocationSourceRaw.flatMap(
                CatalogRevocationSource.init(rawValue:)
            )
            let persistedRevocationSourceIsMalformed =
                revocationSourceRaw != nil && revocationSource == nil

            out.append(CatalogEntry(
                id: id,
                createdAt: createdAt,
                showId: showId,
                episodePosition: position,
                durationSec: duration,
                acousticFingerprint: fingerprint,
                transcriptSnippet: snippet,
                sponsorTokens: tokens,
                originalConfidence: originalConfidence,
                learningSource: learningSource,
                learningLifecycle: learningLifecycle,
                sourceAssetId: sourceAssetId,
                sourceWindowId: sourceWindowId,
                sourceStartTime: sourceStartTime,
                sourceEndTime: sourceEndTime,
                confirmedAt: confirmedAt,
                revokedAt: revokedAt,
                revocationSource: revocationSource,
                persistedRevocationSourceIsMalformed:
                    persistedRevocationSourceIsMalformed
            ))
        }
        return out
    }

    /// Preserve SQLite's reported UTF-8 byte length so embedded NULs remain
    /// visible to canonical-identity validation. C-string decoding truncated
    /// them and could turn a malformed persisted show/source into a valid
    /// prefix namespace.
    private static func decodedText(
        _ stmt: OpaquePointer?,
        _ index: Int32
    ) -> String? {
        guard sqlite3_column_type(stmt, index) != SQLITE_NULL else {
            return nil
        }
        let byteCount = Int(sqlite3_column_bytes(stmt, index))
        if byteCount == 0 { return "" }
        guard byteCount > 0,
              let bytes = sqlite3_column_text(stmt, index) else {
            return nil
        }
        return String(
            bytes: UnsafeBufferPointer(start: bytes, count: byteCount),
            encoding: .utf8
        )
    }

    // MARK: - Migration

    private static func migrate(handle: OpaquePointer, logger: Logger) throws {
        let current = try readUserVersion(handle)
        guard current <= schemaVersion else {
            throw AdCatalogStoreError.migrationFailed(
                "catalog schema \(current) is newer than supported \(schemaVersion)"
            )
        }
        if current == schemaVersion {
            // Repair a partially stamped v3 database transactionally. Columns
            // must precede indexes that reference them.
            try exec(handle, "BEGIN IMMEDIATE")
            do {
                try ensureV3Columns(handle)
                try ensureV3AdditiveObjects(handle)
                try exec(handle, "COMMIT")
            } catch {
                try? exec(handle, "ROLLBACK")
                throw error
            }
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

        if current < 3 {
            // V2 → V3 is additive and transactional. Legacy rows retain their
            // raw bytes but are explicitly quarantined as fingerprint v1 with
            // unconfirmed lifecycle provenance.
            try exec(handle, "BEGIN IMMEDIATE")
            do {
                try ensureV3Columns(handle)
                try ensureV3AdditiveObjects(handle)
                try exec(handle, "PRAGMA user_version = 3")
                try exec(handle, "COMMIT")
            } catch {
                try? exec(handle, "ROLLBACK")
                throw error
            }
        }

        try exec(handle, "PRAGMA user_version = \(schemaVersion)")
        logger.info("AdCatalogStore migrated to schema v\(schemaVersion, privacy: .public)")
    }

    private static func ensureV3Columns(
        _ handle: OpaquePointer
    ) throws {
        // `CREATE TABLE IF NOT EXISTS` repairs the most extreme partially
        // stamped shape (v3 metadata with no table). Existing v1/v2 tables are
        // then upgraded column-by-column without rewriting rows.
        try exec(
            handle,
            """
            CREATE TABLE IF NOT EXISTS ad_catalog_entries (
                id TEXT PRIMARY KEY NOT NULL,
                created_at REAL NOT NULL,
                show_id TEXT,
                episode_position TEXT NOT NULL,
                duration_sec REAL NOT NULL,
                fingerprint_blob BLOB NOT NULL,
                transcript_snippet TEXT,
                sponsor_tokens_json TEXT,
                original_confidence REAL,
                fingerprint_version INTEGER NOT NULL DEFAULT 1,
                learning_source TEXT NOT NULL DEFAULT 'legacyUnconfirmed',
                learning_lifecycle TEXT NOT NULL DEFAULT 'legacyUnconfirmed',
                source_asset_id TEXT,
                source_window_id TEXT,
                source_start_time REAL,
                source_end_time REAL,
                confirmed_at REAL,
                revoked_at REAL,
                revocation_source TEXT
            );
            """
        )
        for column in [
            ("fingerprint_version", "INTEGER NOT NULL DEFAULT 1"),
            (
                "learning_source",
                "TEXT NOT NULL DEFAULT 'legacyUnconfirmed'"
            ),
            (
                "learning_lifecycle",
                "TEXT NOT NULL DEFAULT 'legacyUnconfirmed'"
            ),
            ("source_asset_id", "TEXT"),
            ("source_window_id", "TEXT"),
            ("source_start_time", "REAL"),
            ("source_end_time", "REAL"),
            ("confirmed_at", "REAL"),
            ("revoked_at", "REAL"),
            ("revocation_source", "TEXT"),
        ] {
            try addColumnIfNeeded(
                handle,
                table: "ad_catalog_entries",
                column: column.0,
                definition: column.1
            )
        }
    }

    private static func ensureV3AdditiveObjects(
        _ handle: OpaquePointer
    ) throws {
        try exec(
            handle,
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_show_id
            ON ad_catalog_entries(show_id);
            CREATE INDEX IF NOT EXISTS idx_catalog_created_at
            ON ad_catalog_entries(created_at);
            DROP INDEX IF EXISTS idx_catalog_show_fingerprint;
            DELETE FROM ad_catalog_entries
            WHERE show_id IS NOT NULL
              AND rowid NOT IN (
                  SELECT rowid FROM (
                      SELECT rowid,
                             ROW_NUMBER() OVER (
                                 PARTITION BY show_id, fingerprint_version,
                                              fingerprint_blob
                                 ORDER BY CASE
                                              WHEN revoked_at IS NOT NULL
                                              THEN 1 ELSE 0
                                          END DESC,
                                          IFNULL(
                                              original_confidence, -1.0
                                          ) DESC,
                                          rowid DESC
                             ) AS rn
                      FROM ad_catalog_entries
                      WHERE show_id IS NOT NULL
                  )
                  WHERE rn = 1
              );
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_catalog_show_version_fingerprint
            ON ad_catalog_entries(
                show_id, fingerprint_version, fingerprint_blob
            )
            WHERE show_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_catalog_active_show_version
            ON ad_catalog_entries(
                show_id, fingerprint_version, created_at
            )
            WHERE revoked_at IS NULL;
            CREATE TABLE IF NOT EXISTS ad_catalog_revocations (
                source_asset_id TEXT NOT NULL,
                source_window_id TEXT NOT NULL,
                revoked_at REAL NOT NULL,
                revocation_source TEXT NOT NULL,
                PRIMARY KEY(source_asset_id, source_window_id)
            );
            """
        )
    }

    /// Crash-safe helper for additive migrations. It also tolerates a database
    /// recovered from an older partially-applied migration.
    private static func addColumnIfNeeded(
        _ handle: OpaquePointer,
        table: String,
        column: String,
        definition: String
    ) throws {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(
            handle,
            "PRAGMA table_info(\(table))",
            -1,
            &stmt,
            nil
        ) == SQLITE_OK else {
            throw AdCatalogStoreError.migrationFailed(
                String(cString: sqlite3_errmsg(handle))
            )
        }
        defer { sqlite3_finalize(stmt) }
        while true {
            let stepResult = sqlite3_step(stmt)
            if stepResult == SQLITE_DONE {
                break
            }
            guard stepResult == SQLITE_ROW else {
                throw AdCatalogStoreError.migrationFailed(
                    String(cString: sqlite3_errmsg(handle))
                )
            }
            if let raw = sqlite3_column_text(stmt, 1) {
                if String(cString: raw) == column {
                    return
                }
            }
        }
        try exec(
            handle,
            "ALTER TABLE \(table) ADD COLUMN \(column) \(definition)"
        )
    }

    private static func normalizedIdentifier(_ value: String?) -> String? {
        guard let value, !value.utf8.contains(0) else { return nil }
        let normalized = value.trimmingCharacters(
            in: .whitespacesAndNewlines
        )
        return normalized.isEmpty ? nil : normalized
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
