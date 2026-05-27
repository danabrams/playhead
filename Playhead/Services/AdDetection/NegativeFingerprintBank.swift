// NegativeFingerprintBank.swift
// playhead-xsdz.9: On-device HARD-NEGATIVE fingerprint bank — the novel
// precision lever of the cross-episode "memory" signal.
//
// What it is
// ----------
// A store of compact token-sequence fingerprints of CONFIRMED FALSE POSITIVES:
// spans the user reverted / undid after an auto-skip ("that wasn't an ad").
// Future candidates whose transcript tokens align strongly (Smith-Waterman
// local alignment) to a negative-bank entry are SUPPRESSED — their skip
// confidence is multiplicatively reduced — because we have direct evidence
// that this exact copy is NOT an ad on this show.
//
// MEMORY-POLLUTION GUARD (correctness requirement, not optional)
// --------------------------------------------------------------
// This bank ingests ONLY confirmed false positives (correction-gated). It must
// NEVER auto-ingest positives — the positive auto-skip path runs at ~0.54
// precision, so auto-confirming positives here would poison the suppressor with
// real ads and start suppressing legitimate skips. The ONLY write entry point
// is `recordConfirmedFalsePositive`, called from the user-correction
// (reversion) seam.
//
// Persistence
// -----------
// Self-contained SQLite file (`negative_fingerprints.sqlite`) in Application
// Support, schema-versioned via `PRAGMA user_version`. Deliberately independent
// of `AnalysisStore` (a corruption here cannot take the analysis DB down) and
// modeled directly on `AdCatalogStore`: per-show LRU eviction, optional global
// (`show_id IS NULL`) scope, and time-decay so stale entries lose influence as
// ad inventory drifts.
//
// On-device mandate (legal)
// -------------------------
// Token fingerprints NEVER leave the device. This store has no export path.

import Foundation
import OSLog
import SQLite3

// MARK: - Supporting types

/// A single stored hard-negative fingerprint: the normalized token sequence of
/// a confirmed false-positive span.
struct NegativeFingerprintEntry: Sendable, Hashable {
    let id: UUID
    /// When this confirmed-FP fingerprint was recorded (Unix seconds). Drives
    /// both LRU eviction and time-decay weighting.
    let recordedAt: Double
    /// Most recent time this entry MATCHED a candidate (Unix seconds). Refreshed
    /// on each suppression so genuinely-recurring negatives are retained over
    /// one-off ones during LRU eviction. Equals `recordedAt` at insert.
    let lastMatchedAt: Double
    /// Podcast / show identifier; `nil` means a global negative that matches
    /// across all shows (used sparingly — see scope notes on the write API).
    let showId: String?
    /// Normalized token sequence of the confirmed-FP span, joined with single
    /// spaces. Stored joined for compact storage; split back to tokens for
    /// Smith-Waterman alignment.
    let tokensJoined: String
    /// Number of distinct confirmations (user reverted this same copy N times).
    let confirmationCount: Int

    /// The normalized token array for alignment.
    var tokens: [String] {
        tokensJoined.isEmpty ? [] : tokensJoined.split(separator: " ").map(String.init)
    }

    init(
        id: UUID = UUID(),
        recordedAt: Double = Date().timeIntervalSince1970,
        lastMatchedAt: Double? = nil,
        showId: String?,
        tokensJoined: String,
        confirmationCount: Int = 1
    ) {
        self.id = id
        self.recordedAt = recordedAt
        self.lastMatchedAt = lastMatchedAt ?? recordedAt
        self.showId = showId
        self.tokensJoined = tokensJoined
        self.confirmationCount = confirmationCount
    }
}

/// A negative-bank match against a candidate token sequence.
struct NegativeFingerprintMatch: Sendable, Hashable {
    let entry: NegativeFingerprintEntry
    /// Smith-Waterman normalized alignment score in `[0, 1]`.
    let similarity: Double
    /// Time-decay weight in `[decayFloor, 1]` derived from `recordedAt`.
    let decayWeight: Double
    /// `similarity * decayWeight` — the effective suppression strength.
    let effectiveStrength: Double
}

// MARK: - Errors

enum NegativeFingerprintBankError: Error, Equatable {
    case openFailed(String)
    case migrationFailed(String)
    case insertFailed(String)
    case queryFailed(String)
}

// MARK: - Store

/// Actor-backed hard-negative fingerprint bank. All reads/writes are serialized
/// on the actor so SQLite's single-connection model is honored without an
/// explicit mutex. Mirrors `AdCatalogStore`'s lifecycle exactly.
actor NegativeFingerprintBank {

    // MARK: Constants / tunables

    /// Schema version stamped into `PRAGMA user_version`.
    static let schemaVersion: Int32 = 1

    /// Minimum normalized Smith-Waterman score for a candidate to count as a
    /// near-match to a stored negative. Conservative — a hard-negative match
    /// SUPPRESSES a skip, so a false suppression would let a real ad through.
    /// 0.80 means the candidate must align with ~80% of the shorter sequence.
    static let defaultMatchThreshold: Double = 0.80

    /// Shortest token sequence we will store or match. Below this a coincidental
    /// alignment is too likely (e.g. two unrelated spans sharing "and the").
    static let minTokenCount: Int = 4

    /// Maximum tokens retained per stored sequence. Caps Smith-Waterman cost at
    /// O(candidate × 100). Sequences longer than this are truncated to their
    /// leading tokens (the disclosure / brand mention that recurs verbatim).
    static let maxTokenCount: Int = 100

    /// Maximum entries retained per (show_id) bucket. Older rows beyond this are
    /// evicted by `last_matched_at ASC` (least-recently-useful) after each
    /// insert, bounding total rows at O(maxEntriesPerShow × distinct shows).
    static let maxEntriesPerShow: Int = 200

    /// Half-life-style linear decay: weight is 1.0 at age 0, decaying to
    /// `decayFloor` at `decayHorizonDays`, clamped to `[decayFloor, 1]`.
    /// 120 days reflects ad-inventory churn — a brand the user de-flagged 4
    /// months ago may have rotated out, so its suppression influence fades but
    /// never vanishes entirely (a re-match refreshes `last_matched_at`).
    static let decayHorizonDays: Double = 120.0
    static let decayFloor: Double = 0.2

    /// Linear time-decay weight in `[decayFloor, 1]` for an entry of the given
    /// age in days. Pure; exposed for unit testing.
    static func decayWeight(ageDays: Double) -> Double {
        guard ageDays.isFinite, ageDays > 0 else { return 1.0 }
        let raw = 1.0 - (ageDays / decayHorizonDays) * (1.0 - decayFloor)
        return Swift.max(decayFloor, Swift.min(1.0, raw))
    }

    // MARK: State

    nonisolated let dbURL: URL
    nonisolated(unsafe) private var db: OpaquePointer?
    private let logger = Logger(subsystem: "com.playhead", category: "NegativeFingerprintBank")

    // MARK: - Lifecycle

    /// Open or create the bank at `directoryURL` (created if needed). File is
    /// `negative_fingerprints.sqlite`. Lazy first-use bootstrap mirrors
    /// `AdCatalogStore` so the DDL stays off the launch path.
    init(directoryURL: URL) throws {
        try FileManager.default.createDirectory(
            at: directoryURL,
            withIntermediateDirectories: true
        )
        self.dbURL = directoryURL.appendingPathComponent("negative_fingerprints.sqlite")
    }

    private func ensureOpen() throws {
        if db != nil { return }
        var handle: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        let rc = sqlite3_open_v2(dbURL.path, &handle, flags, nil)
        guard rc == SQLITE_OK, let handle else {
            let msg = handle.map { String(cString: sqlite3_errmsg($0)) } ?? "sqlite3_open_v2 rc=\(rc)"
            if let handle { sqlite3_close(handle) }
            throw NegativeFingerprintBankError.openFailed(msg)
        }
        do {
            // SECURE_DELETE so overwritten/evicted negative fingerprints don't
            // leave token bytes lying around (on-device sensitivity mandate).
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
        return base.appendingPathComponent("NegativeFingerprints", isDirectory: true)
    }

    // MARK: - Write (CORRECTION-GATED ONLY)

    /// Record a CONFIRMED FALSE POSITIVE from raw span transcript text.
    ///
    /// This is the ONLY public ingress for positives-as-negatives. Callers MUST
    /// only invoke it when a user has actually reverted / vetoed an auto-skip
    /// (a confirmed FP). There is intentionally no "record positive" path — see
    /// the memory-pollution guard in the file header.
    ///
    /// If a near-identical sequence already exists for the same show, the
    /// existing row's `confirmation_count` is incremented and its
    /// `last_matched_at` refreshed instead of inserting a duplicate.
    ///
    /// - Returns: `true` when a new row was inserted or an existing one
    ///   confirmed; `false` when the input was rejected (too short after
    ///   normalization).
    @discardableResult
    func recordConfirmedFalsePositive(
        text: String,
        showId: String?,
        recordedAt: Double = Date().timeIntervalSince1970
    ) throws -> Bool {
        let tokens = clampTokens(SmithWatermanAligner.tokenize(text))
        guard tokens.count >= Self.minTokenCount else {
            logger.debug("recordConfirmedFalsePositive: skipping short sequence (\(tokens.count) tokens)")
            return false
        }
        return try recordConfirmedFalsePositive(
            tokens: tokens,
            showId: showId,
            recordedAt: recordedAt
        )
    }

    /// Token-level variant. Tokens must already be normalized (caller's
    /// responsibility) — use `SmithWatermanAligner.tokenize`.
    @discardableResult
    func recordConfirmedFalsePositive(
        tokens: [String],
        showId: String?,
        recordedAt: Double = Date().timeIntervalSince1970
    ) throws -> Bool {
        try ensureOpen()
        guard let db else { throw NegativeFingerprintBankError.insertFailed("database closed") }
        let clamped = clampTokens(tokens)
        guard clamped.count >= Self.minTokenCount else { return false }
        let joined = clamped.joined(separator: " ")

        // Dedup: if a stored sequence for this show aligns near-identically
        // (>= a tight 0.95 threshold) to the incoming one, confirm it in place
        // rather than storing a near-duplicate. Scope the dedup scan to the
        // same show (NULL-show negatives dedup against each other).
        let existing = try loadEntries(forShow: showId, includeGlobal: false)
        for candidate in existing {
            let r = SmithWatermanAligner.align(clamped, candidate.tokens)
            if r.normalizedScore >= 0.95 {
                try confirmExisting(id: candidate.id, at: recordedAt)
                return true
            }
        }

        let entry = NegativeFingerprintEntry(
            recordedAt: recordedAt,
            showId: showId,
            tokensJoined: joined,
            confirmationCount: 1
        )
        let sql = """
        INSERT INTO negative_fingerprints
            (id, recorded_at, last_matched_at, show_id, tokens_joined, confirmation_count)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NegativeFingerprintBankError.insertFailed(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, entry.id.uuidString, -1, Self.SQLITE_TRANSIENT)
        sqlite3_bind_double(stmt, 2, entry.recordedAt)
        sqlite3_bind_double(stmt, 3, entry.lastMatchedAt)
        if let show = entry.showId {
            sqlite3_bind_text(stmt, 4, show, -1, Self.SQLITE_TRANSIENT)
        } else {
            sqlite3_bind_null(stmt, 4)
        }
        sqlite3_bind_text(stmt, 5, entry.tokensJoined, -1, Self.SQLITE_TRANSIENT)
        sqlite3_bind_int(stmt, 6, Int32(entry.confirmationCount))
        guard sqlite3_step(stmt) == SQLITE_DONE else {
            throw NegativeFingerprintBankError.insertFailed(String(cString: sqlite3_errmsg(db)))
        }

        try evictForShowIfNeeded(showId: showId)
        return true
    }

    /// Increment confirmation count + refresh `last_matched_at` for a row.
    private func confirmExisting(id: UUID, at now: Double) throws {
        guard let db else { return }
        let sql = """
        UPDATE negative_fingerprints
        SET confirmation_count = confirmation_count + 1,
            last_matched_at = ?
        WHERE id = ?
        """
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else { return }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_double(stmt, 1, now)
        sqlite3_bind_text(stmt, 2, id.uuidString, -1, Self.SQLITE_TRANSIENT)
        _ = sqlite3_step(stmt)
    }

    // MARK: - Query (decision path)

    /// Find the strongest negative-bank match for a candidate's token sequence.
    ///
    /// Only entries for `show` (plus global `show_id IS NULL` negatives) are
    /// considered. Returns the single match with the highest `effectiveStrength`
    /// (`similarity * decayWeight`) at or above `threshold`, or `nil` when
    /// nothing qualifies. Bounded cost: O(rows × candidate × stored-len), and
    /// rows are capped by `maxEntriesPerShow`.
    ///
    /// Side effect: the matched row's `last_matched_at` is refreshed so a
    /// genuinely-recurring negative survives LRU eviction. This is the only
    /// query-path write and is best-effort (failures are logged, not thrown).
    func bestMatch(
        candidateTokens: [String],
        show: String?,
        threshold: Double = NegativeFingerprintBank.defaultMatchThreshold,
        now: Double = Date().timeIntervalSince1970
    ) -> NegativeFingerprintMatch? {
        do {
            try ensureOpen()
        } catch {
            logger.error("bestMatch: ensureOpen failed: \(String(describing: error), privacy: .public)")
            return nil
        }
        guard db != nil else { return nil }
        let clamped = clampTokens(candidateTokens)
        guard clamped.count >= Self.minTokenCount else { return nil }

        let entries: [NegativeFingerprintEntry]
        do {
            entries = try loadEntries(forShow: show, includeGlobal: true)
        } catch {
            logger.error("bestMatch: query failed: \(String(describing: error), privacy: .public)")
            return nil
        }
        guard !entries.isEmpty else { return nil }

        var best: NegativeFingerprintMatch?
        for entry in entries {
            let r = SmithWatermanAligner.align(clamped, entry.tokens)
            guard r.normalizedScore >= threshold else { continue }
            let ageDays = Swift.max(0.0, (now - entry.recordedAt) / 86_400.0)
            let decay = Self.decayWeight(ageDays: ageDays)
            let effective = r.normalizedScore * decay
            let match = NegativeFingerprintMatch(
                entry: entry,
                similarity: r.normalizedScore,
                decayWeight: decay,
                effectiveStrength: effective
            )
            if best == nil || effective > best!.effectiveStrength {
                best = match
            }
        }

        if let best {
            // Best-effort recency refresh so recurring negatives aren't evicted.
            do { try confirmMatchRecency(id: best.entry.id, at: now) } catch {
                logger.debug("bestMatch: recency refresh failed (non-fatal)")
            }
        }
        return best
    }

    /// Refresh only `last_matched_at` (no confirmation bump) on a query hit.
    private func confirmMatchRecency(id: UUID, at now: Double) throws {
        guard let db else { return }
        let sql = "UPDATE negative_fingerprints SET last_matched_at = ? WHERE id = ?"
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else { return }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_double(stmt, 1, now)
        sqlite3_bind_text(stmt, 2, id.uuidString, -1, Self.SQLITE_TRANSIENT)
        _ = sqlite3_step(stmt)
    }

    // MARK: - Diagnostics / maintenance

    /// Row count (telemetry / tests).
    func count() throws -> Int {
        try ensureOpen()
        guard let db else { throw NegativeFingerprintBankError.queryFailed("database closed") }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM negative_fingerprints", -1, &stmt, nil) == SQLITE_OK else {
            throw NegativeFingerprintBankError.queryFailed(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
        return Int(sqlite3_column_int(stmt, 0))
    }

    /// All entries (diagnostic / test). No filter.
    func allEntries() throws -> [NegativeFingerprintEntry] {
        try ensureOpen()
        guard db != nil else { throw NegativeFingerprintBankError.queryFailed("database closed") }
        let sql = """
        SELECT id, recorded_at, last_matched_at, show_id, tokens_joined, confirmation_count
        FROM negative_fingerprints
        ORDER BY recorded_at DESC
        """
        return try runLoad(sql: sql, bind: nil)
    }

    /// Delete all rows (test helper / "reset" debug action).
    func clear() throws {
        try ensureOpen()
        guard let db else { throw NegativeFingerprintBankError.queryFailed("database closed") }
        try Self.exec(db, "DELETE FROM negative_fingerprints")
    }

    // MARK: - Private: token clamping

    private func clampTokens(_ tokens: [String]) -> [String] {
        tokens.count > Self.maxTokenCount
            ? Array(tokens.prefix(Self.maxTokenCount))
            : tokens
    }

    // MARK: - Private: load

    /// Load entries for a show. When `includeGlobal` is true, also include
    /// `show_id IS NULL` rows (the cross-show negatives). When `show` is nil,
    /// only `show_id IS NULL` rows are returned (a NULL-show write/read scope).
    private func loadEntries(forShow show: String?, includeGlobal: Bool) throws -> [NegativeFingerprintEntry] {
        // Deterministic ordering (`recorded_at DESC, id ASC`) so `bestMatch`'s
        // first-wins tie-break is stable across runs — SQLite's natural row
        // order is otherwise unspecified, which would make the suppression
        // non-deterministic when two negatives tie on effective strength.
        let orderBy = "ORDER BY recorded_at DESC, id ASC"
        let sql: String
        if show != nil {
            sql = includeGlobal
                ? """
                  SELECT id, recorded_at, last_matched_at, show_id, tokens_joined, confirmation_count
                  FROM negative_fingerprints WHERE show_id = ? OR show_id IS NULL \(orderBy)
                  """
                : """
                  SELECT id, recorded_at, last_matched_at, show_id, tokens_joined, confirmation_count
                  FROM negative_fingerprints WHERE show_id = ? \(orderBy)
                  """
        } else {
            sql = """
            SELECT id, recorded_at, last_matched_at, show_id, tokens_joined, confirmation_count
            FROM negative_fingerprints WHERE show_id IS NULL \(orderBy)
            """
        }
        return try runLoad(sql: sql, bind: { stmt in
            if let show { sqlite3_bind_text(stmt, 1, show, -1, Self.SQLITE_TRANSIENT) }
        })
    }

    private func runLoad(
        sql: String,
        bind: ((OpaquePointer?) -> Void)?
    ) throws -> [NegativeFingerprintEntry] {
        guard let db else { throw NegativeFingerprintBankError.queryFailed("database closed") }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NegativeFingerprintBankError.queryFailed(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(stmt) }
        bind?(stmt)
        var out: [NegativeFingerprintEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            guard let idC = sqlite3_column_text(stmt, 0) else { continue }
            let idStr = String(cString: idC)
            guard let id = UUID(uuidString: idStr) else { continue }
            let recordedAt = sqlite3_column_double(stmt, 1)
            let lastMatchedAt = sqlite3_column_double(stmt, 2)
            let showId: String? = sqlite3_column_text(stmt, 3).map { String(cString: $0) }
            let tokensJoined: String = sqlite3_column_text(stmt, 4).map { String(cString: $0) } ?? ""
            let confirmations = Int(sqlite3_column_int(stmt, 5))
            out.append(NegativeFingerprintEntry(
                id: id,
                recordedAt: recordedAt,
                lastMatchedAt: lastMatchedAt,
                showId: showId,
                tokensJoined: tokensJoined,
                confirmationCount: confirmations
            ))
        }
        return out
    }

    // MARK: - Private: LRU eviction

    /// Cap the per-show row count at `maxEntriesPerShow`, deleting the
    /// least-recently-matched rows first. Called after each insert.
    private func evictForShowIfNeeded(showId: String?) throws {
        guard let db else { return }
        let countSQL = showId != nil
            ? "SELECT COUNT(*) FROM negative_fingerprints WHERE show_id = ?"
            : "SELECT COUNT(*) FROM negative_fingerprints WHERE show_id IS NULL"
        var countStmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, countSQL, -1, &countStmt, nil) == SQLITE_OK else { return }
        defer { sqlite3_finalize(countStmt) }
        if let showId { sqlite3_bind_text(countStmt, 1, showId, -1, Self.SQLITE_TRANSIENT) }
        guard sqlite3_step(countStmt) == SQLITE_ROW else { return }
        let total = Int(sqlite3_column_int(countStmt, 0))
        guard total > Self.maxEntriesPerShow else { return }
        let toEvict = total - Self.maxEntriesPerShow

        let deleteSQL = showId != nil
            ? """
              DELETE FROM negative_fingerprints WHERE id IN (
                  SELECT id FROM negative_fingerprints WHERE show_id = ?
                  ORDER BY last_matched_at ASC LIMIT \(toEvict)
              )
              """
            : """
              DELETE FROM negative_fingerprints WHERE id IN (
                  SELECT id FROM negative_fingerprints WHERE show_id IS NULL
                  ORDER BY last_matched_at ASC LIMIT \(toEvict)
              )
              """
        var delStmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, deleteSQL, -1, &delStmt, nil) == SQLITE_OK else { return }
        defer { sqlite3_finalize(delStmt) }
        if let showId { sqlite3_bind_text(delStmt, 1, showId, -1, Self.SQLITE_TRANSIENT) }
        _ = sqlite3_step(delStmt)
    }

    // MARK: - Migration

    private static func migrate(handle: OpaquePointer, logger: Logger) throws {
        let current = try readUserVersion(handle)
        if current >= schemaVersion { return }
        if current < 1 {
            let createSQL = """
            CREATE TABLE IF NOT EXISTS negative_fingerprints (
                id TEXT PRIMARY KEY NOT NULL,
                recorded_at REAL NOT NULL,
                last_matched_at REAL NOT NULL,
                show_id TEXT,
                tokens_joined TEXT NOT NULL,
                confirmation_count INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_negfp_show_id ON negative_fingerprints(show_id);
            CREATE INDEX IF NOT EXISTS idx_negfp_last_matched ON negative_fingerprints(last_matched_at);
            """
            try exec(handle, createSQL)
        }
        try exec(handle, "PRAGMA user_version = \(schemaVersion)")
        logger.info("NegativeFingerprintBank migrated to schema v\(schemaVersion, privacy: .public)")
    }

    private static func readUserVersion(_ handle: OpaquePointer) throws -> Int32 {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(handle, "PRAGMA user_version", -1, &stmt, nil) == SQLITE_OK else {
            throw NegativeFingerprintBankError.migrationFailed(String(cString: sqlite3_errmsg(handle)))
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else {
            throw NegativeFingerprintBankError.migrationFailed("user_version step failed")
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
            throw NegativeFingerprintBankError.migrationFailed(msg)
        }
    }
}
