// RepeatedAdCacheStorage.swift
// Protocol-shaped storage seam for the RepeatedAdCache.
//
// Two implementations live behind this protocol:
//   * `InMemoryRepeatedAdCacheStorage` — used by the service unit tests
//     so eviction / 14-day window logic can be exercised in milliseconds.
//   * `AnalysisStoreRepeatedAdCacheStorage` — production, backed by the
//     `repeated_ad_cache` table introduced by schema migration v21.
//
// All methods are async because the production backing actor is itself
// async-isolated.

import Foundation

protocol RepeatedAdCacheStorage: Sendable {

    // MARK: Entries

    /// Insert-or-replace by primary key `(showId, fingerprint)`. Updates
    /// `lastSeenAt`, `boundaryStart`, `boundaryEnd`, and `confidence` on
    /// existing rows.
    func upsert(_ entry: RepeatedAdCacheEntry) async throws

    /// Returns ALL rows for the given `showId`. Caller filters by
    /// Hamming distance — the storage layer does not know the threshold.
    /// Sorted by `lastSeenAt` DESC so callers iterating LRU-style can
    /// short-circuit.
    func fetchAll(showId: String) async throws -> [RepeatedAdCacheEntry]

    /// Refresh `lastSeenAt` for `(showId, fingerprint)`. No-op if the row
    /// doesn't exist. Used on cache hit to update LRU.
    func touch(showId: String, fingerprint: RepeatedAdFingerprint, at: Date) async throws

    /// Total number of rows for a given show. Used by per-show LRU eviction.
    func count(showId: String) async throws -> Int

    /// Total number of rows across all shows. Used by global LRU eviction.
    func totalCount() async throws -> Int

    /// Evict the oldest `lastSeenAt` row for a given show. Returns
    /// `true` if a row was deleted.
    func evictOldest(showId: String) async throws -> Bool

    /// Evict the oldest `lastSeenAt` row across all shows. Returns
    /// `true` if a row was deleted.
    func evictOldestGlobal() async throws -> Bool

    /// Purge any row with `lastSeenAt` older than `olderThan`. Returns
    /// the number of rows removed.
    @discardableResult
    func purgeStale(olderThan: Date) async throws -> Int

    /// Delete every row.
    func clearEntries() async throws

    // MARK: Outcome samples (auto-disable telemetry)

    /// Append a single outcome sample to the rolling window.
    func appendOutcome(_ sample: RepeatedAdCacheOutcomeSample) async throws

    /// All outcome samples newer than `olderThan` (i.e. within the
    /// active window).
    func fetchOutcomes(newerThan: Date) async throws -> [RepeatedAdCacheOutcomeSample]

    /// Trim outcomes older than `olderThan`. Returns number deleted.
    @discardableResult
    func purgeOutcomes(olderThan: Date) async throws -> Int

    /// Wipe outcome samples too. Called when the cache is cleared
    /// (kill-switch flip or explicit `clear()`).
    func clearOutcomes() async throws
}

// MARK: - In-memory implementation

/// In-memory storage backing for unit tests. Thread-safe via `actor`.
actor InMemoryRepeatedAdCacheStorage: RepeatedAdCacheStorage {

    private struct Key: Hashable {
        let showId: String
        let fingerprint: RepeatedAdFingerprint
    }

    private var entries: [Key: RepeatedAdCacheEntry] = [:]
    private var outcomes: [RepeatedAdCacheOutcomeSample] = []

    init() {}

    func upsert(_ entry: RepeatedAdCacheEntry) async throws {
        let key = Key(showId: entry.showId, fingerprint: entry.fingerprint)
        entries[key] = entry
    }

    func fetchAll(showId: String) async throws -> [RepeatedAdCacheEntry] {
        entries.values
            .filter { $0.showId == showId }
            .sorted { $0.lastSeenAt > $1.lastSeenAt }
    }

    func touch(showId: String, fingerprint: RepeatedAdFingerprint, at: Date) async throws {
        let key = Key(showId: showId, fingerprint: fingerprint)
        guard let existing = entries[key] else { return }
        entries[key] = RepeatedAdCacheEntry(
            showId: existing.showId,
            fingerprint: existing.fingerprint,
            boundaryStart: existing.boundaryStart,
            boundaryEnd: existing.boundaryEnd,
            confidence: existing.confidence,
            lastSeenAt: at
        )
    }

    func count(showId: String) async throws -> Int {
        entries.values.filter { $0.showId == showId }.count
    }

    func totalCount() async throws -> Int { entries.count }

    func evictOldest(showId: String) async throws -> Bool {
        let candidate = entries
            .filter { $0.value.showId == showId }
            .min { $0.value.lastSeenAt < $1.value.lastSeenAt }
        guard let candidate else { return false }
        entries.removeValue(forKey: candidate.key)
        return true
    }

    func evictOldestGlobal() async throws -> Bool {
        let candidate = entries.min { $0.value.lastSeenAt < $1.value.lastSeenAt }
        guard let candidate else { return false }
        entries.removeValue(forKey: candidate.key)
        return true
    }

    @discardableResult
    func purgeStale(olderThan: Date) async throws -> Int {
        let stale = entries.filter { $0.value.lastSeenAt < olderThan }
        for key in stale.keys { entries.removeValue(forKey: key) }
        return stale.count
    }

    func clearEntries() async throws {
        entries.removeAll()
    }

    func appendOutcome(_ sample: RepeatedAdCacheOutcomeSample) async throws {
        outcomes.append(sample)
    }

    func fetchOutcomes(newerThan: Date) async throws -> [RepeatedAdCacheOutcomeSample] {
        outcomes.filter { $0.timestamp >= newerThan }
    }

    @discardableResult
    func purgeOutcomes(olderThan: Date) async throws -> Int {
        let before = outcomes.count
        outcomes.removeAll { $0.timestamp < olderThan }
        return before - outcomes.count
    }

    func clearOutcomes() async throws {
        outcomes.removeAll()
    }
}
