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
    @discardableResult
    func upsert(_ entry: RepeatedAdCacheEntry) async throws -> Bool

    /// Upsert and enforce both capacity limits as one storage transaction.
    /// A failed count/eviction rolls the write and all evictions back.
    @discardableResult
    func upsertEnforcingCapacity(
        _ entry: RepeatedAdCacheEntry,
        perShowCap: Int,
        globalCap: Int
    ) async throws -> Bool

    /// Persist a source-window veto. Tombstones are intentionally separate
    /// from cache entries and survive `clearEntries()` so a delayed learning
    /// task cannot resurrect a correction after a kill-switch cycle.
    @discardableResult
    func recordRevocation(
        sourceAssetId: String,
        sourceWindowId: String,
        source: CatalogRevocationSource,
        at: Date
    ) async throws -> Bool

    /// Persist a show-scoped creative veto. It is separate from the exact
    /// source tombstone because the same corrected creative can have been
    /// learned from another source row. Tombstones survive cache clearing.
    @discardableResult
    func recordFingerprintRevocation(
        showId: String,
        fingerprint: RepeatedAdFingerprint,
        sourceAssetId: String,
        sourceWindowId: String,
        source: CatalogRevocationSource,
        at: Date
    ) async throws -> Bool

    /// Persist the exact-source tombstone, optionally persist its creative
    /// tombstone, and delete matching rows as one atomic storage operation.
    /// A storage error must roll every mutation back.
    @discardableResult
    func revokeMatchesAtomically(
        showId: String?,
        fingerprint: RepeatedAdFingerprint?,
        sourceAssetId: String,
        sourceWindowId: String,
        sourceStartTime: Double?,
        sourceEndTime: Double?,
        source: CatalogRevocationSource,
        at: Date
    ) async throws -> Int

    /// Fetch all durable creative vetoes for one canonical show.
    func fetchRevokedFingerprints(
        showId: String
    ) async throws -> [RepeatedAdFingerprint]

    /// Returns ALL rows for the given `showId`. Caller filters by
    /// Hamming distance — the storage layer does not know the threshold.
    /// Sorted by `lastSeenAt` DESC so callers iterating LRU-style can
    /// short-circuit.
    func fetchAll(showId: String) async throws -> [RepeatedAdCacheEntry]

    /// Refresh `lastSeenAt` for `(showId, fingerprint)`. No-op if the row
    /// doesn't exist. Used on cache hit to update LRU.
    func touch(showId: String, fingerprint: RepeatedAdFingerprint, at: Date) async throws

    /// Delete one exact persisted fingerprint.
    @discardableResult
    func delete(
        showId: String,
        fingerprint: RepeatedAdFingerprint
    ) async throws -> Bool

    /// Delete every row learned from one exact source window. This fallback
    /// makes correction revocation effective even when the corrected audio
    /// cannot be re-fingerprinted or its show identity is unavailable.
    @discardableResult
    func delete(
        sourceAssetId: String,
        sourceWindowId: String
    ) async throws -> Int

    /// Delete `entry` only when the row at its `(showId, fingerprint)` key is
    /// still owned by the same opaque producer revision. Used by a stale actor
    /// writer to retract its own write without deleting a byte-identical newer
    /// UPSERT.
    @discardableResult
    func deleteIfUnchanged(
        _ entry: RepeatedAdCacheEntry
    ) async throws -> Bool

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

    private struct RevocationKey: Hashable {
        let sourceAssetId: String
        let sourceWindowId: String
    }

    private var entries: [Key: RepeatedAdCacheEntry] = [:]
    private var revocations: [RevocationKey: Date] = [:]
    private var fingerprintRevocations: [Key: Date] = [:]
    private var outcomes: [RepeatedAdCacheOutcomeSample] = []

#if DEBUG
    private var appendOutcomeBarrierForTesting:
        (@Sendable () async -> Void)?

    func _setAppendOutcomeBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        appendOutcomeBarrierForTesting = barrier
    }
#endif

    init() {}

    @discardableResult
    func upsert(_ entry: RepeatedAdCacheEntry) async throws -> Bool {
        upsertValidated(entry)
    }

    @discardableResult
    func upsertEnforcingCapacity(
        _ entry: RepeatedAdCacheEntry,
        perShowCap: Int,
        globalCap: Int
    ) async throws -> Bool {
        guard perShowCap > 0,
              globalCap > 0,
              let normalizedShowId = Self.normalizedIdentifier(entry.showId)
        else {
            return false
        }
        let originalEntries = entries
        guard upsertValidated(entry) else { return false }

        while entries.values.filter({ $0.showId == normalizedShowId }).count
                > perShowCap {
            guard evictOldestEntry(showId: normalizedShowId) else {
                entries = originalEntries
                return false
            }
        }
        while entries.count > globalCap {
            guard evictOldestGlobalEntry() else {
                entries = originalEntries
                return false
            }
        }
        return true
    }

    private func upsertValidated(_ entry: RepeatedAdCacheEntry) -> Bool {
        // Mirror the production-storage zero-fingerprint guard
        // (review/v0.5-head-polish L3) so service-layer tests
        // running against the in-memory storage observe the same
        // drop-on-zero semantics as the production SQLite-backed
        // storage. The mirror is silent (matching production); for
        // tests that want to *assert* the drop, see the dedicated
        // tests in `RepeatedAdCacheStorageZeroFingerprintTests`.
        guard !entry.fingerprint.isZero,
              let showId = Self.normalizedIdentifier(entry.showId)
        else {
            return false
        }
        let sourceAssetId = Self.normalizedIdentifier(entry.sourceAssetId)
        let sourceWindowId = Self.normalizedIdentifier(entry.sourceWindowId)
        guard (entry.sourceAssetId == nil || sourceAssetId != nil),
              (entry.sourceWindowId == nil || sourceWindowId != nil),
              Self.normalizedIdentifier(entry.producerRevision)
                == entry.producerRevision else {
            return false
        }
        if let sourceAssetId,
           let sourceWindowId {
            guard let exactWindowKey =
                    RecurrenceMaterialIdentity.tombstoneWindowKey(
                        sourceWindowId: sourceWindowId,
                        sourceStartTime: entry.boundaryStart,
                        sourceEndTime: entry.boundaryEnd
                    ) else {
                return false
            }
            let exactKey = RevocationKey(
                sourceAssetId: sourceAssetId,
                sourceWindowId: exactWindowKey
            )
            let legacyKey = RevocationKey(
                sourceAssetId: sourceAssetId,
                sourceWindowId: sourceWindowId
            )
            let signedZeroLegacyKey =
                RecurrenceMaterialIdentity
                .legacyNegativeZeroTombstoneWindowKey(
                    sourceWindowId: sourceWindowId,
                    sourceStartTime: entry.boundaryStart,
                    sourceEndTime: entry.boundaryEnd
                )
                .map {
                    RevocationKey(
                        sourceAssetId: sourceAssetId,
                        sourceWindowId: $0
                    )
                }
            if revocations[exactKey] != nil
                || revocations[legacyKey] != nil
                || signedZeroLegacyKey.map({
                    revocations[$0] != nil
                }) == true {
                return false
            }
        }
        let key = Key(showId: showId, fingerprint: entry.fingerprint)
        guard fingerprintRevocations[key] == nil else { return false }
        let existingEntry = entries[key]
        if let existing = existingEntry {
            let existingRank = Self.lifecycleRank(
                existing.learningLifecycle
            )
            let incomingRank = Self.lifecycleRank(
                entry.learningLifecycle
            )
            guard incomingRank > existingRank
                    || (
                        incomingRank == existingRank
                        && entry.lastSeenAt >= existing.lastSeenAt
                    ) else {
                return false
            }
        }
        entries[key] = RepeatedAdCacheEntry(
            showId: showId,
            fingerprint: entry.fingerprint,
            boundaryStart: entry.boundaryStart,
            boundaryEnd: entry.boundaryEnd,
            confidence: entry.confidence,
            lastSeenAt: max(
                existingEntry?.lastSeenAt ?? entry.lastSeenAt,
                entry.lastSeenAt
            ),
            learningSource: entry.learningSource,
            learningLifecycle: entry.learningLifecycle,
            sourceAssetId: sourceAssetId,
            sourceWindowId: sourceWindowId,
            producerRevision: entry.producerRevision
        )
        return true
    }

    @discardableResult
    func recordRevocation(
        sourceAssetId: String,
        sourceWindowId: String,
        source: CatalogRevocationSource,
        at: Date
    ) async throws -> Bool {
        guard let sourceAssetId = Self.normalizedIdentifier(sourceAssetId),
              let sourceWindowId = Self.normalizedIdentifier(sourceWindowId),
              at.timeIntervalSince1970.isFinite,
              at.timeIntervalSince1970 >= 0 else {
            return false
        }
        let key = RevocationKey(
            sourceAssetId: sourceAssetId,
            sourceWindowId: sourceWindowId
        )
        if let existing = revocations[key], existing > at {
            return true
        }
        revocations[key] = at
        return true
    }

    @discardableResult
    func recordFingerprintRevocation(
        showId: String,
        fingerprint: RepeatedAdFingerprint,
        sourceAssetId: String,
        sourceWindowId: String,
        source: CatalogRevocationSource,
        at: Date
    ) async throws -> Bool {
        guard let showId = Self.normalizedIdentifier(showId),
              !fingerprint.isZero,
              Self.normalizedIdentifier(sourceAssetId) != nil,
              Self.normalizedIdentifier(sourceWindowId) != nil,
              at.timeIntervalSince1970.isFinite,
              at.timeIntervalSince1970 >= 0 else {
            return false
        }
        let key = Key(showId: showId, fingerprint: fingerprint)
        if let existing = fingerprintRevocations[key], existing > at {
            return true
        }
        fingerprintRevocations[key] = at
        return true
    }

    @discardableResult
    func revokeMatchesAtomically(
        showId: String?,
        fingerprint: RepeatedAdFingerprint?,
        sourceAssetId: String,
        sourceWindowId: String,
        sourceStartTime: Double? = nil,
        sourceEndTime: Double? = nil,
        source: CatalogRevocationSource,
        at: Date
    ) async throws -> Int {
        guard let sourceAssetId = Self.normalizedIdentifier(sourceAssetId),
              let sourceWindowId = Self.normalizedIdentifier(sourceWindowId),
              let tombstoneWindowKey =
                RecurrenceMaterialIdentity.tombstoneWindowKey(
                    sourceWindowId: sourceWindowId,
                    sourceStartTime: sourceStartTime,
                    sourceEndTime: sourceEndTime
                ),
              at.timeIntervalSince1970.isFinite,
              at.timeIntervalSince1970 >= 0 else {
            return 0
        }

        let revocationKey = RevocationKey(
            sourceAssetId: sourceAssetId,
            sourceWindowId: tombstoneWindowKey
        )
        if revocations[revocationKey, default: .distantPast] <= at {
            revocations[revocationKey] = at
        }

        let sourceKeys = entries.compactMap { key, entry in
            entry.sourceAssetId == sourceAssetId
                && entry.sourceWindowId == sourceWindowId
                && (
                    sourceStartTime == nil
                        || (
                            RecurrenceMaterialIdentity
                                .canonicalTimeBitPattern(
                                    entry.boundaryStart
                                )
                                == sourceStartTime.map(
                                    RecurrenceMaterialIdentity
                                        .canonicalTimeBitPattern
                                )
                            && RecurrenceMaterialIdentity
                                .canonicalTimeBitPattern(
                                    entry.boundaryEnd
                                )
                                == sourceEndTime.map(
                                    RecurrenceMaterialIdentity
                                        .canonicalTimeBitPattern
                                )
                        )
                )
                ? key
                : nil
        }
        for key in sourceKeys {
            entries.removeValue(forKey: key)
        }
        var revoked = sourceKeys.count

        if let showId = Self.normalizedIdentifier(showId),
           let fingerprint,
           !fingerprint.isZero {
            let fingerprintKey = Key(
                showId: showId,
                fingerprint: fingerprint
            )
            if fingerprintRevocations[
                fingerprintKey,
                default: .distantPast
            ] <= at {
                fingerprintRevocations[fingerprintKey] = at
            }
            if entries.removeValue(forKey: fingerprintKey) != nil {
                revoked += 1
            }
        }
        return revoked
    }

    func fetchRevokedFingerprints(
        showId: String
    ) async throws -> [RepeatedAdFingerprint] {
        guard let showId = Self.normalizedIdentifier(showId) else {
            return []
        }
        return fingerprintRevocations.keys
            .filter { $0.showId == showId }
            .map(\.fingerprint)
            .sorted { $0.bits < $1.bits }
    }

    func fetchAll(showId: String) async throws -> [RepeatedAdCacheEntry] {
        guard let showId = Self.normalizedIdentifier(showId) else {
            return []
        }
        return entries.values
            .filter { $0.showId == showId }
            .sorted {
                if $0.lastSeenAt != $1.lastSeenAt {
                    return $0.lastSeenAt > $1.lastSeenAt
                }
                return $0.fingerprint.bits < $1.fingerprint.bits
            }
    }

    func touch(showId: String, fingerprint: RepeatedAdFingerprint, at: Date) async throws {
        guard let showId = Self.normalizedIdentifier(showId),
              at.timeIntervalSince1970.isFinite,
              at.timeIntervalSince1970 >= 0 else {
            return
        }
        let key = Key(showId: showId, fingerprint: fingerprint)
        guard let existing = entries[key],
              at >= existing.lastSeenAt else {
            return
        }
        entries[key] = RepeatedAdCacheEntry(
            showId: existing.showId,
            fingerprint: existing.fingerprint,
            boundaryStart: existing.boundaryStart,
            boundaryEnd: existing.boundaryEnd,
            confidence: existing.confidence,
            lastSeenAt: at,
            learningSource: existing.learningSource,
            learningLifecycle: existing.learningLifecycle,
            sourceAssetId: existing.sourceAssetId,
            sourceWindowId: existing.sourceWindowId,
            producerRevision: existing.producerRevision
        )
    }

    @discardableResult
    func delete(
        showId: String,
        fingerprint: RepeatedAdFingerprint
    ) async throws -> Bool {
        guard let showId = Self.normalizedIdentifier(showId) else {
            return false
        }
        return entries.removeValue(
            forKey: Key(showId: showId, fingerprint: fingerprint)
        ) != nil
    }

    @discardableResult
    func delete(
        sourceAssetId: String,
        sourceWindowId: String
    ) async throws -> Int {
        guard let sourceAssetId = Self.normalizedIdentifier(sourceAssetId),
              let sourceWindowId = Self.normalizedIdentifier(sourceWindowId)
        else {
            return 0
        }
        let matchingKeys = entries.compactMap { key, entry in
            entry.sourceAssetId == sourceAssetId
                && entry.sourceWindowId == sourceWindowId
                ? key
                : nil
        }
        for key in matchingKeys {
            entries.removeValue(forKey: key)
        }
        return matchingKeys.count
    }

    @discardableResult
    func deleteIfUnchanged(
        _ entry: RepeatedAdCacheEntry
    ) async throws -> Bool {
        guard let showId = Self.normalizedIdentifier(entry.showId),
              Self.normalizedIdentifier(entry.producerRevision)
                == entry.producerRevision else {
            return false
        }
        let key = Key(showId: showId, fingerprint: entry.fingerprint)
        guard entries[key]?.producerRevision == entry.producerRevision else {
            return false
        }
        entries.removeValue(forKey: key)
        return true
    }

    func count(showId: String) async throws -> Int {
        guard let showId = Self.normalizedIdentifier(showId) else {
            return 0
        }
        return entries.values.filter { $0.showId == showId }.count
    }

    func totalCount() async throws -> Int { entries.count }

    func evictOldest(showId: String) async throws -> Bool {
        evictOldestEntry(showId: showId)
    }

    private func evictOldestEntry(showId: String) -> Bool {
        guard let showId = Self.normalizedIdentifier(showId) else {
            return false
        }
        let candidate = entries
            .filter { $0.value.showId == showId }
            .min {
                if $0.value.lastSeenAt != $1.value.lastSeenAt {
                    return $0.value.lastSeenAt < $1.value.lastSeenAt
                }
                return $0.key.fingerprint.bits < $1.key.fingerprint.bits
            }
        guard let candidate else { return false }
        entries.removeValue(forKey: candidate.key)
        return true
    }

    func evictOldestGlobal() async throws -> Bool {
        evictOldestGlobalEntry()
    }

    private func evictOldestGlobalEntry() -> Bool {
        let candidate = entries.min {
            if $0.value.lastSeenAt != $1.value.lastSeenAt {
                return $0.value.lastSeenAt < $1.value.lastSeenAt
            }
            if $0.key.showId != $1.key.showId {
                return $0.key.showId < $1.key.showId
            }
            return $0.key.fingerprint.bits < $1.key.fingerprint.bits
        }
        guard let candidate else { return false }
        entries.removeValue(forKey: candidate.key)
        return true
    }

    @discardableResult
    func purgeStale(olderThan: Date) async throws -> Int {
        guard olderThan.timeIntervalSince1970.isFinite else { return 0 }
        let stale = entries.filter { $0.value.lastSeenAt < olderThan }
        for key in stale.keys { entries.removeValue(forKey: key) }
        return stale.count
    }

    func clearEntries() async throws {
        entries.removeAll()
    }

    func appendOutcome(_ sample: RepeatedAdCacheOutcomeSample) async throws {
        guard sample.timestamp.timeIntervalSince1970.isFinite,
              sample.timestamp.timeIntervalSince1970 >= 0 else {
            throw RepeatedAdCacheError.storageFailure(
                "invalid outcome timestamp"
            )
        }
#if DEBUG
        if let barrier = appendOutcomeBarrierForTesting {
            await barrier()
        }
#endif
        outcomes.append(sample)
    }

    func fetchOutcomes(newerThan: Date) async throws -> [RepeatedAdCacheOutcomeSample] {
        guard newerThan.timeIntervalSince1970.isFinite else {
            throw RepeatedAdCacheError.storageFailure(
                "invalid outcome cutoff"
            )
        }
        return outcomes.filter { $0.timestamp >= newerThan }
    }

    @discardableResult
    func purgeOutcomes(olderThan: Date) async throws -> Int {
        guard olderThan.timeIntervalSince1970.isFinite else { return 0 }
        let before = outcomes.count
        outcomes.removeAll { $0.timestamp < olderThan }
        return before - outcomes.count
    }

    func clearOutcomes() async throws {
        outcomes.removeAll()
    }

    private static func lifecycleRank(
        _ lifecycle: CatalogLearningLifecycle
    ) -> Int {
        switch lifecycle {
        case .legacyUnconfirmed: 0
        case .consumed: 1
        case .explicitConfirmation: 2
        }
    }

    private static func normalizedIdentifier(
        _ value: String?
    ) -> String? {
        RecurrenceMaterialIdentity.canonicalIdentifier(value)
    }
}
