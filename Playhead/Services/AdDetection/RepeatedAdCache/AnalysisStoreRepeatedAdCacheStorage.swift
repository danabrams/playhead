// AnalysisStoreRepeatedAdCacheStorage.swift
// Production storage backing for `RepeatedAdCacheService`.
//
// Delegates every method to actor-isolated CRUD on `AnalysisStore`.
// The protocol's `async throws` shape lines up directly with the
// AnalysisStore actor's implicit `async` boundary, so each call here
// is a one-line forward.
//
// Why an adapter (rather than `AnalysisStore: RepeatedAdCacheStorage`):
//   1. AnalysisStore is a 10k-line god actor used by half the codebase.
//      Adding a protocol conformance there would pull every consumer of
//      RepeatedAdCacheStorage into an `import` graph that already
//      includes AnalysisStore. The adapter keeps the dependency surface
//      narrow.
//   2. Tests of RepeatedAdCacheService can swap in
//      `InMemoryRepeatedAdCacheStorage` without standing up SQLite.

import Foundation

struct AnalysisStoreRepeatedAdCacheStorage: RepeatedAdCacheStorage {

    let store: AnalysisStore

    init(store: AnalysisStore) {
        self.store = store
    }

    // MARK: Entries

    func upsert(_ entry: RepeatedAdCacheEntry) async throws {
        try await store.repeatedAdCacheUpsert(entry)
    }

    func fetchAll(showId: String) async throws -> [RepeatedAdCacheEntry] {
        try await store.repeatedAdCacheFetchAll(showId: showId)
    }

    func touch(showId: String, fingerprint: RepeatedAdFingerprint, at: Date) async throws {
        try await store.repeatedAdCacheTouch(
            showId: showId,
            fingerprintHex: fingerprint.hexString,
            at: at
        )
    }

    func count(showId: String) async throws -> Int {
        try await store.repeatedAdCacheCount(showId: showId)
    }

    func totalCount() async throws -> Int {
        try await store.repeatedAdCacheTotalCount()
    }

    func evictOldest(showId: String) async throws -> Bool {
        try await store.repeatedAdCacheEvictOldest(showId: showId)
    }

    func evictOldestGlobal() async throws -> Bool {
        try await store.repeatedAdCacheEvictOldestGlobal()
    }

    @discardableResult
    func purgeStale(olderThan: Date) async throws -> Int {
        try await store.repeatedAdCachePurgeStale(olderThan: olderThan)
    }

    func clearEntries() async throws {
        try await store.repeatedAdCacheClearEntries()
    }

    // MARK: Outcome samples

    func appendOutcome(_ sample: RepeatedAdCacheOutcomeSample) async throws {
        try await store.repeatedAdCacheAppendOutcome(sample)
    }

    func fetchOutcomes(newerThan: Date) async throws -> [RepeatedAdCacheOutcomeSample] {
        try await store.repeatedAdCacheFetchOutcomes(newerThan: newerThan)
    }

    @discardableResult
    func purgeOutcomes(olderThan: Date) async throws -> Int {
        try await store.repeatedAdCachePurgeOutcomes(olderThan: olderThan)
    }

    func clearOutcomes() async throws {
        try await store.repeatedAdCacheClearOutcomes()
    }
}
