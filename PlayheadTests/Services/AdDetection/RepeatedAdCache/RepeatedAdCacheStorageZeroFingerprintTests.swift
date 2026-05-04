// RepeatedAdCacheStorageZeroFingerprintTests.swift
// review/v0.5-head-polish C2 MT-3 — pin the storage-boundary
// drop-on-zero invariant on BOTH `RepeatedAdCacheStorage`
// implementations.
//
// Background: review/v0.5-head-polish L3 added defense-in-depth so a
// zero-fingerprint entry is silently dropped at the storage boundary
// even if a future call path skips the service-level guard. The cycle-2
// reviewer flagged that we have no behavioral test that pins this drop
// — the only existing coverage is a doc comment in
// `AnalysisStoreRepeatedAdCacheStorage.upsert(_:)`. A future refactor
// could remove the guard and only the comment would still talk about
// it.
//
// These tests construct a zero-fingerprint `RepeatedAdCacheEntry`,
// upsert it, then assert the storage stayed empty:
//   * `count(showId:) == 0` — direct denominator check.
//   * `fetchAll(showId:).isEmpty` — confirms no row leaked through any
//     other read path either.
//
// Two storage layers are exercised:
//   1. `AnalysisStoreRepeatedAdCacheStorage` (production, SQLite-backed)
//   2. `InMemoryRepeatedAdCacheStorage` (test-mirror that exists to
//      preserve drop-on-zero parity with production).
//
// If either layer regresses (someone deletes the `guard !isZero`),
// these tests fail loudly.

import Foundation
import Testing

@testable import Playhead

@Suite("RepeatedAdCacheStorage drop-on-zero (review/v0.5-head-polish C2 MT-3)")
struct RepeatedAdCacheStorageZeroFingerprintTests {

    private static let testShowId = "show-zero-fp"

    private static func makeZeroEntry() -> RepeatedAdCacheEntry {
        RepeatedAdCacheEntry(
            showId: testShowId,
            fingerprint: .zero,
            boundaryStart: 12.0,
            boundaryEnd: 42.0,
            confidence: 0.99,
            lastSeenAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    @Test("AnalysisStore-backed storage silently drops a zero-fingerprint upsert")
    func productionStorageDropsZeroFingerprint() async throws {
        let dir = try makeTempDir(prefix: "RepeatedAdCacheZeroFP")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)

        try await storage.upsert(Self.makeZeroEntry())

        #expect(try await storage.count(showId: Self.testShowId) == 0)
        #expect(try await storage.totalCount() == 0)
        #expect(try await storage.fetchAll(showId: Self.testShowId).isEmpty)
    }

    @Test("InMemory storage silently drops a zero-fingerprint upsert (mirrors production)")
    func inMemoryStorageDropsZeroFingerprint() async throws {
        let storage = InMemoryRepeatedAdCacheStorage()

        try await storage.upsert(Self.makeZeroEntry())

        #expect(try await storage.count(showId: Self.testShowId) == 0)
        #expect(try await storage.totalCount() == 0)
        #expect(try await storage.fetchAll(showId: Self.testShowId).isEmpty)
    }
}
