// RepeatedAdCacheV21MigrationTests.swift
// playhead-43ed: pin the V21 migration that introduces the
// `repeated_ad_cache` and `repeated_ad_cache_outcomes` tables.
//
// Coverage targets:
//   1. Fresh-DB migrate() leaves the schema at v21 with both tables and
//      both expected indexes present.
//   2. A v20-shaped DB climbs to v21 — pins the ladder boundary.
//   3. The migration is idempotent: running twice does not duplicate
//      indexes or fail.
//   4. Round-trip CRUD via the AnalysisStore-backed adapter writes,
//      reads, touches, evicts, and clears as advertised — pins the
//      adapter's wiring and the `INSERT ... ON CONFLICT` upsert.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("RepeatedAdCache V21 migration (playhead-43ed)")
struct RepeatedAdCacheV21MigrationTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "RepeatedAdCacheV21")
    }

    @Test("fresh DB migrate() lands repeated_ad_cache + outcomes tables and indexes at v21")
    func freshDbHasV21Tables() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 21)
        #expect(try probeTableExists(in: dir, table: "repeated_ad_cache"))
        #expect(try probeTableExists(in: dir, table: "repeated_ad_cache_outcomes"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_repeated_ad_cache_lastseen"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_repeated_ad_cache_show_lastseen"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_repeated_ad_cache_outcomes_ts"))
    }

    @Test("v20-seeded DB picks up repeated_ad_cache at v21")
    func seededV20ChainsToV21() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // First, real migrate to build full v21 shape.
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        #expect(try await bootstrap.schemaVersion() == 21)

        // Rewind: drop the v21 tables/indexes and reset _meta to '20' so
        // the v20 → v21 block runs on the next open.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            DROP INDEX IF EXISTS idx_repeated_ad_cache_lastseen;
            DROP INDEX IF EXISTS idx_repeated_ad_cache_show_lastseen;
            DROP INDEX IF EXISTS idx_repeated_ad_cache_outcomes_ts;
            DROP TABLE IF EXISTS repeated_ad_cache;
            DROP TABLE IF EXISTS repeated_ad_cache_outcomes;
            UPDATE _meta SET value = '20' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        // Sanity: the rewind actually removed the tables.
        #expect(!(try probeTableExists(in: dir, table: "repeated_ad_cache")))
        #expect(!(try probeTableExists(in: dir, table: "repeated_ad_cache_outcomes")))

        // Re-migrate via a fresh store.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 21)
        #expect(try probeTableExists(in: dir, table: "repeated_ad_cache"))
        #expect(try probeTableExists(in: dir, table: "repeated_ad_cache_outcomes"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_repeated_ad_cache_lastseen"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_repeated_ad_cache_show_lastseen"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_repeated_ad_cache_outcomes_ts"))
    }

    @Test("V21 migration is idempotent across resetMigratedPathsForTesting")
    func v21MigrationIsIdempotent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let v1 = try await store.schemaVersion()

        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()
        let v2 = try await store.schemaVersion()

        #expect(v1 == 21)
        #expect(v2 == 21)
    }

    // MARK: - Adapter round-trip

    @Test("adapter round-trips entries: upsert → fetchAll → touch → evictOldest")
    func adapterEntryRoundTrip() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)

        let fp1 = RepeatedAdFingerprint(bits: 0x1111_2222_3333_4444)
        let fp2 = RepeatedAdFingerprint(bits: 0x5555_6666_7777_8888)
        let t0 = Date(timeIntervalSince1970: 1_700_000_000)
        let t1 = Date(timeIntervalSince1970: 1_700_001_000)

        try await storage.upsert(.init(
            showId: "show-A", fingerprint: fp1,
            boundaryStart: 12.0, boundaryEnd: 42.0,
            confidence: 0.9, lastSeenAt: t0
        ))
        try await storage.upsert(.init(
            showId: "show-A", fingerprint: fp2,
            boundaryStart: 100, boundaryEnd: 130,
            confidence: 0.95, lastSeenAt: t1
        ))
        try await storage.upsert(.init(
            showId: "show-B", fingerprint: fp1,
            boundaryStart: 7, boundaryEnd: 17,
            confidence: 0.88, lastSeenAt: t0
        ))

        // fetchAll respects show boundary and returns DESC by lastSeenAt.
        let aRows = try await storage.fetchAll(showId: "show-A")
        #expect(aRows.count == 2)
        #expect(aRows[0].fingerprint == fp2) // newer first
        #expect(aRows[1].fingerprint == fp1)
        #expect(aRows[0].boundaryStart == 100)
        #expect(aRows[0].boundaryEnd == 130)
        #expect(aRows[0].confidence == 0.95)

        let bRows = try await storage.fetchAll(showId: "show-B")
        #expect(bRows.count == 1)
        #expect(bRows[0].showId == "show-B")
        #expect(bRows[0].fingerprint == fp1)

        // count + totalCount.
        #expect(try await storage.count(showId: "show-A") == 2)
        #expect(try await storage.count(showId: "show-B") == 1)
        #expect(try await storage.totalCount() == 3)

        // touch updates lastSeenAt — reordering fetchAll.
        let t2 = Date(timeIntervalSince1970: 1_700_002_000)
        try await storage.touch(showId: "show-A", fingerprint: fp1, at: t2)
        let aRowsAfterTouch = try await storage.fetchAll(showId: "show-A")
        #expect(aRowsAfterTouch[0].fingerprint == fp1) // touched, now newest
        #expect(aRowsAfterTouch[0].lastSeenAt.timeIntervalSince1970 == 1_700_002_000)

        // evictOldest removes the LRU row for the show.
        let evicted = try await storage.evictOldest(showId: "show-A")
        #expect(evicted == true)
        let aRowsAfterEvict = try await storage.fetchAll(showId: "show-A")
        #expect(aRowsAfterEvict.count == 1)
        #expect(aRowsAfterEvict[0].fingerprint == fp1) // touched survivor

        // upsert with same primary key updates in place (no second row).
        try await storage.upsert(.init(
            showId: "show-A", fingerprint: fp1,
            boundaryStart: 999, boundaryEnd: 1099,
            confidence: 0.99, lastSeenAt: t2
        ))
        let updated = try await storage.fetchAll(showId: "show-A")
        #expect(updated.count == 1)
        #expect(updated[0].boundaryStart == 999)
        #expect(updated[0].boundaryEnd == 1099)
        #expect(updated[0].confidence == 0.99)

        // purgeStale removes rows older than threshold.
        try await storage.upsert(.init(
            showId: "show-C", fingerprint: fp2,
            boundaryStart: 0, boundaryEnd: 1,
            confidence: 0.85,
            lastSeenAt: Date(timeIntervalSince1970: 1_000_000_000)
        ))
        let purged = try await storage.purgeStale(olderThan: t0)
        #expect(purged == 1) // only show-C row was older than t0
        #expect(try await storage.count(showId: "show-C") == 0)

        // evictOldestGlobal works across shows.
        try await storage.upsert(.init(
            showId: "show-D", fingerprint: fp1,
            boundaryStart: 0, boundaryEnd: 1,
            confidence: 0.85,
            lastSeenAt: Date(timeIntervalSince1970: 1_500_000_000)
        ))
        let globalEvicted = try await storage.evictOldestGlobal()
        #expect(globalEvicted == true)
        #expect(try await storage.count(showId: "show-D") == 0)

        // clearEntries wipes everything.
        try await storage.clearEntries()
        #expect(try await storage.totalCount() == 0)
    }

    @Test("adapter round-trips outcomes: append → fetch by window → purge")
    func adapterOutcomeRoundTrip() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)

        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let oneDay: TimeInterval = 86_400
        let fifteenDaysAgo = now.addingTimeInterval(-15 * oneDay)
        let fiveDaysAgo = now.addingTimeInterval(-5 * oneDay)
        let oneDayAgo = now.addingTimeInterval(-oneDay)

        try await storage.appendOutcome(.init(timestamp: fifteenDaysAgo, isHit: false))
        try await storage.appendOutcome(.init(timestamp: fiveDaysAgo, isHit: true))
        try await storage.appendOutcome(.init(timestamp: oneDayAgo, isHit: true))
        try await storage.appendOutcome(.init(timestamp: now, isHit: false))

        // 14-day window keeps 3 of the 4 samples.
        let window = now.addingTimeInterval(-14 * oneDay)
        let recent = try await storage.fetchOutcomes(newerThan: window)
        #expect(recent.count == 3)
        #expect(recent.allSatisfy { $0.timestamp >= window })

        let purged = try await storage.purgeOutcomes(olderThan: window)
        #expect(purged == 1)
        let afterPurge = try await storage.fetchOutcomes(newerThan: Date.distantPast)
        #expect(afterPurge.count == 3)

        try await storage.clearOutcomes()
        let afterClear = try await storage.fetchOutcomes(newerThan: Date.distantPast)
        #expect(afterClear.isEmpty)
    }
}
