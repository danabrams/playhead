// RepeatedAdCacheV21MigrationTests.swift
// playhead-43ed: pin the V21 migration that introduces the
// `repeated_ad_cache` and `repeated_ad_cache_outcomes` tables.
//
// Note (q45f.1): the head schema is now v22 (ad_listen_rewinds was added
// after the v21 repeated_ad_cache work). These tests exercise the V21
// migration boundary specifically, but the post-migrate schemaVersion
// they assert against is the *current head* (v22), since migrate() always
// climbs the full ladder. The V21-specific structural checks (tables,
// indexes) are what pin this file's invariants.
//
// Coverage targets:
//   1. Fresh-DB migrate() reaches the head schema with both v21 tables
//      and both expected indexes present.
//   2. A v20-shaped DB climbs through v21 to head — pins the ladder
//      boundary.
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

    @Test("fresh DB migrate() lands repeated_ad_cache + outcomes tables and indexes (V21 contribution)")
    func freshDbHasV21Tables() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: "repeated_ad_cache"))
        #expect(try probeTableExists(in: dir, table: "repeated_ad_cache_outcomes"))
        #expect(try probeTableExists(
            in: dir,
            table: "repeated_ad_cache_revocations"
        ))
        #expect(try probeTableExists(
            in: dir,
            table: "repeated_ad_cache_fingerprint_revocations"
        ))
        #expect(try probeIndexExists(in: dir, indexName: "idx_repeated_ad_cache_lastseen"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_repeated_ad_cache_show_lastseen"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_repeated_ad_cache_outcomes_ts"))
        for column in [
            "learningSource",
            "learningLifecycle",
            "sourceAssetId",
            "sourceWindowId",
            "producerRevision",
        ] {
            #expect(try probeColumnExists(
                in: dir,
                table: "repeated_ad_cache",
                column: column
            ))
        }
    }

    @Test("v20-seeded DB picks up repeated_ad_cache via the v20→v21 step")
    func seededV20ChainsToV21() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // First, real migrate to build the full head shape (post-V21 + V22).
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        #expect(try await bootstrap.schemaVersion() == AnalysisStore.currentSchemaVersion)

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

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
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

        #expect(v1 == AnalysisStore.currentSchemaVersion)
        #expect(v2 == AnalysisStore.currentSchemaVersion)
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
            confidence: 0.95, lastSeenAt: t1,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-confirmed",
            sourceWindowId: "window-confirmed"
        ))
        try await storage.upsert(.init(
            showId: "show-B", fingerprint: fp1,
            boundaryStart: 7, boundaryEnd: 17,
            confidence: 0.88, lastSeenAt: t0
        ))
        try await storage.upsert(.init(
            showId: "show-A", fingerprint: fp2,
            boundaryStart: 999, boundaryEnd: 1099,
            confidence: 0.99, lastSeenAt: t1,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "asset-consumed",
            sourceWindowId: "window-consumed"
        ))
        #expect(try await storage.upsert(.init(
            showId: "show-A", fingerprint: fp2,
            boundaryStart: 777, boundaryEnd: 888,
            confidence: 1, lastSeenAt: t0,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-stale-explicit",
            sourceWindowId: "window-stale-explicit"
        )) == false)

        // fetchAll respects show boundary and returns DESC by lastSeenAt.
        let aRows = try await storage.fetchAll(showId: "show-A")
        #expect(aRows.count == 2)
        #expect(aRows[0].fingerprint == fp2) // newer first
        #expect(aRows[1].fingerprint == fp1)
        #expect(aRows[0].boundaryStart == 100)
        #expect(aRows[0].boundaryEnd == 130)
        #expect(aRows[0].confidence == 0.95)
        #expect(aRows[0].learningSource == .confirmedSuggestion)
        #expect(aRows[0].learningLifecycle == .explicitConfirmation)
        #expect(aRows[0].sourceAssetId == "asset-confirmed")
        #expect(aRows[0].sourceWindowId == "window-confirmed")

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

        // Exact deletion is the correction/revocation storage seam.
        #expect(try await storage.delete(
            showId: "show-B",
            fingerprint: fp1
        ))
        #expect(try await storage.count(showId: "show-B") == 0)

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

    @Test("capacity eviction failure rolls back the cache admission")
    func capacityEvictionFailureRollsBackUpsert() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)
        let original = RepeatedAdCacheEntry(
            showId: "show-capacity-rollback",
            fingerprint: RepeatedAdFingerprint(bits: 0x1000),
            boundaryStart: 10,
            boundaryEnd: 40,
            confidence: 0.95,
            lastSeenAt: Date(timeIntervalSince1970: 10),
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "capacity-original-asset",
            sourceWindowId: "capacity-original-window"
        )
        #expect(try await storage.upsert(original))
        try await store.execForTesting("""
            CREATE TRIGGER fail_repeated_cache_eviction
            BEFORE DELETE ON repeated_ad_cache
            BEGIN
                SELECT RAISE(ABORT, 'injected eviction failure');
            END
            """)

        let replacement = RepeatedAdCacheEntry(
            showId: "show-capacity-rollback",
            fingerprint: RepeatedAdFingerprint(bits: 0x2000),
            boundaryStart: 50,
            boundaryEnd: 80,
            confidence: 0.96,
            lastSeenAt: Date(timeIntervalSince1970: 20),
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "capacity-replacement-asset",
            sourceWindowId: "capacity-replacement-window"
        )
        await #expect(throws: AnalysisStoreError.self) {
            _ = try await storage.upsertEnforcingCapacity(
                replacement,
                perShowCap: 1,
                globalCap: 10
            )
        }

        #expect(
            try await storage.fetchAll(showId: "show-capacity-rollback")
                == [original]
        )
    }

    @Test("correction revocation failure rolls back tombstones and deletes")
    func correctionRevocationFailureRollsBackAtomically() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)
        let original = RepeatedAdCacheEntry(
            showId: "show-revocation-rollback",
            fingerprint: RepeatedAdFingerprint(bits: 0x3000),
            boundaryStart: 10,
            boundaryEnd: 40,
            confidence: 0.95,
            lastSeenAt: Date(timeIntervalSince1970: 10),
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "revocation-rollback-asset",
            sourceWindowId: "revocation-rollback-window",
            producerRevision: "original-revision"
        )
        #expect(try await storage.upsert(original))
        try await store.execForTesting("""
            CREATE TRIGGER fail_repeated_cache_creative_revocation
            BEFORE INSERT ON repeated_ad_cache_fingerprint_revocations
            BEGIN
                SELECT RAISE(ABORT, 'injected creative revocation failure');
            END
            """)

        await #expect(throws: AnalysisStoreError.self) {
            _ = try await storage.revokeMatchesAtomically(
                showId: original.showId,
                fingerprint: original.fingerprint,
                sourceAssetId: "revocation-rollback-asset",
                sourceWindowId: "revocation-rollback-window",
                source: .manualVeto,
                at: Date(timeIntervalSince1970: 20)
            )
        }
        try await store.execForTesting(
            "DROP TRIGGER fail_repeated_cache_creative_revocation"
        )

        #expect(
            try await storage.fetchAll(showId: original.showId) == [original],
            "the source delete must roll back with the failed tombstone"
        )
        #expect(try await storage.upsert(.init(
            showId: original.showId,
            fingerprint: RepeatedAdFingerprint(bits: 0x4000),
            boundaryStart: 50,
            boundaryEnd: 80,
            confidence: 0.96,
            lastSeenAt: Date(timeIntervalSince1970: 30),
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "revocation-rollback-asset",
            sourceWindowId: "revocation-rollback-window",
            producerRevision: "replacement-revision"
        )), "the source tombstone must roll back too")
    }

    @Test("SQLite source tombstones are exact-geometry scoped")
    func persistentRevocationPreservesSameIDReplacement() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)
        let service = RepeatedAdCacheService(storage: storage)
        let oldFingerprint = RepeatedAdFingerprint(bits: 0x1000)
        let replacementFingerprint = RepeatedAdFingerprint(bits: 0x8000)
        for (fingerprint, start, end) in [
            (oldFingerprint, 10.0, 40.0),
            (replacementFingerprint, 50.0, 80.0),
        ] {
            #expect(try await service.store(
                showId: "show-persistent-exact-geometry",
                fingerprint: fingerprint,
                boundaryStart: start,
                boundaryEnd: end,
                confidence: 1,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "persistent-exact-geometry-asset",
                sourceWindowId: "reused-window-id"
            ))
        }

        #expect(try await service.revokeMatches(
            showId: nil,
            fingerprint: nil,
            sourceAssetId: "persistent-exact-geometry-asset",
            sourceWindowId: "reused-window-id",
            sourceStartTime: 10,
            sourceEndTime: 40,
            source: .manualVeto
        ) == 1)
        #expect(
            try await storage.fetchAll(
                showId: "show-persistent-exact-geometry"
            ).map(\.fingerprint) == [replacementFingerprint]
        )

        let reopened = RepeatedAdCacheService(
            storage: AnalysisStoreRepeatedAdCacheStorage(store: store)
        )
        #expect(try await reopened.store(
            showId: "show-persistent-exact-geometry",
            fingerprint: RepeatedAdFingerprint(bits: 0x4000),
            boundaryStart: 10,
            boundaryEnd: 40,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "persistent-exact-geometry-asset",
            sourceWindowId: "reused-window-id"
        ) == false)
        #expect(try await reopened.store(
            showId: "show-persistent-exact-geometry",
            fingerprint: RepeatedAdFingerprint(bits: 0x2000),
            boundaryStart: 90,
            boundaryEnd: 120,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "persistent-exact-geometry-asset",
            sourceWindowId: "reused-window-id"
        ))
    }

    @Test("SQLite signed-zero source geometry shares one tombstone")
    func persistentSignedZeroGeometryCannotResurrect() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)
        let service = RepeatedAdCacheService(storage: storage)
        #expect(try await service.store(
            showId: "show-persistent-signed-zero",
            fingerprint: RepeatedAdFingerprint(bits: 0x1000),
            boundaryStart: 0.0,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "persistent-signed-zero-asset",
            sourceWindowId: "persistent-signed-zero-window"
        ))
        #expect(try await service.revokeMatches(
            showId: nil,
            fingerprint: nil,
            sourceAssetId: "persistent-signed-zero-asset",
            sourceWindowId: "persistent-signed-zero-window",
            sourceStartTime: -0.0,
            sourceEndTime: 30,
            source: .manualVeto
        ) == 1)
        #expect(
            try await storage.fetchAll(
                showId: "show-persistent-signed-zero"
            ).isEmpty
        )

        let reopened = RepeatedAdCacheService(
            storage: AnalysisStoreRepeatedAdCacheStorage(store: store)
        )
        #expect(try await reopened.store(
            showId: "show-persistent-signed-zero",
            fingerprint: RepeatedAdFingerprint(bits: 0x2000),
            boundaryStart: 0.0,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "persistent-signed-zero-asset",
            sourceWindowId: "persistent-signed-zero-window"
        ) == false)

        let legacyKey = try #require(
            RecurrenceMaterialIdentity
                .legacyNegativeZeroTombstoneWindowKey(
                    sourceWindowId: "persistent-legacy-zero-window",
                    sourceStartTime: -0.0,
                    sourceEndTime: 30
                )
        )
        #expect(try await storage.recordRevocation(
            sourceAssetId: "persistent-legacy-zero-asset",
            sourceWindowId: legacyKey,
            source: .manualVeto,
            at: Date(timeIntervalSince1970: 10)
        ))
        let legacyReopened = RepeatedAdCacheService(
            storage: AnalysisStoreRepeatedAdCacheStorage(store: store)
        )
        #expect(try await legacyReopened.store(
            showId: "show-persistent-legacy-zero",
            fingerprint: RepeatedAdFingerprint(bits: 0x4000),
            boundaryStart: 0.0,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "persistent-legacy-zero-asset",
            sourceWindowId: "persistent-legacy-zero-window"
        ) == false)
    }

    @Test("persistent storage rejects noncanonical identities and producer revisions")
    func persistentStorageRejectsNoncanonicalIdentity() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)

        for (index, material) in [
            (
                " show-invalid-producer ",
                "invalid-producer-asset",
                "invalid-producer-window",
                "revision"
            ),
            (
                "show-invalid-producer",
                " invalid-producer-asset ",
                "invalid-producer-window",
                "revision"
            ),
            (
                "show-invalid-producer",
                "invalid-producer-asset",
                " invalid-producer-window ",
                "revision"
            ),
            (
                "show-invalid-producer",
                "invalid-producer-asset",
                "invalid-producer-window",
                " revision-with-whitespace "
            ),
            (
                "show-invalid-producer\u{0}other",
                "invalid-producer-asset",
                "invalid-producer-window",
                "revision"
            ),
            (
                "show-invalid-producer",
                "invalid-producer-asset\u{0}other",
                "invalid-producer-window",
                "revision"
            ),
            (
                "show-invalid-producer",
                "invalid-producer-asset",
                "invalid-producer-window\u{0}other",
                "revision"
            ),
            (
                "show-invalid-producer",
                "invalid-producer-asset",
                "invalid-producer-window",
                "revision\u{0}other"
            ),
        ].enumerated() {
            #expect(try await storage.upsert(.init(
                showId: material.0,
                fingerprint: RepeatedAdFingerprint(
                    bits: UInt64(0x5000 + index)
                ),
                boundaryStart: 10,
                boundaryEnd: 40,
                confidence: 0.95,
                lastSeenAt: Date(timeIntervalSince1970: 10),
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: material.1,
                sourceWindowId: material.2,
                producerRevision: material.3
            )) == false)
        }
        #expect(try await storage.totalCount() == 0)
    }

    @Test("SQLite lifecycle upgrade preserves a newer LRU timestamp")
    func persistentLifecycleUpgradeKeepsRecencyMonotonic() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)
        let fingerprint = RepeatedAdFingerprint(bits: 0x3456)
        #expect(try await storage.upsert(.init(
            showId: "show-persistent-clock",
            fingerprint: fingerprint,
            boundaryStart: 10,
            boundaryEnd: 40,
            confidence: 0.95,
            lastSeenAt: Date(timeIntervalSince1970: 200),
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "persistent-consumed-asset",
            sourceWindowId: "persistent-consumed-window"
        )))
        #expect(try await storage.upsert(.init(
            showId: "show-persistent-clock",
            fingerprint: fingerprint,
            boundaryStart: 20,
            boundaryEnd: 50,
            confidence: 1,
            lastSeenAt: Date(timeIntervalSince1970: 100),
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "persistent-explicit-asset",
            sourceWindowId: "persistent-explicit-window"
        )))

        let retained = try #require(
            try await storage.fetchAll(showId: "show-persistent-clock").first
        )
        #expect(retained.lastSeenAt == Date(timeIntervalSince1970: 200))
        #expect(retained.learningLifecycle == .explicitConfirmation)
        #expect(retained.boundaryStart == 20)
        #expect(retained.sourceAssetId == "persistent-explicit-asset")
    }

    @Test("corrupt outcome numerics fail closed instead of affecting auto-disable")
    func corruptOutcomeRowsFailClosed() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(
            sqlite3_open_v2(
                dbURL.path,
                &db,
                SQLITE_OPEN_READWRITE,
                nil
            ) == SQLITE_OK
        )
        defer { sqlite3_close_v2(db) }
        var insert: OpaquePointer?
        #expect(sqlite3_prepare_v2(
            db,
            """
            INSERT INTO repeated_ad_cache_outcomes (timestamp, isHit)
            VALUES (?, ?)
            """,
            -1,
            &insert,
            nil
        ) == SQLITE_OK)
        defer { sqlite3_finalize(insert) }
        sqlite3_bind_double(insert, 1, .infinity)
        sqlite3_bind_int(insert, 2, 2)
        #expect(sqlite3_step(insert) == SQLITE_DONE)

        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)
        await #expect(throws: AnalysisStoreError.self) {
            _ = try await storage.fetchOutcomes(
                newerThan: Date(timeIntervalSince1970: 0)
            )
        }
    }

    @Test("malformed fingerprint revocation rows fail closed")
    func malformedFingerprintRevocationFailsClosed() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.execForTesting("""
            INSERT INTO repeated_ad_cache_fingerprint_revocations
                (showId, fingerprint, revokedAt, sourceAssetId,
                 sourceWindowId, revocationSource)
            VALUES
                ('show-corrupt-veto', 'not-a-fingerprint', 1800000000,
                 'source-asset', 'source-window', 'manualVeto')
            """)

        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)
        await #expect(throws: AnalysisStoreError.self) {
            _ = try await storage.fetchRevokedFingerprints(
                showId: "show-corrupt-veto"
            )
        }
    }

    @Test("persistent source tombstone suppresses delayed learning after reopen")
    func revocationTombstonePersistsAcrossServiceRecreation() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)
        let first = RepeatedAdCacheService(storage: storage)
        _ = try await first.revokeMatches(
            showId: nil,
            fingerprint: nil,
            sourceAssetId: "persistent-race-asset",
            sourceWindowId: "persistent-race-window",
            source: .manualVeto,
            at: Date(timeIntervalSince1970: 1_800_000_000)
        )

        let reopened = RepeatedAdCacheService(
            storage: AnalysisStoreRepeatedAdCacheStorage(store: store)
        )
        let stored = try await reopened.store(
            showId: "show-persistent-race",
            fingerprint: RepeatedAdFingerprint(bits: 0x1234),
            boundaryStart: 12,
            boundaryEnd: 42,
            confidence: 0.99,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "persistent-race-asset",
            sourceWindowId: "persistent-race-window"
        )

        #expect(stored == false)
        #expect(try await storage.totalCount() == 0)
    }

    @Test("persistent fingerprint tombstone blocks second-best resurrection after reopen")
    func fingerprintRevocationPersistsAcrossServiceRecreation() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)
        let config = RepeatedAdCacheConfig.production
        let corrected = RepeatedAdFingerprint(bits: 0x1000)
        let nearby = RepeatedAdFingerprint(bits: 0x1007)
        let bridge = RepeatedAdFingerprint(bits: 0x103f)
        #expect(
            corrected.hammingDistance(to: nearby)
                == config.hammingDistanceThreshold
        )
        #expect(
            nearby.hammingDistance(to: bridge)
                == config.hammingDistanceThreshold
        )
        #expect(
            corrected.hammingDistance(to: bridge)
                > config.hammingDistanceThreshold
        )
        let first = RepeatedAdCacheService(config: config, storage: storage)
        #expect(try await first.store(
            showId: "show-fingerprint-veto",
            fingerprint: nearby,
            boundaryStart: 12,
            boundaryEnd: 42,
            confidence: 0.99,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "nearby-asset",
            sourceWindowId: "nearby-window"
        ))
        _ = try await first.revokeMatches(
            showId: "show-fingerprint-veto",
            fingerprint: corrected,
            sourceAssetId: "corrected-asset",
            sourceWindowId: "corrected-window",
            source: .manualVeto,
            at: Date(timeIntervalSince1970: 1_800_000_000)
        )

        let reopened = RepeatedAdCacheService(
            config: config,
            storage: AnalysisStoreRepeatedAdCacheStorage(store: store)
        )
        for probe in [corrected, nearby, bridge] {
            if case .hit = try await reopened.lookup(
                showId: "show-fingerprint-veto",
                fingerprint: probe
            ) {
                Issue.record(
                    "persisted creative veto must block direct, nearby, and retained-bridge probes"
                )
            }
        }
        #expect(try await reopened.store(
            showId: "show-fingerprint-veto",
            fingerprint: corrected,
            boundaryStart: 13,
            boundaryEnd: 43,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "later-asset",
            sourceWindowId: "later-window"
        ) == false)
        #expect(
            try await storage.count(showId: "show-fingerprint-veto") == 1,
            "nearby persisted evidence remains for audit but cannot promote"
        )
    }

    @Test("persistent revocation deletes an existing source row without a fingerprint")
    func revocationDeletesExistingSourceWithoutRefingerprint() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)
        let cache = RepeatedAdCacheService(storage: storage)
        let fingerprint = RepeatedAdFingerprint(bits: 0x5678)
        #expect(try await cache.store(
            showId: "show-persistent-source",
            fingerprint: fingerprint,
            boundaryStart: 12,
            boundaryEnd: 42,
            confidence: 0.99,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "persistent-source-asset",
            sourceWindowId: "persistent-source-window"
        ))

        let revoked = try await cache.revokeMatches(
            showId: nil,
            fingerprint: nil,
            sourceAssetId: "persistent-source-asset",
            sourceWindowId: "persistent-source-window",
            source: .manualVeto,
            at: Date(timeIntervalSince1970: 1_800_000_001)
        )

        #expect(revoked == 1)
        #expect(try await storage.totalCount() == 0)
        #expect(try await cache.store(
            showId: "show-persistent-source",
            fingerprint: fingerprint,
            boundaryStart: 12,
            boundaryEnd: 42,
            confidence: 0.99,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "persistent-source-asset",
            sourceWindowId: "persistent-source-window"
        ) == false)
    }
}
