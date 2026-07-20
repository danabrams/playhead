// EpisodeFingerprintsV27MigrationTests.swift
// playhead-xsdz.27: pin the V27 migration that introduces the
// `episode_fingerprints` table plus the store's read/write API and — the
// whole reason the table exists — the algorithm-version STALENESS contract.
//
// Coverage targets:
//   1. Fresh-DB migrate() reaches head with `episode_fingerprints` present.
//   2. A v26-shaped DB climbs through v27 (ladder boundary).
//   3. The migration is idempotent.
//   4. Schema round-trip: upsert → fetch returns the exact stream + metadata.
//   5. STALENESS: a stored fingerprint whose algorithmVersion != current
//      reads back as nil (re-fingerprint), NEVER Hamming-compared across
//      versions; `storedEpisodeFingerprintVersion` still surfaces the stale
//      value so callers can tell "stale" from "never fingerprinted".
//   6. Corrupt/mismatched blob → nil (treated as absent).
//   7. Retention: FK ON DELETE CASCADE — deleting the asset removes the
//      fingerprint row; an explicit delete works; a live asset is never
//      over-deleted. Inserting for a missing asset is rejected by the FK.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("episode_fingerprints V27 migration + store (playhead-xsdz.27)")
struct EpisodeFingerprintsV27MigrationTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "EpisodeFingerprintsV27")
    }

    private func makeAsset(id: String, fingerprint: String = "fp") -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: fingerprint,
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeRecord(
        assetId: String,
        version: UInt32 = ChromaFingerprinter.algorithmVersion,
        fingerprints: [UInt32] = [0x0000_0001, 0xDEAD_BEEF, 0xFFFF_FFFF, 0x0000_0000, 0x1234_5678],
        identity: String = "fp",
        capturedAt: Double = 1_700_000_000
    ) -> EpisodeFingerprintRecord {
        EpisodeFingerprintRecord(
            analysisAssetId: assetId,
            algorithmVersion: version,
            secondsPerFingerprint: ChromaFingerprinter.secondsPerFingerprint,
            fingerprints: fingerprints,
            sourceAudioIdentity: identity,
            capturedAt: capturedAt
        )
    }

    // MARK: - Migration ladder

    @Test("fresh DB migrate() lands episode_fingerprints at head")
    func freshDbHasV27Table() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // Head moved 27 → 28 (playhead-xsdz.36 rediff_refetch_state) → 29
        // (playhead-hdgk ad_windows edge-anchor columns); the V27 table pins
        // below are unchanged.
        #expect(AnalysisStore.currentSchemaVersion == 29)
        #expect(try probeTableExists(in: dir, table: "episode_fingerprints"))
    }

    @Test("v26-seeded DB picks up episode_fingerprints via the v26→v27 step")
    func seededV26ChainsToV27() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Build the full head shape, then regress to v26 by dropping the
        // v27 table and rewinding `_meta.schema_version`.
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        #expect(try probeTableExists(in: dir, table: "episode_fingerprints"))

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            DROP TABLE IF EXISTS episode_fingerprints;
            UPDATE _meta SET value = '26' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)
        #expect(!(try probeTableExists(in: dir, table: "episode_fingerprints")))

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: "episode_fingerprints"))

        // Migrated store accepts a row through the live CRUD path.
        try await store.insertAsset(makeAsset(id: "asset-v26-chain"))
        try await store.upsertEpisodeFingerprints(makeRecord(assetId: "asset-v26-chain"))
        #expect(try await store.episodeFingerprintCount() == 1)
    }

    @Test("V27 migration is idempotent across resetMigratedPathsForTesting")
    func v27MigrationIsIdempotent() async throws {
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
        #expect(try probeTableExists(in: dir, table: "episode_fingerprints"))
    }

    // MARK: - Schema round-trip

    @Test("upsert → fetch round-trips the exact stream + metadata")
    func schemaRoundTrip() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset(id: "asset-rt", fingerprint: "sha-rt"))

        let record = makeRecord(
            assetId: "asset-rt",
            fingerprints: [0, 1, 2, 0xFFFF_FFFF, 0x8000_0000, 0x7FFF_FFFF],
            identity: "sha-rt",
            capturedAt: 1_699_111_222
        )
        try await store.upsertEpisodeFingerprints(record)

        let fetched = try await store.fetchEpisodeFingerprints(assetId: "asset-rt")
        #expect(fetched == record)
        #expect(fetched?.fingerprints == record.fingerprints)
        #expect(fetched?.algorithmVersion == ChromaFingerprinter.algorithmVersion)
        #expect(fetched?.secondsPerFingerprint == ChromaFingerprinter.secondsPerFingerprint)
        #expect(fetched?.sourceAudioIdentity == "sha-rt")
        #expect(fetched?.capturedAt == 1_699_111_222)
    }

    @Test("upsert replaces the prior stream in place (one row per asset)")
    func upsertReplacesInPlace() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-replace"))

        try await store.upsertEpisodeFingerprints(
            makeRecord(assetId: "asset-replace", fingerprints: [1, 2, 3]))
        try await store.upsertEpisodeFingerprints(
            makeRecord(assetId: "asset-replace", fingerprints: [9, 8, 7, 6], capturedAt: 1_700_000_999))

        #expect(try await store.episodeFingerprintCount() == 1)
        let fetched = try await store.fetchEpisodeFingerprints(assetId: "asset-replace")
        #expect(fetched?.fingerprints == [9, 8, 7, 6])
        #expect(fetched?.capturedAt == 1_700_000_999)
    }

    @Test("fetch of an unfingerprinted asset returns nil")
    func fetchMissingReturnsNil() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-none"))
        #expect(try await store.fetchEpisodeFingerprints(assetId: "asset-none") == nil)
        #expect(try await store.storedEpisodeFingerprintVersion(assetId: "asset-none") == nil)
    }

    // MARK: - STALENESS contract (the whole point)

    @Test("stored fingerprint with a DIFFERENT algorithmVersion reads back as nil")
    func staleVersionReadsAsAbsent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-stale"))

        // Persist a stream stamped with a version that is NOT the current one.
        let staleVersion = ChromaFingerprinter.algorithmVersion &+ 1
        try await store.upsertEpisodeFingerprints(
            makeRecord(assetId: "asset-stale", version: staleVersion, fingerprints: [0xAAAA_AAAA, 0xBBBB_BBBB]))

        // The staleness gate: fetch returns nil (caller must re-fingerprint) —
        // the store NEVER hands back a cross-version stream to Hamming-compare.
        #expect(try await store.fetchEpisodeFingerprints(assetId: "asset-stale") == nil)

        // …but the row is still present on disk, and its stale version is
        // surfaced so callers can distinguish "stale" from "never captured".
        #expect(try await store.storedEpisodeFingerprintVersion(assetId: "asset-stale") == staleVersion)
        #expect(try await store.episodeFingerprintCount() == 1)
    }

    @Test("re-fingerprinting at the current version replaces a stale row and fetches")
    func refingerprintReplacesStale() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-refp"))

        let staleVersion = ChromaFingerprinter.algorithmVersion &+ 7
        try await store.upsertEpisodeFingerprints(
            makeRecord(assetId: "asset-refp", version: staleVersion, fingerprints: [1, 2]))
        #expect(try await store.fetchEpisodeFingerprints(assetId: "asset-refp") == nil)

        // Re-capture at the current version.
        let fresh = makeRecord(assetId: "asset-refp", fingerprints: [4, 5, 6])
        try await store.upsertEpisodeFingerprints(fresh)
        let fetched = try await store.fetchEpisodeFingerprints(assetId: "asset-refp")
        #expect(fetched == fresh)
        #expect(try await store.episodeFingerprintCount() == 1)
    }

    @Test("an older stale version (current - 1) also reads back as nil")
    func olderStaleVersionReadsAsAbsent() async throws {
        // Catches a naive `stored <= current` over-accept (the newer-version
        // test catches a `stored >= current` over-accept); together they pin
        // that the gate is EXACT equality — any inequality is stale.
        // `algorithmVersion` is UInt32, so `current - 1` only exists when
        // current >= 1 (it is, and v0 is never shipped); skip otherwise so
        // this stays honest if that ever changes.
        guard ChromaFingerprinter.algorithmVersion >= 1 else { return }
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-old"))
        let older = ChromaFingerprinter.algorithmVersion - 1
        try await store.upsertEpisodeFingerprints(
            makeRecord(assetId: "asset-old", version: older, fingerprints: [7, 7, 7]))
        #expect(try await store.fetchEpisodeFingerprints(assetId: "asset-old") == nil)
        #expect(try await store.storedEpisodeFingerprintVersion(assetId: "asset-old") == older)
    }

    // MARK: - Corrupt-blob defense

    @Test("a blob whose length disagrees with fingerprintCount reads as nil")
    func mismatchedCountReadsAsAbsent() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset(id: "asset-bad"))
        try await store.upsertEpisodeFingerprints(
            makeRecord(assetId: "asset-bad", fingerprints: [1, 2, 3, 4]))

        // Corrupt the stored count so it disagrees with the 16-byte blob.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        #expect(sqlite3_exec(db,
            "UPDATE episode_fingerprints SET fingerprintCount = 99 WHERE analysisAssetId = 'asset-bad'",
            nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        #expect(try await store.fetchEpisodeFingerprints(assetId: "asset-bad") == nil)
    }

    // MARK: - Retention / cascade

    @Test("deleting the asset cascades the fingerprint row away (FK ON DELETE CASCADE)")
    func deleteAssetCascadesFingerprints() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-cascade-A"))
        try await store.insertAsset(makeAsset(id: "asset-cascade-B"))
        try await store.upsertEpisodeFingerprints(makeRecord(assetId: "asset-cascade-A"))
        try await store.upsertEpisodeFingerprints(makeRecord(assetId: "asset-cascade-B"))
        #expect(try await store.episodeFingerprintCount() == 2)

        try await store.deleteAsset(id: "asset-cascade-A")

        // A's fingerprints are gone; B's survive (no over-deletion).
        #expect(try await store.fetchEpisodeFingerprints(assetId: "asset-cascade-A") == nil)
        #expect(try await store.storedEpisodeFingerprintVersion(assetId: "asset-cascade-A") == nil)
        #expect(try await store.fetchEpisodeFingerprints(assetId: "asset-cascade-B") != nil)
        #expect(try await store.episodeFingerprintCount() == 1)
    }

    @Test("explicit delete removes only the target row and is idempotent")
    func explicitDeleteIsScopedAndIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-del-A"))
        try await store.insertAsset(makeAsset(id: "asset-del-B"))
        try await store.upsertEpisodeFingerprints(makeRecord(assetId: "asset-del-A"))
        try await store.upsertEpisodeFingerprints(makeRecord(assetId: "asset-del-B"))

        try await store.deleteEpisodeFingerprints(assetId: "asset-del-A")
        #expect(try await store.fetchEpisodeFingerprints(assetId: "asset-del-A") == nil)
        #expect(try await store.fetchEpisodeFingerprints(assetId: "asset-del-B") != nil)
        // The asset row itself is untouched by a fingerprint delete.
        #expect(try await store.fetchAsset(id: "asset-del-A") != nil)

        // Idempotent: deleting again is a clean no-op.
        try await store.deleteEpisodeFingerprints(assetId: "asset-del-A")
        #expect(try await store.episodeFingerprintCount() == 1)
    }

    @Test("upserting fingerprints for a non-existent asset is rejected by the FK")
    func insertWithoutParentIsRejected() async throws {
        let store = try await makeTestStore()
        await #expect(throws: AnalysisStoreError.self) {
            try await store.upsertEpisodeFingerprints(makeRecord(assetId: "asset-ghost"))
        }
        #expect(try await store.episodeFingerprintCount() == 0)
    }
}
