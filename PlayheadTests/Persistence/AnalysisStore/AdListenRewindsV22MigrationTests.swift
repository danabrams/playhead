// AdListenRewindsV22MigrationTests.swift
// playhead-q45f.1: pin the V22 migration that introduces the
// `ad_listen_rewinds` table — the on-device event log of user
// listen-rewinds (taps of "Listen" on an auto-skipped window). The
// q45f counterfactual gate replays this log against frozen traces;
// without it, the gate is structurally unsatisfiable.
//
// Coverage targets:
//   1. Fresh-DB migrate() leaves the schema at v22 with the
//      `ad_listen_rewinds` table and both indexes present.
//   2. A v21-shaped DB climbs to v22 — pins the ladder boundary.
//   3. The migration is idempotent: running twice does not duplicate
//      indexes or fail.
//   4. Round-trip CRUD via `insertListenRewind` /
//      `fetchListenRewinds(forAssetId:)` writes and reads.
//   5. Fetch joins through `ad_windows.analysisAssetId` so the
//      exporter can attribute rewinds to a specific captured episode.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("AdListenRewinds V22 migration (playhead-q45f.1)")
struct AdListenRewindsV22MigrationTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "AdListenRewindsV22")
    }

    private func makeAsset(
        id: String,
        episodeId: String = "ep"
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeAdWindow(
        id: String,
        assetId: String,
        startTime: Double = 60,
        endTime: Double = 90
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            confidence: 0.9,
            boundaryState: "lexical",
            decisionState: "confirmed",
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: nil
        )
    }

    // MARK: - Migration

    @Test("fresh DB migrate() lands ad_listen_rewinds + indexes at v22")
    func freshDbHasV22Table() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 24)
        #expect(try probeTableExists(in: dir, table: "ad_listen_rewinds"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_ad_listen_rewinds_window"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_ad_listen_rewinds_podcast_time"))
    }

    @Test("v21-seeded DB picks up ad_listen_rewinds at v22")
    func seededV21ChainsToV22() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        #expect(try await bootstrap.schemaVersion() == 24)

        // Rewind: drop the v22 table/indexes and reset _meta to '21' so
        // the v21 → v22 block runs on the next open.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            DROP INDEX IF EXISTS idx_ad_listen_rewinds_window;
            DROP INDEX IF EXISTS idx_ad_listen_rewinds_podcast_time;
            DROP TABLE IF EXISTS ad_listen_rewinds;
            UPDATE _meta SET value = '21' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        // Sanity: the rewind actually removed the table.
        #expect(!(try probeTableExists(in: dir, table: "ad_listen_rewinds")))

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == 24)
        #expect(try probeTableExists(in: dir, table: "ad_listen_rewinds"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_ad_listen_rewinds_window"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_ad_listen_rewinds_podcast_time"))
    }

    @Test("V22 migration is idempotent across resetMigratedPathsForTesting")
    func v22MigrationIsIdempotent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let v1 = try await store.schemaVersion()

        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()
        let v2 = try await store.schemaVersion()

        #expect(v1 == 24)
        #expect(v2 == 24)
    }

    // MARK: - Round-trip

    @Test("insertListenRewind / fetchListenRewinds round-trip via ad_windows JOIN")
    func roundTripViaAssetJoin() async throws {
        let store = try await makeTestStore()

        // Seed two assets and three ad_windows: two on asset-A, one on
        // asset-B. We then insert two listen-rewinds for win-A1 and one
        // for win-B1, and verify scoping by assetId.
        try await store.insertAsset(makeAsset(id: "asset-A", episodeId: "ep-1"))
        try await store.insertAsset(makeAsset(id: "asset-B", episodeId: "ep-2"))
        try await store.insertAdWindow(makeAdWindow(id: "win-A1", assetId: "asset-A", startTime: 60, endTime: 90))
        try await store.insertAdWindow(makeAdWindow(id: "win-A2", assetId: "asset-A", startTime: 120, endTime: 150))
        try await store.insertAdWindow(makeAdWindow(id: "win-B1", assetId: "asset-B", startTime: 30, endTime: 60))

        try await store.insertListenRewind(
            windowId: "win-A1",
            podcastId: "pod-1",
            time: 60.0,
            createdAt: Date(timeIntervalSince1970: 1_700_000_100)
        )
        try await store.insertListenRewind(
            windowId: "win-A1",
            podcastId: "pod-1",
            time: 60.0,
            createdAt: Date(timeIntervalSince1970: 1_700_000_200)
        )
        try await store.insertListenRewind(
            windowId: "win-B1",
            podcastId: "pod-2",
            time: 30.0,
            createdAt: Date(timeIntervalSince1970: 1_700_000_300)
        )

        let aRows = try await store.fetchListenRewinds(forAssetId: "asset-A")
        #expect(aRows.count == 2)
        #expect(aRows.allSatisfy { $0.windowId == "win-A1" })
        #expect(aRows.allSatisfy { $0.podcastId == "pod-1" })
        #expect(aRows.allSatisfy { $0.time == 60.0 })

        let bRows = try await store.fetchListenRewinds(forAssetId: "asset-B")
        #expect(bRows.count == 1)
        #expect(bRows.first?.windowId == "win-B1")
        #expect(bRows.first?.podcastId == "pod-2")
        #expect(bRows.first?.time == 30.0)

        // Repeated inserts persist as distinct events.
        let aAgain = try await store.fetchListenRewinds(forAssetId: "asset-A")
        #expect(aAgain.count == 2)

        // Asset with no rewinds returns empty.
        try await store.insertAsset(makeAsset(id: "asset-C", episodeId: "ep-3"))
        let cRows = try await store.fetchListenRewinds(forAssetId: "asset-C")
        #expect(cRows.isEmpty)
    }
}
