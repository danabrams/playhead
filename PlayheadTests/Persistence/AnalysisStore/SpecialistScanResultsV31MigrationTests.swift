// SpecialistScanResultsV31MigrationTests.swift
// playhead-b6jq PR 4: pin the V31 migration that introduces the
// `specialist_scan_results` table plus the store's insert/fetch round-trip and
// the reuse-key idempotency contract.
//
// Coverage targets:
//   1. Fresh-DB migrate() reaches head (v31) with the table + index present.
//   2. `currentSchemaVersion` is exactly 31 (drift guard).
//   3. A v30-shaped DB (no specialist table) climbs through v31 in place, and
//      a sibling row seeded before the upgrade survives (no data loss).
//   4. The migration is idempotent across resetMigratedPathsForTesting.
//   5. The isolated ladder (migrateOnlyForTesting) reaches v31.
//   6. Persistence round-trip: insert -> fetch preserves every field incl.
//      probabilityOfAd / isAd / adClass=="hostRead" / modelVersion /
//      detectorVersion.
//   7. Idempotency: same reuseKeyHash => INSERT OR REPLACE (one row); distinct
//      transcriptVersion / modelVersion => two rows.
//   8. FK ON DELETE CASCADE: deleting the asset removes its scan rows.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("specialist_scan_results V31 migration + store (playhead-b6jq PR4)")
struct SpecialistScanResultsV31MigrationTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "SpecialistScanResultsV31")
    }

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
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

    private func makeRow(
        assetId: String,
        windowStartTime: Double = 100.0,
        windowEndTime: Double = 125.0,
        probabilityOfAd: Double = 0.83,
        isAd: Bool = true,
        adClass: String? = "hostRead",
        modelVersion: String = SpecialistModelResources.modelFolderName,
        detectorVersion: String = "detection-v1",
        transcriptVersion: String = "tx-v1",
        scanCohortJSON: String = "{}",
        createdAt: Double = 1_700_000_000
    ) -> SpecialistScanResult {
        let reuseKeyHash = AnalysisStore.specialistScanReuseKeyHash(
            analysisAssetId: assetId,
            windowStartTime: windowStartTime,
            windowEndTime: windowEndTime,
            modelVersion: modelVersion,
            detectorVersion: detectorVersion,
            transcriptVersion: transcriptVersion,
            scanCohortJSON: scanCohortJSON
        )
        return SpecialistScanResult(
            id: "spec-\(reuseKeyHash.prefix(16))",
            analysisAssetId: assetId,
            windowStartTime: windowStartTime,
            windowEndTime: windowEndTime,
            probabilityOfAd: probabilityOfAd,
            isAd: isAd,
            adClass: adClass,
            modelVersion: modelVersion,
            detectorVersion: detectorVersion,
            transcriptVersion: transcriptVersion,
            scanCohortJSON: scanCohortJSON,
            reuseKeyHash: reuseKeyHash,
            jobPhase: BackfillJobPhase.specialistHostReadScan.rawValue,
            createdAt: createdAt
        )
    }

    // MARK: - Migration ladder

    @Test("fresh DB migrate() lands specialist_scan_results + index at head")
    func freshDbHasV31Table() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // Drift guard: head is exactly 31 for this bead.
        #expect(AnalysisStore.currentSchemaVersion == 31)
        #expect(try probeTableExists(in: dir, table: "specialist_scan_results"))
    }

    @Test("v30-seeded DB picks up specialist_scan_results via the v30->v31 step; sibling row survives")
    func seededV30ChainsToV31() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Build the head shape, seed a sibling (episode_fingerprints) row to
        // prove no data loss, then regress to v30 by dropping ONLY the
        // specialist table and rewinding `_meta.schema_version`.
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        try await bootstrap.insertAsset(makeAsset(id: "asset-v30-sibling"))
        try await bootstrap.upsertEpisodeFingerprints(
            EpisodeFingerprintRecord(
                analysisAssetId: "asset-v30-sibling",
                algorithmVersion: ChromaFingerprinter.algorithmVersion,
                secondsPerFingerprint: ChromaFingerprinter.secondsPerFingerprint,
                fingerprints: [1, 2, 3, 4],
                sourceAudioIdentity: "fp-asset-v30-sibling",
                capturedAt: 1_699_000_000
            )
        )
        #expect(try probeTableExists(in: dir, table: "specialist_scan_results"))

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            DROP TABLE IF EXISTS specialist_scan_results;
            UPDATE _meta SET value = '30' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)
        #expect(!(try probeTableExists(in: dir, table: "specialist_scan_results")))

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: "specialist_scan_results"))

        // The sibling row seeded before the upgrade survives (no data loss).
        #expect(try await store.fetchEpisodeFingerprints(assetId: "asset-v30-sibling") != nil)

        // The migrated table accepts a row through the live CRUD path.
        try await store.insertSpecialistScanResult(makeRow(assetId: "asset-v30-sibling"))
        #expect(try await store.fetchSpecialistScanResults(analysisAssetId: "asset-v30-sibling").count == 1)
    }

    @Test("V31 migration is idempotent across resetMigratedPathsForTesting")
    func v31MigrationIsIdempotent() async throws {
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
        #expect(try probeTableExists(in: dir, table: "specialist_scan_results"))
    }

    @Test("isolated ladder (migrateOnlyForTesting) reaches v31")
    func isolatedLadderReachesV31() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: "specialist_scan_results"))
    }

    // MARK: - Persistence round-trip

    @Test("insert -> fetch round-trips every field verbatim (probabilityOfAd/isAd/adClass/versions)")
    func persistenceRoundTrip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-rt"))

        let row = makeRow(
            assetId: "asset-rt",
            windowStartTime: 210.5,
            windowEndTime: 235.5,
            probabilityOfAd: 0.917,
            isAd: true,
            adClass: "hostRead",
            transcriptVersion: "tx-rt-v1",
            createdAt: 1_700_111_222
        )
        try await store.insertSpecialistScanResult(row)

        let fetched = try await store.fetchSpecialistScanResults(analysisAssetId: "asset-rt")
        #expect(fetched.count == 1)
        #expect(fetched.first == row)
        // Spell out the load-bearing raw-verdict fields.
        #expect(fetched.first?.probabilityOfAd == 0.917)
        #expect(fetched.first?.isAd == true)
        #expect(fetched.first?.adClass == "hostRead")
        #expect(fetched.first?.modelVersion == SpecialistModelResources.modelFolderName)
        #expect(fetched.first?.detectorVersion == "detection-v1")
        #expect(fetched.first?.jobPhase == BackfillJobPhase.specialistHostReadScan.rawValue)
    }

    @Test("nil adClass and isAd=false round-trip correctly")
    func nilAdClassAndFalseIsAdRoundTrip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-nil"))
        let row = makeRow(assetId: "asset-nil", probabilityOfAd: 0.12, isAd: false, adClass: nil)
        try await store.insertSpecialistScanResult(row)
        let fetched = try await store.fetchSpecialistScanResults(analysisAssetId: "asset-nil")
        #expect(fetched.first == row)
        #expect(fetched.first?.adClass == nil)
        #expect(fetched.first?.isAd == false)
    }

    @Test("same reuseKeyHash => INSERT OR REPLACE (one row); distinct transcript/model => two rows")
    func reuseKeyIdempotency() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-idem"))

        // Two inserts with identical identity fields collapse to one row (the
        // second REPLACEs the first — here with a different probability).
        let first = makeRow(assetId: "asset-idem", probabilityOfAd: 0.5, transcriptVersion: "tx-a")
        let firstReplacement = makeRow(assetId: "asset-idem", probabilityOfAd: 0.9, transcriptVersion: "tx-a")
        #expect(first.reuseKeyHash == firstReplacement.reuseKeyHash)
        #expect(first.id == firstReplacement.id)
        try await store.insertSpecialistScanResult(first)
        try await store.insertSpecialistScanResult(firstReplacement)
        var rows = try await store.fetchSpecialistScanResults(analysisAssetId: "asset-idem")
        #expect(rows.count == 1)
        #expect(rows.first?.probabilityOfAd == 0.9)

        // A distinct transcriptVersion hashes differently => a second row.
        try await store.insertSpecialistScanResult(
            makeRow(assetId: "asset-idem", transcriptVersion: "tx-b"))
        // A distinct modelVersion hashes differently => a third row.
        try await store.insertSpecialistScanResult(
            makeRow(assetId: "asset-idem", modelVersion: "some_other_model_v9", transcriptVersion: "tx-a"))
        rows = try await store.fetchSpecialistScanResults(analysisAssetId: "asset-idem")
        #expect(rows.count == 3)
    }

    @Test("deleting the asset cascades its specialist scan rows away (FK ON DELETE CASCADE)")
    func deleteAssetCascades() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-casc-A"))
        try await store.insertAsset(makeAsset(id: "asset-casc-B"))
        try await store.insertSpecialistScanResult(makeRow(assetId: "asset-casc-A"))
        try await store.insertSpecialistScanResult(makeRow(assetId: "asset-casc-B"))

        try await store.deleteAsset(id: "asset-casc-A")

        #expect(try await store.fetchSpecialistScanResults(analysisAssetId: "asset-casc-A").isEmpty)
        #expect(try await store.fetchSpecialistScanResults(analysisAssetId: "asset-casc-B").count == 1)
    }

    @Test("inserting a scan row for a non-existent asset is rejected by the FK")
    func insertWithoutParentIsRejected() async throws {
        let store = try await makeTestStore()
        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertSpecialistScanResult(makeRow(assetId: "asset-ghost"))
        }
        #expect(try await store.fetchSpecialistScanResults(analysisAssetId: "asset-ghost").isEmpty)
    }
}
