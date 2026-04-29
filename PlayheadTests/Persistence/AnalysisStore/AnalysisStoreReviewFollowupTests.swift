// AnalysisStoreReviewFollowupTests.swift
// Review-followup (csp / persistence L2 + L3): pin two contracts that
// were previously test-uncovered:
//
//   - `vacuumInto(destinationURL:)` must produce a valid sqlite file
//     that can be opened independently and round-trip a SELECT (L2).
//   - The `assetSelectColumns` ordering contract — the SELECT column
//     list must agree with `readAsset`'s field-by-field decode (L3).
//     A future migration that appends a column to one side without
//     updating the other would silently shift indices and corrupt
//     decode.

#if DEBUG

import Foundation
import Testing
import SQLite3
@testable import Playhead

@Suite("AnalysisStore review-followup csp")
struct AnalysisStoreReviewFollowupTests {

    // MARK: - vacuumInto (persistence L2)

    @Test("vacuumInto produces a valid standalone sqlite file")
    func vacuumIntoProducesValidFile() async throws {
        let store = try await makeTestStore()

        // Seed a known row so the snapshot has something to verify.
        let assetId = "asset-vacuum-l2"
        let episodeId = "ep-vacuum-l2"
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-vacuum",
            weakFingerprint: nil,
            sourceURL: "file:///vacuum.m4a",
            featureCoverageEndTime: 12.5,
            fastTranscriptCoverageEndTime: 8.0,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 300
        ))

        // Snapshot into a temp directory. The destination file must
        // not exist beforehand (vacuumInto's contract).
        let dir = try makeTempDir(prefix: "VacuumIntoSmoke")
        let destURL = dir.appendingPathComponent("snapshot.sqlite")

        try await store.vacuumInto(destinationURL: destURL)

        // The file must exist and be non-empty.
        let attrs = try FileManager.default.attributesOfItem(atPath: destURL.path)
        let size = (attrs[.size] as? NSNumber)?.intValue ?? 0
        #expect(size > 0, "vacuumInto produced an empty file at \(destURL.path)")

        // Open the file with raw sqlite3 and run a sanity SELECT —
        // proves the bytes are a valid sqlite DB and the seeded row
        // round-tripped through VACUUM INTO.
        var rawDB: OpaquePointer?
        let openRC = sqlite3_open_v2(destURL.path, &rawDB, SQLITE_OPEN_READONLY, nil)
        defer { if rawDB != nil { sqlite3_close(rawDB) } }
        try #require(openRC == SQLITE_OK,
                     "sqlite3_open_v2 on snapshot returned rc=\(openRC)")

        var stmt: OpaquePointer?
        let prepareRC = sqlite3_prepare_v2(
            rawDB,
            "SELECT id, episodeId FROM analysis_assets WHERE id = ?",
            -1,
            &stmt,
            nil
        )
        defer { if stmt != nil { sqlite3_finalize(stmt) } }
        try #require(prepareRC == SQLITE_OK,
                     "sqlite3_prepare_v2 returned rc=\(prepareRC)")

        sqlite3_bind_text(stmt, 1, assetId, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
        let stepRC = sqlite3_step(stmt)
        try #require(stepRC == SQLITE_ROW, "expected one row, got rc=\(stepRC)")

        let readId = String(cString: sqlite3_column_text(stmt, 0))
        let readEp = String(cString: sqlite3_column_text(stmt, 1))
        #expect(readId == assetId, "snapshot lost the seeded asset id")
        #expect(readEp == episodeId, "snapshot lost the seeded episode id")
    }

    // MARK: - assetSelectColumns ordering (persistence L3)

    @Test("Asset round-trips field-by-field through the assetSelectColumns ORDER contract")
    func assetColumnOrderingRoundTrips() async throws {
        // The ordering contract under test: the column list in
        // `assetSelectColumns` (private) must match `readAsset`'s
        // index-positional decode. We exercise it indirectly via the
        // public `insertAsset` + `fetchAsset(id:)` path — both go
        // through the same column list, so a mismatch surfaces as a
        // round-trip field corruption.
        //
        // Set every nullable column to a non-default value so a swap
        // between any two same-typed columns shows up. (E.g. if
        // `featureCoverageEndTime` and `fastTranscriptCoverageEndTime`
        // were transposed, the original test that left
        // fastTranscript=nil would be silently green.)
        // NOTE: `insertAsset` does NOT write `terminalReason` — that
        // column is populated separately by `markAssetTerminal`. So we
        // exercise the ordering contract for the 14 columns that DO go
        // through the insert path. `terminalReason` round-trips on a
        // separate column index path that the SELECT decode also has
        // to keep aligned, but proving _that_ would require a separate
        // markAssetTerminal call in the test.
        let store = try await makeTestStore()
        let original = AnalysisAsset(
            id: "asset-roundtrip",
            episodeId: "ep-roundtrip",
            assetFingerprint: "fp-roundtrip",
            weakFingerprint: "weak-fp-roundtrip",
            sourceURL: "file:///roundtrip.m4a",
            featureCoverageEndTime: 11.0,
            fastTranscriptCoverageEndTime: 22.0,
            confirmedAdCoverageEndTime: 33.0,
            analysisState: "transcribing",
            analysisVersion: 7,
            capabilitySnapshot: "{\"thermal\":\"nominal\"}",
            artifactClass: .media,
            episodeDurationSec: 999.5,
            episodeTitle: "Roundtrip Episode"
        )
        try await store.insertAsset(original)

        let fetched = try #require(await store.fetchAsset(id: original.id),
                                   "round-trip asset must be readable")

        #expect(fetched.id == original.id)
        #expect(fetched.episodeId == original.episodeId)
        #expect(fetched.assetFingerprint == original.assetFingerprint)
        #expect(fetched.weakFingerprint == original.weakFingerprint)
        #expect(fetched.sourceURL == original.sourceURL)
        #expect(fetched.featureCoverageEndTime == original.featureCoverageEndTime)
        #expect(fetched.fastTranscriptCoverageEndTime == original.fastTranscriptCoverageEndTime)
        #expect(fetched.confirmedAdCoverageEndTime == original.confirmedAdCoverageEndTime)
        #expect(fetched.analysisState == original.analysisState)
        #expect(fetched.analysisVersion == original.analysisVersion)
        #expect(fetched.capabilitySnapshot == original.capabilitySnapshot)
        #expect(fetched.artifactClass == original.artifactClass)
        #expect(fetched.episodeDurationSec == original.episodeDurationSec)
        #expect(fetched.episodeTitle == original.episodeTitle)
    }
}

#endif
