// BulkAssetFetchTests.swift
// playhead-hkn1 follow-up coverage: pin the latest-per-episode dedupe
// semantics of ``AnalysisStore.fetchAssetsByEpisodeIds`` and
// ``AnalysisStore.fetchLatestAssetByEpisodeIdMap``. Both methods return
// `[episodeId: AnalysisAsset]` and must collapse multiple rows for the
// same `episodeId` to the most recent one (`ORDER BY createdAt DESC,
// rowid DESC` + first-row-wins). Without these tests a regression in the
// dedupe could silently surface a stale asset on the Activity screen
// (the canonical caller via `LiveActivitySnapshotProvider`).

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisStore bulk asset fetch dedupe (playhead-hkn1)")
struct BulkAssetFetchTests {

    /// Latest-rowid wins when two rows share the same `episodeId`. The
    /// store inserts assign `createdAt` server-side at row time, so the
    /// later insert is also the higher rowid; production tie-breaking
    /// goes through `ORDER BY createdAt DESC, rowid DESC`. Test pins
    /// the *behavior* — never the implementation — so a future move to
    /// a window-function based query that produces the same result is
    /// equally green.
    @Test("fetchAssetsByEpisodeIds returns the latest row per episodeId")
    func bulkFetchReturnsLatestPerEpisode() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-shared"

        let older = AnalysisAsset(
            id: "asset-older",
            episodeId: episodeId,
            assetFingerprint: "fp-older",
            weakFingerprint: nil,
            sourceURL: "file:///older.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        let newer = AnalysisAsset(
            id: "asset-newer",
            episodeId: episodeId,
            assetFingerprint: "fp-newer",
            weakFingerprint: nil,
            sourceURL: "file:///newer.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(older)
        try await store.insertAsset(newer)

        let result = try await store.fetchAssetsByEpisodeIds([episodeId])
        #expect(result.count == 1, "exactly one entry per episodeId, got \(result)")
        #expect(result[episodeId]?.id == "asset-newer",
                "later insert (asset-newer) must win the per-episode tie-break")
    }

    /// Same dedupe contract for the parameter-less map variant. The two
    /// methods diverge only in their `WHERE` clause; their post-filter
    /// dedupe must agree exactly so the Activity provider never sees a
    /// different asset depending on which method ran.
    @Test("fetchLatestAssetByEpisodeIdMap returns the latest row per episodeId")
    func mapFetchReturnsLatestPerEpisode() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-shared"

        let older = AnalysisAsset(
            id: "asset-older",
            episodeId: episodeId,
            assetFingerprint: "fp-older",
            weakFingerprint: nil,
            sourceURL: "file:///older.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        let newer = AnalysisAsset(
            id: "asset-newer",
            episodeId: episodeId,
            assetFingerprint: "fp-newer",
            weakFingerprint: nil,
            sourceURL: "file:///newer.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(older)
        try await store.insertAsset(newer)

        let result = try await store.fetchLatestAssetByEpisodeIdMap()
        #expect(result.count == 1, "exactly one entry per episodeId, got \(result)")
        #expect(result[episodeId]?.id == "asset-newer",
                "later insert (asset-newer) must win the per-episode tie-break")
    }

    /// Empty input must short-circuit to an empty dictionary without
    /// preparing a SQL statement at all (zero placeholders is otherwise
    /// invalid SQL: `WHERE episodeId IN ()`).
    @Test("fetchAssetsByEpisodeIds returns empty for empty input")
    func bulkFetchEmptyInputReturnsEmpty() async throws {
        let store = try await makeTestStore()
        let result = try await store.fetchAssetsByEpisodeIds([])
        #expect(result.isEmpty)
    }

    /// Episodes with no row in `analysis_assets` are simply absent from
    /// the dictionary — callers downstream of `LiveActivitySnapshotProvider`
    /// treat absence as "no data yet" and route the row through the
    /// SwiftData predicate filter rather than rendering a placeholder
    /// summary.
    @Test("fetchAssetsByEpisodeIds omits episodeIds that have no row")
    func bulkFetchOmitsMissingEpisodes() async throws {
        let store = try await makeTestStore()
        let presentId = "ep-present"
        let asset = AnalysisAsset(
            id: "asset-present",
            episodeId: presentId,
            assetFingerprint: "fp-present",
            weakFingerprint: nil,
            sourceURL: "file:///present.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        let result = try await store.fetchAssetsByEpisodeIds([presentId, "ep-absent-1", "ep-absent-2"])
        #expect(result.count == 1)
        #expect(result[presentId]?.id == "asset-present")
        #expect(result["ep-absent-1"] == nil)
        #expect(result["ep-absent-2"] == nil)
    }

    /// Inputs larger than the chunk size (500) must still produce a
    /// single dictionary covering every present episode. A regression
    /// that drops the loop's later chunks would silently strand the
    /// Activity screen on the first 500 episodes once a user crosses
    /// that library size.
    @Test("fetchAssetsByEpisodeIds handles inputs above the chunk-size boundary")
    func bulkFetchHandlesMultipleChunks() async throws {
        let store = try await makeTestStore()
        let n = 750  // > chunkSize=500 to force at least two SQL passes
        var ids: Set<String> = []
        ids.reserveCapacity(n)
        for i in 0..<n {
            let id = "ep-\(i)"
            ids.insert(id)
            let asset = AnalysisAsset(
                id: "asset-\(i)",
                episodeId: id,
                assetFingerprint: "fp-\(i)",
                weakFingerprint: nil,
                sourceURL: "file:///\(i).m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "queued",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
            try await store.insertAsset(asset)
        }

        let result = try await store.fetchAssetsByEpisodeIds(ids)
        #expect(result.count == n,
                "expected \(n) entries across the chunk boundary, got \(result.count)")
    }
}
