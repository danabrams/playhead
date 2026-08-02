// FastTranscriptCoverageIndexTests.swift
// playhead-mptr — the skip decision that stops a partly-transcribed episode
// from re-paying for ASR it already bought.
//
// The field shape these tests are held to: a 3929.9 s episode whose transcript
// watermark sat at exactly 2700.000 s across FIVE consecutive attempts, each
// journaled `engine_silent_timeout` with `chunks_persisted = 0`. The runner
// caps the transcription stage at a flat 300 s, and the loop re-transcribed the
// covered 0–2700 s prefix before it could reach anything new, so the cap always
// won first and the watermark could never advance. The two properties that
// matter are therefore:
//
//   1. the covered prefix is SKIPPED, so the budget reaches new audio, and
//   2. nothing is skipped on the strength of the watermark alone — review
//      playhead-rfu-aac H3's counterexamples must still take the full pass.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-mptr: FastTranscriptCoverageIndex")
struct FastTranscriptCoverageIndexTests {

    // MARK: - Merging

    @Test("overlapping ranges merge into one interval")
    func overlappingRangesMerge() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 10), (5, 20)])
        #expect(index.intervals == [.init(start: 0, end: 20)])
    }

    @Test("exactly touching ranges merge — ASR segments abut all the time")
    func touchingRangesMerge() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 10), (10, 20)])
        #expect(index.intervals == [.init(start: 0, end: 20)])
    }

    @Test("a gap keeps the intervals separate")
    func gapKeepsIntervalsSeparate() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 10), (12, 20)])
        #expect(index.intervals == [.init(start: 0, end: 10), .init(start: 12, end: 20)])
    }

    @Test("unordered input is sorted before merging")
    func unorderedInputIsSorted() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(30, 40), (0, 10), (5, 12)])
        #expect(index.intervals == [.init(start: 0, end: 12), .init(start: 30, end: 40)])
    }

    @Test("a nested range does not shorten its container")
    func nestedRangeDoesNotShortenContainer() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 100), (10, 20)])
        #expect(index.intervals == [.init(start: 0, end: 100)])
    }

    @Test("degenerate and non-finite ranges are dropped, not merged")
    func degenerateRangesAreDropped() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [
            (10, 10),                       // zero width — covers nothing
            (30, 20),                       // inverted
            (.nan, 50),                     // would poison every comparison
            (60, .infinity),
            (0, 5),                         // the only usable row
        ])
        #expect(index.intervals == [.init(start: 0, end: 5)])
    }

    @Test("an empty index covers nothing")
    func emptyIndexCoversNothing() {
        #expect(FastTranscriptCoverageIndex.empty.intervals.isEmpty)
        #expect(FastTranscriptCoverageIndex.empty.overlaps(start: 0, end: 10) == false)
    }

    // MARK: - Overlap

    @Test("overlap is detected at the leading edge, the trailing edge and inside")
    func overlapDetectedAtEdgesAndInside() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(100, 200)])
        #expect(index.overlaps(start: 90, end: 110))    // straddles the start
        #expect(index.overlaps(start: 120, end: 130))   // fully inside
        #expect(index.overlaps(start: 190, end: 210))   // straddles the end
        #expect(index.overlaps(start: 50, end: 500))    // contains the interval
    }

    @Test("a query entirely inside a gap does not overlap")
    func queryInsideGapDoesNotOverlap() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 100), (200, 300)])
        #expect(index.overlaps(start: 120, end: 180) == false)
    }

    @Test("queries before the first and after the last interval do not overlap")
    func queriesOutsideAllIntervalsDoNotOverlap() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(100, 200)])
        #expect(index.overlaps(start: 0, end: 50) == false)
        #expect(index.overlaps(start: 300, end: 400) == false)
    }

    @Test("touching at a boundary is not an overlap — the ranges are half-open")
    func boundaryTouchIsNotOverlap() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(100, 200)])
        #expect(index.overlaps(start: 200, end: 300) == false)
        #expect(index.overlaps(start: 50, end: 100) == false)
    }

    @Test("a degenerate or non-finite query never overlaps")
    func degenerateQueryNeverOverlaps() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 1000)])
        #expect(index.overlaps(start: 50, end: 50) == false)
        #expect(index.overlaps(start: 50, end: 40) == false)
        #expect(index.overlaps(start: .nan, end: 100) == false)
        #expect(index.overlaps(start: 0, end: .nan) == false)
    }

    @Test("the binary search finds an interval far from the array's midpoint")
    func binarySearchFindsDistantInterval() {
        // 500 disjoint intervals; probe the first, the last, and a gap between
        // two late ones. A scan-based implementation passes this too — the point
        // is that the search does not mis-handle the candidate/successor seam.
        let ranges = (0..<500).map { (start: Double($0) * 100, end: Double($0) * 100 + 50) }
        let index = FastTranscriptCoverageIndex(chunkRanges: ranges)
        #expect(index.intervals.count == 500)
        #expect(index.overlaps(start: 0, end: 10))
        #expect(index.overlaps(start: 49_900, end: 49_950))
        #expect(index.overlaps(start: 49_960, end: 49_990) == false)
    }

    // MARK: - The skip decision

    @Test("a shard fully backed by chunks under a reaching watermark is skipped")
    func backedShardUnderReachingWatermarkIsSkipped() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 2700)])
        #expect(index.isShardAlreadyTranscribed(shardStart: 100, shardEnd: 120, watermark: 2700))
    }

    @Test("H3 counterexample: watermark reaches past the shard but NO chunk backs it")
    func watermarkWithoutChunksDoesNotAuthoriseASkip() {
        // The behind-playhead shard review playhead-rfu-aac H3 is about: it sits
        // under the watermark and was never transcribed. It must still run.
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 100), (500, 2700)])
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 200, shardEnd: 220, watermark: 2700) == false
        )
    }

    @Test("playhead-0sro shape: a watermark that outlived its chunks skips nothing")
    func watermarkOutlivingItsChunksSkipsNothing() {
        let index = FastTranscriptCoverageIndex.empty
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 20, watermark: 2700) == false
        )
    }

    @Test("a shard past the watermark is never skipped, even where chunks exist")
    func shardPastWatermarkIsNeverSkipped() {
        // A chunk can extend past the watermark (the watermark tracks SHARD ends
        // and is reconciled only at completion). Coverage alone is not licence.
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 2750)])
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 2690, shardEnd: 2710, watermark: 2700) == false
        )
    }

    @Test("a shard ending exactly at the watermark is skipped; one second past is not")
    func watermarkBoundaryIsInclusiveAtTheShardEnd() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 3000)])
        #expect(index.isShardAlreadyTranscribed(shardStart: 2680, shardEnd: 2700, watermark: 2700))
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 2681, shardEnd: 2701, watermark: 2700) == false
        )
    }

    @Test("a nil or non-finite watermark is never a licence to skip")
    func absentWatermarkIsNeverALicenceToSkip() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 3000)])
        #expect(index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 20, watermark: nil) == false)
        #expect(index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 20, watermark: .nan) == false)
    }

    // MARK: - The field regression

    @Test("the D9B513CD shape: the covered prefix is skipped and the tail is not")
    func fieldShapeSkipsThePrefixAndKeepsTheTail() {
        // 3929.9 s episode, watermark stuck at 2700.000, dense speech behind it.
        //
        // The watermark tracks SHARD ends (`updateCoverage` is called with
        // `shard.startTime + shard.duration`) and is reconciled to the chunk
        // MAX only at `.completed`, which this asset never reached. Shards are
        // `AnalysisAudioService.defaultShardDuration` = 30 s, so 2700 is not a
        // 45-minute cap — there is no such constant in the tree — it is exactly
        // the 90th shard boundary, 90 x 30. That is the whole of the roundness.
        //
        // Those 90 shards are already-bought ASR the loop used to re-run inside
        // a 300 s stage budget, so it needed better than 9x realtime just to
        // ARRIVE at shard 91. It never did, five times running.
        let episodeDuration = 3929.9
        let watermark = 2700.0
        let shardDuration = AnalysisAudioService.defaultShardDuration
        let index = FastTranscriptCoverageIndex(
            chunkRanges: stride(from: 0.0, to: watermark, by: 5.0).map { (start: $0, end: $0 + 4.5) }
        )

        var skipped = 0
        var transcribed = 0
        var start = 0.0
        while start < episodeDuration {
            let end = min(start + shardDuration, episodeDuration)
            if index.isShardAlreadyTranscribed(shardStart: start, shardEnd: end, watermark: watermark) {
                skipped += 1
            } else {
                transcribed += 1
            }
            start += shardDuration
        }

        // Every shard wholly inside the covered prefix is skipped — 2700 / 30.
        #expect(skipped == 90)
        // ...and every shard at or past the watermark still runs: 131 shards in
        // the episode, so 41 remain. That is the last third of the show, the
        // audio the episode was permanently stranded without.
        #expect(transcribed == 41)
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 2700, shardEnd: 2730, watermark: watermark) == false
        )
    }

    @Test("a fully transcribed episode skips every shard")
    func fullyTranscribedEpisodeSkipsEverything() {
        let duration = 3929.9
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, duration)])
        var start = 0.0
        while start < duration {
            let end = min(start + 20, duration)
            #expect(index.isShardAlreadyTranscribed(shardStart: start, shardEnd: end, watermark: duration))
            start += 20
        }
    }

    @Test("a virgin asset skips nothing")
    func virginAssetSkipsNothing() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [])
        #expect(index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 20, watermark: 0) == false)
        #expect(index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 20, watermark: nil) == false)
    }
}

// MARK: - The store query behind the index

/// playhead-mptr: `fetchFastTranscriptCoveredRanges` is what makes the skip an
/// ARTIFACT check rather than a watermark check, so its two filters are
/// load-bearing rather than cosmetic. A `pass = 'final'` row describes a
/// different pass and must not authorise skipping the fast one; a degenerate
/// row covers no time and must not authorise skipping anything.
@Suite("playhead-mptr: fetchFastTranscriptCoveredRanges")
struct FastTranscriptCoveredRangesStoreTests {

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "MptrCoveredRanges")
    }

    private func makeStore(_ dir: URL) async throws -> AnalysisStore {
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
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

    private func makeChunk(
        assetId: String,
        index: Int,
        start: Double,
        end: Double,
        pass: String = "fast"
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "chunk-\(assetId)-\(index)",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(assetId)-\(index)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "t\(index)",
            normalizedText: "t\(index)",
            pass: pass,
            modelVersion: "v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    @Test("returns fast-pass ranges ascending by start")
    func returnsFastPassRangesAscending() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await makeStore(dir)
        try await store.insertAsset(makeAsset(id: "a1"))
        _ = try await store.insertTranscriptChunks([
            makeChunk(assetId: "a1", index: 2, start: 40, end: 50),
            makeChunk(assetId: "a1", index: 0, start: 0, end: 10),
            makeChunk(assetId: "a1", index: 1, start: 20, end: 30),
        ])

        let ranges = try await store.fetchFastTranscriptCoveredRanges(assetId: "a1")
        #expect(ranges.map(\.start) == [0, 20, 40])
        #expect(ranges.map(\.end) == [10, 30, 50])
    }

    @Test("a final-pass chunk is not reported as fast coverage")
    func finalPassChunkIsExcluded() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await makeStore(dir)
        try await store.insertAsset(makeAsset(id: "a2"))
        _ = try await store.insertTranscriptChunks([
            makeChunk(assetId: "a2", index: 0, start: 0, end: 10, pass: "final"),
            makeChunk(assetId: "a2", index: 1, start: 20, end: 30),
        ])

        let ranges = try await store.fetchFastTranscriptCoveredRanges(assetId: "a2")
        #expect(ranges.count == 1)
        #expect(ranges.first?.start == 20)
    }

    @Test("a degenerate chunk covers no time and is excluded")
    func degenerateChunkIsExcluded() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await makeStore(dir)
        try await store.insertAsset(makeAsset(id: "a3"))
        _ = try await store.insertTranscriptChunks([
            makeChunk(assetId: "a3", index: 0, start: 10, end: 10),
            makeChunk(assetId: "a3", index: 1, start: 30, end: 20),
            makeChunk(assetId: "a3", index: 2, start: 40, end: 50),
        ])

        let ranges = try await store.fetchFastTranscriptCoveredRanges(assetId: "a3")
        #expect(ranges.count == 1)
        #expect(ranges.first?.start == 40)
    }

    @Test("another asset's chunks never leak into this asset's coverage")
    func otherAssetsChunksDoNotLeak() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await makeStore(dir)
        try await store.insertAsset(makeAsset(id: "a4"))
        try await store.insertAsset(makeAsset(id: "a5"))
        _ = try await store.insertTranscriptChunks([
            makeChunk(assetId: "a4", index: 0, start: 0, end: 10),
            makeChunk(assetId: "a5", index: 0, start: 100, end: 110),
        ])

        let ranges = try await store.fetchFastTranscriptCoveredRanges(assetId: "a4")
        #expect(ranges.count == 1)
        #expect(ranges.first?.start == 0)
    }

    @Test("an asset with no chunks reports no coverage, and the index skips nothing")
    func assetWithNoChunksReportsNothing() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await makeStore(dir)
        try await store.insertAsset(makeAsset(id: "a6"))

        let ranges = try await store.fetchFastTranscriptCoveredRanges(assetId: "a6")
        #expect(ranges.isEmpty)
        // The playhead-0sro shape end to end: a watermark with nothing behind it.
        let index = FastTranscriptCoverageIndex(chunkRanges: ranges)
        #expect(index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 20, watermark: 2700) == false)
    }
}
