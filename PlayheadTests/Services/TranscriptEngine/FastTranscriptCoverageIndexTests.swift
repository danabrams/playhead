// FastTranscriptCoverageIndexTests.swift
// playhead-mptr — the ordering that stops a partly-transcribed episode from
// spending its whole budget re-reading audio it already has.
//
// The field shape these tests are held to: a 3929.9 s episode whose transcript
// watermark sat at exactly 2700.000 s across FIVE consecutive attempts, each
// journaled `engine_silent_timeout` with `chunks_persisted = 0`. The runner caps
// the transcription stage at a flat 300 s, and the loop re-transcribed the
// covered 0–2700 s prefix before it could reach anything new, so the cap always
// won first and the watermark could never advance. 2700 is not a configured
// bound — no such constant exists — it is 90 x 30, the 90th shard boundary.
//
// Three properties matter:
//
//   1. the unread tail is ordered FIRST, so the budget reaches new audio;
//   2. NOTHING is dropped — this is a reordering, so `transcribeShard`'s
//      duplicate-fingerprint `speakerId` / `avgConfidence` upgrades still run on
//      the covered shards; and
//   3. a shard counts as already-transcribed only on ARTIFACT evidence — review
//      playhead-rfu-aac H3's counterexamples sort as uncovered and go first.

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

    // MARK: - The already-transcribed classification

    @Test("a shard fully backed by chunks under a reaching watermark counts as transcribed")
    func backedShardUnderReachingWatermarkCountsAsTranscribed() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 2700)])
        #expect(index.isShardAlreadyTranscribed(shardStart: 100, shardEnd: 120, watermark: 2700))
    }

    @Test("H3 counterexample: watermark reaches past the shard but NO chunk backs it")
    func watermarkWithoutChunksIsNotEvidence() {
        // The behind-playhead shard review playhead-rfu-aac H3 is about: it sits
        // under the watermark and was never transcribed. It must sort as uncovered.
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 100), (500, 2700)])
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 200, shardEnd: 220, watermark: 2700) == false
        )
    }

    @Test("playhead-0sro shape: a watermark that outlived its chunks is not evidence")
    func watermarkOutlivingItsChunksIsNotEvidence() {
        let index = FastTranscriptCoverageIndex.empty
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 20, watermark: 2700) == false
        )
    }

    @Test("a shard past the watermark never counts as transcribed, even where chunks exist")
    func shardPastWatermarkNeverCountsAsTranscribed() {
        // A chunk can extend past the watermark (the watermark tracks SHARD ends
        // and is reconciled only at completion). Coverage alone is not licence.
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 2750)])
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 2690, shardEnd: 2710, watermark: 2700) == false
        )
    }

    @Test("a shard ending exactly at the watermark counts; one second past does not")
    func watermarkBoundaryIsInclusiveAtTheShardEnd() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 3000)])
        #expect(index.isShardAlreadyTranscribed(shardStart: 2680, shardEnd: 2700, watermark: 2700))
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 2681, shardEnd: 2701, watermark: 2700) == false
        )
    }

    @Test("a nil or non-finite watermark is never evidence")
    func absentWatermarkIsNeverEvidence() {
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, 3000)])
        #expect(index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 20, watermark: nil) == false)
        #expect(index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 20, watermark: .nan) == false)
    }

    // MARK: - The field regression

    @Test("the D9B513CD shape: the unread tail is ordered ahead of the covered prefix")
    func fieldShapeOrdersTheUnreadTailFirst() {
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

        var shards: [AnalysisShard] = []
        var start = 0.0
        var shardId = 0
        while start < episodeDuration {
            let end = min(start + shardDuration, episodeDuration)
            shards.append(
                AnalysisShard(
                    id: shardId,
                    episodeID: "D9B513CD",
                    startTime: start,
                    duration: end - start,
                    samples: []
                )
            )
            shardId += 1
            start += shardDuration
        }
        #expect(shards.count == 131)

        let ordered = index.orderingUncoveredFirst(shards, watermark: watermark)

        // NOTHING IS DROPPED — this is a reordering, not a filter. Every shard
        // still runs, so the duplicate-fingerprint metadata upgrades survive.
        #expect(ordered.count == shards.count)
        #expect(Set(ordered.map(\.id)) == Set(shards.map(\.id)))

        // The 41 shards at or past the watermark — the last third of the show,
        // the audio the episode was permanently stranded without — now come
        // FIRST, so the 300 s stage budget reaches them.
        let leading = ordered.prefix(41)
        #expect(leading.allSatisfy { $0.startTime >= 2700 })
        #expect(ordered.first?.startTime == 2700)

        // ...and the 90 already-covered shards (2700 / 30) follow, still in
        // ascending order, still transcribed if the budget allows.
        let trailing = ordered.dropFirst(41)
        #expect(trailing.count == 90)
        #expect(trailing.allSatisfy { $0.startTime + $0.duration <= 2700 })
        #expect(trailing.map(\.startTime) == stride(from: 0.0, to: 2700.0, by: 30.0).map { $0 })
    }

    @Test("a fully transcribed episode classifies every shard as transcribed")
    func fullyTranscribedEpisodeClassifiesEverything() {
        let duration = 3929.9
        let index = FastTranscriptCoverageIndex(chunkRanges: [(0, duration)])
        var start = 0.0
        while start < duration {
            let end = min(start + 20, duration)
            #expect(index.isShardAlreadyTranscribed(shardStart: start, shardEnd: end, watermark: duration))
            start += 20
        }
    }

    @Test("a virgin asset has no transcribed shards")
    func virginAssetHasNoTranscribedShards() {
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
