// TranscriptCoverageIndexTests.swift
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

@Suite("playhead-mptr: TranscriptCoverageIndex")
struct TranscriptCoverageIndexTests {

    // MARK: - Merging

    @Test("overlapping ranges merge into one interval")
    func overlappingRangesMerge() {
        let index = TranscriptCoverageIndex(chunkRanges: [(0, 10), (5, 20)])
        #expect(index.intervals == [.init(start: 0, end: 20)])
    }

    @Test("exactly touching ranges merge — ASR segments abut all the time")
    func touchingRangesMerge() {
        let index = TranscriptCoverageIndex(chunkRanges: [(0, 10), (10, 20)])
        #expect(index.intervals == [.init(start: 0, end: 20)])
    }

    @Test("a gap keeps the intervals separate")
    func gapKeepsIntervalsSeparate() {
        let index = TranscriptCoverageIndex(chunkRanges: [(0, 10), (12, 20)])
        #expect(index.intervals == [.init(start: 0, end: 10), .init(start: 12, end: 20)])
    }

    @Test("unordered input is sorted before merging")
    func unorderedInputIsSorted() {
        let index = TranscriptCoverageIndex(chunkRanges: [(30, 40), (0, 10), (5, 12)])
        #expect(index.intervals == [.init(start: 0, end: 12), .init(start: 30, end: 40)])
    }

    @Test("a nested range does not shorten its container")
    func nestedRangeDoesNotShortenContainer() {
        let index = TranscriptCoverageIndex(chunkRanges: [(0, 100), (10, 20)])
        #expect(index.intervals == [.init(start: 0, end: 100)])
    }

    @Test("degenerate and non-finite ranges are dropped, not merged")
    func degenerateRangesAreDropped() {
        let index = TranscriptCoverageIndex(chunkRanges: [
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
        #expect(TranscriptCoverageIndex.empty.intervals.isEmpty)
        #expect(TranscriptCoverageIndex.empty.overlaps(start: 0, end: 10) == false)
    }

    // MARK: - Overlap

    @Test("overlap is detected at the leading edge, the trailing edge and inside")
    func overlapDetectedAtEdgesAndInside() {
        let index = TranscriptCoverageIndex(chunkRanges: [(100, 200)])
        #expect(index.overlaps(start: 90, end: 110))    // straddles the start
        #expect(index.overlaps(start: 120, end: 130))   // fully inside
        #expect(index.overlaps(start: 190, end: 210))   // straddles the end
        #expect(index.overlaps(start: 50, end: 500))    // contains the interval
    }

    @Test("a query entirely inside a gap does not overlap")
    func queryInsideGapDoesNotOverlap() {
        let index = TranscriptCoverageIndex(chunkRanges: [(0, 100), (200, 300)])
        #expect(index.overlaps(start: 120, end: 180) == false)
    }

    @Test("queries before the first and after the last interval do not overlap")
    func queriesOutsideAllIntervalsDoNotOverlap() {
        let index = TranscriptCoverageIndex(chunkRanges: [(100, 200)])
        #expect(index.overlaps(start: 0, end: 50) == false)
        #expect(index.overlaps(start: 300, end: 400) == false)
    }

    @Test("touching at a boundary is not an overlap — the ranges are half-open")
    func boundaryTouchIsNotOverlap() {
        let index = TranscriptCoverageIndex(chunkRanges: [(100, 200)])
        #expect(index.overlaps(start: 200, end: 300) == false)
        #expect(index.overlaps(start: 50, end: 100) == false)
    }

    @Test("a degenerate or non-finite query never overlaps")
    func degenerateQueryNeverOverlaps() {
        let index = TranscriptCoverageIndex(chunkRanges: [(0, 1000)])
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
        let index = TranscriptCoverageIndex(chunkRanges: ranges)
        #expect(index.intervals.count == 500)
        #expect(index.overlaps(start: 0, end: 10))
        #expect(index.overlaps(start: 49_900, end: 49_950))
        #expect(index.overlaps(start: 49_960, end: 49_990) == false)
    }

    // MARK: - The already-transcribed classification

    @Test("a shard fully backed by chunks under a reaching watermark counts as transcribed")
    func backedShardUnderReachingWatermarkCountsAsTranscribed() {
        let index = TranscriptCoverageIndex(chunkRanges: [(0, 2700)])
        #expect(index.isShardAlreadyTranscribed(shardStart: 100, shardEnd: 120, watermark: 2700))
    }

    @Test("H3 counterexample: watermark reaches past the shard but NO chunk backs it")
    func watermarkWithoutChunksIsNotEvidence() {
        // The behind-playhead shard review playhead-rfu-aac H3 is about: it sits
        // under the watermark and was never transcribed. It must sort as uncovered.
        let index = TranscriptCoverageIndex(chunkRanges: [(0, 100), (500, 2700)])
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 200, shardEnd: 220, watermark: 2700) == false
        )
    }

    @Test("playhead-0sro shape: a watermark that outlived its chunks is not evidence")
    func watermarkOutlivingItsChunksIsNotEvidence() {
        let index = TranscriptCoverageIndex.empty
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 20, watermark: 2700) == false
        )
    }

    @Test("a shard past the watermark never counts as transcribed, even where chunks exist")
    func shardPastWatermarkNeverCountsAsTranscribed() {
        // A chunk can extend past the watermark (the watermark tracks SHARD ends
        // and is reconciled only at completion). Coverage alone is not licence.
        let index = TranscriptCoverageIndex(chunkRanges: [(0, 2750)])
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 2690, shardEnd: 2710, watermark: 2700) == false
        )
    }

    @Test("a shard ending exactly at the watermark counts, one second past does not")
    func watermarkBoundaryIsInclusiveAtTheShardEnd() {
        let index = TranscriptCoverageIndex(chunkRanges: [(0, 3000)])
        #expect(index.isShardAlreadyTranscribed(shardStart: 2680, shardEnd: 2700, watermark: 2700))
        #expect(
            index.isShardAlreadyTranscribed(shardStart: 2681, shardEnd: 2701, watermark: 2700) == false
        )
    }

    @Test("a nil or non-finite watermark is never evidence")
    func absentWatermarkIsNeverEvidence() {
        let index = TranscriptCoverageIndex(chunkRanges: [(0, 3000)])
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
        let index = TranscriptCoverageIndex(
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
        let index = TranscriptCoverageIndex(chunkRanges: [(0, duration)])
        var start = 0.0
        while start < duration {
            let end = min(start + 20, duration)
            #expect(index.isShardAlreadyTranscribed(shardStart: start, shardEnd: end, watermark: duration))
            start += 20
        }
    }

    @Test("a virgin asset has no transcribed shards")
    func virginAssetHasNoTranscribedShards() {
        let index = TranscriptCoverageIndex(chunkRanges: [])
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
        let index = TranscriptCoverageIndex(chunkRanges: ranges)
        #expect(index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 20, watermark: 2700) == false)
    }
}

// MARK: - playhead-6r4z: the index reads BOTH passes

/// playhead-6r4z — the shard-ordering index shipped reading `pass = 'fast'`
/// alone, so audio the FINAL pass covers had no artifact it could see: it failed
/// the artifact test, sorted UNCOVERED, and — the partition being stable over
/// playhead proximity — floated to the FRONT of the very pass minted to read the
/// audio behind it. The fix was partly defeating itself.
///
/// Re-derived from the 2026-08-03 device pull at 30 s shards for this bead: 215
/// shards (6,450 s) sit below their asset's fast watermark backed ONLY by a
/// final-pass chunk, out of 1,174 shards below a watermark, across all 12 assets
/// in the pull; SEVEN of the twelve carry some. On `48E903D7` that re-read prefix
/// is 1,230 s against 103.1 s of genuinely new audio — 11.9:1 the wrong way,
/// inside a flat 300 s stage cap. And ZERO of the 215 lacked a chunk of *both*
/// passes, so widening the artifact test to the union moves only audio a real row
/// on disk already backs; it invents no coverage.
///
/// **Every case here carries its own control on the fast-only input, and that is
/// not ceremony.** An ordering assertion is exactly the shape that reads true
/// vacuously — "the shards I expect last are last" holds of an index that
/// classifies nothing, whenever they were last to begin with. So each case reads
/// the SAME fixture through the narrow population as well and asserts the
/// opposite, which is the behaviour that shipped.
@Suite("playhead-6r4z: TranscriptCoverageIndex reads both passes")
struct TranscriptCoverageBothPassesTests {

    /// 34 shards of 30 s over a 1,020 s episode; watermark 900 (shard 30 is the
    /// first not below it).
    private func shards(count: Int = 34) -> [AnalysisShard] {
        (0..<count).map {
            AnalysisShard(
                id: $0,
                episodeID: "ep-6r4z",
                startTime: Double($0) * 30,
                duration: 30,
                samples: []
            )
        }
    }

    /// The field shape reduced to its smallest honest form, and the proportions
    /// are `2C5C3699`'s: 20 of the 30 shards below its watermark are backed only
    /// by a final-pass chunk (600 s), and the new audio behind them is what the
    /// pass exists to reach.
    ///
    ///   * fast chunks  `[0, 300)`   — shards 0-9, already sorted covered
    ///   * final chunks `[300, 900)` — shards 10-29, the 600 s at issue
    ///   * nothing at   `[900, 1020)` — shards 30-33, the genuinely new audio
    @Test("audio only a FINAL-pass chunk backs sorts LAST, and read fast-only it sorts FIRST")
    func finalPassOnlyCoverageSortsLast() {
        let watermark = 900.0
        let fastOnly = TranscriptCoverageIndex(chunkRanges: [(0, 300)])
        let bothPasses = TranscriptCoverageIndex(chunkRanges: [(0, 300), (300, 900)])

        // THE CONTROL — the shipped behaviour, and what makes the assertion
        // below non-vacuous. Read fast-only, the 20 already-transcribed shards
        // are the FIRST 20 the stage decodes, and the four shards of new audio
        // wait behind 600 s of re-read.
        let shipped = fastOnly.orderingUncoveredFirst(shards(), watermark: watermark)
        #expect(shipped.prefix(20).map(\.id) == Array(10...29))
        #expect(shipped.dropFirst(20).prefix(4).map(\.id) == Array(30...33))

        // THE FIX — the new audio is what the budget reaches first.
        let fixed = bothPasses.orderingUncoveredFirst(shards(), watermark: watermark)
        #expect(fixed.prefix(4).map(\.id) == Array(30...33))
        #expect(fixed.dropFirst(4).map(\.id) == Array(0...29))
    }

    /// Review playhead-rfu-aac H3, preserved verbatim through the widening: the
    /// watermark is a high-water REACH, not a promise that every second below it
    /// was transcribed, so a shard under it with NO chunk of EITHER pass must
    /// still sort first. Here `[600, 660)` is that hole.
    ///
    /// This is the direction the widening could have broken and did not — and it
    /// is why the fix is condition 2 only. Condition 1 still consults the fast
    /// watermark alone.
    @Test("a shard below the watermark with no chunk of either pass still sorts FIRST")
    func aHoleUnderTheWatermarkStillSortsFirst() {
        let watermark = 900.0
        let bothPasses = TranscriptCoverageIndex(
            chunkRanges: [(0, 300), (300, 600), (660, 900)]
        )

        let ordered = bothPasses.orderingUncoveredFirst(shards(), watermark: watermark)
        // Shards 20 and 21 span [600, 660): nothing backs them, so they lead,
        // ahead even of the new audio, because the partition is stable over the
        // playhead-proximity order it is given.
        #expect(ordered.prefix(6).map(\.id) == [20, 21, 30, 31, 32, 33])

        // The control: had the hole been backed, those two would sort last —
        // so the assertion above is reading the hole and not the shard numbers.
        let filled = TranscriptCoverageIndex(chunkRanges: [(0, 900)])
        #expect(filled.orderingUncoveredFirst(shards(), watermark: watermark)
            .prefix(4).map(\.id) == Array(30...33))
    }

    /// NOTHING IS DROPPED. The widening changes which shards are classified, and
    /// this is still a reordering, so `transcribeShard`'s duplicate-fingerprint
    /// arm still reaches every covered shard when the budget allows.
    @Test("widening the artifact test to both passes drops no shard")
    func nothingIsDropped() {
        let all = shards()
        let ordered = TranscriptCoverageIndex(chunkRanges: [(0, 300), (300, 900)])
            .orderingUncoveredFirst(all, watermark: 900)
        #expect(ordered.count == all.count)
        #expect(Set(ordered.map(\.id)) == Set(all.map(\.id)))
    }

    /// `0C2FC22E`, whole: the two passes ran over DISJOINT spans — final
    /// `[0, 930)`, fast `[930, 2086)`, watermark 2,086 — which is the shape the
    /// store's own doc names, and 31 of its 70 shards below the watermark carry
    /// no fast artifact at all.
    @Test("the 0C2FC22E shape: disjoint passes leave 31 shards phantom-unread read fast-only")
    func disjointPassesFieldShape() {
        let duration = 2086.0
        let watermark = 2086.0
        var episode: [AnalysisShard] = []
        var start = 0.0
        while start < duration {
            episode.append(
                AnalysisShard(
                    id: episode.count,
                    episodeID: "0C2FC22E",
                    startTime: start,
                    duration: min(30, duration - start),
                    samples: []
                )
            )
            start += 30
        }
        #expect(episode.count == 70)

        let fastOnly = TranscriptCoverageIndex(chunkRanges: [(930, 2086)])
        let bothPasses = TranscriptCoverageIndex(chunkRanges: [(0, 930), (930, 2086)])

        // Read fast-only, the first 31 shards of a fully transcribed episode are
        // classified as never read — 930 s of ASR the stage would buy again.
        let phantom = episode.filter {
            !fastOnly.isShardAlreadyTranscribed(
                shardStart: $0.startTime,
                shardEnd: $0.startTime + $0.duration,
                watermark: watermark
            )
        }
        #expect(phantom.count == 31)
        #expect(phantom.map(\.id) == Array(0...30))

        // Read across both passes, none of it is.
        #expect(episode.allSatisfy {
            bothPasses.isShardAlreadyTranscribed(
                shardStart: $0.startTime,
                shardEnd: $0.startTime + $0.duration,
                watermark: watermark
            )
        })
    }
}

// MARK: - playhead-6r4z: the store read behind the widened index

/// playhead-6r4z — the index is only as wide as what it is handed, and the two
/// store getters were interchangeable by type until playhead-x0lb.
/// `fetchFastTranscriptCoveredRanges` still exists and is still correct for the
/// narrower question, so these tests read the SAME fixture through both and
/// state the difference rather than asserting one in isolation.
@Suite("playhead-6r4z: fetchTranscribedRegion is what the index is built from")
struct TranscriptCoverageIndexStoreInputTests {

    private func makeAsset(id: String, watermark: Double?) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: watermark,
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
        pass: String
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

    @Test("an index built from the region classifies final-pass audio the fast-only read misses")
    func regionBuiltIndexSeesFinalPassCoverage() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-6r4z", watermark: 900))
        _ = try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-6r4z", index: 0, start: 0, end: 300, pass: "fast"),
            makeChunk(assetId: "a-6r4z", index: 1, start: 300, end: 900, pass: "final")
        ])

        let episode = (0..<34).map {
            AnalysisShard(
                id: $0, episodeID: "ep-a-6r4z",
                startTime: Double($0) * 30, duration: 30, samples: []
            )
        }

        // THE CONTROL, and it is the observer check this bead's queue keeps
        // paying for: the fast-only read really can see the fixture — it returns
        // the one fast row — so an empty answer below would be a broken query
        // rather than evidence.
        let fastRanges = try await store.fetchFastTranscriptCoveredRanges(assetId: "a-6r4z")
        #expect(fastRanges.count == 1)
        #expect(fastRanges.first?.end == 300)
        let shipped = TranscriptCoverageIndex(chunkRanges: fastRanges)
            .orderingUncoveredFirst(episode, watermark: 900)
        #expect(shipped.prefix(20).map(\.id) == Array(10...29),
                "the shipped read puts 600 s of already-transcribed audio first")

        // THE PRODUCTION PATH.
        let region = try await store.fetchTranscribedRegion(assetId: "a-6r4z")
        #expect(region.intervalCount == 2)
        let fixed = TranscriptCoverageIndex(transcribedRegion: region)
            .orderingUncoveredFirst(episode, watermark: 900)
        #expect(fixed.prefix(4).map(\.id) == Array(30...33))
        #expect(fixed.count == episode.count)
    }

    @Test("a degenerate row in either pass cannot authorise re-ordering a shard last")
    func degenerateRowsAreDroppedFromTheRegion() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-6r4z-degen", watermark: 900))
        _ = try await store.insertTranscriptChunks([
            makeChunk(assetId: "a-6r4z-degen", index: 0, start: 300, end: 300, pass: "final"),
            makeChunk(assetId: "a-6r4z-degen", index: 1, start: 660, end: 600, pass: "final"),
            makeChunk(assetId: "a-6r4z-degen", index: 2, start: 0, end: 300, pass: "fast")
        ])

        let region = try await store.fetchTranscribedRegion(assetId: "a-6r4z-degen")
        // Only the fast row survives — the zero-width and the inverted final rows
        // cover no time, and a row covering no time must not move a shard.
        #expect(region.intervalCount == 1)
        #expect(region.unionedSeconds == 300)

        let index = TranscriptCoverageIndex(transcribedRegion: region)
        #expect(index.isShardAlreadyTranscribed(shardStart: 0, shardEnd: 30, watermark: 900))
        #expect(index.isShardAlreadyTranscribed(shardStart: 300, shardEnd: 330, watermark: 900) == false)
        #expect(index.isShardAlreadyTranscribed(shardStart: 600, shardEnd: 630, watermark: 900) == false)
    }
}
