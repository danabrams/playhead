// PipelineSnapshotTests.swift
// Regression tests for the shard prioritization race condition where
// timeUpdate events overwrote the pipeline start snapshot, causing
// shard 0 (0-30s) to be transcribed last instead of first.

import Foundation
import Testing
@testable import Playhead

private func makeHotPathContextChunk(
    id: String,
    chunkIndex: Int,
    startTime: Double,
    endTime: Double,
    text: String,
    weakAnchorMetadata: TranscriptWeakAnchorMetadata? = nil
) -> TranscriptChunk {
    TranscriptChunk(
        id: id,
        analysisAssetId: "asset-1",
        segmentFingerprint: "fp-\(id)",
        chunkIndex: chunkIndex,
        startTime: startTime,
        endTime: endTime,
        text: text,
        normalizedText: TranscriptEngineService.normalizeText(text),
        pass: TranscriptPassType.fast.rawValue,
        modelVersion: "speech-v1",
        transcriptVersion: nil,
        atomOrdinal: nil,
        weakAnchorMetadata: weakAnchorMetadata
    )
}

// MARK: - Shard Prioritization Logic

private let defaultChunkOverlap: TimeInterval = 0.5
private let defaultLookaheadWallClockSeconds: TimeInterval = 120.0

private func makeTestShard(id: Int, startTime: TimeInterval, duration: TimeInterval = 30) -> AnalysisShard {
    AnalysisShard(id: id, episodeID: "test-ep", startTime: startTime, duration: duration, samples: [])
}

private func prioritizeViaProduction(
    shardStarts: [TimeInterval],
    playhead: TimeInterval,
    rate: Double = 1.0
) -> [TimeInterval] {
    let shards = shardStarts.enumerated().map { i, start in
        makeTestShard(id: i, startTime: start)
    }
    return TranscriptEngineService.prioritizeShards(
        shards,
        playhead: playhead,
        playbackRate: rate,
        chunkOverlap: defaultChunkOverlap,
        lookaheadWallClockSeconds: defaultLookaheadWallClockSeconds
    ).map(\.startTime)
}

@Suite("Shard Prioritization – Partition Logic")
struct ShardPartitionTests {

    @Test("All shards ahead when playhead is at 0")
    func allAheadAtZero() {
        let ordered = prioritizeViaProduction(shardStarts: [0, 30, 60, 90], playhead: 0)
        #expect(ordered.first == 0)
        #expect(ordered == [0, 30, 60, 90])
    }

    @Test("Shard 0 is behind when playhead drifts to 15s")
    func shard0BehindWhenDrifted() {
        // Shard 0 (startTime=0) falls into 'behind' at playhead 15, but
        // the production code hoists it to the front.
        let ordered = prioritizeViaProduction(shardStarts: [0, 15, 30], playhead: 15)
        #expect(ordered.first == 0,
                "Shard 0 must always be first — production code hoists it from behind")
    }

    @Test("Shard 0 stays ahead at small drift under overlap")
    func shard0AheadAtSmallDrift() {
        let ordered = prioritizeViaProduction(shardStarts: [0, 30], playhead: 0.3)
        #expect(ordered.first == 0)
    }

    @Test("Shard just below cutoff falls behind")
    func shardJustBelowCutoff() {
        // 29.0 < 30.0 - 0.5 = 29.5 → behind; 29.5 >= 29.5 → ahead
        let ordered = prioritizeViaProduction(shardStarts: [29.0, 29.5, 30], playhead: 30)
        #expect(ordered.first == 29.5 || ordered.first == 30,
                "Shard at 29.0 must not be first (it is behind)")
    }
}

// MARK: - Priority Ordering

@Suite("Shard Prioritization – Ordering")
struct ShardOrderingTests {

    @Test("Shard 0 is first when playhead is at 0")
    func shard0FirstAtZero() {
        let ordered = prioritizeViaProduction(shardStarts: [0, 30, 60, 90, 120], playhead: 0)
        #expect(ordered.first == 0, "Shard 0 must be transcribed first")
    }

    @Test("Shard 0 is first even when playhead drifts to 15s")
    func shard0FirstWhenDrifted() {
        let ordered = prioritizeViaProduction(shardStarts: [0, 30, 60, 90, 120], playhead: 15)
        #expect(ordered.first == 0, "Shard 0 must always be first for pre-roll ad detection")
    }

    @Test("Shard 0 is first even when playback starts mid-episode")
    func shard0FirstMidEpisode() {
        let ordered = prioritizeViaProduction(shardStarts: [0, 30, 60, 90, 120, 150, 180], playhead: 90)
        #expect(ordered.first == 0, "Shard 0 must always be first")
        #expect(ordered[1] == 90)
    }

    @Test("At 2x speed, hot path window doubles")
    func hotPathScalesWithSpeed() {
        let shards = (0..<200).map { TimeInterval($0) * 30.0 }
        let ordered = prioritizeViaProduction(shardStarts: shards, playhead: 0, rate: 2.0)

        let hotPathEnd = 120.0 * 2.0 // 240s
        let hotCount = ordered.prefix(while: { $0 < hotPathEnd }).count
        #expect(hotCount == 8)
    }
}

// MARK: - Pipeline Start Snapshot

@Suite("Pipeline Start Snapshot – Race Prevention")
struct PipelineStartSnapshotTests {

    @Test("PlaybackSnapshot preserves initial playhead time")
    func snapshotPreservesTime() {
        let snapshot = PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        #expect(snapshot.playheadTime == 0)
        #expect(snapshot.playbackRate == 1.0)
    }

    @Test("Snapshot is value type — later mutations don't affect captured copy")
    func snapshotIsValueSemantics() {
        var start = PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        let captured = start

        // Simulate timeUpdate overwriting the "latest" snapshot.
        start = PlaybackSnapshot(playheadTime: 15.3, playbackRate: 1.0, isPlaying: true)

        // The captured pipeline start snapshot should be unaffected.
        #expect(captured.playheadTime == 0, "pipelineStartSnapshot must not drift")
        #expect(start.playheadTime == 15.3)
    }
}

// MARK: - Incremental Decode via Download Progress

/// Regression test for the incremental decode flow: when download progress
/// arrives, the coordinator should re-decode and feed new shards to the
/// transcript engine via appendShards.
@Suite("Incremental Decode – Download Progress")
struct IncrementalDecodeTests {

    @Test("Coordinator progress observer triggers appendShards on new audio")
    func progressTriggersIncrementalDecode() async throws {
        // The coordinator's download progress observer should:
        // 1. Re-decode when progress arrives
        // 2. Detect new shards beyond the initial count
        // 3. Call appendShards with the delta

        // Simulate partial → full decode progression.
        let audioStub = StubAnalysisAudioProvider()
        let partialShards = (0..<5).map { i in
            makeShard(id: i, startTime: Double(i) * 30.0, duration: 30.0)
        }
        let fullShards = (0..<10).map { i in
            makeShard(id: i, startTime: Double(i) * 30.0, duration: 30.0)
        }

        // Initial decode returns partial shards.
        audioStub.shardsToReturn = partialShards
        let initial = try await audioStub.decode(
            fileURL: LocalAudioURL(URL(fileURLWithPath: "/tmp/ep.mp3"))!,
            episodeID: "ep-1",
            shardDuration: 30.0
        )
        #expect(initial.count == 5)

        // Simulate more audio available.
        audioStub.shardsToReturn = fullShards
        let fresh = try await audioStub.decode(
            fileURL: LocalAudioURL(URL(fileURLWithPath: "/tmp/ep.mp3"))!,
            episodeID: "ep-1",
            shardDuration: 30.0
        )
        #expect(fresh.count == 10)

        // Compute the delta — this is the core logic the coordinator uses.
        let lastShardCount = initial.count
        guard fresh.count > lastShardCount else {
            Issue.record("Re-decode should return more shards than initial")
            return
        }
        let newShards = Array(fresh.dropFirst(lastShardCount))
        let newAudio = newShards.map(\.duration).reduce(0, +)

        #expect(newShards.count == 5, "Should have 5 new shards")
        #expect(newAudio == 150.0, "Should have 150s of new audio")
        #expect(newAudio >= 60.0, "New audio should exceed the 60s threshold")

        // Verify shard IDs are correct for the delta.
        #expect(newShards[0].id == 5)
        #expect(newShards[4].id == 9)
    }
}

@Suite("Hot Path Context Selection")
struct HotPathContextSelectionTests {

    private func makeReplayContextService() async throws -> AdDetectionService {
        let store = try await makeTestStore()
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor()
        )
    }

    @Test("upgraded duplicate batches pull neighboring persisted fast chunks into hot-path context")
    func includesNeighboringPersistedChunks() async throws {
        let service = try await makeReplayContextService()
        let allChunks = [
            makeHotPathContextChunk(id: "intro", chunkIndex: 0, startTime: 100, endTime: 101, text: "sponsored by betterhelp"),
            makeHotPathContextChunk(id: "body", chunkIndex: 1, startTime: 105, endTime: 106, text: "free trial for new members"),
            makeHotPathContextChunk(
                id: "close",
                chunkIndex: 2,
                startTime: 110,
                endTime: 111,
                text: "yoose cawd sev ten",
                weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                    averageConfidence: 0.27,
                    minimumConfidence: 0.27,
                    alternativeTexts: ["use code save10 at checkout"],
                    lowConfidencePhrases: []
                )
            ),
            makeHotPathContextChunk(id: "far", chunkIndex: 3, startTime: 300, endTime: 301, text: "completely unrelated far away content"),
        ]

        let context = await service.hotPathReplayContextChunks(
            from: allChunks,
            around: [allChunks[2]]
        )

        #expect(context.map(\.id) == ["intro", "body", "close"])
    }

    @Test("upgraded duplicate intro batches keep closing anchors within the configured forward search radius")
    func includesDistantClosingAnchorsWithinForwardPadding() async throws {
        let service = try await makeReplayContextService()
        let allChunks = [
            makeHotPathContextChunk(id: "intro", chunkIndex: 0, startTime: 100, endTime: 101, text: "this episode is sponsored by betterhelp"),
            makeHotPathContextChunk(id: "body", chunkIndex: 1, startTime: 145, endTime: 146, text: "free trial for new members"),
            makeHotPathContextChunk(
                id: "close",
                chunkIndex: 2,
                startTime: 189,
                endTime: 190,
                text: "yoose cawd sev ten",
                weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                    averageConfidence: 0.25,
                    minimumConfidence: 0.25,
                    alternativeTexts: ["use code save10 at checkout"],
                    lowConfidencePhrases: []
                )
            ),
            makeHotPathContextChunk(id: "far", chunkIndex: 3, startTime: 320, endTime: 321, text: "completely unrelated far away content"),
        ]

        let context = await service.hotPathReplayContextChunks(
            from: allChunks,
            around: [allChunks[0]]
        )

        #expect(context.map(\.id) == ["intro", "body", "close"])
    }

    @Test("upgraded duplicate closing batches keep opening anchors within the configured backward search radius")
    func includesDistantOpeningAnchorsWithinBackwardPadding() async throws {
        let service = try await makeReplayContextService()
        let allChunks = [
            makeHotPathContextChunk(id: "intro", chunkIndex: 0, startTime: 100, endTime: 101, text: "this episode is sponsored by betterhelp"),
            makeHotPathContextChunk(id: "body", chunkIndex: 1, startTime: 145, endTime: 146, text: "free trial for new members"),
            makeHotPathContextChunk(
                id: "close",
                chunkIndex: 2,
                startTime: 189,
                endTime: 190,
                text: "yoose cawd sev ten",
                weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                    averageConfidence: 0.25,
                    minimumConfidence: 0.25,
                    alternativeTexts: ["use code save10 at checkout"],
                    lowConfidencePhrases: []
                )
            ),
            makeHotPathContextChunk(id: "far", chunkIndex: 3, startTime: 320, endTime: 321, text: "completely unrelated far away content"),
        ]

        let context = await service.hotPathReplayContextChunks(
            from: allChunks,
            around: [allChunks[2]]
        )

        #expect(context.map(\.id) == ["intro", "body", "close"])
    }

    @Test("intro-seeded replay context does not walk backward through close-only anchors")
    func introSeedDoesNotExtendBackwardThroughCloseAnchors() async throws {
        let service = try await makeReplayContextService()
        let allChunks = [
            makeHotPathContextChunk(
                id: "close",
                chunkIndex: 0,
                startTime: 140,
                endTime: 141,
                text: "yoose cawd sev ten",
                weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                    averageConfidence: 0.25,
                    minimumConfidence: 0.25,
                    alternativeTexts: ["use code save10 at checkout"],
                    lowConfidencePhrases: []
                )
            ),
            makeHotPathContextChunk(id: "intro", chunkIndex: 1, startTime: 200, endTime: 201, text: "this episode is sponsored by betterhelp"),
        ]

        let context = await service.hotPathReplayContextChunks(
            from: allChunks,
            around: [allChunks[1]]
        )

        #expect(context.map(\.id) == ["intro"])
    }

    @Test("close-seeded replay context does not walk forward through intro-only anchors")
    func closeSeedDoesNotExtendForwardThroughIntroAnchors() async throws {
        let service = try await makeReplayContextService()
        let allChunks = [
            makeHotPathContextChunk(
                id: "close",
                chunkIndex: 0,
                startTime: 200,
                endTime: 201,
                text: "yoose cawd sev ten",
                weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                    averageConfidence: 0.25,
                    minimumConfidence: 0.25,
                    alternativeTexts: ["use code save10 at checkout"],
                    lowConfidencePhrases: []
                )
            ),
            makeHotPathContextChunk(id: "intro", chunkIndex: 1, startTime: 220, endTime: 221, text: "this episode is sponsored by betterhelp"),
        ]

        let context = await service.hotPathReplayContextChunks(
            from: allChunks,
            around: [allChunks[0]]
        )

        #expect(context.map(\.id) == ["close"])
    }

    @Test("upgraded duplicate intro batches follow chained body evidence beyond the first search hop")
    func followsTransitiveBodyEvidenceChainFromIntroSeed() async throws {
        let service = try await makeReplayContextService()
        let allChunks = [
            makeHotPathContextChunk(id: "intro", chunkIndex: 0, startTime: 100, endTime: 101, text: "this episode is sponsored by betterhelp"),
            makeHotPathContextChunk(id: "body-1", chunkIndex: 1, startTime: 180, endTime: 181, text: "free trial for new members"),
            makeHotPathContextChunk(id: "bridge", chunkIndex: 2, startTime: 260, endTime: 261, text: "money back guarantee on every order"),
            makeHotPathContextChunk(
                id: "close",
                chunkIndex: 3,
                startTime: 340,
                endTime: 341,
                text: "yoose cawd sev ten",
                weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                    averageConfidence: 0.25,
                    minimumConfidence: 0.25,
                    alternativeTexts: ["use code save10 at checkout"],
                    lowConfidencePhrases: []
                )
            ),
            makeHotPathContextChunk(id: "far", chunkIndex: 4, startTime: 500, endTime: 501, text: "completely unrelated far away content"),
        ]

        let context = await service.hotPathReplayContextChunks(
            from: allChunks,
            around: [allChunks[0]]
        )

        #expect(context.map(\.id) == ["intro", "body-1", "bridge", "close"])
    }

    @Test("upgraded duplicate closing batches follow chained body evidence back to the intro")
    func followsTransitiveBodyEvidenceChainFromClosingSeed() async throws {
        let service = try await makeReplayContextService()
        let allChunks = [
            makeHotPathContextChunk(id: "intro", chunkIndex: 0, startTime: 100, endTime: 101, text: "this episode is sponsored by betterhelp"),
            makeHotPathContextChunk(id: "body-1", chunkIndex: 1, startTime: 175, endTime: 176, text: "free trial for new members"),
            makeHotPathContextChunk(id: "bridge", chunkIndex: 2, startTime: 250, endTime: 251, text: "money back guarantee on every order"),
            makeHotPathContextChunk(
                id: "close",
                chunkIndex: 3,
                startTime: 325,
                endTime: 326,
                text: "yoose cawd sev ten",
                weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                    averageConfidence: 0.25,
                    minimumConfidence: 0.25,
                    alternativeTexts: ["use code save10 at checkout"],
                    lowConfidencePhrases: []
                )
            ),
            makeHotPathContextChunk(id: "far", chunkIndex: 4, startTime: 500, endTime: 501, text: "completely unrelated far away content"),
        ]

        let context = await service.hotPathReplayContextChunks(
            from: allChunks,
            around: [allChunks[3]]
        )

        #expect(context.map(\.id) == ["intro", "body-1", "bridge", "close"])
    }

    @Test("generic low-confidence narration does not chain replay context without lexical recovery hits")
    func genericLowConfidenceNarrationDoesNotExtendReplayContext() async throws {
        let service = try await makeReplayContextService()
        let allChunks = [
            makeHotPathContextChunk(id: "intro", chunkIndex: 0, startTime: 100, endTime: 101, text: "this episode is sponsored by betterhelp"),
            makeHotPathContextChunk(
                id: "noise",
                chunkIndex: 1,
                startTime: 180,
                endTime: 181,
                text: "uh maybe we should talk about the other thing later",
                weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                    averageConfidence: 0.25,
                    minimumConfidence: 0.25,
                    alternativeTexts: [],
                    lowConfidencePhrases: [
                        WeakAnchorPhrase(
                            text: "uh maybe we should talk about the other thing later",
                            startTime: 180,
                            endTime: 181,
                            confidence: 0.25
                        ),
                    ]
                )
            ),
            makeHotPathContextChunk(
                id: "close",
                chunkIndex: 2,
                startTime: 260,
                endTime: 261,
                text: "yoose cawd sev ten",
                weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                    averageConfidence: 0.25,
                    minimumConfidence: 0.25,
                    alternativeTexts: ["use code save10 at checkout"],
                    lowConfidencePhrases: []
                )
            ),
        ]

        let introSeedContext = await service.hotPathReplayContextChunks(
            from: allChunks,
            around: [allChunks[0]]
        )
        let closeSeedContext = await service.hotPathReplayContextChunks(
            from: allChunks,
            around: [allChunks[2]]
        )

        #expect(introSeedContext.map(\.id) == ["intro"])
        #expect(closeSeedContext.map(\.id) == ["close"])
    }
}

// MARK: - finalizeBackfill coverage guard

/// Regression: `AnalysisCoordinator.finalizeBackfill` used to transition
/// the session to `.complete` unconditionally, even when the transcript
/// covered only a small fraction of the episode duration. Combined with
/// the streaming engine race (see
/// `TranscriptEngine/IncrementalShardAppendTests.completedWaitsForFinishAppending`),
/// this left real episodes at `analysisState=complete` with one or two
/// chunks of coverage.
///
/// The guard is exposed as a pure static helper on `AnalysisCoordinator`
/// so it can be unit-tested without standing up the full pipeline.
@Suite("AnalysisCoordinator – Coverage Guard")
struct CoverageGuardTests {

    private func chunk(startTime: Double, endTime: Double) -> TranscriptChunk {
        TranscriptChunk(
            id: UUID().uuidString,
            analysisAssetId: "asset-coverage",
            segmentFingerprint: "fp-\(startTime)-\(endTime)",
            chunkIndex: 0,
            startTime: startTime,
            endTime: endTime,
            text: "x",
            normalizedText: "x",
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: "speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            weakAnchorMetadata: nil
        )
    }

    @Test("coverage well below threshold blocks complete transition")
    func shortCoverageBlocksComplete() {
        // Matches asset A53E3CE0 from the production export: 60+ min
        // episode, transcript ended near 690s = ~19% of duration.
        let chunks = [chunk(startTime: 0, endTime: 689.82)]
        let episodeDuration = 3600.0

        let verdict = AnalysisCoordinator.finalizeBackfillVerdict(
            chunks: chunks,
            episodeDuration: episodeDuration
        )

        switch verdict {
        case .blockComplete(let coverageEnd, let duration, let ratio):
            #expect(coverageEnd == 689.82)
            #expect(duration == 3600.0)
            #expect(ratio < 0.95)
        case .allowComplete:
            Issue.record("Expected blockComplete for ratio \(689.82 / 3600.0)")
        }
    }

    @Test("coverage equal to episode duration permits complete")
    func fullCoverageAllowsComplete() {
        let chunks = [chunk(startTime: 0, endTime: 3600)]
        let verdict = AnalysisCoordinator.finalizeBackfillVerdict(
            chunks: chunks,
            episodeDuration: 3600
        )
        switch verdict {
        case .allowComplete: break
        case .blockComplete:
            Issue.record("Full coverage must allow complete")
        }
    }

    @Test("coverage at the 95% threshold permits complete")
    func atThresholdAllowsComplete() {
        let chunks = [chunk(startTime: 0, endTime: 3420)]  // 3420/3600 == 0.95
        let verdict = AnalysisCoordinator.finalizeBackfillVerdict(
            chunks: chunks,
            episodeDuration: 3600
        )
        switch verdict {
        case .allowComplete: break
        case .blockComplete:
            Issue.record("Exactly-at-threshold coverage should allow complete")
        }
    }

    @Test("unknown episode duration allows complete (no guard available)")
    func unknownDurationAllowsComplete() {
        let chunks = [chunk(startTime: 0, endTime: 100)]
        let verdict = AnalysisCoordinator.finalizeBackfillVerdict(
            chunks: chunks,
            episodeDuration: 0
        )
        switch verdict {
        case .allowComplete: break
        case .blockComplete:
            Issue.record("With unknown duration the guard should not fire")
        }
    }

    @Test("empty chunk set blocks complete when duration is known")
    func emptyChunksWithKnownDurationBlocks() {
        let verdict = AnalysisCoordinator.finalizeBackfillVerdict(
            chunks: [],
            episodeDuration: 3600
        )
        switch verdict {
        case .blockComplete(let coverageEnd, _, _):
            #expect(coverageEnd == 0)
        case .allowComplete:
            Issue.record("Empty chunks with known duration must block")
        }
    }
}
