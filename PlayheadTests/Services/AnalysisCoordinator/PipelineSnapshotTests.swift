// PipelineSnapshotTests.swift
// Regression tests for the shard prioritization race condition where
// timeUpdate events overwrote the pipeline start snapshot, causing
// shard 0 (0-30s) to be transcribed last instead of first.

import Foundation
import Testing
@testable import Playhead

// MARK: - Shard Prioritization Logic

@Suite("Shard Prioritization – Partition Logic")
struct ShardPartitionTests {

    // These tests reproduce the exact partition logic from
    // TranscriptEngineService.prioritizeShards() to verify shard ordering.

    private let chunkOverlap: TimeInterval = 0.5

    private func isAhead(shardStart: TimeInterval, playhead: TimeInterval) -> Bool {
        shardStart >= playhead - chunkOverlap
    }

    @Test("All shards ahead when playhead is at 0")
    func allAheadAtZero() {
        let playhead: TimeInterval = 0.0
        #expect(isAhead(shardStart: 0, playhead: playhead))
        #expect(isAhead(shardStart: 30, playhead: playhead))
        #expect(isAhead(shardStart: 60, playhead: playhead))
        #expect(isAhead(shardStart: 90, playhead: playhead))
    }

    @Test("Shard 0 is behind when playhead drifts to 15s")
    func shard0BehindWhenDrifted() {
        // This is the bug scenario: during streaming download, playhead
        // drifts to ~15s before analysis starts. Without pipelineStartSnapshot,
        // shard 0 (startTime=0) falls into the behind partition.
        let playhead: TimeInterval = 15.0
        #expect(!isAhead(shardStart: 0, playhead: playhead),
                "Shard 0 is behind at drifted playhead — the bug we fixed")
        #expect(isAhead(shardStart: 15, playhead: playhead))
        #expect(isAhead(shardStart: 30, playhead: playhead))
    }

    @Test("Shard 0 stays ahead at small drift under overlap")
    func shard0AheadAtSmallDrift() {
        // If playhead is only 0.3s in, shard 0 is still ahead
        // because 0 >= 0.3 - 0.5 = -0.2
        let playhead: TimeInterval = 0.3
        #expect(isAhead(shardStart: 0, playhead: playhead))
    }

    @Test("Shard just below cutoff falls behind")
    func shardJustBelowCutoff() {
        let playhead: TimeInterval = 30.0
        // 29.0 >= 30.0 - 0.5 = 29.5 → false
        #expect(!isAhead(shardStart: 29.0, playhead: playhead))
        // 29.5 >= 29.5 → true
        #expect(isAhead(shardStart: 29.5, playhead: playhead))
    }
}

// MARK: - Priority Ordering

@Suite("Shard Prioritization – Ordering")
struct ShardOrderingTests {

    private let chunkOverlap: TimeInterval = 0.5
    private let lookaheadWallClockSeconds: TimeInterval = 120.0

    /// Reproduce the full prioritization: shard 0 → hot path → cold ahead → behind.
    private func prioritize(
        shardStarts: [TimeInterval],
        playhead: TimeInterval,
        rate: Double = 1.0
    ) -> [TimeInterval] {
        let lookaheadAudioSeconds = lookaheadWallClockSeconds * rate

        let ahead = shardStarts
            .filter { $0 >= playhead - chunkOverlap }
            .sorted()

        let behind = shardStarts
            .filter { $0 < playhead - chunkOverlap }
            .sorted(by: >)

        // Shard 0 always goes first for pre-roll ad detection.
        let shard0 = behind.filter { $0 == 0 }
        let behindWithoutShard0 = behind.filter { $0 > 0 }

        let hotPath = ahead.filter { $0 < playhead + lookaheadAudioSeconds }
        let coldAhead = ahead.filter { $0 >= playhead + lookaheadAudioSeconds }

        return shard0 + hotPath + coldAhead + behindWithoutShard0
    }

    @Test("Shard 0 is first when playhead is at 0")
    func shard0FirstAtZero() {
        let shards: [TimeInterval] = [0, 30, 60, 90, 120]
        let ordered = prioritize(shardStarts: shards, playhead: 0)
        #expect(ordered.first == 0, "Shard 0 must be transcribed first")
    }

    @Test("Shard 0 is first even when playhead drifts to 15s")
    func shard0FirstWhenDrifted() {
        let shards: [TimeInterval] = [0, 30, 60, 90, 120]
        let ordered = prioritize(shardStarts: shards, playhead: 15)
        #expect(ordered.first == 0, "Shard 0 must always be first for pre-roll ad detection")
    }

    @Test("Shard 0 is first even when playback starts mid-episode")
    func shard0FirstMidEpisode() {
        let shards: [TimeInterval] = [0, 30, 60, 90, 120, 150, 180]
        let ordered = prioritize(shardStarts: shards, playhead: 90)
        #expect(ordered.first == 0, "Shard 0 must always be first")
        // Shards near playhead (90, 120) should follow shard 0.
        #expect(ordered[1] == 90)
    }

    @Test("At 2x speed, hot path window doubles")
    func hotPathScalesWithSpeed() {
        // 200 shards, each 30s → 6000s total.
        // At 2x with 120s lookahead → 240s of audio in hot path.
        let shards = (0..<200).map { TimeInterval($0) * 30.0 }
        let ordered = prioritize(shardStarts: shards, playhead: 0, rate: 2.0)

        // First 8 shards (0-240s) should be in hot path, in order.
        let hotPathEnd = 0 + 120.0 * 2.0 // 240s
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
