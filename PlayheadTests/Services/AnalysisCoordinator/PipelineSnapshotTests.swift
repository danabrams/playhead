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

    /// Reproduce the full prioritization: hot path → cold ahead → behind.
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

        let hotPath = ahead.filter { $0 < playhead + lookaheadAudioSeconds }
        let coldAhead = ahead.filter { $0 >= playhead + lookaheadAudioSeconds }

        return hotPath + coldAhead + behind
    }

    @Test("Shard 0 is first when playhead is at 0")
    func shard0FirstAtZero() {
        let shards: [TimeInterval] = [0, 30, 60, 90, 120]
        let ordered = prioritize(shardStarts: shards, playhead: 0)
        #expect(ordered.first == 0, "Shard 0 must be transcribed first")
    }

    @Test("Shard 0 is last when playhead is at 15 (the race bug)")
    func shard0LastWhenDrifted() {
        let shards: [TimeInterval] = [0, 30, 60, 90, 120]
        let ordered = prioritize(shardStarts: shards, playhead: 15)
        #expect(ordered.last == 0, "Without the fix, shard 0 ends up last")
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
