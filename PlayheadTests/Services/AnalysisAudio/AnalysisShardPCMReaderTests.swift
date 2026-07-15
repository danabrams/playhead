// AnalysisShardPCMReaderTests.swift
// playhead-l2f.6: hermetic tests for the ranged shard-cache PCM reader that
// feeds the stinger-refinement search envelopes.
//
// The reader is the PRODUCTION seam between `StingerRefiner` and the
// persisted 16 kHz analysis shards, so its index math and its
// timeline-continuity guard get direct coverage here (the wire-in tests
// inject a synthetic `StingerPCMProvider` and never touch this path).
//
// Fixtures write manifest + `shard_<N>.pcm` files straight into the same
// `Application Support/AnalysisShards/<episodeID>/` layout `ShardCache`
// persists (unique episodeID per test, removed on exit — the pattern
// `AnalysisAudioStreamingTests` uses). Writing the documented on-disk shape
// directly ALSO pins the persisted schema: if `ShardCache` ever moves the
// directory or renames a manifest key, these tests fail alongside the
// orphaned production caches such a change would create.
//
// Sample values encode their global sample index (`Float` is exact for
// integers < 2^24), so every assertion can verify both WHERE the slice
// starts and WHICH samples it carries.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisShardPCMReader (playhead-l2f.6)")
struct AnalysisShardPCMReaderTests {

    private static let sampleRate = Int(AnalysisAudioService.targetSampleRate)

    /// `Application Support/AnalysisShards` — must match `ShardCache`'s
    /// (file-private) layout, which is the documented persistence contract.
    private static func shardsRoot() -> URL {
        FileManager.default.urls(
            for: .applicationSupportDirectory, in: .userDomainMask
        ).first!
        .appendingPathComponent("AnalysisShards", isDirectory: true)
    }

    /// Ramp PCM: sample i carries the value `Float(firstGlobalIndex + i)`.
    private static func ramp(from firstGlobalIndex: Int, count: Int) -> [Float] {
        (0..<count).map { Float(firstGlobalIndex + $0) }
    }

    /// Write a manifest + shard files for a synthetic episode. A nil
    /// `samples` entry lists the shard in the manifest WITHOUT writing its
    /// file (the missing-shard corruption case). Returns the episode
    /// directory; callers remove it in a `defer`.
    private static func writeCache(
        episodeID: String,
        shards: [(id: Int, startTime: Double, duration: Double, samples: [Float]?)]
    ) throws -> URL {
        let dir = shardsRoot().appendingPathComponent(episodeID, isDirectory: true)
        try FileManager.default.createDirectory(
            at: dir, withIntermediateDirectories: true
        )
        let manifest: [[String: Any]] = shards.map {
            ["id": $0.id, "startTime": $0.startTime, "duration": $0.duration]
        }
        try JSONSerialization.data(withJSONObject: manifest)
            .write(to: dir.appendingPathComponent("manifest.json"))
        for shard in shards {
            guard let samples = shard.samples else { continue }
            let data = samples.withUnsafeBufferPointer { Data(buffer: $0) }
            try data.write(to: dir.appendingPathComponent("shard_\(shard.id).pcm"))
        }
        return dir
    }

    /// Three contiguous 2 s shards covering [0, 6) with global-index ramp
    /// values 0..95999.
    private static func writeContiguousThreeShardCache(
        episodeID: String
    ) throws -> URL {
        let perShard = 2 * sampleRate
        return try writeCache(episodeID: episodeID, shards: [
            (0, 0.0, 2.0, ramp(from: 0, count: perShard)),
            (1, 2.0, 2.0, ramp(from: perShard, count: perShard)),
            (2, 4.0, 2.0, ramp(from: 2 * perShard, count: perShard)),
        ])
    }

    // MARK: - Happy paths

    @Test("Mid-range read across shard boundaries returns exact samples and start time")
    func midRangeAcrossShards() throws {
        let episodeID = "l2f6-reader-mid-\(UUID().uuidString)"
        let dir = try Self.writeContiguousThreeShardCache(episodeID: episodeID)
        defer { try? FileManager.default.removeItem(at: dir) }

        let slice = try #require(AnalysisShardPCMReader.loadSamples(
            episodeID: episodeID, from: 1.5, to: 4.5
        ))
        #expect(slice.startSeconds == 1.5)
        #expect(slice.samples.count == 3 * Self.sampleRate)
        // First sample = global index at 1.5 s; last = the sample before 4.5 s.
        #expect(slice.samples.first == Float(3 * Self.sampleRate / 2))
        #expect(slice.samples.last == Float(9 * Self.sampleRate / 2 - 1))
        // Shard 0→1 boundary lands 0.5 s into the slice — the sample there
        // must be the FIRST sample of shard 1 (global index 2 s), not a
        // duplicate or a skip.
        #expect(slice.samples[Self.sampleRate / 2] == Float(2 * Self.sampleRate))
        // Shard 1→2 boundary (2.5 s into the slice → global index 4 s).
        #expect(slice.samples[5 * Self.sampleRate / 2] == Float(4 * Self.sampleRate))
    }

    @Test("Out-of-bounds request is clipped to what the cache holds")
    func requestClippedToCacheBounds() throws {
        let episodeID = "l2f6-reader-clip-\(UUID().uuidString)"
        let dir = try Self.writeContiguousThreeShardCache(episodeID: episodeID)
        defer { try? FileManager.default.removeItem(at: dir) }

        let slice = try #require(AnalysisShardPCMReader.loadSamples(
            episodeID: episodeID, from: -3.0, to: 100.0
        ))
        #expect(slice.startSeconds == 0.0)
        #expect(slice.samples.count == 6 * Self.sampleRate)
        #expect(slice.samples.first == Float(0))
        #expect(slice.samples.last == Float(6 * Self.sampleRate - 1))
    }

    @Test("A short tail shard clips the slice to the decoded audio")
    func tailShardShorterThanFull() throws {
        let episodeID = "l2f6-reader-tail-\(UUID().uuidString)"
        let perShard = 2 * Self.sampleRate
        let dir = try Self.writeCache(episodeID: episodeID, shards: [
            (0, 0.0, 2.0, Self.ramp(from: 0, count: perShard)),
            (1, 2.0, 2.0, Self.ramp(from: perShard, count: perShard)),
            // 1 s tail — the normal shape ShardCache writes for the
            // episode remainder.
            (2, 4.0, 1.0, Self.ramp(from: 2 * perShard, count: Self.sampleRate)),
        ])
        defer { try? FileManager.default.removeItem(at: dir) }

        let slice = try #require(AnalysisShardPCMReader.loadSamples(
            episodeID: episodeID, from: 3.9, to: 60.0
        ))
        #expect(abs(slice.startSeconds - 3.9) < 1e-9)
        // [3.9, 5.0) of audio exists: 0.1 s of shard 1 + the 1 s tail.
        // Integer math for the expected values — `Int(3.9 * 16_000)` would
        // truncate the binary representation of 3.9 down a sample.
        let expectedCount = (11 * Self.sampleRate) / 10
        let expectedFirstGlobalIndex = (39 * Self.sampleRate) / 10
        #expect(slice.samples.count == expectedCount)
        #expect(slice.samples.first == Float(expectedFirstGlobalIndex))
        #expect(slice.samples.last == Float(2 * perShard + Self.sampleRate - 1))
    }

    // MARK: - Unavailable / corrupt caches

    @Test("Missing manifest returns nil")
    func missingManifestReturnsNil() {
        #expect(AnalysisShardPCMReader.loadSamples(
            episodeID: "l2f6-reader-absent-\(UUID().uuidString)", from: 0, to: 10
        ) == nil)
    }

    @Test("A manifest gap inside the range returns nil; outside the range it is harmless")
    func manifestGapGuard() throws {
        let episodeID = "l2f6-reader-gap-\(UUID().uuidString)"
        let perShard = 2 * Self.sampleRate
        // Shard 1 ([2, 4)) is missing from the manifest entirely.
        let dir = try Self.writeCache(episodeID: episodeID, shards: [
            (0, 0.0, 2.0, Self.ramp(from: 0, count: perShard)),
            (2, 4.0, 2.0, Self.ramp(from: 2 * perShard, count: perShard)),
        ])
        defer { try? FileManager.default.removeItem(at: dir) }

        // Range spanning the hole: concatenating [0,2) + [4,6) would shift
        // every post-hole sample's implied time by 2 s — must refuse.
        #expect(AnalysisShardPCMReader.loadSamples(
            episodeID: episodeID, from: 1.0, to: 5.0
        ) == nil)

        // Range entirely inside the surviving second shard: the hole is
        // OUTSIDE the request and must not poison the read.
        let slice = try #require(AnalysisShardPCMReader.loadSamples(
            episodeID: episodeID, from: 4.5, to: 5.5
        ))
        #expect(slice.startSeconds == 4.5)
        #expect(slice.samples.count == Self.sampleRate)
        #expect(slice.samples.first == Float(2 * perShard + Self.sampleRate / 2))
    }

    @Test("A shard file shorter than its manifest duration mid-range returns nil")
    func truncatedShardFileMidRangeReturnsNil() throws {
        let episodeID = "l2f6-reader-trunc-\(UUID().uuidString)"
        let perShard = 2 * Self.sampleRate
        let dir = try Self.writeCache(episodeID: episodeID, shards: [
            (0, 0.0, 2.0, Self.ramp(from: 0, count: perShard)),
            // Manifest claims 2 s but the file only holds 1 s — the reader
            // must detect the discontinuity against shard 2 and bail.
            (1, 2.0, 2.0, Self.ramp(from: perShard, count: Self.sampleRate)),
            (2, 4.0, 2.0, Self.ramp(from: 2 * perShard, count: perShard)),
        ])
        defer { try? FileManager.default.removeItem(at: dir) }

        #expect(AnalysisShardPCMReader.loadSamples(
            episodeID: episodeID, from: 1.0, to: 5.0
        ) == nil)
    }

    @Test("A manifest entry whose shard file is missing returns nil")
    func missingShardFileMidRangeReturnsNil() throws {
        let episodeID = "l2f6-reader-nofile-\(UUID().uuidString)"
        let perShard = 2 * Self.sampleRate
        let dir = try Self.writeCache(episodeID: episodeID, shards: [
            (0, 0.0, 2.0, Self.ramp(from: 0, count: perShard)),
            (1, 2.0, 2.0, nil), // listed in the manifest, file never written
            (2, 4.0, 2.0, Self.ramp(from: 2 * perShard, count: perShard)),
        ])
        defer { try? FileManager.default.removeItem(at: dir) }

        #expect(AnalysisShardPCMReader.loadSamples(
            episodeID: episodeID, from: 1.0, to: 5.0
        ) == nil)
    }

    @Test("Empty, inverted, and fully out-of-range requests return nil")
    func degenerateRangesReturnNil() throws {
        let episodeID = "l2f6-reader-degenerate-\(UUID().uuidString)"
        let dir = try Self.writeContiguousThreeShardCache(episodeID: episodeID)
        defer { try? FileManager.default.removeItem(at: dir) }

        #expect(AnalysisShardPCMReader.loadSamples(
            episodeID: episodeID, from: 3.0, to: 3.0
        ) == nil)
        #expect(AnalysisShardPCMReader.loadSamples(
            episodeID: episodeID, from: 5.0, to: 1.0
        ) == nil)
        // Entirely past the cached audio ([0, 6)).
        #expect(AnalysisShardPCMReader.loadSamples(
            episodeID: episodeID, from: 10.0, to: 12.0
        ) == nil)
    }
}
