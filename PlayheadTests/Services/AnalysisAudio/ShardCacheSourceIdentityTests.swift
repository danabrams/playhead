// ShardCacheSourceIdentityTests.swift
// playhead-8ysk part 1 — a decode cache keyed on episodeID alone is immortal.
//
// THE DEFECT. `ShardCache` stored `[{id, startTime, duration}]` and looked it
// up by `episodeID`. Nothing recorded what the shards had been decoded FROM,
// so nothing could ever decide the entry was stale. Three consequences, all
// observed on the owner's device across six days (147 jobs acquired, 9
// finalized):
//
//   1. Retry was a no-op by construction. Every scheduler epoch and job
//      generation re-ran the identical decode result, so an episode that
//      failed once failed the same way forever.
//   2. The download-completeness gate was inert. It is byte-exact
//      (`DownloadManager.servingURLIfComplete` compares `fileSize` against
//      `pin.expectedBytes` and verifies SHA) but it guards the URL, and the
//      URL was discarded on a cache hit.
//   3. The 8 MiB `defaultPlayableThreshold` prefix — the file analysis
//      spools against the instant playback becomes possible — could be
//      cached as if it were the whole episode.
//
// WHY `isTruncated` DOES NOT ALREADY COVER (3). `performDecode` refuses to
// cache a decode that falls short of the duration the CONTAINER declares.
// Whether a partial file declares the whole episode depends on its header,
// and it was measured directly for this bead (600 s MP3s cut to a 20 %
// prefix, AVFoundation on this machine):
//
//   with a Xing/Info header    declared 600.00 s, decoded 119.95 s -> truncated
//   headerless CBR (lame -t)   declared 120.01 s, decoded 120.01 s -> NOT truncated
//
// A headerless prefix is complete as far as it is concerned. `isTruncated` is
// false, and the partial decode is cached. That shape — CBR MP3 with no Xing
// frame count — is ordinary podcast audio.
//
// THE FIX under test: the manifest records the source file's byte length, and
// a cache hit is only served when the file at the decode URL still has that
// length. A prefix and its completed download never share one.

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("playhead-8ysk — the shard cache is keyed on its source, not just the episode")
struct ShardCacheSourceIdentityTests {

    private static let tempDirs = TestTempDirTracker()

    // MARK: - Fixture

    /// Write a mono 16 kHz float CAF of `seconds` at `url`, replacing
    /// whatever is there. CAF because it round-trips exactly and needs no
    /// encoder — the container is not what is under test here, the cache
    /// key is.
    private func writeSynthAudio(seconds: TimeInterval, to url: URL) throws {
        if FileManager.default.fileExists(atPath: url.path) {
            try FileManager.default.removeItem(at: url)
        }
        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: 16_000,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "ShardCacheSourceIdentityTests", code: -1)
        }
        let file = try AVAudioFile(
            forWriting: url,
            settings: format.settings,
            commonFormat: .pcmFormatFloat32,
            interleaved: false
        )
        let totalFrames = AVAudioFramePosition(seconds * 16_000)
        var written = AVAudioFramePosition(0)
        while written < totalFrames {
            let frames = AVAudioFrameCount(
                min(AVAudioFramePosition(16_000), totalFrames - written)
            )
            guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames) else {
                throw NSError(domain: "ShardCacheSourceIdentityTests", code: -2)
            }
            buffer.frameLength = frames
            if let channel = buffer.floatChannelData?[0] {
                for i in 0..<Int(frames) {
                    channel[i] = Float(sin(
                        2.0 * Double.pi * 440.0
                            * Double(written + AVAudioFramePosition(i)) / 16_000.0
                    )) * 0.25
                }
            }
            try file.write(from: buffer)
            written += AVAudioFramePosition(frames)
        }
    }

    private func makeAudioURL() throws -> URL {
        let dir = try makeTempDir(prefix: "Bd8yskAudio")
        Self.tempDirs.track(dir)
        return dir.appendingPathComponent("source-\(UUID().uuidString).caf")
    }

    /// The on-disk cache directory for an episode. Built the same way
    /// `AnalysisShardPCMReaderTests` builds it — from the root the service
    /// exposes — so the layout constant is not duplicated.
    private func cacheDirectory(episodeID: String) -> URL {
        AnalysisAudioService.shardCacheRootDirectory
            .appendingPathComponent(episodeID, isDirectory: true)
    }

    // MARK: - 1. The predicate

    @Test("a cache entry stamped with a different source length is not valid")
    func lengthMismatchIsInvalid() {
        // The incident, in numbers: 8 MiB of an episode whose completed
        // download is 90 MB.
        #expect(!AnalysisAudioService.isShardCacheValid(
            recordedSourceByteLength: 8_388_608,
            currentSourceByteLength: 94_371_840
        ))
        #expect(AnalysisAudioService.isShardCacheValid(
            recordedSourceByteLength: 94_371_840,
            currentSourceByteLength: 94_371_840
        ))
        // Off by a single byte is still a different file.
        #expect(!AnalysisAudioService.isShardCacheValid(
            recordedSourceByteLength: 94_371_839,
            currentSourceByteLength: 94_371_840
        ))
    }

    @Test("a pre-8ysk manifest carries no stamp and is never served over a readable source")
    func unstampedEntryIsInvalidWhenSourceIsReadable() {
        #expect(!AnalysisAudioService.isShardCacheValid(
            recordedSourceByteLength: nil,
            currentSourceByteLength: 8_388_608
        ))
    }

    /// The one case where an unvalidatable entry is still served: the source
    /// is gone. The shard cache outlives the audio file on purpose —
    /// boundary refinement, chapter snapshots and the final pass all read it
    /// after the download has been evicted — so "cannot measure" must mean
    /// "serve", not "discard". Reversing this would silently disable every
    /// post-eviction consumer.
    @Test("an unmeasurable source serves the cache rather than discarding it")
    func unmeasurableSourceStillServesTheCache() {
        #expect(AnalysisAudioService.isShardCacheValid(
            recordedSourceByteLength: 8_388_608,
            currentSourceByteLength: nil
        ))
        #expect(AnalysisAudioService.isShardCacheValid(
            recordedSourceByteLength: nil,
            currentSourceByteLength: nil
        ))
    }

    @Test("sourceByteLength measures a real file and returns nil for a missing one")
    func sourceByteLengthMeasuresRealFiles() throws {
        let url = try makeAudioURL()
        try writeSynthAudio(seconds: 3, to: url)
        let measured = try #require(AnalysisAudioService.sourceByteLength(of: url))
        let expected = try #require(
            (try FileManager.default.attributesOfItem(atPath: url.path))[.size] as? NSNumber
        ).int64Value
        #expect(measured == expected)
        #expect(measured > 0)

        try FileManager.default.removeItem(at: url)
        #expect(AnalysisAudioService.sourceByteLength(of: url) == nil)
    }

    // MARK: - 2. The defect itself, end to end

    /// THE REGRESSION. Decode a short file, let it be cached, then let the
    /// same URL grow — exactly what happens when a download crosses the
    /// 8 MiB playable threshold, gets analysed, and then finishes. Before
    /// this bead the second decode returned the SHORT result, permanently.
    ///
    /// The assertion is on decoded extent, not on a cache-internals probe,
    /// so it fails for the user-visible reason: the pipeline is looking at a
    /// prefix of the episode and can never advance past it.
    @Test("a cached decode is discarded once the source file grows at the same URL")
    func cacheIsDiscardedWhenSourceGrows() async throws {
        let url = try makeAudioURL()
        let service = AnalysisAudioService()
        let episodeID = "bd8ysk-grow-\(UUID().uuidString)"
        defer { Task { await service.evictCache(episodeID: episodeID) } }

        // The mid-download prefix.
        try writeSynthAudio(seconds: 6, to: url)
        let prefixBytes = try #require(AnalysisAudioService.sourceByteLength(of: url))
        let local = try #require(LocalAudioURL(url))
        let first = try await service.decodeOutcome(fileURL: local, episodeID: episodeID)
        let firstDuration = first.shards.map(\.duration).reduce(0, +)
        try #require(abs(firstDuration - 6) < 1.0, "prefix decoded \(firstDuration)s, expected ~6s")
        // It really is in the cache — otherwise the second pass would be a
        // plain recompute and would prove nothing about invalidation.
        try #require(
            FileManager.default.fileExists(
                atPath: cacheDirectory(episodeID: episodeID)
                    .appendingPathComponent("manifest.json").path
            ),
            "the prefix decode must have been cached for this test to mean anything"
        )

        // The download completes: same URL, more bytes.
        try writeSynthAudio(seconds: 45, to: url)
        let completeBytes = try #require(AnalysisAudioService.sourceByteLength(of: url))
        try #require(completeBytes > prefixBytes, "the fixture must actually grow")

        let second = try await service.decodeOutcome(fileURL: local, episodeID: episodeID)
        let secondDuration = second.shards.map(\.duration).reduce(0, +)
        #expect(
            abs(secondDuration - 45) < 1.0,
            """
            the completed download must be re-decoded, not answered from the \
            prefix's cache (got \(secondDuration)s, expected ~45s)
            """
        )
        #expect(second.shards.count > first.shards.count)
    }

    /// A pre-8ysk manifest is a bare JSON array with no source stamp. It has
    /// to be discarded the first time it is read against a readable source —
    /// that is what clears the entries already sitting on the owner's device,
    /// which is where the six days of evidence came from.
    ///
    /// The staged cache is COMPLETE (manifest plus a real PCM file), so a
    /// build that ignores the stamp genuinely serves it. Staging a manifest
    /// with no PCM behind it would be rejected by the existing
    /// missing-shard-file guard and the test would pass against the bug.
    @Test("a legacy unstamped manifest is discarded when the source is readable")
    func legacyManifestIsDiscarded() async throws {
        let url = try makeAudioURL()
        try writeSynthAudio(seconds: 6, to: url)
        let service = AnalysisAudioService()
        let episodeID = "bd8ysk-legacy-\(UUID().uuidString)"
        defer { Task { await service.evictCache(episodeID: episodeID) } }

        // Stage a v1 cache by hand: one 30 s shard of silence, and the bare
        // array manifest the old writer produced.
        let dir = cacheDirectory(episodeID: episodeID)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let samples = [Float](repeating: 0, count: 30 * 16_000)
        let pcm = samples.withUnsafeBufferPointer { Data(buffer: $0) }
        try pcm.write(to: dir.appendingPathComponent("shard_0.pcm"))
        let legacyManifest = #"[{"id":0,"startTime":0,"duration":30}]"#
        try Data(legacyManifest.utf8).write(to: dir.appendingPathComponent("manifest.json"))

        // Prove the staged cache is loadable in its own right — otherwise a
        // pass here would say nothing about the stamp.
        let staged = try #require(
            AnalysisShardPCMReader.loadSamples(episodeID: episodeID, from: 0, to: 30),
            "the staged v1 cache must be readable, or this test proves nothing"
        )
        #expect(staged.samples.count > 0)

        let local = try #require(LocalAudioURL(url))
        let outcome = try await service.decodeOutcome(fileURL: local, episodeID: episodeID)
        let decoded = outcome.shards.map(\.duration).reduce(0, +)
        #expect(
            abs(decoded - 6) < 1.0,
            """
            an unstamped entry must be re-decoded from the real 6 s file, not \
            served as the staged 30 s cache (got \(decoded)s)
            """
        )
    }

    /// And the entry is EVICTED, not merely bypassed. A stale directory that
    /// is skipped on every read but never removed is ~230 MB per decoded hour
    /// of non-purgeable Application Support that nothing else sweeps — the
    /// same leak `ShardCache.removeOrphanedDirectories` exists to bound for
    /// the rediff B-side.
    ///
    /// The invalidation here goes the other way — the source SHRINKS — for a
    /// reason: it is the only direction that can distinguish eviction from a
    /// plain overwrite. When the replacement decode is longer, its own
    /// `saveShards` overwrites every stale file and a build that never
    /// evicted anything would still pass. When it is shorter, the surplus
    /// shard files have no overwriter and survive unless the directory was
    /// removed.
    @Test("an invalidated cache directory is removed, not just ignored")
    func invalidatedCacheIsEvicted() async throws {
        let url = try makeAudioURL()
        let service = AnalysisAudioService()
        let episodeID = "bd8ysk-evict-\(UUID().uuidString)"
        defer { Task { await service.evictCache(episodeID: episodeID) } }

        // 45 s at a 30 s shard duration is two shards.
        try writeSynthAudio(seconds: 45, to: url)
        let local = try #require(LocalAudioURL(url))
        _ = try await service.decodeOutcome(fileURL: local, episodeID: episodeID)

        func cachedShardFiles() throws -> [String] {
            try FileManager.default.contentsOfDirectory(
                atPath: cacheDirectory(episodeID: episodeID).path
            ).filter { $0.hasPrefix("shard_") }.sorted()
        }
        try #require(try cachedShardFiles() == ["shard_0.pcm", "shard_1.pcm"])

        // Replace the source with a shorter one: a single-shard decode.
        try writeSynthAudio(seconds: 6, to: url)
        _ = try await service.decodeOutcome(fileURL: local, episodeID: episodeID)

        #expect(
            try cachedShardFiles() == ["shard_0.pcm"],
            """
            the invalidated directory must be removed — shard_1.pcm belongs to \
            the superseded decode and nothing else in the app will ever sweep it
            """
        )
    }
}
