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
    private func writeSynthAudio(
        seconds: TimeInterval,
        to url: URL,
        amplitude: Float = 0.25
    ) throws {
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
                    )) * amplitude
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

    /// A stamp with an arbitrary but fixed modification time, so length-only
    /// cases read as they did before review r2 added the second field.
    private func identity(_ bytes: Int64, at nanos: Int64 = 700_000_000_000_000_000)
        -> AnalysisAudioService.SourceIdentity {
        AnalysisAudioService.SourceIdentity(byteLength: bytes, modificationTimeNanos: nanos)
    }

    @Test("a cache entry stamped with a different source length is not valid")
    func lengthMismatchIsInvalid() {
        // The incident, in numbers: 8 MiB of an episode whose completed
        // download is 90 MB.
        #expect(!AnalysisAudioService.isShardCacheValid(
            recorded: identity(8_388_608),
            current: identity(94_371_840)
        ))
        #expect(AnalysisAudioService.isShardCacheValid(
            recorded: identity(94_371_840),
            current: identity(94_371_840)
        ))
        // Off by a single byte is still a different file.
        #expect(!AnalysisAudioService.isShardCacheValid(
            recorded: identity(94_371_839),
            current: identity(94_371_840)
        ))
    }

    /// REVIEW R2. The case a byte length cannot see: the same URL holding a
    /// DIFFERENT file of exactly the same size. Round 1 dismissed this as
    /// benign on the premise that production mutation is monotone growth at
    /// one URL — but `DownloadManager.evictIfNeeded()` (LRU) and
    /// `clearCache()` both delete complete audio without touching the shard
    /// cache, so a re-download writes a new file at the old path. And the
    /// re-download is a DAI re-stitch: the cross-network survey behind
    /// `RediffFetchPersona` found 0 of 6 networks re-encode the body, so two
    /// stitches differ only by the ad pods, and ad inventory is
    /// duration-quantized at a fixed bitrate. Equal totals are ordinary.
    @Test("a same-length replacement at the same path is not valid")
    func sameLengthDifferentFileIsInvalid() {
        let downloaded = identity(94_371_840, at: 700_000_000_000_000_000)
        let reDownloaded = identity(94_371_840, at: 700_086_400_000_000_000)
        #expect(
            !AnalysisAudioService.isShardCacheValid(
                recorded: downloaded, current: reDownloaded
            ),
            """
            a re-download of identical length must invalidate. Serving here \
            means ad windows located in the OLD stitch are applied to the new \
            audio — silently, since nothing downstream can tell
            """
        )
        // Same file, untouched: still valid. Otherwise the rule above is just
        // "never serve", which is the over-correction round 1 caught.
        #expect(AnalysisAudioService.isShardCacheValid(
            recorded: downloaded, current: downloaded
        ))
    }

    @Test("a pre-8ysk manifest carries no stamp and is never served over a readable source")
    func unstampedEntryIsInvalidWhenSourceIsReadable() {
        #expect(!AnalysisAudioService.isShardCacheValid(
            recorded: nil,
            current: identity(8_388_608)
        ))
    }

    /// The one case where an unvalidatable entry is still served: the source
    /// is gone. The shard cache outlives the audio file on purpose, so
    /// "cannot measure" must mean "serve", not "discard".
    ///
    /// The dependent is a REAL test, named so a future reader can check the
    /// claim: playhead-0hi9's `cachedDecodeReportsNotTruncated` deletes the
    /// audio between two `decodeOutcome` calls and requires the second to be
    /// answered from the cache — the shape of a re-spool after the download
    /// has been evicted, which `persistSpooledEpisodeDuration` depends on.
    /// The post-analysis consumers (boundary refinement, chapter snapshots)
    /// are NOT dependents: they read `AnalysisShardPCMReader`, which goes
    /// straight to the manifest and never reaches this predicate.
    ///
    /// The second expectation is the one that costs something: an UNSTAMPED
    /// entry over a vanished source is served, so a pre-8ysk manifest for an
    /// episode whose audio is gone survives this bead. That is deliberate —
    /// with no source there is nothing to re-decode from, so discarding it
    /// would trade a stale answer for no answer — and it self-heals, because
    /// the moment the audio is back the source is measurable and the
    /// unstamped entry is discarded by the rule above.
    @Test("an unmeasurable source serves the cache rather than discarding it")
    func unmeasurableSourceStillServesTheCache() {
        #expect(AnalysisAudioService.isShardCacheValid(
            recorded: identity(8_388_608),
            current: nil
        ))
        #expect(AnalysisAudioService.isShardCacheValid(
            recorded: nil,
            current: nil
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

    /// THE OTHER HALF OF THE PREDICATE, and it was missing (round-1 review).
    ///
    /// Every other test in this file, and every test in
    /// `TruncatedDecodeDurationTests`, asserts that the cache is DISCARDED —
    /// on a grown source, on an unstamped manifest, on a shrunk source. The
    /// one existing test that proves a cache HIT
    /// (`cachedDecodeReportsNotTruncated`) deletes the audio first, so it
    /// travels the `currentSourceByteLength == nil` fail-open branch and says
    /// nothing about a stamp that matches.
    ///
    /// Measured, not argued: mutating `ShardCache.readManifest` to return
    /// `(envelope.entries, nil)` — which makes every entry read as unstamped
    /// and so disables the cache entirely against any readable source — left
    /// `ShardCacheSourceIdentityTests`, `TruncatedDecodeDurationTests` and
    /// `AnalysisShardPCMReaderTests` fully green (59 tests, 0 failures). A
    /// build that re-decodes the whole episode on every single retry was
    /// indistinguishable from a correct one. That is the failure mode this
    /// bead is one careless follow-up away from: the fix for an over-serving
    /// cache is an over-discarding cache, and on device the cost is a full
    /// re-decode of an hour of audio per scheduler epoch.
    ///
    /// The probe is a source that CHANGES CONTENT AT A CONSTANT LENGTH: the
    /// same duration of the same tone at a different amplitude is byte-for-byte
    /// the same size of CAF. The stamp therefore still matches, the cache must
    /// still be served, and the samples that come back must be the ORIGINAL
    /// quiet ones. A build that re-decoded would hand back the loud ones.
    /// REVIEW R2 REWORK. Round 1 proved the serve by replacing the source's
    /// CONTENT at a constant LENGTH and requiring the cached (quiet) samples
    /// back. That probe no longer says what it used to: the stamp now carries
    /// the source's modification time as well, so a constant-length
    /// replacement is correctly invalidated — and it is the subject of its own
    /// test below (`constantLengthReplacementIsRedecoded`), which is the
    /// end-to-end proof that the second field does real work.
    ///
    /// The serve is now proved WITHOUT touching the source at all, which is
    /// strictly stronger: the cache is primed with MARKER shards — three of
    /// them, samples at a flat 0.25 — that no decode of a 45 s sine file could
    /// ever produce. If `decodeOutcome` hands those back, it did not decode.
    /// A build that re-decodes returns two shards of sine and fails on both
    /// the count and the flatness.
    @Test("a cache whose stamp still matches is served, not re-decoded")
    func matchingStampIsServedFromCache() async throws {
        let url = try makeAudioURL()
        let service = AnalysisAudioService()
        let episodeID = "bd8ysk-hit-\(UUID().uuidString)"
        let controlID = "bd8ysk-hit-control-\(UUID().uuidString)"
        defer {
            Task { await service.evictCache(episodeID: episodeID) }
            Task { await service.evictCache(episodeID: controlID) }
        }

        try writeSynthAudio(seconds: 45, to: url, amplitude: 0.25)
        let local = try #require(LocalAudioURL(url))

        // What a REAL decode of this file looks like, under a fresh id so no
        // cache can answer. Without this control the marker assertions could
        // be satisfied by a decoder that happens to produce flat shards.
        let control = try await service.decodeOutcome(fileURL: local, episodeID: controlID)
        try #require(control.shards.count == 2, "45 s at a 30 s shard duration is two shards")
        let controlIsFlat = Set(control.shards[0].samples).count == 1
        try #require(!controlIsFlat, "a real decode of a sine must not be flat")

        // Prime the cache with something a decode could not have produced.
        let markers = [
            AnalysisShard(
                id: 0, episodeID: episodeID, startTime: 0, duration: 30,
                samples: Array(repeating: Float(0.25), count: 4_000)
            ),
            AnalysisShard(
                id: 1, episodeID: episodeID, startTime: 30, duration: 30,
                samples: Array(repeating: Float(0.25), count: 4_000)
            ),
            AnalysisShard(
                id: 2, episodeID: episodeID, startTime: 60, duration: 30,
                samples: Array(repeating: Float(0.25), count: 4_000)
            )
        ]
        AnalysisAudioService.saveShardsForTesting(markers, episodeID: episodeID, sourceURL: url)

        func assertServed(_ outcome: AnalysisDecodeOutcome, _ label: String) {
            #expect(
                outcome.shards.count == 3,
                """
                \(label): a stamp that still matches must be SERVED. Got \
                \(outcome.shards.count) shards; the cache holds 3 and a \
                re-decode of this file yields 2. The cache is being discarded \
                when it is valid, so every retry pays a full re-decode
                """
            )
            #expect(
                outcome.shards.allSatisfy { Set($0.samples) == [Float(0.25)] },
                "\(label): the samples came from a decode, not from the cache"
            )
            #expect(!outcome.isTruncated)
        }

        assertServed(
            try await service.decodeOutcome(fileURL: local, episodeID: episodeID), "first read"
        )

        // And it does not THRASH: a valid entry survives being served over and
        // over. Repetition is asserted with the marker probe rather than by
        // comparing manifest bytes — an evict-and-re-decode rewrites
        // `manifest.json`, and after the first re-decode its content is stable,
        // so a byte comparison would pass against a cache that churns the
        // whole episode on every read.
        for pass in 0..<3 {
            assertServed(
                try await service.decodeOutcome(fileURL: local, episodeID: episodeID),
                "repeat pass \(pass)"
            )
        }
    }

    /// REVIEW R2. The end-to-end complement: the source is replaced with
    /// DIFFERENT AUDIO OF EXACTLY THE SAME LENGTH — the delete-and-
    /// re-download shape, where a DAI re-stitch swaps one ad pod for another
    /// of equal encoded size — and the cache must re-decode.
    ///
    /// Round 1 reasoned this collision was benign because "production
    /// mutation is monotone growth at one URL". It is not:
    /// `DownloadManager.evictIfNeeded()` and `clearCache()` both delete
    /// complete audio and leave the shard cache standing. This test drives
    /// the real `decodeOutcome` path, so it fails against a build that stamps
    /// only the byte length.
    @Test("a same-length replacement of the source is re-decoded, not served")
    func constantLengthReplacementIsRedecoded() async throws {
        let url = try makeAudioURL()
        let service = AnalysisAudioService()
        let episodeID = "bd8ysk-restitch-\(UUID().uuidString)"
        defer { Task { await service.evictCache(episodeID: episodeID) } }

        func peak(_ shards: [AnalysisShard]) -> Float {
            shards.flatMap(\.samples).reduce(Float(0)) { Swift.max($0, abs($1)) }
        }

        try writeSynthAudio(seconds: 45, to: url, amplitude: 0.25)
        let quietBytes = try #require(AnalysisAudioService.sourceByteLength(of: url))
        let local = try #require(LocalAudioURL(url))

        let first = try await service.decodeOutcome(fileURL: local, episodeID: episodeID)
        let quietPeak = peak(first.shards)
        try #require(quietPeak > 0.2 && quietPeak < 0.4, "fixture peak \(quietPeak), expected ~0.25")

        // Replace the CONTENT without changing the LENGTH.
        try writeSynthAudio(seconds: 45, to: url, amplitude: 0.9)
        let loudBytes = try #require(AnalysisAudioService.sourceByteLength(of: url))
        try #require(
            loudBytes == quietBytes,
            "the probe needs a constant-length replacement (\(quietBytes) -> \(loudBytes))"
        )

        let second = try await service.decodeOutcome(fileURL: local, episodeID: episodeID)
        #expect(
            peak(second.shards) > 0.7,
            """
            a different file of the same length was served from cache: got peak \
            \(peak(second.shards)), expected the replacement's ~0.9. A byte length \
            alone cannot separate two DAI stitches whose ad pods encode to the same \
            size, and the failure is silent — ad windows from the OLD stitch applied \
            to the new audio
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

    // MARK: - 5. The write-side guard (review r2)

    private func syntheticShards(episodeID: String) -> [AnalysisShard] {
        [
            AnalysisShard(
                id: 0, episodeID: episodeID, startTime: 0, duration: 30,
                samples: Array(repeating: Float(0.25), count: 480_000)
            ),
            AnalysisShard(
                id: 1, episodeID: episodeID, startTime: 30, duration: 15,
                samples: Array(repeating: Float(0.25), count: 240_000)
            )
        ]
    }

    /// ROUND 1's SURVIVING MUTATION, now killable.
    ///
    /// `saveShards` refuses to write anything when it cannot measure the
    /// source. Round 1 mutated that guard to `?? 0` — stamp zero and cache
    /// anyway — and the mutant survived the whole suite, because `ShardCache`
    /// is file-private and the decode→save window is not reachable from
    /// outside. `AnalysisAudioService.saveShardsForTesting` makes it reachable.
    ///
    /// The assertion is deliberately about the MANIFEST FILE, not about what
    /// a later read returns. The `?? 0` mutant writes a manifest stamped `0`,
    /// and a read against a real 700 KB source then correctly discards it —
    /// so asserting only on the read would pass against the mutant and prove
    /// nothing. What separates the two builds is whether a cache entry was
    /// created at all.
    @Test("a source that cannot be measured is not cached at all")
    func unmeasurableSourceIsNotCachedAtAll() async throws {
        let service = AnalysisAudioService()
        let episodeID = "bd8ysk-unmeasurable-\(UUID().uuidString)"
        defer { Task { await service.evictCache(episodeID: episodeID) } }

        // A URL in a real directory that has no file at it — exactly the
        // state the decode→save window enters when the audio is deleted
        // (LRU eviction, or the user removing the download) after the
        // decode drained and before the stamp is taken.
        let missing = try makeAudioURL()
        try #require(
            AnalysisAudioService.sourceByteLength(of: missing) == nil,
            "fixture must be unmeasurable or this test proves nothing"
        )

        AnalysisAudioService.saveShardsForTesting(
            syntheticShards(episodeID: episodeID),
            episodeID: episodeID,
            sourceURL: missing
        )

        #expect(
            !AnalysisAudioService.manifestExistsForTesting(episodeID: episodeID),
            """
            an unmeasurable source must produce NO cache entry. A manifest \
            here is an entry stamped with a length that never described any \
            file — and because `isShardCacheValid` fails OPEN on an \
            unmeasurable source, such an entry is served unconditionally \
            every time the audio is absent
            """
        )
    }

    /// And the guard is not vacuous: the same call with a measurable source
    /// DOES cache. Without this control, `unmeasurableSourceIsNotCachedAtAll`
    /// passes against a `saveShards` that writes nothing ever — the negative-
    /// without-a-positive shape that round 1 found across this whole suite.
    @Test("control: a measurable source is cached and served back")
    func measurableSourceIsCached() async throws {
        let service = AnalysisAudioService()
        let episodeID = "bd8ysk-measurable-\(UUID().uuidString)"
        defer { Task { await service.evictCache(episodeID: episodeID) } }

        let url = try makeAudioURL()
        try writeSynthAudio(seconds: 45, to: url)
        let shards = syntheticShards(episodeID: episodeID)

        AnalysisAudioService.saveShardsForTesting(
            shards, episodeID: episodeID, sourceURL: url
        )

        #expect(AnalysisAudioService.manifestExistsForTesting(episodeID: episodeID))
        let served = try #require(
            AnalysisAudioService.loadShardsForTesting(episodeID: episodeID, sourceURL: url),
            "a stamp taken from this very file must validate against it"
        )
        #expect(served.map(\.id) == [0, 1])
        #expect(served[0].samples.count == 480_000)
    }

    /// RUN THE SELF-HEALING CLAIM ROUND 1 ONLY REASONED ABOUT.
    ///
    /// Round 1 judged the `?? 0` mutant's impact bounded because an entry
    /// stamped `0` "self-heals the moment the source is measurable again".
    /// That disposition was reached by reading, and this project has had
    /// by-reading dispositions invert. Here it is executed, through a state
    /// production can actually reach: a source that measures ZERO BYTES — a
    /// file the downloader created and had not yet written to. `saveShards`
    /// accepts it (0 is measurable, and `guard let` admits it — only `nil` is
    /// refused), stamping an entry that describes an empty file.
    ///
    /// The claim under test is that such an entry cannot survive the source
    /// acquiring any content. It holds — and note WHY it is not a substitute
    /// for the guard above: healing needs the source to be MEASURABLE, and
    /// the `?? 0` mutant's entries are created precisely when it is not.
    @Test("a zero-byte stamp is discarded the moment the source has content")
    func zeroByteStampSelfHealsWhenTheSourceGrows() async throws {
        let service = AnalysisAudioService()
        let episodeID = "bd8ysk-zerostamp-\(UUID().uuidString)"
        defer { Task { await service.evictCache(episodeID: episodeID) } }

        let url = try makeAudioURL()
        try Data().write(to: url)
        try #require(
            AnalysisAudioService.sourceByteLength(of: url) == 0,
            "the fixture must measure zero, not nil"
        )

        AnalysisAudioService.saveShardsForTesting(
            syntheticShards(episodeID: episodeID), episodeID: episodeID, sourceURL: url
        )
        try #require(
            AnalysisAudioService.manifestExistsForTesting(episodeID: episodeID),
            "zero IS measurable, so the entry must be written — otherwise this test measures the wrong guard"
        )
        // Still self-consistent while the source stays empty.
        #expect(
            AnalysisAudioService.loadShardsForTesting(episodeID: episodeID, sourceURL: url) != nil
        )

        // The source acquires content. The stamp can no longer describe it.
        try writeSynthAudio(seconds: 45, to: url)
        #expect(
            AnalysisAudioService.loadShardsForTesting(episodeID: episodeID, sourceURL: url) == nil,
            """
            a stamp of 0 must not validate against a source with content. \
            This is the self-healing round 1 asserted by reading; if it ever \
            stops holding, an entry that describes no file at all becomes \
            immortal
            """
        )
        #expect(
            !AnalysisAudioService.manifestExistsForTesting(episodeID: episodeID),
            "and the healed entry is evicted, not merely bypassed"
        )
    }
}
