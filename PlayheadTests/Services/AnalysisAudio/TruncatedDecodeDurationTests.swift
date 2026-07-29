// TruncatedDecodeDurationTests.swift
// playhead-0hi9 — a duration may never be derived from an unverified decode.
//
// `AnalysisAudio.performDecode` has always computed `isTruncated` and used it
// to suppress the shard cache ("a cached partial result would be returned
// permanently"). It then threw the flag away, so
// `AnalysisCoordinator.runFromSpooling` persisted the shard sum of a partial
// decode as `analysis_assets.episodeDurationSec`.
//
// The observed shape on the owner's device: `DownloadManager`'s 8 MiB
// `defaultPlayableThreshold` resumes playback the instant it is crossed, the
// pipeline spools against that byte prefix, and 8_388_608 / 16_000 B/s
// ≈ 524 s — so episodes of 2933 s, 1746 s and 4379 s all recorded ~540 s.
// A fixed BYTE prefix across a fixed BITRATE cohort.
//
// Covered here:
//   1. the truncation predicate itself, at the exact numbers from the
//      incident;
//   2. that a complete decode of a real audio file reports `isTruncated ==
//      false` and still returns its shards;
//   3. that the coordinator's spool-time duration write honours the flag,
//      against a real `AnalysisStore`.

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("playhead-0hi9 — truncated decode never sets episodeDurationSec")
struct TruncatedDecodeDurationTests {

    // MARK: - Fixture

    private static let tempDirs = TestTempDirTracker()

    /// Mono 16 kHz float CAF of the requested length. CAF avoids container
    /// quirks and matches the `AnalysisAudioStreamingTests` pattern.
    private func writeSynthAudio(seconds: TimeInterval) throws -> URL {
        let dir = try makeTempDir(prefix: "Bd0hi9Audio")
        Self.tempDirs.track(dir)
        let fileURL = dir.appendingPathComponent("synth-\(UUID().uuidString).caf")

        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: 16_000,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "TruncatedDecodeDurationTests", code: -1)
        }
        let file = try AVAudioFile(
            forWriting: fileURL,
            settings: format.settings,
            commonFormat: .pcmFormatFloat32,
            interleaved: false
        )
        let totalFrames = AVAudioFramePosition(seconds * 16_000)
        var written = AVAudioFramePosition(0)
        while written < totalFrames {
            let frames = AVAudioFrameCount(min(AVAudioFramePosition(16_000), totalFrames - written))
            guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames) else {
                throw NSError(domain: "TruncatedDecodeDurationTests", code: -2)
            }
            buffer.frameLength = frames
            if let channel = buffer.floatChannelData?[0] {
                for i in 0..<Int(frames) {
                    channel[i] = Float(sin(2.0 * Double.pi * 440.0 * Double(written + AVAudioFramePosition(i)) / 16_000.0)) * 0.25
                }
            }
            try file.write(from: buffer)
            written += AVAudioFramePosition(frames)
        }
        return fileURL
    }

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "Bd0hi9Store")
        Self.tempDirs.track(dir)
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    private func makeCoordinator(store: AnalysisStore) -> AnalysisCoordinator {
        AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: SpeechService(
                    vocabularyProvider: ASRVocabularyProvider(store: store)
                ),
                store: store
            ),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: AdDetectionService(
                store: store,
                metadataExtractor: FallbackExtractor(),
                backfillJobRunnerFactory: nil,
                canUseFoundationModelsProvider: { false }
            ),
            skipOrchestrator: SkipOrchestrator(store: store)
        )
    }

    private func seedAsset(store: AnalysisStore, assetId: String) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: "ep-\(assetId)",
            assetFingerprint: assetId,
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(assetId).mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))
    }

    private func shards(totalSeconds: Double) -> [AnalysisShard] {
        var out: [AnalysisShard] = []
        var start = 0.0
        var index = 0
        while start < totalSeconds {
            let duration = min(30.0, totalSeconds - start)
            out.append(AnalysisShard(
                id: index,
                episodeID: "ep",
                startTime: start,
                duration: duration,
                samples: []
            ))
            start += duration
            index += 1
        }
        return out
    }

    // MARK: - 1. The predicate

    @Test("8 MiB prefix of a 2933 s episode reads as truncated")
    func eightMiBPrefixIsTruncated() {
        // 8 * 1024 * 1024 bytes at ~123 kbps ≈ 543 s decoded.
        #expect(AnalysisAudioService.isTruncatedDecode(
            decodedDuration: 543,
            assetDuration: 2933
        ))
        #expect(AnalysisAudioService.isTruncatedDecode(
            decodedDuration: 528,
            assetDuration: 1746
        ))
        #expect(AnalysisAudioService.isTruncatedDecode(
            decodedDuration: 561,
            assetDuration: 4379
        ))
    }

    @Test("a decode inside the 5% tolerance is not truncated")
    func completeDecodeIsNotTruncated() {
        // Exactly equal, and a decoder tail inside tolerance.
        #expect(!AnalysisAudioService.isTruncatedDecode(
            decodedDuration: 2933,
            assetDuration: 2933
        ))
        #expect(!AnalysisAudioService.isTruncatedDecode(
            decodedDuration: 2933 * 0.96,
            assetDuration: 2933
        ))
        // Just outside tolerance is truncated — pins the boundary so a
        // widened tolerance cannot slip through unnoticed.
        #expect(AnalysisAudioService.isTruncatedDecode(
            decodedDuration: 2933 * 0.94,
            assetDuration: 2933
        ))
    }

    @Test("unknown declared duration is never called truncated")
    func unknownAssetDurationIsNotTruncated() {
        #expect(!AnalysisAudioService.isTruncatedDecode(decodedDuration: 0, assetDuration: 0))
        #expect(!AnalysisAudioService.isTruncatedDecode(decodedDuration: 12, assetDuration: -1))
    }

    // MARK: - 2. The flag is surfaced, not discarded

    @Test("decodeOutcome on a complete file returns shards AND isTruncated == false")
    func completeFileSurfacesNotTruncated() async throws {
        let url = try writeSynthAudio(seconds: 45)
        let service = AnalysisAudioService()
        let local = try #require(LocalAudioURL(url))
        let outcome = try await service.decodeOutcome(
            fileURL: local,
            episodeID: "bd0hi9-complete-\(UUID().uuidString)"
        )
        #expect(!outcome.isTruncated)
        #expect(!outcome.shards.isEmpty)
        let decoded = outcome.shards.map(\.duration).reduce(0, +)
        #expect(abs(decoded - 45) < 1.0, "decoded \(decoded)s of a 45s file")
        // `decode` must stay a faithful projection of the outcome.
        let plain = try await service.decode(
            fileURL: local,
            episodeID: "bd0hi9-complete-plain-\(UUID().uuidString)"
        )
        #expect(plain.count == outcome.shards.count)
    }

    /// R2: the shard-cache hit is a SECOND, independent producer of
    /// `AnalysisDecodeOutcome`, and it asserts `isTruncated == false` by
    /// construction (step 9 of `performDecode` refuses to cache a truncated
    /// decode, so anything in the cache covered the whole asset). Nothing
    /// exercised that branch: every existing test decodes under a fresh
    /// episode id, so all of them take the compute path. If the cached branch
    /// ever reported `true`, `persistSpooledEpisodeDuration` would silently
    /// stop writing a duration for every episode with a warm cache — a
    /// coverage regression with no failing test anywhere.
    ///
    /// The cache hit is PROVEN, not assumed: the audio file is deleted between
    /// the two calls, so a recompute would throw `fileNotFound` at step 1.
    @Test("a shard-cache hit returns its shards AND reports isTruncated == false")
    func cachedDecodeReportsNotTruncated() async throws {
        let url = try writeSynthAudio(seconds: 6)
        let service = AnalysisAudioService()
        let local = try #require(LocalAudioURL(url))
        let episodeID = "bd0hi9-cache-hit-\(UUID().uuidString)"
        defer { Task { await service.evictCache(episodeID: episodeID) } }

        let first = try await service.decodeOutcome(fileURL: local, episodeID: episodeID)
        try #require(!first.shards.isEmpty)
        #expect(!first.isTruncated)

        // Remove the source. Only the cache can answer now.
        try FileManager.default.removeItem(at: url)
        let second = try await service.decodeOutcome(fileURL: local, episodeID: episodeID)
        #expect(!second.shards.isEmpty,
                "the second pass must have been served from the shard cache — the file is gone")
        #expect(second.shards.count == first.shards.count)
        #expect(!second.isTruncated,
                "a cached decode covered the whole asset when it was written; calling it truncated would stop every warm-cache episode from ever recording a duration")
    }

    // MARK: - 2b. The true branch, reached by a REAL decode

    /// R1: writes an AAC `.m4a` and then STRETCHES its sample-timing table, so
    /// the asset declares `seconds` while the decoder yields roughly
    /// `seconds / factor`.
    ///
    /// WHY THIS SHAPE. The production case is MP3-with-Xing: the container
    /// header declares the whole episode, the bytes on disk are a prefix, and
    /// `AVAssetReader` still reports `.completed` because for a headerless
    /// stream end-of-file IS end-of-stream. No Apple encoder can author an
    /// MP3, and the obvious substitutes were measured and do not work:
    /// truncating a CAF (or inflating its `data`-chunk size) makes the reader
    /// report `.failed`, which `performDecode` throws on at step 7 before the
    /// truncation check; a WAV with an inflated `data`-chunk size has its
    /// duration clamped back to the bytes present; and inflating an MP4's
    /// `mvhd`/`tkhd`/`mdhd` durations is ignored outright, because
    /// AVFoundation derives duration from the sample tables.
    ///
    /// Stretching `stts.sample_delta` is the one lever that reproduces the
    /// observable: every packet the reader needs is present, so it COMPLETES,
    /// but the decode yields far less audio than the asset declares.
    ///
    /// This is a synthetic container, and the test asserts the fixture really
    /// is that shape before asserting anything about the code — if a future OS
    /// stops honouring it, this fails loudly rather than passing vacuously.
    private func writeStretchedTimelineM4A(seconds: TimeInterval, factor: UInt32) throws -> URL {
        let dir = try makeTempDir(prefix: "Bd0hi9Stretch")
        Self.tempDirs.track(dir)
        let sourceURL = dir.appendingPathComponent("source-\(UUID().uuidString).m4a")
        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: 44_100,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "TruncatedDecodeDurationTests", code: -3)
        }
        // Scope the writer so the container is FINALIZED before the bytes are
        // read back — an AVAudioFile still in scope has not written its `moov`
        // yet, and the patcher below would find nothing to patch. Same reason
        // `CorpusAudioFixtures.aacRoundTrip` scopes its writer.
        do {
            let file = try AVAudioFile(
                forWriting: sourceURL,
                settings: [
                    AVFormatIDKey: kAudioFormatMPEG4AAC,
                    AVSampleRateKey: 44_100.0,
                    AVNumberOfChannelsKey: 1,
                    AVEncoderBitRateKey: 64_000,
                ],
                commonFormat: .pcmFormatFloat32,
                interleaved: false
            )
            let total = AVAudioFramePosition(seconds * 44_100)
            var written = AVAudioFramePosition(0)
            while written < total {
                let frames = AVAudioFrameCount(min(AVAudioFramePosition(44_100), total - written))
                guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames) else {
                    throw NSError(domain: "TruncatedDecodeDurationTests", code: -4)
                }
                buffer.frameLength = frames
                if let channel = buffer.floatChannelData?[0] {
                    for i in 0..<Int(frames) {
                        channel[i] = Float(sin(
                            2.0 * Double.pi * 440.0
                                * Double(written + AVAudioFramePosition(i)) / 44_100.0
                        )) * 0.2
                    }
                }
                try file.write(from: buffer)
                written += AVAudioFramePosition(frames)
            }
        }

        var bytes = [UInt8](try Data(contentsOf: sourceURL))
        func be32(_ offset: Int) -> UInt32 {
            (UInt32(bytes[offset]) << 24) | (UInt32(bytes[offset + 1]) << 16)
                | (UInt32(bytes[offset + 2]) << 8) | UInt32(bytes[offset + 3])
        }
        func put32(_ offset: Int, _ value: UInt32) {
            bytes[offset] = UInt8((value >> 24) & 0xFF)
            bytes[offset + 1] = UInt8((value >> 16) & 0xFF)
            bytes[offset + 2] = UInt8((value >> 8) & 0xFF)
            bytes[offset + 3] = UInt8(value & 0xFF)
        }
        // Locate `stts` by scanning for the 4CC rather than descending
        // moov/trak/mdia/minf/stbl. A hierarchical walk has to get `mdat`'s
        // 64-bit and extends-to-EOF size forms exactly right or it silently
        // stops before reaching `moov`, and the writer's box layout is not
        // ours to depend on. The scan is self-validating instead: a full
        // `stts` box is
        //   [size:4]["stts"][version+flags:4][entryCount:4]
        //   then entryCount x [sampleCount:4][sampleDelta:4]
        // so requiring `size == 16 + entryCount * 8` rejects a chance match.
        var stretchedEntries = 0
        var cursor = 4
        while cursor + 12 <= bytes.count {
            guard bytes[cursor] == UInt8(ascii: "s"),
                  bytes[cursor + 1] == UInt8(ascii: "t"),
                  bytes[cursor + 2] == UInt8(ascii: "t"),
                  bytes[cursor + 3] == UInt8(ascii: "s")
            else {
                cursor += 1
                continue
            }
            let boxSize = Int(be32(cursor - 4))
            let entryCount = Int(be32(cursor + 8))
            guard entryCount > 0,
                  boxSize == 16 + entryCount * 8,
                  cursor - 4 + boxSize <= bytes.count
            else {
                cursor += 1
                continue
            }
            for entry in 0..<entryCount {
                let deltaOffset = cursor + 12 + entry * 8 + 4
                put32(deltaOffset, be32(deltaOffset) * factor)
                stretchedEntries += 1
            }
            cursor += boxSize
        }
        guard stretchedEntries > 0 else {
            // The fixture is the whole point; a silently unpatched file would
            // make the test pass without exercising anything.
            throw NSError(
                domain: "TruncatedDecodeDurationTests",
                code: -5,
                userInfo: [NSLocalizedDescriptionKey:
                    "no stts box patched in \(bytes.count) bytes — re-derive the fixture"]
            )
        }

        let patchedURL = dir.appendingPathComponent("stretched-\(UUID().uuidString).m4a")
        try Data(bytes).write(to: patchedURL)
        return patchedURL
    }

    @Test("a REAL decode that falls short of its declared duration reports isTruncated and writes no duration")
    func realTruncatedDecodeIsSurfacedEndToEnd() async throws {
        let url = try writeStretchedTimelineM4A(seconds: 60, factor: 6)
        let asset = AVURLAsset(url: url)
        let declared = CMTimeGetSeconds(try await asset.load(.duration))

        let service = AnalysisAudioService()
        let local = try #require(LocalAudioURL(url))
        let episodeID = "bd0hi9-real-truncated-\(UUID().uuidString)"
        let outcome = try await service.decodeOutcome(fileURL: local, episodeID: episodeID)
        let decoded = outcome.shards.map(\.duration).reduce(0, +)

        // The fixture really is the shape under test: a reader that COMPLETED
        // (a `.failed` reader throws before step 8 and never gets here) over
        // materially less audio than the asset declares.
        try #require(declared > 0)
        try #require(!outcome.shards.isEmpty, "a truncated decode must still return its prefix")
        try #require(
            decoded < declared * (1.0 - AnalysisAudioService.truncationTolerance),
            "fixture no longer truncates: declared \(declared)s, decoded \(decoded)s — re-derive it rather than deleting the test"
        )

        #expect(outcome.isTruncated,
                "performDecode computes truncation for the shard cache; the same verdict must reach the caller")

        // The shard cache refused it — proven without touching the private
        // cache type: a cached decode reports `isTruncated == false` by
        // construction, so a second pass still reporting `true` is a miss.
        let second = try await service.decodeOutcome(fileURL: local, episodeID: episodeID)
        #expect(second.isTruncated,
                "a truncated decode must not have been cached; a cache hit reports not-truncated")

        // And the whole point: that outcome sets no duration on a real store.
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)
        let assetId = "asset-real-trunc-\(UUID().uuidString)"
        try await seedAsset(store: store, assetId: assetId)
        await coordinator.persistSpooledEpisodeDuration(
            assetId: assetId,
            episodeId: "ep-\(assetId)",
            outcome: outcome
        )
        let persisted = try await store.fetchAsset(id: assetId)
        #expect(persisted?.episodeDurationSec == nil,
                "real file -> real decode -> truncation flag -> no duration written (got \(String(describing: persisted?.episodeDurationSec)))")
    }

    // MARK: - 3. The coordinator gate

    @Test("truncated outcome leaves episodeDurationSec NULL")
    func truncatedOutcomeDoesNotWriteDuration() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)
        let assetId = "asset-trunc-\(UUID().uuidString)"
        try await seedAsset(store: store, assetId: assetId)

        await coordinator.persistSpooledEpisodeDuration(
            assetId: assetId,
            episodeId: "ep-\(assetId)",
            outcome: AnalysisDecodeOutcome(
                shards: shards(totalSeconds: 543),
                isTruncated: true
            )
        )

        let asset = try await store.fetchAsset(id: assetId)
        #expect(asset?.episodeDurationSec == nil,
                "a truncated decode must not become the episode's duration (got \(String(describing: asset?.episodeDurationSec)))")
    }

    @Test("complete outcome still writes the shard-sum duration (gtt9.1.1 preserved)")
    func completeOutcomeWritesDuration() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)
        let assetId = "asset-full-\(UUID().uuidString)"
        try await seedAsset(store: store, assetId: assetId)

        await coordinator.persistSpooledEpisodeDuration(
            assetId: assetId,
            episodeId: "ep-\(assetId)",
            outcome: AnalysisDecodeOutcome(
                shards: shards(totalSeconds: 2933),
                isTruncated: false
            )
        )

        let asset = try await store.fetchAsset(id: assetId)
        let persisted = try #require(asset?.episodeDurationSec)
        #expect(abs(persisted - 2933) < 0.001)
    }

    @Test("a truncated decode never overwrites a duration already on the row")
    func truncatedOutcomeDoesNotClobberExistingDuration() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)
        let assetId = "asset-keep-\(UUID().uuidString)"
        try await seedAsset(store: store, assetId: assetId)
        try await store.updateEpisodeDuration(id: assetId, episodeDurationSec: 2933)

        await coordinator.persistSpooledEpisodeDuration(
            assetId: assetId,
            episodeId: "ep-\(assetId)",
            outcome: AnalysisDecodeOutcome(
                shards: shards(totalSeconds: 543),
                isTruncated: true
            )
        )

        let asset = try await store.fetchAsset(id: assetId)
        let persisted = try #require(asset?.episodeDurationSec)
        #expect(abs(persisted - 2933) < 0.001,
                "the real duration must survive a later mid-download spool (got \(persisted))")
    }
}
