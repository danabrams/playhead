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
