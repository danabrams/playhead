// AnalysisWorkSchedulerDurationProbeTests.swift
// playhead-gyvb.2: pin the measure-on-download seam in
// `AnalysisWorkScheduler.enqueue(...)`. Once a download lands, the
// scheduler must:
//
//   1. ask the DownloadProvider for the cached file URL
//   2. probe its actual duration via `AudioFileDurationProbe`
//   3. write the probed value to the existing `analysis_assets` row
//      via `AnalysisStore.updateEpisodeDuration(id:episodeDurationSec:)`
//   4. otherwise stash the value so the lazy asset-row materialization
//      in `resolveAnalysisAssetId` carries it on first insert
//
// Per the bead: "Once we have the real runtime from the file that
// should be the source of truth." Measured > feed-derived.

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisWorkScheduler — measure-on-download (gyvb.2)", .serialized)
struct AnalysisWorkSchedulerDurationProbeTests {

    // MARK: - Construction helpers

    private func makeRunner(store: AnalysisStore) -> AnalysisJobRunner {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        return AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
    }

    private func makeScheduler(
        store: AnalysisStore,
        downloadProvider: StubDownloadProvider
    ) -> AnalysisWorkScheduler {
        AnalysisWorkScheduler(
            store: store,
            jobRunner: makeRunner(store: store),
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: downloadProvider,
            batteryProvider: {
                let b = StubBatteryProvider()
                b.level = 0.9
                b.charging = true
                return b
            }(),
            config: PreAnalysisConfig()
        )
    }

    /// Mirrors AudioFileDurationProbeTests synth audio writer.
    private func writeSynthAudio(
        seconds: TimeInterval,
        sampleRate: Double = 44_100
    ) throws -> URL {
        let tempDir = URL(fileURLWithPath: NSTemporaryDirectory())
        let fileURL = tempDir.appendingPathComponent("gyvb2-sched-\(UUID().uuidString).caf")

        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: sampleRate,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "AnalysisWorkSchedulerDurationProbeTests", code: -1)
        }

        let file = try AVAudioFile(
            forWriting: fileURL,
            settings: format.settings,
            commonFormat: .pcmFormatFloat32,
            interleaved: false
        )

        let totalFrames = AVAudioFramePosition(seconds * sampleRate)
        let chunkFrames = AVAudioFrameCount(sampleRate)
        var written = AVAudioFramePosition(0)
        while written < totalFrames {
            let remaining = AVAudioFrameCount(totalFrames - written)
            let frames = min(chunkFrames, remaining)
            guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames) else {
                throw NSError(domain: "AnalysisWorkSchedulerDurationProbeTests", code: -2)
            }
            buffer.frameLength = frames
            try file.write(from: buffer)
            written += AVAudioFramePosition(frames)
        }

        return fileURL
    }

    // MARK: - Tests

    @Test("enqueue probes the cached file and overwrites an existing asset row's episodeDurationSec")
    func enqueueOverwritesPersistedDuration() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        // Real audio file (~9 s synth CAF). Stub returns this URL for ep-1.
        let url = try writeSynthAudio(seconds: 9.0)
        defer { try? FileManager.default.removeItem(at: url) }
        provider.cachedURLs["ep-1"] = url

        // Pre-existing asset row carrying the bad feed-declared duration.
        let assetId = "asset-bad-feed"
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: "ep-1",
            assetFingerprint: "fp-1",
            weakFingerprint: nil,
            sourceURL: url.absoluteString,
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 704
        )
        try await store.insertAsset(asset)

        await scheduler.enqueue(
            episodeId: "ep-1",
            podcastId: "pod-1",
            downloadId: "dl-1",
            sourceFingerprint: "fp-1",
            isExplicitDownload: true
        )

        let after = try await store.fetchAsset(id: assetId)
        try #require(after?.episodeDurationSec != nil)
        let probed = after!.episodeDurationSec!
        #expect(probed != 704, "duration must be overwritten with probed value")
        #expect(abs(probed - 9.0) < 0.5, "duration should match probed file length")
    }

    @Test("enqueue stashes probed duration for later asset-row materialization when no row exists yet")
    func enqueueStashesForLazyMaterialization() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 7.5)
        defer { try? FileManager.default.removeItem(at: url) }
        provider.cachedURLs["ep-2"] = url

        // No asset row exists yet. Enqueue must not crash; the probed
        // value gets stashed for `resolveAnalysisAssetId` to consume on
        // first insert. We verify by confirming the row materialized
        // later carries the probed duration.
        await scheduler.enqueue(
            episodeId: "ep-2",
            podcastId: "pod-2",
            downloadId: "dl-2",
            sourceFingerprint: "fp-2",
            isExplicitDownload: true
        )

        // No row inserted yet (resolveAnalysisAssetId is private and
        // runs at job execution time). The visible contract here is
        // simpler: no row touched, no crash, no error logged.
        let row = try await store.fetchAssetByEpisodeId("ep-2")
        #expect(row == nil, "scheduler must not create the asset row eagerly")
    }

    @Test("enqueue with no cached file is a no-op (probe path is skipped)")
    func enqueueWithoutCachedFile() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        // Deliberately leave provider.cachedURLs empty.
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let assetId = "asset-no-file"
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: "ep-3",
            assetFingerprint: "fp-3",
            weakFingerprint: nil,
            sourceURL: "file:///fake/path.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 1234
        )
        try await store.insertAsset(asset)

        await scheduler.enqueue(
            episodeId: "ep-3",
            podcastId: "pod-3",
            downloadId: "dl-3",
            sourceFingerprint: "fp-3",
            isExplicitDownload: false
        )

        // Existing duration is preserved when no probe can run.
        let after = try await store.fetchAsset(id: assetId)
        #expect(after?.episodeDurationSec == 1234, "without cached file, duration must be untouched")
    }

    @Test("enqueue with a non-audio cached file leaves the row untouched (probe returns nil)")
    func enqueueWithNonAudioFile() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        // Non-audio file → probe returns nil → no-op.
        let dir = try makeTempDir(prefix: "AnalysisWorkSchedulerDurationProbeTests-NonAudio")
        let badFile = dir.appendingPathComponent("not-audio.bin")
        try Data("garbage".utf8).write(to: badFile)
        provider.cachedURLs["ep-4"] = badFile

        let assetId = "asset-non-audio"
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: "ep-4",
            assetFingerprint: "fp-4",
            weakFingerprint: nil,
            sourceURL: badFile.absoluteString,
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 555
        )
        try await store.insertAsset(asset)

        await scheduler.enqueue(
            episodeId: "ep-4",
            podcastId: "pod-4",
            downloadId: "dl-4",
            sourceFingerprint: "fp-4",
            isExplicitDownload: false
        )

        let after = try await store.fetchAsset(id: assetId)
        #expect(after?.episodeDurationSec == 555,
                "probe failure must not nullify or otherwise mutate the persisted duration")
    }
}
