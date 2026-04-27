// EpisodeDurationBackfillSweepTests.swift
// playhead-gyvb.2: launch-time one-shot sweep that re-probes the actual
// duration of cached audio files when the persisted
// `analysis_assets.episodeDurationSec` is missing or contradicted by a
// later coverage watermark (the 2026-04-27 libsyn/flightcast incident:
// declared 704s, actual 9700s).
//
// These tests pin the contract the sweep must honor:
//   - rewrite when `episodeDurationSec` is nil or non-positive
//   - rewrite when `featureCoverageEndTime > episodeDurationSec`
//     (or the same for fast-transcript coverage)
//   - leave alone when the persisted duration is consistent
//   - skip rows whose audio file is no longer cached
//   - leave the row alone when the probe itself returns nil
//   - mark the run via `_meta.did_duration_backfill_v1='1'` so a
//     second invocation short-circuits

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisCoordinator – runEpisodeDurationBackfillIfNeeded (gyvb.2)", .serialized)
struct EpisodeDurationBackfillSweepTests {

    // MARK: - Construction helpers

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "EpisodeDurationBackfillSweepTests")
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    private func makeCoordinator(store: AnalysisStore) -> AnalysisCoordinator {
        let speechService = SpeechService(
            vocabularyProvider: ASRVocabularyProvider(store: store)
        )
        return AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: speechService,
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

    private func makeAsset(
        id: String,
        episodeId: String,
        episodeDurationSec: Double?,
        featureCoverageEndTime: Double? = nil,
        fastTranscriptCoverageEndTime: Double? = nil
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(id).m4a",
            featureCoverageEndTime: featureCoverageEndTime,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec
        )
    }

    /// Mirrors the AudioFileDurationProbeTests synth audio writer —
    /// produces a CAF whose `AVURLAsset.duration` reports approximately
    /// the requested seconds.
    private func writeSynthAudio(
        seconds: TimeInterval,
        sampleRate: Double = 44_100
    ) throws -> URL {
        let tempDir = URL(fileURLWithPath: NSTemporaryDirectory())
        let fileURL = tempDir.appendingPathComponent("gyvb2-sweep-\(UUID().uuidString).caf")

        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: sampleRate,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "EpisodeDurationBackfillSweepTests", code: -1)
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
                throw NSError(domain: "EpisodeDurationBackfillSweepTests", code: -2)
            }
            buffer.frameLength = frames
            try file.write(from: buffer)
            written += AVAudioFramePosition(frames)
        }

        return fileURL
    }

    // MARK: - Tests

    @Test("rewrites episodeDurationSec when watermarks exceed declared duration (libsyn/flightcast incident)")
    func watermarkExceedsDurationTriggersRewrite() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        // Seed: declared 704s but a feature watermark of 9000s already
        // landed before the buggy duration was caught. The actual file
        // is ~12.5s of synth CAF (any positive number that differs from
        // the declared 704 demonstrates the rewrite).
        let url = try writeSynthAudio(seconds: 12.5)
        defer { try? FileManager.default.removeItem(at: url) }

        let assetId = "asset-libsyn-1"
        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeId: "ep-libsyn-1",
            episodeDurationSec: 704,
            featureCoverageEndTime: 9000,
            fastTranscriptCoverageEndTime: nil
        ))

        let summary = await coordinator.runEpisodeDurationBackfillIfNeeded { episodeId in
            episodeId == "ep-libsyn-1" ? url : nil
        }

        #expect(summary.alreadyDone == false)
        #expect(summary.rewritten == 1, "asset whose watermark exceeds dur must be rewritten")
        #expect(summary.probeFailed == 0)

        let after = try await store.fetchAsset(id: assetId)
        try #require(after?.episodeDurationSec != nil)
        let newDur = after!.episodeDurationSec!
        #expect(newDur != 704, "duration must change away from the bad declared value")
        #expect(abs(newDur - 12.5) < 0.5, "should match probed file duration")
    }

    @Test("rewrites episodeDurationSec when nil")
    func nilDurationTriggersRewrite() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let url = try writeSynthAudio(seconds: 8.0)
        defer { try? FileManager.default.removeItem(at: url) }

        let assetId = "asset-nil-dur"
        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeId: "ep-nil-dur",
            episodeDurationSec: nil
        ))

        let summary = await coordinator.runEpisodeDurationBackfillIfNeeded { episodeId in
            episodeId == "ep-nil-dur" ? url : nil
        }

        #expect(summary.rewritten == 1)

        let after = try await store.fetchAsset(id: assetId)
        try #require(after?.episodeDurationSec != nil)
        #expect(abs(after!.episodeDurationSec! - 8.0) < 0.5)
    }

    @Test("leaves rows alone when persisted duration is consistent with watermarks")
    func consistentRowsAreSkipped() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let assetId = "asset-consistent"
        // 1800s declared, watermarks are well within bounds — no probe.
        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeId: "ep-consistent",
            episodeDurationSec: 1800,
            featureCoverageEndTime: 90,
            fastTranscriptCoverageEndTime: 90
        ))

        let summary = await coordinator.runEpisodeDurationBackfillIfNeeded { _ in
            // Should never be called for this asset.
            Issue.record("cachedFileURL invoked for a consistent row")
            return nil
        }

        #expect(summary.rewritten == 0)
        #expect(summary.inspected == 0)

        let after = try await store.fetchAsset(id: assetId)
        #expect(after?.episodeDurationSec == 1800, "consistent row must be untouched")
    }

    @Test("skips rows whose audio file is no longer cached")
    func skipsWhenFileMissing() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        // Bad row, but `cachedFileURL` returns nil.
        let assetId = "asset-no-file"
        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeId: "ep-no-file",
            episodeDurationSec: 100,
            featureCoverageEndTime: 9000
        ))

        let summary = await coordinator.runEpisodeDurationBackfillIfNeeded { _ in nil }

        #expect(summary.rewritten == 0)
        #expect(summary.inspected == 0, "rows without a cached file must not count as inspected")

        let after = try await store.fetchAsset(id: assetId)
        #expect(after?.episodeDurationSec == 100, "no file -> no rewrite")
    }

    @Test("counts probe failures separately and leaves the row untouched")
    func probeFailureLeavesRowUntouched() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        // Bad row + a non-audio file. Probe must return nil.
        let dir = try makeTempDir(prefix: "EpisodeDurationBackfillSweepTests-Probefail")
        let nonAudio = dir.appendingPathComponent("not-audio.bin")
        try Data("garbage".utf8).write(to: nonAudio)

        let assetId = "asset-probe-fails"
        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeId: "ep-probe-fails",
            episodeDurationSec: 100,
            featureCoverageEndTime: 9000
        ))

        let summary = await coordinator.runEpisodeDurationBackfillIfNeeded { _ in nonAudio }

        #expect(summary.rewritten == 0)
        #expect(summary.inspected == 1)
        #expect(summary.probeFailed == 1)

        let after = try await store.fetchAsset(id: assetId)
        #expect(after?.episodeDurationSec == 100, "probe failure must not blank the column")
    }

    @Test("idempotent — second invocation reports alreadyDone and does not re-probe")
    func idempotenceShortCircuit() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let url = try writeSynthAudio(seconds: 10.0)
        defer { try? FileManager.default.removeItem(at: url) }

        let assetId = "asset-idempotent"
        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeId: "ep-idempotent",
            episodeDurationSec: 100,
            featureCoverageEndTime: 9000
        ))

        // First sweep does the work.
        let first = await coordinator.runEpisodeDurationBackfillIfNeeded { episodeId in
            episodeId == "ep-idempotent" ? url : nil
        }
        #expect(first.rewritten == 1)
        #expect(first.alreadyDone == false)

        // Second sweep must short-circuit. The closure is wired to fail the
        // test if it's invoked at all.
        let second = await coordinator.runEpisodeDurationBackfillIfNeeded { _ in
            Issue.record("cachedFileURL must not be called once the marker is set")
            return nil
        }
        #expect(second.alreadyDone == true)
        #expect(second.rewritten == 0)
        #expect(second.inspected == 0)
    }
}
