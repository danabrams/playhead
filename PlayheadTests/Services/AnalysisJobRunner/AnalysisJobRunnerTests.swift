// AnalysisJobRunnerTests.swift
// Tests for the bounded-range analysis engine.

import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private func makeTestRequest(
    desiredCoverageSec: Double = 120,
    outputPolicy: OutputPolicy = .writeWindowsAndCues,
    priority: TaskPriority = .medium
) -> AnalysisRangeRequest {
    let tmpDir = FileManager.default.temporaryDirectory
        .appendingPathComponent("AnalysisJobRunnerTests-\(UUID().uuidString)", isDirectory: true)
    try? FileManager.default.createDirectory(at: tmpDir, withIntermediateDirectories: true)
    let audioFile = tmpDir.appendingPathComponent("episode.m4a")
    FileManager.default.createFile(atPath: audioFile.path, contents: Data())
    let localURL = LocalAudioURL(audioFile)!

    return AnalysisRangeRequest(
        jobId: UUID().uuidString,
        episodeId: "test-ep",
        podcastId: "test-pod",
        analysisAssetId: "test-asset",
        audioURL: localURL,
        desiredCoverageSec: desiredCoverageSec,
        mode: .preRollWarmup,
        outputPolicy: outputPolicy,
        priority: priority
    )
}

private func makeShards(count: Int, shardDuration: Double = 30) -> [AnalysisShard] {
    (0..<count).map { i in
        makeShard(
            id: i,
            episodeID: "test-ep",
            startTime: Double(i) * shardDuration,
            duration: shardDuration
        )
    }
}

/// Seed the store with a minimal AnalysisAsset row so fetches succeed.
private func seedAsset(store: AnalysisStore, assetId: String = "test-asset") async throws {
    let asset = AnalysisAsset(
        id: assetId,
        episodeId: "test-ep",
        assetFingerprint: assetId,
        weakFingerprint: nil,
        sourceURL: "",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: SessionState.queued.rawValue,
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
    try await store.insertAsset(asset)
}

// MARK: - Tests

@Suite("AnalysisJobRunner")
struct AnalysisJobRunnerTests {

    @Test("Happy path runs all stages and returns reachedTarget")
    func testHappyPath() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store)

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 4) // 0-120s

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let materializer = SkipCueMaterializer(store: store)

        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            cueMaterializer: materializer
        )

        let request = makeTestRequest(desiredCoverageSec: 120)
        let outcome = await runner.run(request)

        #expect(outcome.assetId == "test-asset")
        #expect(outcome.requestedCoverageSec == 120)
        if case .reachedTarget = outcome.stopReason {
            // expected
        } else {
            Issue.record("Expected .reachedTarget but got \(outcome.stopReason)")
        }
    }

    @Test("Shard filtering by desired coverage depth")
    func testShardFilteringByDepth() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store)

        let audioStub = StubAnalysisAudioProvider()
        // 10 shards covering 0-300s
        audioStub.shardsToReturn = makeShards(count: 10)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let materializer = SkipCueMaterializer(store: store)

        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            cueMaterializer: materializer
        )

        // Only want first 90s — shards 0 (0s), 1 (30s), 2 (60s) have startTime < 90
        let request = makeTestRequest(desiredCoverageSec: 90)
        let outcome = await runner.run(request)

        // Feature coverage should be <= 90s (3 shards * 30s = 90s max end time)
        #expect(outcome.featureCoverageSec <= 90)
        if case .reachedTarget = outcome.stopReason {
            // expected
        } else {
            Issue.record("Expected .reachedTarget but got \(outcome.stopReason)")
        }
    }

    @Test("writeWindowsOnly policy skips cue materialization")
    func testWriteWindowsOnlySkipsCueMaterialization() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store)

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 2)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )

        // Return some ad windows from hot path so materialization would have work.
        let adStub = StubAdDetectionProvider()
        adStub.hotPathResult = [
            AdWindow(
                id: "win-1",
                analysisAssetId: "test-asset",
                startTime: 10,
                endTime: 40,
                confidence: 0.85,
                boundaryState: AdBoundaryState.lexical.rawValue,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: "test-v1",
                advertiser: nil,
                product: nil,
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: nil,
                metadataSource: "none",
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false
            ),
        ]
        let materializer = SkipCueMaterializer(store: store)

        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            cueMaterializer: materializer
        )

        let request = makeTestRequest(outputPolicy: .writeWindowsOnly)
        let outcome = await runner.run(request)

        // No cues should have been created.
        #expect(outcome.newCueCount == 0)

        // Verify no SkipCue rows in the store.
        let cues = try await store.fetchSkipCues(for: "test-asset")
        #expect(cues.isEmpty)
    }

    @Test("Blocked by model returns failed outcome")
    func testBlockedByModel() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store)

        let audioStub = StubAnalysisAudioProvider()
        audioStub.errorToThrow = AnalysisAudioError.decodingFailed("model not available")

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let materializer = SkipCueMaterializer(store: store)

        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            cueMaterializer: materializer
        )

        let request = makeTestRequest()
        let outcome = await runner.run(request)

        if case .failed(let msg) = outcome.stopReason {
            #expect(msg.contains("decode"))
        } else {
            Issue.record("Expected .failed but got \(outcome.stopReason)")
        }

        // No coverage should have been recorded.
        #expect(outcome.featureCoverageSec == 0)
        #expect(outcome.transcriptCoverageSec == 0)
        #expect(outcome.cueCoverageSec == 0)
    }
}
