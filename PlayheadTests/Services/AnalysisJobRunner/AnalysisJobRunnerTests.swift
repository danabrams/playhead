// AnalysisJobRunnerTests.swift
// Tests for the bounded-range analysis engine.

import CryptoKit
import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private func makeTestRequest(
    desiredCoverageSec: Double = 120,
    outputPolicy: OutputPolicy = .writeWindowsAndCues,
    priority: TaskPriority = .medium
) -> AnalysisRangeRequest {
    let tmpDir = try! makeTempDir(prefix: "AnalysisJobRunnerTests")
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

private func makeTranscriptSegment(
    text: String = "hello",
    startTime: TimeInterval = 0,
    endTime: TimeInterval = 0.5,
    id: Int = 0,
    passType: TranscriptPassType = .fast
) -> TranscriptSegment {
    let word = TranscriptWord(text: text, startTime: startTime, endTime: endTime, confidence: 0.95)
    return TranscriptSegment(
        id: id,
        words: [word],
        text: text,
        startTime: startTime,
        endTime: endTime,
        avgConfidence: 0.95,
        passType: passType
    )
}

private func makeSegmentFingerprint(
    text: String,
    startTime: TimeInterval,
    endTime: TimeInterval
) -> String {
    let input = "\(text)|\(startTime)|\(endTime)"
    let digest = SHA256.hash(data: Data(input.utf8))
    return digest.prefix(16).map { String(format: "%02x", $0) }.joined()
}

private func makeTranscriptChunk(
    from segment: TranscriptSegment,
    analysisAssetId: String = "test-asset",
    chunkIndex: Int = 0
) -> TranscriptChunk {
    TranscriptChunk(
        id: UUID().uuidString,
        analysisAssetId: analysisAssetId,
        segmentFingerprint: makeSegmentFingerprint(
            text: segment.text,
            startTime: segment.startTime,
            endTime: segment.endTime
        ),
        chunkIndex: chunkIndex,
        startTime: segment.startTime,
        endTime: segment.endTime,
        text: segment.text,
        normalizedText: TranscriptEngineService.normalizeText(segment.text),
        pass: segment.passType.rawValue,
        modelVersion: "apple-speech-v1",
        transcriptVersion: nil,
        atomOrdinal: nil
    )
}

/// Seed the store with a minimal AnalysisAsset row so fetches succeed.
private func seedAsset(
    store: AnalysisStore,
    assetId: String = "test-asset",
    fastTranscriptCoverageEndTime: Double? = nil
) async throws {
    let asset = AnalysisAsset(
        id: assetId,
        episodeId: "test-ep",
        assetFingerprint: assetId,
        weakFingerprint: nil,
        sourceURL: "",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
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
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: 120)

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 4) // 0-120s

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
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
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: 90)

        let audioStub = StubAnalysisAudioProvider()
        // 10 shards covering 0-300s
        audioStub.shardsToReturn = makeShards(count: 10)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
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
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: 60)

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 2)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
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

    @Test("serious thermal does not pause the bounded analysis run")
    func testSeriousThermalDoesNotPauseRun() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: 30)

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 1)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
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
            cueMaterializer: materializer,
            thermalStateProvider: { .serious }
        )

        let outcome = await runner.run(makeTestRequest(desiredCoverageSec: 30))

        if case .pausedForThermal = outcome.stopReason {
            Issue.record("Serious thermal should no longer pause bounded analysis")
        }
    }

    @Test("critical thermal pauses the bounded analysis run")
    func testCriticalThermalPausesRun() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store)

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 1)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
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
            cueMaterializer: materializer,
            thermalStateProvider: { .critical }
        )

        let outcome = await runner.run(makeTestRequest(desiredCoverageSec: 30))

        if case .pausedForThermal = outcome.stopReason {
            // expected
        } else {
            Issue.record("Expected .pausedForThermal but got \(outcome.stopReason)")
        }
    }

    @Test("duplicate transcript pass skips hot path and backfill when windows are already resolved")
    func testDuplicateTranscriptPassSkipsResolvedDetectionWork() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: 30)

        let segment = makeTranscriptSegment()
        try await store.insertTranscriptChunks([makeTranscriptChunk(from: segment)])
        try await store.insertAdWindow(
            AdWindow(
                id: "resolved-window",
                analysisAssetId: "test-asset",
                startTime: 5,
                endTime: 20,
                confidence: 0.4,
                boundaryState: AdBoundaryState.lexical.rawValue,
                decisionState: AdDecisionState.suppressed.rawValue,
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
            )
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 1)

        let featureService = FeatureExtractionService(store: store)
        let recognizer = MockSpeechRecognizer()
        recognizer.transcribeResult = [segment]
        let speechService = SpeechService(recognizer: recognizer)
        try await speechService.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
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

        let outcome = await runner.run(makeTestRequest(desiredCoverageSec: 30, outputPolicy: .writeWindowsOnly))

        #expect(adStub.hotPathCallCount == 0)
        #expect(adStub.backfillCallCount == 0)
        if case .reachedTarget = outcome.stopReason {
            // expected
        } else {
            Issue.record("Expected .reachedTarget but got \(outcome.stopReason)")
        }
    }

    @Test("duplicate transcript pass still runs backfill when candidate windows remain")
    func testDuplicateTranscriptPassStillRunsBackfillForCandidates() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: 30)

        let segment = makeTranscriptSegment()
        try await store.insertTranscriptChunks([makeTranscriptChunk(from: segment)])
        try await store.insertAdWindow(
            AdWindow(
                id: "candidate-window",
                analysisAssetId: "test-asset",
                startTime: 5,
                endTime: 20,
                confidence: 0.8,
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
            )
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 1)

        let featureService = FeatureExtractionService(store: store)
        let recognizer = MockSpeechRecognizer()
        recognizer.transcribeResult = [segment]
        let speechService = SpeechService(recognizer: recognizer)
        try await speechService.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
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

        let outcome = await runner.run(makeTestRequest(desiredCoverageSec: 30, outputPolicy: .writeWindowsOnly))

        #expect(adStub.hotPathCallCount == 0)
        #expect(adStub.backfillCallCount == 1)
        if case .reachedTarget = outcome.stopReason {
            // expected
        } else {
            Issue.record("Expected .reachedTarget but got \(outcome.stopReason)")
        }
    }

    @Test("duplicate transcript pass still runs hot path when no windows exist")
    func testDuplicateTranscriptPassStillRunsHotPathWithoutWindows() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: 30)

        let segment = makeTranscriptSegment()
        try await store.insertTranscriptChunks([makeTranscriptChunk(from: segment)])

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 1)

        let featureService = FeatureExtractionService(store: store)
        let recognizer = MockSpeechRecognizer()
        recognizer.transcribeResult = [segment]
        let speechService = SpeechService(recognizer: recognizer)
        try await speechService.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
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

        let outcome = await runner.run(makeTestRequest(desiredCoverageSec: 30, outputPolicy: .writeWindowsOnly))

        #expect(adStub.hotPathCallCount == 1)
        #expect(adStub.backfillCallCount == 1)
        if case .reachedTarget = outcome.stopReason {
            // expected
        } else {
            Issue.record("Expected .reachedTarget but got \(outcome.stopReason)")
        }
    }

    // MARK: - playhead-5uvz.6 (Gap-7) episodeDurationSec persistence

    /// Pipeline B (scheduler-driven) must persist the shard-sum
    /// `episodeDurationSec` after stage 1 so the coverage guard at
    /// `AnalysisCoordinator.runFromBackfill` has a denominator. Without
    /// this, an episode driven exclusively through Pipeline B leaves
    /// the column NULL and the gtt9.1.1 fail-safe shortcut to
    /// `.restart` triggers on every Pipeline-B-only episode.
    @Test("Stage 1 persists episodeDurationSec when NULL")
    func testStage1PersistsEpisodeDurationWhenNull() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: 120)

        // Confirm seed actually leaves episodeDurationSec NULL — this
        // is the precondition for the bug we're closing.
        let seeded = try await store.fetchAsset(id: "test-asset")
        #expect(seeded?.episodeDurationSec == nil)

        let audioStub = StubAnalysisAudioProvider()
        // 4 shards × 30s = 120s of decoded audio.
        audioStub.shardsToReturn = makeShards(count: 4)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
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

        _ = await runner.run(makeTestRequest(desiredCoverageSec: 120))

        let after = try await store.fetchAsset(id: "test-asset")
        #expect(after?.episodeDurationSec == 120.0)
    }

    /// When the desired coverage is shorter than the full episode, the
    /// persisted `episodeDurationSec` must reflect the full episode
    /// (sum of `allShards`), not the bounded slice. Otherwise the
    /// coverage guard would compute a too-small denominator and
    /// over-report coverage on resume.
    @Test("Stage 1 persists full episode duration even for bounded coverage requests")
    func testStage1PersistsFullDurationForBoundedCoverage() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: 90)

        let audioStub = StubAnalysisAudioProvider()
        // 10 shards × 30s = 300s episode; request only the first 90s.
        audioStub.shardsToReturn = makeShards(count: 10)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
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

        _ = await runner.run(makeTestRequest(desiredCoverageSec: 90))

        let after = try await store.fetchAsset(id: "test-asset")
        #expect(after?.episodeDurationSec == 300.0)
    }

    /// If `episodeDurationSec` is already populated (e.g. Pipeline A
    /// ran first via `runFromSpooling`), Pipeline B must not overwrite
    /// it. This keeps the persistence write idempotent and avoids
    /// clobbering an authoritative value with a re-decoded sum that
    /// could differ by floating-point noise.
    @Test("Stage 1 does not overwrite existing episodeDurationSec")
    func testStage1DoesNotOverwriteExistingDuration() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: 60)

        // Pre-populate as if Pipeline A had already written it.
        try await store.updateEpisodeDuration(id: "test-asset", episodeDurationSec: 999.0)

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 2) // 60s

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
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

        _ = await runner.run(makeTestRequest(desiredCoverageSec: 60))

        let after = try await store.fetchAsset(id: "test-asset")
        #expect(after?.episodeDurationSec == 999.0)
    }
}
