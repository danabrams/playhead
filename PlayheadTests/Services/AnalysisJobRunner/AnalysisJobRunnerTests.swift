// AnalysisJobRunnerTests.swift
// Tests for the bounded-range analysis engine.

import CryptoKit
import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private func makeTestRequest(
    desiredCoverageSec: Double = 120,
    podcastId: String = "test-pod",
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
        podcastId: podcastId,
        analysisAssetId: "test-asset",
        audioURL: localURL,
        desiredCoverageSec: desiredCoverageSec,
        mode: .preRollWarmup,
        outputPolicy: outputPolicy,
        priority: priority
    )
}

/// playhead-ngev (review r1): throws `CancellationError` out of the recognizer,
/// which is how a scrub reaches the transcription loop in production — one
/// `TranscriptEngineService` is shared by `AnalysisCoordinator` and
/// `AnalysisJobRunner`, so `startTranscription` from the playback lane cancels
/// whatever the other owner was running.
///
/// `SpeechService` rethrows recognizer errors unchanged, so the loop's
/// `catch is CancellationError` arm fires and reports an INTERRUPTED run —
/// the state the runner must not respond to by tearing the engine down.
private final class CancellingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private var loaded = false

    func loadModel() async throws { loaded = true }
    func unloadModel() async { loaded = false }
    func isModelLoaded() async -> Bool { loaded }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard loaded else { throw TranscriptEngineError.modelNotLoaded }
        throw CancellationError()
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        [VADResult(isSpeech: true, speechProbability: 1.0,
                   startTime: shard.startTime,
                   endTime: shard.startTime + shard.duration)]
    }
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
    fastTranscriptCoverageEndTime: Double? = nil,
    assetFingerprint: String? = nil,
    episodeDurationSec: Double? = nil
) async throws {
    let asset = AnalysisAsset(
        id: assetId,
        episodeId: "test-ep",
        assetFingerprint: assetFingerprint ?? assetId,
        weakFingerprint: nil,
        sourceURL: "",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
        confirmedAdCoverageEndTime: nil,
        analysisState: SessionState.queued.rawValue,
        analysisVersion: 1,
        capabilitySnapshot: nil,
        episodeDurationSec: episodeDurationSec
    )
    try await store.insertAsset(asset)
}

private final class StubCrossUserAnalysisSharingProvider: CrossUserAnalysisSharingProviding, @unchecked Sendable {
    let isEnabled = true
    var snapshot: CrossUserAnalysisSnapshot?
    private(set) var requestedKeys: [CrossUserAnalysisShareKey] = []
    private(set) var importedWindows: [AdWindow] = []
    private(set) var publishedSnapshots: [CrossUserAnalysisSnapshot] = []

    func matchingSnapshot(for key: CrossUserAnalysisShareKey) async -> CrossUserAnalysisSnapshot? {
        requestedKeys.append(key)
        guard snapshot?.key == key else { return nil }
        return snapshot
    }

    func publish(_ snapshot: CrossUserAnalysisSnapshot) async throws {
        publishedSnapshots.append(snapshot)
    }

    func didImportSharedAdWindows(_ windows: [AdWindow]) async {
        importedWindows.append(contentsOf: windows)
    }
}

private func makeSharedAnalysisSnapshot(
    key: CrossUserAnalysisShareKey,
    analysisCoverageEndSec: Double = 60,
    sourceAnalysisVersion: Int = 1
) -> CrossUserAnalysisSnapshot {
    CrossUserAnalysisSnapshot(
        key: key,
        provenance: CrossUserAnalysisProvenance(
            exportedAt: 1_800_000_000,
            sourceAnalysisVersion: sourceAnalysisVersion,
            sourceAppBuild: "runner-test"
        ),
        analysisCoverageEndSec: analysisCoverageEndSec,
        measurements: CrossUserAnalysisMeasurements(
            fmMinutesSaved: 2,
            queueToReadyLatencySec: 1.25,
            batteryDeltaPercent: nil
        ),
        windows: [
            CrossUserAnalysisSnapshot.Window(
                sourceWindowId: "peer-window",
                startTime: 10,
                endTime: 60,
                confidence: 0.9,
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
                decisionState: AdDecisionState.confirmed.rawValue,
                detectorVersion: "fm-test-v1",
                advertiser: "Acme",
                product: "Widget",
                adDescription: "Imported promo",
                metadataSource: "foundation-model",
                metadataConfidence: 0.82,
                metadataPromptVersion: "prompt-v1",
                evidenceSources: "semantic,fusion",
                eligibilityGate: SkipEligibilityGate.eligible.rawValue,
                catalogStoreMatchSimilarity: nil
            ),
        ]
    )
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
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub
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
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub
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
        try await speechService.loadFastModel()
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
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub
        )

        let request = makeTestRequest(outputPolicy: .writeWindowsOnly)
        let outcome = await runner.run(request)

        // No cues should have been counted (Bug 5: skip_cues table
        // and SkipCueMaterializer were deleted, so newCueCount is the
        // sole on-the-record signal).
        #expect(outcome.newCueCount == 0)
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
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub
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
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
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
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
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
                confidence: 0.9,
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
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub
        )

        let outcome = await runner.run(makeTestRequest(desiredCoverageSec: 30, outputPolicy: .writeWindowsOnly))

        #expect(adStub.hotPathCallCount == 0)
        #expect(adStub.backfillCallCount == 0)
        #expect(outcome.cueCoverageSec == 0)
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
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub
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
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub
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

    @Test("matching shared analysis import persists windows and skips ad detection")
    func testSharedAnalysisImportHitSkipsDetection() async throws {
        let store = try await makeTestStore()
        try await seedAsset(
            store: store,
            fastTranscriptCoverageEndTime: nil,
            assetFingerprint: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            episodeDurationSec: 120
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "test-pod",
            fileSHA: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            analysisVersion: 1
        )
        let sharingProvider = StubCrossUserAnalysisSharingProvider()
        sharingProvider.snapshot = makeSharedAnalysisSnapshot(key: key)

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 4)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            analysisSharingProvider: sharingProvider
        )

        let outcome = await runner.run(makeTestRequest(desiredCoverageSec: 60))

        #expect(sharingProvider.requestedKeys == [key])
        #expect(audioStub.decodeCallCount == 0)
        #expect(adStub.hotPathCallCount == 0)
        #expect(adStub.backfillCallCount == 0)
        #expect(sharingProvider.importedWindows.count == 1)
        #expect(sharingProvider.importedWindows.first?.analysisAssetId == "test-asset")
        #expect(sharingProvider.importedWindows.first?.adDescription == "Imported promo")
        if case .reachedTarget = outcome.stopReason {
            // expected
        } else {
            Issue.record("Expected .reachedTarget but got \(outcome.stopReason)")
        }
        #expect(outcome.cueCoverageSec == 60)
        #expect(outcome.newCueCount == 1)

        let windows = try await store.fetchAdWindows(assetId: "test-asset")
        #expect(windows.count == 1)
        #expect(windows.first?.analysisAssetId == "test-asset")
        #expect(windows.first?.adDescription == "Imported promo")
        #expect(windows.first?.evidenceText == nil)
    }

    @Test("sharing provider is not queried when the local fingerprint is not a full-file SHA")
    func testSharedAnalysisSkipsProviderForWeakFingerprint() async throws {
        let store = try await makeTestStore()
        try await seedAsset(
            store: store,
            fastTranscriptCoverageEndTime: nil,
            assetFingerprint: "https://example.com/audio.mp3|etag|12345|Tue, 01 Jan 2030 00:00:00 GMT",
            episodeDurationSec: 120
        )

        let sharingProvider = StubCrossUserAnalysisSharingProvider()
        sharingProvider.snapshot = makeSharedAnalysisSnapshot(
            key: CrossUserAnalysisShareKey(
                podcastId: "test-pod",
                fileSHA: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                analysisVersion: 1
            )
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 2)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            analysisSharingProvider: sharingProvider
        )

        _ = await runner.run(makeTestRequest(desiredCoverageSec: 60))

        #expect(sharingProvider.requestedKeys.isEmpty)
        #expect(sharingProvider.publishedSnapshots.isEmpty)
        #expect(audioStub.decodeCallCount == 1)
        #expect(adStub.hotPathCallCount == 1)
        #expect(adStub.backfillCallCount == 1)
    }

    @Test("sharing provider is not queried when the podcast id is missing")
    func testSharedAnalysisSkipsProviderForMissingPodcastId() async throws {
        let store = try await makeTestStore()
        try await seedAsset(
            store: store,
            fastTranscriptCoverageEndTime: nil,
            assetFingerprint: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            episodeDurationSec: 120
        )

        let sharingProvider = StubCrossUserAnalysisSharingProvider()
        sharingProvider.snapshot = makeSharedAnalysisSnapshot(
            key: CrossUserAnalysisShareKey(
                podcastId: "",
                fileSHA: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                analysisVersion: 1
            )
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 2)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            analysisSharingProvider: sharingProvider
        )

        _ = await runner.run(makeTestRequest(desiredCoverageSec: 60, podcastId: ""))

        #expect(sharingProvider.requestedKeys.isEmpty)
        #expect(sharingProvider.publishedSnapshots.isEmpty)
        #expect(audioStub.decodeCallCount == 1)
        #expect(adStub.hotPathCallCount == 1)
        #expect(adStub.backfillCallCount == 1)
    }

    @Test("shared analysis hit preserves writeWindowsOnly semantics and publishes on later live pass")
    func testSharedAnalysisImportHitHonorsOutputPolicyAndPublishesExistingImport() async throws {
        let store = try await makeTestStore()
        try await seedAsset(
            store: store,
            assetFingerprint: "1111111111111111111111111111111111111111111111111111111111111111",
            episodeDurationSec: 120
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "test-pod",
            fileSHA: "1111111111111111111111111111111111111111111111111111111111111111",
            analysisVersion: 1
        )
        let sharingProvider = StubCrossUserAnalysisSharingProvider()
        sharingProvider.snapshot = makeSharedAnalysisSnapshot(key: key)

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 4)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            analysisSharingProvider: sharingProvider
        )

        let outcome = await runner.run(makeTestRequest(desiredCoverageSec: 60, outputPolicy: .writeWindowsOnly))

        #expect(audioStub.decodeCallCount == 0)
        #expect(adStub.hotPathCallCount == 0)
        #expect(adStub.backfillCallCount == 0)
        #expect(outcome.cueCoverageSec == 60)
        #expect(outcome.newCueCount == 0)
        #expect(sharingProvider.importedWindows.isEmpty)

        let liveOutcome = await runner.run(makeTestRequest(
            desiredCoverageSec: 60,
            outputPolicy: .writeWindowsAndPushLive
        ))

        #expect(audioStub.decodeCallCount == 0)
        #expect(adStub.hotPathCallCount == 0)
        #expect(adStub.backfillCallCount == 0)
        #expect(liveOutcome.cueCoverageSec == 60)
        #expect(liveOutcome.newCueCount == 0)
        #expect(sharingProvider.importedWindows.count == 1)
        #expect(sharingProvider.importedWindows.first?.analysisAssetId == "test-asset")
        #expect(sharingProvider.importedWindows.first?.adDescription == "Imported promo")
    }

    @Test("shared non-ad import skips detection without publishing banner windows")
    func testSharedNonAdImportDoesNotPublishBannerWindows() async throws {
        let store = try await makeTestStore()
        try await seedAsset(
            store: store,
            assetFingerprint: "2222222222222222222222222222222222222222222222222222222222222222",
            episodeDurationSec: 120
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "test-pod",
            fileSHA: "2222222222222222222222222222222222222222222222222222222222222222",
            analysisVersion: 1
        )
        let sharingProvider = StubCrossUserAnalysisSharingProvider()
        sharingProvider.snapshot = CrossUserAnalysisSnapshot(
            key: key,
            provenance: CrossUserAnalysisProvenance(
                exportedAt: 1_800_000_000,
                sourceAnalysisVersion: 1,
                sourceAppBuild: "runner-test"
            ),
            analysisCoverageEndSec: 60,
            measurements: CrossUserAnalysisMeasurements(),
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    sourceWindowId: "peer-non-ad-window",
                    startTime: 10,
                    endTime: 60,
                    confidence: 0.99,
                    boundaryState: AdBoundaryState.acousticRefined.rawValue,
                    decisionState: AdDecisionState.suppressed.rawValue,
                    isAd: false,
                    detectorVersion: "fm-test-v1",
                    advertiser: nil,
                    product: nil,
                    adDescription: nil,
                    metadataSource: "foundation-model",
                    metadataConfidence: 0.82,
                    metadataPromptVersion: "prompt-v1",
                    evidenceSources: "semantic,fusion",
                    eligibilityGate: nil,
                    catalogStoreMatchSimilarity: nil
                ),
            ]
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 4)
        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            analysisSharingProvider: sharingProvider
        )

        let outcome = await runner.run(makeTestRequest(desiredCoverageSec: 60))

        #expect(audioStub.decodeCallCount == 0)
        #expect(adStub.hotPathCallCount == 0)
        #expect(adStub.backfillCallCount == 0)
        #expect(outcome.cueCoverageSec == 0)
        #expect(outcome.newCueCount == 0)
        #expect(sharingProvider.importedWindows.isEmpty)

        let windows = try await store.fetchAdWindows(assetId: "test-asset")
        #expect(windows.count == 1)
        #expect(windows.first?.decisionState == AdDecisionState.suppressed.rawValue)
    }

    @Test("shared analysis below requested coverage falls through to local detection")
    func testSharedAnalysisBelowRequestedCoverageFallsThrough() async throws {
        let store = try await makeTestStore()
        try await seedAsset(
            store: store,
            fastTranscriptCoverageEndTime: nil,
            assetFingerprint: "3333333333333333333333333333333333333333333333333333333333333333",
            episodeDurationSec: 120
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "test-pod",
            fileSHA: "3333333333333333333333333333333333333333333333333333333333333333",
            analysisVersion: 1
        )
        let sharingProvider = StubCrossUserAnalysisSharingProvider()
        sharingProvider.snapshot = makeSharedAnalysisSnapshot(
            key: key,
            analysisCoverageEndSec: 30
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 2)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            analysisSharingProvider: sharingProvider
        )

        _ = await runner.run(makeTestRequest(desiredCoverageSec: 60))

        #expect(sharingProvider.requestedKeys == [key])
        #expect(audioStub.decodeCallCount == 1)
        #expect(adStub.hotPathCallCount == 1)
        #expect(adStub.backfillCallCount == 1)
    }

    @Test("shared analysis with inflated coverage falls through to local detection")
    func testSharedAnalysisWithInflatedCoverageFallsThrough() async throws {
        let store = try await makeTestStore()
        try await seedAsset(
            store: store,
            fastTranscriptCoverageEndTime: nil,
            assetFingerprint: "4444444444444444444444444444444444444444444444444444444444444444",
            episodeDurationSec: 120
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "test-pod",
            fileSHA: "4444444444444444444444444444444444444444444444444444444444444444",
            analysisVersion: 1
        )
        let sharingProvider = StubCrossUserAnalysisSharingProvider()
        sharingProvider.snapshot = CrossUserAnalysisSnapshot(
            key: key,
            provenance: CrossUserAnalysisProvenance(
                exportedAt: 1_800_000_000,
                sourceAnalysisVersion: 1,
                sourceAppBuild: "runner-test"
            ),
            analysisCoverageEndSec: 60,
            measurements: CrossUserAnalysisMeasurements(),
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    sourceWindowId: "peer-window",
                    startTime: 10,
                    endTime: 30,
                    confidence: 0.9,
                    boundaryState: AdBoundaryState.acousticRefined.rawValue,
                    decisionState: AdDecisionState.confirmed.rawValue,
                    detectorVersion: "fm-test-v1",
                    advertiser: "Acme",
                    product: "Widget",
                    adDescription: "Inflated promo",
                    metadataSource: "foundation-model",
                    metadataConfidence: 0.82,
                    metadataPromptVersion: "prompt-v1",
                    evidenceSources: "semantic,fusion",
                    eligibilityGate: SkipEligibilityGate.eligible.rawValue,
                    catalogStoreMatchSimilarity: nil
                ),
            ]
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 2)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            analysisSharingProvider: sharingProvider
        )

        _ = await runner.run(makeTestRequest(desiredCoverageSec: 60))

        #expect(sharingProvider.requestedKeys == [key])
        #expect(audioStub.decodeCallCount == 1)
        #expect(adStub.hotPathCallCount == 1)
        #expect(adStub.backfillCallCount == 1)
    }

    @Test("shared analysis with non-actionable eligibility gate falls through to local detection")
    func testSharedAnalysisWithNonActionableEligibilityGateFallsThrough() async throws {
        let store = try await makeTestStore()
        try await seedAsset(
            store: store,
            fastTranscriptCoverageEndTime: nil,
            assetFingerprint: "4545454545454545454545454545454545454545454545454545454545454545",
            episodeDurationSec: 120
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "test-pod",
            fileSHA: "4545454545454545454545454545454545454545454545454545454545454545",
            analysisVersion: 1
        )
        let sharingProvider = StubCrossUserAnalysisSharingProvider()
        sharingProvider.snapshot = CrossUserAnalysisSnapshot(
            key: key,
            provenance: CrossUserAnalysisProvenance(
                exportedAt: 1_800_000_000,
                sourceAnalysisVersion: 1,
                sourceAppBuild: "runner-test"
            ),
            analysisCoverageEndSec: 60,
            measurements: CrossUserAnalysisMeasurements(),
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    sourceWindowId: "peer-blocked-window",
                    startTime: 10,
                    endTime: 60,
                    confidence: 0.9,
                    boundaryState: AdBoundaryState.acousticRefined.rawValue,
                    decisionState: AdDecisionState.confirmed.rawValue,
                    detectorVersion: "fm-test-v1",
                    advertiser: "Acme",
                    product: "Widget",
                    adDescription: "Blocked promo",
                    metadataSource: "foundation-model",
                    metadataConfidence: 0.82,
                    metadataPromptVersion: "prompt-v1",
                    evidenceSources: "semantic,fusion",
                    eligibilityGate: SkipEligibilityGate.blockedByUserCorrection.rawValue,
                    catalogStoreMatchSimilarity: nil
                ),
            ]
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 2)

        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            analysisSharingProvider: sharingProvider
        )

        _ = await runner.run(makeTestRequest(desiredCoverageSec: 60))

        #expect(sharingProvider.requestedKeys == [key])
        #expect(audioStub.decodeCallCount == 1)
        #expect(adStub.hotPathCallCount == 1)
        #expect(adStub.backfillCallCount == 1)
    }

    @Test("local analysis success publishes a shared snapshot when provider is enabled")
    func testLocalAnalysisPublishesSharedSnapshot() async throws {
        let store = try await makeTestStore()
        try await seedAsset(
            store: store,
            fastTranscriptCoverageEndTime: 30,
            assetFingerprint: "5555555555555555555555555555555555555555555555555555555555555555",
            episodeDurationSec: 30
        )
        let segment = makeTranscriptSegment()
        try await store.insertTranscriptChunks([makeTranscriptChunk(from: segment)])
        try await store.insertAdWindow(
            AdWindow(
                id: "publish-window",
                analysisAssetId: "test-asset",
                startTime: 5,
                endTime: 20,
                confidence: 0.9,
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
                decisionState: AdDecisionState.confirmed.rawValue,
                detectorVersion: "test-v1",
                advertiser: "Acme",
                product: "Widget",
                adDescription: "Imported later",
                evidenceText: "local transcript evidence",
                evidenceStartTime: 5,
                metadataSource: "foundation-model",
                metadataConfidence: 0.8,
                metadataPromptVersion: "prompt-v1",
                wasSkipped: false,
                userDismissedBanner: false
            )
        )

        let sharingProvider = StubCrossUserAnalysisSharingProvider()
        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 1)
        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            analysisSharingProvider: sharingProvider
        )

        let outcome = await runner.run(makeTestRequest(desiredCoverageSec: 30))

        if case .reachedTarget = outcome.stopReason {
            // expected
        } else {
            Issue.record("Expected .reachedTarget but got \(outcome.stopReason)")
        }
        #expect(sharingProvider.publishedSnapshots.count == 1)
        let snapshot = try #require(sharingProvider.publishedSnapshots.first)
        #expect(snapshot.key == CrossUserAnalysisShareKey(
            podcastId: "test-pod",
            fileSHA: "5555555555555555555555555555555555555555555555555555555555555555",
            analysisVersion: 1
        ))
        #expect(snapshot.windows.count == 1)
        #expect(snapshot.windows.first?.sourceWindowId == "publish-window")

        let encodedData = try JSONEncoder().encode(snapshot)
        let encoded = String(data: encodedData, encoding: .utf8) ?? ""
        #expect(!encoded.contains("local transcript evidence"))
        #expect(!encoded.contains("evidenceText"))
    }

    @Test("local writeWindowsOnly analysis does not publish until a later live-capable pass")
    func testLocalWriteWindowsOnlyAnalysisDefersSharedSnapshotPublish() async throws {
        let store = try await makeTestStore()
        try await seedAsset(
            store: store,
            fastTranscriptCoverageEndTime: 30,
            assetFingerprint: "6666666666666666666666666666666666666666666666666666666666666666",
            episodeDurationSec: 30
        )
        let segment = makeTranscriptSegment()
        try await store.insertTranscriptChunks([makeTranscriptChunk(from: segment)])
        try await store.insertAdWindow(
            AdWindow(
                id: "deferred-publish-window",
                analysisAssetId: "test-asset",
                startTime: 5,
                endTime: 20,
                confidence: 0.9,
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
                decisionState: AdDecisionState.confirmed.rawValue,
                detectorVersion: "test-v1",
                advertiser: "Acme",
                product: "Widget",
                adDescription: "Deferred publish",
                evidenceText: "local transcript evidence",
                evidenceStartTime: 5,
                metadataSource: "foundation-model",
                metadataConfidence: 0.8,
                metadataPromptVersion: "prompt-v1",
                wasSkipped: false,
                userDismissedBanner: false
            )
        )

        let sharingProvider = StubCrossUserAnalysisSharingProvider()
        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 1)
        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub,
            analysisSharingProvider: sharingProvider
        )

        _ = await runner.run(makeTestRequest(
            desiredCoverageSec: 30,
            outputPolicy: .writeWindowsOnly
        ))

        #expect(sharingProvider.publishedSnapshots.isEmpty)

        _ = await runner.run(makeTestRequest(
            desiredCoverageSec: 30,
            outputPolicy: .writeWindowsAndPushLive
        ))

        #expect(sharingProvider.publishedSnapshots.count == 1)
        let snapshot = try #require(sharingProvider.publishedSnapshots.first)
        #expect(snapshot.windows.count == 1)
        #expect(snapshot.windows.first?.sourceWindowId == "deferred-publish-window")
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
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub
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
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub
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
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub
        )

        _ = await runner.run(makeTestRequest(desiredCoverageSec: 60))

        let after = try await store.fetchAsset(id: "test-asset")
        #expect(after?.episodeDurationSec == 999.0)
    }

    // MARK: - playhead-5uvz.7 (Gap-9) transcription-timeout journaling

    /// When stage 3 produces zero coverage (timeout firing ahead of
    /// `.completed`, or a stream that ends without ever advancing the
    /// watermark), the runner must emit a structured `work_journal` row
    /// with `eventType=.failed`, `cause=.asrFailed`, and metadata
    /// describing the engine's progress at the moment of timeout
    /// (`episode_duration`, `transcript_coverage_end_time`,
    /// `chunks_persisted`, `chunk_rate_per_sec`). Without this, a class
    /// of episodes that systematically times out (long, refusal-prone,
    /// music-heavy) only shows up if operators grep `lastErrorCode` —
    /// the journal row makes the pattern visible in aggregate.
    @Test("Zero-coverage transcription emits a work_journal failed row with structured metadata")
    func testZeroCoverageTranscriptionEmitsJournalRow() async throws {
        let store = try await makeTestStore()
        // Note: NO `fastTranscriptCoverageEndTime` seeded — the engine
        // running with a stub recognizer that returns `[]` leaves
        // coverage at nil → 0, driving the runner into the
        // `transcription:zeroCoverage` branch.
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: nil)

        // Seed an analysis_jobs row + acquire a real lease so the
        // runner's `fetchJob(byId:)` returns a {generationID,
        // schedulerEpoch} pair on the journal-emit path. The
        // `acquireLeaseWithJournal` call mirrors the production
        // scheduler's atomic acquire (playhead-5uvz.1): it stamps the
        // analysis_jobs row with a fresh generation + epoch and writes
        // an `acquired` journal row in the same transaction.
        let jobId = UUID().uuidString
        let inserted = try await store.insertJob(
            makeAnalysisJob(
                jobId: jobId,
                episodeId: "test-ep",
                analysisAssetId: "test-asset",
                workKey: "wk-zero-cov-\(UUID().uuidString)"
            )
        )
        #expect(inserted, "insertJob must succeed for the test premise to hold")
        let acquired = try await store.acquireLeaseWithJournal(
            jobId: jobId,
            episodeId: "test-ep",
            owner: "test-owner",
            expiresAt: Date().timeIntervalSince1970 + 300
        )
        #expect(acquired, "Lease acquire must succeed for the test premise to hold")
        let leasedJob = try await store.fetchJob(byId: jobId)
        let generationID = leasedJob?.generationID ?? ""
        let schedulerEpoch = leasedJob?.schedulerEpoch ?? 0
        #expect(!generationID.isEmpty)

        let audioStub = StubAnalysisAudioProvider()
        // 4 shards × 30s = 120s of decoded audio.
        audioStub.shardsToReturn = makeShards(count: 4)

        let featureService = FeatureExtractionService(store: store)
        // Drive zero-coverage by having the recognizer throw on every
        // shard. The transcription loop catches the throw and continues
        // to the next shard WITHOUT advancing coverage, so the asset's
        // `fastTranscriptCoverageEndTime` stays nil. The loop emits
        // `.completed` after exhausting the shard list — the runner
        // observes coverage=0 and falls into the zero-coverage failure
        // branch (the same branch the 5-minute timeout would land on).
        let recognizer = MockSpeechRecognizer()
        let speechService = SpeechService(recognizer: recognizer)
        try await speechService.loadFastModel()
        // Flip after load so loadModel doesn't throw — only transcribe
        // calls fail.
        recognizer.shouldThrow = true
        let transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: store
        )
        let adStub = StubAdDetectionProvider()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adStub
        )

        let request = AnalysisRangeRequest(
            jobId: jobId,
            episodeId: "test-ep",
            podcastId: "test-pod",
            analysisAssetId: "test-asset",
            audioURL: makeTestRequest().audioURL,
            desiredCoverageSec: 120,
            mode: .preRollWarmup,
            outputPolicy: .writeWindowsAndCues,
            priority: .medium
        )
        let outcome = await runner.run(request)

        // Pin the precondition: we did land on the zero-coverage failure
        // branch. If the upstream pipeline ever changes such that this
        // branch no longer fires, the entire test premise is invalid.
        //
        // playhead-8ysk: the reason is no longer the fixed literal
        // `transcription:zeroCoverage`. That one string stood for nine
        // distinguishable causes, so `lastErrorCode` could not tell a silent
        // shard from an unloaded model from a missing locale asset. It now
        // names the class the engine actually reported — here
        // `transcription_failed`, because `MockSpeechRecognizer` throws
        // `TranscriptEngineError.transcriptionFailed` from every shard.
        if case .failed(let msg) = outcome.stopReason {
            #expect(msg == "transcription:\(TranscriptFailureClass.transcriptionFailed.rawValue)",
                    "got \(msg)")
            #expect(msg != "transcription:zeroCoverage",
                    "the fallback literal means the engine's .failed event never reached the runner")
        } else {
            Issue.record("Expected .failed(transcription:...), got \(outcome.stopReason)")
        }

        // The journal row should be discoverable via the {episode,
        // generation} lookup the lease lifecycle uses.
        let entries = try await store.fetchWorkJournalEntries(
            episodeId: "test-ep",
            generationID: generationID
        )
        let failedRows = entries.filter {
            $0.eventType == .failed && $0.cause == .asrFailed
        }
        #expect(failedRows.count == 1,
                "Expected exactly one failed/asrFailed row; got \(failedRows.count) (entries=\(entries.map { ($0.eventType, $0.cause?.rawValue ?? "nil") }))")
        guard let row = failedRows.first else { return }

        #expect(row.episodeId == "test-ep")
        #expect(row.schedulerEpoch == schedulerEpoch)
        #expect(row.artifactClass == .scratch)

        // Metadata is a JSON blob; assert the structural keys the bead
        // enumerates are present and parseable. We do NOT pin specific
        // numeric values — `chunk_rate_per_sec` depends on stage-3
        // wall-clock and is non-deterministic — but the keys are the
        // observability contract.
        let metadataData = Data(row.metadata.utf8)
        let parsed = try JSONSerialization.jsonObject(with: metadataData) as? [String: Any]
        #expect(parsed != nil, "metadata must be valid JSON")
        if let parsed {
            #expect(parsed["episode_duration"] != nil,
                    "metadata must carry episode_duration; got keys=\(Array(parsed.keys).sorted())")
            #expect(parsed["transcript_coverage_end_time"] != nil,
                    "metadata must carry transcript_coverage_end_time; got keys=\(Array(parsed.keys).sorted())")
            #expect(parsed["chunks_persisted"] != nil,
                    "metadata must carry chunks_persisted; got keys=\(Array(parsed.keys).sorted())")
            #expect(parsed["chunk_rate_per_sec"] != nil,
                    "metadata must carry chunk_rate_per_sec; got keys=\(Array(parsed.keys).sorted())")
            // Structural sibling — the SliceCompletionInstrumentation
            // helper guarantees these too.
            #expect(parsed["device_class"] != nil)
            #expect(parsed["slice_duration_ms"] != nil)
            // job_id surfaces the failing run for cross-correlation
            // with the analysis_jobs row's lastErrorCode.
            #expect((parsed["job_id"] as? String) == jobId)
            #expect((parsed["stage"] as? String) == "analysisJobRunner.run.transcriptionTimeout")
            // Episode duration reflects the full decoded audio (4×30s).
            #expect((parsed["episode_duration"] as? String) == "120.000")
            // Zero chunks persisted is the headline observability signal
            // for the bug class this row exists to surface.
            #expect((parsed["chunks_persisted"] as? String) == "0")
            // No coverage advance — the asset's watermark stayed nil.
            #expect((parsed["transcript_coverage_end_time"] as? String) == "0.000")

            // playhead-8ysk: the keys that make this row diagnostic rather
            // than merely present. This is the ONLY end-to-end assertion
            // that the engine's `.failed` reason actually traverses
            // `AnalysisJobRunner.run` into the journal — every other test of
            // the taxonomy stops at one seam or the other.
            #expect(
                (parsed[DiagnosticsFailureKeys.failureClass] as? String)
                    == TranscriptFailureClass.transcriptionFailed.rawValue,
                "metadata must name the cause; got keys=\(Array(parsed.keys).sorted())"
            )
            // playhead-ngev: and the two keys that say how the run ended. The
            // engine reported, and it reported having reached its own
            // conclusion — as opposed to a 300 s silence or a run cut short by
            // playback, which are the same blank column without these.
            #expect(
                (parsed[DiagnosticsFailureKeys.failureObservation] as? String)
                    == AnalysisJobRunner.TranscriptRunObservation.engineReported.rawValue,
                "got \(String(describing: parsed[DiagnosticsFailureKeys.failureObservation]))"
            )
            #expect(
                (parsed[DiagnosticsFailureKeys.failureTermination] as? String)
                    == TranscriptRunTermination.ranToConclusion.rawValue
            )
            // Four shards were decoded and every one of them failed.
            #expect((parsed[DiagnosticsFailureKeys.failedShardCount] as? String) == "4")
            // `TranscriptEngineError` is a Swift-native enum, so its bridged
            // NSError code is just a case ordinal — the emitter must omit
            // the key rather than export a meaningless 0 that would read as
            // a real framework code.
            #expect(parsed[DiagnosticsFailureKeys.failureCode] == nil)

            // And the whole point: it survives the projection into a bundle
            // a support engineer reads without a device attached. Before
            // this bead the row reached SQLite and died there.
            let bundle = DiagnosticsBundleBuilder.buildDefault(
                appVersion: "1.0", osVersion: "iOS 27", deviceClass: .iPhone17Pro,
                buildType: .debug,
                eligibility: AnalysisEligibility(
                    hardwareSupported: true, appleIntelligenceEnabled: true,
                    regionSupported: true, languageSupported: true,
                    modelAvailableNow: true, capturedAt: Date()
                ),
                workJournalEntries: [row], installID: UUID()
            )
            let projected = bundle.workJournalTail.first { $0.id == row.id }
            #expect(projected?.failureClass == TranscriptFailureClass.transcriptionFailed.rawValue,
                    "the cause must survive the bundle projection, not just SQLite")
            #expect(
                projected?.failureObservation
                    == AnalysisJobRunner.TranscriptRunObservation.engineReported.rawValue,
                "the observation must survive the projection too"
            )
            #expect(projected?.failureTermination
                    == TranscriptRunTermination.ranToConclusion.rawValue)
        }
    }

    // MARK: - playhead-ngev: `cause` stops contradicting `failure_class`

    /// A ROW THAT CONTRADICTED ITSELF, END TO END.
    ///
    /// `cause` was a hardcoded `.asrFailed` at both emission sites with no
    /// reference to the failure at all, so a run in which the recognizer was
    /// never invoked — the model never loaded, which is the post-swallow state
    /// of the launch-time `loadFastModel()` failure — still produced a row
    /// reading `cause = asr_failed` beside `failure_class =
    /// speech_engine_not_ready`. Whichever half an aggregate counted, one of
    /// them was wrong, and the `asr_failed` count is the one two dogfood
    /// cycles were read from.
    ///
    /// The engine-side class is already pinned by
    /// `notReadyEngineEmitsFailedInsteadOfNothing`; what is new here is that
    /// the RUNNER attributes it correctly on its way into `work_journal`.
    @Test("A run where the recognizer never loaded is journaled as a pipeline error, not an ASR failure")
    func testNotReadyEngineJournalsPipelineErrorNotASRFailure() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: nil)

        let jobId = UUID().uuidString
        let inserted = try await store.insertJob(
            makeAnalysisJob(
                jobId: jobId,
                episodeId: "test-ep",
                analysisAssetId: "test-asset",
                workKey: "wk-not-ready-\(UUID().uuidString)"
            )
        )
        #expect(inserted, "insertJob must succeed for the test premise to hold")
        let acquired = try await store.acquireLeaseWithJournal(
            jobId: jobId,
            episodeId: "test-ep",
            owner: "test-owner",
            expiresAt: Date().timeIntervalSince1970 + 300
        )
        #expect(acquired, "Lease acquire must succeed for the test premise to hold")
        let generationID = try await store.fetchJob(byId: jobId)?.generationID ?? ""

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 4)

        // The post-swallow device state: a recognizer that never loaded.
        // Deliberately NO `loadFastModel()`.
        let speechService = SpeechService(recognizer: MockSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: speechService,
                store: store
            ),
            adDetection: StubAdDetectionProvider()
        )

        let outcome = await runner.run(
            AnalysisRangeRequest(
                jobId: jobId,
                episodeId: "test-ep",
                podcastId: "test-pod",
                analysisAssetId: "test-asset",
                audioURL: makeTestRequest().audioURL,
                desiredCoverageSec: 120,
                mode: .preRollWarmup,
                outputPolicy: .writeWindowsAndCues,
                priority: .medium
            )
        )

        // The premise: we landed on the zero-coverage branch with the engine's
        // own class, not on the fallback literal.
        if case .failed(let msg) = outcome.stopReason {
            #expect(msg == "transcription:\(TranscriptFailureClass.speechEngineNotReady.rawValue)",
                    "got \(msg)")
        } else {
            Issue.record("Expected .failed(transcription:...), got \(outcome.stopReason)")
        }

        let entries = try await store.fetchWorkJournalEntries(
            episodeId: "test-ep", generationID: generationID
        )
        let failedRows = entries.filter { $0.eventType == .failed }
        #expect(failedRows.count == 1, "expected one failed row; got \(failedRows.count)")
        guard let row = failedRows.first else { return }

        #expect(
            row.cause == .pipelineError,
            """
            the recognizer was never invoked — `SpeechService.isReady()` was \
            false before a single shard was handed over — and the row still \
            says \(row.cause?.rawValue ?? "nil"). That contradiction is what \
            makes an `asr_failed` count unusable
            """
        )
        #expect(row.cause != .asrFailed)

        let parsed = try JSONSerialization.jsonObject(
            with: Data(row.metadata.utf8)
        ) as? [String: Any]
        #expect((parsed?[DiagnosticsFailureKeys.failureClass] as? String)
                == TranscriptFailureClass.speechEngineNotReady.rawValue)
        #expect((parsed?[DiagnosticsFailureKeys.failureObservation] as? String)
                == AnalysisJobRunner.TranscriptRunObservation.engineReported.rawValue)

        // And the projection carries both, so the contradiction cannot be
        // re-introduced downstream of the journal either.
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 27", deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: AnalysisEligibility(
                hardwareSupported: true, appleIntelligenceEnabled: true,
                regionSupported: true, languageSupported: true,
                modelAvailableNow: true, capturedAt: Date()
            ),
            workJournalEntries: [row], installID: UUID()
        )
        let projected = bundle.workJournalTail.first { $0.id == row.id }
        #expect(projected?.cause == InternalMissCause.pipelineError.rawValue)
        #expect(projected?.failureClass == TranscriptFailureClass.speechEngineNotReady.rawValue)
    }

    // MARK: - playhead-ngev: the runner must not fence an engine a live owner holds

    /// THE CALL SITE, NOT THE PREDICATE.
    ///
    /// `shouldStopEngine(after:)` is a pure function and is unit-tested both
    /// ways in `TranscriptObservationTests`. That proves nothing about whether
    /// the runner CONSULTS it: the runner holds a concrete
    /// `TranscriptEngineService` with no protocol seam, so `stopTranscription`
    /// cannot be spied on, and a build that dropped the predicate from the
    /// zero-coverage `if` would pass every test that existed before this one.
    ///
    /// That call site is where this bead can hurt a listener. The stop exists
    /// (playhead-5uvz.5 Gap-6) to fence an ORPHANED engine. An interrupted run
    /// is the one case where the engine is not orphaned but RE-TASKED: the
    /// cancel came from `startTranscription` on the shared engine — a scrub, a
    /// speed change, a different episode — so stopping it there cancels the
    /// listener's own transcription and gates the asset against the appends
    /// that owner is about to make.
    ///
    /// Only reachable at all since this bead: an interruption used to be
    /// silent, so the runner sat out its 300 s timeout and the successor was
    /// usually finished before the stop landed.
    @Test("an interrupted run leaves the shared engine unfenced")
    func testInterruptedRunDoesNotFenceTheAsset() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: nil)

        let jobId = UUID().uuidString
        let inserted = try await store.insertJob(
            makeAnalysisJob(
                jobId: jobId,
                episodeId: "test-ep",
                analysisAssetId: "test-asset",
                workKey: "wk-interrupted-\(UUID().uuidString)"
            )
        )
        #expect(inserted, "insertJob must succeed for the test premise to hold")
        let acquired = try await store.acquireLeaseWithJournal(
            jobId: jobId,
            episodeId: "test-ep",
            owner: "test-owner",
            expiresAt: Date().timeIntervalSince1970 + 300
        )
        #expect(acquired, "Lease acquire must succeed for the test premise to hold")
        let generationID = try await store.fetchJob(byId: jobId)?.generationID ?? ""

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 4)

        let speechService = SpeechService(
            recognizer: CancellingRecognizer(), serializesRecognizerRequests: false
        )
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(speechService: speechService, store: store)
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: transcriptEngine,
            adDetection: StubAdDetectionProvider()
        )

        let outcome = await runner.run(
            AnalysisRangeRequest(
                jobId: jobId,
                episodeId: "test-ep",
                podcastId: "test-pod",
                analysisAssetId: "test-asset",
                audioURL: makeTestRequest().audioURL,
                desiredCoverageSec: 120,
                mode: .preRollWarmup,
                outputPolicy: .writeWindowsAndCues,
                priority: .medium
            )
        )

        // The premise: we really did land on the zero-coverage branch carrying
        // an INTERRUPTED failure, not some other zero-coverage shape.
        if case .failed(let msg) = outcome.stopReason {
            #expect(msg == "transcription:\(TranscriptFailureClass.cancelled.rawValue)", "got \(msg)")
        } else {
            Issue.record("Expected .failed(transcription:cancelled), got \(outcome.stopReason)")
        }

        // THE ASSERTION. The engine is shared and its live owner has already
        // re-tasked it; the runner must not have fenced the asset.
        #expect(
            await transcriptEngine.isStoppedForTesting(analysisAssetId: "test-asset") == false,
            """
            the runner fenced an asset whose transcription a live owner still \
            holds. In production that cancels the listener's own transcription \
            mid-episode and drops every shard that owner appends next
            """
        )

        // And the row still names what happened, so declining to stop is not
        // also declining to report.
        let entries = try await store.fetchWorkJournalEntries(
            episodeId: "test-ep", generationID: generationID
        )
        let failedRows = entries.filter { $0.eventType == .failed }
        #expect(failedRows.count == 1, "expected one failed row; got \(failedRows.count)")
        guard let row = failedRows.first else { return }
        #expect(
            row.cause == .pipelineError,
            "a scrub is not an ASR failure (got \(row.cause?.rawValue ?? "nil"))"
        )
        let parsed = try JSONSerialization.jsonObject(
            with: Data(row.metadata.utf8)
        ) as? [String: Any]
        #expect((parsed?[DiagnosticsFailureKeys.failureClass] as? String)
                == TranscriptFailureClass.cancelled.rawValue)
        #expect((parsed?[DiagnosticsFailureKeys.failureTermination] as? String)
                == TranscriptRunTermination.interrupted.rawValue)
        #expect((parsed?[DiagnosticsFailureKeys.failureObservation] as? String)
                == AnalysisJobRunner.TranscriptRunObservation.engineReported.rawValue)
    }

    /// The control, and it is what keeps the carve-out from reading "never
    /// stop" — which would reinstate the orphaned writer playhead-5uvz.5
    /// fenced. A run that reached its own conclusion over zero coverage still
    /// gets the engine fenced, through the same call site.
    @Test("control: a run that concluded on its own still fences the asset")
    func testConcludedRunStillFencesTheAsset() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store: store, fastTranscriptCoverageEndTime: nil)

        let jobId = UUID().uuidString
        _ = try await store.insertJob(
            makeAnalysisJob(
                jobId: jobId,
                episodeId: "test-ep",
                analysisAssetId: "test-asset",
                workKey: "wk-concluded-\(UUID().uuidString)"
            )
        )
        _ = try await store.acquireLeaseWithJournal(
            jobId: jobId,
            episodeId: "test-ep",
            owner: "test-owner",
            expiresAt: Date().timeIntervalSince1970 + 300
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards(count: 4)

        let recognizer = MockSpeechRecognizer()
        let speechService = SpeechService(
            recognizer: recognizer, serializesRecognizerRequests: false
        )
        try await speechService.loadFastModel()
        recognizer.shouldThrow = true
        let transcriptEngine = TranscriptEngineService(speechService: speechService, store: store)
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: transcriptEngine,
            adDetection: StubAdDetectionProvider()
        )

        let outcome = await runner.run(
            AnalysisRangeRequest(
                jobId: jobId,
                episodeId: "test-ep",
                podcastId: "test-pod",
                analysisAssetId: "test-asset",
                audioURL: makeTestRequest().audioURL,
                desiredCoverageSec: 120,
                mode: .preRollWarmup,
                outputPolicy: .writeWindowsAndCues,
                priority: .medium
            )
        )

        if case .failed(let msg) = outcome.stopReason {
            #expect(msg == "transcription:\(TranscriptFailureClass.transcriptionFailed.rawValue)",
                    "got \(msg)")
        } else {
            Issue.record("Expected .failed(transcription:...), got \(outcome.stopReason)")
        }

        #expect(
            await transcriptEngine.isStoppedForTesting(analysisAssetId: "test-asset"),
            """
            the runner left an orphaned engine unfenced. Its later chunk writes \
            and coverage updates land behind the back of a scheduler that has \
            already moved on — the defect playhead-5uvz.5 Gap-6 closed
            """
        )
    }
}
