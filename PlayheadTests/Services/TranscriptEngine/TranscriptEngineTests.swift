// TranscriptEngineTests.swift
// Tests for the Speech integration layer and dual-pass transcript engine.

import AVFoundation
import Foundation
import os
import Testing
@testable import Playhead

#if canImport(Speech)
import Speech
#endif

// MARK: - Mock Speech Recognizer

/// Controllable mock for testing SpeechService without real Apple Speech.
final class MockSpeechRecognizer: SpeechRecognizer, @unchecked Sendable {
    private var loaded = false
    var transcribeResult: [TranscriptSegment] = []
    var vadResult: [VADResult] = []
    var transcribeCallCount = 0
    var vadCallCount = 0
    var shouldThrow = false

    func loadModel() async throws {
        if shouldThrow { throw TranscriptEngineError.transcriptionFailed("mock load error") }
        loaded = true
    }

    func unloadModel() async {
        loaded = false
    }

    func isModelLoaded() async -> Bool {
        loaded
    }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard loaded else { throw TranscriptEngineError.modelNotLoaded }
        if shouldThrow { throw TranscriptEngineError.transcriptionFailed("mock error") }
        transcribeCallCount += 1
        return transcribeResult
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        guard loaded else { throw TranscriptEngineError.modelNotLoaded }
        vadCallCount += 1
        return vadResult
    }
}

/// Probe recognizer used to detect overlap between async transcribe calls.
private final class ConcurrentProbeSpeechRecognizer: SpeechRecognizer, @unchecked Sendable {
    private struct State {
        var loaded = false
        var transcribeCallCount = 0
        var concurrentTranscribes = 0
        var maxConcurrentTranscribes = 0
    }

    private let state = OSAllocatedUnfairLock(initialState: State())
    private let delay: Duration

    init(delay: Duration = .milliseconds(100)) {
        self.delay = delay
    }

    var maxConcurrentTranscribes: Int {
        state.withLock { $0.maxConcurrentTranscribes }
    }

    var transcribeCallCount: Int {
        state.withLock { $0.transcribeCallCount }
    }

    func loadModel() async throws {
        state.withLock { $0.loaded = true }
    }

    func unloadModel() async {
        state.withLock { $0.loaded = false }
    }

    func isModelLoaded() async -> Bool {
        state.withLock { $0.loaded }
    }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard state.withLock({ $0.loaded }) else { throw TranscriptEngineError.modelNotLoaded }

        state.withLock { state in
            state.transcribeCallCount += 1
            state.concurrentTranscribes += 1
            state.maxConcurrentTranscribes = max(
                state.maxConcurrentTranscribes,
                state.concurrentTranscribes
            )
        }
        defer {
            state.withLock { $0.concurrentTranscribes -= 1 }
        }

        try? await Task.sleep(for: delay)

        return [TranscriptSegment(
            id: shard.id,
            words: [TranscriptWord(
                text: "probe-\(shard.id)",
                startTime: shard.startTime,
                endTime: shard.startTime + shard.duration,
                confidence: 0.9
            )],
            text: "probe-\(shard.id)",
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration,
            avgConfidence: 0.9,
            passType: .fast
        )]
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        [VADResult(
            isSpeech: true,
            speechProbability: 1.0,
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration
        )]
    }
}

// MARK: - Helpers

private func makeSegment(id: Int = 0, passType: TranscriptPassType = .fast) -> TranscriptSegment {
    let word = TranscriptWord(text: "hello", startTime: 0, endTime: 0.5, confidence: 0.95)
    return TranscriptSegment(
        id: id,
        words: [word],
        text: "hello",
        startTime: 0,
        endTime: 0.5,
        avgConfidence: 0.95,
        passType: passType
    )
}

private func makeWeakAnchorMetadata(
    averageConfidence: Double = 0.52,
    minimumConfidence: Double = 0.31
) -> TranscriptWeakAnchorMetadata {
    TranscriptWeakAnchorMetadata(
        averageConfidence: averageConfidence,
        minimumConfidence: minimumConfidence,
        alternativeTexts: [
            "visit betterhelp.com/podcast for details",
        ],
        lowConfidencePhrases: [
            TranscriptWeakAnchorMetadata.LowConfidencePhrase(
                text: "use code save10 at checkout",
                startTime: 0.2,
                endTime: 0.8,
                confidence: minimumConfidence
            ),
        ]
    )
}

private func transcriptEngineSource() throws -> String {
    let repoRoot = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
    let sourceURL = repoRoot.appendingPathComponent("Playhead/Services/TranscriptEngine/TranscriptEngine.swift")
    return try String(contentsOf: sourceURL, encoding: .utf8)
}

private func appleSpeechAnalyzerRunnerSource() throws -> String {
    let source = try transcriptEngineSource()
    let start = try #require(source.range(of: "struct AppleSpeechAnalyzerRunner"))
    let end = try #require(source.range(of: "enum AppleSpeechResultMapper"))
    return String(source[start.lowerBound..<end.lowerBound])
}

private func makeWeakAnchorMetadata() -> TranscriptWeakAnchorMetadata {
    TranscriptWeakAnchorMetadata(
        averageConfidence: 0.46,
        minimumConfidence: 0.19,
        alternativeTexts: ["sponsored by betterhelp"],
        lowConfidencePhrases: [
            WeakAnchorPhrase(
                text: "better help",
                startTime: 0.2,
                endTime: 0.4,
                confidence: 0.19
            )
        ]
    )
}

private func makeTranscriptAsset(
    id: String,
    episodeId: String
) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: episodeId,
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///test/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "queued",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
}

#if canImport(Speech)

private func makeSpeechAttributedString(
    _ runs: [(text: String, startTime: Double, endTime: Double, confidence: Double?)]
) -> AttributedString {
    var attributed = AttributedString()

    for run in runs {
        var segment = AttributedString(run.text)
        var attributes = AttributeContainer()
        attributes.audioTimeRange = CMTimeRange(
            start: CMTime(seconds: run.startTime, preferredTimescale: 600),
            duration: CMTime(seconds: run.endTime - run.startTime, preferredTimescale: 600)
        )
        if let confidence = run.confidence {
            attributes.transcriptionConfidence = confidence
        }
        segment.mergeAttributes(attributes)
        attributed.append(segment)
    }

    return attributed
}

#endif

// MARK: - SpeechService Tests

@Suite("SpeechService – Model Management")
struct SpeechServiceModelTests {

    @Test("Starts with no model loaded")
    func initialState() async {
        let mock = MockSpeechRecognizer()
        let service = SpeechService(recognizer: mock)
        let ready = await service.isReady()
        #expect(!ready)
        let role = await service.activeModelRole
        #expect(role == nil)
    }

    @Test("loadFastModel sets active role to asrFast")
    func loadFast() async throws {
        let mock = MockSpeechRecognizer()
        let service = SpeechService(recognizer: mock)
        try await service.loadFastModel()
        let role = await service.activeModelRole
        #expect(role == .asrFast)
        let ready = await service.isReady()
        #expect(ready)
    }

    @Test("loadFinalModel sets active role to asrFinal")
    func loadFinal() async throws {
        let mock = MockSpeechRecognizer()
        let service = SpeechService(recognizer: mock)
        try await service.loadFinalModel()
        let role = await service.activeModelRole
        #expect(role == .asrFinal)
    }

    @Test("loadFinalModel unloads existing model first")
    func finalUnloadsCurrent() async throws {
        let mock = MockSpeechRecognizer()
        let service = SpeechService(recognizer: mock)
        try await service.loadFastModel()
        try await service.loadFinalModel()
        let role = await service.activeModelRole
        #expect(role == .asrFinal)
    }

    @Test("unloadCurrentModel clears state")
    func unload() async throws {
        let mock = MockSpeechRecognizer()
        let service = SpeechService(recognizer: mock)
        try await service.loadFastModel()
        await service.unloadCurrentModel()
        let ready = await service.isReady()
        #expect(!ready)
        let role = await service.activeModelRole
        #expect(role == nil)
    }
}

@Suite("SpeechService – Transcription")
struct SpeechServiceTranscriptionTests {

    @Test("Transcribe fails without loaded model")
    func transcribeNoModel() async {
        let mock = MockSpeechRecognizer()
        let service = SpeechService(recognizer: mock)
        let shard = makeShard()
        await #expect(throws: TranscriptEngineError.self) {
            try await service.transcribe(shard: shard)
        }
    }

    @Test("Transcribe returns segments with correct pass type (fast)")
    func transcribeFast() async throws {
        let mock = MockSpeechRecognizer()
        mock.transcribeResult = [makeSegment()]
        let service = SpeechService(recognizer: mock)
        try await service.loadFastModel()

        let shard = makeShard()
        let segments = try await service.transcribe(shard: shard)
        #expect(segments.count == 1)
        #expect(segments[0].passType == .fast)
    }

    @Test("Transcribe returns segments with correct pass type (final)")
    func transcribeFinal() async throws {
        let mock = MockSpeechRecognizer()
        mock.transcribeResult = [makeSegment()]
        let service = SpeechService(recognizer: mock)
        try await service.loadFinalModel()

        let shard = makeShard()
        let segments = try await service.transcribe(shard: shard)
        #expect(segments.count == 1)
        #expect(segments[0].passType == .final_)
    }

    @Test("Concurrent transcribe calls are serialized across await points")
    func concurrentTranscribesStaySerialized() async throws {
        let recognizer = ConcurrentProbeSpeechRecognizer()
        let service = SpeechService(recognizer: recognizer)
        try await service.loadFastModel()

        async let first = service.transcribe(
            shard: makeShard(id: 0, startTime: 0, duration: 30)
        )
        async let second = service.transcribe(
            shard: makeShard(id: 1, startTime: 30, duration: 30)
        )

        _ = try await first
        _ = try await second

        #expect(recognizer.transcribeCallCount == 2)
        #expect(
            recognizer.maxConcurrentTranscribes == 1,
            "SpeechService should not overlap recognizer transcribes while awaiting Apple Speech"
        )
    }
}

@Suite("SpeechService – VAD Integration")
struct SpeechServiceVADTests {

    @Test("transcribeWithVAD skips non-speech shards")
    func vadSkipsSilence() async throws {
        let mock = MockSpeechRecognizer()
        mock.vadResult = [VADResult(isSpeech: false, speechProbability: 0.1,
                                    startTime: 0, endTime: 30)]
        mock.transcribeResult = [makeSegment()]
        let service = SpeechService(recognizer: mock)
        try await service.loadFastModel()

        let shards = [makeShard(id: 0), makeShard(id: 1)]
        let segments = try await service.transcribeWithVAD(shards: shards)
        #expect(segments.isEmpty)
        #expect(mock.transcribeCallCount == 0)
    }

    @Test("transcribeWithVAD processes speech shards")
    func vadProcessesSpeech() async throws {
        let mock = MockSpeechRecognizer()
        mock.vadResult = [VADResult(isSpeech: true, speechProbability: 0.9,
                                    startTime: 0, endTime: 30)]
        mock.transcribeResult = [makeSegment()]
        let service = SpeechService(recognizer: mock)
        try await service.loadFastModel()

        let shards = [makeShard()]
        let segments = try await service.transcribeWithVAD(shards: shards)
        #expect(segments.count == 1)
        #expect(mock.transcribeCallCount == 1)
    }
}

// MARK: - StubSpeechRecognizer Tests

@Suite("StubSpeechRecognizer")
struct StubSpeechRecognizerTests {

    @Test("Stub loads and unloads")
    func stubLifecycle() async throws {
        let stub = StubSpeechRecognizer()
        #expect(await !stub.isModelLoaded())
        try await stub.loadModel()
        #expect(await stub.isModelLoaded())
        await stub.unloadModel()
        #expect(await !stub.isModelLoaded())
    }

    @Test("Stub transcribe returns empty when loaded")
    func stubTranscribe() async throws {
        let stub = StubSpeechRecognizer()
        try await stub.loadModel()
        let result = try await stub.transcribe(shard: makeShard(), podcastId: nil)
        #expect(result.isEmpty)
    }

    @Test("Stub throws when not loaded")
    func stubThrowsUnloaded() async {
        let stub = StubSpeechRecognizer()
        await #expect(throws: TranscriptEngineError.self) {
            try await stub.transcribe(shard: makeShard(), podcastId: nil)
        }
    }

    @Test("Stub VAD returns speech detected")
    func stubVAD() async throws {
        let stub = StubSpeechRecognizer()
        try await stub.loadModel()
        let results = try await stub.detectVoiceActivity(shard: makeShard())
        #expect(results.count == 1)
        #expect(results[0].isSpeech)
        #expect(results[0].speechProbability == 1.0)
    }
}

// MARK: - Transcript Types Tests

@Suite("Transcript Types")
struct TranscriptTypeTests {

    @Test("TranscriptWord equality")
    func wordEquality() {
        let a = TranscriptWord(text: "hello", startTime: 0, endTime: 0.5, confidence: 0.9)
        let b = TranscriptWord(text: "hello", startTime: 0, endTime: 0.5, confidence: 0.9)
        let c = TranscriptWord(text: "world", startTime: 0.5, endTime: 1.0, confidence: 0.8)
        #expect(a == b)
        #expect(a != c)
    }

    @Test("TranscriptSegment equality")
    func segmentEquality() {
        let seg1 = makeSegment(id: 0, passType: .fast)
        let seg2 = makeSegment(id: 0, passType: .fast)
        let seg3 = makeSegment(id: 1, passType: .final_)
        #expect(seg1 == seg2)
        #expect(seg1 != seg3)
    }

    @Test("TranscriptPassType raw values")
    func passTypeValues() {
        #expect(TranscriptPassType.fast.rawValue == "fast")
        #expect(TranscriptPassType.final_.rawValue == "final")
    }
}

// MARK: - ASRVocabularyProvider Tests

@Suite("ASRVocabularyProvider")
struct ASRVocabularyProviderTests {

    @Test("Compiled entries keep active sponsor knowledge ahead of podcast lexicon")
    func prioritizesActiveSponsorKnowledge() {
        let activeEntries = [
            SponsorKnowledgeEntry(
                podcastId: "pod-1",
                entityType: .sponsor,
                entityValue: "Alpha",
                state: .active,
                aliases: ["Alpha Plus"]
            ),
            SponsorKnowledgeEntry(
                podcastId: "pod-1",
                entityType: .sponsor,
                entityValue: "Beta",
                state: .active
            )
        ]

        let compiled = ASRVocabularyProvider.compiledEntries(
            activeSponsorEntries: activeEntries,
            sponsorLexicon: "Gamma, Beta, Delta"
        )

        #expect(compiled.map(\.text) == ["Alpha", "Alpha Plus", "Beta", "Gamma", "Delta"])
        #expect(compiled.map(\.source) == [
            .activeSponsorKnowledge,
            .activeSponsorKnowledge,
            .activeSponsorKnowledge,
            .podcastSponsorLexicon,
            .podcastSponsorLexicon
        ])
    }

    @Test("Domain lexicon terms expand to spoken URL templates")
    func parsesDomainTermsToSpokenTemplates() {
        let expanded = ASRVocabularyProvider.parseSponsorLexicon(
            "example.com, https://my-site.com/podcast"
        )

        #expect(expanded == [
            "example.com",
            "example dot com",
            "https://my-site.com/podcast",
            "my site dot com",
            "my site dot com slash podcast"
        ])
    }

    @Test("Empty inputs compile to an empty vocabulary")
    func emptyInputs() {
        let compiled = ASRVocabularyProvider.compiledEntries(
            activeSponsorEntries: [],
            sponsorLexicon: nil
        )
        #expect(compiled.isEmpty)
        #expect(ASRVocabularyProvider.parseSponsorLexicon(nil).isEmpty)
    }

    @Test("Store-backed provider builds ordered AnalysisContext contextual strings")
    func analysisContextWiring() async throws {
        let store = try await makeTestStore()
        try await store.upsertProfile(
            PodcastProfile(
                podcastId: "pod-1",
                sponsorLexicon: "Gamma, Delta",
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: 1.0,
                observationCount: 1,
                mode: "active",
                recentFalseSkipSignals: 0
            )
        )
        try await store.upsertKnowledgeEntry(
            SponsorKnowledgeEntry(
                podcastId: "pod-1",
                entityType: .sponsor,
                entityValue: "Alpha",
                state: .active,
                aliases: ["Alpha Plus"]
            )
        )
        try await store.upsertKnowledgeEntry(
            SponsorKnowledgeEntry(
                podcastId: "pod-1",
                entityType: .sponsor,
                entityValue: "Beta",
                state: .active
            )
        )

        let provider = ASRVocabularyProvider(store: store)
        let strings = await provider.contextualStrings(forPodcastId: "pod-1")
        #expect(strings == ["Alpha", "Alpha Plus", "Beta", "Gamma", "Delta"])

        #if canImport(Speech)
        if #available(iOS 26.0, *) {
            let context = await provider.analysisContext(forPodcastId: "pod-1")
            #expect(context?.contextualStrings[.general] == ["Alpha", "Alpha Plus", "Beta", "Gamma", "Delta"])
        }
        #endif
    }
}

// MARK: - Tracking Recognizer

/// Mock recognizer that records which shard IDs were transcribed.
private final class TrackingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private var loaded = false
    private let _shardIds = OSAllocatedUnfairLock(initialState: [Int]())
    var transcribedShardIds: [Int] { _shardIds.withLock { $0 } }

    func loadModel() async throws { loaded = true }
    func unloadModel() async { loaded = false }
    func isModelLoaded() async -> Bool { loaded }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard loaded else { throw TranscriptEngineError.modelNotLoaded }
        _shardIds.withLock { $0.append(shard.id) }

        let word = TranscriptWord(
            text: "shard\(shard.id)",
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration,
            confidence: 0.9
        )
        return [TranscriptSegment(
            id: shard.id,
            words: [word],
            text: "shard\(shard.id)",
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration,
            avgConfidence: 0.9,
            passType: .fast
        )]
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        [VADResult(isSpeech: true, speechProbability: 1.0,
                   startTime: shard.startTime,
                   endTime: shard.startTime + shard.duration)]
    }
}

// MARK: - Incremental Shard Append Regression Tests

@Suite("TranscriptEngine – Incremental Shard Append")
struct IncrementalShardAppendTests {

    @Test("appendShards processes new shards after initial loop completes", .timeLimit(.minutes(1)))
    func appendShardsAfterCompletion() async throws {
        let store = try await makeTestStore()
        let recognizer = TrackingRecognizer()
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)

        let initialShards = [
            makeShard(id: 0, startTime: 0, duration: 30),
            makeShard(id: 1, startTime: 30, duration: 30),
            makeShard(id: 2, startTime: 60, duration: 30),
        ]

        // Start transcription with initial shards and wait for completion.
        let events = await engine.events()
        await engine.startTranscription(
            shards: initialShards,
            analysisAssetId: "asset-1",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        // Signal end-of-input for the initial batch so `.completed`
        // fires once the backlog drains. The engine no longer emits
        // completion on a momentarily empty queue; see
        // `completedWaitsForFinishAppending`.
        await engine.finishAppending(analysisAssetId: "asset-1")

        // Wait for .completed event.
        for await event in events {
            if case .completed = event { break }
        }

        // Verify initial shards were transcribed.
        let initialCount = recognizer.transcribedShardIds.count
        #expect(initialCount >= 3, "Expected at least 3 shards transcribed, got \(initialCount)")

        // Subscribe to events before appending so we don't miss .completed.
        let appendEvents = await engine.events()

        // Now append new shards (simulating incremental download).
        let newShards = [
            makeShard(id: 10, startTime: 300, duration: 30),
            makeShard(id: 11, startTime: 330, duration: 30),
        ]
        await engine.appendShards(
            newShards,
            analysisAssetId: "asset-1",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        // Signal end-of-input for the appended batch.
        await engine.finishAppending(analysisAssetId: "asset-1")

        // Wait for the append loop to complete (it starts a new loop that emits .completed).
        for await event in appendEvents {
            if case .completed = event { break }
        }

        // The appended shards must have been transcribed.
        let allIds = recognizer.transcribedShardIds
        #expect(allIds.contains(10), "Shard 10 should have been transcribed after append")
        #expect(allIds.contains(11), "Shard 11 should have been transcribed after append")
    }

    /// Regression: the engine used to emit `.completed` as soon as
    /// `appendedShards` was momentarily empty, even when a streaming
    /// producer still had more shards to deliver. The resulting race
    /// caused `AnalysisCoordinator.finalizeBackfill` to run on partial
    /// coverage and mark the asset `.complete`.
    ///
    /// After the fix, `.completed` only fires once the caller
    /// explicitly calls `finishAppending(analysisAssetId:)` to signal
    /// that no more shards are coming.
    @Test("engine does not emit .completed until finishAppending is called", .timeLimit(.minutes(1)))
    func completedWaitsForFinishAppending() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTranscriptAsset(id: "asset-race", episodeId: "ep-race"))
        let recognizer = TrackingRecognizer()
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)
        // Use independent event subscriptions for the two windows. Each
        // `events()` call creates a distinct AsyncStream continuation
        // inside the engine, so cancelling one consumer does not drop
        // the other's pending events.
        let earlyEvents = await engine.events()

        // Single shard. Without a fix, the engine drains the backlog
        // and fires `.completed` unconditionally within a few ms.
        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-race",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )

        // Poll briefly — `.completed` must NOT fire in this window
        // because no finishAppending signal has been sent yet.
        let earlyCompletion = await firstCompletion(from: earlyEvents, within: .milliseconds(500))
        #expect(earlyCompletion == nil, "engine must not emit .completed before finishAppending")

        // Subscribe to a fresh event stream before sending end-of-input
        // so the completion event lands on a live consumer.
        let lateEvents = await engine.events()

        // Now signal end-of-input.
        await engine.finishAppending(analysisAssetId: "asset-race")

        // Completion should now fire within a generous timeout.
        let lateCompletion = await firstCompletion(from: lateEvents, within: .seconds(5))
        #expect(lateCompletion == "asset-race",
                "engine must emit .completed for asset-race after finishAppending")
    }
}

/// Consume the event stream until the first `.completed` event or the timeout
/// expires. Returns the completed asset ID, or nil if the deadline elapsed.
private func firstCompletion(
    from events: AsyncStream<TranscriptEngineEvent>,
    within duration: Duration
) async -> String? {
    await withTaskGroup(of: String?.self) { group in
        group.addTask {
            for await event in events {
                if case .completed(let assetId) = event {
                    return assetId
                }
            }
            return nil
        }
        group.addTask {
            try? await Task.sleep(for: duration)
            return nil
        }
        let first = await group.next() ?? nil
        group.cancelAll()
        return first
    }
}

@Suite("TranscriptEngine – Asset Switching")
struct TranscriptEngineAssetSwitchingTests {

    @Test("startTranscription resets per-asset chunk index on asset switch")
    func assetSwitchResetsChunkIndex() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTranscriptAsset(id: "asset-1", episodeId: "ep-1"))
        try await store.insertAsset(makeTranscriptAsset(id: "asset-2", episodeId: "ep-2"))
        let recognizer = TrackingRecognizer()
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)
        let firstEvents = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-1",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-1")

        for await event in firstEvents {
            if case .completed(let assetId) = event, assetId == "asset-1" { break }
        }

        let secondEvents = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 1, startTime: 0, duration: 30)],
            analysisAssetId: "asset-2",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-2")

        for await event in secondEvents {
            if case .completed(let assetId) = event, assetId == "asset-2" { break }
        }

        let asset1Chunks = try await store.fetchTranscriptChunks(assetId: "asset-1")
        let asset2Chunks = try await store.fetchTranscriptChunks(assetId: "asset-2")

        #expect(asset1Chunks.count == 1)
        #expect(asset2Chunks.count == 1)
        #expect(asset1Chunks[0].chunkIndex == 0)
        #expect(asset2Chunks[0].chunkIndex == 0, "new asset should not inherit chunk index from prior asset")
    }

    @Test("appendShards ignores stale asset after asset switch")
    func staleAppendIsDropped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTranscriptAsset(id: "asset-1", episodeId: "ep-1"))
        try await store.insertAsset(makeTranscriptAsset(id: "asset-2", episodeId: "ep-2"))
        let recognizer = TrackingRecognizer()
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-1",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-1")

        for await event in events {
            if case .completed(let assetId) = event, assetId == "asset-1" { break }
        }

        await engine.appendShards(
            [makeShard(id: 99, episodeID: "stale-ep", startTime: 300, duration: 30)],
            analysisAssetId: "asset-2",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )

        try await Task.sleep(for: .milliseconds(300))

        let allIds = recognizer.transcribedShardIds
        let asset2Chunks = try await store.fetchTranscriptChunks(assetId: "asset-2")

        #expect(!allIds.contains(99), "stale appended shard should be dropped")
        #expect(asset2Chunks.isEmpty, "no transcript should be written for stale asset append")
    }

    @Test("startTranscription persists weak-anchor metadata from transcript segments")
    func weakAnchorMetadataPersistsToChunks() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTranscriptAsset(id: "asset-weak", episodeId: "ep-weak"))

        let recognizer = MockSpeechRecognizer()
        let weakAnchorMetadata = makeWeakAnchorMetadata()
        recognizer.transcribeResult = [
            TranscriptSegment(
                id: 0,
                words: [
                    TranscriptWord(text: "visit", startTime: 0, endTime: 0.4, confidence: 0.9),
                    TranscriptWord(text: "betterhelp", startTime: 0.4, endTime: 0.8, confidence: 0.4),
                ],
                text: "visit betterhelp",
                startTime: 0,
                endTime: 0.8,
                avgConfidence: 0.65,
                passType: .fast,
                weakAnchorMetadata: weakAnchorMetadata
            ),
        ]
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-weak",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-weak")

        for await event in events {
            if case .completed(let assetId) = event, assetId == "asset-weak" { break }
        }

        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-weak")
        let chunk = try #require(chunks.first)
        #expect(chunk.weakAnchorMetadata == weakAnchorMetadata)
    }
}

// MARK: - Partial Result Promotion Tests (playhead-4ck)

/// Helper: creates an AsyncThrowingStream from an array of RecognitionSnapshots.
private func snapshotStream(
    _ snapshots: [RecognitionSnapshot]
) -> AsyncThrowingStream<RecognitionSnapshot, Error> {
    AsyncThrowingStream { continuation in
        for s in snapshots {
            continuation.yield(s)
        }
        continuation.finish()
    }
}

private func makeSnapshot(
    isFinal: Bool,
    text: String,
    startTime: TimeInterval = 0,
    endTime: TimeInterval = 1,
    confidence: Float = 0.9,
    weakAnchorMetadata: TranscriptWeakAnchorMetadata? = nil
) -> RecognitionSnapshot {
    let words = text.isEmpty ? [] : [
        TranscriptWord(text: text, startTime: startTime, endTime: endTime, confidence: confidence)
    ]
    return RecognitionSnapshot(
        isFinal: isFinal,
        text: text,
        words: words,
        startTime: startTime,
        endTime: endTime,
        weakAnchorMetadata: weakAnchorMetadata
    )
}

@Suite("TranscriptWeakAnchorMetadata")
struct TranscriptWeakAnchorMetadataTests {

    @Test("build captures distinct alternatives and grouped low-confidence spans")
    func buildCapturesAlternativesAndLowConfidenceSpans() throws {
        let metadata = try #require(TranscriptWeakAnchorMetadata.build(
            primaryText: "visit better halp dot com for details",
            words: [
                TranscriptWord(text: "visit", startTime: 0.0, endTime: 0.2, confidence: 0.95),
                TranscriptWord(text: "better", startTime: 0.2, endTime: 0.4, confidence: 0.42),
                TranscriptWord(text: "halp", startTime: 0.4, endTime: 0.6, confidence: 0.41),
                TranscriptWord(text: "dot", startTime: 0.6, endTime: 0.8, confidence: 0.40),
                TranscriptWord(text: "com", startTime: 0.8, endTime: 1.0, confidence: 0.39),
            ],
            alternatives: [
                "visit betterhelp.com/podcast for details",
                "visit better halp dot com for details",
            ],
            startTime: 0,
            endTime: 1
        ))

        #expect(abs(metadata.averageConfidence - 0.514) < 0.000_001)
        #expect(abs(metadata.minimumConfidence - 0.39) < 0.000_001)
        #expect(metadata.alternativeTexts.contains("visit betterhelp.com/podcast for details"))
        #expect(metadata.lowConfidencePhrases.contains {
            $0.text == "better halp dot com"
        })
    }
}

#if canImport(Speech)

private func makeAnalyzerStyleInt16Buffer(
    frameCount: AVAudioFrameCount = 16_000
) throws -> (buffer: AVAudioPCMBuffer, format: AVAudioFormat) {
    guard let format = AVAudioFormat(
        commonFormat: .pcmFormatInt16,
        sampleRate: 16_000,
        channels: 1,
        interleaved: false
    ) else {
        throw NSError(domain: "TranscriptEngineTests", code: 1, userInfo: [NSLocalizedDescriptionKey: "Failed to create analyzer test format"])
    }

    guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frameCount) else {
        throw NSError(domain: "TranscriptEngineTests", code: 2, userInfo: [NSLocalizedDescriptionKey: "Failed to allocate analyzer test buffer"])
    }

    buffer.frameLength = frameCount

    guard let channelData = buffer.int16ChannelData?.pointee else {
        throw NSError(domain: "TranscriptEngineTests", code: 3, userInfo: [NSLocalizedDescriptionKey: "Failed to access analyzer test channel data"])
    }

    for index in 0..<Int(frameCount) {
        channelData[index] = Int16(truncatingIfNeeded: (index % 2048) - 1024)
    }

    return (buffer, format)
}

@Suite("AppleSpeechAudioBridge")
struct AppleSpeechAudioBridgeTests {

    @Test("makeAnalyzerBuffer rejects silent shards at the audio bridge boundary")
    func makeAnalyzerBufferRejectsSilentShard() throws {
        let (_, targetFormat) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)

        #expect(throws: AppleSpeechBoundaryError.self) {
            try AppleSpeechAudioBridge.makeAnalyzerBuffer(from: makeShard(), targetFormat: targetFormat)
        }
    }

    @Test("makeAnalyzerBuffer rejects shards containing NaN samples")
    func makeAnalyzerBufferRejectsNaNSamples() throws {
        let (_, targetFormat) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)
        var samples = [Float](repeating: 0.5, count: 512)
        samples[100] = Float.nan

        let shard = AnalysisShard(id: 0, episodeID: "test", startTime: 0, duration: 0.032, samples: samples)
        #expect(throws: AppleSpeechBoundaryError.self) {
            try AppleSpeechAudioBridge.makeAnalyzerBuffer(from: shard, targetFormat: targetFormat)
        }
    }

    @Test("makeAnalyzerBuffer rejects shards containing Inf samples")
    func makeAnalyzerBufferRejectsInfSamples() throws {
        let (_, targetFormat) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)
        var samples = [Float](repeating: 0.5, count: 512)
        samples[200] = Float.infinity

        let shard = AnalysisShard(id: 0, episodeID: "test", startTime: 0, duration: 0.032, samples: samples)
        #expect(throws: AppleSpeechBoundaryError.self) {
            try AppleSpeechAudioBridge.makeAnalyzerBuffer(from: shard, targetFormat: targetFormat)
        }
    }
}

@Suite("AppleSpeechAnalyzerRunner")
struct AppleSpeechAnalyzerRunnerTests {

    @Test("single buffer analyzer input leaves timeline implicit")
    func singleBufferAnalyzerInputLeavesTimelineImplicit() throws {
        let (buffer, _) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)
        let input = try AppleSpeechAnalyzerRunner.makeAnalyzerInput(buffer: buffer)

        #expect(input.buffer === buffer)
        #expect(input.bufferStartTime == nil)
    }

    @Test("explicit buffer start time is preserved in analyzer input")
    func explicitBufferStartTimePreserved() throws {
        let (buffer, _) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)
        let startTime = CMTime(seconds: 5.0, preferredTimescale: 600_000)
        let input = try AppleSpeechAnalyzerRunner.makeAnalyzerInput(buffer: buffer, bufferStartTime: startTime)

        #expect(input.buffer === buffer)
        #expect(input.bufferStartTime == startTime)
    }

    @Test("timeline validator allows implicit analyzer inputs")
    func timelineValidatorAllowsImplicitAnalyzerInputs() throws {
        let (bufferA, _) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)
        let (bufferB, _) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)

        try AppleSpeechAnalyzerRunner.validateAnalyzerInputTimeline([
            try AppleSpeechAnalyzerRunner.makeAnalyzerInput(buffer: bufferA),
            try AppleSpeechAnalyzerRunner.makeAnalyzerInput(buffer: bufferB),
        ])
    }

    @Test("timeline validator rejects mixed implicit and explicit timestamps")
    func timelineValidatorRejectsMixedTimestampModes() throws {
        let (bufferA, _) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)
        let (bufferB, _) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)

        #expect(throws: AppleSpeechBoundaryError.self) {
            try AppleSpeechAnalyzerRunner.validateAnalyzerInputTimeline([
                try AppleSpeechAnalyzerRunner.makeAnalyzerInput(buffer: bufferA),
                try AppleSpeechAnalyzerRunner.makeAnalyzerInput(
                    buffer: bufferB,
                    bufferStartTime: CMTime(seconds: 0.032, preferredTimescale: 600_000)
                ),
            ])
        }
    }

    @Test("timeline validator rejects overlapping explicit timestamps")
    func timelineValidatorRejectsOverlappingExplicitTimestamps() throws {
        let (bufferA, _) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)
        let (bufferB, _) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)

        #expect(throws: AppleSpeechBoundaryError.self) {
            try AppleSpeechAnalyzerRunner.validateAnalyzerInputTimeline([
                try AppleSpeechAnalyzerRunner.makeAnalyzerInput(
                    buffer: bufferA,
                    bufferStartTime: CMTime(seconds: 0.0, preferredTimescale: 600_000)
                ),
                try AppleSpeechAnalyzerRunner.makeAnalyzerInput(
                    buffer: bufferB,
                    bufferStartTime: CMTime(seconds: 0.01, preferredTimescale: 600_000)
                ),
            ])
        }
    }

    @Test("timeline validator allows contiguous explicit timestamps")
    func timelineValidatorAllowsContiguousExplicitTimestamps() throws {
        let (bufferA, _) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)
        let (bufferB, _) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)
        let durationSeconds = Double(bufferA.frameLength) / bufferA.format.sampleRate

        try AppleSpeechAnalyzerRunner.validateAnalyzerInputTimeline([
            try AppleSpeechAnalyzerRunner.makeAnalyzerInput(
                buffer: bufferA,
                bufferStartTime: CMTime(seconds: 0.0, preferredTimescale: 600_000)
            ),
            try AppleSpeechAnalyzerRunner.makeAnalyzerInput(
                buffer: bufferB,
                bufferStartTime: CMTime(seconds: durationSeconds, preferredTimescale: 600_000)
            ),
        ])
    }

    @Test("makeAnalyzerInput rejects invalid CMTime before reaching Speech framework")
    func makeAnalyzerInputRejectsInvalidCMTime() throws {
        let (buffer, _) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)

        #expect(throws: AppleSpeechBoundaryError.self) {
            try AppleSpeechAnalyzerRunner.makeAnalyzerInput(
                buffer: buffer,
                bufferStartTime: CMTime.invalid
            )
        }
    }

    @Test("runner does not mix file-backed analyzer input with buffer-sequence analysis")
    func runnerAvoidsMixedAnalyzerInputModes() throws {
        let source = try appleSpeechAnalyzerRunnerSource()

        #expect(source.contains("SpeechAnalyzer(modules:"))
        #expect(!source.contains("inputAudioFile:"))
        #expect(!source.contains("makeAnalysisAudioFile("))
        #expect(!source.contains("bufferStartTime: .zero"))
    }

    // Regression guard for SFSpeechErrorDomain Code=16 cascade. If apply()
    // or prepare() throw outside the do/catch, cancelAndFinishNow() never
    // runs and the analyzer's session slot leaks, poisoning every
    // subsequent shard with "Maximum number of simultaneous requests reached".
    @Test("apply/prepare failures always reach cancelAndFinishNow cleanup")
    func runnerCleansUpAnalyzerOnApplyOrPrepareFailure() throws {
        let source = try appleSpeechAnalyzerRunnerSource()

        for funcName in ["func transcribe(", "func detectVoiceActivity("] {
            let funcStart = try #require(source.range(of: funcName))
            let funcBody = String(source[funcStart.lowerBound...])
            let doStart = try #require(funcBody.range(of: "do {"))

            if let prepareRange = funcBody.range(of: "try await prepare(analyzer:") {
                #expect(
                    prepareRange.lowerBound > doStart.lowerBound,
                    "\(funcName) calls prepare() before the do-block — a prepare failure would leak the SF session slot"
                )
            }
            if let applyRange = funcBody.range(of: "try await apply(context:") {
                #expect(
                    applyRange.lowerBound > doStart.lowerBound,
                    "\(funcName) calls apply() before the do-block — an apply failure would leak the SF session slot"
                )
            }
        }
    }

    // playhead-rfu-aac (cycle-3 M3) regression guard: if a collector Task
    // throws while pulling from `transcriber.results` / `detector.results`,
    // we still need cancelAndFinishNow() on the analyzer (the result stream
    // is what the collector is awaiting; if results never resolve the
    // analyzer would otherwise hang). The fix sets `needsCleanup = false`
    // ONLY after `try await collector!.value` resolves successfully — a
    // pre-await false-set lets the catch fall through with cleanup
    // skipped. We can't easily inject a thrown error into the real
    // SpeechAnalyzer result stream from a unit test (the collector is
    // constructed inline against concrete framework types), so this is a
    // structural source-grep guard mirroring the apply/prepare test above.
    @Test("collector-throw path always reaches cancelAndFinishNow cleanup")
    func runnerCleansUpAnalyzerOnCollectorThrow() throws {
        let source = try appleSpeechAnalyzerRunnerSource()

        for funcName in ["func transcribe(", "func detectVoiceActivity("] {
            let funcStart = try #require(source.range(of: funcName))
            let funcBody = String(source[funcStart.lowerBound...])

            let collectorAwait = try #require(
                funcBody.range(of: "try await collector!.value"),
                "\(funcName) no longer awaits the collector Task — review the cleanup-order guard"
            )
            let needsCleanupFalse = try #require(
                funcBody.range(of: "needsCleanup = false"),
                "\(funcName) no longer flips needsCleanup=false — review the cleanup-order guard"
            )

            #expect(
                needsCleanupFalse.lowerBound > collectorAwait.lowerBound,
                "\(funcName) sets needsCleanup=false BEFORE awaiting collector.value; a throwing collector would skip cancelAndFinishNow and the analyzer would hang on its result stream"
            )
        }
    }
}

@Suite("SpeechTranscriber extraction")
struct SpeechTranscriberExtractionTests {

    @Test("configured SpeechTranscriber preset requests alternatives and confidence attributes")
    func configuredPresetRequestsWeakAnchorSignals() {
        let base = SpeechTranscriber.Preset.timeIndexedProgressiveTranscription
        let preset = AppleSpeechResultMapper.speechTranscriberPreset()

        #expect(preset.transcriptionOptions == base.transcriptionOptions)
        #expect(preset.reportingOptions.isSuperset(of: base.reportingOptions))
        #expect(preset.reportingOptions.contains(.alternativeTranscriptions))
        #expect(preset.attributeOptions.isSuperset(of: base.attributeOptions))
        #expect(preset.attributeOptions.contains(.transcriptionConfidence))
    }

    @Test("extractWords maps Speech alternatives and run-level weak-anchor timing")
    func extractWordsMapsAlternativesAndWeakAnchorTiming() throws {
        let primaryText = makeSpeechAttributedString([
            ("visit ", 1.0, 1.4, 0.96),
            ("better halp dot com", 1.4, 3.8, 0.34),
        ])
        let extracted = AppleSpeechResultMapper.extractWords(
            from: primaryText,
            alternatives: [
                AttributedString("visit betterhelp.com/podcast"),
                AttributedString("visit better halp dot com"),
            ],
            fallbackStart: 10.0,
            fallbackEnd: 12.0
        )

        #expect(extracted.words.map(\.text) == ["visit", "better", "halp", "dot", "com"])
        #expect(abs(extracted.words[0].startTime - 1.0) < 0.000_001)
        #expect(abs(extracted.words[1].startTime - 1.4) < 0.000_001)
        #expect(abs(extracted.words[4].endTime - 3.8) < 0.000_001)
        #expect(abs(extracted.words[1].confidence - 0.34) < 0.000_001)

        let metadata = try #require(extracted.weakAnchorMetadata)
        #expect(metadata.alternativeTexts == ["visit betterhelp.com/podcast"])
        #expect(abs(metadata.averageConfidence - 0.464) < 0.000_001)
        #expect(abs(metadata.minimumConfidence - 0.34) < 0.000_001)
        #expect(metadata.lowConfidencePhrases.count == 1)
        #expect(metadata.lowConfidencePhrases[0].text == "better halp dot com")
        #expect(abs(metadata.lowConfidencePhrases[0].startTime - 1.4) < 0.000_001)
        #expect(abs(metadata.lowConfidencePhrases[0].endTime - 3.8) < 0.000_001)
        #expect(abs(metadata.lowConfidencePhrases[0].confidence - 0.34) < 0.000_001)
    }

    @Test("extracted weak-anchor metadata can be offset to shard-relative episode time")
    func extractedWeakAnchorMetadataOffsetsForShardStart() throws {
        let primaryText = makeSpeechAttributedString([
            ("visit ", 0.0, 0.2, 0.95),
            ("better help", 0.2, 0.8, 0.19),
        ])
        let extracted = AppleSpeechResultMapper.extractWords(
            from: primaryText,
            alternatives: [AttributedString("visit betterhelp dot com")],
            fallbackStart: 0.0,
            fallbackEnd: 1.0
        )

        let offsetMetadata = try #require(extracted.weakAnchorMetadata?.offsettingTimes(by: 30.0))
        let phrase = try #require(offsetMetadata.lowConfidencePhrases.first)
        #expect(offsetMetadata.alternativeTexts == ["visit betterhelp dot com"])
        #expect(abs(phrase.startTime - 30.2) < 0.000_001)
        #expect(abs(phrase.endTime - 30.8) < 0.000_001)
    }
}

@Suite("collectSegmentsFromSnapshots – Partial Promotion")
struct CollectSegmentsPartialPromotionTests {

    @Test("Final results are collected normally")
    func finalResultsCollected() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: true, text: "Hello world", startTime: 0, endTime: 1),
            makeSnapshot(isFinal: true, text: "Second segment", startTime: 1, endTime: 2),
        ])
        let segments = try await AppleSpeechResultMapper.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 2)
        #expect(segments[0].text == "Hello world")
        #expect(segments[1].text == "Second segment")
    }

    @Test("Trailing partial is promoted when stream ends without final")
    func trailingPartialPromoted() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: true, text: "First sentence", startTime: 0, endTime: 1),
            makeSnapshot(isFinal: false, text: "Trailing partial", startTime: 1, endTime: 2),
        ])
        let segments = try await AppleSpeechResultMapper.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 2, "Trailing partial should be promoted to a segment")
        #expect(segments[1].text == "Trailing partial")
    }

    @Test("Partial superseded by final is not duplicated")
    func partialSupersededByFinal() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: false, text: "Partial attempt", startTime: 0, endTime: 1),
            makeSnapshot(isFinal: true, text: "Final version", startTime: 0, endTime: 1),
        ])
        let segments = try await AppleSpeechResultMapper.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 1, "Superseded partial must not be double-counted")
        #expect(segments[0].text == "Final version")
    }

    @Test("Only the latest partial is promoted (earlier partials are overwritten)")
    func onlyLatestPartialPromoted() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: true, text: "Finalized", startTime: 0, endTime: 1),
            makeSnapshot(isFinal: false, text: "Partial v1", startTime: 1, endTime: 2),
            makeSnapshot(isFinal: false, text: "Partial v2", startTime: 1, endTime: 2.5),
        ])
        let segments = try await AppleSpeechResultMapper.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 2)
        #expect(segments[1].text == "Partial v2", "Only the latest partial should be promoted")
    }

    @Test("Empty partial is not promoted")
    func emptyPartialNotPromoted() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: true, text: "Content", startTime: 0, endTime: 1),
            makeSnapshot(isFinal: false, text: "", startTime: 1, endTime: 2),
        ])
        let segments = try await AppleSpeechResultMapper.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 1, "Empty trailing partial should not be promoted")
    }

    @Test("All-partial stream promotes only the last one")
    func allPartialStream() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: false, text: "Partial 1", startTime: 0, endTime: 1),
            makeSnapshot(isFinal: false, text: "Partial 2", startTime: 0, endTime: 1.5),
            makeSnapshot(isFinal: false, text: "Partial 3", startTime: 0, endTime: 2),
        ])
        let segments = try await AppleSpeechResultMapper.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 1, "Only the last partial should survive")
        #expect(segments[0].text == "Partial 3")
    }

    @Test("Segments are sorted by startTime after partial promotion")
    func sortedAfterPromotion() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: true, text: "Later segment", startTime: 5, endTime: 6),
            makeSnapshot(isFinal: false, text: "Earlier partial", startTime: 2, endTime: 3),
        ])
        let segments = try await AppleSpeechResultMapper.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 2)
        #expect(segments[0].text == "Earlier partial", "Promoted partial should be sorted by startTime")
        #expect(segments[1].text == "Later segment")
    }

    @Test("Empty stream produces no segments")
    func emptyStream() async throws {
        let stream = snapshotStream([])
        let segments = try await AppleSpeechResultMapper.collectSegmentsFromSnapshots(stream)
        #expect(segments.isEmpty)
    }

    @Test("Empty final clears a preceding partial")
    func emptyFinalClearsPartial() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: false, text: "Speculative", startTime: 0, endTime: 1),
            makeSnapshot(isFinal: true, text: "", startTime: 0, endTime: 1),
        ])
        let segments = try await AppleSpeechResultMapper.collectSegmentsFromSnapshots(stream)
        #expect(segments.isEmpty, "Empty final retracts the preceding partial")
    }

    @Test("Multi-shard sequence: final, partial, final, trailing partial")
    func multiShardSequence() async throws {
        let stream = snapshotStream([
            // Shard 1: partial then final
            makeSnapshot(isFinal: false, text: "Hel", startTime: 0, endTime: 0.5),
            makeSnapshot(isFinal: true, text: "Hello world", startTime: 0, endTime: 1),
            // Shard 2: partial then final
            makeSnapshot(isFinal: false, text: "How", startTime: 1, endTime: 1.5),
            makeSnapshot(isFinal: true, text: "How are you", startTime: 1, endTime: 2),
            // Shard 3: only a trailing partial (shard boundary truncation)
            makeSnapshot(isFinal: false, text: "I am fi", startTime: 2, endTime: 2.5),
        ])
        let segments = try await AppleSpeechResultMapper.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 3, "Two finals + one promoted trailing partial")
        #expect(segments[0].text == "Hello world")
        #expect(segments[1].text == "How are you")
        #expect(segments[2].text == "I am fi", "Trailing partial from shard 3 should be promoted")
    }

    @Test("weak-anchor metadata survives snapshot promotion into transcript segments")
    func weakAnchorMetadataSurvivesSnapshotPromotion() async throws {
        let metadata = makeWeakAnchorMetadata()
        let stream = snapshotStream([
            makeSnapshot(
                isFinal: true,
                text: "sponsored by betterhelp",
                startTime: 0,
                endTime: 1,
                confidence: 0.19,
                weakAnchorMetadata: metadata
            )
        ])

        let segments = try await AppleSpeechResultMapper.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 1)
        #expect(segments[0].weakAnchorMetadata == metadata)
    }
}

@Suite("AppleSpeechResultMapper – shard offset translation")
struct AppleSpeechResultMapperOffsetTests {

    @Test("offsetSegments shifts segment, word, and weak-anchor times by shard delta")
    func offsetSegmentsShiftsAllTimes() throws {
        let metadata = TranscriptWeakAnchorMetadata(
            averageConfidence: 0.46,
            minimumConfidence: 0.19,
            alternativeTexts: ["sponsored by betterhelp"],
            lowConfidencePhrases: [
                WeakAnchorPhrase(
                    text: "better help",
                    startTime: 0.2,
                    endTime: 0.4,
                    confidence: 0.19
                )
            ]
        )
        let segment = TranscriptSegment(
            id: 7,
            words: [
                TranscriptWord(text: "visit", startTime: 0.0, endTime: 0.2, confidence: 0.95),
                TranscriptWord(text: "betterhelp", startTime: 0.2, endTime: 0.6, confidence: 0.30),
            ],
            text: "visit betterhelp",
            startTime: 0.0,
            endTime: 0.6,
            avgConfidence: 0.625,
            passType: .fast,
            weakAnchorMetadata: metadata
        )

        let offset = AppleSpeechResultMapper.offsetSegments([segment], by: 30.0)

        #expect(offset.count == 1)
        let shifted = try #require(offset.first)
        #expect(shifted.id == 7)
        #expect(shifted.text == "visit betterhelp")
        #expect(shifted.passType == .fast)
        #expect(abs(shifted.avgConfidence - 0.625) < 0.000_001)
        #expect(abs(shifted.startTime - 30.0) < 0.000_001)
        #expect(abs(shifted.endTime - 30.6) < 0.000_001)
        #expect(shifted.words.count == 2)
        #expect(abs(shifted.words[0].startTime - 30.0) < 0.000_001)
        #expect(abs(shifted.words[0].endTime - 30.2) < 0.000_001)
        #expect(abs(shifted.words[1].startTime - 30.2) < 0.000_001)
        #expect(abs(shifted.words[1].endTime - 30.6) < 0.000_001)
        #expect(shifted.words[0].text == "visit")
        #expect(shifted.words[1].text == "betterhelp")
        #expect(abs(shifted.words[1].confidence - 0.30) < 0.000_001)

        let shiftedMeta = try #require(shifted.weakAnchorMetadata)
        let phrase = try #require(shiftedMeta.lowConfidencePhrases.first)
        #expect(abs(phrase.startTime - 30.2) < 0.000_001)
        #expect(abs(phrase.endTime - 30.4) < 0.000_001)
        #expect(shiftedMeta.alternativeTexts == metadata.alternativeTexts)
    }

    @Test("offsetSegments with zero delta is a value-identity transform")
    func offsetSegmentsZeroDeltaPreservesValues() {
        let segment = TranscriptSegment(
            id: 0,
            words: [TranscriptWord(text: "hello", startTime: 1.0, endTime: 1.5, confidence: 0.9)],
            text: "hello",
            startTime: 1.0,
            endTime: 1.5,
            avgConfidence: 0.9,
            passType: .final_
        )
        let offset = AppleSpeechResultMapper.offsetSegments([segment], by: 0)

        #expect(offset == [segment])
    }

    @Test("offsetSegments shifts segment, word, and weak-anchor times by negative delta")
    func offsetSegmentsShiftsAllTimesByNegativeDelta() throws {
        let metadata = TranscriptWeakAnchorMetadata(
            averageConfidence: 0.46,
            minimumConfidence: 0.19,
            alternativeTexts: ["sponsored by betterhelp"],
            lowConfidencePhrases: [
                WeakAnchorPhrase(
                    text: "better help",
                    startTime: 5.2,
                    endTime: 5.4,
                    confidence: 0.19
                )
            ]
        )
        let segment = TranscriptSegment(
            id: 7,
            words: [
                TranscriptWord(text: "visit", startTime: 5.0, endTime: 5.2, confidence: 0.95),
                TranscriptWord(text: "betterhelp", startTime: 5.2, endTime: 5.6, confidence: 0.30),
            ],
            text: "visit betterhelp",
            startTime: 5.0,
            endTime: 5.6,
            avgConfidence: 0.625,
            passType: .fast,
            weakAnchorMetadata: metadata
        )

        let offset = AppleSpeechResultMapper.offsetSegments([segment], by: -3.0)

        #expect(offset.count == 1)
        let shifted = try #require(offset.first)
        #expect(shifted.id == 7)
        #expect(shifted.text == "visit betterhelp")
        #expect(shifted.passType == .fast)
        #expect(abs(shifted.avgConfidence - 0.625) < 0.000_001)
        #expect(abs(shifted.startTime - 2.0) < 0.000_001)
        #expect(abs(shifted.endTime - 2.6) < 0.000_001)
        #expect(shifted.words.count == 2)
        #expect(abs(shifted.words[0].startTime - 2.0) < 0.000_001)
        #expect(abs(shifted.words[0].endTime - 2.2) < 0.000_001)
        #expect(abs(shifted.words[1].startTime - 2.2) < 0.000_001)
        #expect(abs(shifted.words[1].endTime - 2.6) < 0.000_001)
        #expect(shifted.words[0].text == "visit")
        #expect(shifted.words[1].text == "betterhelp")
        #expect(abs(shifted.words[1].confidence - 0.30) < 0.000_001)

        let shiftedMeta = try #require(shifted.weakAnchorMetadata)
        let phrase = try #require(shiftedMeta.lowConfidencePhrases.first)
        #expect(abs(phrase.startTime - 2.2) < 0.000_001)
        #expect(abs(phrase.endTime - 2.4) < 0.000_001)
        #expect(shiftedMeta.alternativeTexts == metadata.alternativeTexts)
    }

    @Test("offsetSegments preserves nil weakAnchorMetadata")
    func offsetSegmentsPreservesNilMetadata() throws {
        let segment = TranscriptSegment(
            id: 0,
            words: [TranscriptWord(text: "hi", startTime: 0, endTime: 0.1, confidence: 1.0)],
            text: "hi",
            startTime: 0,
            endTime: 0.1,
            avgConfidence: 1.0,
            passType: .fast,
            weakAnchorMetadata: nil
        )
        let offset = AppleSpeechResultMapper.offsetSegments([segment], by: 5.0)
        let shifted = try #require(offset.first)

        #expect(shifted.weakAnchorMetadata == nil)
    }

    @Test("offsetVADResults shifts startTime and endTime by delta")
    func offsetVADResultsShiftsTimestamps() {
        let results = [
            VADResult(isSpeech: true, speechProbability: 1.0, startTime: 0.0, endTime: 0.5),
            VADResult(isSpeech: false, speechProbability: 0.1, startTime: 1.0, endTime: 2.0),
        ]

        let offset = AppleSpeechResultMapper.offsetVADResults(results, by: 12.5)

        #expect(offset.count == 2)
        #expect(abs(offset[0].startTime - 12.5) < 0.000_001)
        #expect(abs(offset[0].endTime - 13.0) < 0.000_001)
        #expect(offset[0].isSpeech == true)
        #expect(abs(offset[0].speechProbability - 1.0) < 0.000_001)
        #expect(abs(offset[1].startTime - 13.5) < 0.000_001)
        #expect(abs(offset[1].endTime - 14.5) < 0.000_001)
        #expect(offset[1].isSpeech == false)
        #expect(abs(offset[1].speechProbability - 0.1) < 0.000_001)
    }

    @Test("offsetVADResults on empty input returns empty")
    func offsetVADResultsEmptyInputEmptyOutput() {
        let offset = AppleSpeechResultMapper.offsetVADResults([], by: 100.0)
        #expect(offset.isEmpty)
    }
}

@Suite("AppleSpeechBoundaryError – diagnostic descriptions")
struct AppleSpeechBoundaryErrorDescriptionTests {

    @Test("speechAssetsUnsupported description embeds the locale identifier")
    func speechAssetsUnsupportedDescription() {
        let error = AppleSpeechBoundaryError.speechAssetsUnsupported(localeIdentifier: "en-US")
        #expect(error.description == "Speech assets unsupported for en-US")
    }

    @Test("analyzerFormatUnavailable description embeds the locale identifier")
    func analyzerFormatUnavailableDescription() {
        let error = AppleSpeechBoundaryError.analyzerFormatUnavailable(localeIdentifier: "en-GB")
        #expect(error.description == "SpeechAnalyzer did not negotiate a usable audio format for en-GB")
    }

    @Test("audioBridgeFailure description passes through reason verbatim")
    func audioBridgeFailureDescriptionPassthrough() {
        let reason = "shard 4 contains 3 NaN and 0 Inf samples"
        let error = AppleSpeechBoundaryError.audioBridgeFailure(reason)
        #expect(error.description == reason)
    }

    @Test("invalidAnalyzerInputTimeline description passes through reason verbatim")
    func invalidAnalyzerInputTimelineDescriptionPassthrough() {
        let reason = "SpeechAnalyzer input buffer timestamps overlap or precede prior audio input"
        let error = AppleSpeechBoundaryError.invalidAnalyzerInputTimeline(reason)
        #expect(error.description == reason)
    }

    @Test("analyzerSessionFailure description passes through reason verbatim")
    func analyzerSessionFailureDescriptionPassthrough() {
        let reason = "SpeechAnalyzer prepare failed: simulated"
        let error = AppleSpeechBoundaryError.analyzerSessionFailure(reason)
        #expect(error.description == reason)
    }
}

@Suite("AppleSpeechAudioBridge – happy-path conversion")
struct AppleSpeechAudioBridgeHappyPathTests {

    @Test("makeAnalyzerBuffer produces a buffer matching the requested target format")
    func makeAnalyzerBufferReturnsTargetFormat() throws {
        let (_, targetFormat) = try makeAnalyzerStyleInt16Buffer(frameCount: 512)
        let samples = (0..<16_000).map { Float(sin(Double($0) * 0.05)) * 0.5 }
        let shard = AnalysisShard(
            id: 0,
            episodeID: "test-ep",
            startTime: 0,
            duration: 1.0,
            samples: samples
        )

        let buffer = try AppleSpeechAudioBridge.makeAnalyzerBuffer(
            from: shard,
            targetFormat: targetFormat
        )

        // Source is 16,000 Float32 samples at 16 kHz; converted Int16 buffer at
        // 16 kHz target should preserve the exact frame count (no rate change).
        // A broken converter emitting a single frame must fail this assertion.
        #expect(buffer.format == targetFormat)
        #expect(buffer.frameLength == 16_000)
    }

    @Test("makeAnalyzerBuffer passes Float32 16 kHz audio through when source matches target")
    func makeAnalyzerBufferPassthroughWhenSourceMatchesTarget() throws {
        guard let targetFormat = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: 16_000,
            channels: 1,
            interleaved: false
        ) else {
            Issue.record("Failed to allocate Float32 16kHz target format")
            return
        }

        // Sentinel pattern: a recognizable alternating sequence at known
        // indices so a broken implementation returning a zero-filled or
        // garbage buffer of the right shape can't pass.
        let frameCount = 16_000
        var samples = (0..<frameCount).map { Float(sin(Double($0) * 0.05)) * 0.5 }
        let sentinelIndices = [0, 1, 2, 3, 100, 1_000, 8_000, 15_999]
        let sentinelValues: [Float] = [0.125, -0.25, 0.375, -0.5, 0.625, -0.75, 0.875, -0.9375]
        for (i, index) in sentinelIndices.enumerated() {
            samples[index] = sentinelValues[i]
        }
        let shard = AnalysisShard(
            id: 0,
            episodeID: "test-ep",
            startTime: 0,
            duration: 1.0,
            samples: samples
        )

        let buffer = try AppleSpeechAudioBridge.makeAnalyzerBuffer(
            from: shard,
            targetFormat: targetFormat
        )

        // Source already matches target → no resampling, exact frame count preserved.
        #expect(buffer.format == targetFormat)
        #expect(Int(buffer.frameLength) == frameCount)

        // Pin passthrough fidelity: the sentinel values must round-trip
        // verbatim through the bridge. A zero-filled or otherwise garbage
        // buffer would diverge here.
        let channelData = try #require(buffer.floatChannelData?.pointee)
        for (i, index) in sentinelIndices.enumerated() {
            #expect(
                abs(channelData[index] - sentinelValues[i]) < 0.000_001,
                "Sample at index \(index) should pass through verbatim (expected \(sentinelValues[i]), got \(channelData[index]))"
            )
        }
    }
}

#endif

@Suite("TranscriptEngineService – Weak Anchor Metadata")
struct TranscriptEngineWeakAnchorMetadataTests {

    @Test("transcribed segment metadata persists into transcript chunks")
    func weakAnchorMetadataPersistsIntoChunks() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTranscriptAsset(id: "asset-weak-anchor", episodeId: "ep-weak-anchor"))

        let mock = MockSpeechRecognizer()
        mock.transcribeResult = [
            TranscriptSegment(
                id: 0,
                words: [
                    TranscriptWord(text: "hello", startTime: 0, endTime: 0.5, confidence: 0.19)
                ],
                text: "hello",
                startTime: 0,
                endTime: 0.5,
                avgConfidence: 0.19,
                passType: .fast,
                weakAnchorMetadata: makeWeakAnchorMetadata()
            )
        ]

        let speech = SpeechService(recognizer: mock)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-weak-anchor",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-weak-anchor")

        for await event in events {
            if case .completed(let assetId) = event, assetId == "asset-weak-anchor" {
                break
            }
        }

        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-weak-anchor")
        #expect(chunks.count == 1)
        #expect(chunks[0].weakAnchorMetadata == makeWeakAnchorMetadata())
    }

    @Test("duplicate fingerprints upgrade persisted weak-anchor metadata and re-emit the chunk")
    func duplicateFingerprintUpgradesWeakAnchorMetadata() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTranscriptAsset(id: "asset-weak-upgrade", episodeId: "ep-weak-upgrade"))

        let initialMetadata = TranscriptWeakAnchorMetadata(
            averageConfidence: 0.22,
            minimumConfidence: 0.22,
            alternativeTexts: [],
            lowConfidencePhrases: []
        )
        let upgradedMetadata = makeWeakAnchorMetadata()

        let mock = MockSpeechRecognizer()
        mock.transcribeResult = [
            TranscriptSegment(
                id: 0,
                words: [
                    TranscriptWord(text: "hello", startTime: 0, endTime: 0.5, confidence: 0.22)
                ],
                text: "hello",
                startTime: 0,
                endTime: 0.5,
                avgConfidence: 0.22,
                passType: .fast,
                weakAnchorMetadata: initialMetadata
            )
        ]

        let speech = SpeechService(recognizer: mock)
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)

        let firstEvents = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-weak-upgrade",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-weak-upgrade")
        for await event in firstEvents {
            if case .completed(let assetId) = event, assetId == "asset-weak-upgrade" {
                break
            }
        }

        mock.transcribeResult = [
            TranscriptSegment(
                id: 0,
                words: [
                    TranscriptWord(text: "hello", startTime: 0, endTime: 0.5, confidence: 0.19)
                ],
                text: "hello",
                startTime: 0,
                endTime: 0.5,
                avgConfidence: 0.19,
                passType: .fast,
                weakAnchorMetadata: upgradedMetadata
            )
        ]

        let secondEvents = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-weak-upgrade",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-weak-upgrade")

        var upgradedEventChunks: [TranscriptChunk] = []
        var sawCompletion = false
        for await event in secondEvents {
            switch event {
            case .chunksPersisted(let assetId, let chunks) where assetId == "asset-weak-upgrade":
                upgradedEventChunks = chunks
            case .completed(let assetId) where assetId == "asset-weak-upgrade":
                sawCompletion = true
            default:
                continue
            }
            if !upgradedEventChunks.isEmpty || sawCompletion {
                break
            }
        }

        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-weak-upgrade")
        #expect(chunks.count == 1)
        #expect(chunks[0].weakAnchorMetadata == upgradedMetadata)
        #expect(upgradedEventChunks.count == 1)
        #expect(upgradedEventChunks[0].id == chunks[0].id)
        #expect(upgradedEventChunks[0].weakAnchorMetadata == upgradedMetadata)
    }

    @Test("duplicate fingerprints do not replace richer weak-anchor metadata with poorer payloads")
    func duplicateFingerprintDoesNotDowngradeWeakAnchorMetadata() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTranscriptAsset(id: "asset-weak-downgrade", episodeId: "ep-weak-downgrade"))

        let richMetadata = TranscriptWeakAnchorMetadata(
            averageConfidence: 0.19,
            minimumConfidence: 0.12,
            alternativeTexts: [
                "visit betterhelp.com/podcast",
                "use code save10",
            ],
            lowConfidencePhrases: [
                WeakAnchorPhrase(
                    text: "better help",
                    startTime: 0.2,
                    endTime: 0.6,
                    confidence: 0.12
                )
            ]
        )
        let poorerMetadata = TranscriptWeakAnchorMetadata(
            averageConfidence: 0.42,
            minimumConfidence: 0.42,
            alternativeTexts: ["visit betterhelp.com/podcast"],
            lowConfidencePhrases: []
        )

        let mock = MockSpeechRecognizer()
        mock.transcribeResult = [
            TranscriptSegment(
                id: 0,
                words: [
                    TranscriptWord(text: "hello", startTime: 0, endTime: 0.5, confidence: 0.19)
                ],
                text: "hello",
                startTime: 0,
                endTime: 0.5,
                avgConfidence: 0.19,
                passType: .fast,
                weakAnchorMetadata: richMetadata
            )
        ]

        let speech = SpeechService(recognizer: mock)
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)

        let firstEvents = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-weak-downgrade",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-weak-downgrade")
        for await event in firstEvents {
            if case .completed(let assetId) = event, assetId == "asset-weak-downgrade" {
                break
            }
        }

        mock.transcribeResult = [
            TranscriptSegment(
                id: 0,
                words: [
                    TranscriptWord(text: "hello", startTime: 0, endTime: 0.5, confidence: 0.42)
                ],
                text: "hello",
                startTime: 0,
                endTime: 0.5,
                avgConfidence: 0.42,
                passType: .fast,
                weakAnchorMetadata: poorerMetadata
            )
        ]

        let secondEvents = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-weak-downgrade",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-weak-downgrade")

        var sawPersistedChunk = false
        for await event in secondEvents {
            switch event {
            case .chunksPersisted(let assetId, _) where assetId == "asset-weak-downgrade":
                sawPersistedChunk = true
            case .completed(let assetId) where assetId == "asset-weak-downgrade":
                break
            default:
                continue
            }
            break
        }

        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-weak-downgrade")
        #expect(chunks.count == 1)
        #expect(chunks[0].weakAnchorMetadata == richMetadata)
        #expect(!sawPersistedChunk)
    }
}

// MARK: - StreamingAudioDecoder Tests

@Suite("StreamingAudioDecoder")
struct StreamingAudioDecoderTests {

    @Test("No shards emitted when data is below format detection threshold")
    func belowDetectionThreshold() async {
        let decoder = StreamingAudioDecoder(episodeID: "test", shardDuration: 30.0, contentType: "mp3")
        let stream = await decoder.shards()

        // Feed less than 16KB (minimum for format detection)
        await decoder.feedData(Data(count: 1000))
        await decoder.finish()

        var shards: [AnalysisShard] = []
        for await shard in stream {
            shards.append(shard)
        }
        #expect(shards.isEmpty, "No shards should be emitted from insufficient data")
    }

    @Test("finish() terminates the shard stream")
    func finishTerminatesStream() async {
        let decoder = StreamingAudioDecoder(episodeID: "test", shardDuration: 30.0)
        let stream = await decoder.shards()

        await decoder.finish()

        var count = 0
        for await _ in stream {
            count += 1
        }
        // Stream should have terminated (loop exits)
        #expect(count == 0)
    }

    @Test("cleanup() removes temporary file")
    func cleanupRemovesTempFile() async {
        let decoder = StreamingAudioDecoder(episodeID: "test", shardDuration: 30.0)

        // Feed some data to create the temp file
        await decoder.feedData(Data(repeating: 0xFF, count: 100))
        await decoder.cleanup()

        // The temp file should be gone. We can't easily check the path
        // since it's private, but cleanup should not crash.
    }

    @Test("shards() can be called once without crash")
    func shardsCalledOncePrecondition() async {
        let decoder = StreamingAudioDecoder(episodeID: "test", shardDuration: 30.0)
        _ = await decoder.shards()
        // Second call would hit precondition failure.
        // We can't test precondition failures directly in Swift Testing,
        // so just verify the first call works.
    }

    @Test("Supports common podcast content types")
    func contentTypeMapping() async {
        // These should all initialize without crashing
        let types = ["mp3", "m4a", "audio/mpeg", "audio/mp4", "audio/aac", "audio/wav"]
        for type in types {
            let decoder = StreamingAudioDecoder(episodeID: "test", contentType: type)
            await decoder.cleanup()
        }
    }

    @Test("Decodes WAV data into shards")
    func decodesWAVData() async throws {
        // Use 1-second shards for fast testing
        let decoder = StreamingAudioDecoder(episodeID: "test", shardDuration: 1.0, contentType: "wav")
        let stream = await decoder.shards()

        // Create a minimal 16kHz mono 16-bit WAV with 3 seconds of silence
        let wavData = makeWAVData(seconds: 3)

        // Feed all at once
        await decoder.feedData(wavData)
        await decoder.finish()

        var shards: [AnalysisShard] = []
        for await shard in stream {
            shards.append(shard)
        }

        // With 3 seconds of 16kHz audio and 1-second shards, expect 3 shards.
        // The converter may produce slightly different counts due to resampling,
        // but we should get at least 2 shards.
        #expect(shards.count >= 2, "Expected at least 2 shards from 3 seconds of WAV, got \(shards.count)")

        // Verify shard properties
        if let first = shards.first {
            #expect(first.id == 0)
            #expect(first.episodeID == "test")
            #expect(first.startTime == 0)
            #expect(first.duration > 0.5, "First shard should be ~1 second")
        }

        // Verify shards are in order
        for i in 1..<shards.count {
            #expect(shards[i].startTime > shards[i-1].startTime, "Shards should be time-ordered")
        }

        await decoder.cleanup()
    }

    @Test("Incremental feed produces shards like bulk feed")
    func incrementalVsBulkFeed() async throws {
        let wavData = makeWAVData(seconds: 3)

        // Feed incrementally in 4KB chunks
        let decoder = StreamingAudioDecoder(episodeID: "test", shardDuration: 1.0, contentType: "wav")
        let stream = await decoder.shards()

        let chunkSize = 4096
        var offset = 0
        while offset < wavData.count {
            let end = min(offset + chunkSize, wavData.count)
            await decoder.feedData(wavData[offset..<end])
            offset = end
        }
        await decoder.finish()

        var shards: [AnalysisShard] = []
        for await shard in stream {
            shards.append(shard)
        }

        #expect(shards.count >= 2, "Incremental feed should produce at least 2 shards from 3s WAV, got \(shards.count)")

        await decoder.cleanup()
    }

    @Test("Peak accumulator stays bounded across many shard-emit cycles")
    func peakAccumulatorBoundedAcrossManyShards() async throws {
        // 5-minute synthetic at shardDuration=1.0s exercises ~300 emit cycles.
        // The bug bound is duration-independent, so 300 cycles proves the same
        // invariant a literal 5-hour run would (without the 576 MB temp file).
        let seconds: UInt32 = 300
        let wavData = Self.makeWAVData(seconds: seconds)

        let decoder = StreamingAudioDecoder(
            episodeID: "test",
            shardDuration: 1.0,
            contentType: "wav"
        )
        let stream = await decoder.shards()

        // Drain shards on a child task so the AsyncStream's internal buffer
        // doesn't grow without bound while we feed.
        let drain = Task<Int, Never> {
            var count = 0
            for await _ in stream { count += 1 }
            return count
        }

        // Bulk-feed the entire WAV in one call. This matches the production
        // seed path in AnalysisCoordinator (`feedData(existingData)` after
        // reading the whole downloaded file from disk) and is the codepath
        // where the unbounded-accumulator bug manifests: a single
        // `decodeAvailableFrames()` call chews through the entire episode's
        // frames before `emitFullShards()` runs. Smaller per-call feeds drain
        // the accumulator naturally and would mask the bug.
        await decoder.feedData(wavData)
        await decoder.finish()

        let shardCount = await drain.value

        // 5 minutes at shardDuration=1.0s yields >= ~290 shards (resampler
        // can drop a frame or two at the boundary).
        #expect(shardCount >= 290, "Expected >=290 shards from 5-min WAV at 1s shards, got \(shardCount)")

        // Bound: samplesPerShard (1.0s x 16_000 = 16_000) + at most one
        // converter chunk (8192 frames x ratio=1.0 for 16 kHz source = 8192).
        // Allow 4x slack for converter framing variance and the final flush.
        let peak = await decoder.peakAccumulatedSampleCountForTesting()
        let allowedMax = 16_000 + 8_192 * 4
        #expect(
            peak <= allowedMax,
            "Peak accumulator was \(peak) samples, expected <= \(allowedMax) (~\(allowedMax * 4 / 1024) KB)"
        )

        await decoder.cleanup()
    }

    @Test("Corrupt mid-stream bytes finish the stream with a failure reason (H1)")
    func corruptMidStreamSurfacesFailure() async {
        // Bytes that don't match any audio header — `AVAudioFile` should
        // reject the file once enough bytes are present to attempt
        // detection. The decoder must finish the AsyncStream so the
        // consumer's `for await` returns rather than stalling.
        let decoder = StreamingAudioDecoder(
            episodeID: "test",
            shardDuration: 1.0,
            contentType: "wav"
        )
        let stream = await decoder.shards()

        // 32 KB of garbage with a fake "RIFF" prefix. Enough to clear
        // `minimumBytesForDetection` so a decode attempt actually fires.
        var garbage = Data()
        garbage.append(contentsOf: "RIFF".utf8)
        garbage.append(Data(repeating: 0xAB, count: 32_000))
        await decoder.feedData(garbage)
        await decoder.finish()

        var count = 0
        for await _ in stream { count += 1 }
        #expect(count == 0, "Garbage bytes must not produce any shards")
        // playhead-rfu-aac (cycle-3 M2): the prior assertion gated the
        // stage check inside `if let r = reason`, so a regression that
        // dropped the failure-reason recording entirely (reason == nil)
        // would silently pass. Require the reason to be set, then pin the
        // stage to one of the two declared cases.
        let reason = await decoder.failureReason()
        #expect(reason != nil, "Garbage bytes must record a sticky failure reason")
        if let r = reason {
            #expect(r.stage == .converterSetup || r.stage == .converterError)
        }
        await decoder.cleanup()
    }

    @Test("cleanup() is idempotent (H2)")
    func cleanupIdempotent() async {
        let decoder = StreamingAudioDecoder(episodeID: "test", shardDuration: 30.0)
        await decoder.feedData(Data(repeating: 0xFF, count: 100))
        await decoder.cleanup()
        await decoder.cleanup()  // Must not crash, must not throw.
        await decoder.cleanup()  // Triple-cleanup also fine.
    }

    @Test("Peak accumulator pins the docstring's stated bound (L1)")
    func peakAccumulatorPinsDocstringBound() async throws {
        // playhead-rfu-aac L1: the StreamingAudioDecoder docstring claims
        //   accumulatedSamples.count peaks at roughly
        //   samplesPerShard + readFramesPerCycle × (16_000 / sourceSampleRate).
        // The pre-existing peakAccumulatorBoundedAcrossManyShards test allows
        // 4x slack to absorb converter framing variance. This test pins the
        // tighter docstring claim — a regression that loosened the
        // accumulator behavior would still pass that earlier test but
        // should fail this one, surfacing the drift to whoever bumps
        // readFramesPerCycle or the converter chunking.
        //
        // For a 16 kHz mono source feeding a 16 kHz target, the ratio
        // collapses to 1.0 so the docstring formula reduces to
        // `samplesPerShard + readFramesPerCycle`.
        let seconds: UInt32 = 60
        let wavData = Self.makeWAVData(seconds: seconds)
        let decoder = StreamingAudioDecoder(
            episodeID: "test",
            shardDuration: 1.0,
            contentType: "wav"
        )
        let stream = await decoder.shards()
        let drain = Task<Int, Never> {
            var count = 0
            for await _ in stream { count += 1 }
            return count
        }
        await decoder.feedData(wavData)
        await decoder.finish()
        _ = await drain.value

        let peak = await decoder.peakAccumulatedSampleCountForTesting()
        // samplesPerShard = 1.0s × 16_000 = 16_000.
        // readFramesPerCycle = 8_192. Source/target ratio = 1.0.
        // playhead-rfu-aac (cycle-3 L7): a full extra read cycle (8192) of
        // epsilon is too generous — that's the size of an entire converter
        // pass, and a regression that loosened the bound by exactly one
        // cycle would still pass. Use a fraction of one cycle (1024 ≈
        // ⅛ cycle) to actually pin the docstring's stated formula.
        let docstringBound = 16_000 + 8_192
        let epsilon = 1_024   // partial-frame boundary slop only
        let allowedMax = docstringBound + epsilon
        #expect(
            peak <= allowedMax,
            "Peak \(peak) violated docstring-pinned bound \(allowedMax) (samplesPerShard+readFramesPerCycle+\(epsilon))"
        )

        await decoder.cleanup()
    }

    // MARK: - WAV Helper

    /// Builds a minimal 16kHz mono 16-bit PCM WAV in memory.
    private static func makeWAVData(seconds: UInt32) -> Data {
        let sampleRate: UInt32 = 16_000
        let numSamples: UInt32 = sampleRate * seconds
        let dataSize: UInt32 = numSamples * 2  // 16-bit = 2 bytes per sample

        var wav = Data()
        // RIFF header
        wav.append(contentsOf: "RIFF".utf8)
        wav.append(withUnsafeBytes(of: (36 + dataSize).littleEndian) { Data($0) })
        wav.append(contentsOf: "WAVE".utf8)
        // fmt chunk
        wav.append(contentsOf: "fmt ".utf8)
        wav.append(withUnsafeBytes(of: UInt32(16).littleEndian) { Data($0) })       // chunk size
        wav.append(withUnsafeBytes(of: UInt16(1).littleEndian) { Data($0) })        // PCM format
        wav.append(withUnsafeBytes(of: UInt16(1).littleEndian) { Data($0) })        // mono
        wav.append(withUnsafeBytes(of: sampleRate.littleEndian) { Data($0) })       // sample rate
        wav.append(withUnsafeBytes(of: (sampleRate * 2).littleEndian) { Data($0) }) // byte rate
        wav.append(withUnsafeBytes(of: UInt16(2).littleEndian) { Data($0) })        // block align
        wav.append(withUnsafeBytes(of: UInt16(16).littleEndian) { Data($0) })       // bits per sample
        // data chunk
        wav.append(contentsOf: "data".utf8)
        wav.append(withUnsafeBytes(of: dataSize.littleEndian) { Data($0) })
        // Silence (16-bit zeros)
        wav.append(Data(count: Int(dataSize)))
        return wav
    }

    private func makeWAVData(seconds: UInt32) -> Data {
        Self.makeWAVData(seconds: seconds)
    }
}
