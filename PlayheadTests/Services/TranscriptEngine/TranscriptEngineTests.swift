// TranscriptEngineTests.swift
// Tests for the Speech integration layer and dual-pass transcript engine.

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

    func loadModel(from directory: URL) async throws {
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
        try await service.loadFastModel(from: URL(fileURLWithPath: "/tmp/model"))
        let role = await service.activeModelRole
        #expect(role == .asrFast)
        let ready = await service.isReady()
        #expect(ready)
    }

    @Test("loadFinalModel sets active role to asrFinal")
    func loadFinal() async throws {
        let mock = MockSpeechRecognizer()
        let service = SpeechService(recognizer: mock)
        try await service.loadFinalModel(from: URL(fileURLWithPath: "/tmp/model"))
        let role = await service.activeModelRole
        #expect(role == .asrFinal)
    }

    @Test("loadFinalModel unloads existing model first")
    func finalUnloadsCurrent() async throws {
        let mock = MockSpeechRecognizer()
        let service = SpeechService(recognizer: mock)
        try await service.loadFastModel(from: URL(fileURLWithPath: "/tmp/fast"))
        try await service.loadFinalModel(from: URL(fileURLWithPath: "/tmp/final"))
        let role = await service.activeModelRole
        #expect(role == .asrFinal)
    }

    @Test("unloadCurrentModel clears state")
    func unload() async throws {
        let mock = MockSpeechRecognizer()
        let service = SpeechService(recognizer: mock)
        try await service.loadFastModel(from: URL(fileURLWithPath: "/tmp/model"))
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
        try await service.loadFastModel(from: URL(fileURLWithPath: "/tmp/model"))

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
        try await service.loadFinalModel(from: URL(fileURLWithPath: "/tmp/model"))

        let shard = makeShard()
        let segments = try await service.transcribe(shard: shard)
        #expect(segments.count == 1)
        #expect(segments[0].passType == .final_)
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
        try await service.loadFastModel(from: URL(fileURLWithPath: "/tmp/model"))

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
        try await service.loadFastModel(from: URL(fileURLWithPath: "/tmp/model"))

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
        try await stub.loadModel(from: URL(fileURLWithPath: "/tmp/model"))
        #expect(await stub.isModelLoaded())
        await stub.unloadModel()
        #expect(await !stub.isModelLoaded())
    }

    @Test("Stub transcribe returns empty when loaded")
    func stubTranscribe() async throws {
        let stub = StubSpeechRecognizer()
        try await stub.loadModel(from: URL(fileURLWithPath: "/tmp/model"))
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
        try await stub.loadModel(from: URL(fileURLWithPath: "/tmp/model"))
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

    func loadModel(from directory: URL) async throws { loaded = true }
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

    @Test("appendShards processes new shards after initial loop completes")
    func appendShardsAfterCompletion() async throws {
        let store = try await makeTestStore()
        let recognizer = TrackingRecognizer()
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel(from: URL(fileURLWithPath: "/tmp"))

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

        // Wait for .completed event.
        for await event in events {
            if case .completed = event { break }
        }

        // Verify initial shards were transcribed.
        let initialCount = recognizer.transcribedShardIds.count
        #expect(initialCount >= 3, "Expected at least 3 shards transcribed, got \(initialCount)")

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

        // Give the loop time to process (it runs in a Task on the actor).
        try await Task.sleep(for: .seconds(2))

        // The appended shards must have been transcribed.
        let allIds = recognizer.transcribedShardIds
        #expect(allIds.contains(10), "Shard 10 should have been transcribed after append")
        #expect(allIds.contains(11), "Shard 11 should have been transcribed after append")
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
        try await speech.loadFastModel(from: URL(fileURLWithPath: "/tmp"))

        let engine = TranscriptEngineService(speechService: speech, store: store)
        let firstEvents = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-1",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )

        for await event in firstEvents {
            if case .completed(let assetId) = event, assetId == "asset-1" { break }
        }

        let secondEvents = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 1, startTime: 0, duration: 30)],
            analysisAssetId: "asset-2",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )

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
        try await speech.loadFastModel(from: URL(fileURLWithPath: "/tmp"))

        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-1",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )

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
        try await speech.loadFastModel(from: URL(fileURLWithPath: "/tmp"))

        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-weak",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )

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

@Suite("SpeechTranscriber extraction")
struct SpeechTranscriberExtractionTests {

    @Test("configured SpeechTranscriber preset requests alternatives and confidence attributes")
    func configuredPresetRequestsWeakAnchorSignals() {
        let base = SpeechTranscriber.Preset.timeIndexedProgressiveTranscription
        let preset = AppleSpeechRecognizer.speechTranscriberPreset()

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
        let extracted = AppleSpeechRecognizer.extractWords(
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
        let extracted = AppleSpeechRecognizer.extractWords(
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
        let segments = try await AppleSpeechRecognizer.collectSegmentsFromSnapshots(stream)
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
        let segments = try await AppleSpeechRecognizer.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 2, "Trailing partial should be promoted to a segment")
        #expect(segments[1].text == "Trailing partial")
    }

    @Test("Partial superseded by final is not duplicated")
    func partialSupersededByFinal() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: false, text: "Partial attempt", startTime: 0, endTime: 1),
            makeSnapshot(isFinal: true, text: "Final version", startTime: 0, endTime: 1),
        ])
        let segments = try await AppleSpeechRecognizer.collectSegmentsFromSnapshots(stream)
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
        let segments = try await AppleSpeechRecognizer.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 2)
        #expect(segments[1].text == "Partial v2", "Only the latest partial should be promoted")
    }

    @Test("Empty partial is not promoted")
    func emptyPartialNotPromoted() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: true, text: "Content", startTime: 0, endTime: 1),
            makeSnapshot(isFinal: false, text: "", startTime: 1, endTime: 2),
        ])
        let segments = try await AppleSpeechRecognizer.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 1, "Empty trailing partial should not be promoted")
    }

    @Test("All-partial stream promotes only the last one")
    func allPartialStream() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: false, text: "Partial 1", startTime: 0, endTime: 1),
            makeSnapshot(isFinal: false, text: "Partial 2", startTime: 0, endTime: 1.5),
            makeSnapshot(isFinal: false, text: "Partial 3", startTime: 0, endTime: 2),
        ])
        let segments = try await AppleSpeechRecognizer.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 1, "Only the last partial should survive")
        #expect(segments[0].text == "Partial 3")
    }

    @Test("Segments are sorted by startTime after partial promotion")
    func sortedAfterPromotion() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: true, text: "Later segment", startTime: 5, endTime: 6),
            makeSnapshot(isFinal: false, text: "Earlier partial", startTime: 2, endTime: 3),
        ])
        let segments = try await AppleSpeechRecognizer.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 2)
        #expect(segments[0].text == "Earlier partial", "Promoted partial should be sorted by startTime")
        #expect(segments[1].text == "Later segment")
    }

    @Test("Empty stream produces no segments")
    func emptyStream() async throws {
        let stream = snapshotStream([])
        let segments = try await AppleSpeechRecognizer.collectSegmentsFromSnapshots(stream)
        #expect(segments.isEmpty)
    }

    @Test("Empty final clears a preceding partial")
    func emptyFinalClearsPartial() async throws {
        let stream = snapshotStream([
            makeSnapshot(isFinal: false, text: "Speculative", startTime: 0, endTime: 1),
            makeSnapshot(isFinal: true, text: "", startTime: 0, endTime: 1),
        ])
        let segments = try await AppleSpeechRecognizer.collectSegmentsFromSnapshots(stream)
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
        let segments = try await AppleSpeechRecognizer.collectSegmentsFromSnapshots(stream)
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

        let segments = try await AppleSpeechRecognizer.collectSegmentsFromSnapshots(stream)
        #expect(segments.count == 1)
        #expect(segments[0].weakAnchorMetadata == metadata)
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
        try await speech.loadFastModel(from: URL(fileURLWithPath: "/tmp"))

        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-weak-anchor",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )

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
        try await speech.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
        let engine = TranscriptEngineService(speechService: speech, store: store)

        let firstEvents = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-weak-upgrade",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
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
        try await speech.loadFastModel(from: URL(fileURLWithPath: "/tmp"))
        let engine = TranscriptEngineService(speechService: speech, store: store)

        let firstEvents = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 0, startTime: 0, duration: 30)],
            analysisAssetId: "asset-weak-downgrade",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
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
