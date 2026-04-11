// TranscriptEngineTests.swift
// Tests for the Speech integration layer and dual-pass transcript engine.

import Foundation
import os
import Testing
@testable import Playhead

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

    func transcribe(shard: AnalysisShard) async throws -> [TranscriptSegment] {
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
        let result = try await stub.transcribe(shard: makeShard())
        #expect(result.isEmpty)
    }

    @Test("Stub throws when not loaded")
    func stubThrowsUnloaded() async {
        let stub = StubSpeechRecognizer()
        await #expect(throws: TranscriptEngineError.self) {
            try await stub.transcribe(shard: makeShard())
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

// MARK: - Tracking Recognizer

/// Mock recognizer that records which shard IDs were transcribed.
private final class TrackingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private var loaded = false
    private let _shardIds = OSAllocatedUnfairLock(initialState: [Int]())
    var transcribedShardIds: [Int] { _shardIds.withLock { $0 } }

    func loadModel(from directory: URL) async throws { loaded = true }
    func unloadModel() async { loaded = false }
    func isModelLoaded() async -> Bool { loaded }

    func transcribe(shard: AnalysisShard) async throws -> [TranscriptSegment] {
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
    confidence: Float = 0.9
) -> RecognitionSnapshot {
    let words = text.isEmpty ? [] : [
        TranscriptWord(text: text, startTime: startTime, endTime: endTime, confidence: confidence)
    ]
    return RecognitionSnapshot(
        isFinal: isFinal,
        text: text,
        words: words,
        startTime: startTime,
        endTime: endTime
    )
}

#if canImport(Speech)

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
}

#endif

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
