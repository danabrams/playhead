// TranscriptEngineTests.swift
// Tests for the Speech integration layer and dual-pass transcript engine.

import Foundation
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
