// TranscriptEngine.swift
// On-device speech-to-text via Apple's Speech framework (SpeechService),
// with a stub fallback for tests and unsupported environments.
//
// The protocol boundary stays intentionally small so the runtime can swap
// between the Apple backend and a lightweight stub without touching callers.

import AVFoundation
import CoreMedia
import Foundation
import os
import OSLog
#if canImport(Speech)
import Speech
#endif

// MARK: - Transcript Types

/// A single word with precise timing from the ASR pass.
struct TranscriptWord: Sendable, Equatable {
    let text: String
    /// Start time in seconds relative to the episode audio.
    let startTime: TimeInterval
    /// End time in seconds relative to the episode audio.
    let endTime: TimeInterval
    /// Model confidence, 0.0...1.0.
    let confidence: Float
}

/// A segment (roughly a sentence or phrase) produced by the ASR model.
struct TranscriptSegment: Sendable, Equatable {
    let id: Int
    /// Ordered words within this segment.
    let words: [TranscriptWord]
    /// Full text of the segment.
    let text: String
    /// Start time of the first word.
    let startTime: TimeInterval
    /// End time of the last word.
    let endTime: TimeInterval
    /// Average confidence across words.
    let avgConfidence: Float
    /// Which model pass produced this segment.
    let passType: TranscriptPassType
}

/// Distinguishes fast-path (real-time) from final-path (backfill) results.
enum TranscriptPassType: String, Sendable, Codable {
    case fast
    case final_ = "final"
}

// MARK: - VAD Types

/// Voice Activity Detection result for a chunk of audio.
struct VADResult: Sendable {
    /// Whether speech was detected in this chunk.
    let isSpeech: Bool
    /// Probability of speech, 0.0...1.0.
    let speechProbability: Float
    /// Start time relative to episode audio.
    let startTime: TimeInterval
    /// End time relative to episode audio.
    let endTime: TimeInterval
}

// MARK: - SpeechRecognizer Protocol

/// Protocol boundary for the ASR engine. The default runtime implementation
/// uses Apple's Speech framework on iOS 26+, while tests can inject the stub.
protocol SpeechRecognizer: Sendable {

    /// Load the model from a local directory path.
    /// Called after AssetProvider promotes a model to the active directory.
    func loadModel(from directory: URL) async throws

    /// Unload the current model, freeing memory and ANE resources.
    func unloadModel() async

    /// Whether a model is currently loaded and ready for inference.
    func isModelLoaded() async -> Bool

    /// Transcribe a single shard of 16 kHz mono Float32 audio.
    /// Returns segments with word-level timestamps.
    func transcribe(
        shard: AnalysisShard
    ) async throws -> [TranscriptSegment]

    /// Run VAD on a shard to detect speech boundaries.
    /// Used to find natural chunk boundaries before transcription.
    func detectVoiceActivity(
        shard: AnalysisShard
    ) async throws -> [VADResult]
}

// MARK: - SpeechService

/// Actor wrapping the runtime ASR backend for thread-safe transcription.
/// Manages dual-pass model loading and segment-level callbacks.
///
/// Uses Apple's Speech framework on-device. Runs transcription on a
/// background thread — never touches the playback audio session or main thread.
actor SpeechService {
    private let logger = Logger(subsystem: "com.playhead", category: "SpeechEngine")

    /// The underlying recognizer (Apple Speech or stub).
    private let recognizer: any SpeechRecognizer

    /// Which model role is currently loaded.
    private(set) var activeModelRole: ModelRole?

    /// Callback for segment-level results as they arrive.
    private let segmentContinuation: AsyncStream<TranscriptSegment>.Continuation

    /// Subscribe for real-time segment results.
    nonisolated let segmentStream: AsyncStream<TranscriptSegment>

    // MARK: - Init

    init() {
        self.recognizer = makeDefaultSpeechRecognizer()

        let (stream, continuation) = AsyncStream<TranscriptSegment>.makeStream()
        self.segmentStream = stream
        self.segmentContinuation = continuation
    }

    init(recognizer: any SpeechRecognizer) {
        self.recognizer = recognizer

        let (stream, continuation) = AsyncStream<TranscriptSegment>.makeStream()
        self.segmentStream = stream
        self.segmentContinuation = continuation
    }

    // MARK: - Model Management

    /// Load the fast-path model for real-time ad lookahead.
    /// Apple Speech prepares locale assets; the directory is ignored but
    /// keeps the runtime shape compatible with the model-based interface.
    func loadFastModel(from directory: URL) async throws {
        logger.info("Preparing fast-path Speech model…")
        let start = ContinuousClock.now
        try await recognizer.loadModel(from: directory)
        activeModelRole = .asrFast
        let elapsed = ContinuousClock.now - start
        logger.info("Fast-path Speech model ready (\(elapsed))")
    }

    /// Load the final-path model for backfill transcription.
    /// Apple Speech prepares locale assets; the directory is ignored but
    /// keeps the runtime shape compatible with the model-based interface.
    func loadFinalModel(from directory: URL) async throws {
        if await recognizer.isModelLoaded() {
            logger.info("Unloading current model before loading final-path model")
            await recognizer.unloadModel()
        }
        logger.info("Preparing final-path Speech model…")
        let start = ContinuousClock.now
        try await recognizer.loadModel(from: directory)
        activeModelRole = .asrFinal
        let elapsed = ContinuousClock.now - start
        logger.info("Final-path Speech model ready (\(elapsed))")
    }

    /// Unload whatever model is currently active.
    func unloadCurrentModel() async {
        await recognizer.unloadModel()
        activeModelRole = nil
        logger.info("ASR model unloaded")
    }

    // MARK: - Transcription

    /// Transcribe a shard and yield segments through the stream.
    /// Caller decides which pass type to tag based on which model is loaded.
    func transcribe(shard: AnalysisShard) async throws -> [TranscriptSegment] {
        guard await recognizer.isModelLoaded() else {
            throw TranscriptEngineError.modelNotLoaded
        }

        let passType: TranscriptPassType = activeModelRole == .asrFinal
            ? .final_
            : .fast

        logger.info("""
            Transcribing shard \(shard.id) \
            [start=\(String(format: "%.2f", shard.startTime))s, \
            end=\(String(format: "%.2f", shard.startTime + shard.duration))s, \
            duration=\(String(format: "%.2f", shard.duration))s, \
            samples=\(shard.sampleCount), \
            episode=\(shard.episodeID)]
            """)
        let start = ContinuousClock.now
        let rawSegments = try await recognizer.transcribe(shard: shard)
        let elapsed = ContinuousClock.now - start
        logger.info("Shard \(shard.id) transcribed in \(elapsed) → \(rawSegments.count) segments")

        // Re-tag with the correct pass type and yield to stream.
        let segments = rawSegments.map { seg in
            TranscriptSegment(
                id: seg.id,
                words: seg.words,
                text: seg.text,
                startTime: seg.startTime,
                endTime: seg.endTime,
                avgConfidence: seg.avgConfidence,
                passType: passType
            )
        }

        for segment in segments {
            segmentContinuation.yield(segment)
        }

        return segments
    }

    /// Transcribe a sequence of shards, using VAD to skip silence.
    /// Yields segments as they're produced for real-time consumption.
    func transcribeWithVAD(
        shards: [AnalysisShard],
        speechThreshold: Float = 0.5
    ) async throws -> [TranscriptSegment] {
        guard await recognizer.isModelLoaded() else {
            throw TranscriptEngineError.modelNotLoaded
        }

        var allSegments: [TranscriptSegment] = []

        for shard in shards {
            try Task.checkCancellation()

            // Run VAD first to skip non-speech chunks.
            let vadResults = try await recognizer.detectVoiceActivity(shard: shard)
            let hasSpeech = vadResults.contains { $0.speechProbability >= speechThreshold }

            guard hasSpeech else {
                logger.debug("Skipping shard \(shard.id) — no speech detected")
                continue
            }

            let segments = try await transcribe(shard: shard)
            allSegments.append(contentsOf: segments)
        }

        return allSegments
    }

    // MARK: - Status

    /// Whether the service is ready for transcription.
    func isReady() async -> Bool {
        await recognizer.isModelLoaded()
    }
}

// MARK: - TranscriptEngineError

enum TranscriptEngineError: Error, CustomStringConvertible {
    case modelNotLoaded
    case transcriptionFailed(String)
    case vadFailed(String)
    case unsupportedSampleRate(expected: Int, actual: Int)

    var description: String {
        switch self {
        case .modelNotLoaded:
            "No ASR recognizer is ready"
        case .transcriptionFailed(let reason):
            "Transcription failed: \(reason)"
        case .vadFailed(let reason):
            "Voice activity detection failed: \(reason)"
        case .unsupportedSampleRate(let expected, let actual):
            "Expected \(expected) Hz sample rate, got \(actual) Hz"
        }
    }
}

// MARK: - StubSpeechRecognizer

/// Stub implementation for development and testing. Returns empty results.
final class StubSpeechRecognizer: SpeechRecognizer, Sendable {
    private let _loaded = OSAllocatedUnfairLock(initialState: false)

    func loadModel(from directory: URL) async throws {
        _loaded.withLock { $0 = true }
    }

    func unloadModel() async {
        _loaded.withLock { $0 = false }
    }

    func isModelLoaded() async -> Bool {
        _loaded.withLock { $0 }
    }

    func transcribe(shard: AnalysisShard) async throws -> [TranscriptSegment] {
        guard _loaded.withLock({ $0 }) else { throw TranscriptEngineError.modelNotLoaded }
        // Stub: return empty transcript.
        return []
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        guard _loaded.withLock({ $0 }) else { throw TranscriptEngineError.modelNotLoaded }
        // Stub: report speech detected for entire shard.
        return [VADResult(
            isSpeech: true,
            speechProbability: 1.0,
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration
        )]
    }
}

// MARK: - Default Recognizer Factory

private func makeDefaultSpeechRecognizer() -> any SpeechRecognizer {
#if canImport(Speech)
    let env = ProcessInfo.processInfo.environment
    let shouldUseStub =
        env["XCTestConfigurationFilePath"] != nil ||
        env["XCODE_RUNNING_FOR_PREVIEWS"] == "1" ||
        env["PLAYHEAD_USE_STUB_SPEECH"] == "1"

    if !shouldUseStub {
        return AppleSpeechRecognizer()
    }
#endif

    return StubSpeechRecognizer()
}

#if canImport(Speech)

// MARK: - AppleSpeechRecognizer

/// Production recognizer using SFSpeechRecognizer (the pre-iOS 26 API).
/// The newer SpeechAnalyzer API crashes with EXC_BREAKPOINT on iOS 26,
/// so we use the stable SFSpeechRecognizer + SFSpeechAudioBufferRecognitionRequest
/// path instead.
actor AppleSpeechRecognizer: SpeechRecognizer {
    private let logger = Logger(subsystem: "com.playhead", category: "AppleSpeechRecognizer")
    private var recognizer: SFSpeechRecognizer?
    private var prepared = false

    func loadModel(from directory: URL) async throws {
        logger.info("Preparing SFSpeechRecognizer backend")
        let locale = Locale(identifier: "en-US")
        guard let sfRecognizer = SFSpeechRecognizer(locale: locale) else {
            throw TranscriptEngineError.transcriptionFailed(
                "SFSpeechRecognizer unavailable for locale en-US"
            )
        }
        recognizer = sfRecognizer
        prepared = true

        // Request authorization if needed.
        let status = SFSpeechRecognizer.authorizationStatus()
        if status == .notDetermined {
            await withCheckedContinuation { continuation in
                SFSpeechRecognizer.requestAuthorization { _ in
                    continuation.resume()
                }
            }
        }

        let finalStatus = SFSpeechRecognizer.authorizationStatus()
        guard finalStatus == .authorized else {
            throw TranscriptEngineError.transcriptionFailed(
                "Speech recognition not authorized (status: \(finalStatus.rawValue))"
            )
        }
        logger.info("SFSpeechRecognizer ready")
    }

    func unloadModel() async {
        recognizer = nil
        prepared = false
    }

    func isModelLoaded() async -> Bool {
        prepared && recognizer?.isAvailable == true
    }

    func transcribe(shard: AnalysisShard) async throws -> [TranscriptSegment] {
        guard let recognizer else {
            throw TranscriptEngineError.modelNotLoaded
        }

        let buffer = try Self.makeBuffer(from: shard)
        let request = SFSpeechAudioBufferRecognitionRequest()
        request.shouldReportPartialResults = false
        request.requiresOnDeviceRecognition = true
        request.addsPunctuation = true

        // Feed the entire buffer and signal end of audio.
        request.append(buffer)
        request.endAudio()

        // Extract Sendable data inside the callback to avoid sending
        // SFSpeechRecognitionResult across actor boundaries.
        let timeOffset = shard.startTime
        let segments: [TranscriptSegment] = try await withCheckedThrowingContinuation { continuation in
            recognizer.recognitionTask(with: request) { result, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                guard let result, result.isFinal else { return }

                let transcription = result.bestTranscription
                let fullText = transcription.formattedString
                    .trimmingCharacters(in: .whitespacesAndNewlines)

                guard !fullText.isEmpty else {
                    continuation.resume(returning: [])
                    return
                }

                var words: [TranscriptWord] = []
                for sfWord in transcription.segments {
                    words.append(TranscriptWord(
                        text: sfWord.substring,
                        startTime: sfWord.timestamp + timeOffset,
                        endTime: sfWord.timestamp + sfWord.duration + timeOffset,
                        confidence: sfWord.confidence
                    ))
                }

                let segStart = words.first?.startTime ?? timeOffset
                let segEnd = words.last?.endTime ?? (timeOffset + shard.duration)
                let avgConf = words.isEmpty
                    ? Float(1.0)
                    : words.map(\.confidence).reduce(0, +) / Float(words.count)

                let segment = TranscriptSegment(
                    id: 0,
                    words: words,
                    text: fullText,
                    startTime: segStart,
                    endTime: segEnd,
                    avgConfidence: avgConf,
                    passType: .fast
                )
                continuation.resume(returning: [segment])
            }
        }

        return segments
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        // SFSpeechRecognizer doesn't have dedicated VAD. Report all audio
        // as speech — the transcription step will produce empty results for
        // silence, which is handled downstream.
        return [VADResult(
            isSpeech: true,
            speechProbability: 1.0,
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration
        )]
    }

    // MARK: - Buffer Creation

    private static let bufferLogger = Logger(subsystem: "com.playhead", category: "AudioBuffer")

    private static func makeBuffer(from shard: AnalysisShard) throws -> AVAudioPCMBuffer {
        guard !shard.samples.isEmpty else {
            throw TranscriptEngineError.transcriptionFailed("empty audio shard")
        }

        // Validate sample data — NaN/Inf/denormalized floats crash the analyzer.
        var nanCount = 0
        var infCount = 0
        var zeroCount = 0
        var sumSquares: Double = 0
        for sample in shard.samples {
            if sample.isNaN { nanCount += 1 }
            else if sample.isInfinite { infCount += 1 }
            else if sample == 0 { zeroCount += 1 }
            sumSquares += Double(sample * sample)
        }
        let rms = sqrt(sumSquares / Double(shard.samples.count))

        bufferLogger.info("""
            Shard \(shard.id) audio: \(shard.samples.count) samples, \
            rms=\(String(format: "%.6f", rms)), \
            zeros=\(zeroCount)/\(shard.samples.count), \
            nan=\(nanCount), inf=\(infCount)
            """)

        if nanCount > 0 || infCount > 0 {
            throw TranscriptEngineError.transcriptionFailed(
                "shard \(shard.id) contains \(nanCount) NaN and \(infCount) Inf samples"
            )
        }

        if zeroCount == shard.samples.count {
            throw TranscriptEngineError.transcriptionFailed(
                "shard \(shard.id) is entirely silent (all zeros)"
            )
        }

        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: 16_000,
            channels: 1,
            interleaved: false
        ) else {
            throw TranscriptEngineError.transcriptionFailed("failed to create analysis audio format")
        }

        guard let buffer = AVAudioPCMBuffer(
            pcmFormat: format,
            frameCapacity: AVAudioFrameCount(shard.samples.count)
        ) else {
            throw TranscriptEngineError.transcriptionFailed("failed to allocate audio buffer")
        }

        buffer.frameLength = AVAudioFrameCount(shard.samples.count)

        guard let channelData = buffer.floatChannelData?.pointee else {
            throw TranscriptEngineError.transcriptionFailed(
                "failed to access buffer channel data"
            )
        }
        shard.samples.withUnsafeBufferPointer { samples in
            guard let source = samples.baseAddress else { return }
            channelData.assign(from: source, count: shard.samples.count)
        }

        return buffer
    }
}

#endif
