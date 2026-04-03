// TranscriptEngine.swift
// On-device speech-to-text via WhisperKit (Apple-optimized CoreML/ANE).
// Defines a protocol boundary so the real WhisperKit SPM dependency can
// be swapped in without changing callers.
//
// Dual-pass strategy:
//   Fast path — low-latency model (whisper-tiny/base) for immediate ad
//               lookahead during playback. Downloaded first via AssetProvider.
//   Final path — higher-accuracy model (whisper-small) for durable
//                transcript quality. Backfills once available.
//
// SPM dependency to add (not yet wired):
//   .package(url: "https://github.com/argmaxinc/WhisperKit.git", from: "0.9.0")
//   target dependency: .product(name: "WhisperKit", package: "WhisperKit")

import Foundation
import OSLog

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

/// Protocol boundary for the ASR engine. WhisperKit conforms to this
/// through ``WhisperKitService``. Enables testing with a stub and
/// future engine swaps without touching callers.
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

// MARK: - WhisperKitService

/// Actor wrapping WhisperKit for thread-safe on-device transcription.
/// Manages dual-pass model loading and segment-level callbacks.
///
/// Runs transcription on a background thread — never touches the
/// playback audio session or main thread.
actor WhisperKitService {
    private let logger = Logger(subsystem: "com.playhead", category: "WhisperKit")

    /// The underlying recognizer (real WhisperKit or stub).
    private let recognizer: any SpeechRecognizer

    /// Which model role is currently loaded.
    private(set) var activeModelRole: ModelRole?

    /// Callback for segment-level results as they arrive.
    private let segmentContinuation: AsyncStream<TranscriptSegment>.Continuation

    /// Subscribe for real-time segment results.
    nonisolated let segmentStream: AsyncStream<TranscriptSegment>

    // MARK: - Init

    init(recognizer: any SpeechRecognizer) {
        self.recognizer = recognizer

        let (stream, continuation) = AsyncStream<TranscriptSegment>.makeStream()
        self.segmentStream = stream
        self.segmentContinuation = continuation
    }

    // MARK: - Model Management

    /// Load the fast-path model for real-time ad lookahead.
    /// The fast-path model (whisper-tiny/base) is downloaded first by
    /// AssetProvider and should be loaded as soon as it's ready.
    func loadFastModel(from directory: URL) async throws {
        logger.info("Loading fast-path ASR model from \(directory.lastPathComponent)")
        try await recognizer.loadModel(from: directory)
        activeModelRole = .asrFast
        logger.info("Fast-path ASR model loaded")
    }

    /// Load the final-path model for backfill transcription.
    /// Unloads the current model first if one is loaded.
    func loadFinalModel(from directory: URL) async throws {
        if await recognizer.isModelLoaded() {
            logger.info("Unloading current model before loading final-path model")
            await recognizer.unloadModel()
        }
        logger.info("Loading final-path ASR model from \(directory.lastPathComponent)")
        try await recognizer.loadModel(from: directory)
        activeModelRole = .asrFinal
        logger.info("Final-path ASR model loaded")
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

        let rawSegments = try await recognizer.transcribe(shard: shard)

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
            "No ASR model loaded — call loadFastModel or loadFinalModel first"
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
/// Replace with real WhisperKit integration once the SPM dependency is added.
///
/// To integrate WhisperKit:
/// 1. Add SPM dependency: https://github.com/argmaxinc/WhisperKit.git (from: "0.9.0")
/// 2. Create a class conforming to `SpeechRecognizer` that wraps `WhisperKit.WhisperKit`
/// 3. In `loadModel(from:)`, call `WhisperKit(modelFolder: directory.path)`
/// 4. In `transcribe(shard:)`, call `whisperKit.transcribe(audioArray: shard.samples)`
/// 5. Map `TranscriptionResult` segments/words to `TranscriptSegment`/`TranscriptWord`
/// 6. For VAD, use `WhisperKit.detectVoiceActivity(audioArray:)` or the built-in
///    energy-based VAD from AudioUtils
final class StubSpeechRecognizer: SpeechRecognizer, @unchecked Sendable {
    private var loaded = false

    func loadModel(from directory: URL) async throws {
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
        // Stub: return empty transcript.
        return []
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        guard loaded else { throw TranscriptEngineError.modelNotLoaded }
        // Stub: report speech detected for entire shard.
        return [VADResult(
            isSpeech: true,
            speechProbability: 1.0,
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration
        )]
    }
}
