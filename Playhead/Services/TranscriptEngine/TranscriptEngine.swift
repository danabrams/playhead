// TranscriptEngine.swift
// On-device speech-to-text via Apple's Speech framework, with a stub
// fallback for tests and unsupported environments.
//
// The protocol boundary stays intentionally small so the runtime can swap
// between the Apple backend and a lightweight stub without touching callers.

import AVFoundation
import CoreMedia
import Foundation
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

// MARK: - WhisperKitService

/// Actor wrapping the runtime ASR backend for thread-safe transcription.
/// Manages dual-pass model loading and segment-level callbacks.
///
/// Runs transcription on a background thread — never touches the
/// playback audio session or main thread.
actor WhisperKitService {
    private let logger = Logger(subsystem: "com.playhead", category: "SpeechEngine")

    /// The underlying recognizer (real WhisperKit or stub).
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
    /// Apple Speech ignores the directory, but the call keeps the runtime
    /// shape compatible with the earlier model-backed implementation.
    func loadFastModel(from directory: URL) async throws {
        logger.info("Loading fast-path ASR model from \(directory.lastPathComponent)")
        try await recognizer.loadModel(from: directory)
        activeModelRole = .asrFast
        logger.info("Fast-path ASR model loaded")
    }

    /// Load the final-path model for backfill transcription.
    /// Apple Speech ignores the directory, but the call keeps the runtime
    /// shape compatible with the earlier model-backed implementation.
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

// MARK: - Default Recognizer Factory

private func makeDefaultSpeechRecognizer() -> any SpeechRecognizer {
#if canImport(Speech)
    let env = ProcessInfo.processInfo.environment
    let shouldUseStub =
        env["XCTestConfigurationFilePath"] != nil ||
        env["XCODE_RUNNING_FOR_PREVIEWS"] == "1" ||
        env["PLAYHEAD_USE_STUB_SPEECH"] == "1"

    if !shouldUseStub && SpeechTranscriber.isAvailable {
        return AppleSpeechRecognizer()
    }
#endif

    return StubSpeechRecognizer()
}

#if canImport(Speech)

// MARK: - AppleSpeechRecognizer

/// Production recognizer backed by Speech.framework's transcriber and detector.
actor AppleSpeechRecognizer: SpeechRecognizer {
    private let logger = Logger(subsystem: "com.playhead", category: "AppleSpeechRecognizer")
    private var selectedLocale: Locale?
    private var prepared = false

    func loadModel(from directory: URL) async throws {
        logger.info("Preparing Apple Speech backend; ignoring model directory \(directory.lastPathComponent, privacy: .public)")
        _ = try await ensureLocaleAssetsPrepared()
    }

    func unloadModel() async {
        // Speech.framework manages its own system model lifecycle.
        prepared = false
    }

    func isModelLoaded() async -> Bool {
        prepared || SpeechTranscriber.isAvailable
    }

    func transcribe(shard: AnalysisShard) async throws -> [TranscriptSegment] {
        let locale = try await ensureLocaleAssetsPrepared()

        let buffer = try Self.makeBuffer(from: shard)
        let transcriber = SpeechTranscriber(locale: locale, preset: .timeIndexedProgressiveTranscription)
        let analyzer = SpeechAnalyzer(
            modules: [transcriber],
            options: .init(priority: .utility, modelRetention: .lingering)
        )

        try await analyzer.prepareToAnalyze(in: buffer.format)

        let inputSequence = Self.singleBufferSequence(
            buffer: buffer,
            startTime: shard.startTime
        )

        let collector = Task { try await Self.collectTranscriptSegments(from: transcriber.results) }

        do {
            _ = try await analyzer.analyzeSequence(inputSequence)
            return try await collector.value
        } catch {
            await analyzer.cancelAndFinishNow()
            collector.cancel()
            throw error
        }
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        _ = try await ensureLocaleAssetsPrepared()

        let buffer = try Self.makeBuffer(from: shard)
        let detector = SpeechDetector()
        let analyzer = SpeechAnalyzer(
            modules: [detector],
            options: .init(priority: .utility, modelRetention: .lingering)
        )

        try await analyzer.prepareToAnalyze(in: buffer.format)

        let inputSequence = Self.singleBufferSequence(
            buffer: buffer,
            startTime: shard.startTime
        )

        let collector = Task { try await Self.collectVoiceActivity(from: detector.results) }

        do {
            _ = try await analyzer.analyzeSequence(inputSequence)
            return try await collector.value
        } catch {
            await analyzer.cancelAndFinishNow()
            collector.cancel()
            throw error
        }
    }

    private func ensureLocaleAssetsPrepared() async throws -> Locale {
        let locale = await Self.resolveLocale(preferred: selectedLocale)
        let transcriber = SpeechTranscriber(
            locale: locale,
            preset: .timeIndexedProgressiveTranscription
        )
        let modules: [any SpeechModule] = [transcriber]

        let status = await AssetInventory.status(forModules: modules)
        switch status {
        case .unsupported:
            throw TranscriptEngineError.transcriptionFailed(
                "Speech assets are unsupported for locale \(locale.identifier)"
            )

        case .supported, .downloading:
            if let request = try await AssetInventory.assetInstallationRequest(
                supporting: modules
            ) {
                try await request.downloadAndInstall()
            }

        case .installed:
            break
        }

        selectedLocale = locale
        prepared = true
        return locale
    }

    private static func resolveLocale(preferred: Locale?) async -> Locale {
        if let preferred {
            return preferred
        }

        let current = Locale.current
        if let supported = await SpeechTranscriber.supportedLocale(equivalentTo: current) {
            return supported
        }

        let installed = await SpeechTranscriber.installedLocales
        if let firstInstalled = installed.first {
            return firstInstalled
        }

        return current
    }

    private static func makeBuffer(from shard: AnalysisShard) throws -> AVAudioPCMBuffer {
        guard !shard.samples.isEmpty else {
            throw TranscriptEngineError.transcriptionFailed("empty audio shard")
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
        shard.samples.withUnsafeBufferPointer { samples in
            guard let channelData = buffer.floatChannelData?.pointee,
                  let source = samples.baseAddress else { return }
            channelData.assign(from: source, count: shard.samples.count)
        }

        return buffer
    }

    private static func singleBufferSequence(
        buffer: AVAudioPCMBuffer,
        startTime: TimeInterval
    ) -> AsyncStream<AnalyzerInput> {
        AsyncStream { continuation in
            let cmTime = CMTime(seconds: startTime, preferredTimescale: 600)
            continuation.yield(AnalyzerInput(buffer: buffer, bufferStartTime: cmTime))
            continuation.finish()
        }
    }

    private static func collectTranscriptSegments<Results>(
        from results: Results
    ) async throws -> [TranscriptSegment]
    where Results: AsyncSequence, Results.Element == SpeechTranscriber.Result, Results.Failure == Error {
        var segments: [TranscriptSegment] = []
        var nextId = 0

        for try await result in results {
            guard result.isFinal else { continue }

            let words = words(from: result)
            let fullText = String(result.text.characters).trimmingCharacters(in: .whitespacesAndNewlines)
            guard !fullText.isEmpty else { continue }

            let startTime = words.first?.startTime ?? seconds(from: result.range.start)
            let endTime = words.last?.endTime ?? seconds(from: CMTimeAdd(result.range.start, result.range.duration))
            let avgConfidence = words.isEmpty
                ? 1.0
                : words.map(\.confidence).reduce(0, +) / Float(words.count)

            segments.append(TranscriptSegment(
                id: nextId,
                words: words.isEmpty ? [TranscriptWord(
                    text: fullText,
                    startTime: startTime,
                    endTime: endTime,
                    confidence: avgConfidence
                )] : words,
                text: fullText,
                startTime: startTime,
                endTime: endTime,
                avgConfidence: avgConfidence,
                passType: .fast
            ))
            nextId += 1
        }

        segments.sort { $0.startTime < $1.startTime }
        return segments
    }

    private static func words(from result: SpeechTranscriber.Result) -> [TranscriptWord] {
        let fallbackStart = seconds(from: result.range.start)
        let fallbackEnd = seconds(from: CMTimeAdd(result.range.start, result.range.duration))
        let fallbackRange = max(fallbackEnd - fallbackStart, 0)

        var words: [TranscriptWord] = []

        for run in result.text.runs {
            let runText = String(result.text[run.range].characters)
                .trimmingCharacters(in: .whitespacesAndNewlines)
            guard !runText.isEmpty else { continue }

            let pieces = runText.split(whereSeparator: \.isWhitespace).map(String.init)
            guard !pieces.isEmpty else { continue }

            let timeRange = run.audioTimeRange
            let runStart = timeRange.map { seconds(from: $0.start) } ?? fallbackStart
            let runEnd = timeRange.map { seconds(from: CMTimeAdd($0.start, $0.duration)) } ?? fallbackEnd
            let runDuration = max(runEnd - runStart, 0)
            let step = pieces.count > 0 ? runDuration / Double(pieces.count) : 0
            let confidence = Float(run.transcriptionConfidence ?? 1.0)

            for (index, piece) in pieces.enumerated() {
                let start = runStart + (Double(index) * step)
                let end = index == pieces.count - 1 ? runEnd : min(runEnd, start + step)
                words.append(TranscriptWord(
                    text: piece,
                    startTime: start,
                    endTime: end,
                    confidence: confidence
                ))
            }
        }

        if words.isEmpty {
            let text = String(result.text.characters)
                .trimmingCharacters(in: .whitespacesAndNewlines)
            guard !text.isEmpty else { return [] }

            words = [TranscriptWord(
                text: text,
                startTime: fallbackStart,
                endTime: fallbackStart + fallbackRange,
                confidence: 1.0
            )]
        }

        return words
    }

    private static func collectVoiceActivity<Results>(
        from results: Results
    ) async throws -> [VADResult]
    where Results: AsyncSequence, Results.Element == SpeechDetector.Result, Results.Failure == Error {
        var vadResults: [VADResult] = []

        for try await result in results {
            guard result.isFinal, result.speechDetected else { continue }

            let start = seconds(from: result.range.start)
            let end = seconds(from: CMTimeAdd(result.range.start, result.range.duration))

            vadResults.append(VADResult(
                isSpeech: true,
                speechProbability: 1.0,
                startTime: start,
                endTime: end
            ))
        }

        vadResults.sort { $0.startTime < $1.startTime }
        return vadResults
    }

    private static func seconds(from time: CMTime) -> TimeInterval {
        let seconds = CMTimeGetSeconds(time)
        guard seconds.isFinite else { return 0 }
        return max(seconds, 0)
    }
}

#endif
