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

struct WeakAnchorPhrase: Sendable, Codable, Equatable {
    let text: String
    let startTime: TimeInterval
    let endTime: TimeInterval
    let confidence: Double

    func offsettingTimes(by delta: TimeInterval) -> WeakAnchorPhrase {
        WeakAnchorPhrase(
            text: text,
            startTime: startTime + delta,
            endTime: endTime + delta,
            confidence: confidence
        )
    }
}

struct TranscriptWeakAnchorMetadata: Sendable, Codable, Equatable {
    typealias LowConfidencePhrase = WeakAnchorPhrase

    private struct PhraseSignature: Hashable {
        let text: String
        let startMicroseconds: Int
        let endMicroseconds: Int

        init(_ phrase: WeakAnchorPhrase) {
            self.text = phrase.text.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            self.startMicroseconds = Int((phrase.startTime * 1_000_000).rounded())
            self.endMicroseconds = Int((phrase.endTime * 1_000_000).rounded())
        }
    }

    let averageConfidence: Double
    let minimumConfidence: Double
    let alternativeTexts: [String]
    let lowConfidencePhrases: [WeakAnchorPhrase]

    var hasRecoveryText: Bool {
        !alternativeTexts.isEmpty || !lowConfidencePhrases.isEmpty
    }

    func offsettingTimes(by delta: TimeInterval) -> TranscriptWeakAnchorMetadata {
        TranscriptWeakAnchorMetadata(
            averageConfidence: averageConfidence,
            minimumConfidence: minimumConfidence,
            alternativeTexts: alternativeTexts,
            lowConfidencePhrases: lowConfidencePhrases.map { $0.offsettingTimes(by: delta) }
        )
    }

    func merged(with other: TranscriptWeakAnchorMetadata) -> TranscriptWeakAnchorMetadata {
        var seenAlternatives = Set<String>()
        let mergedAlternatives = (alternativeTexts + other.alternativeTexts).filter { text in
            let normalized = text.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            guard !normalized.isEmpty else { return false }
            return seenAlternatives.insert(normalized).inserted
        }

        var seenPhrases = Set<PhraseSignature>()
        let mergedPhrases = (lowConfidencePhrases + other.lowConfidencePhrases).filter { phrase in
            seenPhrases.insert(PhraseSignature(phrase)).inserted
        }

        return TranscriptWeakAnchorMetadata(
            averageConfidence: min(averageConfidence, other.averageConfidence),
            minimumConfidence: min(minimumConfidence, other.minimumConfidence),
            alternativeTexts: mergedAlternatives,
            lowConfidencePhrases: mergedPhrases
        )
    }

    static func build(
        primaryText: String,
        words: [TranscriptWord],
        alternatives: [String],
        startTime: TimeInterval,
        endTime: TimeInterval,
        lowConfidenceThreshold: Float = 0.6
    ) -> TranscriptWeakAnchorMetadata? {
        guard !words.isEmpty || !alternatives.isEmpty || !primaryText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return nil
        }

        let averageConfidence: Double
        let minimumConfidence: Double
        if words.isEmpty {
            averageConfidence = 1.0
            minimumConfidence = 1.0
        } else {
            let confidences = words.map { Double($0.confidence) }
            averageConfidence = confidences.reduce(0, +) / Double(confidences.count)
            minimumConfidence = confidences.min() ?? 1.0
        }

        var phrases: [WeakAnchorPhrase] = []
        var currentGroup: [TranscriptWord] = []

        func flushGroup() {
            guard let first = currentGroup.first, let last = currentGroup.last else { return }
            phrases.append(
                WeakAnchorPhrase(
                    text: currentGroup.map(\.text).joined(separator: " "),
                    startTime: first.startTime,
                    endTime: max(first.endTime, last.endTime),
                    confidence: currentGroup.map { Double($0.confidence) }.min() ?? 1.0
                )
            )
            currentGroup.removeAll(keepingCapacity: true)
        }

        for word in words {
            if word.confidence < lowConfidenceThreshold {
                currentGroup.append(word)
            } else {
                flushGroup()
            }
        }
        flushGroup()

        return TranscriptWeakAnchorMetadata(
            averageConfidence: averageConfidence,
            minimumConfidence: minimumConfidence,
            alternativeTexts: alternatives.filter {
                !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                    && $0.caseInsensitiveCompare(primaryText) != .orderedSame
            },
            lowConfidencePhrases: phrases.isEmpty && !words.isEmpty && minimumConfidence < Double(lowConfidenceThreshold)
                ? [
                    WeakAnchorPhrase(
                        text: primaryText,
                        startTime: startTime,
                        endTime: endTime,
                        confidence: minimumConfidence
                    )
                ]
                : phrases
        )
    }
}

struct ExtractedTranscriptContent: Sendable, Equatable {
    let words: [TranscriptWord]
    let weakAnchorMetadata: TranscriptWeakAnchorMetadata?
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
    /// Alternative-transcription and weak-anchor recovery metadata.
    let weakAnchorMetadata: TranscriptWeakAnchorMetadata?

    init(
        id: Int,
        words: [TranscriptWord],
        text: String,
        startTime: TimeInterval,
        endTime: TimeInterval,
        avgConfidence: Float,
        passType: TranscriptPassType,
        weakAnchorMetadata: TranscriptWeakAnchorMetadata? = nil
    ) {
        self.id = id
        self.words = words
        self.text = text
        self.startTime = startTime
        self.endTime = endTime
        self.avgConfidence = avgConfidence
        self.passType = passType
        self.weakAnchorMetadata = weakAnchorMetadata
    }
}

/// Distinguishes fast-path (real-time) from final-path (backfill) results.
enum TranscriptPassType: String, Sendable, Codable {
    case fast
    case final_ = "final"
}

// MARK: - Recognition Snapshot

/// Lightweight, framework-agnostic snapshot of a speech recognition result.
/// Used to decouple the partial-promotion logic from Apple's SpeechTranscriber
/// types so it can be unit-tested without a live SpeechAnalyzer session.
struct RecognitionSnapshot: Sendable {
    let isFinal: Bool
    let text: String
    let words: [TranscriptWord]
    let startTime: TimeInterval
    let endTime: TimeInterval
    let weakAnchorMetadata: TranscriptWeakAnchorMetadata?

    init(
        isFinal: Bool,
        text: String,
        words: [TranscriptWord],
        startTime: TimeInterval,
        endTime: TimeInterval,
        weakAnchorMetadata: TranscriptWeakAnchorMetadata? = nil
    ) {
        self.isFinal = isFinal
        self.text = text
        self.words = words
        self.startTime = startTime
        self.endTime = endTime
        self.weakAnchorMetadata = weakAnchorMetadata
    }
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
        shard: AnalysisShard,
        podcastId: String?
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

    init(vocabularyProvider: ASRVocabularyProvider? = nil) {
        self.recognizer = makeDefaultSpeechRecognizer(vocabularyProvider: vocabularyProvider)

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
    func transcribe(shard: AnalysisShard, podcastId: String? = nil) async throws -> [TranscriptSegment] {
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
        let rawSegments = try await recognizer.transcribe(shard: shard, podcastId: podcastId)
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
                passType: passType,
                weakAnchorMetadata: seg.weakAnchorMetadata
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

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
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

private func makeDefaultSpeechRecognizer(
    vocabularyProvider: ASRVocabularyProvider? = nil
) -> any SpeechRecognizer {
#if canImport(Speech)
    let env = ProcessInfo.processInfo.environment
    let shouldUseStub =
        env["XCTestConfigurationFilePath"] != nil ||
        env["XCODE_RUNNING_FOR_PREVIEWS"] == "1" ||
        env["PLAYHEAD_USE_STUB_SPEECH"] == "1"

    if !shouldUseStub {
        return AppleSpeechRecognizer(vocabularyProvider: vocabularyProvider)
    }
#endif

    return StubSpeechRecognizer()
}

#if canImport(Speech)

// MARK: - AppleSpeechRecognizer

/// Production recognizer using SpeechAnalyzer (iOS 26+).
/// Uses bestAvailableAudioFormat to resolve the format SpeechAnalyzer
/// expects (16kHz Int16), then converts our Float32 buffers to match.
/// No microphone or speech recognition permission required.
actor AppleSpeechRecognizer: SpeechRecognizer {
    private let logger = Logger(subsystem: "com.playhead", category: "AppleSpeechRecognizer")
    private static let lowConfidenceThreshold: Float = 0.6
    private let vocabularyProvider: ASRVocabularyProvider?
    private var selectedLocale: Locale?
    private var analyzerFormat: AVAudioFormat?
    private var prepared = false

    init(vocabularyProvider: ASRVocabularyProvider? = nil) {
        self.vocabularyProvider = vocabularyProvider
    }

    // MARK: - Model Lifecycle

    func loadModel(from directory: URL) async throws {
        logger.info("Preparing SpeechAnalyzer backend")
        let locale = Locale(identifier: "en-US")
        selectedLocale = locale

        let transcriber = Self.makeSpeechTranscriber(locale: locale)
        let modules: [any SpeechModule] = [transcriber]
        let status = await AssetInventory.status(forModules: modules)
        logger.info("Speech asset status: \(String(describing: status), privacy: .public)")

        switch status {
        case .unsupported:
            throw TranscriptEngineError.transcriptionFailed(
                "Speech assets unsupported for \(locale.identifier)"
            )
        case .supported, .downloading:
            logger.info("Downloading Speech assets…")
            let start = ContinuousClock.now
            if let request = try await AssetInventory.assetInstallationRequest(supporting: modules) {
                try await request.downloadAndInstall()
            }
            logger.info("Speech assets downloaded in \(ContinuousClock.now - start)")
        case .installed:
            logger.info("Speech assets already installed")
        @unknown default:
            break
        }

        let resolvedFormat = await SpeechAnalyzer.bestAvailableAudioFormat(
            compatibleWith: [transcriber]
        )
        analyzerFormat = resolvedFormat
        logger.info("SpeechAnalyzer format: \(String(describing: resolvedFormat))")

        prepared = true
        logger.info("SpeechAnalyzer ready")
    }

    func unloadModel() async {
        analyzerFormat = nil
        prepared = false
    }

    func isModelLoaded() async -> Bool {
        prepared && analyzerFormat != nil
    }

    // MARK: - Transcription

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard let locale = selectedLocale, let targetFormat = analyzerFormat else {
            throw TranscriptEngineError.modelNotLoaded
        }

        let sourceBuffer = try Self.makeBuffer(from: shard)
        let analyzerBuffer = try Self.convert(sourceBuffer, to: targetFormat)

        guard analyzerBuffer.format == targetFormat else {
            throw TranscriptEngineError.transcriptionFailed(
                "Buffer format \(analyzerBuffer.format) does not match analyzer format \(targetFormat)"
            )
        }

        let transcriber = Self.makeSpeechTranscriber(locale: locale)
        let analysisContext = await Self.makeAnalysisContext(
            podcastId: podcastId,
            vocabularyProvider: vocabularyProvider
        )

        let analysisAudioURL = try Self.makeAnalysisAudioFile(from: analyzerBuffer, format: targetFormat)
        defer { try? FileManager.default.removeItem(at: analysisAudioURL) }
        let analysisInputFile = try AVAudioFile(forReading: analysisAudioURL)
        let analyzer = try await SpeechAnalyzer(
            inputAudioFile: analysisInputFile,
            modules: [transcriber],
            options: .init(priority: .utility, modelRetention: .lingering),
            analysisContext: analysisContext
        )

        logger.debug("Preparing SpeechAnalyzer for shard \(shard.id)")
        try await analyzer.prepareToAnalyze(in: targetFormat)

        let inputSequence = Self.singleBufferSequence(buffer: analyzerBuffer)
        let collector = Task { try await Self.collectSegments(from: transcriber.results) }

        do {
            // analyzeSequence consumes the stream and returns the last sample.
            // finalizeAndFinish closes the session and terminates transcriber.results,
            // which unblocks the collector. Without this call, results hangs forever.
            if let lastSample = try await analyzer.analyzeSequence(inputSequence) {
                try await analyzer.finalizeAndFinish(through: lastSample)
            }
            withExtendedLifetime(analyzerBuffer) {}

            let rawSegments = try await collector.value
            let timeOffset = shard.startTime
            return rawSegments.map { seg in
                TranscriptSegment(
                    id: seg.id,
                    words: seg.words.map { w in
                        TranscriptWord(
                            text: w.text,
                            startTime: w.startTime + timeOffset,
                            endTime: w.endTime + timeOffset,
                            confidence: w.confidence
                        )
                    },
                    text: seg.text,
                    startTime: seg.startTime + timeOffset,
                    endTime: seg.endTime + timeOffset,
                    avgConfidence: seg.avgConfidence,
                    passType: seg.passType,
                    weakAnchorMetadata: seg.weakAnchorMetadata?.offsettingTimes(by: timeOffset)
                )
            }
        } catch {
            await analyzer.cancelAndFinishNow()
            collector.cancel()
            throw error
        }
    }

    // MARK: - VAD

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        guard let targetFormat = analyzerFormat else {
            throw TranscriptEngineError.modelNotLoaded
        }

        let sourceBuffer = try Self.makeBuffer(from: shard)
        let analyzerBuffer = try Self.convert(sourceBuffer, to: targetFormat)

        let detector = SpeechDetector()
        let analysisAudioURL = try Self.makeAnalysisAudioFile(from: analyzerBuffer, format: targetFormat)
        defer { try? FileManager.default.removeItem(at: analysisAudioURL) }
        let analysisInputFile = try AVAudioFile(forReading: analysisAudioURL)
        let analyzer = try await SpeechAnalyzer(
            inputAudioFile: analysisInputFile,
            modules: [detector],
            options: .init(priority: .utility, modelRetention: .lingering)
        )

        try await analyzer.prepareToAnalyze(in: targetFormat)
        let inputSequence = Self.singleBufferSequence(buffer: analyzerBuffer)
        let collector = Task { try await Self.collectVAD(from: detector.results) }

        do {
            if let lastSample = try await analyzer.analyzeSequence(inputSequence) {
                try await analyzer.finalizeAndFinish(through: lastSample)
            }
            withExtendedLifetime(analyzerBuffer) {}

            let timeOffset = shard.startTime
            return try await collector.value.map { vad in
                VADResult(
                    isSpeech: vad.isSpeech,
                    speechProbability: vad.speechProbability,
                    startTime: vad.startTime + timeOffset,
                    endTime: vad.endTime + timeOffset
                )
            }
        } catch {
            await analyzer.cancelAndFinishNow()
            collector.cancel()
            throw error
        }
    }

    @available(iOS 26.0, *)
    private static func makeAnalysisContext(
        podcastId: String?,
        vocabularyProvider: ASRVocabularyProvider?
    ) async -> AnalysisContext {
        let context = AnalysisContext()
        guard let podcastId, let vocabularyProvider else { return context }

        let contextualStrings = await vocabularyProvider.contextualStrings(forPodcastId: podcastId)
        if !contextualStrings.isEmpty {
            context.contextualStrings[.general] = contextualStrings
            Logger(subsystem: "com.playhead", category: "AppleSpeechRecognizer")
                .debug("Applied \(contextualStrings.count) ASR contextual strings for podcast \(podcastId, privacy: .public)")
        }
        return context
    }

    // MARK: - Format Conversion

    /// Convert a 16kHz Float32 buffer to the format SpeechAnalyzer expects.
    private static func convert(
        _ source: AVAudioPCMBuffer,
        to targetFormat: AVAudioFormat
    ) throws -> AVAudioPCMBuffer {
        if source.format == targetFormat { return source }

        guard let converter = AVAudioConverter(from: source.format, to: targetFormat) else {
            throw TranscriptEngineError.transcriptionFailed(
                "Cannot create converter from \(source.format) to \(targetFormat)"
            )
        }

        let ratio = targetFormat.sampleRate / source.format.sampleRate
        let targetFrameCount = AVAudioFrameCount(Double(source.frameLength) * ratio)

        guard let targetBuffer = AVAudioPCMBuffer(
            pcmFormat: targetFormat,
            frameCapacity: targetFrameCount
        ) else {
            throw TranscriptEngineError.transcriptionFailed("Failed to allocate conversion buffer")
        }

        var error: NSError?
        converter.convert(to: targetBuffer, error: &error) { _, outStatus in
            outStatus.pointee = .haveData
            return source
        }
        if let error {
            throw TranscriptEngineError.transcriptionFailed("Audio conversion failed: \(error)")
        }

        return targetBuffer
    }

    // MARK: - Buffer & Stream Helpers

    private static func singleBufferSequence(buffer: AVAudioPCMBuffer) -> AsyncStream<AnalyzerInput> {
        AsyncStream { continuation in
            continuation.yield(AnalyzerInput(buffer: buffer, bufferStartTime: .zero))
            continuation.finish()
        }
    }

    private static func collectSegments<S>(
        from results: S
    ) async throws -> [TranscriptSegment]
    where S: AsyncSequence, S.Element == SpeechTranscriber.Result, S.Failure == Error {
        // Map Apple Speech results to lightweight snapshots, then delegate to
        // the testable helper that handles partial-result promotion.
        let snapshots = results.map { result -> RecognitionSnapshot in
            let fullText = String(result.text.characters)
                .trimmingCharacters(in: .whitespacesAndNewlines)
            let extracted = Self.extractWords(from: result)
            let startTime = extracted.words.first?.startTime ?? Self.seconds(from: result.range.start)
            let endTime = extracted.words.last?.endTime ?? Self.seconds(from: CMTimeAdd(result.range.start, result.range.duration))
            return RecognitionSnapshot(
                isFinal: result.isFinal,
                text: fullText,
                words: extracted.words,
                startTime: startTime,
                endTime: endTime,
                weakAnchorMetadata: extracted.weakAnchorMetadata
            )
        }
        return try await collectSegmentsFromSnapshots(snapshots)
    }

    /// Builds transcript segments from a stream of recognition snapshots,
    /// promoting the last partial result when the stream ends without a
    /// superseding final result. This prevents tail-audio truncation at
    /// shard boundaries (playhead-4ck).
    static func collectSegmentsFromSnapshots<S>(
        _ snapshots: S
    ) async throws -> [TranscriptSegment]
    where S: AsyncSequence, S.Element == RecognitionSnapshot, S.Failure == Error {
        var segments: [TranscriptSegment] = []
        var nextId = 0
        var latestPartial: RecognitionSnapshot?

        for try await snapshot in snapshots {
            if snapshot.isFinal {
                // Final result supersedes any tracked partial.
                latestPartial = nil

                guard !snapshot.text.isEmpty else { continue }
                segments.append(Self.buildSegment(from: snapshot, id: &nextId))
            } else {
                // Track the latest partial; it will be promoted if no final
                // result supersedes it before the stream ends.
                latestPartial = snapshot
            }
        }

        // Promote the trailing partial if it was never superseded.
        if let partial = latestPartial, !partial.text.isEmpty {
            segments.append(Self.buildSegment(from: partial, id: &nextId))
        }

        segments.sort { $0.startTime < $1.startTime }
        return segments
    }

    private static func buildSegment(
        from snapshot: RecognitionSnapshot,
        id nextId: inout Int
    ) -> TranscriptSegment {
        let avgConf = snapshot.words.isEmpty
            ? Float(1.0)
            : snapshot.words.map(\.confidence).reduce(0, +) / Float(snapshot.words.count)
        let segment = TranscriptSegment(
            id: nextId,
            words: snapshot.words.isEmpty
                ? [TranscriptWord(text: snapshot.text, startTime: snapshot.startTime, endTime: snapshot.endTime, confidence: avgConf)]
                : snapshot.words,
            text: snapshot.text,
            startTime: snapshot.startTime,
            endTime: snapshot.endTime,
            avgConfidence: avgConf,
            passType: .fast,
            weakAnchorMetadata: snapshot.weakAnchorMetadata
        )
        nextId += 1
        return segment
    }

    /// Build a time-indexed progressive preset that explicitly asks Speech
    /// for both alternative transcriptions and run-level confidence. The base
    /// progressive preset does not request confidence attributes on its own.
    static func speechTranscriberPreset() -> SpeechTranscriber.Preset {
        let base = SpeechTranscriber.Preset.timeIndexedProgressiveTranscription
        return SpeechTranscriber.Preset(
            transcriptionOptions: base.transcriptionOptions,
            reportingOptions: base.reportingOptions.union([.alternativeTranscriptions]),
            attributeOptions: base.attributeOptions.union([.transcriptionConfidence])
        )
    }

    private static func makeSpeechTranscriber(locale: Locale) -> SpeechTranscriber {
        SpeechTranscriber(locale: locale, preset: speechTranscriberPreset())
    }

    static func extractWords(from result: SpeechTranscriber.Result) -> ExtractedTranscriptContent {
        let fallbackStart = seconds(from: result.range.start)
        let fallbackEnd = seconds(from: CMTimeAdd(result.range.start, result.range.duration))
        return extractWords(
            from: result.text,
            alternatives: result.alternatives,
            fallbackStart: fallbackStart,
            fallbackEnd: fallbackEnd
        )
    }

    static func extractWords(
        from text: AttributedString,
        alternatives: [AttributedString],
        fallbackStart: TimeInterval,
        fallbackEnd: TimeInterval
    ) -> ExtractedTranscriptContent {
        var words: [TranscriptWord] = []

        for run in text.runs {
            let runText = String(text[run.range].characters)
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

            for (i, piece) in pieces.enumerated() {
                let start = runStart + (Double(i) * step)
                let end = i == pieces.count - 1 ? runEnd : min(runEnd, start + step)
                words.append(TranscriptWord(text: piece, startTime: start, endTime: end, confidence: confidence))
            }
        }

        if words.isEmpty {
            let rawText = String(text.characters)
                .trimmingCharacters(in: .whitespacesAndNewlines)
            guard !rawText.isEmpty else {
                return ExtractedTranscriptContent(words: [], weakAnchorMetadata: nil)
            }
            words = [TranscriptWord(text: rawText, startTime: fallbackStart, endTime: fallbackEnd, confidence: 1.0)]
        }

        let primaryText = String(text.characters)
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let metadata = TranscriptWeakAnchorMetadata.build(
            primaryText: primaryText,
            words: words,
            alternatives: uniqueAlternativeTexts(
                from: alternatives,
                excluding: primaryText
            ),
            startTime: fallbackStart,
            endTime: fallbackEnd,
            lowConfidenceThreshold: lowConfidenceThreshold
        )
        return ExtractedTranscriptContent(words: words, weakAnchorMetadata: metadata)
    }

    private static func uniqueAlternativeTexts(
        from alternatives: [AttributedString],
        excluding primaryText: String
    ) -> [String] {
        let normalizedPrimary = primaryText.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        var seen = Set<String>()
        var ordered: [String] = []

        for alternative in alternatives {
            let text = String(alternative.characters).trimmingCharacters(in: .whitespacesAndNewlines)
            guard !text.isEmpty else { continue }
            let normalized = text.lowercased()
            guard normalized != normalizedPrimary else { continue }
            guard seen.insert(normalized).inserted else { continue }
            ordered.append(text)
        }

        return ordered
    }

    private static func collectVAD<S>(
        from results: S
    ) async throws -> [VADResult]
    where S: AsyncSequence, S.Element == SpeechDetector.Result, S.Failure == Error {
        var vadResults: [VADResult] = []
        for try await result in results {
            guard result.isFinal, result.speechDetected else { continue }
            let start = seconds(from: result.range.start)
            let end = seconds(from: CMTimeAdd(result.range.start, result.range.duration))
            vadResults.append(VADResult(isSpeech: true, speechProbability: 1.0, startTime: start, endTime: end))
        }
        vadResults.sort { $0.startTime < $1.startTime }
        return vadResults
    }

    private static func seconds(from time: CMTime) -> TimeInterval {
        let s = CMTimeGetSeconds(time)
        guard s.isFinite else { return 0 }
        return max(s, 0)
    }

    // MARK: - Buffer Creation

    private static let bufferLogger = Logger(subsystem: "com.playhead", category: "AudioBuffer")

    private static func makeBuffer(from shard: AnalysisShard) throws -> AVAudioPCMBuffer {
        guard !shard.samples.isEmpty else {
            throw TranscriptEngineError.transcriptionFailed("empty audio shard")
        }

        // Validate sample data.
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
            throw TranscriptEngineError.transcriptionFailed("failed to access buffer channel data")
        }
        shard.samples.withUnsafeBufferPointer { samples in
            guard let source = samples.baseAddress else { return }
            channelData.assign(from: source, count: shard.samples.count)
        }

        return buffer
    }

    static func analysisAudioFileSettings(for format: AVAudioFormat) -> [String: Any] {
        var settings = format.settings
        // Persist interleaved linear PCM to disk even when the analyzer's
        // processing buffer is non-interleaved. AVAudioFile can then accept
        // the explicit buffer processing format below without rejecting the
        // write on iOS device builds.
        settings[AVLinearPCMIsNonInterleaved] = false
        return settings
    }

    static func makeAnalysisAudioFile(
        from buffer: AVAudioPCMBuffer,
        format: AVAudioFormat
    ) throws -> URL {
        let fileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("playhead-transcript-\(UUID().uuidString).caf")

        let file = try AVAudioFile(
            forWriting: fileURL,
            settings: analysisAudioFileSettings(for: format),
            commonFormat: buffer.format.commonFormat,
            interleaved: buffer.format.isInterleaved
        )
        try file.write(from: buffer)
        return fileURL
    }
}

#endif
