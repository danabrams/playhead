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

/// Serializes recognizer calls that ultimately touch Apple's Speech framework.
/// Swift actors are reentrant across `await`, so a cancelled transcription can
/// still overlap a restarted one unless we hold an explicit permit across the
/// full async recognizer call. Overlap triggers `SFSpeechErrorDomain Code=16`
/// ("Maximum number of simultaneous requests reached") on `prepareToAnalyze`.
private actor SpeechRecognitionRequestGate {
    private var isHeld = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func withExclusiveAccess<T>(
        _ operation: @Sendable () async throws -> T
    ) async throws -> T {
        await acquire()

        do {
            try Task.checkCancellation()
            let result = try await operation()
            release()
            return result
        } catch {
            release()
            throw error
        }
    }

    private func acquire() async {
        guard !isHeld else {
            await withCheckedContinuation { continuation in
                waiters.append(continuation)
            }
            return
        }

        isHeld = true
    }

    private func release() {
        guard !waiters.isEmpty else {
            isHeld = false
            return
        }

        let waiter = waiters.removeFirst()
        waiter.resume()
    }
}

// MARK: - SpeechService

/// Actor wrapping the runtime ASR backend for thread-safe transcription.
/// Manages dual-pass model loading and segment-level callbacks.
///
/// Uses Apple's Speech framework on-device. Runs transcription on a
/// background thread — never touches the playback audio session or main thread.
actor SpeechService {
    private let logger = Logger(subsystem: "com.playhead", category: "SpeechEngine")
    private static let requestGate = SpeechRecognitionRequestGate()

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
        let recognizer = self.recognizer
        let rawSegments = try await Self.requestGate.withExclusiveAccess {
            try await recognizer.transcribe(shard: shard, podcastId: podcastId)
        }
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
            let recognizer = self.recognizer
            let vadResults = try await Self.requestGate.withExclusiveAccess {
                try await recognizer.detectVoiceActivity(shard: shard)
            }
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

struct AppleSpeechPreparedModel {
    let locale: Locale
    let analyzerFormat: AVAudioFormat
}

enum AppleSpeechBoundaryError: Error, CustomStringConvertible {
    case speechAssetsUnsupported(localeIdentifier: String)
    case analyzerFormatUnavailable(localeIdentifier: String)
    case audioBridgeFailure(String)
    case invalidAnalyzerInputTimeline(String)
    case analyzerSessionFailure(String)

    var description: String {
        switch self {
        case .speechAssetsUnsupported(let localeIdentifier):
            "Speech assets unsupported for \(localeIdentifier)"
        case .analyzerFormatUnavailable(let localeIdentifier):
            "SpeechAnalyzer did not negotiate a usable audio format for \(localeIdentifier)"
        case .audioBridgeFailure(let reason):
            reason
        case .invalidAnalyzerInputTimeline(let reason):
            reason
        case .analyzerSessionFailure(let reason):
            reason
        }
    }
}

// playhead-sw69: injection seams so AppleSpeechAssetBootstrapper can be unit
// tested without touching the live AssetInventory/SpeechAnalyzer statics.
// Mirrors the seam style used elsewhere in this file (small protocol + a
// default struct that forwards to the framework call).

/// Minimal seam over `Speech.AssetInventory` covering only what the
/// bootstrapper actually consults: the per-module status query and the
/// download-and-install side effect for the supported/downloading branches.
protocol AppleSpeechAssetStatusProviding: Sendable {
    func status(forModules modules: [any SpeechModule]) async -> AssetInventory.Status
    func installAssets(supporting modules: [any SpeechModule]) async throws
}

/// Production implementation forwarding to the live `AssetInventory` statics.
struct DefaultAppleSpeechAssetStatusProvider: AppleSpeechAssetStatusProviding {
    func status(forModules modules: [any SpeechModule]) async -> AssetInventory.Status {
        await AssetInventory.status(forModules: modules)
    }

    func installAssets(supporting modules: [any SpeechModule]) async throws {
        if let request = try await AssetInventory.assetInstallationRequest(supporting: modules) {
            try await request.downloadAndInstall()
        }
    }
}

/// Minimal seam over `SpeechAnalyzer.bestAvailableAudioFormat(...)`. The
/// bootstrapper only ever consults the unconditional, single-argument variant.
protocol AppleSpeechAnalyzerFormatProviding: Sendable {
    func bestAvailableAudioFormat(compatibleWith modules: [any SpeechModule]) async -> AVAudioFormat?
}

/// Production implementation forwarding to the live `SpeechAnalyzer` static.
struct DefaultAppleSpeechAnalyzerFormatProvider: AppleSpeechAnalyzerFormatProviding {
    func bestAvailableAudioFormat(compatibleWith modules: [any SpeechModule]) async -> AVAudioFormat? {
        await SpeechAnalyzer.bestAvailableAudioFormat(compatibleWith: modules)
    }
}

struct AppleSpeechAssetBootstrapper {
    private let logger = Logger(subsystem: "com.playhead", category: "AppleSpeechBootstrapper")
    private let assetStatusProvider: any AppleSpeechAssetStatusProviding
    private let analyzerFormatProvider: any AppleSpeechAnalyzerFormatProviding

    init(
        assetStatusProvider: any AppleSpeechAssetStatusProviding = DefaultAppleSpeechAssetStatusProvider(),
        analyzerFormatProvider: any AppleSpeechAnalyzerFormatProviding = DefaultAppleSpeechAnalyzerFormatProvider()
    ) {
        self.assetStatusProvider = assetStatusProvider
        self.analyzerFormatProvider = analyzerFormatProvider
    }

    func prepare(localeIdentifier: String = "en-US") async throws -> AppleSpeechPreparedModel {
        let locale = Locale(identifier: localeIdentifier)
        let transcriber = AppleSpeechResultMapper.makeSpeechTranscriber(locale: locale)
        let modules: [any SpeechModule] = [transcriber]
        let status = await assetStatusProvider.status(forModules: modules)
        logger.info("Speech asset status: \(String(describing: status), privacy: .public)")

        switch status {
        case .unsupported:
            throw AppleSpeechBoundaryError.speechAssetsUnsupported(localeIdentifier: locale.identifier)
        case .supported, .downloading:
            logger.info("Downloading Speech assets…")
            let start = ContinuousClock.now
            try await assetStatusProvider.installAssets(supporting: modules)
            logger.info("Speech assets downloaded in \(ContinuousClock.now - start)")
        case .installed:
            logger.info("Speech assets already installed")
        @unknown default:
            break
        }

        guard let resolvedFormat = await analyzerFormatProvider.bestAvailableAudioFormat(
            compatibleWith: [transcriber]
        ) else {
            throw AppleSpeechBoundaryError.analyzerFormatUnavailable(localeIdentifier: locale.identifier)
        }
        logger.info("SpeechAnalyzer format: \(String(describing: resolvedFormat))")
        return AppleSpeechPreparedModel(locale: locale, analyzerFormat: resolvedFormat)
    }
}

enum AppleSpeechAudioBridge {
    private static let bufferLogger = Logger(subsystem: "com.playhead", category: "AudioBuffer")

    static func makeAnalyzerBuffer(
        from shard: AnalysisShard,
        targetFormat: AVAudioFormat
    ) throws -> AVAudioPCMBuffer {
        let sourceBuffer = try makeSourceBuffer(from: shard)
        let analyzerBuffer = try convert(sourceBuffer, to: targetFormat)

        guard analyzerBuffer.format == targetFormat else {
            throw AppleSpeechBoundaryError.audioBridgeFailure(
                "Buffer format \(analyzerBuffer.format) does not match analyzer format \(targetFormat)"
            )
        }

        return analyzerBuffer
    }

    private static func convert(
        _ source: AVAudioPCMBuffer,
        to targetFormat: AVAudioFormat
    ) throws -> AVAudioPCMBuffer {
        if source.format == targetFormat { return source }

        guard let converter = AVAudioConverter(from: source.format, to: targetFormat) else {
            throw AppleSpeechBoundaryError.audioBridgeFailure(
                "Cannot create converter from \(source.format) to \(targetFormat)"
            )
        }

        let ratio = targetFormat.sampleRate / source.format.sampleRate
        let targetFrameCount = AVAudioFrameCount(Double(source.frameLength) * ratio)

        guard let targetBuffer = AVAudioPCMBuffer(
            pcmFormat: targetFormat,
            frameCapacity: targetFrameCount
        ) else {
            throw AppleSpeechBoundaryError.audioBridgeFailure("Failed to allocate conversion buffer")
        }

        var error: NSError?
        var inputConsumed = false
        let status = converter.convert(to: targetBuffer, error: &error) { _, outStatus in
            if inputConsumed {
                outStatus.pointee = .endOfStream
                return nil
            }
            inputConsumed = true
            outStatus.pointee = .haveData
            return source
        }
        if let error {
            throw AppleSpeechBoundaryError.audioBridgeFailure("Audio conversion failed: \(error)")
        }
        if status == .error {
            throw AppleSpeechBoundaryError.audioBridgeFailure("Audio conversion returned error status without details")
        }

        return targetBuffer
    }

    private static func makeSourceBuffer(from shard: AnalysisShard) throws -> AVAudioPCMBuffer {
        guard !shard.samples.isEmpty else {
            throw AppleSpeechBoundaryError.audioBridgeFailure("empty audio shard")
        }

        var nanCount = 0
        var infCount = 0
        var zeroCount = 0
        var sumSquares: Double = 0
        for sample in shard.samples {
            if sample.isNaN { nanCount += 1 }
            else if sample.isInfinite { infCount += 1 }
            else {
                if sample == 0 { zeroCount += 1 }
                sumSquares += Double(sample * sample)
            }
        }

        if nanCount > 0 || infCount > 0 {
            throw AppleSpeechBoundaryError.audioBridgeFailure(
                "shard \(shard.id) contains \(nanCount) NaN and \(infCount) Inf samples"
            )
        }
        if zeroCount == shard.samples.count {
            throw AppleSpeechBoundaryError.audioBridgeFailure(
                "shard \(shard.id) is entirely silent (all zeros)"
            )
        }

        let rms = sqrt(sumSquares / Double(shard.samples.count))
        bufferLogger.info("""
            Shard \(shard.id) audio: \(shard.samples.count) samples, \
            rms=\(String(format: "%.6f", rms)), \
            zeros=\(zeroCount)/\(shard.samples.count)
            """)

        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: 16_000,
            channels: 1,
            interleaved: false
        ) else {
            throw AppleSpeechBoundaryError.audioBridgeFailure("failed to create analysis audio format")
        }

        guard let buffer = AVAudioPCMBuffer(
            pcmFormat: format,
            frameCapacity: AVAudioFrameCount(shard.samples.count)
        ) else {
            throw AppleSpeechBoundaryError.audioBridgeFailure("failed to allocate audio buffer")
        }

        buffer.frameLength = AVAudioFrameCount(shard.samples.count)

        guard let channelData = buffer.floatChannelData?.pointee else {
            throw AppleSpeechBoundaryError.audioBridgeFailure("failed to access buffer channel data")
        }
        shard.samples.withUnsafeBufferPointer { samples in
            guard let source = samples.baseAddress else { return }
            channelData.update(from: source, count: shard.samples.count)
        }

        return buffer
    }
}

struct AppleSpeechAnalyzerRunner {
    private let vocabularyProvider: ASRVocabularyProvider?
    private let options = SpeechAnalyzer.Options(priority: .utility, modelRetention: .lingering)

    init(vocabularyProvider: ASRVocabularyProvider? = nil) {
        self.vocabularyProvider = vocabularyProvider
    }

    // Single-shard analysis must use exactly one input mode. Mixing the
    // file-backed SpeechAnalyzer initializer with analyzeSequence(buffer...)
    // replays the same audio on a conflicting timeline and triggers
    // SFSpeechErrorDomain Code=2 for overlapping timestamps.
    func transcribe(
        buffer: AVAudioPCMBuffer,
        format: AVAudioFormat,
        locale: Locale,
        podcastId: String?
    ) async throws -> [TranscriptSegment] {
        let transcriber = AppleSpeechResultMapper.makeSpeechTranscriber(locale: locale)
        let analysisContext = await makeAnalysisContext(for: podcastId)
        let analyzer = SpeechAnalyzer(modules: [transcriber], options: options)
        var collector: Task<[TranscriptSegment], Error>?
        var needsCleanup = true

        do {
            try await apply(context: analysisContext, to: analyzer)
            try await prepare(analyzer: analyzer, in: format)
            let inputSequence = try Self.singleBufferSequence(buffer: buffer)

            collector = Task { try await AppleSpeechResultMapper.collectSegments(from: transcriber.results) }

            if let lastSample = try await analyzer.analyzeSequence(inputSequence) {
                try await analyzer.finalizeAndFinish(through: lastSample)
            } else {
                try await analyzer.finalizeAndFinishThroughEndOfInput()
            }
            withExtendedLifetime(buffer) {}
            // playhead-rfu-aac M4: only mark "no cleanup needed" once the
            // collector's `.value` has resolved without throwing. Setting
            // it pre-await meant a thrown collector error fell into the
            // catch with `needsCleanup == false`, skipping
            // `cancelAndFinishNow()` and leaving the analyzer hung on
            // its result stream. The order matters because the result
            // stream is what the collector is awaiting — if results never
            // resolve we still need the explicit cancel-and-finish.
            let segments = try await collector!.value
            needsCleanup = false
            return segments
        } catch {
            if needsCleanup {
                await analyzer.cancelAndFinishNow()
            }
            collector?.cancel()
            if let boundaryError = error as? AppleSpeechBoundaryError {
                throw boundaryError
            }
            throw AppleSpeechBoundaryError.analyzerSessionFailure("SpeechAnalyzer transcription failed: \(error)")
        }
    }

    func detectVoiceActivity(
        buffer: AVAudioPCMBuffer,
        format: AVAudioFormat
    ) async throws -> [VADResult] {
        let detector = SpeechDetector()
        let analyzer = SpeechAnalyzer(modules: [detector], options: options)
        var collector: Task<[VADResult], Error>?
        var needsCleanup = true

        do {
            try await prepare(analyzer: analyzer, in: format)
            let inputSequence = try Self.singleBufferSequence(buffer: buffer)

            collector = Task { try await AppleSpeechResultMapper.collectVAD(from: detector.results) }

            if let lastSample = try await analyzer.analyzeSequence(inputSequence) {
                try await analyzer.finalizeAndFinish(through: lastSample)
            } else {
                try await analyzer.finalizeAndFinishThroughEndOfInput()
            }
            withExtendedLifetime(buffer) {}
            // playhead-rfu-aac M4: see the matching note in `transcribe`.
            // Hold `needsCleanup = true` until the collector resolves so
            // a result-stream failure still triggers cancelAndFinishNow.
            let vadResults = try await collector!.value
            needsCleanup = false
            return vadResults
        } catch {
            if needsCleanup {
                await analyzer.cancelAndFinishNow()
            }
            collector?.cancel()
            if let boundaryError = error as? AppleSpeechBoundaryError {
                throw boundaryError
            }
            throw AppleSpeechBoundaryError.analyzerSessionFailure("SpeechAnalyzer VAD failed: \(error)")
        }
    }

    private func makeAnalysisContext(for podcastId: String?) async -> AnalysisContext {
        let context = AnalysisContext()
        guard let podcastId, let vocabularyProvider else { return context }

        let contextualStrings = await vocabularyProvider.contextualStrings(forPodcastId: podcastId)
        if !contextualStrings.isEmpty {
            context.contextualStrings[.general] = contextualStrings
            Logger(subsystem: "com.playhead", category: "AppleSpeechAnalyzerRunner")
                .debug("Applied \(contextualStrings.count) ASR contextual strings for podcast \(podcastId, privacy: .public)")
        }
        return context
    }

    private func apply(context: AnalysisContext, to analyzer: SpeechAnalyzer) async throws {
        do {
            try await analyzer.setContext(context)
        } catch {
            throw AppleSpeechBoundaryError.analyzerSessionFailure("SpeechAnalyzer context setup failed: \(error)")
        }
    }

    private func prepare(analyzer: SpeechAnalyzer, in format: AVAudioFormat) async throws {
        do {
            try await analyzer.prepareToAnalyze(in: format)
        } catch {
            throw AppleSpeechBoundaryError.analyzerSessionFailure("SpeechAnalyzer prepare failed: \(error)")
        }
    }

    static func makeAnalyzerInput(
        buffer: AVAudioPCMBuffer,
        bufferStartTime: CMTime? = nil
    ) throws -> AnalyzerInput {
        if let bufferStartTime {
            guard bufferStartTime.isValid else {
                throw AppleSpeechBoundaryError.invalidAnalyzerInputTimeline(
                    "SpeechAnalyzer input buffer start time must be valid"
                )
            }
            return AnalyzerInput(buffer: buffer, bufferStartTime: bufferStartTime)
        }
        return AnalyzerInput(buffer: buffer)
    }

    static func validateAnalyzerInputTimeline(_ inputs: [AnalyzerInput]) throws {
        guard !inputs.isEmpty else { return }

        let hasExplicitTimestamps = inputs.contains { $0.bufferStartTime != nil }
        let hasImplicitTimestamps = inputs.contains { $0.bufferStartTime == nil }

        if hasExplicitTimestamps && hasImplicitTimestamps {
            throw AppleSpeechBoundaryError.invalidAnalyzerInputTimeline(
                "SpeechAnalyzer inputs must use either all implicit or all explicit buffer start times"
            )
        }

        guard hasExplicitTimestamps else { return }

        var previousEndTime: CMTime?
        for input in inputs {
            guard let startTime = input.bufferStartTime else {
                continue
            }
            guard startTime.isValid else {
                throw AppleSpeechBoundaryError.invalidAnalyzerInputTimeline(
                    "SpeechAnalyzer input at timeline position has invalid buffer start time"
                )
            }

            let endTime = try analyzerInputEndTime(input)
            if let previousEndTime, CMTimeCompare(startTime, previousEndTime) < 0 {
                throw AppleSpeechBoundaryError.invalidAnalyzerInputTimeline(
                    "SpeechAnalyzer input buffer timestamps overlap or precede prior audio input"
                )
            }
            previousEndTime = endTime
        }
    }

    static func makeAnalyzerSequence(inputs: [AnalyzerInput]) throws -> AsyncStream<AnalyzerInput> {
        try validateAnalyzerInputTimeline(inputs)
        return AsyncStream { continuation in
            for input in inputs {
                continuation.yield(input)
            }
            continuation.finish()
        }
    }

    private static func singleBufferSequence(buffer: AVAudioPCMBuffer) throws -> AsyncStream<AnalyzerInput> {
        try makeAnalyzerSequence(inputs: [makeAnalyzerInput(buffer: buffer)])
    }

    private static func analyzerInputEndTime(_ input: AnalyzerInput) throws -> CMTime {
        let sampleRate = input.buffer.format.sampleRate
        guard sampleRate > 0 else {
            throw AppleSpeechBoundaryError.invalidAnalyzerInputTimeline(
                "SpeechAnalyzer input buffer sample rate must be positive"
            )
        }

        let durationSeconds = Double(input.buffer.frameLength) / sampleRate
        let duration = CMTime(seconds: durationSeconds, preferredTimescale: 600_000)
        return CMTimeAdd(input.bufferStartTime ?? .zero, duration)
    }
}

enum AppleSpeechResultMapper {
    private static let lowConfidenceThreshold: Float = 0.6

    static func offsetSegments(
        _ segments: [TranscriptSegment],
        by delta: TimeInterval
    ) -> [TranscriptSegment] {
        segments.map { segment in
            TranscriptSegment(
                id: segment.id,
                words: segment.words.map { word in
                    TranscriptWord(
                        text: word.text,
                        startTime: word.startTime + delta,
                        endTime: word.endTime + delta,
                        confidence: word.confidence
                    )
                },
                text: segment.text,
                startTime: segment.startTime + delta,
                endTime: segment.endTime + delta,
                avgConfidence: segment.avgConfidence,
                passType: segment.passType,
                weakAnchorMetadata: segment.weakAnchorMetadata?.offsettingTimes(by: delta)
            )
        }
    }

    static func offsetVADResults(
        _ results: [VADResult],
        by delta: TimeInterval
    ) -> [VADResult] {
        results.map { result in
            VADResult(
                isSpeech: result.isSpeech,
                speechProbability: result.speechProbability,
                startTime: result.startTime + delta,
                endTime: result.endTime + delta
            )
        }
    }

    static func collectSegments<S>(
        from results: S
    ) async throws -> [TranscriptSegment]
    where S: AsyncSequence, S.Element == SpeechTranscriber.Result, S.Failure == Error {
        let snapshots = results.map { result -> RecognitionSnapshot in
            let fullText = String(result.text.characters)
                .trimmingCharacters(in: .whitespacesAndNewlines)
            let extracted = extractWords(from: result)
            let startTime = extracted.words.first?.startTime ?? seconds(from: result.range.start)
            let endTime = extracted.words.last?.endTime ?? seconds(from: CMTimeAdd(result.range.start, result.range.duration))
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

    static func collectSegmentsFromSnapshots<S>(
        _ snapshots: S
    ) async throws -> [TranscriptSegment]
    where S: AsyncSequence, S.Element == RecognitionSnapshot, S.Failure == Error {
        var segments: [TranscriptSegment] = []
        var nextId = 0
        var latestPartial: RecognitionSnapshot?

        for try await snapshot in snapshots {
            if snapshot.isFinal {
                latestPartial = nil

                guard !snapshot.text.isEmpty else { continue }
                segments.append(buildSegment(from: snapshot, id: &nextId))
            } else {
                latestPartial = snapshot
            }
        }

        if let partial = latestPartial, !partial.text.isEmpty {
            segments.append(buildSegment(from: partial, id: &nextId))
        }

        segments.sort { $0.startTime < $1.startTime }
        return segments
    }

    static func speechTranscriberPreset() -> SpeechTranscriber.Preset {
        let base = SpeechTranscriber.Preset.timeIndexedProgressiveTranscription
        return SpeechTranscriber.Preset(
            transcriptionOptions: base.transcriptionOptions,
            reportingOptions: base.reportingOptions.union([.alternativeTranscriptions]),
            attributeOptions: base.attributeOptions.union([.transcriptionConfidence])
        )
    }

    static func makeSpeechTranscriber(locale: Locale) -> SpeechTranscriber {
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
            let step = runDuration / Double(pieces.count)
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

    static func collectVAD<S>(
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
            passType: .fast, // SpeechService.transcribe re-tags pass type at the shard level
            weakAnchorMetadata: snapshot.weakAnchorMetadata
        )
        nextId += 1
        return segment
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

    private static func seconds(from time: CMTime) -> TimeInterval {
        let s = CMTimeGetSeconds(time)
        guard s.isFinite else { return 0 }
        return max(s, 0)
    }
}

// MARK: - AppleSpeechRecognizer

/// Production recognizer using SpeechAnalyzer (iOS 26+).
/// Uses bestAvailableAudioFormat to resolve the format SpeechAnalyzer
/// expects (16kHz Int16), then converts our Float32 buffers to match.
/// No microphone or speech recognition permission required.
actor AppleSpeechRecognizer: SpeechRecognizer {
    private let logger = Logger(subsystem: "com.playhead", category: "AppleSpeechRecognizer")
    private let vocabularyProvider: ASRVocabularyProvider?
    private let assetBootstrapper: AppleSpeechAssetBootstrapper
    private let analyzerRunner: AppleSpeechAnalyzerRunner
    private var selectedLocale: Locale?
    private var analyzerFormat: AVAudioFormat?
    private var prepared = false

    init(vocabularyProvider: ASRVocabularyProvider? = nil) {
        self.vocabularyProvider = vocabularyProvider
        self.assetBootstrapper = AppleSpeechAssetBootstrapper()
        self.analyzerRunner = AppleSpeechAnalyzerRunner(vocabularyProvider: vocabularyProvider)
    }

    // MARK: - Model Lifecycle

    func loadModel(from directory: URL) async throws {
        logger.info("Preparing SpeechAnalyzer backend")
        do {
            let preparedModel = try await assetBootstrapper.prepare()
            selectedLocale = preparedModel.locale
            analyzerFormat = preparedModel.analyzerFormat
            prepared = true
            logger.info("SpeechAnalyzer ready")
        } catch let error as TranscriptEngineError {
            throw error
        } catch let error as AppleSpeechBoundaryError {
            throw TranscriptEngineError.transcriptionFailed(error.description)
        } catch {
            throw TranscriptEngineError.transcriptionFailed("Failed to prepare SpeechAnalyzer backend: \(error)")
        }
    }

    func unloadModel() async {
        selectedLocale = nil
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

        do {
            let analyzerBuffer = try AppleSpeechAudioBridge.makeAnalyzerBuffer(from: shard, targetFormat: targetFormat)
            logger.debug("Preparing SpeechAnalyzer for shard \(shard.id)")
            let rawSegments = try await analyzerRunner.transcribe(
                buffer: analyzerBuffer,
                format: targetFormat,
                locale: locale,
                podcastId: podcastId
            )
            return AppleSpeechResultMapper.offsetSegments(rawSegments, by: shard.startTime)
        } catch let error as TranscriptEngineError {
            throw error
        } catch let error as AppleSpeechBoundaryError {
            throw TranscriptEngineError.transcriptionFailed(error.description)
        } catch {
            throw TranscriptEngineError.transcriptionFailed("Speech transcription failed for shard \(shard.id): \(error)")
        }
    }

    // MARK: - VAD

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        guard let targetFormat = analyzerFormat else {
            throw TranscriptEngineError.modelNotLoaded
        }

        do {
            let analyzerBuffer = try AppleSpeechAudioBridge.makeAnalyzerBuffer(from: shard, targetFormat: targetFormat)
            let vadResults = try await analyzerRunner.detectVoiceActivity(
                buffer: analyzerBuffer,
                format: targetFormat
            )
            return AppleSpeechResultMapper.offsetVADResults(vadResults, by: shard.startTime)
        } catch let error as TranscriptEngineError {
            throw error
        } catch let error as AppleSpeechBoundaryError {
            throw TranscriptEngineError.transcriptionFailed(error.description)
        } catch {
            throw TranscriptEngineError.transcriptionFailed("Speech VAD failed for shard \(shard.id): \(error)")
        }
    }

}

#endif
