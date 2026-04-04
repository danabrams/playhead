// AnalysisAudio.swift
// Audio buffer extraction and format conversion for the analysis pipeline.
//
// Decodes cached podcast audio into 16 kHz mono Float32 shards suitable for
// Apple Speech transcription and feature extraction. Completely separate from
// the playback path — different queue, no shared audio session.

@preconcurrency import AVFoundation
import Foundation
import OSLog

// MARK: - AnalysisShard

/// A short segment of decoded audio ready for ASR or feature extraction.
/// Stored as 16 kHz mono Float32 — the format Apple Speech expects.
struct AnalysisShard: Sendable {
    /// Unique identifier for this shard within the episode.
    let id: Int
    /// Episode identifier this shard belongs to.
    let episodeID: String
    /// Start time in the original audio, in seconds.
    let startTime: TimeInterval
    /// Duration of this shard in seconds.
    let duration: TimeInterval
    /// 16 kHz mono Float32 PCM samples.
    let samples: [Float]

    /// Number of samples in this shard.
    var sampleCount: Int { samples.count }
}

// MARK: - LocalAudioURL

/// A URL that is guaranteed to be a local `file://` path.
/// Use this instead of bare `URL` in the analysis pipeline so the compiler
/// prevents remote URLs from reaching the audio decoder.
struct LocalAudioURL: Sendable, Equatable {
    let url: URL

    /// Returns nil if the URL is not a file URL.
    init?(_ url: URL) {
        guard url.isFileURL else { return nil }
        self.url = url
    }

    var path: String { url.path }
    var absoluteString: String { url.absoluteString }
    var lastPathComponent: String { url.lastPathComponent }
}

// MARK: - AnalysisAudioError

enum AnalysisAudioError: Error, CustomStringConvertible {
    case fileNotFound(URL)
    case assetUnreadable(URL, underlying: Error?)
    case readerSetupFailed(String)
    case converterSetupFailed
    case decodingFailed(String)
    case truncatedFile(URL, expectedDuration: TimeInterval, decodedDuration: TimeInterval)
    case cancelled

    var description: String {
        switch self {
        case .fileNotFound(let url):
            "Analysis file not found: \(url.lastPathComponent)"
        case .assetUnreadable(let url, let err):
            "Cannot read asset \(url.lastPathComponent): \(err?.localizedDescription ?? "unknown")"
        case .readerSetupFailed(let msg):
            "AVAssetReader setup failed: \(msg)"
        case .converterSetupFailed:
            "AVAudioConverter setup failed"
        case .decodingFailed(let msg):
            "Decoding failed: \(msg)"
        case .truncatedFile(let url, let expected, let decoded):
            "Truncated file \(url.lastPathComponent): expected \(expected)s, decoded \(decoded)s"
        case .cancelled:
            "Analysis decoding cancelled"
        }
    }
}

// MARK: - ShardCache

/// File-backed cache for persisted analysis shards. Avoids redundant decoding
/// when hot-path detection, boundary snapping, and backfill share decode work.
private struct ShardCache: Sendable {

    private static var cacheDirectory: URL {
        let appSupport = FileManager.default.urls(
            for: .applicationSupportDirectory, in: .userDomainMask
        ).first!
        return appSupport.appendingPathComponent("AnalysisShards", isDirectory: true)
    }

    /// Directory for a specific episode's shards.
    private static func episodeDirectory(episodeID: String) -> URL {
        cacheDirectory.appendingPathComponent(episodeID, isDirectory: true)
    }

    /// Path for a single shard file.
    private static func shardPath(episodeID: String, shardID: Int) -> URL {
        episodeDirectory(episodeID: episodeID)
            .appendingPathComponent("shard_\(shardID).pcm")
    }

    /// Path for the shard manifest (metadata JSON).
    private static func manifestPath(episodeID: String) -> URL {
        episodeDirectory(episodeID: episodeID)
            .appendingPathComponent("manifest.json")
    }

    /// Check whether cached shards exist for the given episode.
    static func hasCachedShards(episodeID: String) -> Bool {
        FileManager.default.fileExists(atPath: manifestPath(episodeID: episodeID).path)
    }

    /// Load cached shards from disk.
    static func loadShards(episodeID: String) -> [AnalysisShard]? {
        let manifest = manifestPath(episodeID: episodeID)
        guard let data = try? Data(contentsOf: manifest),
              let entries = try? JSONDecoder().decode([ShardManifestEntry].self, from: data)
        else {
            return nil
        }

        var shards: [AnalysisShard] = []
        for entry in entries {
            let path = shardPath(episodeID: episodeID, shardID: entry.id)
            guard let pcmData = try? Data(contentsOf: path) else { return nil }
            let samples = pcmData.withUnsafeBytes { raw in
                Array(raw.bindMemory(to: Float.self))
            }
            shards.append(AnalysisShard(
                id: entry.id,
                episodeID: episodeID,
                startTime: entry.startTime,
                duration: entry.duration,
                samples: samples
            ))
        }
        return shards
    }

    /// Persist shards to disk.
    static func saveShards(_ shards: [AnalysisShard], episodeID: String) {
        let dir = episodeDirectory(episodeID: episodeID)
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)

        // Write PCM data for each shard.
        for shard in shards {
            let path = shardPath(episodeID: episodeID, shardID: shard.id)
            let data = shard.samples.withUnsafeBufferPointer { buf in
                Data(buffer: buf)
            }
            try? data.write(to: path, options: .atomic)
        }

        // Write manifest.
        let entries = shards.map { ShardManifestEntry(
            id: $0.id, startTime: $0.startTime, duration: $0.duration
        ) }
        if let data = try? JSONEncoder().encode(entries) {
            try? data.write(to: manifestPath(episodeID: episodeID), options: .atomic)
        }
    }

    /// Remove cached shards for an episode.
    static func removeShards(episodeID: String) {
        try? FileManager.default.removeItem(at: episodeDirectory(episodeID: episodeID))
    }
}

/// Manifest entry for a cached shard (metadata only, PCM stored separately).
private struct ShardManifestEntry: Codable, Sendable {
    let id: Int
    let startTime: TimeInterval
    let duration: TimeInterval
}

// MARK: - AnalysisAudioProviding

/// Protocol abstraction for audio decoding, enabling test stubs.
protocol AnalysisAudioProviding: Sendable {
    func decode(fileURL: LocalAudioURL, episodeID: String, shardDuration: TimeInterval) async throws -> [AnalysisShard]
}

// MARK: - AnalysisAudioService

/// Decodes cached podcast audio into reusable 16 kHz mono shards for the
/// analysis pipeline.
///
/// Runs on a dedicated background queue. Never touches the playback audio
/// session or its threads.
actor AnalysisAudioService {

    // MARK: - Configuration

    /// Target sample rate for analysis output (Apple Speech standard).
    static let targetSampleRate: Double = 16_000

    /// Default shard duration in seconds. 30 s keeps memory pressure
    /// reasonable and aligns with typical ASR input windows.
    static let defaultShardDuration: TimeInterval = 30.0

    /// Truncation tolerance — if decoded audio is shorter than the asset
    /// duration by more than this fraction, treat as truncated.
    private static let truncationTolerance: Double = 0.05

    /// Converter output buffer size in frames per conversion cycle.
    private static let converterFramesPerCycle: AVAudioFrameCount = 8192

    // MARK: - Output format

    /// 16 kHz mono Float32 — the canonical analysis format.
    private let outputFormat: AVAudioFormat

    // MARK: - State

    private let logger = Logger(subsystem: "com.playhead", category: "AnalysisAudio")
    private var activeTasks: [String: Task<[AnalysisShard], Error>] = [:]

    // MARK: - Init

    init() {
        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: Self.targetSampleRate,
            channels: 1,
            interleaved: false
        ) else {
            fatalError("Failed to create 16 kHz mono Float32 AVAudioFormat")
        }
        self.outputFormat = format
    }

    // MARK: - Public API

    /// Decode a locally cached audio file into analysis shards.
    ///
    /// Returns cached shards if a prior decode has been persisted. Otherwise
    /// decodes from scratch and persists the result.
    ///
    /// - Parameters:
    ///   - fileURL: Path to a locally cached audio file (mp3, m4a, etc.).
    ///   - episodeID: Identifier for the episode (used in shard metadata).
    ///   - shardDuration: Duration of each shard in seconds.
    /// - Returns: An array of `AnalysisShard` covering the file.
    func decode(
        fileURL: LocalAudioURL,
        episodeID: String,
        shardDuration: TimeInterval = AnalysisAudioService.defaultShardDuration
    ) async throws -> [AnalysisShard] {
        // Return persisted shards if available.
        if let cached = ShardCache.loadShards(episodeID: episodeID) {
            return cached
        }

        // If a decode for this episode is already in flight, await it.
        if let existing = activeTasks[episodeID] {
            return try await existing.value
        }

        let task = Task<[AnalysisShard], Error> {
            try await self.performDecode(
                fileURL: fileURL,
                episodeID: episodeID,
                shardDuration: shardDuration
            )
        }

        activeTasks[episodeID] = task

        do {
            let shards = try await task.value
            activeTasks[episodeID] = nil
            return shards
        } catch {
            activeTasks[episodeID] = nil
            throw error
        }
    }

    /// Cancel an in-progress decode for the given episode.
    func cancelDecode(episodeID: String) {
        activeTasks[episodeID]?.cancel()
        activeTasks[episodeID] = nil
    }

    /// Remove persisted shards for an episode.
    func evictCache(episodeID: String) {
        ShardCache.removeShards(episodeID: episodeID)
    }

    // MARK: - Decoding pipeline

    private func performDecode(
        fileURL: LocalAudioURL,
        episodeID: String,
        shardDuration: TimeInterval
    ) async throws -> [AnalysisShard] {
        // 1. Validate the file exists locally.
        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            throw AnalysisAudioError.fileNotFound(fileURL.url)
        }

        // 2. Load the asset and get its audio track.
        let asset = AVURLAsset(url: fileURL.url)
        let assetDuration: TimeInterval
        let audioTrack: AVAssetTrack

        do {
            let duration = try await asset.load(.duration)
            assetDuration = CMTimeGetSeconds(duration)

            let tracks = try await asset.loadTracks(withMediaType: .audio)
            guard let track = tracks.first else {
                throw AnalysisAudioError.assetUnreadable(fileURL.url, underlying: nil)
            }
            audioTrack = track
        } catch let error as AnalysisAudioError {
            throw error
        } catch {
            throw AnalysisAudioError.assetUnreadable(fileURL.url, underlying: error)
        }

        // 3. Load the source audio format from the track.
        let sourceDescriptions: [AudioStreamBasicDescription]
        do {
            let formatDescs = try await audioTrack.load(.formatDescriptions)
            sourceDescriptions = formatDescs.compactMap { desc in
                CMAudioFormatDescriptionGetStreamBasicDescription(desc)?.pointee
            }
        } catch {
            throw AnalysisAudioError.readerSetupFailed(
                "Cannot load track format: \(error.localizedDescription)"
            )
        }

        guard let sourceASBD = sourceDescriptions.first else {
            throw AnalysisAudioError.readerSetupFailed("No audio format description on track")
        }

        // 4. Set up AVAssetReader to decode to native PCM (source sample rate).
        let reader: AVAssetReader
        do {
            reader = try AVAssetReader(asset: asset)
        } catch {
            throw AnalysisAudioError.readerSetupFailed(error.localizedDescription)
        }

        // Decode to Float32 at the source sample rate — AVAudioConverter handles
        // resampling to 16 kHz. Asking AVAssetReaderTrackOutput to resample is
        // unreliable across codecs.
        let decodedSampleRate = sourceASBD.mSampleRate
        let readerOutputSettings: [String: Any] = [
            AVFormatIDKey: kAudioFormatLinearPCM,
            AVLinearPCMBitDepthKey: 32,
            AVLinearPCMIsFloatKey: true,
            AVLinearPCMIsNonInterleaved: false,
            AVSampleRateKey: decodedSampleRate,
            AVNumberOfChannelsKey: 1,
        ]

        let trackOutput = AVAssetReaderTrackOutput(
            track: audioTrack,
            outputSettings: readerOutputSettings
        )
        trackOutput.alwaysCopiesSampleData = false

        guard reader.canAdd(trackOutput) else {
            throw AnalysisAudioError.readerSetupFailed("Cannot add track output to reader")
        }
        reader.add(trackOutput)

        guard reader.startReading() else {
            let msg = reader.error?.localizedDescription ?? "unknown error"
            throw AnalysisAudioError.readerSetupFailed(msg)
        }

        // 5. Set up AVAudioConverter for sample-rate conversion to 16 kHz mono.
        guard let sourceFormat = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: decodedSampleRate,
            channels: 1,
            interleaved: false
        ) else {
            throw AnalysisAudioError.converterSetupFailed
        }

        guard let converter = AVAudioConverter(from: sourceFormat, to: outputFormat) else {
            throw AnalysisAudioError.converterSetupFailed
        }

        // 6. Read and convert all samples.
        var allSamples: [Float] = []
        allSamples.reserveCapacity(Int(assetDuration * Self.targetSampleRate))

        // Accumulated source samples waiting for conversion.
        var sourceSampleBuffer: [Float] = []

        while let sampleBuffer = trackOutput.copyNextSampleBuffer() {
            try Task.checkCancellation()

            guard let blockBuffer = CMSampleBufferGetDataBuffer(sampleBuffer) else {
                continue
            }

            var lengthAtOffset: Int = 0
            var totalLength: Int = 0
            var dataPointer: UnsafeMutablePointer<Int8>?

            let status = CMBlockBufferGetDataPointer(
                blockBuffer,
                atOffset: 0,
                lengthAtOffsetOut: &lengthAtOffset,
                totalLengthOut: &totalLength,
                dataPointerOut: &dataPointer
            )

            guard status == kCMBlockBufferNoErr, let ptr = dataPointer else {
                continue
            }

            let floatCount = totalLength / MemoryLayout<Float>.size
            let floatPtr = UnsafeRawPointer(ptr).bindMemory(
                to: Float.self, capacity: floatCount
            )
            let buffer = UnsafeBufferPointer(start: floatPtr, count: floatCount)
            sourceSampleBuffer.append(contentsOf: buffer)
        }

        // Convert accumulated source samples through AVAudioConverter.
        if !sourceSampleBuffer.isEmpty {
            let converted = try convertSamples(
                sourceSampleBuffer,
                using: converter,
                sourceFormat: sourceFormat
            )
            allSamples.append(contentsOf: converted)
        }

        // 7. Check reader status.
        switch reader.status {
        case .completed:
            break
        case .cancelled:
            throw AnalysisAudioError.cancelled
        case .failed:
            throw AnalysisAudioError.decodingFailed(
                reader.error?.localizedDescription ?? "unknown"
            )
        default:
            break
        }

        // 8. Check for truncation — log but still return partial shards.
        let decodedDuration = Double(allSamples.count) / Self.targetSampleRate
        let isTruncated = assetDuration > 0
            && decodedDuration < assetDuration * (1.0 - Self.truncationTolerance)

        // 9. Slice into shards.
        let samplesPerShard = Int(shardDuration * Self.targetSampleRate)
        var shards: [AnalysisShard] = []
        var offset = 0
        var shardIndex = 0

        while offset < allSamples.count {
            try Task.checkCancellation()

            let remaining = allSamples.count - offset
            let count = min(samplesPerShard, remaining)
            let slice = Array(allSamples[offset..<(offset + count)])

            let shard = AnalysisShard(
                id: shardIndex,
                episodeID: episodeID,
                startTime: Double(offset) / Self.targetSampleRate,
                duration: Double(count) / Self.targetSampleRate,
                samples: slice
            )
            shards.append(shard)

            offset += count
            shardIndex += 1
        }

        // 10. Persist shards for reuse across hot-path, boundary snapping, and
        //     backfill passes. Truncated files are still cached — partial data
        //     is better than re-decoding every time.
        ShardCache.saveShards(shards, episodeID: episodeID)

        // 11. Log truncation warning but return partial shards — throwing here
        //     causes the coordinator to treat it as noAudioAvailable and retry-loop.
        if isTruncated {
            let pct = assetDuration > 0
                ? Int(decodedDuration / assetDuration * 100)
                : 0
            logger.warning("Truncated file \(fileURL.lastPathComponent): decoded \(decodedDuration, format: .fixed(precision: 1))s of \(assetDuration, format: .fixed(precision: 1))s (\(pct)%)")
        }

        return shards
    }

    // MARK: - Sample rate conversion

    /// Convert source-rate Float32 samples to 16 kHz using AVAudioConverter.
    private func convertSamples(
        _ sourceSamples: [Float],
        using converter: AVAudioConverter,
        sourceFormat: AVAudioFormat
    ) throws -> [Float] {
        let frameCount = AVAudioFrameCount(sourceSamples.count)
        guard let inputBuffer = AVAudioPCMBuffer(
            pcmFormat: sourceFormat,
            frameCapacity: frameCount
        ) else {
            throw AnalysisAudioError.converterSetupFailed
        }

        // Copy source samples into the input buffer.
        inputBuffer.frameLength = frameCount
        let channelData = inputBuffer.floatChannelData!
        sourceSamples.withUnsafeBufferPointer { src in
            channelData[0].update(from: src.baseAddress!, count: sourceSamples.count)
        }

        // Estimate output size based on sample rate ratio.
        let ratio = Self.targetSampleRate / sourceFormat.sampleRate
        let estimatedOutputFrames = AVAudioFrameCount(
            Double(frameCount) * ratio + Double(Self.converterFramesPerCycle)
        )
        guard let outputBuffer = AVAudioPCMBuffer(
            pcmFormat: outputFormat,
            frameCapacity: estimatedOutputFrames
        ) else {
            throw AnalysisAudioError.converterSetupFailed
        }

        // nonisolated(unsafe) needed because AVAudioConverterInputBlock is
        // @Sendable but we know conversion is synchronous and single-shot.
        nonisolated(unsafe) var inputConsumed = false
        nonisolated(unsafe) let capturedInput = inputBuffer
        let inputBlock: AVAudioConverterInputBlock = { _, outStatus in
            if inputConsumed {
                outStatus.pointee = .endOfStream
                return nil
            }
            inputConsumed = true
            outStatus.pointee = .haveData
            return capturedInput
        }

        var conversionError: NSError?
        let status = converter.convert(
            to: outputBuffer,
            error: &conversionError,
            withInputFrom: inputBlock
        )

        switch status {
        case .haveData, .endOfStream, .inputRanDry:
            break
        case .error:
            throw AnalysisAudioError.decodingFailed(
                conversionError?.localizedDescription ?? "AVAudioConverter error"
            )
        @unknown default:
            break
        }

        let outputCount = Int(outputBuffer.frameLength)
        guard outputCount > 0, let outData = outputBuffer.floatChannelData else {
            return []
        }

        return Array(UnsafeBufferPointer(start: outData[0], count: outputCount))
    }
}

// MARK: - AnalysisAudioProviding Conformance

extension AnalysisAudioService: AnalysisAudioProviding {}
