// AnalysisAudio.swift
// Audio buffer extraction and format conversion for the analysis pipeline.
//
// Decodes cached podcast audio into 16 kHz mono Float32 shards suitable for
// WhisperKit transcription and feature extraction. Completely separate from
// the playback path — different queue, no shared audio session.

import AVFoundation
import Foundation

// MARK: - AnalysisShard

/// A short segment of decoded audio ready for ASR or feature extraction.
/// Stored as 16 kHz mono Float32 — the format WhisperKit expects.
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

// MARK: - AnalysisAudioService

/// Decodes cached podcast audio into reusable 16 kHz mono shards for the
/// analysis pipeline.
///
/// Runs on a dedicated background queue. Never touches the playback audio
/// session or its threads.
actor AnalysisAudioService {

    // MARK: - Configuration

    /// Target sample rate for analysis output (WhisperKit standard).
    static let targetSampleRate: Double = 16_000

    /// Default shard duration in seconds. 30 s matches WhisperKit's preferred
    /// input length and keeps memory pressure reasonable.
    static let defaultShardDuration: TimeInterval = 30.0

    /// Truncation tolerance — if decoded audio is shorter than the asset
    /// duration by more than this fraction, treat as truncated.
    private static let truncationTolerance: Double = 0.05

    // MARK: - Output format

    /// 16 kHz mono Float32 — the canonical analysis format.
    private let outputFormat: AVAudioFormat

    // MARK: - State

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
    /// - Parameters:
    ///   - fileURL: Path to a locally cached audio file (mp3, m4a, etc.).
    ///   - episodeID: Identifier for the episode (used in shard metadata).
    ///   - shardDuration: Duration of each shard in seconds.
    /// - Returns: An array of `AnalysisShard` covering the full file.
    func decode(
        fileURL: URL,
        episodeID: String,
        shardDuration: TimeInterval = AnalysisAudioService.defaultShardDuration
    ) async throws -> [AnalysisShard] {
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

    // MARK: - Decoding pipeline

    private func performDecode(
        fileURL: URL,
        episodeID: String,
        shardDuration: TimeInterval
    ) async throws -> [AnalysisShard] {
        // 1. Validate the file exists locally.
        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            throw AnalysisAudioError.fileNotFound(fileURL)
        }

        // 2. Load the asset and get its audio track.
        let asset = AVURLAsset(url: fileURL)
        let assetDuration: TimeInterval
        let audioTrack: AVAssetTrack

        do {
            let duration = try await asset.load(.duration)
            assetDuration = CMTimeGetSeconds(duration)

            let tracks = try await asset.loadTracks(withMediaType: .audio)
            guard let track = tracks.first else {
                throw AnalysisAudioError.assetUnreadable(fileURL, underlying: nil)
            }
            audioTrack = track
        } catch let error as AnalysisAudioError {
            throw error
        } catch {
            throw AnalysisAudioError.assetUnreadable(fileURL, underlying: error)
        }

        // 3. Set up AVAssetReader with PCM output.
        let reader: AVAssetReader
        do {
            reader = try AVAssetReader(asset: asset)
        } catch {
            throw AnalysisAudioError.readerSetupFailed(error.localizedDescription)
        }

        let readerOutputSettings: [String: Any] = [
            AVFormatIDKey: kAudioFormatLinearPCM,
            AVLinearPCMBitDepthKey: 32,
            AVLinearPCMIsFloatKey: true,
            AVLinearPCMIsNonInterleaved: false,
            AVSampleRateKey: Self.targetSampleRate,
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

        // 4. Read all samples into a buffer.
        let samplesPerShard = Int(shardDuration * Self.targetSampleRate)
        var allSamples: [Float] = []
        allSamples.reserveCapacity(Int(assetDuration * Self.targetSampleRate))

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
            allSamples.append(contentsOf: buffer)
        }

        // 5. Check reader status.
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

        // 6. Check for truncation.
        let decodedDuration = Double(allSamples.count) / Self.targetSampleRate
        if assetDuration > 0,
           decodedDuration < assetDuration * (1.0 - Self.truncationTolerance)
        {
            throw AnalysisAudioError.truncatedFile(
                fileURL,
                expectedDuration: assetDuration,
                decodedDuration: decodedDuration
            )
        }

        // 7. Slice into shards.
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

        return shards
    }
}
