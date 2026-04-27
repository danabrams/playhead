// StreamingAudioDecoder.swift
// Incrementally decodes compressed audio bytes into 16 kHz mono Float32
// AnalysisShards as they arrive from a progressive download.
//
// Unlike AnalysisAudioService (which requires the complete file), this actor
// accepts chunks of compressed data and emits shards via AsyncStream as soon
// as enough audio has been decoded. This lets transcription begin while the
// download is still in progress.

@preconcurrency import AVFoundation
import Foundation
import OSLog

// MARK: - StreamingAudioDecoder

/// Incrementally decodes compressed audio bytes into 16 kHz mono Float32
/// AnalysisShards. Feed it chunks of compressed audio data as they arrive
/// from a download; it emits a shard every ~30 seconds of accumulated audio.
actor StreamingAudioDecoder {

    // MARK: - Configuration

    private let episodeID: String
    private let shardDuration: TimeInterval
    private let targetSampleRate: Double = 16_000

    /// Frames to read per AVAudioFile read cycle.
    private static let readFramesPerCycle: AVAudioFrameCount = 8192

    /// Frames per AVAudioConverter output buffer.
    private static let converterFramesPerCycle: AVAudioFrameCount = 8192

    /// Minimum bytes before attempting format detection. Small files like ID3
    /// headers alone can cause AVAudioFile to fail; 16 KB is enough for most
    /// compressed audio headers.
    private static let minimumBytesForDetection: Int = 16_384

    // MARK: - Format detection

    private var formatDetected = false
    private var sourceFormat: AVAudioFormat?
    private var converter: AVAudioConverter?

    // MARK: - Temp file

    private let tempFileURL: URL
    private var tempFileHandle: FileHandle?
    private var totalBytesWritten: Int64 = 0

    // MARK: - Reading position

    private var framesRead: AVAudioFramePosition = 0

    // MARK: - PCM accumulator

    private var accumulatedSamples: [Float] = []
    #if DEBUG
    /// Test-only watermark of the largest `accumulatedSamples.count` ever
    /// observed across the lifetime of this decoder. Used by
    /// `StreamingAudioDecoderTests` to pin the bounded-accumulator invariant.
    private var _peakAccumulatedSampleCountForTesting: Int = 0

    /// Test-only accessor for the peak watermark.
    func peakAccumulatedSampleCountForTesting() -> Int {
        _peakAccumulatedSampleCountForTesting
    }
    #endif
    private var nextShardID: Int = 0
    private var totalSamplesEmitted: Int = 0

    // MARK: - Output stream

    private var shardContinuation: AsyncStream<AnalysisShard>.Continuation?
    private var streamCreated = false

    // MARK: - Output format (16 kHz mono Float32)

    private let outputFormat: AVAudioFormat

    // MARK: - Logging

    private let logger = Logger(subsystem: "com.playhead", category: "StreamingAudioDecoder")

    // MARK: - Init

    /// Create a streaming decoder for the given episode.
    ///
    /// - Parameters:
    ///   - episodeID: Identifier for the episode (used in shard metadata).
    ///   - shardDuration: Duration of each emitted shard in seconds.
    ///   - contentType: MIME type or file extension hint (e.g. "audio/mpeg",
    ///     "mp3", "audio/aac", "m4a"). Used to pick the temp file extension
    ///     so AVAudioFile can detect the format correctly.
    init(episodeID: String, shardDuration: TimeInterval = 30.0, contentType: String = "mp3") {
        self.episodeID = episodeID
        self.shardDuration = shardDuration

        let ext = Self.fileExtension(for: contentType)
        self.tempFileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("streaming-decode-\(UUID().uuidString)")
            .appendingPathExtension(ext)

        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: 16_000,
            channels: 1,
            interleaved: false
        ) else {
            fatalError("Failed to create 16 kHz mono Float32 AVAudioFormat")
        }
        self.outputFormat = format
    }

    // MARK: - Public API

    /// Returns the shard stream. Call `feedData(_:)` to push compressed bytes,
    /// then `finish()` when the download completes.
    ///
    /// Only one stream may be created per decoder instance.
    func shards() -> AsyncStream<AnalysisShard> {
        precondition(!streamCreated, "shards() may only be called once per StreamingAudioDecoder")
        streamCreated = true

        return AsyncStream<AnalysisShard> { continuation in
            self.shardContinuation = continuation
        }
    }

    /// Feed compressed audio bytes from the download.
    ///
    /// Errors are logged but do not throw -- partial decode failures should
    /// not stop the stream. The decoder will retry on the next call.
    func feedData(_ data: Data) {
        guard !data.isEmpty else { return }

        // 1. Write bytes to temp file.
        do {
            try appendToTempFile(data)
        } catch {
            logger.error("Failed to write to temp file: \(error.localizedDescription)")
            return
        }

        // 2. Wait for enough bytes before attempting format detection.
        guard totalBytesWritten >= Self.minimumBytesForDetection else {
            return
        }

        // 3. Try to open the temp file and decode new frames.
        decodeAvailableFrames()
    }

    /// Signal that no more data will arrive. Flushes any remaining
    /// accumulated samples as a final (possibly shorter) shard, then
    /// finishes the output stream.
    func finish() {
        // One last decode pass to pick up any trailing frames.
        if totalBytesWritten > 0 {
            decodeAvailableFrames()
        }

        // Flush remaining accumulated samples as a final shard.
        if !accumulatedSamples.isEmpty {
            emitShard(samples: accumulatedSamples, isFinal: true)
            accumulatedSamples.removeAll()
        }

        shardContinuation?.finish()
        shardContinuation = nil
    }

    /// Remove the temp file. Call after the stream has been fully consumed
    /// or when the decode is no longer needed.
    func cleanup() {
        try? tempFileHandle?.close()
        tempFileHandle = nil
        try? FileManager.default.removeItem(at: tempFileURL)
    }

    // MARK: - Temp file management

    private func appendToTempFile(_ data: Data) throws {
        if tempFileHandle == nil {
            // Create the file on first write.
            FileManager.default.createFile(atPath: tempFileURL.path, contents: nil)
            tempFileHandle = try FileHandle(forWritingTo: tempFileURL)
        }

        let handle = tempFileHandle!
        handle.seekToEndOfFile()
        handle.write(data)
        totalBytesWritten += Int64(data.count)
    }

    // MARK: - Decoding

    private func decodeAvailableFrames() {
        // Try to open the temp file as an AVAudioFile. This may fail if the
        // file doesn't yet contain a valid header -- that's expected early on.
        let audioFile: AVAudioFile
        do {
            audioFile = try AVAudioFile(forReading: tempFileURL)
        } catch {
            // Not enough data for a valid audio file yet. Try again next feed.
            if !formatDetected {
                logger.debug("Waiting for valid audio header (\(self.totalBytesWritten) bytes so far)")
            } else {
                logger.warning("Failed to reopen audio file: \(error.localizedDescription)")
            }
            return
        }

        // Detect format on first successful open.
        if !formatDetected {
            let srcFormat = audioFile.processingFormat
            guard let conv = AVAudioConverter(from: srcFormat, to: outputFormat) else {
                logger.error("Failed to create AVAudioConverter from \(srcFormat) to \(self.outputFormat)")
                return
            }
            self.sourceFormat = srcFormat
            self.converter = conv
            self.formatDetected = true

            logger.info("Format detected: \(srcFormat.sampleRate) Hz, \(srcFormat.channelCount) ch -> 16 kHz mono")
        }

        guard let srcFormat = sourceFormat, let converter = converter else { return }

        // Seek to where we left off.
        let fileLength = audioFile.length
        guard fileLength > framesRead else { return }

        audioFile.framePosition = framesRead

        // Read available frames in chunks and convert each chunk with a
        // fresh converter. AVAudioConverter enters a "finished" state after
        // the input block returns .endOfStream, so reusing a converter
        // across multiple convert calls produces no output after the first.
        let framesToRead = fileLength - framesRead
        var framesRemaining = AVAudioFrameCount(framesToRead)

        while framesRemaining > 0 {
            let chunkSize = min(Self.readFramesPerCycle, framesRemaining)

            guard let readBuffer = AVAudioPCMBuffer(
                pcmFormat: srcFormat,
                frameCapacity: chunkSize
            ) else {
                logger.error("Failed to allocate read buffer")
                break
            }

            do {
                try audioFile.read(into: readBuffer, frameCount: chunkSize)
            } catch {
                logger.debug("Read stopped (likely partial frame): \(error.localizedDescription)")
                break
            }

            let framesActuallyRead = readBuffer.frameLength
            if framesActuallyRead == 0 { break }

            framesRead += AVAudioFramePosition(framesActuallyRead)
            framesRemaining -= framesActuallyRead

            // Fresh converter per chunk — endOfStream kills the converter state.
            guard let chunkConverter = AVAudioConverter(from: srcFormat, to: outputFormat) else {
                logger.error("Failed to create chunk converter")
                break
            }
            let converted = convertBuffer(readBuffer, using: chunkConverter)
            if !converted.isEmpty {
                accumulatedSamples.append(contentsOf: converted)
                #if DEBUG
                if accumulatedSamples.count > _peakAccumulatedSampleCountForTesting {
                    _peakAccumulatedSampleCountForTesting = accumulatedSamples.count
                }
                #endif
            }
        }

        // Emit full shards from accumulated samples.
        emitFullShards()
    }

    // MARK: - Sample rate conversion

    private func convertBuffer(
        _ inputBuffer: AVAudioPCMBuffer,
        using converter: AVAudioConverter
    ) -> [Float] {
        // Estimate output frames from sample rate ratio.
        let ratio = targetSampleRate / (sourceFormat?.sampleRate ?? targetSampleRate)
        let estimatedFrames = AVAudioFrameCount(
            Double(inputBuffer.frameLength) * ratio
        ) + Self.converterFramesPerCycle

        guard let outputBuffer = AVAudioPCMBuffer(
            pcmFormat: outputFormat,
            frameCapacity: estimatedFrames
        ) else {
            logger.error("Failed to allocate converter output buffer")
            return []
        }

        // The input block provides the source buffer exactly once, then
        // signals end-of-stream. This is safe because AVAudioConverter.convert
        // runs synchronously and consumes the block before returning.
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

        var error: NSError?
        let status = converter.convert(to: outputBuffer, error: &error, withInputFrom: inputBlock)

        switch status {
        case .haveData, .endOfStream, .inputRanDry:
            break
        case .error:
            logger.error("Converter error: \(error?.localizedDescription ?? "unknown")")
            return []
        @unknown default:
            break
        }

        let count = Int(outputBuffer.frameLength)
        guard count > 0, let channelData = outputBuffer.floatChannelData else {
            return []
        }

        return Array(UnsafeBufferPointer(start: channelData[0], count: count))
    }

    // MARK: - Shard emission

    private var samplesPerShard: Int {
        Int(shardDuration * targetSampleRate)
    }

    private func emitFullShards() {
        let threshold = samplesPerShard
        while accumulatedSamples.count >= threshold {
            let shardSamples = Array(accumulatedSamples.prefix(threshold))
            accumulatedSamples.removeFirst(threshold)
            emitShard(samples: shardSamples, isFinal: false)
        }
    }

    private func emitShard(samples: [Float], isFinal: Bool) {
        let startTime = Double(totalSamplesEmitted) / targetSampleRate
        let duration = Double(samples.count) / targetSampleRate

        let shard = AnalysisShard(
            id: nextShardID,
            episodeID: episodeID,
            startTime: startTime,
            duration: duration,
            samples: samples
        )

        nextShardID += 1
        totalSamplesEmitted += samples.count

        shardContinuation?.yield(shard)

        if isFinal {
            logger.info("Final shard \(shard.id): \(duration, format: .fixed(precision: 1))s at \(startTime, format: .fixed(precision: 1))s")
        } else {
            logger.debug("Shard \(shard.id): \(duration, format: .fixed(precision: 1))s at \(startTime, format: .fixed(precision: 1))s")
        }
    }

    // MARK: - Content type mapping

    private static func fileExtension(for contentType: String) -> String {
        let normalized = contentType.lowercased().trimmingCharacters(in: .whitespaces)

        // Already a file extension.
        switch normalized {
        case "mp3", "m4a", "aac", "wav", "caf", "aiff", "mp4":
            return normalized
        default:
            break
        }

        // MIME type mapping.
        switch normalized {
        case "audio/mpeg", "audio/mp3":
            return "mp3"
        case "audio/aac":
            return "aac"
        case "audio/mp4", "audio/x-m4a", "audio/m4a":
            return "m4a"
        case "audio/wav", "audio/x-wav", "audio/wave":
            return "wav"
        case "audio/aiff", "audio/x-aiff":
            return "aiff"
        default:
            // Default to mp3 -- the most common podcast format.
            return "mp3"
        }
    }
}
