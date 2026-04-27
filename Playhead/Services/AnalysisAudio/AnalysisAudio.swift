// AnalysisAudio.swift
// Audio buffer extraction and format conversion for the analysis pipeline.
//
// Decodes cached podcast audio into 16 kHz mono Float32 shards suitable for
// Apple Speech transcription and feature extraction. Completely separate from
// the playback path — different queue, no shared audio session.

@preconcurrency import AVFoundation
import Foundation
import os
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
    /// 30 s of 16 kHz Float32 mono = 480 000 frames ≈ 1.92 MB. Pre-allocated
    /// once and reused across `converter.convert(...)` calls so the converter
    /// can stream a 5-hour episode without ever holding more than this much
    /// output in memory.
    private static let converterFramesPerCycle: AVAudioFrameCount = 480_000

    /// Hard cap on a single source CMSampleBuffer batch in bytes. AVAssetReader
    /// typically delivers tens of KB per fetch; a single batch above 50 MB is
    /// pathological and surfaced as `decodingFailed` rather than swallowed
    /// silently. Note: this bounds *per-batch* source RAM, not total decode
    /// RAM — total in-flight is bounded separately by per-shard emission.
    static let peakSourceBatchBytesCeiling: Int = 50 * 1024 * 1024

    // MARK: - Output format

    /// 16 kHz mono Float32 — the canonical analysis format.
    private let outputFormat: AVAudioFormat

    // MARK: - State

    private let logger = Logger(subsystem: "com.playhead", category: "AnalysisAudio")
    private let signposter = OSSignposter(subsystem: "com.playhead", category: "AnalysisAudio")
    private var activeTasks: [String: Task<[AnalysisShard], Error>] = [:]

    // MARK: - Instrumentation (test seam)

    /// Peak observed source CMSampleBuffer batch size in bytes for the most
    /// recent `performDecode` invocation. Updated per CMSampleBuffer fetch.
    /// Exposed for memory-budget regression tests.
    private(set) var peakSourceBatchBytes: Int = 0

    /// Peak observed per-shard accumulator size in bytes for the most recent
    /// `performDecode` invocation. Bounds total in-flight Float32 output RAM:
    /// at most one full shard plus the pre-allocated 30 s converter output
    /// buffer. Exposed for memory-budget regression tests.
    private(set) var peakShardAccumulatorBytes: Int = 0

    /// Cumulative count of converted output samples produced by the most
    /// recent `performDecode` invocation. Exposed for tests.
    private(set) var cumulativeOutputSamples: Int = 0

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

        // 6. Stream-decode the asset through AVAudioConverter and emit
        //    shards as samples are produced.
        //
        //    Pre-fix shape (playhead-s8dq): the loop appended every
        //    CMSampleBuffer's worth of source-rate Float32 into a single
        //    `[Float]` array, then handed the entire array to a one-shot
        //    `convertSamples` after the loop. On a 5-hour 44.1 kHz episode
        //    that array is 3.18 GiB — exact match to the production OOM
        //    `failed to allocate 3221225440 bytes of memory with alignment 8`.
        //    `convertSamples` then allocated a matching-size AVAudioPCMBuffer,
        //    so peak in-flight was ~6 GiB.
        //
        //    Streaming shape:
        //    * A single outer `converter.convert(...)` call is driven by an
        //      `inputBlock` that pulls one CMSampleBuffer at a time and
        //      decodes it into a per-batch source AVAudioPCMBuffer.
        //    * The converter output buffer is pre-allocated once
        //      (`converterFramesPerCycle` frames ≈ 30 s of 16 kHz Float32
        //      ≈ 1.92 MB) and reused.
        //    * Each cycle's produced frames are appended to a per-shard
        //      accumulator. When the accumulator hits `samplesPerShard`,
        //      the shard is sealed, persistently appended, and the
        //      accumulator reset. This is the change vs. the prior
        //      half-fix: total in-flight Float32 output is now bounded to
        //      one shard's worth (~1.92 MB at 30 s / 16 kHz) instead of
        //      growing linearly with episode duration.
        //    * Converter state — including sample-rate FIR taps — carries
        //      across calls, so the result matches a one-shot decode
        //      within fp tolerance.
        //    See comparable streaming uses of AVAudioConverter in
        //    StreamingAudioDecoder.swift.
        let samplesPerShard = Int(shardDuration * Self.targetSampleRate)
        guard samplesPerShard > 0 else {
            throw AnalysisAudioError.decodingFailed(
                "shardDuration must be > 0 (got \(shardDuration))"
            )
        }
        var shards: [AnalysisShard] = []
        var shardSamples: [Float] = []
        shardSamples.reserveCapacity(samplesPerShard)
        var shardIndex = 0
        var shardStartSampleOffset = 0
        var peakShardBytes = 0

        // Reset per-decode instrumentation. Updated inside the inputBlock
        // and the convert-loop body; surfaced via `peakSourceBatchBytes`,
        // `peakShardAccumulatorBytes`, and `cumulativeOutputSamples` for
        // the streaming-RAM regression tests
        // (PlayheadTests/Services/AnalysisAudio/AnalysisAudioStreamingTests).
        peakSourceBatchBytes = 0
        peakShardAccumulatorBytes = 0
        cumulativeOutputSamples = 0

        // Seal the current `shardSamples` into an AnalysisShard, append it
        // to `shards`, and reset the accumulator. Inlined as a closure so
        // the streaming convert loop and the post-loop tail can share it.
        func emitShard() {
            let count = shardSamples.count
            guard count > 0 else { return }
            let shard = AnalysisShard(
                id: shardIndex,
                episodeID: episodeID,
                startTime: Double(shardStartSampleOffset) / Self.targetSampleRate,
                duration: Double(count) / Self.targetSampleRate,
                samples: shardSamples
            )
            shards.append(shard)
            shardIndex += 1
            shardStartSampleOffset += count
            // Replace rather than `removeAll(keepingCapacity:)` — the
            // newly-constructed AnalysisShard now shares the buffer with
            // `shardSamples` via Array's COW, so keeping the old buffer
            // would force a copy on the next append. A fresh buffer with
            // re-reserved capacity gives steady-state amortized O(1)
            // appends across the whole decode.
            shardSamples = []
            shardSamples.reserveCapacity(samplesPerShard)
        }

        // Pre-allocate the single output buffer reused across every
        // converter.convert(...) call. ~1.92 MB at 16 kHz Float32 mono.
        guard let outputBuffer = AVAudioPCMBuffer(
            pcmFormat: outputFormat,
            frameCapacity: Self.converterFramesPerCycle
        ) else {
            throw AnalysisAudioError.converterSetupFailed
        }

        // The inputBlock and the convert-loop both need to read/write
        // these. `nonisolated(unsafe)` is safe because AVAudioConverter
        // calls the inputBlock synchronously from the same call as
        // `converter.convert(...)`; there is no concurrent access.
        nonisolated(unsafe) var inputAborted = false
        nonisolated(unsafe) var lastInputBlockError: Error?
        nonisolated(unsafe) var peakBatchBytes: Int = 0

        // Pull the next CMSampleBuffer-backed AVAudioPCMBuffer.
        // - Returns `.buffer(b)` with a freshly-allocated PCM buffer.
        // - Returns `.skip` on a transient/empty CMSampleBuffer the
        //   converter should ignore (caller should retry the next one).
        // - Returns `.endOfStream` once the reader is exhausted or the
        //   decode has been aborted (cancellation / error / size cap).
        enum PullResult {
            case buffer(AVAudioPCMBuffer)
            case skip
            case endOfStream
        }

        // Per-CMSampleBuffer fetch + decode. Only the inputBlock calls
        // this; isolated to keep the convert loop body small.
        let pullNextBuffer: @Sendable () -> PullResult = {
            // Cancellation check — the inputBlock cannot throw, so we
            // surface cancellation by signalling end-of-stream and
            // re-throwing in the post-convert path below.
            if Task.isCancelled {
                inputAborted = true
                return .endOfStream
            }

            guard let sampleBuffer = trackOutput.copyNextSampleBuffer() else {
                // Reader is either exhausted (.completed) or failed —
                // disambiguated in the post-convert reader.status check.
                return .endOfStream
            }

            guard let blockBuffer = CMSampleBufferGetDataBuffer(sampleBuffer) else {
                // Skip empty sample buffers — same behaviour as pre-fix.
                return .skip
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
                return .skip
            }

            let floatCount = totalLength / MemoryLayout<Float>.size
            guard floatCount > 0 else { return .skip }

            // Track per-batch in-flight size. Cap at the configured
            // ceiling — defensive: if a single CMSampleBuffer ever grew
            // unboundedly, we'd surface that as an error rather than
            // silently OOM again.
            let batchBytes = totalLength
            if batchBytes > peakBatchBytes { peakBatchBytes = batchBytes }
            if batchBytes > Self.peakSourceBatchBytesCeiling {
                lastInputBlockError = AnalysisAudioError.decodingFailed(
                    "Source CMSampleBuffer exceeded \(Self.peakSourceBatchBytesCeiling) bytes"
                )
                inputAborted = true
                return .endOfStream
            }

            // Allocate a per-batch source PCM buffer the converter can
            // consume. AVAudioConverter does not retain the buffer past
            // the next inputBlock call, so a fresh allocation per batch
            // is the cheapest correct shape. ~tens of KB per batch.
            guard let pcm = AVAudioPCMBuffer(
                pcmFormat: sourceFormat,
                frameCapacity: AVAudioFrameCount(floatCount)
            ) else {
                lastInputBlockError = AnalysisAudioError.converterSetupFailed
                inputAborted = true
                return .endOfStream
            }
            pcm.frameLength = AVAudioFrameCount(floatCount)
            let floatPtr = UnsafeRawPointer(ptr).bindMemory(
                to: Float.self, capacity: floatCount
            )
            pcm.floatChannelData![0].update(from: floatPtr, count: floatCount)
            return .buffer(pcm)
        }

        let inputBlock: AVAudioConverterInputBlock = { _, outStatus in
            if inputAborted {
                outStatus.pointee = .endOfStream
                return nil
            }

            // Loop past skippable (transient/empty) sample buffers so
            // they don't terminate the stream prematurely — same intent
            // as the pre-fix `continue` in the read loop. Each call to
            // the inputBlock returns at most one PCM buffer; the previous
            // one's strong ref is owned by the converter until its next
            // inputBlock invocation, then released — bounding in-flight
            // source RAM to ~one CMSampleBuffer batch.
            while true {
                switch pullNextBuffer() {
                case .buffer(let pcm):
                    outStatus.pointee = .haveData
                    return pcm
                case .skip:
                    continue
                case .endOfStream:
                    outStatus.pointee = .endOfStream
                    return nil
                }
            }
        }

        // Wrap the streaming decode in an os_signpost interval so future
        // regressions in decode latency or memory can be measured in
        // Instruments without re-instrumenting. Per-batch events are
        // emitted from the convert loop below.
        let signpostState = signposter.beginInterval("decode-streaming", "\(episodeID)")
        defer { signposter.endInterval("decode-streaming", signpostState) }

        // Drive the converter: each call fills (or partially fills) the
        // pre-allocated output buffer, then we copy out and continue. The
        // converter signals `.endOfStream` only when our inputBlock has
        // returned nil + `.endOfStream`.
        var conversionError: NSError?
        var done = false
        while !done {
            try Task.checkCancellation()

            outputBuffer.frameLength = 0
            let status = converter.convert(
                to: outputBuffer,
                error: &conversionError,
                withInputFrom: inputBlock
            )

            switch status {
            case .haveData:
                break
            case .inputRanDry:
                // Per Apple docs `.inputRanDry` shouldn't surface when the
                // inputBlock returns endOfStream itself — but if it does,
                // and the converter produced no frames this cycle, advance
                // to end-of-stream rather than spinning. Combined with the
                // 0-frame guard below, this prevents the convert loop from
                // looping forever on a misbehaving converter.
                if outputBuffer.frameLength == 0 || inputAborted {
                    done = true
                }
            case .endOfStream:
                done = true
            case .error:
                // Prefer the more specific inputBlock error if one was set
                // — the generic converter error is typically a downstream
                // symptom of the inputBlock failure.
                if let inputError = lastInputBlockError {
                    throw inputError
                }
                throw AnalysisAudioError.decodingFailed(
                    conversionError?.localizedDescription ?? "AVAudioConverter error"
                )
            @unknown default:
                throw AnalysisAudioError.decodingFailed(
                    "AVAudioConverter returned unknown status"
                )
            }

            // Drain whatever the converter produced this cycle into the
            // active shard accumulator. Emit shards as soon as they hit
            // `samplesPerShard` so the in-flight Float32 output is bounded
            // to one shard's worth (~1.92 MB at 30 s / 16 kHz) regardless
            // of how long the source episode is.
            let producedFrames = Int(outputBuffer.frameLength)
            if producedFrames > 0, let outData = outputBuffer.floatChannelData {
                let basePtr = outData[0]
                var copied = 0
                while copied < producedFrames {
                    try Task.checkCancellation()
                    let room = max(samplesPerShard - shardSamples.count, 0)
                    let chunk = min(producedFrames - copied, room)
                    if chunk > 0 {
                        let slice = UnsafeBufferPointer(
                            start: basePtr.advanced(by: copied),
                            count: chunk
                        )
                        shardSamples.append(contentsOf: slice)
                        copied += chunk
                    }
                    let shardBytes = shardSamples.count * MemoryLayout<Float>.size
                    if shardBytes > peakShardBytes { peakShardBytes = shardBytes }
                    if shardSamples.count >= samplesPerShard {
                        emitShard()
                    }
                }
                cumulativeOutputSamples += producedFrames
                signposter.emitEvent(
                    "decode-batch",
                    "produced_frames=\(producedFrames) peak_source_bytes=\(peakBatchBytes) cumulative_out=\(self.cumulativeOutputSamples)"
                )
            } else if status == .haveData {
                // Defensive: `.haveData` with zero frames would otherwise
                // spin forever. Treat as effective end-of-stream.
                done = true
            }
        }

        // Emit the tail shard (anything < samplesPerShard left over).
        emitShard()

        // Surface the final peaks to the actor's instrumentation seam.
        peakSourceBatchBytes = peakBatchBytes
        peakShardAccumulatorBytes = peakShardBytes

        // 7. Check inputBlock-side errors and reader status. inputBlock
        //    error wins over reader.status — the input block sees the
        //    failure first.
        if let inputError = lastInputBlockError {
            throw inputError
        }
        if Task.isCancelled {
            throw AnalysisAudioError.cancelled
        }
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
        //    `cumulativeOutputSamples` is the streaming-equivalent of the
        //    pre-fix `allSamples.count` and is the source of truth for
        //    decoded duration now that no whole-episode buffer exists.
        let decodedDuration = Double(cumulativeOutputSamples) / Self.targetSampleRate
        let isTruncated = assetDuration > 0
            && decodedDuration < assetDuration * (1.0 - Self.truncationTolerance)

        // 9. Persist shards for reuse — but only when the file was fully decoded.
        //     Truncated files (still downloading) must not be cached, otherwise
        //     the partial result is returned permanently even after download completes.
        if !isTruncated {
            ShardCache.saveShards(shards, episodeID: episodeID)
        }

        // 10. Log truncation warning but return partial shards — throwing here
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
    //
    // The previous one-shot `convertSamples(_:using:sourceFormat:)` helper
    // was removed in playhead-s8dq. Streaming conversion is now inlined
    // in `performDecode` step 6, where a single outer `converter.convert`
    // call is driven by an `inputBlock` that pulls one CMSampleBuffer at
    // a time. Inlining keeps the per-batch source/output buffers visible
    // alongside the cancellation + reader-status checks, and makes the
    // peak-RAM budget explicit (one source batch + one ~1.92 MB output
    // buffer in flight at a time).
}

// MARK: - AnalysisAudioProviding Conformance

extension AnalysisAudioService: AnalysisAudioProviding {}
