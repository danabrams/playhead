// TranscriptEngineService.swift
// Orchestrates on-device transcription for the analysis pipeline.
//
// Accepts decoded audio shards from AnalysisAudioService, runs them through
// WhisperKit via WhisperKitService, and writes TranscriptChunks to SQLite.
//
// Processing strategy:
//   - VAD/pause-anchored chunks (target 8-20 s with small overlap)
//   - Dynamic wall-clock safety margin ahead of the playhead
//   - Hot-path coverage independent from final-pass completeness
//   - Immediate cancel/reprioritize on scrubs and speed changes
//   - Checkpoint per chunk hash for resumability
//   - Stream fast-pass chunks as they complete
//   - Promote to final-pass when idle/charging

import CryptoKit
import Foundation
import OSLog

// MARK: - Configuration

struct TranscriptEngineServiceConfig: Sendable {
    /// Target chunk duration in seconds for VAD-anchored splits.
    let targetChunkDuration: TimeInterval
    /// Minimum chunk duration in seconds.
    let minChunkDuration: TimeInterval
    /// Maximum chunk duration in seconds.
    let maxChunkDuration: TimeInterval
    /// Overlap between consecutive chunks in seconds.
    let chunkOverlap: TimeInterval
    /// Wall-clock seconds of lookahead to maintain ahead of the playhead.
    let lookaheadWallClockSeconds: TimeInterval
    /// Minimum speech probability to consider a VAD frame as speech.
    let vadSpeechThreshold: Float
    /// Model version tag written to each TranscriptChunk.
    let modelVersion: String

    static let `default` = TranscriptEngineServiceConfig(
        targetChunkDuration: 12.0,
        minChunkDuration: 8.0,
        maxChunkDuration: 20.0,
        chunkOverlap: 0.5,
        lookaheadWallClockSeconds: 30.0,
        vadSpeechThreshold: 0.5,
        modelVersion: "whisper-tiny-v1"
    )
}

// MARK: - Playback state snapshot

/// Snapshot of playback state used to compute transcription priorities.
/// Provided by the caller (e.g., AnalysisCoordinator) each time
/// transcription is kicked or the playhead moves significantly.
struct PlaybackSnapshot: Sendable {
    /// Current playhead position in audio seconds.
    let playheadTime: TimeInterval
    /// Current playback rate (1.0 = normal, 2.0 = 2x, etc.).
    let playbackRate: Double
    /// Whether playback is actively playing.
    let isPlaying: Bool
}

// MARK: - TranscriptEngineServiceError

enum TranscriptEngineServiceError: Error, CustomStringConvertible {
    case noShardsAvailable
    case whisperKitNotReady
    case chunkingFailed(String)

    var description: String {
        switch self {
        case .noShardsAvailable:
            "No analysis shards available for transcription"
        case .whisperKitNotReady:
            "WhisperKitService is not ready — no model loaded"
        case .chunkingFailed(let reason):
            "Chunk boundary computation failed: \(reason)"
        }
    }
}

// MARK: - TranscriptEngineService

/// Orchestrates transcription of decoded audio shards into TranscriptChunks
/// persisted to SQLite. Manages prioritization around the playhead, handles
/// scrub reprioritization, and supports resumable checkpointing.
actor TranscriptEngineService {

    private let logger = Logger(subsystem: "com.playhead", category: "TranscriptEngineService")

    private let whisperKit: WhisperKitService
    private let store: AnalysisStore
    private let config: TranscriptEngineServiceConfig

    /// Currently active transcription task, cancelled on scrubs or shutdown.
    private var activeTask: Task<Void, Never>?

    /// The analysis asset ID currently being processed.
    private var activeAssetId: String?

    /// Last known playback snapshot for priority computation.
    private var latestSnapshot: PlaybackSnapshot?

    /// Running chunk index counter per asset, for ordering.
    private var chunkCounter: Int = 0

    // MARK: - Init

    init(
        whisperKit: WhisperKitService,
        store: AnalysisStore,
        config: TranscriptEngineServiceConfig = .default
    ) {
        self.whisperKit = whisperKit
        self.store = store
        self.config = config
    }

    // MARK: - Public API

    /// Start or resume transcription for an episode.
    ///
    /// Transcribes shards in priority order (near the playhead first),
    /// writing chunks to SQLite as each completes. The operation is
    /// cancellable and resumable.
    ///
    /// - Parameters:
    ///   - shards: Decoded audio shards from AnalysisAudioService.
    ///   - analysisAssetId: The analysis asset these shards belong to.
    ///   - snapshot: Current playback state for prioritization.
    func startTranscription(
        shards: [AnalysisShard],
        analysisAssetId: String,
        snapshot: PlaybackSnapshot
    ) {
        // Cancel any existing work — we're starting fresh or reprioritizing.
        activeTask?.cancel()

        activeAssetId = analysisAssetId
        latestSnapshot = snapshot

        activeTask = Task { [weak self] in
            guard let self else { return }
            await self.runTranscriptionLoop(
                shards: shards,
                analysisAssetId: analysisAssetId
            )
        }
    }

    /// Notify the service that the playhead has scrubbed to a new position.
    /// Cancels in-flight work and restarts with new priorities.
    func handleScrub(
        shards: [AnalysisShard],
        analysisAssetId: String,
        snapshot: PlaybackSnapshot
    ) {
        logger.info("Scrub detected — reprioritizing from \(snapshot.playheadTime, format: .fixed(precision: 1))s")
        startTranscription(
            shards: shards,
            analysisAssetId: analysisAssetId,
            snapshot: snapshot
        )
    }

    /// Notify the service that playback speed changed significantly.
    /// Updates the snapshot and reprioritizes — the lookahead window scales
    /// with playback rate, so in-flight ordering may be stale.
    func handleSpeedChange(
        shards: [AnalysisShard],
        analysisAssetId: String,
        snapshot: PlaybackSnapshot
    ) {
        logger.info("Speed changed to \(snapshot.playbackRate, format: .fixed(precision: 1))x — reprioritizing")
        startTranscription(
            shards: shards,
            analysisAssetId: analysisAssetId,
            snapshot: snapshot
        )
    }

    /// Stop all transcription work (e.g., episode ended or user switched).
    func stop() {
        activeTask?.cancel()
        activeTask = nil
        activeAssetId = nil
        latestSnapshot = nil
        chunkCounter = 0
    }

    /// Whether transcription is currently in progress.
    var isActive: Bool {
        activeTask != nil && activeTask?.isCancelled == false
    }

    // MARK: - Transcription loop

    private func runTranscriptionLoop(
        shards: [AnalysisShard],
        analysisAssetId: String
    ) async {
        guard !shards.isEmpty else {
            logger.warning("No shards to transcribe")
            return
        }

        guard await whisperKit.isReady() else {
            logger.error("WhisperKit not ready — aborting transcription")
            return
        }

        // Load existing coverage to skip already-transcribed regions.
        let existingCoverage: Double
        do {
            let asset = try await store.fetchAsset(id: analysisAssetId)
            existingCoverage = asset?.fastTranscriptCoverageEndTime ?? 0
        } catch {
            logger.error("Failed to fetch asset coverage: \(error)")
            existingCoverage = 0
        }

        // Prioritize shards by proximity to the playhead.
        let prioritized = prioritizeShards(
            shards,
            existingCoverage: existingCoverage
        )

        for shard in prioritized {
            guard !Task.isCancelled else {
                logger.info("Transcription cancelled")
                return
            }

            // Skip shards fully covered.
            let shardEnd = shard.startTime + shard.duration
            if shardEnd <= existingCoverage { continue }

            do {
                try await transcribeShard(
                    shard,
                    analysisAssetId: analysisAssetId
                )
            } catch is CancellationError {
                logger.info("Transcription cancelled during shard \(shard.id)")
                return
            } catch {
                logger.error("Transcription failed for shard \(shard.id): \(error)")
                // Continue with next shard — partial coverage is better than none.
                continue
            }
        }

        logger.info("Transcription loop complete for asset \(analysisAssetId)")
    }

    // MARK: - Single shard transcription

    private func transcribeShard(
        _ shard: AnalysisShard,
        analysisAssetId: String
    ) async throws {
        try Task.checkCancellation()

        // Run WhisperKit transcription (includes VAD internally).
        let segments = try await whisperKit.transcribe(shard: shard)

        guard !segments.isEmpty else {
            logger.debug("No segments from shard \(shard.id) — silence or noise")
            // Still update coverage so we don't re-process.
            try await updateCoverage(
                analysisAssetId: analysisAssetId,
                endTime: shard.startTime + shard.duration
            )
            return
        }

        // Convert segments to TranscriptChunks and persist.
        var chunks: [TranscriptChunk] = []

        for segment in segments {
            try Task.checkCancellation()

            let fingerprint = computeFingerprint(
                text: segment.text,
                startTime: segment.startTime,
                endTime: segment.endTime
            )

            // Dedup: skip if this fingerprint already exists.
            let exists = try await store.hasTranscriptChunk(
                analysisAssetId: analysisAssetId,
                segmentFingerprint: fingerprint
            )
            if exists {
                logger.debug("Skipping duplicate segment: \(fingerprint.prefix(8))")
                continue
            }

            let chunk = TranscriptChunk(
                id: UUID().uuidString,
                analysisAssetId: analysisAssetId,
                segmentFingerprint: fingerprint,
                chunkIndex: chunkCounter,
                startTime: segment.startTime,
                endTime: segment.endTime,
                text: segment.text,
                normalizedText: normalizeText(segment.text),
                pass: segment.passType.rawValue,
                modelVersion: config.modelVersion
            )
            chunks.append(chunk)
            chunkCounter += 1
        }

        // Batch-insert to SQLite.
        if !chunks.isEmpty {
            try await store.insertTranscriptChunks(chunks)
        }

        // Update coverage watermark.
        let shardEnd = shard.startTime + shard.duration
        try await updateCoverage(
            analysisAssetId: analysisAssetId,
            endTime: shardEnd
        )

        logger.info("Wrote \(chunks.count) chunks for shard \(shard.id) [\(String(format: "%.1f", shard.startTime))-\(String(format: "%.1f", shardEnd))s]")
    }

    // MARK: - Prioritization

    /// Order shards so that those nearest the playhead (and ahead of it)
    /// are processed first. Shards behind the playhead are deprioritized.
    private func prioritizeShards(
        _ shards: [AnalysisShard],
        existingCoverage: Double
    ) -> [AnalysisShard] {
        guard let snapshot = latestSnapshot else {
            // No playback info — process in natural order.
            return shards
        }

        let playhead = snapshot.playheadTime
        let rate = max(snapshot.playbackRate, 1.0)

        // Compute the lookahead window in audio seconds.
        let lookaheadAudioSeconds = config.lookaheadWallClockSeconds * rate

        // Partition: ahead-of-playhead first (sorted by proximity),
        // then behind-playhead (sorted by proximity descending for
        // backfill from recent to old).
        let ahead = shards
            .filter { $0.startTime >= playhead - config.chunkOverlap }
            .filter { $0.startTime + $0.duration > existingCoverage }
            .sorted { $0.startTime < $1.startTime }

        let behind = shards
            .filter { $0.startTime < playhead - config.chunkOverlap }
            .filter { $0.startTime + $0.duration > existingCoverage }
            .sorted { $0.startTime > $1.startTime }

        // Hot path: shards within the lookahead window come first.
        let hotPath = ahead.filter { $0.startTime < playhead + lookaheadAudioSeconds }
        let coldAhead = ahead.filter { $0.startTime >= playhead + lookaheadAudioSeconds }

        return hotPath + coldAhead + behind
    }

    // MARK: - Coverage updates

    private func updateCoverage(
        analysisAssetId: String,
        endTime: Double
    ) async throws {
        try await store.updateFastTranscriptCoverage(
            id: analysisAssetId,
            endTime: endTime
        )
    }

    // MARK: - Fingerprinting

    /// Compute a stable fingerprint for dedup across passes.
    /// Based on content + timing so the same text at a different position
    /// is treated as a distinct chunk.
    private func computeFingerprint(
        text: String,
        startTime: TimeInterval,
        endTime: TimeInterval
    ) -> String {
        let input = "\(text)|\(startTime)|\(endTime)"
        let digest = SHA256.hash(data: Data(input.utf8))
        return digest.prefix(16).map { String(format: "%02x", $0) }.joined()
    }

    // MARK: - Text normalization

    /// Normalize text for FTS indexing. Lowercases, strips punctuation,
    /// collapses whitespace.
    private func normalizeText(_ text: String) -> String {
        text.lowercased()
            .components(separatedBy: CharacterSet.alphanumerics.inverted)
            .filter { !$0.isEmpty }
            .joined(separator: " ")
    }
}
