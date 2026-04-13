// TranscriptEngineService.swift
// Orchestrates on-device transcription for the analysis pipeline.
//
// Accepts decoded audio shards from AnalysisAudioService, runs them through
// Apple Speech via SpeechService, and writes TranscriptChunks to SQLite.
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
        modelVersion: "apple-speech-v1"
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
    case speechServiceNotReady
    case chunkingFailed(String)

    var description: String {
        switch self {
        case .noShardsAvailable:
            "No analysis shards available for transcription"
        case .speechServiceNotReady:
            "Speech engine is not ready"
        case .chunkingFailed(let reason):
            "Chunk boundary computation failed: \(reason)"
        }
    }
}

enum TranscriptEngineEvent: Sendable {
    case chunksPersisted(analysisAssetId: String, chunks: [TranscriptChunk])
    case completed(analysisAssetId: String)
}

// MARK: - TranscriptEngineService

/// Orchestrates transcription of decoded audio shards into TranscriptChunks
/// persisted to SQLite. Manages prioritization around the playhead, handles
/// scrub reprioritization, and supports resumable checkpointing.
actor TranscriptEngineService {

    private let logger = Logger(subsystem: "com.playhead", category: "TranscriptEngineService")

    private let speechService: SpeechService
    private let store: AnalysisStore
    private let config: TranscriptEngineServiceConfig

    /// Currently active transcription task, cancelled on scrubs or shutdown.
    private var activeTask: Task<Void, Never>?

    /// The analysis asset ID currently being processed.
    private var activeAssetId: String?

    /// The podcast ID currently being processed, used for ASR vocabulary
    /// biasing on the SpeechAnalyzer path.
    private var activePodcastId: String?

    /// Last known playback snapshot for priority computation.
    private var latestSnapshot: PlaybackSnapshot?

    /// Running chunk index counter per asset, for ordering.
    private var chunkCounter: Int = 0

    /// Shards queued for processing while the main loop is running.
    private var appendedShards: [AnalysisShard] = []

    /// True while the transcription loop is actively processing.
    /// Used by appendShards to decide whether to start a new loop.
    private var loopRunning: Bool = false

    /// Broadcasts persisted chunk batches and completion signals to the
    /// analysis coordinator without forcing it to poll SQLite.
    private var eventContinuations: [UUID: AsyncStream<TranscriptEngineEvent>.Continuation] = [:]

    // MARK: - Init

    init(
        speechService: SpeechService,
        store: AnalysisStore,
        config: TranscriptEngineServiceConfig = .default
    ) {
        self.speechService = speechService
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
        snapshot: PlaybackSnapshot,
        podcastId: String? = nil
    ) {
        // Cancel any existing work — we're starting fresh or reprioritizing.
        activeTask?.cancel()

        // A fresh start should not inherit queued append work from a prior
        // loop. When the asset changes, also reset the per-asset chunk index.
        if activeAssetId != analysisAssetId {
            chunkCounter = 0
        }
        appendedShards = []

        activeAssetId = analysisAssetId
        activePodcastId = podcastId
        latestSnapshot = snapshot

        activeTask = Task { [weak self] in
            guard let self else { return }
            await self.runTranscriptionLoop(
                shards: shards,
                analysisAssetId: analysisAssetId
            )
            await self.clearActiveTask()
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
            snapshot: snapshot,
            podcastId: activePodcastId
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
            snapshot: snapshot,
            podcastId: activePodcastId
        )
    }

    /// Append new shards to the running transcription without cancelling.
    /// If no transcription is active, starts a new loop.
    func appendShards(
        _ newShards: [AnalysisShard],
        analysisAssetId: String,
        snapshot: PlaybackSnapshot
    ) {
        guard !newShards.isEmpty else { return }
        latestSnapshot = snapshot

        // The transcript engine is single-asset. If a late append arrives for
        // an asset that is no longer active, drop it instead of mixing old
        // streaming output into the current session.
        if let activeAssetId, activeAssetId != analysisAssetId {
            logger.info(
                "Dropping \(newShards.count) appended shards for stale asset \(analysisAssetId); active asset is \(activeAssetId)"
            )
            return
        }

        if activeAssetId == nil {
            activeAssetId = analysisAssetId
            chunkCounter = 0
        }

        appendedShards.append(contentsOf: newShards)

        // If no active loop, start one for the appended shards.
        if !loopRunning {
            activeAssetId = analysisAssetId
            activeTask = Task { [weak self] in
                guard let self else { return }
                await self.runTranscriptionLoop(
                    shards: [],  // empty — the loop will pick up from appendedShards
                    analysisAssetId: analysisAssetId
                )
            }
        }
    }

    /// Stop all transcription work (e.g., episode ended or user switched).
    func stop() {
        activeTask?.cancel()
        activeTask = nil
        activeAssetId = nil
        activePodcastId = nil
        latestSnapshot = nil
        chunkCounter = 0
        appendedShards = []
        loopRunning = false
    }

    /// Clear the active task reference when the loop completes,
    /// so appendShards knows to start a new loop.
    private func clearActiveTask() {
        activeTask = nil
    }

    /// Whether transcription is currently in progress.
    var isActive: Bool {
        activeTask != nil && activeTask?.isCancelled == false
    }

    func events() -> AsyncStream<TranscriptEngineEvent> {
        let id = UUID()
        return AsyncStream { continuation in
            self.eventContinuations[id] = continuation
            continuation.onTermination = { @Sendable _ in
                Task { [weak self] in
                    await self?.removeEventContinuation(id: id)
                }
            }
        }
    }

    // MARK: - Transcription loop

    private func runTranscriptionLoop(
        shards: [AnalysisShard],
        analysisAssetId: String
    ) async {
        loopRunning = true
        defer { loopRunning = false }
        logger.info("Starting transcription loop: \(shards.count) shards for asset \(analysisAssetId)")
        let loopStart = ContinuousClock.now

        guard !shards.isEmpty || !appendedShards.isEmpty else {
            logger.warning("No shards to transcribe")
            return
        }

        guard await speechService.isReady() else {
            logger.error("Speech engine not ready — aborting transcription")
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

            // The coverage watermark is a high-water mark from ahead-of-playhead
            // processing. Behind-playhead shards may not have been transcribed
            // even if their time range falls within the watermark. Use per-shard
            // fingerprint dedup (in transcribeShard) instead of skipping here.

            do {
                try await transcribeShard(
                    shard,
                    analysisAssetId: analysisAssetId
                )
            } catch is CancellationError {
                logger.info("Transcription cancelled during shard \(shard.id)")
                return
            } catch {
                logger.error("""
                    Transcription failed for shard \(shard.id) \
                    [start=\(String(format: "%.2f", shard.startTime))s, \
                    duration=\(String(format: "%.2f", shard.duration))s, \
                    samples=\(shard.sampleCount), \
                    episode=\(shard.episodeID)]: \(error)
                    """)
                // Continue with next shard — partial coverage is better than none.
                continue
            }
        }

        // Drain any shards that were appended while we were processing.
        while !appendedShards.isEmpty {
            let newBatch = appendedShards
            appendedShards = []

            let newPrioritized = prioritizeShards(
                newBatch,
                existingCoverage: existingCoverage
            )

            for shard in newPrioritized {
                guard !Task.isCancelled else {
                    logger.info("Transcription cancelled during appended batch")
                    return
                }
                do {
                    try await transcribeShard(shard, analysisAssetId: analysisAssetId)
                } catch is CancellationError {
                    logger.info("Transcription cancelled during appended shard \(shard.id)")
                    return
                } catch {
                    logger.error("""
                        Transcription failed for appended shard \(shard.id) \
                        [start=\(String(format: "%.2f", shard.startTime))s]: \(error)
                        """)
                    continue
                }
            }
        }

        // Verify the first shard was transcribed. If the first 30s is missing,
        // transcribe shard 0 explicitly.
        if let firstShard = shards.first(where: { $0.id == 0 }) {
            let hasFirst = try? await store.hasTranscriptChunk(
                analysisAssetId: analysisAssetId,
                segmentFingerprint: computeFingerprint(
                    text: "", startTime: 0, endTime: 0 // won't match — check by time range instead
                )
            )
            // Simpler: check if any chunk starts before 30s.
            let allChunks = (try? await store.fetchTranscriptChunks(assetId: analysisAssetId)) ?? []
            let hasEarlyChunk = allChunks.contains { $0.startTime < 30 }
            if !hasEarlyChunk {
                logger.warning("First 30s missing — transcribing shard 0")
                try? await transcribeShard(firstShard, analysisAssetId: analysisAssetId)
            }
        }

        let loopElapsed = ContinuousClock.now - loopStart
        logger.info("Transcription loop complete for asset \(analysisAssetId) in \(loopElapsed)")
        emitEvent(.completed(analysisAssetId: analysisAssetId))
    }

    // MARK: - Single shard transcription

    private func transcribeShard(
        _ shard: AnalysisShard,
        analysisAssetId: String
    ) async throws {
        try Task.checkCancellation()

        // Run Apple Speech transcription.
        let segments = try await speechService.transcribe(shard: shard, podcastId: activePodcastId)

        guard !segments.isEmpty else {
            logger.debug("No segments from shard \(shard.id) — silence or noise")
            // Still update coverage so we don't re-process.
            try await updateCoverage(
                analysisAssetId: analysisAssetId,
                endTime: shard.startTime + shard.duration
            )
            return
        }

        // Convert segments to TranscriptChunks and persist. Metadata upgrades on
        // duplicate fingerprints re-emit the upgraded chunk but must not be
        // inserted as a new row.
        var chunksToInsert: [TranscriptChunk] = []
        var emittedChunks: [TranscriptChunk] = []

        for segment in segments {
            try Task.checkCancellation()

            let fingerprint = computeFingerprint(
                text: segment.text,
                startTime: segment.startTime,
                endTime: segment.endTime
            )

            // Dedup: preserve the existing row, but let later passes upgrade
            // weak-anchor metadata when the same text/timing arrives with
            // richer recovery text.
            if let existingChunk = try await store.fetchTranscriptChunk(
                analysisAssetId: analysisAssetId,
                segmentFingerprint: fingerprint
            ) {
                let mergedMetadata = mergedWeakAnchorMetadata(
                    existing: existingChunk.weakAnchorMetadata,
                    candidate: segment.weakAnchorMetadata
                )
                if mergedMetadata != existingChunk.weakAnchorMetadata {
                    let didUpdate = try await store.updateTranscriptChunkWeakAnchorMetadata(
                        analysisAssetId: analysisAssetId,
                        segmentFingerprint: fingerprint,
                        weakAnchorMetadata: mergedMetadata
                    )
                    if didUpdate {
                        emittedChunks.append(
                            TranscriptChunk(
                                id: existingChunk.id,
                                analysisAssetId: existingChunk.analysisAssetId,
                                segmentFingerprint: existingChunk.segmentFingerprint,
                                chunkIndex: existingChunk.chunkIndex,
                                startTime: existingChunk.startTime,
                                endTime: existingChunk.endTime,
                                text: existingChunk.text,
                                normalizedText: existingChunk.normalizedText,
                                pass: existingChunk.pass,
                                modelVersion: existingChunk.modelVersion,
                                transcriptVersion: existingChunk.transcriptVersion,
                                atomOrdinal: existingChunk.atomOrdinal,
                                weakAnchorMetadata: mergedMetadata
                            )
                        )
                    }
                } else {
                    logger.debug("Skipping duplicate segment: \(fingerprint.prefix(8))")
                }
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
                modelVersion: config.modelVersion,
                transcriptVersion: nil,
                atomOrdinal: nil,
                weakAnchorMetadata: segment.weakAnchorMetadata
            )
            chunksToInsert.append(chunk)
            emittedChunks.append(chunk)
            chunkCounter += 1
        }

        // Batch-insert to SQLite.
        if !chunksToInsert.isEmpty {
            try await store.insertTranscriptChunks(chunksToInsert)
        }
        if !emittedChunks.isEmpty {
            emitEvent(.chunksPersisted(analysisAssetId: analysisAssetId, chunks: emittedChunks))
        }

        // Update coverage watermark.
        let shardEnd = shard.startTime + shard.duration
        try await updateCoverage(
            analysisAssetId: analysisAssetId,
            endTime: shardEnd
        )

        logger.info("Wrote \(emittedChunks.count) chunks for shard \(shard.id) [\(String(format: "%.1f", shard.startTime))-\(String(format: "%.1f", shardEnd))s]")
    }

    // MARK: - Prioritization

    /// Order shards so that those nearest the playhead (and ahead of it)
    /// are processed first. Shards behind the playhead are deprioritized.
    private func prioritizeShards(
        _ shards: [AnalysisShard],
        existingCoverage: Double
    ) -> [AnalysisShard] {
        guard let snapshot = latestSnapshot else {
            return shards
        }
        return Self.prioritizeShards(
            shards,
            playhead: snapshot.playheadTime,
            playbackRate: snapshot.playbackRate,
            chunkOverlap: config.chunkOverlap,
            lookaheadWallClockSeconds: config.lookaheadWallClockSeconds
        )
    }

    static func prioritizeShards(
        _ shards: [AnalysisShard],
        playhead: Double,
        playbackRate: Double,
        chunkOverlap: TimeInterval,
        lookaheadWallClockSeconds: TimeInterval
    ) -> [AnalysisShard] {
        let rate = max(playbackRate, 1.0)
        let lookaheadAudioSeconds = lookaheadWallClockSeconds * rate

        let ahead = shards
            .filter { $0.startTime >= playhead - chunkOverlap }
            .sorted { $0.startTime < $1.startTime }

        let behind = shards
            .filter { $0.startTime < playhead - chunkOverlap }
            .sorted { $0.startTime > $1.startTime }

        let shard0 = behind.filter { $0.startTime == 0 }
        let behindWithoutShard0 = behind.filter { $0.startTime > 0 }

        let hotPath = ahead.filter { $0.startTime < playhead + lookaheadAudioSeconds }
        let coldAhead = ahead.filter { $0.startTime >= playhead + lookaheadAudioSeconds }

        return shard0 + hotPath + coldAhead + behindWithoutShard0
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

    private func mergedWeakAnchorMetadata(
        existing: TranscriptWeakAnchorMetadata?,
        candidate: TranscriptWeakAnchorMetadata?
    ) -> TranscriptWeakAnchorMetadata? {
        guard let candidate else { return existing }
        guard candidate.hasRecoveryText else { return existing }
        guard let existing else { return candidate }
        guard existing.hasRecoveryText else { return candidate }
        return existing.merged(with: candidate)
    }

    // MARK: - Text normalization

    /// Normalize text for FTS indexing. Lowercases, strips punctuation,
    /// collapses whitespace.
    ///
    /// Exposed as `internal static` so test fixtures (e.g. real-episode
    /// benchmark fixtures) can produce `chunk.normalizedText` that matches
    /// production exactly. Any change to this function automatically
    /// flows through to the test pipeline. Do not call from app code
    /// outside this service — use the instance method delegating below.
    static func normalizeText(_ text: String) -> String {
        text.lowercased()
            .components(separatedBy: CharacterSet.alphanumerics.inverted)
            .filter { !$0.isEmpty }
            .joined(separator: " ")
    }

    private func normalizeText(_ text: String) -> String {
        Self.normalizeText(text)
    }

    private func removeEventContinuation(id: UUID) {
        eventContinuations.removeValue(forKey: id)
    }

    private func emitEvent(_ event: TranscriptEngineEvent) {
        for continuation in eventContinuations.values {
            continuation.yield(event)
        }
    }
}
