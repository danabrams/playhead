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

/// Thrown by `transcribeShard` when the playhead-01t8 preemption
/// signal flips after a chunk batch has been persisted. The
/// transcription loop catches this to exit cleanly at the safe point
/// without logging a shard failure.
struct TranscriptEnginePreempted: Error {}

/// playhead-5uvz.5 (Gap-6): thrown by `transcribeShard` when a
/// `stopTranscription(analysisAssetId:)` lands while a shard is
/// in-flight. The transcription loop catches this to exit cleanly
/// without logging a shard failure or persisting partial output for
/// the stopped asset.
struct TranscriptEngineStopped: Error {}

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

    /// True once the caller has explicitly signalled that no more shards
    /// will be appended for the currently active asset (via
    /// `finishAppending(analysisAssetId:)`). The transcription loop will
    /// only emit `.completed` after this flag is set — a momentarily
    /// empty `appendedShards` queue is NOT sufficient.
    ///
    /// Reset to `false` in `startTranscription` / `stop` so a new session
    /// does not inherit the prior session's end-of-input signal.
    private var inputClosed: Bool = false

    /// Continuations waiting for additional shards (or an end-of-input
    /// signal). `waitForMoreShards` appends here; `appendShards`,
    /// `finishAppending`, and `stop` resume every pending continuation.
    private var appendWaiters: [CheckedContinuation<Void, Never>] = []

    /// True while the transcription loop is actively processing.
    /// Used by appendShards to decide whether to start a new loop.
    private var loopRunning: Bool = false

    /// Optional preemption context threaded in by AnalysisJobRunner
    /// (playhead-01t8). Polled after each TranscriptChunk batch
    /// persists; on a preempt request the loop acknowledges and
    /// exits at that safe point.
    private var preemption: PreemptionContext?

    /// Broadcasts persisted chunk batches and completion signals to the
    /// analysis coordinator without forcing it to poll SQLite.
    private var eventContinuations: [UUID: AsyncStream<TranscriptEngineEvent>.Continuation] = [:]

    /// playhead-5uvz.5 (Gap-6): assets the caller explicitly stopped via
    /// `stopTranscription(analysisAssetId:)`. Used to drop late writes,
    /// late event emissions, and any queued append shards that race the
    /// stop. The set is small (one entry per stopped asset) and is
    /// cleared opportunistically when a fresh `startTranscription` is
    /// called for that asset (so a re-run after stop is not silently
    /// suppressed).
    ///
    /// Why a set rather than a single flag: `appendShards` from a
    /// streaming producer can land for an asset that the runner has
    /// already stopped — those late appends must be dropped on contact,
    /// not enqueued and then re-dropped at transcribe time. Tracking
    /// the asset id (rather than just the active id) lets us reject
    /// post-stop appends even if `activeAssetId` has rotated to a
    /// different asset in the meantime.
    private var stoppedAssetIds: Set<String> = []

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
        podcastId: String? = nil,
        preemption: PreemptionContext? = nil
    ) {
        // Cancel any existing work — we're starting fresh or reprioritizing.
        activeTask?.cancel()

        // A fresh start should not inherit queued append work from a prior
        // loop. When the asset changes, also reset the per-asset chunk
        // index and the end-of-input flag. When the asset matches,
        // preserve `inputClosed` so a streaming producer that already
        // finished (and called `finishAppending`) earlier in the pipeline
        // does not get its end-of-input signal silently discarded by this
        // reset.
        if activeAssetId != analysisAssetId {
            chunkCounter = 0
            inputClosed = false
        }
        appendedShards = []
        // playhead-5uvz.5: an explicit `startTranscription` for this
        // asset rescinds any prior `stopTranscription` gate — re-runs
        // are allowed and must not be silently suppressed by a stale
        // stop. We only clear the entry for *this* asset; stops for
        // other assets remain in place.
        stoppedAssetIds.remove(analysisAssetId)
        // Wake any leftover waiters from a previous loop so they exit
        // promptly; this keeps stale continuations from being orphaned.
        resumeAllAppendWaiters()

        activeAssetId = analysisAssetId
        activePodcastId = podcastId
        latestSnapshot = snapshot
        self.preemption = preemption

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
        // playhead-5uvz.5: drop appends targeting an asset that the
        // runner has explicitly stopped. Without this guard a streaming
        // producer racing the timeout could re-arm the session by
        // appending shards (and wake a waiter that is no longer parked
        // in any meaningful sense) right after the runner moved on.
        if stoppedAssetIds.contains(analysisAssetId) {
            logger.info(
                "Dropping \(newShards.count) appended shards for stopped asset \(analysisAssetId)"
            )
            return
        }
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

        // Appending more work cancels any prior end-of-input signal and
        // wakes the loop if it was suspended waiting for shards.
        inputClosed = false
        resumeAllAppendWaiters()

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

    /// Signal that no more shards will be appended for the given asset.
    /// The transcription loop will drain any remaining backlog and then
    /// emit `.completed`.
    ///
    /// The assetId is validated against `activeAssetId`; a mismatch is
    /// logged and ignored so a stale end-of-input signal from a previous
    /// session cannot terminate the current loop early.
    func finishAppending(analysisAssetId: String) {
        guard let activeAssetId else {
            logger.debug("finishAppending(\(analysisAssetId)): no active asset — ignoring")
            return
        }
        guard activeAssetId == analysisAssetId else {
            logger.info(
                "finishAppending(\(analysisAssetId)): stale signal; active asset is \(activeAssetId)"
            )
            return
        }
        inputClosed = true
        resumeAllAppendWaiters()
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
        preemption = nil
        // Close input and release any suspended waiter so a loop that is
        // parked on `waitForMoreShards()` returns promptly when the task
        // is cancelled.
        inputClosed = true
        resumeAllAppendWaiters()
    }

    /// playhead-5uvz.5 (Gap-6): Stop transcription for a specific asset
    /// without disturbing other engine state.
    ///
    /// `AnalysisJobRunner.run` calls this from its 5-minute zero-coverage
    /// timeout branch. Before the fix, the runner would return
    /// `.failed("transcription:zeroCoverage")` while leaving
    /// `TranscriptEngineService` running in the background. The orphan's
    /// subsequent `transcript_chunks` writes and
    /// `analysis_assets.fastTranscriptCoverageEndTime` updates targeted
    /// an `analysisAssetId` whose owning scheduler had already moved on
    /// — so the asset's coverage advanced out-of-band after the job row
    /// was marked failed, confusing both the coverage-guard recovery
    /// path and the partial-coverage gate.
    ///
    /// Contract:
    /// - Cancels the underlying SpeechAnalyzer task if `analysisAssetId`
    ///   matches the active asset. (Mismatch is a no-op — a stale stop
    ///   call must not tear down an unrelated session.)
    /// - Drops any in-flight `appendedShards` tagged for the active
    ///   session (whose asset id, on a match, is the one being stopped).
    /// - Records the asset id so any late `transcribeShard` writes,
    ///   late `appendShards` calls, or late `emitEvent(.completed/...)`
    ///   for that asset are dropped instead of persisted.
    /// - Resumes any waiter parked in `waitForMoreShards()` so the loop
    ///   can observe cancellation and exit promptly.
    ///
    /// Idempotent: stopping an already-stopped asset is a no-op aside
    /// from re-asserting the gate. A subsequent
    /// `startTranscription(...)` for the same asset clears the stopped
    /// flag — explicit re-run is allowed.
    func stopTranscription(analysisAssetId: String) {
        // Always record the stopped asset, even on a stale call. A
        // streaming producer that already started racing more shards
        // toward this asset must see the gate on its next append even
        // if the active session has rotated away.
        stoppedAssetIds.insert(analysisAssetId)

        // Mismatch: don't tear down an unrelated active session. The
        // gate on `stoppedAssetIds` still covers the late-write case
        // even though we don't cancel.
        guard activeAssetId == analysisAssetId else {
            logger.info(
                "stopTranscription(\(analysisAssetId)): asset is not active; gate set but no task to cancel"
            )
            return
        }

        logger.info("stopTranscription(\(analysisAssetId)): cancelling active task")

        activeTask?.cancel()
        activeTask = nil
        activeAssetId = nil
        activePodcastId = nil
        latestSnapshot = nil
        chunkCounter = 0
        // Drop the queued append backlog for the now-stopped session.
        // Anything still queued was destined for the asset id we just
        // gated; the gate would drop it later anyway, but emptying the
        // queue here avoids spinning the loop through dead work.
        appendedShards = []
        loopRunning = false
        preemption = nil
        // Close input and wake any waiter so a loop parked on
        // `waitForMoreShards()` exits promptly. Cancellation is the
        // primary stop signal but the wake makes the exit deterministic.
        inputClosed = true
        resumeAllAppendWaiters()
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
        defer {
            loopRunning = false
            // playhead-01t8: clear the per-job preemption context so a
            // stale signal reference does not linger across jobs. The
            // next `startTranscription` call assigns its own context.
            preemption = nil
        }
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

        // Prioritize shards by proximity to the playhead.
        // Coverage filtering is intentionally NOT applied here — per-shard
        // fingerprint dedup in `transcribeShard` handles already-transcribed
        // regions, including behind-playhead shards that fall within the
        // coverage watermark but were never actually transcribed.
        // See review playhead-rfu-aac H3 for the rationale.
        let prioritized = prioritizeShards(shards)

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
            } catch is TranscriptEnginePreempted {
                logger.info("Transcription preempted at safe point after shard \(shard.id) [end=\(String(format: "%.1f", shard.startTime + shard.duration))s]")
                return
            } catch is TranscriptEngineStopped {
                // playhead-5uvz.5: caller invoked
                // `stopTranscription(analysisAssetId:)`. Exit the loop
                // without emitting `.completed`; the asset is gated so
                // any late writes/events from this point on would be
                // dropped anyway.
                logger.info("Transcription stopped for asset \(analysisAssetId) during shard \(shard.id)")
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

        // Drain any shards that were appended while we were processing,
        // and wait for either more shards or an explicit end-of-input
        // signal before emitting `.completed`. Emitting on a momentarily
        // empty queue was the root cause of analysisState=complete
        // races against a streaming decoder that hadn't finished yet.
        drainLoop: while true {
            while !appendedShards.isEmpty {
                let newBatch = appendedShards
                appendedShards = []

                let newPrioritized = prioritizeShards(newBatch)

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
                    } catch is TranscriptEnginePreempted {
                        logger.info("Transcription preempted at safe point after appended shard \(shard.id)")
                        return
                    } catch is TranscriptEngineStopped {
                        logger.info("Transcription stopped for asset \(analysisAssetId) during appended shard \(shard.id)")
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

            // Backlog is empty. If the caller has signalled end-of-input,
            // we're done. Otherwise suspend until someone appends more
            // shards, calls finishAppending, or stops the engine.
            if inputClosed { break drainLoop }
            if Task.isCancelled { return }
            await waitForMoreShards()
        }

        // If the task was cancelled while we were suspended on a waiter,
        // exit without emitting `.completed`. Cancellation is not a
        // legitimate end-of-input.
        if Task.isCancelled { return }

        // playhead-5uvz.5: a stop landing while we were parked on a
        // waiter is also not a legitimate end-of-input. Bail before
        // running the shard-0 backfill or emitting `.completed`.
        if stoppedAssetIds.contains(analysisAssetId) {
            logger.info("Transcription loop exiting for stopped asset \(analysisAssetId) — no .completed emitted")
            return
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

    // MARK: - Append-wait plumbing

    /// Suspend until another actor touches the append queue or the
    /// session ends. The resume side is any of `appendShards`,
    /// `finishAppending`, `stop`, or a fresh `startTranscription`.
    private func waitForMoreShards() async {
        await withCheckedContinuation { continuation in
            appendWaiters.append(continuation)
        }
    }

    /// Wake every suspended waiter. Safe to call repeatedly; it drains
    /// the continuation list before resuming so a waiter that
    /// immediately re-suspends (because `appendedShards` is still empty
    /// and `inputClosed` is still false) does not race a resume from a
    /// previous wake cycle.
    private func resumeAllAppendWaiters() {
        let waiters = appendWaiters
        appendWaiters = []
        for continuation in waiters {
            continuation.resume()
        }
    }

    // MARK: - Single shard transcription

    private func transcribeShard(
        _ shard: AnalysisShard,
        analysisAssetId: String
    ) async throws {
        try Task.checkCancellation()
        // playhead-5uvz.5: per-shard stopped check at entry. The check
        // re-runs after every await point inside the shard so a
        // `stopTranscription(analysisAssetId:)` that lands mid-shard
        // exits before any subsequent store write or event emission.
        try checkStopped(analysisAssetId: analysisAssetId)

        // Run Apple Speech transcription.
        let segments = try await speechService.transcribe(shard: shard, podcastId: activePodcastId)

        // The await above can release the actor; a stop call could land
        // here. Re-check before any persistence work.
        try checkStopped(analysisAssetId: analysisAssetId)

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
            try checkStopped(analysisAssetId: analysisAssetId)

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

        // playhead-5uvz.5: final pre-persistence check. The previous
        // `await store.fetchTranscriptChunk(...)` / `await
        // store.updateTranscriptChunkWeakAnchorMetadata(...)` calls
        // inside the segment loop release the actor; a
        // `stopTranscription` could land before the batch insert. Bail
        // before writing rows or emitting events for a stopped asset.
        try checkStopped(analysisAssetId: analysisAssetId)

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

        // playhead-01t8 safe point (c): post-TranscriptChunk. Every
        // chunk in `emittedChunks` is durable in SQLite and the
        // coverage watermark has advanced. If a higher-lane admission
        // has flipped the preemption signal, acknowledge it here and
        // let the loop terminate — the next run resumes from this
        // shard's coverage end time via the standard dedup-by-
        // fingerprint path.
        if let preemption, await preemption.isPreemptionRequested() {
            await preemption.acknowledge()
            throw TranscriptEnginePreempted()
        }
    }

    // MARK: - Prioritization

    /// Order shards so that those nearest the playhead (and ahead of it)
    /// are processed first. Shards behind the playhead are deprioritized.
    /// Wraps the static `prioritizeShards` with the latest playback
    /// snapshot. Coverage filtering is deliberately not applied here —
    /// see the comment in `runTranscriptionLoop` next to the per-shard
    /// fingerprint dedup. The parameter that previously accepted
    /// `existingCoverage` was never read; it has been removed (review
    /// playhead-rfu-aac H3) so callers can no longer be misled by it.
    private func prioritizeShards(
        _ shards: [AnalysisShard]
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

    // MARK: - Stop gate (playhead-5uvz.5)

    /// Throws `TranscriptEngineStopped` if the asset has been gated by
    /// `stopTranscription(analysisAssetId:)`. Called at every safe
    /// point inside `transcribeShard` so the loop bails before any
    /// post-stop persistence write or event emission.
    private func checkStopped(analysisAssetId: String) throws {
        if stoppedAssetIds.contains(analysisAssetId) {
            throw TranscriptEngineStopped()
        }
    }

    // MARK: - Coverage updates

    private func updateCoverage(
        analysisAssetId: String,
        endTime: Double
    ) async throws {
        // playhead-5uvz.5: a stop landing between the
        // `speechService.transcribe` await and this write would
        // otherwise advance `analysis_assets.fastTranscriptCoverageEndTime`
        // out-of-band after the runner had moved on. Re-check the gate
        // here as a belt-and-suspenders to the per-shard checks in
        // `transcribeShard`.
        try checkStopped(analysisAssetId: analysisAssetId)
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
        // playhead-5uvz.5: silently drop events for assets that have
        // been gated by `stopTranscription`. The bead contract is that
        // a stopped asset must produce no further `.chunksPersisted`
        // or `.completed` notifications — subscribers (e.g. the
        // `AnalysisJobRunner.run` event loop) treat `.completed` as the
        // signal that coverage is durable and queue downstream work.
        let stopped: Bool
        switch event {
        case .chunksPersisted(let assetId, _):
            stopped = stoppedAssetIds.contains(assetId)
        case .completed(let assetId):
            stopped = stoppedAssetIds.contains(assetId)
        }
        if stopped {
            logger.info("Dropping event for stopped asset: \(String(describing: event))")
            return
        }
        for continuation in eventContinuations.values {
            continuation.yield(event)
        }
    }
}
