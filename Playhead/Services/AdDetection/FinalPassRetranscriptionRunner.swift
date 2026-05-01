// FinalPassRetranscriptionRunner.swift
// Bug 9: charge-gated final-pass re-transcription phase.
//
// Background. `SpeechService.activeModelRole` only flips to `.asrFinal` when
// `loadFinalModel()` is called, but pre-Bug-9 production never invoked
// `loadFinalModel()`. As a result every persisted `transcript_chunks.pass`
// value was `"fast"` and downstream consumers that expected `pass='final'`
// rows either silently fell back to fast chunks (degraded text) or — at
// `AdDetectionService.retryShadowFMPhaseForSession` — bailed unconditionally.
// Part A of the bug fix added a fallback to the broken consumer; this file
// (Part B) adds the missing producer side: a sibling backfill phase that
// actually re-transcribes audio with the final-pass recogniser.
//
// Design (decisions documented inline so a reviewer can audit every edge
// without re-reading the bead prompt):
//
//  • Persistence — APPEND `pass='final'` rows; do NOT replace `pass='fast'`
//    rows. Fast and final chunks have distinct `segmentFingerprint`s
//    (computed from text + timing, both of which differ across passes) and
//    distinct `transcriptVersion`s (the version is what `BackfillJobRunner`
//    keys its dedupe on, so collapsing them would corrupt FM job
//    deduplication). Existing FTS, search, and shadow-replay consumers
//    keep working unchanged on the fast rows; new consumers can opt in to
//    the higher-quality final rows by filtering on `pass='final'`.
//
//  • State tracking — the `analysis_assets.finalPassCoverageEndTime`
//    column (added in this bead) carries the maximum `endTime` of any
//    AdWindow that has been re-transcribed. The runner reads it to
//    short-circuit fully-covered assets and to skip individual windows
//    whose end is already covered. This is intentionally watermark-style
//    rather than per-row bitmap: simpler, monotonic, and resume-safe.
//
//  • Job table — sibling table `final_pass_jobs`. We chose a sibling
//    rather than extending `backfill_jobs` with a new phase value because
//    (1) the unit of work is fundamentally different (audio decode + ASR
//    vs. FM text classification), (2) the runner shape is leaner, and
//    (3) keeping the two pipelines independent prevents one from
//    mass-deferring the other under thermal pressure. As a corollary, this
//    runner does NOT share the FM `AdmissionController` queue; gating is
//    derived inline from a `CapabilitySnapshot` + battery + charge read.
//
//  • Admission — three independent gates evaluated at runner-entry AND
//    once per window (so an unplug or thermal spike mid-drain terminates
//    promptly):
//      1. `isCharging == true` — re-transcription is heavy, the bead
//         spec scopes it to plugged-in devices.
//      2. `QualityProfile.derive(...) != .critical` (i.e. `pauseAllWork`
//         is false) — same thermal/LPM/low-battery gate the
//         `AdmissionController` uses, applied via the shared derivation.
//      3. `thermalState == .nominal` — the bead spec asks for a strict
//         "nominal thermal" floor, stricter than `pauseAllWork` (which
//         only blocks at `.critical`). Heavy ASR work is throttled out
//         even at `.fair` so we don't push warm devices into `.serious`.
//
//  • Confidence threshold — defaults to `0.5`, configurable via
//    `PreAnalysisConfig.finalPassRetranscriptionConfidenceFloor`. Only
//    AdWindows whose persisted `confidence` clears the floor are
//    re-transcribed. The whole point is high-quality text on the
//    candidate windows the classifier has already flagged; running the
//    final model over the entire episode would burn battery for no
//    classifier gain.
//
//  • Idempotency — the runner consults the watermark before scheduling
//    work for a window AND consults `transcript_chunks` to confirm there
//    are no already-persisted `pass='final'` rows that overlap. A second
//    invocation against an asset whose runner-watermark equals the
//    maximum confidence-cleared AdWindow endTime is a guaranteed no-op.
//
//  • Wiring — composed in `PlayheadRuntime` alongside the existing
//    `BackfillJobRunner`. `AnalysisJobReconciler.reconcile()` enqueues
//    eligible assets at launch via `enqueueAssetsNeedingFinalPass`.
//
// Out of scope (per bead prompt): the existing `BackfillJobRunner` is
// untouched, the `pass` column and `loadFinalModel` enum case are
// preserved, and classifier thresholds (the *coarse* classifier's, not
// the per-window confidence floor introduced here) are not changed.

import CryptoKit
import Foundation
import OSLog

// MARK: - FinalPassRetranscriptionRunner

/// Runs the charge-gated final-pass re-transcription phase for one asset.
/// Idempotent against the persisted watermark + `pass='final'` chunks.
actor FinalPassRetranscriptionRunner {

    // MARK: - Production Defaults

    /// Default confidence floor for AdWindow eligibility. AdWindows whose
    /// `confidence` is strictly less than this value are skipped.
    /// Configurable via the `FinalPassRetranscriptionRunner.init`
    /// `confidenceFloor` parameter so future per-device cohort tuning can
    /// move it without touching the runner.
    static let defaultConfidenceFloor: Double = 0.5

    /// Distinct from the fast-pass `apple-speech-v1` so persisted
    /// `pass='final'` rows record which model produced them. Bumping this
    /// invalidates prior final-pass coverage; the watermark approach
    /// makes a re-run a one-shot reprocess rather than a destructive
    /// migration.
    static let defaultModelVersion: String = "apple-speech-final-v1"

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let speechService: SpeechService
    private let audioProvider: AnalysisAudioProviding
    private let capabilitySnapshotProvider: @Sendable () async -> CapabilitySnapshot
    private let batteryLevelProvider: @Sendable () async -> Float
    private let chargeStateProvider: @Sendable () async -> Bool
    private let confidenceFloor: Double
    private let modelVersion: String
    private let logger = Logger(subsystem: "com.playhead", category: "FinalPassRetranscription")

    // MARK: - Inputs / Outputs

    struct AssetInput: Sendable, Equatable {
        let analysisAssetId: String
        let podcastId: String?
        /// Resolved local file URL for the asset's audio. The caller (the
        /// reconciler at launch time, or `runtime.runFinalPassBackfill`
        /// from a BG-task wakeup) is responsible for verifying that the
        /// file is on disk before constructing this input — if the file
        /// has been evicted, no AdWindow can be re-transcribed anyway.
        let audioURL: LocalAudioURL
        /// Episode identifier used as the audio shard cache key.
        let episodeId: String
    }

    struct RunResult: Sendable, Equatable {
        /// IDs of `final_pass_jobs` rows that were admitted and ran to
        /// completion in this drain. Empty when admission deferred or
        /// when the asset was already fully covered.
        let admittedJobIds: [String]
        /// IDs of windows that were re-transcribed during this run. A
        /// re-run against the same asset must produce an empty array
        /// here once `idempotency` has converged.
        let reTranscribedWindowIds: [String]
        /// IDs of `final_pass_jobs` rows that were enqueued but deferred
        /// by admission (charge / thermal / battery / LPM).
        let deferredJobIds: [String]
        /// Reason a top-level run did not start at all (e.g. not on
        /// charge). `nil` when the runner reached the per-window loop.
        let topLevelDeferReason: AdmissionDeferReason?

        static let empty = RunResult(
            admittedJobIds: [],
            reTranscribedWindowIds: [],
            deferredJobIds: [],
            topLevelDeferReason: nil
        )
    }

    // MARK: - Init

    init(
        store: AnalysisStore,
        speechService: SpeechService,
        audioProvider: AnalysisAudioProviding,
        capabilitySnapshotProvider: @escaping @Sendable () async -> CapabilitySnapshot,
        batteryLevelProvider: @escaping @Sendable () async -> Float,
        chargeStateProvider: @escaping @Sendable () async -> Bool,
        confidenceFloor: Double,
        modelVersion: String
    ) {
        self.store = store
        self.speechService = speechService
        self.audioProvider = audioProvider
        self.capabilitySnapshotProvider = capabilitySnapshotProvider
        self.batteryLevelProvider = batteryLevelProvider
        self.chargeStateProvider = chargeStateProvider
        self.confidenceFloor = confidenceFloor
        self.modelVersion = modelVersion
    }

    // MARK: - Entry Point

    /// Runs the final-pass re-transcription phase for one asset.
    ///
    /// Sequence:
    ///   1. Top-level admission gate: charge + nominal thermal + LPM=false +
    ///      QualityProfile permits work. Bails with a populated
    ///      `topLevelDeferReason` on the first failed gate.
    ///   2. Loads the asset's persisted AdWindows and the runner
    ///      watermark.
    ///   3. Filters to windows where `confidence >= confidenceFloor` and
    ///      `endTime > finalPassCoverageEndTime` (idempotent skip).
    ///   4. Per surviving window: re-checks the gates, calls
    ///      `loadFinalModel()` (idempotent via `activeModelRole`), slices
    ///      the relevant audio shards out of the cached decoded set,
    ///      transcribes each shard, and appends the resulting
    ///      `pass='final'` rows.
    ///   5. Advances `analysis_assets.finalPassCoverageEndTime` to the
    ///      maximum window endTime that ran in this drain.
    ///
    /// Idempotency contract: a second call with no thermal / battery /
    /// charge change must be a no-op (zero re-transcribed windows).
    @discardableResult
    func runFinalPassBackfill(for input: AssetInput) async throws -> RunResult {
        // Step 1 — top-level admission gate.
        if let reason = await currentDeferReason() {
            logger.info("Final-pass run deferred: reason=\(reason.rawValue, privacy: .public) (asset=\(input.analysisAssetId, privacy: .public))")
            return RunResult(
                admittedJobIds: [],
                reTranscribedWindowIds: [],
                deferredJobIds: [],
                topLevelDeferReason: reason
            )
        }

        // Step 2 — load AdWindows + watermark.
        let asset: AnalysisAsset?
        do {
            asset = try await store.fetchAsset(id: input.analysisAssetId)
        } catch {
            logger.warning("Final-pass: failed to fetch asset \(input.analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)")
            throw error
        }
        guard let asset else {
            logger.debug("Final-pass: asset \(input.analysisAssetId, privacy: .public) not found")
            return .empty
        }
        let watermark = asset.finalPassCoverageEndTime ?? 0

        let allWindows: [AdWindow]
        do {
            allWindows = try await store.fetchAdWindows(assetId: input.analysisAssetId)
        } catch {
            logger.warning("Final-pass: failed to fetch ad windows for \(input.analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)")
            throw error
        }

        // Step 3 — filter. A window is eligible iff:
        //   • its confidence cleared the configured floor, AND
        //   • its endTime is strictly past the watermark (resume guard).
        // The per-row `pass='final'` chunk overlap check is in
        // `retranscribeWindow` so it can short-circuit the full audio
        // decode without an extra DB hop here.
        let eligibleWindows = allWindows.filter { window in
            guard window.confidence >= confidenceFloor else { return false }
            guard window.endTime > watermark else { return false }
            return true
        }
        guard !eligibleWindows.isEmpty else {
            logger.debug("Final-pass: no eligible windows for \(input.analysisAssetId, privacy: .public) (windows=\(allWindows.count, privacy: .public), floor=\(self.confidenceFloor, privacy: .public), watermark=\(watermark, privacy: .public))")
            return .empty
        }

        // Materialize the existing pass='final' chunks so the per-window
        // inner check can run without an extra DB hop per window.
        let existingFinalChunks: [TranscriptChunk]
        do {
            existingFinalChunks = try await store.fetchTranscriptChunks(
                assetId: input.analysisAssetId
            ).filter { $0.pass == TranscriptPassType.final_.rawValue }
        } catch {
            existingFinalChunks = []
        }

        // Step 4 — enqueue + run each window. Each window is its own row
        // in `final_pass_jobs` so admission and retry tracking are
        // per-window.
        let now = Date().timeIntervalSince1970
        var jobs: [FinalPassJob] = []
        for window in eligibleWindows {
            let job = FinalPassJob(
                jobId: "fpj-\(input.analysisAssetId)-\(window.id)",
                analysisAssetId: input.analysisAssetId,
                podcastId: input.podcastId,
                adWindowId: window.id,
                windowStartTime: window.startTime,
                windowEndTime: window.endTime,
                status: .queued,
                retryCount: 0,
                deferReason: nil,
                createdAt: now
            )
            do {
                try await store.insertOrIgnoreFinalPassJob(job)
                jobs.append(job)
            } catch {
                logger.warning("Final-pass: failed to insert job \(job.jobId, privacy: .public): \(error.localizedDescription, privacy: .public)")
            }
        }

        var admittedJobIds: [String] = []
        var reTranscribedWindowIds: [String] = []
        var deferredJobIds: [String] = []
        var maxRetranscribedEnd: Double = watermark

        for job in jobs {
            // Re-check the gates before every window. A device unplug or
            // thermal spike mid-drain must terminate the loop promptly
            // without loading a new shard.
            if let reason = await currentDeferReason() {
                logger.info("Final-pass: gate failed mid-drain (\(reason.rawValue, privacy: .public)), deferring remaining jobs")
                for remainingJob in jobs where !admittedJobIds.contains(remainingJob.jobId)
                    && !deferredJobIds.contains(remainingJob.jobId)
                {
                    do {
                        try await store.markFinalPassJobDeferred(
                            jobId: remainingJob.jobId,
                            reason: reason.rawValue
                        )
                        deferredJobIds.append(remainingJob.jobId)
                    } catch {
                        logger.warning("Final-pass: failed to mark deferred for \(remainingJob.jobId, privacy: .public)")
                    }
                }
                break
            }

            do {
                try await store.markFinalPassJobRunning(jobId: job.jobId)
                let didRun = try await retranscribeWindow(
                    job: job,
                    input: input,
                    existingFinalChunks: existingFinalChunks
                )
                try await store.markFinalPassJobComplete(jobId: job.jobId)
                admittedJobIds.append(job.jobId)
                if didRun {
                    reTranscribedWindowIds.append(job.adWindowId)
                    maxRetranscribedEnd = max(maxRetranscribedEnd, job.windowEndTime)
                }
            } catch {
                logger.warning("Final-pass: retranscribe failed for \(job.jobId, privacy: .public): \(error.localizedDescription, privacy: .public)")
                try? await store.markFinalPassJobFailed(
                    jobId: job.jobId,
                    reason: "retranscribeFailed"
                )
            }
        }

        // Step 5 — advance watermark monotonically. Use the max END of
        // any window that re-transcribed in this drain (or the prior
        // watermark, whichever is greater — `advanceFinalPassCoverage`
        // enforces monotonicity in SQL).
        if maxRetranscribedEnd > watermark {
            do {
                try await store.advanceFinalPassCoverage(
                    id: input.analysisAssetId,
                    endTime: maxRetranscribedEnd
                )
            } catch {
                logger.warning("Final-pass: failed to advance watermark for \(input.analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)")
            }
        }

        return RunResult(
            admittedJobIds: admittedJobIds,
            reTranscribedWindowIds: reTranscribedWindowIds,
            deferredJobIds: deferredJobIds,
            topLevelDeferReason: nil
        )
    }

    // MARK: - Gating

    /// Returns `nil` when all admission gates pass, or the most-specific
    /// `AdmissionDeferReason` enum case when at least one gate fails.
    /// Precedence mirrors `AdmissionController.deferReason`: thermal
    /// first, then low-battery-and-unplugged (here: not-charging is the
    /// blocker), then low-power-mode.
    private func currentDeferReason() async -> AdmissionDeferReason? {
        let snapshot = await capabilitySnapshotProvider()
        let battery = await batteryLevelProvider()
        let isCharging = await chargeStateProvider()

        // Bug 9 spec — strict thermal `.nominal` floor (stricter than
        // QualityProfile's `pauseAllWork` which only fires at `.critical`).
        if snapshot.thermalState != .nominal {
            return .thermalThrottled
        }
        // Bug 9 spec — must be on charge.
        if !isCharging {
            return .batteryTooLow
        }
        // Bug 9 spec — LPM must be off.
        if snapshot.isLowPowerMode {
            return .lowPowerMode
        }
        // Defensive: respect `QualityProfile.pauseAllWork` even though
        // the strict gates above subsume it for nominal devices. If a
        // future profile change demotes nominal devices, we honor it.
        let profile = snapshot.qualityProfile(
            batteryLevel: battery,
            isCharging: isCharging
        )
        if profile.schedulerPolicy.pauseAllWork {
            return .thermalThrottled
        }
        return nil
    }

    // MARK: - Per-window retranscribe

    /// Transcribes the audio range [windowStartTime, windowEndTime] using
    /// the final-pass model and appends `pass='final'` chunks. Returns
    /// `true` when at least one shard was processed (and the watermark
    /// should advance), `false` when the window was skipped because
    /// existing `pass='final'` chunks already covered it.
    private func retranscribeWindow(
        job: FinalPassJob,
        input: AssetInput,
        existingFinalChunks: [TranscriptChunk]
    ) async throws -> Bool {
        // Inner idempotency rail: skip if existing pass='final' chunks
        // already cover this window. The outer watermark check usually
        // already bails on this, but we keep the inner check as defense.
        let coversWindow = existingFinalChunks.contains { chunk in
            chunk.startTime <= job.windowStartTime
                && chunk.endTime >= job.windowEndTime
        }
        if coversWindow {
            logger.debug("Final-pass: window \(job.adWindowId, privacy: .public) already covered by pass='final' chunks")
            return false
        }

        // Load the final model once per drain; the SpeechService caches
        // model state across calls so subsequent windows reuse it.
        if await speechService.activeModelRole != .asrFinal {
            try await speechService.loadFinalModel()
        }

        // Decode the asset's audio into shards (cache-hit if the audio
        // service already has them on disk).
        let allShards = try await audioProvider.decode(
            fileURL: input.audioURL,
            episodeID: input.episodeId,
            shardDuration: AnalysisAudioService.defaultShardDuration
        )
        // Slice to shards intersecting the window. We keep an entire
        // shard even if only the tail straddles the window — Apple
        // Speech's recogniser is window-context-sensitive, and clipping
        // the shard would degrade the very accuracy we're chasing.
        let intersectingShards = allShards.filter { shard in
            let shardEnd = shard.startTime + shard.duration
            return shard.startTime < job.windowEndTime
                && shardEnd > job.windowStartTime
        }
        guard !intersectingShards.isEmpty else {
            logger.debug("Final-pass: no shards intersect window \(job.adWindowId, privacy: .public) [\(job.windowStartTime, privacy: .public)..\(job.windowEndTime, privacy: .public)]")
            return false
        }

        var newChunks: [TranscriptChunk] = []
        var nextChunkIndex = await nextFinalChunkIndex(
            forAsset: input.analysisAssetId
        )
        for shard in intersectingShards {
            let segments = try await speechService.transcribe(
                shard: shard,
                podcastId: input.podcastId
            )
            for segment in segments {
                let chunk = TranscriptChunk(
                    id: UUID().uuidString,
                    analysisAssetId: input.analysisAssetId,
                    segmentFingerprint: Self.computeFinalPassFingerprint(
                        text: segment.text,
                        startTime: segment.startTime,
                        endTime: segment.endTime
                    ),
                    chunkIndex: nextChunkIndex,
                    startTime: segment.startTime,
                    endTime: segment.endTime,
                    text: segment.text,
                    normalizedText: segment.text.lowercased(),
                    pass: TranscriptPassType.final_.rawValue,
                    modelVersion: modelVersion,
                    transcriptVersion: nil,
                    atomOrdinal: nil,
                    weakAnchorMetadata: segment.weakAnchorMetadata
                )
                // Skip insert if a row with the same fingerprint exists
                // (idempotent re-run guard).
                let exists = (try? await store.hasTranscriptChunk(
                    analysisAssetId: input.analysisAssetId,
                    segmentFingerprint: chunk.segmentFingerprint
                )) ?? false
                if !exists {
                    newChunks.append(chunk)
                    nextChunkIndex += 1
                }
            }
        }

        if !newChunks.isEmpty {
            try await store.insertTranscriptChunks(newChunks)
            logger.info("Final-pass: appended \(newChunks.count, privacy: .public) pass='final' chunks for window \(job.adWindowId, privacy: .public)")
        } else {
            logger.debug("Final-pass: no new chunks produced for window \(job.adWindowId, privacy: .public) — likely silence or all segments deduped")
        }
        return true
    }

    /// Compute the next `chunkIndex` for newly-inserted final-pass rows.
    /// Final-pass rows live alongside fast-pass rows in the same table;
    /// we pick a chunkIndex that is strictly greater than the highest
    /// existing index for the asset so positional ordering in
    /// `fetchTranscriptChunks(assetId:)` interleaves correctly.
    private func nextFinalChunkIndex(forAsset assetId: String) async -> Int {
        let existing = (try? await store.fetchTranscriptChunks(assetId: assetId)) ?? []
        return (existing.map { $0.chunkIndex }.max() ?? -1) + 1
    }

    /// Fingerprint scheme for final-pass chunks. Uses SHA-256 over a
    /// final-pass-prefixed key so the fingerprint is **stable across
    /// process launches** (Swift's `Hasher` re-seeds per process and
    /// would silently break the cross-launch idempotency guard). The
    /// prefix `fp-final-` cannot collide with `TranscriptEngineService`'s
    /// fast-pass scheme (which uses `text|start|end` without a prefix).
    /// Two chunks with identical text and timing but different passes
    /// therefore hash to different fingerprints — both rows persist,
    /// neither is confused for a duplicate of the other.
    static func computeFinalPassFingerprint(
        text: String,
        startTime: Double,
        endTime: Double
    ) -> String {
        let key = "fp-final-\(text)|\(startTime)|\(endTime)"
        let digest = SHA256.hash(data: Data(key.utf8))
        return digest.prefix(16).map { String(format: "%02x", $0) }.joined()
    }
}

// MARK: - FinalPassJob

/// Persisted row in `final_pass_jobs`. Mirrors `BackfillJob` but uses a
/// per-AdWindow grain rather than per-asset.
struct FinalPassJob: Sendable, Equatable {
    let jobId: String
    let analysisAssetId: String
    let podcastId: String?
    let adWindowId: String
    let windowStartTime: Double
    let windowEndTime: Double
    let status: BackfillJobStatus
    let retryCount: Int
    let deferReason: String?
    let createdAt: Double
}
