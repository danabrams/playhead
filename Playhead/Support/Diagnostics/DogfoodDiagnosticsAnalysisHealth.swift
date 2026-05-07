// DogfoodDiagnosticsAnalysisHealth.swift
// Phase 1.5 playhead-hygc.1.9 — actionable no-progress summary for the
// dogfood diagnostics export.
//
// Adds an OPTIONAL `analysis_health` object to the dogfood archive that
// summarizes the May 6 dogfood diagnostic question directly — *why* did
// an episode not progress, and *what* should the user (or support) try
// next? The summary is computed deterministically from the existing
// support-safe `DogfoodDiagnosticsActivitySnapshot` plus a small
// `LearningCounts` / `DuplicateCounts` carrier the caller can populate
// from sibling subsystems (correction store, final-pass jobs, shadow
// FM responses, ingested learning artifacts).
//
// Privacy / redaction:
//   * No raw transcript text is ever copied into the summary.
//   * No audio paths or remote source URLs are copied — the activity
//     snapshot already runs `failureReason`/`terminalReason`/`lastErrorCode`
//     through `LiveActivitySnapshotProvider.sanitizedDiagnosticString`
//     before we see them, and the summary forwards those sanitized
//     fields without re-introducing raw values.
//   * Episode identity is referenced only by the `episode_id_hash` the
//     activity row already carries (a salted SHA-256 keyed off the
//     install-id-hash via `EpisodeIdHasher`).
//
// Schema versioning:
//   * `analysis_health` is a NEW optional field on
//     `DogfoodDiagnosticsArchive`. v1 archives (no activity snapshot,
//     no analysis health) continue to decode unchanged.
//   * When `analysis_health` is present the archive bumps
//     `schema_version` to 2. Older tooling reading a v2 archive sees
//     the extra key — JSONDecoder ignores unknown keys by default, so
//     the v1 reader does not crash.
//   * Older v1 archives still decode against the v2 struct because
//     every new field is optional.

import Foundation

// MARK: - Top-level analysis_health object

/// Structured per-asset and global "why did nothing progress?" summary.
/// All counts are derived synchronously and deterministically from the
/// inputs — no clocks, no random IDs, so two consecutive exports over
/// the same activity snapshot produce byte-equal `analysis_health`
/// blocks (mod the caller-supplied `generatedAt`).
struct DogfoodDiagnosticsAnalysisHealth: Codable, Sendable, Equatable {

    /// Schema version of the analysis_health object itself. Distinct
    /// from `DogfoodDiagnosticsArchive.schemaVersion` so future
    /// extensions can evolve without forcing the outer archive to bump
    /// in lockstep.
    let summarySchemaVersion: Int

    /// `now` snapshot. Used downstream to compute "minutes since last
    /// background run" once the archive lands at support; we record
    /// it here so the comparison anchor is whatever moment the export
    /// actually ran, not whenever the JSON was opened.
    let generatedAt: Date

    /// Cross-asset summary.
    let global: GlobalSummary

    /// Per-asset summary, one row per `DogfoodDiagnosticsActivityRow`
    /// in the input snapshot. Order matches the input — sorting by
    /// `episode_id_hash` would alphabetize hash bytes, which is not a
    /// useful order for support eyeballs.
    let assets: [AssetSummary]

    /// Aggregated staleness / contradiction flags. Empty when the
    /// snapshot is internally consistent.
    let stalenessFlags: [StalenessFlag]

    /// Optional duplicate-record counts. When `nil`, the export
    /// deliberately skipped this aggregate (e.g. AnalysisStore was
    /// not opened at export time); a present-but-zero block means
    /// "we looked and there were none".
    let duplicates: DuplicateCounts?

    /// Optional learning-pipeline aggregates. Same semantics as
    /// `duplicates` — `nil` means "not collected", a populated block
    /// means "this is what we observed".
    let learning: LearningCounts?

    /// Free-form note explaining gaps when the summary could not be
    /// fully populated (e.g. activity snapshot was nil because the
    /// AnalysisStore had not opened yet). Surfaced so support can
    /// distinguish "everything looks fine" from "we did not capture
    /// the data we'd need to tell you whether it does".
    let captureNote: String?

    enum CodingKeys: String, CodingKey {
        case summarySchemaVersion = "summary_schema_version"
        case generatedAt = "generated_at"
        case global
        case assets
        case stalenessFlags = "staleness_flags"
        case duplicates
        case learning
        case captureNote = "capture_note"
    }

    /// Current summary schema version. Bumped manually; bump should
    /// be paired with a migration note in this file's header.
    static let currentSummarySchemaVersion = 1
}

// MARK: - Global summary

extension DogfoodDiagnosticsAnalysisHealth {

    struct GlobalSummary: Codable, Sendable, Equatable {
        let totalAssets: Int
        let runningCount: Int
        let queuedCount: Int
        let pausedCount: Int
        let failedCount: Int
        let unavailableCount: Int
        let terminalCompletedCount: Int
        /// Assets whose persisted analysis state says "complete" but
        /// whose canonical coverage (transcript_covered_sec or
        /// feature_coverage_end_sec) is missing or below the duration.
        /// Counted so the global card flashes a single number instead
        /// of forcing the reader to walk every per-asset row.
        let staleTerminalCount: Int
        /// Assets whose persisted fast_transcript_watermark_sec lags
        /// behind the actually-aggregated transcript_covered_sec by
        /// more than `staleWatermarkToleranceSec`.
        let staleWatermarkCount: Int
        /// Assets where every progress signal is unknown (`--%`).
        /// These are the rows the user sees as "stuck with no
        /// explanation"; surfacing the count up front is the whole
        /// point of this bead.
        let unknownProgressCount: Int
        /// ISO-8601 timestamp of the most recent durable job
        /// `updatedAt` across the snapshot, formatted as a Double
        /// epoch-seconds. nil when no job rows have ever been seen.
        let latestJobUpdateAt: Double?
        /// Most recent terminal work-journal timestamp across the
        /// snapshot. Tells support "the BG-task system did *some*
        /// work N minutes ago" without forcing them to scan every
        /// per-asset row.
        let latestTerminalWorkAt: Double?
        /// Most recent transcript or feature artifact watermark
        /// across the snapshot — same role as latestTerminalWorkAt
        /// but for the actual canonical artifacts.
        let latestArtifactWatermarkSec: Double?
        /// Outcome of the latest terminal work-journal entry: one of
        /// `finalized`, `failed`, `preempted`, `acquired`,
        /// `checkpointed`, or nil when no terminal entry exists.
        let latestTerminalWorkOutcome: String?

        enum CodingKeys: String, CodingKey {
            case totalAssets = "total_assets"
            case runningCount = "running_count"
            case queuedCount = "queued_count"
            case pausedCount = "paused_count"
            case failedCount = "failed_count"
            case unavailableCount = "unavailable_count"
            case terminalCompletedCount = "terminal_completed_count"
            case staleTerminalCount = "stale_terminal_count"
            case staleWatermarkCount = "stale_watermark_count"
            case unknownProgressCount = "unknown_progress_count"
            case latestJobUpdateAt = "latest_job_update_at"
            case latestTerminalWorkAt = "latest_terminal_work_at"
            case latestArtifactWatermarkSec = "latest_artifact_watermark_sec"
            case latestTerminalWorkOutcome = "latest_terminal_work_outcome"
        }
    }
}

// MARK: - Per-asset summary

extension DogfoodDiagnosticsAnalysisHealth {

    struct AssetSummary: Codable, Sendable, Equatable {
        let episodeIdHash: String
        let section: String
        /// Mirrors `analysis_asset.analysis_state` for quick eyeballing
        /// without cross-referencing the activity snapshot.
        let analysisState: String
        let isRunning: Bool
        let queuePosition: Int?
        let cachedAudioPresent: Bool
        let downloadPercent: String
        let transcriptPercent: String
        let analysisPercent: String
        let transcriptCoveredSec: Double?
        let transcriptWatermarkSec: Double?
        let fastTranscriptWatermarkSec: Double?
        /// Difference, in seconds, between `transcript_covered_sec`
        /// (real chunk coverage) and `fast_transcript_watermark_sec`
        /// (the persisted scheduler high-water mark). Positive means
        /// the persisted watermark lags the real coverage — the
        /// stale-watermark hazard from playhead-3bv.2. nil when
        /// either value is unknown.
        let watermarkDeltaSec: Double?
        let analysisWatermarkSec: Double?
        let finalPassCoverageEndSec: Double?
        /// Why each percent is shown: e.g. `cached_audio` for download,
        /// `fast_transcript_chunks` for transcript, `feature_coverage`
        /// for analysis. The strings are taken verbatim from the
        /// activity snapshot's `pipeline.*_source` fields so the
        /// vocabulary stays one-to-one with the production reducer.
        let progressProvenance: ProgressProvenance
        /// Latest session/job/work-journal status as recorded in the
        /// activity snapshot. Already sanitized.
        let latestSessionState: String?
        let latestSessionFailureReason: String?
        let latestJobState: String?
        let latestJobLastErrorCode: String?
        let latestJobNextEligibleAt: Double?
        let latestJobLeasePresent: Bool?
        let latestTerminalWorkEvent: String?
        let latestTerminalWorkCause: String?
        let terminalReason: String?
        /// Recommended next action for this asset, drawn from a small
        /// fixed vocabulary so support tooling can route on it.
        /// See `RecommendedAction`.
        let recommendedAction: RecommendedAction
        /// Free-form supplementary note for the recommended action
        /// (e.g. "lease expires in 12s, will auto-recover", "no audio
        /// cached and not currently downloading"). Already sanitized.
        let recommendedActionNote: String?

        enum CodingKeys: String, CodingKey {
            case episodeIdHash = "episode_id_hash"
            case section
            case analysisState = "analysis_state"
            case isRunning = "is_running"
            case queuePosition = "queue_position"
            case cachedAudioPresent = "cached_audio_present"
            case downloadPercent = "download_percent"
            case transcriptPercent = "transcript_percent"
            case analysisPercent = "analysis_percent"
            case transcriptCoveredSec = "transcript_covered_sec"
            case transcriptWatermarkSec = "transcript_watermark_sec"
            case fastTranscriptWatermarkSec = "fast_transcript_watermark_sec"
            case watermarkDeltaSec = "watermark_delta_sec"
            case analysisWatermarkSec = "analysis_watermark_sec"
            case finalPassCoverageEndSec = "final_pass_coverage_end_sec"
            case progressProvenance = "progress_provenance"
            case latestSessionState = "latest_session_state"
            case latestSessionFailureReason = "latest_session_failure_reason"
            case latestJobState = "latest_job_state"
            case latestJobLastErrorCode = "latest_job_last_error_code"
            case latestJobNextEligibleAt = "latest_job_next_eligible_at"
            case latestJobLeasePresent = "latest_job_lease_present"
            case latestTerminalWorkEvent = "latest_terminal_work_event"
            case latestTerminalWorkCause = "latest_terminal_work_cause"
            case terminalReason = "terminal_reason"
            case recommendedAction = "recommended_action"
            case recommendedActionNote = "recommended_action_note"
        }
    }

    struct ProgressProvenance: Codable, Sendable, Equatable {
        let downloadSource: String
        let transcriptSource: String
        let analysisSource: String

        enum CodingKeys: String, CodingKey {
            case downloadSource = "download_source"
            case transcriptSource = "transcript_source"
            case analysisSource = "analysis_source"
        }
    }
}

// MARK: - Recommended actions (closed vocabulary)

extension DogfoodDiagnosticsAnalysisHealth {

    /// Closed vocabulary of recommended next actions. Support tooling
    /// can branch on these without parsing free text. New cases must
    /// stay in lockstep with summary_schema_version.
    enum RecommendedAction: String, Codable, Sendable, Equatable, CaseIterable {
        /// Nothing to do — the asset has reached a satisfactory
        /// terminal state and canonical coverage agrees.
        case wait
        /// Asset is queued but device-thermal or download blocked it;
        /// the user should plug in / wait for thermal recovery.
        case plugInOrWait = "plug_in_or_wait"
        /// Asset has no cached audio and no live download — user
        /// needs to open the app so the foreground download manager
        /// resumes.
        case openApp = "open_app"
        /// A session/job is sitting on a recoverable failure — try
        /// re-running once the device is on charger or after a cold
        /// start.
        case retry
        /// Lease/expiry hazard: the durable job has a stale lease
        /// blocking re-acquisition. The next BG-task tick or app
        /// foregrounding clears it; surfaced for visibility, not
        /// because the user has to do anything.
        case clearStaleLease = "clear_stale_lease"
        /// Internal contradiction (terminal state vs canonical
        /// coverage) that warrants filing a bug — the data is
        /// already in the diagnostics archive the user is about
        /// to share.
        case fileBug = "file_bug"
        /// Default for rows the summary could not classify (e.g.
        /// missing snapshot inputs). Distinct from `wait` so support
        /// tooling can flag the noise.
        case unknown
    }
}

// MARK: - Staleness flags

extension DogfoodDiagnosticsAnalysisHealth {

    /// One staleness/contradiction observation. Encoded with a
    /// closed-vocabulary `kind` so support tooling can branch.
    struct StalenessFlag: Codable, Sendable, Equatable {
        let episodeIdHash: String
        let kind: Kind
        /// Free-form description, already sanitized. Length-bounded
        /// to keep the summary compact.
        let detail: String

        enum CodingKeys: String, CodingKey {
            case episodeIdHash = "episode_id_hash"
            case kind
            case detail
        }

        enum Kind: String, Codable, Sendable, Equatable, CaseIterable {
            /// `analysis_state` is a terminal completion but
            /// `transcript_covered_sec` or `feature_coverage_end_sec`
            /// is missing / below `episode_duration_sec * 0.95`.
            case terminalStateContradictsCoverage = "terminal_state_contradicts_coverage"
            /// `fast_transcript_watermark_sec` lags
            /// `transcript_covered_sec` by more than the tolerance.
            case staleFastTranscriptWatermark = "stale_fast_transcript_watermark"
            /// All three pipeline percents are unknown despite the
            /// row not being in a paused / unavailable state.
            case unknownProgressWithoutPause = "unknown_progress_without_pause"
            /// Downstream-job lease is held but
            /// `next_eligible_at` is in the past — either the lease
            /// is stale or a clock skew exists.
            case staleJobLease = "stale_job_lease"
            /// Terminal failure recorded but no canonical
            /// failure_reason captured (we lost the why).
            case missingFailureReason = "missing_failure_reason"
        }
    }
}

// MARK: - Caller-supplied aggregates

extension DogfoodDiagnosticsAnalysisHealth {

    /// Caller-supplied duplicate-record counts. The summary builder
    /// is intentionally agnostic about which subsystem produced these
    /// — `playhead-hygc.1.6` (correction-event dedupe) and
    /// `playhead-hygc.1.5` (final-pass span dedupe) own the queries.
    /// Defining the shape here lets the export call site pass `nil`
    /// when the dependency hasn't landed yet without forcing the
    /// archive shape to wobble.
    struct DuplicateCounts: Codable, Sendable, Equatable {
        let duplicateCorrectionScopes: Int
        let duplicateFinalPassWindows: Int

        enum CodingKeys: String, CodingKey {
            case duplicateCorrectionScopes = "duplicate_correction_scopes"
            case duplicateFinalPassWindows = "duplicate_final_pass_windows"
        }

        init(
            duplicateCorrectionScopes: Int,
            duplicateFinalPassWindows: Int
        ) {
            self.duplicateCorrectionScopes = duplicateCorrectionScopes
            self.duplicateFinalPassWindows = duplicateFinalPassWindows
        }
    }

    /// Caller-supplied learning-pipeline aggregates. Mirrors the
    /// .1.7 contract: count distinct raw events the user produced
    /// (corrections, shadow FM responses) vs how many made it
    /// through the deduper into a learning artifact, plus the bag
    /// of skipped reasons (already a closed vocabulary in the
    /// ingestion pipeline).
    struct LearningCounts: Codable, Sendable, Equatable {
        let rawCorrections: Int
        let dedupedCorrections: Int
        let shadowFMResponses: Int
        let ingestedLearningArtifacts: Int
        /// Distinct reason → count. Reason strings come from the
        /// learning pipeline's closed vocabulary (e.g.
        /// `duplicate_scope`, `unverified_window`,
        /// `transcript_unavailable`); mapping is preserved verbatim
        /// for support tooling. Empty when nothing was skipped.
        let skippedIngestionReasons: [String: Int]

        enum CodingKeys: String, CodingKey {
            case rawCorrections = "raw_corrections"
            case dedupedCorrections = "deduped_corrections"
            case shadowFMResponses = "shadow_fm_responses"
            case ingestedLearningArtifacts = "ingested_learning_artifacts"
            case skippedIngestionReasons = "skipped_ingestion_reasons"
        }

        init(
            rawCorrections: Int,
            dedupedCorrections: Int,
            shadowFMResponses: Int,
            ingestedLearningArtifacts: Int,
            skippedIngestionReasons: [String: Int]
        ) {
            self.rawCorrections = rawCorrections
            self.dedupedCorrections = dedupedCorrections
            self.shadowFMResponses = shadowFMResponses
            self.ingestedLearningArtifacts = ingestedLearningArtifacts
            self.skippedIngestionReasons = skippedIngestionReasons
        }
    }
}

// MARK: - Builder

extension DogfoodDiagnosticsAnalysisHealth {

    /// Tolerance, in seconds, before a `fast_transcript_watermark`
    /// gap counts as stale. Single chunks are ~30 s today; we set the
    /// threshold at 60 s so a normal one-chunk lag is not flagged but
    /// the multi-minute drifts the May 6 dogfood saw are.
    static let staleWatermarkToleranceSec: Double = 60.0

    /// Coverage ratio at or above which a terminal-completion state
    /// is considered consistent with canonical coverage. Mirrors the
    /// finalize-threshold semantics in `AnalysisCoordinator` — sub-95%
    /// coverage in a `completeFull` row is the contradiction
    /// playhead-hygc.1.3 fixed.
    static let terminalCoverageRatioThreshold: Double = 0.95

    /// Build the analysis_health summary from the support-safe
    /// activity snapshot. The builder is pure — no I/O, no clock,
    /// no actor hops — so the test suite can drive it with a
    /// hand-rolled fixture and the production exporter can call it
    /// off-main without further serialization.
    ///
    /// Returns `nil` for a missing snapshot; the caller is expected
    /// to either skip the analysis_health field entirely (preserving
    /// schema v1 shape) or attach an empty-but-noted summary via
    /// `noSnapshot(...)`.
    static func build(
        from activitySnapshot: DogfoodDiagnosticsActivitySnapshot,
        duplicates: DuplicateCounts? = nil,
        learning: LearningCounts? = nil,
        generatedAt: Date
    ) -> DogfoodDiagnosticsAnalysisHealth {
        let rows = activitySnapshot.rows
        let assets = rows.map { row in
            buildAssetSummary(from: row)
        }
        let allStalenessFlags = rows.flatMap { row in
            stalenessFlags(for: row)
        }
        let global = buildGlobalSummary(
            rows: rows,
            assets: assets,
            stalenessFlags: allStalenessFlags
        )
        return DogfoodDiagnosticsAnalysisHealth(
            summarySchemaVersion: currentSummarySchemaVersion,
            generatedAt: generatedAt,
            global: global,
            assets: assets,
            stalenessFlags: allStalenessFlags,
            duplicates: duplicates,
            learning: learning,
            captureNote: activitySnapshot.captureError.map {
                redactedTruncated("activity_capture_error: \($0)")
            }
        )
    }

    /// Construct a summary whose `assets` list is empty and whose
    /// `captureNote` explains why. Used when the activity snapshot
    /// itself is `nil` (typically: AnalysisStore failed to open
    /// before export ran).
    static func noSnapshot(
        reason: String,
        generatedAt: Date,
        duplicates: DuplicateCounts? = nil,
        learning: LearningCounts? = nil
    ) -> DogfoodDiagnosticsAnalysisHealth {
        DogfoodDiagnosticsAnalysisHealth(
            summarySchemaVersion: currentSummarySchemaVersion,
            generatedAt: generatedAt,
            global: GlobalSummary(
                totalAssets: 0,
                runningCount: 0,
                queuedCount: 0,
                pausedCount: 0,
                failedCount: 0,
                unavailableCount: 0,
                terminalCompletedCount: 0,
                staleTerminalCount: 0,
                staleWatermarkCount: 0,
                unknownProgressCount: 0,
                latestJobUpdateAt: nil,
                latestTerminalWorkAt: nil,
                latestArtifactWatermarkSec: nil,
                latestTerminalWorkOutcome: nil
            ),
            assets: [],
            stalenessFlags: [],
            duplicates: duplicates,
            learning: learning,
            captureNote: redactedTruncated("no_activity_snapshot: \(reason)")
        )
    }

    // MARK: - Internal helpers

    private static func buildAssetSummary(
        from row: DogfoodDiagnosticsActivityRow
    ) -> AssetSummary {
        let pipeline = row.pipeline
        let asset = row.analysisAsset
        let session = row.latestSession
        let job = row.latestJob
        let work = row.latestTerminalWorkJournal

        let watermarkDelta: Double?
        switch (pipeline.transcriptCoveredSec, pipeline.fastTranscriptWatermarkSec) {
        case let (covered?, watermark?):
            watermarkDelta = covered - watermark
        default:
            watermarkDelta = nil
        }

        let recommendation = recommendation(for: row, watermarkDelta: watermarkDelta)

        return AssetSummary(
            episodeIdHash: row.episodeIdHash,
            section: row.section,
            analysisState: asset.analysisState,
            isRunning: row.isRunning,
            queuePosition: row.queuePosition,
            cachedAudioPresent: row.cachedAudioPresent,
            downloadPercent: pipeline.downloadPercent,
            transcriptPercent: pipeline.transcriptPercent,
            analysisPercent: pipeline.analysisPercent,
            transcriptCoveredSec: pipeline.transcriptCoveredSec,
            transcriptWatermarkSec: pipeline.transcriptWatermarkSec,
            fastTranscriptWatermarkSec: pipeline.fastTranscriptWatermarkSec,
            watermarkDeltaSec: watermarkDelta,
            analysisWatermarkSec: pipeline.analysisWatermarkSec,
            finalPassCoverageEndSec: pipeline.finalPassCoverageEndSec,
            progressProvenance: ProgressProvenance(
                downloadSource: pipeline.downloadSource,
                transcriptSource: pipeline.transcriptSource,
                analysisSource: pipeline.analysisSource
            ),
            latestSessionState: session?.state,
            latestSessionFailureReason: session?.failureReason,
            latestJobState: job?.state,
            latestJobLastErrorCode: job?.lastErrorCode,
            latestJobNextEligibleAt: job?.nextEligibleAt,
            latestJobLeasePresent: job?.leasePresent,
            latestTerminalWorkEvent: work?.eventType,
            latestTerminalWorkCause: work?.cause,
            terminalReason: asset.terminalReason,
            recommendedAction: recommendation.action,
            recommendedActionNote: recommendation.note
        )
    }

    private static func stalenessFlags(
        for row: DogfoodDiagnosticsActivityRow
    ) -> [StalenessFlag] {
        var flags: [StalenessFlag] = []
        let pipeline = row.pipeline
        let asset = row.analysisAsset

        // Terminal-state contradictions. Each completion terminal has
        // a different "what should be covered" contract (gtt9.8):
        //   * completeFull          → feature AND transcript at threshold
        //   * completeFeatureOnly   → feature at threshold; transcript
        //                             intentionally low (preview-only)
        //   * completeTranscriptPartial → transcript intentionally short
        //                                 of threshold; feature should
        //                                 still cover up to the
        //                                 transcript ceiling
        //   * complete (legacy)     → both should be roughly covered
        // We only flag contradictions for the cases where the
        // expectation actually exists — flagging completeFeatureOnly's
        // low transcript coverage would be noise, not signal.
        if let duration = pipeline.episodeDurationSec, duration > 0 {
            let threshold = duration * terminalCoverageRatioThreshold
            let featureBest = bestKnown(
                pipeline.featureCoverageEndSec,
                pipeline.confirmedAdCoverageEndSec
            )
            // For completeFull/legacy-complete the contract is feature AND
            // transcript at threshold — a shortfall on EITHER axis is a
            // contradiction. We compute the two axes independently (taking
            // the max-known value within each axis, so a non-nil watermark
            // can stand in for a nil chunk-coverage) and flag if either
            // axis fails. Folding the axes into a single max would mask
            // the case where one is healthy and the other is empty.
            let transcriptAxis = bestKnown(
                pipeline.transcriptCoveredSec,
                pipeline.fastTranscriptWatermarkSec
            ) ?? 0
            let featureAxis = featureBest ?? 0
            switch asset.analysisState {
            case "completeFull", "complete":
                if transcriptAxis < threshold || featureAxis < threshold {
                    flags.append(StalenessFlag(
                        episodeIdHash: row.episodeIdHash,
                        kind: .terminalStateContradictsCoverage,
                        detail: redactedTruncated(
                            "state=\(asset.analysisState) duration=\(formatSeconds(duration)) "
                                + "transcript_axis=\(formatSeconds(transcriptAxis)) "
                                + "feature_axis=\(formatSeconds(featureAxis)) "
                                + "threshold=\(formatSeconds(threshold))"
                        )
                    ))
                }
            case "completeFeatureOnly":
                let featureCoverage = featureBest ?? 0
                if featureCoverage < threshold {
                    flags.append(StalenessFlag(
                        episodeIdHash: row.episodeIdHash,
                        kind: .terminalStateContradictsCoverage,
                        detail: redactedTruncated(
                            "state=completeFeatureOnly duration=\(formatSeconds(duration)) "
                                + "feature_coverage=\(formatSeconds(featureCoverage)) "
                                + "threshold=\(formatSeconds(threshold))"
                        )
                    ))
                }
            case "completeTranscriptPartial":
                // Transcript is intentionally partial; flag only if
                // transcript coverage is genuinely zero (nothing
                // advanced) — that's the contradiction worth
                // surfacing.
                let transcriptCoverage = pipeline.transcriptCoveredSec ?? 0
                if transcriptCoverage < 1.0 {
                    flags.append(StalenessFlag(
                        episodeIdHash: row.episodeIdHash,
                        kind: .terminalStateContradictsCoverage,
                        detail: redactedTruncated(
                            "state=completeTranscriptPartial transcript_coverage=\(formatSeconds(transcriptCoverage))"
                        )
                    ))
                }
            default:
                break
            }
        }

        // Stale fast-transcript watermark — playhead-3bv.2 hazard.
        if let covered = pipeline.transcriptCoveredSec,
           let watermark = pipeline.fastTranscriptWatermarkSec,
           covered - watermark > staleWatermarkToleranceSec {
            flags.append(StalenessFlag(
                episodeIdHash: row.episodeIdHash,
                kind: .staleFastTranscriptWatermark,
                detail: redactedTruncated(
                    "transcript_covered=\(formatSeconds(covered)) "
                        + "fast_watermark=\(formatSeconds(watermark)) "
                        + "delta=\(formatSeconds(covered - watermark))"
                )
            ))
        }

        // Unknown progress without pause — the user-visible "stuck"
        // state.
        let allUnknown = pipeline.downloadPercent == "--%"
            && pipeline.transcriptPercent == "--%"
            && pipeline.analysisPercent == "--%"
        if allUnknown,
           row.status.disposition != "paused",
           row.status.disposition != "unavailable" {
            flags.append(StalenessFlag(
                episodeIdHash: row.episodeIdHash,
                kind: .unknownProgressWithoutPause,
                detail: redactedTruncated(
                    "disposition=\(row.status.disposition) "
                        + "reason=\(row.status.reason)"
                )
            ))
        }

        // Stale lease — the durable job is leased but the latest
        // session row's `updatedAt` already overtook the lease's
        // `leaseExpiresAt` (the runner finished the work but the
        // lease row is still pinned). The session's updatedAt is
        // the only deterministic anchor in the snapshot — we
        // intentionally do not compare against wall-clock `now`
        // because the snapshot may have been captured arbitrarily
        // before the export finally serialized.
        if let job = row.latestJob,
           job.leasePresent,
           let expires = job.leaseExpiresAt,
           let sessionUpdatedAt = row.latestSession?.updatedAt,
           expires < sessionUpdatedAt {
            flags.append(StalenessFlag(
                episodeIdHash: row.episodeIdHash,
                kind: .staleJobLease,
                detail: redactedTruncated(
                    "lease_expires_at=\(expires) "
                        + "session_updated_at=\(sessionUpdatedAt)"
                )
            ))
        }

        // Missing failure reason on a terminal-failure asset.
        if isTerminalFailureState(asset.analysisState),
           (asset.terminalReason ?? "").isEmpty,
           (row.latestSession?.failureReason ?? "").isEmpty {
            flags.append(StalenessFlag(
                episodeIdHash: row.episodeIdHash,
                kind: .missingFailureReason,
                detail: "state=\(asset.analysisState)"
            ))
        }

        return flags
    }

    private static func buildGlobalSummary(
        rows: [DogfoodDiagnosticsActivityRow],
        assets: [AssetSummary],
        stalenessFlags: [StalenessFlag]
    ) -> GlobalSummary {
        var running = 0
        var queued = 0
        var paused = 0
        var failed = 0
        var unavailable = 0
        var terminalCompleted = 0
        var unknownProgress = 0
        var latestJobUpdate: Double?
        var latestTerminalWork: Double?
        var latestArtifactWatermark: Double?
        var latestTerminalWorkOutcome: String?

        for row in rows {
            if row.isRunning { running += 1 }
            switch row.status.disposition {
            case "queued":
                queued += 1
            case "paused":
                paused += 1
            case "failed":
                failed += 1
            case "unavailable":
                unavailable += 1
            default:
                break
            }
            if isTerminalCompletionState(row.analysisAsset.analysisState) {
                terminalCompleted += 1
            }
            if row.pipeline.downloadPercent == "--%"
                && row.pipeline.transcriptPercent == "--%"
                && row.pipeline.analysisPercent == "--%" {
                unknownProgress += 1
            }
            if let updatedAt = row.latestJob?.updatedAt {
                latestJobUpdate = max(latestJobUpdate ?? updatedAt, updatedAt)
            }
            if let work = row.latestTerminalWorkJournal {
                if work.timestamp > (latestTerminalWork ?? -.infinity) {
                    latestTerminalWork = work.timestamp
                    latestTerminalWorkOutcome = work.eventType
                }
            }
            let candidates: [Double?] = [
                row.pipeline.transcriptCoveredSec,
                row.pipeline.featureCoverageEndSec,
                row.pipeline.confirmedAdCoverageEndSec,
                row.pipeline.finalPassCoverageEndSec
            ]
            for value in candidates {
                if let value, value > (latestArtifactWatermark ?? -.infinity) {
                    latestArtifactWatermark = value
                }
            }
        }

        let staleTerminal = stalenessFlags.filter {
            $0.kind == .terminalStateContradictsCoverage
        }.count
        let staleWatermark = stalenessFlags.filter {
            $0.kind == .staleFastTranscriptWatermark
        }.count

        return GlobalSummary(
            totalAssets: rows.count,
            runningCount: running,
            queuedCount: queued,
            pausedCount: paused,
            failedCount: failed,
            unavailableCount: unavailable,
            terminalCompletedCount: terminalCompleted,
            staleTerminalCount: staleTerminal,
            staleWatermarkCount: staleWatermark,
            unknownProgressCount: unknownProgress,
            latestJobUpdateAt: latestJobUpdate,
            latestTerminalWorkAt: latestTerminalWork,
            latestArtifactWatermarkSec: latestArtifactWatermark,
            latestTerminalWorkOutcome: latestTerminalWorkOutcome
        )
    }

    private struct Recommendation {
        let action: RecommendedAction
        let note: String?
    }

    private static func recommendation(
        for row: DogfoodDiagnosticsActivityRow,
        watermarkDelta: Double?
    ) -> Recommendation {
        let asset = row.analysisAsset

        // Terminal failure outweighs everything else.
        if isTerminalFailureState(asset.analysisState) {
            return Recommendation(
                action: .retry,
                note: redactedTruncated(
                    "session_state=\(asset.analysisState) "
                        + "reason=\(asset.terminalReason ?? row.latestSession?.failureReason ?? "unknown")"
                )
            )
        }

        // Terminal-completion contradiction → file a bug. Mirror the
        // per-state coverage contracts the staleness builder uses so
        // the recommendation never disagrees with the flag list.
        if let duration = row.pipeline.episodeDurationSec,
           duration > 0 {
            let threshold = duration * terminalCoverageRatioThreshold
            let featureBest = bestKnown(
                row.pipeline.featureCoverageEndSec,
                row.pipeline.confirmedAdCoverageEndSec
            )
            // Same axis-independent OR semantics as `stalenessFlags(for:)`.
            let transcriptAxis = bestKnown(
                row.pipeline.transcriptCoveredSec,
                row.pipeline.fastTranscriptWatermarkSec
            ) ?? 0
            let featureAxis = featureBest ?? 0
            switch asset.analysisState {
            case "completeFull", "complete":
                if transcriptAxis < threshold || featureAxis < threshold {
                    return Recommendation(
                        action: .fileBug,
                        note: "terminal state but transcript=\(formatSeconds(transcriptAxis)) "
                            + "feature=\(formatSeconds(featureAxis)) "
                            + "threshold=\(formatSeconds(threshold))"
                    )
                }
            case "completeFeatureOnly":
                let featureCoverage = featureBest ?? 0
                if featureCoverage < threshold {
                    return Recommendation(
                        action: .fileBug,
                        note: "completeFeatureOnly but feature coverage \(formatSeconds(featureCoverage))/\(formatSeconds(duration))"
                    )
                }
            case "completeTranscriptPartial":
                let transcriptCoverage = row.pipeline.transcriptCoveredSec ?? 0
                if transcriptCoverage < 1.0 {
                    return Recommendation(
                        action: .fileBug,
                        note: "completeTranscriptPartial with transcript_coverage=\(formatSeconds(transcriptCoverage))"
                    )
                }
            default:
                break
            }
        }

        // No cached audio + no live progress → user has to open the
        // app so the foreground manager resumes.
        if !row.cachedAudioPresent,
           row.liveDownloadFraction == nil,
           row.pipeline.downloadPercent == "--%" {
            return Recommendation(
                action: .openApp,
                note: "no_cached_audio_and_no_live_download"
            )
        }

        // Stale lease hint — surfaced for visibility, not blocking.
        if let job = row.latestJob,
           job.leasePresent,
           let expires = job.leaseExpiresAt,
           let updated = row.latestSession?.updatedAt,
           expires < updated {
            return Recommendation(
                action: .clearStaleLease,
                note: "lease_expires_at=\(expires) session_updated_at=\(updated)"
            )
        }

        // Stale watermark gap → wait (the next backfill tick will
        // reconcile playhead-3bv.2).
        if let delta = watermarkDelta, delta > staleWatermarkToleranceSec {
            return Recommendation(
                action: .plugInOrWait,
                note: "stale_watermark_delta=\(formatSeconds(delta))"
            )
        }

        // Already-running rows just need to wait.
        if row.isRunning {
            return Recommendation(action: .wait, note: "currently_running")
        }

        // Cached + queued → wait for next backfill tick. If transcript
        // hasn't progressed, it's almost always thermal.
        if row.cachedAudioPresent,
           row.status.disposition == "queued" {
            if row.pipeline.transcriptPercent == "--%" {
                return Recommendation(
                    action: .plugInOrWait,
                    note: "queued_with_cached_audio_no_transcript"
                )
            }
            return Recommendation(action: .wait, note: "queued_with_progress")
        }

        return Recommendation(action: .unknown, note: nil)
    }

    // MARK: - Closed-vocabulary state helpers

    private static func isTerminalCompletionState(_ raw: String) -> Bool {
        switch raw {
        case "complete", "completeFull", "completeFeatureOnly", "completeTranscriptPartial":
            return true
        default:
            return false
        }
    }

    private static func isTerminalFailureState(_ raw: String) -> Bool {
        switch raw {
        case "failed", "failedTranscript", "failedFeature", "cancelledBudget":
            return true
        default:
            return false
        }
    }

    // MARK: - Formatting / sanitization

    /// Format seconds as `12.3s` so the JSON is readable at a glance
    /// without inflating the payload with full Float64 precision.
    private static func formatSeconds(_ value: Double) -> String {
        String(format: "%.1fs", value)
    }

    /// Pick the largest non-nil value; returns nil when all are nil.
    /// Used by the staleness builders to find the "most progress
    /// observed" across multiple potentially-stale watermarks.
    private static func bestKnown(_ values: Double?...) -> Double? {
        values.compactMap { $0 }.max()
    }

    /// Defense-in-depth scrub for the `detail` strings in
    /// `StalenessFlag` / `recommended_action_note`. The activity
    /// snapshot already sanitizes upstream `failureReason` /
    /// `terminalReason` / `lastErrorCode`, but we re-run the same
    /// strip here so a future caller passing in raw strings cannot
    /// accidentally widen the export's privacy surface. Length-bound
    /// to keep the JSON compact.
    static func redactedTruncated(
        _ raw: String,
        limit: Int = 200
    ) -> String {
        let stripPatterns = [
            #"file://[^\s]*"#,
            #"https?://[^\s]*"#,
            #"/Users/[^\s]*"#,
            #"/var/mobile/[^\s]*"#,
            #"/private/var/[^\s]*"#
        ]
        var s = raw
        for pattern in stripPatterns {
            s = s.replacingOccurrences(
                of: pattern,
                with: "[redacted]",
                options: .regularExpression
            )
        }
        if s.count > limit {
            s = String(s.prefix(limit)) + "…"
        }
        return s
    }
}
