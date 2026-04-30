// EpisodeSummaryBackfillCoordinator.swift
// playhead-jzik: lazy, off-hot-path coordinator that walks the
// `analysis_assets` table for rows that have cleared the
// transcript-coverage threshold and fills in the corresponding
// `episode_summaries` rows by calling `EpisodeSummaryExtractor`.
//
// Why a separate actor (not a hook into AnalysisWorkScheduler):
// the scheduler's job-row machinery is built around active analysis
// — `analysis_jobs` lifecycle, lease semantics, the Now/Soon/Background
// lane invariants. Episode summaries are a derived artifact that the
// playback hot path never depends on, so threading them through the
// scheduler would force schema migrations on `analysis_jobs`, a new
// `JobKind` enum, and lease coordination that this bead does not need
// to pay for. A standalone polling coordinator is the smaller, more
// reversible structure: if Phase-N work later wants summaries on the
// scheduler's lanes the coordinator can be retired without leaving
// orphaned scheduler state behind.
//
// Lifecycle:
//   - Started by `PlayheadRuntime` after the analysis store has
//     migrated and the FM stack has had a chance to warm.
//   - Cancellable: a single long-running `Task` that loops
//     `pollOnce()` with a sleep between iterations. Stops cleanly on
//     task cancellation.
//   - Idempotent: missing capability or empty candidate list is a
//     no-op pass.

import Foundation
import OSLog

/// Configuration for the coordinator's polling cadence and per-pass
/// budget. Defaults are conservative — episode summaries are not on
/// the playback hot path, so a slow drip with backoff is fine.
struct EpisodeSummaryBackfillConfig: Sendable, Equatable {
    /// Minimum transcript coverage (≥80%) before an asset is summary-
    /// eligible. Mirrors the `Episode.coverageSummary.fastPercent`
    /// threshold the UI uses for the "ready" affordance.
    let coverageFraction: Double
    /// Maximum number of summaries to extract per polling pass. The
    /// coordinator processes candidates serially within a pass; this
    /// caps how long a single pass can run before yielding.
    let maxPerPass: Int
    /// Sleep between polling passes when the previous pass found
    /// candidates (so a backlog drains relatively quickly).
    let activePollInterval: TimeInterval
    /// Sleep between polling passes when the previous pass was a
    /// no-op (no candidates, capability unavailable, etc.). Longer so
    /// an idle device isn't constantly scanning the asset table.
    let idlePollInterval: TimeInterval

    static let `default` = EpisodeSummaryBackfillConfig(
        coverageFraction: 0.8,
        maxPerPass: 5,
        activePollInterval: 30,
        idlePollInterval: 5 * 60
    )
}

/// Outcome of a single polling pass. Used by tests to assert the
/// coordinator's routing without spinning the loop.
enum EpisodeSummaryBackfillPassOutcome: Sendable, Equatable {
    /// Capability snapshot reports FM unavailable — pass yields immediately.
    case capabilityUnavailable
    /// User has the feature toggled off in Settings.
    case userDisabled
    /// No candidates needed work this pass.
    case noCandidates
    /// `processed` candidates ran the extractor; `succeeded` produced
    /// rows and `terminallyRefused` hit `bothPathsRefused`.
    case processed(succeeded: Int, terminallyRefused: Int)
}

/// Narrow seam over `UserPreferences.episodeSummariesEnabled`. The
/// preference lives in SwiftData on the main actor; we don't want to
/// leak that dependency into the actor, so the runtime hands the
/// coordinator a closure that returns the current value.
typealias EpisodeSummaryUserToggle = @Sendable () async -> Bool

/// Narrow seam over the candidate-listing step. In production wires
/// the SQLite query on `AnalysisStore`; tests inject canned lists.
protocol EpisodeSummaryBackfillCandidateProvider: Sendable {
    func candidates(coverageFraction: Double, currentSchemaVersion: Int, limit: Int) async throws -> [String]
    /// Hydrate the input the extractor needs for `assetId`. Returns
    /// `nil` when the asset has been deleted, transcript chunks have
    /// been pruned, or any other reason the row no longer exists.
    func hydrate(assetId: String) async throws -> EpisodeSummaryBackfillInput?
}

/// Materialized inputs needed for one summary extraction. The
/// coordinator hydrates this off the analysis store before invoking
/// the extractor so the extractor itself stays storage-agnostic.
struct EpisodeSummaryBackfillInput: Sendable {
    let analysisAssetId: String
    let episodeTitle: String?
    let showTitle: String?
    let transcriptVersion: String?
    let chunks: [TranscriptChunk]
}

/// Production conformer that talks to `AnalysisStore` for the candidate
/// query and the chunk hydration. Show title is left `nil` here —
/// pulling it would require a SwiftData hop on the main actor that
/// this bead would rather not invent. Episode title comes from the
/// asset row's persisted `episodeTitle` column (lazily backfilled by
/// playhead-i9dj).
struct AnalysisStoreEpisodeSummaryBackfillCandidateProvider: EpisodeSummaryBackfillCandidateProvider {

    let store: AnalysisStore

    func candidates(
        coverageFraction: Double,
        currentSchemaVersion: Int,
        limit: Int
    ) async throws -> [String] {
        try await store.fetchEpisodeSummaryBackfillCandidates(
            coverageFraction: coverageFraction,
            currentSchemaVersion: currentSchemaVersion,
            limit: limit
        )
    }

    func hydrate(assetId: String) async throws -> EpisodeSummaryBackfillInput? {
        guard let asset = try await store.fetchAsset(id: assetId) else {
            return nil
        }
        let chunks = try await store.fetchTranscriptChunks(assetId: assetId)
        guard !chunks.isEmpty else { return nil }
        // Pick the most recent transcriptVersion observed across the
        // chunks. Fast-pass chunks carry `nil`, so we fall back to the
        // first non-nil if any. The coordinator stores this on the
        // summary row as the invalidation key.
        let transcriptVersion = chunks
            .compactMap(\.transcriptVersion)
            .last
        return EpisodeSummaryBackfillInput(
            analysisAssetId: assetId,
            episodeTitle: asset.episodeTitle,
            showTitle: nil,
            transcriptVersion: transcriptVersion,
            chunks: chunks
        )
    }
}

/// Narrow seam over the persistence write. Production wires
/// `AnalysisStore.upsertEpisodeSummary`; tests assert against an
/// in-memory recorder.
protocol EpisodeSummaryBackfillSink: Sendable {
    func persist(_ summary: EpisodeSummary) async throws
}

struct AnalysisStoreEpisodeSummaryBackfillSink: EpisodeSummaryBackfillSink {
    let store: AnalysisStore
    func persist(_ summary: EpisodeSummary) async throws {
        try await store.upsertEpisodeSummary(summary)
    }
}

/// Periodic coordinator that pulls summary-eligible assets and feeds
/// them through the extractor. One coordinator per process lifetime;
/// `start()` is idempotent and can be called from a deferred bootstrap
/// `Task` without worrying about double-start.
actor EpisodeSummaryBackfillCoordinator {

    private let extractor: EpisodeSummaryExtractor
    private let candidates: any EpisodeSummaryBackfillCandidateProvider
    private let sink: any EpisodeSummaryBackfillSink
    private let userToggle: EpisodeSummaryUserToggle
    private let config: EpisodeSummaryBackfillConfig
    private let clock: any Clock<Duration>
    private let logger: Logger

    private var loopTask: Task<Void, Never>?

    init(
        extractor: EpisodeSummaryExtractor,
        candidates: any EpisodeSummaryBackfillCandidateProvider,
        sink: any EpisodeSummaryBackfillSink,
        userToggle: @escaping EpisodeSummaryUserToggle,
        config: EpisodeSummaryBackfillConfig = .default,
        clock: any Clock<Duration> = ContinuousClock(),
        logger: Logger = Logger(subsystem: "com.playhead", category: "EpisodeSummaryBackfill")
    ) {
        self.extractor = extractor
        self.candidates = candidates
        self.sink = sink
        self.userToggle = userToggle
        self.config = config
        self.clock = clock
        self.logger = logger
    }

    /// Idempotent: subsequent calls observe the existing loop and
    /// return immediately. The loop runs until the actor is torn down
    /// or `stop()` is called.
    func start() {
        if loopTask != nil { return }
        loopTask = Task { [weak self] in
            await self?.runLoop()
        }
    }

    /// Stops the polling loop. Safe to call multiple times.
    func stop() {
        loopTask?.cancel()
        loopTask = nil
    }

    /// Run a single polling pass. Public for tests; production
    /// invocations come from the actor's own `runLoop`.
    func pollOnce() async -> EpisodeSummaryBackfillPassOutcome {
        guard await userToggle() else { return .userDisabled }
        let candidateIds: [String]
        do {
            candidateIds = try await candidates.candidates(
                coverageFraction: config.coverageFraction,
                currentSchemaVersion: EpisodeSummary.currentSchemaVersion,
                limit: config.maxPerPass
            )
        } catch {
            logger.warning("episode_summary_backfill_query_failed: \(String(describing: error), privacy: .private)")
            return .noCandidates
        }
        guard !candidateIds.isEmpty else { return .noCandidates }

        var succeeded = 0
        var terminallyRefused = 0
        var hitCapabilityUnavailable = false

        for assetId in candidateIds {
            if Task.isCancelled { break }
            let input: EpisodeSummaryBackfillInput?
            do {
                input = try await candidates.hydrate(assetId: assetId)
            } catch {
                logger.debug("episode_summary_backfill_hydrate_failed for \(assetId, privacy: .public): \(String(describing: error), privacy: .private)")
                continue
            }
            guard let input else { continue }

            do {
                let summary = try await extractor.extract(
                    analysisAssetId: input.analysisAssetId,
                    episodeTitle: input.episodeTitle,
                    showTitle: input.showTitle,
                    transcriptVersion: input.transcriptVersion,
                    chunks: input.chunks
                )
                try await sink.persist(summary)
                succeeded += 1
            } catch let error as EpisodeSummaryExtractionError {
                switch error {
                case .capabilityUnavailable:
                    // Capability vanished mid-pass — bail and let the
                    // outer loop pick the longer idle interval. We don't
                    // want to thrash the table while FM is unavailable.
                    hitCapabilityUnavailable = true
                    return .capabilityUnavailable
                case .bothPathsRefused:
                    terminallyRefused += 1
                    // No retry on this pass; the row stays a candidate
                    // until `transcriptVersion` shifts.
                case .insufficientCoverage,
                     .unparseableResponse:
                    // Transient — leave for next pass.
                    continue
                }
            } catch is CancellationError {
                break
            } catch {
                logger.debug("episode_summary_backfill_extract_failed for \(assetId, privacy: .public): \(String(describing: error), privacy: .private)")
                continue
            }
        }

        if hitCapabilityUnavailable {
            return .capabilityUnavailable
        }
        return .processed(succeeded: succeeded, terminallyRefused: terminallyRefused)
    }

    private func runLoop() async {
        while !Task.isCancelled {
            let outcome = await pollOnce()
            let interval: TimeInterval
            switch outcome {
            case .processed(let succeeded, _):
                interval = succeeded > 0 ? config.activePollInterval : config.idlePollInterval
            case .userDisabled, .noCandidates, .capabilityUnavailable:
                interval = config.idlePollInterval
            }
            do {
                try await clock.sleep(for: .seconds(interval))
            } catch {
                // Cancellation surfaces as a thrown error — exit cleanly.
                return
            }
        }
    }
}
