// SilenceCompressionCoordinator.swift
// playhead-epii — host coordinator that drives `SilenceCompressor`
// from the live playback stream and translates its decisions into
// `PlaybackService.beginCompression` / `endCompression` calls.
//
// Why a separate file: `SilenceCompressor` is pure (no AVFoundation,
// no AnalysisStore, no actor). The coordinator is the AVFoundation-
// and SQLite-aware glue. Splitting them keeps the planner trivially
// unit-testable and isolates the integration concerns (lookahead
// pacing, asset-id transitions, race avoidance) into one place.
//
// Lifecycle (per episode):
//   1. PlayheadRuntime calls `beginEpisode(assetId:keepFullMusic:)`
//      after `analysisCoordinator.handlePlaybackEvent(.playStarted)`
//      resolves the asset id.
//   2. The playback observation loop calls `notePlayhead(time:)` on
//      every state tick (already coalesced upstream); the coordinator
//      decides whether to refresh the lookahead window plan and
//      calls `tick(currentTime:)` on the underlying compressor at the
//      configured cadence.
//   3. SkipOrchestrator's `setSkipCueHandler` notifies the
//      coordinator of changed skip cues so plans are filtered.
//   4. On `endEpisode()` (or asset change) the coordinator clears
//      compressor state and disengages on PlaybackService.

@preconcurrency import AVFoundation
import Foundation
import OSLog

// MARK: - SilenceCompressionPlaybackControlling

/// Narrow seam over the PlaybackService surface this coordinator
/// touches. Production wires in the real `PlaybackService`; tests
/// pass a recording double so they can assert on the
/// `beginCompression` / `endCompression` calls without standing up
/// AVFoundation.
protocol SilenceCompressionPlaybackControlling: Sendable {
    func beginCompression(multiplier: Float, algorithm: AVAudioTimePitchAlgorithm) async
    func endCompression() async
}

extension PlaybackService: SilenceCompressionPlaybackControlling {
    // PlaybackService already exposes the two methods on
    // PlaybackServiceActor; this conformance is the thin async
    // bridge. The actor isolation is preserved by the `await` at
    // the call site.
}

// MARK: - SilenceCompressionAnalysisSourcing

/// Narrow seam over `AnalysisStore.fetchFeatureWindows`. Production
/// passes a closure that hops onto the store actor; tests pass a
/// recording closure that returns canned window arrays.
protocol SilenceCompressionAnalysisSourcing: Sendable {
    func fetchWindows(
        assetId: String, from: Double, to: Double
    ) async throws -> [FeatureWindow]
}

// MARK: - SilenceCompressionCoordinator

/// Drives the `SilenceCompressor` planner from playback events.
/// Single-writer (the playback observation Task in PlayheadRuntime
/// is the only caller); not `Sendable` itself because the underlying
/// compressor isn't, but all mutating methods are async-confined to
/// that single caller in production.
///
/// MainActor-bound because PlayheadRuntime is, and because the
/// "Keep full music" SwiftData read happens on the main context.
@MainActor
final class SilenceCompressionCoordinator {

    // MARK: - Dependencies

    private let playback: any SilenceCompressionPlaybackControlling
    private let source: any SilenceCompressionAnalysisSourcing
    private let compressor: SilenceCompressor
    private let config: SilenceCompressorConfig
    private let logger = Logger(
        subsystem: "com.playhead.app",
        category: "SilenceCompressionCoordinator"
    )

    // MARK: - State

    private var currentAssetId: String?
    private var lastWindowsRefreshTime: TimeInterval = -.greatestFiniteMagnitude
    private var lastTickTime: TimeInterval = -.greatestFiniteMagnitude
    private var keepFullMusic: Bool = false
    private var inFlightWindowsRefresh: Task<Void, Never>?

    // MARK: - Init

    init(
        playback: any SilenceCompressionPlaybackControlling,
        source: any SilenceCompressionAnalysisSourcing,
        config: SilenceCompressorConfig = .default
    ) {
        self.playback = playback
        self.source = source
        self.config = config
        self.compressor = SilenceCompressor(config: config)
    }

    // MARK: - Episode lifecycle

    /// Called when a new episode begins playback (or the asset id
    /// resolves slightly later, in the streaming-download path).
    /// Cancels any in-flight lookahead from a prior asset and resets
    /// internal pacing state so the next `notePlayhead(time:)` call
    /// always triggers a fresh fetch.
    func beginEpisode(assetId: String, keepFullMusic: Bool) async {
        inFlightWindowsRefresh?.cancel()
        inFlightWindowsRefresh = nil
        currentAssetId = assetId
        self.keepFullMusic = keepFullMusic
        compressor.recordKeepFullMusicOverride(keepFullMusic)
        compressor.replaceWindows([], assetId: assetId)
        lastWindowsRefreshTime = -.greatestFiniteMagnitude
        lastTickTime = -.greatestFiniteMagnitude
        await playback.endCompression()
    }

    /// Called when playback stops or the user navigates away. Forces
    /// the planner back to idle and disengages PlaybackService.
    func endEpisode() async {
        inFlightWindowsRefresh?.cancel()
        inFlightWindowsRefresh = nil
        currentAssetId = nil
        compressor.clearAll()
        await playback.endCompression()
    }

    // MARK: - Per-show override

    /// Push a per-show "Keep full music" toggle change. Idempotent.
    ///
    /// On flip-ON (override enabled) we disengage compression
    /// immediately and clear the planner so any in-flight rate change
    /// drops to base speed in real time.
    ///
    /// On flip-OFF (override disabled) we force the next `notePlayhead`
    /// call to refresh the lookahead window — without this the
    /// coordinator's `lastWindowsRefreshTime` cadence guard could leave
    /// the user without compression for up to `lookaheadCadenceSeconds`
    /// after re-enabling. Resetting the sentinel guarantees the next
    /// tick refetches and the compressor can re-engage as soon as a
    /// fresh plan covers the playhead.
    func updateKeepFullMusicOverride(_ enabled: Bool) async {
        keepFullMusic = enabled
        compressor.recordKeepFullMusicOverride(enabled)
        if enabled {
            await playback.endCompression()
        } else {
            // Force a re-fetch on the very next tick so a flip-OFF
            // mid-episode doesn't strand the user without compression
            // for an entire cadence interval. Also reset the per-tick
            // sentinel so the next `notePlayhead` is not held by the
            // tick-cadence early-return.
            lastWindowsRefreshTime = -.greatestFiniteMagnitude
            lastTickTime = -.greatestFiniteMagnitude
        }
    }

    // MARK: - Skip cues

    /// Skip orchestrator finished computing a new cue list. Filter
    /// plans to never overlap a cue (the skip path will jump past
    /// those regions wholesale).
    func updateSkipRanges(_ ranges: [(start: Double, end: Double)]) {
        compressor.updateSkipRanges(ranges)
    }

    // MARK: - Seek

    /// User initiated a seek. Drop any in-flight compression and
    /// force the next tick to re-fetch lookahead windows.
    func recordUserSeek(to time: TimeInterval) async {
        compressor.recordSeek(to: time)
        lastWindowsRefreshTime = -.greatestFiniteMagnitude
        // Reset the per-tick cadence sentinel so the next playhead
        // tick after the seek is not gated by the "tickDelta < cadence
        // AND not currently compressing" early-return path. After a
        // seek, the very next tick must re-evaluate from idle.
        lastTickTime = -.greatestFiniteMagnitude
        await playback.endCompression()
    }

    // MARK: - Speed change

    /// User changed their base playback speed. `PlaybackService.setSpeed`
    /// already cleared the compression multiplier and reset the
    /// algorithm to `.spectral`, but the planner's state machine has
    /// no visibility into that side-channel — without this hook it
    /// stays in `.compressing(plan)` and returns `.noChange` while
    /// inside the plan, leaving the rate at the user's new base for
    /// the rest of the plan. Resetting the planner to idle here means
    /// the very next tick re-evaluates the plan from a clean slate
    /// and re-engages compression on top of the new base speed.
    ///
    /// Also forces a windows refresh in case the user's speed change
    /// shifts the trade-off (e.g. user goes 1.0×→2.0× and we need to
    /// reconsider whether further compression is warranted).
    func recordUserSpeedChange() async {
        compressor.markIdle()
        lastWindowsRefreshTime = -.greatestFiniteMagnitude
        // Reset the per-tick cadence sentinel as well so the very
        // next call to `notePlayhead` is not stalled by the
        // "compressor not currently compressing AND tickDelta < cadence"
        // guard (which fires by design once compression has just
        // dropped, but we want the planner to re-evaluate
        // immediately after a manual speed change).
        lastTickTime = -.greatestFiniteMagnitude
        // Don't call endCompression — setSpeed already did, and a
        // redundant end is a no-op anyway.
    }

    // MARK: - Playhead tick

    /// Called from the playback observation loop on every state
    /// update where the transport is `.playing`. Performs three jobs:
    ///   - refresh the lookahead window plan if the cadence elapsed
    ///     OR the playhead crossed the trailing edge of the current
    ///     plan horizon,
    ///   - tick the planner at `lookaheadCadenceSeconds` granularity,
    ///   - apply the planner's decision to PlaybackService.
    /// Cheap when none of the above conditions fire — most calls
    /// land in the no-op path.
    func notePlayhead(time: TimeInterval) async {
        guard let assetId = currentAssetId, !keepFullMusic else { return }

        // Lookahead refresh: if we've moved past the cadence, fire a
        // fetch in the background and return — the next tick will
        // pick up the new plan. Fire-and-forget is safe because the
        // task captures `assetId` by value and bails on mismatch.
        let refreshDelta = time - lastWindowsRefreshTime
        if refreshDelta >= config.lookaheadCadenceSeconds {
            lastWindowsRefreshTime = time
            inFlightWindowsRefresh?.cancel()
            inFlightWindowsRefresh = Task { [weak self, assetId, source, config] in
                let from = max(0, time - 1.0)
                let to = time + config.lookaheadHorizonSeconds
                let windows = (try? await source.fetchWindows(
                    assetId: assetId, from: from, to: to
                )) ?? []
                guard let self else { return }
                guard self.currentAssetId == assetId else { return }
                self.compressor.replaceWindows(windows, assetId: assetId)
            }
        }

        // Tick cadence: don't re-evaluate the planner more often than
        // the configured cadence. AVPlayer periodic time observers
        // fire ~4 Hz; throttling prevents pointless decision churn.
        let tickDelta = time - lastTickTime
        if tickDelta < config.lookaheadCadenceSeconds, lastTickTime > 0 {
            // Always allow a tick if the planner is currently
            // compressing — we need to detect playhead crossing the
            // exit boundary promptly. Without this, exit can lag
            // by up to lookaheadCadenceSeconds.
            if !compressor.isCurrentlyCompressing { return }
        }
        lastTickTime = time
        let decision = compressor.tick(currentTime: time)
        await apply(decision)
    }

    // MARK: - Decision application

    private func apply(_ decision: SilenceCompressorDecision) async {
        switch decision {
        case .noChange:
            return
        case .engage(let multiplier, let algorithm, let plan):
            logger.debug(
                "→ engage @ \(plan.startTime, format: .fixed(precision: 2))s..\(plan.endTime, format: .fixed(precision: 2))s × \(multiplier) (\(algorithm.rawValue, privacy: .public))"
            )
            await playback.beginCompression(
                multiplier: multiplier,
                algorithm: avAlgorithm(for: algorithm)
            )
        case .disengage:
            logger.debug("→ disengage")
            await playback.endCompression()
        }
    }

    private func avAlgorithm(
        for algorithm: CompressionAlgorithm
    ) -> AVAudioTimePitchAlgorithm {
        switch algorithm {
        case .spectral: return .spectral
        case .varispeed: return .varispeed
        }
    }

    // MARK: - Test inspection

    var isCompressingForTesting: Bool { compressor.isCurrentlyCompressing }
    var currentPlansForTesting: [CompressionPlan] { compressor.currentPlans }
}

// MARK: - Default analysis source

/// Production source: hops onto the AnalysisStore actor and reads
/// the requested window range. Held by PlayheadRuntime; produced
/// fresh per coordinator construction so the closure capture is a
/// single retain on the store.
struct AnalysisStoreSilenceSource: SilenceCompressionAnalysisSourcing {
    let store: AnalysisStore

    func fetchWindows(
        assetId: String, from: Double, to: Double
    ) async throws -> [FeatureWindow] {
        try await store.fetchFeatureWindows(assetId: assetId, from: from, to: to)
    }
}
