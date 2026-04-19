// EpisodeSurfaceStatusObserver.swift
// Minimal production consumer that routes real episode lifecycle signals
// through `EpisodeSurfaceStatusReducer` + `SurfaceStatusReadyTransitionEmitter`
// so `ready_entered` events are emitted to the ol05 JSONL audit log.
//
// Scope: playhead-o45p — closes the scope gap flagged by spec review
// (emitter existed but had zero production call sites; without this
// consumer `ready_entered` would never fire in production and the
// false_ready_rate metric's denominator would collapse to zero).
//
// Deliberate minimalism:
//   * Two entry points only — `observeEpisodePlayStarted(...)` for the
//     cold-start path (called from `PlayheadRuntime.playEpisode`) and
//     `observeAnalysisSessionComplete(...)` for the analysis-completion
//     path (called from `AnalysisCoordinator.transition` on a successful
//     transition to `.complete`).
//   * Builds `AnalysisState` + `AnalysisEligibility` inline from the
//     already-available persistence and capability snapshots. No new
//     subsystem for coverage summaries, readiness anchors, or fine-
//     grained eligibility gating — those are Phase 2 concerns.
//   * No UI work. The observer emits to the JSONL log and returns. Badge
//     / banner / timeline updates are downstream beads.
//
// Concurrency: actor-isolated so the non-thread-safe
// `SurfaceStatusReadyTransitionEmitter`'s per-episode memory is
// accessed from a single serial context. Callable `await`-ed from any
// other actor (MainActor, AnalysisCoordinator, etc.).
//
// Hashing: uses `SurfaceStatusInvariantLogger.hashEpisodeId(_:)` so the
// episode_id_hash on `ready_entered` events is byte-identical to the
// hash SkipOrchestrator stamps on `auto_skip_fired`. Cross-event
// correlation in `scripts/false_ready_rate.swift` depends on this.

import Foundation
import os

/// Production consumer of `EpisodeSurfaceStatusReducer`. Feeds real
/// episode lifecycle signals through the reducer + transition emitter so
/// `ready_entered` events are emitted to the surface-status JSONL log.
///
/// One instance per `PlayheadRuntime`; wired into the runtime's DI graph
/// and called from `playEpisode(_:)` (cold start) and
/// `AnalysisCoordinator.transition(...)` (analysis-completion edge).
actor EpisodeSurfaceStatusObserver {

    // MARK: - Dependencies

    /// Persistence handle. The observer reads the episode's current
    /// `AnalysisAsset` row to build the reducer's `AnalysisState` input.
    private let store: AnalysisStore

    /// Live capability snapshot provider. Closure so tests can inject a
    /// deterministic snapshot without spinning up the real
    /// `CapabilitiesService`.
    private let capabilitySnapshotProvider: @Sendable () async -> CapabilitySnapshot?

    /// Pluggable hasher so tests can assert on a known hash value
    /// without depending on the global logger salt. Production passes
    /// `SurfaceStatusInvariantLogger.hashEpisodeId` so production events
    /// are byte-identical to the `auto_skip_fired` hashes emitted by
    /// SkipOrchestrator.
    private let episodeIdHasher: @Sendable (String) -> String

    /// The stateful emitter. Owns per-episode memory of the last
    /// observed readiness so `ready_entered` fires exactly once per
    /// transition INTO ready.
    private let emitter: SurfaceStatusReadyTransitionEmitter

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "EpisodeSurfaceStatusObserver"
    )

    // MARK: - Init

    init(
        store: AnalysisStore,
        capabilitySnapshotProvider: @escaping @Sendable () async -> CapabilitySnapshot?,
        episodeIdHasher: @escaping @Sendable (String) -> String = SurfaceStatusInvariantLogger.hashEpisodeId,
        emitter: SurfaceStatusReadyTransitionEmitter = SurfaceStatusReadyTransitionEmitter()
    ) {
        self.store = store
        self.capabilitySnapshotProvider = capabilitySnapshotProvider
        self.episodeIdHasher = episodeIdHasher
        self.emitter = emitter
    }

    // MARK: - Production entry points

    /// Run the reducer for `episodeId` in the "episode play started"
    /// context. The emitter infers a `.coldStart` trigger when the
    /// episode has not been seen before in this process.
    ///
    /// Called from `PlayheadRuntime.playEpisode(_:)` once an analysis
    /// asset has been resolved. Best-effort — failures to fetch the
    /// asset or build the inputs are swallowed after a warning log.
    func observeEpisodePlayStarted(episodeId: String) async {
        await runReducerFor(
            episodeId: episodeId,
            explicitTrigger: nil
        )
    }

    /// Run the reducer for `episodeId` after a session-state transition.
    /// When the transition is to `.complete`, an explicit
    /// `.analysisCompleted` trigger is passed so the JSONL entry's
    /// `entry_trigger` distinguishes analysis-completion ready events
    /// from cold-start ready events.
    ///
    /// Called from `AnalysisCoordinator.transition(...)` immediately
    /// after `updateAssetState` succeeds. Best-effort — failures are
    /// logged and swallowed; analysis pipeline correctness does not
    /// depend on the audit signal.
    func observeAnalysisSessionComplete(episodeId: String) async {
        await runReducerFor(
            episodeId: episodeId,
            explicitTrigger: .analysisCompleted
        )
    }

    // MARK: - Internal

    /// Fetch the latest persisted snapshot for `episodeId`, map it to the
    /// reducer's input shape, and route through the emitter.
    private func runReducerFor(
        episodeId: String,
        explicitTrigger: SurfaceStateTransitionEntryTrigger?
    ) async {
        let asset: AnalysisAsset?
        do {
            asset = try await store.fetchAssetByEpisodeId(episodeId)
        } catch {
            logger.warning(
                "EpisodeSurfaceStatusObserver: fetchAssetByEpisodeId failed for episode \(episodeId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return
        }

        // No asset yet — the episode has never been queued for analysis.
        // Not a ready transition; nothing to emit.
        guard let asset else { return }

        let capabilitySnapshot = await capabilitySnapshotProvider()
        let eligibility = Self.eligibility(from: capabilitySnapshot)
        let state = Self.analysisState(from: asset)
        let episodeIdHash = episodeIdHasher(episodeId)

        // Cold-start path (`explicitTrigger == nil`): the emitter will
        // infer `.coldStart` when it has never seen this episode before,
        // `.unblocked` otherwise.
        //
        // Analysis-completion path (`explicitTrigger == .analysisCompleted`):
        // the emitter always stamps the entry with `.analysisCompleted`.
        _ = emitter.reduceAndEmit(
            episodeIdHash: episodeIdHash,
            state: state,
            cause: nil,
            eligibility: eligibility,
            coverage: nil,
            readinessAnchor: nil,
            trigger: explicitTrigger
        )
    }

    // MARK: - Mapping helpers

    /// Map a persisted `AnalysisAsset.analysisState` string to the
    /// reducer's `AnalysisState` value object.
    ///
    /// `SessionState` has more intermediate states (`spooling`,
    /// `featuresReady`, `hotPathReady`, `backfill`) than the reducer's
    /// `PersistedStatus` enum cares about. Every non-terminal state maps
    /// to `.running`; `complete` maps to `.done`; `failed` stays
    /// `.failed`; `queued` stays `.queued`. Unknown strings fall through
    /// to `.new` (the reducer treats `.new` as equivalent to `.queued`).
    static func analysisState(from asset: AnalysisAsset) -> AnalysisState {
        let persisted: AnalysisState.PersistedStatus
        if let sessionState = SessionState(rawValue: asset.analysisState) {
            switch sessionState {
            case .queued:
                persisted = .queued
            case .spooling, .featuresReady, .hotPathReady, .backfill:
                persisted = .running
            case .complete:
                persisted = .done
            case .failed:
                persisted = .failed
            }
        } else {
            // Forward-compat: an unknown persisted string falls to
            // `.new`, which the reducer treats as ready-for-playback
            // when no blocking cause is live.
            persisted = .new
        }

        // Phase 1.5 — the observer does not yet consume work-journal
        // terminal causes. The reducer tolerates both flags being
        // `false` (falls through to Rule 5 "queued" when no cause is
        // supplied), which is the correct minimal-scope behavior.
        return AnalysisState(
            persistedStatus: persisted,
            hasUserPreemptedJob: false,
            hasAppForceQuitFlag: false,
            pendingSinceEnqueuedAt: nil,
            hasAnyConfirmedAnalysis: (asset.confirmedAdCoverageEndTime ?? 0) > 0
        )
    }

    /// Map a live `CapabilitySnapshot` to the reducer's
    /// `AnalysisEligibility` input.
    ///
    /// Three of the five eligibility fields are directly observable from
    /// the snapshot (`appleIntelligenceEnabled`, `languageSupported` via
    /// `foundationModelsLocaleSupported`, `modelAvailableNow` via
    /// `canUseFoundationModels`). `hardwareSupported` and
    /// `regionSupported` default to `true` here — the dedicated
    /// `AnalysisEligibilityEvaluator` with per-field providers is not
    /// yet wired into production (Phase 2 scope). Defaulting to `true`
    /// is the conservative choice: it does NOT gate the reducer on
    /// Rule 1 (eligibility-blocks), so ready transitions are measurable
    /// on all hardware. When a truly-ineligible device shows up in the
    /// audit, the paired `auto_skip_fired` denominator will still be
    /// zero and the false_ready_rate surfaces the miss.
    ///
    /// `nil` snapshot (capability service not yet primed) maps to a
    /// fully-eligible snapshot for the same reason.
    static func eligibility(from snapshot: CapabilitySnapshot?) -> AnalysisEligibility {
        guard let snapshot else {
            return AnalysisEligibility(
                hardwareSupported: true,
                appleIntelligenceEnabled: true,
                regionSupported: true,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: Date()
            )
        }
        return AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: snapshot.appleIntelligenceEnabled,
            regionSupported: true,
            languageSupported: snapshot.foundationModelsLocaleSupported,
            modelAvailableNow: snapshot.canUseFoundationModels,
            capturedAt: snapshot.capturedAt
        )
    }
}
