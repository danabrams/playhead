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
// Hashing: receives a hasher closure bound to the same
// `SurfaceStatusInvariantLogger` instance that SkipOrchestrator uses, so
// the episode_id_hash on `ready_entered` events is byte-identical to
// the hash stamped on `auto_skip_fired`. Cross-event correlation in
// `scripts/false_ready_rate.swift` depends on this.

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

    /// Live eligibility provider. Closure so tests can inject a
    /// deterministic verdict without standing up the real
    /// `AnalysisEligibilityEvaluator` + provider stack.
    ///
    /// playhead-4nt1: replaced the previous `capabilitySnapshotProvider`
    /// + static `eligibility(from:)` mapping. The observer now consumes
    /// the evaluator's structured `AnalysisEligibility` verdict directly
    /// — `hardwareSupported` and `regionSupported` are no longer
    /// hardcoded `true` inside the observer.
    private let eligibilityProvider: @Sendable () async -> AnalysisEligibility

    /// Pluggable hasher so tests can assert on a known hash value
    /// without depending on the production installId. Production passes
    /// a closure bound to the same `SurfaceStatusInvariantLogger`
    /// instance used by SkipOrchestrator so `ready_entered` and
    /// `auto_skip_fired` hashes are byte-identical.
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

    /// Production wiring passes `invariantLogger` and (usually) a hasher
    /// bound to it; the observer builds the emitter internally with a
    /// reducer closure that threads the logger through (so impossible-
    /// state warnings land on the injected logger) and with a sink
    /// closure that calls `invariantLogger.recordReadyEntered(...)`.
    ///
    /// Tests that only want to exercise the mapping+emission flow can
    /// pass just `store` + `eligibilityProvider` and accept the
    /// default logger (a fresh isolated instance) and derived hasher.
    ///
    /// playhead-jzdc: the `emitter:` defaulted parameter was removed so
    /// the emitter is provably single-owner (constructed inside the
    /// observer, never reachable from outside). Tests that need to
    /// observe what the emitter would emit use the sink-closure init
    /// below.
    ///
    /// playhead-4nt1: replaced `capabilitySnapshotProvider` with
    /// `eligibilityProvider` — production threads in the live
    /// `AnalysisEligibilityEvaluator`'s verdict so the observer no
    /// longer hardcodes `hardwareSupported` / `regionSupported`.
    init(
        store: AnalysisStore,
        eligibilityProvider: @escaping @Sendable () async -> AnalysisEligibility,
        invariantLogger: SurfaceStatusInvariantLogger = SurfaceStatusInvariantLogger(),
        episodeIdHasher: (@Sendable (String) -> String)? = nil
    ) {
        self.store = store
        self.eligibilityProvider = eligibilityProvider
        self.episodeIdHasher = episodeIdHasher ?? { [invariantLogger] episodeId in
            invariantLogger.hashEpisodeId(episodeId)
        }
        self.emitter = SurfaceStatusReadyTransitionEmitter(
            reducer: { state, cause, eligibility, coverage, anchor in
                episodeSurfaceStatus(
                    state: state,
                    cause: cause,
                    eligibility: eligibility,
                    coverage: coverage,
                    readinessAnchor: anchor,
                    invariantLogger: invariantLogger
                )
            },
            loggerSink: { [invariantLogger] episodeIdHash, trigger in
                invariantLogger.recordReadyEntered(
                    episodeIdHash: episodeIdHash,
                    trigger: trigger
                )
            }
        )
    }

    /// Test-only seam (playhead-jzdc): construct an observer whose
    /// internally-owned emitter routes its `ready_entered` notifications
    /// through `emitterSink` instead of the production
    /// `SurfaceStatusInvariantLogger`. Lets tests observe what the
    /// emitter would emit without ever taking a reference to the
    /// emitter object — preserves the single-owner invariant on
    /// `SurfaceStatusReadyTransitionEmitter`.
    ///
    /// This init also bypasses the reducer's `invariantLogger`
    /// passthrough: tests using the sink seam are exercising the
    /// mapping+emission flow, not the impossible-state logger.
    #if DEBUG
    init(
        store: AnalysisStore,
        eligibilityProvider: @escaping @Sendable () async -> AnalysisEligibility,
        episodeIdHasher: @escaping @Sendable (String) -> String,
        emitterSink: @escaping @Sendable (
            _ episodeIdHash: String?,
            _ trigger: SurfaceStateTransitionEntryTrigger?
        ) -> Void
    ) {
        self.store = store
        self.eligibilityProvider = eligibilityProvider
        self.episodeIdHasher = episodeIdHasher
        self.emitter = SurfaceStatusReadyTransitionEmitter(
            reducer: { state, cause, eligibility, coverage, anchor in
                episodeSurfaceStatus(
                    state: state,
                    cause: cause,
                    eligibility: eligibility,
                    coverage: coverage,
                    readinessAnchor: anchor
                )
            },
            loggerSink: emitterSink
        )
    }
    #endif

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

        let eligibility = await eligibilityProvider()
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
            case .spooling, .featuresReady, .hotPathReady, .waitingForBackfill, .backfill:
                // playhead-gtt9.8: `.waitingForBackfill` is a
                // non-terminal hold while the coordinator waits for
                // thermal/budget to allow the backfill drain; the
                // reducer treats it as an in-progress session.
                persisted = .running
            case .complete, .completeFull, .completeFeatureOnly, .completeTranscriptPartial:
                // playhead-gtt9.8: the three richer terminals + the
                // legacy `.complete` all map to `.done`. Commit 6 will
                // split degraded-ready (feature-only / transcript-
                // partial) from full-ready once the downstream reducer
                // has a degraded bucket.
                persisted = .done
            case .failed, .failedTranscript, .failedFeature, .cancelledBudget:
                // playhead-gtt9.8: the richer failure terminals all map
                // to the reducer's `.failed` today. Distinct reason
                // strings flow through `analysis_assets.terminalReason`
                // (persisted independently) so the UI copy layer can
                // disambiguate.
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
    /// `canUseFoundationModels`). `hardwareSupported` defaults to `true`
    /// here. `regionSupported` consults a live
    /// `LocaleRegionSupportProvider` — playhead-kgn5 replaced the
    /// previous `regionSupported -> true` placeholder; the read is
    /// non-blocking (one `Locale.current` call against a `Set<String>`).
    ///
    /// playhead-4nt1: this static helper is **no longer called from the
    /// observer's runtime path** — the production observer consumes the
    /// live `AnalysisEligibilityEvaluator`'s verdict via
    /// `eligibilityProvider`, which surfaces real hardware/region
    /// values from the per-field providers (see
    /// `CapabilityBackedEligibilityProviders` and
    /// `LocaleRegionSupportProvider`). The helper is retained only for
    /// the two non-observer callers
    /// (`LiveActivitySnapshotProvider.loadInputs` and any test that
    /// imports the snapshot→eligibility approximation directly) that
    /// still want a one-shot mapping without spinning up the full
    /// evaluator. Migrating those callers to the evaluator is out of
    /// scope for 4nt1 — the observer was the load-bearing call site
    /// that the bead targeted.
    ///
    /// `nil` snapshot (capability service not yet primed) maps to a
    /// fully-eligible snapshot for the same reason — apart from
    /// `regionSupported`, which still consults the live provider since
    /// the device's region is observable independent of the capability
    /// service warmup.
    static func eligibility(
        from snapshot: CapabilitySnapshot?,
        regionProvider: RegionSupportProviding = LocaleRegionSupportProvider()
    ) -> AnalysisEligibility {
        let regionSupported = regionProvider.isRegionSupported()
        guard let snapshot else {
            return AnalysisEligibility(
                hardwareSupported: true,
                appleIntelligenceEnabled: true,
                regionSupported: regionSupported,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: Date()
            )
        }
        return AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: snapshot.appleIntelligenceEnabled,
            regionSupported: regionSupported,
            languageSupported: snapshot.foundationModelsLocaleSupported,
            modelAvailableNow: snapshot.canUseFoundationModels,
            capturedAt: snapshot.capturedAt
        )
    }
}
