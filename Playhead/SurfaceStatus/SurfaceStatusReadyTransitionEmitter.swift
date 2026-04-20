// SurfaceStatusReadyTransitionEmitter.swift
// Canonical emission helper for the `ready_entered` event. Wraps the
// pure `episodeSurfaceStatus(...)` reducer in a per-episode memory of
// the last observed disposition so it can fire the `readyEntered` log
// event ONLY on actual transitions into a ready-for-playback state.
//
// Scope: playhead-o45p — Wave 4 dogfood pass criterion 3 instrumentation.
//
// The reducer itself is stateless (a pure function over snapshots). But
// the bead's contract requires the event to fire when the episode
// "transitions INTO" a ready disposition — which requires remembering
// the prior state. This helper is the narrow stateful shim that owns
// that memory. Production consumers of `episodeSurfaceStatus(...)`
// should route through this helper so every ready-transition is
// observed exactly once per transition (not once per reduction).
//
// The helper is not an actor; callers are expected to drive it on
// whatever serialization context they already own (e.g. an actor that
// already owns the surface-status cache). The internal dictionary is
// therefore NOT thread-safe on its own. Callers that need thread-safety
// must wrap the helper.

import Foundation

/// Tracks per-episode disposition history so `ready_entered` events fire
/// exactly once on each transition INTO a ready-for-playback disposition
/// (queued + no blocking cause). Pure stateful machinery — does NOT own
/// the logger writes it delegates to.
///
/// Marked `@unchecked Sendable` so it can be handed to an owning actor
/// (e.g. `EpisodeSurfaceStatusObserver`) at init time. The internal
/// `lastReadyByEpisode` dictionary is NOT thread-safe on its own — per
/// the class contract, the owning actor must serialize all calls to
/// `reduceAndEmit(...)`. This conformance is a compile-time assertion
/// that single-actor ownership is the only supported usage pattern.
final class SurfaceStatusReadyTransitionEmitter: @unchecked Sendable {

    /// The reducer this emitter wraps. Pluggable so tests can inject a
    /// deterministic override, and so the production wiring can swap in
    /// a batched / coalesced variant later.
    ///
    /// The typealias is 5 arguments — the optional `invariantLogger`
    /// parameter on `episodeSurfaceStatus(...)` is bound at composition
    /// time (see `EpisodeSurfaceStatusObserver.init`) rather than passed
    /// on every call, so the emitter itself does not need to know about
    /// the logger.
    typealias Reducer = @Sendable (
        _ state: AnalysisState,
        _ cause: InternalMissCause?,
        _ eligibility: AnalysisEligibility,
        _ coverage: CoverageSummary?,
        _ readinessAnchor: TimeInterval?
    ) -> EpisodeSurfaceStatus

    /// The logger sink. Pluggable for tests — the composition root
    /// (`PlayheadRuntime` → `EpisodeSurfaceStatusObserver`) passes a
    /// closure bound to the injected `SurfaceStatusInvariantLogger`
    /// instance.
    typealias LoggerSink = (
        _ episodeIdHash: String?,
        _ trigger: SurfaceStateTransitionEntryTrigger?
    ) -> Void

    private let reducer: Reducer
    private let loggerSink: LoggerSink

    /// Last observed `isReady` state per episode (keyed by the
    /// episodeIdHash the caller supplies). `true` means the episode's
    /// last reduction produced a ready-for-playback disposition. Missing
    /// keys mean the emitter has never seen the episode — the first
    /// ready reduction is classified as `coldStart`.
    private var lastReadyByEpisode: [String: Bool] = [:]

    /// Default reducer that forwards to `episodeSurfaceStatus(...)`
    /// without the optional invariant logger. Callers that want the
    /// reducer's impossible-state paths to log to a specific logger
    /// instance should construct a bound reducer closure themselves
    /// (see `EpisodeSurfaceStatusObserver`).
    static let defaultReducer: Reducer = { state, cause, eligibility, coverage, anchor in
        episodeSurfaceStatus(
            state: state,
            cause: cause,
            eligibility: eligibility,
            coverage: coverage,
            readinessAnchor: anchor
        )
    }

    init(
        reducer: @escaping Reducer = SurfaceStatusReadyTransitionEmitter.defaultReducer,
        loggerSink: @escaping LoggerSink
    ) {
        self.reducer = reducer
        self.loggerSink = loggerSink
    }

    /// Reduce the supplied inputs AND emit a `ready_entered` event iff
    /// this reduction is a transition INTO ready. Returns the reduced
    /// status for the caller to consume as usual.
    ///
    /// `episodeIdHash` is used both to index the per-episode memory and
    /// as the log entry payload. Callers that cannot supply one (e.g.
    /// very-early-boot reductions before the hasher is primed) pass
    /// `nil` — such reductions cannot participate in the false_ready
    /// metric but the helper still returns the reduced status.
    ///
    /// `trigger` lets the caller classify the transition explicitly.
    /// When `nil`, the helper infers:
    ///   * `.coldStart` when the episode has never been seen before;
    ///   * `.unblocked` otherwise (prior state was non-ready).
    @discardableResult
    func reduceAndEmit(
        episodeIdHash: String?,
        state: AnalysisState,
        cause: InternalMissCause?,
        eligibility: AnalysisEligibility,
        coverage: CoverageSummary?,
        readinessAnchor: TimeInterval?,
        trigger: SurfaceStateTransitionEntryTrigger? = nil
    ) -> EpisodeSurfaceStatus {
        let status = reducer(state, cause, eligibility, coverage, readinessAnchor)
        let isReadyNow = Self.isReady(status: status, cause: cause)

        guard let episodeIdHash else {
            // Nothing to index on — emit only if the caller explicitly
            // supplied a non-nil trigger AND the reduction is ready.
            if isReadyNow, let trigger {
                loggerSink(nil, trigger)
            }
            return status
        }

        let hasSeenBefore = (lastReadyByEpisode[episodeIdHash] != nil)
        let wasReady = lastReadyByEpisode[episodeIdHash] ?? false
        lastReadyByEpisode[episodeIdHash] = isReadyNow

        if isReadyNow && !wasReady {
            // Transition INTO ready — emit exactly one event.
            let inferredTrigger: SurfaceStateTransitionEntryTrigger
            if let trigger {
                inferredTrigger = trigger
            } else if !hasSeenBefore {
                inferredTrigger = .coldStart
            } else {
                inferredTrigger = .unblocked
            }
            loggerSink(episodeIdHash, inferredTrigger)
        }
        return status
    }

    /// Test-only: clear the per-episode memory. Use between tests that
    /// share the same emitter instance.
    func resetForTesting() {
        lastReadyByEpisode.removeAll()
    }

    // MARK: - Internal helpers

    /// Classify a reduced status + cause as ready-for-playback.
    ///
    /// Ready = disposition is `.queued` AND no blocking cause. Rule 5 of
    /// the reducer's precedence ladder. Exposing this as a static helper
    /// means the false_ready_rate aggregation script (scripts/
    /// false_ready_rate.*) and this emitter agree on the definition.
    static func isReady(status: EpisodeSurfaceStatus, cause: InternalMissCause?) -> Bool {
        return status.disposition == .queued && cause == nil
    }
}
