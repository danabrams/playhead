// Q45fReplayGate.swift
// playhead-q45f.2: Replay-side counterfactual gate for the q45f
// listen-rewind state machine. Mirrors `TrustScoringService.recordWeakFalseSkipSignal`
// without storage so the NARL eval harness can answer counterfactuals like
// "given these listenRewindEvents, when would trust have flipped this show
// out of auto?". Calls into `TrustScoringService.evaluateDemotion` directly
// so production retunes propagate here automatically — the contract pins
// are in `Q45fReplayGateTests` (the parity test there ties this gate to
// production's `recordWeakFalseSkipSignal` so any drift surfaces on CI,
// not as silent eval drift).

import Foundation
@testable import Playhead

enum Q45fReplayGate {

    struct State: Equatable {
        var trustScore: Double
        var recentFalseSkipSignals: Int
        var mode: SkipMode
    }

    struct DemotionEvent: Equatable {
        let time: Double
        let from: SkipMode
        let to: SkipMode
        let trustAfter: Double
        let falseSignalsAfter: Int
    }

    struct ReplayResult: Equatable {
        let initialState: State
        let finalState: State
        let demotions: [DemotionEvent]
    }

    /// Replay listen-rewind events through the production demotion math.
    ///
    /// Preconditions:
    /// - `events` must be in chronological order (i.e. sorted by source
    ///   `createdAt`). `FrozenListenRewindEvent.time` is episode-relative
    ///   (anchored to the source ad-window's `startTime`), so two rewinds
    ///   on the same window can share the same `time`; ordering must come
    ///   from the caller. `DemotionEvent.time` is stamped from the
    ///   threshold-crossing event's `time`, i.e. the window-startTime
    ///   anchor of the rewind that flipped the mode.
    /// - All events must share the same `podcastId`. Counterfactual replay
    ///   is a per-show question; mixed-podcast input is a caller bug.
    /// - `initialState.trustScore` must be finite. Non-finite or negative
    ///   values are clamped to `[0, ∞)` on entry to keep `max(0, …)`
    ///   semantics aligned with production (`max` is unordered on NaN).
    ///   The clamped value is reflected in BOTH `result.initialState` and
    ///   `result.finalState` so consumers can safely compute deltas like
    ///   `finalState.trustScore - initialState.trustScore`.
    ///
    /// Caller responsibility (NOT enforced here):
    /// - The gate has no concept of "profile missing". Production's
    ///   `recordWeakFalseSkipSignal` is a no-op when the podcast has no
    ///   `PodcastProfile` row; calling the gate for such a podcast will
    ///   apply demotion math and silently diverge from production. The
    ///   harness must filter out no-profile podcasts before calling.
    static func replay(
        initialState: State,
        events: [FrozenTrace.FrozenListenRewindEvent],
        config: TrustScoringConfig = .default
    ) -> ReplayResult {
        if let firstId = events.first?.podcastId {
            precondition(
                events.allSatisfy { $0.podcastId == firstId },
                "Q45fReplayGate.replay requires all events to share the same podcastId; counterfactual replay is per-show."
            )
        }

        var sanitizedInitial = initialState
        if !sanitizedInitial.trustScore.isFinite || sanitizedInitial.trustScore < 0 {
            sanitizedInitial.trustScore = 0
        }
        var state = sanitizedInitial
        var demotions: [DemotionEvent] = []

        for event in events {
            state.trustScore = max(0.0, state.trustScore - config.weakFalseSignalPenalty)
            state.recentFalseSkipSignals += 1

            let nextMode = TrustScoringService.evaluateDemotion(
                config: config,
                currentMode: state.mode,
                trustScore: state.trustScore,
                recentFalseSignals: state.recentFalseSkipSignals
            )

            if nextMode != state.mode {
                demotions.append(DemotionEvent(
                    time: event.time,
                    from: state.mode,
                    to: nextMode,
                    trustAfter: state.trustScore,
                    falseSignalsAfter: state.recentFalseSkipSignals
                ))
                state.mode = nextMode
            }
        }

        return ReplayResult(
            initialState: sanitizedInitial,
            finalState: state,
            demotions: demotions
        )
    }
}
