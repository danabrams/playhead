// NarlQ45fCounterfactual.swift
// playhead-q45f.3: NARL eval harness wiring of `Q45fReplayGate`. Two value
// types attached to the report:
//
//   - `NarlQ45fCounterfactual` — per-episode answer to "would auto-mode have
//     flipped this episode out of auto, given a fresh trust state of
//     (auto, 0.90, 0)?". The initial state is synthesized rather than read
//     from a profile snapshot because `FrozenTrace` does not carry the
//     show's `PodcastProfile` row at capture time. Useful for spotting
//     individual episodes that would unilaterally trigger demotion.
//
//   - `NarlQ45fCarryforwardRollup` — per-show answer to "given the show's
//     full rewind history in chronological order, where would auto-mode
//     have flipped?". Threads `Q45fReplayGate.State` across episodes
//     sorted by `trace.capturedAt` so the first demotion within the show's
//     history is identifiable.
//
// Both compute methods filter out cross-podcast rewind events as a defensive
// check: `Q45fReplayGate.replay` precondition-traps on mixed-podcast input,
// and a corrupt or cross-show fixture should not crash the harness.

import Foundation
@testable import Playhead

/// Per-episode counterfactual: replay this trace's listen-rewind events
/// against a synthesized fresh trust state (`auto`, 0.90, 0 false signals)
/// and surface whether the episode would have flipped out of auto on its
/// own. Carries the first demotion's time so the report can answer "where
/// in the episode did the flip happen?".
struct NarlQ45fCounterfactual: Sendable, Codable, Equatable {
    /// True when the gate produced at least one demotion transition.
    let wouldDemote: Bool
    /// Episode-relative time (in seconds, from `FrozenListenRewindEvent.time`)
    /// of the first demotion transition. Nil when no demotion occurred.
    let demotionTime: Double?
    /// SkipMode raw value at the end of the replay. `"auto"` when no
    /// demotion fired; otherwise the lowest mode reached in this episode.
    let finalMode: String
    /// Number of mode transitions during the replay (auto→manual,
    /// manual→shadow). Capped implicitly at 2 by the production state
    /// machine but not asserted here.
    let demotionsCount: Int
    /// Number of rewind events actually replayed (post-cross-podcast filter).
    let rewindEventCount: Int

    /// Sentinel for "no rewinds in this trace" or pre-q45f.3 fixtures
    /// where the field is absent. Equality with `.empty` is the canonical
    /// "no signal" check.
    static let empty = NarlQ45fCounterfactual(
        wouldDemote: false,
        demotionTime: nil,
        finalMode: SkipMode.auto.rawValue,
        demotionsCount: 0,
        rewindEventCount: 0
    )

    /// Replay `trace.listenRewindEvents` (filtered to the trace's own
    /// `podcastId`) through `Q45fReplayGate` starting from a fresh
    /// `(auto, 0.90, 0)` state. Returns `.empty` when the trace has no
    /// applicable events.
    static func compute(trace: FrozenTrace) -> NarlQ45fCounterfactual {
        let events = trace.listenRewindEvents.filter { $0.podcastId == trace.podcastId }
        guard !events.isEmpty else { return .empty }
        let initialState = Q45fReplayGate.State(
            trustScore: 0.90,
            recentFalseSkipSignals: 0,
            mode: .auto
        )
        let result = Q45fReplayGate.replay(
            initialState: initialState,
            events: events
        )
        return NarlQ45fCounterfactual(
            wouldDemote: !result.demotions.isEmpty,
            demotionTime: result.demotions.first?.time,
            finalMode: result.finalState.mode.rawValue,
            demotionsCount: result.demotions.count,
            rewindEventCount: events.count
        )
    }
}

/// Per-show carryforward: replay all of a show's episodes' rewind events
/// in chronological order, threading `Q45fReplayGate.State` across episode
/// boundaries. Answers the realistic question "across this show's history,
/// would the user have ended up in manual or shadow mode?".
///
/// Distinct from `NarlQ45fCounterfactual` (per-episode w/ fresh state)
/// because the per-show answer is what production actually does — false
/// signals accumulate across episodes within a single `PodcastProfile`.
struct NarlQ45fCarryforwardRollup: Sendable, Codable, Equatable {
    /// SkipMode raw value at the end of the chronological replay.
    let finalMode: String
    /// Total mode transitions across all episodes in the show.
    let totalDemotionsCount: Int
    /// Total rewind events actually replayed (post-cross-podcast filter,
    /// summed across episodes).
    let totalRewindEventCount: Int
    /// `episodeId` of the first episode whose replay produced a demotion.
    /// Nil when no demotion fired. The "first" is in chronological order
    /// (by `trace.capturedAt`), not input order.
    let firstDemotionEpisodeId: String?
    /// Number of input episodes (whether they had rewinds or not). Useful
    /// for distinguishing "5 episodes, no rewinds" from "0 episodes".
    let episodeCount: Int

    static let empty = NarlQ45fCarryforwardRollup(
        finalMode: SkipMode.auto.rawValue,
        totalDemotionsCount: 0,
        totalRewindEventCount: 0,
        firstDemotionEpisodeId: nil,
        episodeCount: 0
    )

    /// Replay every episode's rewinds in chronological order (by
    /// `capturedAt`), threading state across boundaries. Episodes with no
    /// applicable rewinds are skipped (no replay call) but still counted
    /// in `episodeCount`.
    static func compute(showEpisodes: [FrozenTrace]) -> NarlQ45fCarryforwardRollup {
        let sorted = showEpisodes.sorted { $0.capturedAt < $1.capturedAt }
        var state = Q45fReplayGate.State(
            trustScore: 0.90,
            recentFalseSkipSignals: 0,
            mode: .auto
        )
        var totalDemotions = 0
        var totalEvents = 0
        var firstDemotionEpisodeId: String?
        for trace in sorted {
            let events = trace.listenRewindEvents.filter { $0.podcastId == trace.podcastId }
            guard !events.isEmpty else { continue }
            let result = Q45fReplayGate.replay(initialState: state, events: events)
            state = result.finalState
            totalEvents += events.count
            totalDemotions += result.demotions.count
            if firstDemotionEpisodeId == nil && !result.demotions.isEmpty {
                firstDemotionEpisodeId = trace.episodeId
            }
        }
        return NarlQ45fCarryforwardRollup(
            finalMode: state.mode.rawValue,
            totalDemotionsCount: totalDemotions,
            totalRewindEventCount: totalEvents,
            firstDemotionEpisodeId: firstDemotionEpisodeId,
            episodeCount: sorted.count
        )
    }
}
