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
//   - `NarlQ45fCarryforwardRollup` — per-PODCAST chronological replay of
//     every episode of a single podcast, threading `Q45fReplayGate.State`
//     across episode boundaries. Production keys trust state on
//     `podcastId` (see `TrustScoringService.recordWeakFalseSkipSignal`),
//     so the carryforward must follow the same key — multiple
//     `podcastId`s that share a heuristic show label (e.g. "DoaC" maps
//     to both `flightcast:01KM…` and `https://rss2.flightcast.com/…`)
//     are NOT a single production trust profile and must not be
//     collapsed. The `NarlReportRollup` carries one rollup per
//     podcastId-in-show as `[NarlQ45fCarryforwardRollup]`.
//
// Both `compute` methods sort their input events by `time` (the source
// ad-window's `startTime`, episode-relative) to satisfy
// `Q45fReplayGate.replay`'s "events must be in chronological order"
// precondition (the corpus parser does not guarantee this — see
// `NarlEvalCorpusBuilderTests.swift` listen-rewind ingest).
//
// **Sort-key divergence from production.** Production records signals
// in tap-order (wall-clock `createdAt` of the rewind tap).
// `FrozenListenRewindEvent` only carries `time` because the corpus
// builder strips the source `createdAt` at export. For linear listening
// these orders are identical; for a user who rewinds *backwards* (taps
// Listen on an earlier window after a later one) they disagree, and
// the replay's demotion timeline may diverge from what production
// recorded. Treating `time` as the chronological key is a documented
// approximation; the alternative is re-baking fixtures with `createdAt`
// preserved.
//
// Both also filter cross-podcast events as defense-in-depth and log a
// warning to `narl.q45f:` when the filter drops anything; the gate's
// precondition would precondition-trap on mixed input but a corrupt
// fixture should produce a visible warning, not a silent miss.

import Foundation
@testable import Playhead

/// Shared default fresh trust state for q45f counterfactual replays.
/// Mirrors what production assumes for a podcast with no prior
/// `PodcastProfile` row: auto mode, fully-trusted (0.90), zero recent
/// false signals. Hoisted onto the gate's `State` so both wrapper types
/// (per-episode and per-podcast carryforward) share one source of truth —
/// changing the fresh-state convention happens in one place.
extension Q45fReplayGate.State {
    static let freshDefault = Q45fReplayGate.State(
        trustScore: 0.90,
        recentFalseSkipSignals: 0,
        mode: .auto
    )
}

/// Internal warning sink. Plain stdout `print` so the message is visible
/// in xcodebuild test logs and so harness operators / test scrapes that
/// grep on the `narl.q45f:` channel prefix work the same way as the
/// existing `narl.normalizer:` channel (see `NarlEvalHarnessTests`,
/// which uses `print("narl.normalizer: ...")` for the same purpose).
/// `os.Logger` was tried briefly but it routes to OSLog, which doesn't
/// surface in `xcodebuild test` stdout — defeating the point.
private func logQ45fWarning(_ message: String) {
    print("narl.q45f: WARN \(message)")
}

/// Per-episode counterfactual: replay this trace's listen-rewind events
/// against a synthesized fresh trust state (`auto`, 0.90, 0 false signals)
/// and surface whether the episode would have flipped out of auto on its
/// own. Carries the first demotion's time so the report can answer "where
/// in the episode did the flip happen?".
///
/// **Caveat — divergence from production:** `compute(trace:)` always uses
/// the fresh `(auto, 0.90, 0)` initial state because `FrozenTrace` does
/// not carry a `PodcastProfile` snapshot at capture time. In production a
/// trace can be played from a podcast whose profile already has elevated
/// `recentFalseSkipSignals` or a depressed `trustScore`; the counterfactual
/// here ignores that history. This is "would auto-mode have flipped on
/// this episode in isolation?" — not "what would production have done?".
/// The per-podcast carryforward (`NarlQ45fCarryforwardRollup`) closes
/// some of that gap by threading state across the podcast's episodes,
/// but only for episodes captured in the corpus.
struct NarlQ45fCounterfactual: Sendable, Codable, Equatable {
    /// True when the gate produced at least one demotion transition.
    let wouldDemote: Bool
    /// Episode-relative time (in seconds, from `FrozenListenRewindEvent.time`)
    /// of the first demotion transition. Nil when no demotion occurred.
    /// Note: ties are possible — two rewinds against the same source
    /// ad-window share the same `time`, so a same-window double-tap that
    /// crosses the auto→manual threshold reports the shared anchor time.
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
    /// `podcastId`, sorted by `time`) through `Q45fReplayGate` starting
    /// from `Q45fReplayGate.State.freshDefault`. Returns `.empty` when the
    /// trace has no applicable events. Pinned to `TrustScoringConfig.default`
    /// — sweep/what-if eval would need a config-parameterized variant.
    static func compute(trace: FrozenTrace) -> NarlQ45fCounterfactual {
        let raw = trace.listenRewindEvents
        let filtered = raw.filter { $0.podcastId == trace.podcastId }
        if filtered.count != raw.count {
            logQ45fWarning(
                "compute(trace: \(trace.episodeId)) dropped \(raw.count - filtered.count) "
                + "cross-podcast listenRewindEvents (trace.podcastId=\(trace.podcastId)). "
                + "This is a corpus bug — investigate the capture."
            )
        }
        guard !filtered.isEmpty else { return .empty }
        // H2: Q45fReplayGate.replay requires chronological order. The
        // corpus parser does not sort. Sort by event.time here. Ties on
        // `time` (multiple rewinds against the same window) are
        // order-independent under the gate's math: each contributes the
        // same penalty + signal increment, so the demotion time stamped
        // on a tie group is identical regardless of within-tie order.
        let events = filtered.sorted { $0.time < $1.time }
        let result = Q45fReplayGate.replay(
            initialState: .freshDefault,
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

/// Per-PODCAST carryforward: replay every episode of one podcast in
/// chronological order, threading `Q45fReplayGate.State` across episode
/// boundaries. Diagnostic-grade approximation of "across this podcast's
/// captured history, would the user have ended up in manual or shadow
/// mode?".
///
/// Production keys trust state on `podcastId` — false signals from one
/// podcast never affect another. A "show" in the harness is a heuristic
/// label that can collapse multiple `podcastId`s (e.g. legacy + URL forms
/// of the same feed); the harness emits one rollup per **podcastId** in a
/// show, never collapsing across podcastIds. The "ALL" pseudo-show emits
/// no carryforward rollups at all because cross-podcast trust state has
/// no production analogue.
///
/// **Caveats — divergence from production.** This is NOT a faithful
/// counterfactual of what production would have done; it is a replay of
/// the corpus's captured rewind events through the gate's math. Callers
/// drawing conclusions from this field should account for:
///
///   1. **Initial state.** Always synthesized as
///      `(auto, 0.90, 0)`. Production starts from the persisted
///      `PodcastProfile` row, which `FrozenTrace` does not snapshot.
///   2. **No-profile no-op.** Production's
///      `recordWeakFalseSkipSignal` early-returns on the first false
///      signal for a brand-new podcast (no row yet → no state to
///      mutate). The replay here counts that first signal toward the
///      demotion threshold.
///   3. **Event ordering.** Sorted by `event.time` (episode-relative
///      timeline position of the source ad-window). `FrozenTrace` does
///      not preserve the source `createdAt` (wall-clock at user tap),
///      so a user who rewinds backwards in the asset (taps Listen on
///      an earlier window after a later one) replays in the wrong
///      order vs. production. For linear listening these match.
///   4. **Config pinned.** `TrustScoringConfig.default` only —
///      sweep/what-if eval would need a config-parameterized variant.
///
/// The first two caveats can flip the demotion answer for any podcast
/// whose rewinds straddle the no-profile boundary. The third only
/// matters under non-linear listening. Treat the field as a diagnostic
/// signal, not a production verdict.
struct NarlQ45fCarryforwardRollup: Sendable, Codable, Equatable {
    /// The podcastId this carryforward belongs to. Required because a show
    /// can carry multiple podcastIds.
    let podcastId: String
    /// SkipMode raw value at the end of the chronological replay.
    let finalMode: String
    /// Total mode transitions across all episodes in the podcast.
    let totalDemotionsCount: Int
    /// Total rewind events actually replayed (post-cross-podcast filter
    /// + post-time-sort, summed across episodes).
    let totalRewindEventCount: Int
    /// `episodeId` of the first episode whose replay produced a demotion.
    /// Nil when no demotion fired. The "first" is in chronological order
    /// (by `trace.capturedAt`), not input order.
    let firstDemotionEpisodeId: String?
    /// Number of input traces (whether they had rewinds or not). NOT the
    /// same as `NarlReportRollup.episodeCount`, which counts only non-
    /// excluded entries, nor `excludedEpisodeCount`, which counts only
    /// vetoed/coverage-limited entries. This is "every trace fed to the
    /// carryforward, regardless of harness exclusion status" — a
    /// vetoed episode's user-rewinds still imply trust state in
    /// production, so the replay includes them. Operators comparing
    /// `traceCount` to `episodeCount + excludedEpisodeCount` should
    /// expect equality only under the per-(show, config) intersection
    /// of the same podcastId, not in general.
    let traceCount: Int

    /// Empty sentinel for an unspecified-podcast slot. Used as a Codable
    /// fallback for pre-q45f.3 report artifacts that don't carry the
    /// field at all (the field decodes to `[]`, not `[.empty]`).
    ///
    /// **Caution:** equality-with-`.empty` is NOT a reliable "no signal"
    /// check on real harness output. A fixture with a genuinely empty
    /// `podcastId` field (a corrupt or pre-frozen-trace-v3 capture)
    /// produces a real rollup whose `podcastId == ""` — indistinguishable
    /// from this sentinel by equality. Prefer `traceCount == 0` as the
    /// "synthesized empty" check instead.
    static let empty = NarlQ45fCarryforwardRollup(
        podcastId: "",
        finalMode: SkipMode.auto.rawValue,
        totalDemotionsCount: 0,
        totalRewindEventCount: 0,
        firstDemotionEpisodeId: nil,
        traceCount: 0
    )

    /// Compute one carryforward for a single podcast's traces. All input
    /// traces must share the same `podcastId` (precondition); cross-
    /// podcast input is a programming error at the call site.
    static func compute(podcastEpisodes: [FrozenTrace]) -> NarlQ45fCarryforwardRollup {
        guard let firstId = podcastEpisodes.first?.podcastId else {
            return .empty
        }
        precondition(
            podcastEpisodes.allSatisfy { $0.podcastId == firstId },
            "NarlQ45fCarryforwardRollup.compute(podcastEpisodes:) requires every trace to share the same podcastId; got mixed input."
        )
        let sorted = podcastEpisodes.sorted { $0.capturedAt < $1.capturedAt }
        var state = Q45fReplayGate.State.freshDefault
        var totalDemotions = 0
        var totalEvents = 0
        var firstDemotionEpisodeId: String?
        for trace in sorted {
            let raw = trace.listenRewindEvents
            let filtered = raw.filter { $0.podcastId == trace.podcastId }
            if filtered.count != raw.count {
                logQ45fWarning(
                    "carryforward(\(firstId), trace=\(trace.episodeId)) dropped "
                    + "\(raw.count - filtered.count) cross-podcast events. "
                    + "This is a corpus bug — investigate the capture."
                )
            }
            guard !filtered.isEmpty else { continue }
            // H2: gate precondition requires chronological order; sort
            // by event.time. Ties are order-independent (see compute(trace:)).
            let events = filtered.sorted { $0.time < $1.time }
            let result = Q45fReplayGate.replay(initialState: state, events: events)
            state = result.finalState
            totalEvents += events.count
            totalDemotions += result.demotions.count
            if firstDemotionEpisodeId == nil && !result.demotions.isEmpty {
                firstDemotionEpisodeId = trace.episodeId
            }
        }
        return NarlQ45fCarryforwardRollup(
            podcastId: firstId,
            finalMode: state.mode.rawValue,
            totalDemotionsCount: totalDemotions,
            totalRewindEventCount: totalEvents,
            firstDemotionEpisodeId: firstDemotionEpisodeId,
            traceCount: sorted.count
        )
    }

    /// Convenience: bucket a show's traces by `podcastId` and produce one
    /// carryforward per podcastId. Returned rollups are sorted by
    /// `podcastId` for stable rendering.
    static func computePerPodcast(showEpisodes: [FrozenTrace]) -> [NarlQ45fCarryforwardRollup] {
        Dictionary(grouping: showEpisodes, by: \.podcastId)
            .map { compute(podcastEpisodes: $0.value) }
            .sorted { $0.podcastId < $1.podcastId }
    }
}
