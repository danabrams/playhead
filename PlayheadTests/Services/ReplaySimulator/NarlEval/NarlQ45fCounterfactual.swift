// NarlQ45fCounterfactual.swift
// playhead-q45f.3: NARL eval harness wiring of `Q45fReplayGate`. Two value
// types attached to the report:
//
//   - `NarlQ45fCounterfactual` — per-episode answer to "would auto-mode have
//     flipped this episode out of auto, given a best-case fresh trust state
//     of (auto, 0.90, 0)?". The initial state is synthesized rather than
//     read from a profile snapshot because `FrozenTrace` does not carry
//     the show's `PodcastProfile` row at capture time. Useful for spotting
//     individual episodes whose rewinds would unilaterally trigger
//     demotion *if* the user had reached fully-trusted auto mode first.
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
// Both `compute` methods sort their input events by `(time, windowId)` to
// satisfy `Q45fReplayGate.replay`'s "events must be in chronological
// order" precondition (the corpus parser does not guarantee this — see
// `NarlEvalCorpusBuilderTests.swift` listen-rewind ingest). The
// secondary `windowId` key stabilizes ties so two rewinds with the same
// `time` always replay in the same order across runs/toolchains.
// Choice of secondary key: `FrozenListenRewindEvent` carries three
// fields — `time`, `windowId`, `podcastId`. `time` is the primary key.
// `podcastId` cannot be the secondary key because the cross-podcast
// filter pre-narrows events to a single `podcastId`, so within the
// sorted slice it is constant and provides no ordering signal.
// `windowId` is the only remaining field and carries enough entropy
// (one rewind tap per ad-window per user) to break ties deterministically.
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
// Both also filter cross-podcast events as defense-in-depth and surface
// the dropped count both as a `crossPodcastDroppedCount` field on the
// report (so a corrupt-fixture episode is distinguishable from a
// genuinely empty one without scanning stdout) and as a `narl.q45f:`
// warning row to the test log (mirroring the `narl.normalizer:`
// channel). The gate's precondition would precondition-trap on mixed
// input, but a corrupt fixture should produce a visible warning + a
// queryable telemetry field, not a silent miss.

import Foundation
@testable import Playhead

/// Shared default fresh trust state for q45f counterfactual replays.
///
/// **Best-case auto-mode steady state — counterfactual seed, NOT a
/// "fresh production state".** Production never starts a `PodcastProfile`
/// in `(auto, 0.90, 0)`:
///
///   - For a no-row podcast, `recordWeakFalseSkipSignal` early-returns
///     (no row → no state to mutate). The "first signal" is a no-op,
///     not a penalty toward the auto→manual threshold.
///   - When `recordSuccessfulObservation` first creates a row, it
///     initializes mode to `.shadow` (trust 0.2) or `.manual` (trust
///     `shadowToManualTrustScore + 0.1`, ≈ 0.5) — never `.auto`.
///   - Reaching `(auto, 0.90)` requires sustained `correctObservationBonus`
///     promotions over multiple episodes.
///
/// The seed therefore answers a counterfactual: "*if* this podcast had
/// already earned full-trust auto mode, would these rewinds have
/// demoted it?". It is NOT "what production would have done starting
/// from the user's actual state" — that question requires snapshotting
/// each trace's `PodcastProfile` row, which the corpus does not capture.
///
/// Hoisted onto the gate's `State` so both wrapper types (per-episode
/// and per-podcast carryforward) share one source of truth — changing
/// the counterfactual seed convention happens in one place. Lives in
/// this file (rather than next to `Q45fReplayGate.State`) because the
/// "best-case auto" semantic is a NARL-eval concept; the gate itself
/// is policy-free and accepts any caller-provided state.
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
/// surface in `xcodebuild test` stdout — defeating the point. Format
/// matches `narl.normalizer:` (channel prefix + free-form message; no
/// severity token like "WARN" in between, since the channel itself is
/// warning-only).
private func logQ45fWarning(_ message: String) {
    print("narl.q45f: \(message)")
}

/// Strip cross-podcast events from a trace's listen-rewind set and
/// surface the drop both as a returned count (for telemetry on the
/// report value) and a stdout warning. Defense-in-depth: a corpus
/// built correctly never contains foreign events for a given trace,
/// but a bug in the capture path would otherwise be a silent miss.
private func filterCrossPodcastEvents(
    _ events: [FrozenTrace.FrozenListenRewindEvent],
    expectedPodcastId: String,
    contextLabel: @autoclosure () -> String
) -> (kept: [FrozenTrace.FrozenListenRewindEvent], droppedCount: Int) {
    let kept = events.filter { $0.podcastId == expectedPodcastId }
    let droppedCount = events.count - kept.count
    if droppedCount > 0 {
        logQ45fWarning(
            "\(contextLabel()) dropped \(droppedCount) cross-podcast "
            + "listenRewindEvents (expected podcastId=\(expectedPodcastId)). "
            + "This is a corpus bug — investigate the capture."
        )
    }
    return (kept, droppedCount)
}

/// Per-episode counterfactual: replay this trace's listen-rewind events
/// against a synthesized best-case `(auto, 0.90, 0)` initial trust state
/// and surface whether the episode would have flipped out of auto on its
/// own. Carries the first demotion's time so the report can answer "where
/// in the episode did the flip happen?".
///
/// **Caveat — divergence from production:** `compute(trace:)` always uses
/// `Q45fReplayGate.State.freshDefault` because `FrozenTrace` does not
/// carry a `PodcastProfile` snapshot at capture time. In production a
/// trace can be played from a podcast whose profile already has elevated
/// `recentFalseSkipSignals` or a depressed `trustScore`; the counterfactual
/// here ignores that history. This is "would best-case auto-mode have
/// flipped on this episode in isolation?" — not "what would production
/// have done?". The per-podcast carryforward
/// (`NarlQ45fCarryforwardRollup`) closes some of that gap by threading
/// state across the podcast's episodes, but only for episodes captured
/// in the corpus, and only starting from the same best-case seed.
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
    /// Number of rewind events dropped by the cross-podcast filter
    /// (defense-in-depth). On a correctly-built corpus this is always 0;
    /// a non-zero value flags a capture bug. Surfacing this on the report
    /// — rather than only as a `narl.q45f:` stdout warning — lets a
    /// downstream `jq` reader distinguish "episode had no rewinds" from
    /// "episode's rewinds were all foreign and silently dropped". Both
    /// produce `wouldDemote == false`, but the second is a corpus alarm.
    /// Defaults to 0 for pre-q45f.3 fixtures (Codable back-compat).
    let crossPodcastDroppedCount: Int

    init(
        wouldDemote: Bool,
        demotionTime: Double?,
        finalMode: String,
        demotionsCount: Int,
        rewindEventCount: Int,
        crossPodcastDroppedCount: Int = 0
    ) {
        self.wouldDemote = wouldDemote
        self.demotionTime = demotionTime
        self.finalMode = finalMode
        self.demotionsCount = demotionsCount
        self.rewindEventCount = rewindEventCount
        self.crossPodcastDroppedCount = crossPodcastDroppedCount
    }

    /// Sentinel for "no rewinds in this trace" or pre-q45f.3 fixtures
    /// where the field is absent.
    ///
    /// **Returned by `compute(trace:)` only when there is genuinely
    /// nothing to surface.** Cycle-4 M-2 disambiguated this: the path
    /// where every listen-rewind event was for a foreign `podcastId`
    /// (corpus bug) deliberately does NOT collapse to `.empty` — it
    /// returns a non-`.empty` value with `crossPodcastDroppedCount > 0`
    /// and `rewindEventCount == 0` so the corpus alarm survives schema-
    /// level inspection (a `jq` reader can distinguish "no rewinds"
    /// from "rewinds were all foreign and got dropped"). Equality-with-
    /// `.empty` is therefore a reliable "no rewinds at all, foreign or
    /// otherwise" check on `compute(trace:)` output. Mirrors the same
    /// disambiguation on `NarlQ45fCarryforwardRollup.empty`.
    static let empty = NarlQ45fCounterfactual(
        wouldDemote: false,
        demotionTime: nil,
        finalMode: SkipMode.auto.rawValue,
        demotionsCount: 0,
        rewindEventCount: 0,
        crossPodcastDroppedCount: 0
    )

    /// Codable: `crossPodcastDroppedCount` was added in q45f.3 cycle 4;
    /// pre-cycle-4 fixtures decode to 0 via `decodeIfPresent`. Encode
    /// always emits the field. (Distinct from the broader pre-q45f.3
    /// back-compat handled at the parent type's decoder, which defaults
    /// the entire `q45fCounterfactual` field to `.empty` when absent.)
    enum CodingKeys: String, CodingKey {
        case wouldDemote, demotionTime, finalMode
        case demotionsCount, rewindEventCount
        case crossPodcastDroppedCount
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            wouldDemote: try c.decode(Bool.self, forKey: .wouldDemote),
            demotionTime: try c.decodeIfPresent(Double.self, forKey: .demotionTime),
            finalMode: try c.decode(String.self, forKey: .finalMode),
            demotionsCount: try c.decode(Int.self, forKey: .demotionsCount),
            rewindEventCount: try c.decode(Int.self, forKey: .rewindEventCount),
            crossPodcastDroppedCount: try c.decodeIfPresent(
                Int.self, forKey: .crossPodcastDroppedCount
            ) ?? 0
        )
    }

    /// Replay `trace.listenRewindEvents` (filtered to the trace's own
    /// `podcastId`, sorted by `(time, windowId)`) through `Q45fReplayGate`
    /// starting from `Q45fReplayGate.State.freshDefault`. Returns `.empty`
    /// when the trace has no applicable events (post-filter); the
    /// pre-filter dropped count is preserved on the return value's
    /// `crossPodcastDroppedCount`. Pinned to `TrustScoringConfig.default`
    /// — sweep/what-if eval would need a config-parameterized variant.
    static func compute(trace: FrozenTrace) -> NarlQ45fCounterfactual {
        let (filtered, droppedCount) = filterCrossPodcastEvents(
            trace.listenRewindEvents,
            expectedPodcastId: trace.podcastId,
            contextLabel: "compute(trace: \(trace.episodeId))"
        )
        guard !filtered.isEmpty else {
            // Even when there's nothing to replay, surface the dropped
            // count if any so the operator can distinguish "no rewinds"
            // from "all rewinds were foreign and got dropped".
            if droppedCount == 0 {
                return .empty
            }
            return NarlQ45fCounterfactual(
                wouldDemote: false,
                demotionTime: nil,
                finalMode: SkipMode.auto.rawValue,
                demotionsCount: 0,
                rewindEventCount: 0,
                crossPodcastDroppedCount: droppedCount
            )
        }
        // H2: Q45fReplayGate.replay requires chronological order. The
        // corpus parser does not sort. Sort by event.time here, with
        // `windowId` as a stable tiebreaker — Swift's sort is unstable,
        // so without a secondary key two events with the same `time`
        // could swap places between runs/toolchains, flipping
        // tie-bound fields like `demotionTime` for same-window double-
        // taps. Ties on `time` (multiple rewinds against the same
        // window) are order-independent under the gate's math: each
        // contributes the same penalty + signal increment, so the
        // demotion time stamped on a tie group is identical regardless
        // of within-tie order — but the explicit secondary key makes
        // that a property of the input ordering, not a happy accident.
        let events = filtered.sorted {
            ($0.time, $0.windowId) < ($1.time, $1.windowId)
        }
        let result = Q45fReplayGate.replay(
            initialState: .freshDefault,
            events: events
        )
        return NarlQ45fCounterfactual(
            wouldDemote: !result.demotions.isEmpty,
            demotionTime: result.demotions.first?.time,
            finalMode: result.finalState.mode.rawValue,
            demotionsCount: result.demotions.count,
            rewindEventCount: events.count,
            crossPodcastDroppedCount: droppedCount
        )
    }
}

/// Per-PODCAST carryforward: replay every episode of one podcast in
/// chronological order, threading `Q45fReplayGate.State` across episode
/// boundaries. Diagnostic-grade approximation of "across this podcast's
/// captured history, would best-case auto-mode have flipped to manual or
/// shadow?".
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
///      `(auto, 0.90, 0)` — the best-case auto-mode steady state.
///      Production starts from the persisted `PodcastProfile` row,
///      which `FrozenTrace` does not snapshot. Production never *creates*
///      a row in `.auto` (see `Q45fReplayGate.State.freshDefault`'s
///      docstring) — that mode is earned via repeated promotions.
///   2. **No-profile no-op.** Production's
///      `recordWeakFalseSkipSignal` early-returns on the first false
///      signal for a brand-new podcast (no row yet → no state to
///      mutate). The replay here counts that first signal toward the
///      demotion threshold.
///   3. **Event ordering.** Sorted by `(event.time, event.windowId)`
///      (episode-relative timeline position of the source ad-window,
///      with windowId as a stable tiebreaker). `FrozenTrace` does not
///      preserve the source `createdAt` (wall-clock at user tap), so
///      a user who rewinds backwards in the asset (taps Listen on an
///      earlier window after a later one) replays in the wrong order
///      vs. production. For linear listening these match.
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
    /// (by `(trace.capturedAt, trace.episodeId)`), not input order.
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
    /// Total cross-podcast events dropped across all traces of this
    /// podcast (sum of each trace's defense-in-depth filter result).
    /// 0 on a correctly-built corpus; a non-zero value flags a capture
    /// bug. Mirrors `NarlQ45fCounterfactual.crossPodcastDroppedCount`
    /// at the per-podcast aggregation level. Defaults to 0 for pre-q45f.3
    /// fixtures and pre-cycle-4 fixtures (Codable back-compat via
    /// `decodeIfPresent`).
    let totalCrossPodcastDroppedCount: Int

    init(
        podcastId: String,
        finalMode: String,
        totalDemotionsCount: Int,
        totalRewindEventCount: Int,
        firstDemotionEpisodeId: String?,
        traceCount: Int,
        totalCrossPodcastDroppedCount: Int = 0
    ) {
        self.podcastId = podcastId
        self.finalMode = finalMode
        self.totalDemotionsCount = totalDemotionsCount
        self.totalRewindEventCount = totalRewindEventCount
        self.firstDemotionEpisodeId = firstDemotionEpisodeId
        self.traceCount = traceCount
        self.totalCrossPodcastDroppedCount = totalCrossPodcastDroppedCount
    }

    /// Empty sentinel for an unspecified-podcast slot. Used as a Codable
    /// fallback for pre-q45f.3 report artifacts that don't carry the
    /// field at all (the field decodes to `[]`, not `[.empty]`).
    ///
    /// **Returned by `compute(podcastEpisodes:)` only when input is
    /// empty.** Mirrors the cycle-4 M-2 disambiguation on
    /// `NarlQ45fCounterfactual.empty`: a podcast whose every trace
    /// carries only foreign events (corpus-wide capture bug)
    /// deliberately does NOT collapse to `.empty` — it returns a non-
    /// `.empty` rollup with `totalCrossPodcastDroppedCount > 0`,
    /// `totalRewindEventCount == 0`, and `traceCount > 0` so the
    /// corpus alarm survives at the per-podcast aggregation level
    /// (verified by `carryforwardAllForeignAcrossAllTracesSurfacesDropCount`).
    ///
    /// **Caution:** equality-with-`.empty` is NOT a reliable "no signal"
    /// check on real harness output. A fixture with a genuinely empty
    /// `podcastId` field (a corrupt or pre-frozen-trace-v3 capture)
    /// produces a real rollup whose `podcastId == ""` — indistinguishable
    /// from this sentinel by equality. Prefer `traceCount == 0` as the
    /// "synthesized empty" check; that works because the all-foreign-
    /// across-all-traces case above produces `traceCount > 0`, so the
    /// `.empty`-vs-`traceCount > 0` split is the right boundary.
    static let empty = NarlQ45fCarryforwardRollup(
        podcastId: "",
        finalMode: SkipMode.auto.rawValue,
        totalDemotionsCount: 0,
        totalRewindEventCount: 0,
        firstDemotionEpisodeId: nil,
        traceCount: 0,
        totalCrossPodcastDroppedCount: 0
    )

    /// Codable: `totalCrossPodcastDroppedCount` was added in q45f.3
    /// cycle 4; pre-cycle-4 fixtures decode to 0 via `decodeIfPresent`.
    /// Encode always emits the field.
    enum CodingKeys: String, CodingKey {
        case podcastId, finalMode
        case totalDemotionsCount, totalRewindEventCount
        case firstDemotionEpisodeId, traceCount
        case totalCrossPodcastDroppedCount
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            podcastId: try c.decode(String.self, forKey: .podcastId),
            finalMode: try c.decode(String.self, forKey: .finalMode),
            totalDemotionsCount: try c.decode(Int.self, forKey: .totalDemotionsCount),
            totalRewindEventCount: try c.decode(Int.self, forKey: .totalRewindEventCount),
            firstDemotionEpisodeId: try c.decodeIfPresent(String.self, forKey: .firstDemotionEpisodeId),
            traceCount: try c.decode(Int.self, forKey: .traceCount),
            totalCrossPodcastDroppedCount: try c.decodeIfPresent(
                Int.self, forKey: .totalCrossPodcastDroppedCount
            ) ?? 0
        )
    }

    /// Compute one carryforward for a single podcast's traces. All input
    /// traces must share the same `podcastId` (precondition); cross-
    /// podcast input is a programming error at the call site.
    static func compute(podcastEpisodes: [FrozenTrace]) -> NarlQ45fCarryforwardRollup {
        guard let firstId = podcastEpisodes.first?.podcastId else {
            return .empty
        }
        precondition(
            podcastEpisodes.allSatisfy { $0.podcastId == firstId },
            "NarlQ45fCarryforwardRollup.compute(podcastEpisodes:) requires every trace to share the same podcastId; got mixed input. Counterfactual carryforward is per-podcastId because production keys trust state on podcastId, not on the heuristic show label."
        )
        // Stable sort by `(capturedAt, episodeId)` — Swift's stdlib sort
        // is unstable, so two traces with the same `capturedAt` (real-
        // world: batch-imported fixtures, simultaneous downloads,
        // synthesized-timestamp test fixtures) could swap order across
        // runs/toolchains and flip `firstDemotionEpisodeId`. Adding
        // `episodeId` as a deterministic secondary key pins the order.
        let sorted = podcastEpisodes.sorted {
            ($0.capturedAt, $0.episodeId) < ($1.capturedAt, $1.episodeId)
        }
        var state = Q45fReplayGate.State.freshDefault
        var totalDemotions = 0
        var totalEvents = 0
        var totalDropped = 0
        var firstDemotionEpisodeId: String?
        for trace in sorted {
            let (filtered, droppedCount) = filterCrossPodcastEvents(
                trace.listenRewindEvents,
                expectedPodcastId: trace.podcastId,
                contextLabel: "carryforward(\(firstId), trace=\(trace.episodeId))"
            )
            totalDropped += droppedCount
            guard !filtered.isEmpty else { continue }
            // H2 + cycle-4 M-3: gate precondition requires chronological
            // order; sort by (event.time, event.windowId). See
            // NarlQ45fCounterfactual.compute(trace:) for the tiebreaker
            // rationale.
            let events = filtered.sorted {
                ($0.time, $0.windowId) < ($1.time, $1.windowId)
            }
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
            traceCount: sorted.count,
            totalCrossPodcastDroppedCount: totalDropped
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
