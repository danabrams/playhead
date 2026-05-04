// NetworkPriorsBuilder.swift
// playhead-spxs: Real producer for `NetworkPriors` from persisted
// per-show `PodcastProfile` rows.
//
// Until this bead landed, the network-priors tier of the 4-level prior
// hierarchy was a no-op — `AdDetectionService.resolveEpisodePriors`
// passed `networkPriors: nil` because no production aggregator existed.
//
// This builder reads the `adDurationStatsJSON` aggregates from a
// collection of `PodcastProfile` rows that share a network identity and
// turns them into a single `NetworkPriors` value the resolver can blend
// in via `NetworkPriors.decayedWeight(...)`. The cross-show data is
// already durably persisted on each profile (one row per show); this
// builder is a computed-on-read aggregator that doesn't add any new
// persisted state — it just plumbs the rows that already exist into
// `NetworkPriorAggregator.aggregate(_:)`.
//
// Threshold: profiles below `ShowLocalPriorsBuilder.minSampleCount`
// (5 confirmed ads) are filtered out before aggregation. This prevents
// a single-observation show from dragging the network mean. Mirrors the
// gate the show-local tier already enforces — same minimum sample bar
// across all tiers that consume `adDurationStatsJSON`.
//
// GUARDRAIL: The builder is pure. Persistence (the per-profile
// `networkId` column + per-profile `adDurationStatsJSON` carry-forward)
// lives in `AnalysisStore`; the cross-network lookup
// (`fetchProfiles(forNetworkId:)`) lives there too. The builder simply
// composes them.
//
// CURRENT CONSUMPTION (cycle-1, 2026-05-03):
//   • `typicalAdDuration` — flows to a production knob via
//     `PriorHierarchy.resolve` -> `DurationPrior`. Real data.
//   • `commonSponsors` — flows to `sponsorRecurrenceExpectation` via
//     `min(1.0, Float(commonSponsors.count) * 0.15)` in
//     `PriorHierarchy.resolve`. Currently always `[:]` (the snapshot's
//     `sponsors` map is `[:]`), so the derived recurrence is 0 and the
//     blend pulls `sponsorRecurrenceExpectation` toward 0 with weight
//     `networkDecay`. The global default for this axis is 0.3 (NOT 0,
//     see `GlobalPriorDefaults.standard.sponsorRecurrenceExpectation`),
//     so the blend is *materially* changing the scalar that flows out
//     of the resolver today — but only on this one axis is the math
//     non-trivial. Note: "mathematically active" is NOT the same as
//     "load-bearing on a production knob". `ResolvedPriors.sponsor-
//     RecurrenceExpectation` is currently flagged "Reserved for future
//     consumers" in `PriorHierarchy.swift` and is not read by any
//     downstream production code, so the blend's effect on this axis
//     does not influence detector behavior today. Pinned in
//     `PriorHierarchyWireUpTests.globalPriorDefaultsStandardValuesArePinned`.
//   • `typicalSlotPositions` — computed by the aggregator but NOT
//     consumed by `PriorHierarchy.resolve`. Truly dormant.
//   • `musicBracketPrevalence`, `metadataTrustAverage` — DO flow through
//     `PriorHierarchy.resolve` blends into `ResolvedPriors.musicBracketTrust`
//     and `metadataTrust`. With the snapshot's neutral defaults
//     (`musicBracketRate: 0.5`, `metadataTrust: 0.5`) and the global
//     defaults pinned at exactly 0.5, the blend is currently a numeric
//     no-op (`0.5 → 0.5` regardless of `networkDecay` weight). The
//     network tier is therefore *active* on these axes — it just
//     contributes a value identical to the global baseline today.
// What this means in practice: the network tier currently influences
// detector behavior on exactly one axis — `typicalAdDuration` —
// because that is the only axis whose `ResolvedPriors` field is read
// by a downstream consumer (`DurationPrior(resolvedPriors:)`). The
// other axes are wired correctly so a future producer that lights
// them up (e.g. a producer that fills `sponsors` from sibling
// profiles, or switches the snapshot's `musicBracketRate` to a
// real-data value) AND a future consumer that reads
// `sponsorRecurrenceExpectation` / `musicBracketTrust` /
// `metadataTrust` will flow through without any further plumbing.

import Foundation

// MARK: - NetworkPriorsBuilder

/// Pure helper that converts a collection of persisted per-show
/// `PodcastProfile` rows into the `NetworkPriors` value the
/// `PriorHierarchyResolver` expects.
///
/// Aggregation proceeds in two stages:
///   1. Filter profiles down to those with a usable
///      `adDurationStatsJSON` (decodable, sampleCount >= threshold).
///   2. Project each surviving profile into a `ShowPriorSnapshot` and
///      hand the batch to `NetworkPriorAggregator.aggregate(_:)`.
enum NetworkPriorsBuilder {

    /// Build network priors from a set of `PodcastProfile` rows that
    /// share a network identity, or `nil` when nothing usable survives
    /// the filter.
    ///
    /// Returns nil when:
    ///   • `profiles` is empty.
    ///   • Every profile is missing or has corrupt `adDurationStatsJSON`.
    ///   • Every profile is below
    ///     `ShowLocalPriorsBuilder.minSampleCount`.
    /// All three nil paths funnel the resolver to fall through the
    /// network tier — graceful degradation when the cross-show signal
    /// isn't yet rich enough to be a useful prior.
    static func build(from profiles: [PodcastProfile]) -> NetworkPriors? {
        guard !profiles.isEmpty else { return nil }

        let snapshots: [ShowPriorSnapshot] = profiles.compactMap { profile in
            guard let json = profile.adDurationStatsJSON,
                  !json.isEmpty,
                  let data = json.data(using: .utf8),
                  let stats = try? JSONDecoder().decode(AdDurationStats.self, from: data),
                  stats.sampleCount >= ShowLocalPriorsBuilder.minSampleCount,
                  stats.meanDuration > 0
            else {
                return nil
            }
            // Other ShowPriorSnapshot fields are not yet populated by a
            // production producer — see the file-header note. We pass
            // neutral defaults so the aggregator's weighted averages
            // don't fabricate signal from missing data. The
            // duration-driven part of the aggregate is what currently
            // flows to a production knob.
            // `weight: max(1.0, ...)` guards against a future change to
            // `minSampleCount` that drops below 1 (e.g. an experiment
            // that loosens the threshold to 0). With the threshold at 5
            // today, the clamp is a no-op for live data. The guarded
            // aggregators (`aggregateSponsors`, `weightedAverage`) early-
            // return on `totalWeight == 0`, so they're safe — but
            // `NetworkPriorAggregator.clusterPositions` divides by
            // `(old.totalWeight + weight)` with no guard, which would
            // produce NaN if both addends were 0. That path is
            // unreachable today because `slotPositions: []` is always
            // empty, but a future producer that lights up real slot
            // positions would expose it. Defense-in-depth: clamp the
            // input weight before it can flow there.
            return ShowPriorSnapshot(
                sponsors: [:],
                slotPositions: [],
                averageAdDuration: stats.meanDuration,
                musicBracketRate: 0.5,
                metadataTrust: 0.5,
                weight: max(1.0, Float(stats.sampleCount))
            )
        }

        guard !snapshots.isEmpty else { return nil }

        return NetworkPriorAggregator.aggregate(snapshots)
    }
}
