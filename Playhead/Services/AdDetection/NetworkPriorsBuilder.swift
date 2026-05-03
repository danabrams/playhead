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
// CURRENT CONSUMPTION (cycle-1, 2026-05-03): only the
// `typicalAdDuration` field on the produced `NetworkPriors` flows into a
// production knob via `PriorHierarchyResolver` -> `DurationPrior`. The
// other aggregated fields (`commonSponsors`, `typicalSlotPositions`,
// `musicBracketPrevalence`, `metadataTrustAverage`) are computed but not
// yet consumed in production — they're reserved for future tier
// upgrades. Snapshots populate the duration field with real data and
// leave the others at neutral defaults; this matches the
// `ShowLocalPriorsBuilder` shape (only `typicalAdDuration` lit, others
// `nil`).

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
