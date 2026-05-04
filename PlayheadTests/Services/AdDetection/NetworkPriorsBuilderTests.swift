// NetworkPriorsBuilderTests.swift
// playhead-spxs: Unit tests for the NetworkPriorsBuilder producer.
//
// The builder is the production aggregator that rolls up per-show
// `PodcastProfile` rows (one per show in a network) into a single
// `NetworkPriors` value the prior hierarchy can consume. Mirrors the
// `ShowLocalPriorsBuilder` pattern: pure static `build(...)` taking
// the persisted profiles and returning either a non-nil aggregate or
// `nil` when there is no usable signal.
//
// Threshold + shape calibration:
//   • Empty input -> nil (no shows).
//   • Profiles whose `adDurationStatsJSON` is missing/corrupt are
//     skipped — we don't synthesize a default mean for them. If every
//     profile is missing the column, the whole network falls back to
//     nil.
//   • Profiles below `ShowLocalPriorsBuilder.minSampleCount` are also
//     skipped so the network aggregate is never derived from a single
//     ad observation per show.
//   • A single qualifying show still yields a valid aggregate (the
//     `NetworkPriorAggregator` minShows-1 fallback handles this).

import Foundation
import Testing

@testable import Playhead

@Suite("NetworkPriorsBuilder (playhead-spxs)")
struct NetworkPriorsBuilderTests {

    // MARK: - Empty / nil paths

    @Test("empty profiles list yields nil")
    func builderEmptyProfiles() {
        let result = NetworkPriorsBuilder.build(from: [])
        #expect(result == nil)
    }

    @Test("profiles all without adDurationStatsJSON yield nil")
    func builderAllProfilesMissingStats() {
        let profiles = [
            makeProfile(podcastId: "p1", adDurationStatsJSON: nil, observationCount: 5),
            makeProfile(podcastId: "p2", adDurationStatsJSON: nil, observationCount: 10)
        ]
        #expect(NetworkPriorsBuilder.build(from: profiles) == nil)
    }

    @Test("profiles all with corrupt JSON yield nil")
    func builderAllProfilesCorrupt() {
        let profiles = [
            makeProfile(podcastId: "p1", adDurationStatsJSON: "{bad", observationCount: 5),
            makeProfile(podcastId: "p2", adDurationStatsJSON: "junk", observationCount: 5)
        ]
        #expect(NetworkPriorsBuilder.build(from: profiles) == nil)
    }

    @Test("profiles below sample threshold are filtered out")
    func builderBelowSampleThresholdFiltered() {
        // Both profiles have stats but sampleCount below
        // `ShowLocalPriorsBuilder.minSampleCount` (5) — too few observations
        // to use as a per-show input. Builder returns nil.
        let undersampled1 = AdDurationStats(meanDuration: 30, sampleCount: 1)
        let undersampled2 = AdDurationStats(meanDuration: 60, sampleCount: 2)
        let profiles = [
            makeProfile(podcastId: "p1", adDurationStatsJSON: undersampled1.encodeForTesting(), observationCount: 1),
            makeProfile(podcastId: "p2", adDurationStatsJSON: undersampled2.encodeForTesting(), observationCount: 2)
        ]
        #expect(NetworkPriorsBuilder.build(from: profiles) == nil)
    }

    // MARK: - Single qualifying show

    @Test("single qualifying show produces an aggregate")
    func builderSingleShow() {
        let stats = AdDurationStats(meanDuration: 45, sampleCount: 10)
        let profiles = [
            makeProfile(podcastId: "p1", adDurationStatsJSON: stats.encodeForTesting(), observationCount: 8)
        ]
        let result = NetworkPriorsBuilder.build(from: profiles)
        #expect(result != nil)
        #expect(result?.showCount == 1)
        // Range should bracket the mean. NetworkPriorAggregator widens to
        // a 10-second minimum range when a single show contributes a
        // single duration value.
        let range = try! #require(result?.typicalAdDuration)
        #expect(range.lowerBound <= 45)
        #expect(range.upperBound >= 45)
        let width = range.upperBound - range.lowerBound
        #expect(width >= 10)
    }

    // MARK: - Multi-show aggregation

    @Test("multiple qualifying shows produce a valid aggregate")
    func builderMultipleShows() {
        let s1 = AdDurationStats(meanDuration: 30, sampleCount: 6)
        let s2 = AdDurationStats(meanDuration: 60, sampleCount: 8)
        let s3 = AdDurationStats(meanDuration: 90, sampleCount: 12)
        let profiles = [
            makeProfile(podcastId: "p1", adDurationStatsJSON: s1.encodeForTesting(), observationCount: 6),
            makeProfile(podcastId: "p2", adDurationStatsJSON: s2.encodeForTesting(), observationCount: 8),
            makeProfile(podcastId: "p3", adDurationStatsJSON: s3.encodeForTesting(), observationCount: 12)
        ]
        let result = try! #require(NetworkPriorsBuilder.build(from: profiles))
        #expect(result.showCount == 3)
        // Range should bracket [30, 90].
        #expect(result.typicalAdDuration.lowerBound <= 35)
        #expect(result.typicalAdDuration.upperBound >= 85)
    }

    @Test("profiles below threshold are filtered, qualifying ones drive the aggregate")
    func builderMixedQualifying() {
        // Two qualifying, one undersampled: only the qualifying ones
        // should contribute. The undersampled show's mean (1000s — wildly
        // out of range) must NOT pull the aggregate.
        let qualifying1 = AdDurationStats(meanDuration: 30, sampleCount: 10)
        let qualifying2 = AdDurationStats(meanDuration: 60, sampleCount: 10)
        let undersampled = AdDurationStats(meanDuration: 1000, sampleCount: 1)
        let profiles = [
            makeProfile(podcastId: "p1", adDurationStatsJSON: qualifying1.encodeForTesting(), observationCount: 10),
            makeProfile(podcastId: "p2", adDurationStatsJSON: qualifying2.encodeForTesting(), observationCount: 10),
            makeProfile(podcastId: "p3", adDurationStatsJSON: undersampled.encodeForTesting(), observationCount: 1)
        ]
        let result = try! #require(NetworkPriorsBuilder.build(from: profiles))
        #expect(result.showCount == 2)
        // The bogus 1000s outlier was filtered before aggregation, so the
        // upper bound should remain near the qualifying shows' max (60s),
        // not anywhere near 1000.
        #expect(result.typicalAdDuration.upperBound < 200)
    }

    // MARK: - Aggregator-level weight favoritism (cycle-2 L-6)

    /// playhead-spxs cycle-2 L-6: pin the weighted-average favoritism
    /// path inside `NetworkPriorAggregator.aggregate(_:)`.
    ///
    /// The cycle-2 reviewer asked for "very different sampleCount
    /// weights (5 vs 50_000) confirming weighted average favors
    /// heavier — pins the `weight: max(1.0, Float(stats.sampleCount))`
    /// favoritism." Through the *builder's* homogeneous snapshots
    /// (`sponsors: [:]`, `slotPositions: []`, `musicBracketRate: 0.5`,
    /// `metadataTrust: 0.5` for every show — see `NetworkPriorsBuilder.swift`
    /// line ~115), weight is unobservable today: the only varying field
    /// is `averageAdDuration`, and `aggregateDuration` uses min/max
    /// (not weighted average) on those values. So a builder-level
    /// favoritism test would be vacuous against the current snapshot
    /// shape.
    ///
    /// This test instead targets the aggregator directly, with two
    /// snapshots whose weights differ by 4 orders of magnitude AND
    /// whose `musicBracketRate` values differ. The inline
    /// `weightedAverage(...)` call that produces `musicBracketPrevalence`
    /// (and the `metadataTrustAverage` sibling — both call sites in
    /// `NetworkPriorAggregator.aggregate`) MUST favor the heavier-
    /// weighted snapshot's value. If the producer is ever changed to
    /// surface real (non-uniform) `musicBracketRate` snapshots through
    /// the builder, this favoritism becomes load-bearing on a
    /// production knob — pinning it now keeps the contract honest in
    /// advance of that producer.
    @Test("aggregator weighted-average favors heavier-weighted snapshot")
    func aggregatorWeightedAverageFavorsHeavier() {
        let lightweight = ShowPriorSnapshot(
            sponsors: [:],
            slotPositions: [],
            averageAdDuration: 30,
            musicBracketRate: 0.0,
            metadataTrust: 0.0,
            weight: 5
        )
        let heavyweight = ShowPriorSnapshot(
            sponsors: [:],
            slotPositions: [],
            averageAdDuration: 30,
            musicBracketRate: 1.0,
            metadataTrust: 1.0,
            weight: 50_000
        )
        let priors = try! #require(
            NetworkPriorAggregator.aggregate([lightweight, heavyweight])
        )
        // Expected weighted average: (0.0 * 5 + 1.0 * 50_000) / 50_005
        // ≈ 0.9999. Anything above 0.99 demonstrates the heavier
        // snapshot dominates; anything below 0.5 would mean the
        // weighted-average path has gone unweighted.
        #expect(priors.musicBracketPrevalence > 0.99)
        #expect(priors.metadataTrustAverage > 0.99)
    }

    /// playhead-spxs cycle-5 missing-test: regression alarm pinning the
    /// neutral-default fields.
    ///
    /// The current `NetworkPriorsBuilder` is single-signal: it derives
    /// only `typicalAdDuration` from real data, and feeds the aggregator
    /// homogeneous neutral defaults for the rest (`sponsors: [:]`,
    /// `slotPositions: []`, `musicBracketRate: 0.5`, `metadataTrust: 0.5`).
    /// This test pins that today's network tier is non-load-bearing on
    /// those four fields — when the producer is later extended to surface
    /// a real signal for any of them, this test will fail and force the
    /// author to update the contract intentionally rather than silently
    /// shifting downstream behavior. Pair with cycle-2 L-6 in
    /// `NetworkPriorsBuilderTests.aggregatorWeightedAverageFavorsHeavier`,
    /// which pins the aggregator math the producer is currently bypassing.
    @Test("builder-fed priors land on neutral defaults for non-duration fields (cycle-5 missing-test)")
    func builderNeutralDefaultsForNonDurationFields() {
        let s1 = AdDurationStats(meanDuration: 30, sampleCount: 10)
        let s2 = AdDurationStats(meanDuration: 60, sampleCount: 20)
        let s3 = AdDurationStats(meanDuration: 45, sampleCount: 50_000)
        let profiles = [
            makeProfile(podcastId: "p1", adDurationStatsJSON: s1.encodeForTesting(), observationCount: 10),
            makeProfile(podcastId: "p2", adDurationStatsJSON: s2.encodeForTesting(), observationCount: 20),
            makeProfile(podcastId: "p3", adDurationStatsJSON: s3.encodeForTesting(), observationCount: 50_000)
        ]
        let result = try! #require(NetworkPriorsBuilder.build(from: profiles))

        // Sponsors empty: builder feeds `sponsors: [:]` for every
        // snapshot. A regression that surfaces real sponsor data without
        // updating the docstring would fail this expectation.
        #expect(result.commonSponsors.isEmpty,
                "expected no common sponsors today; builder feeds empty maps")

        // Slot positions empty: builder feeds `slotPositions: []`.
        #expect(result.typicalSlotPositions.isEmpty,
                "expected no slot positions today; builder feeds empty arrays")

        // musicBracketPrevalence collapses to exactly 0.5 — every
        // snapshot's value is the same constant, so the weighted average
        // is the constant regardless of weight. Even with the 50k-sample
        // outlier weight, the result is 0.5.
        #expect(result.musicBracketPrevalence == 0.5,
                "expected musicBracketPrevalence = 0.5; got \(result.musicBracketPrevalence)")

        // Same for metadataTrust.
        #expect(result.metadataTrustAverage == 0.5,
                "expected metadataTrustAverage = 0.5; got \(result.metadataTrustAverage)")
    }

    /// playhead-spxs cycle-14 missing-test #4: pin the documented
    /// "builder aggregates ALL matching profiles, including the current
    /// show" contract that the `wireUpShowLocalDominatesNetwork`
    /// fixture-caveat docstring depends on.
    ///
    /// `NetworkPriorsBuilder.build(from:)` is a pure aggregator over an
    /// already-filtered profile collection; it does NOT know which row
    /// corresponds to the "current show" being scored. The resolver
    /// (`AdDetectionService.resolveEpisodePriors`) calls
    /// `AnalysisStore.fetchProfiles(forNetworkId:)` which returns ALL
    /// rows whose `networkId` matches — the current show included. This
    /// test pins that no silent self-filter has been introduced into
    /// the builder (e.g. an "ignore the heaviest weight" outlier rule
    /// that would coincidentally elide the current show when its
    /// sample count exceeds siblings).
    ///
    /// If a future change adds a "remove the current show before
    /// aggregating" pre-filter at the resolver layer, that's fine —
    /// but the builder itself must stay current-show-agnostic, and
    /// this test is the contract pin.
    @Test("builder aggregates every matching profile passed in (no self-filter)")
    func builderIncludesAllProfilesWithMatchingNetworkId() {
        // Current-show profile (heavy sample count, distinctive mean
        // far from siblings — would be detectable as an outlier).
        let currentStats = AdDurationStats(meanDuration: 60, sampleCount: 50)
        // Two sibling profiles tightly clustered at 10s — a builder that
        // "intelligently" dropped the highest-weight row would land on a
        // narrow ~10s aggregate.
        let siblingStats = AdDurationStats(meanDuration: 10, sampleCount: 20)
        let profiles = [
            makeProfile(podcastId: "pod-current", adDurationStatsJSON: currentStats.encodeForTesting(), observationCount: 50),
            makeProfile(podcastId: "pod-sibling-1", adDurationStatsJSON: siblingStats.encodeForTesting(), observationCount: 20),
            makeProfile(podcastId: "pod-sibling-2", adDurationStatsJSON: siblingStats.encodeForTesting(), observationCount: 20)
        ]
        let result = try! #require(NetworkPriorsBuilder.build(from: profiles))

        // showCount counts every qualifying profile, including the
        // current-show row.
        #expect(result.showCount == 3,
                "builder must include all matching profiles in showCount; got \(result.showCount). A self-filter on the current show would land at 2.")

        // typicalAdDuration must span both regimes — current at 60 must
        // not have been silently elided. With NetworkPriorAggregator
        // count=3 (trimOutliers no-op) and aggregateDuration min/max of
        // means [60, 10, 10], the range is exactly 10...60.
        #expect(result.typicalAdDuration.lowerBound <= 12,
                "lowerBound should be near 10 (siblings'); got \(result.typicalAdDuration.lowerBound)")
        #expect(result.typicalAdDuration.upperBound >= 55,
                "upperBound should be near 60 (current show); got \(result.typicalAdDuration.upperBound). A regression that elided the current show would land near 20 (the 10s minimum-width bracket around the siblings' mean).")
    }

    @Test("network aggregate is measurably narrower than global default")
    func builderNarrowsRangeBelowGlobal() {
        // A network whose shows all average around 10s should yield a
        // typicalAdDuration much narrower than the global default's
        // 30...90 (60s wide).
        let stats = AdDurationStats(meanDuration: 10, sampleCount: 20)
        let profiles = (0..<3).map { i in
            makeProfile(
                podcastId: "p\(i)",
                adDurationStatsJSON: stats.encodeForTesting(),
                observationCount: 8
            )
        }
        let result = try! #require(NetworkPriorsBuilder.build(from: profiles))
        let width = result.typicalAdDuration.upperBound - result.typicalAdDuration.lowerBound
        // Global default is 60s wide; a network of converging 10s ad shows
        // should be much tighter.
        #expect(width < 60)
        // And the center should be near 10, not 60 (the global midpoint).
        let center = (result.typicalAdDuration.lowerBound + result.typicalAdDuration.upperBound) / 2.0
        #expect(center < 30)
    }

    // MARK: - Helpers

    private func makeProfile(
        podcastId: String,
        adDurationStatsJSON: String?,
        observationCount: Int
    ) -> PodcastProfile {
        PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: observationCount,
            mode: SkipMode.shadow.rawValue,
            recentFalseSkipSignals: 0,
            traitProfileJSON: nil,
            title: nil,
            adDurationStatsJSON: adDurationStatsJSON,
            networkId: nil
        )
    }
}
