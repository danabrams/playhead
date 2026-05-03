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
