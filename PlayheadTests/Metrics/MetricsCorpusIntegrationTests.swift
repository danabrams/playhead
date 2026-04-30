// MetricsCorpusIntegrationTests.swift
// playhead-352 — A7: Metrics framework integration test.
//
// Wires the metric framework through a fixture-driven pipeline. Lives
// as an XCTest class (NOT Swift Testing) so the test plan's
// `skippedTests` filter can keep it out of `PlayheadFastTests` —
// xctestplan filters silently ignore Swift Testing identifiers, so
// XCTest is the only way to gate by test plan today (per CLAUDE.md).
//
// Per the bead, baseline value capture against the production corpus
// is DEFERRED to a follow-up bead. This test currently exercises the
// integration point with a synthetic mini-corpus that mirrors the
// shape of `TestEpisodeAnnotation` so the conversion path
// (`TestAdSegment` → `MetricGroundTruthAd`) is tested end-to-end. When the
// real corpus is wired up, the real `TestEpisodeAnnotation` fixtures
// can be plugged into this same code path with no rewriting.

import Foundation
import XCTest
@testable import Playhead

final class MetricsCorpusIntegrationTests: XCTestCase {

    /// Smoke test the full path: fixture-shaped GT → MetricsBatch →
    /// 9 metrics + slicing. Uses a small synthetic corpus with two
    /// podcasts, three formats, and live + backfill detections.
    func testSyntheticCorpusProducesAllNineMetricsAcrossSlices() {
        // Two podcasts, three formats represented across them.
        let diaryAds = [
            buildSegment(start: 60, end: 90, advertiser: "Squarespace", style: .hostRead),
            buildSegment(start: 600, end: 630, advertiser: "BetterHelp", style: .producedSegment),
        ]
        let conanAds = [
            buildSegment(start: 30, end: 50, advertiser: "NordVPN", style: .dynamicInsertion),
            buildSegment(start: 800, end: 830, advertiser: "HelloFresh", style: .hostRead),
        ]

        let diaryGT = diaryAds.enumerated().map { idx, seg in
            convertSegmentToGT(seg, podcast: "diary", episode: "e1", index: idx)
        }
        let conanGT = conanAds.enumerated().map { idx, seg in
            convertSegmentToGT(seg, podcast: "conan", episode: "e1", index: idx)
        }
        let allGT = diaryGT + conanGT

        // Synthesize detections that approximate the GT but with realistic
        // imperfections: small boundary drift, one miss, one false positive.
        let detections: [MetricDetectedAd] = [
            // Diary: Squarespace — perfect TP on live path
            MetricDetectedAd(id: "det-diary-0",
                             podcastId: "diary", episodeId: "e1",
                             startTime: 60, endTime: 90,
                             path: .live, firstConfirmationTime: 58, confidence: 0.95),
            // Diary: BetterHelp — backfill, slightly late start, slightly early end
            MetricDetectedAd(id: "det-diary-1",
                             podcastId: "diary", episodeId: "e1",
                             startTime: 602, endTime: 628,
                             path: .backfill, firstConfirmationTime: nil, confidence: 0.85),
            // Conan: NordVPN — MISSED (no detection)
            // Conan: HelloFresh — partial overlap on live path
            MetricDetectedAd(id: "det-conan-1",
                             podcastId: "conan", episodeId: "e1",
                             startTime: 805, endTime: 825,
                             path: .live, firstConfirmationTime: 803, confidence: 0.82),
            // Conan: false positive in editorial content
            MetricDetectedAd(id: "det-conan-fp",
                             podcastId: "conan", episodeId: "e1",
                             startTime: 1200, endTime: 1230,
                             path: .live, firstConfirmationTime: 1198, confidence: 0.78),
        ]

        let batch = MetricsBatch.pair(groundTruth: allGT, detections: detections)

        // Expected pair shape: 4 GT, 4 detections → 3 TP + 1 miss + 1 FP = 5 pairs
        XCTAssertEqual(batch.pairs.count, 5)
        XCTAssertEqual(batch.pairs.filter { $0.isTruePositive }.count, 3)
        XCTAssertEqual(batch.pairs.filter { $0.isMiss }.count, 1)
        XCTAssertEqual(batch.pairs.filter { $0.isFalsePositive }.count, 1)

        // Compute the full summary and assert the 9 metrics make sense.
        let summary = MetricsSummary(batch: batch)

        // Seed recall over GT ads: synthetic GT defaults seedFired=true →
        // 4/4 = 1.0
        XCTAssertEqual(summary.seedRecall.numerator, 4)
        XCTAssertEqual(summary.seedRecall.denominator, 4)
        XCTAssertEqual(summary.seedRecall.ratio, 1.0)

        // Span IoU: 3 TPs (perfect, near-perfect, partial overlap)
        XCTAssertEqual(summary.spanIoU.count, 3)
        // Median IoU should be close to the BetterHelp value, which has
        // ~26s overlap on a ~30s GT and a ~26s detection → IoU ≈ 26/30
        XCTAssertNotNil(summary.spanIoU.median)

        // Errors should be small for the well-aligned pairs
        XCTAssertNotNil(summary.medianStartError)
        XCTAssertNotNil(summary.medianEndError)

        // Coverage recall: most GT seconds covered, but the missed Conan ad
        // (20s) is not, so recall should be < 1.0
        if let recall = summary.coverageRecall {
            XCTAssertLessThan(recall, 1.0)
            XCTAssertGreaterThan(recall, 0.5)
        } else {
            XCTFail("coverageRecall should not be nil with 4 GT ads")
        }

        // Coverage precision: detection includes a 30s FP outside any GT
        // (and one detection is partial), so precision < 1.0
        if let precision = summary.coveragePrecision {
            XCTAssertLessThan(precision, 1.0)
        } else {
            XCTFail("coveragePrecision should not be nil with 4 detections")
        }

        // Lead time: 3 TPs but only 2 have confirmation timestamps (the
        // BetterHelp backfill detection has nil confirmation)
        XCTAssertEqual(summary.leadTime.count, 2)
        XCTAssertNotNil(summary.leadTime.median)

        // Slicing checks
        let live = batch.sliced(byPath: .live)
        // TPs on live: Squarespace + HelloFresh; FP on live; (BetterHelp is backfill, NordVPN miss has no path)
        XCTAssertEqual(live.pairs.count, 3)

        let diary = batch.sliced(byPodcast: "diary")
        XCTAssertEqual(diary.pairs.count, 2)

        let hostRead = batch.sliced(byFormat: .hostRead)
        // Squarespace + HelloFresh GT pairs → 2
        XCTAssertEqual(hostRead.pairs.count, 2)

        let dynamic = batch.sliced(byFormat: .dynamic)
        // Only the NordVPN miss
        XCTAssertEqual(dynamic.pairs.count, 1)
        XCTAssertTrue(dynamic.pairs[0].isMiss)
    }

    /// Sanity-check the bridge from existing `TestAdSegment` fixture
    /// shape to the metrics' `MetricGroundTruthAd`. This is the integration
    /// point the corpus baseline bead will plug into.
    func testTestAdSegmentBridgeRoundTrip() {
        let seg = buildSegment(start: 100, end: 130, advertiser: "Audible", style: .blendedHostRead)
        let gt = convertSegmentToGT(seg, podcast: "diary", episode: "e1", index: 0)
        XCTAssertEqual(gt.startTime, 100)
        XCTAssertEqual(gt.endTime, 130)
        XCTAssertEqual(gt.format, .hostRead)  // blendedHostRead folds in
        XCTAssertEqual(gt.podcastId, "diary")
        XCTAssertEqual(gt.episodeId, "e1")
        XCTAssertTrue(gt.seedFired)
    }

    /// Empty input — the integration path must produce a sensible empty
    /// summary (every metric returns its empty form, no crash).
    func testEmptyCorpusProducesEmptySummary() {
        let batch = MetricsBatch.pair(groundTruth: [], detections: [])
        let summary = MetricsSummary(batch: batch)
        XCTAssertNil(summary.seedRecall.ratio)
        XCTAssertTrue(summary.spanIoU.isEmpty)
        XCTAssertNil(summary.medianStartError)
        XCTAssertNil(summary.coverageRecall)
        XCTAssertTrue(summary.leadTime.isEmpty)
    }

    // MARK: - Fixtures

    private func buildSegment(
        start: Double,
        end: Double,
        advertiser: String,
        style: TestAdSegment.DeliveryStyle
    ) -> TestAdSegment {
        TestAdSegment(
            startTime: start,
            endTime: end,
            advertiser: advertiser,
            product: nil,
            adType: .midRoll,
            deliveryStyle: style,
            difficulty: .medium,
            notes: nil
        )
    }

    private func convertSegmentToGT(
        _ seg: TestAdSegment,
        podcast: String,
        episode: String,
        index: Int
    ) -> MetricGroundTruthAd {
        MetricGroundTruthAd(
            id: "\(podcast)-\(episode)-\(index)",
            podcastId: podcast,
            episodeId: episode,
            startTime: seg.startTime,
            endTime: seg.endTime,
            format: AdFormat.from(seg.deliveryStyle),
            seedFired: true   // synthetic — defaults to fired
        )
    }
}
