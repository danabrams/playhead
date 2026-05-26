// FusionLiftScoringTests.swift
// playhead-au2v.1.27 — Phase A tests: hermetic, SYNTHETIC-span unit tests
// for the fusion-lift scorer foundation. No audio, no Foundation Models,
// no live pipeline — every input is a hand-built value.
//
// Coverage:
//   * Ground-truth bridge: CorpusAnnotation.AdWindow → MetricGroundTruthAd,
//     including the AdType → AdFormat fold.
//   * Detection bridge: AdWindow → MetricDetectedAd, including the
//     decisionState filter (candidate/confirmed/applied count;
//     suppressed/reverted/unknown drop).
//   * SpanF1 math: precision/recall/F1 from TP/FP/miss, undefined cases.
//   * FusionLiftResult: positive / zero / negative deltas, and nil
//     propagation from undefined metrics.

import Foundation
import Testing
@testable import Playhead

@Suite("FusionLift scorer foundation (au2v.1.27 Phase A)")
struct FusionLiftScoringTests {

    // MARK: - Fixtures

    private static func annotationWindow(
        start: Double,
        end: Double,
        adType: CorpusAnnotation.AdType = .hostRead
    ) -> CorpusAnnotation.AdWindow {
        CorpusAnnotation.AdWindow(
            startSeconds: start,
            endSeconds: end,
            advertiser: "Acme",
            product: "Widget",
            adType: adType,
            transitionType: .explicit,
            confidenceNotes: nil
        )
    }

    private static func storeAdWindow(
        id: String,
        start: Double,
        end: Double,
        confidence: Double = 0.9,
        decisionState: String
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: decisionState,
            detectorVersion: "test-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    // MARK: - Ground-truth bridge

    @Test("GT bridge copies bounds and threads identity verbatim")
    func gtBridge_copiesBounds() {
        let gt = MetricGroundTruthAd(
            annotationWindow: Self.annotationWindow(start: 180, end: 240),
            id: "gt-1",
            podcastId: "pod",
            episodeId: "ep"
        )
        #expect(gt.id == "gt-1")
        #expect(gt.podcastId == "pod")
        #expect(gt.episodeId == "ep")
        #expect(gt.startTime == 180)
        #expect(gt.endTime == 240)
        #expect(gt.duration == 60)
        #expect(gt.seedFired == false, "seedFired defaults to false (corpus has no anchor signal)")
    }

    @Test("GT bridge folds AdType onto AdFormat")
    func gtBridge_adTypeFold() {
        #expect(MetricGroundTruthAd.adFormat(for: .hostRead) == .hostRead)
        #expect(MetricGroundTruthAd.adFormat(for: .blendedHostRead) == .hostRead)
        #expect(MetricGroundTruthAd.adFormat(for: .producedSegment) == .produced)
        #expect(MetricGroundTruthAd.adFormat(for: .promo) == .produced)
        #expect(MetricGroundTruthAd.adFormat(for: .dynamicInsertion) == .dynamic)
    }

    // MARK: - Detection bridge: decisionState filter

    @Test("detection bridge admits candidate/confirmed/applied, drops suppressed/reverted/unknown")
    func detectionBridge_decisionStateFilter() {
        // Admitted (real ad predictions)
        for state in [
            AdDecisionState.candidate.rawValue,
            AdDecisionState.confirmed.rawValue,
            AdDecisionState.applied.rawValue,
        ] {
            let row = Self.storeAdWindow(id: "w-\(state)", start: 0, end: 30, decisionState: state)
            let detected = MetricDetectedAd(adWindow: row, podcastId: "pod", episodeId: "ep")
            #expect(detected != nil, "decisionState=\(state) must bridge to a detection")
            #expect(detected?.startTime == 0)
            #expect(detected?.endTime == 30)
            #expect(detected?.confidence == 0.9)
            #expect(detected?.path == .backfill)
        }

        // Dropped (audit/observability rows + garbage)
        for state in [
            AdDecisionState.suppressed.rawValue,
            AdDecisionState.reverted.rawValue,
            "totally-unknown-state",
        ] {
            let row = Self.storeAdWindow(id: "w-\(state)", start: 0, end: 30, decisionState: state)
            let detected = MetricDetectedAd(adWindow: row, podcastId: "pod", episodeId: "ep")
            #expect(detected == nil, "decisionState=\(state) must NOT bridge to a detection")
        }
    }

    @Test("skipEligibleDetections filters a mixed list down to real ad predictions")
    func detectionBridge_batchFilter() {
        let rows = [
            Self.storeAdWindow(id: "a", start: 0, end: 30, decisionState: AdDecisionState.confirmed.rawValue),
            Self.storeAdWindow(id: "b", start: 60, end: 90, decisionState: AdDecisionState.suppressed.rawValue),
            Self.storeAdWindow(id: "c", start: 120, end: 150, decisionState: AdDecisionState.applied.rawValue),
            Self.storeAdWindow(id: "d", start: 180, end: 210, decisionState: AdDecisionState.reverted.rawValue),
            Self.storeAdWindow(id: "e", start: 240, end: 270, decisionState: AdDecisionState.candidate.rawValue),
        ]
        let detections = MetricsBatch.skipEligibleDetections(
            from: rows, podcastId: "pod", episodeId: "ep"
        )
        #expect(detections.count == 3, "only confirmed/applied/candidate survive")
        #expect(Set(detections.map(\.id)) == ["a", "c", "e"])
    }

    // MARK: - End-to-end pairing through the bridges

    @Test("bridged GT + detections pair into TP/FP/miss correctly")
    func bridges_pairThroughMetricsBatch() {
        let gts = [
            MetricGroundTruthAd(annotationWindow: Self.annotationWindow(start: 0, end: 30),
                                id: "gt-a", podcastId: "pod", episodeId: "ep"),
            MetricGroundTruthAd(annotationWindow: Self.annotationWindow(start: 100, end: 130),
                                id: "gt-b", podcastId: "pod", episodeId: "ep"),
        ]
        let rows = [
            // overlaps gt-a → TP
            Self.storeAdWindow(id: "d-a", start: 2, end: 28, decisionState: AdDecisionState.confirmed.rawValue),
            // overlaps nothing → FP
            Self.storeAdWindow(id: "d-fp", start: 200, end: 230, decisionState: AdDecisionState.confirmed.rawValue),
            // suppressed → dropped before pairing
            Self.storeAdWindow(id: "d-drop", start: 100, end: 130, decisionState: AdDecisionState.suppressed.rawValue),
        ]
        let detections = MetricsBatch.skipEligibleDetections(from: rows, podcastId: "pod", episodeId: "ep")
        let batch = MetricsBatch.pair(groundTruth: gts, detections: detections)

        let counts = SpanF1(batch: batch)
        #expect(counts.truePositives == 1, "gt-a paired with d-a")
        #expect(counts.falsePositives == 1, "d-fp matched no GT")
        #expect(counts.misses == 1, "gt-b had no detection (d-drop was filtered out)")
    }

    // MARK: - SpanF1 math

    @Test("SpanF1 computes precision/recall/F1 from counts")
    func spanF1_basicMath() {
        // TP=8, FP=2, miss=2 → P=0.8, R=0.8, F1=0.8
        // (F1 uses a tolerance: 2·0.8·0.8/1.6 is 0.8000000000000002 in
        // IEEE-754, not exactly the 0.8 literal.)
        let f1 = SpanF1(truePositives: 8, falsePositives: 2, misses: 2)
        #expect(f1.precision == 0.8)
        #expect(f1.recall == 0.8)
        #expect(abs((f1.f1 ?? -1) - 0.8) < 1e-12)
    }

    @Test("SpanF1 precision/recall differ when FP != miss")
    func spanF1_asymmetric() {
        // TP=6, FP=2, miss=4 → P=0.75, R=0.6, F1=2*.75*.6/(1.35)=0.6667
        let f1 = SpanF1(truePositives: 6, falsePositives: 2, misses: 4)
        #expect(f1.precision == 0.75)
        #expect(f1.recall == 0.6)
        let expectedF1 = 2 * 0.75 * 0.6 / (0.75 + 0.6)
        #expect(abs((f1.f1 ?? -1) - expectedF1) < 1e-12)
    }

    @Test("SpanF1 undefined cases return nil, never NaN")
    func spanF1_undefined() {
        // No detections at all → precision undefined.
        let noDet = SpanF1(truePositives: 0, falsePositives: 0, misses: 5)
        #expect(noDet.precision == nil)
        #expect(noDet.recall == 0.0, "5 GT ads, 0 paired → recall defined as 0")
        #expect(noDet.f1 == nil, "F1 undefined when precision is undefined")

        // No ground truth at all → recall undefined.
        let noGT = SpanF1(truePositives: 0, falsePositives: 3, misses: 0)
        #expect(noGT.recall == nil)
        #expect(noGT.precision == 0.0, "3 detections, 0 paired → precision defined as 0")
        #expect(noGT.f1 == nil, "F1 undefined when recall is undefined")

        // Empty batch → everything undefined.
        let empty = SpanF1(truePositives: 0, falsePositives: 0, misses: 0)
        #expect(empty.precision == nil)
        #expect(empty.recall == nil)
        #expect(empty.f1 == nil)
    }

    @Test("SpanF1 zero-overlap (defined P=0,R=0) yields F1=0, not nil or NaN")
    func spanF1_zeroOverlapSingularity() {
        // Detections AND GT ads exist but none paired: TP=0, FP=3, miss=4.
        // P = 0/3 = 0 (defined), R = 0/4 = 0 (defined), P+R == 0 → F1 = 0.
        let f1 = SpanF1(truePositives: 0, falsePositives: 3, misses: 4)
        #expect(f1.precision == 0.0)
        #expect(f1.recall == 0.0)
        #expect(f1.f1 == 0.0, "harmonic-mean singularity must resolve to 0, not NaN/nil")
    }

    // MARK: - FusionLiftResult (count-based)

    @Test("FusionLiftResult positive lift when enabled improves precision and recall")
    func lift_positive() {
        // OFF:  TP=6, FP=4, miss=4 → P=0.6, R=0.6
        // ON :  TP=8, FP=2, miss=2 → P=0.8, R=0.8
        let off = SpanF1(truePositives: 6, falsePositives: 4, misses: 4)
        let on = SpanF1(truePositives: 8, falsePositives: 2, misses: 2)
        let lift = FusionLiftResult(off: off, enabled: on)

        #expect(abs((lift.precisionDelta ?? 0) - 0.2) < 1e-12)
        #expect(abs((lift.recallDelta ?? 0) - 0.2) < 1e-12)
        let f1Delta = try! #require(lift.f1Delta)
        #expect(f1Delta > 0, "F1 lift must be positive: \(lift.description)")
    }

    @Test("FusionLiftResult zero lift when arms are identical")
    func lift_zero() {
        let arm = SpanF1(truePositives: 7, falsePositives: 3, misses: 3)
        let lift = FusionLiftResult(off: arm, enabled: arm)
        #expect(lift.precisionDelta == 0.0)
        #expect(lift.recallDelta == 0.0)
        #expect(lift.f1Delta == 0.0)
    }

    @Test("FusionLiftResult negative lift when enabled regresses")
    func lift_negative() {
        // OFF better than ON.
        let off = SpanF1(truePositives: 9, falsePositives: 1, misses: 1)
        let on = SpanF1(truePositives: 5, falsePositives: 5, misses: 5)
        let lift = FusionLiftResult(off: off, enabled: on)
        #expect((lift.precisionDelta ?? 0) < 0)
        #expect((lift.recallDelta ?? 0) < 0)
        #expect((lift.f1Delta ?? 0) < 0)
    }

    @Test("FusionLiftResult nil-propagates when one arm's metric is undefined")
    func lift_nilPropagation() {
        // ON arm has no detections → precision undefined → precisionDelta and
        // f1Delta must be nil; recallDelta is still defined (both recalls
        // defined).
        let off = SpanF1(truePositives: 6, falsePositives: 4, misses: 4)
        let on = SpanF1(truePositives: 0, falsePositives: 0, misses: 10)
        let lift = FusionLiftResult(off: off, enabled: on)
        #expect(lift.precisionDelta == nil, "ON precision undefined ⇒ precisionDelta nil")
        #expect(lift.f1Delta == nil, "ON F1 undefined ⇒ f1Delta nil")
        #expect(lift.recallDelta != nil, "both recalls defined ⇒ recallDelta defined")
    }

    // MARK: - FusionLiftResult (coverage / MetricsSummary based)

    @Test("FusionLiftResult over MetricsSummary diffs coverage precision/recall and derives F1")
    func lift_fromSummaries() {
        // OFF: detection covers half of a 100s GT ad → recall 0.5, and the
        // detection is fully inside the GT → precision 1.0.
        let gtOff = MetricGroundTruthAd(
            annotationWindow: Self.annotationWindow(start: 0, end: 100),
            id: "g", podcastId: "pod", episodeId: "ep")
        let detOff = MetricDetectedAd(
            adWindow: Self.storeAdWindow(id: "d", start: 0, end: 50,
                                         decisionState: AdDecisionState.confirmed.rawValue),
            podcastId: "pod", episodeId: "ep")!
        let offSummary = MetricsSummary(
            batch: MetricsBatch.pair(groundTruth: [gtOff], detections: [detOff]))
        #expect(offSummary.coverageRecall == 0.5)
        #expect(offSummary.coveragePrecision == 1.0)

        // ENABLED: detection now covers the full 100s GT ad → recall 1.0,
        // precision 1.0.
        let gtOn = gtOff
        let detOn = MetricDetectedAd(
            adWindow: Self.storeAdWindow(id: "d2", start: 0, end: 100,
                                         decisionState: AdDecisionState.confirmed.rawValue),
            podcastId: "pod", episodeId: "ep")!
        let enabledSummary = MetricsSummary(
            batch: MetricsBatch.pair(groundTruth: [gtOn], detections: [detOn]))
        #expect(enabledSummary.coverageRecall == 1.0)

        let lift = FusionLiftResult(off: offSummary, enabled: enabledSummary)
        #expect(lift.precisionDelta == 0.0, "precision stayed 1.0")
        #expect(abs((lift.recallDelta ?? 0) - 0.5) < 1e-12, "recall improved 0.5 → 1.0")
        // offF1 = 2*1*0.5/1.5 = 0.6667; enabledF1 = 1.0 → positive f1 lift.
        #expect((lift.f1Delta ?? 0) > 0)
        #expect(lift.offF1 != nil && lift.enabledF1 == 1.0)
    }

    @Test("description renders n/a for undefined deltas")
    func lift_description() {
        let lift = FusionLiftResult(precisionDelta: nil, recallDelta: 0.1, f1Delta: nil)
        #expect(lift.description.contains("precisionΔ=n/a"))
        #expect(lift.description.contains("recallΔ=+0.1000"))
        #expect(lift.description.contains("f1Δ=n/a"))
    }
}
