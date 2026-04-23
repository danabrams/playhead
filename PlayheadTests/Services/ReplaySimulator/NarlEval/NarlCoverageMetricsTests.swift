// NarlCoverageMetricsTests.swift
// playhead-gtt9.6: Unit tests for NarlCoverageMetricsCompute.
//
// See docs/narl/2026-04-23-expert-response.md §10 for why the eval metric
// set now splits scored coverage, transcript coverage, candidate recall,
// auto-skip precision/recall, segment IoU, and the unscored-FN rate —
// previously the harness conflated pipeline coverage failures with
// classifier false negatives.

import Foundation
import Testing
@testable import Playhead

@Suite("NarlCoverageMetrics.compute")
struct NarlCoverageMetricsComputeTests {

    // MARK: - Basic shape

    @Test("empty trace yields all-zero coverage")
    func emptyTraceYieldsAllZeroCoverage() {
        let trace = makeTrace(episodeDuration: 600)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace,
            predicted: [],
            groundTruth: []
        )
        #expect(result.metrics.scoredCoverageRatio == 0)
        #expect(result.metrics.transcriptCoverageRatio == 0)
        #expect(result.metrics.candidateRecall == 0)
        #expect(result.metrics.unscoredFNRate == 0)
        #expect(result.fnDecomposition.isEmpty)
    }

    // MARK: - Scored coverage ratio

    @Test("scoredCoverageRatio computes from windowScores")
    func scoredCoverageRatioComputesFromWindowScores() {
        let ws = [
            Self.score(start: 0, end: 20, confidence: 0),
            Self.score(start: 40, end: 70, confidence: 0),
        ]
        let trace = makeTrace(episodeDuration: 100, windowScores: ws)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: [], groundTruth: []
        )
        #expect(abs(result.metrics.scoredCoverageRatio - 0.5) < 1e-9)
    }

    @Test("scoredCoverageRatio handles overlap")
    func scoredCoverageRatioHandlesOverlap() {
        let ws = [
            Self.score(start: 0, end: 30, confidence: 0),
            Self.score(start: 20, end: 50, confidence: 0),
        ]
        let trace = makeTrace(episodeDuration: 100, windowScores: ws)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: [], groundTruth: []
        )
        #expect(abs(result.metrics.scoredCoverageRatio - 0.5) < 1e-9)
    }

    // MARK: - Transcript coverage

    @Test("transcriptCoverage uses only transcript-dependent sources")
    func transcriptCoverageUsesOnlyTranscriptDependentSources() {
        let evidence: [FrozenTrace.FrozenEvidenceEntry] = [
            .init(source: "classifier", weight: 1, windowStart: 0, windowEnd: 100),
            .init(source: "lexical", weight: 1, windowStart: 0, windowEnd: 50),
            .init(source: "fm", weight: 1, windowStart: 30, windowEnd: 60),
            .init(source: "catalog", weight: 1, windowStart: 80, windowEnd: 90),
            .init(source: "metadata", weight: 1, windowStart: 0, windowEnd: 100),
            .init(source: "acoustic", weight: 1, windowStart: 0, windowEnd: 100),
        ]
        let trace = makeTrace(episodeDuration: 100, evidenceCatalog: evidence)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: [], groundTruth: []
        )
        // Union of lexical [0,50), fm [30,60), catalog [80,90) = [0,60) ∪ [80,90) = 60 + 10 = 70
        // episodeDuration = 100 → 0.7
        #expect(abs(result.metrics.transcriptCoverageRatio - 0.7) < 1e-9)
    }

    // MARK: - Candidate threshold

    @Test("candidate ranges require confidence threshold")
    func candidateRangesRequireConfidenceThreshold() {
        let ws = [
            Self.score(start: 0, end: 10, confidence: 0.39),
            Self.score(start: 10, end: 20, confidence: 0.40),
            Self.score(start: 20, end: 30, confidence: 0.41),
        ]
        // GT fully covers all three windows so candidate recall = fraction of
        // GT overlapping candidate set. Candidates = [10,30), GT = [0,30),
        // so 20/30 of GT is covered.
        let gt = [NarlTimeRange(start: 0, end: 30)]
        let trace = makeTrace(episodeDuration: 30, windowScores: ws)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: [], groundTruth: gt
        )
        #expect(abs(result.metrics.candidateRecall - (20.0 / 30.0)) < 1e-9)
    }

    // MARK: - FN decomposition

    @Test("fn decomposition: pipelineCoverage when GT has no scored overlap")
    func fnDecompositionPipelineCoverage() {
        let ws = [Self.score(start: 0, end: 50, confidence: 1)]
        let gt = [NarlTimeRange(start: 100, end: 200)]
        let trace = makeTrace(episodeDuration: 300, windowScores: ws)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: [], groundTruth: gt
        )
        #expect(result.fnDecomposition.count == 1)
        let entry = result.fnDecomposition[0]
        #expect(entry.kind == .pipelineCoverage)
        #expect(entry.span == NarlTimeRange(start: 100, end: 200))
        #expect(entry.reason.localizedCaseInsensitiveContains("no scored") == true)
    }

    @Test("fn decomposition: classifierRecall when scored but no candidate overlap")
    func fnDecompositionClassifierRecall() {
        // GT fully scored at low confidence — no candidate set.
        let ws = [
            Self.score(start: 0, end: 100, confidence: 0.2),
        ]
        let gt = [NarlTimeRange(start: 0, end: 100)]
        let trace = makeTrace(episodeDuration: 100, windowScores: ws)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: [], groundTruth: gt
        )
        #expect(result.fnDecomposition.count == 1)
        #expect(result.fnDecomposition[0].kind == .classifierRecall)
    }

    @Test("fn decomposition: promotionRecall when candidate but no autoSkip")
    func fnDecompositionPromotionRecall() {
        let ws = [Self.score(start: 0, end: 100, confidence: 0.8)]
        let gt = [NarlTimeRange(start: 0, end: 100)]
        // Predicted auto-skip set is empty (candidates never promoted).
        let trace = makeTrace(episodeDuration: 100, windowScores: ws)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: [], groundTruth: gt
        )
        #expect(result.fnDecomposition.count == 1)
        #expect(result.fnDecomposition[0].kind == .promotionRecall)
    }

    @Test("fn decomposition: correctlyDetected spans are not in FN list")
    func fnDecompositionCorrectlyDetected() {
        let ws = [Self.score(start: 0, end: 100, confidence: 0.9)]
        let gt = [NarlTimeRange(start: 0, end: 100)]
        let predicted = [NarlTimeRange(start: 20, end: 80)]
        let trace = makeTrace(episodeDuration: 100, windowScores: ws)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: predicted, groundTruth: gt
        )
        #expect(result.fnDecomposition.isEmpty)
    }

    // MARK: - Unscored FN rate

    @Test("unscoredFNRate only counts pipelineCoverage failures")
    func unscoredFNRateOnlyCountsPipelineFailures() {
        // Three disjoint GT spans, each partly unscored differently.
        //
        // Span A [0,60): outside any scored window → pipelineCoverage (60s)
        // Span B [100,140): fully scored at low confidence → classifierRecall (40s)
        // Span C [200,230): fully scored + candidate but not promoted → promotionRecall (30s)
        //
        // Total GT = 130s; pipelineCoverage = 60s → unscoredFNRate = 60/130.
        let ws = [
            Self.score(start: 100, end: 140, confidence: 0.2),  // scored, below candidate
            Self.score(start: 200, end: 230, confidence: 0.8),  // scored, above candidate
        ]
        let gt = [
            NarlTimeRange(start: 0, end: 60),
            NarlTimeRange(start: 100, end: 140),
            NarlTimeRange(start: 200, end: 230),
        ]
        let trace = makeTrace(episodeDuration: 300, windowScores: ws)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: [], groundTruth: gt
        )
        #expect(abs(result.metrics.unscoredFNRate - (60.0 / 130.0)) < 1e-9)
    }

    // MARK: - pipelineCoverageFailureAsset flag

    @Test("pipelineCoverageFailureAsset flags above 50% unscored-FN rate")
    func pipelineCoverageFailureAssetFlagsAbove50Percent() {
        // 60s pipelineCoverage + 40s classifierRecall = 100s GT; 60/100 = 0.6 > 0.5.
        let ws1 = [Self.score(start: 60, end: 100, confidence: 0.2)]
        let gt1 = [
            NarlTimeRange(start: 0, end: 60),
            NarlTimeRange(start: 60, end: 100),
        ]
        let trace1 = makeTrace(episodeDuration: 200, windowScores: ws1)
        let result1 = NarlCoverageMetricsCompute.compute(
            trace: trace1, predicted: [], groundTruth: gt1
        )
        #expect(result1.metrics.pipelineCoverageFailureAsset == true)

        // 40s pipelineCoverage + 60s classifierRecall = 100s GT; 40/100 = 0.4 ≤ 0.5.
        let ws2 = [Self.score(start: 40, end: 100, confidence: 0.2)]
        let gt2 = [
            NarlTimeRange(start: 0, end: 40),
            NarlTimeRange(start: 40, end: 100),
        ]
        let trace2 = makeTrace(episodeDuration: 200, windowScores: ws2)
        let result2 = NarlCoverageMetricsCompute.compute(
            trace: trace2, predicted: [], groundTruth: gt2
        )
        #expect(result2.metrics.pipelineCoverageFailureAsset == false)
    }

    // MARK: - Auto-skip precision / recall / IoU

    @Test("autoSkip precision and recall compute on partial overlap")
    func autoSkipPrecisionAndRecall() {
        // predicted=[10,20], GT=[15,25]: overlap=5, pred=10, gt=10.
        let predicted = [NarlTimeRange(start: 10, end: 20)]
        let gt = [NarlTimeRange(start: 15, end: 25)]
        let trace = makeTrace(episodeDuration: 30)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: predicted, groundTruth: gt
        )
        #expect(abs(result.metrics.autoSkipPrecision - 0.5) < 1e-9)
        #expect(abs(result.metrics.autoSkipRecall - 0.5) < 1e-9)
    }

    @Test("segmentIoU computes set-based IoU")
    func segmentIoUComputesSetBasedIoU() {
        // predicted=[0,50], GT=[0,100]: intersection=50, union=100 → 0.5.
        let predicted = [NarlTimeRange(start: 0, end: 50)]
        let gt = [NarlTimeRange(start: 0, end: 100)]
        let trace = makeTrace(episodeDuration: 100)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: predicted, groundTruth: gt
        )
        #expect(abs(result.metrics.segmentIoU - 0.5) < 1e-9)
    }

    // MARK: - Integration-shape test mimicking the 2026-04-23 DF5C1832 capture

    @Test("2026-04-23 DF5C1832 shape: late-episode GT span is pipelineCoverage")
    func shape2026_04_23DF5C1832IsPipelineCoverage() {
        // WindowScores cover only the first ~90s; GT span sits at ~1550-1621.
        // Expected: that span classifies as pipelineCoverage and the asset
        // flag fires (100% of GT is unscored).
        let ws = (0..<3).map { i in
            Self.score(
                start: Double(i) * 30,
                end: Double(i + 1) * 30,
                confidence: 0.5
            )
        }
        let gt = [NarlTimeRange(start: 1550, end: 1621)]
        let trace = makeTrace(episodeDuration: 3600, windowScores: ws)
        let result = NarlCoverageMetricsCompute.compute(
            trace: trace, predicted: [], groundTruth: gt
        )
        #expect(result.fnDecomposition.count == 1)
        #expect(result.fnDecomposition[0].kind == .pipelineCoverage)
        #expect(result.metrics.pipelineCoverageFailureAsset == true)
    }

    // MARK: - Test helpers

    static func makeTrace(
        episodeDuration: Double,
        windowScores: [FrozenTrace.FrozenWindowScore] = [],
        evidenceCatalog: [FrozenTrace.FrozenEvidenceEntry] = []
    ) -> FrozenTrace {
        FrozenTrace(
            episodeId: "ep-test",
            podcastId: "test",
            episodeDuration: episodeDuration,
            traceVersion: "frozen-trace-v2",
            capturedAt: Date(timeIntervalSince1970: 0),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: evidenceCatalog,
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training,
            windowScores: windowScores
        )
    }

    static func score(
        start: Double,
        end: Double,
        confidence: Double
    ) -> FrozenTrace.FrozenWindowScore {
        FrozenTrace.FrozenWindowScore(
            windowStart: start,
            windowEnd: end,
            fusedSkipConfidence: confidence,
            classificationTrust: 0,
            hasMetadataEvidence: false,
            isAdUnderDefault: false
        )
    }

    private func makeTrace(
        episodeDuration: Double,
        windowScores: [FrozenTrace.FrozenWindowScore] = [],
        evidenceCatalog: [FrozenTrace.FrozenEvidenceEntry] = []
    ) -> FrozenTrace {
        Self.makeTrace(
            episodeDuration: episodeDuration,
            windowScores: windowScores,
            evidenceCatalog: evidenceCatalog
        )
    }
}
