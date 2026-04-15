// CounterfactualEvaluatorTests.swift
// Tests for the counterfactual evaluator: FrozenTrace persistence,
// counterfactual comparison, metrics computation, and holdout infrastructure.

import XCTest
@testable import Playhead

// MARK: - FrozenTrace Tests

final class FrozenTraceTests: XCTestCase {

    // MARK: - Helpers

    private func makeSampleTrace(
        episodeId: String = "ep-001",
        holdout: Bool = false
    ) -> FrozenTrace {
        let featureWindows: [FrozenTrace.FrozenFeatureWindow] = [
            .init(startTime: 0, endTime: 1, rms: 0.3, spectralFlux: 0.1, musicProbability: 0.05),
            .init(startTime: 1, endTime: 2, rms: 0.8, spectralFlux: 0.4, musicProbability: 0.7),
        ]
        let atoms: [FrozenTrace.FrozenAtom] = [
            .init(startTime: 0, endTime: 10, text: "welcome back to the show"),
            .init(startTime: 120, endTime: 130, text: "this episode is brought to you by acme"),
        ]
        let evidence: [FrozenTrace.FrozenEvidenceEntry] = [
            .init(source: "lexical", weight: 0.6, windowStart: 120, windowEnd: 130),
            .init(source: "fm", weight: 0.8, windowStart: 120, windowEnd: 130),
        ]
        let corrections: [FrozenTrace.FrozenCorrection] = [
            .init(source: "listenRevert", scope: "exactSpan:ep-001:120:180", createdAt: 1000),
        ]
        let decisions: [FrozenTrace.FrozenDecisionEvent] = [
            .init(
                windowId: "w1",
                proposalConfidence: 0.85,
                skipConfidence: 0.9,
                eligibilityGate: "eligible",
                policyAction: "skip",
                explanationJSON: "{\"dominant_source\":\"fm\",\"margin\":0.3}"
            ),
        ]
        let baselineSpans: [SpanDecision] = [
            .init(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "baseline"),
        ]
        return FrozenTrace(
            episodeId: episodeId,
            podcastId: "podcast-001",
            episodeDuration: 3600,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000),
            featureWindows: featureWindows,
            atoms: atoms,
            evidenceCatalog: evidence,
            corrections: corrections,
            decisionEvents: decisions,
            baselineSpanDecisions: baselineSpans,
            holdoutDesignation: holdout ? .holdout : .training
        )
    }

    // MARK: - Codable round-trip

    func testFrozenTraceCodableRoundTrip() throws {
        let trace = makeSampleTrace()
        let encoder = JSONEncoder()
        let data = try encoder.encode(trace)
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(FrozenTrace.self, from: data)

        XCTAssertEqual(decoded.episodeId, trace.episodeId)
        XCTAssertEqual(decoded.podcastId, trace.podcastId)
        XCTAssertEqual(decoded.episodeDuration, trace.episodeDuration)
        XCTAssertEqual(decoded.traceVersion, FrozenTrace.currentTraceVersion)
        XCTAssertEqual(decoded.featureWindows.count, 2)
        XCTAssertEqual(decoded.atoms.count, 2)
        XCTAssertEqual(decoded.evidenceCatalog.count, 2)
        XCTAssertEqual(decoded.corrections.count, 1)
        XCTAssertEqual(decoded.decisionEvents.count, 1)
        XCTAssertEqual(decoded.baselineSpanDecisions.count, 1)
        XCTAssertEqual(decoded.holdoutDesignation, .training)
    }

    func testFrozenTraceHoldoutDesignationEncoding() throws {
        let holdoutTrace = makeSampleTrace(holdout: true)
        let data = try JSONEncoder().encode(holdoutTrace)
        let decoded = try JSONDecoder().decode(FrozenTrace.self, from: data)
        XCTAssertEqual(decoded.holdoutDesignation, .holdout)
    }

    func testFrozenTracePreservesExplanationJSON() throws {
        let trace = makeSampleTrace()
        let data = try JSONEncoder().encode(trace)
        let decoded = try JSONDecoder().decode(FrozenTrace.self, from: data)
        let event = decoded.decisionEvents[0]
        XCTAssertEqual(event.explanationJSON, "{\"dominant_source\":\"fm\",\"margin\":0.3}")
    }

    func testSpanDecisionCodableRoundTrip() throws {
        let span = SpanDecision(startTime: 10, endTime: 20, confidence: 0.75, isAd: true, sourceTag: "test")
        let data = try JSONEncoder().encode(span)
        let decoded = try JSONDecoder().decode(SpanDecision.self, from: data)
        XCTAssertEqual(decoded.startTime, 10)
        XCTAssertEqual(decoded.endTime, 20)
        XCTAssertEqual(decoded.confidence, 0.75)
        XCTAssertTrue(decoded.isAd)
        XCTAssertEqual(decoded.sourceTag, "test")
    }

    func testFrozenTraceEmptyCollectionsRoundTrip() throws {
        let trace = FrozenTrace(
            episodeId: "empty",
            podcastId: "p",
            episodeDuration: 100,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineSpanDecisions: [],
            holdoutDesignation: .training
        )
        let data = try JSONEncoder().encode(trace)
        let decoded = try JSONDecoder().decode(FrozenTrace.self, from: data)
        XCTAssertTrue(decoded.featureWindows.isEmpty)
        XCTAssertTrue(decoded.atoms.isEmpty)
        XCTAssertTrue(decoded.baselineSpanDecisions.isEmpty)
    }
}

// MARK: - Counterfactual Evaluator Tests

final class CounterfactualEvaluatorTests: XCTestCase {

    // MARK: - Helpers

    private func makeTrace(
        baselineSpans: [SpanDecision],
        holdout: Bool = false
    ) -> FrozenTrace {
        FrozenTrace(
            episodeId: "ep-cf",
            podcastId: "pod-cf",
            episodeDuration: 3600,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [
                .init(source: "fm", weight: 0.8, windowStart: 120, windowEnd: 130),
                .init(source: "lexical", weight: 0.5, windowStart: 120, windowEnd: 130),
            ],
            corrections: [],
            decisionEvents: [
                .init(
                    windowId: "w1",
                    proposalConfidence: 0.85,
                    skipConfidence: 0.9,
                    eligibilityGate: "eligible",
                    policyAction: "skip",
                    explanationJSON: nil
                ),
            ],
            baselineSpanDecisions: baselineSpans,
            holdoutDesignation: holdout ? .holdout : .training
        )
    }

    // MARK: - Counterfactual comparison

    func testCounterfactualIdenticalDecisionsProduceZeroRegret() {
        let baseline = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "baseline"),
        ]
        let trace = makeTrace(baselineSpans: baseline)
        // New pipeline returns identical decisions
        let newDecisions = baseline.map { SpanDecision(startTime: $0.startTime, endTime: $0.endTime, confidence: $0.confidence, isAd: $0.isAd, sourceTag: "new") }

        let result = CounterfactualEvaluator.compare(trace: trace, newDecisions: newDecisions)
        XCTAssertEqual(result.metrics.counterfactualRegret, 0, accuracy: 0.001)
        XCTAssertEqual(result.metrics.shadowLiveDisagreementRate, 0, accuracy: 0.001)
    }

    func testCounterfactualDivergentDecisionsProduceNonzeroRegret() {
        let baseline = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "baseline"),
        ]
        let trace = makeTrace(baselineSpans: baseline)
        // New pipeline says NOT an ad
        let newDecisions = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.3, isAd: false, sourceTag: "new"),
        ]

        let result = CounterfactualEvaluator.compare(trace: trace, newDecisions: newDecisions)
        XCTAssertGreaterThan(result.metrics.counterfactualRegret, 0)
        XCTAssertEqual(result.metrics.shadowLiveDisagreementRate, 1.0, accuracy: 0.001)
    }

    func testCounterfactualScoreDistributionShift() {
        let baseline = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "baseline"),
            SpanDecision(startTime: 300, endTime: 360, confidence: 0.8, isAd: true, sourceTag: "baseline"),
        ]
        let trace = makeTrace(baselineSpans: baseline)
        let newDecisions = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.5, isAd: true, sourceTag: "new"),
            SpanDecision(startTime: 300, endTime: 360, confidence: 0.4, isAd: false, sourceTag: "new"),
        ]

        let result = CounterfactualEvaluator.compare(trace: trace, newDecisions: newDecisions)
        // Mean confidence dropped from 0.85 to 0.45 — shift should be -0.4
        XCTAssertEqual(result.metrics.scoreDistributionShift, -0.4, accuracy: 0.01)
    }

    func testCounterfactualEmptyDecisions() {
        let trace = makeTrace(baselineSpans: [])
        let result = CounterfactualEvaluator.compare(trace: trace, newDecisions: [])
        XCTAssertEqual(result.metrics.counterfactualRegret, 0, accuracy: 0.001)
        XCTAssertEqual(result.metrics.shadowLiveDisagreementRate, 0, accuracy: 0.001)
        XCTAssertEqual(result.metrics.scoreDistributionShift, 0, accuracy: 0.001)
    }

    func testCounterfactualPerSourceCalibrationError() {
        let baseline = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "baseline"),
        ]
        let trace = FrozenTrace(
            episodeId: "ep-cal",
            podcastId: "pod-cal",
            episodeDuration: 3600,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [
                .init(source: "fm", weight: 0.9, windowStart: 120, windowEnd: 130),
                .init(source: "lexical", weight: 0.3, windowStart: 120, windowEnd: 130),
            ],
            corrections: [],
            decisionEvents: [],
            baselineSpanDecisions: baseline,
            holdoutDesignation: .training
        )

        let newDecisions = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "new"),
        ]

        let result = CounterfactualEvaluator.compare(trace: trace, newDecisions: newDecisions)
        // Per-source calibration: evidence weights vs actual outcome (isAd=true→1.0)
        // fm: Brier = (0.9 - 1.0)^2 = 0.01
        // lexical: Brier = (0.3 - 1.0)^2 = 0.49
        let fmCal = result.metrics.perSourceCalibrationError["fm"]
        let lexCal = result.metrics.perSourceCalibrationError["lexical"]
        XCTAssertNotNil(fmCal)
        XCTAssertNotNil(lexCal)
        XCTAssertEqual(fmCal!, 0.01, accuracy: 0.001)
        XCTAssertEqual(lexCal!, 0.49, accuracy: 0.001)
    }

    func testCounterfactualBaselineHasMoreSpansThanNew() {
        // Baseline has 2 spans, new pipeline only returns 1.
        // The dropped span should count as a disagreement.
        let baseline = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "baseline"),
            SpanDecision(startTime: 300, endTime: 360, confidence: 0.8, isAd: true, sourceTag: "baseline"),
        ]
        let trace = makeTrace(baselineSpans: baseline)
        let newDecisions = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "new"),
        ]

        let result = CounterfactualEvaluator.compare(trace: trace, newDecisions: newDecisions)
        // 2 total spans: 1 matched (agree), 1 unmatched baseline (disagree because isAd=true was dropped)
        XCTAssertEqual(result.diffs.count, 2)
        XCTAssertFalse(result.diffs[0].decisionFlipped)
        XCTAssertTrue(result.diffs[1].decisionFlipped)
        XCTAssertEqual(result.metrics.shadowLiveDisagreementRate, 0.5, accuracy: 0.001)
        XCTAssertGreaterThan(result.metrics.counterfactualRegret, 0)
    }

    func testCounterfactualNewHasMoreSpansThanBaseline() {
        // Baseline has 1 span, new pipeline returns 2.
        // The added span should count as a disagreement.
        let baseline = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "baseline"),
        ]
        let trace = makeTrace(baselineSpans: baseline)
        let newDecisions = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "new"),
            SpanDecision(startTime: 300, endTime: 360, confidence: 0.7, isAd: true, sourceTag: "new"),
        ]

        let result = CounterfactualEvaluator.compare(trace: trace, newDecisions: newDecisions)
        // 2 total spans: 1 matched (agree), 1 unmatched new (disagree because isAd=true was added)
        XCTAssertEqual(result.diffs.count, 2)
        XCTAssertFalse(result.diffs[0].decisionFlipped)
        XCTAssertTrue(result.diffs[1].decisionFlipped)
        XCTAssertEqual(result.metrics.shadowLiveDisagreementRate, 0.5, accuracy: 0.001)
    }

    func testCounterfactualBaselineNonEmptyNewEmpty() {
        // Baseline has spans but new pipeline returns nothing -- full disagreement.
        let baseline = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "baseline"),
        ]
        let trace = makeTrace(baselineSpans: baseline)

        let result = CounterfactualEvaluator.compare(trace: trace, newDecisions: [])
        XCTAssertEqual(result.diffs.count, 1)
        XCTAssertTrue(result.diffs[0].decisionFlipped)
        XCTAssertEqual(result.metrics.shadowLiveDisagreementRate, 1.0, accuracy: 0.001)
        XCTAssertGreaterThan(result.metrics.counterfactualRegret, 0)
    }

    func testCounterfactualDiffEntriesArePopulated() {
        let baseline = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "baseline"),
            SpanDecision(startTime: 300, endTime: 360, confidence: 0.7, isAd: true, sourceTag: "baseline"),
        ]
        let trace = makeTrace(baselineSpans: baseline)
        let newDecisions = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "new"),
            SpanDecision(startTime: 300, endTime: 360, confidence: 0.3, isAd: false, sourceTag: "new"),
        ]

        let result = CounterfactualEvaluator.compare(trace: trace, newDecisions: newDecisions)
        XCTAssertEqual(result.diffs.count, 2)
        // First span: same decision
        XCTAssertFalse(result.diffs[0].decisionFlipped)
        // Second span: flipped
        XCTAssertTrue(result.diffs[1].decisionFlipped)
        XCTAssertEqual(result.diffs[1].confidenceDelta, -0.4, accuracy: 0.001)
    }

    func testCounterfactualUnmatchedNonAdNewSpanDoesNotInflateFlippedCount() {
        // Baseline has 1 ad span. New pipeline returns the same span + a second non-ad span.
        // The unmatched non-ad span should NOT inflate flippedCount or regret.
        let baseline = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "baseline"),
        ]
        let trace = makeTrace(baselineSpans: baseline)
        let newDecisions = [
            SpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "new"),
            SpanDecision(startTime: 300, endTime: 360, confidence: 0.4, isAd: false, sourceTag: "new"),
        ]

        let result = CounterfactualEvaluator.compare(trace: trace, newDecisions: newDecisions)
        // totalSpanCount = max(1, 2) = 2; 2 diffs total
        XCTAssertEqual(result.diffs.count, 2)
        // First span: matched, same decision
        XCTAssertFalse(result.diffs[0].decisionFlipped)
        // Second span: unmatched new, but isAd=false so decisionFlipped=false
        XCTAssertFalse(result.diffs[1].decisionFlipped)
        // flippedCount = 0, so disagreementRate = 0/2 = 0
        XCTAssertEqual(result.metrics.shadowLiveDisagreementRate, 0, accuracy: 0.001)
        // regret = 0 (no flipped decisions)
        XCTAssertEqual(result.metrics.counterfactualRegret, 0, accuracy: 0.001)
    }

    func testCounterfactualNewNonAdSpansProduceZeroRegret() {
        // Baseline has 0 spans, new pipeline returns 2 non-ad spans.
        // Non-ad additions should produce zero regret and zero flippedCount.
        let trace = makeTrace(baselineSpans: [])
        let newDecisions = [
            SpanDecision(startTime: 100, endTime: 150, confidence: 0.5, isAd: false, sourceTag: "new"),
            SpanDecision(startTime: 200, endTime: 250, confidence: 0.6, isAd: false, sourceTag: "new"),
        ]

        let result = CounterfactualEvaluator.compare(trace: trace, newDecisions: newDecisions)
        XCTAssertEqual(result.metrics.counterfactualRegret, 0, accuracy: 0.001)
        XCTAssertEqual(result.metrics.shadowLiveDisagreementRate, 0, accuracy: 0.001)
        // Both diffs should show decisionFlipped=false since isAd=false
        XCTAssertEqual(result.diffs.count, 2)
        XCTAssertFalse(result.diffs[0].decisionFlipped)
        XCTAssertFalse(result.diffs[1].decisionFlipped)
    }
}

// MARK: - Holdout Infrastructure Tests

final class HoldoutDesignationTests: XCTestCase {

    func testFilterTrainingOnly() {
        let traces = [
            makeMinimalTrace(id: "t1", holdout: false),
            makeMinimalTrace(id: "t2", holdout: true),
            makeMinimalTrace(id: "t3", holdout: false),
        ]
        let training = TraceCorpus.filterTraining(traces)
        XCTAssertEqual(training.count, 2)
        XCTAssertEqual(training.map(\.episodeId), ["t1", "t3"])
    }

    func testFilterHoldoutOnly() {
        let traces = [
            makeMinimalTrace(id: "t1", holdout: false),
            makeMinimalTrace(id: "t2", holdout: true),
            makeMinimalTrace(id: "t3", holdout: true),
        ]
        let holdout = TraceCorpus.filterHoldout(traces)
        XCTAssertEqual(holdout.count, 2)
        XCTAssertEqual(holdout.map(\.episodeId), ["t2", "t3"])
    }

    func testDesignateHoldoutByFraction() {
        // 10 traces, 30% holdout = 3 holdout
        let traces = (0..<10).map { makeMinimalTrace(id: "ep-\($0)", holdout: false) }
        let designated = TraceCorpus.designateHoldout(traces, fraction: 0.3, seed: 42)
        let holdoutCount = designated.filter { $0.holdoutDesignation == .holdout }.count
        XCTAssertEqual(holdoutCount, 3)
        // Deterministic: same seed → same split
        let designated2 = TraceCorpus.designateHoldout(traces, fraction: 0.3, seed: 42)
        XCTAssertEqual(
            designated.map(\.holdoutDesignation),
            designated2.map(\.holdoutDesignation)
        )
    }

    func testDesignateHoldoutZeroFraction() {
        let traces = (0..<5).map { makeMinimalTrace(id: "ep-\($0)", holdout: false) }
        let designated = TraceCorpus.designateHoldout(traces, fraction: 0.0, seed: 1)
        let holdoutCount = designated.filter { $0.holdoutDesignation == .holdout }.count
        XCTAssertEqual(holdoutCount, 0)
    }

    func testDesignateHoldoutFullFraction() {
        let traces = (0..<5).map { makeMinimalTrace(id: "ep-\($0)", holdout: false) }
        let designated = TraceCorpus.designateHoldout(traces, fraction: 1.0, seed: 1)
        let holdoutCount = designated.filter { $0.holdoutDesignation == .holdout }.count
        XCTAssertEqual(holdoutCount, 5)
    }

    func testDifferentSeedsProduceDifferentSplits() {
        // Verify the hash is seed-dependent: different seeds should produce different orderings.
        // Use 50 traces with distinct IDs to make collision astronomically unlikely.
        let traces = (0..<50).map { makeMinimalTrace(id: "podcast-episode-\($0)-abc", holdout: false) }
        let splitA = TraceCorpus.designateHoldout(traces, fraction: 0.3, seed: 1)
        let splitB = TraceCorpus.designateHoldout(traces, fraction: 0.3, seed: 1_000_000)
        let holdoutIdsA = Set(splitA.filter { $0.holdoutDesignation == .holdout }.map(\.episodeId))
        let holdoutIdsB = Set(splitB.filter { $0.holdoutDesignation == .holdout }.map(\.episodeId))
        XCTAssertNotEqual(holdoutIdsA, holdoutIdsB,
                          "Different seeds must produce different holdout assignments")
    }

    private func makeMinimalTrace(id: String, holdout: Bool) -> FrozenTrace {
        FrozenTrace(
            episodeId: id,
            podcastId: "p",
            episodeDuration: 100,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineSpanDecisions: [],
            holdoutDesignation: holdout ? .holdout : .training
        )
    }
}

// MARK: - CounterfactualMetrics Codable Tests

final class CounterfactualMetricsCodableTests: XCTestCase {

    func testCounterfactualMetricsCodableRoundTrip() throws {
        let metrics = CounterfactualMetrics(
            counterfactualRegret: 0.15,
            scoreDistributionShift: -0.2,
            perSourceCalibrationError: ["fm": 0.01, "lexical": 0.49],
            shadowLiveDisagreementRate: 0.5
        )
        let data = try JSONEncoder().encode(metrics)
        let decoded = try JSONDecoder().decode(CounterfactualMetrics.self, from: data)
        XCTAssertEqual(decoded.counterfactualRegret, 0.15, accuracy: 0.001)
        XCTAssertEqual(decoded.scoreDistributionShift, -0.2, accuracy: 0.001)
        XCTAssertEqual(decoded.perSourceCalibrationError["fm"] ?? -1, 0.01, accuracy: 0.001)
        XCTAssertEqual(decoded.shadowLiveDisagreementRate, 0.5, accuracy: 0.001)
    }

    func testCounterfactualResultCodableRoundTrip() throws {
        let result = CounterfactualResult(
            traceEpisodeId: "ep-001",
            diffs: [
                SpanDecisionDiff(
                    startTime: 120,
                    endTime: 180,
                    baselineConfidence: 0.9,
                    newConfidence: 0.3,
                    baselineIsAd: true,
                    newIsAd: false,
                    confidenceDelta: -0.6,
                    decisionFlipped: true
                ),
            ],
            metrics: CounterfactualMetrics(
                counterfactualRegret: 0.6,
                scoreDistributionShift: -0.6,
                perSourceCalibrationError: [:],
                shadowLiveDisagreementRate: 1.0
            )
        )
        let data = try JSONEncoder().encode(result)
        let decoded = try JSONDecoder().decode(CounterfactualResult.self, from: data)
        XCTAssertEqual(decoded.traceEpisodeId, "ep-001")
        XCTAssertEqual(decoded.diffs.count, 1)
        XCTAssertTrue(decoded.diffs[0].decisionFlipped)
        XCTAssertEqual(decoded.metrics.counterfactualRegret, 0.6, accuracy: 0.001)
    }
}

// MARK: - EpisodeReplayReport Counterfactual Extension Tests

final class EpisodeReplayReportCounterfactualTests: XCTestCase {

    func testReplayReportWithCounterfactualRoundTrip() throws {
        let report = EpisodeReplayReport(
            episodeId: "ep-001",
            episodeTitle: "Test Episode",
            condition: SimulationCondition(
                audioMode: .cached,
                playbackSpeed: 1.0,
                interactions: []
            ),
            detectionQuality: DetectionQualityMetrics(
                falsePositiveSkipSeconds: 0,
                falseNegativeAdSeconds: 0,
                precision: 1.0,
                recall: 1.0,
                f1Score: 1.0,
                missedSegmentCount: 0,
                spuriousSegmentCount: 0
            ),
            boundaryQuality: BoundaryQualityMetrics(),
            latency: LatencyMetrics(),
            userOverrides: UserOverrideMetrics(
                listenTapCount: 0,
                rewindAfterSkipCount: 0,
                overrideRate: 0
            ),
            samples: [],
            simulatorVersion: EpisodeReplayReport.currentSimulatorVersion,
            generatedAt: Date(),
            replayDurationSeconds: 10,
            counterfactualResult: CounterfactualResult(
                traceEpisodeId: "ep-001",
                diffs: [],
                metrics: CounterfactualMetrics(
                    counterfactualRegret: 0,
                    scoreDistributionShift: 0,
                    perSourceCalibrationError: [:],
                    shadowLiveDisagreementRate: 0
                )
            )
        )

        let data = try JSONEncoder().encode(report)
        let decoded = try JSONDecoder().decode(EpisodeReplayReport.self, from: data)
        XCTAssertNotNil(decoded.counterfactualResult)
        XCTAssertEqual(decoded.counterfactualResult?.traceEpisodeId, "ep-001")
    }

    func testReplayReportWithoutCounterfactualStillDecodes() throws {
        // Backward compatibility: existing reports without counterfactual field
        let report = EpisodeReplayReport(
            episodeId: "ep-old",
            episodeTitle: "Old Episode",
            condition: SimulationCondition(
                audioMode: .cached,
                playbackSpeed: 1.0,
                interactions: []
            ),
            detectionQuality: DetectionQualityMetrics(
                falsePositiveSkipSeconds: 0,
                falseNegativeAdSeconds: 0,
                precision: 1.0,
                recall: 1.0,
                f1Score: 1.0,
                missedSegmentCount: 0,
                spuriousSegmentCount: 0
            ),
            boundaryQuality: BoundaryQualityMetrics(),
            latency: LatencyMetrics(),
            userOverrides: UserOverrideMetrics(
                listenTapCount: 0,
                rewindAfterSkipCount: 0,
                overrideRate: 0
            ),
            samples: [],
            simulatorVersion: EpisodeReplayReport.currentSimulatorVersion,
            generatedAt: Date(),
            replayDurationSeconds: 10
        )

        let data = try JSONEncoder().encode(report)
        let decoded = try JSONDecoder().decode(EpisodeReplayReport.self, from: data)
        XCTAssertNil(decoded.counterfactualResult)
    }
}
