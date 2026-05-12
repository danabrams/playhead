import Foundation
import Testing
@testable import Playhead

@Suite("ReplayHarness")
struct ReplayHarnessTests {
    @Test("Perfect deterministic replay passes and reports ideal metrics")
    func perfectReplayPasses() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-perfect",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 60, endTime: 120, verdict: .paidPromotion),
                        .init(id: "house-1", startTime: 600, endTime: 630, verdict: .housePromo),
                        .init(id: "brand-mention", startTime: 900, endTime: 910, verdict: .editorialMention),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-1", startTime: 60, endTime: 120),
                    .init(id: "pred-2", startTime: 600, endTime: 630),
                ]
            ],
            thresholds: Self.strictThresholds
        )

        #expect(evaluation.passed)
        #expect(evaluation.reasons.isEmpty)
        #expect(evaluation.metrics.truePositiveSpans == 2)
        #expect(evaluation.metrics.falsePositiveSpans == 0)
        #expect(evaluation.metrics.falseNegativeSpans == 0)
        #expect(evaluation.metrics.spanPrecision == 1)
        #expect(evaluation.metrics.spanRecall == 1)
        #expect(evaluation.metrics.falsePositiveSecondsPerHour == 0)
        #expect(evaluation.metrics.averageBoundaryError == 0)
        #expect(evaluation.metrics.maximumBoundaryError == 0)
    }

    @Test("Replay computes span precision, recall, FP seconds per hour, and boundary error")
    func computesPromotionMetrics() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-mixed",
                    duration: 1800,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 100, endTime: 160, verdict: .paidPromotion),
                        .init(id: "ad-2", startTime: 500, endTime: 560, verdict: .paidPromotion),
                        .init(id: "editorial", startTime: 700, endTime: 720, verdict: .editorialMention),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-1", startTime: 98, endTime: 163),
                    .init(id: "pred-fp", startTime: 700, endTime: 710),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 0.1,
                minimumSpanRecall: 0.1,
                maximumFalsePositiveSecondsPerHour: 100,
                maximumAverageBoundaryError: 10,
                maximumBoundaryError: 10
            )
        )

        #expect(evaluation.metrics.truePositiveSpans == 1)
        #expect(evaluation.metrics.falsePositiveSpans == 1)
        #expect(evaluation.metrics.falseNegativeSpans == 1)
        #expect(evaluation.metrics.spanPrecision == 0.5)
        #expect(evaluation.metrics.spanRecall == 0.5)
        #expect(abs(evaluation.metrics.falsePositiveSecondsPerHour - 30) < 0.0001)
        #expect(abs(evaluation.metrics.averageBoundaryError - 2.5) < 0.0001)
        #expect(evaluation.metrics.maximumBoundaryError == 3)
    }

    @Test("Threshold regressions block promotion and report all failing reasons")
    func thresholdRegressionsBlockPromotion() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-regression",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 100, endTime: 160, verdict: .paidPromotion),
                        .init(id: "ad-2", startTime: 500, endTime: 560, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-wide", startTime: 70, endTime: 190),
                    .init(id: "pred-fp", startTime: 800, endTime: 830),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 0.75,
                minimumSpanRecall: 0.75,
                maximumFalsePositiveSecondsPerHour: 20,
                maximumAverageBoundaryError: 10,
                maximumBoundaryError: 20
            )
        )

        #expect(!evaluation.passed)
        #expect(evaluation.reasons.count == 5)
        #expect(evaluation.reasons.contains { $0.contains("span precision") })
        #expect(evaluation.reasons.contains { $0.contains("span recall") })
        #expect(evaluation.reasons.contains { $0.contains("false-positive seconds/hour") })
        #expect(evaluation.reasons.contains { $0.contains("average boundary error") })
        #expect(evaluation.reasons.contains { $0.contains("maximum boundary error") })
    }

    @Test("Overlapping predictions do not double count false-positive seconds")
    func overlappingPredictionsDoNotDoubleCountFPSeconds() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-overlap",
                    duration: 3600,
                    labeledSpans: []
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "fp-1", startTime: 10, endTime: 40),
                    .init(id: "fp-2", startTime: 20, endTime: 50),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 0,
                minimumSpanRecall: 0,
                maximumFalsePositiveSecondsPerHour: 60,
                maximumAverageBoundaryError: 0,
                maximumBoundaryError: 0
            )
        )

        #expect(evaluation.metrics.falsePositiveSecondsPerHour == 40)
    }

    @Test("All-negative episodes retain perfect recall while precision and FP seconds catch predictions")
    func allNegativeEpisodesRetainPerfectRecall() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-all-negative",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "editorial", startTime: 100, endTime: 130, verdict: .editorialMention),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "fp-1", startTime: 100, endTime: 130),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 0,
                minimumSpanRecall: 1,
                maximumFalsePositiveSecondsPerHour: 30,
                maximumAverageBoundaryError: 0,
                maximumBoundaryError: 0
            )
        )

        #expect(evaluation.metrics.truePositiveSpans == 0)
        #expect(evaluation.metrics.falsePositiveSpans == 1)
        #expect(evaluation.metrics.falseNegativeSpans == 0)
        #expect(evaluation.metrics.spanPrecision == 0)
        #expect(evaluation.metrics.spanRecall == 1)
        #expect(evaluation.metrics.falsePositiveSecondsPerHour == 30)
        #expect(evaluation.passed)
    }

    @Test("False-positive seconds are clipped to the episode timeline")
    func falsePositiveSecondsAreClippedToEpisodeTimeline() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-clipped",
                    duration: 100,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 80, endTime: 100, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "before-start", startTime: -20, endTime: 10),
                    .init(id: "after-end", startTime: 90, endTime: 130),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 0,
                minimumSpanRecall: 0,
                maximumFalsePositiveSecondsPerHour: 400,
                maximumAverageBoundaryError: 100,
                maximumBoundaryError: 100,
                matchingIoUThreshold: 0.1
            )
        )

        #expect(evaluation.metrics.truePositiveSpans == 1)
        #expect(evaluation.metrics.falsePositiveSpans == 1)
        #expect(evaluation.metrics.falsePositiveSecondsPerHour == 360)
    }

    @Test("Prediction span matching is clipped to the episode timeline")
    func predictionSpanMatchingIsClippedToEpisodeTimeline() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-match-clipped",
                    duration: 100,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 80, endTime: 100, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "inside-tail", startTime: 90, endTime: 130),
                    .init(id: "outside-only", startTime: 100, endTime: 130),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 1,
                minimumSpanRecall: 1,
                maximumFalsePositiveSecondsPerHour: 0,
                maximumAverageBoundaryError: 5,
                maximumBoundaryError: 10,
                matchingIoUThreshold: 0.5
            )
        )

        #expect(evaluation.passed)
        #expect(evaluation.metrics.truePositiveSpans == 1)
        #expect(evaluation.metrics.falsePositiveSpans == 0)
        #expect(evaluation.metrics.falseNegativeSpans == 0)
        #expect(evaluation.metrics.averageBoundaryError == 5)
        #expect(evaluation.metrics.maximumBoundaryError == 10)
    }

    @Test("Labeled spans outside the episode timeline fail closed")
    func labeledSpansOutsideEpisodeTimelineFailClosed() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-outside-label",
                    duration: 100,
                    labeledSpans: [
                        .init(id: "outside-label", startTime: 120, endTime: 140, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [[]],
            thresholds: Self.strictThresholds
        )

        #expect(!evaluation.passed)
        #expect(evaluation.reasons.contains { $0.contains("labeled span outside-label time range must be within the episode timeline") })
    }

    @Test("Partially out-of-window labeled spans fail closed without being clipped into matches")
    func partiallyOutOfWindowLabeledSpansFailClosedWithoutMatching() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-partial-labels",
                    duration: 100,
                    labeledSpans: [
                        .init(id: "starts-before", startTime: -10, endTime: 10, verdict: .paidPromotion),
                        .init(id: "ends-after", startTime: 90, endTime: 110, verdict: .paidPromotion),
                        .init(id: "valid-label", startTime: 40, endTime: 50, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-start", startTime: 0, endTime: 10),
                    .init(id: "pred-end", startTime: 90, endTime: 100),
                    .init(id: "pred-valid", startTime: 40, endTime: 50),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 0,
                minimumSpanRecall: 1,
                maximumFalsePositiveSecondsPerHour: 1_000,
                maximumAverageBoundaryError: 0,
                maximumBoundaryError: 0
            )
        )

        #expect(!evaluation.passed)
        #expect(evaluation.metrics.truePositiveSpans == 1)
        #expect(evaluation.metrics.falsePositiveSpans == 2)
        #expect(evaluation.metrics.falseNegativeSpans == 0)
        #expect(evaluation.reasons.contains { $0.contains("labeled span starts-before time range must be within the episode timeline") })
        #expect(evaluation.reasons.contains { $0.contains("labeled span ends-after time range must be within the episode timeline") })
    }

    @Test("Zero-duration fixture spans fail closed without skewing valid span metrics")
    func zeroDurationFixtureSpansFailClosedWithoutSkewingValidSpanMetrics() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-degenerate",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "zero-truth", startTime: 50, endTime: 50, verdict: .paidPromotion),
                        .init(id: "ad-1", startTime: 100, endTime: 160, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "zero-prediction", startTime: 25, endTime: 25),
                    .init(id: "pred-1", startTime: 100, endTime: 160),
                ]
            ],
            thresholds: Self.strictThresholds
        )

        #expect(!evaluation.passed)
        #expect(evaluation.metrics.truePositiveSpans == 1)
        #expect(evaluation.metrics.falsePositiveSpans == 0)
        #expect(evaluation.metrics.falseNegativeSpans == 0)
        #expect(evaluation.metrics.spanPrecision == 1)
        #expect(evaluation.metrics.spanRecall == 1)
        #expect(evaluation.reasons.contains { $0.contains("labeled span zero-truth time range must be greater than zero") })
        #expect(evaluation.reasons.contains { $0.contains("predicted span zero-prediction time range must be greater than zero") })
    }

    @Test("Matching maximizes valid span pairs before computing precision and recall")
    func matchingMaximizesValidSpanPairs() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-cross-match",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 0, endTime: 100, verdict: .paidPromotion),
                        .init(id: "ad-2", startTime: 100, endTime: 200, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-wide", startTime: 0, endTime: 200),
                    .init(id: "pred-ad-1", startTime: 0, endTime: 100),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 1,
                minimumSpanRecall: 1,
                maximumFalsePositiveSecondsPerHour: 0,
                maximumAverageBoundaryError: 50,
                maximumBoundaryError: 100,
                matchingIoUThreshold: 0.5
            )
        )

        #expect(evaluation.metrics.truePositiveSpans == 2)
        #expect(evaluation.metrics.falsePositiveSpans == 0)
        #expect(evaluation.metrics.falseNegativeSpans == 0)
        #expect(evaluation.metrics.spanPrecision == 1)
        #expect(evaluation.metrics.spanRecall == 1)
    }

    @Test("Matching cardinality still wins for very large boundary penalties")
    func matchingCardinalityWinsForVeryLargeBoundaryPenalties() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-large-boundary",
                    duration: 2_000_000_000,
                    labeledSpans: [
                        .init(id: "ad-large", startTime: 0, endTime: 1_000_000_000, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-large", startTime: 0, endTime: 1_500_000_000),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 1,
                minimumSpanRecall: 1,
                maximumFalsePositiveSecondsPerHour: 1_000,
                maximumAverageBoundaryError: 250_000_000,
                maximumBoundaryError: 500_000_000,
                matchingIoUThreshold: 0.5
            )
        )

        #expect(evaluation.passed)
        #expect(evaluation.metrics.truePositiveSpans == 1)
        #expect(evaluation.metrics.falsePositiveSpans == 0)
        #expect(evaluation.metrics.falseNegativeSpans == 0)
    }

    @Test("Matching maximizes cardinality even when reassignment has large boundary cost")
    func matchingReassignsExpensivePairsToMaximizeCardinality() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-expensive-reassignment",
                    duration: 3_000_000_000,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 1_000_000_000, endTime: 2_000_000_000, verdict: .paidPromotion),
                        .init(id: "ad-2", startTime: 1_999_999_999, endTime: 2_999_999_999, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-exact-ad-1", startTime: 1_000_000_000, endTime: 2_000_000_000),
                    .init(id: "pred-barely-ad-1", startTime: 0, endTime: 1_000_000_001),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 1,
                minimumSpanRecall: 1,
                maximumFalsePositiveSecondsPerHour: 2_000,
                maximumAverageBoundaryError: 1_000_000_000,
                maximumBoundaryError: 1_000_000_000,
                matchingIoUThreshold: 0
            )
        )

        #expect(evaluation.passed)
        #expect(evaluation.metrics.truePositiveSpans == 2)
        #expect(evaluation.metrics.falsePositiveSpans == 0)
        #expect(evaluation.metrics.falseNegativeSpans == 0)
    }

    @Test("Matching chooses the lowest-boundary assignment among valid competing predictions")
    func matchingOptimizesBoundaryErrorForCompetingPredictions() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-competing-boundaries",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 100, endTime: 160, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-wide", startTime: 92, endTime: 166),
                    .init(id: "pred-exact", startTime: 100, endTime: 160),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 0.5,
                minimumSpanRecall: 1,
                maximumFalsePositiveSecondsPerHour: 20,
                maximumAverageBoundaryError: 0,
                maximumBoundaryError: 0
            )
        )

        #expect(evaluation.passed)
        #expect(evaluation.metrics.truePositiveSpans == 1)
        #expect(evaluation.metrics.falsePositiveSpans == 1)
        #expect(evaluation.metrics.falseNegativeSpans == 0)
        #expect(evaluation.metrics.averageBoundaryError == 0)
        #expect(evaluation.metrics.maximumBoundaryError == 0)
    }

    @Test("Matching uses IoU as deterministic tie-breaker after boundary error")
    func matchingUsesIoUAsTieBreakerAfterBoundaryError() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-iou-tie-break",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 100, endTime: 200, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-lower-iou", startTime: 50, endTime: 150),
                    .init(id: "pred-higher-iou", startTime: 0, endTime: 200),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 0.5,
                minimumSpanRecall: 1,
                maximumFalsePositiveSecondsPerHour: 200,
                maximumAverageBoundaryError: 25,
                maximumBoundaryError: 100,
                matchingIoUThreshold: 0.1
            )
        )

        #expect(evaluation.metrics.truePositiveSpans == 1)
        #expect(evaluation.metrics.averageBoundaryError == 50)
        #expect(evaluation.metrics.maximumBoundaryError == 100)
    }

    @Test("Matching prioritizes lower boundary error before IoU tie-breaks")
    func matchingPrioritizesBoundaryErrorBeforeIoUTieBreaks() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-boundary-before-iou",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 100, endTime: 101, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-lower-boundary", startTime: 99.9, endTime: 100.9),
                    .init(id: "pred-higher-iou", startTime: 100, endTime: 101.201),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 0.5,
                minimumSpanRecall: 1,
                maximumFalsePositiveSecondsPerHour: 2,
                maximumAverageBoundaryError: 0.1,
                maximumBoundaryError: 0.1,
                matchingIoUThreshold: 0.5
            )
        )

        #expect(evaluation.passed)
        #expect(evaluation.metrics.truePositiveSpans == 1)
        #expect(abs(evaluation.metrics.averageBoundaryError - 0.1) < 0.0001)
        #expect(abs(evaluation.metrics.maximumBoundaryError - 0.1) < 0.0001)
    }

    @Test("Matching never pairs disjoint spans when threshold allows zero IoU")
    func matchingDoesNotPairDisjointSpansAtZeroThreshold() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-disjoint-zero-threshold",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 100, endTime: 160, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-disjoint", startTime: 300, endTime: 360),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 0,
                minimumSpanRecall: 0,
                maximumFalsePositiveSecondsPerHour: 60,
                maximumAverageBoundaryError: 500,
                maximumBoundaryError: 500,
                matchingIoUThreshold: 0
            )
        )

        #expect(evaluation.metrics.truePositiveSpans == 0)
        #expect(evaluation.metrics.falsePositiveSpans == 1)
        #expect(evaluation.metrics.falseNegativeSpans == 1)
        #expect(evaluation.metrics.falsePositiveSecondsPerHour == 60)
    }

    @Test("Matching keeps duplicate episode identifiers isolated")
    func matchingKeepsDuplicateEpisodeIdentifiersIsolated() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "duplicate-id",
                    duration: 300,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 100, endTime: 160, verdict: .paidPromotion),
                    ]
                ),
                ReplayHarness.BenchmarkEpisode(
                    id: "duplicate-id",
                    duration: 300,
                    labeledSpans: [
                        .init(id: "editorial", startTime: 100, endTime: 160, verdict: .editorialMention),
                    ]
                ),
            ],
            predictionsByEpisode: [
                [],
                [
                    .init(id: "pred-other-episode", startTime: 100, endTime: 160),
                ],
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: 1,
                minimumSpanRecall: 1,
                maximumFalsePositiveSecondsPerHour: 360,
                maximumAverageBoundaryError: 0,
                maximumBoundaryError: 0
            )
        )

        #expect(!evaluation.passed)
        #expect(evaluation.metrics.truePositiveSpans == 0)
        #expect(evaluation.metrics.falsePositiveSpans == 1)
        #expect(evaluation.metrics.falseNegativeSpans == 1)
        #expect(evaluation.metrics.falsePositiveSecondsPerHour == 360)
    }

    @Test("Invalid gate thresholds fail closed")
    func invalidGateThresholdsFailClosed() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-invalid-thresholds",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 100, endTime: 160, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-1", startTime: 100, endTime: 160),
                ]
            ],
            thresholds: ReplayHarness.GateThresholds(
                minimumSpanPrecision: .nan,
                minimumSpanRecall: 1,
                maximumFalsePositiveSecondsPerHour: 0,
                maximumAverageBoundaryError: 0,
                maximumBoundaryError: 0,
                matchingIoUThreshold: 0.5
            )
        )

        #expect(!evaluation.passed)
        #expect(evaluation.reasons.contains { $0.contains("minimum span precision threshold") })
    }

    @Test("Invalid benchmark span time ranges fail closed")
    func invalidBenchmarkSpanTimeRangesFailClosed() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-invalid-spans",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 100, endTime: 160, verdict: .paidPromotion),
                        .init(id: "bad-label", startTime: .nan, endTime: 260, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-1", startTime: 100, endTime: 160),
                    .init(id: "bad-prediction", startTime: 300, endTime: .infinity),
                ]
            ],
            thresholds: Self.strictThresholds
        )

        #expect(!evaluation.passed)
        #expect(evaluation.reasons.contains { $0.contains("labeled span bad-label time range must be finite") })
        #expect(evaluation.reasons.contains { $0.contains("predicted span bad-prediction time range must be finite") })
    }

    @Test("Reversed benchmark span ranges fail closed without skewing valid span metrics")
    func reversedBenchmarkSpanTimeRangesFailClosed() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-reversed-spans",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 100, endTime: 160, verdict: .paidPromotion),
                        .init(id: "reversed-label", startTime: 260, endTime: 200, verdict: .paidPromotion),
                    ]
                )
            ],
            predictionsByEpisode: [
                [
                    .init(id: "pred-1", startTime: 100, endTime: 160),
                    .init(id: "reversed-prediction", startTime: 340, endTime: 300),
                ]
            ],
            thresholds: Self.strictThresholds
        )

        #expect(!evaluation.passed)
        #expect(evaluation.metrics.truePositiveSpans == 1)
        #expect(evaluation.metrics.falsePositiveSpans == 0)
        #expect(evaluation.metrics.falseNegativeSpans == 0)
        #expect(evaluation.metrics.spanPrecision == 1)
        #expect(evaluation.metrics.spanRecall == 1)
        #expect(evaluation.reasons.contains { $0.contains("labeled span reversed-label time range must have start before end") })
        #expect(evaluation.reasons.contains { $0.contains("predicted span reversed-prediction time range must have start before end") })
    }

    @Test("Non-finite benchmark episode duration fails closed without contaminating valid metrics")
    func nonFiniteBenchmarkEpisodeDurationFailsClosed() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-non-finite-duration",
                    duration: .nan,
                    labeledSpans: [
                        .init(id: "invalid-duration-ad", startTime: 100, endTime: 160, verdict: .paidPromotion),
                    ]
                ),
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-valid",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 200, endTime: 260, verdict: .paidPromotion),
                    ]
                ),
            ],
            predictionsByEpisode: [
                [
                    .init(id: "invalid-duration-prediction", startTime: 100, endTime: 160),
                ],
                [
                    .init(id: "pred-1", startTime: 200, endTime: 260),
                ],
            ],
            thresholds: Self.strictThresholds
        )

        #expect(!evaluation.passed)
        #expect(evaluation.metrics.truePositiveSpans == 1)
        #expect(evaluation.metrics.falsePositiveSpans == 0)
        #expect(evaluation.metrics.falseNegativeSpans == 0)
        #expect(evaluation.metrics.spanPrecision == 1)
        #expect(evaluation.metrics.spanRecall == 1)
        #expect(evaluation.metrics.falsePositiveSecondsPerHour == 0)
        #expect(evaluation.reasons.contains { $0.contains("episode-non-finite-duration duration must be finite") })
    }

    @Test("Zero-duration benchmark episode fails closed")
    func zeroDurationBenchmarkEpisodeFailsClosed() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-zero-duration",
                    duration: 0,
                    labeledSpans: []
                )
            ],
            predictionsByEpisode: [[]],
            thresholds: Self.strictThresholds
        )

        #expect(!evaluation.passed)
        #expect(evaluation.reasons.contains { $0.contains("duration must be greater than zero") })
    }

    @Test("Benchmark episodes without labeled spans fail closed")
    func benchmarkEpisodeWithoutLabeledSpansFailsClosed() {
        let evaluation = Self.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-empty-labels",
                    duration: 3600,
                    labeledSpans: []
                )
            ],
            predictionsByEpisode: [[]],
            thresholds: Self.strictThresholds
        )

        #expect(!evaluation.passed)
        #expect(evaluation.metrics.spanPrecision == 1)
        #expect(evaluation.metrics.spanRecall == 1)
        #expect(evaluation.reasons.contains { $0.contains("episode-empty-labels must include at least one labeled span") })
    }

    @Test("Replay provider failures fail closed")
    func replayProviderFailuresFailClosed() {
        let evaluation = ReplayHarness.evaluate(
            episodes: [
                ReplayHarness.BenchmarkEpisode(
                    id: "episode-replay-failure",
                    duration: 3600,
                    labeledSpans: [
                        .init(id: "ad-1", startTime: 100, endTime: 160, verdict: .paidPromotion),
                    ]
                )
            ],
            thresholds: Self.strictThresholds
        ) { _, _ in
            throw ReplayProviderError.synthetic
        }

        #expect(!evaluation.passed)
        #expect(evaluation.metrics.falseNegativeSpans == 1)
        #expect(evaluation.reasons.contains { $0.contains("episode-replay-failure replay failed") })
    }

    @Test("Empty benchmark fails closed instead of promoting without evidence")
    func emptyBenchmarkFailsClosed() {
        let evaluation = Self.evaluate(
            episodes: [],
            predictionsByEpisode: [],
            thresholds: Self.strictThresholds
        )

        #expect(!evaluation.passed)
        #expect(evaluation.metrics.spanPrecision == 1)
        #expect(evaluation.metrics.spanRecall == 1)
        #expect(evaluation.reasons.contains { $0.contains("at least one episode") })
    }

    private enum ReplayProviderError: Error {
        case synthetic
    }

    private static func evaluate(
        episodes: [ReplayHarness.BenchmarkEpisode],
        predictionsByEpisode: [[ReplayHarness.PredictedSpan]],
        thresholds: ReplayHarness.GateThresholds
    ) -> ReplayHarness.Evaluation {
        ReplayHarness.evaluate(episodes: episodes, thresholds: thresholds) { _, episodeIndex in
            guard predictionsByEpisode.indices.contains(episodeIndex) else {
                return []
            }
            return predictionsByEpisode[episodeIndex]
        }
    }

    private static let strictThresholds = ReplayHarness.GateThresholds(
        minimumSpanPrecision: 1,
        minimumSpanRecall: 1,
        maximumFalsePositiveSecondsPerHour: 0,
        maximumAverageBoundaryError: 0,
        maximumBoundaryError: 0
    )
}
