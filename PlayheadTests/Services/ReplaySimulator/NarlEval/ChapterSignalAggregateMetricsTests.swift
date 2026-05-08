// ChapterSignalAggregateMetricsTests.swift
// playhead-au2v.1.19: deterministic synthetic-corpus tests for
// `ChapterSignalAggregateMetrics`. Pin every observable contract:
//   - structural fields (PerModeMetrics / ChapterSignalLiftReport shapes)
//   - precision / recall / F1 math
//   - phase latency p50 / p90 / p99 from the gate's per-episode latency
//   - FM-cost-multiplier semantics (including the zero-baseline clamp)
//   - barMet evaluation under {lift, no-regression, cost} combinations
//   - per-show breakdown determinism
//   - JSON round-trip
//   - persist() filename + payload contract

import Foundation
import Testing
@testable import Playhead

@Suite("ChapterSignalAggregateMetrics")
struct ChapterSignalAggregateMetricsTests {

    // MARK: - Synthetic helpers

    /// Build a deterministic synthetic trace with a single ad span at
    /// `[adStart, adEnd)`. The baseline span is what
    /// `NarlGroundTruth.build(for:)` walks to compute ground-truth
    /// positives; making it a single span keeps the math hand-checkable.
    private static func makeTrace(
        episodeId: String,
        podcastId: String,
        episodeDuration: Double,
        adStart: Double,
        adEnd: Double,
        atomCount: Int = 100
    ) -> FrozenTrace {
        let atoms: [FrozenTrace.FrozenAtom] = (0..<atomCount).map { i in
            FrozenTrace.FrozenAtom(
                startTime: Double(i) * (episodeDuration / Double(max(1, atomCount))),
                endTime: Double(i + 1) * (episodeDuration / Double(max(1, atomCount))),
                text: "atom-\(i)"
            )
        }
        let baseline = ReplaySpanDecision(
            startTime: adStart,
            endTime: adEnd,
            confidence: 0.9,
            isAd: true,
            sourceTag: "baseline-synthetic"
        )
        return FrozenTrace(
            episodeId: episodeId,
            podcastId: podcastId,
            episodeDuration: episodeDuration,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000),
            featureWindows: [],
            atoms: atoms,
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [baseline],
            holdoutDesignation: .training
        )
    }

    /// Stable synthetic clock — lets tests assert on `generatedAt` and on
    /// the date stamped into the persisted filename.
    private static let testClock: Date = Date(timeIntervalSince1970: 1_777_708_800)
    // 1_777_708_800 = 2026-05-02T00:00:00Z (a Saturday) — picked far
    // enough from a UTC midnight that any small timezone slip would
    // surface as a date-string mismatch in `persistsReportWithExpectedFilename`.

    /// Default gate config used by tests: the production default. Overrides
    /// land per-test.
    private static let defaultGateConfig = ChapterSignalGate.Config.default

    /// Stable predictor config: production default.
    private static let defaultPredictorConfig = MetadataActivationConfig.default

    /// Per-trace show classifier so synthetic traces don't depend on the
    /// (heuristic, real-corpus-targeted) `NarlEvalHarnessTests.showName`.
    private static func showNameByPodcastId(_ trace: FrozenTrace) -> String {
        trace.podcastId
    }

    // MARK: - PerModeMetrics shape

    @Test("PerModeMetrics.empty produces all-zero baseline metrics")
    func perModeEmptyIsZero() {
        let m = PerModeMetrics.empty(mode: .off)
        #expect(m.mode == .off)
        #expect(m.episodeCount == 0)
        #expect(m.precision == 0)
        #expect(m.recall == 0)
        #expect(m.f1 == 0)
        #expect(m.totalFMCalls == 0)
        #expect(m.phaseLatencyP50Ms == 0)
        #expect(m.phaseLatencyP90Ms == 0)
        #expect(m.phaseLatencyP99Ms == 0)
        #expect(m.abortRate == 0)
        #expect(m.episodeReplayCount == 0)
    }

    // MARK: - Empty corpus

    @Test("compute() on empty corpus returns zeroed report with limitations and barMet=false")
    func emptyCorpusReturnsZeroedReport() {
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [],
            runId: "empty-test",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        #expect(report.corpusSize == 0)
        #expect(report.baseline.episodeCount == 0)
        #expect(report.shadow.episodeCount == 0)
        #expect(report.enabled.episodeCount == 0)
        #expect(report.liftPrecision == 0)
        #expect(report.liftRecall == 0)
        #expect(report.liftF1 == 0)
        // Zero-baseline + zero-enabled FM calls clamp to 0/1 = 0.0
        #expect(report.fmCostMultiplier == 0.0)
        #expect(report.barMet == false, "empty corpus must never satisfy the bar")
        #expect(report.perShowLift.isEmpty)
        #expect(report.limitations.contains(where: { $0.contains("empty") }),
                "limitations must call out the empty corpus")
    }

    // MARK: - Detection math

    @Test("compute() detection: asymmetric precision/recall produces correct F1 (harmonic mean)")
    func asymmetricPrecisionRecallF1Math() {
        // Build a trace where predicted > ground truth so precision < recall.
        // Baseline span [60, 120] (60s) → predictor predicts [60, 120].
        // falsePositive correction at [90, 120] (30s) → ground truth = [60, 90] (30s).
        // Expected: TP=30, predicted=60, gt=30
        //   precision = 30/60 = 0.5
        //   recall    = 30/30 = 1.0
        //   F1        = 2 * 0.5 * 1.0 / (0.5 + 1.0) = 1.0/1.5 = 0.6666...
        // Pin the exact harmonic-mean math so a future patch that mistakenly
        // computes arithmetic mean (would give 0.75) or weighted-something is
        // caught.
        let baseline = ReplaySpanDecision(
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            isAd: true,
            sourceTag: "baseline-asymmetric"
        )
        // exactTimeSpan format: "exactTimeSpan:<assetId>:<start>:<end>"
        let falsePositive = FrozenTrace.FrozenCorrection(
            source: "user.falsePositive",
            scope: "exactTimeSpan:asset-asym:90.000:120.000",
            createdAt: 0,
            correctionType: "falsePositive"
        )
        let trace = FrozenTrace(
            episodeId: "ep-asym",
            podcastId: "pod-asym",
            episodeDuration: 600,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Self.testClock,
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [falsePositive],
            decisionEvents: [],
            baselineReplaySpanDecisions: [baseline],
            holdoutDesignation: .training
        )
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [trace],
            runId: "asym-f1",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        #expect(abs(report.baseline.predictedAdSeconds - 60) < 1e-9,
                "predictor falls back to baseline span (60s)")
        #expect(abs(report.baseline.groundTruthAdSeconds - 30) < 1e-9,
                "ground truth subtracts the [90,120] falsePositive correction → 30s")
        #expect(abs(report.baseline.truePositiveSeconds - 30) < 1e-9,
                "true positives = intersection of predicted and ground truth = 30s")
        #expect(abs(report.baseline.precision - 0.5) < 1e-9,
                "precision = TP/predicted = 30/60 = 0.5")
        #expect(abs(report.baseline.recall - 1.0) < 1e-9,
                "recall = TP/GT = 30/30 = 1.0")
        // F1 = 2 * 0.5 * 1.0 / 1.5 = 2/3 = 0.6666...
        #expect(abs(report.baseline.f1 - (2.0 / 3.0)) < 1e-9,
                "F1 = harmonic mean = 2*P*R/(P+R) = 0.6666... NOT arithmetic mean (0.75)")
    }

    @Test("compute() detection: predictor that perfectly recovers ground truth yields precision=recall=f1=1")
    func perfectPredictionYieldsUnitMetrics() {
        // The default `NarlReplayPredictor` falls back to baseline ad
        // spans when there are no per-window scores or evidence — so a
        // synthetic trace whose only baseline span IS the ad window
        // produces a perfect prediction. That is exactly what we want
        // here: pin precision=recall=f1=1 in the deterministic case so a
        // future change to the predictor's behavior is caught.
        let trace = Self.makeTrace(
            episodeId: "ep-perfect",
            podcastId: "pod-perfect",
            episodeDuration: 600,
            adStart: 60,
            adEnd: 90
        )
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [trace],
            runId: "test-perfect",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        #expect(report.baseline.groundTruthAdSeconds == 30)
        #expect(report.baseline.predictedAdSeconds == 30)
        #expect(report.baseline.truePositiveSeconds == 30)
        #expect(report.baseline.precision == 1.0)
        #expect(report.baseline.recall == 1.0)
        #expect(report.baseline.f1 == 1.0)

        // All three modes equal at the scaffold (predictor mode-independent).
        // This pins the documented "shadow detection matches off byte-for-
        // byte" contract until bead 14/16's consumer wiring lands.
        #expect(report.shadow.precision == report.baseline.precision)
        #expect(report.shadow.recall == report.baseline.recall)
        #expect(report.shadow.f1 == report.baseline.f1)
        #expect(report.enabled.precision == report.baseline.precision)
        #expect(report.enabled.recall == report.baseline.recall)
        #expect(report.enabled.f1 == report.baseline.f1)
        // Cross-corpus lift values must be exactly 0 when modes are
        // identical at the scaffold.
        #expect(report.liftPrecision == 0)
        #expect(report.liftRecall == 0)
        #expect(report.liftF1 == 0)
        // barMet must be FALSE at the scaffold: zero-lift on both axes
        // does not satisfy "measurable lift on at least one axis". This
        // is the key safety invariant for production rollout — we must
        // never accidentally ship `barMet == true` until bead 14/16 wire
        // mode-aware prediction.
        #expect(report.barMet == false,
                "scaffold mode-independence must produce barMet == false")
    }

    @Test("compute() detection: predictUnderMode is the single mode-aware swap point")
    func predictUnderModeReturnsIdenticalUnderToday() {
        // Pin the contract that today the `mode` parameter is unused —
        // required for the bead 14/16 forward compatibility comment to
        // be honest. If a future patch wires mode-aware prediction, this
        // test must update with the new contract.
        let trace = Self.makeTrace(
            episodeId: "ep-mode-equal",
            podcastId: "pod-mode-equal",
            episodeDuration: 300,
            adStart: 30,
            adEnd: 60
        )
        let off = ChapterSignalAggregateMetrics.predictUnderMode(
            trace: trace, mode: .off, config: Self.defaultPredictorConfig
        )
        let shadow = ChapterSignalAggregateMetrics.predictUnderMode(
            trace: trace, mode: .shadow, config: Self.defaultPredictorConfig
        )
        let enabled = ChapterSignalAggregateMetrics.predictUnderMode(
            trace: trace, mode: .enabled, config: Self.defaultPredictorConfig
        )
        #expect(off.windows == shadow.windows)
        #expect(off.windows == enabled.windows)
    }

    // MARK: - Phase / cost telemetry

    @Test("compute() phase: .off mode reports zero FM calls and zero latency")
    func offModeStructuralZero() {
        let traces = (0..<3).map { i in
            Self.makeTrace(
                episodeId: "ep-\(i)",
                podcastId: "pod-A",
                episodeDuration: 600,
                adStart: 60,
                adEnd: 90
            )
        }
        let report = ChapterSignalAggregateMetrics.compute(
            traces: traces,
            runId: "off-zero",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        #expect(report.baseline.totalFMCalls == 0)
        #expect(report.baseline.phaseLatencyP50Ms == 0)
        #expect(report.baseline.phaseLatencyP90Ms == 0)
        #expect(report.baseline.phaseLatencyP99Ms == 0)
        #expect(report.baseline.abortRate == 0)
        // .shadow / .enabled both run the phase, so FM calls > 0.
        #expect(report.shadow.totalFMCalls > 0)
        #expect(report.enabled.totalFMCalls > 0)
        // Phase telemetry must be byte-identical between shadow and
        // enabled (both run the phase the same way; only consumer
        // wiring differs, which doesn't affect phase counters). Pin
        // this so a future regression where shadow and enabled diverge
        // on phase telemetry is caught.
        #expect(report.shadow.totalFMCalls == report.enabled.totalFMCalls,
                "shadow and enabled must charge identical FM cost")
        #expect(report.shadow.phaseLatencyP50Ms == report.enabled.phaseLatencyP50Ms,
                "shadow and enabled must report identical phase latency")
    }

    @Test("compute() phase: latency percentiles match a hand-computed corpus")
    func latencyPercentileMath() {
        // Force a known latency vector: stub returns a chapter count
        // proportional to atom count. Each episode pays
        //   perEpisodeOverhead (5ms) + chapterCount * 25ms.
        // Build five traces with chapter counts 1, 2, 3, 4, 5 → latencies
        //   30, 55, 80, 105, 130 ms.
        // p50 → 80 (index 2)
        // p90 → 130 - 0.4 * (130 - 105) = 120  (per percentile() linear interp)
        //   index = 0.9 * 4 = 3.6, lower=3 (105), upper=4 (130), frac=0.6
        //   105 * 0.4 + 130 * 0.6 = 42 + 78 = 120
        // p99 → 0.99 * 4 = 3.96, lower=3 (105), upper=4 (130), frac=0.96
        //   105 * 0.04 + 130 * 0.96 = 4.2 + 124.8 = 129.0
        let traces = (1...5).map { i in
            Self.makeTrace(
                episodeId: "ep-lat-\(i)",
                podcastId: "pod-lat",
                episodeDuration: 600,
                adStart: 60,
                adEnd: 90
            )
        }
        let stubByEpisode: [String: Int] = Dictionary(uniqueKeysWithValues:
            zip((1...5).map { "ep-lat-\($0)" }, [1, 2, 3, 4, 5])
        )
        let config = ChapterSignalGate.Config(
            stubChapterCount: { trace in
                stubByEpisode[trace.episodeId] ?? 1
            }
        )
        let report = ChapterSignalAggregateMetrics.compute(
            traces: traces,
            gateConfig: config,
            runId: "latency-test",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        // Use approximate equality on all three percentiles for
        // consistency. Even though p50 lands exactly on a sample point
        // (no interpolation needed at index 2 for 5 samples), pinning
        // exact equality across the board protects against a future
        // change to MetricMath.percentile that introduces a sub-eps
        // FP residue at sample-aligned percentiles.
        #expect(abs(report.shadow.phaseLatencyP50Ms - 80) < 1e-9)
        #expect(abs(report.shadow.phaseLatencyP90Ms - 120) < 1e-9)
        #expect(abs(report.shadow.phaseLatencyP99Ms - 129) < 1e-9)
        // Same shape for enabled (phase-side identical to shadow).
        #expect(abs(report.enabled.phaseLatencyP50Ms - 80) < 1e-9)
        // .off must produce structural-zero percentiles regardless of
        // input shape — pinning this catches regressions where .off
        // accidentally records non-zero latency from measurement
        // overhead.
        #expect(report.baseline.phaseLatencyP50Ms == 0)
        #expect(report.baseline.phaseLatencyP90Ms == 0)
        #expect(report.baseline.phaseLatencyP99Ms == 0)
    }

    @Test("compute() FM-cost-multiplier: zero baseline clamps denominator to 1")
    func fmCostMultiplierZeroBaselineClamp() {
        // .off charges 0 FM calls, .enabled charges N. The multiplier is
        // therefore N (not +∞).
        let traces = (0..<2).map { i in
            Self.makeTrace(
                episodeId: "ep-fm-\(i)",
                podcastId: "pod-fm",
                episodeDuration: 600,
                adStart: 60,
                adEnd: 90
            )
        }
        let config = ChapterSignalGate.Config(stubChapterCount: { _ in 3 })
        let report = ChapterSignalAggregateMetrics.compute(
            traces: traces,
            gateConfig: config,
            runId: "fm-clamp",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        #expect(report.baseline.totalFMCalls == 0)
        // 2 episodes × 3 calls each.
        #expect(report.enabled.totalFMCalls == 6)
        #expect(report.fmCostMultiplier == 6.0)
        #expect(report.limitations.contains { $0.contains("clamped") },
                "limitation must call out the zero-baseline clamp")
    }

    @Test("computeFMCostMultiplier with non-zero baseline returns ratio")
    func fmCostMultiplierWithNonZeroBaseline() {
        // Direct unit test of the helper since wiring a non-zero baseline
        // through `compute()` requires a baseline mode that runs the
        // phase, which `.off` does not.
        #expect(ChapterSignalAggregateMetrics.computeFMCostMultiplier(baseline: 4, enabled: 8) == 2.0)
        #expect(ChapterSignalAggregateMetrics.computeFMCostMultiplier(baseline: 10, enabled: 5) == 0.5)
        #expect(ChapterSignalAggregateMetrics.computeFMCostMultiplier(baseline: 0, enabled: 0) == 0.0)
    }

    // MARK: - Bar evaluation

    @Test("evaluateBar: positive precision lift, no recall regression, cost in bar → met")
    func evaluateBarPositivePrecisionPasses() {
        #expect(ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: 0.05,
            liftRecall: 0.0,
            fmCostMultiplier: 1.5
        ))
    }

    @Test("evaluateBar: positive recall lift, no precision regression, cost in bar → met")
    func evaluateBarPositiveRecallPasses() {
        #expect(ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: 0.0,
            liftRecall: 0.05,
            fmCostMultiplier: 2.0
        ))
    }

    @Test("evaluateBar: precision regresses while recall lifts → fails")
    func evaluateBarPrecisionRegressionFails() {
        #expect(!ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: -0.05,
            liftRecall: 0.05,
            fmCostMultiplier: 1.0
        ))
    }

    @Test("evaluateBar: cost multiplier above 2.0 → fails even with lift")
    func evaluateBarCostBlowsBar() {
        #expect(!ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: 0.10,
            liftRecall: 0.10,
            fmCostMultiplier: 2.01
        ))
    }

    @Test("evaluateBar: zero lift in both axes → fails")
    func evaluateBarZeroLiftFails() {
        #expect(!ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: 0.0,
            liftRecall: 0.0,
            fmCostMultiplier: 1.0
        ))
    }

    @Test("evaluateBar: tiny noise below epsilon does not count as lift")
    func evaluateBarSubEpsilonLiftFails() {
        #expect(!ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: 1e-12,
            liftRecall: 1e-12,
            fmCostMultiplier: 1.0
        ))
    }

    @Test("evaluateBar: cost exactly at maxFMCostMultiplier → passes (≤ check)")
    func evaluateBarCostExactlyAtBar() {
        #expect(ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: 0.05,
            liftRecall: 0.0,
            fmCostMultiplier: 2.0
        ))
    }

    @Test("evaluateBar: both axes lift and cost ≤ 2.0 → passes")
    func evaluateBarBothAxesLift() {
        #expect(ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: 0.10,
            liftRecall: 0.05,
            fmCostMultiplier: 1.0
        ))
    }

    @Test("evaluateBar: one axis at +epsilon, other at -epsilon → passes (epsilon tolerance)")
    func evaluateBarBoundaryEpsilonSymmetry() {
        // Pin the symmetric epsilon-tolerance: lift exactly at +eps is
        // "measurable", and a regression of exactly -eps is "not
        // regressed" (within tolerance). Together this means the bar
        // passes at the symmetric boundary. If a future patch tightens
        // this (e.g. requires strictly > eps), this test must update
        // intentionally.
        let eps = ChapterSignalAggregateMetrics.measurableLiftEpsilon
        #expect(ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: eps,
            liftRecall: -eps,
            fmCostMultiplier: 1.0
        ))
    }

    @Test("evaluateBar: one axis at +epsilon, other regressed past -epsilon → fails")
    func evaluateBarBoundaryEpsilonAsymmetric() {
        // The other side of the symmetric boundary: when the regression
        // is past -epsilon (strictly more negative), the bar must fail.
        let eps = ChapterSignalAggregateMetrics.measurableLiftEpsilon
        #expect(!ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: eps,
            liftRecall: -2 * eps,
            fmCostMultiplier: 1.0
        ))
    }

    @Test("evaluateBar: both axes at +epsilon (minimum measurable lift) → passes")
    func evaluateBarBothAxesAtPlusEpsilon() {
        // Pin that the symmetric-minimum case (BOTH axes at exactly +eps)
        // passes the bar. This is the "barely passes" boundary — every
        // input above it must also pass, every input below it must fail.
        // Without this test, a future patch that tightens "measurable"
        // to strict-greater-than (`> eps` instead of `>= eps`) could
        // silently flip this case from pass to fail.
        let eps = ChapterSignalAggregateMetrics.measurableLiftEpsilon
        #expect(ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: eps,
            liftRecall: eps,
            fmCostMultiplier: 1.0
        ))
    }

    @Test("evaluateBar: both axes at -epsilon (no measurable lift) → fails")
    func evaluateBarBothAxesAtMinusEpsilon() {
        // Mirror of `evaluateBarBothAxesAtPlusEpsilon`: when BOTH axes
        // are at exactly -eps (within-tolerance regression on both),
        // there's no measurable lift on either axis so clause (1) of the
        // bar fails. Catches a future patch that mistakenly treats
        // "-eps" as a lift via sign flip.
        let eps = ChapterSignalAggregateMetrics.measurableLiftEpsilon
        #expect(!ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: -eps,
            liftRecall: -eps,
            fmCostMultiplier: 1.0
        ))
    }

    @Test("evaluateBar: one axis lifted, other in gray zone (0, +eps) → passes")
    func evaluateBarGrayZoneOnOtherAxis() {
        // The "gray zone" between strict-zero and epsilon: when one axis
        // is genuinely lifted (≥ eps) and the other is positive but
        // sub-epsilon (counts as "not measurably lifted" but also "not
        // regressed"), the bar should pass. Pin the contract so a
        // future patch that requires both axes to clear epsilon is an
        // intentional change.
        let eps = ChapterSignalAggregateMetrics.measurableLiftEpsilon
        #expect(ChapterSignalAggregateMetrics.evaluateBar(
            liftPrecision: 10 * eps,
            liftRecall: 0.5 * eps, // gray zone — not a "measurable lift" but not a regression
            fmCostMultiplier: 1.0
        ))
    }

    // MARK: - Per-show lift

    @Test("compute() per-show lift partitions by show classifier")
    func perShowLiftPartitions() {
        let traces = [
            Self.makeTrace(episodeId: "a1", podcastId: "showA", episodeDuration: 300, adStart: 30, adEnd: 60),
            Self.makeTrace(episodeId: "a2", podcastId: "showA", episodeDuration: 300, adStart: 30, adEnd: 60),
            Self.makeTrace(episodeId: "b1", podcastId: "showB", episodeDuration: 300, adStart: 30, adEnd: 60),
        ]
        let report = ChapterSignalAggregateMetrics.compute(
            traces: traces,
            runId: "per-show",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        #expect(Set(report.perShowLift.keys) == Set(["showA", "showB"]))
        // Both shows should report .enabled as their delta-mode label.
        for (_, delta) in report.perShowLift {
            #expect(delta.mode == .enabled)
            // At the bead scaffold, predictor is mode-independent so all
            // detection deltas are exactly 0. Pin this so a future
            // regression where mode-awareness leaks into the predictor
            // without updating the limitations note is caught.
            #expect(delta.deltaPrecision == 0)
            #expect(delta.deltaRecall == 0)
            #expect(delta.deltaF1 == 0)
            #expect(delta.deltaTruePositiveSeconds == 0)
            // Phase / cost deltas are non-zero (`.enabled` runs the
            // phase, `.off` does not). Sign check, not exact value.
            #expect(delta.deltaTotalFMCalls > 0)
        }
        // showA has 2 episodes, showB has 1.
        #expect(report.perShowLift["showA"]?.episodeCount == 2)
        #expect(report.perShowLift["showB"]?.episodeCount == 1)
    }

    @Test("compute() per-show lift is deterministic across two identical runs")
    func perShowLiftDeterministic() throws {
        let traces = (0..<5).map { i in
            Self.makeTrace(
                episodeId: "ep-\(i)",
                podcastId: i.isMultiple(of: 2) ? "showA" : "showB",
                episodeDuration: 300,
                adStart: 30,
                adEnd: 60
            )
        }
        let r1 = ChapterSignalAggregateMetrics.compute(
            traces: traces,
            runId: "det-1",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let r2 = ChapterSignalAggregateMetrics.compute(
            traces: traces,
            runId: "det-1",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        #expect(r1 == r2, "two runs with identical inputs must produce identical reports")
    }

    // MARK: - Round-trip

    @Test("ChapterSignalLiftReport round-trips through JSONEncoder/JSONDecoder")
    func reportRoundTrips() throws {
        let traces = [
            Self.makeTrace(
                episodeId: "rt-1",
                podcastId: "rt-pod",
                episodeDuration: 600,
                adStart: 100,
                adEnd: 130
            )
        ]
        let original = ChapterSignalAggregateMetrics.compute(
            traces: traces,
            runId: "round-trip-test",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(original)

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(ChapterSignalLiftReport.self, from: data)

        #expect(decoded == original)
        #expect(decoded.schemaVersion == ChapterSignalLiftReport.currentSchemaVersion)
        #expect(decoded.runId == "round-trip-test")
    }

    @Test("JSON wire shape carries all documented field names")
    func wireShapeCarriesDocumentedFields() throws {
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [],
            runId: "shape-test",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(report)
        let text = String(decoding: data, as: UTF8.self)
        for key in [
            "\"schemaVersion\"",
            "\"runId\"",
            "\"generatedAt\"",
            "\"corpusSize\"",
            "\"baseline\"",
            "\"shadow\"",
            "\"enabled\"",
            "\"liftPrecision\"",
            "\"liftRecall\"",
            "\"liftF1\"",
            "\"fmCostMultiplier\"",
            "\"perShowLift\"",
            "\"barMet\"",
            "\"limitations\"",
        ] {
            #expect(text.contains(key), "wire format must carry \(key)")
        }
    }

    @Test("ChapterSignalMode encodes as lowercase string in wire format")
    func modeWireEncodingIsLowercaseString() throws {
        // Pin the JSON encoding of `ChapterSignalMode` so a future patch
        // that switches to a different raw representation (e.g. integer,
        // PascalCase) is caught at the wire boundary. Downstream readers
        // (CI dashboards) parse strings.
        let trace = Self.makeTrace(
            episodeId: "ep-mode-wire",
            podcastId: "pod-mode-wire",
            episodeDuration: 300,
            adStart: 30,
            adEnd: 60
        )
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [trace],
            runId: "mode-wire",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(report)
        let text = String(decoding: data, as: UTF8.self)
        // Each PerModeMetrics carries a `"mode":"..."` field. The three
        // modes currently encode as lowercase strings via Swift's
        // synthesized Codable for `enum ChapterSignalMode: String`. The
        // encoder is `.sortedKeys` only (no `.prettyPrinted`), so there
        // is no whitespace between the colon and the value.
        #expect(text.contains("\"mode\":\"off\""), "baseline.mode must encode as lowercase \"off\"")
        #expect(text.contains("\"mode\":\"shadow\""), "shadow.mode must encode as lowercase \"shadow\"")
        #expect(text.contains("\"mode\":\"enabled\""), "enabled.mode must encode as lowercase \"enabled\"")
    }

    @Test("PerShowLiftDelta wire shape carries delta-prefixed field names")
    func perShowLiftDeltaWireShape() throws {
        let trace = Self.makeTrace(
            episodeId: "ep-delta-wire",
            podcastId: "pod-delta-wire",
            episodeDuration: 300,
            adStart: 30,
            adEnd: 60
        )
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [trace],
            runId: "delta-wire",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(report)
        let text = String(decoding: data, as: UTF8.self)
        // Pin the delta-prefixed wire names so downstream tooling can
        // distinguish per-show deltas from absolute PerModeMetrics.
        for key in [
            "\"deltaPrecision\"",
            "\"deltaRecall\"",
            "\"deltaF1\"",
            "\"deltaTotalFMCalls\"",
            "\"deltaPhaseLatencyP50Ms\"",
            "\"deltaAbortRate\"",
        ] {
            #expect(text.contains(key), "perShowLift wire format must carry \(key)")
        }
    }

    // MARK: - Persistence

    @Test("persist() writes a date-stamped JSON file under the supplied directory")
    func persistsReportWithExpectedFilename() throws {
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [],
            runId: "persist-test",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let tmpDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("chapter-signal-lift-test-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: tmpDir) }

        let url = try ChapterSignalAggregateMetrics.persist(report: report, to: tmpDir)
        #expect(url.lastPathComponent == "chapter-signal-lift-2026-05-02.json",
                "filename must be UTC date-stamped from generatedAt")
        #expect(FileManager.default.fileExists(atPath: url.path))

        let data = try Data(contentsOf: url)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(ChapterSignalLiftReport.self, from: data)
        #expect(decoded == report,
                "persist() must round-trip without losing fields")
    }

    @Test("persist() output bytes are deterministic across two runs")
    func persistDeterministicAcrossRuns() throws {
        // Pin BYTE-EQUAL output for two compute()+persist() cycles with
        // identical inputs. The round-trip test only verifies decoder
        // re-parses; this catches a future encoder change that produces
        // logically-equal but byte-unequal output (e.g. unsorted keys,
        // pretty-printed -> compact, ISO-8601 fractional-second drift).
        let trace = Self.makeTrace(
            episodeId: "ep-det-bytes",
            podcastId: "pod-det-bytes",
            episodeDuration: 300,
            adStart: 30,
            adEnd: 60
        )
        let report1 = ChapterSignalAggregateMetrics.compute(
            traces: [trace],
            runId: "det-bytes",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let report2 = ChapterSignalAggregateMetrics.compute(
            traces: [trace],
            runId: "det-bytes",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let dir1 = FileManager.default.temporaryDirectory
            .appendingPathComponent("chapter-signal-lift-det1-\(UUID().uuidString)")
        let dir2 = FileManager.default.temporaryDirectory
            .appendingPathComponent("chapter-signal-lift-det2-\(UUID().uuidString)")
        defer {
            try? FileManager.default.removeItem(at: dir1)
            try? FileManager.default.removeItem(at: dir2)
        }
        let url1 = try ChapterSignalAggregateMetrics.persist(report: report1, to: dir1)
        let url2 = try ChapterSignalAggregateMetrics.persist(report: report2, to: dir2)
        let bytes1 = try Data(contentsOf: url1)
        let bytes2 = try Data(contentsOf: url2)
        #expect(bytes1 == bytes2,
                "persist() must produce byte-equal output for identical inputs")
    }

    @Test("persist() creates the output directory if missing")
    func persistsCreatesOutputDirectoryIfMissing() throws {
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [],
            runId: "mkdir-test",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let parent = FileManager.default.temporaryDirectory
            .appendingPathComponent("chapter-signal-lift-mkdir-\(UUID().uuidString)")
        let nested = parent.appendingPathComponent("nested/dir/that/does/not/exist")
        defer { try? FileManager.default.removeItem(at: parent) }

        // Pre-condition: nested dir does not exist.
        #expect(!FileManager.default.fileExists(atPath: nested.path))

        let url = try ChapterSignalAggregateMetrics.persist(report: report, to: nested)
        #expect(FileManager.default.fileExists(atPath: nested.path))
        #expect(FileManager.default.fileExists(atPath: url.path))
    }

    @Test("filenameDateString uses UTC regardless of host timezone")
    func filenameDateStringUTC() {
        // 2026-05-02T23:30:00Z is still 2026-05-02 in UTC; in
        // America/Los_Angeles it is 2026-05-02T16:30:00 (same date).
        // The risky case is the other side — pick a UTC time that is
        // 2026-05-03 in UTC but 2026-05-02 in PT to prove we use UTC.
        let nightUTC = Date(timeIntervalSince1970: 1_777_795_200) // 2026-05-03T00:00:00Z
        #expect(ChapterSignalAggregateMetrics.filenameDateString(from: nightUTC) == "2026-05-03",
                "must format in UTC, not host timezone")
    }

    // MARK: - Limitations always populated

    @Test("compute() always emits at least one limitation note")
    func limitationsAlwaysPopulated() {
        let trace = Self.makeTrace(
            episodeId: "ep-lim",
            podcastId: "pod-lim",
            episodeDuration: 300,
            adStart: 30,
            adEnd: 60
        )
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [trace],
            runId: "lim-test",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        #expect(!report.limitations.isEmpty,
                "limitations array empty would be a bug — must always carry the scaffold caveat")
        #expect(report.limitations.contains { $0.contains("ChapterPlan") },
                "must call out the consumer-wiring scaffold caveat")
    }

    @Test("compute() appends caller-supplied extra limitations after structural ones")
    func extraLimitationsAppended() {
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [],
            runId: "extra-lim",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId,
            extraLimitations: ["custom note 1", "custom note 2"]
        )
        let last2 = Array(report.limitations.suffix(2))
        #expect(last2 == ["custom note 1", "custom note 2"],
                "extra limitations must be appended in caller-supplied order, after structural ones")
        // Tightened invariant: structural limitations come FIRST. The
        // first entry must be the scaffold-mode caveat (the always-on
        // structural note that appears in every run).
        #expect(report.limitations.first?.contains("ChapterPlan") == true,
                "first limitation must be the structural scaffold-mode caveat, not a caller-supplied note")
    }

    // MARK: - Excluded episodes

    @Test("compute() excludes whole-asset-vetoed episodes from detection but counts replays")
    func excludedEpisodesNotInDetectionButCountedInPhase() {
        // Build a trace with a wholeAssetVeto correction to trigger
        // `NarlGroundTruth.excluded`. The whole-asset-veto wire format is
        // `exactSpan:<assetId>:0:<Int64.max>` — the historical encoding
        // produced by ClosedRange's upperBound on 64-bit platforms (see
        // `NarlCorrectionScope.parse` for the contract). Using the literal
        // string "wholeAssetVeto:..." would parse as `.unhandled` and the
        // exclusion path would not fire, which is a real footgun this
        // test must avoid.
        let veto = FrozenTrace.FrozenCorrection(
            source: "user.wholeAssetVeto",
            scope: "exactSpan:asset-1:0:\(Int64.max)",
            createdAt: 0,
            correctionType: nil
        )
        let traceVetoed = FrozenTrace(
            episodeId: "veto",
            podcastId: "pod-vetoed",
            episodeDuration: 600,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Self.testClock,
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [veto],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training
        )
        let traceClean = Self.makeTrace(
            episodeId: "clean",
            podcastId: "pod-clean",
            episodeDuration: 300,
            adStart: 30,
            adEnd: 60
        )
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [traceVetoed, traceClean],
            runId: "veto-test",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        #expect(report.baseline.episodeCount == 1, "only the clean trace contributes to detection")
        #expect(report.baseline.excludedEpisodeCount == 1)
        // Phase replay still ran on both.
        #expect(report.baseline.episodeReplayCount == 2)
        #expect(report.shadow.episodeReplayCount == 2)
        #expect(report.enabled.episodeReplayCount == 2)
        // Per-show breakdown: vetoed and clean traces have different
        // podcastIds, so they appear in separate per-show entries. The
        // vetoed show must surface its excludedEpisodeCount, the clean
        // show must surface its real episodeCount.
        let vetoedShowEntry = report.perShowLift["pod-vetoed"]
        let cleanShowEntry = report.perShowLift["pod-clean"]
        #expect(vetoedShowEntry?.episodeCount == 0,
                "vetoed show contributes 0 to detection episode count")
        #expect(vetoedShowEntry?.excludedEpisodeCount == 1,
                "vetoed show carries the exclusion count through to per-show breakdown")
        #expect(cleanShowEntry?.episodeCount == 1,
                "clean show contributes 1 to detection episode count")
        #expect(cleanShowEntry?.excludedEpisodeCount == 0)
        // PerShowLiftDelta.mode is documented as "always .enabled" — the
        // higher arm of the diff. Pin this even on degenerate (all-
        // excluded) shows so a future patch that conditions mode on
        // detection contribution is caught.
        #expect(vetoedShowEntry?.mode == .enabled,
                "PerShowLiftDelta.mode must be .enabled even when episodeCount=0")
        #expect(cleanShowEntry?.mode == .enabled)
    }

    // MARK: - Schema version is current

    @Test("ChapterSignalLiftReport.schemaVersion is the current version")
    func schemaVersionIsCurrent() {
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [],
            runId: "schema",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        #expect(report.schemaVersion == ChapterSignalLiftReport.currentSchemaVersion)
    }

    @Test("empty corpus encodes perShowLift as {} (not null) on the wire")
    func emptyPerShowLiftWireShape() throws {
        // Defensive pin: an empty dictionary must encode as `{}` not
        // `null` — downstream parsers that expect a key-value map (e.g.
        // a CI dashboard's deserializer) will throw on null. Without
        // this test, a future change to the encoding strategy could
        // silently break wire compatibility on empty-corpus runs.
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [],
            runId: "empty-wire",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(report)
        let text = String(decoding: data, as: UTF8.self)
        #expect(text.contains("\"perShowLift\":{}"),
                "empty perShowLift must encode as {} (object), not null")
    }

    @Test("ground-truth ad seconds are mode-independent across baseline/shadow/enabled")
    func groundTruthSecondsAreModeIndependent() {
        // Pin the contract that `NarlGroundTruth.build(for:)` is called the
        // same way for every mode — i.e. ground truth does not silently
        // change between modes. Without this, a future patch that mistakenly
        // routes ground-truth construction through the predictor (or makes
        // it mode-aware) could ship inflated lift numbers because both the
        // numerator (TP) and denominator (groundTruthAdSeconds) drift
        // together. The PerShowLiftDelta.deltaGroundTruthAdSeconds doc says
        // "Should be 0 since ground truth is mode-independent" — this test
        // is the tripwire that doc references.
        let traces: [FrozenTrace] = (0..<3).map { i in
            let start = Double(60 + i * 10)
            let end = Double(90 + i * 10)
            return Self.makeTrace(
                episodeId: "ep-gt-\(i)",
                podcastId: "pod-gt",
                episodeDuration: 600,
                adStart: start,
                adEnd: end
            )
        }
        let report = ChapterSignalAggregateMetrics.compute(
            traces: traces,
            runId: "gt-invariant",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        #expect(report.baseline.groundTruthAdSeconds == report.shadow.groundTruthAdSeconds,
                "ground truth must be mode-independent (baseline vs shadow)")
        #expect(report.shadow.groundTruthAdSeconds == report.enabled.groundTruthAdSeconds,
                "ground truth must be mode-independent (shadow vs enabled)")
        // Per-show delta must be exactly 0 — pin the doc claim directly.
        for (_, delta) in report.perShowLift {
            #expect(delta.deltaGroundTruthAdSeconds == 0,
                    "per-show ground-truth delta must be 0 (ground truth is mode-independent)")
        }
    }

    @Test("per-show FM-call deltas sum to the cross-corpus FM-call delta")
    func perShowFMCallDeltasSumToCrossCorpus() {
        // Pin the no-Simpson's-paradox invariant: the per-show breakdown
        // and the cross-corpus aggregate must agree on FM-call totals
        // because FM-call counts are simple sums (no proportion math).
        // Specifically: sum over shows of (enabled.FMCalls - off.FMCalls)
        // for the show MUST equal the cross-corpus (enabled.FMCalls -
        // baseline.FMCalls). If a future change buckets episodes
        // differently between cross-corpus and per-show passes (e.g. one
        // pass dedupes by episodeId, the other doesn't), this test is the
        // canary.
        let traces = [
            Self.makeTrace(episodeId: "a1", podcastId: "showA", episodeDuration: 300, adStart: 30, adEnd: 60),
            Self.makeTrace(episodeId: "a2", podcastId: "showA", episodeDuration: 300, adStart: 30, adEnd: 60),
            Self.makeTrace(episodeId: "b1", podcastId: "showB", episodeDuration: 300, adStart: 30, adEnd: 60),
            Self.makeTrace(episodeId: "b2", podcastId: "showB", episodeDuration: 300, adStart: 30, adEnd: 60),
            Self.makeTrace(episodeId: "c1", podcastId: "showC", episodeDuration: 300, adStart: 30, adEnd: 60),
        ]
        let config = ChapterSignalGate.Config(stubChapterCount: { _ in 2 })
        let report = ChapterSignalAggregateMetrics.compute(
            traces: traces,
            gateConfig: config,
            runId: "sum-invariant",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let crossCorpusDelta = report.enabled.totalFMCalls - report.baseline.totalFMCalls
        let perShowSum = report.perShowLift.values.reduce(0) { $0 + $1.deltaTotalFMCalls }
        #expect(perShowSum == crossCorpusDelta,
                "sum of per-show deltaTotalFMCalls must equal cross-corpus FM-call delta (no Simpson's paradox on additive metrics)")
    }

    @Test("pretty-printed wire format still encodes mode as lowercase string")
    func prettyPrintedModeWireEncodingMatchesCompact() throws {
        // The persist() path uses [.sortedKeys, .prettyPrinted] while the
        // round-trip and wire-shape tests probe with [.sortedKeys] only.
        // Pretty-printing inserts whitespace around colons, so the literal
        // "mode":"off" string from the compact tests would NOT match the
        // pretty-printed bytes. Pin that the LOGICAL contract (mode value
        // is the lowercase string) survives the pretty-print encoding —
        // i.e. the persisted file readers see "off"/"shadow"/"enabled"
        // regardless of which formatting flag the encoder uses.
        let trace = Self.makeTrace(
            episodeId: "ep-mode-pretty",
            podcastId: "pod-mode-pretty",
            episodeDuration: 300,
            adStart: 30,
            adEnd: 60
        )
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [trace],
            runId: "mode-pretty",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .prettyPrinted]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(report)
        let text = String(decoding: data, as: UTF8.self)
        // Pretty-printed format inserts " : " between key and value.
        #expect(text.contains("\"mode\" : \"off\""),
                "pretty-printed format must encode mode as lowercase \"off\"")
        #expect(text.contains("\"mode\" : \"shadow\""),
                "pretty-printed format must encode mode as lowercase \"shadow\"")
        #expect(text.contains("\"mode\" : \"enabled\""),
                "pretty-printed format must encode mode as lowercase \"enabled\"")
    }

    @Test("schemaVersion encodes as `\"schemaVersion\":1` on the wire")
    func schemaVersionWireValue() throws {
        // Pin not just the field NAME (covered by wireShapeCarriesDocumentedFields)
        // but also the literal numeric VALUE on the wire. Without this,
        // a future patch that changes the default schemaVersion without
        // bumping `currentSchemaVersion` would silently encode the new
        // number while the symbolic test still passed (both sides drift
        // together).
        let report = ChapterSignalAggregateMetrics.compute(
            traces: [],
            runId: "schema-wire",
            generatedAt: Self.testClock,
            showName: Self.showNameByPodcastId
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(report)
        let text = String(decoding: data, as: UTF8.self)
        #expect(text.contains("\"schemaVersion\":1"),
                "wire format must carry the literal numeric schemaVersion 1; bump this test when bumping the schema")
    }
}
