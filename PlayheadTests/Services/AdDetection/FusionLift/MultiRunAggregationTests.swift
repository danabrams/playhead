// MultiRunAggregationTests.swift
// playhead-xsdz.14 — hermetic tests for the noise-aware multi-run
// aggregation + material-lift classifier added to
// `FusionLiftHarnessSupport.swift`. Every input here is a hand-built value
// (no audio, no Foundation Models, no live pipeline), so the suite runs in
// the default `PlayheadFastTests` plan on the simulator and pins the
// classifier's documented rules + the activation-campaign noise-band
// constants so a future change is visible in diff.
//
// Coverage:
//   * median + IQR (p25 / p75) computation against a known input.
//   * NoiseBand constants pinned to the activation-campaign defaults.
//   * classifyDelta — all four documented rule branches:
//       - realEffect: IQRs disjoint AND |Δmedian| > band
//       - withinNoise: IQRs overlap AND |Δmedian| ≤ band
//       - ambiguous: IQRs overlap but |Δmedian| > band
//       - ambiguous: IQRs disjoint but |Δmedian| ≤ band
//   * MultiRunReport JSON round-trip.
//   * ArmAggregate edge cases: single-run, identical-runs.

import Foundation
import Testing
@testable import Playhead

@Suite("Multi-run aggregation (xsdz.14)")
struct MultiRunAggregationTests {

    // MARK: - Fixtures

    /// Build an `ArmRunResult` with the given span-count tuple. Coverage
    /// metrics default to `nil` (most tests only exercise the count lens).
    private static func makeRun(
        tp: Int, fp: Int, miss: Int,
        spanPrecision: Double? = nil,
        spanRecall: Double? = nil,
        spanF1: Double? = nil,
        coveragePrecision: Double? = nil,
        coverageRecall: Double? = nil,
        fireCount: [String: Int] = [:]
    ) -> ArmRunResult {
        ArmRunResult(
            episodeCount: 12,
            truePositives: tp,
            falsePositives: fp,
            misses: miss,
            spanPrecision: spanPrecision,
            spanRecall: spanRecall,
            spanF1: spanF1,
            coveragePrecision: coveragePrecision,
            coverageRecall: coverageRecall,
            fireCount: fireCount
        )
    }

    // MARK: - Median + IQR + mean + stdev

    @Test("median and IQR computed correctly for known input")
    func medianAndIQRComputation() {
        // Five samples: 1, 2, 3, 4, 5. Median is 3, p25 is 2, p75 is 4
        // (NumPy linear / Hyndman-Fan type 7).
        let dist = MetricDistribution([1.0, 2.0, 3.0, 4.0, 5.0])
        #expect(dist.median == 3.0)
        #expect(dist.p25 == 2.0)
        #expect(dist.p75 == 4.0)
        #expect(dist.mean == 3.0)
        // Sample stdev of {1..5} = sqrt(10/4) = sqrt(2.5).
        let expectedStdev = (2.5 as Double).squareRoot()
        #expect((dist.stdev ?? .nan) - expectedStdev < 1.0e-12)
        #expect(dist.definedCount == 5)
    }

    @Test("median + IQR handle 4 samples via linear interpolation (type-7)")
    func medianAndIQR_fourSamples_interpolated() {
        // {10, 20, 30, 40}. Type-7 quartiles: p25 = 17.5, p50 = 25, p75 = 32.5
        // (positions 0.75, 1.5, 2.25 in a 4-element array).
        let dist = MetricDistribution([10.0, 20.0, 30.0, 40.0])
        #expect(dist.median == 25.0)
        #expect(dist.p25 == 17.5)
        #expect(dist.p75 == 32.5)
    }

    @Test("metric distribution skips nil samples; all-nil collapses to empty")
    func metricDistribution_skipsNils() {
        let mixed = MetricDistribution([nil, 2.0, nil, 4.0, 6.0])
        #expect(mixed.definedCount == 3)
        #expect(mixed.median == 4.0)

        let allNil = MetricDistribution([nil, nil, nil] as [Double?])
        #expect(allNil.median == nil)
        #expect(allNil.p25 == nil)
        #expect(allNil.p75 == nil)
        #expect(allNil.mean == nil)
        #expect(allNil.stdev == nil)
        #expect(allNil.definedCount == 0)
    }

    // MARK: - Noise-band constants (pinned)

    @Test("noise band constants pin the documented activation-campaign values")
    func noiseBandConstants() {
        let band = NoiseBand.activationDefault
        #expect(band.truePositives == 2.0)
        #expect(band.falsePositives == 2.0)
        #expect(band.misses == 2.0)
        #expect(band.spanPrecision == 0.04)
        #expect(band.spanRecall == 0.04)
        #expect(band.spanF1 == 0.04)
        #expect(band.coveragePrecision == 0.14)
        #expect(band.coverageRecall == 0.04)
    }

    @Test("noise band lookup by metric matches the field")
    func noiseBandLookup() {
        let band = NoiseBand.activationDefault
        #expect(band.band(for: .falsePositives) == 2.0)
        #expect(band.band(for: .coveragePrecision) == 0.14)
        #expect(band.band(for: .spanF1) == 0.04)
    }

    // MARK: - classifyDelta — REAL effect

    @Test("classifyDelta returns .realEffect when IQRs are disjoint AND delta exceeds the noise band")
    func classifyDelta_realEffect_whenIQRsDisjointAndDeltaBeyondBand() {
        // Baseline FPs: {10, 11, 12, 13, 14} → p25=11, median=12, p75=13.
        // Treatment FPs: {4, 5, 6, 7, 8}  → p25=5, median=6, p75=7.
        // |Δmedian| = 6, band(FP) = 2 → outside band.
        // IQR [5,7] vs [11,13] are disjoint (7 < 11).
        let baseline = ArmAggregate(armLabel: "baseline", runs: [
            Self.makeRun(tp: 5, fp: 10, miss: 0),
            Self.makeRun(tp: 5, fp: 11, miss: 0),
            Self.makeRun(tp: 5, fp: 12, miss: 0),
            Self.makeRun(tp: 5, fp: 13, miss: 0),
            Self.makeRun(tp: 5, fp: 14, miss: 0),
        ])
        let treatment = ArmAggregate(armLabel: "treatment", runs: [
            Self.makeRun(tp: 5, fp: 4, miss: 0),
            Self.makeRun(tp: 5, fp: 5, miss: 0),
            Self.makeRun(tp: 5, fp: 6, miss: 0),
            Self.makeRun(tp: 5, fp: 7, miss: 0),
            Self.makeRun(tp: 5, fp: 8, miss: 0),
        ])
        #expect(
            classifyDelta(metric: .falsePositives, treatmentAgg: treatment, baselineAgg: baseline)
                == .realEffect
        )
    }

    // MARK: - classifyDelta — WITHIN noise

    @Test("classifyDelta returns .withinNoise when IQRs overlap AND delta is within the noise band")
    func classifyDelta_withinNoise_whenIQRsOverlapAndDeltaSmall() {
        // Baseline FPs: {10, 11, 12, 13, 14}. Treatment FPs: {11, 12, 13, 14, 15}.
        // |Δmedian| = 1, band = 2 → inside band.
        // IQRs [11,13] and [12,14] overlap.
        let baseline = ArmAggregate(armLabel: "baseline", runs: [
            Self.makeRun(tp: 5, fp: 10, miss: 0),
            Self.makeRun(tp: 5, fp: 11, miss: 0),
            Self.makeRun(tp: 5, fp: 12, miss: 0),
            Self.makeRun(tp: 5, fp: 13, miss: 0),
            Self.makeRun(tp: 5, fp: 14, miss: 0),
        ])
        let treatment = ArmAggregate(armLabel: "treatment", runs: [
            Self.makeRun(tp: 5, fp: 11, miss: 0),
            Self.makeRun(tp: 5, fp: 12, miss: 0),
            Self.makeRun(tp: 5, fp: 13, miss: 0),
            Self.makeRun(tp: 5, fp: 14, miss: 0),
            Self.makeRun(tp: 5, fp: 15, miss: 0),
        ])
        #expect(
            classifyDelta(metric: .falsePositives, treatmentAgg: treatment, baselineAgg: baseline)
                == .withinNoise
        )
    }

    // MARK: - classifyDelta — AMBIGUOUS (overlapping IQR, big delta)

    @Test("classifyDelta returns .ambiguous when IQRs overlap but |Δmedian| exceeds the band")
    func classifyDelta_ambiguous_whenIQRsOverlapButDeltaBeyondBand() {
        // Baseline FPs: {0, 1, 10, 19, 20}. p25=1, median=10, p75=19.
        // Treatment FPs: {0, 1, 14, 19, 20}. p25=1, median=14, p75=19.
        // |Δmedian| = 4, band = 2 → outside band.
        // IQRs [1,19] and [1,19] overlap (identical). One criterion only.
        let baseline = ArmAggregate(armLabel: "baseline", runs: [
            Self.makeRun(tp: 5, fp: 0, miss: 0),
            Self.makeRun(tp: 5, fp: 1, miss: 0),
            Self.makeRun(tp: 5, fp: 10, miss: 0),
            Self.makeRun(tp: 5, fp: 19, miss: 0),
            Self.makeRun(tp: 5, fp: 20, miss: 0),
        ])
        let treatment = ArmAggregate(armLabel: "treatment", runs: [
            Self.makeRun(tp: 5, fp: 0, miss: 0),
            Self.makeRun(tp: 5, fp: 1, miss: 0),
            Self.makeRun(tp: 5, fp: 14, miss: 0),
            Self.makeRun(tp: 5, fp: 19, miss: 0),
            Self.makeRun(tp: 5, fp: 20, miss: 0),
        ])
        #expect(
            classifyDelta(metric: .falsePositives, treatmentAgg: treatment, baselineAgg: baseline)
                == .ambiguous
        )
    }

    // MARK: - classifyDelta — AMBIGUOUS (disjoint IQR, tiny delta)

    @Test("classifyDelta returns .ambiguous when IQRs are disjoint but |Δmedian| is within the band")
    func classifyDelta_ambiguous_whenIQRsDisjointButDeltaSmall() {
        // Use spanPrecision (band 0.04) so we can craft tight, disjoint IQRs
        // with a tiny median delta:
        //   Baseline: {0.70, 0.70, 0.70, 0.70, 0.70}  → p25=p50=p75=0.70.
        //   Treatment:{0.72, 0.72, 0.72, 0.72, 0.72} → p25=p50=p75=0.72.
        // IQRs [0.70,0.70] vs [0.72,0.72] are disjoint (0.70 < 0.72).
        // |Δmedian| = 0.02, band = 0.04 → within band.
        // One criterion only → ambiguous.
        let baseline = ArmAggregate(armLabel: "baseline", runs: (0..<5).map { _ in
            Self.makeRun(tp: 5, fp: 0, miss: 0, spanPrecision: 0.70)
        })
        let treatment = ArmAggregate(armLabel: "treatment", runs: (0..<5).map { _ in
            Self.makeRun(tp: 5, fp: 0, miss: 0, spanPrecision: 0.72)
        })
        #expect(
            classifyDelta(metric: .spanPrecision, treatmentAgg: treatment, baselineAgg: baseline)
                == .ambiguous
        )
    }

    @Test("classifyDelta returns .ambiguous when either arm's metric is entirely nil")
    func classifyDelta_ambiguous_whenMetricUndefined() {
        // Baseline has defined spanPrecision; treatment is entirely nil.
        let baseline = ArmAggregate(armLabel: "baseline", runs: (0..<5).map { _ in
            Self.makeRun(tp: 5, fp: 0, miss: 0, spanPrecision: 0.80)
        })
        let treatment = ArmAggregate(armLabel: "treatment", runs: (0..<5).map { _ in
            Self.makeRun(tp: 0, fp: 0, miss: 0, spanPrecision: nil)
        })
        #expect(
            classifyDelta(metric: .spanPrecision, treatmentAgg: treatment, baselineAgg: baseline)
                == .ambiguous
        )
    }

    // MARK: - MultiRunReport JSON round-trip

    @Test("MultiRunReport round-trips through JSON Encoder/Decoder")
    func multiRunReport_roundTrip() throws {
        let baselineRuns = (0..<3).map { i in
            Self.makeRun(
                tp: 10 + i, fp: 5 + i, miss: 2,
                spanPrecision: 0.80, spanRecall: 0.75, spanF1: 0.775,
                coveragePrecision: 0.60, coverageRecall: 0.70,
                fireCount: ["channelA": i, "channelB": 1]
            )
        }
        let treatmentRuns = (0..<3).map { i in
            Self.makeRun(
                tp: 12 + i, fp: 3 + i, miss: 1,
                spanPrecision: 0.85, spanRecall: 0.80, spanF1: 0.825,
                coveragePrecision: 0.65, coverageRecall: 0.72,
                fireCount: ["channelA": 2 + i]
            )
        }
        let report = MultiRunReport(
            runCount: 3,
            configHash: "deadbeef",
            armLabels: ["baseline", "treatment"],
            runsByArm: ["baseline": baselineRuns, "treatment": treatmentRuns]
        )

        let data = try report.toJSON()
        let decoder = JSONDecoder()
        let round = try decoder.decode(MultiRunReport.self, from: data)

        #expect(round == report, "encode → decode must reproduce the report exactly")
        #expect(round.runCount == 3)
        #expect(round.configHash == "deadbeef")
        #expect(round.armAggregates.count == 2)
        #expect(round.pairwiseDeltas.count == 1)
        #expect(round.pairwiseDeltas.first?.baseline == "baseline")
        #expect(round.pairwiseDeltas.first?.treatment == "treatment")
        // Noise-band constants should round-trip exactly.
        #expect(round.noiseBand == NoiseBand.activationDefault)
    }

    // MARK: - ArmAggregate edge cases

    @Test("ArmAggregate handles a single run: stdev 0, IQR endpoints equal the value")
    func armAggregate_handlesSingleRun() {
        let agg = ArmAggregate(armLabel: "solo", runs: [
            Self.makeRun(tp: 7, fp: 3, miss: 2, spanPrecision: 0.7)
        ])
        #expect(agg.runCount == 1)
        #expect(agg.falsePositives.median == 3.0)
        #expect(agg.falsePositives.p25 == 3.0)
        #expect(agg.falsePositives.p75 == 3.0)
        #expect(agg.falsePositives.stdev == 0.0)
        #expect(agg.spanPrecision.median == 0.7)
        #expect(agg.spanPrecision.stdev == 0.0)
    }

    @Test("ArmAggregate handles identical N runs: stdev 0, classifier never returns .realEffect for any small delta")
    func armAggregate_handlesIdenticalRuns() {
        // All 5 baseline runs identical at FP=10; all 5 treatment runs
        // identical at FP=11. IQRs collapse to a single point each:
        //   baseline IQR = [10, 10], treatment IQR = [11, 11].
        // Per the documented closed-interval overlap rule, [10,10] and
        // [11,11] are DISJOINT (10 < 11). But |Δmedian| = 1, which is
        // within the ±2 FP band → outside-band check fails →
        // the verdict MUST be .ambiguous (one criterion, not both).
        let baseline = ArmAggregate(armLabel: "baseline", runs: (0..<5).map { _ in
            Self.makeRun(tp: 5, fp: 10, miss: 0)
        })
        let treatment = ArmAggregate(armLabel: "treatment", runs: (0..<5).map { _ in
            Self.makeRun(tp: 5, fp: 11, miss: 0)
        })
        #expect(baseline.falsePositives.stdev == 0.0)
        #expect(treatment.falsePositives.stdev == 0.0)
        #expect(
            classifyDelta(metric: .falsePositives, treatmentAgg: treatment, baselineAgg: baseline)
                == .ambiguous,
            "identical runs with delta inside the noise band must NOT be .realEffect even though IQRs are technically disjoint"
        )
    }

    // MARK: - Env-var helpers

    @Test("multiRunCountFromEnv parses and clamps the env var")
    func multiRunCountFromEnv_parsesAndClamps() {
        #expect(multiRunCountFromEnv([:]) == 5, "unset → default 5")
        #expect(multiRunCountFromEnv(["PLAYHEAD_MULTIRUN_N": "8"]) == 8)
        #expect(multiRunCountFromEnv(["PLAYHEAD_MULTIRUN_N": "1"]) == 2, "below floor → 2")
        #expect(multiRunCountFromEnv(["PLAYHEAD_MULTIRUN_N": "999"]) == 20, "above ceiling → 20")
        #expect(multiRunCountFromEnv(["PLAYHEAD_MULTIRUN_N": "garbage"]) == 5, "unparseable → fallback")
    }

    @Test("multiRunABEnabled checks the combined-AB env var")
    func multiRunABEnabled_check() {
        #expect(multiRunABEnabled([:]) == false)
        #expect(multiRunABEnabled(["PLAYHEAD_MULTIRUN_AB": "1"]) == true)
        #expect(multiRunABEnabled(["PLAYHEAD_MULTIRUN_AB": "0"]) == false)
        #expect(multiRunABEnabled(["PLAYHEAD_MULTIRUN_AB": "true"]) == false, "only literal '1' enables — matches sibling AB env-var pattern")
    }

    // MARK: - Driver

    @Test("runMultiRunAggregation invokes the closure N times per arm in order")
    func runMultiRunAggregation_callsClosureNPerArm() async throws {
        actor Call { var rows: [(String, Int)] = []
            func append(_ arm: String, _ idx: Int) { rows.append((arm, idx)) } }
        let calls = Call()

        let report = try await runMultiRunAggregation(
            arms: ["baseline", "treatment"],
            config: MultiRunDriverConfig(runCount: 3, configHash: "test-hash")
        ) { armLabel, runIndex in
            await calls.append(armLabel, runIndex)
            return Self.makeRun(tp: 5 + runIndex, fp: 0, miss: 0)
        }

        let rows = await calls.rows
        #expect(rows.count == 6, "2 arms × 3 runs = 6 calls")
        #expect(rows.prefix(3).map(\.0) == ["baseline", "baseline", "baseline"])
        #expect(rows.suffix(3).map(\.0) == ["treatment", "treatment", "treatment"])
        #expect(rows.prefix(3).map(\.1) == [0, 1, 2])

        #expect(report.runCount == 3)
        #expect(report.configHash == "test-hash")
        #expect(report.armAggregates.count == 2)
        #expect(report.pairwiseDeltas.count == 1)
    }
}
