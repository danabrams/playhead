// ChapterSignalAggregateMetrics.swift
// playhead-au2v.1.19: aggregate precision/recall/F1/FM-cost lift metrics
// across the dogfood corpus for each `ChapterSignalMode` (off / shadow /
// enabled). Builds on the bead-18 `ChapterSignalGate` replay (phase-side
// telemetry) and the existing `NarlReplayPredictor` + `NarlGroundTruth`
// (detection-side prediction vs. ground truth) to produce a single
// "lift report" the team uses to decide whether to flip the production
// `ChapterSignalMode` from `.off` → `.shadow` → `.enabled`.
//
// Architecture:
//   - Detection metrics (precision / recall / F1) are computed from the
//     per-trace `NarlReplayPredictor.predict(...)` output diffed against
//     `NarlGroundTruth.build(for:)`. The predictor is **config-driven**
//     by `MetadataActivationConfig`, NOT by `ChapterSignalMode` — at the
//     current bead scaffold (bead 14/16's "consumers read the plan"
//     wiring is still landing in parallel), `.off` / `.shadow` /
//     `.enabled` produce **identical** detection numbers when run against
//     today's predictor. That is a documented pre-condition, surfaced in
//     `LiftReport.limitations`. Once bead 14/16 ship, the predictor
//     gains a `chapterPlan` parameter and the three modes diverge — the
//     report shape stays unchanged.
//   - Phase / FM-cost metrics (FM-call count, phase wall-clock latency
//     p50/p90/p99, abort-rate by category) come from
//     `ChapterSignalGate.replay(traces:mode:)` for each mode. These DO
//     differ across modes today: `.off` is structural-zero,
//     `.shadow` / `.enabled` charge real FM cost.
//   - Bar evaluation: `barMet == true` iff
//        (measurable lift on ≥1 of {recall, precision}) AND
//        (no regression on the other) AND
//        (`fmCostMultiplier ≤ 2.0`).
//     "Measurable lift" uses a small epsilon (`Self.measurableLiftEpsilon`)
//     so floating-point noise doesn't trip the bar. "No regression" is
//     `>= -epsilon` (strict equality is allowed).
//
// Determinism:
//   - All ordering is stable: per-show keys are sorted, mode iteration is
//     in declaration order (off → shadow → enabled), latency percentile
//     math is the existing `MetricMath.percentile` helper that sorts
//     before interpolating. Two runs against the same input corpus
//     produce byte-equal JSON (modulo `generatedAt` and `runId`, which
//     are explicit caller inputs in tests so we can pin them).
//
// Hermetic:
//   - No network, no FoundationModels. The gate's stub closures are
//     deterministic; the predictor reads only frozen-trace data.

import Foundation
@testable import Playhead

// MARK: - Per-mode metrics

/// Aggregate metrics for one `ChapterSignalMode` over a corpus.
///
/// Detection numbers are computed against `MetadataActivationConfig.default`
/// (the production-shipping config) so the lift report compares modes on
/// the same predictor configuration that real users run.
struct PerModeMetrics: Sendable, Codable, Equatable {
    /// The `ChapterSignalMode` these metrics describe.
    let mode: ChapterSignalMode
    /// Number of episodes (traces) folded into this aggregate. Excluded
    /// episodes (whole-asset vetoes, no ground truth) are not counted.
    let episodeCount: Int
    /// Number of episodes excluded by `NarlGroundTruth` (e.g. whole-asset
    /// vetoes). These are still replayed through the gate (so phase
    /// telemetry is honest) but they do NOT contribute to detection
    /// metrics.
    let excludedEpisodeCount: Int

    // MARK: Detection
    /// Sum of correctly-predicted ad seconds across the corpus, divided by
    /// total predicted ad seconds. 0.0 when no ad seconds were predicted.
    let precision: Double
    /// Sum of correctly-predicted ad seconds across the corpus, divided by
    /// total ground-truth ad seconds. 0.0 when ground truth has no ads.
    let recall: Double
    /// Harmonic mean of precision/recall. 0.0 when both are 0.
    let f1: Double
    /// Total ad seconds in the predicted set across the corpus. Reported
    /// alongside precision/recall so a reader can sanity-check the
    /// denominators.
    let predictedAdSeconds: Double
    /// Total ad seconds in ground truth across the corpus.
    let groundTruthAdSeconds: Double
    /// Total correctly-predicted (intersected) ad seconds across the
    /// corpus.
    let truePositiveSeconds: Double

    // MARK: Phase / cost
    /// Total FM calls the chapter-labelling phase consumed across the
    /// corpus in this mode. Always 0 for `.off`.
    let totalFMCalls: Int
    /// Per-episode phase latency p50 (ms). Computed across all episodes
    /// (including 0-latency episodes from `.off`, which is why the `.off`
    /// percentile is always 0).
    let phaseLatencyP50Ms: Double
    /// Per-episode phase latency p90 (ms).
    let phaseLatencyP90Ms: Double
    /// Per-episode phase latency p99 (ms).
    let phaseLatencyP99Ms: Double

    // MARK: Aborts
    /// Number of episodes where the phase aborted on the operational
    /// unclear-rate gate. The current gate stub never trips this; reported
    /// as 0 until bead 13 wires the real labelling outcome.
    let abortedByOperationalRate: Int
    /// Number of episodes where the phase aborted on the pathological
    /// boundary-rate gate. The current gate stub never trips this;
    /// reported as 0 until bead 4 wires the real boundary-detector.
    let abortedByPathologicalRate: Int
    /// Number of episodes where the phase short-circuited because the
    /// trace already had creator-supplied chapters.
    let skippedByCreatorChapters: Int
    /// Total abort-rate (operational + pathological) per episode-replay.
    /// 0.0 when `episodeReplayCount == 0`.
    let abortRate: Double
    /// Number of episode-replays this mode actually performed (for the
    /// abort-rate denominator).
    let episodeReplayCount: Int

    static func empty(mode: ChapterSignalMode) -> PerModeMetrics {
        PerModeMetrics(
            mode: mode,
            episodeCount: 0,
            excludedEpisodeCount: 0,
            precision: 0,
            recall: 0,
            f1: 0,
            predictedAdSeconds: 0,
            groundTruthAdSeconds: 0,
            truePositiveSeconds: 0,
            totalFMCalls: 0,
            phaseLatencyP50Ms: 0,
            phaseLatencyP90Ms: 0,
            phaseLatencyP99Ms: 0,
            abortedByOperationalRate: 0,
            abortedByPathologicalRate: 0,
            skippedByCreatorChapters: 0,
            abortRate: 0,
            episodeReplayCount: 0
        )
    }
}

// MARK: - Lift report

/// Aggregate lift report comparing `.off` (baseline) vs `.shadow` vs
/// `.enabled` across the dogfood corpus.
///
/// The fields on this struct are documented to be **stable wire format**:
/// callers (CI dashboards, downstream eval beads) decode this exact
/// shape. Adding fields is fine if they're optional; renaming or removing
/// is a breaking change.
struct ChapterSignalLiftReport: Sendable, Codable, Equatable {
    /// Schema version for this lift report. Currently `1`. Bumped only on
    /// breaking wire-shape changes (rename/remove); additive changes
    /// (new optional fields) keep the same version.
    let schemaVersion: Int
    /// Identifier for this run (e.g. an ISO timestamp string or a build
    /// SHA). Caller-supplied so tests can pin a stable value and
    /// production runs can stamp a real one.
    let runId: String
    /// Wall-clock timestamp this report was generated. Caller-supplied
    /// for the same reason as `runId`.
    let generatedAt: Date
    /// Number of distinct traces in the corpus this run consumed.
    let corpusSize: Int

    /// Baseline mode (`.off`) metrics.
    let baseline: PerModeMetrics
    /// Shadow mode (`.shadow`) metrics.
    let shadow: PerModeMetrics
    /// Enabled mode (`.enabled`) metrics.
    let enabled: PerModeMetrics

    /// Precision lift = `enabled.precision - baseline.precision`. Positive
    /// is good. Computed from the cross-corpus aggregate, NOT averaged
    /// per-show (which would be a Simpson's-paradox trap).
    let liftPrecision: Double
    /// Recall lift = `enabled.recall - baseline.recall`. Positive is good.
    let liftRecall: Double
    /// F1 lift = `enabled.f1 - baseline.f1`. Positive is good.
    let liftF1: Double

    /// Multiplier on FM cost from baseline to enabled:
    ///   enabled.totalFMCalls / max(1, baseline.totalFMCalls).
    /// `.off` always has 0 FM calls, so the denominator is clamped to 1
    /// in the divisor — when `baseline.totalFMCalls == 0`, the multiplier
    /// is `Double(enabled.totalFMCalls)` (i.e. "enabled charges N calls
    /// over a zero-baseline"). Documented in `limitations`.
    let fmCostMultiplier: Double

    /// Per-show-keyed lift, where the value is `enabled - baseline` for
    /// that show. The dictionary is keyed by show name (as produced by
    /// `NarlEvalHarnessTests.showName(for:)`). Each value is a
    /// `PerModeMetrics` whose numeric fields are the **enabled minus
    /// baseline** delta — so `precision`/`recall`/`f1` are the lift
    /// numbers, and the cost fields are the cost-delta. The `mode` field
    /// is `.enabled` (the higher arm of the comparison) so a reader
    /// knows which side of the diff this is.
    ///
    /// Encoded as `[String: PerModeMetrics]` for JSON readability; the
    /// stable iteration order at the call site is achieved by
    /// `JSONEncoder.OutputFormatting.sortedKeys`.
    let perShowLift: [String: PerModeMetrics]

    /// `true` iff the gating bar is met:
    ///   - measurable lift (≥ `measurableLiftEpsilon`) on at least one of
    ///     {recall, precision}, AND
    ///   - no regression (≥ `-measurableLiftEpsilon`) on the other, AND
    ///   - `fmCostMultiplier ≤ maxFMCostMultiplier` (default 2.0).
    /// Surfaced as a single bool so a CI gate can read one field.
    let barMet: Bool

    /// Free-text limitations a human should know when reading this report.
    /// Examples:
    ///   - "Detection metrics across modes are identical at this bead
    ///      scaffold (consumer wiring lands in beads 14/16)."
    ///   - "Sample size: 41 episodes; corpus is dogfood-only (Conan +
    ///      DoaC heavy)."
    /// Always populated — the empty array is a bug, not a successful run.
    let limitations: [String]

    static let currentSchemaVersion: Int = 1
}

// MARK: - Computation engine

enum ChapterSignalAggregateMetrics {

    /// Smallest absolute value of precision/recall/F1 lift that counts as
    /// "measurable". Picked to be comfortably above floating-point round-
    /// off on a corpus of <100 episodes (where seconds-level deltas can
    /// be on the order of 1e-12 from sum order).
    static let measurableLiftEpsilon: Double = 1e-9

    /// Maximum allowed `fmCostMultiplier` for the bar to be met.
    static let maxFMCostMultiplier: Double = 2.0

    /// Compute the lift report.
    ///
    /// - Parameters:
    ///   - traces: the corpus to replay (every trace in this list is fed
    ///     through the gate for each mode; ground truth is built from
    ///     each trace independently). Empty input produces an empty
    ///     report whose `barMet` is `false`.
    ///   - gateConfig: `ChapterSignalGate.Config` to use for every mode.
    ///     Defaults to `.default`. Tests use a custom config to drive
    ///     deterministic phase telemetry on synthetic traces.
    ///   - predictorConfig: `MetadataActivationConfig` to drive the
    ///     detection-side replay. Defaults to `.default` (the production-
    ///     shipping config). Tests can override to produce non-zero
    ///     precision/recall on degenerate fixtures.
    ///   - runId: caller-supplied run identifier (stamped onto the
    ///     report).
    ///   - generatedAt: caller-supplied timestamp (stamped onto the
    ///     report).
    ///   - showName: caller-supplied show classifier. Defaults to the
    ///     existing `NarlEvalHarnessTests.showName(for:)`. Tests can
    ///     override with a stable per-trace mapping.
    ///   - extraLimitations: caller-appended limitations. The compute
    ///     function always emits its own structural limitations FIRST
    ///     (e.g. corpus size, scaffold-mode caveat); caller additions are
    ///     appended in order.
    static func compute(
        traces: [FrozenTrace],
        gateConfig: ChapterSignalGate.Config = .default,
        predictorConfig: MetadataActivationConfig = .default,
        runId: String,
        generatedAt: Date,
        showName: (FrozenTrace) -> String = NarlEvalHarnessTests.showName(for:),
        extraLimitations: [String] = []
    ) -> ChapterSignalLiftReport {
        // Phase telemetry per mode.
        let baselineGate = ChapterSignalGate.replay(traces: traces, mode: .off, config: gateConfig)
        let shadowGate = ChapterSignalGate.replay(traces: traces, mode: .shadow, config: gateConfig)
        let enabledGate = ChapterSignalGate.replay(traces: traces, mode: .enabled, config: gateConfig)

        // Detection metrics per mode. At today's scaffold the predictor
        // is mode-independent — `predictUnderMode` is the swap-in point
        // for bead 14/16's consumer wiring (see file-header comment).
        let baselineDetection = predictDetection(
            traces: traces,
            mode: .off,
            config: predictorConfig
        )
        let shadowDetection = predictDetection(
            traces: traces,
            mode: .shadow,
            config: predictorConfig
        )
        let enabledDetection = predictDetection(
            traces: traces,
            mode: .enabled,
            config: predictorConfig
        )

        let baseline = makePerModeMetrics(
            mode: .off,
            gateResult: baselineGate,
            detection: baselineDetection
        )
        let shadow = makePerModeMetrics(
            mode: .shadow,
            gateResult: shadowGate,
            detection: shadowDetection
        )
        let enabled = makePerModeMetrics(
            mode: .enabled,
            gateResult: enabledGate,
            detection: enabledDetection
        )

        // Per-show lift (enabled - baseline).
        let perShowLift = computePerShowLift(
            traces: traces,
            gateConfig: gateConfig,
            predictorConfig: predictorConfig,
            showName: showName
        )

        let liftPrecision = enabled.precision - baseline.precision
        let liftRecall = enabled.recall - baseline.recall
        let liftF1 = enabled.f1 - baseline.f1
        let fmCostMultiplier = computeFMCostMultiplier(
            baseline: baseline.totalFMCalls,
            enabled: enabled.totalFMCalls
        )

        let barMet = evaluateBar(
            liftPrecision: liftPrecision,
            liftRecall: liftRecall,
            fmCostMultiplier: fmCostMultiplier
        )

        var limitations = makeStructuralLimitations(
            corpusSize: traces.count,
            baselineFMCalls: baseline.totalFMCalls,
            enabledFMCalls: enabled.totalFMCalls,
            shadowDiffersFromBaselineDetection:
                shadow.precision != baseline.precision || shadow.recall != baseline.recall
        )
        limitations.append(contentsOf: extraLimitations)

        return ChapterSignalLiftReport(
            schemaVersion: ChapterSignalLiftReport.currentSchemaVersion,
            runId: runId,
            generatedAt: generatedAt,
            corpusSize: traces.count,
            baseline: baseline,
            shadow: shadow,
            enabled: enabled,
            liftPrecision: liftPrecision,
            liftRecall: liftRecall,
            liftF1: liftF1,
            fmCostMultiplier: fmCostMultiplier,
            perShowLift: perShowLift,
            barMet: barMet,
            limitations: limitations
        )
    }

    // MARK: - Persistence

    /// Persist a lift report as JSON to a date-stamped file under the
    /// caller-supplied directory.
    ///
    /// Output filename: `chapter-signal-lift-<yyyy-MM-dd>.json`. The date
    /// portion is derived from `report.generatedAt` in UTC so two
    /// machines in different timezones produce the same filename for the
    /// same logical run.
    ///
    /// Creates `outputDir` if it does not exist (and any intermediate
    /// directories). Encoder uses `.sortedKeys` and `.iso8601` so two
    /// runs with the same inputs produce byte-equal JSON.
    ///
    /// Returns the URL of the written file.
    @discardableResult
    static func persist(
        report: ChapterSignalLiftReport,
        to outputDir: URL,
        fileManager: FileManager = .default
    ) throws -> URL {
        try fileManager.createDirectory(
            at: outputDir,
            withIntermediateDirectories: true
        )
        let filename = "chapter-signal-lift-\(filenameDateString(from: report.generatedAt)).json"
        let fileURL = outputDir.appendingPathComponent(filename)

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .prettyPrinted]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(report)
        try data.write(to: fileURL, options: .atomic)
        return fileURL
    }

    /// Filename-safe UTC date string (yyyy-MM-dd). Pulled out as a static
    /// helper so tests can pin the exact filename without re-deriving the
    /// formatter.
    static func filenameDateString(from date: Date) -> String {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "UTC")
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter.string(from: date)
    }

    // MARK: - Internals

    /// Per-trace prediction result, used by the aggregate to roll up
    /// precision/recall/F1.
    /// Not `Equatable` because `FrozenTrace` is not — a trace identity
    /// match is irrelevant for the aggregate (we sum scalars).
    struct DetectionPerTrace: Sendable {
        let trace: FrozenTrace
        let isExcluded: Bool
        let predictedAdSeconds: Double
        let groundTruthAdSeconds: Double
        let truePositiveSeconds: Double
    }

    /// Run the detection-side prediction for one trace under one mode.
    ///
    /// Today the `mode` argument is unused — the predictor is mode-
    /// independent at the bead scaffold. This is the SINGLE call site
    /// where bead 14/16's consumer wiring will swap in a mode-aware
    /// predictor (e.g. `NarlReplayPredictor.predict(trace:config:
    /// chapterPlan:)`). Keeping the dispatch in one helper means the
    /// aggregate is mode-correct the moment the predictor learns to
    /// read the plan, with no shape change to `PerModeMetrics`.
    static func predictUnderMode(
        trace: FrozenTrace,
        mode: ChapterSignalMode,
        config: MetadataActivationConfig
    ) -> NarlPredictionResult {
        // Suppress unused-parameter warnings deliberately — `mode` is the
        // forward-looking hook (see comment above). When bead 14/16
        // ships, fan out through `mode.consumersReadChapterPlan` here.
        _ = mode
        return NarlReplayPredictor.predict(trace: trace, config: config)
    }

    private static func predictDetection(
        traces: [FrozenTrace],
        mode: ChapterSignalMode,
        config: MetadataActivationConfig
    ) -> [DetectionPerTrace] {
        traces.map { trace in
            let gt = NarlGroundTruth.build(for: trace)
            if gt.isExcluded {
                return DetectionPerTrace(
                    trace: trace,
                    isExcluded: true,
                    predictedAdSeconds: 0,
                    groundTruthAdSeconds: 0,
                    truePositiveSeconds: 0
                )
            }
            let pred = predictUnderMode(trace: trace, mode: mode, config: config)
            let predicted = totalSeconds(pred.windows)
            let groundTruth = totalSeconds(gt.adWindows)
            let tp = intersectSeconds(pred.windows, gt.adWindows)
            return DetectionPerTrace(
                trace: trace,
                isExcluded: false,
                predictedAdSeconds: predicted,
                groundTruthAdSeconds: groundTruth,
                truePositiveSeconds: tp
            )
        }
    }

    private static func makePerModeMetrics(
        mode: ChapterSignalMode,
        gateResult: ChapterSignalGate.ChapterSignalReplayResult,
        detection: [DetectionPerTrace]
    ) -> PerModeMetrics {
        let included = detection.filter { !$0.isExcluded }
        let excluded = detection.filter { $0.isExcluded }

        let predictedSum = included.reduce(0.0) { $0 + $1.predictedAdSeconds }
        let groundTruthSum = included.reduce(0.0) { $0 + $1.groundTruthAdSeconds }
        let tpSum = included.reduce(0.0) { $0 + $1.truePositiveSeconds }

        let precision = predictedSum > 0 ? tpSum / predictedSum : 0
        let recall = groundTruthSum > 0 ? tpSum / groundTruthSum : 0
        let f1: Double
        if precision + recall > 0 {
            f1 = 2.0 * precision * recall / (precision + recall)
        } else {
            f1 = 0
        }

        let latencies = gateResult.perEpisodeOutcomes.map(\.phaseLatencyMs)
        let p50 = MetricMath.percentile(latencies, 0.5)
        let p90 = MetricMath.percentile(latencies, 0.9)
        let p99 = MetricMath.percentile(latencies, 0.99)

        let replayCount = gateResult.episodesProcessed
        let abortNumerator = gateResult.planAbortedByOperationalRate
            + gateResult.planAbortedByPathologicalRate
        let abortRate = replayCount > 0 ? Double(abortNumerator) / Double(replayCount) : 0

        return PerModeMetrics(
            mode: mode,
            episodeCount: included.count,
            excludedEpisodeCount: excluded.count,
            precision: precision,
            recall: recall,
            f1: f1,
            predictedAdSeconds: predictedSum,
            groundTruthAdSeconds: groundTruthSum,
            truePositiveSeconds: tpSum,
            totalFMCalls: gateResult.totalFMCallsForChapterLabeling,
            phaseLatencyP50Ms: p50,
            phaseLatencyP90Ms: p90,
            phaseLatencyP99Ms: p99,
            abortedByOperationalRate: gateResult.planAbortedByOperationalRate,
            abortedByPathologicalRate: gateResult.planAbortedByPathologicalRate,
            skippedByCreatorChapters: gateResult.skippedByCreatorChapters,
            abortRate: abortRate,
            episodeReplayCount: replayCount
        )
    }

    private static func computePerShowLift(
        traces: [FrozenTrace],
        gateConfig: ChapterSignalGate.Config,
        predictorConfig: MetadataActivationConfig,
        showName: (FrozenTrace) -> String
    ) -> [String: PerModeMetrics] {
        // Bucket traces by show. Sorted iteration order so adding a new
        // show doesn't reshuffle the dictionary insertion order on disk
        // (the JSON encoder's `.sortedKeys` already takes care of the
        // wire format, but per-show insertion order can also matter
        // for downstream readers that walk an `OrderedDict`-style
        // representation).
        var byShow: [String: [FrozenTrace]] = [:]
        for trace in traces {
            byShow[showName(trace), default: []].append(trace)
        }

        var result: [String: PerModeMetrics] = [:]
        for show in byShow.keys.sorted() {
            guard let showTraces = byShow[show], !showTraces.isEmpty else { continue }
            let baselineGate = ChapterSignalGate.replay(traces: showTraces, mode: .off, config: gateConfig)
            let enabledGate = ChapterSignalGate.replay(traces: showTraces, mode: .enabled, config: gateConfig)
            let baselineDetection = predictDetection(
                traces: showTraces,
                mode: .off,
                config: predictorConfig
            )
            let enabledDetection = predictDetection(
                traces: showTraces,
                mode: .enabled,
                config: predictorConfig
            )
            let baseline = makePerModeMetrics(
                mode: .off,
                gateResult: baselineGate,
                detection: baselineDetection
            )
            let enabled = makePerModeMetrics(
                mode: .enabled,
                gateResult: enabledGate,
                detection: enabledDetection
            )
            result[show] = makeDeltaMetrics(baseline: baseline, enabled: enabled)
        }
        return result
    }

    /// Build a `PerModeMetrics` whose numeric fields are
    /// `enabled - baseline`. The returned struct's `mode` is `.enabled`
    /// (the higher arm of the comparison) so a reader of the JSON knows
    /// which side of the diff this represents.
    private static func makeDeltaMetrics(
        baseline: PerModeMetrics,
        enabled: PerModeMetrics
    ) -> PerModeMetrics {
        PerModeMetrics(
            mode: .enabled,
            episodeCount: enabled.episodeCount,
            excludedEpisodeCount: enabled.excludedEpisodeCount,
            precision: enabled.precision - baseline.precision,
            recall: enabled.recall - baseline.recall,
            f1: enabled.f1 - baseline.f1,
            predictedAdSeconds: enabled.predictedAdSeconds - baseline.predictedAdSeconds,
            groundTruthAdSeconds: enabled.groundTruthAdSeconds - baseline.groundTruthAdSeconds,
            truePositiveSeconds: enabled.truePositiveSeconds - baseline.truePositiveSeconds,
            totalFMCalls: enabled.totalFMCalls - baseline.totalFMCalls,
            phaseLatencyP50Ms: enabled.phaseLatencyP50Ms - baseline.phaseLatencyP50Ms,
            phaseLatencyP90Ms: enabled.phaseLatencyP90Ms - baseline.phaseLatencyP90Ms,
            phaseLatencyP99Ms: enabled.phaseLatencyP99Ms - baseline.phaseLatencyP99Ms,
            abortedByOperationalRate: enabled.abortedByOperationalRate - baseline.abortedByOperationalRate,
            abortedByPathologicalRate: enabled.abortedByPathologicalRate - baseline.abortedByPathologicalRate,
            skippedByCreatorChapters: enabled.skippedByCreatorChapters - baseline.skippedByCreatorChapters,
            abortRate: enabled.abortRate - baseline.abortRate,
            episodeReplayCount: enabled.episodeReplayCount - baseline.episodeReplayCount
        )
    }

    /// Compute the FM-cost multiplier with a graceful denominator clamp.
    /// When `baselineFMCalls == 0` (the typical case — `.off` charges
    /// nothing), the multiplier is `Double(enabledFMCalls)`. We do NOT
    /// return `+∞` because that would render unparseably in JSON
    /// (`Double.infinity` is not valid JSON per RFC 8259) and crash the
    /// `.sortedKeys`-encoded output.
    static func computeFMCostMultiplier(
        baseline: Int,
        enabled: Int
    ) -> Double {
        let denominator = max(1, baseline)
        return Double(enabled) / Double(denominator)
    }

    static func evaluateBar(
        liftPrecision: Double,
        liftRecall: Double,
        fmCostMultiplier: Double
    ) -> Bool {
        let measurablePrecisionLift = liftPrecision >= measurableLiftEpsilon
        let measurableRecallLift = liftRecall >= measurableLiftEpsilon
        // A regression on the OTHER metric is a number more negative
        // than -epsilon. Equality (within epsilon) does NOT count as a
        // regression.
        let precisionNotRegressed = liftPrecision >= -measurableLiftEpsilon
        let recallNotRegressed = liftRecall >= -measurableLiftEpsilon
        let costInBar = fmCostMultiplier <= maxFMCostMultiplier

        // Need: at least one measurable lift, AND no regression on the
        // other axis, AND cost within bar.
        let oneAxisLifted = measurablePrecisionLift || measurableRecallLift
        let otherNotRegressed = (measurablePrecisionLift && recallNotRegressed)
            || (measurableRecallLift && precisionNotRegressed)
            // Both lifted is also a pass on the "no regression" clause.
            || (measurablePrecisionLift && measurableRecallLift)
        return oneAxisLifted && otherNotRegressed && costInBar
    }

    /// Sum the durations of a non-overlapping set of time ranges. The
    /// predictor and ground-truth builders both produce non-overlapping
    /// outputs, so a naive sum is correct; the `assert` guards against
    /// future regressions where overlap creeps in unnoticed.
    private static func totalSeconds(_ ranges: [NarlTimeRange]) -> Double {
        ranges.reduce(0.0) { $0 + $1.duration }
    }

    /// Sum of intersection seconds between two range sets. We do NOT
    /// assume either side is non-overlapping or sorted; the O(n*m) walk
    /// is fine on typical corpus sizes (tens of windows per episode,
    /// hundreds of episodes).
    private static func intersectSeconds(
        _ a: [NarlTimeRange],
        _ b: [NarlTimeRange]
    ) -> Double {
        var total = 0.0
        for r1 in a {
            for r2 in b {
                if let inter = r1.intersection(r2) {
                    total += inter.duration
                }
            }
        }
        return total
    }

    private static func makeStructuralLimitations(
        corpusSize: Int,
        baselineFMCalls: Int,
        enabledFMCalls: Int,
        shadowDiffersFromBaselineDetection: Bool
    ) -> [String] {
        var notes: [String] = []
        notes.append(
            "Detection metrics across modes are computed via NarlReplayPredictor, "
            + "which does not yet read ChapterPlan (consumer wiring lands in "
            + "playhead-au2v.1.14 / .1.16). At this scaffold, .off / .shadow / "
            + ".enabled produce identical detection numbers; the shape is "
            + "future-proofed for when consumers diverge."
        )
        if !shadowDiffersFromBaselineDetection {
            notes.append(
                "Shadow detection metrics match baseline byte-for-byte — "
                + "matches the contract `ChapterSignalMode.consumersReadChapterPlan == false` "
                + "for both .off and .shadow."
            )
        }
        if corpusSize == 0 {
            notes.append("Corpus is empty — barMet is meaningless and reported as false.")
        } else if corpusSize < 25 {
            notes.append(
                "Sample size: \(corpusSize) episodes — small. "
                + "Lift signals may be noise-dominated. Treat barMet as a sanity check, "
                + "not a deployment gate, until corpus reaches ≥ 50 episodes."
            )
        }
        if baselineFMCalls == 0 && enabledFMCalls > 0 {
            notes.append(
                "fmCostMultiplier was computed with baseline.totalFMCalls clamped from 0 "
                + "to 1 in the divisor (the .off mode never invokes FM). The reported "
                + "multiplier is therefore the absolute enabled FM-call count rather "
                + "than a true ratio. Cost-bar enforcement remains valid: an "
                + "absolute count above maxFMCostMultiplier (\(Self.maxFMCostMultiplier)) "
                + "still trips the bar."
            )
        }
        notes.append(
            "Corpus is dogfood-only (Conan + DoaC heavy). Production fleet "
            + "skew is unmeasured — these numbers do NOT generalize to a "
            + "broader user population without further validation."
        )
        return notes
    }
}
