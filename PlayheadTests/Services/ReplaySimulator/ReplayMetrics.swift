// ReplayMetrics.swift
// Metric types and report format for the replay simulator evaluation harness.
//
// Tracks detection quality, skip boundary precision, latency, and user-override
// signals against a labeled ground-truth corpus.

import Foundation
@testable import Playhead

// MARK: - Metric Types

/// A single measured value with context about when and how it was captured.
struct MetricSample: Sendable, Codable {
    let name: String
    let value: Double
    let unit: MetricUnit
    let timestamp: TimeInterval
    let context: [String: String]
}

/// Units for metric values.
enum MetricUnit: String, Sendable, Codable {
    case seconds
    case milliseconds
    case percentage
    case count
    case boolean
}

// MARK: - Detection Quality Metrics

/// Aggregated detection quality against ground-truth labels.
struct DetectionQualityMetrics: Sendable, Codable {
    /// Total seconds incorrectly skipped (no ad in ground truth).
    let falsePositiveSkipSeconds: Double
    /// Total seconds of ads that were not detected.
    let falseNegativeAdSeconds: Double
    /// Recall of anchor/seed events before span refinement.
    let seedRecall: Double
    /// Number of ground-truth segments with an observed seed/anchor event.
    let seededSegmentCount: Int?
    /// Number of ground-truth segments considered when computing seed recall.
    let groundTruthSegmentCount: Int?
    /// Precision: correctly-detected ad seconds / total detected seconds.
    let precision: Double
    /// Recall: correctly-detected ad seconds / total ground-truth ad seconds.
    let recall: Double
    /// F1 score.
    let f1Score: Double
    /// Number of ground-truth ad segments fully missed (zero overlap).
    let missedSegmentCount: Int
    /// Number of detected segments with no ground-truth overlap.
    let spuriousSegmentCount: Int

    init(
        falsePositiveSkipSeconds: Double,
        falseNegativeAdSeconds: Double,
        seedRecall: Double = 0,
        seededSegmentCount: Int? = nil,
        groundTruthSegmentCount: Int? = nil,
        precision: Double,
        recall: Double,
        f1Score: Double,
        missedSegmentCount: Int,
        spuriousSegmentCount: Int
    ) {
        self.falsePositiveSkipSeconds = falsePositiveSkipSeconds
        self.falseNegativeAdSeconds = falseNegativeAdSeconds
        self.seedRecall = seedRecall
        self.seededSegmentCount = seededSegmentCount
        self.groundTruthSegmentCount = groundTruthSegmentCount
        self.precision = precision
        self.recall = recall
        self.f1Score = f1Score
        self.missedSegmentCount = missedSegmentCount
        self.spuriousSegmentCount = spuriousSegmentCount
    }
}

// MARK: - Boundary Quality Metrics

/// Measures how cleanly skip transitions land relative to ad boundaries.
struct BoundaryQualityMetrics: Sendable, Codable {
    /// Milliseconds of speech cut at skip-start (entered ad too late).
    let cutSpeechAtEntryMs: [Double]
    /// Milliseconds of speech cut at skip-end (resumed too early or late).
    let cutSpeechAtResumeMs: [Double]
    /// Signed entry errors in milliseconds (positive = late).
    let signedEntryErrorMs: [Double]
    /// Signed resume errors in milliseconds (positive = late).
    let signedResumeErrorMs: [Double]
    /// Per-segment intersection-over-union against ground truth.
    let spanIoUs: [Double]
    /// Per-segment ground-truth coverage recall.
    let coverageRecalls: [Double]
    /// Per-segment detected coverage precision.
    let coveragePrecisions: [Double]
    /// p50 boundary error at entry.
    let p50EntryErrorMs: Double
    /// p95 boundary error at entry.
    let p95EntryErrorMs: Double
    /// p50 boundary error at resume.
    let p50ResumeErrorMs: Double
    /// p95 boundary error at resume.
    let p95ResumeErrorMs: Double
    /// Median signed entry error.
    let medianSignedEntryErrorMs: Double
    /// Median signed resume error.
    let medianSignedResumeErrorMs: Double
    /// Median per-segment IoU.
    let medianSpanIoU: Double
    /// Median per-segment coverage recall.
    let medianCoverageRecall: Double
    /// Median per-segment coverage precision.
    let medianCoveragePrecision: Double

    init(
        cutSpeechAtEntryMs: [Double] = [],
        cutSpeechAtResumeMs: [Double] = [],
        signedEntryErrorMs: [Double] = [],
        signedResumeErrorMs: [Double] = [],
        spanIoUs: [Double] = [],
        coverageRecalls: [Double] = [],
        coveragePrecisions: [Double] = [],
        p50EntryErrorMs: Double = 0,
        p95EntryErrorMs: Double = 0,
        p50ResumeErrorMs: Double = 0,
        p95ResumeErrorMs: Double = 0,
        medianSignedEntryErrorMs: Double = 0,
        medianSignedResumeErrorMs: Double = 0,
        medianSpanIoU: Double = 0,
        medianCoverageRecall: Double = 0,
        medianCoveragePrecision: Double = 0
    ) {
        self.cutSpeechAtEntryMs = cutSpeechAtEntryMs
        self.cutSpeechAtResumeMs = cutSpeechAtResumeMs
        self.signedEntryErrorMs = signedEntryErrorMs
        self.signedResumeErrorMs = signedResumeErrorMs
        self.spanIoUs = spanIoUs
        self.coverageRecalls = coverageRecalls
        self.coveragePrecisions = coveragePrecisions
        self.p50EntryErrorMs = p50EntryErrorMs
        self.p95EntryErrorMs = p95EntryErrorMs
        self.p50ResumeErrorMs = p50ResumeErrorMs
        self.p95ResumeErrorMs = p95ResumeErrorMs
        self.medianSignedEntryErrorMs = medianSignedEntryErrorMs
        self.medianSignedResumeErrorMs = medianSignedResumeErrorMs
        self.medianSpanIoU = medianSpanIoU
        self.medianCoverageRecall = medianCoverageRecall
        self.medianCoveragePrecision = medianCoveragePrecision
    }
}

// MARK: - Latency Metrics

/// Pipeline timing from audio arrival to actionable skip cue.
struct LatencyMetrics: Sendable, Codable {
    /// Playback time when the first skip is actually applied (seconds).
    let timeToFirstUsableSkip: Double?
    /// Lead time from first skip-eligible confirmation to ad start (seconds).
    let leadTimeAtFirstConfirmationSeconds: Double?
    /// p50 banner appearance latency (ms from ad start to banner shown).
    let p50BannerLatencyMs: Double?
    /// p95 banner appearance latency.
    let p95BannerLatencyMs: Double?
    /// Mean detection pipeline latency per chunk (ms).
    let meanPipelineLatencyMs: Double
    /// p95 detection pipeline latency per chunk (ms).
    let p95PipelineLatencyMs: Double

    init(
        timeToFirstUsableSkip: Double? = nil,
        leadTimeAtFirstConfirmationSeconds: Double? = nil,
        p50BannerLatencyMs: Double? = nil,
        p95BannerLatencyMs: Double? = nil,
        meanPipelineLatencyMs: Double = 0,
        p95PipelineLatencyMs: Double = 0
    ) {
        self.timeToFirstUsableSkip = timeToFirstUsableSkip
        self.leadTimeAtFirstConfirmationSeconds = leadTimeAtFirstConfirmationSeconds
        self.p50BannerLatencyMs = p50BannerLatencyMs
        self.p95BannerLatencyMs = p95BannerLatencyMs
        self.meanPipelineLatencyMs = meanPipelineLatencyMs
        self.p95PipelineLatencyMs = p95PipelineLatencyMs
    }
}

/// Per-style metrics for episodes that contain multiple delivery styles.
struct DeliveryStyleMetricReport: Sendable, Codable {
    let style: GroundTruthAdSegment.DeliveryStyle
    let detectionQuality: DetectionQualityMetrics
    let boundaryQuality: BoundaryQualityMetrics
    let latency: LatencyMetrics
    let userOverrides: UserOverrideMetrics
}

// MARK: - User Override Metrics

/// Tracks simulated user corrections: Listen taps, rewind-after-skip.
struct UserOverrideMetrics: Sendable, Codable {
    /// Number of "Listen" taps (user reverted a skip).
    let listenTapCount: Int
    /// Number of rewind-after-skip events.
    let rewindAfterSkipCount: Int
    /// Manual override rate: overrides / total skips applied.
    let overrideRate: Double
}

// MARK: - Simulation Condition

/// Describes the simulated playback condition for a test run.
struct SimulationCondition: Sendable, Codable {
    /// Whether audio was streamed or fully cached.
    let audioMode: AudioMode
    /// Playback speed (0.5x to 3.0x).
    let playbackSpeed: Float
    /// Simulated user interactions during playback.
    let interactions: [SimulatedInteraction]
    /// Whether this replay models live or backfill analysis.
    let analysisPath: AnalysisPath

    init(
        audioMode: AudioMode,
        playbackSpeed: Float,
        interactions: [SimulatedInteraction],
        analysisPath: AnalysisPath = .live
    ) {
        self.audioMode = audioMode
        self.playbackSpeed = playbackSpeed
        self.interactions = interactions
        self.analysisPath = analysisPath
    }

    enum AudioMode: String, Sendable, Codable {
        case streamed
        case cached
    }

    enum AnalysisPath: String, Sendable, Codable {
        case live
        case backfill
    }
}

/// A simulated user interaction injected during replay.
struct SimulatedInteraction: Sendable, Codable {
    let type: InteractionType
    /// Playback time (in episode seconds) when the interaction occurs.
    let atTime: TimeInterval
    /// For scrub: the target position.
    let targetTime: TimeInterval?
    /// For speed change: the new speed.
    let newSpeed: Float?

    enum InteractionType: String, Sendable, Codable {
        /// User scrubs to a different position.
        case scrub
        /// User taps forward skip (30s).
        case skipForward
        /// User taps "Listen" to revert an ad skip.
        case listenTap
        /// User changes playback speed.
        case speedChange
        /// Simulate a late detection arriving after playhead passed the ad.
        case lateDetection
    }
}

// MARK: - Episode Replay Report

/// Complete metrics report for a single episode replay.
struct EpisodeReplayReport: Sendable, Codable {
    let episodeId: String
    let episodeTitle: String
    let podcastId: String
    let condition: SimulationCondition
    /// Unique delivery styles present in the episode's ground truth.
    let deliveryStyles: [GroundTruthAdSegment.DeliveryStyle]
    /// Style-specific metrics so mixed-format episodes can be sliced honestly.
    let deliveryStyleMetrics: [DeliveryStyleMetricReport]
    let detectionQuality: DetectionQualityMetrics
    let boundaryQuality: BoundaryQualityMetrics
    let latency: LatencyMetrics
    let userOverrides: UserOverrideMetrics
    /// Raw metric samples for detailed analysis.
    let samples: [MetricSample]
    /// Replay simulator version.
    let simulatorVersion: String
    /// Timestamp of this report.
    let generatedAt: Date
    /// Duration of the simulated replay (wall clock).
    let replayDurationSeconds: Double
    /// Counterfactual evaluation result, if a comparison was run.
    let counterfactualResult: CounterfactualResult?

    static let currentSimulatorVersion = "replay-sim-v1"

    init(
        episodeId: String,
        episodeTitle: String,
        podcastId: String = "",
        condition: SimulationCondition,
        deliveryStyles: [GroundTruthAdSegment.DeliveryStyle] = [],
        deliveryStyleMetrics: [DeliveryStyleMetricReport] = [],
        detectionQuality: DetectionQualityMetrics,
        boundaryQuality: BoundaryQualityMetrics,
        latency: LatencyMetrics,
        userOverrides: UserOverrideMetrics,
        samples: [MetricSample],
        simulatorVersion: String,
        generatedAt: Date,
        replayDurationSeconds: Double,
        counterfactualResult: CounterfactualResult? = nil
    ) {
        self.episodeId = episodeId
        self.episodeTitle = episodeTitle
        self.podcastId = podcastId
        self.condition = condition
        self.deliveryStyles = deliveryStyles
        self.deliveryStyleMetrics = deliveryStyleMetrics
        self.detectionQuality = detectionQuality
        self.boundaryQuality = boundaryQuality
        self.latency = latency
        self.userOverrides = userOverrides
        self.samples = samples
        self.simulatorVersion = simulatorVersion
        self.generatedAt = generatedAt
        self.replayDurationSeconds = replayDurationSeconds
        self.counterfactualResult = counterfactualResult
    }
}

// MARK: - Corpus Replay Report

/// Aggregated report across all episodes in a replay run.
struct CorpusReplayReport: Sendable, Codable {
    let episodeReports: [EpisodeReplayReport]
    let aggregateDetectionQuality: DetectionQualityMetrics
    let aggregateBoundaryQuality: BoundaryQualityMetrics
    let aggregateLatency: LatencyMetrics
    let aggregateUserOverrides: UserOverrideMetrics
    let slices: [MetricSliceReport]
    let conditions: [SimulationCondition]
    let simulatorVersion: String
    let generatedAt: Date

    init(
        episodeReports: [EpisodeReplayReport],
        aggregateDetectionQuality: DetectionQualityMetrics,
        aggregateBoundaryQuality: BoundaryQualityMetrics,
        aggregateLatency: LatencyMetrics,
        aggregateUserOverrides: UserOverrideMetrics,
        slices: [MetricSliceReport] = [],
        conditions: [SimulationCondition],
        simulatorVersion: String,
        generatedAt: Date
    ) {
        self.episodeReports = episodeReports
        self.aggregateDetectionQuality = aggregateDetectionQuality
        self.aggregateBoundaryQuality = aggregateBoundaryQuality
        self.aggregateLatency = aggregateLatency
        self.aggregateUserOverrides = aggregateUserOverrides
        self.slices = slices
        self.conditions = conditions
        self.simulatorVersion = simulatorVersion
        self.generatedAt = generatedAt
    }

    /// Build an aggregate report from individual episode reports.
    static func aggregate(from reports: [EpisodeReplayReport]) -> CorpusReplayReport {
        let aggregate = ReplayMetricsComputation.aggregateMetrics(from: reports)

        return CorpusReplayReport(
            episodeReports: reports,
            aggregateDetectionQuality: aggregate.detectionQuality,
            aggregateBoundaryQuality: aggregate.boundaryQuality,
            aggregateLatency: aggregate.latency,
            aggregateUserOverrides: aggregate.userOverrides,
            slices: ReplayMetricsComputation.makeSlices(from: reports),
            conditions: reports.map(\.condition),
            simulatorVersion: EpisodeReplayReport.currentSimulatorVersion,
            generatedAt: Date()
        )
    }
}

// MARK: - Sliced Reports

/// Dimension used for sliced corpus summaries.
enum MetricSliceDimension: String, Sendable, Codable {
    case deliveryStyle
    case podcast
    case analysisPath
}

/// Aggregated metrics for one slice of the corpus report.
struct MetricSliceReport: Sendable, Codable {
    let dimension: MetricSliceDimension
    let value: String
    let episodeIds: [String]
    let detectionQuality: DetectionQualityMetrics
    let boundaryQuality: BoundaryQualityMetrics
    let latency: LatencyMetrics
    let userOverrides: UserOverrideMetrics
}

extension DetectionQualityMetrics {
    private enum CodingKeys: String, CodingKey {
        case falsePositiveSkipSeconds
        case falseNegativeAdSeconds
        case seedRecall
        case seededSegmentCount
        case groundTruthSegmentCount
        case precision
        case recall
        case f1Score
        case missedSegmentCount
        case spuriousSegmentCount
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            falsePositiveSkipSeconds: try container.decode(Double.self, forKey: .falsePositiveSkipSeconds),
            falseNegativeAdSeconds: try container.decode(Double.self, forKey: .falseNegativeAdSeconds),
            seedRecall: try container.decodeIfPresent(Double.self, forKey: .seedRecall) ?? 0,
            seededSegmentCount: try container.decodeIfPresent(Int.self, forKey: .seededSegmentCount),
            groundTruthSegmentCount: try container.decodeIfPresent(Int.self, forKey: .groundTruthSegmentCount),
            precision: try container.decode(Double.self, forKey: .precision),
            recall: try container.decode(Double.self, forKey: .recall),
            f1Score: try container.decode(Double.self, forKey: .f1Score),
            missedSegmentCount: try container.decode(Int.self, forKey: .missedSegmentCount),
            spuriousSegmentCount: try container.decode(Int.self, forKey: .spuriousSegmentCount)
        )
    }
}

extension BoundaryQualityMetrics {
    private enum CodingKeys: String, CodingKey {
        case cutSpeechAtEntryMs
        case cutSpeechAtResumeMs
        case signedEntryErrorMs
        case signedResumeErrorMs
        case spanIoUs
        case coverageRecalls
        case coveragePrecisions
        case p50EntryErrorMs
        case p95EntryErrorMs
        case p50ResumeErrorMs
        case p95ResumeErrorMs
        case medianSignedEntryErrorMs
        case medianSignedResumeErrorMs
        case medianSpanIoU
        case medianCoverageRecall
        case medianCoveragePrecision
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            cutSpeechAtEntryMs: try container.decodeIfPresent([Double].self, forKey: .cutSpeechAtEntryMs) ?? [],
            cutSpeechAtResumeMs: try container.decodeIfPresent([Double].self, forKey: .cutSpeechAtResumeMs) ?? [],
            signedEntryErrorMs: try container.decodeIfPresent([Double].self, forKey: .signedEntryErrorMs) ?? [],
            signedResumeErrorMs: try container.decodeIfPresent([Double].self, forKey: .signedResumeErrorMs) ?? [],
            spanIoUs: try container.decodeIfPresent([Double].self, forKey: .spanIoUs) ?? [],
            coverageRecalls: try container.decodeIfPresent([Double].self, forKey: .coverageRecalls) ?? [],
            coveragePrecisions: try container.decodeIfPresent([Double].self, forKey: .coveragePrecisions) ?? [],
            p50EntryErrorMs: try container.decodeIfPresent(Double.self, forKey: .p50EntryErrorMs) ?? 0,
            p95EntryErrorMs: try container.decodeIfPresent(Double.self, forKey: .p95EntryErrorMs) ?? 0,
            p50ResumeErrorMs: try container.decodeIfPresent(Double.self, forKey: .p50ResumeErrorMs) ?? 0,
            p95ResumeErrorMs: try container.decodeIfPresent(Double.self, forKey: .p95ResumeErrorMs) ?? 0,
            medianSignedEntryErrorMs: try container.decodeIfPresent(Double.self, forKey: .medianSignedEntryErrorMs) ?? 0,
            medianSignedResumeErrorMs: try container.decodeIfPresent(Double.self, forKey: .medianSignedResumeErrorMs) ?? 0,
            medianSpanIoU: try container.decodeIfPresent(Double.self, forKey: .medianSpanIoU) ?? 0,
            medianCoverageRecall: try container.decodeIfPresent(Double.self, forKey: .medianCoverageRecall) ?? 0,
            medianCoveragePrecision: try container.decodeIfPresent(Double.self, forKey: .medianCoveragePrecision) ?? 0
        )
    }
}

extension LatencyMetrics {
    private enum CodingKeys: String, CodingKey {
        case timeToFirstUsableSkip
        case leadTimeAtFirstConfirmationSeconds
        case p50BannerLatencyMs
        case p95BannerLatencyMs
        case meanPipelineLatencyMs
        case p95PipelineLatencyMs
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            timeToFirstUsableSkip: try container.decodeIfPresent(Double.self, forKey: .timeToFirstUsableSkip),
            leadTimeAtFirstConfirmationSeconds: try container.decodeIfPresent(Double.self, forKey: .leadTimeAtFirstConfirmationSeconds),
            p50BannerLatencyMs: try container.decodeIfPresent(Double.self, forKey: .p50BannerLatencyMs),
            p95BannerLatencyMs: try container.decodeIfPresent(Double.self, forKey: .p95BannerLatencyMs),
            meanPipelineLatencyMs: try container.decodeIfPresent(Double.self, forKey: .meanPipelineLatencyMs) ?? 0,
            p95PipelineLatencyMs: try container.decodeIfPresent(Double.self, forKey: .p95PipelineLatencyMs) ?? 0
        )
    }
}

extension SimulationCondition {
    private enum CodingKeys: String, CodingKey {
        case audioMode
        case playbackSpeed
        case interactions
        case analysisPath
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            audioMode: try container.decode(AudioMode.self, forKey: .audioMode),
            playbackSpeed: try container.decode(Float.self, forKey: .playbackSpeed),
            interactions: try container.decodeIfPresent([SimulatedInteraction].self, forKey: .interactions) ?? [],
            analysisPath: try container.decodeIfPresent(AnalysisPath.self, forKey: .analysisPath) ?? .live
        )
    }
}

extension EpisodeReplayReport {
    private enum CodingKeys: String, CodingKey {
        case episodeId
        case episodeTitle
        case podcastId
        case condition
        case deliveryStyles
        case deliveryStyleMetrics
        case detectionQuality
        case boundaryQuality
        case latency
        case userOverrides
        case samples
        case simulatorVersion
        case generatedAt
        case replayDurationSeconds
        case counterfactualResult
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            episodeId: try container.decode(String.self, forKey: .episodeId),
            episodeTitle: try container.decode(String.self, forKey: .episodeTitle),
            podcastId: try container.decodeIfPresent(String.self, forKey: .podcastId) ?? "",
            condition: try container.decode(SimulationCondition.self, forKey: .condition),
            deliveryStyles: try container.decodeIfPresent([GroundTruthAdSegment.DeliveryStyle].self, forKey: .deliveryStyles) ?? [],
            deliveryStyleMetrics: try container.decodeIfPresent([DeliveryStyleMetricReport].self, forKey: .deliveryStyleMetrics) ?? [],
            detectionQuality: try container.decode(DetectionQualityMetrics.self, forKey: .detectionQuality),
            boundaryQuality: try container.decode(BoundaryQualityMetrics.self, forKey: .boundaryQuality),
            latency: try container.decode(LatencyMetrics.self, forKey: .latency),
            userOverrides: try container.decode(UserOverrideMetrics.self, forKey: .userOverrides),
            samples: try container.decodeIfPresent([MetricSample].self, forKey: .samples) ?? [],
            simulatorVersion: try container.decode(String.self, forKey: .simulatorVersion),
            generatedAt: try container.decode(Date.self, forKey: .generatedAt),
            replayDurationSeconds: try container.decode(Double.self, forKey: .replayDurationSeconds),
            counterfactualResult: try container.decodeIfPresent(CounterfactualResult.self, forKey: .counterfactualResult)
        )
    }
}

extension CorpusReplayReport {
    private enum CodingKeys: String, CodingKey {
        case episodeReports
        case aggregateDetectionQuality
        case aggregateBoundaryQuality
        case aggregateLatency
        case aggregateUserOverrides
        case slices
        case conditions
        case simulatorVersion
        case generatedAt
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            episodeReports: try container.decode([EpisodeReplayReport].self, forKey: .episodeReports),
            aggregateDetectionQuality: try container.decode(DetectionQualityMetrics.self, forKey: .aggregateDetectionQuality),
            aggregateBoundaryQuality: try container.decode(BoundaryQualityMetrics.self, forKey: .aggregateBoundaryQuality),
            aggregateLatency: try container.decode(LatencyMetrics.self, forKey: .aggregateLatency),
            aggregateUserOverrides: try container.decode(UserOverrideMetrics.self, forKey: .aggregateUserOverrides),
            slices: try container.decodeIfPresent([MetricSliceReport].self, forKey: .slices) ?? [],
            conditions: try container.decodeIfPresent([SimulationCondition].self, forKey: .conditions) ?? [],
            simulatorVersion: try container.decode(String.self, forKey: .simulatorVersion),
            generatedAt: try container.decode(Date.self, forKey: .generatedAt)
        )
    }
}

// MARK: - Helpers

/// Namespace for metric math utilities.
enum MetricMath {
    /// Compute the p-th percentile of an array using linear interpolation.
    static func percentile(_ values: [Double], _ p: Double) -> Double {
        guard !values.isEmpty else { return 0 }
        let sorted = values.sorted()
        let index = p * Double(sorted.count - 1)
        let lower = Int(index.rounded(.down))
        let upper = min(lower + 1, sorted.count - 1)
        let fraction = index - Double(lower)
        return sorted[lower] * (1 - fraction) + sorted[upper] * fraction
    }
}

/// Convenience free-function wrapper for backward compatibility with existing call sites.
func percentile(_ values: [Double], _ p: Double) -> Double {
    MetricMath.percentile(values, p)
}

// MARK: - Span Decision

/// A single ad span decision — the atomic unit that counterfactual comparison diffs against.
struct ReplaySpanDecision: Sendable, Codable, Equatable {
    /// Start of the span in episode seconds.
    let startTime: Double
    /// End of the span in episode seconds.
    let endTime: Double
    /// Pipeline confidence for this span.
    let confidence: Double
    /// Whether the pipeline classified this span as an ad.
    let isAd: Bool
    /// Tag identifying which pipeline produced this decision (e.g. "baseline", "new").
    let sourceTag: String
}

// MARK: - Frozen Trace

/// Canonical serializable artifact capturing a complete episode analysis trace.
/// Used for offline counterfactual evaluation: replay a new pipeline configuration
/// against the same inputs and diff the resulting ReplaySpanDecisions.
///
/// Schema history:
///   - `frozen-trace-v1`: original capture shape. Added fields on sub-types had
///     no `classificationTrust`, `correctionType`, or per-window scoring.
///   - `frozen-trace-v2` (playhead-narl.1 review): adds
///     `FrozenEvidenceEntry.classificationTrust`,
///     `FrozenCorrection.correctionType`, and `windowScores` (per-window
///     fused skip confidence + classificationTrust, needed for an honest
///     replay of the `MetadataPriorShift` + lexical-injection gate logic).
///     All new fields decode as optional to preserve forward/backward
///     compatibility with `frozen-trace-v1` fixtures.
///   - `frozen-trace-v2` (gtt9.8 follow-up, 2026-04-24): adds optional
///     lifecycle telemetry — `durationSec`, `analysisState`,
///     `terminalReason`, `fastTranscriptCoverageEndTime`,
///     `featureCoverageEndTime` — sourced from `asset-lifecycle-log.jsonl`
///     at capture time. Older fixtures (2026-04-22 / 2026-04-23) decode
///     with all five fields nil. No schema-version bump: the shape is
///     strictly additive and all fields are optional.
///   - `frozen-trace-v2` (gtt9.21 follow-up, 2026-04-24): adds capture
///     provenance — `detectorVersion` (mirror of
///     `AdDetectionConfig.detectorVersion` at session-start time) and
///     `buildCommitSHA` (short git SHA of the binary that produced the
///     DecisionLog, sourced from `BuildInfo.commitSHA`). Older fixtures
///     decode both as **empty string** (not nil) — the tradeoff: two
///     fewer optional unwraps in every consumer call site, in exchange
///     for a string sentinel `""` that means "absent". Empty string is
///     fine here because the fields are categorical labels, not
///     continuous values; no consumer needs to distinguish "missing"
///     from "explicitly empty" (an explicitly empty SHA would be a bug
///     in the producer, not legitimate data). See
///     `FrozenTraceProvenanceTests` for the back-compat contract.
struct FrozenTrace: Sendable, Codable {
    static let currentTraceVersion = "frozen-trace-v2"

    let episodeId: String
    let podcastId: String
    let episodeDuration: Double
    let traceVersion: String
    let capturedAt: Date

    /// Snapshot of audio feature windows at capture time.
    let featureWindows: [FrozenFeatureWindow]
    /// Transcript atoms (chunks) at capture time.
    let atoms: [FrozenAtom]
    /// Evidence catalog entries that contributed to decisions.
    let evidenceCatalog: [FrozenEvidenceEntry]
    /// User corrections recorded for this episode.
    let corrections: [FrozenCorrection]
    /// Decision events with optional explanation traces.
    let decisionEvents: [FrozenDecisionEvent]
    /// The baseline span decisions to diff against.
    let baselineReplaySpanDecisions: [ReplaySpanDecision]
    /// Whether this trace is held out from calibrator training.
    let holdoutDesignation: HoldoutDesignation
    /// Per-window replay inputs: fused skip confidence + classificationTrust,
    /// used by the narl counterfactual replay to faithfully model the
    /// `MetadataPriorShift` gate and lexical-injection boost. Empty array is
    /// a valid default when the capture didn't persist per-window decisions.
    /// (Added in `frozen-trace-v2`; defaults to `[]` when absent.)
    let windowScores: [FrozenWindowScore]
    /// Optional human-readable show label (e.g. "DoaC", "Conan"). Capture
    /// writes the mapped label when known so the harness doesn't need to
    /// heuristically match on podcastId substrings. Absent = fall back to
    /// podcastId-based lookup in `NarlShowLabels.json`.
    /// (Added in `frozen-trace-v2`.)
    let showLabel: String?

    // MARK: - Lifecycle telemetry (gtt9.8 follow-up)

    /// Episode duration (seconds) as recorded on the terminal lifecycle
    /// transition. Distinct from `episodeDuration`, which the corpus
    /// builder derives from the furthest-out decision window. Nil when
    /// no lifecycle log was available (pre-9.8 captures).
    let durationSec: Double?
    /// Terminal `SessionState.rawValue` the asset reached — e.g.
    /// `completeFull`, `completeFeatureOnly`, `completeTranscriptPartial`,
    /// `backfill` (when stalled), `failedTranscript`, `failedFeature`,
    /// `cancelledBudget`. Nil when the capture has no lifecycle log.
    let analysisState: String?
    /// Human-readable classifier output that accompanied the terminal
    /// transition (e.g. `"full coverage: transcript 1.000, feature 1.000"`).
    /// Nil when the asset never reached a terminal row OR the capture
    /// predates gtt9.8.
    let terminalReason: String?
    /// Transcript coverage end time (seconds) at the terminal lifecycle
    /// row. Named `fast` to mirror the production-side field that tracks
    /// the fast-path transcript pipeline. Nil when absent from the
    /// terminal row (e.g. `queued → spooling` transitions never carry
    /// a transcript coverage value).
    let fastTranscriptCoverageEndTime: Double?
    /// Feature coverage end time (seconds) at the terminal lifecycle
    /// row. Nil when absent.
    let featureCoverageEndTime: Double?

    // MARK: - Capture provenance (gtt9.21)

    /// Detector version recorded by the device when this fixture was
    /// captured (mirrors `AdDetectionConfig.detectorVersion` at session-
    /// start time). Empty string when the capture predates gtt9.21
    /// (back-compat sentinel for "absent").
    let detectorVersion: String
    /// Short git SHA of the binary that produced the DecisionLog this
    /// fixture was built from (mirrors `BuildInfo.commitSHA` at capture
    /// time). Empty string when the capture predates gtt9.21 — a
    /// non-empty value is `"unknown"` (the runtime fallback when the
    /// build phase couldn't resolve a SHA), never an empty string.
    let buildCommitSHA: String

    init(
        episodeId: String,
        podcastId: String,
        episodeDuration: Double,
        traceVersion: String,
        capturedAt: Date,
        featureWindows: [FrozenFeatureWindow],
        atoms: [FrozenAtom],
        evidenceCatalog: [FrozenEvidenceEntry],
        corrections: [FrozenCorrection],
        decisionEvents: [FrozenDecisionEvent],
        baselineReplaySpanDecisions: [ReplaySpanDecision],
        holdoutDesignation: HoldoutDesignation,
        windowScores: [FrozenWindowScore] = [],
        showLabel: String? = nil,
        durationSec: Double? = nil,
        analysisState: String? = nil,
        terminalReason: String? = nil,
        fastTranscriptCoverageEndTime: Double? = nil,
        featureCoverageEndTime: Double? = nil,
        detectorVersion: String = "",
        buildCommitSHA: String = ""
    ) {
        self.episodeId = episodeId
        self.podcastId = podcastId
        self.episodeDuration = episodeDuration
        self.traceVersion = traceVersion
        self.capturedAt = capturedAt
        self.featureWindows = featureWindows
        self.atoms = atoms
        self.evidenceCatalog = evidenceCatalog
        self.corrections = corrections
        self.decisionEvents = decisionEvents
        self.baselineReplaySpanDecisions = baselineReplaySpanDecisions
        self.holdoutDesignation = holdoutDesignation
        self.windowScores = windowScores
        self.showLabel = showLabel
        self.durationSec = durationSec
        self.analysisState = analysisState
        self.terminalReason = terminalReason
        self.fastTranscriptCoverageEndTime = fastTranscriptCoverageEndTime
        self.featureCoverageEndTime = featureCoverageEndTime
        self.detectorVersion = detectorVersion
        self.buildCommitSHA = buildCommitSHA
    }

    // MARK: - Codable (manual to keep new fields optional)

    private enum CodingKeys: String, CodingKey {
        case episodeId
        case podcastId
        case episodeDuration
        case traceVersion
        case capturedAt
        case featureWindows
        case atoms
        case evidenceCatalog
        case corrections
        case decisionEvents
        case baselineReplaySpanDecisions
        case holdoutDesignation
        case windowScores
        case showLabel
        case durationSec
        case analysisState
        case terminalReason
        case fastTranscriptCoverageEndTime
        case featureCoverageEndTime
        // gtt9.21: capture provenance.
        case detectorVersion
        case buildCommitSHA
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            episodeId: try c.decode(String.self, forKey: .episodeId),
            podcastId: try c.decode(String.self, forKey: .podcastId),
            episodeDuration: try c.decode(Double.self, forKey: .episodeDuration),
            traceVersion: try c.decode(String.self, forKey: .traceVersion),
            capturedAt: try c.decode(Date.self, forKey: .capturedAt),
            featureWindows: try c.decodeIfPresent([FrozenFeatureWindow].self, forKey: .featureWindows) ?? [],
            atoms: try c.decodeIfPresent([FrozenAtom].self, forKey: .atoms) ?? [],
            evidenceCatalog: try c.decodeIfPresent([FrozenEvidenceEntry].self, forKey: .evidenceCatalog) ?? [],
            corrections: try c.decodeIfPresent([FrozenCorrection].self, forKey: .corrections) ?? [],
            decisionEvents: try c.decodeIfPresent([FrozenDecisionEvent].self, forKey: .decisionEvents) ?? [],
            baselineReplaySpanDecisions: try c.decodeIfPresent([ReplaySpanDecision].self, forKey: .baselineReplaySpanDecisions) ?? [],
            holdoutDesignation: try c.decode(HoldoutDesignation.self, forKey: .holdoutDesignation),
            windowScores: try c.decodeIfPresent([FrozenWindowScore].self, forKey: .windowScores) ?? [],
            showLabel: try c.decodeIfPresent(String.self, forKey: .showLabel),
            durationSec: try c.decodeIfPresent(Double.self, forKey: .durationSec),
            analysisState: try c.decodeIfPresent(String.self, forKey: .analysisState),
            terminalReason: try c.decodeIfPresent(String.self, forKey: .terminalReason),
            fastTranscriptCoverageEndTime: try c.decodeIfPresent(Double.self, forKey: .fastTranscriptCoverageEndTime),
            featureCoverageEndTime: try c.decodeIfPresent(Double.self, forKey: .featureCoverageEndTime),
            // gtt9.21: empty string is the legitimate "absent" sentinel
            // for back-compat. See the schema-history note above for
            // why we picked empty-string over Optional<String>.
            detectorVersion: try c.decodeIfPresent(String.self, forKey: .detectorVersion) ?? "",
            buildCommitSHA: try c.decodeIfPresent(String.self, forKey: .buildCommitSHA) ?? ""
        )
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(episodeId, forKey: .episodeId)
        try c.encode(podcastId, forKey: .podcastId)
        try c.encode(episodeDuration, forKey: .episodeDuration)
        try c.encode(traceVersion, forKey: .traceVersion)
        try c.encode(capturedAt, forKey: .capturedAt)
        try c.encode(featureWindows, forKey: .featureWindows)
        try c.encode(atoms, forKey: .atoms)
        try c.encode(evidenceCatalog, forKey: .evidenceCatalog)
        try c.encode(corrections, forKey: .corrections)
        try c.encode(decisionEvents, forKey: .decisionEvents)
        try c.encode(baselineReplaySpanDecisions, forKey: .baselineReplaySpanDecisions)
        try c.encode(holdoutDesignation, forKey: .holdoutDesignation)
        try c.encode(windowScores, forKey: .windowScores)
        try c.encodeIfPresent(showLabel, forKey: .showLabel)
        try c.encodeIfPresent(durationSec, forKey: .durationSec)
        try c.encodeIfPresent(analysisState, forKey: .analysisState)
        try c.encodeIfPresent(terminalReason, forKey: .terminalReason)
        try c.encodeIfPresent(fastTranscriptCoverageEndTime, forKey: .fastTranscriptCoverageEndTime)
        try c.encodeIfPresent(featureCoverageEndTime, forKey: .featureCoverageEndTime)
        // gtt9.21: always emit the keys (even when empty) so a freshly
        // captured fixture's wire format is unambiguously self-
        // describing. A test that round-trips an empty fixture would
        // otherwise drop the keys silently and look identical to a pre-
        // gtt9.21 capture.
        try c.encode(detectorVersion, forKey: .detectorVersion)
        try c.encode(buildCommitSHA, forKey: .buildCommitSHA)
    }

    /// Return a copy with a different holdout designation.
    /// Centralizes the copy logic so new fields don't get silently dropped.
    func withHoldoutDesignation(_ designation: HoldoutDesignation) -> FrozenTrace {
        FrozenTrace(
            episodeId: episodeId,
            podcastId: podcastId,
            episodeDuration: episodeDuration,
            traceVersion: traceVersion,
            capturedAt: capturedAt,
            featureWindows: featureWindows,
            atoms: atoms,
            evidenceCatalog: evidenceCatalog,
            corrections: corrections,
            decisionEvents: decisionEvents,
            baselineReplaySpanDecisions: baselineReplaySpanDecisions,
            holdoutDesignation: designation,
            windowScores: windowScores,
            showLabel: showLabel,
            durationSec: durationSec,
            analysisState: analysisState,
            terminalReason: terminalReason,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
            featureCoverageEndTime: featureCoverageEndTime,
            detectorVersion: detectorVersion,
            buildCommitSHA: buildCommitSHA
        )
    }

    // MARK: - Nested Codable types

    struct FrozenFeatureWindow: Sendable, Codable {
        let startTime: Double
        let endTime: Double
        let rms: Double
        let spectralFlux: Double
        let musicProbability: Double
    }

    struct FrozenAtom: Sendable, Codable {
        let startTime: Double
        let endTime: Double
        let text: String
    }

    /// Per-source evidence entry with (v2) per-window classificationTrust.
    /// `classificationTrust` is what the production
    /// `EvidenceLedgerEntry.classificationTrust` carried at capture time —
    /// 0.0 when absent (e.g. sources that don't score metadata trust).
    /// playhead-epfk: optional `subSource` carries the producer label
    /// for sources that have multiple distinct backends under one
    /// umbrella. Today only `.catalog` populates it (`transcriptCatalog`
    /// for `EvidenceCatalogBuilder`'s sponsor tokens versus
    /// `fingerprintStore` for `AdCatalogStore` matches). Absent on v1
    /// fixtures → `nil`; readers that don't care can ignore the key.
    struct FrozenEvidenceEntry: Sendable, Codable {
        let source: String
        let weight: Double
        let windowStart: Double
        let windowEnd: Double
        /// v2: classificationTrust at decision time. Absent = 0.0.
        let classificationTrust: Double
        /// playhead-epfk: optional disambiguator for `.catalog`-sourced
        /// entries. `nil` on every other source and on pre-epfk fixtures.
        let subSource: String?

        init(
            source: String,
            weight: Double,
            windowStart: Double,
            windowEnd: Double,
            classificationTrust: Double = 0.0,
            subSource: String? = nil
        ) {
            self.source = source
            self.weight = weight
            self.windowStart = windowStart
            self.windowEnd = windowEnd
            self.classificationTrust = classificationTrust
            self.subSource = subSource
        }

        private enum CodingKeys: String, CodingKey {
            case source, weight, windowStart, windowEnd, classificationTrust, subSource
        }

        init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.init(
                source: try c.decode(String.self, forKey: .source),
                weight: try c.decode(Double.self, forKey: .weight),
                windowStart: try c.decode(Double.self, forKey: .windowStart),
                windowEnd: try c.decode(Double.self, forKey: .windowEnd),
                classificationTrust: try c.decodeIfPresent(Double.self, forKey: .classificationTrust) ?? 0.0,
                subSource: try c.decodeIfPresent(String.self, forKey: .subSource)
            )
        }

        // playhead-epfk: explicit encode so `subSource` is omitted from the
        // JSON when nil (matching the v1 fixture wire shape) rather than
        // emitted as `null`. Auto-synth'd Codable would always emit the
        // key, which would break byte-equal round-trip tests on pre-epfk
        // fixtures.
        func encode(to encoder: Encoder) throws {
            var c = encoder.container(keyedBy: CodingKeys.self)
            try c.encode(source, forKey: .source)
            try c.encode(weight, forKey: .weight)
            try c.encode(windowStart, forKey: .windowStart)
            try c.encode(windowEnd, forKey: .windowEnd)
            try c.encode(classificationTrust, forKey: .classificationTrust)
            try c.encodeIfPresent(subSource, forKey: .subSource)
        }
    }

    /// User correction. `correctionType` carries the production
    /// `CorrectionType.rawValue` verbatim (v2); when present, the harness
    /// prefers it over the heuristic `source`-substring classifier.
    struct FrozenCorrection: Sendable, Codable {
        let source: String
        let scope: String
        let createdAt: Double
        /// v2: raw value of production `CorrectionType` (e.g. "falsePositive",
        /// "falseNegative", "startTooEarly"). Absent on v1 fixtures.
        let correctionType: String?

        init(
            source: String,
            scope: String,
            createdAt: Double,
            correctionType: String? = nil
        ) {
            self.source = source
            self.scope = scope
            self.createdAt = createdAt
            self.correctionType = correctionType
        }

        private enum CodingKeys: String, CodingKey {
            case source, scope, createdAt, correctionType
        }

        init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.init(
                source: try c.decode(String.self, forKey: .source),
                scope: try c.decode(String.self, forKey: .scope),
                createdAt: try c.decode(Double.self, forKey: .createdAt),
                correctionType: try c.decodeIfPresent(String.self, forKey: .correctionType)
            )
        }
    }

    struct FrozenDecisionEvent: Sendable, Codable {
        let windowId: String
        let proposalConfidence: Double
        let skipConfidence: Double
        let eligibilityGate: String
        let policyAction: String
        /// Serialized explanation from DecisionExplanation system (ef2.1.4).
        let explanationJSON: String?
    }

    /// Per-window replay input: fused skip confidence + classificationTrust
    /// at decision time, with a flag for metadata presence. Captured from
    /// the production decision-log (one row per sliding detection window).
    /// The predictor iterates these to model the prior-shift + lexical-
    /// injection gate transitions under `.allEnabled`.
    /// (Introduced in `frozen-trace-v2` for playhead-narl.1.)
    struct FrozenWindowScore: Sendable, Codable, Equatable {
        let windowStart: Double
        let windowEnd: Double
        /// Post-fusion skip confidence at capture time (under `.default`).
        let fusedSkipConfidence: Double
        /// Max classificationTrust across ledger entries for this window —
        /// matches the aggregate metadataTrust scalar that
        /// `MetadataPriorShift.isShiftActive(metadataTrust:)` consumes in
        /// production. Sources that don't score trust contribute 0.
        let classificationTrust: Double
        /// Whether the episode metadata injector would have contributed
        /// to this window — true iff the window has any source="metadata"
        /// ledger entry OR a metadata-origin lexicon boost. Used by the
        /// replay to know where lexical injection could have fired.
        let hasMetadataEvidence: Bool
        /// Whether the capture's `.default` path marked this window as an ad
        /// (post-gate). Replayed `.default` predictions rely on this.
        let isAdUnderDefault: Bool
    }
}

// MARK: - Holdout Designation

/// Whether a frozen trace is reserved for trust validation (holdout)
/// or available for calibrator training.
enum HoldoutDesignation: String, Sendable, Codable {
    case training
    case holdout
}

// MARK: - Trace Corpus Utilities

/// Utilities for partitioning and filtering trace corpora.
enum TraceCorpus {
    /// Return only training-designated traces.
    static func filterTraining(_ traces: [FrozenTrace]) -> [FrozenTrace] {
        traces.filter { $0.holdoutDesignation == .training }
    }

    /// Return only holdout-designated traces.
    static func filterHoldout(_ traces: [FrozenTrace]) -> [FrozenTrace] {
        traces.filter { $0.holdoutDesignation == .holdout }
    }

    /// Deterministically designate a fraction of traces as holdout.
    /// Uses a seeded PRNG for reproducibility.
    static func designateHoldout(
        _ traces: [FrozenTrace],
        fraction: Double,
        seed: UInt64
    ) -> [FrozenTrace] {
        guard !traces.isEmpty else { return [] }
        let holdoutCount = Int((Double(traces.count) * fraction).rounded())
        // Deterministic shuffle: hash episodeId with seed for stable ordering.
        let sorted = traces.enumerated().sorted { lhs, rhs in
            let lhsHash = stableHash(lhs.element.episodeId, seed: seed)
            let rhsHash = stableHash(rhs.element.episodeId, seed: seed)
            return lhsHash < rhsHash
        }
        return sorted.enumerated().map { index, pair in
            let designation: HoldoutDesignation = index < holdoutCount ? .holdout : .training
            return pair.element.withHoldoutDesignation(designation)
        }
    }

    /// Simple deterministic hash for stable ordering.
    private static func stableHash(_ string: String, seed: UInt64) -> UInt64 {
        var hash: UInt64 = seed
        for byte in string.utf8 {
            hash = hash &* 31 &+ UInt64(byte)
        }
        return hash
    }
}

// MARK: - Counterfactual Metrics

/// Metrics comparing a new pipeline configuration against a frozen baseline.
struct CounterfactualMetrics: Sendable, Codable {
    /// Weighted decision regret: sum of |confidenceDelta| for flipped decisions,
    /// normalized by total span count. Zero means no regressions.
    let counterfactualRegret: Double
    /// Shift in mean confidence between new and baseline decisions.
    /// Negative means the new pipeline is less confident on average.
    let scoreDistributionShift: Double
    /// Per-source Brier-like calibration error. Key is source name (e.g. "fm", "lexical").
    /// Value is mean squared error between evidence weight and actual outcome.
    let perSourceCalibrationError: [String: Double]
    /// Fraction of spans where baseline and new pipeline disagree on isAd.
    let shadowLiveDisagreementRate: Double
}

// MARK: - Span Decision Diff

/// Per-span comparison between baseline and new pipeline decisions.
struct ReplaySpanDecisionDiff: Sendable, Codable {
    let startTime: Double
    let endTime: Double
    let baselineConfidence: Double
    let newConfidence: Double
    let baselineIsAd: Bool
    let newIsAd: Bool
    /// newConfidence - baselineConfidence.
    let confidenceDelta: Double
    /// Whether the isAd classification flipped.
    let decisionFlipped: Bool
}

// MARK: - Counterfactual Result

/// Complete result of running a counterfactual comparison on a single trace.
struct CounterfactualResult: Sendable, Codable {
    let traceEpisodeId: String
    let diffs: [ReplaySpanDecisionDiff]
    let metrics: CounterfactualMetrics
}

// MARK: - Counterfactual Evaluator

/// Runs counterfactual comparison: diffs new pipeline ReplaySpanDecisions against
/// baseline decisions stored in a FrozenTrace, and computes regression metrics.
enum CounterfactualEvaluator {

    /// Compare new decisions against the baseline stored in the trace.
    /// Spans are matched by index position (assumes aligned time ordering).
    /// Unmatched tails (count mismatch) are treated as implicit disagreements.
    static func compare(
        trace: FrozenTrace,
        newDecisions: [ReplaySpanDecision]
    ) -> CounterfactualResult {
        let baseline = trace.baselineReplaySpanDecisions

        // Match spans by index (assumes aligned ordering).
        // Unmatched tails (count mismatch) are treated as implicit disagreements.
        let totalSpanCount = max(baseline.count, newDecisions.count)
        guard totalSpanCount > 0 else {
            return CounterfactualResult(
                traceEpisodeId: trace.episodeId,
                diffs: [],
                metrics: CounterfactualMetrics(
                    counterfactualRegret: 0,
                    scoreDistributionShift: 0,
                    perSourceCalibrationError: [:],
                    shadowLiveDisagreementRate: 0
                )
            )
        }

        var diffs: [ReplaySpanDecisionDiff] = []
        var flippedCount = 0
        var totalRegret = 0.0

        // Paired spans: both baseline and new exist at this index.
        let pairedCount = min(baseline.count, newDecisions.count)
        for i in 0..<pairedCount {
            let b = baseline[i]
            let n = newDecisions[i]
            let delta = n.confidence - b.confidence
            let flipped = b.isAd != n.isAd
            if flipped {
                flippedCount += 1
                totalRegret += abs(delta)
            }
            diffs.append(ReplaySpanDecisionDiff(
                startTime: b.startTime,
                endTime: b.endTime,
                baselineConfidence: b.confidence,
                newConfidence: n.confidence,
                baselineIsAd: b.isAd,
                newIsAd: n.isAd,
                confidenceDelta: delta,
                decisionFlipped: flipped
            ))
        }

        // Unmatched baseline spans (new pipeline dropped them).
        for i in pairedCount..<baseline.count {
            let b = baseline[i]
            flippedCount += 1
            totalRegret += b.confidence
            diffs.append(ReplaySpanDecisionDiff(
                startTime: b.startTime,
                endTime: b.endTime,
                baselineConfidence: b.confidence,
                newConfidence: 0,
                baselineIsAd: b.isAd,
                newIsAd: false,
                confidenceDelta: -b.confidence,
                decisionFlipped: b.isAd
            ))
        }

        // Unmatched new spans (new pipeline added them).
        for i in pairedCount..<newDecisions.count {
            let n = newDecisions[i]
            if n.isAd {
                flippedCount += 1
                totalRegret += n.confidence
            }
            diffs.append(ReplaySpanDecisionDiff(
                startTime: n.startTime,
                endTime: n.endTime,
                baselineConfidence: 0,
                newConfidence: n.confidence,
                baselineIsAd: false,
                newIsAd: n.isAd,
                confidenceDelta: n.confidence,
                decisionFlipped: n.isAd
            ))
        }

        let baselineMeanConf = baseline.isEmpty ? 0 : baseline.map(\.confidence).reduce(0, +) / Double(baseline.count)
        let newMeanConf = newDecisions.isEmpty ? 0 : newDecisions.map(\.confidence).reduce(0, +) / Double(newDecisions.count)
        let shift = (baseline.isEmpty && newDecisions.isEmpty) ? 0 : newMeanConf - baselineMeanConf

        let regret = totalRegret / Double(totalSpanCount)
        let disagreement = Double(flippedCount) / Double(totalSpanCount)

        return CounterfactualResult(
            traceEpisodeId: trace.episodeId,
            diffs: diffs,
            metrics: CounterfactualMetrics(
                counterfactualRegret: regret,
                scoreDistributionShift: shift,
                perSourceCalibrationError: computeCalibrationError(trace: trace),
                shadowLiveDisagreementRate: disagreement
            )
        )
    }

    /// Compute per-source Brier-like calibration error against the **baseline** decisions.
    /// This measures how well the evidence weights in the frozen trace predicted the
    /// baseline outcomes — it is a fixed reference metric, not a counterfactual comparison.
    /// The `newDecisions` parameter is intentionally not used here; per-source calibration
    /// against new decisions would require re-running evidence extraction, which is outside
    /// the scope of the counterfactual evaluator.
    private static func computeCalibrationError(
        trace: FrozenTrace
    ) -> [String: Double] {
        // Group evidence entries by source.
        var sourceErrors: [String: [Double]] = [:]
        for entry in trace.evidenceCatalog {
            // Find baseline span overlapping this evidence window.
            let outcome: Double
            if let span = trace.baselineReplaySpanDecisions.first(where: { s in
                s.startTime <= entry.windowEnd && s.endTime >= entry.windowStart
            }) {
                outcome = span.isAd ? 1.0 : 0.0
            } else {
                outcome = 0.0
            }
            let brierTerm = (entry.weight - outcome) * (entry.weight - outcome)
            sourceErrors[entry.source, default: []].append(brierTerm)
        }
        return sourceErrors.mapValues { errors in
            errors.isEmpty ? 0 : errors.reduce(0, +) / Double(errors.count)
        }
    }
}
