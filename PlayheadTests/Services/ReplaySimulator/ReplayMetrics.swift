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
        replayDurationSeconds: Double
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
            replayDurationSeconds: try container.decode(Double.self, forKey: .replayDurationSeconds)
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
