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
}

// MARK: - Boundary Quality Metrics

/// Measures how cleanly skip transitions land relative to ad boundaries.
struct BoundaryQualityMetrics: Sendable, Codable {
    /// Milliseconds of speech cut at skip-start (entered ad too late).
    let cutSpeechAtEntryMs: [Double]
    /// Milliseconds of speech cut at skip-end (resumed too early or late).
    let cutSpeechAtResumeMs: [Double]
    /// p50 boundary error at entry.
    let p50EntryErrorMs: Double
    /// p95 boundary error at entry.
    let p95EntryErrorMs: Double
    /// p50 boundary error at resume.
    let p50ResumeErrorMs: Double
    /// p95 boundary error at resume.
    let p95ResumeErrorMs: Double
}

// MARK: - Latency Metrics

/// Pipeline timing from audio arrival to actionable skip cue.
struct LatencyMetrics: Sendable, Codable {
    /// Time from episode start to first usable skip cue (seconds).
    let timeToFirstUsableSkip: Double?
    /// p50 banner appearance latency (ms from ad start to banner shown).
    let p50BannerLatencyMs: Double?
    /// p95 banner appearance latency.
    let p95BannerLatencyMs: Double?
    /// Mean detection pipeline latency per chunk (ms).
    let meanPipelineLatencyMs: Double
    /// p95 detection pipeline latency per chunk (ms).
    let p95PipelineLatencyMs: Double
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

    enum AudioMode: String, Sendable, Codable {
        case streamed
        case cached
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
    let condition: SimulationCondition
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
}

// MARK: - Corpus Replay Report

/// Aggregated report across all episodes in a replay run.
struct CorpusReplayReport: Sendable, Codable {
    let episodeReports: [EpisodeReplayReport]
    let aggregateDetectionQuality: DetectionQualityMetrics
    let aggregateBoundaryQuality: BoundaryQualityMetrics
    let aggregateLatency: LatencyMetrics
    let aggregateUserOverrides: UserOverrideMetrics
    let conditions: [SimulationCondition]
    let simulatorVersion: String
    let generatedAt: Date

    /// Build an aggregate report from individual episode reports.
    static func aggregate(from reports: [EpisodeReplayReport]) -> CorpusReplayReport {
        let totalFPSeconds = reports.map(\.detectionQuality.falsePositiveSkipSeconds).reduce(0, +)
        let totalFNSeconds = reports.map(\.detectionQuality.falseNegativeAdSeconds).reduce(0, +)
        let totalMissed = reports.map(\.detectionQuality.missedSegmentCount).reduce(0, +)
        let totalSpurious = reports.map(\.detectionQuality.spuriousSegmentCount).reduce(0, +)

        let meanPrecision = reports.isEmpty ? 0 : reports.map(\.detectionQuality.precision).reduce(0, +) / Double(reports.count)
        let meanRecall = reports.isEmpty ? 0 : reports.map(\.detectionQuality.recall).reduce(0, +) / Double(reports.count)
        let f1 = (meanPrecision + meanRecall) > 0
            ? 2.0 * meanPrecision * meanRecall / (meanPrecision + meanRecall) : 0

        let aggDetection = DetectionQualityMetrics(
            falsePositiveSkipSeconds: totalFPSeconds,
            falseNegativeAdSeconds: totalFNSeconds,
            precision: meanPrecision,
            recall: meanRecall,
            f1Score: f1,
            missedSegmentCount: totalMissed,
            spuriousSegmentCount: totalSpurious
        )

        let allEntryErrors = reports.flatMap(\.boundaryQuality.cutSpeechAtEntryMs)
        let allResumeErrors = reports.flatMap(\.boundaryQuality.cutSpeechAtResumeMs)

        let aggBoundary = BoundaryQualityMetrics(
            cutSpeechAtEntryMs: allEntryErrors,
            cutSpeechAtResumeMs: allResumeErrors,
            p50EntryErrorMs: percentile(allEntryErrors, 0.50),
            p95EntryErrorMs: percentile(allEntryErrors, 0.95),
            p50ResumeErrorMs: percentile(allResumeErrors, 0.50),
            p95ResumeErrorMs: percentile(allResumeErrors, 0.95)
        )

        let pipelineLatencies = reports.map(\.latency.meanPipelineLatencyMs)
        let p95Latencies = reports.map(\.latency.p95PipelineLatencyMs)
        let bannerP95s = reports.compactMap(\.latency.p95BannerLatencyMs)

        let aggLatency = LatencyMetrics(
            timeToFirstUsableSkip: reports.compactMap(\.latency.timeToFirstUsableSkip).min(),
            p50BannerLatencyMs: reports.compactMap(\.latency.p50BannerLatencyMs).isEmpty
                ? nil : percentile(reports.compactMap(\.latency.p50BannerLatencyMs), 0.50),
            p95BannerLatencyMs: bannerP95s.isEmpty ? nil : percentile(bannerP95s, 0.95),
            meanPipelineLatencyMs: pipelineLatencies.isEmpty ? 0 : pipelineLatencies.reduce(0, +) / Double(pipelineLatencies.count),
            p95PipelineLatencyMs: p95Latencies.isEmpty ? 0 : percentile(p95Latencies, 0.95)
        )

        let totalListens = reports.map(\.userOverrides.listenTapCount).reduce(0, +)
        let totalRewinds = reports.map(\.userOverrides.rewindAfterSkipCount).reduce(0, +)
        let totalSkips = reports.map { r in
            let rate = r.userOverrides.overrideRate
            guard rate > 0 else { return 0 }
            return Int(Double(r.userOverrides.listenTapCount + r.userOverrides.rewindAfterSkipCount) / rate)
        }.reduce(0, +)
        let aggOverrideRate = totalSkips > 0 ? Double(totalListens + totalRewinds) / Double(totalSkips) : 0

        let aggOverrides = UserOverrideMetrics(
            listenTapCount: totalListens,
            rewindAfterSkipCount: totalRewinds,
            overrideRate: aggOverrideRate
        )

        return CorpusReplayReport(
            episodeReports: reports,
            aggregateDetectionQuality: aggDetection,
            aggregateBoundaryQuality: aggBoundary,
            aggregateLatency: aggLatency,
            aggregateUserOverrides: aggOverrides,
            conditions: reports.map(\.condition),
            simulatorVersion: EpisodeReplayReport.currentSimulatorVersion,
            generatedAt: Date()
        )
    }
}

// MARK: - Helpers

/// Compute the p-th percentile of a sorted array.
func percentile(_ values: [Double], _ p: Double) -> Double {
    guard !values.isEmpty else { return 0 }
    let sorted = values.sorted()
    let index = p * Double(sorted.count - 1)
    let lower = Int(index.rounded(.down))
    let upper = min(lower + 1, sorted.count - 1)
    let fraction = index - Double(lower)
    return sorted[lower] * (1 - fraction) + sorted[upper] * fraction
}
