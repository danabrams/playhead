// ReplayMetricsComputation.swift
// Pure metric helpers shared by the replay simulator driver and the corpus
// aggregation layer.

import Foundation
@testable import Playhead

enum ReplayMetricsComputation {

    static func detectionQuality(
        groundTruth: [GroundTruthAdSegment],
        detected: [AdWindow],
        episodeDuration: TimeInterval,
        seededGroundTruthIndices: [Int] = []
    ) -> DetectionQualityMetrics {
        let resolution = 0.1
        let totalSamples = Int(episodeDuration / resolution)
        let seededIndices = Set(seededGroundTruthIndices)

        var gtMask = [Bool](repeating: false, count: totalSamples)
        var detMask = [Bool](repeating: false, count: totalSamples)

        for seg in groundTruth {
            let startIdx = max(0, Int(seg.startTime / resolution))
            let endIdx = min(totalSamples, Int(seg.endTime / resolution))
            for i in startIdx..<endIdx { gtMask[i] = true }
        }

        for win in detected {
            let startIdx = max(0, Int(win.startTime / resolution))
            let endIdx = min(totalSamples, Int(win.endTime / resolution))
            for i in startIdx..<endIdx { detMask[i] = true }
        }

        var truePos = 0, falsePos = 0, falseNeg = 0
        for i in 0..<totalSamples {
            if detMask[i] && gtMask[i] { truePos += 1 }
            if detMask[i] && !gtMask[i] { falsePos += 1 }
            if !detMask[i] && gtMask[i] { falseNeg += 1 }
        }

        let fpSeconds = Double(falsePos) * resolution
        let fnSeconds = Double(falseNeg) * resolution
        let precision = truePos + falsePos > 0 ? Double(truePos) / Double(truePos + falsePos) : 0
        let recall = truePos + falseNeg > 0 ? Double(truePos) / Double(truePos + falseNeg) : 0
        let f1 = (precision + recall) > 0 ? 2 * precision * recall / (precision + recall) : 0

        let missedCount = groundTruth.filter { seg in
            !detected.contains { win in
                max(seg.startTime, win.startTime) < min(seg.endTime, win.endTime)
            }
        }.count

        let spuriousCount = detected.filter { win in
            !groundTruth.contains { seg in
                max(seg.startTime, win.startTime) < min(seg.endTime, win.endTime)
            }
        }.count

        let seededCount = groundTruth.indices.filter { seededIndices.contains($0) }.count
        let seedRecall = groundTruth.isEmpty ? 0 : Double(seededCount) / Double(groundTruth.count)

        return DetectionQualityMetrics(
            falsePositiveSkipSeconds: fpSeconds,
            falseNegativeAdSeconds: fnSeconds,
            seedRecall: seedRecall,
            seededSegmentCount: seededCount,
            groundTruthSegmentCount: groundTruth.count,
            precision: precision,
            recall: recall,
            f1Score: f1,
            missedSegmentCount: missedCount,
            spuriousSegmentCount: spuriousCount
        )
    }

    static func boundaryQuality(
        groundTruth: [GroundTruthAdSegment],
        detected: [AdWindow]
    ) -> BoundaryQualityMetrics {
        var cutSpeechAtEntryMs: [Double] = []
        var cutSpeechAtResumeMs: [Double] = []
        var signedEntryErrorMs: [Double] = []
        var signedResumeErrorMs: [Double] = []
        var spanIoUs: [Double] = []
        var coverageRecalls: [Double] = []
        var coveragePrecisions: [Double] = []

        for gt in groundTruth {
            let overlaps = detected.filter { win in
                max(gt.startTime, win.startTime) < min(gt.endTime, win.endTime)
            }

            guard let bestMatch = overlaps.max(by: { lhs, rhs in
                let lhsOverlap = min(gt.endTime, lhs.endTime) - max(gt.startTime, lhs.startTime)
                let rhsOverlap = min(gt.endTime, rhs.endTime) - max(gt.startTime, rhs.startTime)
                if lhsOverlap != rhsOverlap { return lhsOverlap < rhsOverlap }
                return lhs.startTime > rhs.startTime
            }) else {
                spanIoUs.append(0)
                coverageRecalls.append(0)
                coveragePrecisions.append(0)
                continue
            }

            let overlap = max(0, min(gt.endTime, bestMatch.endTime) - max(gt.startTime, bestMatch.startTime))
            let gtDuration = max(0, gt.endTime - gt.startTime)
            let detectedDuration = max(0, bestMatch.endTime - bestMatch.startTime)
            let union = gtDuration + detectedDuration - overlap

            let signedEntry = (bestMatch.startTime - gt.startTime) * 1000
            let signedResume = (bestMatch.endTime - gt.endTime) * 1000

            cutSpeechAtEntryMs.append(abs(signedEntry))
            cutSpeechAtResumeMs.append(abs(signedResume))
            signedEntryErrorMs.append(signedEntry)
            signedResumeErrorMs.append(signedResume)
            spanIoUs.append(union > 0 ? overlap / union : 0)
            coverageRecalls.append(gtDuration > 0 ? overlap / gtDuration : 0)
            coveragePrecisions.append(detectedDuration > 0 ? overlap / detectedDuration : 0)
        }

        return BoundaryQualityMetrics(
            cutSpeechAtEntryMs: cutSpeechAtEntryMs,
            cutSpeechAtResumeMs: cutSpeechAtResumeMs,
            signedEntryErrorMs: signedEntryErrorMs,
            signedResumeErrorMs: signedResumeErrorMs,
            spanIoUs: spanIoUs,
            coverageRecalls: coverageRecalls,
            coveragePrecisions: coveragePrecisions,
            p50EntryErrorMs: percentile(cutSpeechAtEntryMs, 0.50),
            p95EntryErrorMs: percentile(cutSpeechAtEntryMs, 0.95),
            p50ResumeErrorMs: percentile(cutSpeechAtResumeMs, 0.50),
            p95ResumeErrorMs: percentile(cutSpeechAtResumeMs, 0.95),
            medianSignedEntryErrorMs: percentile(signedEntryErrorMs, 0.50),
            medianSignedResumeErrorMs: percentile(signedResumeErrorMs, 0.50),
            medianSpanIoU: percentile(spanIoUs, 0.50),
            medianCoverageRecall: percentile(coverageRecalls, 0.50),
            medianCoveragePrecision: percentile(coveragePrecisions, 0.50)
        )
    }

    static func latencyMetrics(
        timeToFirstUsableSkip: Double?,
        leadTimeAtFirstConfirmationSeconds: Double?,
        pipelineLatencies: [Double],
        bannerLatencies: [Double]
    ) -> LatencyMetrics {
        LatencyMetrics(
            timeToFirstUsableSkip: timeToFirstUsableSkip,
            leadTimeAtFirstConfirmationSeconds: leadTimeAtFirstConfirmationSeconds,
            p50BannerLatencyMs: bannerLatencies.isEmpty ? nil : percentile(bannerLatencies, 0.50),
            p95BannerLatencyMs: bannerLatencies.isEmpty ? nil : percentile(bannerLatencies, 0.95),
            meanPipelineLatencyMs: pipelineLatencies.isEmpty ? 0 : pipelineLatencies.reduce(0, +) / Double(pipelineLatencies.count),
            p95PipelineLatencyMs: pipelineLatencies.isEmpty ? 0 : percentile(pipelineLatencies, 0.95)
        )
    }

    static func aggregateMetrics(from reports: [EpisodeReplayReport]) -> AggregatedReplayMetrics {
        aggregateMetrics(from: reports.map(metricInput(from:)))
    }

    static func makeSlices(from reports: [EpisodeReplayReport]) -> [MetricSliceReport] {
        var grouped: [MetricSliceDimension: [String: [EpisodeReplayReport]]] = [:]
        var deliveryStyleGrouped: [String: [(episodeId: String, metrics: DeliveryStyleMetricReport)]] = [:]

        func append(_ report: EpisodeReplayReport, dimension: MetricSliceDimension, value: String) {
            var dimensionGroups = grouped[dimension, default: [:]]
            var sliceReports = dimensionGroups[value, default: []]
            sliceReports.append(report)
            dimensionGroups[value] = sliceReports
            grouped[dimension] = dimensionGroups
        }

        func appendDeliveryStyle(_ styleMetrics: DeliveryStyleMetricReport, episodeId: String) {
            var metrics = deliveryStyleGrouped[styleMetrics.style.rawValue, default: []]
            metrics.append((episodeId: episodeId, metrics: styleMetrics))
            deliveryStyleGrouped[styleMetrics.style.rawValue] = metrics
        }

        for report in reports {
            append(report, dimension: .podcast, value: report.podcastId)
            append(report, dimension: .analysisPath, value: report.condition.analysisPath.rawValue)

            if !report.deliveryStyleMetrics.isEmpty {
                for styleMetrics in report.deliveryStyleMetrics {
                    appendDeliveryStyle(styleMetrics, episodeId: report.episodeId)
                }
            } else if report.deliveryStyles.count == 1, let style = report.deliveryStyles.first {
                appendDeliveryStyle(
                    DeliveryStyleMetricReport(
                        style: style,
                        detectionQuality: report.detectionQuality,
                        boundaryQuality: report.boundaryQuality,
                        latency: report.latency,
                        userOverrides: report.userOverrides
                    ),
                    episodeId: report.episodeId
                )
            }
        }

        var slices: [MetricSliceReport] = []
        for (dimension, values) in grouped {
            for (value, sliceReports) in values {
                let aggregate = aggregateMetrics(from: sliceReports)
                slices.append(
                    MetricSliceReport(
                        dimension: dimension,
                        value: value,
                        episodeIds: sliceReports.map(\.episodeId).sorted(),
                        detectionQuality: aggregate.detectionQuality,
                        boundaryQuality: aggregate.boundaryQuality,
                        latency: aggregate.latency,
                        userOverrides: aggregate.userOverrides
                    )
                )
            }
        }

        for (value, sliceMetrics) in deliveryStyleGrouped {
            let aggregate = aggregateMetrics(from: sliceMetrics.map { metricInput(from: $0.metrics) })
            slices.append(
                MetricSliceReport(
                    dimension: .deliveryStyle,
                    value: value,
                    episodeIds: Array(Set(sliceMetrics.map(\.episodeId))).sorted(),
                    detectionQuality: aggregate.detectionQuality,
                    boundaryQuality: aggregate.boundaryQuality,
                    latency: aggregate.latency,
                    userOverrides: aggregate.userOverrides
                )
            )
        }

        return slices.sorted {
            if $0.dimension != $1.dimension { return $0.dimension.rawValue < $1.dimension.rawValue }
            return $0.value < $1.value
        }
    }

    private static func aggregateMetrics(from inputs: [MetricAggregateInput]) -> AggregatedReplayMetrics {
        let totalFPSeconds = inputs.map(\.detectionQuality.falsePositiveSkipSeconds).reduce(0, +)
        let totalFNSeconds = inputs.map(\.detectionQuality.falseNegativeAdSeconds).reduce(0, +)
        let totalMissed = inputs.map(\.detectionQuality.missedSegmentCount).reduce(0, +)
        let totalSpurious = inputs.map(\.detectionQuality.spuriousSegmentCount).reduce(0, +)
        let aggregatedSeedCounts = aggregatedSeedCounts(from: inputs)
        let meanPrecision = inputs.isEmpty ? 0 : inputs.map(\.detectionQuality.precision).reduce(0, +) / Double(inputs.count)
        let meanRecall = inputs.isEmpty ? 0 : inputs.map(\.detectionQuality.recall).reduce(0, +) / Double(inputs.count)
        let f1 = (meanPrecision + meanRecall) > 0
            ? 2.0 * meanPrecision * meanRecall / (meanPrecision + meanRecall) : 0

        let aggDetection = DetectionQualityMetrics(
            falsePositiveSkipSeconds: totalFPSeconds,
            falseNegativeAdSeconds: totalFNSeconds,
            seedRecall: aggregatedSeedCounts.map {
                $0.groundTruthSegmentCount > 0 ? Double($0.seededSegmentCount) / Double($0.groundTruthSegmentCount) : 0
            } ?? (inputs.isEmpty ? 0 : inputs.map(\.detectionQuality.seedRecall).reduce(0, +) / Double(inputs.count)),
            seededSegmentCount: aggregatedSeedCounts?.seededSegmentCount,
            groundTruthSegmentCount: aggregatedSeedCounts?.groundTruthSegmentCount,
            precision: meanPrecision,
            recall: meanRecall,
            f1Score: f1,
            missedSegmentCount: totalMissed,
            spuriousSegmentCount: totalSpurious
        )

        let allEntryErrors = inputs.flatMap(\.boundaryQuality.cutSpeechAtEntryMs)
        let allResumeErrors = inputs.flatMap(\.boundaryQuality.cutSpeechAtResumeMs)
        let allSignedEntryErrors = inputs.flatMap(\.boundaryQuality.signedEntryErrorMs)
        let allSignedResumeErrors = inputs.flatMap(\.boundaryQuality.signedResumeErrorMs)
        let allIoUs = inputs.flatMap(\.boundaryQuality.spanIoUs)
        let allCoverageRecalls = inputs.flatMap(\.boundaryQuality.coverageRecalls)
        let allCoveragePrecisions = inputs.flatMap(\.boundaryQuality.coveragePrecisions)

        let aggBoundary = BoundaryQualityMetrics(
            cutSpeechAtEntryMs: allEntryErrors,
            cutSpeechAtResumeMs: allResumeErrors,
            signedEntryErrorMs: allSignedEntryErrors,
            signedResumeErrorMs: allSignedResumeErrors,
            spanIoUs: allIoUs,
            coverageRecalls: allCoverageRecalls,
            coveragePrecisions: allCoveragePrecisions,
            p50EntryErrorMs: percentile(allEntryErrors, 0.50),
            p95EntryErrorMs: percentile(allEntryErrors, 0.95),
            p50ResumeErrorMs: percentile(allResumeErrors, 0.50),
            p95ResumeErrorMs: percentile(allResumeErrors, 0.95),
            medianSignedEntryErrorMs: percentile(allSignedEntryErrors, 0.50),
            medianSignedResumeErrorMs: percentile(allSignedResumeErrors, 0.50),
            medianSpanIoU: percentile(allIoUs, 0.50),
            medianCoverageRecall: percentile(allCoverageRecalls, 0.50),
            medianCoveragePrecision: percentile(allCoveragePrecisions, 0.50)
        )

        let pipelineLatencies = inputs.map(\.latency.meanPipelineLatencyMs)
        let p95Latencies = inputs.map(\.latency.p95PipelineLatencyMs)
        let bannerP95s = inputs.compactMap(\.latency.p95BannerLatencyMs)
        let firstConfirmationLeadTimes = inputs.compactMap(\.latency.leadTimeAtFirstConfirmationSeconds)

        let aggLatency = LatencyMetrics(
            timeToFirstUsableSkip: inputs.compactMap(\.latency.timeToFirstUsableSkip).min(),
            leadTimeAtFirstConfirmationSeconds: firstConfirmationLeadTimes.isEmpty ? nil : firstConfirmationLeadTimes.min(),
            p50BannerLatencyMs: inputs.compactMap(\.latency.p50BannerLatencyMs).isEmpty
                ? nil : percentile(inputs.compactMap(\.latency.p50BannerLatencyMs), 0.50),
            p95BannerLatencyMs: bannerP95s.isEmpty ? nil : percentile(bannerP95s, 0.95),
            meanPipelineLatencyMs: pipelineLatencies.isEmpty ? 0 : pipelineLatencies.reduce(0, +) / Double(pipelineLatencies.count),
            p95PipelineLatencyMs: p95Latencies.isEmpty ? 0 : percentile(p95Latencies, 0.95)
        )

        let totalListens = inputs.map(\.userOverrides.listenTapCount).reduce(0, +)
        let totalRewinds = inputs.map(\.userOverrides.rewindAfterSkipCount).reduce(0, +)
        let totalOverrides = totalListens + totalRewinds
        let totalSkips: Int = inputs.map { input in
            let overrides = input.userOverrides.listenTapCount + input.userOverrides.rewindAfterSkipCount
            let rate = input.userOverrides.overrideRate
            guard rate > 0, overrides > 0 else { return 0 }
            return Int((Double(overrides) / rate).rounded())
        }.reduce(0, +)
        let aggOverrideRate = totalSkips > 0 ? Double(totalOverrides) / Double(totalSkips) : 0

        let aggOverrides = UserOverrideMetrics(
            listenTapCount: totalListens,
            rewindAfterSkipCount: totalRewinds,
            overrideRate: aggOverrideRate
        )

        return AggregatedReplayMetrics(
            detectionQuality: aggDetection,
            boundaryQuality: aggBoundary,
            latency: aggLatency,
            userOverrides: aggOverrides
        )
    }

    private static func metricInput(from report: EpisodeReplayReport) -> MetricAggregateInput {
        MetricAggregateInput(
            detectionQuality: report.detectionQuality,
            boundaryQuality: report.boundaryQuality,
            latency: report.latency,
            userOverrides: report.userOverrides
        )
    }

    private static func metricInput(from metrics: DeliveryStyleMetricReport) -> MetricAggregateInput {
        MetricAggregateInput(
            detectionQuality: metrics.detectionQuality,
            boundaryQuality: metrics.boundaryQuality,
            latency: metrics.latency,
            userOverrides: metrics.userOverrides
        )
    }
}

private func aggregatedSeedCounts(from inputs: [MetricAggregateInput]) -> (seededSegmentCount: Int, groundTruthSegmentCount: Int)? {
    guard inputs.allSatisfy({
        $0.detectionQuality.seededSegmentCount != nil
            && $0.detectionQuality.groundTruthSegmentCount != nil
    }) else {
        return nil
    }

    let totalSeeded = inputs.compactMap(\.detectionQuality.seededSegmentCount).reduce(0, +)
    let totalGroundTruth = inputs.compactMap(\.detectionQuality.groundTruthSegmentCount).reduce(0, +)
    return (seededSegmentCount: totalSeeded, groundTruthSegmentCount: totalGroundTruth)
}

struct AggregatedReplayMetrics {
    let detectionQuality: DetectionQualityMetrics
    let boundaryQuality: BoundaryQualityMetrics
    let latency: LatencyMetrics
    let userOverrides: UserOverrideMetrics
}

private struct MetricAggregateInput {
    let detectionQuality: DetectionQualityMetrics
    let boundaryQuality: BoundaryQualityMetrics
    let latency: LatencyMetrics
    let userOverrides: UserOverrideMetrics
}
