// ReplayHarness.swift
// Deterministic benchmark gate for Foundation Model rollout promotion.

import Foundation

enum ReplayHarness {
    struct BenchmarkEpisode: Sendable, Codable, Equatable {
        let id: String
        let duration: TimeInterval
        let labeledSpans: [LabeledSpan]

        init(
            id: String,
            duration: TimeInterval,
            labeledSpans: [LabeledSpan]
        ) {
            self.id = id
            self.duration = duration
            self.labeledSpans = labeledSpans
        }
    }

    struct LabeledSpan: Sendable, Codable, Equatable {
        let id: String
        let startTime: TimeInterval
        let endTime: TimeInterval
        let verdict: Verdict

        init(id: String, startTime: TimeInterval, endTime: TimeInterval, verdict: Verdict) {
            self.id = id
            self.startTime = startTime
            self.endTime = endTime
            self.verdict = verdict
        }

        var isPositive: Bool {
            switch verdict {
            case .paidPromotion, .housePromo:
                true
            case .editorialMention, .none:
                false
            }
        }

    }

    struct PredictedSpan: Sendable, Codable, Equatable {
        let id: String
        let startTime: TimeInterval
        let endTime: TimeInterval

        init(id: String, startTime: TimeInterval, endTime: TimeInterval) {
            self.id = id
            self.startTime = startTime
            self.endTime = endTime
        }
    }

    enum Verdict: String, Sendable, Codable, Equatable {
        case paidPromotion
        case housePromo
        case editorialMention
        case none
    }

    struct GateThresholds: Sendable, Codable, Equatable {
        let minimumSpanPrecision: Double
        let minimumSpanRecall: Double
        let maximumFalsePositiveSecondsPerHour: Double
        let maximumAverageBoundaryError: TimeInterval
        let maximumBoundaryError: TimeInterval
        let matchingIoUThreshold: Double

        static let `default` = GateThresholds(
            minimumSpanPrecision: 0.95,
            minimumSpanRecall: 0.95,
            maximumFalsePositiveSecondsPerHour: 5,
            maximumAverageBoundaryError: 2,
            maximumBoundaryError: 5,
            matchingIoUThreshold: 0.5
        )

        init(
            minimumSpanPrecision: Double,
            minimumSpanRecall: Double,
            maximumFalsePositiveSecondsPerHour: Double,
            maximumAverageBoundaryError: TimeInterval,
            maximumBoundaryError: TimeInterval,
            matchingIoUThreshold: Double = 0.5
        ) {
            self.minimumSpanPrecision = minimumSpanPrecision
            self.minimumSpanRecall = minimumSpanRecall
            self.maximumFalsePositiveSecondsPerHour = maximumFalsePositiveSecondsPerHour
            self.maximumAverageBoundaryError = maximumAverageBoundaryError
            self.maximumBoundaryError = maximumBoundaryError
            self.matchingIoUThreshold = matchingIoUThreshold
        }
    }

    struct Metrics: Sendable, Codable, Equatable {
        let truePositiveSpans: Int
        let falsePositiveSpans: Int
        let falseNegativeSpans: Int
        let spanPrecision: Double
        let spanRecall: Double
        let falsePositiveSecondsPerHour: Double
        let averageBoundaryError: TimeInterval
        let maximumBoundaryError: TimeInterval
    }

    struct Evaluation: Sendable, Codable, Equatable {
        let metrics: Metrics
        let passed: Bool
        let reasons: [String]
    }

    typealias PredictionProvider = @Sendable (_ episode: BenchmarkEpisode, _ episodeIndex: Int) throws -> [PredictedSpan]

    static func evaluate(
        episodes: [BenchmarkEpisode],
        thresholds: GateThresholds = .default,
        predictionProvider: PredictionProvider
    ) -> Evaluation {
        var replayedPredictions = Array(repeating: [PredictedSpan](), count: episodes.count)
        var replayFailures: [String] = []
        for (episodeIndex, episode) in episodes.enumerated() {
            do {
                replayedPredictions[episodeIndex] = try predictionProvider(episode, episodeIndex)
            } catch {
                replayFailures.append("benchmark episode \(episode.id) replay failed: \(error)")
            }
        }

        let positiveTruth = episodes.enumerated().flatMap { episodeIndex, episode in
            episode.labeledSpans
                .filter(\.isPositive)
                .compactMap {
                    SpanRef(
                        episodeIndex: episodeIndex,
                        id: $0.id,
                        interval: labeledInterval($0, episodeDuration: episode.duration)
                    )
                }
        }
        let predictions = episodes.enumerated().flatMap { episodeIndex, episode in
            replayedPredictions[episodeIndex].compactMap {
                SpanRef(
                    episodeIndex: episodeIndex,
                    id: $0.id,
                    interval: clampedInterval(
                        ($0.startTime, $0.endTime),
                        to: (0, episode.duration)
                    )
                )
            }
        }

        let matches = match(
            predicted: predictions,
            groundTruth: positiveTruth,
            minimumIoU: thresholds.matchingIoUThreshold
        )
        let truePositiveCount = matches.count
        let falsePositiveCount = predictions.count - truePositiveCount
        let falseNegativeCount = positiveTruth.count - truePositiveCount
        let precision = precision(truePositiveCount: truePositiveCount, predictedCount: predictions.count, truthCount: positiveTruth.count)
        let recall = recall(truePositiveCount: truePositiveCount, predictedCount: predictions.count, truthCount: positiveTruth.count)
        let totalDuration = episodes
            .map(\.duration)
            .filter { $0.isFinite && $0 > 0 }
            .reduce(0, +)
        let fpSeconds = falsePositiveSeconds(
            episodes: episodes,
            predictionsByEpisode: replayedPredictions
        )
        let fpSecondsPerHour = totalDuration > 0 ? fpSeconds / (totalDuration / 3600) : (fpSeconds > 0 ? .infinity : 0)
        let boundaryErrors = matches.flatMap { match in
            [
                abs(match.predicted.startTime - match.groundTruth.startTime),
                abs(match.predicted.endTime - match.groundTruth.endTime),
            ]
        }
        let averageBoundaryError = boundaryErrors.isEmpty ? 0 : boundaryErrors.reduce(0, +) / Double(boundaryErrors.count)
        let maximumBoundaryError = boundaryErrors.max() ?? 0

        let metrics = Metrics(
            truePositiveSpans: truePositiveCount,
            falsePositiveSpans: falsePositiveCount,
            falseNegativeSpans: falseNegativeCount,
            spanPrecision: precision,
            spanRecall: recall,
            falsePositiveSecondsPerHour: fpSecondsPerHour,
            averageBoundaryError: averageBoundaryError,
            maximumBoundaryError: maximumBoundaryError
        )
        let reasons = failureReasons(
            metrics: metrics,
            thresholds: thresholds,
            episodes: episodes,
            predictionsByEpisode: replayedPredictions,
            replayFailures: replayFailures
        )
        return Evaluation(metrics: metrics, passed: reasons.isEmpty, reasons: reasons)
    }
}

private extension ReplayHarness {
    struct SpanRef: Equatable {
        let episodeIndex: Int
        let id: String
        let startTime: TimeInterval
        let endTime: TimeInterval

        init?(episodeIndex: Int, id: String, interval: (TimeInterval, TimeInterval)?) {
            guard let interval else {
                return nil
            }
            self.init(episodeIndex: episodeIndex, id: id, startTime: interval.0, endTime: interval.1)
        }

        init?(episodeIndex: Int, id: String, startTime: TimeInterval, endTime: TimeInterval) {
            guard startTime.isFinite, endTime.isFinite, endTime > startTime else {
                return nil
            }
            self.episodeIndex = episodeIndex
            self.id = id
            self.startTime = startTime
            self.endTime = endTime
        }

        var duration: TimeInterval { endTime - startTime }
    }

    struct Match {
        let predicted: SpanRef
        let groundTruth: SpanRef
        let iou: Double
    }

    struct FlowEdge {
        let to: Int
        let reverseIndex: Int
        var capacity: Int
        let cost: Int
        let predictionIndex: Int?
        let truthIndex: Int?
    }

    static func match(
        predicted: [SpanRef],
        groundTruth: [SpanRef],
        minimumIoU: Double
    ) -> [Match] {
        let effectiveMinimumIoU = minimumIoU.isFinite ? max(0, min(1, minimumIoU)) : 1
        let predictionOffset = 1
        let truthOffset = predictionOffset + predicted.count
        let sink = truthOffset + groundTruth.count
        var graph = Array(repeating: [FlowEdge](), count: sink + 1)

        func addEdge(
            from: Int,
            to: Int,
            capacity: Int,
            cost: Int,
            predictionIndex: Int? = nil,
            truthIndex: Int? = nil
        ) {
            let forwardIndex = graph[from].count
            let reverseIndex = graph[to].count
            graph[from].append(
                FlowEdge(
                    to: to,
                    reverseIndex: reverseIndex,
                    capacity: capacity,
                    cost: cost,
                    predictionIndex: predictionIndex,
                    truthIndex: truthIndex
                )
            )
            graph[to].append(
                FlowEdge(
                    to: from,
                    reverseIndex: forwardIndex,
                    capacity: 0,
                    cost: -cost,
                    predictionIndex: nil,
                    truthIndex: nil
                )
            )
        }

        for predictionIndex in predicted.indices {
            addEdge(from: 0, to: predictionOffset + predictionIndex, capacity: 1, cost: 0)
        }
        for truthIndex in groundTruth.indices {
            addEdge(from: truthOffset + truthIndex, to: sink, capacity: 1, cost: 0)
        }
        for predictionIndex in predicted.indices {
            for truthIndex in groundTruth.indices {
                guard predicted[predictionIndex].episodeIndex == groundTruth[truthIndex].episodeIndex else { continue }
                let iou = iou(predicted[predictionIndex], groundTruth[truthIndex])
                guard iou > 0, iou >= effectiveMinimumIoU else { continue }
                addEdge(
                    from: predictionOffset + predictionIndex,
                    to: truthOffset + truthIndex,
                    capacity: 1,
                    cost: matchCost(
                        predicted: predicted[predictionIndex],
                        groundTruth: groundTruth[truthIndex],
                        iou: iou
                    ),
                    predictionIndex: predictionIndex,
                    truthIndex: truthIndex
                )
            }
        }

        // Keep augmenting until no valid path remains. A cardinality-increasing
        // reassignment can raise total cost when it displaces an exact match.
        while shortestAugmentingPath(in: &graph, source: 0, sink: sink) {}

        return graph[predictionOffset..<truthOffset]
            .flatMap { edges in
                edges.compactMap { edge -> Match? in
                    guard
                        edge.capacity == 0,
                        let predictionIndex = edge.predictionIndex,
                        let truthIndex = edge.truthIndex
                    else {
                        return nil
                    }
                    return Match(
                        predicted: predicted[predictionIndex],
                        groundTruth: groundTruth[truthIndex],
                        iou: iou(predicted[predictionIndex], groundTruth[truthIndex])
                    )
                }
            }
            .sorted {
                if $0.predicted.episodeIndex != $1.predicted.episodeIndex {
                    return $0.predicted.episodeIndex < $1.predicted.episodeIndex
                }
                if $0.predicted.id != $1.predicted.id {
                    return $0.predicted.id < $1.predicted.id
                }
                return $0.groundTruth.id < $1.groundTruth.id
            }
    }

    static func shortestAugmentingPath(
        in graph: inout [[FlowEdge]],
        source: Int,
        sink: Int
    ) -> Bool {
        let infinity = Int.max / 4
        var distance = Array(repeating: infinity, count: graph.count)
        var previousNode = Array(repeating: -1, count: graph.count)
        var previousEdge = Array(repeating: -1, count: graph.count)
        var isQueued = Array(repeating: false, count: graph.count)
        var queue = [source]
        var head = 0
        distance[source] = 0
        isQueued[source] = true

        while head < queue.count {
            let node = queue[head]
            head += 1
            isQueued[node] = false

            for edgeIndex in graph[node].indices {
                let edge = graph[node][edgeIndex]
                guard edge.capacity > 0 else { continue }
                let nextDistance = distance[node] + edge.cost
                guard nextDistance < distance[edge.to] else { continue }

                distance[edge.to] = nextDistance
                previousNode[edge.to] = node
                previousEdge[edge.to] = edgeIndex

                if !isQueued[edge.to] {
                    queue.append(edge.to)
                    isQueued[edge.to] = true
                }
            }
        }

        guard previousNode[sink] != -1 else {
            return false
        }

        var node = sink
        while node != source {
            let from = previousNode[node]
            let edgeIndex = previousEdge[node]
            let reverseIndex = graph[from][edgeIndex].reverseIndex
            graph[from][edgeIndex].capacity -= 1
            graph[node][reverseIndex].capacity += 1
            node = from
        }
        return true
    }

    static func matchCost(predicted: SpanRef, groundTruth: SpanRef, iou: Double) -> Int {
        let boundaryScale = 1_001
        let maximumBoundaryPenalty = maximumBoundaryMilliseconds * boundaryScale
        let matchReward = maximumBoundaryPenalty + maximumIoUScore + 1
        let boundaryMilliseconds = clampedMilliseconds(
            abs(predicted.startTime - groundTruth.startTime) +
                abs(predicted.endTime - groundTruth.endTime)
        )
        let iouScore = Int((max(0, min(1, iou)) * Double(maximumIoUScore)).rounded())
        return -matchReward + (boundaryMilliseconds * boundaryScale) - iouScore
    }

    static var maximumBoundaryMilliseconds: Int { 1_000_000_000 }
    static var maximumIoUScore: Int { 1_000 }

    static func clampedMilliseconds(_ seconds: TimeInterval) -> Int {
        guard seconds.isFinite, seconds > 0 else { return 0 }
        return Int(min(seconds * 1_000, Double(maximumBoundaryMilliseconds)))
    }

    static func iou(_ lhs: SpanRef, _ rhs: SpanRef) -> Double {
        let intersection = max(0, min(lhs.endTime, rhs.endTime) - max(lhs.startTime, rhs.startTime))
        let union = lhs.duration + rhs.duration - intersection
        return union > 0 ? intersection / union : 0
    }

    static func precision(truePositiveCount: Int, predictedCount: Int, truthCount: Int) -> Double {
        if predictedCount == 0, truthCount == 0 { return 1 }
        return predictedCount > 0 ? Double(truePositiveCount) / Double(predictedCount) : 0
    }

    static func recall(truePositiveCount: Int, predictedCount: Int, truthCount: Int) -> Double {
        if truthCount == 0 { return 1 }
        return truthCount > 0 ? Double(truePositiveCount) / Double(truthCount) : 0
    }

    static func falsePositiveSeconds(
        episodes: [BenchmarkEpisode],
        predictionsByEpisode: [[PredictedSpan]]
    ) -> Double {
        episodes.enumerated().reduce(0) { total, pair in
            let episodeIndex = pair.offset
            let episode = pair.element
            let positives = mergeIntervals(
                episode.labeledSpans
                    .filter(\.isPositive)
                    .compactMap { labeledInterval($0, episodeDuration: episode.duration) }
            )
            let episodeBounds: (TimeInterval, TimeInterval) = (0, episode.duration)
            let falsePositiveIntervals = predictionsByEpisode[episodeIndex].flatMap { prediction in
                guard let interval = clampedInterval((prediction.startTime, prediction.endTime), to: episodeBounds) else {
                    return [(TimeInterval, TimeInterval)]()
                }
                return subtract(interval: interval, coveredBy: positives)
            }
            return total + mergeIntervals(falsePositiveIntervals).reduce(0) { $0 + max(0, $1.1 - $1.0) }
        }
    }

    static func clampedInterval(
        _ interval: (TimeInterval, TimeInterval),
        to bounds: (TimeInterval, TimeInterval)
    ) -> (TimeInterval, TimeInterval)? {
        guard interval.0.isFinite, interval.1.isFinite, bounds.0.isFinite, bounds.1.isFinite else {
            return nil
        }
        guard interval.1 > interval.0, bounds.1 > bounds.0 else {
            return nil
        }
        let start = max(bounds.0, interval.0)
        let end = min(bounds.1, interval.1)
        return end > start ? (start, end) : nil
    }

    static func labeledInterval(
        _ span: LabeledSpan,
        episodeDuration: TimeInterval
    ) -> (TimeInterval, TimeInterval)? {
        guard
            span.startTime.isFinite,
            span.endTime.isFinite,
            episodeDuration.isFinite,
            episodeDuration > 0,
            span.endTime > span.startTime,
            span.startTime >= 0,
            span.endTime <= episodeDuration
        else {
            return nil
        }
        return (span.startTime, span.endTime)
    }

    static func mergeIntervals(_ intervals: [(TimeInterval, TimeInterval)]) -> [(TimeInterval, TimeInterval)] {
        let sorted = intervals
            .filter { $0.0.isFinite && $0.1.isFinite }
            .map { (min($0.0, $0.1), max($0.0, $0.1)) }
            .filter { $0.1 > $0.0 }
            .sorted {
                if $0.0 != $1.0 { return $0.0 < $1.0 }
                return $0.1 < $1.1
            }
        guard var current = sorted.first else { return [] }
        var merged: [(TimeInterval, TimeInterval)] = []

        for interval in sorted.dropFirst() {
            if interval.0 <= current.1 {
                current.1 = max(current.1, interval.1)
            } else {
                merged.append(current)
                current = interval
            }
        }
        merged.append(current)
        return merged
    }

    static func subtract(
        interval: (TimeInterval, TimeInterval),
        coveredBy coveredIntervals: [(TimeInterval, TimeInterval)]
    ) -> [(TimeInterval, TimeInterval)] {
        var fragments = [(min(interval.0, interval.1), max(interval.0, interval.1))]
        for covered in coveredIntervals {
            fragments = fragments.flatMap { fragment -> [(TimeInterval, TimeInterval)] in
                let overlapStart = max(fragment.0, covered.0)
                let overlapEnd = min(fragment.1, covered.1)
                guard overlapEnd > overlapStart else { return [fragment] }

                var next: [(TimeInterval, TimeInterval)] = []
                if fragment.0 < overlapStart {
                    next.append((fragment.0, overlapStart))
                }
                if overlapEnd < fragment.1 {
                    next.append((overlapEnd, fragment.1))
                }
                return next
            }
        }
        return fragments.filter { $0.1 > $0.0 }
    }

    static func hasNonFiniteTimeRange(startTime: TimeInterval, endTime: TimeInterval) -> Bool {
        !startTime.isFinite || !endTime.isFinite
    }

    static func hasZeroDurationTimeRange(startTime: TimeInterval, endTime: TimeInterval) -> Bool {
        startTime.isFinite && endTime.isFinite && startTime == endTime
    }

    static func hasReversedTimeRange(startTime: TimeInterval, endTime: TimeInterval) -> Bool {
        startTime.isFinite && endTime.isFinite && startTime > endTime
    }

    static func failureReasons(
        metrics: Metrics,
        thresholds: GateThresholds,
        episodes: [BenchmarkEpisode],
        predictionsByEpisode: [[PredictedSpan]],
        replayFailures: [String]
    ) -> [String] {
        var reasons = replayFailures
        if episodes.isEmpty {
            reasons.append("benchmark must include at least one episode")
        }
        for (episodeIndex, episode) in episodes.enumerated() {
            let hasValidEpisodeDuration = episode.duration.isFinite && episode.duration > 0

            if !episode.duration.isFinite {
                reasons.append("benchmark episode \(episode.id) duration must be finite")
            } else if episode.duration <= 0 {
                reasons.append("benchmark episode \(episode.id) duration must be greater than zero")
            }
            if episode.labeledSpans.isEmpty {
                reasons.append("benchmark episode \(episode.id) must include at least one labeled span")
            }

            for span in episode.labeledSpans {
                if hasNonFiniteTimeRange(startTime: span.startTime, endTime: span.endTime) {
                    reasons.append("benchmark episode \(episode.id) labeled span \(span.id) time range must be finite")
                } else if hasZeroDurationTimeRange(startTime: span.startTime, endTime: span.endTime) {
                    reasons.append("benchmark episode \(episode.id) labeled span \(span.id) time range must be greater than zero")
                } else if hasReversedTimeRange(startTime: span.startTime, endTime: span.endTime) {
                    reasons.append("benchmark episode \(episode.id) labeled span \(span.id) time range must have start before end")
                } else if hasValidEpisodeDuration && labeledInterval(span, episodeDuration: episode.duration) == nil {
                    reasons.append("benchmark episode \(episode.id) labeled span \(span.id) time range must be within the episode timeline")
                }
            }

            for span in predictionsByEpisode[episodeIndex] {
                if hasNonFiniteTimeRange(startTime: span.startTime, endTime: span.endTime) {
                    reasons.append("benchmark episode \(episode.id) predicted span \(span.id) time range must be finite")
                } else if hasZeroDurationTimeRange(startTime: span.startTime, endTime: span.endTime) {
                    reasons.append("benchmark episode \(episode.id) predicted span \(span.id) time range must be greater than zero")
                } else if hasReversedTimeRange(startTime: span.startTime, endTime: span.endTime) {
                    reasons.append("benchmark episode \(episode.id) predicted span \(span.id) time range must have start before end")
                }
            }
        }
        if !thresholds.minimumSpanPrecision.isFinite || thresholds.minimumSpanPrecision < 0 || thresholds.minimumSpanPrecision > 1 {
            reasons.append("minimum span precision threshold \(thresholds.minimumSpanPrecision) is invalid")
        } else if metrics.spanPrecision < thresholds.minimumSpanPrecision {
            reasons.append("span precision \(metrics.spanPrecision) below required \(thresholds.minimumSpanPrecision)")
        }
        if !thresholds.minimumSpanRecall.isFinite || thresholds.minimumSpanRecall < 0 || thresholds.minimumSpanRecall > 1 {
            reasons.append("minimum span recall threshold \(thresholds.minimumSpanRecall) is invalid")
        } else if metrics.spanRecall < thresholds.minimumSpanRecall {
            reasons.append("span recall \(metrics.spanRecall) below required \(thresholds.minimumSpanRecall)")
        }
        if !thresholds.maximumFalsePositiveSecondsPerHour.isFinite || thresholds.maximumFalsePositiveSecondsPerHour < 0 {
            reasons.append("maximum false-positive seconds/hour threshold \(thresholds.maximumFalsePositiveSecondsPerHour) is invalid")
        } else if metrics.falsePositiveSecondsPerHour > thresholds.maximumFalsePositiveSecondsPerHour {
            reasons.append("false-positive seconds/hour \(metrics.falsePositiveSecondsPerHour) above allowed \(thresholds.maximumFalsePositiveSecondsPerHour)")
        }
        if !thresholds.maximumAverageBoundaryError.isFinite || thresholds.maximumAverageBoundaryError < 0 {
            reasons.append("maximum average boundary error threshold \(thresholds.maximumAverageBoundaryError) is invalid")
        } else if metrics.averageBoundaryError > thresholds.maximumAverageBoundaryError {
            reasons.append("average boundary error \(metrics.averageBoundaryError) above allowed \(thresholds.maximumAverageBoundaryError)")
        }
        if !thresholds.maximumBoundaryError.isFinite || thresholds.maximumBoundaryError < 0 {
            reasons.append("maximum boundary error threshold \(thresholds.maximumBoundaryError) is invalid")
        } else if metrics.maximumBoundaryError > thresholds.maximumBoundaryError {
            reasons.append("maximum boundary error \(metrics.maximumBoundaryError) above allowed \(thresholds.maximumBoundaryError)")
        }
        if !thresholds.matchingIoUThreshold.isFinite || thresholds.matchingIoUThreshold < 0 || thresholds.matchingIoUThreshold > 1 {
            reasons.append("matching IoU threshold \(thresholds.matchingIoUThreshold) is invalid")
        }
        return reasons
    }
}
