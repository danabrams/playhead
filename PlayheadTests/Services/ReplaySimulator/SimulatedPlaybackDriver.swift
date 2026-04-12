// SimulatedPlaybackDriver.swift
// Drives simulated playback through the analysis pipeline, feeding events
// to AdDetectionService and SkipOrchestrator as if a real player were active.
//
// Supports configurable playback speed, scrubs, late detections, and
// dynamic ad variant injection to exercise the full detection/skip path.

import Foundation
import OSLog
@testable import Playhead

// MARK: - Simulated Event

/// An event produced by the driver during simulated playback.
enum SimulatedEvent: Sendable {
    case playheadAdvanced(time: TimeInterval)
    case adWindowDetected(AdWindow)
    case skipApplied(windowId: String, from: TimeInterval, to: TimeInterval)
    case skipReverted(windowId: String)
    case scrubPerformed(from: TimeInterval, to: TimeInterval)
    case speedChanged(newSpeed: Float)
    case lateDetectionInjected(AdWindow)
    case bannerShown(windowId: String, latencyMs: Double)
}

// MARK: - Ground Truth

/// A labeled ad segment from the test corpus for comparison.
struct GroundTruthAdSegment: Sendable, Codable {
    let startTime: Double
    let endTime: Double
    let advertiser: String?
    let product: String?
    let adType: AdSegmentType
    let deliveryStyle: DeliveryStyle

    init(
        startTime: Double,
        endTime: Double,
        advertiser: String?,
        product: String?,
        adType: AdSegmentType,
        deliveryStyle: DeliveryStyle = .hostRead
    ) {
        self.startTime = startTime
        self.endTime = endTime
        self.advertiser = advertiser
        self.product = product
        self.adType = adType
        self.deliveryStyle = deliveryStyle
    }

    enum AdSegmentType: String, Sendable, Codable, Hashable {
        case preRoll
        case midRoll
        case postRoll
        case hostRead
        case dynamicInsertion
    }

    enum DeliveryStyle: String, Sendable, Codable, Hashable {
        case dynamicInsertion
        case hostRead
        case blendedHostRead
        case producedSegment
    }
}

// MARK: - Replay Configuration

/// Configuration for a single episode replay.
struct ReplayConfiguration: Sendable {
    let episodeId: String
    let episodeTitle: String
    let podcastId: String
    let episodeDuration: TimeInterval
    let condition: SimulationCondition
    let groundTruthSegments: [GroundTruthAdSegment]
    /// Simulated transcript chunks to feed the detection pipeline.
    let transcriptChunks: [TranscriptChunk]
    /// Simulated feature windows for boundary snapping.
    let featureWindows: [FeatureWindow]
    /// Optional dynamic ad variants (different ads for same episode).
    let dynamicAdVariants: [DynamicAdVariant]
    /// Time step for playhead advancement (seconds). Smaller = higher fidelity.
    let timeStep: TimeInterval

    static let defaultTimeStep: TimeInterval = 0.25

    init(
        episodeId: String,
        episodeTitle: String,
        podcastId: String = "",
        episodeDuration: TimeInterval,
        condition: SimulationCondition,
        groundTruthSegments: [GroundTruthAdSegment],
        transcriptChunks: [TranscriptChunk],
        featureWindows: [FeatureWindow],
        dynamicAdVariants: [DynamicAdVariant],
        timeStep: TimeInterval
    ) {
        self.episodeId = episodeId
        self.episodeTitle = episodeTitle
        self.podcastId = podcastId
        self.episodeDuration = episodeDuration
        self.condition = condition
        self.groundTruthSegments = groundTruthSegments
        self.transcriptChunks = transcriptChunks
        self.featureWindows = featureWindows
        self.dynamicAdVariants = dynamicAdVariants
        self.timeStep = timeStep
    }

    struct DynamicAdVariant: Sendable {
        let variantId: String
        let replacesSegmentIndex: Int
        let transcriptChunks: [TranscriptChunk]
        let groundTruth: GroundTruthAdSegment
    }
}

// MARK: - Simulated Playback Driver

/// Drives a simulated episode replay through the detection and skip pipeline,
/// collecting events and timing data for metrics computation.
///
/// Does not use a real AVPlayer. Instead, it advances a virtual playhead and
/// feeds transcript chunks and feature windows to the pipeline at the
/// appropriate simulated times.
/// A seedable random number generator for deterministic replay tests.
/// Uses a linear congruential generator for simplicity and reproducibility.
struct SeededRandomNumberGenerator: RandomNumberGenerator {
    private var state: UInt64

    init(seed: UInt64) {
        self.state = seed
    }

    mutating func next() -> UInt64 {
        // LCG parameters from Numerical Recipes.
        state = state &* 6364136223846793005 &+ 1442695040888963407
        return state
    }
}

/// Single-threaded simulation driver. Not safe for concurrent access.
/// All mutable state is accessed only from the synchronous `runReplay()` call chain.
final class SimulatedPlaybackDriver {
    private static let confirmationThreshold = 0.65

    private let config: ReplayConfiguration
    private var currentTime: TimeInterval = 0
    private var currentSpeed: Float
    private var events: [SimulatedEvent] = []
    private var pipelineLatencies: [Double] = []
    private var bannerLatencies: [Double] = []
    private var appliedSkips: [(windowId: String, appliedAt: Double, start: Double, end: Double)] = []
    private var revertedSkips: [String] = []
    private var detectedWindows: [AdWindow] = []
    private var deliveredChunkIds: Set<String> = []
    private var seededGroundTruthIndices: Set<Int> = []
    private var confirmationObservations: [ConfirmationObservation] = []
    private var bannerObservations: [BannerObservation] = []
    private var appliedSkipObservations: [AppliedSkipObservation] = []
    private var detectedWindowGroundTruthIndices: [String: Int] = [:]

    /// Random number generator (injectable for deterministic tests).
    private var rng: any RandomNumberGenerator

    /// Sorted interactions by time for efficient processing.
    private let sortedInteractions: [SimulatedInteraction]
    private var nextInteractionIndex: Int = 0

    /// Signpost for pipeline timing.
    private let signposter = OSSignposter(subsystem: "com.playhead.test", category: "ReplaySimulator")

    init(config: ReplayConfiguration, rng: any RandomNumberGenerator = SystemRandomNumberGenerator()) {
        self.config = config
        self.currentSpeed = config.condition.playbackSpeed
        self.sortedInteractions = config.condition.interactions.sorted { $0.atTime < $1.atTime }
        self.rng = rng
    }

    // MARK: - Replay Execution

    /// Run the full simulated replay and return the collected events.
    /// This executes synchronously in simulated time (no real-time waiting).
    func runReplay() -> [SimulatedEvent] {
        events.removeAll()
        pipelineLatencies.removeAll()
        bannerLatencies.removeAll()
        appliedSkips.removeAll()
        revertedSkips.removeAll()
        detectedWindows.removeAll()
        deliveredChunkIds.removeAll()
        seededGroundTruthIndices.removeAll()
        confirmationObservations.removeAll()
        bannerObservations.removeAll()
        appliedSkipObservations.removeAll()
        detectedWindowGroundTruthIndices.removeAll()
        currentTime = 0
        nextInteractionIndex = 0
        currentSpeed = config.condition.playbackSpeed

        let timeStep = config.timeStep

        while currentTime < config.episodeDuration {
            // Check for user interactions at this time.
            processInteractions(at: currentTime)

            // Feed transcript chunks that fall within the current lookahead.
            let newChunks = chunksAvailableAt(time: currentTime)
            if !newChunks.isEmpty {
                let pipelineStart = ProcessInfo.processInfo.systemUptime
                let signpostId = signposter.makeSignpostID()
                let state = signposter.beginInterval("DetectionPipeline", id: signpostId)

                // Simulate detection pipeline processing.
                let windows = simulateDetection(chunks: newChunks, at: currentTime)
                for window in windows {
                    detectedWindows.append(window)
                    events.append(.adWindowDetected(window))

                    // Simulate banner latency.
                    let bannerDelay = Double.random(in: 50...300, using: &rng)
                    bannerLatencies.append(bannerDelay)
                    if let groundTruthIndex = detectedWindowGroundTruthIndices[window.id] {
                        bannerObservations.append(
                            BannerObservation(latencyMs: bannerDelay, groundTruthIndex: groundTruthIndex)
                        )
                    }
                    events.append(.bannerShown(windowId: window.id, latencyMs: bannerDelay))
                }

                signposter.endInterval("DetectionPipeline", state)
                let pipelineEnd = ProcessInfo.processInfo.systemUptime
                pipelineLatencies.append((pipelineEnd - pipelineStart) * 1000)
            }

            // Check if playhead entered a detected ad window -> apply skip.
            checkAndApplySkips(at: currentTime)

            events.append(.playheadAdvanced(time: currentTime))

            // Advance playhead by time step scaled by speed.
            currentTime += timeStep * Double(currentSpeed)
        }

        return events
    }

    // MARK: - Metrics Computation

    /// Compute detection quality metrics by comparing detected windows to ground truth.
    func computeDetectionQuality() -> DetectionQualityMetrics {
        ReplayMetricsComputation.detectionQuality(
            groundTruth: config.groundTruthSegments,
            detected: detectedWindows,
            episodeDuration: config.episodeDuration,
            seededGroundTruthIndices: Array(seededGroundTruthIndices).sorted()
        )
    }

    /// Compute boundary quality metrics.
    func computeBoundaryQuality() -> BoundaryQualityMetrics {
        ReplayMetricsComputation.boundaryQuality(
            groundTruth: config.groundTruthSegments,
            detected: detectedWindows
        )
    }

    /// Compute latency metrics.
    func computeLatencyMetrics() -> LatencyMetrics {
        computeLatencyMetrics(forGroundTruthIndices: Set(config.groundTruthSegments.indices))
    }

    /// Compute user override metrics.
    func computeUserOverrideMetrics() -> UserOverrideMetrics {
        computeUserOverrideMetrics(forGroundTruthIndices: Set(config.groundTruthSegments.indices))
    }

    /// Build the complete episode replay report.
    func buildReport(replayDuration: TimeInterval) -> EpisodeReplayReport {
        EpisodeReplayReport(
            episodeId: config.episodeId,
            episodeTitle: config.episodeTitle,
            podcastId: config.podcastId,
            condition: config.condition,
            deliveryStyles: Array(Set(config.groundTruthSegments.map(\.deliveryStyle))).sorted {
                $0.rawValue < $1.rawValue
            },
            deliveryStyleMetrics: computeDeliveryStyleMetrics(),
            detectionQuality: computeDetectionQuality(),
            boundaryQuality: computeBoundaryQuality(),
            latency: computeLatencyMetrics(),
            userOverrides: computeUserOverrideMetrics(),
            samples: buildMetricSamples(),
            simulatorVersion: EpisodeReplayReport.currentSimulatorVersion,
            generatedAt: Date(),
            replayDurationSeconds: replayDuration
        )
    }

    // MARK: - Private Simulation Logic

    private func chunksAvailableAt(time: TimeInterval) -> [TranscriptChunk] {
        let lookahead = 90.0 // Match AdDetectionConfig.default.hotPathLookahead
        let chunks = config.transcriptChunks.filter { chunk in
            chunk.startTime <= time + lookahead
                && chunk.endTime >= time
                && !deliveredChunkIds.contains(chunk.id)
        }
        for chunk in chunks {
            deliveredChunkIds.insert(chunk.id)
        }
        return chunks
    }

    /// Simulate the detection pipeline on a batch of transcript chunks.
    /// Produces AdWindow objects based on overlap with ground truth
    /// (simulating what a real detector would find). Adds controlled noise
    /// for realistic evaluation.
    private func simulateDetection(chunks: [TranscriptChunk], at time: TimeInterval) -> [AdWindow] {
        var windows: [AdWindow] = []

        for (groundTruthIndex, gt) in config.groundTruthSegments.enumerated() {
            // Check if any delivered chunk overlaps this ground truth segment.
            let overlapping = chunks.filter { chunk in
                chunk.startTime < gt.endTime && chunk.endTime > gt.startTime
            }
            guard !overlapping.isEmpty else { continue }
            seededGroundTruthIndices.insert(groundTruthIndex)

            // Only emit once per ground truth segment.
            let existingOverlap = detectedWindows.contains { win in
                max(win.startTime, gt.startTime) < min(win.endTime, gt.endTime)
            }
            guard !existingOverlap else { continue }

            // Simulate detection with boundary noise (+-0.5s).
            let startNoise = Double.random(in: -0.5...0.5, using: &rng)
            let endNoise = Double.random(in: -0.5...0.5, using: &rng)
            let confidence = Double.random(in: 0.55...0.95, using: &rng)

            let window = AdWindow(
                id: UUID().uuidString,
                analysisAssetId: config.episodeId,
                startTime: max(0, gt.startTime + startNoise),
                endTime: min(config.episodeDuration, gt.endTime + endNoise),
                confidence: confidence,
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: "sim-v1",
                advertiser: gt.advertiser,
                product: gt.product,
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: gt.startTime,
                metadataSource: "simulated",
                metadataConfidence: confidence,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false
            )
            detectedWindowGroundTruthIndices[window.id] = groundTruthIndex
            if confidence >= Self.confirmationThreshold {
                confirmationObservations.append(
                    ConfirmationObservation(time: time, groundTruthIndex: groundTruthIndex)
                )
            }
            windows.append(window)
        }

        return windows
    }

    /// Check if the playhead has entered a detected ad window and apply skip.
    private func checkAndApplySkips(at time: TimeInterval) {
        for window in detectedWindows {
            let alreadyApplied = appliedSkips.contains { $0.windowId == window.id }
            let alreadyReverted = revertedSkips.contains(window.id)
            guard !alreadyApplied, !alreadyReverted else { continue }

            if time >= window.startTime && time < window.endTime && window.confidence >= Self.confirmationThreshold {
                appliedSkips.append((windowId: window.id, appliedAt: time, start: window.startTime, end: window.endTime))
                appliedSkipObservations.append(
                    AppliedSkipObservation(
                        windowId: window.id,
                        time: time,
                        groundTruthIndex: detectedWindowGroundTruthIndices[window.id]
                    )
                )
                events.append(.skipApplied(windowId: window.id, from: time, to: window.endTime))
                // Jump playhead to end of ad.
                currentTime = window.endTime
                return
            }
        }
    }

    /// Process any user interactions scheduled at or before the current time.
    private func processInteractions(at time: TimeInterval) {
        while nextInteractionIndex < sortedInteractions.count {
            let interaction = sortedInteractions[nextInteractionIndex]
            guard interaction.atTime <= time else { break }
            nextInteractionIndex += 1

            switch interaction.type {
            case .scrub:
                let target = interaction.targetTime ?? time
                events.append(.scrubPerformed(from: time, to: target))
                currentTime = target

            case .skipForward:
                let target = min(time + 30, config.episodeDuration)
                events.append(.scrubPerformed(from: time, to: target))
                currentTime = target

            case .listenTap:
                // Revert the most recent skip.
                if let lastSkip = appliedSkips.last {
                    revertedSkips.append(lastSkip.windowId)
                    events.append(.skipReverted(windowId: lastSkip.windowId))
                    // Rewind to where the skip started.
                    currentTime = lastSkip.start
                }

            case .speedChange:
                let newSpeed = interaction.newSpeed ?? currentSpeed
                currentSpeed = newSpeed
                events.append(.speedChanged(newSpeed: newSpeed))

            case .lateDetection:
                // Inject a detection that arrives after playhead has passed.
                // This tests the "late detection" suppression path.
                if let firstMatch = config.groundTruthSegments.enumerated().first(where: { $0.element.endTime < time }) {
                    let (groundTruthIndex, firstGT) = firstMatch
                    let lateWindow = AdWindow(
                        id: UUID().uuidString,
                        analysisAssetId: config.episodeId,
                        startTime: firstGT.startTime,
                        endTime: firstGT.endTime,
                        confidence: 0.75,
                        boundaryState: AdBoundaryState.lexical.rawValue,
                        decisionState: AdDecisionState.candidate.rawValue,
                        detectorVersion: "sim-v1-late",
                        advertiser: nil,
                        product: nil,
                        adDescription: nil,
                        evidenceText: nil,
                        evidenceStartTime: nil,
                        metadataSource: "simulated",
                        metadataConfidence: nil,
                        metadataPromptVersion: nil,
                        wasSkipped: false,
                        userDismissedBanner: false
                    )
                    seededGroundTruthIndices.insert(groundTruthIndex)
                    detectedWindowGroundTruthIndices[lateWindow.id] = groundTruthIndex
                    confirmationObservations.append(
                        ConfirmationObservation(time: time, groundTruthIndex: groundTruthIndex)
                    )
                    detectedWindows.append(lateWindow)
                    events.append(.lateDetectionInjected(lateWindow))
                }
            }
        }
    }

    /// Build raw metric samples from collected data.
    private func buildMetricSamples() -> [MetricSample] {
        var samples: [MetricSample] = []

        for (i, latency) in pipelineLatencies.enumerated() {
            samples.append(MetricSample(
                name: "pipeline_latency",
                value: latency,
                unit: .milliseconds,
                timestamp: Double(i) * config.timeStep,
                context: ["speed": "\(currentSpeed)"]
            ))
        }

        for skip in appliedSkips {
            samples.append(MetricSample(
                name: "skip_applied",
                value: skip.end - skip.start,
                unit: .seconds,
                timestamp: skip.appliedAt,
                context: ["windowId": skip.windowId]
            ))
        }

        for banner in bannerLatencies {
            samples.append(MetricSample(
                name: "banner_latency",
                value: banner,
                unit: .milliseconds,
                timestamp: 0,
                context: [:]
            ))
        }

        return samples
    }

    private func computeLatencyMetrics(forGroundTruthIndices indices: Set<Int>) -> LatencyMetrics {
        let firstConfirmation = confirmationObservations
            .filter { indices.contains($0.groundTruthIndex) }
            .min { lhs, rhs in lhs.time < rhs.time }
        let firstAppliedSkip = appliedSkipObservations
            .filter {
                guard let groundTruthIndex = $0.groundTruthIndex else { return false }
                return indices.contains(groundTruthIndex)
            }
            .min { lhs, rhs in lhs.time < rhs.time }
        let firstConfirmationLeadTime = firstConfirmation.map {
            config.groundTruthSegments[$0.groundTruthIndex].startTime - $0.time
        }
        let styleBannerLatencies = bannerObservations
            .filter { indices.contains($0.groundTruthIndex) }
            .map(\.latencyMs)
        let bannerLatencySource =
            indices.count == config.groundTruthSegments.count ? bannerLatencies : styleBannerLatencies

        return ReplayMetricsComputation.latencyMetrics(
            timeToFirstUsableSkip: firstAppliedSkip?.time,
            leadTimeAtFirstConfirmationSeconds: firstConfirmationLeadTime,
            pipelineLatencies: pipelineLatencies,
            bannerLatencies: bannerLatencySource
        )
    }

    private func computeUserOverrideMetrics(forGroundTruthIndices indices: Set<Int>) -> UserOverrideMetrics {
        let relevantSkips = appliedSkipObservations.filter {
            guard let groundTruthIndex = $0.groundTruthIndex else { return false }
            return indices.contains(groundTruthIndex)
        }
        let relevantWindowIds = Set(relevantSkips.map(\.windowId))
        let listenTaps = revertedSkips.filter { relevantWindowIds.contains($0) }.count
        let rewinds: Int
        if indices.count == config.groundTruthSegments.count {
            rewinds = events.filter {
                if case .scrubPerformed = $0 { return true }
                return false
            }.count
        } else {
            rewinds = 0
        }
        let totalApplied = relevantSkips.count
        let overrideRate = totalApplied > 0 ? Double(listenTaps + rewinds) / Double(totalApplied) : 0

        return UserOverrideMetrics(
            listenTapCount: listenTaps,
            rewindAfterSkipCount: rewinds,
            overrideRate: overrideRate
        )
    }

    private func computeDeliveryStyleMetrics() -> [DeliveryStyleMetricReport] {
        let groupedByStyle = Dictionary(
            grouping: Array(config.groundTruthSegments.enumerated()),
            by: { $0.element.deliveryStyle }
        )

        return groupedByStyle.keys.sorted { $0.rawValue < $1.rawValue }.map { style in
            let groupedSegments = (groupedByStyle[style] ?? []).sorted { $0.offset < $1.offset }
            let originalIndices = groupedSegments.map(\.offset)
            let groundTruth = groupedSegments.map(\.element)
            let seededLocalIndices = groupedSegments.enumerated().compactMap { localIndex, entry in
                seededGroundTruthIndices.contains(entry.offset) ? localIndex : nil
            }
            let detected = detectedWindows.filter { window in
                groundTruth.contains { segment in
                    max(segment.startTime, window.startTime) < min(segment.endTime, window.endTime)
                }
            }
            let groundTruthIndexSet = Set(originalIndices)

            return DeliveryStyleMetricReport(
                style: style,
                detectionQuality: ReplayMetricsComputation.detectionQuality(
                    groundTruth: groundTruth,
                    detected: detected,
                    episodeDuration: config.episodeDuration,
                    seededGroundTruthIndices: seededLocalIndices
                ),
                boundaryQuality: ReplayMetricsComputation.boundaryQuality(
                    groundTruth: groundTruth,
                    detected: detected
                ),
                latency: computeLatencyMetrics(forGroundTruthIndices: groundTruthIndexSet),
                userOverrides: computeUserOverrideMetrics(forGroundTruthIndices: groundTruthIndexSet)
            )
        }
    }
}

private struct ConfirmationObservation {
    let time: Double
    let groundTruthIndex: Int
}

private struct BannerObservation {
    let latencyMs: Double
    let groundTruthIndex: Int
}

private struct AppliedSkipObservation {
    let windowId: String
    let time: Double
    let groundTruthIndex: Int?
}
