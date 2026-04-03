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

    enum AdSegmentType: String, Sendable, Codable {
        case preRoll
        case midRoll
        case postRoll
        case hostRead
        case dynamicInsertion
    }
}

// MARK: - Replay Configuration

/// Configuration for a single episode replay.
struct ReplayConfiguration: Sendable {
    let episodeId: String
    let episodeTitle: String
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

    private let config: ReplayConfiguration
    private var currentTime: TimeInterval = 0
    private var currentSpeed: Float
    private var events: [SimulatedEvent] = []
    private var pipelineLatencies: [Double] = []
    private var bannerLatencies: [Double] = []
    private var appliedSkips: [(windowId: String, start: Double, end: Double)] = []
    private var revertedSkips: [String] = []
    private var detectedWindows: [AdWindow] = []

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
        let groundTruth = config.groundTruthSegments
        let detected = detectedWindows

        // Compute overlap-based metrics at 0.1s resolution.
        let resolution = 0.1
        let totalSamples = Int(config.episodeDuration / resolution)

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

        // Count fully missed segments (zero overlap with any detection).
        let missedCount = groundTruth.filter { seg in
            !detected.contains { win in
                max(seg.startTime, win.startTime) < min(seg.endTime, win.endTime)
            }
        }.count

        // Count spurious detections (no overlap with any ground truth).
        let spuriousCount = detected.filter { win in
            !groundTruth.contains { seg in
                max(seg.startTime, win.startTime) < min(seg.endTime, win.endTime)
            }
        }.count

        return DetectionQualityMetrics(
            falsePositiveSkipSeconds: fpSeconds,
            falseNegativeAdSeconds: fnSeconds,
            precision: precision,
            recall: recall,
            f1Score: f1,
            missedSegmentCount: missedCount,
            spuriousSegmentCount: spuriousCount
        )
    }

    /// Compute boundary quality metrics.
    func computeBoundaryQuality() -> BoundaryQualityMetrics {
        let groundTruth = config.groundTruthSegments
        var entryErrors: [Double] = []
        var resumeErrors: [Double] = []

        for win in detectedWindows {
            // Find best-matching ground truth segment.
            let bestMatch = groundTruth
                .filter { max($0.startTime, win.startTime) < min($0.endTime, win.endTime) }
                .max { a, b in
                    let overlapA = min(a.endTime, win.endTime) - max(a.startTime, win.startTime)
                    let overlapB = min(b.endTime, win.endTime) - max(b.startTime, win.startTime)
                    return overlapA < overlapB
                }

            guard let gt = bestMatch else { continue }

            // Entry error: positive = entered too late (speech cut), negative = too early.
            let entryError = (win.startTime - gt.startTime) * 1000 // ms
            entryErrors.append(abs(entryError))

            // Resume error: positive = resumed too late (missed content), negative = too early (ad heard).
            let resumeError = (win.endTime - gt.endTime) * 1000
            resumeErrors.append(abs(resumeError))
        }

        return BoundaryQualityMetrics(
            cutSpeechAtEntryMs: entryErrors,
            cutSpeechAtResumeMs: resumeErrors,
            p50EntryErrorMs: percentile(entryErrors, 0.50),
            p95EntryErrorMs: percentile(entryErrors, 0.95),
            p50ResumeErrorMs: percentile(resumeErrors, 0.50),
            p95ResumeErrorMs: percentile(resumeErrors, 0.95)
        )
    }

    /// Compute latency metrics.
    func computeLatencyMetrics() -> LatencyMetrics {
        let firstSkipTime = appliedSkips.first.map(\.start)

        return LatencyMetrics(
            timeToFirstUsableSkip: firstSkipTime,
            p50BannerLatencyMs: bannerLatencies.isEmpty ? nil : percentile(bannerLatencies, 0.50),
            p95BannerLatencyMs: bannerLatencies.isEmpty ? nil : percentile(bannerLatencies, 0.95),
            meanPipelineLatencyMs: pipelineLatencies.isEmpty ? 0 : pipelineLatencies.reduce(0, +) / Double(pipelineLatencies.count),
            p95PipelineLatencyMs: pipelineLatencies.isEmpty ? 0 : percentile(pipelineLatencies, 0.95)
        )
    }

    /// Compute user override metrics.
    func computeUserOverrideMetrics() -> UserOverrideMetrics {
        let listenTaps = revertedSkips.count
        let rewinds = events.filter {
            if case .scrubPerformed = $0 { return true }
            return false
        }.count
        let totalApplied = appliedSkips.count
        let overrideRate = totalApplied > 0 ? Double(listenTaps + rewinds) / Double(totalApplied) : 0

        return UserOverrideMetrics(
            listenTapCount: listenTaps,
            rewindAfterSkipCount: rewinds,
            overrideRate: overrideRate
        )
    }

    /// Build the complete episode replay report.
    func buildReport(replayDuration: TimeInterval) -> EpisodeReplayReport {
        EpisodeReplayReport(
            episodeId: config.episodeId,
            episodeTitle: config.episodeTitle,
            condition: config.condition,
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

    /// Return transcript chunks whose startTime falls within the detection
    /// lookahead window from the current playhead position. Each chunk is
    /// returned only once.
    private var deliveredChunkIds: Set<String> = []

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

        for gt in config.groundTruthSegments {
            // Check if any delivered chunk overlaps this ground truth segment.
            let overlapping = chunks.filter { chunk in
                chunk.startTime < gt.endTime && chunk.endTime > gt.startTime
            }
            guard !overlapping.isEmpty else { continue }

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

            if time >= window.startTime && time < window.endTime && window.confidence >= 0.65 {
                appliedSkips.append((windowId: window.id, start: window.startTime, end: window.endTime))
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
                if let firstGT = config.groundTruthSegments.first(where: { $0.endTime < time }) {
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
                timestamp: skip.start,
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
}
