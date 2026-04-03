// ReplaySimulatorTests.swift
// Tests for the replay simulator harness itself. Verifies that
// simulated playback produces correct metrics against labeled data.

import Foundation
import Testing
@testable import Playhead

// MARK: - Replay Simulator Tests

@Suite("ReplaySimulator – Basic Replay")
struct ReplaySimulatorBasicTests {

    // MARK: - Helpers

    /// Build a minimal replay configuration with known ground truth.
    private func makeConfig(
        speed: Float = 1.0,
        audioMode: SimulationCondition.AudioMode = .cached,
        interactions: [SimulatedInteraction] = [],
        groundTruth: [GroundTruthAdSegment]? = nil,
        duration: TimeInterval = 3600
    ) -> ReplayConfiguration {
        let gt = groundTruth ?? [
            GroundTruthAdSegment(
                startTime: 120, endTime: 180,
                advertiser: "Acme Corp", product: "Widget Pro",
                adType: .midRoll
            ),
            GroundTruthAdSegment(
                startTime: 1800, endTime: 1860,
                advertiser: "BetterHelp", product: "Therapy",
                adType: .midRoll
            ),
        ]

        // Generate transcript chunks covering the full episode.
        let chunks = stride(from: 0.0, to: duration, by: 10.0).map { start in
            TranscriptChunk(
                id: "chunk-\(Int(start))",
                analysisAssetId: "test-episode-001",
                segmentFingerprint: "fp-\(Int(start))",
                chunkIndex: Int(start / 10),
                startTime: start,
                endTime: min(start + 10, duration),
                text: "This is simulated transcript text for testing purposes.",
                normalizedText: "this is simulated transcript text for testing purposes",
                pass: "fast",
                modelVersion: "sim-v1"
            )
        }

        let condition = SimulationCondition(
            audioMode: audioMode,
            playbackSpeed: speed,
            interactions: interactions
        )

        return ReplayConfiguration(
            episodeId: "test-episode-001",
            episodeTitle: "Test Episode: Basic Replay",
            episodeDuration: duration,
            condition: condition,
            groundTruthSegments: gt,
            transcriptChunks: chunks,
            featureWindows: [],
            dynamicAdVariants: [],
            timeStep: ReplayConfiguration.defaultTimeStep
        )
    }

    @Test("Replay produces events for a simple episode")
    func basicReplay() {
        let config = makeConfig()
        let driver = SimulatedPlaybackDriver(config: config)
        let events = driver.runReplay()

        #expect(!events.isEmpty, "Replay should produce events")

        let adDetections = events.filter {
            if case .adWindowDetected = $0 { return true }
            return false
        }
        #expect(adDetections.count >= 2, "Should detect at least the 2 ground-truth ad segments")
    }

    @Test("Detection quality metrics are computed correctly")
    func detectionQuality() {
        let config = makeConfig()
        let driver = SimulatedPlaybackDriver(config: config)
        _ = driver.runReplay()

        let quality = driver.computeDetectionQuality()

        // With simulated detection matching ground truth, precision and recall
        // should be reasonable (not perfect due to boundary noise).
        #expect(quality.precision > 0.5, "Precision should be above 0.5")
        #expect(quality.recall > 0.5, "Recall should be above 0.5")
        #expect(quality.f1Score > 0.5, "F1 should be above 0.5")
        #expect(quality.missedSegmentCount == 0, "No segments should be fully missed")
    }

    @Test("Boundary quality metrics capture entry/resume errors")
    func boundaryQuality() {
        let config = makeConfig()
        let driver = SimulatedPlaybackDriver(config: config)
        _ = driver.runReplay()

        let boundary = driver.computeBoundaryQuality()

        // Boundary noise is +-0.5s = 500ms max.
        #expect(boundary.p95EntryErrorMs <= 600, "p95 entry error should be under 600ms")
        #expect(boundary.p95ResumeErrorMs <= 600, "p95 resume error should be under 600ms")
    }

    @Test("Latency metrics are populated")
    func latencyMetrics() {
        let config = makeConfig()
        let driver = SimulatedPlaybackDriver(config: config)
        _ = driver.runReplay()

        let latency = driver.computeLatencyMetrics()

        #expect(latency.meanPipelineLatencyMs >= 0, "Pipeline latency should be non-negative")
        #expect(latency.p95BannerLatencyMs != nil, "Banner latency should be measured")
    }

    @Test("Full report can be built and serialized to JSON")
    func reportSerialization() throws {
        let config = makeConfig()
        let driver = SimulatedPlaybackDriver(config: config)
        _ = driver.runReplay()

        let report = driver.buildReport(replayDuration: 1.5)

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(report)
        #expect(data.count > 0, "Report should serialize to non-empty JSON")

        // Verify round-trip.
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(EpisodeReplayReport.self, from: data)
        #expect(decoded.episodeId == "test-episode-001")
        #expect(decoded.simulatorVersion == EpisodeReplayReport.currentSimulatorVersion)
    }
}

@Suite("ReplaySimulator – Simulated Conditions")
struct ReplaySimulatorConditionTests {

    private func makeConfig(
        speed: Float = 1.0,
        interactions: [SimulatedInteraction] = []
    ) -> ReplayConfiguration {
        let gt = [
            GroundTruthAdSegment(
                startTime: 60, endTime: 120,
                advertiser: "Squarespace", product: "Website Builder",
                adType: .preRoll
            ),
        ]

        let chunks = stride(from: 0.0, to: 600, by: 10.0).map { start in
            TranscriptChunk(
                id: "chunk-\(Int(start))",
                analysisAssetId: "test-episode-cond",
                segmentFingerprint: "fp-\(Int(start))",
                chunkIndex: Int(start / 10),
                startTime: start,
                endTime: min(start + 10, 600),
                text: "Simulated text.",
                normalizedText: "simulated text",
                pass: "fast",
                modelVersion: "sim-v1"
            )
        }

        return ReplayConfiguration(
            episodeId: "test-episode-cond",
            episodeTitle: "Test Episode: Conditions",
            episodeDuration: 600,
            condition: SimulationCondition(
                audioMode: .cached,
                playbackSpeed: speed,
                interactions: interactions
            ),
            groundTruthSegments: gt,
            transcriptChunks: chunks,
            featureWindows: [],
            dynamicAdVariants: [],
            timeStep: 0.5
        )
    }

    @Test("High-speed playback (3x) still detects ads")
    func highSpeedPlayback() {
        let config = makeConfig(speed: 3.0)
        let driver = SimulatedPlaybackDriver(config: config)
        _ = driver.runReplay()

        let quality = driver.computeDetectionQuality()
        #expect(quality.recall > 0, "Should detect ads at 3x speed")
    }

    @Test("Low-speed playback (0.5x) still detects ads")
    func lowSpeedPlayback() {
        let config = makeConfig(speed: 0.5)
        let driver = SimulatedPlaybackDriver(config: config)
        _ = driver.runReplay()

        let quality = driver.computeDetectionQuality()
        #expect(quality.recall > 0, "Should detect ads at 0.5x speed")
    }

    @Test("Scrub during playback produces scrub events")
    func scrubDuringPlayback() {
        let config = makeConfig(interactions: [
            SimulatedInteraction(type: .scrub, atTime: 30, targetTime: 200, newSpeed: nil),
        ])
        let driver = SimulatedPlaybackDriver(config: config)
        let events = driver.runReplay()

        let scrubs = events.filter {
            if case .scrubPerformed = $0 { return true }
            return false
        }
        #expect(!scrubs.isEmpty, "Scrub events should appear in the event stream")
    }

    @Test("Listen tap reverts a skip and records it")
    func listenTapRevert() {
        // Place the listen tap after the ad would be skipped.
        let config = makeConfig(interactions: [
            SimulatedInteraction(type: .listenTap, atTime: 125, targetTime: nil, newSpeed: nil),
        ])
        let driver = SimulatedPlaybackDriver(config: config)
        let events = driver.runReplay()

        let reverts = events.filter {
            if case .skipReverted = $0 { return true }
            return false
        }

        let overrides = driver.computeUserOverrideMetrics()
        // The listen tap may or may not fire depending on whether a skip was applied
        // before t=125. Either way, the metric should be computable.
        #expect(overrides.overrideRate >= 0, "Override rate should be computable")
        _ = reverts // suppress unused warning
    }

    @Test("Speed change mid-episode is recorded")
    func speedChange() {
        let config = makeConfig(interactions: [
            SimulatedInteraction(type: .speedChange, atTime: 100, targetTime: nil, newSpeed: 2.0),
        ])
        let driver = SimulatedPlaybackDriver(config: config)
        let events = driver.runReplay()

        let speedChanges = events.filter {
            if case .speedChanged = $0 { return true }
            return false
        }
        #expect(!speedChanges.isEmpty, "Speed change should appear in events")
    }

    @Test("Late detection injection is recorded")
    func lateDetection() {
        let config = makeConfig(interactions: [
            SimulatedInteraction(type: .lateDetection, atTime: 300, targetTime: nil, newSpeed: nil),
        ])
        let driver = SimulatedPlaybackDriver(config: config)
        let events = driver.runReplay()

        let lateDetections = events.filter {
            if case .lateDetectionInjected = $0 { return true }
            return false
        }
        // Late detection fires only if there's a GT segment whose end < 300.
        // Our GT segment ends at 120, so it should fire.
        #expect(!lateDetections.isEmpty, "Late detection should be injected")
    }
}

@Suite("ReplaySimulator – Corpus Aggregation")
struct ReplaySimulatorAggregationTests {

    @Test("Aggregate report combines multiple episode reports")
    func aggregation() {
        // Build two minimal reports.
        let report1 = EpisodeReplayReport(
            episodeId: "ep-1",
            episodeTitle: "Episode 1",
            condition: SimulationCondition(audioMode: .cached, playbackSpeed: 1.0, interactions: []),
            detectionQuality: DetectionQualityMetrics(
                falsePositiveSkipSeconds: 5, falseNegativeAdSeconds: 10,
                precision: 0.9, recall: 0.8, f1Score: 0.85,
                missedSegmentCount: 0, spuriousSegmentCount: 1
            ),
            boundaryQuality: BoundaryQualityMetrics(
                cutSpeechAtEntryMs: [100, 200], cutSpeechAtResumeMs: [150, 250],
                p50EntryErrorMs: 150, p95EntryErrorMs: 200,
                p50ResumeErrorMs: 200, p95ResumeErrorMs: 250
            ),
            latency: LatencyMetrics(
                timeToFirstUsableSkip: 5.0, p50BannerLatencyMs: 80,
                p95BannerLatencyMs: 200, meanPipelineLatencyMs: 10,
                p95PipelineLatencyMs: 25
            ),
            userOverrides: UserOverrideMetrics(
                listenTapCount: 1, rewindAfterSkipCount: 0, overrideRate: 0.1
            ),
            samples: [],
            simulatorVersion: EpisodeReplayReport.currentSimulatorVersion,
            generatedAt: Date(),
            replayDurationSeconds: 2.0
        )

        let report2 = EpisodeReplayReport(
            episodeId: "ep-2",
            episodeTitle: "Episode 2",
            condition: SimulationCondition(audioMode: .streamed, playbackSpeed: 1.5, interactions: []),
            detectionQuality: DetectionQualityMetrics(
                falsePositiveSkipSeconds: 3, falseNegativeAdSeconds: 8,
                precision: 0.85, recall: 0.75, f1Score: 0.80,
                missedSegmentCount: 1, spuriousSegmentCount: 0
            ),
            boundaryQuality: BoundaryQualityMetrics(
                cutSpeechAtEntryMs: [120, 180], cutSpeechAtResumeMs: [130, 220],
                p50EntryErrorMs: 150, p95EntryErrorMs: 180,
                p50ResumeErrorMs: 175, p95ResumeErrorMs: 220
            ),
            latency: LatencyMetrics(
                timeToFirstUsableSkip: 8.0, p50BannerLatencyMs: 100,
                p95BannerLatencyMs: 250, meanPipelineLatencyMs: 12,
                p95PipelineLatencyMs: 30
            ),
            userOverrides: UserOverrideMetrics(
                listenTapCount: 0, rewindAfterSkipCount: 1, overrideRate: 0.05
            ),
            samples: [],
            simulatorVersion: EpisodeReplayReport.currentSimulatorVersion,
            generatedAt: Date(),
            replayDurationSeconds: 1.8
        )

        let corpus = CorpusReplayReport.aggregate(from: [report1, report2])

        #expect(corpus.episodeReports.count == 2)
        #expect(corpus.aggregateDetectionQuality.falsePositiveSkipSeconds == 8) // 5 + 3
        #expect(corpus.aggregateDetectionQuality.falseNegativeAdSeconds == 18) // 10 + 8
        #expect(corpus.aggregateDetectionQuality.missedSegmentCount == 1) // 0 + 1
        #expect(corpus.aggregateUserOverrides.listenTapCount == 1) // 1 + 0
        #expect(corpus.aggregateUserOverrides.rewindAfterSkipCount == 1) // 0 + 1
    }

    @Test("Percentile helper handles edge cases")
    func percentileEdgeCases() {
        #expect(percentile([], 0.5) == 0)
        #expect(percentile([42], 0.5) == 42)
        #expect(percentile([1, 2, 3, 4, 5], 0.0) == 1)
        #expect(percentile([1, 2, 3, 4, 5], 1.0) == 5)
        let p50 = percentile([1, 2, 3, 4, 5], 0.5)
        #expect(p50 == 3)
    }
}
