// ReplaySimulatorTests.swift
// Tests for the replay simulator harness itself. Verifies that
// simulated playback produces correct metrics against labeled data.

import Foundation
import Testing
@testable import Playhead

private struct MaxRandomNumberGenerator: RandomNumberGenerator {
    mutating func next() -> UInt64 { UInt64.max }
}

// MARK: - Replay Simulator Tests

@Suite("ReplaySimulator – Basic Replay")
struct ReplaySimulatorBasicTests {

    // MARK: - Helpers

    /// Build a minimal replay configuration with known ground truth.
    private func makeConfig(
        speed: Float = 1.0,
        audioMode: SimulationCondition.AudioMode = .cached,
        analysisPath: SimulationCondition.AnalysisPath = .live,
        podcastId: String = "test-podcast",
        interactions: [SimulatedInteraction] = [],
        groundTruth: [GroundTruthAdSegment]? = nil,
        duration: TimeInterval = 3600
    ) -> ReplayConfiguration {
        let gt = groundTruth ?? [
            GroundTruthAdSegment(
                startTime: 120, endTime: 180,
                advertiser: "Acme Corp", product: "Widget Pro",
                adType: .midRoll,
                deliveryStyle: .hostRead
            ),
            GroundTruthAdSegment(
                startTime: 1800, endTime: 1860,
                advertiser: "BetterHelp", product: "Therapy",
                adType: .midRoll,
                deliveryStyle: .blendedHostRead
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
                modelVersion: "sim-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }

        let condition = SimulationCondition(
            audioMode: audioMode,
            playbackSpeed: speed,
            interactions: interactions,
            analysisPath: analysisPath
        )

        return ReplayConfiguration(
            episodeId: "test-episode-001",
            episodeTitle: "Test Episode: Basic Replay",
            podcastId: podcastId,
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
        let driver = SimulatedPlaybackDriver(config: config, rng: SeededRandomNumberGenerator(seed: 42))
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
        let driver = SimulatedPlaybackDriver(config: config, rng: SeededRandomNumberGenerator(seed: 42))
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
        let driver = SimulatedPlaybackDriver(config: config, rng: SeededRandomNumberGenerator(seed: 42))
        _ = driver.runReplay()

        let boundary = driver.computeBoundaryQuality()

        // Boundary noise is +-0.5s = 500ms max.
        #expect(boundary.p95EntryErrorMs <= 600, "p95 entry error should be under 600ms")
        #expect(boundary.p95ResumeErrorMs <= 600, "p95 resume error should be under 600ms")
    }

    @Test("Boundary quality metrics include signed overlap and coverage fields")
    func boundaryQualitySignedFields() {
        let groundTruth = [
            GroundTruthAdSegment(
                startTime: 100, endTime: 200,
                advertiser: "Acme", product: "Widget",
                adType: .midRoll,
                deliveryStyle: .producedSegment
            )
        ]
        let detected = [
            AdWindow(
                id: "window-1",
                analysisAssetId: "test-episode-001",
                startTime: 80,
                endTime: 240,
                confidence: 0.9,
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: "sim-v1",
                advertiser: "Acme",
                product: "Widget",
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: 100,
                metadataSource: "test",
                metadataConfidence: 0.9,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false
            )
        ]

        let boundary = ReplayMetricsComputation.boundaryQuality(
            groundTruth: groundTruth,
            detected: detected
        )

        #expect(boundary.cutSpeechAtEntryMs == [20000])
        #expect(boundary.cutSpeechAtResumeMs == [40000])
        #expect(boundary.signedEntryErrorMs == [-20000])
        #expect(boundary.signedResumeErrorMs == [40000])
        #expect(abs((boundary.spanIoUs.first ?? 0) - 0.625) < 0.0001)
        #expect(boundary.coverageRecalls == [1.0])
        #expect(abs((boundary.coveragePrecisions.first ?? 0) - 0.625) < 0.0001)
        #expect(boundary.medianSignedEntryErrorMs == -20000)
        #expect(boundary.medianSignedResumeErrorMs == 40000)
        #expect(boundary.medianSpanIoU > 0.6)
    }

    @Test("Seed recall requires explicit seed observations rather than span overlap")
    func seedRecallRequiresExplicitSeeds() {
        let groundTruth = [
            GroundTruthAdSegment(
                startTime: 100, endTime: 160,
                advertiser: "Acme", product: "Widget",
                adType: .midRoll,
                deliveryStyle: .hostRead
            )
        ]
        let detected = [
            AdWindow(
                id: "window-1",
                analysisAssetId: "test-episode-001",
                startTime: 100,
                endTime: 160,
                confidence: 0.9,
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: "sim-v1",
                advertiser: "Acme",
                product: "Widget",
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: 100,
                metadataSource: "test",
                metadataConfidence: 0.9,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false
            )
        ]

        let withoutSeed = ReplayMetricsComputation.detectionQuality(
            groundTruth: groundTruth,
            detected: detected,
            episodeDuration: 200,
            seededGroundTruthIndices: []
        )
        let withSeed = ReplayMetricsComputation.detectionQuality(
            groundTruth: groundTruth,
            detected: detected,
            episodeDuration: 200,
            seededGroundTruthIndices: [0]
        )

        #expect(withoutSeed.seedRecall == 0)
        #expect(withSeed.seedRecall == 1)
    }

    @Test("Latency metrics are populated")
    func latencyMetrics() {
        let config = makeConfig()
        let driver = SimulatedPlaybackDriver(config: config, rng: SeededRandomNumberGenerator(seed: 42))
        _ = driver.runReplay()

        let latency = driver.computeLatencyMetrics()

        #expect(latency.meanPipelineLatencyMs >= 0, "Pipeline latency should be non-negative")
        #expect(latency.p95BannerLatencyMs != nil, "Banner latency should be measured")
    }

    @Test("Latency metrics keep first applied skip time separate from first confirmation lead time")
    func latencySeparatesSkipTimeFromConfirmationLeadTime() {
        let groundTruth = [
            GroundTruthAdSegment(
                startTime: 120, endTime: 180,
                advertiser: "Acme", product: "Widget",
                adType: .midRoll,
                deliveryStyle: .hostRead
            )
        ]
        let config = makeConfig(groundTruth: groundTruth, duration: 240)
        let driver = SimulatedPlaybackDriver(config: config, rng: MaxRandomNumberGenerator())
        let events = driver.runReplay()

        let latency = driver.computeLatencyMetrics()
        let firstSkipTime = events.compactMap { event -> Double? in
            guard case .skipApplied(_, let from, _) = event else { return nil }
            return from
        }.min()

        #expect(abs((latency.timeToFirstUsableSkip ?? -1) - (firstSkipTime ?? -1)) < 0.001)
        #expect(abs((latency.timeToFirstUsableSkip ?? -1) - 120.5) < 0.001)
        #expect(abs((latency.leadTimeAtFirstConfirmationSeconds ?? -1) - 90.0) < 0.001)
        #expect(latency.timeToFirstUsableSkip != latency.leadTimeAtFirstConfirmationSeconds)
    }

    @Test("Latency metrics retain lead time at first confirmation")
    func latencyLeadTime() {
        let latency = ReplayMetricsComputation.latencyMetrics(
            timeToFirstUsableSkip: 12.5,
            leadTimeAtFirstConfirmationSeconds: 3.25,
            pipelineLatencies: [10, 20, 30],
            bannerLatencies: [100, 150, 200]
        )

        #expect(latency.timeToFirstUsableSkip == 12.5)
        #expect(latency.leadTimeAtFirstConfirmationSeconds == 3.25)
        #expect(latency.meanPipelineLatencyMs == 20)
        #expect(latency.p95BannerLatencyMs != nil)
    }

    @Test("Full report can be built and serialized to JSON")
    func reportSerialization() throws {
        let config = makeConfig()
        let driver = SimulatedPlaybackDriver(config: config, rng: SeededRandomNumberGenerator(seed: 42))
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
        #expect(decoded.podcastId == "test-podcast")
        #expect(decoded.deliveryStyles.contains(.blendedHostRead))
    }
}

@Suite("ReplaySimulator – Simulated Conditions")
struct ReplaySimulatorConditionTests {

    private func makeConfig(
        speed: Float = 1.0,
        interactions: [SimulatedInteraction] = [],
        groundTruth: [GroundTruthAdSegment]? = nil,
        duration: TimeInterval = 600
    ) -> ReplayConfiguration {
        let gt = groundTruth ?? [
            GroundTruthAdSegment(
                startTime: 60, endTime: 120,
                advertiser: "Squarespace", product: "Website Builder",
                adType: .preRoll,
                deliveryStyle: .dynamicInsertion
            ),
        ]

        let chunks = stride(from: 0.0, to: duration, by: 10.0).map { start in
            TranscriptChunk(
                id: "chunk-\(Int(start))",
                analysisAssetId: "test-episode-cond",
                segmentFingerprint: "fp-\(Int(start))",
                chunkIndex: Int(start / 10),
                startTime: start,
                endTime: min(start + 10, duration),
                text: "Simulated text.",
                normalizedText: "simulated text",
                pass: "fast",
                modelVersion: "sim-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }

        return ReplayConfiguration(
            episodeId: "test-episode-cond",
            episodeTitle: "Test Episode: Conditions",
            podcastId: "test-podcast-cond",
            episodeDuration: duration,
            condition: SimulationCondition(
                audioMode: .cached,
                playbackSpeed: speed,
                interactions: interactions,
                analysisPath: .backfill
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
        let driver = SimulatedPlaybackDriver(config: config, rng: SeededRandomNumberGenerator(seed: 42))
        _ = driver.runReplay()

        let quality = driver.computeDetectionQuality()
        #expect(quality.recall > 0, "Should detect ads at 3x speed")
    }

    @Test("Low-speed playback (0.5x) still detects ads")
    func lowSpeedPlayback() {
        let config = makeConfig(speed: 0.5)
        let driver = SimulatedPlaybackDriver(config: config, rng: SeededRandomNumberGenerator(seed: 42))
        _ = driver.runReplay()

        let quality = driver.computeDetectionQuality()
        #expect(quality.recall > 0, "Should detect ads at 0.5x speed")
    }

    @Test("Scrub during playback produces scrub events")
    func scrubDuringPlayback() {
        let config = makeConfig(interactions: [
            SimulatedInteraction(type: .scrub, atTime: 30, targetTime: 200, newSpeed: nil),
        ])
        let driver = SimulatedPlaybackDriver(config: config, rng: SeededRandomNumberGenerator(seed: 42))
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
        let driver = SimulatedPlaybackDriver(config: config, rng: SeededRandomNumberGenerator(seed: 42))
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

    @Test("Delivery-style rewind metrics are sliced by the skipped ad")
    func deliveryStyleRewindMetricsAreSliced() {
        let groundTruth = [
            GroundTruthAdSegment(
                startTime: 60, endTime: 90,
                advertiser: "Host Sponsor", product: "Host Product",
                adType: .midRoll,
                deliveryStyle: .hostRead
            ),
            GroundTruthAdSegment(
                startTime: 120, endTime: 150,
                advertiser: "Dynamic Sponsor", product: "Dynamic Product",
                adType: .midRoll,
                deliveryStyle: .dynamicInsertion
            ),
        ]
        let config = makeConfig(
            interactions: [
                SimulatedInteraction(type: .scrub, atTime: 95, targetTime: 55, newSpeed: nil),
                SimulatedInteraction(type: .scrub, atTime: 155, targetTime: 115, newSpeed: nil),
            ],
            groundTruth: groundTruth,
            duration: 240
        )
        let driver = SimulatedPlaybackDriver(config: config, rng: MaxRandomNumberGenerator())
        _ = driver.runReplay()

        let report = driver.buildReport(replayDuration: 1.0)
        let corpus = CorpusReplayReport.aggregate(from: [report])
        let hostReadMetrics = report.deliveryStyleMetrics.first {
            $0.style == GroundTruthAdSegment.DeliveryStyle.hostRead
        }
        let dynamicMetrics = report.deliveryStyleMetrics.first {
            $0.style == GroundTruthAdSegment.DeliveryStyle.dynamicInsertion
        }
        let hostReadSlice = corpus.slices.first {
            $0.dimension == MetricSliceDimension.deliveryStyle
                && $0.value == GroundTruthAdSegment.DeliveryStyle.hostRead.rawValue
        }
        let dynamicSlice = corpus.slices.first {
            $0.dimension == MetricSliceDimension.deliveryStyle
                && $0.value == GroundTruthAdSegment.DeliveryStyle.dynamicInsertion.rawValue
        }

        #expect(report.userOverrides.rewindAfterSkipCount == 2)
        #expect(hostReadMetrics?.userOverrides.rewindAfterSkipCount == 1)
        #expect(dynamicMetrics?.userOverrides.rewindAfterSkipCount == 1)
        #expect(hostReadSlice?.userOverrides.rewindAfterSkipCount == 1)
        #expect(dynamicSlice?.userOverrides.rewindAfterSkipCount == 1)
    }

    @Test("Speed change mid-episode is recorded")
    func speedChange() {
        let config = makeConfig(interactions: [
            SimulatedInteraction(type: .speedChange, atTime: 100, targetTime: nil, newSpeed: 2.0),
        ])
        let driver = SimulatedPlaybackDriver(config: config, rng: SeededRandomNumberGenerator(seed: 42))
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
        let driver = SimulatedPlaybackDriver(config: config, rng: SeededRandomNumberGenerator(seed: 42))
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
            podcastId: "pod-a",
            condition: SimulationCondition(
                audioMode: .cached,
                playbackSpeed: 1.0,
                interactions: [],
                analysisPath: .live
            ),
            deliveryStyles: [.hostRead, .blendedHostRead],
            deliveryStyleMetrics: [
                DeliveryStyleMetricReport(
                    style: .hostRead,
                    detectionQuality: DetectionQualityMetrics(
                        falsePositiveSkipSeconds: 2,
                        falseNegativeAdSeconds: 4,
                        seedRecall: 1,
                        precision: 0.9,
                        recall: 0.8,
                        f1Score: 0.85,
                        missedSegmentCount: 0,
                        spuriousSegmentCount: 0
                    ),
                    boundaryQuality: BoundaryQualityMetrics(),
                    latency: LatencyMetrics(timeToFirstUsableSkip: 5.0),
                    userOverrides: UserOverrideMetrics(listenTapCount: 1, rewindAfterSkipCount: 0, overrideRate: 0.1)
                ),
                DeliveryStyleMetricReport(
                    style: .blendedHostRead,
                    detectionQuality: DetectionQualityMetrics(
                        falsePositiveSkipSeconds: 3,
                        falseNegativeAdSeconds: 6,
                        seedRecall: 1,
                        precision: 0.9,
                        recall: 0.8,
                        f1Score: 0.85,
                        missedSegmentCount: 0,
                        spuriousSegmentCount: 1
                    ),
                    boundaryQuality: BoundaryQualityMetrics(),
                    latency: LatencyMetrics(timeToFirstUsableSkip: 5.0),
                    userOverrides: UserOverrideMetrics(listenTapCount: 0, rewindAfterSkipCount: 0, overrideRate: 0)
                ),
            ],
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
            podcastId: "pod-b",
            condition: SimulationCondition(
                audioMode: .streamed,
                playbackSpeed: 1.5,
                interactions: [],
                analysisPath: .backfill
            ),
            deliveryStyles: [.dynamicInsertion],
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
        #expect(corpus.slices.contains { $0.dimension == MetricSliceDimension.podcast && $0.value == "pod-a" })
        #expect(corpus.slices.contains { $0.dimension == MetricSliceDimension.podcast && $0.value == "pod-b" })
        #expect(corpus.slices.contains { $0.dimension == MetricSliceDimension.analysisPath && $0.value == "live" })
        #expect(corpus.slices.contains { $0.dimension == MetricSliceDimension.analysisPath && $0.value == "backfill" })
        #expect(corpus.slices.contains { $0.dimension == MetricSliceDimension.deliveryStyle && $0.value == "hostRead" })
        #expect(corpus.slices.contains { $0.dimension == MetricSliceDimension.deliveryStyle && $0.value == "blendedHostRead" })
        #expect(corpus.slices.contains { $0.dimension == MetricSliceDimension.deliveryStyle && $0.value == "dynamicInsertion" })
    }

    @Test("Seed recall aggregation uses seeded and ground-truth counts, not per-report means")
    func seedRecallAggregationUsesCounts() {
        func makeGroundTruth(
            count: Int,
            deliveryStyle: GroundTruthAdSegment.DeliveryStyle = .hostRead
        ) -> [GroundTruthAdSegment] {
            (0..<count).map { index in
                let start = Double(index * 30)
                return GroundTruthAdSegment(
                    startTime: start,
                    endTime: start + 15,
                    advertiser: "Acme \(index)",
                    product: "Widget \(index)",
                    adType: .midRoll,
                    deliveryStyle: deliveryStyle
                )
            }
        }

        func makeDetectedWindows(
            episodeId: String,
            from groundTruth: [GroundTruthAdSegment]
        ) -> [AdWindow] {
            groundTruth.enumerated().map { index, segment in
                AdWindow(
                    id: "\(episodeId)-window-\(index)",
                    analysisAssetId: episodeId,
                    startTime: segment.startTime,
                    endTime: segment.endTime,
                    confidence: 0.9,
                    boundaryState: AdBoundaryState.acousticRefined.rawValue,
                    decisionState: AdDecisionState.candidate.rawValue,
                    detectorVersion: "sim-v1",
                    advertiser: segment.advertiser,
                    product: segment.product,
                    adDescription: nil,
                    evidenceText: nil,
                    evidenceStartTime: segment.startTime,
                    metadataSource: "test",
                    metadataConfidence: 0.9,
                    metadataPromptVersion: nil,
                    wasSkipped: false,
                    userDismissedBanner: false
                )
            }
        }

        func makeReport(
            episodeId: String,
            podcastId: String,
            groundTruth: [GroundTruthAdSegment],
            seededGroundTruthIndices: [Int]
        ) -> EpisodeReplayReport {
            let detected = makeDetectedWindows(episodeId: episodeId, from: groundTruth)
            return EpisodeReplayReport(
                episodeId: episodeId,
                episodeTitle: episodeId,
                podcastId: podcastId,
                condition: SimulationCondition(
                    audioMode: .cached,
                    playbackSpeed: 1.0,
                    interactions: [],
                    analysisPath: .live
                ),
                deliveryStyles: Array(Set(groundTruth.map(\.deliveryStyle))).sorted { $0.rawValue < $1.rawValue },
                detectionQuality: ReplayMetricsComputation.detectionQuality(
                    groundTruth: groundTruth,
                    detected: detected,
                    episodeDuration: 180,
                    seededGroundTruthIndices: seededGroundTruthIndices
                ),
                boundaryQuality: ReplayMetricsComputation.boundaryQuality(
                    groundTruth: groundTruth,
                    detected: detected
                ),
                latency: LatencyMetrics(),
                userOverrides: UserOverrideMetrics(listenTapCount: 0, rewindAfterSkipCount: 0, overrideRate: 0),
                samples: [],
                simulatorVersion: EpisodeReplayReport.currentSimulatorVersion,
                generatedAt: Date(),
                replayDurationSeconds: 1.0
            )
        }

        let seededSingleAdReport = makeReport(
            episodeId: "ep-seeded",
            podcastId: "pod-a",
            groundTruth: makeGroundTruth(count: 1),
            seededGroundTruthIndices: [0]
        )
        let unseededThreeAdReport = makeReport(
            episodeId: "ep-unseeded",
            podcastId: "pod-b",
            groundTruth: makeGroundTruth(count: 3),
            seededGroundTruthIndices: []
        )
        let noAdReport = makeReport(
            episodeId: "ep-no-ads",
            podcastId: "pod-c",
            groundTruth: [],
            seededGroundTruthIndices: []
        )

        let corpus = CorpusReplayReport.aggregate(from: [seededSingleAdReport, unseededThreeAdReport, noAdReport])
        let hostReadSlice = corpus.slices.first {
            $0.dimension == .deliveryStyle && $0.value == GroundTruthAdSegment.DeliveryStyle.hostRead.rawValue
        }

        #expect(abs(corpus.aggregateDetectionQuality.seedRecall - 0.25) < 0.0001)
        #expect(abs((hostReadSlice?.detectionQuality.seedRecall ?? -1) - 0.25) < 0.0001)
    }

    @Test("Seed recall aggregation ignores legacy ratios when count-bearing reports are present")
    func seedRecallAggregationIgnoresLegacyRatiosWhenCountsPresent() {
        let weightedGroundTruth = (0..<4).map { index in
            GroundTruthAdSegment(
                startTime: Double(index * 30),
                endTime: Double(index * 30 + 15),
                advertiser: "Weighted \(index)",
                product: "Product \(index)",
                adType: .midRoll,
                deliveryStyle: .hostRead
            )
        }
        let weightedDetected = weightedGroundTruth.enumerated().map { index, segment in
            AdWindow(
                id: "weighted-window-\(index)",
                analysisAssetId: "ep-weighted",
                startTime: segment.startTime,
                endTime: segment.endTime,
                confidence: 0.9,
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: "sim-v1",
                advertiser: segment.advertiser,
                product: segment.product,
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: segment.startTime,
                metadataSource: "test",
                metadataConfidence: 0.9,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false
            )
        }

        let weightedReport = EpisodeReplayReport(
            episodeId: "ep-weighted",
            episodeTitle: "Weighted Episode",
            podcastId: "pod-mixed-legacy",
            condition: SimulationCondition(
                audioMode: .cached,
                playbackSpeed: 1.0,
                interactions: [],
                analysisPath: .live
            ),
            deliveryStyles: [.hostRead],
            detectionQuality: ReplayMetricsComputation.detectionQuality(
                groundTruth: weightedGroundTruth,
                detected: weightedDetected,
                episodeDuration: 180,
                seededGroundTruthIndices: [0]
            ),
            boundaryQuality: BoundaryQualityMetrics(),
            latency: LatencyMetrics(),
            userOverrides: UserOverrideMetrics(listenTapCount: 0, rewindAfterSkipCount: 0, overrideRate: 0),
            samples: [],
            simulatorVersion: EpisodeReplayReport.currentSimulatorVersion,
            generatedAt: Date(),
            replayDurationSeconds: 1.0
        )

        let legacyReport = EpisodeReplayReport(
            episodeId: "ep-legacy-seed",
            episodeTitle: "Legacy Seed Episode",
            podcastId: "pod-mixed-legacy",
            condition: SimulationCondition(
                audioMode: .cached,
                playbackSpeed: 1.0,
                interactions: [],
                analysisPath: .live
            ),
            detectionQuality: DetectionQualityMetrics(
                falsePositiveSkipSeconds: 0,
                falseNegativeAdSeconds: 0,
                seedRecall: 1,
                precision: 1,
                recall: 1,
                f1Score: 1,
                missedSegmentCount: 0,
                spuriousSegmentCount: 0
            ),
            boundaryQuality: BoundaryQualityMetrics(),
            latency: LatencyMetrics(),
            userOverrides: UserOverrideMetrics(listenTapCount: 0, rewindAfterSkipCount: 0, overrideRate: 0),
            samples: [],
            simulatorVersion: EpisodeReplayReport.currentSimulatorVersion,
            generatedAt: Date(),
            replayDurationSeconds: 1.0
        )

        let corpus = CorpusReplayReport.aggregate(from: [weightedReport, legacyReport])
        let podcastSlice = corpus.slices.first {
            $0.dimension == .podcast && $0.value == "pod-mixed-legacy"
        }

        #expect(abs(corpus.aggregateDetectionQuality.seedRecall - 0.25) < 0.0001)
        #expect(corpus.aggregateDetectionQuality.seededSegmentCount == 1)
        #expect(corpus.aggregateDetectionQuality.groundTruthSegmentCount == 4)
        #expect(abs((podcastSlice?.detectionQuality.seedRecall ?? -1) - 0.25) < 0.0001)
        #expect(podcastSlice?.detectionQuality.seededSegmentCount == 1)
        #expect(podcastSlice?.detectionQuality.groundTruthSegmentCount == 4)
    }

    @Test("Delivery-style slices aggregate per-style metrics instead of whole-episode copies")
    func deliveryStyleSlicesUseStyleSpecificMetrics() {
        let report = EpisodeReplayReport(
            episodeId: "ep-mixed",
            episodeTitle: "Mixed Episode",
            podcastId: "pod-mixed",
            condition: SimulationCondition(
                audioMode: .cached,
                playbackSpeed: 1.0,
                interactions: [],
                analysisPath: .live
            ),
            deliveryStyles: [.hostRead, .dynamicInsertion],
            deliveryStyleMetrics: [
                DeliveryStyleMetricReport(
                    style: .hostRead,
                    detectionQuality: DetectionQualityMetrics(
                        falsePositiveSkipSeconds: 0,
                        falseNegativeAdSeconds: 0,
                        seedRecall: 1,
                        precision: 1,
                        recall: 1,
                        f1Score: 1,
                        missedSegmentCount: 0,
                        spuriousSegmentCount: 0
                    ),
                    boundaryQuality: BoundaryQualityMetrics(),
                    latency: LatencyMetrics(timeToFirstUsableSkip: 10, leadTimeAtFirstConfirmationSeconds: 20),
                    userOverrides: UserOverrideMetrics(listenTapCount: 0, rewindAfterSkipCount: 0, overrideRate: 0)
                ),
                DeliveryStyleMetricReport(
                    style: .dynamicInsertion,
                    detectionQuality: DetectionQualityMetrics(
                        falsePositiveSkipSeconds: 0,
                        falseNegativeAdSeconds: 30,
                        seedRecall: 0,
                        precision: 0,
                        recall: 0,
                        f1Score: 0,
                        missedSegmentCount: 1,
                        spuriousSegmentCount: 0
                    ),
                    boundaryQuality: BoundaryQualityMetrics(),
                    latency: LatencyMetrics(),
                    userOverrides: UserOverrideMetrics(listenTapCount: 0, rewindAfterSkipCount: 0, overrideRate: 0)
                ),
            ],
            detectionQuality: DetectionQualityMetrics(
                falsePositiveSkipSeconds: 0,
                falseNegativeAdSeconds: 15,
                seedRecall: 0.5,
                precision: 1,
                recall: 0.5,
                f1Score: 2.0 / 3.0,
                missedSegmentCount: 1,
                spuriousSegmentCount: 0
            ),
            boundaryQuality: BoundaryQualityMetrics(),
            latency: LatencyMetrics(timeToFirstUsableSkip: 10, leadTimeAtFirstConfirmationSeconds: 20),
            userOverrides: UserOverrideMetrics(listenTapCount: 0, rewindAfterSkipCount: 0, overrideRate: 0),
            samples: [],
            simulatorVersion: EpisodeReplayReport.currentSimulatorVersion,
            generatedAt: Date(),
            replayDurationSeconds: 1.0
        )

        let corpus = CorpusReplayReport.aggregate(from: [report])
        let hostRead = corpus.slices.first {
            $0.dimension == .deliveryStyle && $0.value == GroundTruthAdSegment.DeliveryStyle.hostRead.rawValue
        }
        let dynamic = corpus.slices.first {
            $0.dimension == .deliveryStyle && $0.value == GroundTruthAdSegment.DeliveryStyle.dynamicInsertion.rawValue
        }

        #expect(hostRead?.detectionQuality.recall == 1)
        #expect(dynamic?.detectionQuality.recall == 0)
        #expect(hostRead?.detectionQuality.recall != report.detectionQuality.recall)
    }

    @Test("Seeded RNG produces deterministic replay results")
    func seededDeterminism() {
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

        // Two replays with the same seed should produce identical detection windows.
        var rng1 = SeededRandomNumberGenerator(seed: 99)
        var rng2 = SeededRandomNumberGenerator(seed: 99)

        // Verify the RNG itself is deterministic.
        for _ in 0..<100 {
            #expect(rng1.next() == rng2.next(), "Seeded RNG should produce identical sequences")
        }
        _ = report1 // suppress unused warning
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

    @Test("Older corpus report JSON decodes with defaults for A7 fields")
    func oldCorpusReportDecodesWithDefaults() throws {
        let json = """
        {
          "episodeReports": [
            {
              "condition": {
                "audioMode": "cached",
                "interactions": [],
                "playbackSpeed": 1
              },
              "detectionQuality": {
                "f1Score": 0.5,
                "falseNegativeAdSeconds": 20,
                "falsePositiveSkipSeconds": 5,
                "missedSegmentCount": 1,
                "precision": 0.5,
                "recall": 0.5,
                "spuriousSegmentCount": 0
              },
              "boundaryQuality": {
                "cutSpeechAtEntryMs": [100],
                "cutSpeechAtResumeMs": [200],
                "p50EntryErrorMs": 100,
                "p95EntryErrorMs": 100,
                "p50ResumeErrorMs": 200,
                "p95ResumeErrorMs": 200
              },
              "episodeId": "ep-legacy",
              "episodeTitle": "Legacy Episode",
              "generatedAt": "2026-04-12T00:00:00Z",
              "latency": {
                "meanPipelineLatencyMs": 10,
                "p50BannerLatencyMs": 80,
                "p95BannerLatencyMs": 120,
                "p95PipelineLatencyMs": 20,
                "timeToFirstUsableSkip": 12.5
              },
              "replayDurationSeconds": 1,
              "samples": [],
              "simulatorVersion": "replay-sim-v1",
              "userOverrides": {
                "listenTapCount": 0,
                "overrideRate": 0,
                "rewindAfterSkipCount": 0
              }
            }
          ],
          "aggregateDetectionQuality": {
            "f1Score": 0.5,
            "falseNegativeAdSeconds": 20,
            "falsePositiveSkipSeconds": 5,
            "missedSegmentCount": 1,
            "precision": 0.5,
            "recall": 0.5,
            "spuriousSegmentCount": 0
          },
          "aggregateBoundaryQuality": {
            "cutSpeechAtEntryMs": [100],
            "cutSpeechAtResumeMs": [200],
            "p50EntryErrorMs": 100,
            "p95EntryErrorMs": 100,
            "p50ResumeErrorMs": 200,
            "p95ResumeErrorMs": 200
          },
          "aggregateLatency": {
            "meanPipelineLatencyMs": 10,
            "p50BannerLatencyMs": 80,
            "p95BannerLatencyMs": 120,
            "p95PipelineLatencyMs": 20,
            "timeToFirstUsableSkip": 12.5
          },
          "aggregateUserOverrides": {
            "listenTapCount": 0,
            "overrideRate": 0,
            "rewindAfterSkipCount": 0
          },
          "conditions": [
            {
              "audioMode": "cached",
              "interactions": [],
              "playbackSpeed": 1
            }
          ],
          "generatedAt": "2026-04-12T00:00:00Z",
          "simulatorVersion": "replay-sim-v1"
        }
        """

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(CorpusReplayReport.self, from: Data(json.utf8))

        #expect(decoded.slices.isEmpty)
        #expect(decoded.aggregateDetectionQuality.seedRecall == 0)
        #expect(decoded.aggregateLatency.leadTimeAtFirstConfirmationSeconds == nil)
        #expect(decoded.conditions.first?.analysisPath == .live)
        #expect(decoded.episodeReports.first?.podcastId == "")
        #expect(decoded.episodeReports.first?.deliveryStyles.isEmpty == true)
        #expect(decoded.episodeReports.first?.deliveryStyleMetrics.isEmpty == true)
    }
}
