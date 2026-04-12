// FeatureExtractionSignalTests.swift
// Regression tests for the helper signals computed during feature extraction.

import Foundation
import Testing
@testable import Playhead

@Suite("FeatureExtraction signal helpers")
struct FeatureExtractionSignalTests {

    private struct FullBufferTimelineBuilder {
        final class Recorder: @unchecked Sendable {
            private let lock = NSLock()
            private(set) var sampleCounts: [Int] = []

            func record(sampleCount: Int) {
                lock.lock()
                sampleCounts.append(sampleCount)
                lock.unlock()
            }
        }

        let recorder = Recorder()
        let fullBufferSampleCount: Int
        let observations: [FeatureSignalExtraction.MusicProbabilityTimeline.Observation]

        func makeBuilder() -> @Sendable ([Float], Double) -> FeatureSignalExtraction.MusicProbabilityTimeline? {
            { samples, _ in
                recorder.record(sampleCount: samples.count)
                guard samples.count == fullBufferSampleCount else { return nil }
                return FeatureSignalExtraction.MusicProbabilityTimeline(observations: observations)
            }
        }
    }

    private func approximatelyEqual(
        _ lhs: Double,
        _ rhs: Double,
        tolerance: Double = 1e-9
    ) -> Bool {
        abs(lhs - rhs) <= tolerance
    }

    private func makeShardAwareTimelineBuilder(
        firstShardSamples: [Float],
        secondShardSamples: [Float]
    ) -> @Sendable ([Float], Double) -> FeatureSignalExtraction.MusicProbabilityTimeline? {
        { samples, _ in
            switch samples.count {
            case firstShardSamples.count + secondShardSamples.count:
                return FeatureSignalExtraction.MusicProbabilityTimeline(observations: [
                    .init(startTime: 0.0, endTime: 2.0, probability: 0.1),
                    .init(startTime: 2.0, endTime: 4.0, probability: 0.1),
                    .init(startTime: 4.0, endTime: 6.0, probability: 0.9),
                    .init(startTime: 6.0, endTime: 8.0, probability: 0.9)
                ])
            case firstShardSamples.count:
                let probability = samples.first == secondShardSamples.first ? 0.9 : 0.1
                return FeatureSignalExtraction.MusicProbabilityTimeline(observations: [
                    .init(startTime: 0.0, endTime: 2.0, probability: probability),
                    .init(startTime: 2.0, endTime: 4.0, probability: probability)
                ])
            default:
                return nil
            }
        }
    }

    private func assertMatchesWholeBufferReference(
        extracted: [FeatureWindow],
        reference: [FeatureWindow],
        tolerance: Double = 1e-6
    ) {
        #expect(extracted.count == reference.count)
        for (actual, expected) in zip(extracted, reference) {
            #expect(approximatelyEqual(actual.startTime, expected.startTime, tolerance: tolerance))
            #expect(approximatelyEqual(actual.endTime, expected.endTime, tolerance: tolerance))
            #expect(approximatelyEqual(actual.musicProbability, expected.musicProbability, tolerance: tolerance))
            #expect(approximatelyEqual(actual.musicBedChangeScore, expected.musicBedChangeScore, tolerance: tolerance))
            #expect(approximatelyEqual(actual.speakerChangeProxyScore, expected.speakerChangeProxyScore, tolerance: tolerance))
        }
    }

    private func makeFeatureExtractionConfig() -> FeatureExtractionConfig {
        FeatureExtractionConfig(
            windowDuration: 2.0,
            overlapFraction: 0.0,
            sampleRate: 8,
            fftSize: 8,
            pauseRmsThreshold: 0.03,
            featureVersion: FeatureExtractionConfig.default.featureVersion
        )
    }

    private func makeAnalysisAsset() -> AnalysisAsset {
        AnalysisAsset(
            id: "asset-1",
            episodeId: "ep-1",
            assetFingerprint: "fp-asset-1",
            weakFingerprint: nil,
            sourceURL: "file:///test/asset-1.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeFeatureWindow(
        startTime: Double,
        endTime: Double,
        musicProbability: Double,
        speakerChangeProxyScore: Double,
        musicBedChangeScore: Double,
        featureVersion: Int
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-1",
            startTime: startTime,
            endTime: endTime,
            rms: 0.05,
            spectralFlux: 0.01,
            musicProbability: musicProbability,
            speakerChangeProxyScore: speakerChangeProxyScore,
            musicBedChangeScore: musicBedChangeScore,
            pauseProbability: 0.1,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: featureVersion
        )
    }

    @Test("music probability prefers timeline observations from the full pass")
    func musicProbabilityPrefersTimelineObservations() {
        let timeline = FeatureSignalExtraction.MusicProbabilityTimeline(observations: [
            .init(startTime: 0.0, endTime: 2.0, probability: 0.25),
            .init(startTime: 2.0, endTime: 4.0, probability: 0.85)
        ])

        let earlyWindow = FeatureSignalExtraction.musicProbability(
            acousticProbability: 0.1,
            timeline: timeline,
            windowStartTime: 0.5,
            windowEndTime: 1.5
        )
        let laterWindow = FeatureSignalExtraction.musicProbability(
            acousticProbability: 0.1,
            timeline: timeline,
            windowStartTime: 2.5,
            windowEndTime: 3.5
        )

        #expect(earlyWindow == 0.25)
        #expect(laterWindow == 0.85)
    }

    @Test("music bed change score tracks the music probability derivative")
    func musicBedChangeScoreTracksDerivative() {
        let flat = FeatureSignalExtraction.musicBedChangeScore(
            currentMusicProbability: 0.4,
            previousMusicProbability: 0.4
        )
        let small = FeatureSignalExtraction.musicBedChangeScore(
            currentMusicProbability: 0.55,
            previousMusicProbability: 0.4
        )
        let large = FeatureSignalExtraction.musicBedChangeScore(
            currentMusicProbability: 0.95,
            previousMusicProbability: 0.1
        )

        #expect(flat == 0)
        #expect(small > flat)
        #expect(large > small)
        #expect(large > 0.5)
    }

    @Test("speaker change proxy smoothing uses the +/-1 weighted kernel")
    func speakerChangeProxySmoothingUsesPlusMinusOneKernel() {
        let smoothed = FeatureSignalExtraction.smoothSpeakerChangeProxyScores([0.0, 1.0, 0.0])

        #expect(smoothed.count == 3)
        #expect(approximatelyEqual(smoothed[0], 1.0 / 3.0, tolerance: 1e-6))
        #expect(approximatelyEqual(smoothed[1], 0.5, tolerance: 1e-6))
        #expect(approximatelyEqual(smoothed[2], 1.0 / 3.0, tolerance: 1e-6))
    }

    @Test("speaker change proxy rises with pause, flux, and spectral shift")
    func speakerChangeProxyRisesWithTransition() {
        let steadyMagnitudes = Array(repeating: Float(1.0), count: 32)
        let shiftedMagnitudes = (0..<32).map { index in
            index < 16 ? Float(0.1) : Float(1.9)
        }

        let quiet = FeatureSignalExtraction.speakerChangeProxyScore(
            currentRms: 0.18,
            previousRms: 0.18,
            currentMagnitudes: steadyMagnitudes,
            previousMagnitudes: steadyMagnitudes,
            pauseProbability: 0.05,
            spectralFlux: 0.03
        )

        let transition = FeatureSignalExtraction.speakerChangeProxyScore(
            currentRms: 0.34,
            previousRms: 0.08,
            currentMagnitudes: shiftedMagnitudes,
            previousMagnitudes: steadyMagnitudes,
            pauseProbability: 0.86,
            spectralFlux: 0.72
        )

        #expect(quiet < 0.25)
        #expect(transition > quiet)
        #expect(transition > 0.6)
    }

    @Test("extract uses one full-buffer timeline pass for music probability")
    func extractUsesOneFullBufferTimelinePass() async throws {
        let store = try await makeTestStore()
        let samples = Array(repeating: Float(0.001), count: 32)
        let config = makeFeatureExtractionConfig()
        let builder = FullBufferTimelineBuilder(
            fullBufferSampleCount: samples.count,
            observations: [
                .init(startTime: 0.0, endTime: 2.0, probability: 0.95),
                .init(startTime: 2.0, endTime: 4.0, probability: 0.85)
            ]
        )
        let service = FeatureExtractionService(
            store: store,
            config: config,
            musicProbabilityTimelineBuilder: builder.makeBuilder()
        )

        let windows = await service.extract(
            from: samples,
            startTime: 0,
            analysisAssetId: "asset-1"
        )

        #expect(builder.recorder.sampleCounts == [samples.count])
        #expect(windows.count == 2)
        #expect(approximatelyEqual(windows[0].musicProbability, 0.95, tolerance: 1e-6))
        #expect(approximatelyEqual(windows[1].musicProbability, 0.85, tolerance: 1e-6))
    }

    @Test("extract offsets full-buffer timeline observations for non-zero shard start times")
    func extractOffsetsTimelineForNonZeroShardStartTimes() async throws {
        let store = try await makeTestStore()
        let samples = Array(repeating: Float(0.001), count: 32)
        let config = makeFeatureExtractionConfig()
        let builder = FullBufferTimelineBuilder(
            fullBufferSampleCount: samples.count,
            observations: [
                .init(startTime: 0.0, endTime: 2.0, probability: 0.95),
                .init(startTime: 2.0, endTime: 4.0, probability: 0.85)
            ]
        )
        let service = FeatureExtractionService(
            store: store,
            config: config,
            musicProbabilityTimelineBuilder: builder.makeBuilder()
        )

        let windows = await service.extract(
            from: samples,
            startTime: 100,
            analysisAssetId: "asset-1"
        )

        #expect(windows.count == 2)
        #expect(approximatelyEqual(windows[0].musicProbability, 0.95, tolerance: 1e-6))
        #expect(approximatelyEqual(windows[1].musicProbability, 0.85, tolerance: 1e-6))
    }

    @Test("extractAndPersist carries seam cues across shard boundaries")
    func extractAndPersistCarriesSeamCuesAcrossShardBoundaries() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())

        let firstShardSamples = Array(repeating: Float(0.001), count: 32)
        let secondShardSamples = Array(repeating: Float(0.45), count: 32)
        let config = makeFeatureExtractionConfig()
        let timelineBuilder = makeShardAwareTimelineBuilder(
            firstShardSamples: firstShardSamples,
            secondShardSamples: secondShardSamples
        )
        let service = FeatureExtractionService(
            store: store,
            config: config,
            musicProbabilityTimelineBuilder: timelineBuilder
        )
        let reference = await service.extract(
            from: firstShardSamples + secondShardSamples,
            startTime: 0,
            analysisAssetId: "asset-1"
        )

        let extracted = try await service.extractAndPersist(
            shards: [
                AnalysisShard(
                    id: 0,
                    episodeID: "ep-1",
                    startTime: 0,
                    duration: 4,
                    samples: firstShardSamples
                ),
                AnalysisShard(
                    id: 1,
                    episodeID: "ep-1",
                    startTime: 4,
                    duration: 4,
                    samples: secondShardSamples
                )
            ],
            analysisAssetId: "asset-1",
            existingCoverage: 0
        )

        assertMatchesWholeBufferReference(extracted: extracted, reference: reference)
    }

    @Test("extractAndPersist keeps seam smoothing when shards arrive in separate calls")
    func extractAndPersistPreservesSeamStateAcrossCalls() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())

        let firstShardSamples = Array(repeating: Float(0.001), count: 32)
        let secondShardSamples = Array(repeating: Float(0.45), count: 32)
        let config = makeFeatureExtractionConfig()
        let timelineBuilder = makeShardAwareTimelineBuilder(
            firstShardSamples: firstShardSamples,
            secondShardSamples: secondShardSamples
        )
        let service = FeatureExtractionService(
            store: store,
            config: config,
            musicProbabilityTimelineBuilder: timelineBuilder
        )
        let reference = await service.extract(
            from: firstShardSamples + secondShardSamples,
            startTime: 0,
            analysisAssetId: "asset-1"
        )

        let firstBatch = try await service.extractAndPersist(
            shards: [
                AnalysisShard(
                    id: 0,
                    episodeID: "ep-1",
                    startTime: 0,
                    duration: 4,
                    samples: firstShardSamples
                )
            ],
            analysisAssetId: "asset-1",
            existingCoverage: 0
        )
        let checkpoint = try await store.fetchFeatureExtractionCheckpoint(
            assetId: "asset-1",
            featureVersion: config.featureVersion,
            endingAt: 4
        )
        let secondBatch = try await service.extractAndPersist(
            shards: [
                AnalysisShard(
                    id: 1,
                    episodeID: "ep-1",
                    startTime: 4,
                    duration: 4,
                    samples: secondShardSamples
                )
            ],
            analysisAssetId: "asset-1",
            existingCoverage: 4
        )
        let fetched = try await store.fetchFeatureWindows(assetId: "asset-1", from: 0, to: 8)

        #expect(firstBatch.count == 2)
        #expect(secondBatch.count == 2)
        #expect(checkpoint != nil)
        assertMatchesWholeBufferReference(extracted: fetched, reference: reference)
    }

    @Test("extractAndPersist rewinds stale feature coverage and replaces older-version rows")
    func extractAndPersistReplacesStaleFeatureVersionRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAnalysisAsset())
        let staleFeatureVersion = FeatureExtractionConfig.default.featureVersion - 1
        try await store.insertFeatureWindows([
            makeFeatureWindow(
                startTime: 0,
                endTime: 2,
                musicProbability: 0,
                speakerChangeProxyScore: 0,
                musicBedChangeScore: 0,
                featureVersion: staleFeatureVersion
            ),
            makeFeatureWindow(
                startTime: 2,
                endTime: 4,
                musicProbability: 0,
                speakerChangeProxyScore: 0,
                musicBedChangeScore: 0,
                featureVersion: staleFeatureVersion
            )
        ])
        try await store.updateFeatureCoverage(id: "asset-1", endTime: 4)

        let samples = Array(repeating: Float(0.001), count: 32)
        let config = makeFeatureExtractionConfig()
        let builder = FullBufferTimelineBuilder(
            fullBufferSampleCount: samples.count,
            observations: [
                .init(startTime: 0.0, endTime: 2.0, probability: 0.95),
                .init(startTime: 2.0, endTime: 4.0, probability: 0.85)
            ]
        )
        let service = FeatureExtractionService(
            store: store,
            config: config,
            musicProbabilityTimelineBuilder: builder.makeBuilder()
        )

        let extracted = try await service.extractAndPersist(
            shards: [
                AnalysisShard(
                    id: 0,
                    episodeID: "ep-1",
                    startTime: 0,
                    duration: 4,
                    samples: samples
                )
            ],
            analysisAssetId: "asset-1",
            existingCoverage: 4
        )
        let fetched = try await store.fetchFeatureWindows(assetId: "asset-1", from: 0, to: 4)
        let asset = try await store.fetchAsset(id: "asset-1")

        #expect(extracted.count == 2)
        #expect(fetched.count == 2)
        #expect(fetched.allSatisfy { $0.featureVersion == config.featureVersion })
        #expect(approximatelyEqual(fetched[0].musicProbability, 0.95, tolerance: 1e-6))
        #expect(approximatelyEqual(fetched[1].musicProbability, 0.85, tolerance: 1e-6))
        #expect(asset?.featureCoverageEndTime == 4)
    }
}
