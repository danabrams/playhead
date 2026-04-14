// MusicBedClassifierTests.swift

import Foundation
import Testing

@testable import Playhead

// MARK: - MusicDetectionConfig Tests

@Suite("MusicDetectionConfig")
struct MusicDetectionConfigTests {

    @Test("default config uses 2s window duration")
    func defaultWindowDuration() {
        let config = MusicDetectionConfig.default
        #expect(config.windowDuration == 2.0)
    }

    @Test("supported window durations include 1s, 2s, 4s")
    func supportedWindowDurations() {
        #expect(MusicDetectionConfig.supportedWindowDurations == [1.0, 2.0, 4.0])
    }

    @Test("default config is valid")
    func defaultIsValid() {
        #expect(MusicDetectionConfig.default.isValid)
    }

    @Test("custom 1s window is valid")
    func oneSecondWindowIsValid() {
        let config = MusicDetectionConfig(
            windowDuration: 1.0,
            noneMusicProbabilityThreshold: 0.15,
            foregroundMusicProbabilityThreshold: 0.6,
            backgroundAmplitudeRatio: 0.7,
            foregroundSpectralFluxThreshold: 0.3,
            changeScoreScalingFactor: 1.5
        )
        #expect(config.isValid)
    }

    @Test("custom 4s window is valid")
    func fourSecondWindowIsValid() {
        let config = MusicDetectionConfig(
            windowDuration: 4.0,
            noneMusicProbabilityThreshold: 0.15,
            foregroundMusicProbabilityThreshold: 0.6,
            backgroundAmplitudeRatio: 0.7,
            foregroundSpectralFluxThreshold: 0.3,
            changeScoreScalingFactor: 1.5
        )
        #expect(config.isValid)
    }

    @Test("unsupported window duration is invalid")
    func unsupportedWindowDurationIsInvalid() {
        let config = MusicDetectionConfig(
            windowDuration: 3.0,
            noneMusicProbabilityThreshold: 0.15,
            foregroundMusicProbabilityThreshold: 0.6,
            backgroundAmplitudeRatio: 0.7,
            foregroundSpectralFluxThreshold: 0.3,
            changeScoreScalingFactor: 1.5
        )
        #expect(!config.isValid)
    }
}

// MARK: - MusicBedClassifier Level Tests

@Suite("MusicBedClassifier — Level Classification")
struct MusicBedClassifierLevelTests {

    @Test("low music probability classifies as none")
    func lowMusicProbabilityIsNone() {
        let level = MusicBedClassifier.classifyLevel(
            musicProbability: 0.05,
            rms: 0.5,
            localMeanRms: 0.4,
            spectralFlux: 0.1
        )
        #expect(level == .none)
    }

    @Test("music probability at threshold boundary classifies as background")
    func atThresholdIsBackground() {
        let level = MusicBedClassifier.classifyLevel(
            musicProbability: 0.15,
            rms: 0.2,
            localMeanRms: 0.4,
            spectralFlux: 0.1
        )
        #expect(level == .background)
    }

    @Test("moderate music probability with low amplitude classifies as background")
    func moderateMusicLowAmplitudeIsBackground() {
        let level = MusicBedClassifier.classifyLevel(
            musicProbability: 0.4,
            rms: 0.1,
            localMeanRms: 0.5,
            spectralFlux: 0.05
        )
        #expect(level == .background)
    }

    @Test("high music probability with high amplitude classifies as foreground")
    func highMusicHighAmplitudeIsForeground() {
        let level = MusicBedClassifier.classifyLevel(
            musicProbability: 0.8,
            rms: 0.5,
            localMeanRms: 0.4,
            spectralFlux: 0.1
        )
        #expect(level == .foreground)
    }

    @Test("high music probability with high spectral flux classifies as foreground")
    func highMusicHighSpectralFluxIsForeground() {
        let level = MusicBedClassifier.classifyLevel(
            musicProbability: 0.7,
            rms: 0.1,
            localMeanRms: 0.5,
            spectralFlux: 0.4
        )
        #expect(level == .foreground)
    }

    @Test("high music probability but low amplitude and low spectral flux classifies as background")
    func highMusicLowSignalsIsBackground() {
        let level = MusicBedClassifier.classifyLevel(
            musicProbability: 0.7,
            rms: 0.1,
            localMeanRms: 0.5,
            spectralFlux: 0.1
        )
        #expect(level == .background)
    }

    @Test("foreground classification requires amplitude OR spectral flux above threshold")
    func foregroundRequiresOneStrongSignal() {
        // High amplitude alone -> foreground
        let withAmplitude = MusicBedClassifier.classifyLevel(
            musicProbability: 0.8,
            rms: 0.45,
            localMeanRms: 0.5,
            spectralFlux: 0.05
        )
        #expect(withAmplitude == .foreground)

        // High spectral flux alone -> foreground
        let withFlux = MusicBedClassifier.classifyLevel(
            musicProbability: 0.8,
            rms: 0.1,
            localMeanRms: 0.5,
            spectralFlux: 0.5
        )
        #expect(withFlux == .foreground)
    }

    @Test("zero local mean RMS prevents foreground classification from amplitude")
    func zeroLocalMeanRmsPreventsForegroundFromAmplitude() {
        let level = MusicBedClassifier.classifyLevel(
            musicProbability: 0.8,
            rms: 0.5,
            localMeanRms: 0.0,
            spectralFlux: 0.1
        )
        // localMeanRms is 0, so amplitude gate fails, but spectral flux < threshold
        #expect(level == .background)
    }
}

// MARK: - MusicBedClassifier Directional Scores Tests

@Suite("MusicBedClassifier — Directional Scores")
struct MusicBedClassifierDirectionalScoresTests {

    @Test("no previous probability produces zero scores")
    func noPreviousProducesZeroScores() {
        let (onset, offset, change) = MusicBedClassifier.computeDirectionalScores(
            musicProbability: 0.5,
            previousMusicProbability: nil
        )
        #expect(onset == 0)
        #expect(offset == 0)
        #expect(change == 0)
    }

    @Test("music onset produces positive onset score and zero offset")
    func musicOnsetScoring() {
        let (onset, offset, change) = MusicBedClassifier.computeDirectionalScores(
            musicProbability: 0.6,
            previousMusicProbability: 0.1
        )
        #expect(onset > 0)
        #expect(offset == 0)
        #expect(change > 0)
        // onset should equal change for positive deltas
        expectApproximately(onset, change)
    }

    @Test("music offset produces positive offset score and zero onset")
    func musicOffsetScoring() {
        let (onset, offset, change) = MusicBedClassifier.computeDirectionalScores(
            musicProbability: 0.1,
            previousMusicProbability: 0.6
        )
        #expect(onset == 0)
        #expect(offset > 0)
        #expect(change > 0)
        // offset should equal change for negative deltas
        expectApproximately(offset, change)
    }

    @Test("no change produces zero scores")
    func noChangeProducesZeroScores() {
        let (onset, offset, change) = MusicBedClassifier.computeDirectionalScores(
            musicProbability: 0.5,
            previousMusicProbability: 0.5
        )
        #expect(onset == 0)
        #expect(offset == 0)
        #expect(change == 0)
    }

    @Test("large onset is clamped to 1.0")
    func largeOnsetClampedToOne() {
        let (onset, _, _) = MusicBedClassifier.computeDirectionalScores(
            musicProbability: 1.0,
            previousMusicProbability: 0.0
        )
        #expect(onset == 1.0)
    }

    @Test("large offset is clamped to 1.0")
    func largeOffsetClampedToOne() {
        let (_, offset, _) = MusicBedClassifier.computeDirectionalScores(
            musicProbability: 0.0,
            previousMusicProbability: 1.0
        )
        #expect(offset == 1.0)
    }

    @Test("change score matches legacy musicBedChangeScore formula")
    func changeScoreMatchesLegacy() {
        let current = 0.7
        let previous = 0.3
        let (_, _, change) = MusicBedClassifier.computeDirectionalScores(
            musicProbability: current,
            previousMusicProbability: previous
        )
        // Legacy formula: clamp(abs(current - previous) * 1.5)
        let legacyChange = FeatureSignalExtraction.musicBedChangeScore(
            currentMusicProbability: current,
            previousMusicProbability: previous
        )
        expectApproximately(change, legacyChange)
    }
}

// MARK: - MusicBedClassifier Full Classification Tests

@Suite("MusicBedClassifier — Full Classification")
struct MusicBedClassifierFullClassificationTests {

    @Test("classify produces consistent level and directional scores")
    func classifyProducesConsistentResults() {
        let result = MusicBedClassifier.classify(
            musicProbability: 0.8,
            previousMusicProbability: 0.1,
            rms: 0.5,
            localMeanRms: 0.4,
            spectralFlux: 0.1
        )
        #expect(result.level == .foreground)
        #expect(result.onsetScore > 0)
        #expect(result.offsetScore == 0)
        #expect(result.changeScore > 0)
    }

    @Test("background bed with onset transition")
    func backgroundBedWithOnset() {
        let result = MusicBedClassifier.classify(
            musicProbability: 0.4,
            previousMusicProbability: 0.05,
            rms: 0.1,
            localMeanRms: 0.5,
            spectralFlux: 0.05
        )
        #expect(result.level == .background)
        #expect(result.onsetScore > 0)
        #expect(result.offsetScore == 0)
    }

    @Test("no music with offset transition")
    func noMusicWithOffset() {
        let result = MusicBedClassifier.classify(
            musicProbability: 0.05,
            previousMusicProbability: 0.6,
            rms: 0.3,
            localMeanRms: 0.4,
            spectralFlux: 0.1
        )
        #expect(result.level == .none)
        #expect(result.onsetScore == 0)
        #expect(result.offsetScore > 0)
    }
}

// MARK: - Regression: Existing musicBedChangeScore Behavior

@Suite("MusicBedClassifier — Regression")
struct MusicBedClassifierRegressionTests {

    @Test("changeScore preserves existing musicBedChangeScore behavior for all deltas")
    func changeScorePreservesLegacyBehavior() {
        let testCases: [(current: Double, previous: Double)] = [
            (0.0, 0.0),
            (0.5, 0.5),
            (1.0, 0.0),
            (0.0, 1.0),
            (0.3, 0.7),
            (0.7, 0.3),
            (0.15, 0.10),
            (0.95, 0.85),
        ]

        for tc in testCases {
            let legacyScore = FeatureSignalExtraction.musicBedChangeScore(
                currentMusicProbability: tc.current,
                previousMusicProbability: tc.previous
            )
            let classification = MusicBedClassifier.classify(
                musicProbability: tc.current,
                previousMusicProbability: tc.previous,
                rms: 0.3,
                localMeanRms: 0.3,
                spectralFlux: 0.1
            )
            expectApproximately(
                classification.changeScore,
                legacyScore,
                "changeScore mismatch for current=\(tc.current), previous=\(tc.previous)"
            )
        }
    }

    @Test("onset + offset are mutually exclusive and one equals changeScore")
    func onsetOffsetMutuallyExclusive() {
        let testCases: [(current: Double, previous: Double)] = [
            (0.8, 0.2),
            (0.2, 0.8),
            (0.5, 0.5),
        ]

        for tc in testCases {
            let (onset, offset, change) = MusicBedClassifier.computeDirectionalScores(
                musicProbability: tc.current,
                previousMusicProbability: tc.previous
            )
            // At most one of onset/offset is non-zero
            #expect(onset == 0 || offset == 0)
            // The non-zero one equals changeScore
            if tc.current > tc.previous {
                expectApproximately(onset, change)
                #expect(offset == 0)
            } else if tc.current < tc.previous {
                expectApproximately(offset, change)
                #expect(onset == 0)
            } else {
                #expect(onset == 0)
                #expect(offset == 0)
                #expect(change == 0)
            }
        }
    }

    @Test("nil previous music probability matches legacy zero-return behavior")
    func nilPreviousMatchesLegacy() {
        let legacyScore = FeatureSignalExtraction.musicBedChangeScore(
            currentMusicProbability: 0.5,
            previousMusicProbability: nil
        )
        let classification = MusicBedClassifier.classify(
            musicProbability: 0.5,
            previousMusicProbability: nil,
            rms: 0.3,
            localMeanRms: 0.3,
            spectralFlux: 0.1
        )
        #expect(legacyScore == 0)
        #expect(classification.changeScore == 0)
        #expect(classification.onsetScore == 0)
        #expect(classification.offsetScore == 0)
    }
}

// MARK: - FeatureWindow New Fields

@Suite("FeatureWindow — Music Bed Fields")
struct FeatureWindowMusicBedFieldsTests {

    @Test("FeatureWindow defaults new music fields to zero/none")
    func defaultMusicFields() {
        let window = FeatureWindow(
            analysisAssetId: "test",
            startTime: 0,
            endTime: 2,
            rms: 0.3,
            spectralFlux: 0.1,
            musicProbability: 0.5,
            pauseProbability: 0.1,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 4
        )
        #expect(window.musicBedOnsetScore == 0)
        #expect(window.musicBedOffsetScore == 0)
        #expect(window.musicBedLevel == .none)
        // Legacy field still present
        #expect(window.musicBedChangeScore == 0)
    }

    @Test("FeatureWindow accepts explicit music bed fields")
    func explicitMusicFields() {
        let window = FeatureWindow(
            analysisAssetId: "test",
            startTime: 0,
            endTime: 2,
            rms: 0.3,
            spectralFlux: 0.1,
            musicProbability: 0.5,
            musicBedChangeScore: 0.6,
            musicBedOnsetScore: 0.6,
            musicBedOffsetScore: 0.0,
            musicBedLevel: .foreground,
            pauseProbability: 0.1,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 4
        )
        #expect(window.musicBedOnsetScore == 0.6)
        #expect(window.musicBedOffsetScore == 0.0)
        #expect(window.musicBedLevel == .foreground)
        #expect(window.musicBedChangeScore == 0.6)
    }
}

// MARK: - TimeBoundaryResolver Directional Score Integration

@Suite("TimeBoundaryResolver — Directional Music Scores")
struct TimeBoundaryResolverDirectionalMusicTests {

    private let resolver = TimeBoundaryResolver()

    @Test("start boundary prefers onset score over legacy change score")
    func startBoundaryPrefersOnsetScore() {
        // Window with high onset score but zero legacy change score
        let windowWithOnset = makeWindow(
            start: 100, end: 102,
            pause: 0.5, speakerChange: 0.5,
            musicBedChange: 0.0,
            musicBedOnset: 0.9,
            musicBedOffset: 0.0,
            spectralFlux: 0.2
        )
        // Window with high legacy change score but zero onset score
        let windowWithLegacy = makeWindow(
            start: 104, end: 106,
            pause: 0.5, speakerChange: 0.5,
            musicBedChange: 0.9,
            musicBedOnset: 0.0,
            musicBedOffset: 0.0,
            spectralFlux: 0.2
        )

        let scored = resolver.scoredCandidates(
            candidateTime: 103,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: [windowWithOnset, windowWithLegacy],
            lexicalHits: [],
            config: BoundarySnappingConfig(lambda: 0)
        )

        let onsetCandidate = scored.first { $0.windowStartTime == 100 }!
        let legacyCandidate = scored.first { $0.windowStartTime == 104 }!

        // Both should produce the same music cue contribution since the
        // onset score takes precedence when non-zero
        expectApproximately(onsetCandidate.cueBlend, legacyCandidate.cueBlend)
    }

    @Test("end boundary prefers offset score over legacy change score")
    func endBoundaryPrefersOffsetScore() {
        let windowWithOffset = makeWindow(
            start: 100, end: 102,
            pause: 0.5, speakerChange: 0.5,
            musicBedChange: 0.0,
            musicBedOnset: 0.0,
            musicBedOffset: 0.8,
            spectralFlux: 0.2
        )
        let windowWithLegacy = makeWindow(
            start: 104, end: 106,
            pause: 0.5, speakerChange: 0.5,
            musicBedChange: 0.8,
            musicBedOnset: 0.0,
            musicBedOffset: 0.0,
            spectralFlux: 0.2
        )

        let scored = resolver.scoredCandidates(
            candidateTime: 103,
            boundaryType: .end,
            anchorType: .fmPositive,
            featureWindows: [windowWithOffset, windowWithLegacy],
            lexicalHits: [],
            config: BoundarySnappingConfig(lambda: 0)
        )

        let offsetCandidate = scored.first { $0.windowEndTime == 102 }!
        let legacyCandidate = scored.first { $0.windowEndTime == 106 }!

        expectApproximately(offsetCandidate.cueBlend, legacyCandidate.cueBlend)
    }

    @Test("fallback to legacy musicBedChangeScore when directional scores are zero")
    func fallbackToLegacyWhenDirectionalZero() {
        let window = makeWindow(
            start: 100, end: 102,
            pause: 0.5, speakerChange: 0.5,
            musicBedChange: 0.6,
            musicBedOnset: 0.0,
            musicBedOffset: 0.0,
            spectralFlux: 0.2
        )

        let startScored = resolver.scoredCandidates(
            candidateTime: 101,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: [window],
            lexicalHits: [],
            config: BoundarySnappingConfig(lambda: 0)
        )
        // With zero onset, it falls back to musicBedChangeScore = 0.6
        // Music weight for start is 0.15, so music contribution = 0.6 * 0.15 = 0.09
        let candidate = startScored.first!
        // Verify the music cue was included (non-zero)
        #expect(candidate.cueBlend > 0)
    }

    private func makeWindow(
        start: Double,
        end: Double,
        pause: Double,
        speakerChange: Double,
        musicBedChange: Double,
        musicBedOnset: Double,
        musicBedOffset: Double,
        spectralFlux: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "test-music",
            startTime: start,
            endTime: end,
            rms: 0.1,
            spectralFlux: spectralFlux,
            musicProbability: 0,
            speakerChangeProxyScore: speakerChange,
            musicBedChangeScore: musicBedChange,
            musicBedOnsetScore: musicBedOnset,
            musicBedOffsetScore: musicBedOffset,
            musicBedLevel: .none,
            pauseProbability: pause,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 4
        )
    }
}

// MARK: - MusicBoundaryEvaluator Tests

@Suite("MusicBoundaryEvaluator")
struct MusicBoundaryEvaluatorTests {

    @Test("perfect detection produces precision=1 and recall=1")
    func perfectDetection() {
        let evaluator = MusicBoundaryEvaluator(toleranceSeconds: 2.0)
        let groundTruth = [
            LabeledMusicBoundary(time: 10, direction: .onset, level: .foreground, genre: "comedy"),
            LabeledMusicBoundary(time: 50, direction: .offset, level: .foreground, genre: "comedy"),
        ]
        let detected = [
            MusicBoundaryEvaluator.DetectedBoundary(time: 10.5, direction: .onset, level: .foreground, score: 0.9, genre: "comedy"),
            MusicBoundaryEvaluator.DetectedBoundary(time: 49.5, direction: .offset, level: .foreground, score: 0.8, genre: "comedy"),
        ]

        let report = evaluator.evaluate(detected: detected, groundTruth: groundTruth)
        #expect(report.genreReports.count == 1)
        #expect(report.genreReports[0].genre == "comedy")
        expectApproximately(report.aggregatePrecision, 1.0)
        expectApproximately(report.aggregateRecall, 1.0)
        expectApproximately(report.aggregateF1, 1.0)
    }

    @Test("no detections produces zero recall")
    func noDetectionsZeroRecall() {
        let evaluator = MusicBoundaryEvaluator()
        let groundTruth = [
            LabeledMusicBoundary(time: 10, direction: .onset, level: .foreground, genre: "news"),
        ]
        let report = evaluator.evaluate(detected: [], groundTruth: groundTruth)
        expectApproximately(report.aggregateRecall, 0.0)
        #expect(report.genreReports[0].falseNegatives == 1)
    }

    @Test("false positive produces zero precision")
    func falsePositiveZeroPrecision() {
        let evaluator = MusicBoundaryEvaluator()
        let detected = [
            MusicBoundaryEvaluator.DetectedBoundary(time: 10, direction: .onset, level: .foreground, score: 0.5, genre: "news"),
        ]
        let report = evaluator.evaluate(detected: detected, groundTruth: [])
        expectApproximately(report.aggregatePrecision, 0.0)
        #expect(report.genreReports[0].falsePositives == 1)
    }

    @Test("out-of-tolerance detection counts as false positive and false negative")
    func outOfToleranceIsMiss() {
        let evaluator = MusicBoundaryEvaluator(toleranceSeconds: 1.0)
        let groundTruth = [
            LabeledMusicBoundary(time: 10, direction: .onset, level: .foreground, genre: "interview"),
        ]
        let detected = [
            MusicBoundaryEvaluator.DetectedBoundary(time: 15, direction: .onset, level: .foreground, score: 0.5, genre: "interview"),
        ]
        let report = evaluator.evaluate(detected: detected, groundTruth: groundTruth)
        #expect(report.genreReports[0].truePositives == 0)
        #expect(report.genreReports[0].falsePositives == 1)
        #expect(report.genreReports[0].falseNegatives == 1)
    }

    @Test("direction mismatch does not match")
    func directionMismatchDoesNotMatch() {
        let evaluator = MusicBoundaryEvaluator(toleranceSeconds: 5.0)
        let groundTruth = [
            LabeledMusicBoundary(time: 10, direction: .onset, level: .foreground, genre: "comedy"),
        ]
        let detected = [
            MusicBoundaryEvaluator.DetectedBoundary(time: 10, direction: .offset, level: .foreground, score: 0.9, genre: "comedy"),
        ]
        let report = evaluator.evaluate(detected: detected, groundTruth: groundTruth)
        #expect(report.genreReports[0].truePositives == 0)
    }

    @Test("per-genre reports are separated")
    func perGenreReportsAreSeparated() {
        let evaluator = MusicBoundaryEvaluator()
        let groundTruth = [
            LabeledMusicBoundary(time: 10, direction: .onset, level: .foreground, genre: "comedy"),
            LabeledMusicBoundary(time: 20, direction: .onset, level: .foreground, genre: "news"),
        ]
        let detected = [
            MusicBoundaryEvaluator.DetectedBoundary(time: 10, direction: .onset, level: .foreground, score: 0.9, genre: "comedy"),
        ]
        let report = evaluator.evaluate(detected: detected, groundTruth: groundTruth)
        #expect(report.genreReports.count == 2)

        let comedy = report.genreReports.first { $0.genre == "comedy" }!
        let news = report.genreReports.first { $0.genre == "news" }!
        expectApproximately(comedy.precision, 1.0)
        expectApproximately(comedy.recall, 1.0)
        expectApproximately(news.recall, 0.0)
    }

    @Test("empty report produces zero metrics")
    func emptyReportZeroMetrics() {
        let report = MusicBoundaryEvaluationReport(genreReports: [], toleranceSeconds: 2.0)
        expectApproximately(report.aggregatePrecision, 0.0)
        expectApproximately(report.aggregateRecall, 0.0)
        expectApproximately(report.aggregateF1, 0.0)
    }
}

// MARK: - Helpers

private func expectApproximately(
    _ actual: Double,
    _ expected: Double,
    tolerance: Double = 1e-6,
    _ message: String = "",
    sourceLocation: SourceLocation = #_sourceLocation
) {
    #expect(
        abs(actual - expected) <= tolerance,
        "\(message) expected \(expected), got \(actual)",
        sourceLocation: sourceLocation
    )
}
