// TimeBoundaryResolverTests.swift
// Regression tests for seconds-based boundary snapping.

import Testing
import Foundation

@testable import Playhead

@Suite("TimeBoundaryResolver")
struct TimeBoundaryResolverTests {

    private let resolver = TimeBoundaryResolver()

    @Test("start scoring uses the weighted cue blend")
    func startScoringFormula() {
        let featureWindows = [
            makeFeatureWindow(
                start: 100,
                end: 104,
                pauseProb: 0.8,
                speakerChangeProxyScore: 0.4,
                musicBedChangeScore: 0.2,
                spectralFlux: 1.0
            ),
            makeFeatureWindow(
                start: 106,
                end: 110,
                pauseProb: 0.0,
                speakerChangeProxyScore: 0.0,
                musicBedChangeScore: 0.0,
                spectralFlux: 3.0
            ),
        ]

        let config = BoundarySnappingConfig(
            lambda: 0.0,
            minBoundaryScore: 0.0,
            minImprovementOverOriginal: -1.0
        )

        #expect(abs(config.startCueWeights.totalWeight - 1.0) < 0.0001)
        #expect(abs(config.endCueWeights.totalWeight - 1.0) < 0.0001)

        let result = resolver.snap(
            candidateTime: 100.0,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: featureWindows,
            lexicalHits: [],
            config: config
        )

        #expect(abs(result.score - 0.41) < 0.0001)
        #expect(result.time == 100.0)
    }

    @Test("distance penalty favors the nearer candidate over a higher-scoring distant one")
    func distancePenaltyPrefersNearerCandidate() {
        let featureWindows = [
            makeFeatureWindow(
                start: 100,
                end: 101,
                pauseProb: 1.0,
                speakerChangeProxyScore: 1.0,
                musicBedChangeScore: 1.0,
                spectralFlux: 1.0
            ),
            makeFeatureWindow(
                start: 100,
                end: 115,
                pauseProb: 1.0,
                speakerChangeProxyScore: 1.0,
                musicBedChangeScore: 1.0,
                spectralFlux: 1.0
            ),
        ]

        let lexicalHits = [
            makeLexicalHit(
                category: .transitionMarker,
                start: 114.5,
                end: 114.9
            ),
        ]

        let result = resolver.snap(
            candidateTime: 100.0,
            boundaryType: .end,
            anchorType: .disclosure,
            featureWindows: featureWindows,
            lexicalHits: lexicalHits,
            config: BoundarySnappingConfig(
                lambda: 0.3,
                minBoundaryScore: 0.0,
                minImprovementOverOriginal: -1.0
            )
        )

        #expect(result.didSnap)
        #expect(result.time == 101.0)
        #expect(result.score > 0.7)
    }

    @Test("anchor type changes the maximum snap distance for start and end boundaries")
    func asymmetricSnapDistances() {
        let startWindows = [
            makeFeatureWindow(
                start: 112,
                end: 113,
                pauseProb: 1.0,
                speakerChangeProxyScore: 1.0,
                musicBedChangeScore: 1.0,
                spectralFlux: 1.0
            ),
        ]
        let endWindows = [
            makeFeatureWindow(
                start: 87,
                end: 88,
                pauseProb: 1.0,
                speakerChangeProxyScore: 1.0,
                musicBedChangeScore: 1.0,
                spectralFlux: 1.0
            ),
        ]
        let config = BoundarySnappingConfig(
            lambda: 0.0,
            minBoundaryScore: 0.0,
            minImprovementOverOriginal: -1.0
        )

        let startDisclosure = resolver.snap(
            candidateTime: 100.0,
            boundaryType: .start,
            anchorType: .disclosure,
            featureWindows: startWindows,
            lexicalHits: [],
            config: config
        )
        let startURL = resolver.snap(
            candidateTime: 100.0,
            boundaryType: .start,
            anchorType: .url,
            featureWindows: startWindows,
            lexicalHits: [],
            config: config
        )
        let startFMPositive = resolver.snap(
            candidateTime: 100.0,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: startWindows,
            lexicalHits: [],
            config: config
        )

        let endDisclosure = resolver.snap(
            candidateTime: 100.0,
            boundaryType: .end,
            anchorType: .disclosure,
            featureWindows: endWindows,
            lexicalHits: [],
            config: config
        )
        let endURL = resolver.snap(
            candidateTime: 100.0,
            boundaryType: .end,
            anchorType: .url,
            featureWindows: endWindows,
            lexicalHits: [],
            config: config
        )
        let endFMPositive = resolver.snap(
            candidateTime: 100.0,
            boundaryType: .end,
            anchorType: .fmPositive,
            featureWindows: endWindows,
            lexicalHits: [],
            config: config
        )

        #expect(!startDisclosure.didSnap)
        #expect(startURL.didSnap)
        #expect(!startFMPositive.didSnap)
        #expect(endDisclosure.didSnap)
        #expect(!endURL.didSnap)
        #expect(!endFMPositive.didSnap)
    }

    @Test("boundary snapping falls back when no candidate clears the minimum score")
    func fallsBackBelowMinimumBoundaryScore() {
        let featureWindows = [
            makeFeatureWindow(
                start: 100,
                end: 101,
                pauseProb: 0.15,
                speakerChangeProxyScore: 0.0,
                musicBedChangeScore: 0.0,
                spectralFlux: 0.0
            ),
        ]

        let result = resolver.snap(
            candidateTime: 100.0,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: featureWindows,
            lexicalHits: [],
            config: BoundarySnappingConfig(
                lambda: 0.0,
                minBoundaryScore: 0.3,
                minImprovementOverOriginal: 0.1
            )
        )

        #expect(!result.didSnap)
        #expect(result.time == 100.0)
    }

    @Test("boundary snapping falls back when improvement over the original is too small")
    func fallsBackBelowImprovementThreshold() {
        let featureWindows = [
            makeFeatureWindow(
                start: 100,
                end: 101,
                pauseProb: 0.4,
                speakerChangeProxyScore: 0.0,
                musicBedChangeScore: 0.0,
                spectralFlux: 0.0
            ),
            makeFeatureWindow(
                start: 105,
                end: 106,
                pauseProb: 0.45,
                speakerChangeProxyScore: 0.0,
                musicBedChangeScore: 0.0,
                spectralFlux: 0.0
            ),
        ]

        let config = BoundarySnappingConfig(
            startCueWeights: BoundaryCueWeights(
                pauseVAD: 1.0,
                speakerChangeProxy: 0.0,
                musicBedChange: 0.0,
                spectralChange: 0.0,
                lexicalDensityDelta: 0.0,
                returnMarker: 0.0
            ),
            endCueWeights: BoundaryCueWeights(
                pauseVAD: 1.0,
                speakerChangeProxy: 0.0,
                musicBedChange: 0.0,
                spectralChange: 0.0,
                lexicalDensityDelta: 0.0,
                returnMarker: 0.0
            ),
            lambda: 0.0,
            minBoundaryScore: 0.3,
            minImprovementOverOriginal: 0.1
        )

        let result = resolver.snap(
            candidateTime: 100.0,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: featureWindows,
            lexicalHits: [],
            config: config
        )

        #expect(!result.didSnap)
        #expect(result.time == 100.0)
    }

    @Test("lexical density delta is driven by overlapping ad-category hits per adjacent window")
    func lexicalDensityDelta() {
        let featureWindows = [
            makeFeatureWindow(
                start: 90,
                end: 95,
                pauseProb: 0.0,
                speakerChangeProxyScore: 0.0,
                musicBedChangeScore: 0.0,
                spectralFlux: 0.0
            ),
            makeFeatureWindow(
                start: 100,
                end: 105,
                pauseProb: 0.0,
                speakerChangeProxyScore: 0.0,
                musicBedChangeScore: 0.0,
                spectralFlux: 0.0
            ),
        ]
        let lexicalHits = [
            makeLexicalHit(category: .sponsor, start: 100.5, end: 101.5),
            makeLexicalHit(category: .urlCTA, start: 102.0, end: 103.0),
        ]
        let config = BoundarySnappingConfig(
            startCueWeights: BoundaryCueWeights(
                pauseVAD: 0.0,
                speakerChangeProxy: 0.0,
                musicBedChange: 0.0,
                spectralChange: 0.0,
                lexicalDensityDelta: 1.0,
                returnMarker: 0.0
            ),
            endCueWeights: BoundaryCueWeights(
                pauseVAD: 0.0,
                speakerChangeProxy: 0.0,
                musicBedChange: 0.0,
                spectralChange: 0.0,
                lexicalDensityDelta: 0.0,
                returnMarker: 1.0
            ),
            lambda: 0.0,
            minBoundaryScore: 0.0,
            minImprovementOverOriginal: -1.0
        )

        let result = resolver.snap(
            candidateTime: 100.0,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: featureWindows,
            lexicalHits: lexicalHits,
            config: config
        )

        #expect(abs(result.score - (2.0 / 3.0)) < 0.0001)
    }

    @Test("transition markers drive the end-boundary return cue")
    func returnMarkerCue() {
        let featureWindows = [
            makeFeatureWindow(
                start: 100,
                end: 105,
                pauseProb: 0.0,
                speakerChangeProxyScore: 0.0,
                musicBedChangeScore: 0.0,
                spectralFlux: 0.0
            ),
        ]
        let lexicalHits = [
            makeLexicalHit(category: .transitionMarker, start: 101.0, end: 104.0),
        ]
        let config = BoundarySnappingConfig(
            startCueWeights: BoundaryCueWeights(
                pauseVAD: 0.0,
                speakerChangeProxy: 0.0,
                musicBedChange: 0.0,
                spectralChange: 0.0,
                lexicalDensityDelta: 0.0,
                returnMarker: 0.0
            ),
            endCueWeights: BoundaryCueWeights(
                pauseVAD: 0.0,
                speakerChangeProxy: 0.0,
                musicBedChange: 0.0,
                spectralChange: 0.0,
                lexicalDensityDelta: 0.0,
                returnMarker: 1.0
            ),
            lambda: 0.0,
            minBoundaryScore: 0.0,
            minImprovementOverOriginal: -1.0
        )

        let result = resolver.snap(
            candidateTime: 105.0,
            boundaryType: .end,
            anchorType: .transitionMarker,
            featureWindows: featureWindows,
            lexicalHits: lexicalHits,
            config: config
        )

        #expect(result.time == 105.0)
        #expect(abs(result.score - 1.0) < 0.0001)
    }

    // MARK: - Helpers

    private func makeFeatureWindow(
        start: Double,
        end: Double,
        pauseProb: Double,
        speakerChangeProxyScore: Double,
        musicBedChangeScore: Double,
        spectralFlux: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "test-asset",
            startTime: start,
            endTime: end,
            rms: 0.0,
            spectralFlux: spectralFlux,
            musicProbability: 0.0,
            speakerChangeProxyScore: speakerChangeProxyScore,
            musicBedChangeScore: musicBedChangeScore,
            pauseProbability: pauseProb,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        )
    }

    private func makeLexicalHit(
        category: LexicalPatternCategory,
        start: Double,
        end: Double
    ) -> LexicalHit {
        LexicalHit(
            category: category,
            matchedText: "hit",
            startTime: start,
            endTime: end,
            weight: 1.0
        )
    }
}
