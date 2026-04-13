import Foundation
import Testing

@testable import Playhead

@Suite("TimeBoundaryResolver")
struct TimeBoundaryResolverTests {

    private let resolver = TimeBoundaryResolver()

    @Test("score formula applies cue blend and distance penalty")
    func scoreFormulaAndDistancePenalty() throws {
        let windows = [
            makeWindow(start: 99, end: 101, pause: 0.05, speakerChange: 0.05, musicBedChange: 0.05, spectralFlux: 0.05),
            makeWindow(start: 102, end: 104, pause: 0.44, speakerChange: 0.45, musicBedChange: 0.40, spectralFlux: 0.20),
            makeWindow(start: 109, end: 111, pause: 0.56, speakerChange: 0.60, musicBedChange: 0.60, spectralFlux: 0.30),
        ]

        let zeroPenalty = BoundarySnappingConfig(lambda: 0)
        let scoredWithoutPenalty = resolver.scoredCandidates(
            candidateTime: 100,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: [],
            config: zeroPenalty
        )

        let nearNoPenalty = try #require(scoredWithoutPenalty.first(where: { $0.boundaryTime == 102 }))
        let farNoPenalty = try #require(scoredWithoutPenalty.first(where: { $0.boundaryTime == 109 }))
        expectApproximately(nearNoPenalty.cueBlend, 0.46)
        expectApproximately(farNoPenalty.cueBlend, 0.55)
        expectApproximately(nearNoPenalty.distancePenalty, 0)
        expectApproximately(farNoPenalty.distancePenalty, 0)
        #expect(farNoPenalty.score > nearNoPenalty.score)
        #expect(
            resolver.snap(
                candidateTime: 100,
                boundaryType: .start,
                anchorType: .fmPositive,
                featureWindows: windows,
                lexicalHits: [],
                config: zeroPenalty
            ) == 109
        )

        let scoredWithPenalty = resolver.scoredCandidates(
            candidateTime: 100,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: [],
            config: .default
        )
        let nearWithPenalty = try #require(scoredWithPenalty.first(where: { $0.boundaryTime == 102 }))
        let farWithPenalty = try #require(scoredWithPenalty.first(where: { $0.boundaryTime == 109 }))
        expectApproximately(nearWithPenalty.normalizedDistance, 0.2)
        expectApproximately(nearWithPenalty.distancePenalty, 0.06)
        expectApproximately(farWithPenalty.normalizedDistance, 0.9)
        expectApproximately(farWithPenalty.distancePenalty, 0.27)
        #expect(nearWithPenalty.score > farWithPenalty.score)
        #expect(
            resolver.snap(
                candidateTime: 100,
                boundaryType: .start,
                anchorType: .fmPositive,
                featureWindows: windows,
                lexicalHits: [],
                config: .default
            ) == 102
        )
    }

    @Test("default snap radii are anchor-type and boundary-type aware")
    func asymmetricSnapRadii() {
        struct Scenario {
            let anchorType: AnchorType
            let boundaryType: BoundaryType
            let within: Double
            let beyond: Double
        }

        let scenarios = [
            Scenario(anchorType: .disclosure, boundaryType: .start, within: 104.8, beyond: 105.2),
            Scenario(anchorType: .disclosure, boundaryType: .end, within: 114.8, beyond: 115.2),
            Scenario(anchorType: .url, boundaryType: .start, within: 114.8, beyond: 115.2),
            Scenario(anchorType: .url, boundaryType: .end, within: 104.8, beyond: 105.2),
            Scenario(anchorType: .fmPositive, boundaryType: .start, within: 109.8, beyond: 110.2),
        ]

        for scenario in scenarios {
            let windows = [
                makeWindow(start: 99, end: 101, pause: 0.05, speakerChange: 0.05, musicBedChange: 0.05, spectralFlux: 0.05),
                makeBoundaryWindow(boundaryTime: scenario.within, boundaryType: scenario.boundaryType),
                makeBoundaryWindow(boundaryTime: scenario.beyond, boundaryType: scenario.boundaryType),
            ]
            let scored = resolver.scoredCandidates(
                candidateTime: 100,
                boundaryType: scenario.boundaryType,
                anchorType: scenario.anchorType,
                featureWindows: windows,
                lexicalHits: []
            )

            #expect(scored.contains(where: { abs($0.boundaryTime - scenario.within) < 0.000_001 }))
            #expect(!scored.contains(where: { abs($0.boundaryTime - scenario.beyond) < 0.000_001 }))
            #expect(
                abs(
                    resolver.snap(
                        candidateTime: 100,
                        boundaryType: scenario.boundaryType,
                        anchorType: scenario.anchorType,
                        featureWindows: windows,
                        lexicalHits: []
                    ) - scenario.within
                ) < 0.000_001
            )
        }
    }

    @Test("start snapping uses lexical density deltas from overlapping hits")
    func lexicalDensityDeltaCue() throws {
        let windows = [
            makeWindow(start: 98, end: 99, pause: 0.15, speakerChange: 0.10, musicBedChange: 0.05, spectralFlux: 0.08),
            makeWindow(start: 100, end: 101, pause: 0.15, speakerChange: 0.10, musicBedChange: 0.05, spectralFlux: 0.08),
            makeWindow(start: 102, end: 103, pause: 0.15, speakerChange: 0.10, musicBedChange: 0.05, spectralFlux: 0.08),
        ]
        let lexicalHits = [
            makeLexicalHit(category: .sponsor, start: 100.1, end: 102.1),
            makeLexicalHit(category: .urlCTA, start: 100.2, end: 102.2),
            makeLexicalHit(category: .purchaseLanguage, start: 100.3, end: 102.3),
        ]

        let scored = resolver.scoredCandidates(
            candidateTime: 98.5,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: lexicalHits
        )

        let firstSharpIncrease = try #require(scored.first(where: { $0.boundaryTime == 100 }))
        let laterFlatWindow = try #require(scored.first(where: { $0.boundaryTime == 102 }))
        expectApproximately(firstSharpIncrease.lexicalDensityDelta, 1)
        expectApproximately(laterFlatWindow.lexicalDensityDelta, 0)
        #expect(
            resolver.snap(
                candidateTime: 98.5,
                boundaryType: .start,
                anchorType: .fmPositive,
                featureWindows: windows,
                lexicalHits: lexicalHits
            ) == 100
        )
    }

    @Test("end snapping uses explicit return markers from lexical hits")
    func returnMarkerCue() throws {
        let windows = [
            makeBoundaryWindow(boundaryTime: 152, boundaryType: .end),
            makeBoundaryWindow(boundaryTime: 154, boundaryType: .end),
        ]
        let lexicalHits = [
            makeLexicalHit(category: .transitionMarker, start: 153.1, end: 153.9, text: "back to the show"),
        ]

        let scored = resolver.scoredCandidates(
            candidateTime: 151.5,
            boundaryType: .end,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: lexicalHits
        )

        let withoutMarker = try #require(scored.first(where: { $0.boundaryTime == 152 }))
        let withMarker = try #require(scored.first(where: { $0.boundaryTime == 154 }))
        expectApproximately(withoutMarker.explicitReturnMarker, 0)
        expectApproximately(withMarker.explicitReturnMarker, 1)
        #expect(
            resolver.snap(
                candidateTime: 151.5,
                boundaryType: .end,
                anchorType: .fmPositive,
                featureWindows: windows,
                lexicalHits: lexicalHits
            ) == 154
        )
    }

    @Test("spectral flux is normalized against each candidate's local baseline")
    func localSpectralBaselineNormalization() throws {
        let windows = [
            makeWindow(start: 94, end: 95, pause: 0.0, speakerChange: 0.0, musicBedChange: 0.0, spectralFlux: 0.05),
            makeWindow(start: 96, end: 97, pause: 0.35, speakerChange: 0.2, musicBedChange: 0.1, spectralFlux: 0.30),
            makeWindow(start: 98, end: 99, pause: 0.0, speakerChange: 0.0, musicBedChange: 0.0, spectralFlux: 0.05),
            makeWindow(start: 102, end: 103, pause: 0.0, speakerChange: 0.0, musicBedChange: 0.0, spectralFlux: 0.70),
            makeWindow(start: 104, end: 105, pause: 0.35, speakerChange: 0.2, musicBedChange: 0.1, spectralFlux: 0.50),
            makeWindow(start: 106, end: 107, pause: 0.0, speakerChange: 0.0, musicBedChange: 0.0, spectralFlux: 0.70),
        ]
        let config = BoundarySnappingConfig(lambda: 0.05, minBoundaryScore: 0.2)

        let scored = resolver.scoredCandidates(
            candidateTime: 100,
            boundaryType: .start,
            anchorType: .disclosure,
            featureWindows: windows,
            lexicalHits: [],
            config: config
        )

        let lowBaselineCandidate = try #require(scored.first(where: { $0.boundaryTime == 96 }))
        let highBaselineCandidate = try #require(scored.first(where: { $0.boundaryTime == 104 }))
        #expect(lowBaselineCandidate.spectralChange > highBaselineCandidate.spectralChange)
        #expect(lowBaselineCandidate.score > highBaselineCandidate.score)
        #expect(
            resolver.snap(
                candidateTime: 100,
                boundaryType: .start,
                anchorType: .disclosure,
                featureWindows: windows,
                lexicalHits: [],
                config: config
            ) == 96
        )
    }

    @Test("snap falls back to original time when thresholds are not met")
    func fallbackToOriginal() {
        struct Scenario {
            let candidateTime: Double
            let config: BoundarySnappingConfig
            let windows: [FeatureWindow]
        }

        let scenarios = [
            Scenario(
                candidateTime: 100,
                config: .default,
                windows: [
                    makeWindow(start: 99, end: 101, pause: 0.05, speakerChange: 0.05, musicBedChange: 0.05, spectralFlux: 0.05),
                    makeWindow(start: 102, end: 103, pause: 0.12, speakerChange: 0.10, musicBedChange: 0.08, spectralFlux: 0.05),
                ]
            ),
            Scenario(
                candidateTime: 100,
                config: BoundarySnappingConfig(lambda: 0),
                windows: [
                    makeWindow(start: 99, end: 101, pause: 0.60, speakerChange: 0.50, musicBedChange: 0.40, spectralFlux: 0.30),
                    makeWindow(start: 102, end: 103, pause: 0.66, speakerChange: 0.55, musicBedChange: 0.45, spectralFlux: 0.30),
                ]
            ),
        ]

        for scenario in scenarios {
            #expect(
                resolver.snap(
                    candidateTime: scenario.candidateTime,
                    boundaryType: .start,
                    anchorType: .fmPositive,
                    featureWindows: scenario.windows,
                    lexicalHits: [],
                    config: scenario.config
                ) == scenario.candidateTime
            )
        }
    }

    private func makeWindow(
        start: Double,
        end: Double,
        pause: Double,
        speakerChange: Double,
        musicBedChange: Double,
        spectralFlux: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-348",
            startTime: start,
            endTime: end,
            rms: 0.1,
            spectralFlux: spectralFlux,
            musicProbability: 0,
            speakerChangeProxyScore: speakerChange,
            musicBedChangeScore: musicBedChange,
            pauseProbability: pause,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 4
        )
    }

    private func makeBoundaryWindow(
        boundaryTime: Double,
        boundaryType: BoundaryType
    ) -> FeatureWindow {
        switch boundaryType {
        case .start:
            makeWindow(
                start: boundaryTime,
                end: boundaryTime + 1,
                pause: 0.8,
                speakerChange: 0.8,
                musicBedChange: 0.8,
                spectralFlux: 0.8
            )
        case .end:
            makeWindow(
                start: boundaryTime - 1,
                end: boundaryTime,
                pause: 0.8,
                speakerChange: 0.8,
                musicBedChange: 0.8,
                spectralFlux: 0.8
            )
        }
    }

    private func makeLexicalHit(
        category: LexicalPatternCategory,
        start: Double,
        end: Double,
        text: String = "ad cue"
    ) -> LexicalHit {
        LexicalHit(
            category: category,
            matchedText: text,
            startTime: start,
            endTime: end,
            weight: 1
        )
    }

    @Test("snap returns original time when feature windows are empty")
    func snapWithEmptyFeatureWindowsReturnsOriginal() {
        let result = resolver.snap(
            candidateTime: 100,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: [],
            lexicalHits: []
        )
        expectApproximately(result, 100)
    }

    @Test("snap returns original time when single window scores below threshold")
    func snapWithSingleLowScoreWindowReturnsOriginal() {
        let windows = [
            makeWindow(start: 105, end: 107, pause: 0.01, speakerChange: 0.01, musicBedChange: 0.01, spectralFlux: 0.01),
        ]
        let result = resolver.snap(
            candidateTime: 100,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: []
        )
        expectApproximately(result, 100)
    }

    private func expectApproximately(
        _ actual: Double,
        _ expected: Double,
        tolerance: Double = 0.000_001
    ) {
        #expect(abs(actual - expected) <= tolerance, "expected \(expected), got \(actual)")
    }
}
