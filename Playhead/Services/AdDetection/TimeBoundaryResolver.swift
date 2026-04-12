// TimeBoundaryResolver.swift
// Seconds-based boundary snapping across acoustic and lexical cues.

import Foundation

enum BoundaryType: Sendable {
    case start
    case end
}

struct BoundaryCueWeights: Sendable, Equatable {
    let pauseVAD: Double
    let speakerChangeProxy: Double
    let musicBedChange: Double
    let spectralChange: Double
    let lexicalDensityDelta: Double
    let returnMarker: Double

    static let defaultStart = BoundaryCueWeights(
        pauseVAD: 0.25,
        speakerChangeProxy: 0.20,
        musicBedChange: 0.15,
        spectralChange: 0.20,
        lexicalDensityDelta: 0.20,
        returnMarker: 0.0
    )

    static let defaultEnd = BoundaryCueWeights(
        pauseVAD: 0.25,
        speakerChangeProxy: 0.20,
        musicBedChange: 0.15,
        spectralChange: 0.15,
        lexicalDensityDelta: 0.0,
        returnMarker: 0.25
    )

    var totalWeight: Double {
        pauseVAD + speakerChangeProxy + musicBedChange + spectralChange + lexicalDensityDelta + returnMarker
    }
}

struct BoundarySnapDistance: Sendable, Equatable {
    let start: TimeInterval
    let end: TimeInterval
}

struct BoundarySnappingConfig: Sendable {
    let startCueWeights: BoundaryCueWeights
    let endCueWeights: BoundaryCueWeights
    let lambda: Double
    let minBoundaryScore: Double
    let minImprovementOverOriginal: Double
    let lexicalDensityNormalizationCap: Double
    let maxSnapDistanceByAnchorType: [AnchorType: BoundarySnapDistance]

    static let defaultMaxSnapDistanceByAnchorType: [AnchorType: BoundarySnapDistance] = [
        .disclosure: BoundarySnapDistance(start: 5.0, end: 15.0),
        .sponsorLexicon: BoundarySnapDistance(start: 5.0, end: 15.0),
        .url: BoundarySnapDistance(start: 15.0, end: 5.0),
        .promoCode: BoundarySnapDistance(start: 15.0, end: 5.0),
        .fmPositive: BoundarySnapDistance(start: 10.0, end: 10.0),
        .transitionMarker: BoundarySnapDistance(start: 15.0, end: 5.0),
    ]

    init(
        startCueWeights: BoundaryCueWeights = .defaultStart,
        endCueWeights: BoundaryCueWeights = .defaultEnd,
        lambda: Double = 0.3,
        minBoundaryScore: Double = 0.3,
        minImprovementOverOriginal: Double = 0.1,
        lexicalDensityNormalizationCap: Double = 3.0,
        maxSnapDistanceByAnchorType: [AnchorType: BoundarySnapDistance] = Self.defaultMaxSnapDistanceByAnchorType
    ) {
        self.startCueWeights = startCueWeights
        self.endCueWeights = endCueWeights
        self.lambda = lambda
        self.minBoundaryScore = minBoundaryScore
        self.minImprovementOverOriginal = minImprovementOverOriginal
        self.lexicalDensityNormalizationCap = lexicalDensityNormalizationCap
        self.maxSnapDistanceByAnchorType = maxSnapDistanceByAnchorType
    }

    static let `default` = BoundarySnappingConfig()

    func maxSnapDistance(for anchorType: AnchorType, boundaryType: BoundaryType) -> TimeInterval {
        guard let distance = maxSnapDistanceByAnchorType[anchorType] else {
            preconditionFailure("Missing snap-distance configuration for \(anchorType)")
        }
        switch boundaryType {
        case .start:
            return distance.start
        case .end:
            return distance.end
        }
    }
}

struct TimeBoundarySnapResult: Sendable, Equatable {
    let originalTime: Double
    let time: Double
    let score: Double
    let maxSnapDistance: TimeInterval
    let didSnap: Bool
}

struct TimeBoundaryResolver: Sendable {

    func snap(
        candidateTime: Double,
        boundaryType: BoundaryType,
        anchorType: AnchorType,
        featureWindows: [FeatureWindow],
        lexicalHits: [LexicalHit],
        config: BoundarySnappingConfig = .default
    ) -> TimeBoundarySnapResult {
        let maxSnapDistance = config.maxSnapDistance(for: anchorType, boundaryType: boundaryType)
        guard maxSnapDistance > 0 else {
            return TimeBoundarySnapResult(
                originalTime: candidateTime,
                time: candidateTime,
                score: 0.0,
                maxSnapDistance: maxSnapDistance,
                didSnap: false
            )
        }

        let orderedWindows = featureWindows.sorted {
            if $0.startTime == $1.startTime {
                return $0.endTime < $1.endTime
            }
            return $0.startTime < $1.startTime
        }

        let nearbyWindows = orderedWindows.filter { window in
            let boundaryTime = boundaryTime(for: window, boundaryType: boundaryType)
            return abs(boundaryTime - candidateTime) <= maxSnapDistance
        }

        guard !nearbyWindows.isEmpty else {
            return TimeBoundarySnapResult(
                originalTime: candidateTime,
                time: candidateTime,
                score: 0.0,
                maxSnapDistance: maxSnapDistance,
                didSnap: false
            )
        }

        let localSpectralMean = nearbyWindows.map(\.spectralFlux).reduce(0.0, +) / Double(nearbyWindows.count)

        let lexicalCountsByWindow = lexicalCountsByWindow(orderedWindows, lexicalHits: lexicalHits)

        var bestCandidate: CandidateScore?
        for window in nearbyWindows {
            let boundaryTime = boundaryTime(for: window, boundaryType: boundaryType)
            let cueBlend = cueBlend(
                for: window,
                boundaryType: boundaryType,
                lexicalHits: lexicalHits,
                lexicalCountsByWindow: lexicalCountsByWindow,
                localSpectralMean: localSpectralMean,
                config: config
            )
            let normalizedDistance = abs(boundaryTime - candidateTime) / maxSnapDistance
            let score = cueBlend - config.lambda * normalizedDistance

            let candidate = CandidateScore(
                boundaryTime: boundaryTime,
                score: score,
                cueBlend: cueBlend,
                distance: abs(boundaryTime - candidateTime)
            )
            if let currentBest = bestCandidate {
                if candidate.isBetter(than: currentBest) {
                    bestCandidate = candidate
                }
            } else {
                bestCandidate = candidate
            }
        }

        guard let bestCandidate else {
            return TimeBoundarySnapResult(
                originalTime: candidateTime,
                time: candidateTime,
                score: 0.0,
                maxSnapDistance: maxSnapDistance,
                didSnap: false
            )
        }

        let originalScore = scoreAtOriginalTime(
            candidateTime: candidateTime,
            boundaryType: boundaryType,
            orderedWindows: orderedWindows,
            lexicalHits: lexicalHits,
            lexicalCountsByWindow: lexicalCountsByWindow,
            localSpectralMean: localSpectralMean,
            config: config
        )

        guard bestCandidate.score >= config.minBoundaryScore else {
            return TimeBoundarySnapResult(
                originalTime: candidateTime,
                time: candidateTime,
                score: bestCandidate.score,
                maxSnapDistance: maxSnapDistance,
                didSnap: false
            )
        }

        guard bestCandidate.score - originalScore >= config.minImprovementOverOriginal else {
            return TimeBoundarySnapResult(
                originalTime: candidateTime,
                time: candidateTime,
                score: bestCandidate.score,
                maxSnapDistance: maxSnapDistance,
                didSnap: false
            )
        }

        return TimeBoundarySnapResult(
            originalTime: candidateTime,
            time: bestCandidate.boundaryTime,
            score: bestCandidate.score,
            maxSnapDistance: maxSnapDistance,
            didSnap: abs(bestCandidate.boundaryTime - candidateTime) > 0.000001
        )
    }

    private struct CandidateScore {
        let boundaryTime: Double
        let score: Double
        let cueBlend: Double
        let distance: Double

        func isBetter(than other: CandidateScore) -> Bool {
            if score == other.score {
                return distance < other.distance
            }
            return score > other.score
        }
    }

    private func boundaryTime(for window: FeatureWindow, boundaryType: BoundaryType) -> Double {
        switch boundaryType {
        case .start:
            return window.startTime
        case .end:
            return window.endTime
        }
    }

    private func cueBlend(
        for window: FeatureWindow,
        boundaryType: BoundaryType,
        lexicalHits: [LexicalHit],
        lexicalCountsByWindow: [Double: WindowLexicalCounts],
        localSpectralMean: Double,
        config: BoundarySnappingConfig
    ) -> Double {
        let weights: BoundaryCueWeights
        switch boundaryType {
        case .start:
            weights = config.startCueWeights
        case .end:
            weights = config.endCueWeights
        }

        let spectralScore: Double
        if localSpectralMean > 0 {
            spectralScore = min(1.0, max(0.0, window.spectralFlux / localSpectralMean))
        } else {
            spectralScore = 0.0
        }

        let pauseScore = clamp01(window.pauseProbability)
        let speakerScore = clamp01(window.speakerChangeProxyScore)
        let musicScore = clamp01(window.musicBedChangeScore)
        let lexicalDensityScore: Double
        let returnMarkerScore: Double

        switch boundaryType {
        case .start:
            let counts = lexicalCountsByWindow[window.startTime] ?? WindowLexicalCounts(current: 0, previous: 0)
            let delta = abs(counts.current - counts.previous)
            lexicalDensityScore = min(1.0, Double(delta) / config.lexicalDensityNormalizationCap)
            returnMarkerScore = 0.0
        case .end:
            lexicalDensityScore = 0.0
            returnMarkerScore = lexicalHits.contains { hit in
                hit.category == .transitionMarker && hit.overlaps(window)
            } ? 1.0 : 0.0
        }

        return pauseScore * weights.pauseVAD
            + speakerScore * weights.speakerChangeProxy
            + musicScore * weights.musicBedChange
            + spectralScore * weights.spectralChange
            + lexicalDensityScore * weights.lexicalDensityDelta
            + returnMarkerScore * weights.returnMarker
    }

    private func scoreAtOriginalTime(
        candidateTime: Double,
        boundaryType: BoundaryType,
        orderedWindows: [FeatureWindow],
        lexicalHits: [LexicalHit],
        lexicalCountsByWindow: [Double: WindowLexicalCounts],
        localSpectralMean: Double,
        config: BoundarySnappingConfig
    ) -> Double {
        guard let window = orderedWindows.first(where: { $0.contains(candidateTime) }) else {
            return 0.0
        }
        return cueBlend(
            for: window,
            boundaryType: boundaryType,
            lexicalHits: lexicalHits,
            lexicalCountsByWindow: lexicalCountsByWindow,
            localSpectralMean: localSpectralMean,
            config: config
        )
    }

    private struct WindowLexicalCounts {
        let current: Int
        let previous: Int
    }

    private func lexicalCountsByWindow(
        _ orderedWindows: [FeatureWindow],
        lexicalHits: [LexicalHit]
    ) -> [Double: WindowLexicalCounts] {
        guard !orderedWindows.isEmpty else { return [:] }

        let adHits = lexicalHits.filter { $0.isAdDensityCategory }
        var counts: [Double: WindowLexicalCounts] = [:]
        var previousCount = 0

        for window in orderedWindows {
            let currentCount = adHits.filter { $0.overlaps(window) }.count
            counts[window.startTime] = WindowLexicalCounts(current: currentCount, previous: previousCount)
            previousCount = currentCount
        }

        return counts
    }

    private func clamp01(_ value: Double) -> Double {
        min(1.0, max(0.0, value))
    }
}

private extension LexicalHit {
    var isAdDensityCategory: Bool {
        category != .transitionMarker
    }

    func overlaps(_ window: FeatureWindow) -> Bool {
        startTime < window.endTime && endTime > window.startTime
    }
}

private extension FeatureWindow {
    func contains(_ time: Double) -> Bool {
        time >= startTime && time <= endTime
    }
}
