import Foundation

enum BoundaryType: Sendable {
    case start
    case end
}

struct BoundarySnapDistance: Sendable, Equatable {
    let start: TimeInterval
    let end: TimeInterval

    func maxSnapDistance(for boundaryType: BoundaryType) -> TimeInterval {
        switch boundaryType {
        case .start:
            start
        case .end:
            end
        }
    }
}

struct StartBoundaryCueWeights: Sendable, Equatable {
    let pauseVAD: Double
    let speakerChangeProxy: Double
    let musicBedChange: Double
    let spectralChange: Double
    let lexicalDensityDelta: Double

    var totalWeight: Double {
        pauseVAD + speakerChangeProxy + musicBedChange + spectralChange + lexicalDensityDelta
    }
}

struct EndBoundaryCueWeights: Sendable, Equatable {
    let pauseVAD: Double
    let speakerChangeProxy: Double
    let musicBedChange: Double
    let spectralChange: Double
    let explicitReturnMarker: Double

    var totalWeight: Double {
        pauseVAD + speakerChangeProxy + musicBedChange + spectralChange + explicitReturnMarker
    }
}

struct BoundarySnappingConfig: Sendable, Equatable {
    let startWeights: StartBoundaryCueWeights
    let endWeights: EndBoundaryCueWeights
    let maxSnapDistanceByAnchorType: [AnchorType: BoundarySnapDistance]
    let lambda: Double
    let minBoundaryScore: Double
    let minImprovementOverOriginal: Double
    let lexicalDensityDeltaCap: Double
    let spectralBaselineFloor: Double

    init(
        startWeights: StartBoundaryCueWeights = .init(
            pauseVAD: 0.25,
            speakerChangeProxy: 0.20,
            musicBedChange: 0.15,
            spectralChange: 0.20,
            lexicalDensityDelta: 0.20
        ),
        endWeights: EndBoundaryCueWeights = .init(
            pauseVAD: 0.25,
            speakerChangeProxy: 0.20,
            musicBedChange: 0.15,
            spectralChange: 0.15,
            explicitReturnMarker: 0.25
        ),
        maxSnapDistanceByAnchorType: [AnchorType: BoundarySnapDistance] = Self.defaultMaxSnapDistanceByAnchorType,
        lambda: Double = 0.3,
        minBoundaryScore: Double = 0.3,
        minImprovementOverOriginal: Double = 0.1,
        lexicalDensityDeltaCap: Double = 3.0,
        spectralBaselineFloor: Double = 0.001
    ) {
        precondition(Self.isApproximatelyOne(startWeights.totalWeight), "Start-boundary cue weights must total 1.0")
        precondition(Self.isApproximatelyOne(endWeights.totalWeight), "End-boundary cue weights must total 1.0")

        self.startWeights = startWeights
        self.endWeights = endWeights
        self.maxSnapDistanceByAnchorType = maxSnapDistanceByAnchorType
        self.lambda = lambda
        self.minBoundaryScore = minBoundaryScore
        self.minImprovementOverOriginal = minImprovementOverOriginal
        self.lexicalDensityDeltaCap = lexicalDensityDeltaCap
        self.spectralBaselineFloor = spectralBaselineFloor
    }

    static let `default` = BoundarySnappingConfig()

    func maxSnapDistance(for anchorType: AnchorType, boundaryType: BoundaryType) -> TimeInterval {
        if let configured = maxSnapDistanceByAnchorType[anchorType] {
            return configured.maxSnapDistance(for: boundaryType)
        }

        let fallback: BoundarySnapDistance
        switch SpanHypothesisConfig.default.config(for: anchorType).polarity {
        case .startAnchored:
            fallback = BoundarySnapDistance(start: 5, end: 15)
        case .endAnchored:
            fallback = BoundarySnapDistance(start: 15, end: 5)
        case .neutral:
            fallback = BoundarySnapDistance(start: 10, end: 10)
        }
        return fallback.maxSnapDistance(for: boundaryType)
    }

    private static let defaultMaxSnapDistanceByAnchorType: [AnchorType: BoundarySnapDistance] = [
        .disclosure: BoundarySnapDistance(start: 5, end: 15),
        .sponsorLexicon: BoundarySnapDistance(start: 5, end: 15),
        .url: BoundarySnapDistance(start: 15, end: 5),
        .promoCode: BoundarySnapDistance(start: 15, end: 5),
        .fmPositive: BoundarySnapDistance(start: 10, end: 10),
        .transitionMarker: BoundarySnapDistance(start: 15, end: 5),
    ]

    private static func isApproximatelyOne(_ value: Double) -> Bool {
        abs(value - 1.0) <= 0.000_001
    }
}

struct ScoredBoundaryCandidate: Sendable, Equatable {
    let boundaryTime: Double
    let cueBlend: Double
    let normalizedDistance: Double
    let distancePenalty: Double
    let score: Double
    let lexicalDensityDelta: Double
    let explicitReturnMarker: Double
    let spectralChange: Double
    let windowStartTime: Double
    let windowEndTime: Double
}

struct TimeBoundaryResolver: Sendable {

    func snap(
        candidateTime: Double,
        boundaryType: BoundaryType,
        anchorType: AnchorType,
        featureWindows: [FeatureWindow],
        lexicalHits: [LexicalHit],
        config: BoundarySnappingConfig = .default
    ) -> Double {
        let scored = scoredCandidates(
            candidateTime: candidateTime,
            boundaryType: boundaryType,
            anchorType: anchorType,
            featureWindows: featureWindows,
            lexicalHits: lexicalHits,
            config: config
        )

        guard !scored.isEmpty else { return candidateTime }

        let originalCueBlend = referenceCueBlend(
            originalTime: candidateTime,
            boundaryType: boundaryType,
            featureWindows: featureWindows,
            lexicalHits: lexicalHits,
            maxSnapDistance: config.maxSnapDistance(for: anchorType, boundaryType: boundaryType),
            config: config
        )

        let localMaxima = localMaxima(in: scored)
        let qualifying = localMaxima.filter {
            $0.score >= config.minBoundaryScore &&
            ($0.score - originalCueBlend) >= config.minImprovementOverOriginal
        }

        guard let best = qualifying.min(by: { lhs, rhs in
            let lhsDistance = abs(lhs.boundaryTime - candidateTime)
            let rhsDistance = abs(rhs.boundaryTime - candidateTime)
            if lhsDistance != rhsDistance {
                return lhsDistance < rhsDistance
            }
            if lhs.score != rhs.score {
                return lhs.score > rhs.score
            }
            return lhs.boundaryTime < rhs.boundaryTime
        }) else {
            return candidateTime
        }

        return best.boundaryTime
    }

    func scoredCandidates(
        candidateTime: Double,
        boundaryType: BoundaryType,
        anchorType: AnchorType,
        featureWindows: [FeatureWindow],
        lexicalHits: [LexicalHit],
        config: BoundarySnappingConfig = .default
    ) -> [ScoredBoundaryCandidate] {
        let maxSnapDistance = config.maxSnapDistance(for: anchorType, boundaryType: boundaryType)
        guard maxSnapDistance > 0 else { return [] }

        let orderedWindows = featureWindows.sorted {
            boundaryTime(for: $0, boundaryType: boundaryType) < boundaryTime(for: $1, boundaryType: boundaryType)
        }
        let candidateWindows = orderedWindows.enumerated().filter { _, window in
            abs(boundaryTime(for: window, boundaryType: boundaryType) - candidateTime) <= maxSnapDistance
        }

        guard !candidateWindows.isEmpty else { return [] }

        let overlapCounts = lexicalOverlapCounts(for: orderedWindows, lexicalHits: lexicalHits)

        return candidateWindows.map { originalIndex, window in
            let boundaryTime = boundaryTime(for: window, boundaryType: boundaryType)
            let lexicalDensityDelta = boundaryType == .start
                ? lexicalDensityDelta(at: originalIndex, overlapCounts: overlapCounts, cap: config.lexicalDensityDeltaCap)
                : 0
            let explicitReturnMarker = boundaryType == .end
                ? explicitReturnMarker(for: window, lexicalHits: lexicalHits)
                : 0
            let spectralChange = spectralChange(
                for: window,
                boundaryType: boundaryType,
                candidateWindows: candidateWindows.map(\.element),
                radius: maxSnapDistance,
                baselineFloor: config.spectralBaselineFloor
            )
            let cueBlend = cueBlend(
                for: window,
                boundaryType: boundaryType,
                lexicalDensityDelta: lexicalDensityDelta,
                explicitReturnMarker: explicitReturnMarker,
                spectralChange: spectralChange,
                config: config
            )
            let normalizedDistance = min(1.0, abs(boundaryTime - candidateTime) / maxSnapDistance)
            let distancePenalty = config.lambda * normalizedDistance

            return ScoredBoundaryCandidate(
                boundaryTime: boundaryTime,
                cueBlend: cueBlend,
                normalizedDistance: normalizedDistance,
                distancePenalty: distancePenalty,
                score: cueBlend - distancePenalty,
                lexicalDensityDelta: lexicalDensityDelta,
                explicitReturnMarker: explicitReturnMarker,
                spectralChange: spectralChange,
                windowStartTime: window.startTime,
                windowEndTime: window.endTime
            )
        }
    }

    private func referenceCueBlend(
        originalTime: Double,
        boundaryType: BoundaryType,
        featureWindows: [FeatureWindow],
        lexicalHits: [LexicalHit],
        maxSnapDistance: Double,
        config: BoundarySnappingConfig
    ) -> Double {
        let orderedWindows = featureWindows.sorted {
            boundaryTime(for: $0, boundaryType: boundaryType) < boundaryTime(for: $1, boundaryType: boundaryType)
        }
        let nearbyWindows = orderedWindows.filter {
            abs(boundaryTime(for: $0, boundaryType: boundaryType) - originalTime) <= maxSnapDistance
        }
        let overlapCounts = lexicalOverlapCounts(for: orderedWindows, lexicalHits: lexicalHits)
        guard let reference = orderedWindows.enumerated().first(where: { _, window in
            window.contains(originalTime)
        }) else {
            return 0
        }

        let index = reference.offset
        let window = reference.element
        let lexicalDensityDelta = boundaryType == .start
            ? lexicalDensityDelta(at: index, overlapCounts: overlapCounts, cap: config.lexicalDensityDeltaCap)
            : 0
        let explicitReturnMarker = boundaryType == .end
            ? explicitReturnMarker(for: window, lexicalHits: lexicalHits)
            : 0
        let spectralChange = spectralChange(
            for: window,
            boundaryType: boundaryType,
            candidateWindows: nearbyWindows,
            radius: maxSnapDistance,
            baselineFloor: config.spectralBaselineFloor
        )

        return cueBlend(
            for: window,
            boundaryType: boundaryType,
            lexicalDensityDelta: lexicalDensityDelta,
            explicitReturnMarker: explicitReturnMarker,
            spectralChange: spectralChange,
            config: config
        )
    }

    private func localMaxima(in candidates: [ScoredBoundaryCandidate]) -> [ScoredBoundaryCandidate] {
        guard !candidates.isEmpty else { return [] }

        return candidates.enumerated().compactMap { index, candidate in
            let previousScore = index > 0 ? candidates[index - 1].score : -.infinity
            let nextScore = index + 1 < candidates.count ? candidates[index + 1].score : -.infinity
            if candidate.score >= previousScore && candidate.score >= nextScore {
                return candidate
            }
            return nil
        }
    }

    private func cueBlend(
        for window: FeatureWindow,
        boundaryType: BoundaryType,
        lexicalDensityDelta: Double,
        explicitReturnMarker: Double,
        spectralChange: Double,
        config: BoundarySnappingConfig
    ) -> Double {
        // Use directional onset/offset scores when available (non-zero),
        // falling back to the legacy musicBedChangeScore for backward
        // compatibility with windows that predate the directional fields.
        let musicCue: Double
        switch boundaryType {
        case .start:
            musicCue = window.musicBedOnsetScore > 0
                ? clamp01(window.musicBedOnsetScore)
                : clamp01(window.musicBedChangeScore)
            return clamp01(window.pauseProbability) * config.startWeights.pauseVAD +
                clamp01(window.speakerChangeProxyScore) * config.startWeights.speakerChangeProxy +
                musicCue * config.startWeights.musicBedChange +
                spectralChange * config.startWeights.spectralChange +
                lexicalDensityDelta * config.startWeights.lexicalDensityDelta
        case .end:
            musicCue = window.musicBedOffsetScore > 0
                ? clamp01(window.musicBedOffsetScore)
                : clamp01(window.musicBedChangeScore)
            return clamp01(window.pauseProbability) * config.endWeights.pauseVAD +
                clamp01(window.speakerChangeProxyScore) * config.endWeights.speakerChangeProxy +
                musicCue * config.endWeights.musicBedChange +
                spectralChange * config.endWeights.spectralChange +
                explicitReturnMarker * config.endWeights.explicitReturnMarker
        }
    }

    private func lexicalOverlapCounts(
        for windows: [FeatureWindow],
        lexicalHits: [LexicalHit]
    ) -> [Int] {
        windows.map { window in
            lexicalHits.reduce(into: 0) { count, hit in
                guard Self.adLexicalCategories.contains(hit.category) else { return }
                if overlaps(window, with: hit) {
                    count += 1
                }
            }
        }
    }

    private func lexicalDensityDelta(
        at index: Int,
        overlapCounts: [Int],
        cap: Double
    ) -> Double {
        let previousCount = index > 0 ? overlapCounts[index - 1] : 0
        let delta = abs(Double(overlapCounts[index] - previousCount))
        guard cap > 0 else { return 0 }
        return clamp01(delta / cap)
    }

    private func explicitReturnMarker(
        for window: FeatureWindow,
        lexicalHits: [LexicalHit]
    ) -> Double {
        lexicalHits.contains {
            $0.category == .transitionMarker && overlaps(window, with: $0)
        } ? 1.0 : 0.0
    }

    private func spectralChange(
        for window: FeatureWindow,
        boundaryType: BoundaryType,
        candidateWindows: [FeatureWindow],
        radius: Double,
        baselineFloor: Double
    ) -> Double {
        let referenceTime = boundaryTime(for: window, boundaryType: boundaryType)
        let neighborhood = candidateWindows.filter {
            abs(boundaryTime(for: $0, boundaryType: boundaryType) - referenceTime) <= radius
        }
        let localMean = neighborhood.map(\.spectralFlux).mean
        let baseline = max(localMean, baselineFloor)
        guard baseline > 0 else { return 0 }
        return clamp01(window.spectralFlux / baseline)
    }

    private func boundaryTime(
        for window: FeatureWindow,
        boundaryType: BoundaryType
    ) -> Double {
        switch boundaryType {
        case .start:
            window.startTime
        case .end:
            window.endTime
        }
    }

    private func overlaps(
        _ window: FeatureWindow,
        with hit: LexicalHit
    ) -> Bool {
        hit.startTime < window.endTime && hit.endTime > window.startTime
    }

    private func clamp01(_ value: Double) -> Double {
        min(max(value, 0), 1)
    }

    private static let adLexicalCategories: Set<LexicalPatternCategory> = [
        .sponsor,
        .promoCode,
        .urlCTA,
        .purchaseLanguage,
    ]
}

private extension FeatureWindow {
    func contains(_ time: Double) -> Bool {
        startTime <= time && time <= endTime
    }
}

private extension Sequence where Element == Double {
    var mean: Double {
        var total = 0.0
        var count = 0.0
        for value in self {
            total += value
            count += 1
        }
        guard count > 0 else { return 0 }
        return total / count
    }
}
