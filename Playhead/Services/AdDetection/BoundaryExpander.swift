// BoundaryExpander.swift
// Stateless utility that expands a single user-tap seed time into full ad
// start/end boundaries by fusing acoustic, lexical, and existing-window signals.
//
// Used when the user taps "Hearing an ad" at a single playback position. Instead
// of logging a whole-episode correction (scope 0...Int.max), this finds the ad's
// actual start and end boundaries.

import Foundation
import OSLog

// MARK: - Output types

/// Source of boundary evidence, ordered by descending priority.
enum BoundarySource: String, Sendable {
    case existingWindow
    case acousticAndLexical
    case acousticOnly
    case fallback
}

/// Expanded ad boundary produced from a single seed tap position.
struct ExpandedBoundary: Sendable, Equatable {
    let startTime: Double
    let endTime: Double
    let boundaryConfidence: Double // 0.0–1.0
    let source: BoundarySource
}

// MARK: - BoundaryExpander

/// Stateless utility that expands a seed time into ad boundaries using three
/// signal layers: existing AdWindows (highest priority), acoustic features
/// resolved through TimeBoundaryResolver, and lexical patterns (LexicalScanner).
struct BoundaryExpander: Sendable {

    struct ExpansionConfig: Sendable, Equatable {
        let acousticBackwardSearchRadius: Double
        let acousticForwardSearchRadius: Double
        let lexicalBackwardSearchRadius: Double
        let lexicalForwardSearchRadius: Double

        static let neutral = ExpansionConfig(
            acousticBackwardSearchRadius: 60.0,
            acousticForwardSearchRadius: 60.0,
            lexicalBackwardSearchRadius: 90.0,
            lexicalForwardSearchRadius: 90.0
        )

        static let startAnchored = ExpansionConfig(
            acousticBackwardSearchRadius: 20.0,
            acousticForwardSearchRadius: 90.0,
            lexicalBackwardSearchRadius: 30.0,
            lexicalForwardSearchRadius: 120.0
        )

        static let endAnchored = ExpansionConfig(
            acousticBackwardSearchRadius: 90.0,
            acousticForwardSearchRadius: 20.0,
            lexicalBackwardSearchRadius: 120.0,
            lexicalForwardSearchRadius: 30.0
        )

        var fallbackBackwardWidth: Double {
            min(30.0, acousticBackwardSearchRadius)
        }

        var fallbackForwardWidth: Double {
            min(30.0, acousticForwardSearchRadius)
        }

        static func forPolarity(_ polarity: AnchorPolarity) -> ExpansionConfig {
            switch polarity {
            case .startAnchored:
                return .startAnchored
            case .endAnchored:
                return .endAnchored
            case .neutral:
                return .neutral
            }
        }
    }

    private let logger = Logger(subsystem: "com.playhead", category: "BoundaryExpander")
    private let boundaryResolver = TimeBoundaryResolver()

    private struct LexicalContext: Sendable {
        let candidates: [LexicalCandidate]
        let hits: [LexicalHit]
    }

    // MARK: - Public API

    /// Expand a seed time into ad boundaries.
    ///
    /// - Parameters:
    ///   - seed: The playback time where the user tapped "Hearing an ad".
    ///   - featureWindows: Available acoustic feature windows for this episode.
    ///   - transcriptChunks: Available transcript chunks for this episode.
    ///   - adWindows: Existing detected ad windows for this episode.
    /// - Returns: Expanded boundary with confidence and source attribution.
    func expand(
        seed: Double,
        featureWindows: [FeatureWindow],
        transcriptChunks: [TranscriptChunk],
        adWindows: [AdWindow],
        config: ExpansionConfig? = nil,
        anchorType: AnchorType = .fmPositive
    ) -> ExpandedBoundary {
        let expansionConfig = config ?? .neutral

        // Signal 3 (highest priority): Check existing AdWindows.
        if let windowBoundary = expandFromExistingWindows(seed: seed, adWindows: adWindows) {
            logger.info("Boundary expansion from existing window: \(windowBoundary.startTime, format: .fixed(precision: 1))–\(windowBoundary.endTime, format: .fixed(precision: 1))")
            return windowBoundary
        }

        let lexicalContext = makeLexicalContext(
            seed: seed,
            transcriptChunks: transcriptChunks,
            config: expansionConfig
        )
        let lexicalHits = lexicalContext?.hits ?? []

        // Signal 1: Acoustic boundaries from FeatureWindows.
        let acousticStart = findAcousticBoundary(
            seed: seed,
            direction: .backward,
            featureWindows: featureWindows,
            searchRadius: expansionConfig.acousticBackwardSearchRadius,
            lexicalHits: lexicalHits,
            anchorType: anchorType
        )
        let acousticEnd = findAcousticBoundary(
            seed: seed,
            direction: .forward,
            featureWindows: featureWindows,
            searchRadius: expansionConfig.acousticForwardSearchRadius,
            lexicalHits: lexicalHits,
            anchorType: anchorType
        )

        // Signal 2: Narrow using lexical patterns if available.
        let lexicalBoundary = lexicalContext.flatMap { context in
            findLexicalBoundaries(
                seed: seed,
                candidates: context.candidates,
                config: expansionConfig
            )
        }

        if let lexical = lexicalBoundary {
            // Combine: use lexical markers to narrow acoustic boundaries.
            let startTime = narrowBoundary(
                acoustic: acousticStart,
                lexical: lexical.startTime,
                direction: .backward,
                seed: seed
            )
            let endTime = narrowBoundary(
                acoustic: acousticEnd,
                lexical: lexical.endTime,
                direction: .forward,
                seed: seed
            )

            let confidence: Double
            if acousticStart != nil || acousticEnd != nil {
                confidence = 0.85
            } else {
                confidence = 0.7
            }

            // Safety clamp: narrowing can invert start/end with noisy data.
            let finalStart = min(startTime, endTime)
            let finalEnd = max(startTime, endTime)

            logger.info("Boundary expansion acoustic+lexical: \(finalStart, format: .fixed(precision: 1))–\(finalEnd, format: .fixed(precision: 1))")
            return ExpandedBoundary(
                startTime: finalStart,
                endTime: finalEnd,
                boundaryConfidence: confidence,
                source: .acousticAndLexical
            )
        }

        // Acoustic only — use best silence points.
        if acousticStart != nil || acousticEnd != nil {
            let startTime = acousticStart ?? (seed - expansionConfig.fallbackBackwardWidth)
            let endTime = acousticEnd ?? (seed + expansionConfig.fallbackForwardWidth)

            logger.info("Boundary expansion acoustic-only: \(startTime, format: .fixed(precision: 1))–\(endTime, format: .fixed(precision: 1))")
            return ExpandedBoundary(
                startTime: max(0, startTime),
                endTime: endTime,
                boundaryConfidence: 0.55,
                source: .acousticOnly
            )
        }

        // Fallback: seed ± 30s, snapped to nearest silence if possible.
        let fallbackStart = snapToNearestSilence(
            time: seed - expansionConfig.fallbackBackwardWidth,
            featureWindows: featureWindows,
            boundaryType: .start,
            lexicalHits: lexicalHits,
            anchorType: anchorType
        )
        let fallbackEnd = snapToNearestSilence(
            time: seed + expansionConfig.fallbackForwardWidth,
            featureWindows: featureWindows,
            boundaryType: .end,
            lexicalHits: lexicalHits,
            anchorType: anchorType
        )

        logger.info("Boundary expansion fallback: \(fallbackStart, format: .fixed(precision: 1))–\(fallbackEnd, format: .fixed(precision: 1))")
        return ExpandedBoundary(
            startTime: max(0, fallbackStart),
            endTime: fallbackEnd,
            boundaryConfidence: 0.3,
            source: .fallback
        )
    }

    // MARK: - Signal 3: Existing AdWindows

    /// Check if any existing AdWindows overlap or adjoin the seed point.
    /// Returns the union of all overlapping/adjoining windows.
    private func expandFromExistingWindows(
        seed: Double,
        adWindows: [AdWindow]
    ) -> ExpandedBoundary? {
        // Find the window that directly contains the seed point.
        let adjacencyThreshold = 5.0
        let containingWindows = adWindows.filter { window in
            seed >= window.startTime - adjacencyThreshold &&
            seed <= window.endTime + adjacencyThreshold
        }

        guard !containingWindows.isEmpty else { return nil }

        // Standard interval union: sort by startTime and merge all overlapping
        // or touching windows. Continue trying subsequent windows even after
        // a gap — they may overlap later windows in the sorted sequence.
        let sorted = containingWindows.sorted { $0.startTime < $1.startTime }
        var intervals: [(start: Double, end: Double, maxConf: Double)] = []
        var curStart = sorted[0].startTime
        var curEnd = sorted[0].endTime
        var curMaxConf = sorted[0].confidence
        for window in sorted.dropFirst() {
            if window.startTime <= curEnd {
                curEnd = max(curEnd, window.endTime)
                curMaxConf = max(curMaxConf, window.confidence)
            } else {
                intervals.append((curStart, curEnd, curMaxConf))
                curStart = window.startTime
                curEnd = window.endTime
                curMaxConf = window.confidence
            }
        }
        intervals.append((curStart, curEnd, curMaxConf))

        // Pick the interval that contains the seed. If the seed falls in a gap
        // between intervals, pick the one whose nearest edge is closest.
        guard let bestInterval = intervals.first(where: { seed >= $0.start && seed <= $0.end })
            ?? intervals.min(by: {
                let d0 = min(abs(seed - $0.start), abs(seed - $0.end))
                let d1 = min(abs(seed - $1.start), abs(seed - $1.end))
                return d0 < d1
            })
        else { return nil }
        let startTime = bestInterval.start
        let endTime = bestInterval.end
        let maxConfidence = bestInterval.maxConf

        return ExpandedBoundary(
            startTime: startTime,
            endTime: endTime,
            boundaryConfidence: min(maxConfidence + 0.1, 1.0),
            source: .existingWindow
        )
    }

    // MARK: - Signal 1: Acoustic Boundaries

    private enum SearchDirection {
        case backward, forward
    }

    /// Search for the best acoustic boundary point within the search radius.
    private func findAcousticBoundary(
        seed: Double,
        direction: SearchDirection,
        featureWindows: [FeatureWindow],
        searchRadius: Double,
        lexicalHits: [LexicalHit],
        anchorType: AnchorType
    ) -> Double? {
        let nearby: [FeatureWindow]
        switch direction {
        case .backward:
            nearby = featureWindows.filter { fw in
                let center = (fw.startTime + fw.endTime) / 2.0
                return center >= seed - searchRadius && center <= seed
            }
        case .forward:
            nearby = featureWindows.filter { fw in
                let center = (fw.startTime + fw.endTime) / 2.0
                return center >= seed && center <= seed + searchRadius
            }
        }

        guard !nearby.isEmpty else { return nil }
        let boundaryType: BoundaryType = direction == .backward ? .start : .end
        let resolved = boundaryResolver.snap(
            candidateTime: seed,
            boundaryType: boundaryType,
            anchorType: anchorType,
            featureWindows: nearby,
            lexicalHits: lexicalHits,
            config: resolverConfig(
                anchorType: anchorType,
                snapDistance: BoundarySnapDistance(start: searchRadius, end: searchRadius)
            )
        )

        if abs(resolved - seed) < 0.000_001 {
            return nearby.contains { featureWindow in
                abs(boundaryTime(for: featureWindow, boundaryType: boundaryType) - seed) < 0.000_001
            } ? seed : nil
        }

        return resolved
    }

    // MARK: - Signal 2: Lexical Boundaries

    private func makeLexicalContext(
        seed: Double,
        transcriptChunks: [TranscriptChunk],
        config: ExpansionConfig
    ) -> LexicalContext? {
        let nearbyChunks = transcriptChunks.filter { chunk in
            chunk.endTime >= seed - config.lexicalBackwardSearchRadius &&
            chunk.startTime <= seed + config.lexicalForwardSearchRadius
        }

        guard !nearbyChunks.isEmpty else { return nil }

        let scanner = LexicalScanner()
        let hits = nearbyChunks
            .flatMap { scanner.scanChunk($0) }
            .sorted { lhs, rhs in
                if lhs.startTime != rhs.startTime {
                    return lhs.startTime < rhs.startTime
                }
                return lhs.endTime < rhs.endTime
            }
        let candidates = scanner.scan(chunks: nearbyChunks, analysisAssetId: nearbyChunks[0].analysisAssetId)

        return LexicalContext(candidates: candidates, hits: hits)
    }

    /// Returns the lexical candidate region that contains or is nearest the seed.
    private func findLexicalBoundaries(
        seed: Double,
        candidates: [LexicalCandidate],
        config: ExpansionConfig
    ) -> (startTime: Double, endTime: Double)? {

        guard !candidates.isEmpty else { return nil }

        // Find the candidate region that contains or is closest to the seed.
        // Prefer candidates that contain the seed; otherwise pick the nearest.
        let containingSeed = candidates.filter { c in
            c.startTime <= seed && c.endTime >= seed
        }

        if let best = containingSeed.first {
            return (startTime: best.startTime, endTime: best.endTime)
        }

        let reachableCandidates = candidates.compactMap { candidate -> (candidate: LexicalCandidate, distance: Double)? in
            guard let distance = lexicalDistance(from: seed, to: candidate, config: config) else {
                return nil
            }
            return (candidate, distance)
        }

        guard let nearest = reachableCandidates.min(by: { lhs, rhs in
            lhs.distance < rhs.distance
        }) else {
            return nil
        }

        return (startTime: nearest.candidate.startTime, endTime: nearest.candidate.endTime)
    }

    private func lexicalDistance(
        from seed: Double,
        to candidate: LexicalCandidate,
        config: ExpansionConfig
    ) -> Double? {
        if candidate.startTime <= seed && candidate.endTime >= seed {
            return 0
        }

        if candidate.endTime < seed {
            let distance = seed - candidate.endTime
            return distance <= config.lexicalBackwardSearchRadius ? distance : nil
        }

        let distance = candidate.startTime - seed
        return distance <= config.lexicalForwardSearchRadius ? distance : nil
    }

    // MARK: - Boundary Narrowing

    /// Narrow an acoustic boundary using a lexical marker.
    /// The lexical marker is treated as a tighter bound.
    private func narrowBoundary(
        acoustic: Double?,
        lexical: Double,
        direction: SearchDirection,
        seed: Double
    ) -> Double {
        guard let acoustic else { return lexical }

        switch direction {
        case .backward:
            // For the start boundary, use the later of acoustic/lexical
            // (lexical narrows inward toward the seed).
            return max(acoustic, lexical)
        case .forward:
            // For the end boundary, use the earlier of acoustic/lexical.
            return min(acoustic, lexical)
        }
    }

    // MARK: - Silence Snapping

    /// Snap a time to the nearest resolver-selected boundary point within ±10s.
    /// Used for fallback boundaries.
    private func snapToNearestSilence(
        time: Double,
        featureWindows: [FeatureWindow],
        boundaryType: BoundaryType,
        lexicalHits: [LexicalHit],
        anchorType: AnchorType
    ) -> Double {
        let snapRadius = 10.0
        let nearby = featureWindows.filter { fw in
            let center = (fw.startTime + fw.endTime) / 2.0
            return abs(center - time) <= snapRadius
        }

        guard !nearby.isEmpty else { return time }

        return boundaryResolver.snap(
            candidateTime: time,
            boundaryType: boundaryType,
            anchorType: anchorType,
            featureWindows: nearby,
            lexicalHits: lexicalHits,
            config: resolverConfig(
                anchorType: anchorType,
                snapDistance: BoundarySnapDistance(start: snapRadius, end: snapRadius)
            )
        )
    }

    private func resolverConfig(
        anchorType: AnchorType,
        snapDistance: BoundarySnapDistance
    ) -> BoundarySnappingConfig {
        BoundarySnappingConfig(
            startWeights: StartBoundaryCueWeights(
                pauseVAD: 0.55,
                speakerChangeProxy: 0.10,
                musicBedChange: 0.10,
                spectralChange: 0.20,
                lexicalDensityDelta: 0.05
            ),
            endWeights: EndBoundaryCueWeights(
                pauseVAD: 0.55,
                speakerChangeProxy: 0.10,
                musicBedChange: 0.10,
                spectralChange: 0.15,
                explicitReturnMarker: 0.10
            ),
            maxSnapDistanceByAnchorType: [anchorType: snapDistance],
            lambda: 0.15,
            minBoundaryScore: 0.3,
            minImprovementOverOriginal: 0.05
        )
    }

    private func boundaryTime(
        for featureWindow: FeatureWindow,
        boundaryType: BoundaryType
    ) -> Double {
        switch boundaryType {
        case .start:
            featureWindow.startTime
        case .end:
            featureWindow.endTime
        }
    }
}
