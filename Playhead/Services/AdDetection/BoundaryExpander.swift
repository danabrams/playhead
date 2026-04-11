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
struct ExpandedBoundary: Sendable {
    let startTime: Double
    let endTime: Double
    let boundaryConfidence: Double // 0.0–1.0
    let source: BoundarySource
}

// MARK: - BoundaryExpander

/// Stateless utility that expands a seed time into ad boundaries using three
/// signal layers: existing AdWindows (highest priority), acoustic features
/// (FeatureWindow pause/RMS scoring), and lexical patterns (LexicalScanner).
struct BoundaryExpander: Sendable {

    private let logger = Logger(subsystem: "com.playhead", category: "BoundaryExpander")

    /// Maximum distance (seconds) to search backward/forward for acoustic boundaries.
    private let acousticSearchRadius: Double = 60.0

    /// Maximum distance (seconds) to search for transcript chunks around the seed.
    private let lexicalSearchRadius: Double = 90.0

    /// Fallback half-width when no signals are found.
    private let fallbackHalfWidth: Double = 30.0

    /// Minimum combined score for a feature window to qualify as a boundary point.
    /// Intentionally lower than SkipOrchestrator's 0.6 threshold: user corrections
    /// should be more generous in finding boundaries since the user has confirmed
    /// an ad is present.
    private let silenceThreshold: Double = 0.4

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
        adWindows: [AdWindow]
    ) -> ExpandedBoundary {
        // Signal 3 (highest priority): Check existing AdWindows.
        if let windowBoundary = expandFromExistingWindows(seed: seed, adWindows: adWindows) {
            logger.info("Boundary expansion from existing window: \(windowBoundary.startTime, format: .fixed(precision: 1))–\(windowBoundary.endTime, format: .fixed(precision: 1))")
            return windowBoundary
        }

        // Signal 1: Acoustic boundaries from FeatureWindows.
        let acousticStart = findAcousticBoundary(seed: seed, direction: .backward, featureWindows: featureWindows)
        let acousticEnd = findAcousticBoundary(seed: seed, direction: .forward, featureWindows: featureWindows)

        // Signal 2: Narrow using lexical patterns if available.
        let lexicalBoundary = findLexicalBoundaries(
            seed: seed,
            transcriptChunks: transcriptChunks
        )

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
            let startTime = acousticStart ?? (seed - fallbackHalfWidth)
            let endTime = acousticEnd ?? (seed + fallbackHalfWidth)

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
            time: seed - fallbackHalfWidth,
            featureWindows: featureWindows
        )
        let fallbackEnd = snapToNearestSilence(
            time: seed + fallbackHalfWidth,
            featureWindows: featureWindows
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

        // Only union windows that truly overlap or are contiguous (no gap).
        // Sort by startTime and merge overlapping/touching windows.
        let sorted = containingWindows.sorted { $0.startTime < $1.startTime }
        var mergedStart = sorted[0].startTime
        var mergedEnd = sorted[0].endTime
        for window in sorted.dropFirst() {
            if window.startTime <= mergedEnd {
                mergedEnd = max(mergedEnd, window.endTime)
            }
            // Non-overlapping windows are not unioned to avoid spanning gaps.
        }

        let relevantWindows = containingWindows
        let startTime = mergedStart
        let endTime = mergedEnd
        let maxConfidence = relevantWindows.map(\.confidence).max()!

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

    /// Score a feature window using the same formula as SkipOrchestrator.snapBoundary:
    /// `combined = pauseProbability * 0.7 + max(0, 1 - rms * 10) * 0.3`
    private func scoreFeatureWindow(_ fw: FeatureWindow) -> Double {
        let pauseScore = fw.pauseProbability
        let quietScore = max(0, 1.0 - fw.rms * 10.0)
        return pauseScore * 0.7 + quietScore * 0.3
    }

    /// Search for the best acoustic boundary point within the search radius.
    private func findAcousticBoundary(
        seed: Double,
        direction: SearchDirection,
        featureWindows: [FeatureWindow]
    ) -> Double? {
        let nearby: [FeatureWindow]
        switch direction {
        case .backward:
            nearby = featureWindows.filter { fw in
                let center = (fw.startTime + fw.endTime) / 2.0
                return center >= seed - acousticSearchRadius && center < seed
            }
        case .forward:
            nearby = featureWindows.filter { fw in
                let center = (fw.startTime + fw.endTime) / 2.0
                return center > seed && center <= seed + acousticSearchRadius
            }
        }

        guard !nearby.isEmpty else { return nil }

        var bestTime: Double?
        var bestScore: Double = -1

        for fw in nearby {
            let score = scoreFeatureWindow(fw)
            if score > bestScore && score >= silenceThreshold {
                bestScore = score
                switch direction {
                case .backward:
                    bestTime = fw.startTime
                case .forward:
                    bestTime = fw.endTime
                }
            }
        }

        return bestTime
    }

    // MARK: - Signal 2: Lexical Boundaries

    /// Run LexicalScanner on transcript chunks near the seed point.
    /// Returns the earliest sponsor-intro boundary and the latest transition boundary.
    private func findLexicalBoundaries(
        seed: Double,
        transcriptChunks: [TranscriptChunk]
    ) -> (startTime: Double, endTime: Double)? {
        let nearbyChunks = transcriptChunks.filter { chunk in
            chunk.endTime >= seed - lexicalSearchRadius &&
            chunk.startTime <= seed + lexicalSearchRadius
        }

        guard !nearbyChunks.isEmpty else { return nil }

        let scanner = LexicalScanner()
        let candidates = scanner.scan(
            chunks: nearbyChunks,
            analysisAssetId: nearbyChunks[0].analysisAssetId
        )

        guard !candidates.isEmpty else { return nil }

        // Find the candidate region that contains or is closest to the seed.
        // Prefer candidates that contain the seed; otherwise pick the nearest.
        let containingSeed = candidates.filter { c in
            c.startTime <= seed && c.endTime >= seed
        }

        if let best = containingSeed.first {
            return (startTime: best.startTime, endTime: best.endTime)
        }

        // Find nearest candidate to the seed.
        let nearest = candidates.min { a, b in
            let distA = min(abs(a.startTime - seed), abs(a.endTime - seed))
            let distB = min(abs(b.startTime - seed), abs(b.endTime - seed))
            return distA < distB
        }

        guard let nearest else { return nil }

        // Only use if reasonably close (within lexical search radius).
        let dist = min(abs(nearest.startTime - seed), abs(nearest.endTime - seed))
        guard dist <= lexicalSearchRadius else { return nil }

        return (startTime: nearest.startTime, endTime: nearest.endTime)
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

    /// Snap a time to the nearest silence point within ±10s.
    /// Used for fallback boundaries.
    private func snapToNearestSilence(
        time: Double,
        featureWindows: [FeatureWindow]
    ) -> Double {
        let snapRadius = 10.0
        let nearby = featureWindows.filter { fw in
            let center = (fw.startTime + fw.endTime) / 2.0
            return abs(center - time) <= snapRadius
        }

        guard !nearby.isEmpty else { return time }

        var bestTime = time
        var bestScore: Double = -1

        for fw in nearby {
            let score = scoreFeatureWindow(fw)
            if score > bestScore && score >= silenceThreshold {
                bestScore = score
                // Snap to the edge closest to the target time.
                if abs(fw.startTime - time) < abs(fw.endTime - time) {
                    bestTime = fw.startTime
                } else {
                    bestTime = fw.endTime
                }
            }
        }

        return bestTime
    }
}
