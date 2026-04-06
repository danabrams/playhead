// AcousticBreakDetector.swift
// Scans FeatureWindows for energy drops, spectral flux spikes, and pause
// clusters that suggest segment boundaries across a full podcast episode.
// Produces candidate break points that seed RegionProposalBuilder (Phase 4).

import Foundation
import OSLog

// MARK: - AcousticBreakSignal

/// Signal types that contribute to a break detection.
enum AcousticBreakSignal: String, Sendable, CaseIterable {
    case energyDrop     // RMS drop > threshold (loud -> quiet, e.g. content -> ad)
    case energyRise     // RMS rise > threshold (quiet -> loud, e.g. ad -> content)
    case spectralSpike  // Spectral flux above 80th percentile
    case pauseCluster   // Consecutive high-pauseProbability windows
}

// MARK: - AcousticBreak

/// A candidate break point in the episode audio.
struct AcousticBreak: Sendable {
    /// Break point time (seconds into episode).
    let time: Double
    /// Combined break strength (0.0...1.0).
    let breakStrength: Double
    /// Signal types that contributed to this break.
    let signals: Set<AcousticBreakSignal>
}

// MARK: - AcousticBreakDetector

/// Stateless detector that identifies acoustic break points from FeatureWindows.
/// Pure computation on arrays of value types — no actor needed.
enum AcousticBreakDetector {

    struct Config: Sendable {
        /// Minimum fractional RMS change between adjacent windows to flag a transition.
        /// Computed as abs(prev - curr) / max(prev, curr).
        let energyDropThreshold: Double
        /// Minimum absolute RMS difference to flag a transition.
        /// Prevents false positives from normal speech variation where relative change
        /// can be high even though the absolute difference is small (e.g., 0.35→0.65 is
        /// 46% relative but only 0.30 absolute — normal speech).
        let minAbsoluteRMSDifference: Double
        /// Pause probability above which a window is considered a pause.
        let pauseProbabilityThreshold: Double
        /// Minimum consecutive high-pause windows to form a cluster.
        let minPauseClusterSize: Int
        /// Minimum RMS level in the louder window to consider an energy drop meaningful.
        /// Prevents false positives from two quiet windows (e.g. both near silence).
        let minRMSForEnergyDrop: Double
        /// Weight of energy drop signal in combined score.
        let energyDropWeight: Double
        /// Weight of spectral spike signal in combined score.
        let spectralSpikeWeight: Double
        /// Weight of pause cluster signal in combined score.
        let pauseClusterWeight: Double

        static let `default` = Config(
            energyDropThreshold: 0.35,
            minAbsoluteRMSDifference: 0.25,
            pauseProbabilityThreshold: 0.6,
            minPauseClusterSize: 2,
            minRMSForEnergyDrop: 0.05,
            energyDropWeight: 0.4,
            spectralSpikeWeight: 0.3,
            pauseClusterWeight: 0.3
        )
    }

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "AcousticBreakDetector"
    )

    // MARK: - Public API

    /// Detect acoustic break points across all feature windows for an episode.
    ///
    /// - Parameters:
    ///   - windows: Feature windows sorted by startTime. Expected to be 2.0s each, no overlap.
    ///   - config: Detection thresholds and weights.
    /// - Returns: Break points sorted by time, deduplicated by merging co-located signals.
    static func detectBreaks(
        in windows: [FeatureWindow],
        config: Config = .default
    ) -> [AcousticBreak] {
        guard windows.count >= 2 else { return [] }

        let sorted = windows.sorted { $0.startTime < $1.startTime }

        // Compute 80th percentile of spectral flux.
        let spectralFluxThreshold = percentile(
            values: sorted.map { $0.spectralFlux },
            p: 0.80
        )

        // Detect each signal type independently.
        let energyTransitions = detectEnergyTransitions(sorted, config: config)
        let spectralSpikes = detectSpectralSpikes(
            sorted,
            threshold: spectralFluxThreshold
        )
        let pauseClusters = detectPauseClusters(sorted, config: config)

        // Merge co-located signals into combined breaks.
        let breaks = mergeSignals(
            energyTransitions: energyTransitions,
            spectralSpikes: spectralSpikes,
            pauseClusters: pauseClusters,
            windowDuration: sorted[0].endTime - sorted[0].startTime,
            config: config
        )

        logger.info("Detected \(breaks.count) breaks from \(sorted.count) windows (energy: \(energyTransitions.count), spectral: \(spectralSpikes.count), pause: \(pauseClusters.count))")

        return breaks
    }

    // MARK: - Energy transition detection

    private struct EnergyTransition {
        let time: Double
        let signal: AcousticBreakSignal // .energyDrop or .energyRise
    }

    /// Finds boundary times where RMS changes by more than the threshold
    /// between adjacent windows. Tags drops vs rises for downstream use.
    private static func detectEnergyTransitions(
        _ windows: [FeatureWindow],
        config: Config
    ) -> [EnergyTransition] {
        var transitions: [EnergyTransition] = []

        for i in 1..<windows.count {
            let prev = windows[i - 1]
            let curr = windows[i]
            let maxRMS = max(prev.rms, curr.rms)

            // Skip if both windows are near-silent — not a meaningful transition.
            guard maxRMS >= config.minRMSForEnergyDrop else { continue }

            let absDiff = abs(prev.rms - curr.rms)
            // Skip if the absolute difference is small — normal speech variation
            // can produce high relative changes with small absolute differences.
            guard absDiff >= config.minAbsoluteRMSDifference else { continue }

            let drop = (prev.rms - curr.rms) / maxRMS
            if drop > config.energyDropThreshold {
                transitions.append(EnergyTransition(time: prev.endTime, signal: .energyDrop))
            }

            // Rising edges: ads start with a drop, end with a rise.
            let rise = (curr.rms - prev.rms) / maxRMS
            if rise > config.energyDropThreshold {
                transitions.append(EnergyTransition(time: curr.startTime, signal: .energyRise))
            }
        }

        return transitions
    }

    // MARK: - Spectral spike detection

    /// Finds times where spectral flux significantly exceeds the 80th percentile.
    /// A spike must be at least 2x the threshold to qualify — this prevents
    /// false positives when spectral flux has low natural variation.
    private static func detectSpectralSpikes(
        _ windows: [FeatureWindow],
        threshold: Double
    ) -> [Double] {
        guard threshold > 0 else { return [] }

        // Require spectral flux to be significantly above the percentile,
        // not just barely above it. A true transition spike (jingle, music bed,
        // ad insertion point) will be dramatically above normal variation.
        let spikeThreshold = threshold * 2.0

        var times: [Double] = []
        for window in windows where window.spectralFlux > spikeThreshold {
            let midpoint = (window.startTime + window.endTime) / 2.0
            times.append(midpoint)
        }
        return times
    }

    // MARK: - Pause cluster detection

    /// Finds clusters of consecutive windows with high pause probability.
    /// Returns the boundary time at the start of each cluster.
    private static func detectPauseClusters(
        _ windows: [FeatureWindow],
        config: Config
    ) -> [Double] {
        var times: [Double] = []
        var runStart: Int?
        var runLength = 0

        for i in 0..<windows.count {
            if windows[i].pauseProbability > config.pauseProbabilityThreshold {
                if runStart == nil { runStart = i }
                runLength += 1
            } else {
                if runLength >= config.minPauseClusterSize, let start = runStart {
                    // Break at the beginning of the pause cluster.
                    times.append(windows[start].startTime)
                }
                runStart = nil
                runLength = 0
            }
        }

        // Handle cluster that extends to end of episode.
        if runLength >= config.minPauseClusterSize, let start = runStart {
            times.append(windows[start].startTime)
        }

        return times
    }

    // MARK: - Signal merging

    /// Merge break times from all signal types. Times within one window
    /// duration of each other are combined into a single break with a
    /// higher composite score.
    private static func mergeSignals(
        energyTransitions: [EnergyTransition],
        spectralSpikes: [Double],
        pauseClusters: [Double],
        windowDuration: Double,
        config: Config
    ) -> [AcousticBreak] {
        // Tag each time with its signal.
        struct TaggedTime {
            let time: Double
            let signal: AcousticBreakSignal
        }

        var tagged: [TaggedTime] = []
        for t in energyTransitions { tagged.append(TaggedTime(time: t.time, signal: t.signal)) }
        for t in spectralSpikes { tagged.append(TaggedTime(time: t, signal: .spectralSpike)) }
        for t in pauseClusters { tagged.append(TaggedTime(time: t, signal: .pauseCluster)) }

        guard !tagged.isEmpty else { return [] }

        tagged.sort { $0.time < $1.time }

        // Group tagged times within windowDuration of each other.
        var breaks: [AcousticBreak] = []
        var groupStart = tagged[0].time
        var groupSignals: Set<AcousticBreakSignal> = [tagged[0].signal]
        var groupTimes: [Double] = [tagged[0].time]

        for item in tagged.dropFirst() {
            if item.time - groupStart <= windowDuration {
                // Same group — accumulate.
                groupSignals.insert(item.signal)
                groupTimes.append(item.time)
            } else {
                // Emit previous group.
                let avg = groupTimes.reduce(0, +) / Double(groupTimes.count)
                let strength = computeStrength(
                    signals: groupSignals,
                    config: config
                )
                breaks.append(AcousticBreak(
                    time: avg,
                    breakStrength: strength,
                    signals: groupSignals
                ))

                // Start new group.
                groupStart = item.time
                groupSignals = [item.signal]
                groupTimes = [item.time]
            }
        }

        // Emit final group.
        let avg = groupTimes.reduce(0, +) / Double(groupTimes.count)
        let strength = computeStrength(signals: groupSignals, config: config)
        breaks.append(AcousticBreak(
            time: avg,
            breakStrength: strength,
            signals: groupSignals
        ))

        return breaks
    }

    // MARK: - Scoring

    /// Compute break strength from the set of contributing signals.
    /// Multi-signal breaks score higher than single-signal breaks.
    private static func computeStrength(
        signals: Set<AcousticBreakSignal>,
        config: Config
    ) -> Double {
        var score = 0.0

        // Energy drop and rise share one weight slot — they represent the same
        // physical phenomenon (energy transition) at different polarities.
        // If both appear in the same merge group (brief dip-and-recovery),
        // they contribute only once to avoid double-counting.
        if signals.contains(.energyDrop) || signals.contains(.energyRise) {
            score += config.energyDropWeight
        }
        if signals.contains(.spectralSpike) { score += config.spectralSpikeWeight }
        if signals.contains(.pauseCluster) { score += config.pauseClusterWeight }

        // Clamp to 0...1.
        return min(max(score, 0.0), 1.0)
    }

    // MARK: - Utilities

    /// Compute the p-th percentile of an array of values (0.0...1.0).
    /// Uses linear interpolation between nearest ranks.
    private static func percentile(values: [Double], p: Double) -> Double {
        guard !values.isEmpty else { return 0 }
        let sorted = values.sorted()
        let rank = p * Double(sorted.count - 1)
        let lower = Int(rank)
        let upper = min(lower + 1, sorted.count - 1)
        let fraction = rank - Double(lower)
        return sorted[lower] + fraction * (sorted[upper] - sorted[lower])
    }
}
