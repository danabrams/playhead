// MusicBedClassifier.swift
// Distinguishes background music beds (low-level production beds under speech)
// from foreground music segments (jingles, song clips) using amplitude-relative
// thresholds and spectral centroid gating.
//
// Pure computation on value types -- no actor needed.

import Foundation

// MARK: - MusicBedLevel

/// Classification of the music presence in a feature window.
enum MusicBedLevel: String, Sendable, Equatable, CaseIterable {
    /// No detectable music in this window.
    case none
    /// Low-level background music bed under speech (production music, beds).
    case background
    /// Foreground music segment (jingles, song clips, stingers).
    case foreground
}

// MARK: - MusicDetectionConfig

/// Configurable parameters for music bed detection.
/// Supports tunable `SNClassifySoundRequest` window durations and
/// classification thresholds.
struct MusicDetectionConfig: Sendable, Equatable {

    /// Duration of the SoundAnalysis classification window in seconds.
    /// Supported values: 1.0, 2.0, 4.0. Default is 2.0 for podcasts.
    let windowDuration: TimeInterval

    /// musicProbability below this threshold is classified as `.none`.
    let noneMusicProbabilityThreshold: Double

    /// musicProbability at or above this threshold is classified as
    /// `.foreground` (subject to amplitude gating).
    let foregroundMusicProbabilityThreshold: Double

    /// When music is detected, if the RMS is below this fraction of
    /// the local mean RMS, classify as `.background` rather than
    /// `.foreground`. This distinguishes quiet production beds from
    /// loud jingles.
    let backgroundAmplitudeRatio: Double

    /// Spectral centroid gating: foreground music (jingles, stingers) tends
    /// to have higher spectral energy. When spectral flux exceeds this
    /// threshold relative to the local baseline, prefer `.foreground`.
    let foregroundSpectralFluxThreshold: Double

    /// Scaling factor applied to the absolute music probability delta
    /// to produce the onset/offset scores.
    let changeScoreScalingFactor: Double

    static let `default` = MusicDetectionConfig(
        windowDuration: 2.0,
        noneMusicProbabilityThreshold: 0.15,
        foregroundMusicProbabilityThreshold: 0.6,
        backgroundAmplitudeRatio: 0.7,
        foregroundSpectralFluxThreshold: 0.3,
        changeScoreScalingFactor: 1.5
    )

    /// Supported window durations for SNClassifySoundRequest.
    static let supportedWindowDurations: [TimeInterval] = [1.0, 2.0, 4.0]

    /// Validate that the window duration is one of the supported values.
    var isValid: Bool {
        Self.supportedWindowDurations.contains(windowDuration)
    }
}

// MARK: - MusicBedClassifier

/// Stateless classifier that determines the music bed level for a feature
/// window and computes directional onset/offset scores.
enum MusicBedClassifier {

    /// Result of classifying a single feature window's music content.
    struct Classification: Sendable, Equatable {
        /// The determined music bed level.
        let level: MusicBedLevel
        /// Score indicating how strongly music is starting (0-1).
        /// High when transitioning from no/low music to music.
        let onsetScore: Double
        /// Score indicating how strongly music is ending (0-1).
        /// High when transitioning from music to no/low music.
        let offsetScore: Double
        /// Legacy-compatible combined change score (absolute delta).
        /// Preserved for backward compatibility with existing consumers.
        let changeScore: Double
    }

    /// Classify a single feature window's music content.
    ///
    /// - Parameters:
    ///   - musicProbability: Current window's music probability (0-1).
    ///   - previousMusicProbability: Previous window's music probability, nil for the first window.
    ///   - rms: Current window's RMS energy.
    ///   - localMeanRms: Mean RMS energy across nearby windows (for amplitude-relative thresholds).
    ///   - spectralFlux: Current window's spectral flux.
    ///   - config: Detection configuration.
    /// - Returns: Classification with level and directional scores.
    static func classify(
        musicProbability: Double,
        previousMusicProbability: Double?,
        rms: Double,
        localMeanRms: Double,
        spectralFlux: Double,
        config: MusicDetectionConfig = .default
    ) -> Classification {
        precondition(config.isValid, "MusicDetectionConfig has unsupported windowDuration \(config.windowDuration)")
        let level = classifyLevel(
            musicProbability: musicProbability,
            rms: rms,
            localMeanRms: localMeanRms,
            spectralFlux: spectralFlux,
            config: config
        )

        let (onsetScore, offsetScore, changeScore) = computeDirectionalScores(
            musicProbability: musicProbability,
            previousMusicProbability: previousMusicProbability,
            config: config
        )

        return Classification(
            level: level,
            onsetScore: onsetScore,
            offsetScore: offsetScore,
            changeScore: changeScore
        )
    }

    /// Classify music bed level from signal features.
    static func classifyLevel(
        musicProbability: Double,
        rms: Double,
        localMeanRms: Double,
        spectralFlux: Double,
        config: MusicDetectionConfig = .default
    ) -> MusicBedLevel {
        // Below threshold: no music.
        guard musicProbability >= config.noneMusicProbabilityThreshold else {
            return .none
        }

        // High music probability with sufficient amplitude and spectral
        // energy suggests foreground music.
        if musicProbability >= config.foregroundMusicProbabilityThreshold {
            let amplitudeIsHigh = localMeanRms > 0 && rms >= localMeanRms * config.backgroundAmplitudeRatio
            let spectralIsHigh = spectralFlux >= config.foregroundSpectralFluxThreshold

            // Foreground requires either sufficient amplitude OR spectral activity.
            if amplitudeIsHigh || spectralIsHigh {
                return .foreground
            }
        }

        // Music detected but quiet relative to speech: background bed.
        return .background
    }

    /// Compute directional onset/offset scores and the legacy change score.
    static func computeDirectionalScores(
        musicProbability: Double,
        previousMusicProbability: Double?,
        config: MusicDetectionConfig = .default
    ) -> (onset: Double, offset: Double, change: Double) {
        guard let previousMusicProbability else {
            return (onset: 0, offset: 0, change: 0)
        }

        let delta = musicProbability - previousMusicProbability
        let absDelta = abs(delta)

        // Legacy change score: absolute delta scaled, same formula as existing
        // FeatureSignalExtraction.musicBedChangeScore for backward compat.
        let changeScore = clamp01(absDelta * config.changeScoreScalingFactor)

        // Directional scores: positive delta = onset, negative delta = offset.
        let onsetScore: Double
        let offsetScore: Double

        if delta > 0 {
            onsetScore = clamp01(delta * config.changeScoreScalingFactor)
            offsetScore = 0
        } else if delta < 0 {
            onsetScore = 0
            offsetScore = clamp01(-delta * config.changeScoreScalingFactor)
        } else {
            onsetScore = 0
            offsetScore = 0
        }

        return (onset: onsetScore, offset: offsetScore, change: changeScore)
    }

    private static func clamp01(_ value: Double) -> Double {
        min(max(value, 0), 1)
    }
}
