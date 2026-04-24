// MusicBedFeature.swift
// playhead-gtt9.12: Validation adapter for the pre-existing MusicBedClassifier.
//
// The `MusicBedClassifier` + `MusicBedLedgerEvaluator` stack already converts
// a window's `musicProbability` / `musicBedLevel` into evidence. This adapter
// replays that existing signal through the gtt9.12 funnel so the feature
// expansion framework reports on it alongside the new detectors — i.e. the
// 2% baseline firing rate from the 2026-04-23 real-data eval shows up in the
// funnel snapshot next to the new features.
//
// No new compute — this is the validation-only leg of feature #1.
//
// Pure function on `FeatureWindow` arrays.

import Foundation

enum MusicBedFeature {

    struct Config: Sendable, Equatable {
        /// `musicProbability` at / above which the window counts as "producing"
        /// music-bed evidence. Matches the existing `MusicDetectionConfig`
        /// `.noneMusicProbabilityThreshold` (0.15) so behaviour stays aligned.
        let producedFloor: Double
        /// Probability at which the score saturates to 1.0.
        let saturation: Double
        /// Fusion gate threshold.
        let gateScore: Double

        static let `default` = Config(
            producedFloor: 0.15,
            saturation: 0.85,
            gateScore: 0.30
        )
    }

    static func scores(
        for windows: [FeatureWindow],
        config: Config = .default,
        funnel: inout AcousticFeatureFunnel
    ) -> [AcousticFeatureScore] {
        guard !windows.isEmpty else { return [] }

        var out: [AcousticFeatureScore] = []
        out.reserveCapacity(windows.count)

        for window in windows {
            let score = mapProbability(window.musicProbability, config: config)
            let levelPresent = window.musicBedLevel != .none
            let produced = levelPresent || window.musicProbability >= config.producedFloor
            let gate = score >= config.gateScore
            funnel.record(
                feature: .musicBed,
                producedSignal: produced,
                passedGate: gate,
                includedInFusion: gate
            )
            out.append(AcousticFeatureScore(
                feature: .musicBed,
                windowStart: window.startTime,
                windowEnd: window.endTime,
                score: score,
                rawMetric: window.musicProbability
            ))
        }
        return out
    }

    static func mapProbability(_ p: Double, config: Config) -> Double {
        guard p >= config.producedFloor else { return 0 }
        let span = config.saturation - config.producedFloor
        guard span > 0 else { return 1 }
        return clampUnit((p - config.producedFloor) / span)
    }
}
