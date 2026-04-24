// DynamicRange.swift
// playhead-gtt9.12: Dynamic range / compression shift.
//
// Why it helps: ad reads are typically mastered with heavier compression than
// conversational podcast content. A window whose crest factor (peak-to-RMS
// ratio) is depressed vs. the show baseline is consistent with tighter
// compression — i.e. produced ad content.
//
// We approximate crest factor from feature windows using:
//
//   crest ≈ rms_local_peak / rms
//
// where `rms_local_peak` is the max RMS observed in a trailing short window.
// A proper crest factor needs sample-peak, but this proxy correlates well
// enough at window granularity for evidence fusion; real peak tracking is
// deferred to gtt9.3.
//
// Pure function on `FeatureWindow` arrays.

import Foundation

enum DynamicRange {

    struct Config: Sendable, Equatable {
        /// Number of adjacent windows to scan for the local peak reference.
        let localPeakRadius: Int
        /// Crest-factor ratio threshold below which signal is "produced".
        /// Default 0.70 matches empirical broadcast-ad compression.
        let compressedRatioFloor: Double
        /// Ratio at which the compression score saturates to 1.0.
        let saturationRatio: Double
        /// Fusion-gate minimum score.
        let gateScore: Double

        static let `default` = Config(
            localPeakRadius: 4,
            compressedRatioFloor: 0.70,
            saturationRatio: 0.40,
            gateScore: 0.25
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

        for (idx, window) in windows.enumerated() {
            let lo = max(0, idx - config.localPeakRadius)
            let hi = min(windows.count - 1, idx + config.localPeakRadius)
            var localPeak = 0.0
            for j in lo...hi {
                if windows[j].rms > localPeak { localPeak = windows[j].rms }
            }
            let ratio = window.rms <= 0 ? 1.0 : min(1.0, window.rms / max(localPeak, 1e-6))
            let score = mapRatioToScore(ratio, config: config)
            let produced = ratio <= config.compressedRatioFloor
            let gate = score >= config.gateScore
            funnel.record(
                feature: .dynamicRange,
                producedSignal: produced,
                passedGate: gate,
                includedInFusion: gate
            )
            out.append(AcousticFeatureScore(
                feature: .dynamicRange,
                windowStart: window.startTime,
                windowEnd: window.endTime,
                score: score,
                rawMetric: ratio
            ))
        }
        return out
    }

    static func mapRatioToScore(_ ratio: Double, config: Config) -> Double {
        guard ratio <= config.compressedRatioFloor else { return 0 }
        let span = config.compressedRatioFloor - config.saturationRatio
        guard span > 0 else { return 1 }
        let normalized = (config.compressedRatioFloor - ratio) / span
        return clampUnit(normalized)
    }
}
