// SpectralShift.swift
// playhead-gtt9.12: Spectral-profile (cepstral / MFCC-proxy) shift detector.
//
// Why it helps: studio-recorded conversational content has a very different
// spectral envelope from a produced ad bed (narrower band-limited voiceover
// on top of music). A rolling change in spectral energy distribution across
// adjacent windows is transcript-free evidence of an ad transition.
//
// A full MFCC distance would require re-running the FFT per window. The
// feature pipeline already caches `spectralFlux` (per-bin magnitude delta
// between adjacent FFT frames) which tracks the same underlying spectral-
// change phenomenon. We treat a run of elevated `spectralFlux` (vs. the
// episode median) as the shift signal.
//
// Real MFCC-delta computation is deferred to gtt9.3 once we decide how much
// extra FFT work to add to the feature extractor. The current signal is
// sufficient for fusion weight calibration.
//
// Pure function on `FeatureWindow` arrays.

import Foundation

enum SpectralShift {

    struct Config: Sendable, Equatable {
        /// Multiplier on episode median flux above which a window counts as
        /// "produced a signal". 1.75 ≈ flagging the top ~20% of windows in
        /// stable podcast content.
        let elevationRatio: Double
        /// Multiplier on median flux at which the score saturates to 1.
        let saturationRatio: Double
        /// Fusion gate threshold.
        let gateScore: Double

        static let `default` = Config(
            elevationRatio: 1.75,
            saturationRatio: 3.5,
            gateScore: 0.30
        )
    }

    static func scores(
        for windows: [FeatureWindow],
        config: Config = .default,
        funnel: inout AcousticFeatureFunnel
    ) -> [AcousticFeatureScore] {
        guard !windows.isEmpty else { return [] }

        let median = medianFlux(windows: windows)
        guard median > 0 else {
            // Degenerate episode — report all zero scores but still count computes.
            return windows.map { w in
                funnel.record(feature: .spectralShift, producedSignal: false, passedGate: false, includedInFusion: false)
                return AcousticFeatureScore(
                    feature: .spectralShift,
                    windowStart: w.startTime,
                    windowEnd: w.endTime,
                    score: 0,
                    rawMetric: 0
                )
            }
        }

        var out: [AcousticFeatureScore] = []
        out.reserveCapacity(windows.count)
        for window in windows {
            let ratio = window.spectralFlux / median
            let score = mapRatioToScore(ratio, config: config)
            let produced = ratio >= config.elevationRatio
            let gate = score >= config.gateScore
            funnel.record(
                feature: .spectralShift,
                producedSignal: produced,
                passedGate: gate,
                includedInFusion: gate
            )
            out.append(AcousticFeatureScore(
                feature: .spectralShift,
                windowStart: window.startTime,
                windowEnd: window.endTime,
                score: score,
                rawMetric: ratio
            ))
        }
        return out
    }

    static func mapRatioToScore(_ ratio: Double, config: Config) -> Double {
        guard ratio >= config.elevationRatio else { return 0 }
        let span = config.saturationRatio - config.elevationRatio
        guard span > 0 else { return 1 }
        return clampUnit((ratio - config.elevationRatio) / span)
    }

    static func medianFlux(windows: [FeatureWindow]) -> Double {
        let sorted = windows.map(\.spectralFlux).sorted()
        guard !sorted.isEmpty else { return 0 }
        let mid = sorted.count / 2
        if sorted.count.isMultiple(of: 2) {
            return (sorted[mid - 1] + sorted[mid]) / 2
        }
        return sorted[mid]
    }
}
