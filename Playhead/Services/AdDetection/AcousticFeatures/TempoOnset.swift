// TempoOnset.swift
// playhead-gtt9.12: Tempo / rhythm / onset-density detector.
//
// Why it helps: ad beds and show-open stingers have markedly higher onset
// density (snare/kick hits, tonal transients) than conversational speech.
// Without running a full beat tracker we can still approximate onset density
// by counting how many windows in a neighbourhood exhibit a combined
// "energetic + music-y + spectrally active" signature.
//
// Signal:
//   * For each window, `onsetScore = sigmoid-like of (rms * spectralFlux * musicProb)`.
//   * Episode's per-window score is the mean of `onsetScore` over a window
//     neighbourhood of `window ± onsetRadius`, renormalised.
//
// Pure function on `FeatureWindow` arrays.

import Foundation

enum TempoOnset {

    struct Config: Sendable, Equatable {
        /// Radius (in windows) of the local onset-density smoother.
        let onsetRadius: Int
        /// Per-window raw product above which the window counts as "onset".
        let onsetThreshold: Double
        /// Value at which the smoothed density score saturates.
        let saturationDensity: Double
        /// Fusion gate threshold.
        let gateScore: Double

        static let `default` = Config(
            onsetRadius: 3,
            onsetThreshold: 0.08,
            saturationDensity: 0.60,
            gateScore: 0.25
        )
    }

    static func scores(
        for windows: [FeatureWindow],
        config: Config = .default,
        funnel: inout AcousticFeatureFunnel
    ) -> [AcousticFeatureScore] {
        guard !windows.isEmpty else { return [] }

        let perWindowProduct: [Double] = windows.map { w in
            w.rms * w.spectralFlux * w.musicProbability
        }
        let isOnset: [Bool] = perWindowProduct.map { $0 >= config.onsetThreshold }

        var out: [AcousticFeatureScore] = []
        out.reserveCapacity(windows.count)

        for (idx, window) in windows.enumerated() {
            let lo = max(0, idx - config.onsetRadius)
            let hi = min(windows.count - 1, idx + config.onsetRadius)
            let span = hi - lo + 1
            var onsetHits = 0
            for j in lo...hi where isOnset[j] { onsetHits += 1 }
            let density = Double(onsetHits) / Double(span)
            let score = clampUnit(density / max(config.saturationDensity, 0.0001))
            let produced = density > 0
            let gate = score >= config.gateScore
            funnel.record(
                feature: .tempoOnset,
                producedSignal: produced,
                passedGate: gate,
                includedInFusion: gate
            )
            out.append(AcousticFeatureScore(
                feature: .tempoOnset,
                windowStart: window.startTime,
                windowEnd: window.endTime,
                score: score,
                rawMetric: density
            ))
        }
        return out
    }
}
