// SilenceBoundary.swift
// playhead-gtt9.12: Silence / bumper boundary detector.
//
// Why it helps: ad breaks are typically bracketed by short (0.5–2s) silence
// runs — the "bumper" space between content and the ad bed. A window whose
// immediate neighborhood contains a plausible silence run is more likely to
// sit at an ad boundary than one embedded in continuous speech.
//
// Signal: a window's score reflects how close it is to a qualifying silence
// run. A run is "qualifying" if it has at least `minSilentWindows` consecutive
// windows whose RMS is under `silenceRmsCeiling`. We credit windows within
// `creditRadius` windows of such a run.
//
// Pure function on `FeatureWindow` arrays.

import Foundation

enum SilenceBoundary {

    struct Config: Sendable, Equatable {
        /// RMS at/below which a window is considered silent.
        let silenceRmsCeiling: Double
        /// Minimum consecutive silent windows to count as a bumper candidate.
        let minSilentWindows: Int
        /// Maximum consecutive silent windows before we stop crediting (very long
        /// silence is more likely a gap in capture than an ad bumper).
        let maxSilentWindows: Int
        /// Number of non-silent windows on either side of a qualifying run that
        /// receive the boundary credit.
        let creditRadius: Int
        /// Fusion gate threshold.
        let gateScore: Double

        static let `default` = Config(
            silenceRmsCeiling: 0.02,
            minSilentWindows: 1,
            maxSilentWindows: 8,
            creditRadius: 2,
            gateScore: 0.30
        )
    }

    static func scores(
        for windows: [FeatureWindow],
        config: Config = .default,
        funnel: inout AcousticFeatureFunnel
    ) -> [AcousticFeatureScore] {
        guard !windows.isEmpty else { return [] }

        let boundaryCredit = computeBoundaryCredit(windows: windows, config: config)

        var out: [AcousticFeatureScore] = []
        out.reserveCapacity(windows.count)
        for (idx, window) in windows.enumerated() {
            let credit = boundaryCredit[idx]
            let score = clampUnit(credit)
            let produced = credit > 0
            let gate = score >= config.gateScore
            funnel.record(
                feature: .silenceBoundary,
                producedSignal: produced,
                passedGate: gate,
                includedInFusion: gate
            )
            out.append(AcousticFeatureScore(
                feature: .silenceBoundary,
                windowStart: window.startTime,
                windowEnd: window.endTime,
                score: score,
                rawMetric: credit
            ))
        }
        return out
    }

    /// Returns a per-window credit value in `[0, 1]`. Windows on either
    /// side of a qualifying silence run receive 1.0; silent windows inside
    /// the run receive the saturated credit themselves so they are not
    /// penalised for being the bumper.
    static func computeBoundaryCredit(windows: [FeatureWindow], config: Config) -> [Double] {
        var credit = Array(repeating: 0.0, count: windows.count)
        var i = 0
        while i < windows.count {
            if windows[i].rms <= config.silenceRmsCeiling {
                var j = i
                while j < windows.count && windows[j].rms <= config.silenceRmsCeiling {
                    j += 1
                }
                let runLength = j - i
                if runLength >= config.minSilentWindows && runLength <= config.maxSilentWindows {
                    // Credit the run itself.
                    for k in i..<j {
                        credit[k] = 1
                    }
                    // Credit neighbours within creditRadius.
                    let lo = max(0, i - config.creditRadius)
                    let hi = min(windows.count - 1, j - 1 + config.creditRadius)
                    for k in lo..<i {
                        credit[k] = max(credit[k], 1)
                    }
                    if j <= hi {
                        for k in j...hi {
                            credit[k] = max(credit[k], 1)
                        }
                    }
                }
                i = j
            } else {
                i += 1
            }
        }
        return credit
    }
}
