// SpeakerShift.swift
// playhead-gtt9.12: Lightweight on-device speaker-shift detector.
//
// Why it helps: ad reads are typically voiced by a single announcer while the
// host segment has 1-2 recurring speakers in natural conversation. A sudden
// shift in speaker identity (or cluster id) at the boundary of a candidate
// ad region is transcript-free evidence of an insertion.
//
// A proper speaker embedding needs a model that we don't want to load on the
// hot path yet (Apple's SpeechAnalyzer speaker diarization is the right long-
// term home — see gtt9.3). Today we reuse two signals the feature pipeline
// already computes:
//
//   * `speakerChangeProxyScore` — already a [0, 1] per-window metric.
//   * `speakerClusterId`        — nullable integer when diarization was run.
//
// A window is "shifted" if its cluster id differs from a majority-vote of
// the `historyRadius` windows immediately preceding it, OR if the proxy
// score is above `proxyFloor`. We emit the max of the two.
//
// Pure function on `FeatureWindow` arrays.

import Foundation

enum SpeakerShift {

    struct Config: Sendable, Equatable {
        /// Number of prior windows voted when checking for cluster change.
        let historyRadius: Int
        /// Minimum proxy score that counts as "produced a signal".
        let proxyFloor: Double
        /// Proxy value that saturates the score to 1.0.
        let proxySaturation: Double
        /// Fusion gate threshold.
        let gateScore: Double

        static let `default` = Config(
            historyRadius: 6,
            proxyFloor: 0.30,
            proxySaturation: 0.80,
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

        for (idx, window) in windows.enumerated() {
            let proxyComponent = mapProxy(window.speakerChangeProxyScore, config: config)
            let clusterComponent = clusterShiftComponent(index: idx, windows: windows, config: config)
            let score = max(proxyComponent, clusterComponent)
            let rawDelta = max(window.speakerChangeProxyScore, clusterComponent)
            let produced = window.speakerChangeProxyScore >= config.proxyFloor || clusterComponent > 0
            let gate = score >= config.gateScore
            funnel.record(
                feature: .speakerShift,
                producedSignal: produced,
                passedGate: gate,
                includedInFusion: gate
            )
            out.append(AcousticFeatureScore(
                feature: .speakerShift,
                windowStart: window.startTime,
                windowEnd: window.endTime,
                score: score,
                rawMetric: rawDelta
            ))
        }
        return out
    }

    static func mapProxy(_ proxy: Double, config: Config) -> Double {
        guard proxy >= config.proxyFloor else { return 0 }
        let span = config.proxySaturation - config.proxyFloor
        guard span > 0 else { return 1 }
        return clampUnit((proxy - config.proxyFloor) / span)
    }

    /// 1.0 when the current window's cluster id differs from the majority
    /// id in the preceding `historyRadius` windows, else 0.
    static func clusterShiftComponent(index: Int, windows: [FeatureWindow], config: Config) -> Double {
        guard index > 0 else { return 0 }
        guard let currentCluster = windows[index].speakerClusterId else { return 0 }
        let lo = max(0, index - config.historyRadius)
        guard lo < index else { return 0 }
        var counts: [Int: Int] = [:]
        for j in lo..<index {
            if let c = windows[j].speakerClusterId {
                counts[c, default: 0] += 1
            }
        }
        guard let (dominant, _) = counts.max(by: { $0.value < $1.value }) else { return 0 }
        return dominant == currentCluster ? 0 : 1
    }
}
