// LufsShift.swift
// playhead-gtt9.12: Loudness / LUFS-shift detector.
//
// Why it helps: inserted ads are mastered separately from the host audio, so
// their integrated loudness differs from the show's baseline. A sustained
// deviation from the episode's rolling loudness baseline is a transcript-free
// hint that a block of windows is structurally different.
//
// Real LUFS needs ITU-R BS.1770 k-weighted gating across ~3s windows. The
// podcast pipeline does not keep a raw loudness track yet (tracked under
// gtt9.3). In the interim we approximate with a `20 * log10(rms)` dBFS proxy
// against an episode-wide baseline. This correlates with LUFS swings that
// exceed ~3 dB (the typical ad-insertion mastering delta) well enough to
// be useful evidence in fusion — calibration will tighten it later.
//
// Pure function on `FeatureWindow` arrays. No state, no I/O.

import Foundation

enum LufsShift {

    struct Config: Sendable, Equatable {
        /// Minimum absolute dBFS delta vs. baseline that counts as "signal produced".
        let signalFloorDb: Double
        /// Absolute dBFS delta at which the per-window score saturates to 1.0.
        let saturationDb: Double
        /// Minimum score at fusion gate (passedGate = score >= gateScore).
        let gateScore: Double

        static let `default` = Config(
            signalFloorDb: 1.5,
            saturationDb: 6.0,
            gateScore: 0.25
        )
    }

    /// Compute per-window LUFS-shift scores against the episode's median dBFS.
    ///
    /// The baseline is the **median** of `20 * log10(max(rms, eps))` across
    /// all windows — robust against the loud insertion biasing the baseline
    /// toward itself. The per-window score linearly maps the absolute
    /// deviation above `signalFloorDb` onto `[0, 1]` and saturates at
    /// `saturationDb`.
    ///
    /// Why median over arithmetic mean: a long sustained-loud insertion
    /// pulls the arithmetic mean toward its own dB, shrinking the delta
    /// the detector sees. The median is unaffected by an insertion that
    /// occupies less than half of the episode (the mainline case).
    ///
    /// - Parameters:
    ///   - windows: All feature windows for the episode. Must be sorted; any
    ///     order is acceptable for baseline computation.
    ///   - config: Detection thresholds.
    ///   - funnel: Inout funnel — each call records compute + downstream stages.
    /// - Returns: One `AcousticFeatureScore` per input window, in the same order.
    static func scores(
        for windows: [FeatureWindow],
        config: Config = .default,
        funnel: inout AcousticFeatureFunnel
    ) -> [AcousticFeatureScore] {
        guard !windows.isEmpty else { return [] }

        let dbValues = windows.map { dbfs(rms: $0.rms) }
        let baseline = medianBaseline(dbValues)

        var out: [AcousticFeatureScore] = []
        out.reserveCapacity(windows.count)

        for (idx, window) in windows.enumerated() {
            let delta = abs(dbValues[idx] - baseline)
            let score = mapDeltaToScore(delta, config: config)
            let produced = delta >= config.signalFloorDb
            let gate = score >= config.gateScore
            funnel.record(
                feature: .lufsShift,
                producedSignal: produced,
                passedGate: gate,
                includedInFusion: gate
            )
            out.append(AcousticFeatureScore(
                feature: .lufsShift,
                windowStart: window.startTime,
                windowEnd: window.endTime,
                score: score,
                rawMetric: delta
            ))
        }
        return out
    }

    // MARK: - Internal helpers (tested directly)

    static func dbfs(rms: Double) -> Double {
        let floored = max(rms, 1e-6)
        return 20 * log10(floored)
    }

    /// Median of an unsorted array. Empty input is undefined behaviour;
    /// callers guard with `!windows.isEmpty`. Even-length arrays return
    /// the upper of the two midpoints (i.e. `sorted[n/2]`, which is the
    /// (n/2 + 1)-th element in 1-indexed terms — no averaging) — adequate
    /// for a detection baseline and avoids floating-point ambiguity.
    static func medianBaseline(_ values: [Double]) -> Double {
        let sorted = values.sorted()
        let n = sorted.count
        precondition(n > 0, "medianBaseline requires non-empty input")
        return sorted[n / 2]
    }

    static func mapDeltaToScore(_ delta: Double, config: Config) -> Double {
        guard delta >= config.signalFloorDb else { return 0 }
        let span = config.saturationDb - config.signalFloorDb
        guard span > 0 else { return 1 }
        let normalized = (delta - config.signalFloorDb) / span
        return clampUnit(normalized)
    }
}
