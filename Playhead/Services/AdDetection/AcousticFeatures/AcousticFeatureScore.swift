// AcousticFeatureScore.swift
// playhead-gtt9.12: Canonical per-window score produced by every acoustic feature.
//
// Each feature emits a `AcousticFeatureScore` in the `[0, 1]` range. The fusion
// combiner multiplies by a per-feature weight (see `AcousticFeatureFusion`).
// Calibrating real weights is gtt9.3's job — features ship with reasonable priors.
//
// Pure value type.

import Foundation

struct AcousticFeatureScore: Sendable, Equatable {
    /// The feature that produced this score.
    let feature: AcousticFeatureKind
    /// Episode-relative start time of the window the score applies to (seconds).
    let windowStart: Double
    /// Episode-relative end time of the window the score applies to (seconds).
    let windowEnd: Double
    /// Score in the inclusive range `[0, 1]`. Higher = stronger evidence this
    /// window is part of an ad.
    let score: Double
    /// Raw metric the feature computed (LUFS delta, cepstral distance, etc.).
    /// Kept for diagnostics / calibration; fusion should ignore it.
    let rawMetric: Double

    init(
        feature: AcousticFeatureKind,
        windowStart: Double,
        windowEnd: Double,
        score: Double,
        rawMetric: Double
    ) {
        self.feature = feature
        self.windowStart = windowStart
        self.windowEnd = windowEnd
        self.score = max(0, min(1, score))
        self.rawMetric = rawMetric
    }
}

// MARK: - Clamp helper

/// Clamp a Double to `[0, 1]`. Shared utility for feature implementations.
@inlinable
func clampUnit(_ value: Double) -> Double {
    if value.isNaN { return 0 }
    return max(0, min(1, value))
}
