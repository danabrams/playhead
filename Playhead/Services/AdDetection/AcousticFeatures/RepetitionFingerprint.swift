// RepetitionFingerprint.swift
// playhead-gtt9.12: Repetition-fingerprint feature — STUB pending gtt9.13.
//
// Why it helps: the same ad creative recurs across episodes in a feed. Once
// gtt9.13 lands `AcousticFingerprint` + `AdCatalogStore`, this feature will
// hash each window and query the catalog for known ad fingerprints. A hit
// (or near-hit) is extremely strong evidence — a classic matched-filter.
//
// State today: gtt9.13 is running in parallel but has not yet pushed the
// shared fingerprint / catalog types. This file:
//
//   * Keeps the feature wired into the funnel (so callers can still iterate
//     over all 8 features) — every window records `.computed` with zero
//     score and `producedSignal = false`.
//   * Carries a single TODO so it's trivial to flip on after the merge.
//
// NO cross-branch file creation: per gtt9.12's coordination directive we do
// NOT create `AdCatalogStore.swift` or `AcousticFingerprint.swift` here;
// those belong to gtt9.13.

import Foundation

enum RepetitionFingerprint {

    struct Config: Sendable, Equatable {
        /// Fusion gate threshold — unused while the feature is stubbed.
        let gateScore: Double

        static let `default` = Config(gateScore: 0.40)
    }

    /// Protocol the feature will consume after gtt9.13 lands. Declared here
    /// as a local empty protocol so the integration code can be written
    /// ahead of time without importing a type that does not yet exist.
    ///
    /// TODO(gtt9.12 → gtt9.13): replace with the real `AdCatalogStore`
    /// type once it merges from `bead/playhead-gtt9.13`. The function
    /// signature then becomes:
    ///
    ///     scores(for: windows, catalog: catalog, ...)
    ///
    /// and each window's score = match similarity against the catalog.
    protocol CatalogLookup: Sendable {}

    /// No-op stub producing zeroed scores but still recording the compute
    /// event so the funnel reports "computed: N, producedSignal: 0" rather
    /// than silently dropping the feature.
    static func scores(
        for windows: [FeatureWindow],
        catalog: CatalogLookup? = nil,
        config: Config = .default,
        funnel: inout AcousticFeatureFunnel
    ) -> [AcousticFeatureScore] {
        guard !windows.isEmpty else { return [] }

        // TODO(gtt9.12 → gtt9.13): once `AcousticFingerprint` / `AdCatalogStore`
        // from gtt9.13 land, replace this loop with a real catalog lookup.
        // Until then the feature reports compute events so funnel telemetry
        // surfaces it as "inactive, awaiting catalog".
        var out: [AcousticFeatureScore] = []
        out.reserveCapacity(windows.count)
        for window in windows {
            funnel.record(
                feature: .repetitionFingerprint,
                producedSignal: false,
                passedGate: false,
                includedInFusion: false
            )
            out.append(AcousticFeatureScore(
                feature: .repetitionFingerprint,
                windowStart: window.startTime,
                windowEnd: window.endTime,
                score: 0,
                rawMetric: 0
            ))
        }
        return out
    }
}
