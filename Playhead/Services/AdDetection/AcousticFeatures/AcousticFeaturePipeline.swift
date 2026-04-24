// AcousticFeaturePipeline.swift
// playhead-gtt9.12: high-level entry point that runs every acoustic feature
// over a single episode's feature windows and returns the fused per-window
// score plus a funnel snapshot.
//
// The pipeline is explicit about the eight features so adding a ninth is a
// visible one-line change. Keeping the orchestration here (instead of inside
// each feature) means:
//
//   * Features remain self-contained pure functions for gtt9.3 tuning.
//   * Call sites (AdDetectionService hot path, gtt9.9 feature-only scoring,
//     funnel-focused tests) invoke a single method.
//
// TODO(gtt9.12 → production wiring): once gtt9.4's source funnel is wired into
// the fusion pipeline in AdDetectionService, call `AcousticFeaturePipeline.run`
// from the window-scoring loop and hand `pipelineResult.fusion` to the
// evidence ledger as `.acoustic(...)` entries. Done here as a separate bead
// to avoid colliding with gtt9.13's parallel work on AdCatalogStore.
//
// TODO(gtt9.12 follow-up): run the 2026-04-23 real corpus through
// `AcousticFeaturePipeline` offline to verify the ≥ 20% combined-acoustic
// firing rate promised by bead acceptance criterion #2. This subagent's
// charter explicitly defers that eval run.

import Foundation

enum AcousticFeaturePipeline {

    struct Result: Sendable, Equatable {
        /// Fused per-window combined score.
        let fusion: [AcousticFeatureFusion.WindowFusion]
        /// Per-feature funnel snapshot (computed / produced-signal /
        /// passed-gate / included-in-fusion).
        let funnel: AcousticFeatureFunnel
        /// Raw per-feature score arrays (for diagnostics / calibration).
        let perFeatureScores: [AcousticFeatureKind: [AcousticFeatureScore]]
    }

    /// Run every acoustic feature over a single episode's windows.
    ///
    /// - Parameters:
    ///   - windows: Sorted feature windows covering (at minimum) the region
    ///     being scored. Fusion assumes every feature was run over the same
    ///     set of windows.
    ///   - weights: Fusion weights — default priors today, calibrated by
    ///     gtt9.3 later.
    ///   - catalog: Optional catalog supplied by gtt9.13 once it lands. Not
    ///     used by any other feature.
    /// - Returns: Fusion output plus a funnel snapshot.
    static func run(
        windows: [FeatureWindow],
        weights: AcousticFeatureFusion.Weights = .defaultPriors,
        catalog: RepetitionFingerprint.CatalogLookup? = nil
    ) -> Result {
        var funnel = AcousticFeatureFunnel()

        let musicBedScores = MusicBedFeature.scores(for: windows, funnel: &funnel)
        let lufsScores = LufsShift.scores(for: windows, funnel: &funnel)
        let drScores = DynamicRange.scores(for: windows, funnel: &funnel)
        let speakerScores = SpeakerShift.scores(for: windows, funnel: &funnel)
        let spectralScores = SpectralShift.scores(for: windows, funnel: &funnel)
        let silenceScores = SilenceBoundary.scores(for: windows, funnel: &funnel)
        let tempoScores = TempoOnset.scores(for: windows, funnel: &funnel)
        let repetitionScores = RepetitionFingerprint.scores(
            for: windows,
            catalog: catalog,
            funnel: &funnel
        )

        let perFeature: [AcousticFeatureKind: [AcousticFeatureScore]] = [
            .musicBed: musicBedScores,
            .lufsShift: lufsScores,
            .dynamicRange: drScores,
            .speakerShift: speakerScores,
            .spectralShift: spectralScores,
            .silenceBoundary: silenceScores,
            .repetitionFingerprint: repetitionScores,
            .tempoOnset: tempoScores
        ]
        let fusion = AcousticFeatureFusion.combine(
            featureScores: perFeature,
            weights: weights
        )
        return Result(fusion: fusion, funnel: funnel, perFeatureScores: perFeature)
    }
}
