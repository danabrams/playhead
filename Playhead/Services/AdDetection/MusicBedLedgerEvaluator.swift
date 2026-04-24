// MusicBedLedgerEvaluator.swift
// Pure helper that turns a window's MusicBedLevel distribution into an
// `EvidenceLedgerEntry(source: .musicBed)` contribution for the fused
// evidence ledger.
//
// Motivation (2026-04-23 real-data eval, Finding 4):
//   `buildAcousticLedgerEntries` currently only fires on RMS drops at the
//   span boundary. That ignores an orthogonal signal the feature pipeline
//   already computes: `FeatureWindow.musicBedLevel` classifies every window
//   as `.none` / `.background` / `.foreground` based on music-probability
//   and amplitude/spectral gating. Ads are characterised by music beds
//   (production beds under voice, jingles, stingers) that extend across
//   the *interior* of the span, not just its edges.
//
// The evaluator is a pure value-type computation — no actor needed.
// It runs on whichever actor or task calls it.

import Foundation

/// Pure helper that produces `.musicBed`-source ledger entries from a
/// span's feature windows.
///
/// Emits at most one entry per span. The entry is a distinct
/// `EvidenceSourceType.musicBed` kind so it increments
/// `distinctKinds.count` in the quorum gate (see `BackfillEvidenceFusion
/// .quorumGateForFMConsensus`). That is the whole point of threading
/// this signal through: unlocking evidence quorums where today only the
/// classifier fires.
enum MusicBedLedgerEvaluator {

    /// Minimum fraction of the span's windows that must carry a non-`.none`
    /// music bed level for the evaluator to emit an entry. 30% is the
    /// simplest defensible floor — below this we're looking at occasional
    /// spectral noise, not a production bed. Ads routinely run ≥50%
    /// music-bed coverage (classic "music under voice" structure).
    static let minPresenceFraction: Double = 0.30

    /// Minimum number of windows needed in the span to even consider the
    /// signal. Prevents a single `.foreground` window on a 2-window span
    /// from producing a maxed-out entry.
    static let minWindowsRequired: Int = 3

    /// Result of evaluating a span. Exposed for test assertions; the
    /// pipeline only consumes the emitted ledger entries.
    struct Evaluation: Sendable, Equatable {
        let presenceFraction: Double
        let foregroundCount: Int
        let backgroundCount: Int
        let fired: Bool
    }

    /// Evaluate a span's feature windows and return an (optional) ledger
    /// entry plus the diagnostic `Evaluation` struct.
    ///
    /// - Parameters:
    ///   - spanWindows: `FeatureWindow`s that overlap the target span,
    ///     already filtered by the caller.
    ///   - fusionConfig: Source of `acousticCap`; the `.musicBed` kind
    ///     reuses the acoustic weight budget since it's the same
    ///     evidence family (per `SourceEvidenceFamily`).
    /// - Returns: A tuple of the (optional) ledger entry and the
    ///   diagnostic struct. Caller decides whether to append the entry.
    static func evaluate(
        spanWindows: [FeatureWindow],
        fusionConfig: FusionWeightConfig
    ) -> (entry: EvidenceLedgerEntry?, evaluation: Evaluation) {
        // Stub — GREEN phase replaces this with the real computation.
        let evaluation = Evaluation(
            presenceFraction: 0,
            foregroundCount: 0,
            backgroundCount: 0,
            fired: false
        )
        return (nil, evaluation)
    }
}
