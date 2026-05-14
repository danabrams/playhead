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

    // MARK: - playhead-2hpn weights

    /// playhead-2hpn baseline music-bed feature weight applied when the
    /// scoped-music-bed-generalization flag is ON but the span does NOT
    /// overlap a confirmed-jingle region. Identical to the per-feature
    /// `musicBedLevel: 0.10` prior in `AcousticLikelihoodScorer` —
    /// keeping the literal aligned lets future readers grep both at
    /// once.
    static let musicBedBaselineWeight: Double = 0.10

    /// playhead-2hpn confirmed weight applied when the flag is ON AND
    /// the show has reached the 3-episode confirmation threshold AND
    /// the span overlaps a detected jingle region. Bead spec: 0.10 →
    /// 0.25 boost for matching spans.
    ///
    /// COUPLING WARNING: this weight is the SOURCE OF TRUTH for the
    /// `.musicBed` ceiling. `FusionWeightConfig.musicBedCap` MUST be
    /// `>= musicBedConfirmedJingleWeight` or the boost is silently
    /// truncated by `BackfillEvidenceFusion.buildLedger()` (the bug
    /// fixed in R2). If you raise this constant, you MUST also raise
    /// `FusionWeightConfig.musicBedCap` to match. The invariant is
    /// enforced at runtime by:
    ///   * the `precondition` in `FusionWeightConfig.init` (R4→R5,
    ///     always-on in both debug and release), and
    ///   * `MusicBedLedgerEvaluatorJingleBoostTests.musicBedCapAccommodatesBoostWeight`
    ///     for the default-init path.
    ///
    /// R7 cross-ref: the asymmetric-coupling rationale (why ONLY
    /// `.musicBed` carries this coupling check while every other
    /// source cap does not) is documented inline above the
    /// `precondition` in `FusionWeightConfig.init`. If you introduce
    /// a similar fixed-emit constant for another source kind (i.e.
    /// the producer emits a weight set independently of its cap),
    /// add the analogous coupling check there too.
    static let musicBedConfirmedJingleWeight: Double = 0.25

    /// Result of evaluating a span. Exposed for test assertions; the
    /// pipeline only consumes the emitted ledger entries.
    struct Evaluation: Sendable, Equatable {
        let presenceFraction: Double
        let foregroundCount: Int
        let backgroundCount: Int
        let fired: Bool
    }

    /// playhead-2hpn boost context passed to `evaluate(...)` when the
    /// `scopedMusicBedGeneralization` feature flag is ON.
    ///
    /// When `nil` (flag OFF), the evaluator runs its legacy
    /// `presenceFraction * acousticCap` weight path — byte-identical to
    /// pre-2hpn behavior. When non-nil, the evaluator switches to the
    /// flag-on weighting:
    ///   * `isConfirmed && spanOverlapsJingle` → `musicBedConfirmedJingleWeight` (0.25)
    ///   * otherwise → `musicBedBaselineWeight` (0.10)
    ///
    /// Cross-show isolation: it is the CALLER's responsibility to look
    /// up the right show's profile and pass `isConfirmed` derived from
    /// THAT show's snapshot. The evaluator does no lookups itself.
    struct JingleBoost: Sendable, Equatable {
        /// True when the show has reached the 3-episode confirmation
        /// threshold AND still holds at least one stored hash.
        let isConfirmed: Bool

        /// True when the span overlaps a region of the episode where
        /// the show's confirmed jingle has been detected. For intro/
        /// outro jingles this means the span's `[startTime, endTime)`
        /// overlaps `[0, jingleSliceSeconds)` or
        /// `[episodeDuration - jingleSliceSeconds, episodeDuration)`.
        let spanOverlapsJingle: Bool
    }

    /// Evaluate a span's feature windows and return an (optional) ledger
    /// entry plus the diagnostic `Evaluation` struct.
    ///
    /// Trigger rule:
    ///   Fire when the span contains ≥`minWindowsRequired` windows AND
    ///   at least `minPresenceFraction` of them carry a non-`.none`
    ///   music bed level. Weight depends on the `jingleBoost` parameter:
    ///     * `jingleBoost == nil` (flag OFF) — legacy path: weight
    ///       scales linearly with presence fraction and is clamped to
    ///       `fusionConfig.acousticCap`. A half-bed span produces a
    ///       half-weight contribution.
    ///     * `jingleBoost != nil` (flag ON) — fixed-weight path:
    ///       `musicBedConfirmedJingleWeight` (0.25) when both
    ///       `isConfirmed` and `spanOverlapsJingle` are true,
    ///       `musicBedBaselineWeight` (0.10) otherwise. Presence
    ///       fraction still gates firing (≥ `minPresenceFraction`)
    ///       but does NOT scale the emitted weight — the boost is a
    ///       categorical promotion, not a multiplier.
    ///
    /// The rule is intentionally orthogonal to
    /// `buildAcousticLedgerEntries`'s boundary-only RMS-drop detector:
    /// this one keys on *interior* music coverage, so a clean dry
    /// ad-read with no RMS transition at the edges (because the
    /// surrounding content also has speech-like RMS) still produces a
    /// music-bed signal if there's a bed under the copy.
    ///
    /// - Parameters:
    ///   - spanWindows: `FeatureWindow`s that overlap the target span,
    ///     already filtered by the caller.
    ///   - fusionConfig: Source of `acousticCap` (legacy path) and
    ///     `musicBedCap` (downstream clamp in `BackfillEvidenceFusion`).
    ///     The 0.25 boosted weight is admitted by `musicBedCap`
    ///     specifically — see `FusionWeightConfig.musicBedCap` for the
    ///     coupling invariant.
    ///   - jingleBoost: When non-nil, switches the evaluator to the
    ///     fixed-weight (0.10/0.25) flag-ON path. When nil, the legacy
    ///     `presenceFraction * acousticCap` path runs unchanged.
    /// - Returns: A tuple of the (optional) ledger entry and the
    ///   diagnostic struct. Caller decides whether to append the entry.
    static func evaluate(
        spanWindows: [FeatureWindow],
        fusionConfig: FusionWeightConfig,
        jingleBoost: JingleBoost? = nil
    ) -> (entry: EvidenceLedgerEntry?, evaluation: Evaluation) {
        let total = spanWindows.count
        guard total > 0 else {
            let evaluation = Evaluation(
                presenceFraction: 0,
                foregroundCount: 0,
                backgroundCount: 0,
                fired: false
            )
            return (nil, evaluation)
        }

        var foreground = 0
        var background = 0
        for fw in spanWindows {
            switch fw.musicBedLevel {
            case .foreground: foreground += 1
            case .background: background += 1
            case .none: break
            }
        }
        let musicWindows = foreground + background
        let presenceFraction = Double(musicWindows) / Double(total)

        let fired = total >= minWindowsRequired && presenceFraction >= minPresenceFraction

        let evaluation = Evaluation(
            presenceFraction: presenceFraction,
            foregroundCount: foreground,
            backgroundCount: background,
            fired: fired
        )

        guard fired else { return (nil, evaluation) }

        // playhead-2hpn: when the scoped-music-bed-generalization flag
        // is ON the caller passes a `JingleBoost` and we switch to
        // fixed weights (0.10 baseline / 0.25 confirmed-jingle-overlap).
        // When `jingleBoost` is nil (flag OFF) we keep the legacy
        // presenceFraction-scaled path — byte-identical to pre-2hpn.
        let weight: Double
        if let boost = jingleBoost {
            weight = (boost.isConfirmed && boost.spanOverlapsJingle)
                ? musicBedConfirmedJingleWeight
                : musicBedBaselineWeight
        } else {
            // Legacy path: scales linearly with coverage, capped at
            // `acousticCap` since `.musicBed` is an acoustic-family peer.
            let rawWeight = presenceFraction * fusionConfig.acousticCap
            weight = min(rawWeight, fusionConfig.acousticCap)
        }

        let entry = EvidenceLedgerEntry(
            source: .musicBed,
            weight: weight,
            detail: .musicBed(
                presenceFraction: presenceFraction,
                foregroundCount: foreground
            )
        )
        return (entry, evaluation)
    }
}
