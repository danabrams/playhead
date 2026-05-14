// ShowCapabilityBudgetModulator.swift
// playhead-h6a6: pure-math layer that translates a
// `ShowCapabilityProfileKind` into a budget adjustment the
// per-show analysis path can apply.
//
// Two outputs:
//   1. `analysisBudgetMultiplier` — a scalar in
//      `[minBudgetFloorRatio, 1.0]`. The runtime caller multiplies its
//      baseline per-episode budget by this scalar, then clamps to a
//      hard floor so a single per-episode minimum can never be zeroed
//      out. The bead spec: profile-guided analysis reduces compute by
//      ≥ 15% on profile-matched shows vs baseline, AND the always-on
//      minimum per-episode budget is preserved.
//
//   2. `detectorBiases` — per-detector relative weight in [0.5, 1.5].
//      Different profiles bias different detectors in the fusion
//      ensemble. The map is consumed by future per-detector knobs;
//      today it is exposed as a value the test gate inspects to
//      assert the contract "different profiles bias different
//      detectors". The map's shape is part of the public contract.
//
// Safety rails:
//   * `.unknown` profile → multiplier 1.0, biases all-1.0. The flag-off
//     contract is implemented by the CALLER passing `.unknown` (which
//     is what the evaluator returns when the flag is off / the floor
//     isn't met / SLIs are out of bounds).
//   * Floor enforcement is here so callers cannot accidentally
//     undercut the minimum: the multiplier is clamped to
//     `[minBudgetFloorRatio, 1.0]` AFTER per-profile reduction.

import Foundation

// MARK: - Detector axis

/// Coarse-grained taxonomy of the fusion-ensemble detectors whose
/// relative weights the profile can bias.
///
/// Intentionally NOT a 1:1 mirror of every internal detector — the
/// budget modulator is a high-level steering input, not a precise
/// knob on each subsystem. The bias map's CONSUMER is a future bead
/// (the per-detector weight wiring lives in BackfillEvidenceFusion);
/// this bead lands the value type + the per-profile defaults that
/// the consumer will read.
enum ShowCapabilityDetector: String, Sendable, Hashable, CaseIterable {

    /// Chapter-evidence builder (`ChapterMetadataEvidenceBuilder`,
    /// `ChapterEvidenceParser`). Most useful on chapter-rich shows.
    case chapter

    /// Lexical / FM sponsor scan (`LexicalScanner`, `FoundationModelClassifier`).
    /// Most useful on host-read-only shows where ads are voiced like
    /// the rest of the episode.
    case lexicalSponsor

    /// Music-bed boost path (`MusicBedLedgerEvaluator` + 2hpn's
    /// `ShowMusicBedProfileStore`). Most useful on music-bed-reliable
    /// shows.
    case musicBed

    /// RSS / show-notes pre-seed (`FeedDescriptionEvidenceBuilder`).
    /// Most useful on sponsor-declared shows.
    case sponsorPreSeed

    /// Boundary refinement (`BoundaryRefiner`, `BracketAwareBoundaryRefiner`,
    /// `FineBoundaryRefiner`). Most useful on dynamic-insertion-heavy
    /// shows where boundary cues shift episode-to-episode.
    case boundaryRefinement
}

// MARK: - Adjustment value type

/// One observed-profile's per-show budget recommendation.
///
/// Returned by `ShowCapabilityBudgetModulator.adjustment(for:)` so the
/// caller can:
///   * Multiply baseline per-episode budget by `analysisBudgetMultiplier`.
///   * Floor the result at the always-on minimum (the caller owns the
///     minimum because it depends on episode duration / device class).
///   * Bias per-detector activation by `detectorBiases[<detector>] ?? 1.0`.
struct ShowCapabilityBudgetAdjustment: Sendable, Equatable {

    /// The profile kind this adjustment was derived from. Carried for
    /// observability — the modulator is deterministic in `kind`.
    let kind: ShowCapabilityProfileKind

    /// Multiplier applied to the per-episode analysis budget. Range
    /// `[minBudgetFloorRatio, 1.0]`. `.unknown` always yields 1.0
    /// (no modulation).
    let analysisBudgetMultiplier: Double

    /// Per-detector relative weight in `[detectorBiasFloor,
    /// detectorBiasCeiling]`. Missing keys imply 1.0 (no bias).
    let detectorBiases: [ShowCapabilityDetector: Double]

    /// The bias for `detector`. Convenience for the consumer that
    /// would otherwise write `adj.detectorBiases[d] ?? 1.0`.
    func bias(for detector: ShowCapabilityDetector) -> Double {
        detectorBiases[detector] ?? 1.0
    }
}

// MARK: - Modulator

enum ShowCapabilityBudgetModulator {

    // MARK: Tunables (defended)

    /// Floor on `analysisBudgetMultiplier`. The bead-spec contract:
    /// "always-on minimum budget preserved — never zero out". The
    /// floor is enforced HERE so a regression in the per-profile
    /// reduction tables can't accidentally produce 0 (or negative)
    /// multipliers. The caller is expected to additionally clamp
    /// against a per-episode absolute minimum budget.
    static let minBudgetFloorRatio: Double = 0.50

    /// Per-detector bias band. Bias < 1.0 means "spend less on this
    /// detector for this profile"; > 1.0 means "lean on this detector
    /// harder". The band is intentionally narrow so a single profile
    /// cannot move detector weight by more than ±50% — the modulator
    /// is a steering input, not a kill switch.
    static let detectorBiasFloor: Double = 0.50
    static let detectorBiasCeiling: Double = 1.50

    /// Per-profile baseline multiplier (BEFORE the safety-floor
    /// clamp). The values target the bead's "≥ 15% compute reduction
    /// on profile-matched shows" gate while leaving comfortable
    /// headroom for the floor clamp.
    ///
    /// `.unknown` → 1.0 (NO modulation, byte-identical to pre-h6a6).
    private static func rawMultiplier(for kind: ShowCapabilityProfileKind) -> Double {
        switch kind {
        case .unknown:               return 1.00
        case .chapterRich:           return 0.70 // publisher chapters cover most boundaries
        case .hostReadOnly:          return 0.80 // lexical scan finds host-voiced ads cheaply
        case .musicBedReliable:      return 0.75 // 2hpn jingle confirms intro/outro
        case .sponsorDeclared:       return 0.80 // pre-seed narrows the search
        case .dynamicInsertionHeavy: return 0.85 // still need full search, but skip redundant priors
        }
    }

    /// Per-profile detector-bias map. Encodes the contract "different
    /// profiles bias different detectors in the fusion ensemble".
    /// Tested in `ShowCapabilityBudgetModulatorTests.detectorBiasesVaryByKind`.
    private static func rawDetectorBiases(
        for kind: ShowCapabilityProfileKind
    ) -> [ShowCapabilityDetector: Double] {
        switch kind {
        case .unknown:
            // No modulation: every detector at 1.0 (no-op).
            return [:]
        case .chapterRich:
            // Lean hard on chapters; spend less on boundary refinement
            // (chapters already give us boundaries).
            return [
                .chapter: 1.40,
                .boundaryRefinement: 0.70,
            ]
        case .hostReadOnly:
            // Lean on lexical/FM; spend less on the music-bed boost
            // (host reads don't have intro jingles by definition).
            return [
                .lexicalSponsor: 1.30,
                .musicBed: 0.70,
            ]
        case .musicBedReliable:
            // Lean on the music-bed boost; spend less on chapter
            // evidence (jingle shows often lack publisher chapters).
            return [
                .musicBed: 1.40,
                .chapter: 0.80,
            ]
        case .sponsorDeclared:
            // Lean on the RSS pre-seed; spend less on boundary
            // refinement (declared sponsors are already named cues).
            return [
                .sponsorPreSeed: 1.40,
                .boundaryRefinement: 0.80,
            ]
        case .dynamicInsertionHeavy:
            // Lean on boundary refinement; spend less on chapter
            // evidence (shifting boundaries don't align with static
            // chapter marks).
            return [
                .boundaryRefinement: 1.30,
                .chapter: 0.70,
            ]
        }
    }

    /// Returns the budget adjustment for `kind`. `.unknown` is a
    /// no-op (multiplier 1.0, empty bias map). All other kinds yield
    /// a multiplier clamped to `[minBudgetFloorRatio, 1.0]` and a
    /// bias map clamped to `[detectorBiasFloor, detectorBiasCeiling]`.
    static func adjustment(for kind: ShowCapabilityProfileKind) -> ShowCapabilityBudgetAdjustment {
        let raw = rawMultiplier(for: kind)
        let clampedMultiplier = min(max(raw, minBudgetFloorRatio), 1.0)
        let rawBiases = rawDetectorBiases(for: kind)
        let clampedBiases = rawBiases.mapValues {
            min(max($0, detectorBiasFloor), detectorBiasCeiling)
        }
        return ShowCapabilityBudgetAdjustment(
            kind: kind,
            analysisBudgetMultiplier: clampedMultiplier,
            detectorBiases: clampedBiases
        )
    }

    /// Apply the adjustment to a caller-supplied baseline budget,
    /// then floor the result at `minimumPerEpisodeBudget`. This
    /// helper exists so every consumer routes through one floor
    /// check — a regression that adds a new caller bypassing the
    /// floor would have to bypass this helper too.
    ///
    /// - Parameters:
    ///   - baseline: the un-modulated per-episode budget (any unit;
    ///     the caller's domain).
    ///   - adjustment: the profile-derived adjustment.
    ///   - minimumPerEpisodeBudget: the always-on minimum the
    ///     caller must never drop below. Caller owns this value
    ///     (units must match `baseline`).
    /// - Returns: `max(baseline * adjustment.multiplier,
    ///   minimumPerEpisodeBudget)`. The result is ≥
    ///   `minimumPerEpisodeBudget` regardless of the adjustment.
    static func applyAdjustment(
        baseline: Double,
        adjustment: ShowCapabilityBudgetAdjustment,
        minimumPerEpisodeBudget: Double
    ) -> Double {
        let scaled = baseline * adjustment.analysisBudgetMultiplier
        return max(scaled, minimumPerEpisodeBudget)
    }
}
