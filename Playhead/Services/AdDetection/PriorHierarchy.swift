// PriorHierarchy.swift
// playhead-ef2.5.3: Prior hierarchy integration — 4-level prior resolution.
//
// Resolves priors for ad detection through a 4-level hierarchy:
//   1. Global defaults — hardcoded baseline for zero-history shows
//   2. Network priors — from NetworkPriors (when network identity exists)
//   3. Trait-derived — computed from ShowTraitProfile (when isReliable)
//   4. Show-local — from the show's own history (wins at >= 5 episodes)
//
// Each level overrides the one above when sufficient data exists.
// Blending uses decay weights so transitions are smooth, not cliff-edges.
//
// GUARDRAIL: These priors affect ranking/prioritization and classifier
// priorScore only. They do NOT grant auto-skip authority and do NOT
// directly modify evidence fusion weights. The fusion layer and skip
// policy matrix remain the sole decision-makers for skip actions.
//
// CURRENT CONSUMPTION (cycle-1 H1, 2026-05-03): only the
// `typicalAdDuration` field on `ResolvedPriors` is consumed in
// production today, via `DurationPrior(resolvedPriors:)`. The other
// scalar fields (`musicBracketTrust`, `metadataTrust`, `fmBudgetBias`,
// `fingerprintTransferConfidence`, `sponsorRecurrenceExpectation`) are
// computed and discarded — their consumers are filed as separate beads.
// The hierarchy structure is still load-bearing: future consumers
// inherit the same global → network → trait → show-local resolution
// without re-implementing the blending math. Don't read this comment
// as gating multiple production knobs today; it gates one (duration)
// and reserves the rest.

import Foundation

// MARK: - PriorLevel

/// The four levels of the prior hierarchy, ordered from weakest to strongest.
enum PriorLevel: Int, Sendable, Equatable, Comparable, CaseIterable {
    case global = 0
    case network = 1
    case traitDerived = 2
    case showLocal = 3

    static func < (lhs: PriorLevel, rhs: PriorLevel) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

// MARK: - GlobalPriorDefaults

/// Level-0 hardcoded baseline priors. Used when no show-specific,
/// trait-derived, or network-level data is available.
struct GlobalPriorDefaults: Sendable, Equatable {
    let musicBracketTrust: Float
    let metadataTrust: Float
    let fmBudgetBias: Float
    let fingerprintTransferConfidence: Float
    let sponsorRecurrenceExpectation: Float
    let typicalAdDuration: ClosedRange<TimeInterval>

    /// The canonical defaults matching the pipeline's conservative baseline.
    static let standard = GlobalPriorDefaults(
        musicBracketTrust: 0.5,
        metadataTrust: 0.5,
        fmBudgetBias: 0.5,
        fingerprintTransferConfidence: 0.5,
        sponsorRecurrenceExpectation: 0.3,
        typicalAdDuration: 30...90
    )
}

// MARK: - ShowLocalPriors

/// Per-show observed priors accumulated from the show's own episode history.
/// All fields are optional — nil means no observation for that dimension.
struct ShowLocalPriors: Sendable, Equatable {
    let musicBracketTrust: Float?
    let metadataTrust: Float?
    let fmBudgetBias: Float?
    let fingerprintTransferConfidence: Float?
    let sponsorRecurrenceExpectation: Float?
    let typicalAdDuration: ClosedRange<TimeInterval>?
    let episodeCount: Int
}

// MARK: - ResolvedPriors

/// The output of prior resolution: a fully-populated set of priors with
/// provenance tracking showing which level determined each value.
///
/// **Field consumption (cycle-1 H1, 2026-05-03):** Today only
/// `typicalAdDuration` is wired into a production knob — `DurationPrior`
/// reads it via `DurationPrior(resolvedPriors:)` inside the backfill fusion
/// loop. The other five scalar fields (`musicBracketTrust`, `metadataTrust`,
/// `fmBudgetBias`, `fingerprintTransferConfidence`,
/// `sponsorRecurrenceExpectation`) are computed by the resolver but
/// currently have no production consumer; they are reserved for future
/// callers (filed as separate beads) and intentionally kept here so the
/// resolver contract doesn't churn when those consumers land. The
/// `activeLevel` and `levelContributions` provenance fields are read by
/// telemetry / debug logging only.
///
/// These priors feed into ranking, prioritization, and classifier priorScore.
/// They do NOT grant auto-skip authority or modify fusion weights directly.
struct ResolvedPriors: Sendable, Equatable {
    /// How much to trust music bracket signals (0-1).
    /// **Reserved for future consumers** — not currently consumed in production.
    let musicBracketTrust: Float
    /// Expected metadata reliability (0-1).
    /// **Reserved for future consumers** — not currently consumed in production.
    let metadataTrust: Float
    /// Bias toward using more/less FM budget (0 = minimal FM, 1 = max FM).
    /// **Reserved for future consumers** — not currently consumed in production.
    let fmBudgetBias: Float
    /// Confidence in fingerprint transfer across episodes (0-1).
    /// **Reserved for future consumers** — not currently consumed in production.
    let fingerprintTransferConfidence: Float
    /// Expected sponsor recurrence rate (0-1).
    /// **Reserved for future consumers** — not currently consumed in production.
    let sponsorRecurrenceExpectation: Float
    /// Expected ad duration range.
    /// **Load-bearing** — read by `DurationPrior(resolvedPriors:)` inside
    /// `AdDetectionService.runBackfill` and folded into the per-span
    /// duration prior used by the DecisionMapper.
    let typicalAdDuration: ClosedRange<TimeInterval>
    /// Which hierarchy level determined these values (the highest active level).
    let activeLevel: PriorLevel
    /// Blend weights showing how much each level contributed (sums to ~1.0).
    let levelContributions: [PriorLevel: Float]
}

// MARK: - PriorHierarchyResolver

/// Stateless resolver that collapses the 4-level prior hierarchy into
/// a single `ResolvedPriors` value.
///
/// Resolution walks the hierarchy bottom-to-top (global -> network ->
/// trait -> show-local), blending each active level into the running
/// result. Higher levels dominate when they have sufficient data.
enum PriorHierarchyResolver {

    /// Minimum episode count before show-local priors override the hierarchy.
    static let showLocalThreshold = 5

    /// Resolve the prior hierarchy into a single set of priors.
    ///
    /// - Parameters:
    ///   - globalDefaults: Level-0 baseline (typically `GlobalPriorDefaults.standard`).
    ///   - networkPriors: Level-1 network-level priors, nil if network unknown.
    ///   - networkDecay: Pre-computed network decay weight (from `NetworkPriors.decayedWeight`).
    ///   - traitProfile: Level-2 show trait profile (checked via `isReliable`).
    ///   - showLocalPriors: Level-3 per-show observations, nil if unavailable.
    ///     The `episodeCount` on `ShowLocalPriors` gates activation (>= 5).
    /// - Returns: Fully resolved priors with provenance tracking.
    static func resolve(
        globalDefaults: GlobalPriorDefaults = .standard,
        networkPriors: NetworkPriors? = nil,
        networkDecay: Float = 0,
        traitProfile: ShowTraitProfile = .unknown,
        showLocalPriors: ShowLocalPriors? = nil
    ) -> ResolvedPriors {
        // Clamp networkDecay to [0, 1] defensively.
        let networkDecay = max(0, min(1, networkDecay))

        // Start with global defaults.
        var musicBracketTrust = globalDefaults.musicBracketTrust
        var metadataTrust = globalDefaults.metadataTrust
        var fmBudgetBias = globalDefaults.fmBudgetBias
        var fingerprintTransferConfidence = globalDefaults.fingerprintTransferConfidence
        var sponsorRecurrenceExpectation = globalDefaults.sponsorRecurrenceExpectation
        var typicalAdDuration = globalDefaults.typicalAdDuration
        var activeLevel = PriorLevel.global
        var contributions: [PriorLevel: Float] = [.global: 1.0]

        // Level 1: Network priors (blend with decay weight).
        if let net = networkPriors, networkDecay > 0 {
            let w = networkDecay
            musicBracketTrust = blend(musicBracketTrust, net.musicBracketPrevalence, weight: w)
            metadataTrust = blend(metadataTrust, net.metadataTrustAverage, weight: w)
            // Network priors don't directly carry fmBudgetBias or fingerprintTransferConfidence,
            // but typicalAdDuration and sponsorRecurrence are available.
            let netSponsorRecurrence: Float = net.commonSponsors.isEmpty ? 0 : min(1.0, Float(net.commonSponsors.count) * 0.15)
            sponsorRecurrenceExpectation = blend(sponsorRecurrenceExpectation, netSponsorRecurrence, weight: w)
            typicalAdDuration = blendRange(typicalAdDuration, net.typicalAdDuration, weight: w)

            activeLevel = .network
            let globalRemaining = 1.0 - w
            contributions = [.global: globalRemaining, .network: w]
        }

        // Level 2: Trait-derived priors (when profile is reliable).
        if traitProfile.isReliable {
            let traitWeight = traitBlendWeight(episodesObserved: traitProfile.episodesObserved)

            let traitMusicBracketTrust = deriveMusicBracketTrust(from: traitProfile)
            let traitMetadataTrust = deriveMetadataTrust(from: traitProfile)
            let traitFmBudgetBias = deriveFmBudgetBias(from: traitProfile)
            let traitFingerprintConfidence = deriveFingerprintConfidence(from: traitProfile)

            musicBracketTrust = blend(musicBracketTrust, traitMusicBracketTrust, weight: traitWeight)
            metadataTrust = blend(metadataTrust, traitMetadataTrust, weight: traitWeight)
            fmBudgetBias = blend(fmBudgetBias, traitFmBudgetBias, weight: traitWeight)
            fingerprintTransferConfidence = blend(fingerprintTransferConfidence, traitFingerprintConfidence, weight: traitWeight)
            sponsorRecurrenceExpectation = blend(sponsorRecurrenceExpectation, traitProfile.sponsorRecurrence, weight: traitWeight)

            activeLevel = .traitDerived
            // Rescale existing contributions and add trait.
            let rescale = 1.0 - traitWeight
            for key in contributions.keys {
                contributions[key] = (contributions[key] ?? 0) * rescale
            }
            contributions[.traitDerived] = traitWeight
        }

        // Level 3: Show-local priors (wins at >= 5 episodes).
        // cycle-1 L1: this gate is now load-bearing for builder-produced
        // priors too. The builder no longer floors `episodeCount`; it
        // passes `PodcastProfile.observationCount` through verbatim.
        // A profile with enough confirmed ad samples (>= 5) but few
        // distinct episodes (e.g. one episode yielding many ads) will
        // be rejected here and fall back to the trait/global blend,
        // which is the right behavior for low-cross-episode-generality
        // shows.
        if let local = showLocalPriors, local.episodeCount >= showLocalThreshold {
            let localWeight = showLocalBlendWeight(episodeCount: local.episodeCount)

            if let v = local.musicBracketTrust {
                musicBracketTrust = blend(musicBracketTrust, v, weight: localWeight)
            }
            if let v = local.metadataTrust {
                metadataTrust = blend(metadataTrust, v, weight: localWeight)
            }
            if let v = local.fmBudgetBias {
                fmBudgetBias = blend(fmBudgetBias, v, weight: localWeight)
            }
            if let v = local.fingerprintTransferConfidence {
                fingerprintTransferConfidence = blend(fingerprintTransferConfidence, v, weight: localWeight)
            }
            if let v = local.sponsorRecurrenceExpectation {
                sponsorRecurrenceExpectation = blend(sponsorRecurrenceExpectation, v, weight: localWeight)
            }
            if let v = local.typicalAdDuration {
                typicalAdDuration = blendRange(typicalAdDuration, v, weight: localWeight)
            }

            activeLevel = .showLocal
            let rescale = 1.0 - localWeight
            for key in contributions.keys {
                contributions[key] = (contributions[key] ?? 0) * rescale
            }
            contributions[.showLocal] = localWeight
        }

        return ResolvedPriors(
            musicBracketTrust: musicBracketTrust,
            metadataTrust: metadataTrust,
            fmBudgetBias: fmBudgetBias,
            fingerprintTransferConfidence: fingerprintTransferConfidence,
            sponsorRecurrenceExpectation: sponsorRecurrenceExpectation,
            typicalAdDuration: typicalAdDuration,
            activeLevel: activeLevel,
            levelContributions: contributions
        )
    }

    // MARK: - Trait-to-prior mappings

    /// musicDensity + structureRegularity -> musicBracketTrust.
    /// Higher both = higher trust in music bracket signals.
    static func deriveMusicBracketTrust(from traits: ShowTraitProfile) -> Float {
        (traits.musicDensity + traits.structureRegularity) / 2.0
    }

    /// structureRegularity -> metadataTrust.
    /// Regular structure implies more trustworthy metadata.
    static func deriveMetadataTrust(from traits: ShowTraitProfile) -> Float {
        traits.structureRegularity
    }

    /// singleSpeakerDominance + low musicDensity -> fmBudgetBias.
    /// Monologue shows with little music benefit more from FM classification.
    static func deriveFmBudgetBias(from traits: ShowTraitProfile) -> Float {
        let lowMusicBonus = max(0, 1.0 - traits.musicDensity)
        return (traits.singleSpeakerDominance + lowMusicBonus) / 2.0
    }

    /// insertionVolatility -> fingerprintTransferConfidence (inverse).
    /// High volatility = low confidence in fingerprint transfer.
    static func deriveFingerprintConfidence(from traits: ShowTraitProfile) -> Float {
        1.0 - traits.insertionVolatility
    }

    // MARK: - Blend weights

    /// Trait blend weight ramps from 0.4 at 3 episodes to 0.6 at 7+.
    static func traitBlendWeight(episodesObserved: Int) -> Float {
        let clamped = Float(min(max(episodesObserved, 3), 7))
        return 0.4 + 0.2 * (clamped - 3.0) / 4.0
    }

    /// Show-local blend weight ramps from 0.6 at 5 episodes to 0.8 at 10+.
    static func showLocalBlendWeight(episodeCount: Int) -> Float {
        let clamped = Float(min(max(episodeCount, 5), 10))
        return 0.6 + 0.2 * (clamped - 5.0) / 5.0
    }

    // MARK: - Private helpers

    /// Linear blend: result = current * (1 - weight) + target * weight.
    private static func blend(_ current: Float, _ target: Float, weight: Float) -> Float {
        current * (1.0 - weight) + target * weight
    }

    /// Blend two ClosedRange<TimeInterval> values by blending their bounds.
    private static func blendRange(
        _ current: ClosedRange<TimeInterval>,
        _ target: ClosedRange<TimeInterval>,
        weight: Float
    ) -> ClosedRange<TimeInterval> {
        let w = TimeInterval(weight)
        let lower = current.lowerBound * (1.0 - w) + target.lowerBound * w
        let upper = current.upperBound * (1.0 - w) + target.upperBound * w
        return lower...max(lower, upper)
    }
}
