// PriorHierarchyTests.swift
// Tests for PriorHierarchy: 4-level prior resolution.

import Foundation
import Testing
@testable import Playhead

@Suite("PriorHierarchy")
struct PriorHierarchyTests {

    // MARK: - Helpers

    private let defaults = GlobalPriorDefaults.standard

    private func makeTraitProfile(
        musicDensity: Float = 0.5,
        speakerTurnRate: Float = 4.0,
        singleSpeakerDominance: Float = 0.5,
        structureRegularity: Float = 0.5,
        sponsorRecurrence: Float = 0.3,
        insertionVolatility: Float = 0.4,
        transcriptReliability: Float = 0.8,
        episodesObserved: Int = 5
    ) -> ShowTraitProfile {
        // Build a reliable profile by updating from unknown N times.
        var profile = ShowTraitProfile.unknown
        for _ in 0..<episodesObserved {
            // First update replaces sentinel; subsequent use EMA.
            // For testing we just need episodesObserved to match, so we
            // create a snapshot with the target values. After enough updates
            // the EMA converges toward the snapshot values.
            profile = profile.updated(from: EpisodeTraitSnapshot(
                musicDensity: musicDensity,
                speakerTurnRate: speakerTurnRate,
                singleSpeakerDominance: singleSpeakerDominance,
                structureRegularity: structureRegularity,
                sponsorRecurrence: sponsorRecurrence,
                insertionVolatility: insertionVolatility,
                transcriptReliability: transcriptReliability
            ))
        }
        return profile
    }

    private func makeNetworkPriors(
        musicBracketPrevalence: Float = 0.7,
        metadataTrustAverage: Float = 0.6,
        typicalAdDuration: ClosedRange<TimeInterval> = 20...60,
        commonSponsors: [String: Float] = ["acme": 0.8, "betacorp": 0.5]
    ) -> NetworkPriors {
        NetworkPriors(
            commonSponsors: commonSponsors,
            typicalSlotPositions: [0.1, 0.5, 0.9],
            typicalAdDuration: typicalAdDuration,
            musicBracketPrevalence: musicBracketPrevalence,
            metadataTrustAverage: metadataTrustAverage,
            showCount: 5
        )
    }

    private func makeShowLocalPriors(
        musicBracketTrust: Float? = 0.9,
        metadataTrust: Float? = 0.8,
        fmBudgetBias: Float? = 0.3,
        fingerprintTransferConfidence: Float? = 0.85,
        sponsorRecurrenceExpectation: Float? = 0.7,
        typicalAdDuration: ClosedRange<TimeInterval>? = 25...55,
        episodeCount: Int = 10
    ) -> ShowLocalPriors {
        ShowLocalPriors(
            musicBracketTrust: musicBracketTrust,
            metadataTrust: metadataTrust,
            fmBudgetBias: fmBudgetBias,
            fingerprintTransferConfidence: fingerprintTransferConfidence,
            sponsorRecurrenceExpectation: sponsorRecurrenceExpectation,
            typicalAdDuration: typicalAdDuration,
            episodeCount: episodeCount
        )
    }

    // MARK: - PriorLevel enum

    @Test("PriorLevel has 4 cases in correct order")
    func priorLevelOrdering() {
        #expect(PriorLevel.global < .network)
        #expect(PriorLevel.network < .traitDerived)
        #expect(PriorLevel.traitDerived < .showLocal)
        #expect(PriorLevel.allCases.count == 4)
    }

    // MARK: - Global defaults (level 0 in isolation)

    @Test("global defaults produce expected values with no other levels")
    func globalOnly() {
        let result = PriorHierarchyResolver.resolve()

        #expect(result.musicBracketTrust == 0.5)
        #expect(result.metadataTrust == 0.5)
        #expect(result.fmBudgetBias == 0.5)
        #expect(result.fingerprintTransferConfidence == 0.5)
        #expect(result.sponsorRecurrenceExpectation == 0.3)
        #expect(result.typicalAdDuration == 30...90)
        #expect(result.activeLevel == .global)
        #expect(result.levelContributions[.global] == 1.0)
    }

    @Test("custom global defaults are respected")
    func customGlobals() {
        let custom = GlobalPriorDefaults(
            musicBracketTrust: 0.7,
            metadataTrust: 0.3,
            fmBudgetBias: 0.8,
            fingerprintTransferConfidence: 0.2,
            sponsorRecurrenceExpectation: 0.1,
            typicalAdDuration: 15...45
        )
        let result = PriorHierarchyResolver.resolve(globalDefaults: custom)

        #expect(result.musicBracketTrust == 0.7)
        #expect(result.metadataTrust == 0.3)
        #expect(result.typicalAdDuration == 15...45)
        #expect(result.activeLevel == .global)
    }

    // MARK: - Network priors (level 1 in isolation)

    @Test("network priors blend with global when decay > 0")
    func networkBlend() {
        let net = makeNetworkPriors()
        let decay: Float = 0.4

        let result = PriorHierarchyResolver.resolve(
            networkPriors: net,
            networkDecay: decay
        )

        #expect(result.activeLevel == .network)
        // musicBracketTrust: blend(0.5, 0.7, weight: 0.4) = 0.5*0.6 + 0.7*0.4 = 0.58
        #expect(abs(result.musicBracketTrust - 0.58) < 0.001)
        // metadataTrust: blend(0.5, 0.6, weight: 0.4) = 0.5*0.6 + 0.6*0.4 = 0.54
        #expect(abs(result.metadataTrust - 0.54) < 0.001)
        #expect(result.levelContributions[.network] == decay)
        #expect(abs((result.levelContributions[.global] ?? 0) - 0.6) < 0.001)
    }

    @Test("network priors with zero decay have no effect")
    func networkZeroDecay() {
        let net = makeNetworkPriors()
        let result = PriorHierarchyResolver.resolve(
            networkPriors: net,
            networkDecay: 0
        )
        #expect(result.activeLevel == .global)
        #expect(result.musicBracketTrust == 0.5)
    }

    @Test("nil network priors leave global intact")
    func networkNil() {
        let result = PriorHierarchyResolver.resolve(
            networkPriors: nil,
            networkDecay: 0.5
        )
        #expect(result.activeLevel == .global)
    }

    @Test("network decay formula matches NetworkPriors.decayedWeight")
    func networkDecayConsistency() {
        // 0 episodes -> 0.5
        #expect(NetworkPriors.decayedWeight(episodesObserved: 0) == 0.5)
        // 5 episodes -> 0.25
        #expect(NetworkPriors.decayedWeight(episodesObserved: 5) == 0.25)
        // 10 episodes -> 0
        #expect(NetworkPriors.decayedWeight(episodesObserved: 10) == 0)
        // 15 episodes -> 0 (clamped)
        #expect(NetworkPriors.decayedWeight(episodesObserved: 15) == 0)
    }

    // MARK: - Trait-derived priors (level 2 in isolation)

    @Test("trait-derived priors activate when profile is reliable")
    func traitDerivedActivates() {
        let traits = makeTraitProfile(episodesObserved: 3) // minimum for isReliable
        #expect(traits.isReliable)

        let result = PriorHierarchyResolver.resolve(traitProfile: traits)

        #expect(result.activeLevel == .traitDerived)
        #expect(result.levelContributions[.traitDerived] != nil)
        #expect((result.levelContributions[.traitDerived] ?? 0) > 0)
    }

    @Test("trait-derived priors do NOT activate when profile is unreliable")
    func traitDerivedUnreliable() {
        let traits = makeTraitProfile(episodesObserved: 2) // below threshold
        #expect(!traits.isReliable)

        let result = PriorHierarchyResolver.resolve(traitProfile: traits)

        #expect(result.activeLevel == .global)
        #expect(result.levelContributions[.traitDerived] == nil)
    }

    @Test("trait blend weight ramps from 0.4 to 0.6")
    func traitBlendWeightRamp() {
        #expect(PriorHierarchyResolver.traitBlendWeight(episodesObserved: 3) == 0.4)
        #expect(abs(PriorHierarchyResolver.traitBlendWeight(episodesObserved: 5) - 0.5) < 0.001)
        #expect(abs(PriorHierarchyResolver.traitBlendWeight(episodesObserved: 7) - 0.6) < 0.001)
        // Clamped above 7.
        #expect(abs(PriorHierarchyResolver.traitBlendWeight(episodesObserved: 20) - 0.6) < 0.001)
    }

    // MARK: - Trait-to-prior mappings

    @Test("musicDensity + structureRegularity -> musicBracketTrust")
    func traitMappingMusicBracket() {
        // High music density + high regularity -> high music bracket trust.
        let highBoth = makeTraitProfile(musicDensity: 0.9, structureRegularity: 0.8, episodesObserved: 5)
        let trust = PriorHierarchyResolver.deriveMusicBracketTrust(from: highBoth)
        // Average of converged values (after 5 EMA updates from 0.5 start).
        // The exact values depend on EMA convergence, but should be > 0.5.
        #expect(trust > 0.5)

        // Low both -> low trust.
        let lowBoth = makeTraitProfile(musicDensity: 0.1, structureRegularity: 0.2, episodesObserved: 5)
        let lowTrust = PriorHierarchyResolver.deriveMusicBracketTrust(from: lowBoth)
        #expect(lowTrust < 0.5)
    }

    @Test("structureRegularity -> metadataTrust")
    func traitMappingMetadata() {
        let highReg = makeTraitProfile(structureRegularity: 0.9, episodesObserved: 5)
        let trust = PriorHierarchyResolver.deriveMetadataTrust(from: highReg)
        #expect(trust > 0.5)

        let lowReg = makeTraitProfile(structureRegularity: 0.1, episodesObserved: 5)
        let lowTrust = PriorHierarchyResolver.deriveMetadataTrust(from: lowReg)
        #expect(lowTrust < 0.5)
    }

    @Test("singleSpeakerDominance + low musicDensity -> fmBudgetBias")
    func traitMappingFmBudget() {
        // Monologue show with no music -> high FM budget.
        let monologue = makeTraitProfile(
            musicDensity: 0.1, singleSpeakerDominance: 0.9, episodesObserved: 5
        )
        let bias = PriorHierarchyResolver.deriveFmBudgetBias(from: monologue)
        #expect(bias > 0.5)

        // Multi-speaker, music-heavy -> low FM budget.
        let musicShow = makeTraitProfile(
            musicDensity: 0.9, singleSpeakerDominance: 0.2, episodesObserved: 5
        )
        let lowBias = PriorHierarchyResolver.deriveFmBudgetBias(from: musicShow)
        #expect(lowBias < bias)
    }

    @Test("insertionVolatility inversely maps to fingerprintTransferConfidence")
    func traitMappingFingerprint() {
        let highVol = makeTraitProfile(insertionVolatility: 0.9, episodesObserved: 5)
        let conf = PriorHierarchyResolver.deriveFingerprintConfidence(from: highVol)
        // High volatility -> low confidence (inverse).
        #expect(conf < 0.5)

        let lowVol = makeTraitProfile(insertionVolatility: 0.1, episodesObserved: 5)
        let highConf = PriorHierarchyResolver.deriveFingerprintConfidence(from: lowVol)
        #expect(highConf > 0.5)
    }

    // MARK: - Show-local priors (level 3 in isolation)

    @Test("show-local priors win at >= 5 episodes")
    func showLocalActivates() {
        let local = makeShowLocalPriors(episodeCount: 10)
        let result = PriorHierarchyResolver.resolve(
            showLocalPriors: local
        )

        #expect(result.activeLevel == .showLocal)
        #expect((result.levelContributions[.showLocal] ?? 0) > 0)
    }

    @Test("show-local priors do NOT activate below 5 episodes")
    func showLocalBelowThreshold() {
        let local = makeShowLocalPriors(episodeCount: 4)
        let result = PriorHierarchyResolver.resolve(
            showLocalPriors: local
        )

        #expect(result.activeLevel == .global)
        #expect(result.levelContributions[.showLocal] == nil)
    }

    @Test("show-local nil fields leave prior values from lower levels")
    func showLocalPartialOverride() {
        let partial = ShowLocalPriors(
            musicBracketTrust: 0.95,
            metadataTrust: nil,
            fmBudgetBias: nil,
            fingerprintTransferConfidence: nil,
            sponsorRecurrenceExpectation: nil,
            typicalAdDuration: nil,
            episodeCount: 10
        )
        let result = PriorHierarchyResolver.resolve(
            showLocalPriors: partial
        )

        #expect(result.activeLevel == .showLocal)
        // musicBracketTrust should be shifted toward 0.95.
        #expect(result.musicBracketTrust > 0.5)
        // metadataTrust should remain at global default.
        #expect(result.metadataTrust == 0.5)
    }

    @Test("show-local blend weight ramps from 0.6 to 0.8")
    func showLocalBlendWeightRamp() {
        #expect(PriorHierarchyResolver.showLocalBlendWeight(episodeCount: 5) == 0.6)
        #expect(abs(PriorHierarchyResolver.showLocalBlendWeight(episodeCount: 10) - 0.8) < 0.001)
        // Clamped above 10.
        #expect(abs(PriorHierarchyResolver.showLocalBlendWeight(episodeCount: 20) - 0.8) < 0.001)
    }

    // MARK: - Multi-level blending

    @Test("all four levels active produces showLocal as activeLevel")
    func allLevelsActive() {
        let net = makeNetworkPriors()
        let decay: Float = 0.3
        let traits = makeTraitProfile(episodesObserved: 5)
        let local = makeShowLocalPriors(episodeCount: 10)

        let result = PriorHierarchyResolver.resolve(
            networkPriors: net,
            networkDecay: decay,
            traitProfile: traits,
            showLocalPriors: local
        )

        #expect(result.activeLevel == .showLocal)
        // All four levels should have nonzero contributions.
        #expect((result.levelContributions[.global] ?? 0) > 0)
        #expect((result.levelContributions[.network] ?? 0) > 0)
        #expect((result.levelContributions[.traitDerived] ?? 0) > 0)
        #expect((result.levelContributions[.showLocal] ?? 0) > 0)
    }

    @Test("level contributions sum to approximately 1.0")
    func contributionsSum() {
        let net = makeNetworkPriors()
        let traits = makeTraitProfile(episodesObserved: 5)
        let local = makeShowLocalPriors(episodeCount: 8)

        let result = PriorHierarchyResolver.resolve(
            networkPriors: net,
            networkDecay: 0.3,
            traitProfile: traits,
            showLocalPriors: local
        )

        let total = result.levelContributions.values.reduce(0, +)
        #expect(abs(total - 1.0) < 0.01)
    }

    @Test("network + trait blend (no show-local) produces traitDerived activeLevel")
    func networkPlusTrait() {
        let net = makeNetworkPriors()
        let traits = makeTraitProfile(episodesObserved: 4)

        let result = PriorHierarchyResolver.resolve(
            networkPriors: net,
            networkDecay: 0.3,
            traitProfile: traits
        )

        #expect(result.activeLevel == .traitDerived)
        #expect(result.levelContributions[.showLocal] == nil)
    }

    // MARK: - Edge cases

    @Test("unknown trait profile does not activate trait level")
    func unknownTraits() {
        let result = PriorHierarchyResolver.resolve(
            traitProfile: .unknown
        )
        #expect(result.activeLevel == .global)
        #expect(result.levelContributions[.traitDerived] == nil)
    }

    @Test("empty network priors (no sponsors) still blend other fields")
    func emptyNetworkSponsors() {
        let net = NetworkPriors(
            commonSponsors: [:],
            typicalSlotPositions: [],
            typicalAdDuration: 30...90,
            musicBracketPrevalence: 0.8,
            metadataTrustAverage: 0.7,
            showCount: 1
        )
        let result = PriorHierarchyResolver.resolve(
            networkPriors: net,
            networkDecay: 0.4
        )
        #expect(result.activeLevel == .network)
        // musicBracketTrust should be shifted toward 0.8.
        #expect(result.musicBracketTrust > 0.5)
    }

    @Test("zero episodes with all levels specified still only activates global")
    func zeroEpisodes() {
        let result = PriorHierarchyResolver.resolve(
            networkPriors: nil,
            networkDecay: 0,
            traitProfile: .unknown,
            showLocalPriors: nil
        )
        #expect(result.activeLevel == .global)
        #expect(result.levelContributions == [.global: 1.0])
    }

    @Test("show-local at exactly 5 episodes activates")
    func showLocalExactThreshold() {
        let local = makeShowLocalPriors(episodeCount: 5)
        let result = PriorHierarchyResolver.resolve(
            showLocalPriors: local
        )
        #expect(result.activeLevel == .showLocal)
    }

    @Test("show-local at 4 episodes does not activate")
    func showLocalJustBelowThreshold() {
        let local = makeShowLocalPriors(episodeCount: 4)
        let result = PriorHierarchyResolver.resolve(
            showLocalPriors: local
        )
        #expect(result.activeLevel == .global)
    }

    @Test("GlobalPriorDefaults.standard has expected values")
    func standardDefaults() {
        let d = GlobalPriorDefaults.standard
        #expect(d.musicBracketTrust == 0.5)
        #expect(d.metadataTrust == 0.5)
        #expect(d.fmBudgetBias == 0.5)
        #expect(d.fingerprintTransferConfidence == 0.5)
        #expect(d.sponsorRecurrenceExpectation == 0.3)
        #expect(d.typicalAdDuration == 30...90)
    }

    @Test("ResolvedPriors is Sendable and Equatable")
    func resolvedPriorsSendable() {
        let a = PriorHierarchyResolver.resolve()
        let b = PriorHierarchyResolver.resolve()
        #expect(a == b)
    }

    // MARK: - Decay interaction

    @Test("network decay at maximum (0.5) fully weighs network priors")
    func maxNetworkDecay() {
        let net = makeNetworkPriors(musicBracketPrevalence: 1.0, metadataTrustAverage: 1.0)
        let result = PriorHierarchyResolver.resolve(
            networkPriors: net,
            networkDecay: 0.5
        )
        // blend(0.5, 1.0, weight: 0.5) = 0.75
        #expect(abs(result.musicBracketTrust - 0.75) < 0.001)
        #expect(abs(result.metadataTrust - 0.75) < 0.001)
    }

    @Test("network decay at near-zero barely affects result")
    func minimalNetworkDecay() {
        let net = makeNetworkPriors(musicBracketPrevalence: 1.0)
        let result = PriorHierarchyResolver.resolve(
            networkPriors: net,
            networkDecay: 0.01
        )
        // blend(0.5, 1.0, weight: 0.01) = 0.505
        #expect(abs(result.musicBracketTrust - 0.505) < 0.001)
    }

    @Test("network decay values outside [0,1] are clamped")
    func networkDecayClamping() {
        let net = makeNetworkPriors(musicBracketPrevalence: 1.0, metadataTrustAverage: 1.0)
        // decay > 1.0 should be clamped to 1.0
        let overResult = PriorHierarchyResolver.resolve(
            networkPriors: net, networkDecay: 1.5
        )
        // blend(0.5, 1.0, weight: 1.0) = 1.0
        #expect(abs(overResult.musicBracketTrust - 1.0) < 0.001)
        let total = overResult.levelContributions.values.reduce(0, +)
        #expect(abs(total - 1.0) < 0.01)

        // decay < 0 should be clamped to 0 (network inactive)
        let underResult = PriorHierarchyResolver.resolve(
            networkPriors: net, networkDecay: -0.5
        )
        #expect(underResult.activeLevel == .global)
    }

    @Test("network sponsor recurrence formula at boundaries")
    func sponsorRecurrenceBoundaries() {
        // 0 sponsors -> 0 recurrence
        let emptyNet = makeNetworkPriors(commonSponsors: [:])
        let r0 = PriorHierarchyResolver.resolve(networkPriors: emptyNet, networkDecay: 0.5)
        #expect(r0.sponsorRecurrenceExpectation < 0.3)

        // 7 sponsors -> capped at 1.0 before blending
        let manySponsors = (0..<7).reduce(into: [String: Float]()) { dict, i in
            dict["sponsor\(i)"] = 0.5
        }
        let bigNet = makeNetworkPriors(commonSponsors: manySponsors)
        let r7 = PriorHierarchyResolver.resolve(networkPriors: bigNet, networkDecay: 0.5)
        #expect(r7.sponsorRecurrenceExpectation > r0.sponsorRecurrenceExpectation)
    }

    @Test("contributions sum to ~1.0 with only global + show-local (two levels)")
    func twoLevelContributions() {
        let local = makeShowLocalPriors(episodeCount: 10)
        let result = PriorHierarchyResolver.resolve(showLocalPriors: local)
        let total = result.levelContributions.values.reduce(0, +)
        #expect(abs(total - 1.0) < 0.01)
        #expect(result.activeLevel == .showLocal)
    }

    @Test("typicalAdDuration blends between levels")
    func durationBlending() {
        let net = makeNetworkPriors(typicalAdDuration: 10...30)
        let result = PriorHierarchyResolver.resolve(
            networkPriors: net,
            networkDecay: 0.5
        )
        // Global: 30...90, Network: 10...30, weight 0.5
        // Lower: 30*0.5 + 10*0.5 = 20, Upper: 90*0.5 + 30*0.5 = 60
        #expect(abs(result.typicalAdDuration.lowerBound - 20) < 0.1)
        #expect(abs(result.typicalAdDuration.upperBound - 60) < 0.1)
    }
}
