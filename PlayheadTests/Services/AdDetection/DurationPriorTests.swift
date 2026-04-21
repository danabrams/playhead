// DurationPriorTests.swift
// playhead-p2iv: Tests for DurationPrior shape function + DecisionMapper integration.
//
// The prior is a soft monotonic multiplier over the fused confidence score.
// Tests verify:
//   - Peak inside typical range produces `peakMultiplier`.
//   - Smooth decay outside the range (no hard cutoffs).
//   - Bounded multiplier range: [floorMultiplier, peakMultiplier].
//   - Long-form host-read ads (~100s, ~120s) still get meaningful weight (>= 1.0).
//   - Very-short (~3s) and very-long (~300s) windows get proportional but
//     non-fatal penalties (floor, not zero).
//   - Identity prior is a no-op.
//   - DecisionMapper integration: a strong ledger on a long-form ad still
//     clears the skip threshold; the prior modulates, it does not veto.

import Foundation
import Testing
@testable import Playhead

@Suite("DurationPrior")
struct DurationPriorTests {

    // MARK: - Peak region

    @Test("Multiplier peaks inside the typical range (30...90)")
    func peakInsideTypicalRange() {
        let prior = DurationPrior(typicalAdDuration: 30...90)
        // The peak plateau is [a, b]. All points within should equal peakMultiplier.
        #expect(prior.multiplier(forDuration: 30) == prior.peakMultiplier)
        #expect(prior.multiplier(forDuration: 45) == prior.peakMultiplier)
        #expect(prior.multiplier(forDuration: 60) == prior.peakMultiplier)
        #expect(prior.multiplier(forDuration: 90) == prior.peakMultiplier)
    }

    @Test("Peak multiplier is > 1.0 (small positive nudge)")
    func peakIsPositiveNudge() {
        let prior = DurationPrior.standard
        #expect(prior.peakMultiplier > 1.0)
        #expect(prior.peakMultiplier <= 1.15)
    }

    // MARK: - Monotonic decay outside the range

    @Test("Bumper region ramps smoothly from 5s to the lower bound")
    func bumperRegionRampsSmoothly() {
        let prior = DurationPrior(typicalAdDuration: 30...90)
        let at5 = prior.multiplier(forDuration: 5)
        let at10 = prior.multiplier(forDuration: 10)
        let at20 = prior.multiplier(forDuration: 20)
        let at30 = prior.multiplier(forDuration: 30)

        // Monotonic increase from 5s → 30s.
        #expect(at5 <= at10)
        #expect(at10 <= at20)
        #expect(at20 <= at30)
        // 5s sits at the bumper floor (not the very-short floor).
        #expect(at5 == prior.bumperFloorMultiplier)
        // 30s sits at the peak.
        #expect(at30 == prior.peakMultiplier)
    }

    @Test("Long-form shoulder decays smoothly from upper bound to 2× upper bound")
    func longFormShoulderDecaysSmoothly() {
        let prior = DurationPrior(typicalAdDuration: 30...90)
        let at90 = prior.multiplier(forDuration: 90)
        let at100 = prior.multiplier(forDuration: 100)
        let at120 = prior.multiplier(forDuration: 120)
        let at150 = prior.multiplier(forDuration: 150)
        let at180 = prior.multiplier(forDuration: 180)

        // Monotonic decrease from 90s → 180s.
        #expect(at90 >= at100)
        #expect(at100 >= at120)
        #expect(at120 >= at150)
        #expect(at150 >= at180)
        // 180s (== 2b) sits at the long-form shoulder floor.
        #expect(at180 == prior.longFormShoulderMultiplier)
    }

    // MARK: - Long-form host-read (bead contract)

    @Test("~100s host-read ad still receives a multiplier >= 1.0")
    func longFormHostReadStillPositive_100s() {
        let prior = DurationPrior.standard
        let m = prior.multiplier(forDuration: 100)
        // 100s is just past the 90s upper bound — still very strong.
        #expect(m >= 1.0, "100s host-read must retain at least a neutral weight, got \(m)")
        #expect(m <= prior.peakMultiplier)
    }

    @Test("~120s host-read ad still receives a meaningful multiplier (>= ~1.0)")
    func longFormHostReadStillPositive_120s() {
        let prior = DurationPrior.standard
        let m = prior.multiplier(forDuration: 120)
        // 120s is 2 min — still in the long-form zone, should be ~1.0 or better
        // (bead: "Long-form host-read ads (90–120s) must still score high").
        #expect(m >= 1.0, "120s host-read must retain at least a neutral weight, got \(m)")
    }

    // MARK: - Very short / very long penalties (not fatal)

    @Test("Very short windows (~3s) receive the floor multiplier, not zero")
    func veryShortIsProportionalPenalty() {
        let prior = DurationPrior.standard
        let m = prior.multiplier(forDuration: 3)
        #expect(m == prior.floorMultiplier)
        #expect(m >= 0.70, "Floor penalty must be mild, not a veto, got \(m)")
        #expect(m <= 0.80)
    }

    @Test("Very long windows (~300s = 5min) receive the floor multiplier, not zero")
    func veryLongIsProportionalPenalty() {
        let prior = DurationPrior.standard
        let m = prior.multiplier(forDuration: 300)
        #expect(m == prior.floorMultiplier)
        #expect(m >= 0.70, "Floor penalty for a 5-minute window must not be a veto, got \(m)")
    }

    @Test("Floor penalty is strictly milder than zero — prior never vetoes")
    func floorIsNeverZero() {
        let prior = DurationPrior.standard
        #expect(prior.floorMultiplier > 0, "Prior is a nudge; floor must never zero-out a score")
    }

    // MARK: - Bounded range

    @Test("Multiplier is bounded by [floorMultiplier, peakMultiplier] for any duration")
    func boundedMultiplierRange() {
        let prior = DurationPrior.standard
        let durations: [TimeInterval] = [
            0, 0.5, 1, 3, 5, 7, 15, 30, 45, 60, 90, 100, 110, 120, 150, 180, 200, 300, 500, 3600
        ]
        for d in durations {
            let m = prior.multiplier(forDuration: d)
            #expect(m >= prior.floorMultiplier, "multiplier(\(d))=\(m) below floor")
            #expect(m <= prior.peakMultiplier, "multiplier(\(d))=\(m) above peak")
        }
    }

    @Test("Default bounds are conservative: floor >= 0.70, peak <= 1.15")
    func defaultBoundsConservative() {
        let prior = DurationPrior.standard
        #expect(prior.floorMultiplier >= 0.70)
        #expect(prior.peakMultiplier <= 1.15)
    }

    // MARK: - Identity / defensive cases

    @Test("Identity prior is a no-op: multiplier == 1.0 for all durations")
    func identityPriorIsNoOp() {
        let prior = DurationPrior.identity
        for d in [0.0, 1, 5, 30, 60, 90, 120, 180, 300, 3600] {
            #expect(prior.multiplier(forDuration: d) == 1.0,
                    "identity prior must return 1.0 at duration=\(d)")
        }
    }

    @Test("Non-finite or negative durations return the floor multiplier")
    func nonFiniteDefensivelyClamps() {
        let prior = DurationPrior.standard
        #expect(prior.multiplier(forDuration: -1) == prior.floorMultiplier)
        #expect(prior.multiplier(forDuration: .nan) == prior.floorMultiplier)
        #expect(prior.multiplier(forDuration: .infinity) == prior.floorMultiplier)
        #expect(prior.multiplier(forDuration: -.infinity) == prior.floorMultiplier)
    }

    // MARK: - ResolvedPriors consumption

    @Test("Construction from ResolvedPriors consumes typicalAdDuration")
    func constructionFromResolvedPriors() {
        // Produce a ResolvedPriors via the actual resolver (no architectural change
        // needed to get one). Default resolution == global defaults (30...90).
        let resolved = PriorHierarchyResolver.resolve()
        let prior = DurationPrior(resolvedPriors: resolved)
        #expect(prior.typicalAdDuration == resolved.typicalAdDuration)
        #expect(prior.multiplier(forDuration: 60) == prior.peakMultiplier,
                "60s inside [30,90] should hit the peak")
    }

    @Test("Show-aware range narrows the peak plateau")
    func showAwarePriorNarrowsPeak() {
        // A show whose ads cluster tighter (e.g. 45...75s) should peak only there.
        let prior = DurationPrior(typicalAdDuration: 45...75)
        #expect(prior.multiplier(forDuration: 35) < prior.peakMultiplier,
                "35s outside [45,75] should be below peak in a tighter-range show")
        #expect(prior.multiplier(forDuration: 60) == prior.peakMultiplier)
        #expect(prior.multiplier(forDuration: 80) < prior.peakMultiplier,
                "80s outside [45,75] should be below peak")
    }

    // MARK: - DecisionMapper integration

    @Test("Default DecisionMapper (no prior) leaves proposalConfidence unchanged")
    func decisionMapperDefaultNoOp() {
        let span = DecodedSpan(
            id: "s1",
            assetId: "a1",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: 100.0,
            endTime: 160.0, // 60s
            anchorProvenance: []
        )
        let ledger: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.30, detail: .classifier(score: 1.0)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
        ]
        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig()
        )
        let result = mapper.map()
        // Raw sum = 0.50 (no prior applied).
        #expect(abs(result.proposalConfidence - 0.50) < 1e-9)
    }

    @Test("DecisionMapper with peak-range duration boosts proposalConfidence")
    func decisionMapperBoostsPeakDuration() {
        let span = DecodedSpan(
            id: "s1",
            assetId: "a1",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: 100.0,
            endTime: 160.0, // 60s — inside peak
            anchorProvenance: []
        )
        let ledger: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.30, detail: .classifier(score: 1.0)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
        ]
        let mapperWithPrior = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            durationPrior: .standard
        )
        let result = mapperWithPrior.map()
        let expected = 0.50 * DurationPrior.standard.peakMultiplier
        #expect(abs(result.proposalConfidence - expected) < 1e-9,
                "Peak-range duration should multiply raw sum by peakMultiplier")
        #expect(result.proposalConfidence > 0.50)
    }

    @Test("DecisionMapper with very-long duration penalizes, but does not veto, strong evidence")
    func decisionMapperDoesNotVetoStrongEvidence() {
        // 300s window with an overwhelming ledger — must still confirm (well above skip threshold).
        let span = DecodedSpan(
            id: "s1",
            assetId: "a1",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: 100.0,
            endTime: 400.0, // 300s — very long region
            anchorProvenance: []
        )
        let ledger: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.30, detail: .classifier(score: 1.0)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url", "promoCode"])),
            .init(source: .acoustic, weight: 0.20, detail: .acoustic(breakStrength: 0.9)),
            .init(source: .catalog, weight: 0.20, detail: .catalog(entryCount: 3)),
            .init(source: .fingerprint, weight: 0.25, detail: .fingerprint(matchCount: 2, averageSimilarity: 0.95)),
        ]
        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            durationPrior: .standard
        )
        let result = mapper.map()
        // Raw ledger clamps to 1.0 × floor (0.75) = 0.75.
        // The prior penalizes (floor=0.75), but the decision should still be very high.
        #expect(result.proposalConfidence >= 0.70,
                "Strong evidence on a 300s window must still clear a high bar, got \(result.proposalConfidence)")
    }

    @Test("DecisionMapper keeps 120s host-read ad's strong evidence above skip threshold")
    func decisionMapperAllows120sHostRead() {
        // 120s host-read, strong evidence — must remain skippable (>= 0.60 band).
        let span = DecodedSpan(
            id: "s1",
            assetId: "a1",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: 100.0,
            endTime: 220.0, // 120s
            anchorProvenance: []
        )
        let ledger: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.30, detail: .classifier(score: 1.0)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url", "promoCode"])),
            .init(source: .acoustic, weight: 0.20, detail: .acoustic(breakStrength: 0.9)),
        ]
        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            durationPrior: .standard
        )
        let result = mapper.map()
        // Raw sum = 0.70; at 120s the multiplier is in the long-form shoulder (~1.03).
        // Expect the prior-adjusted confidence to be >= raw.
        #expect(result.proposalConfidence >= 0.70,
                "120s host-read must remain strong post-prior, got \(result.proposalConfidence)")
    }

    @Test("DecisionMapper proposalConfidence remains in [0, 1] under extreme inputs")
    func decisionMapperClampsToUnitInterval() {
        // A saturated ledger × peak multiplier > 1 must clamp to 1.
        let span = DecodedSpan(
            id: "s1",
            assetId: "a1",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: 0,
            endTime: 60, // peak
            anchorProvenance: []
        )
        let ledger: [EvidenceLedgerEntry] = [
            // Deliberately oversaturate — pre-clamp would sum > 1.
            .init(source: .classifier, weight: 0.30, detail: .classifier(score: 1.0)),
            .init(source: .fm, weight: 0.40, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.20, detail: .acoustic(breakStrength: 0.9)),
            .init(source: .catalog, weight: 0.20, detail: .catalog(entryCount: 3)),
            .init(source: .fingerprint, weight: 0.25, detail: .fingerprint(matchCount: 2, averageSimilarity: 0.95)),
        ]
        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            durationPrior: .standard
        )
        let result = mapper.map()
        #expect(result.proposalConfidence <= 1.0)
        #expect(result.proposalConfidence >= 0.0)
        #expect(result.skipConfidence <= 1.0)
        #expect(result.skipConfidence >= 0.0)
    }
}
