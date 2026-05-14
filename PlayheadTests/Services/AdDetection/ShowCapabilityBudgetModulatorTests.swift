// ShowCapabilityBudgetModulatorTests.swift
// playhead-h6a6: tests for the per-show budget modulator. Covers:
//   * `.unknown` profile → no modulation (multiplier 1.0, empty bias map).
//   * Each non-unknown profile yields a multiplier in
//     `[minBudgetFloorRatio, 1.0]` AND `[detectorBiasFloor,
//      detectorBiasCeiling]` for its bias entries.
//   * `applyAdjustment(...)` enforces the always-on minimum per-episode
//     budget — `max(scaled, minimum)` regardless of multiplier.
//   * Different profiles bias DIFFERENT detectors (the bead's "different
//     profiles bias different detectors in the fusion ensemble" contract).
//   * Floor adversarial: even with the most aggressive raw multiplier
//     the result never undercuts `minBudgetFloorRatio` AND
//     `applyAdjustment` never undercuts the caller-supplied minimum.

import Foundation
import Testing

@testable import Playhead

@Suite("ShowCapabilityBudgetModulator")
struct ShowCapabilityBudgetModulatorTests {

    // MARK: - .unknown is a no-op

    @Test("unknown profile yields baseline (multiplier 1.0, empty bias map)")
    func unknownIsBaseline() {
        // The flag-OFF path lives in `AdDetectionService.runBackfill`,
        // which pins the kind to `.unknown` whenever the flag is off
        // or the profile snapshot is nil. The modulator's contract:
        // .unknown is the no-modulation no-op.
        let adj = ShowCapabilityBudgetModulator.adjustment(for: .unknown)
        #expect(adj.kind == .unknown)
        #expect(adj.analysisBudgetMultiplier == 1.0)
        #expect(adj.detectorBiases.isEmpty)
        for d in ShowCapabilityDetector.allCases {
            #expect(adj.bias(for: d) == 1.0)
        }
    }

    // MARK: - 15% compute-reduction target

    @Test("every non-unknown profile reduces the multiplier by ≥ 15%")
    func reducesByAtLeast15Percent() {
        // Bead-spec: profile-guided analysis reduces compute by ≥ 15%
        // on profile-matched shows vs baseline. The deterministic CI
        // assertion is the BEHAVIORAL contract: every non-unknown
        // profile's multiplier is ≤ 0.85.
        for kind in ShowCapabilityProfileKind.allCases where kind != .unknown {
            let adj = ShowCapabilityBudgetModulator.adjustment(for: kind)
            #expect(adj.analysisBudgetMultiplier <= 0.85,
                    "Profile \(kind.rawValue) must reduce baseline budget by ≥ 15%; got \(adj.analysisBudgetMultiplier)")
        }
    }

    // MARK: - Multiplier safety rails

    @Test("multiplier is clamped to [minBudgetFloorRatio, 1.0] for every kind")
    func multiplierClampedToBand() {
        for kind in ShowCapabilityProfileKind.allCases {
            let adj = ShowCapabilityBudgetModulator.adjustment(for: kind)
            #expect(adj.analysisBudgetMultiplier >= ShowCapabilityBudgetModulator.minBudgetFloorRatio)
            #expect(adj.analysisBudgetMultiplier <= 1.0)
        }
    }

    @Test("detector bias entries are clamped to [detectorBiasFloor, detectorBiasCeiling]")
    func biasClampedToBand() {
        for kind in ShowCapabilityProfileKind.allCases {
            let adj = ShowCapabilityBudgetModulator.adjustment(for: kind)
            for (_, value) in adj.detectorBiases {
                #expect(value >= ShowCapabilityBudgetModulator.detectorBiasFloor)
                #expect(value <= ShowCapabilityBudgetModulator.detectorBiasCeiling)
            }
        }
    }

    // MARK: - Per-profile detector-bias contract

    @Test("different profiles bias different detectors")
    func detectorBiasesVaryByKind() {
        // Bead-spec contract: "different profiles bias different
        // detectors in the fusion ensemble". The assertion: at
        // least one detector's bias under one profile differs from
        // its bias under another profile. We assert it pairwise on
        // a representative selection rather than enumerating the
        // whole matrix; the values themselves are tested below.
        let chapterRich = ShowCapabilityBudgetModulator.adjustment(for: .chapterRich)
        let hostReadOnly = ShowCapabilityBudgetModulator.adjustment(for: .hostReadOnly)
        let musicBedReliable = ShowCapabilityBudgetModulator.adjustment(for: .musicBedReliable)
        let sponsorDeclared = ShowCapabilityBudgetModulator.adjustment(for: .sponsorDeclared)
        let dynamicInsertion = ShowCapabilityBudgetModulator.adjustment(for: .dynamicInsertionHeavy)

        // chapter-rich leans on chapters; host-read-only leans on lexical.
        #expect(chapterRich.bias(for: .chapter) > 1.0)
        #expect(hostReadOnly.bias(for: .lexicalSponsor) > 1.0)
        // host-read-only de-emphasises music-bed (host reads don't have jingles).
        #expect(hostReadOnly.bias(for: .musicBed) < 1.0)
        // music-bed-reliable leans on music-bed.
        #expect(musicBedReliable.bias(for: .musicBed) > 1.0)
        // sponsor-declared leans on pre-seed.
        #expect(sponsorDeclared.bias(for: .sponsorPreSeed) > 1.0)
        // dynamic-insertion-heavy leans on boundary refinement.
        #expect(dynamicInsertion.bias(for: .boundaryRefinement) > 1.0)
    }

    // MARK: - applyAdjustment floor

    @Test("applyAdjustment never undercuts the always-on minimum budget")
    func applyAdjustmentFloorPreserved() {
        // Construct the most aggressive matched-profile path and
        // ensure the per-episode minimum is preserved even when
        // baseline * multiplier would drop below it.
        let adj = ShowCapabilityBudgetModulator.adjustment(for: .chapterRich)
        let baseline = 10.0
        let minimum = 9.0
        // baseline * multiplier = 7.0 < minimum = 9.0; floor must
        // hold the result at 9.0.
        let result = ShowCapabilityBudgetModulator.applyAdjustment(
            baseline: baseline,
            adjustment: adj,
            minimumPerEpisodeBudget: minimum
        )
        #expect(result == minimum)
    }

    @Test("applyAdjustment preserves the floor across every profile kind")
    func applyAdjustmentFloorAcrossEveryKind() {
        // Adversarial sweep: every profile kind, with a minimum
        // pegged AT baseline, must return baseline (never lower).
        let baseline = 100.0
        let minimum = baseline
        for kind in ShowCapabilityProfileKind.allCases {
            let adj = ShowCapabilityBudgetModulator.adjustment(for: kind)
            let result = ShowCapabilityBudgetModulator.applyAdjustment(
                baseline: baseline,
                adjustment: adj,
                minimumPerEpisodeBudget: minimum
            )
            #expect(result >= minimum,
                    "Floor must hold for profile \(kind.rawValue); got \(result) < \(minimum)")
        }
    }

    @Test("applyAdjustment passes through the unmodulated baseline for .unknown")
    func applyAdjustmentUnknownIsPassThrough() {
        // .unknown means "no modulation". Even with the minimum at
        // zero, the result must equal baseline (since multiplier = 1.0).
        let baseline = 42.0
        let adj = ShowCapabilityBudgetModulator.adjustment(for: .unknown)
        let result = ShowCapabilityBudgetModulator.applyAdjustment(
            baseline: baseline,
            adjustment: adj,
            minimumPerEpisodeBudget: 0
        )
        #expect(result == baseline)
    }

    // MARK: - Floor / band invariants

    @Test("minBudgetFloorRatio is at least 0.5")
    func minBudgetFloorRatioIsDefensible() {
        // The floor cannot drift below 0.5 without breaking the
        // bead's safety-rail contract. Pinning here makes a
        // careless edit fail CI rather than silently shrink the
        // floor.
        #expect(ShowCapabilityBudgetModulator.minBudgetFloorRatio >= 0.5)
    }
}
