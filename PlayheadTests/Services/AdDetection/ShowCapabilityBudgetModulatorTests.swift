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

    @Test("applyAdjustment with zero baseline and zero minimum returns zero — floor still holds")
    func applyAdjustmentZeroBaselineZeroMinimum() {
        // h6a6 R1 review gap: adversarial inputs (zero baseline, zero
        // minimum) must not produce NaN, negative numbers, or break
        // the floor contract. `max(0 * multiplier, 0) == 0` is the
        // expected result, and zero is `>= 0` (the floor), so the
        // contract is preserved. Pinning this so a careless refactor
        // that adds e.g. division can't silently regress.
        for kind in ShowCapabilityProfileKind.allCases {
            let adj = ShowCapabilityBudgetModulator.adjustment(for: kind)
            let result = ShowCapabilityBudgetModulator.applyAdjustment(
                baseline: 0,
                adjustment: adj,
                minimumPerEpisodeBudget: 0
            )
            #expect(result == 0,
                    "Zero baseline + zero minimum should return zero for \(kind.rawValue); got \(result)")
            #expect(result.isFinite,
                    "Zero baseline + zero minimum must be finite for \(kind.rawValue)")
        }
    }

    @Test("applyAdjustment at the multiplier band boundary 0.50 preserves the minimum floor")
    func applyAdjustmentAtMultiplierFloorBoundary() {
        // h6a6 R1 review gap: when `minBudgetFloorRatio == 0.50` is
        // both the clamp floor AND in principle a multiplier value a
        // profile COULD raw-emit, ensure `applyAdjustment` still
        // honors the caller-supplied minimum. Construct an
        // `.unknown`-equivalent adjustment with the lowest possible
        // multiplier; baseline * 0.5 vs minimum must always pick the
        // minimum when the latter is higher.
        let adj = ShowCapabilityBudgetAdjustment(
            kind: .chapterRich,
            analysisBudgetMultiplier: ShowCapabilityBudgetModulator.minBudgetFloorRatio,
            detectorBiases: [:]
        )
        let baseline = 10.0
        let minimum = 6.0 // baseline * 0.5 == 5.0; minimum (6.0) wins.
        let result = ShowCapabilityBudgetModulator.applyAdjustment(
            baseline: baseline,
            adjustment: adj,
            minimumPerEpisodeBudget: minimum
        )
        #expect(result == minimum,
                "At multiplier floor 0.50, caller's larger minimum must hold the result")
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

    // MARK: - AdDetectionService init-seed contract

    @Test("AdDetectionService seeds lastCapabilityBudgetAdjustment to the .unknown baseline at init")
    func adDetectionServiceInitSeedsUnknownBaseline() async throws {
        // h6a6 R4 review fix (probe 5): the
        // `lastCapabilityBudgetAdjustmentForTesting()` accessor was
        // documented as a test seam but had NO test exercising it. The
        // accessor's contract: BEFORE the first `runBackfill` (and on
        // every flag-OFF run thereafter) the value is the
        // `.unknown`-yielded baseline so a consumer that reads it
        // pre-backfill never sees a non-unity multiplier. Pin that
        // contract here so a refactor cannot silently change the seed
        // without breaking CI.
        let store = try await makeTestStore()
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .off
            )
        )

        let adjustment = await service.lastCapabilityBudgetAdjustmentForTesting()
        #expect(adjustment.kind == .unknown,
                "Pre-backfill seed must be .unknown (no profile observed yet)")
        #expect(adjustment.analysisBudgetMultiplier == 1.0,
                ".unknown profile must yield the no-modulation multiplier")
        #expect(adjustment.detectorBiases.isEmpty,
                ".unknown profile must yield an empty bias map (every detector at 1.0)")
        for detector in ShowCapabilityDetector.allCases {
            #expect(adjustment.bias(for: detector) == 1.0,
                    "Bias for \(detector) must default to 1.0 on the .unknown baseline")
        }
    }
}
