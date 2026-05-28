// FragilityGateArmConfigTests.swift
// playhead-xsdz.7 live A/B — hermetic, SYNTHETIC unit tests for the
// Evidence-Fragility gate A/B arm configuration in
// `FusionLiftHarnessSupport.swift`. No audio, no Foundation Models, no live
// pipeline — every input is a hand-built value, so these run on the simulator
// in the default `PlayheadFastTests` plan (they do NOT need
// `PLAYHEAD_FRAGILITY_AB=1`; that env var only gates the SLOW live harness in
// `FragilityGateLiveABTests`).
//
// The LOAD-BEARING correctness property of the 2-arm fragility A/B: the two
// arms must differ in EXACTLY one field —
// `AdDetectionConfig.evidenceFragilityPenaltyEnabled` — and nothing else. A
// second field drifting between arms would attribute a live precision/recall
// change to the fragility gate that some OTHER flag actually caused, making the
// measurement meaningless. These hermetic tests pin the one-field isolation on
// the sim before the (expensive, Catalyst-only) live A/B ever runs. They also
// pin the baseline arm to the production state (`AdDetectionConfig.default`),
// so the harness can never silently drift away from what ships.

import Foundation
import Testing
@testable import Playhead

@Suite("Fragility gate A/B arm config (xsdz.7 live A/B)")
struct FragilityGateArmConfigTests {

    // MARK: - Arm enumeration

    @Test("A/B enumerates exactly two arms, baseline first")
    func arm_twoArmsBaselineFirst() {
        #expect(FragilityGateArm.allCases == [.baseline, .treatment])
    }

    @Test("each arm's fragilityEnabled flag matches its intent")
    func arm_fragilityFlagPerArm() {
        #expect(FragilityGateArm.baseline.fragilityEnabled == false)
        #expect(FragilityGateArm.treatment.fragilityEnabled == true)
    }

    // MARK: - Per-arm config

    @Test("baseline arm has the fragility gate OFF (= production default)")
    func config_baselineFragilityOff() {
        let config = FragilityGateArmConfig.adDetectionConfig(for: .baseline)
        #expect(config.evidenceFragilityPenaltyEnabled == false)
        #expect(
            config.evidenceFragilityPenaltyEnabled == AdDetectionConfig.default.evidenceFragilityPenaltyEnabled,
            "baseline must equal the production default (gate off)"
        )
    }

    @Test("treatment arm has the fragility gate ON")
    func config_treatmentFragilityOn() {
        let config = FragilityGateArmConfig.adDetectionConfig(for: .treatment)
        #expect(config.evidenceFragilityPenaltyEnabled == true)
    }

    // MARK: - The load-bearing isolation property

    @Test("the two arms differ ONLY in evidenceFragilityPenaltyEnabled — every other field is equal")
    func config_isolation_onlyFragilityFlagVaries() {
        let baseline = FragilityGateArmConfig.adDetectionConfig(for: .baseline)
        let treatment = FragilityGateArmConfig.adDetectionConfig(for: .treatment)

        // The one field that IS allowed to differ does differ.
        #expect(baseline.evidenceFragilityPenaltyEnabled == false)
        #expect(treatment.evidenceFragilityPenaltyEnabled == true)
        #expect(baseline.evidenceFragilityPenaltyEnabled != treatment.evidenceFragilityPenaltyEnabled)

        // EVERY other field must be byte-identical across the two arms.
        // `AdDetectionConfig` is not `Equatable`, so the field list is
        // enumerated explicitly in `FragilityGateArmConfig.comparableFields`
        // (which intentionally EXCLUDES evidenceFragilityPenaltyEnabled).
        for field in FragilityGateArmConfig.comparableFields {
            #expect(
                field.value(baseline) == field.value(treatment),
                "field \(field.name) drifted between arms: baseline=\(field.value(baseline)) treatment=\(field.value(treatment))"
            )
        }
    }

    @Test("comparableFields does NOT include the one field allowed to vary")
    func config_comparableFieldsExcludesFragilityFlag() {
        // Defensive: if a future edit accidentally adds
        // evidenceFragilityPenaltyEnabled to comparableFields, the isolation
        // test above would FAIL spuriously (the one legal difference would be
        // flagged as drift). Pin its absence so that mistake is caught here
        // with a clear message instead.
        let names = Set(FragilityGateArmConfig.comparableFields.map(\.name))
        #expect(
            !names.contains("evidenceFragilityPenaltyEnabled"),
            "the one field allowed to vary must NOT be in comparableFields"
        )
    }

    // MARK: - Baseline pinned to the production state

    @Test("baseline arm equals the production AdDetectionConfig.default on every field")
    func config_baselineEqualsProductionDefault() {
        // The bead's BASELINE is defined as "current main PRODUCTION state".
        // Pin every comparable field equal to `AdDetectionConfig.default` so the
        // harness can never silently diverge from what ships. (The fragility
        // flag is checked separately above; baseline's value equals the default
        // there too.)
        let baseline = FragilityGateArmConfig.adDetectionConfig(for: .baseline)
        let prod = AdDetectionConfig.default
        for field in FragilityGateArmConfig.comparableFields {
            #expect(
                field.value(baseline) == field.value(prod),
                "baseline field \(field.name) diverged from production default: baseline=\(field.value(baseline)) default=\(field.value(prod))"
            )
        }
        #expect(baseline.evidenceFragilityPenaltyEnabled == prod.evidenceFragilityPenaltyEnabled)
    }

    @Test("baseline pins the explicit production flag/mode invariants the bead names")
    func config_baselineNamedInvariants() {
        let baseline = FragilityGateArmConfig.adDetectionConfig(for: .baseline)
        // fmBackfillMode .full → real FM scan feeds the fusion ledger.
        #expect(baseline.fmBackfillMode == .full)
        // chapterSignalMode .off.
        #expect(baseline.chapterSignalMode == .off)
        // ALL off-by-default evidence-channel flags FALSE.
        #expect(baseline.evidenceFragilityPenaltyEnabled == false)
        #expect(baseline.audioForensicsEnabled == false)
        #expect(baseline.crossEpisodeMemoryEnabled == false)
        #expect(baseline.rhetoricalGrammarEnabled == false)
        #expect(baseline.temporalRegularizationEnabled == false)
        #expect(baseline.crossShowSyndicationEnabled == false)
        #expect(baseline.lexicalAutoAdEnabled == false)
        // lexicalAutoAdQualifiedThreshold at its production default.
        #expect(baseline.lexicalAutoAdQualifiedThreshold == AdDetectionConfig.default.lexicalAutoAdQualifiedThreshold)
    }

    @Test("treatment uses the production-default fragility tuning knobs (only the master flag flips)")
    func config_treatmentUsesDefaultTuning() {
        let treatment = FragilityGateArmConfig.adDetectionConfig(for: .treatment)
        let prod = AdDetectionConfig.default
        #expect(treatment.evidenceFragilityPenaltyEnabled == true)
        #expect(treatment.fragilityThreshold == prod.fragilityThreshold)
        #expect(treatment.fragilityPenalty == prod.fragilityPenalty)
    }

    // MARK: - NarrowingConfig invariant (snap ON for both arms)

    @Test("both arms use NarrowingConfig.default (snap ON — xsdz.2/.3 kept on in production)")
    func narrowing_bothArmsUseDefaultSnapOn() {
        for arm in FragilityGateArm.allCases {
            let narrowing = FragilityGateArmConfig.narrowingConfig(for: arm)
            #expect(narrowing == NarrowingConfig.default, "arm \(arm.rawValue): narrowing must be .default")
            #expect(
                narrowing.lexicalClusterSnapEnabled == true,
                "arm \(arm.rawValue): baseline MUST have snap on (xsdz.2/.3 are production state)"
            )
        }
        // The narrowing config does NOT vary across arms.
        #expect(
            FragilityGateArmConfig.narrowingConfig(for: .baseline)
                == FragilityGateArmConfig.narrowingConfig(for: .treatment),
            "narrowing config must be identical across arms — only the fragility flag varies"
        )
    }
}
