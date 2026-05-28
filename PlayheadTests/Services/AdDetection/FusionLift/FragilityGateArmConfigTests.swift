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
        let config = FragilityGateArmConfig.adDetectionConfig(for: FragilityGateArm.baseline)
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
        let baseline = FragilityGateArmConfig.adDetectionConfig(for: FragilityGateArm.baseline)
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
        let baseline = FragilityGateArmConfig.adDetectionConfig(for: FragilityGateArm.baseline)
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
        let baseline = FragilityGateArmConfig.adDetectionConfig(for: FragilityGateArm.baseline)
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
            FragilityGateArmConfig.narrowingConfig(for: FragilityGateArm.baseline)
                == FragilityGateArmConfig.narrowingConfig(for: .treatment),
            "narrowing config must be identical across arms — only the fragility flag varies"
        )
    }
}

// MARK: - Sweep arm config (playhead-xsdz.7 Part B)

@Suite("Fragility threshold/penalty sweep arm config (xsdz.7 Part B)")
struct FragilitySweepArmConfigTests {

    @Test("sweep enumerates baseline + 4 treatments, baseline first")
    func arm_fiveArmsBaselineFirst() {
        #expect(FragilitySweepArm.allCases == [.baseline, .t15p85, .t10p85, .t07p70, .t05p50])
    }

    @Test("baseline arm is the gate-OFF production state")
    func arm_baselineGateOff() {
        #expect(FragilitySweepArm.baseline.fragilityEnabled == false)
        // Off-arm tuning equals production default (inert, but pinned so the off
        // arm == AdDetectionConfig.default everywhere).
        #expect(FragilitySweepArm.baseline.fragilityThreshold == AdDetectionConfig.default.fragilityThreshold)
        #expect(FragilitySweepArm.baseline.fragilityPenalty == AdDetectionConfig.default.fragilityPenalty)
    }

    @Test("each treatment arm has the gate ON at its named operating point")
    func arm_treatmentOperatingPoints() {
        let expected: [(FragilitySweepArm, Double, Double)] = [
            (.t15p85, 1.5, 0.85),
            (.t10p85, 1.0, 0.85),
            (.t07p70, 0.7, 0.70),
            (.t05p50, 0.5, 0.50),
        ]
        for (arm, thr, pen) in expected {
            #expect(arm.fragilityEnabled == true, "arm \(arm.rawValue) must enable the gate")
            #expect(arm.fragilityThreshold == thr, "arm \(arm.rawValue) threshold")
            #expect(arm.fragilityPenalty == pen, "arm \(arm.rawValue) penalty")
        }
    }

    // MARK: The load-bearing sweep isolation property

    @Test("every sweep arm differs from .default ONLY in the three fragility tuning fields")
    func config_sweepIsolation_onlyTuningVaries() {
        let prod = AdDetectionConfig.default
        for arm in FragilitySweepArm.allCases {
            let config = FragilityGateArmConfig.adDetectionConfig(for: arm)

            // The three fields the sweep is ALLOWED to vary land at the arm's
            // operating point.
            #expect(config.evidenceFragilityPenaltyEnabled == arm.fragilityEnabled)
            #expect(config.fragilityThreshold == arm.fragilityThreshold)
            #expect(config.fragilityPenalty == arm.fragilityPenalty)

            // EVERY other field equals the production default.
            for field in FragilityGateArmConfig.sweepComparableFields {
                #expect(
                    field.value(config) == field.value(prod),
                    "arm \(arm.rawValue): field \(field.name) drifted from .default: arm=\(field.value(config)) default=\(field.value(prod))"
                )
            }
        }
    }

    @Test("any two sweep arms agree on every non-tuning field")
    func config_sweepIsolation_pairwiseEqualOffTuning() {
        let configs = FragilitySweepArm.allCases.map {
            (arm: $0, config: FragilityGateArmConfig.adDetectionConfig(for: $0))
        }
        for i in configs.indices {
            for j in configs.indices where j > i {
                for field in FragilityGateArmConfig.sweepComparableFields {
                    #expect(
                        field.value(configs[i].config) == field.value(configs[j].config),
                        "arms \(configs[i].arm.rawValue)/\(configs[j].arm.rawValue): field \(field.name) drifted"
                    )
                }
            }
        }
    }

    @Test("sweepComparableFields EXCLUDES exactly the three varying fields")
    func config_sweepComparableFieldsExcludesTuning() {
        let names = Set(FragilityGateArmConfig.sweepComparableFields.map(\.name))
        #expect(!names.contains("evidenceFragilityPenaltyEnabled"))
        #expect(!names.contains("fragilityThreshold"))
        #expect(!names.contains("fragilityPenalty"))
        // It must still include a representative non-tuning field so the
        // isolation check is not vacuously empty.
        #expect(names.contains("fmBackfillMode"))
        #expect(names.contains("autoSkipConfidenceThreshold"))
    }

    @Test("every sweep arm uses NarrowingConfig.default (snap ON)")
    func narrowing_everyArmDefaultSnapOn() {
        for arm in FragilitySweepArm.allCases {
            #expect(FragilityGateArmConfig.narrowingConfig(for: arm) == NarrowingConfig.default)
        }
    }

    @Test("the sweep baseline AdDetectionConfig is byte-identical to the 2-arm A/B baseline")
    func config_sweepBaselineMatchesABBaseline() {
        // The single Catalyst pass uses the sweep `.baseline` arm AS the
        // per-span diagnostic + sweep baseline; it must equal the production
        // state the original A/B baseline pins, so the two harnesses never drift.
        let sweepBaseline = FragilityGateArmConfig.adDetectionConfig(for: FragilitySweepArm.baseline)
        let abBaseline = FragilityGateArmConfig.adDetectionConfig(for: FragilityGateArm.baseline)
        #expect(sweepBaseline.evidenceFragilityPenaltyEnabled == abBaseline.evidenceFragilityPenaltyEnabled)
        for field in FragilityGateArmConfig.comparableFields {
            #expect(
                field.value(sweepBaseline) == field.value(abBaseline),
                "sweep baseline field \(field.name) diverged from A/B baseline"
            )
        }
    }
}
