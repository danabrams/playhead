// FbsignalsArmConfigTests.swift
// playhead-fbsignals live A/B — hermetic, SYNTHETIC unit tests for the
// feedback/memory-driven single-signal A/B support in
// `FusionLiftHarnessSupport.swift` plus the two fire-instrumentation observers
// added for it. No audio, no Foundation Models, no live pipeline — every input is
// a hand-built value, so these run on the simulator in the default
// `PlayheadFastTests` plan (they do NOT need `PLAYHEAD_FBSIGNALS_AB=1`; that env
// var only gates the SLOW live harness in
// `CrossEpisodeMemoryPerShowThresholdLiveABTests`).
//
// They pin the LOAD-BEARING correctness properties before the (expensive,
// Catalyst-only) live A/B ever runs:
//   1. ARM ISOLATION (the load-bearing property): for EACH signal the two arms
//      differ ONLY in that signal's single flag (`crossEpisodeMemoryEnabled` for
//      xsdz.9, `perShowThresholdControlEnabled` for xsdz.11); every other field is
//      byte-identical and equal to `AdDetectionConfig.default` — INCLUDING the
//      OTHER signal's flag (no cross-contamination between the two A/Bs).
//   2. BASELINE == PRODUCTION: each signal's baseline arm equals
//      `AdDetectionConfig.default` on every field.
//   3. STORE GATING: only the treatment arm of the signal that owns a given store
//      requires it (baseline + the other signal's arms require none) — the
//      gating-parity property the live harness relies on to wire stores EMPTY
//      exactly as production does at cold-start.
//   4. FIRE INSTRUMENTATION: the nil-default observers record the correct
//      per-signal fire counts so the (expected-0) live result is interpretable —
//      `.crossEpisodeMemory` positive-boost ledger entries + negative-bank
//      suppression counts (xsdz.9), and per-show offset shift counts (xsdz.11).

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("Fbsignals A/B arm config (playhead-fbsignals)")
struct FbsignalsArmConfigTests {

    // MARK: - Arm + signal enumeration

    @Test("the A/B enumerates exactly two signals")
    func signal_twoSignals() {
        #expect(FbsignalsSignal.allCases == [.crossEpisodeMemory, .perShowThreshold])
    }

    @Test("each signal owns the correct varying field name")
    func signal_varyingFieldName() {
        #expect(FbsignalsSignal.crossEpisodeMemory.varyingFieldName == "crossEpisodeMemoryEnabled")
        #expect(FbsignalsSignal.perShowThreshold.varyingFieldName == "perShowThresholdControlEnabled")
    }

    @Test("each signal declares the correct store requirement")
    func signal_storeRequirement() {
        #expect(FbsignalsSignal.crossEpisodeMemory.requiresNegativeFingerprintBank == true)
        #expect(FbsignalsSignal.crossEpisodeMemory.requiresPerShowThresholdControllerStore == false)
        #expect(FbsignalsSignal.perShowThreshold.requiresNegativeFingerprintBank == false)
        #expect(FbsignalsSignal.perShowThreshold.requiresPerShowThresholdControllerStore == true)
    }

    @Test("the A/B enumerates exactly two arms, baseline first")
    func arm_twoArmsBaselineFirst() {
        #expect(FbsignalsArm.allCases == [.baseline, .treatment])
    }

    @Test("each arm's signalEnabled flag matches its intent")
    func arm_signalFlagPerArm() {
        #expect(FbsignalsArm.baseline.signalEnabled == false)
        #expect(FbsignalsArm.treatment.signalEnabled == true)
    }

    // MARK: - Per-arm config flags

    @Test("each signal's treatment arm flips ONLY that signal's flag")
    func config_treatmentFlipsOnlyOwnFlag() {
        // xsdz.9 treatment: crossEpisodeMemory ON, perShowThreshold still OFF.
        let cem = FbsignalsArmConfig.adDetectionConfig(signal: .crossEpisodeMemory, for: .treatment)
        #expect(cem.crossEpisodeMemoryEnabled == true)
        #expect(cem.perShowThresholdControlEnabled == AdDetectionConfig.default.perShowThresholdControlEnabled)
        #expect(cem.perShowThresholdControlEnabled == false)

        // xsdz.11 treatment: perShowThreshold ON, crossEpisodeMemory still OFF.
        let ps = FbsignalsArmConfig.adDetectionConfig(signal: .perShowThreshold, for: .treatment)
        #expect(ps.perShowThresholdControlEnabled == true)
        #expect(ps.crossEpisodeMemoryEnabled == AdDetectionConfig.default.crossEpisodeMemoryEnabled)
        #expect(ps.crossEpisodeMemoryEnabled == false)
    }

    @Test("each signal's baseline arm has its signal OFF (= production default)")
    func config_baselineSignalOff() {
        for signal in FbsignalsSignal.allCases {
            let baseline = FbsignalsArmConfig.adDetectionConfig(signal: signal, for: .baseline)
            #expect(baseline.crossEpisodeMemoryEnabled == false, "signal \(signal.rawValue): cem must be off in baseline")
            #expect(baseline.perShowThresholdControlEnabled == false, "signal \(signal.rawValue): ps must be off in baseline")
        }
    }

    // MARK: - The load-bearing isolation property

    @Test("for each signal, the two arms differ ONLY in that signal's flag")
    func config_isolation_onlyOneFlagVaries() {
        for signal in FbsignalsSignal.allCases {
            let baseline = FbsignalsArmConfig.adDetectionConfig(signal: signal, for: .baseline)
            let treatment = FbsignalsArmConfig.adDetectionConfig(signal: signal, for: .treatment)

            // The one field the signal IS allowed to vary does differ.
            switch signal {
            case .crossEpisodeMemory:
                #expect(baseline.crossEpisodeMemoryEnabled != treatment.crossEpisodeMemoryEnabled)
            case .perShowThreshold:
                #expect(baseline.perShowThresholdControlEnabled != treatment.perShowThresholdControlEnabled)
            }

            // EVERY other field (including the OTHER signal's flag) is byte-identical
            // across the two arms.
            for field in FbsignalsArmConfig.comparableFields(for: signal) {
                #expect(
                    field.value(baseline) == field.value(treatment),
                    "signal \(signal.rawValue): field \(field.name) drifted between arms: baseline=\(field.value(baseline)) treatment=\(field.value(treatment))"
                )
            }
        }
    }

    @Test("for each signal, every arm equals .default on every non-varying field")
    func config_isolation_armsMatchDefault() {
        let prod = AdDetectionConfig.default
        for signal in FbsignalsSignal.allCases {
            for arm in FbsignalsArm.allCases {
                let config = FbsignalsArmConfig.adDetectionConfig(signal: signal, for: arm)
                for field in FbsignalsArmConfig.comparableFields(for: signal) {
                    #expect(
                        field.value(config) == field.value(prod),
                        "signal \(signal.rawValue) arm \(arm.rawValue): field \(field.name) drifted from .default: arm=\(field.value(config)) default=\(field.value(prod))"
                    )
                }
            }
        }
    }

    @Test("comparableFields EXCLUDES only the signal's own flag and keeps the other signal's flag")
    func config_comparableFieldsExclusion() {
        // xsdz.9: excludes crossEpisodeMemoryEnabled, KEEPS perShowThresholdControlEnabled
        // (so cross-contamination — the other A/B's flag drifting — is caught).
        let cemNames = Set(FbsignalsArmConfig.comparableFields(for: .crossEpisodeMemory).map(\.name))
        #expect(!cemNames.contains("crossEpisodeMemoryEnabled"))
        #expect(cemNames.contains("perShowThresholdControlEnabled"))

        let psNames = Set(FbsignalsArmConfig.comparableFields(for: .perShowThreshold).map(\.name))
        #expect(!psNames.contains("perShowThresholdControlEnabled"))
        #expect(psNames.contains("crossEpisodeMemoryEnabled"))

        // Still non-vacuous: representative non-flag fields present in both.
        for names in [cemNames, psNames] {
            #expect(names.contains("fmBackfillMode"))
            #expect(names.contains("autoSkipConfidenceThreshold"))
            #expect(names.contains("chapterSignalMode"))
        }
    }

    // MARK: - Baseline pinned to the production state

    @Test("each signal's baseline equals AdDetectionConfig.default on every field")
    func config_baselineEqualsProductionDefault() {
        let prod = AdDetectionConfig.default
        for signal in FbsignalsSignal.allCases {
            let baseline = FbsignalsArmConfig.adDetectionConfig(signal: signal, for: .baseline)
            for field in FbsignalsArmConfig.comparableFields(for: signal) {
                #expect(
                    field.value(baseline) == field.value(prod),
                    "signal \(signal.rawValue): baseline field \(field.name) diverged from default"
                )
            }
            // And the varied flags themselves equal .default (off) in the baseline.
            #expect(baseline.crossEpisodeMemoryEnabled == prod.crossEpisodeMemoryEnabled)
            #expect(baseline.perShowThresholdControlEnabled == prod.perShowThresholdControlEnabled)
        }
    }

    @Test("baseline pins the explicit production flag/mode invariants the bead names")
    func config_baselineNamedInvariants() {
        for signal in FbsignalsSignal.allCases {
            let baseline = FbsignalsArmConfig.adDetectionConfig(signal: signal, for: .baseline)
            // fmBackfillMode .full → real FM scan feeds the fusion ledger.
            #expect(baseline.fmBackfillMode == .full)
            #expect(baseline.chapterSignalMode == .off)
            // ALL off-by-default evidence-channel / feedback flags FALSE on baseline.
            #expect(baseline.crossEpisodeMemoryEnabled == false)
            #expect(baseline.perShowThresholdControlEnabled == false)
            #expect(baseline.audioForensicsEnabled == false)
            #expect(baseline.temporalRegularizationEnabled == false)
            #expect(baseline.evidenceFragilityPenaltyEnabled == false)
            #expect(baseline.rhetoricalGrammarEnabled == false)
            #expect(baseline.crossShowSyndicationEnabled == false)
            #expect(baseline.lexicalAutoAdEnabled == false)
        }
    }

    @Test("per-show treatment keeps the production-default controller tuning knobs (only the master flag flips)")
    func config_perShowTreatmentUsesDefaultTuning() {
        let prod = AdDetectionConfig.default
        let treatment = FbsignalsArmConfig.adDetectionConfig(signal: .perShowThreshold, for: .treatment)
        #expect(treatment.perShowThresholdControlEnabled == true)
        #expect(treatment.perShowThresholdProportionalGain == prod.perShowThresholdProportionalGain)
        #expect(treatment.perShowThresholdIntegralGain == prod.perShowThresholdIntegralGain)
        #expect(treatment.perShowThresholdMaxOffset == prod.perShowThresholdMaxOffset)
        #expect(treatment.perShowThresholdMinSamples == prod.perShowThresholdMinSamples)
    }

    // MARK: - NarrowingConfig invariant (snap ON for every arm)

    @Test("every arm uses NarrowingConfig.default (snap ON) and the config never varies")
    func narrowing_everyArmDefaultSnapOn() {
        for arm in FbsignalsArm.allCases {
            let narrowing = FbsignalsArmConfig.narrowingConfig(for: arm)
            #expect(narrowing == NarrowingConfig.default, "arm \(arm.rawValue): narrowing must be .default")
            #expect(narrowing.lexicalClusterSnapEnabled == true,
                    "arm \(arm.rawValue): snap must be on (production state)")
        }
        #expect(
            FbsignalsArmConfig.narrowingConfig(for: .baseline)
                == FbsignalsArmConfig.narrowingConfig(for: .treatment),
            "narrowing must be identical across arms — only the one signal flag varies"
        )
    }
}

// MARK: - Fire instrumentation: cross-episode memory positive boost (xsdz.9 ledger)

@Suite("Fbsignals cross-episode-memory positive-boost fire instrumentation (playhead-fbsignals.9)")
struct FbsignalsCrossEpisodePositiveFireTests {

    private func entry(_ source: EvidenceSourceType, weight: Double) -> EvidenceLedgerEntry {
        let detail: EvidenceLedgerDetail
        switch source {
        case .crossEpisodeMemory:
            // Mirror the production positive-boost entry's detail shape
            // (`CrossEpisodeMemoryEvaluator.buildPositiveBoostEntries`).
            detail = .fingerprint(matchCount: 1, averageSimilarity: 0.6)
        default:
            detail = .catalog(entryCount: 1)
        }
        return EvidenceLedgerEntry(source: source, weight: weight, detail: detail)
    }

    @Test("the channel tap counts a span only when .crossEpisodeMemory emitted a positive entry")
    func tap_countsCrossEpisodePositiveOnly() async {
        let tap = BrandAppearanceChannelTapObserver()
        // Span 1: crossEpisodeMemory positive boost fired.
        await tap.record(assetId: "ep1", ledger: [
            entry(.crossEpisodeMemory, weight: 0.3),
            entry(.acoustic, weight: 0.3),
        ])
        // Span 2: no crossEpisodeMemory entry at all.
        await tap.record(assetId: "ep1", ledger: [entry(.lexical, weight: 0.2)])
        // Span 3: a ZERO-weight crossEpisodeMemory entry must NOT count.
        await tap.record(assetId: "ep1", ledger: [entry(.crossEpisodeMemory, weight: 0.0)])

        let counts = await tap.fireCounts(for: "ep1")
        #expect(counts.observedSpans == 3)
        #expect(counts.crossEpisodeMemoryFiredSpans == 1)
        // The brand-appearance / audio-forensics channels stayed silent —
        // generalizing the tap did not perturb the existing counters.
        #expect(counts.rhetoricalGrammarFiredSpans == 0)
        #expect(counts.crossShowSyndicationFiredSpans == 0)
        #expect(counts.audioForensicsFiredSpans == 0)
    }

    @Test("the fbsignals fire tally folds channel-tap counts (positive boost + observed) additively")
    func fireTally_foldsChannelTapAdditively() {
        var tally = FbsignalsFireTally()
        tally.addChannelTap(BrandAppearanceChannelFireCounts(
            crossEpisodeMemoryFiredSpans: 3, observedSpans: 7
        ))
        tally.addChannelTap(BrandAppearanceChannelFireCounts(
            crossEpisodeMemoryFiredSpans: 2, observedSpans: 5
        ))
        #expect(tally.crossEpisodeMemoryPositiveFiredSpans == 5)
        #expect(tally.observedSpans == 12)
        // The xsdz.9 suppression + xsdz.11 counters are untouched by tap folding.
        #expect(tally.crossEpisodeMemorySuppressedSpans == 0)
        #expect(tally.perShowThresholdShiftedSpans == 0)
    }
}

// MARK: - Fire instrumentation: cross-episode memory negative-bank suppression (xsdz.9)

@Suite("Fbsignals negative-bank suppression fire instrumentation (playhead-fbsignals.9)")
struct FbsignalsNegativeSuppressionFireTests {

    @Test("the observer records suppressed + candidate counts per asset")
    func observer_recordsPerAsset() async {
        let observer = NegativeBankSuppressionObserver()
        // 3 candidate spans reached the suppression step; 1 actually suppressed.
        await observer.record(assetId: "epA", didSuppress: true)
        await observer.record(assetId: "epA", didSuppress: false)
        await observer.record(assetId: "epA", didSuppress: false)

        let counts = await observer.fireCounts(for: "epA")
        #expect(counts.candidateSpans == 3)
        #expect(counts.suppressedSpans == 1)

        // An unseen asset returns zeroed defaults (never a crash / nil ambiguity).
        let empty = await observer.fireCounts(for: "never-seen")
        #expect(empty == NegativeBankSuppressionFireCounts())
    }

    @Test("counts accumulate and are isolated across assets")
    func observer_accumulatesAndIsolates() async {
        let observer = NegativeBankSuppressionObserver()
        await observer.record(assetId: "epA", didSuppress: false)
        await observer.record(assetId: "epA", didSuppress: true)
        await observer.record(assetId: "epB", didSuppress: false)

        let a = await observer.fireCounts(for: "epA")
        let b = await observer.fireCounts(for: "epB")
        #expect(a.candidateSpans == 2)
        #expect(a.suppressedSpans == 1)
        #expect(b.candidateSpans == 1)
        #expect(b.suppressedSpans == 0)
    }

    @Test("the fbsignals fire tally folds negative-bank suppression counts additively")
    func fireTally_foldsSuppressionAdditively() {
        var tally = FbsignalsFireTally()
        tally.addNegativeBankSuppression(NegativeBankSuppressionFireCounts(suppressedSpans: 1, candidateSpans: 5))
        tally.addNegativeBankSuppression(NegativeBankSuppressionFireCounts(suppressedSpans: 2, candidateSpans: 4))
        #expect(tally.crossEpisodeMemorySuppressedSpans == 3)
        #expect(tally.crossEpisodeMemorySuppressionCandidateSpans == 9)
        // The positive-boost + xsdz.11 counters are untouched.
        #expect(tally.crossEpisodeMemoryPositiveFiredSpans == 0)
        #expect(tally.perShowThresholdShiftedSpans == 0)
    }
}

// MARK: - Fire instrumentation: per-show threshold offset (xsdz.11)

@Suite("Fbsignals per-show threshold fire instrumentation (playhead-fbsignals.11)")
struct FbsignalsPerShowThresholdFireTests {

    @Test("the observer records the resolved offset and per-span shift outcomes")
    func observer_recordsOffsetAndShifts() async {
        let observer = PerShowThresholdOffsetObserver()
        await observer.recordResolvedOffset(assetId: "epA", offset: 0.03)
        await observer.recordSpan(assetId: "epA", didShiftThreshold: true)
        await observer.recordSpan(assetId: "epA", didShiftThreshold: false)

        let counts = await observer.fireCounts(for: "epA")
        #expect(counts.resolvedOffset == 0.03)
        #expect(counts.candidateSpans == 2)
        #expect(counts.thresholdShiftedSpans == 1)

        // An unseen asset returns zeroed defaults.
        let empty = await observer.fireCounts(for: "never-seen")
        #expect(empty == PerShowThresholdOffsetFireCounts())
    }

    @Test("an EMPTY-controller cold-start records offset 0 and zero shifts (the expected live result)")
    func observer_emptyControllerColdStart() async {
        let observer = PerShowThresholdOffsetObserver()
        // Empty cold-start controller resolves offset 0; gate then shifts no span.
        await observer.recordResolvedOffset(assetId: "epColdStart", offset: 0)
        await observer.recordSpan(assetId: "epColdStart", didShiftThreshold: false)
        await observer.recordSpan(assetId: "epColdStart", didShiftThreshold: false)

        let counts = await observer.fireCounts(for: "epColdStart")
        #expect(counts.resolvedOffset == 0)
        #expect(counts.thresholdShiftedSpans == 0)
        #expect(counts.candidateSpans == 2)
    }

    @Test("the fbsignals fire tally folds per-show threshold counts additively (offsets sum)")
    func fireTally_foldsPerShowAdditively() {
        var tally = FbsignalsFireTally()
        tally.addPerShowThreshold(PerShowThresholdOffsetFireCounts(resolvedOffset: 0.02, thresholdShiftedSpans: 1, candidateSpans: 4))
        tally.addPerShowThreshold(PerShowThresholdOffsetFireCounts(resolvedOffset: 0, thresholdShiftedSpans: 0, candidateSpans: 3))
        #expect(tally.perShowThresholdShiftedSpans == 1)
        #expect(tally.perShowThresholdCandidateSpans == 7)
        #expect(tally.perShowThresholdOffsetSum == 0.02)
        // The xsdz.9 counters are untouched.
        #expect(tally.crossEpisodeMemoryPositiveFiredSpans == 0)
        #expect(tally.crossEpisodeMemorySuppressedSpans == 0)
    }
}

// MARK: - Report shape

@Suite("Fbsignals single-signal report (playhead-fbsignals)")
struct FbsignalsSweepReportTests {

    @Test("the report emits exactly the two arms, baseline first, with the signal label")
    func report_twoArmsBaselineFirst() {
        let arms: [FbsignalsArm] = [.baseline, .treatment]
        let accumulators: [FbsignalsArm: FusionLiftModeAccumulator] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, FusionLiftModeAccumulator()) }
        )
        var treatmentFire = FbsignalsFireTally()
        treatmentFire.crossEpisodeMemorySuppressionCandidateSpans = 20
        treatmentFire.observedSpans = 20
        let fireTallies: [FbsignalsArm: FbsignalsFireTally] = [
            .baseline: FbsignalsFireTally(),
            .treatment: treatmentFire,
        ]

        let report = FbsignalsSweepReport(
            signal: .crossEpisodeMemory,
            episodeCount: 12,
            arms: arms,
            accumulators: accumulators,
            fireTallies: fireTallies
        )
        #expect(report.signal == "crossEpisodeMemory")
        #expect(report.episodeCount == 12)
        #expect(report.rows.map(\.arm) == ["baseline", "treatment"])
        // The (expected-0) fire counts ride into the row so the JSON is interpretable.
        #expect(report.rows[1].crossEpisodeMemorySuppressedSpans == 0)
        #expect(report.rows[1].crossEpisodeMemorySuppressionCandidateSpans == 20)
        #expect(report.rows[1].observedSpans == 20)
        #expect(report.rows[0].crossEpisodeMemorySuppressedSpans == 0)
        // The baseline row's own deltas are zero (measured against itself).
        #expect(report.rows[0].truePositivesDelta == 0)
        #expect(report.rows[0].falsePositivesDelta == 0)
    }

    @Test("the report encodes to JSON with the signal field present")
    func report_encodesJSON() throws {
        let arms: [FbsignalsArm] = [.baseline, .treatment]
        let accumulators: [FbsignalsArm: FusionLiftModeAccumulator] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, FusionLiftModeAccumulator()) }
        )
        let fireTallies: [FbsignalsArm: FbsignalsFireTally] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, FbsignalsFireTally()) }
        )
        let report = FbsignalsSweepReport(
            signal: .perShowThreshold,
            episodeCount: 0,
            arms: arms,
            accumulators: accumulators,
            fireTallies: fireTallies
        )
        let data = try report.jsonData()
        let decoded = try JSONDecoder().decode(FbsignalsSweepReport.self, from: data)
        #expect(decoded == report)
        #expect(decoded.signal == "perShowThreshold")
    }
}

#endif
