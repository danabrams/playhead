// ActempoArmConfigTests.swift
// playhead-actempo live A/B — hermetic, SYNTHETIC unit tests for the
// audio-forensics / temporal-reg single-signal A/B support in
// `FusionLiftHarnessSupport.swift` plus the two fire-instrumentation observers.
// No audio, no Foundation Models, no live pipeline — every input is a hand-built
// value, so these run on the simulator in the default `PlayheadFastTests` plan
// (they do NOT need `PLAYHEAD_ACTEMPO_AB=1`; that env var only gates the SLOW
// live harness in `AudioForensicsTemporalRegLiveABTests`).
//
// They pin the LOAD-BEARING correctness properties before the (expensive,
// Catalyst-only) live A/B ever runs:
//   1. ARM ISOLATION (the load-bearing property): for EACH signal the two arms
//      differ ONLY in that signal's single flag (`audioForensicsEnabled` for
//      xsdz.8, `temporalRegularizationEnabled` for xsdz.10); every other field is
//      byte-identical and equal to `AdDetectionConfig.default` — INCLUDING the
//      OTHER signal's flag (no cross-contamination between the two A/Bs).
//   2. BASELINE == PRODUCTION: each signal's baseline arm equals
//      `AdDetectionConfig.default` on every field.
//   3. FIRE INSTRUMENTATION: the nil-default observers record the correct
//      per-signal fire counts so a null live result is interpretable —
//      `.audioForensics` ledger entries (xsdz.8) and penalty-applied spans
//      (xsdz.10).

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("Actempo A/B arm config (playhead-actempo)")
struct ActempoArmConfigTests {

    // MARK: - Arm + signal enumeration

    @Test("the A/B enumerates exactly two signals")
    func signal_twoSignals() {
        #expect(ActempoSignal.allCases == [.audioForensics, .temporalRegularization])
    }

    @Test("each signal owns the correct varying field name")
    func signal_varyingFieldName() {
        #expect(ActempoSignal.audioForensics.varyingFieldName == "audioForensicsEnabled")
        #expect(ActempoSignal.temporalRegularization.varyingFieldName == "temporalRegularizationEnabled")
    }

    @Test("the A/B enumerates exactly two arms, baseline first")
    func arm_twoArmsBaselineFirst() {
        #expect(ActempoArm.allCases == [.baseline, .treatment])
    }

    @Test("each arm's signalEnabled flag matches its intent")
    func arm_signalFlagPerArm() {
        #expect(ActempoArm.baseline.signalEnabled == false)
        #expect(ActempoArm.treatment.signalEnabled == true)
    }

    // MARK: - Per-arm config flags

    @Test("each signal's treatment arm flips ONLY that signal's flag")
    func config_treatmentFlipsOnlyOwnFlag() {
        // xsdz.8 treatment: audioForensics ON, temporalReg still OFF (= .default).
        let af = ActempoArmConfig.adDetectionConfig(signal: .audioForensics, for: .treatment)
        #expect(af.audioForensicsEnabled == true)
        #expect(af.temporalRegularizationEnabled == AdDetectionConfig.default.temporalRegularizationEnabled)
        #expect(af.temporalRegularizationEnabled == false)

        // xsdz.10 treatment: temporalReg ON, audioForensics still OFF (= .default).
        let tr = ActempoArmConfig.adDetectionConfig(signal: .temporalRegularization, for: .treatment)
        #expect(tr.temporalRegularizationEnabled == true)
        #expect(tr.audioForensicsEnabled == AdDetectionConfig.default.audioForensicsEnabled)
        #expect(tr.audioForensicsEnabled == false)
    }

    @Test("each signal's baseline arm has its signal OFF (= production default)")
    func config_baselineSignalOff() {
        for signal in ActempoSignal.allCases {
            let baseline = ActempoArmConfig.adDetectionConfig(signal: signal, for: .baseline)
            #expect(baseline.audioForensicsEnabled == false, "signal \(signal.rawValue): af must be off in baseline")
            #expect(baseline.temporalRegularizationEnabled == false, "signal \(signal.rawValue): tr must be off in baseline")
        }
    }

    // MARK: - The load-bearing isolation property

    @Test("for each signal, the two arms differ ONLY in that signal's flag")
    func config_isolation_onlyOneFlagVaries() {
        for signal in ActempoSignal.allCases {
            let baseline = ActempoArmConfig.adDetectionConfig(signal: signal, for: .baseline)
            let treatment = ActempoArmConfig.adDetectionConfig(signal: signal, for: .treatment)

            // The one field the signal IS allowed to vary does differ.
            switch signal {
            case .audioForensics:
                #expect(baseline.audioForensicsEnabled != treatment.audioForensicsEnabled)
            case .temporalRegularization:
                #expect(baseline.temporalRegularizationEnabled != treatment.temporalRegularizationEnabled)
            }

            // EVERY other field (including the OTHER signal's flag) is byte-identical
            // across the two arms.
            for field in ActempoArmConfig.comparableFields(for: signal) {
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
        for signal in ActempoSignal.allCases {
            for arm in ActempoArm.allCases {
                let config = ActempoArmConfig.adDetectionConfig(signal: signal, for: arm)
                for field in ActempoArmConfig.comparableFields(for: signal) {
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
        // xsdz.8: excludes audioForensicsEnabled, KEEPS temporalRegularizationEnabled
        // (so cross-contamination — the other A/B's flag drifting — is caught).
        let afNames = Set(ActempoArmConfig.comparableFields(for: .audioForensics).map(\.name))
        #expect(!afNames.contains("audioForensicsEnabled"))
        #expect(afNames.contains("temporalRegularizationEnabled"))

        let trNames = Set(ActempoArmConfig.comparableFields(for: .temporalRegularization).map(\.name))
        #expect(!trNames.contains("temporalRegularizationEnabled"))
        #expect(trNames.contains("audioForensicsEnabled"))

        // Still non-vacuous: representative non-flag fields present in both.
        for names in [afNames, trNames] {
            #expect(names.contains("fmBackfillMode"))
            #expect(names.contains("autoSkipConfidenceThreshold"))
            #expect(names.contains("chapterSignalMode"))
        }
    }

    // MARK: - Baseline pinned to the production state

    @Test("each signal's baseline equals AdDetectionConfig.default on every field")
    func config_baselineEqualsProductionDefault() {
        let prod = AdDetectionConfig.default
        for signal in ActempoSignal.allCases {
            let baseline = ActempoArmConfig.adDetectionConfig(signal: signal, for: .baseline)
            // Every comparable field equals .default.
            for field in ActempoArmConfig.comparableFields(for: signal) {
                #expect(
                    field.value(baseline) == field.value(prod),
                    "signal \(signal.rawValue): baseline field \(field.name) diverged from default"
                )
            }
            // And the varied flag itself equals .default (off) in the baseline.
            #expect(baseline.audioForensicsEnabled == prod.audioForensicsEnabled)
            #expect(baseline.temporalRegularizationEnabled == prod.temporalRegularizationEnabled)
        }
    }

    @Test("baseline pins the explicit production flag/mode invariants the bead names")
    func config_baselineNamedInvariants() {
        for signal in ActempoSignal.allCases {
            let baseline = ActempoArmConfig.adDetectionConfig(signal: signal, for: .baseline)
            // fmBackfillMode .full → real FM scan feeds the fusion ledger.
            #expect(baseline.fmBackfillMode == .full)
            #expect(baseline.chapterSignalMode == .off)
            // ALL off-by-default evidence-channel flags FALSE on the baseline.
            #expect(baseline.audioForensicsEnabled == false)
            #expect(baseline.temporalRegularizationEnabled == false)
            #expect(baseline.evidenceFragilityPenaltyEnabled == false)
            #expect(baseline.crossEpisodeMemoryEnabled == false)
            #expect(baseline.rhetoricalGrammarEnabled == false)
            #expect(baseline.crossShowSyndicationEnabled == false)
            #expect(baseline.lexicalAutoAdEnabled == false)
        }
    }

    @Test("temporal-reg treatment keeps the production-default tuning knobs (only the master flag flips)")
    func config_temporalRegTreatmentUsesDefaultTuning() {
        let prod = AdDetectionConfig.default
        let treatment = ActempoArmConfig.adDetectionConfig(signal: .temporalRegularization, for: .treatment)
        #expect(treatment.temporalRegularizationEnabled == true)
        #expect(treatment.temporalNeighborWindowSeconds == prod.temporalNeighborWindowSeconds)
        #expect(treatment.temporalHighConfidenceNeighborThreshold == prod.temporalHighConfidenceNeighborThreshold)
        #expect(treatment.temporalIsolationPenaltyFactor == prod.temporalIsolationPenaltyFactor)
        #expect(treatment.temporalMinDwellSeconds == prod.temporalMinDwellSeconds)
        #expect(treatment.temporalMinDwellPenaltyFactor == prod.temporalMinDwellPenaltyFactor)
    }

    // MARK: - NarrowingConfig invariant (snap ON for every arm)

    @Test("every arm uses NarrowingConfig.default (snap ON) and the config never varies")
    func narrowing_everyArmDefaultSnapOn() {
        for arm in ActempoArm.allCases {
            let narrowing = ActempoArmConfig.narrowingConfig(for: arm)
            #expect(narrowing == NarrowingConfig.default, "arm \(arm.rawValue): narrowing must be .default")
            #expect(narrowing.lexicalClusterSnapEnabled == true,
                    "arm \(arm.rawValue): snap must be on (production state)")
        }
        #expect(
            ActempoArmConfig.narrowingConfig(for: .baseline)
                == ActempoArmConfig.narrowingConfig(for: .treatment),
            "narrowing must be identical across arms — only the one signal flag varies"
        )
    }
}

// MARK: - Fire instrumentation: audio-forensics (xsdz.8) via the channel tap

@Suite("Actempo audio-forensics fire instrumentation (playhead-actempo.8)")
struct ActempoAudioForensicsFireTests {

    private func entry(_ source: EvidenceSourceType, weight: Double) -> EvidenceLedgerEntry {
        let detail: EvidenceLedgerDetail
        switch source {
        case .audioForensics:
            detail = .audioForensics(boundaryScore: 0.7, dominantSignal: "loudnessJump", contributingSignalCount: 2)
        default:
            detail = .catalog(entryCount: 1)
        }
        return EvidenceLedgerEntry(source: source, weight: weight, detail: detail)
    }

    @Test("the channel tap counts a span only when .audioForensics emitted a positive entry")
    func tap_countsAudioForensicsPositiveOnly() async {
        let tap = BrandAppearanceChannelTapObserver()
        // Span 1: audioForensics fired.
        await tap.record(assetId: "ep1", ledger: [
            entry(.audioForensics, weight: 0.4),
            entry(.acoustic, weight: 0.3),
        ])
        // Span 2: no audioForensics entry at all.
        await tap.record(assetId: "ep1", ledger: [entry(.lexical, weight: 0.2)])
        // Span 3: a ZERO-weight audioForensics entry must NOT count.
        await tap.record(assetId: "ep1", ledger: [entry(.audioForensics, weight: 0.0)])

        let counts = await tap.fireCounts(for: "ep1")
        #expect(counts.observedSpans == 3)
        #expect(counts.audioForensicsFiredSpans == 1)
        // The brand-appearance channels stayed silent — generalizing the tap did
        // not perturb the existing counters.
        #expect(counts.rhetoricalGrammarFiredSpans == 0)
        #expect(counts.crossShowSyndicationFiredSpans == 0)
    }

    @Test("the actempo fire tally folds audio-forensics tap counts additively")
    func fireTally_foldsAudioForensicsAdditively() {
        var tally = ActempoFireTally()
        tally.addAudioForensics(BrandAppearanceChannelFireCounts(
            audioForensicsFiredSpans: 3, observedSpans: 7
        ))
        tally.addAudioForensics(BrandAppearanceChannelFireCounts(
            audioForensicsFiredSpans: 2, observedSpans: 5
        ))
        #expect(tally.audioForensicsFiredSpans == 5)
        #expect(tally.observedSpans == 12)
        // The temporal-reg counters are untouched by audio-forensics folding.
        #expect(tally.temporalRegPenaltyAppliedSpans == 0)
        #expect(tally.temporalRegCandidateSpans == 0)
    }
}

// MARK: - Fire instrumentation: temporal regularization (xsdz.10)

@Suite("Actempo temporal-reg fire instrumentation (playhead-actempo.10)")
struct ActempoTemporalRegFireTests {

    @Test("the observer records the penalty-applied + candidate counts per asset")
    func observer_recordsPerAsset() async {
        let observer = TemporalRegularizationObserver()
        // One backfill over an asset: 5 candidates, 2 penalized.
        await observer.record(assetId: "epA", candidateSpans: 5, penaltyAppliedSpans: 2)

        let counts = await observer.fireCounts(for: "epA")
        #expect(counts.candidateSpans == 5)
        #expect(counts.penaltyAppliedSpans == 2)

        // An unseen asset returns zeroed defaults (never a crash / nil ambiguity).
        let empty = await observer.fireCounts(for: "never-seen")
        #expect(empty == TemporalRegularizationFireCounts())
    }

    @Test("counts accumulate across records and are isolated across assets")
    func observer_accumulatesAndIsolates() async {
        let observer = TemporalRegularizationObserver()
        await observer.record(assetId: "epA", candidateSpans: 4, penaltyAppliedSpans: 1)
        await observer.record(assetId: "epA", candidateSpans: 3, penaltyAppliedSpans: 2)
        await observer.record(assetId: "epB", candidateSpans: 6, penaltyAppliedSpans: 0)

        let a = await observer.fireCounts(for: "epA")
        let b = await observer.fireCounts(for: "epB")
        #expect(a.candidateSpans == 7)
        #expect(a.penaltyAppliedSpans == 3)
        #expect(b.candidateSpans == 6)
        #expect(b.penaltyAppliedSpans == 0)
    }

    @Test("the actempo fire tally folds temporal-reg counts additively")
    func fireTally_foldsTemporalRegAdditively() {
        var tally = ActempoFireTally()
        tally.addTemporalReg(TemporalRegularizationFireCounts(penaltyAppliedSpans: 2, candidateSpans: 5))
        tally.addTemporalReg(TemporalRegularizationFireCounts(penaltyAppliedSpans: 1, candidateSpans: 4))
        #expect(tally.temporalRegPenaltyAppliedSpans == 3)
        #expect(tally.temporalRegCandidateSpans == 9)
        // The audio-forensics counters are untouched by temporal-reg folding.
        #expect(tally.audioForensicsFiredSpans == 0)
        #expect(tally.observedSpans == 0)
    }
}

// MARK: - Report shape

@Suite("Actempo single-signal report (playhead-actempo)")
struct ActempoSweepReportTests {

    @Test("the report emits exactly the two arms, baseline first, with the signal label")
    func report_twoArmsBaselineFirst() {
        let arms: [ActempoArm] = [.baseline, .treatment]
        let accumulators: [ActempoArm: FusionLiftModeAccumulator] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, FusionLiftModeAccumulator()) }
        )
        var treatmentFire = ActempoFireTally()
        treatmentFire.audioForensicsFiredSpans = 4
        treatmentFire.observedSpans = 20
        let fireTallies: [ActempoArm: ActempoFireTally] = [
            .baseline: ActempoFireTally(),
            .treatment: treatmentFire,
        ]

        let report = ActempoSweepReport(
            signal: .audioForensics,
            episodeCount: 12,
            arms: arms,
            accumulators: accumulators,
            fireTallies: fireTallies
        )
        #expect(report.signal == "audioForensics")
        #expect(report.episodeCount == 12)
        #expect(report.rows.map(\.arm) == ["baseline", "treatment"])
        // The fire count rides into the row so the JSON is interpretable.
        #expect(report.rows[1].audioForensicsFiredSpans == 4)
        #expect(report.rows[1].observedSpans == 20)
        #expect(report.rows[0].audioForensicsFiredSpans == 0)
        // The baseline row's own deltas are zero (measured against itself).
        #expect(report.rows[0].truePositivesDelta == 0)
        #expect(report.rows[0].falsePositivesDelta == 0)
    }

    @Test("the report encodes to JSON with the signal field present")
    func report_encodesJSON() throws {
        let arms: [ActempoArm] = [.baseline, .treatment]
        let accumulators: [ActempoArm: FusionLiftModeAccumulator] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, FusionLiftModeAccumulator()) }
        )
        let fireTallies: [ActempoArm: ActempoFireTally] = Dictionary(
            uniqueKeysWithValues: arms.map { ($0, ActempoFireTally()) }
        )
        let report = ActempoSweepReport(
            signal: .temporalRegularization,
            episodeCount: 0,
            arms: arms,
            accumulators: accumulators,
            fireTallies: fireTallies
        )
        let data = try report.jsonData()
        let decoded = try JSONDecoder().decode(ActempoSweepReport.self, from: data)
        #expect(decoded == report)
        #expect(decoded.signal == "temporalRegularization")
    }
}

#endif
