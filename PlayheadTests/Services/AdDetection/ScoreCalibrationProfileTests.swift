// ScoreCalibrationProfileTests.swift
// Tests for MonotonicCalibrator, DecisionThresholds, and ScoreCalibrationProfile.

import Foundation
import Testing
@testable import Playhead

@Suite("MonotonicCalibrator")
struct MonotonicCalibratorTests {

    // MARK: - Construction validation

    @Test("Identity calibrator passes through input unchanged")
    func identityCalibratorPassthrough() {
        let cal = MonotonicCalibrator.identity
        #expect(cal.calibrate(0.0) == 0.0)
        #expect(cal.calibrate(0.5) == 0.5)
        #expect(cal.calibrate(1.0) == 1.0)
        #expect(abs(cal.calibrate(0.37) - 0.37) < 1e-10)
    }

    @Test("Failable init rejects fewer than 2 knots")
    func rejectsSingleKnot() {
        let cal = MonotonicCalibrator(knots: [.init(x: 0.0, y: 0.0)])
        #expect(cal == nil)
    }

    @Test("Failable init rejects non-increasing x values")
    func rejectsNonIncreasingX() {
        let cal = MonotonicCalibrator(knots: [
            .init(x: 0.0, y: 0.0),
            .init(x: 0.5, y: 0.3),
            .init(x: 0.5, y: 0.6),  // duplicate x
        ])
        #expect(cal == nil)
    }

    @Test("Failable init rejects decreasing y values")
    func rejectsDecreasingY() {
        let cal = MonotonicCalibrator(knots: [
            .init(x: 0.0, y: 0.5),
            .init(x: 0.5, y: 0.3),  // y decreases
            .init(x: 1.0, y: 0.8),
        ])
        #expect(cal == nil)
    }

    @Test("Valid knots are accepted")
    func validKnotsAccepted() {
        let cal = MonotonicCalibrator(knots: [
            .init(x: 0.0, y: 0.0),
            .init(x: 0.5, y: 0.3),
            .init(x: 1.0, y: 1.0),
        ])
        #expect(cal != nil)
    }

    @Test("Equal y values (plateau) are valid for monotonically non-decreasing")
    func plateauYValuesValid() {
        let cal = MonotonicCalibrator(knots: [
            .init(x: 0.0, y: 0.0),
            .init(x: 0.5, y: 0.5),
            .init(x: 1.0, y: 0.5),  // plateau — valid
        ])
        #expect(cal != nil)
    }

    // MARK: - Interpolation

    @Test("Piecewise linear interpolation at midpoint of a segment")
    func interpolatesLinearly() {
        let cal = MonotonicCalibrator(knots: [
            .init(x: 0.0, y: 0.0),
            .init(x: 1.0, y: 0.5),
        ])!
        // Midpoint of [0,1] → [0, 0.5] should be 0.25
        #expect(abs(cal.calibrate(0.5) - 0.25) < 1e-10)
    }

    @Test("Interpolation clamps below first knot x")
    func clampsBelow() {
        let cal = MonotonicCalibrator(knots: [
            .init(x: 0.2, y: 0.1),
            .init(x: 0.8, y: 0.9),
        ])!
        #expect(cal.calibrate(0.0) == 0.1)
        #expect(cal.calibrate(-1.0) == 0.1)
    }

    @Test("Interpolation clamps above last knot x")
    func clampsAbove() {
        let cal = MonotonicCalibrator(knots: [
            .init(x: 0.0, y: 0.0),
            .init(x: 0.8, y: 0.7),
        ])!
        #expect(cal.calibrate(1.0) == 0.7)
        #expect(cal.calibrate(5.0) == 0.7)
    }

    @Test("NaN input returns 0.0")
    func nanReturnsZero() {
        let cal = MonotonicCalibrator.identity
        #expect(cal.calibrate(Double.nan) == 0.0)
    }

    @Test("Infinity input returns 0.0")
    func infinityReturnsZero() {
        let cal = MonotonicCalibrator.identity
        #expect(cal.calibrate(Double.infinity) == 0.0)
        #expect(cal.calibrate(-Double.infinity) == 0.0)
    }

    @Test("Interpolation at knot boundary returns exact knot y")
    func interpolationAtKnotBoundary() {
        let cal = MonotonicCalibrator(knots: [
            .init(x: 0.0, y: 0.0),
            .init(x: 0.3, y: 0.2),
            .init(x: 0.7, y: 0.6),
            .init(x: 1.0, y: 1.0),
        ])!
        #expect(abs(cal.calibrate(0.3) - 0.2) < 1e-10)
        #expect(abs(cal.calibrate(0.7) - 0.6) < 1e-10)
    }

    // MARK: - v1 non-identity transforms

    @Test("v1 FM calibrator produces non-identity output")
    func v1FMNonIdentity() {
        let profile = ScoreCalibrationProfile.v1
        let fmCal = profile.calibrator(for: .fm)
        // v1 FM at 0.4 should map to 0.30, not 0.40
        let result = fmCal.calibrate(0.4)
        #expect(abs(result - 0.30) < 1e-10, "v1 FM calibrator at 0.4 should produce 0.30")
        #expect(result != 0.4, "v1 FM calibrator must be non-identity")
    }

    @Test("v1 classifier calibrator produces non-identity output")
    func v1ClassifierNonIdentity() {
        let profile = ScoreCalibrationProfile.v1
        let cal = profile.calibrator(for: .classifier)
        let result = cal.calibrate(0.4)
        #expect(abs(result - 0.20) < 1e-10, "v1 classifier calibrator at 0.4 should produce 0.20")
        #expect(result != 0.4)
    }
}

// MARK: - DecisionThresholds

@Suite("DecisionThresholds")
struct DecisionThresholdsTests {

    @Test("Default thresholds have correct values")
    func defaultValues() {
        let t = DecisionThresholds.default
        #expect(t.candidate == 0.40)
        #expect(t.markOnly == 0.60)
        #expect(t.confirm == 0.70)
        #expect(t.autoSkip == 0.80)
    }

    @Test("Threshold ordering invariant: candidate <= markOnly <= confirm <= autoSkip")
    func orderingInvariant() {
        let t = DecisionThresholds.default
        #expect(t.candidate <= t.markOnly)
        #expect(t.markOnly <= t.confirm)
        #expect(t.confirm <= t.autoSkip)
    }

    @Test("Custom thresholds with valid ordering succeed")
    func customValidOrdering() {
        let t = DecisionThresholds(candidate: 0.30, markOnly: 0.50, confirm: 0.65, autoSkip: 0.90)
        #expect(t.candidate == 0.30)
        #expect(t.autoSkip == 0.90)
    }

    @Test("Equal thresholds are valid (degenerate case)")
    func equalThresholdsValid() {
        let t = DecisionThresholds(candidate: 0.50, markOnly: 0.50, confirm: 0.50, autoSkip: 0.50)
        #expect(t.candidate == 0.50)
    }

    @Test("validateAgainstCorpus stub does not crash")
    func validateAgainstCorpusStub() {
        let t = DecisionThresholds.default
        // Stub should log and return without error
        t.validateAgainstCorpus(corpusName: "test-corpus", spanCount: 42)
    }
}

// MARK: - ScoreCalibrationProfile

@Suite("ScoreCalibrationProfile")
struct ScoreCalibrationProfileTests {

    @Test("v0 profile uses identity calibrators for all sources")
    func v0IsIdentity() {
        let profile = ScoreCalibrationProfile.v0
        #expect(profile.version == .v0)

        for source in EvidenceSourceType.allCases {
            let cal = profile.calibrator(for: source)
            let testValues = [0.0, 0.1, 0.25, 0.5, 0.75, 1.0]
            for v in testValues {
                #expect(abs(cal.calibrate(v) - v) < 1e-10,
                        "v0 calibrator for \(source) must be identity at \(v)")
            }
        }
    }

    @Test("v1 profile has non-identity calibrators for all sources")
    func v1IsNonIdentity() {
        let profile = ScoreCalibrationProfile.v1
        #expect(profile.version == .v1)

        // Every source (except fusedScore which uses identity) should have a non-identity calibrator
        for source in EvidenceSourceType.allCases where source != .fusedScore {
            let cal = profile.calibrator(for: source)
            let testValues = [0.1, 0.3, 0.5, 0.7, 0.9]
            let hasNonIdentity = testValues.contains { abs(cal.calibrate($0) - $0) > 1e-6 }
            #expect(hasNonIdentity,
                    "v1 calibrator for \(source) must differ from identity at some point")
        }
    }

    @Test("v1 knots are valid (construction does not return nil)")
    func v1KnotsValid() {
        // If v1 knots were invalid, the force-unwrap in the static let would crash.
        // This test documents the compile-time safety guarantee.
        let profile = ScoreCalibrationProfile.v1
        #expect(profile.version == .v1)
    }

    @Test("v0 is the default calibration profile")
    func v0IsDefault() {
        // v0 should be usable as a default without behavioral change.
        let profile = ScoreCalibrationProfile.v0
        #expect(profile.version == .v0)
        #expect(profile.thresholds.candidate == 0.40)
    }
}

// MARK: - Calibrated fusion integration

@Suite("CalibratedFusion")
struct CalibratedFusionTests {

    private func makeSpan() -> DecodedSpan {
        let first = 100
        let last = 200
        return DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-1", firstAtomOrdinal: first, lastAtomOrdinal: last),
            assetId: "asset-1",
            firstAtomOrdinal: first,
            lastAtomOrdinal: last,
            startTime: 10.0,
            endTime: 40.0,
            anchorProvenance: []
        )
    }

    @Test("v0 calibration produces identical results to uncalibrated mapper")
    func v0ProducesIdenticalResults() {
        let span = makeSpan()
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7)),
            .init(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["url"])),
        ]

        let mapperV0 = DecisionMapper(
            span: span, ledger: entries, config: FusionWeightConfig(),
            calibrationProfile: .v0
        )
        let mapperDefault = DecisionMapper(
            span: span, ledger: entries, config: FusionWeightConfig()
        )

        let resultV0 = mapperV0.map()
        let resultDefault = mapperDefault.map()
        #expect(abs(resultV0.proposalConfidence - resultDefault.proposalConfidence) < 1e-10)
        #expect(abs(resultV0.skipConfidence - resultDefault.skipConfidence) < 1e-10)
    }

    @Test("v1 calibration produces different results than v0 for same ledger")
    func v1ProducesDifferentResults() {
        let span = makeSpan()
        let entries: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7)),
            .init(source: .lexical, weight: 0.18, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.15, detail: .acoustic(breakStrength: 0.7)),
        ]

        let mapperV0 = DecisionMapper(
            span: span, ledger: entries, config: FusionWeightConfig(),
            calibrationProfile: .v0
        )
        let mapperV1 = DecisionMapper(
            span: span, ledger: entries, config: FusionWeightConfig(),
            calibrationProfile: .v1
        )

        let resultV0 = mapperV0.map()
        let resultV1 = mapperV1.map()

        // v1 calibrators reshape contributions, so the sum should differ
        #expect(abs(resultV0.proposalConfidence - resultV1.proposalConfidence) > 0.01,
                "v1 must produce a different proposalConfidence than v0")
    }

    @Test("v1 calibration applies per-source transforms independently")
    func v1PerSourceCalibration() {
        let span = makeSpan()

        // Single FM entry
        let fmOnly: [EvidenceLedgerEntry] = [
            .init(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
        ]

        let mapperV0 = DecisionMapper(
            span: span, ledger: fmOnly, config: FusionWeightConfig(),
            calibrationProfile: .v0
        )
        let mapperV1 = DecisionMapper(
            span: span, ledger: fmOnly, config: FusionWeightConfig(),
            calibrationProfile: .v1
        )

        let v0Result = mapperV0.map()
        let v1Result = mapperV1.map()

        // v0: proposalConfidence = 0.35 (identity)
        #expect(abs(v0Result.proposalConfidence - 0.35) < 1e-10)
        // v1: FM at 0.35 is between knots (0.2, 0.10) and (0.4, 0.30), interpolated
        let expectedV1 = ScoreCalibrationProfile.v1.calibrator(for: .fm).calibrate(0.35)
        #expect(abs(v1Result.proposalConfidence - expectedV1) < 1e-10)
        #expect(expectedV1 != 0.35, "v1 FM calibrator must transform 0.35 to a different value")
    }
}
