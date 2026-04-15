// ScoreCalibrationProfileTests.swift
// Tests for MonotonicCalibrator, ScoreCalibrationProfile, and CalibrationFeatureFlags.
//
// TDD: tests written first to specify the contract before implementation.

import Foundation
import Testing
@testable import Playhead

@Suite("MonotonicCalibrator")
struct MonotonicCalibratorTests {

    // MARK: - Identity calibrator

    @Test("Identity calibrator returns input unchanged for values in [0,1]")
    func identityPassthrough() {
        let cal = MonotonicCalibrator.identity
        #expect(cal.calibrate(0.0) == 0.0)
        #expect(cal.calibrate(0.5) == 0.5)
        #expect(cal.calibrate(1.0) == 1.0)
        #expect(cal.calibrate(0.123) == 0.123)
    }

    @Test("Identity calibrator clamps out-of-range values")
    func identityClamping() {
        let cal = MonotonicCalibrator.identity
        #expect(cal.calibrate(-0.1) == 0.0)
        #expect(cal.calibrate(1.5) == 1.0)
    }

    @Test("Identity calibrator handles NaN and Inf")
    func identityNonFinite() {
        let cal = MonotonicCalibrator.identity
        #expect(cal.calibrate(Double.nan) == 0.0)
        #expect(cal.calibrate(Double.infinity) == 0.0)
        #expect(cal.calibrate(-Double.infinity) == 0.0)
    }

    // MARK: - Piecewise-linear calibrator

    @Test("Piecewise-linear calibrator interpolates between knots")
    func piecewiseInterpolation() {
        // Knots: (0,0), (0.5,0.8), (1.0,1.0) — steeper in the low range
        let cal = MonotonicCalibrator(knots: [(0.0, 0.0), (0.5, 0.8), (1.0, 1.0)])
        #expect(cal.calibrate(0.0) == 0.0)
        #expect(cal.calibrate(0.5) == 0.8)
        #expect(cal.calibrate(1.0) == 1.0)
        // Midpoint of first segment: 0.25 → 0.4
        #expect(abs(cal.calibrate(0.25) - 0.4) < 1e-10)
        // Midpoint of second segment: 0.75 → 0.9
        #expect(abs(cal.calibrate(0.75) - 0.9) < 1e-10)
    }

    @Test("Piecewise-linear calibrator clamps out-of-range inputs")
    func piecewiseClamping() {
        let cal = MonotonicCalibrator(knots: [(0.0, 0.0), (1.0, 1.0)])
        #expect(cal.calibrate(-0.5) == 0.0)
        #expect(cal.calibrate(2.0) == 1.0)
    }

    @Test("Piecewise-linear calibrator handles NaN/Inf")
    func piecewiseNonFinite() {
        let cal = MonotonicCalibrator(knots: [(0.0, 0.0), (1.0, 1.0)])
        #expect(cal.calibrate(Double.nan) == 0.0)
        #expect(cal.calibrate(Double.infinity) == 0.0)
    }

    @Test("MonotonicCalibrator is Codable")
    func codableRoundTrip() throws {
        let cal = MonotonicCalibrator(knots: [(0.0, 0.0), (0.5, 0.7), (1.0, 1.0)])
        let data = try JSONEncoder().encode(cal)
        let decoded = try JSONDecoder().decode(MonotonicCalibrator.self, from: data)
        #expect(decoded.calibrate(0.25) == cal.calibrate(0.25))
        #expect(decoded.calibrate(0.75) == cal.calibrate(0.75))
    }

    @Test("Single-knot calibrator returns that value for all inputs")
    func singleKnot() {
        // Edge case: only one knot point
        let cal = MonotonicCalibrator(knots: [(0.5, 0.5)])
        #expect(cal.calibrate(0.0) == 0.5)
        #expect(cal.calibrate(1.0) == 0.5)
    }
}

@Suite("ScoreCalibrationProfile")
struct ScoreCalibrationProfileTests {

    // MARK: - v0 identity profile

    @Test("v0 profile preserves identity for all source types")
    func v0Identity() {
        let profile = ScoreCalibrationProfile.v0
        #expect(profile.version == "v0")

        // All source types should pass through unchanged
        for source in EvidenceSourceType.allCases {
            let cal = profile.calibrator(for: source)
            #expect(cal.calibrate(0.0) == 0.0)
            #expect(cal.calibrate(0.5) == 0.5)
            #expect(cal.calibrate(1.0) == 1.0)
        }
    }

    @Test("v0 profile has identity decision thresholds")
    func v0Thresholds() {
        let profile = ScoreCalibrationProfile.v0
        // v0 thresholds must not alter current behavior
        #expect(profile.decisionThresholds.skipMinimum == 0.0)
        #expect(profile.decisionThresholds.proposalMinimum == 0.0)
    }

    @Test("v1Placeholder profile exists and is distinct from v0")
    func v1PlaceholderExists() {
        let v1 = ScoreCalibrationProfile.v1Placeholder
        #expect(v1.version == "v1")
        // v1 placeholder still uses identity calibrators (no real calibrators yet)
        for source in EvidenceSourceType.allCases {
            let cal = v1.calibrator(for: source)
            #expect(cal.calibrate(0.5) == 0.5)
        }
    }

    @Test("ScoreCalibrationProfile is Codable")
    func codableRoundTrip() throws {
        let profile = ScoreCalibrationProfile.v0
        let data = try JSONEncoder().encode(profile)
        let decoded = try JSONDecoder().decode(ScoreCalibrationProfile.self, from: data)
        #expect(decoded.version == profile.version)
        #expect(decoded.decisionThresholds.skipMinimum == profile.decisionThresholds.skipMinimum)
    }
}

@Suite("CalibrationFeatureFlags")
struct CalibrationFeatureFlagTests {

    @Test("Default flags have all phases disabled")
    func defaultAllOff() {
        let flags = CalibrationFeatureFlags.allOff
        #expect(flags.phaseA == .off)
        #expect(flags.phaseB == .off)
        #expect(flags.phaseC == .off)
        #expect(flags.phaseD == .off)
        #expect(flags.phaseE == .off)
    }

    @Test("Individual phases can be activated independently")
    func independentActivation() {
        var flags = CalibrationFeatureFlags.allOff
        flags.phaseA = .shadow
        #expect(flags.phaseA == .shadow)
        #expect(flags.phaseB == .off)

        flags.phaseB = .live
        #expect(flags.phaseA == .shadow)
        #expect(flags.phaseB == .live)
    }

    @Test("FeatureFlagMode has off/shadow/live cases")
    func modeValues() {
        let modes: [FeatureFlagMode] = [.off, .shadow, .live]
        #expect(modes.count == 3)
    }

    @Test("isActive returns true for shadow and live, false for off")
    func isActive() {
        #expect(FeatureFlagMode.off.isActive == false)
        #expect(FeatureFlagMode.shadow.isActive == true)
        #expect(FeatureFlagMode.live.isActive == true)
    }

    @Test("isLive returns true only for live")
    func isLive() {
        #expect(FeatureFlagMode.off.isLive == false)
        #expect(FeatureFlagMode.shadow.isLive == false)
        #expect(FeatureFlagMode.live.isLive == true)
    }

    @Test("CalibrationFeatureFlags is Codable")
    func codableRoundTrip() throws {
        var flags = CalibrationFeatureFlags.allOff
        flags.phaseA = .shadow
        flags.phaseC = .live
        let data = try JSONEncoder().encode(flags)
        let decoded = try JSONDecoder().decode(CalibrationFeatureFlags.self, from: data)
        #expect(decoded.phaseA == .shadow)
        #expect(decoded.phaseB == .off)
        #expect(decoded.phaseC == .live)
    }
}

@Suite("DecisionMapper calibration with profile")
struct DecisionMapperCalibrationProfileTests {

    private func makeSpan(
        startTime: Double = 10.0,
        endTime: Double = 40.0,
        anchorProvenance: [AnchorRef] = []
    ) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "a1", firstAtomOrdinal: 100, lastAtomOrdinal: 200),
            assetId: "a1",
            firstAtomOrdinal: 100,
            lastAtomOrdinal: 200,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: anchorProvenance
        )
    }

    @Test("DecisionMapper with v0 profile produces identical results to current identity mapping")
    func v0ProfileIdentity() {
        let span = makeSpan()
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.25, detail: .classifier(score: 0.8)),
            EvidenceLedgerEntry(source: .lexical, weight: 0.15, detail: .lexical(matchedCategories: ["url"]))
        ]
        let config = FusionWeightConfig()

        // Without profile (current behavior)
        let mapperOld = DecisionMapper(span: span, ledger: ledger, config: config)
        let resultOld = mapperOld.map()

        // With v0 profile (must be identical)
        let mapperNew = DecisionMapper(
            span: span, ledger: ledger, config: config,
            calibrationProfile: .v0
        )
        let resultNew = mapperNew.map()

        #expect(resultNew.proposalConfidence == resultOld.proposalConfidence)
        #expect(resultNew.skipConfidence == resultOld.skipConfidence)
        #expect(resultNew.eligibilityGate == resultOld.eligibilityGate)
    }

    @Test("DecisionMapper defaults to v0 profile when none specified")
    func defaultProfileIsV0() {
        let span = makeSpan()
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.3, detail: .classifier(score: 1.0))
        ]
        let config = FusionWeightConfig()
        let mapper = DecisionMapper(span: span, ledger: ledger, config: config)
        let result = mapper.map()

        // 0.3 clamped to [0,1] = 0.3. Identity calibration preserves it.
        #expect(result.skipConfidence == 0.3)
    }

    @Test("DecisionMapper with NaN weight in ledger does not crash and produces finite result")
    func nanInputSafety() {
        let span = makeSpan()
        // NaN weight in ledger — Swift min(1.0, NaN) returns 1.0 (non-NaN wins),
        // so the NaN is swallowed before reaching calibrate(). The result must be
        // finite regardless.
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: Double.nan, detail: .classifier(score: 0.5))
        ]
        let config = FusionWeightConfig()
        let mapper = DecisionMapper(span: span, ledger: ledger, config: config, calibrationProfile: .v0)
        let result = mapper.map()
        #expect(result.skipConfidence.isFinite)
        #expect(result.proposalConfidence.isFinite)
    }

    @Test("DecisionCohort.production includes calibration profile version")
    func cohortIncludesCalibrationVersion() {
        let cohort = DecisionCohort.production(appBuild: "42", calibrationVersion: "v0")
        #expect(cohort.calibrationVersion == "v0")
    }
}
