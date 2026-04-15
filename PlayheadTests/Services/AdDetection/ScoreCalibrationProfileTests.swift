// ScoreCalibrationProfileTests.swift
// Tests for MonotonicCalibrator, ScoreCalibrationProfile, and CalibrationFeatureFlags.

import Foundation
import XCTest
@testable import Playhead

// MARK: - MonotonicCalibrator Tests

final class MonotonicCalibratorTests: XCTestCase {

    func testIdentityPassthrough() {
        let cal = MonotonicCalibrator.identity
        XCTAssertEqual(cal.calibrate(0.0), 0.0)
        XCTAssertEqual(cal.calibrate(0.5), 0.5)
        XCTAssertEqual(cal.calibrate(1.0), 1.0)
        XCTAssertEqual(cal.calibrate(0.123), 0.123)
    }

    func testIdentityClamping() {
        let cal = MonotonicCalibrator.identity
        XCTAssertEqual(cal.calibrate(-0.1), 0.0)
        XCTAssertEqual(cal.calibrate(1.5), 1.0)
    }

    func testIdentityNonFinite() {
        let cal = MonotonicCalibrator.identity
        XCTAssertEqual(cal.calibrate(Double.nan), 0.0)
        XCTAssertEqual(cal.calibrate(Double.infinity), 0.0)
        XCTAssertEqual(cal.calibrate(-Double.infinity), 0.0)
    }

    func testPiecewiseInterpolation() {
        let cal = MonotonicCalibrator(knots: [(0.0, 0.0), (0.5, 0.8), (1.0, 1.0)])
        XCTAssertEqual(cal.calibrate(0.0), 0.0)
        XCTAssertEqual(cal.calibrate(0.5), 0.8)
        XCTAssertEqual(cal.calibrate(1.0), 1.0)
        XCTAssertEqual(cal.calibrate(0.25), 0.4, accuracy: 1e-10)
        XCTAssertEqual(cal.calibrate(0.75), 0.9, accuracy: 1e-10)
    }

    func testPiecewiseClamping() {
        let cal = MonotonicCalibrator(knots: [(0.0, 0.0), (1.0, 1.0)])
        XCTAssertEqual(cal.calibrate(-0.5), 0.0)
        XCTAssertEqual(cal.calibrate(2.0), 1.0)
    }

    func testPiecewiseNonFinite() {
        let cal = MonotonicCalibrator(knots: [(0.0, 0.0), (1.0, 1.0)])
        XCTAssertEqual(cal.calibrate(Double.nan), 0.0)
        XCTAssertEqual(cal.calibrate(Double.infinity), 0.0)
    }

    func testCodableRoundTrip() throws {
        let cal = MonotonicCalibrator(knots: [(0.0, 0.0), (0.5, 0.7), (1.0, 1.0)])
        let data = try JSONEncoder().encode(cal)
        let decoded = try JSONDecoder().decode(MonotonicCalibrator.self, from: data)
        XCTAssertEqual(decoded.calibrate(0.25), cal.calibrate(0.25))
        XCTAssertEqual(decoded.calibrate(0.75), cal.calibrate(0.75))
    }

    func testSingleKnot() {
        let cal = MonotonicCalibrator(knots: [(0.5, 0.5)])
        XCTAssertEqual(cal.calibrate(0.0), 0.5)
        XCTAssertEqual(cal.calibrate(1.0), 0.5)
    }
}

// MARK: - ScoreCalibrationProfile Tests

final class ScoreCalibrationProfileXCTests: XCTestCase {

    func testV0Identity() {
        let profile = ScoreCalibrationProfile.v0
        XCTAssertEqual(profile.version, "v0")
        for source in EvidenceSourceType.allCases {
            let cal = profile.calibrator(for: source)
            XCTAssertEqual(cal.calibrate(0.0), 0.0)
            XCTAssertEqual(cal.calibrate(0.5), 0.5)
            XCTAssertEqual(cal.calibrate(1.0), 1.0)
        }
    }

    func testV0Thresholds() {
        let profile = ScoreCalibrationProfile.v0
        XCTAssertEqual(profile.decisionThresholds.skipMinimum, 0.0)
        XCTAssertEqual(profile.decisionThresholds.proposalMinimum, 0.0)
    }

    func testV1PlaceholderExists() {
        let v1 = ScoreCalibrationProfile.v1Placeholder
        XCTAssertEqual(v1.version, "v1")
        for source in EvidenceSourceType.allCases {
            let cal = v1.calibrator(for: source)
            XCTAssertEqual(cal.calibrate(0.5), 0.5)
        }
    }

    func testCodableRoundTrip() throws {
        let profile = ScoreCalibrationProfile.v0
        let data = try JSONEncoder().encode(profile)
        let decoded = try JSONDecoder().decode(ScoreCalibrationProfile.self, from: data)
        XCTAssertEqual(decoded.version, profile.version)
        XCTAssertEqual(decoded.decisionThresholds.skipMinimum, profile.decisionThresholds.skipMinimum)
    }
}

// MARK: - CalibrationFeatureFlags Tests

final class CalibrationFeatureFlagXCTests: XCTestCase {

    func testDefaultAllOff() {
        let flags = CalibrationFeatureFlags.allOff
        XCTAssertEqual(flags.phaseA, .off)
        XCTAssertEqual(flags.phaseB, .off)
        XCTAssertEqual(flags.phaseC, .off)
        XCTAssertEqual(flags.phaseD, .off)
        XCTAssertEqual(flags.phaseE, .off)
    }

    func testIndependentActivation() {
        var flags = CalibrationFeatureFlags.allOff
        flags.phaseA = .shadow
        XCTAssertEqual(flags.phaseA, .shadow)
        XCTAssertEqual(flags.phaseB, .off)

        flags.phaseB = .live
        XCTAssertEqual(flags.phaseA, .shadow)
        XCTAssertEqual(flags.phaseB, .live)
    }

    func testModeValues() {
        let modes: [FeatureFlagMode] = [.off, .shadow, .live]
        XCTAssertEqual(modes.count, 3)
    }

    func testIsActive() {
        XCTAssertFalse(FeatureFlagMode.off.isActive)
        XCTAssertTrue(FeatureFlagMode.shadow.isActive)
        XCTAssertTrue(FeatureFlagMode.live.isActive)
    }

    func testIsLive() {
        XCTAssertFalse(FeatureFlagMode.off.isLive)
        XCTAssertFalse(FeatureFlagMode.shadow.isLive)
        XCTAssertTrue(FeatureFlagMode.live.isLive)
    }

    func testCodableRoundTrip() throws {
        var flags = CalibrationFeatureFlags.allOff
        flags.phaseA = .shadow
        flags.phaseC = .live
        let data = try JSONEncoder().encode(flags)
        let decoded = try JSONDecoder().decode(CalibrationFeatureFlags.self, from: data)
        XCTAssertEqual(decoded.phaseA, .shadow)
        XCTAssertEqual(decoded.phaseB, .off)
        XCTAssertEqual(decoded.phaseC, .live)
    }
}

// MARK: - DecisionMapper Calibration Profile Tests

final class DecisionMapperCalibrationProfileTests: XCTestCase {

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

    func testV0ProfileIdentity() {
        let span = makeSpan()
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.25, detail: .classifier(score: 0.8)),
            EvidenceLedgerEntry(source: .lexical, weight: 0.15, detail: .lexical(matchedCategories: ["url"]))
        ]
        let config = FusionWeightConfig()

        let mapperOld = DecisionMapper(span: span, ledger: ledger, config: config)
        let resultOld = mapperOld.map()

        let mapperNew = DecisionMapper(
            span: span, ledger: ledger, config: config,
            calibrationProfile: .v0
        )
        let resultNew = mapperNew.map()

        XCTAssertEqual(resultNew.proposalConfidence, resultOld.proposalConfidence)
        XCTAssertEqual(resultNew.skipConfidence, resultOld.skipConfidence)
        XCTAssertEqual(resultNew.eligibilityGate, resultOld.eligibilityGate)
    }

    func testDefaultProfileIsV0() {
        let span = makeSpan()
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.3, detail: .classifier(score: 1.0))
        ]
        let config = FusionWeightConfig()
        let mapper = DecisionMapper(span: span, ledger: ledger, config: config)
        let result = mapper.map()
        XCTAssertEqual(result.skipConfidence, 0.3)
    }

    func testNanInputSafety() {
        let span = makeSpan()
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: Double.nan, detail: .classifier(score: 0.5))
        ]
        let config = FusionWeightConfig()
        let mapper = DecisionMapper(span: span, ledger: ledger, config: config, calibrationProfile: .v0)
        let result = mapper.map()
        XCTAssertTrue(result.skipConfidence.isFinite)
        XCTAssertTrue(result.proposalConfidence.isFinite)
    }

    func testCohortIncludesCalibrationVersion() {
        let cohort = DecisionCohort.production(appBuild: "42", calibrationVersion: "v0")
        XCTAssertEqual(cohort.calibrationVersion, "v0")
    }
}
