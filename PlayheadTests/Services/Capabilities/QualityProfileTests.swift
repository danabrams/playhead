// QualityProfileTests.swift
// Tests for QualityProfile derivation and scheduler policy mapping.
// playhead-5ih.

import Foundation
import Testing
import UIKit
@testable import Playhead

@Suite("QualityProfile — derivation")
struct QualityProfileDerivationTests {

    // MARK: - Thermal-only baseline (nominal battery, plugged in, no low-power)

    @Test("thermal nominal + healthy battery + charging + no low-power -> nominal")
    func testBaselineNominal() {
        let profile = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.80,
            batteryState: .charging,
            isLowPowerMode: false
        )
        #expect(profile == .nominal)
    }

    @Test("thermal fair + healthy battery + charging -> fair (baseline map)")
    func testBaselineFair() {
        let profile = QualityProfile.derive(
            thermalState: .fair,
            batteryLevel: 0.80,
            batteryState: .charging,
            isLowPowerMode: false
        )
        #expect(profile == .fair)
    }

    @Test("thermal serious + healthy battery + charging -> serious (baseline map)")
    func testBaselineSerious() {
        let profile = QualityProfile.derive(
            thermalState: .serious,
            batteryLevel: 0.80,
            batteryState: .charging,
            isLowPowerMode: false
        )
        #expect(profile == .serious)
    }

    @Test("thermal critical + healthy battery + charging -> critical (baseline map)")
    func testBaselineCritical() {
        let profile = QualityProfile.derive(
            thermalState: .critical,
            batteryLevel: 0.80,
            batteryState: .charging,
            isLowPowerMode: false
        )
        #expect(profile == .critical)
    }

    // MARK: - Low-power-mode demotions

    @Test("thermal nominal + low-power on -> demoted to fair")
    func testLowPowerDemotesNominalToFair() {
        let profile = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.80,
            batteryState: .charging,
            isLowPowerMode: true
        )
        #expect(profile == .fair)
    }

    @Test("thermal fair + low-power on -> demoted to serious")
    func testLowPowerDemotesFairToSerious() {
        let profile = QualityProfile.derive(
            thermalState: .fair,
            batteryLevel: 0.80,
            batteryState: .charging,
            isLowPowerMode: true
        )
        #expect(profile == .serious)
    }

    @Test("thermal serious + low-power on -> stays serious (no further demotion)")
    func testLowPowerDoesNotDemoteSerious() {
        let profile = QualityProfile.derive(
            thermalState: .serious,
            batteryLevel: 0.80,
            batteryState: .charging,
            isLowPowerMode: true
        )
        #expect(profile == .serious)
    }

    @Test("thermal critical + low-power on -> stays critical")
    func testLowPowerDoesNotDemoteCritical() {
        let profile = QualityProfile.derive(
            thermalState: .critical,
            batteryLevel: 0.80,
            batteryState: .charging,
            isLowPowerMode: true
        )
        #expect(profile == .critical)
    }

    // MARK: - Low-battery-unplugged demotions

    @Test("thermal nominal + low battery unplugged -> demoted to fair")
    func testLowBatteryUnpluggedDemotesNominal() {
        let profile = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.10,
            batteryState: .unplugged,
            isLowPowerMode: false
        )
        #expect(profile == .fair)
    }

    @Test("thermal fair + low battery unplugged -> demoted to serious")
    func testLowBatteryUnpluggedDemotesFair() {
        let profile = QualityProfile.derive(
            thermalState: .fair,
            batteryLevel: 0.10,
            batteryState: .unplugged,
            isLowPowerMode: false
        )
        #expect(profile == .serious)
    }

    @Test("thermal nominal + low battery while charging -> NOT demoted")
    func testLowBatteryWhileChargingDoesNotDemote() {
        let profile = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.10,
            batteryState: .charging,
            isLowPowerMode: false
        )
        #expect(profile == .nominal, "Charging prevents battery-level demotion")
    }

    @Test("thermal nominal + low battery full-plugged -> NOT demoted")
    func testLowBatteryWhenFullDoesNotDemote() {
        let profile = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.10,
            batteryState: .full,
            isLowPowerMode: false
        )
        #expect(profile == .nominal, ".full is treated as charging")
    }

    @Test("thermal nominal + battery at threshold (0.20) unplugged -> NOT demoted")
    func testBatteryAtThresholdDoesNotDemote() {
        let profile = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.20,
            batteryState: .unplugged,
            isLowPowerMode: false
        )
        #expect(profile == .nominal, "0.20 is the threshold; strictly-below triggers demotion")
    }

    @Test("thermal nominal + unknown battery (-1) -> NOT demoted")
    func testUnknownBatteryDoesNotDemote() {
        let profile = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: -1,
            batteryState: .unknown,
            isLowPowerMode: false
        )
        #expect(profile == .nominal, "Unknown battery level is treated as not-low")
    }

    // MARK: - Combined demotions (only one step)

    @Test("thermal nominal + low-power + low battery unplugged -> still only one step (fair)")
    func testCombinedDemotionIsStillOneStep() {
        let profile = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.05,
            batteryState: .unplugged,
            isLowPowerMode: true
        )
        #expect(profile == .fair, "Two demote signals still only demote by one step")
    }

    // MARK: - Unplugged healthy battery

    @Test("thermal nominal + healthy battery unplugged -> nominal")
    func testUnpluggedButHealthyBattery() {
        let profile = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.85,
            batteryState: .unplugged,
            isLowPowerMode: false
        )
        #expect(profile == .nominal)
    }
}

@Suite("QualityProfile — scheduler policy")
struct QualityProfileSchedulerPolicyTests {

    @Test("nominal: full slice, all lanes allowed")
    func testNominalPolicy() {
        let policy = QualityProfile.nominal.schedulerPolicy
        #expect(policy.sliceFraction == 1.0)
        #expect(policy.allowSoonLane == true)
        #expect(policy.allowBackgroundLane == true)
        #expect(policy.pauseAllWork == false)
    }

    @Test("fair: full slice, Background lane paused, Soon lane allowed")
    func testFairPolicy() {
        let policy = QualityProfile.fair.schedulerPolicy
        #expect(policy.sliceFraction == 1.0)
        #expect(policy.allowSoonLane == true)
        #expect(policy.allowBackgroundLane == false)
        #expect(policy.pauseAllWork == false)
    }

    @Test("serious: half slice, Soon + Background paused, T0 still runs")
    func testSeriousPolicy() {
        let policy = QualityProfile.serious.schedulerPolicy
        #expect(policy.sliceFraction == 0.5)
        #expect(policy.allowSoonLane == false)
        #expect(policy.allowBackgroundLane == false)
        #expect(policy.pauseAllWork == false)
    }

    @Test("critical: pause all work")
    func testCriticalPolicy() {
        let policy = QualityProfile.critical.schedulerPolicy
        #expect(policy.sliceFraction == 0.0)
        #expect(policy.allowSoonLane == false)
        #expect(policy.allowBackgroundLane == false)
        #expect(policy.pauseAllWork == true)
    }

    @Test("sliceFraction is monotonically non-increasing across severity")
    func testSliceFractionMonotonic() {
        let fractions = QualityProfile.allCases.map { $0.schedulerPolicy.sliceFraction }
        for i in 1..<fractions.count {
            #expect(fractions[i] <= fractions[i - 1],
                    "sliceFraction must not increase as profile worsens")
        }
    }
}
