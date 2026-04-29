// LowBatteryTests.swift
// playhead-rk7: degraded-conditions E2E — low battery (unplugged & charging).
//
// Pins the C1-narrowed low-battery behavior: under nominal thermal,
// plain low-battery (unplugged) demotes the QualityProfile from
// .nominal → .fair, but neither BPS gate fires. Background-lane work
// is throttled by `AnalysisWorkScheduler` (lane-level, separately
// covered by the lane scheduler suites). Charging always keeps the
// profile at .nominal regardless of level.
//
// Contract being pinned:
//   * battery 0.15 unplugged + nominal thermal → derives `.fair`;
//     BPS-level pauseAllWork = false, allowSoonLane = true → neither
//     hot-path nor backfill paused at the BPS layer.
//   * battery 0.15 unplugged + fair thermal → derives `.serious`;
//     BPS backfill gate fires (`!allowSoonLane`); foreground hot-path
//     stays open (`pauseAllWork` still false).
//   * battery 0.15 *charging* + nominal thermal → derives `.nominal`;
//     no demotion because charging.
//   * Recovery: plug-in returns the profile to `.nominal`,
//     re-opening both BPS gates.
//
// We mock ONLY the OS battery boundary via `StubBatteryProvider` and
// the LPM/thermal flags via `makeCapabilitySnapshot`. The BPS actor,
// coordinator stub, and task scheduler stub are the same doubles the
// production-style suites under `Services/PreAnalysis/` use.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-rk7 - low battery", .serialized)
struct LowBatteryTests {

    private func makeBPS(
        batteryLevel: Float,
        charging: Bool
    ) -> (BackgroundProcessingService, StubAnalysisCoordinator, StubBatteryProvider) {
        let coordinator = StubAnalysisCoordinator()
        let scheduler = StubTaskScheduler()
        let battery = StubBatteryProvider()
        battery.level = batteryLevel
        battery.charging = charging
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: scheduler,
            batteryProvider: battery
        )
        return (bps, coordinator, battery)
    }

    // MARK: - Test 1: unplugged low battery + nominal thermal keeps both BPS gates open

    @Test("Unplugged low battery + nominal thermal pauses neither hot-path nor backfill at BPS")
    func unpluggedLowBatteryNominalThermal() async throws {
        let (bps, coordinator, _) = makeBPS(batteryLevel: 0.15, charging: false)

        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .nominal))
        try await Task.sleep(for: .milliseconds(50))

        // Demoted to .fair, but .fair keeps allowSoonLane=true so the
        // BPS backfill gate stays open. The lane scheduler's Background
        // gate is the one that fires here, and that's covered by the
        // AnalysisWorkScheduler suites — out of scope at the BPS layer.
        #expect(
            coordinator.stopCallCount == 0,
            "Plain unplugged low battery must NOT trigger pauseAllWork at the BPS layer."
        )
        #expect(
            await bps.isHotPathActive() == true,
            "Hot-path must stay active under plain low battery — only critical thermal pauses it."
        )
        #expect(
            await bps.isBackfillPaused() == false,
            ".fair profile keeps allowSoonLane=true → BPS backfill gate must NOT fire."
        )
    }

    // MARK: - Test 2: unplugged low battery + fair thermal pauses backfill

    @Test("Unplugged low battery + .fair thermal pauses backfill but keeps hot-path active")
    func unpluggedLowBatteryFairThermal() async throws {
        let (bps, coordinator, _) = makeBPS(batteryLevel: 0.15, charging: false)

        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        // Baseline .fair + battery demotion → .serious. .serious clears
        // allowSoonLane → BPS backfill gate fires. pauseAllWork still
        // false → hot-path stays open.
        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .fair))
        try await Task.sleep(for: .milliseconds(50))

        #expect(
            coordinator.stopCallCount == 0,
            ".serious does NOT trigger pauseAllWork — only .critical does."
        )
        #expect(
            await bps.isHotPathActive() == true,
            "Hot-path stays open under .serious; only .critical pauses the foreground path."
        )
        #expect(
            await bps.isBackfillPaused() == true,
            ".serious clears allowSoonLane → BPS backfill gate must fire."
        )
    }

    // MARK: - Test 3: charging keeps profile nominal regardless of level

    @Test("Charging keeps profile .nominal regardless of low battery level")
    func chargingPreservesNominalProfile() async throws {
        let (bps, coordinator, _) = makeBPS(batteryLevel: 0.15, charging: true)

        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .nominal))
        try await Task.sleep(for: .milliseconds(50))

        // Charging suppresses the battery-level demotion. With nominal
        // thermal + charging, the profile stays .nominal so all gates
        // stay open.
        #expect(
            coordinator.stopCallCount == 0,
            "Charging at low level must NOT trigger pauseAllWork."
        )
        #expect(
            await bps.isHotPathActive() == true
        )
        #expect(
            await bps.isBackfillPaused() == false,
            "Charging keeps profile .nominal → both BPS gates must stay open."
        )
    }

    // MARK: - Test 4: recovery — plugging in restores .nominal profile

    @Test("Plugging in restores .nominal profile and re-opens BPS gates")
    func pluggingInRestoresGates() async throws {
        let coordinator = StubAnalysisCoordinator()
        let scheduler = StubTaskScheduler()
        let battery = StubBatteryProvider()
        battery.level = 0.15
        battery.charging = false
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: scheduler,
            batteryProvider: battery
        )

        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        // Start in the throttled state: low battery + .fair thermal → .serious
        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .fair))
        try await Task.sleep(for: .milliseconds(50))
        #expect(await bps.isBackfillPaused() == true)

        // Recovery: plug in. Battery state changes from `.unplugged` to
        // `.charging`, suppressing the battery-level demotion. With
        // baseline thermal back to .nominal, the profile returns to
        // .nominal and both gates re-open.
        battery.charging = true
        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .nominal))
        try await Task.sleep(for: .milliseconds(50))

        #expect(
            await bps.isHotPathActive() == true,
            "Hot-path must remain active after recovery."
        )
        #expect(
            await bps.isBackfillPaused() == false,
            "Recovery to .nominal must un-pause the BPS backfill gate."
        )
    }

    // MARK: - Test 5: pure QualityProfile derivation under low battery

    @Test("QualityProfile.derive: charging suppresses battery demotion, unplugged + low battery demotes nominal→fair")
    func qualityProfileLowBatteryDerivation() {
        // Charging + low battery + nominal thermal → .nominal (no demotion).
        let chargingLow = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.15,
            batteryState: .charging,
            isLowPowerMode: false
        )
        #expect(chargingLow == .nominal, "Charging must suppress the battery demotion.")

        // Unplugged + low battery + nominal thermal → .fair.
        let unpluggedLowNominal = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.15,
            batteryState: .unplugged,
            isLowPowerMode: false
        )
        #expect(unpluggedLowNominal == .fair, "Unplugged + low battery must demote nominal→fair.")

        // Unplugged + low battery + fair thermal → .serious.
        let unpluggedLowFair = QualityProfile.derive(
            thermalState: .fair,
            batteryLevel: 0.15,
            batteryState: .unplugged,
            isLowPowerMode: false
        )
        #expect(unpluggedLowFair == .serious, "Unplugged + low battery + .fair must demote to .serious.")

        // Battery exactly at threshold (0.20) is NOT considered low.
        let unpluggedAtThreshold = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: QualityProfile.lowBatteryThreshold,
            batteryState: .unplugged,
            isLowPowerMode: false
        )
        #expect(unpluggedAtThreshold == .nominal, "Battery at the threshold (0.20) must NOT trigger demotion.")

        // Unknown battery (-1, monitoring off) does NOT demote.
        let unknownBattery = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: -1,
            batteryState: .unknown,
            isLowPowerMode: false
        )
        #expect(unknownBattery == .nominal, "Unknown battery level (< 0) must NOT trigger demotion.")
    }
}
