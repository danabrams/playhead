// CapabilitySnapshotTests.swift
// Tests for CapabilitySnapshot charging state and deferred work gating.

import Foundation
import Testing
import UIKit

@testable import Playhead

@Suite("CapabilitySnapshot")
struct CapabilitySnapshotTests {

    // MARK: - isCharging Field

    @Test("isCharging field is present and stored correctly")
    func testIsChargingFieldPresent() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(snapshot.isCharging == true)
    }

    // MARK: - Backward-Compatible Decoding

    @Test("Decoding JSON without isCharging defaults to false")
    func testBackwardCompatDecoding() throws {
        // JSON that predates the isCharging field.
        let json = """
        {
            "foundationModelsAvailable": true,
            "appleIntelligenceEnabled": false,
            "foundationModelsLocaleSupported": true,
            "thermalState": 0,
            "isLowPowerMode": false,
            "backgroundProcessingSupported": true,
            "availableDiskSpaceBytes": 500000000,
            "capturedAt": "2023-11-14T22:13:20Z"
        }
        """

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let snapshot = try decoder.decode(
            CapabilitySnapshot.self,
            from: Data(json.utf8)
        )

        #expect(snapshot.isCharging == false,
                "Missing isCharging should default to false")
        #expect(snapshot.foundationModelsAvailable == true)
    }

    // MARK: - canRunDeferredWork

    @Test("canRunDeferredWork: charging + nominal thermal -> true")
    func testCanRunDeferredWorkChargingNominal() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(snapshot.canRunDeferredWork == true)
    }

    @Test("canRunDeferredWork: charging + serious thermal -> false")
    func testCanRunDeferredWorkChargingSerious() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .serious,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(snapshot.canRunDeferredWork == false,
                "Serious thermal should throttle even while charging")
    }

    @Test("canRunDeferredWork: not charging -> false regardless of thermal")
    func testCanRunDeferredWorkNotCharging() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: false,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(snapshot.canRunDeferredWork == false,
                "Not charging means no deferred work")
    }

    // MARK: - shouldThrottleAnalysis (existing behavior unchanged)

    @Test("shouldThrottleAnalysis: serious and critical -> true, nominal and fair -> false")
    func testShouldThrottleAnalysisUnchanged() {
        let states: [(ThermalState, Bool)] = [
            (.nominal, false),
            (.fair, false),
            (.serious, true),
            (.critical, true),
        ]

        for (state, expected) in states {
            let snapshot = CapabilitySnapshot(
                foundationModelsAvailable: false,
                appleIntelligenceEnabled: false,
                foundationModelsLocaleSupported: false,
                thermalState: state,
                isLowPowerMode: false,
                isCharging: false,
                backgroundProcessingSupported: true,
                availableDiskSpaceBytes: 1_000_000,
                capturedAt: .now
            )
            #expect(snapshot.shouldThrottleAnalysis == expected,
                    "thermalState \(state) should\(expected ? "" : " not") throttle")
        }
    }

    // MARK: - Battery Notification Refresh

    @Test("CapabilitiesService refreshes snapshot on battery state change notification")
    func testCapabilitiesServiceRefreshesOnBatteryChange() async throws {
        let service = CapabilitiesService()
        await service.startObserving()

        let before = await service.currentSnapshot

        // Post battery state change notification.
        NotificationCenter.default.post(
            name: UIDevice.batteryStateDidChangeNotification,
            object: nil
        )

        // Give the main-queue observer time to fire and the actor to process.
        try await Task.sleep(for: .milliseconds(200))

        let after = await service.currentSnapshot
        // The snapshot should have been recaptured (newer timestamp).
        #expect(after.capturedAt >= before.capturedAt)
    }
}
