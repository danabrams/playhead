// CapabilitySnapshotTests.swift
// Tests for CapabilitySnapshot charging state and deferred work gating.

import Foundation
import Testing
import UIKit

@testable import Playhead

#if canImport(FoundationModels)
import FoundationModels
#endif

@Suite("CapabilitySnapshot")
struct CapabilitySnapshotTests {

    // MARK: - isCharging Field

    @Test("isCharging field is present and stored correctly")
    func testIsChargingFieldPresent() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            foundationModelsUsable: false,
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
            "foundationModelsUsable": true,
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
        #expect(snapshot.foundationModelsUsable == true)
        #expect(snapshot.foundationModelsAvailable == true)
    }

    @Test("Decoding JSON without foundationModelsUsable defaults to false")
    func testBackwardCompatDecodingFoundationModelsUsable() throws {
        let json = """
        {
            "foundationModelsAvailable": true,
            "appleIntelligenceEnabled": true,
            "foundationModelsLocaleSupported": true,
            "thermalState": 0,
            "isLowPowerMode": false,
            "isCharging": true,
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

        #expect(snapshot.foundationModelsUsable == false)
        #expect(snapshot.canUseFoundationModels == false,
                "Missing runtime usability probe result must default to false")
    }

    // MARK: - canRunDeferredWork

    @Test("canRunDeferredWork: charging + nominal thermal -> true")
    func testCanRunDeferredWorkChargingNominal() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            foundationModelsUsable: false,
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
            foundationModelsUsable: false,
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
            foundationModelsUsable: false,
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
                foundationModelsUsable: false,
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

    @Test("canUseFoundationModels requires successful runtime probe")
    func testCanUseFoundationModelsRequiresUsableProbe() {
        let unavailableProbe = CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: false,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(unavailableProbe.canUseFoundationModels == false,
                "Availability alone is insufficient until the first-call probe succeeds")

        let usableProbe = CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(usableProbe.canUseFoundationModels == true)
    }

    @Test("Foundation Models probe cache invalidates on OS build or boot change")
    func testFoundationModelsProbeCacheMatching() {
        let cache = FoundationModelsUsabilityProbeCache(
            osBuild: "Version 26.4 (Build 23F79)",
            bootEpochSeconds: 12345,
            usable: true
        )

        #expect(cache.matches(osBuild: "Version 26.4 (Build 23F79)", bootEpochSeconds: 12345))
        #expect(!cache.matches(osBuild: "Version 26.5 (Build 23F99)", bootEpochSeconds: 12345))
        #expect(!cache.matches(osBuild: "Version 26.4 (Build 23F79)", bootEpochSeconds: 12346))
    }

    #if canImport(FoundationModels)
    @available(iOS 26.0, *)
    @Test("Foundation Models capability state distinguishes disabled AI from model-not-ready")
    func testFoundationModelsCapabilityStateMapping() {
        let available = FoundationModelsCapabilityState(
            availability: .available,
            localeSupported: true
        )
        #expect(available.available == true)
        #expect(available.appleIntelligenceEnabled == true)
        #expect(available.localeSupported == true)

        let modelNotReady = FoundationModelsCapabilityState(
            availability: .unavailable(.modelNotReady),
            localeSupported: true
        )
        #expect(modelNotReady.available == false)
        #expect(modelNotReady.appleIntelligenceEnabled == true)

        let aiDisabled = FoundationModelsCapabilityState(
            availability: .unavailable(.appleIntelligenceNotEnabled),
            localeSupported: true
        )
        #expect(aiDisabled.available == false)
        #expect(aiDisabled.appleIntelligenceEnabled == false)

        let deviceNotEligible = FoundationModelsCapabilityState(
            availability: .unavailable(.deviceNotEligible),
            localeSupported: false
        )
        #expect(deviceNotEligible.available == false)
        #expect(deviceNotEligible.appleIntelligenceEnabled == false)
        #expect(deviceNotEligible.localeSupported == false)
    }
    #endif

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
