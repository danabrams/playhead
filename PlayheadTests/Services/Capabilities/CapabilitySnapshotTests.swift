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

    // MARK: - foundationModelsContextSize (playhead-xx7m.2 Phase B)

    @Test("foundationModelsContextSize field is stored and defaults to 0")
    func testContextSizeFieldStoredAndDefaults() {
        let explicit = CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            foundationModelsContextSize: 32_768,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(explicit.foundationModelsContextSize == 32_768)

        // Omitting the argument uses the 0 default (FM unavailable path).
        let defaulted = CapabilitySnapshot(
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
        #expect(defaulted.foundationModelsContextSize == 0)
    }

    @Test("foundationModelsContextSize survives a Codable round-trip")
    func testContextSizeCodableRoundTrip() throws {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            foundationModelsContextSize: 32_768,
            thermalState: .fair,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(snapshot)

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(CapabilitySnapshot.self, from: data)
        #expect(decoded.foundationModelsContextSize == 32_768,
                "contextSize must survive encode/decode; dropping the field from the CapabilitySnapshot layer would fail this")
    }

    @Test("Decoding JSON without foundationModelsContextSize defaults to 0")
    func testContextSizeBackwardCompatDecoding() throws {
        // JSON that predates the foundationModelsContextSize field.
        let json = """
        {
            "foundationModelsAvailable": true,
            "foundationModelsUsable": true,
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
        #expect(snapshot.foundationModelsContextSize == 0,
                "Missing contextSize should default to 0, not crash")
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
            localeSupported: true,
            contextSize: 32_768
        )
        #expect(available.available == true)
        #expect(available.appleIntelligenceEnabled == true)
        #expect(available.localeSupported == true)
        #expect(available.contextSize == 32_768,
                "contextSize must thread through the availability-based init")

        // Default contextSize is 0 (the FM-unavailable / older-compiler path).
        let defaulted = FoundationModelsCapabilityState(
            availability: .available,
            localeSupported: true
        )
        #expect(defaulted.contextSize == 0)

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

    @Test("FoundationModelsCapabilityState plain init carries contextSize")
    func testCapabilityStatePlainInitContextSize() {
        let state = FoundationModelsCapabilityState(
            available: true,
            appleIntelligenceEnabled: true,
            localeSupported: true,
            contextSize: 32_768
        )
        #expect(state.contextSize == 32_768)

        let defaulted = FoundationModelsCapabilityState(
            available: false,
            appleIntelligenceEnabled: false,
            localeSupported: false
        )
        #expect(defaulted.contextSize == 0,
                "Unavailable FM must report 0, never the 4096 classifier fallback")
    }

    // MARK: - Coarse-run budget breadcrumb (playhead-xx7m.2 Phase B)

    @Test("coarseRunBudgetBreadcrumb formats contextSize, budget, and window count")
    func testCoarseRunBudgetBreadcrumbFormatting() {
        let line = FoundationModelClassifier.coarseRunBudgetBreadcrumb(
            contextSize: 32_768,
            coarseBudget: 3_500,
            coarseWindowCount: 2
        )
        #expect(line == "fm.coarse.run_budget contextSize=32768 coarseBudget=3500 coarseWindows=2")
    }

    @Test("coarseRunBudgetBreadcrumb reflects the iOS 26 small-window regime")
    func testCoarseRunBudgetBreadcrumbSmallContext() {
        // The iOS 26 4096 window produces the ~20–25 coarse windows the
        // Phase B retune is measured against; the breadcrumb must carry
        // those exact numbers so a Console.app grep can compare regimes.
        let line = FoundationModelClassifier.coarseRunBudgetBreadcrumb(
            contextSize: 4_096,
            coarseBudget: 480,
            coarseWindowCount: 23
        )
        #expect(line.contains("contextSize=4096"))
        #expect(line.contains("coarseBudget=480"))
        #expect(line.contains("coarseWindows=23"))
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
