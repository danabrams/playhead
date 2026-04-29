// CapabilityBackedEligibilityProvidersTests.swift
// Pins the field-by-field mapping from `CapabilitySnapshot` to the
// five `AnalysisEligibility*Providing` protocols. Drives the production
// wiring used by `EpisodeSurfaceStatusObserver` (playhead-4nt1).

import Foundation
import Testing
@testable import Playhead

@Suite("CapabilityBackedEligibilityProviders mapping (playhead-4nt1)")
struct CapabilityBackedEligibilityProvidersTests {

    private static func makeSnapshot(
        foundationModelsAvailable: Bool = true,
        foundationModelsUsable: Bool = true,
        appleIntelligenceEnabled: Bool = true,
        foundationModelsLocaleSupported: Bool = true
    ) -> CapabilitySnapshot {
        CapabilitySnapshot(
            foundationModelsAvailable: foundationModelsAvailable,
            foundationModelsUsable: foundationModelsUsable,
            appleIntelligenceEnabled: appleIntelligenceEnabled,
            foundationModelsLocaleSupported: foundationModelsLocaleSupported,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 10_000_000_000,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    @Test("Empty cache (nil snapshot) returns permissive defaults on every axis")
    func emptyCacheIsPermissive() {
        let cache = CapabilitySnapshotCache()
        let providers = CapabilityBackedEligibilityProviders(cache: cache)
        #expect(providers.isHardwareSupported() == true)
        #expect(providers.isAppleIntelligenceEnabled() == true)
        #expect(providers.isRegionSupported() == true)
        #expect(providers.isLanguageSupported() == true)
        #expect(providers.isModelAvailableNow() == true)
    }

    @Test("Hardware axis tracks foundationModelsAvailable")
    func hardwareTracksFMAvailable() {
        let cache = CapabilitySnapshotCache()
        let providers = CapabilityBackedEligibilityProviders(cache: cache)

        cache.set(Self.makeSnapshot(foundationModelsAvailable: true))
        #expect(providers.isHardwareSupported() == true)

        cache.set(Self.makeSnapshot(foundationModelsAvailable: false))
        #expect(providers.isHardwareSupported() == false)
    }

    @Test("AppleIntelligence axis tracks the snapshot field")
    func aiTracksSnapshot() {
        let cache = CapabilitySnapshotCache()
        let providers = CapabilityBackedEligibilityProviders(cache: cache)

        cache.set(Self.makeSnapshot(appleIntelligenceEnabled: true))
        #expect(providers.isAppleIntelligenceEnabled() == true)

        cache.set(Self.makeSnapshot(appleIntelligenceEnabled: false))
        #expect(providers.isAppleIntelligenceEnabled() == false)
    }

    @Test("Region axis is always true today (no live region provider)")
    func regionIsAlwaysTrue() {
        // Documented gap: no live region API. The seam exists so a
        // future bead can swap in a real provider without touching the
        // observer or the evaluator. Pin the current behavior so the
        // swap is intentional.
        let cache = CapabilitySnapshotCache()
        let providers = CapabilityBackedEligibilityProviders(cache: cache)
        cache.set(Self.makeSnapshot())
        #expect(providers.isRegionSupported() == true)
    }

    @Test("Language axis tracks foundationModelsLocaleSupported")
    func languageTracksLocale() {
        let cache = CapabilitySnapshotCache()
        let providers = CapabilityBackedEligibilityProviders(cache: cache)

        cache.set(Self.makeSnapshot(foundationModelsLocaleSupported: true))
        #expect(providers.isLanguageSupported() == true)

        cache.set(Self.makeSnapshot(foundationModelsLocaleSupported: false))
        #expect(providers.isLanguageSupported() == false)
    }

    @Test("ModelAvailability axis tracks foundationModelsUsable")
    func modelAvailabilityTracksUsable() {
        let cache = CapabilitySnapshotCache()
        let providers = CapabilityBackedEligibilityProviders(cache: cache)

        cache.set(Self.makeSnapshot(foundationModelsUsable: true))
        #expect(providers.isModelAvailableNow() == true)

        cache.set(Self.makeSnapshot(foundationModelsUsable: false))
        #expect(providers.isModelAvailableNow() == false)
    }

    @Test("Cache initial seed is observable")
    func initialSeedIsObservable() {
        let snapshot = Self.makeSnapshot(foundationModelsAvailable: false)
        let cache = CapabilitySnapshotCache(initial: snapshot)
        let providers = CapabilityBackedEligibilityProviders(cache: cache)
        #expect(providers.isHardwareSupported() == false)
    }
}
