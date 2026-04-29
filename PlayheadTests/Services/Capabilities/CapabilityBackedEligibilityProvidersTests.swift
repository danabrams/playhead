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
        // playhead-kgn5: region was moved to a separate provider
        // (`LocaleRegionSupportProvider`) — this struct now carries four
        // of the five eligibility axes.
        let cache = CapabilitySnapshotCache()
        let providers = CapabilityBackedEligibilityProviders(cache: cache)
        #expect(providers.isHardwareSupported() == true)
        #expect(providers.isAppleIntelligenceEnabled() == true)
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

    @Test("Region axis is no longer carried by this provider (playhead-kgn5)")
    func regionAxisIsNotOnThisProvider() {
        // playhead-kgn5: the previous `isRegionSupported() -> true`
        // placeholder was lifted out of `CapabilityBackedEligibilityProviders`
        // and replaced by `LocaleRegionSupportProvider`, which reads
        // `Locale.current.region` against a US-only constant. Production
        // wiring composes this provider for the four snapshot-derived
        // axes and `LocaleRegionSupportProvider()` for the region slot.
        // See `LocaleRegionSupportProviderTests` for the region behavior;
        // this test pins the structural separation.
        #expect(LocaleRegionSupportProvider.supportedRegions == ["US"])
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
