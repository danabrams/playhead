// LiveEnvironmentSignalProviderTests.swift
// playhead-narl.2 continuation: focused coverage for the synchronous
// getters and the DEBUG test hook on the live environment signal
// provider. Swift Testing suite.
//
// Coverage strategy:
//   - Construct the provider with a real `CapabilitiesService`, then drive
//     the internal snapshot via the `#if DEBUG` test hook
//     `_testingSetSnapshot`.
//   - Synchronous reads follow each `_testingSetSnapshot` with NO `await`
//     between set and read so the background `capabilityUpdates()`
//     consumer Task cannot interleave and clobber the test's value.
//
// Why we don't thread a synthetic `CapabilitySnapshot` through the real
// service: `CapabilitiesService` doesn't expose a DEBUG seam for injecting
// snapshots. The observer's thermal-state mapping logic is now covered
// directly via the pure static `thermalIsNominal(_:)` helper (see the
// "Q2=B thermal mapping" tests below) — no need to drive the live
// stream just to exercise the enum switch. The provider's lock-mirror
// plumbing is covered by the three existing tests.

import Foundation
import Testing

@testable import Playhead

@Suite("LiveEnvironmentSignalProvider (playhead-narl.2)")
struct LiveEnvironmentSignalProviderTests {

    @Test("thermalStateIsNominal reflects the snapshot's thermalNominal leg")
    func thermalLegReflectsSnapshot() async throws {
        let service = CapabilitiesService()
        let provider = LiveEnvironmentSignalProvider(capabilitiesService: service)

        provider._testingSetSnapshot(thermalNominal: true, charging: false)
        #expect(provider.thermalStateIsNominal() == true)

        provider._testingSetSnapshot(thermalNominal: false, charging: false)
        #expect(provider.thermalStateIsNominal() == false)
    }

    @Test("deviceIsCharging reflects the snapshot's charging leg")
    func chargingLegReflectsSnapshot() async throws {
        let service = CapabilitiesService()
        let provider = LiveEnvironmentSignalProvider(capabilitiesService: service)

        provider._testingSetSnapshot(thermalNominal: false, charging: true)
        #expect(provider.deviceIsCharging() == true)

        provider._testingSetSnapshot(thermalNominal: false, charging: false)
        #expect(provider.deviceIsCharging() == false)
    }

    @Test("legs are independent — AND composition is the coordinator's job")
    func legsAreIndependent() async throws {
        let service = CapabilitiesService()
        let provider = LiveEnvironmentSignalProvider(capabilitiesService: service)

        // Thermal true, charging false — Lane B gate would reject, but the
        // protocol's two getters MUST report independently so the
        // coordinator can compose them.
        provider._testingSetSnapshot(thermalNominal: true, charging: false)
        #expect(provider.thermalStateIsNominal() == true)
        #expect(provider.deviceIsCharging() == false)

        // And the other way around.
        provider._testingSetSnapshot(thermalNominal: false, charging: true)
        #expect(provider.thermalStateIsNominal() == false)
        #expect(provider.deviceIsCharging() == true)
    }

    // MARK: - Q2=B thermal mapping

    // The locked Q2=B decision is "`.nominal` or `.fair` → true;
    // `.serious` or `.critical` → false". All four cases of
    // `ThermalState` (defined in `CapabilitySnapshot.swift`) must map
    // correctly. A regression here silently flips Lane B's gate
    // threshold, so each case is exercised explicitly.

    @Test(".nominal thermal state maps to true (Q2=B)")
    func thermalMappingNominalIsTrue() {
        #expect(LiveEnvironmentSignalProvider.thermalIsNominal(.nominal) == true)
    }

    @Test(".fair thermal state maps to true (Q2=B widens the threshold)")
    func thermalMappingFairIsTrue() {
        #expect(LiveEnvironmentSignalProvider.thermalIsNominal(.fair) == true)
    }

    @Test(".serious thermal state maps to false (Q2=B refuses)")
    func thermalMappingSeriousIsFalse() {
        #expect(LiveEnvironmentSignalProvider.thermalIsNominal(.serious) == false)
    }

    @Test(".critical thermal state maps to false (Q2=B refuses)")
    func thermalMappingCriticalIsFalse() {
        #expect(LiveEnvironmentSignalProvider.thermalIsNominal(.critical) == false)
    }
}
