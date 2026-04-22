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
// snapshots, and the observer's thermal-state mapping logic (`.nominal`
// OR `.fair` → true) is reached via a simple enum switch. Testing that
// mapping via the stream would require either system-level thermal
// simulation or a parallel fake CapabilitiesService — both out of scope
// for this unit test. The provider's state-plumbing is what we cover
// here; the switch itself is narrow enough that the production call site
// in PlayheadRuntime is the effective integration test.

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
}
