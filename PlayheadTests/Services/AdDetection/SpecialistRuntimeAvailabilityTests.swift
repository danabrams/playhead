// SpecialistRuntimeAvailabilityTests.swift
// playhead-b6jq PR 1: link-state probe for the vendored CoreAILM package.
//
// The vendored specialist runtime (Vendor/coreai-models) is a
// device-only capability. These tests pin the probe's contract per
// destination; the device-side "package links with zero FoundationModels
// symbols" check is a manual nm/otool verification step (no test host on
// device in CI), documented in the PR.

import Testing
@testable import Playhead

@Suite("SpecialistRuntimeAvailability probe")
struct SpecialistRuntimeAvailabilityTests {

    /// The probe must compile in every destination and report the link
    /// state consistently with its compilation conditions. On simulator
    /// (where FastTests run) the specialist runtime is compiled out and
    /// the probe MUST report false; on a device build with the vendored
    /// CoreAILM product linked it reports true.
    @Test func probeMatchesCompilationConditions() {
        #if targetEnvironment(simulator)
        #expect(SpecialistRuntimeAvailability.isSpecialistRuntimeLinkable == false)
        #elseif canImport(CoreAILanguageModels)
        #expect(SpecialistRuntimeAvailability.isSpecialistRuntimeLinkable == true)
        #else
        #expect(SpecialistRuntimeAvailability.isSpecialistRuntimeLinkable == false)
        #endif
    }

    /// The probe is a pure value read: it must be stable across reads
    /// (no hidden state, no engine spin-up side effects).
    @Test func probeIsStableAcrossReads() {
        let first = SpecialistRuntimeAvailability.isSpecialistRuntimeLinkable
        let second = SpecialistRuntimeAvailability.isSpecialistRuntimeLinkable
        #expect(first == second)
    }
}
