import Foundation
import Testing
@testable import Playhead

@Suite("ApprovedCohortRegistry")
struct ApprovedCohortRegistryTests {
    @Test("Unknown cohort defaults to shadow even when caller requests a higher mode")
    func unknownCohortDefaultsToShadow() {
        let registry = ApprovedCohortRegistry()
        let mode = registry.effectiveMode(
            osBuild: "26.4",
            scanCohort: Self.cohort(promptHash: "prompt-a"),
            requestedMode: .full
        )

        #expect(mode == .shadow)
    }

    @Test("Unknown cohort does not override an explicit off request")
    func unknownCohortDoesNotOverrideExplicitOff() {
        let registry = ApprovedCohortRegistry()
        let mode = registry.effectiveMode(
            osBuild: "26.4",
            scanCohort: Self.cohort(promptHash: "prompt-off"),
            requestedMode: .off
        )

        #expect(mode == .off)
    }

    @Test("Approved cohort returns its stored approved mode")
    func approvedCohortReturnsStoredMode() {
        var registry = ApprovedCohortRegistry()
        let cohort = Self.cohort(promptHash: "prompt-approved")

        registry.approve(osBuild: "26.4", scanCohort: cohort, mode: .rescoreOnly)

        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort) == .rescoreOnly)
    }

    @Test("Approved cohort cannot elevate above the caller requested mode")
    func approvedCohortRespectsRequestedMode() {
        var registry = ApprovedCohortRegistry()
        let cohort = Self.cohort(promptHash: "prompt-requested-mode")

        registry.approve(osBuild: "26.4", scanCohort: cohort, mode: .full)

        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .off) == .off)
        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .shadow) == .shadow)
        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .rescoreOnly) == .rescoreOnly)
        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .proposalOnly) == .proposalOnly)
    }

    @Test("Approved mode intersects with requested mode capabilities")
    func approvedModeIntersectsWithRequestedCapabilities() {
        var registry = ApprovedCohortRegistry()
        let cohort = Self.cohort(promptHash: "prompt-intersection")

        registry.approve(osBuild: "26.4", scanCohort: cohort, mode: .rescoreOnly)

        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .full) == .rescoreOnly)
        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .proposalOnly) == .shadow)
    }

    @Test("Approving an existing cohort updates the mode explicitly")
    func approveUpdatesExistingMode() {
        var registry = ApprovedCohortRegistry()
        let cohort = Self.cohort(promptHash: "prompt-updated")

        registry.approve(osBuild: "26.4", scanCohort: cohort, mode: .rescoreOnly)
        registry.approve(osBuild: "26.4", scanCohort: cohort, mode: .proposalOnly)

        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort) == .proposalOnly)
    }

    @Test("Known-bad cohort is forced off and demotes any prior approval")
    func knownBadForcesOff() {
        var registry = ApprovedCohortRegistry()
        let cohort = Self.cohort(promptHash: "prompt-bad")

        registry.approve(osBuild: "26.4", scanCohort: cohort, mode: .full)
        registry.markKnownBad(osBuild: "26.4", scanCohort: cohort, reason: "benchmark regression")

        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .full) == .off)
        #expect(registry.decision(osBuild: "26.4", scanCohort: cohort)?.reason == "benchmark regression")
    }

    @Test("Approving a known-bad cohort transitions it back to the approved mode")
    func approvingKnownBadTransitionsBackToApproved() {
        var registry = ApprovedCohortRegistry()
        let cohort = Self.cohort(promptHash: "prompt-approve-after-bad")

        registry.markKnownBad(osBuild: "26.4", scanCohort: cohort, reason: "bad cohort")
        registry.approve(osBuild: "26.4", scanCohort: cohort, mode: .full)

        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort) == .full)
        #expect(registry.decision(osBuild: "26.4", scanCohort: cohort)?.approvedMode == .full)
        #expect(registry.decision(osBuild: "26.4", scanCohort: cohort)?.reason == nil)
    }

    @Test("Removing state returns the cohort to shadow")
    func removeStateReturnsToShadow() {
        var registry = ApprovedCohortRegistry()
        let cohort = Self.cohort(promptHash: "prompt-clear")

        registry.markKnownBad(osBuild: "26.4", scanCohort: cohort)
        registry.remove(osBuild: "26.4", scanCohort: cohort)

        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort) == .shadow)
        #expect(registry.decision(osBuild: "26.4", scanCohort: cohort) == nil)
    }

    @Test("Mode transition sequence preserves fail-closed rollout behavior")
    func modeTransitionSequence() {
        var registry = ApprovedCohortRegistry()
        let cohort = Self.cohort(promptHash: "prompt-transition")

        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .full) == .shadow)

        registry.approve(osBuild: "26.4", scanCohort: cohort, mode: .rescoreOnly)
        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .full) == .rescoreOnly)

        registry.approve(osBuild: "26.4", scanCohort: cohort, mode: .full)
        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .full) == .full)

        registry.markKnownBad(osBuild: "26.4", scanCohort: cohort, reason: "replay regression")
        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .full) == .off)

        registry.remove(osBuild: "26.4", scanCohort: cohort)
        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohort, requestedMode: .full) == .shadow)
    }

    @Test("OS build and canonical cohort identity are independent approval dimensions")
    func osBuildAndCohortIdentityArePartOfKey() {
        var registry = ApprovedCohortRegistry()
        let cohortA = Self.cohort(promptHash: "prompt-a")
        let cohortB = Self.cohort(promptHash: "prompt-b")
        let cohortAWithRuntimeOS = Self.cohort(promptHash: "prompt-a", osBuild: "runtime-captured-os")

        registry.approve(osBuild: "26.4", scanCohort: cohortA, mode: .full)

        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohortA) == .full)
        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohortAWithRuntimeOS) == .full)
        #expect(registry.effectiveMode(osBuild: "26.5", scanCohort: cohortA) == .shadow)
        #expect(registry.effectiveMode(osBuild: "26.4", scanCohort: cohortB) == .shadow)
    }

    private static func cohort(promptHash: String, osBuild: String = "26.4") -> ScanCohort {
        ScanCohort(
            promptLabel: "phase3-shadow-v1",
            promptHash: promptHash,
            schemaHash: "schema",
            scanPlanHash: "plan",
            normalizationHash: "norm",
            osBuild: osBuild,
            locale: "en_US",
            appBuild: "100"
        )
    }
}
