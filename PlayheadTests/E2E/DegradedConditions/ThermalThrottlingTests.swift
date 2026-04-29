// ThermalThrottlingTests.swift
// playhead-rk7: degraded-conditions E2E — thermal throttling.
//
// Pins the two-tier QualityProfile gate against the real
// `BackgroundProcessingService` actor:
//
//   thermal state    pauseAllWork    !allowSoonLane    behavior
//   .nominal         no              no                hot-path active, backfill not paused
//   .fair            no              no                hot-path active, backfill not paused
//   .serious         no              YES               hot-path active, backfill paused
//   .critical        YES             YES               coordinator.stop() invoked, hot-path NOT active
//
// Also pins:
//   * `AnalysisCoordinator.thermalBackfillAdmission(thermalState:)` — the
//     static helper the coordinator queries when transitioning from
//     hot-path-ready into backfill: `.proceed` for nominal/fair/serious,
//     `.wait` for critical. The two layers (BPS gate vs. coordinator
//     admission helper) cover different parts of the same matrix and
//     this suite checks both so a future "consolidation" rewrite can't
//     silently drop one.
//   * `BackgroundProcessingService.hotPathLookaheadMultiplier()` — drops
//     to 0.5 under .serious thermal so Stage 3 ASR compute doesn't burn
//     the device further; returns 1.0 at .nominal.
//
// We mock ONLY the OS-environment boundary (the CapabilitySnapshot fed
// through `handleCapabilityUpdate`); BPS, the coordinator stub, the task
// scheduler stub, and the battery provider stub are all the same doubles
// production-style tests use under `Services/PreAnalysis/`.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-rk7 - thermal throttling", .serialized)
struct ThermalThrottlingTests {

    // MARK: - Test 1: .serious pauses backfill, hot-path stays active

    @Test("Serious thermal pauses backfill but keeps hot-path active")
    func seriousPausesBackfillKeepsHotPath() async throws {
        let coordinator = StubAnalysisCoordinator()
        let scheduler = StubTaskScheduler()
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: scheduler,
            batteryProvider: battery
        )

        // Hot-path activated by playback start; this is the steady state
        // the user was in when thermal escalated.
        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))
        #expect(await bps.isHotPathActive() == true)

        let serious = makeCapabilitySnapshot(thermalState: .serious)
        await bps.handleCapabilityUpdate(serious)
        try await Task.sleep(for: .milliseconds(50))

        #expect(
            coordinator.stopCallCount == 0,
            ".serious must NOT trigger pauseAllWork — only .critical does. Saw \(coordinator.stopCallCount) stop call(s)."
        )
        #expect(
            await bps.isHotPathActive() == true,
            "Hot-path must remain active under .serious thermal so the user keeps getting skip cues."
        )
        #expect(
            await bps.isBackfillPaused() == true,
            ".serious clears `allowSoonLane` → BPS-level backfill gate must fire."
        )
    }

    // MARK: - Test 2: .critical pauses ALL analysis

    @Test("Critical thermal pauses all analysis and stops the coordinator")
    func criticalPausesAll() async throws {
        let coordinator = StubAnalysisCoordinator()
        let scheduler = StubTaskScheduler()
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: scheduler,
            batteryProvider: battery
        )

        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        let critical = makeCapabilitySnapshot(thermalState: .critical)
        await bps.handleCapabilityUpdate(critical)
        // The coordinator.stop() path runs in a detached Task — give it a
        // moment to land before we assert.
        try await Task.sleep(for: .milliseconds(100))

        #expect(
            coordinator.stopCallCount >= 1,
            ".critical must invoke coordinator.stop() exactly once per transition; saw \(coordinator.stopCallCount)."
        )
        #expect(
            await bps.isHotPathActive() == false,
            "Hot-path must report inactive when allAnalysisPaused is set."
        )
        #expect(
            await bps.isBackfillPaused() == true,
            "Backfill gate is `pauseAllWork || !allowSoonLane`; .critical sets pauseAllWork → backfill paused."
        )
    }

    // MARK: - Test 3: recovery path — .critical → .nominal resumes hot-path

    @Test("Recovery from critical thermal lifts pause flag and resumes hot-path")
    func recoveryFromCriticalResumesHotPath() async throws {
        let coordinator = StubAnalysisCoordinator()
        let scheduler = StubTaskScheduler()
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: scheduler,
            batteryProvider: battery
        )

        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .critical))
        try await Task.sleep(for: .milliseconds(100))
        #expect(coordinator.stopCallCount >= 1)
        #expect(await bps.isHotPathActive() == false)

        // Recover. The capability observer survives stop(), so the
        // coordinator does not need a re-start; only the BPS pause flag
        // gets lifted and hot-path returns true.
        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .nominal))
        try await Task.sleep(for: .milliseconds(50))

        #expect(
            await bps.isHotPathActive() == true,
            "Recovery to .nominal must reactivate the hot-path because playback was still active."
        )
        #expect(
            await bps.isBackfillPaused() == false,
            "At .nominal, allowSoonLane is true and pauseAllWork is false — backfill must un-pause."
        )
        #expect(
            coordinator.startCapabilityObserverCallCount == 0,
            "Capability observer survives stop() — recovery must NOT re-init it."
        )
    }

    // MARK: - Test 4: AnalysisCoordinator.thermalBackfillAdmission contract

    @Test("AnalysisCoordinator.thermalBackfillAdmission proceeds for nominal/fair/serious, waits for critical")
    func thermalBackfillAdmissionMatrix() {
        // The coordinator-level helper is the second of the two thermal
        // gates — it parks the post-hot-path session in `.waitingForBackfill`
        // when the device is too hot to start the deeper passes. Pin every
        // arm so a future refactor can't silently change the threshold.
        #expect(AnalysisCoordinator.thermalBackfillAdmission(thermalState: .nominal) == .proceed)
        #expect(AnalysisCoordinator.thermalBackfillAdmission(thermalState: .fair) == .proceed)
        #expect(AnalysisCoordinator.thermalBackfillAdmission(thermalState: .serious) == .proceed,
                ".serious gates the BPS Soon lane but not the coordinator-level admission — backfill *can* start, it's the BPS gate that throttles it.")
        #expect(AnalysisCoordinator.thermalBackfillAdmission(thermalState: .critical) == .wait,
                ".critical must park the session in `.waitingForBackfill`; otherwise we'd burn battery on a thermal-locked device.")
    }

    // MARK: - Test 5: hotPathLookaheadMultiplier degrades under .serious

    @Test("Hot-path lookahead multiplier drops to 0.5 under .serious thermal")
    func hotPathLookaheadDropsUnderSerious() async throws {
        let coordinator = StubAnalysisCoordinator()
        let scheduler = StubTaskScheduler()
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: scheduler,
            batteryProvider: battery
        )

        // Baseline: nominal thermal → full lookahead.
        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .nominal))
        try await Task.sleep(for: .milliseconds(20))
        #expect(
            await bps.hotPathLookaheadMultiplier() == 1.0,
            "Nominal thermal must run the hot-path at full lookahead (1.0×)."
        )

        // Escalate to .serious — multiplier halves so Stage 3 transcript
        // depth shrinks alongside the BPS Soon-lane backfill pause.
        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .serious))
        try await Task.sleep(for: .milliseconds(20))
        #expect(
            await bps.hotPathLookaheadMultiplier() == 0.5,
            ".serious thermal must halve the hot-path lookahead (0.5×) to relieve compute pressure."
        )

        // Recover — multiplier returns to 1.0×.
        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .nominal))
        try await Task.sleep(for: .milliseconds(20))
        #expect(await bps.hotPathLookaheadMultiplier() == 1.0)
    }
}
