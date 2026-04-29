// BackgroundedPlaybackTests.swift
// playhead-rk7: degraded-conditions E2E — backgrounded playback.
//
// Pins what happens when the app moves from foreground to background
// while playback continues:
//
//   1. `BackgroundProcessingService.appDidEnterBackground()` submits a
//      BGProcessingTask under `BackgroundTaskID.backfillProcessing`.
//      iOS uses that to wake the app for deferred work after the user
//      stops interacting; without this submission, no background
//      enrichment happens.
//   2. The hot-path is NOT torn down by backgrounding alone — the
//      `hotPathActive` flag survives an `appDidEnterBackground` call
//      because backgrounded audio playback (allowed by the `audio`
//      UIBackgroundMode) continues to drive playhead ticks. The
//      orchestrator must remain capable of dispatching skip cues
//      during backgrounded playback.
//   3. A re-foreground (`stop` is not invoked, the app simply moves
//      back to active) should not require any reinit of the skip
//      orchestrator: `updatePlayheadTime` fires cues exactly as it
//      did before backgrounding.
//
// What this suite cannot test in-process:
//   * The actual iOS Audio Session continuing to play when the
//     screen locks — we cannot run an AVPlayer inside the unit-test
//     simulator. The lifecycle assertions here pin the in-app
//     contract: BPS does not stop the coordinator on background,
//     and the skip-cue dispatch path remains functional.

import CoreMedia
import Foundation
import Testing

@testable import Playhead

@Suite("playhead-rk7 - backgrounded playback", .serialized)
struct BackgroundedPlaybackTests {

    // MARK: - Test 1: backgrounding submits a backfill BGProcessingTask

    @Test("appDidEnterBackground submits a BGProcessingTask under the backfill identifier")
    func appDidEnterBackgroundSubmitsBackfillTask() async throws {
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

        let beforeCount = scheduler.submittedRequests.count
        await bps.appDidEnterBackground()
        try await Task.sleep(for: .milliseconds(50))

        let newRequests = Array(scheduler.submittedRequests.dropFirst(beforeCount))
        let backfillRequests = newRequests.filter {
            $0.identifier == BackgroundTaskID.backfillProcessing
        }
        #expect(
            !backfillRequests.isEmpty,
            "appDidEnterBackground must submit at least one BGProcessingTask under `\(BackgroundTaskID.backfillProcessing)` so iOS can wake the app for deferred backfill."
        )
    }

    // MARK: - Test 2: backgrounding does NOT stop the coordinator

    @Test("appDidEnterBackground does NOT call coordinator.stop()")
    func appDidEnterBackgroundDoesNotStopCoordinator() async throws {
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

        await bps.appDidEnterBackground()
        try await Task.sleep(for: .milliseconds(50))

        #expect(
            coordinator.stopCallCount == 0,
            "Backgrounding alone must NOT stop the coordinator — backgrounded audio continues per the `audio` UIBackgroundMode."
        )
        // Hot-path active flag remains true: backgrounding is not the
        // signal that pauses playback. Only an explicit
        // playbackDidStop() or a critical-thermal capability update
        // should flip this flag off.
        #expect(
            await bps.isHotPathActive() == true,
            "Hot-path must remain active across an `appDidEnterBackground` transition — playback continues."
        )
    }

    // MARK: - Test 3: skip cues continue to fire across the background transition

    @Test("Skip cues continue to fire from the orchestrator after appDidEnterBackground")
    func skipCuesContinueAcrossBackgroundTransition() async throws {
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

        // Stand up the orchestrator alongside the BPS — production has
        // both alive simultaneously through PlayheadRuntime. The
        // orchestrator does not depend on BPS state, but co-existing
        // them here mirrors production wiring.
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            config: .default,
            trustService: trustService
        )

        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "rk7-bg-ad",
            startTime: 30,
            endTime: 60,
            confidence: 0.9,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        // Backgrounding event fires.
        await bps.appDidEnterBackground()
        try await Task.sleep(for: .milliseconds(50))

        // After backgrounding, a playhead tick (driven by the audio
        // session continuing in the `audio` UIBackgroundMode) must
        // still dispatch a skip cue. The orchestrator has no awareness
        // of scene phase; this test pins that decoupling.
        await orchestrator.updatePlayheadTime(15)

        #expect(
            !pushedCues.isEmpty,
            "Skip cue must dispatch even after the app is backgrounded — backgrounded audio is the production reality and skip reliability must survive it."
        )
    }

    // MARK: - Test 4: re-foreground after background does not require re-init

    @Test("Skip cue path remains live across background → foreground transitions without re-init")
    func skipCuePathSurvivesRoundTrip() async throws {
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

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            config: .default,
            trustService: trustService
        )

        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "rk7-bg-roundtrip-ad",
            startTime: 30,
            endTime: 60,
            confidence: 0.9,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        // Background → foreground round trip with no re-init in between.
        await bps.appDidEnterBackground()
        try await Task.sleep(for: .milliseconds(20))
        // (No explicit "re-foreground" hook on BPS — the production
        // path is the SwiftUI scene-phase observer firing the
        // scheduler's updateScenePhase; BPS does not care.)

        // Tick from the foreground.
        await orchestrator.updatePlayheadTime(15)

        #expect(
            !pushedCues.isEmpty,
            "Skip cue path must survive a background → foreground round trip with no re-initialization."
        )
        #expect(
            await bps.isHotPathActive() == true,
            "Hot-path must remain active across the round trip."
        )
    }
}
