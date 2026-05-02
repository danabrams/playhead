// AnalysisWorkSchedulerPlaybackContextTests.swift
// playhead-gtt9.14: scene-phase + playback-state aware admission.
//
// The scheduler's admission filter is a 4-state matrix over (scenePhase,
// playbackContext). The four states:
//
//   (foreground, playing)     -> block deferred (audio owns pipeline)
//   (foreground, paused)      -> admit all lanes (MOST aggressive)
//   (foreground, idle)        -> admit all lanes (MOST aggressive)
//   (background,       *)     -> current behavior — if an episode is loaded
//                                the scheduler defers to the BGProcessingTask
//                                window managed by BackgroundProcessingService.
//
// These tests exercise the admission filter through the `wouldAdmitDeferredWork`
// test-only surface so we don't have to spin up a real playback engine.
// Thermal-critical still pauses everything in every state — the existing
// invariant is re-asserted here to prevent regressions.

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — scene-phase + playback-state admission (playhead-gtt9.14)")
struct AnalysisWorkSchedulerPlaybackContextTests {

    // MARK: - Scheduler construction helper

    private func makeScheduler(
        thermalState: ThermalState = .nominal,
        isLowPowerMode: Bool = false,
        isCharging: Bool = true,
        batteryLevel: Float = 0.9
    ) async throws -> AnalysisWorkScheduler {
        let store = try await makeTestStore()
        let capabilities = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(
                thermalState: thermalState,
                isLowPowerMode: isLowPowerMode,
                isCharging: isCharging
            )
        )
        let battery = StubBatteryProvider()
        battery.level = batteryLevel
        battery.charging = isCharging
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider()
        )
    }

    // MARK: - Default state

    @Test("default state: foreground + idle admits deferred work")
    func testDefaultStateAdmitsDeferred() async throws {
        let scheduler = try await makeScheduler()
        let admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits,
                "Fresh scheduler with no episode loaded and no scene-phase update must admit deferred work")
    }

    // MARK: - Foreground matrix

    @Test("foreground + playing: blocks deferred work (current behavior)")
    func testForegroundPlayingBlocksDeferred() async throws {
        let scheduler = try await makeScheduler()
        await scheduler.updateScenePhase(.foreground)
        await scheduler.updatePlaybackContext(.playing)

        let admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(!admits,
                "Foreground + playing audio owns the pipeline; deferred work must be blocked")
    }

    @Test("foreground + paused: admits deferred work (the gtt9.14 fix)")
    func testForegroundPausedAdmitsDeferred() async throws {
        let scheduler = try await makeScheduler()
        await scheduler.updateScenePhase(.foreground)
        await scheduler.updatePlaybackContext(.paused)

        let admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits,
                "Foreground + paused is the MOST aggressive mode: device awake, user engaged, audio idle; deferred must admit")
    }

    @Test("foreground + idle: admits deferred work")
    func testForegroundIdleAdmitsDeferred() async throws {
        let scheduler = try await makeScheduler()
        await scheduler.updateScenePhase(.foreground)
        await scheduler.updatePlaybackContext(.idle)

        let admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits,
                "Foreground + idle admits deferred — no episode, device foregrounded")
    }

    // MARK: - Background matrix (current behavior preserved)

    @Test("background + playing: blocks deferred")
    func testBackgroundPlayingBlocksDeferred() async throws {
        let scheduler = try await makeScheduler()
        await scheduler.updateScenePhase(.background)
        await scheduler.updatePlaybackContext(.playing)

        let admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(!admits,
                "Background scheduler defers to BackgroundProcessingService while an episode is loaded")
    }

    @Test("background + paused: blocks deferred (BPS owns the window)")
    func testBackgroundPausedBlocksDeferred() async throws {
        let scheduler = try await makeScheduler()
        await scheduler.updateScenePhase(.background)
        await scheduler.updatePlaybackContext(.paused)

        let admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(!admits,
                "Background + paused preserves pre-gtt9.14 behavior — BGProcessingTask governs background work")
    }

    @Test("background + idle: admits deferred")
    func testBackgroundIdleAdmitsDeferred() async throws {
        let scheduler = try await makeScheduler()
        await scheduler.updateScenePhase(.background)
        await scheduler.updatePlaybackContext(.idle)

        let admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits,
                "Background with nothing loaded — BPS wake window drains maintenance queue")
    }

    // MARK: - Thermal invariant (.critical pauses all in every state)

    @Test("thermal critical pauses all work even in foreground + paused")
    func testCriticalThermalPausesForegroundPaused() async throws {
        let scheduler = try await makeScheduler(thermalState: .critical)
        await scheduler.updateScenePhase(.foreground)
        await scheduler.updatePlaybackContext(.paused)

        let admission = await scheduler.currentLaneAdmission()
        #expect(admission.pauseAllWork,
                "Critical thermal pauses all work, scene-phase + playback context cannot override")

        let admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(!admits,
                "Critical thermal + foreground-paused must still block deferred")
    }

    @Test("thermal critical pauses all work even in foreground + idle")
    func testCriticalThermalPausesForegroundIdle() async throws {
        let scheduler = try await makeScheduler(thermalState: .critical)
        await scheduler.updateScenePhase(.foreground)
        await scheduler.updatePlaybackContext(.idle)

        let admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(!admits,
                "Critical thermal dominates: no state admits work")
    }

    // MARK: - Foreground-paused thermal relaxation (permit .serious)

    @Test("foreground + paused under serious thermal permits Soon lane")
    func testForegroundPausedSeriousThermalPermitsSoon() async throws {
        let scheduler = try await makeScheduler(thermalState: .serious)
        await scheduler.updateScenePhase(.foreground)
        await scheduler.updatePlaybackContext(.paused)

        let admission = await scheduler.currentLaneAdmission()
        // Baseline .serious blocks both Soon + Background. Under foreground-
        // paused the bead spec says "permit .serious thermal" so the Soon
        // lane must open up. The Background lane remains gated because
        // maintenance transfers still have other reasons (transport,
        // charging heuristics) to prefer a cooler device.
        #expect(admission.policy.allowSoonLane,
                "Foreground-paused under .serious thermal must permit Soon lane (gtt9.14 relaxation)")
    }

    @Test("background under serious thermal keeps Soon + Background paused")
    func testBackgroundSeriousThermalKeepsPaused() async throws {
        let scheduler = try await makeScheduler(thermalState: .serious)
        await scheduler.updateScenePhase(.background)
        await scheduler.updatePlaybackContext(.idle)

        let admission = await scheduler.currentLaneAdmission()
        #expect(!admission.policy.allowSoonLane,
                "Background under .serious keeps the baseline policy — the relaxation is foreground-only")
        #expect(!admission.policy.allowBackgroundLane,
                "Serious thermal in background preserves baseline")
    }

    // MARK: - State transition: foreground → background while paused

    @Test("transition foreground+paused → background+paused flips filter to block")
    func testForegroundPausedToBackgroundPausedReBlocks() async throws {
        let scheduler = try await makeScheduler()
        await scheduler.updateScenePhase(.foreground)
        await scheduler.updatePlaybackContext(.paused)

        var admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits, "Foreground-paused admits")

        // App goes to background while paused (user locks phone mid-pause).
        await scheduler.updateScenePhase(.background)

        admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(!admits,
                "Once backgrounded, BPS owns the wake window — scheduler must not admit opportunistically")
    }

    // MARK: - Compatibility shim: playbackStarted / playbackStopped

    @Test("playbackStarted sets playbackContext to .playing")
    func testPlaybackStartedUpdatesContext() async throws {
        let scheduler = try await makeScheduler()
        await scheduler.updateScenePhase(.foreground)
        await scheduler.playbackStarted(episodeId: "ep-1")

        let ctx = await scheduler.playbackContextForTesting()
        #expect(ctx == .playing,
                "playbackStarted must set the playback context to .playing")

        let admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(!admits, "Foreground + playing blocks deferred")
    }

    @Test("playbackStopped sets playbackContext to .idle")
    func testPlaybackStoppedUpdatesContext() async throws {
        let scheduler = try await makeScheduler()
        await scheduler.updateScenePhase(.foreground)
        await scheduler.playbackStarted(episodeId: "ep-1")
        await scheduler.playbackStopped()

        let ctx = await scheduler.playbackContextForTesting()
        #expect(ctx == .idle,
                "playbackStopped must set context to .idle")

        let admits = await scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits, "Foreground + idle admits deferred")
    }
}

#endif
