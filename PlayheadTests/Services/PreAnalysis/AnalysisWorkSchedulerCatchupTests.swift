// AnalysisWorkSchedulerCatchupTests.swift
// playhead-yqax: foreground transcript catch-up trigger.
//
// The catch-up predicate fires only when the user is foregrounded and
// actively playing AND transcribed-ahead runway is below the policy
// threshold. These tests exercise `currentCatchupOpportunityForTesting`,
// the seam exposed by the scheduler in DEBUG builds, so we don't need
// the run loop to drive a dispatch — we assert the trigger predicate
// directly across the full (scenePhase, playbackContext, position,
// transport thermal) state space the bead specifies.

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — foreground catch-up trigger (playhead-yqax)")
struct AnalysisWorkSchedulerCatchupTests {

    // MARK: - Test fixture

    /// Constructs a scheduler + store + seeded asset/job pair so the
    /// catch-up predicate has real persistence to read against. Returns
    /// the scheduler under test plus the store/asset/job for follow-up
    /// mutation in individual tests.
    private struct Fixture {
        let scheduler: AnalysisWorkScheduler
        let store: AnalysisStore
        let asset: AnalysisAsset
        let job: AnalysisJob
    }

    private func makeFixture(
        thermalState: ThermalState = .nominal,
        catchupPolicy: AnalysisWorkScheduler.PlayheadCatchupPolicy = .default,
        // Asset state knobs.
        episodeDurationSec: Double? = 7200,           // 2 h "Conan-class" episode
        fastTranscriptCoverageEndTime: Double = 600,  // 10 min transcribed
        // Job state knobs.
        desiredCoverageSec: Double = 900              // T2 ladder ceiling
    ) async throws -> Fixture {
        let store = try await makeTestStore()
        let capabilities = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(
                thermalState: thermalState,
                isLowPowerMode: false,
                isCharging: true
            )
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider(),
            cueMaterializer: SkipCueMaterializer(store: store)
        )
        let scheduler = AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            catchupPolicy: catchupPolicy
        )

        let episodeId = "ep-yqax"
        let assetId = "asset-yqax"
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-yqax",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/yqax.m4a",
            featureCoverageEndTime: 1200,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
            confirmedAdCoverageEndTime: nil,
            analysisState: "transcribing",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec
        )
        try await store.insertAsset(asset)
        let job = makeAnalysisJob(
            jobId: "job-yqax",
            jobType: "playback",
            episodeId: episodeId,
            analysisAssetId: assetId,
            sourceFingerprint: "fp-yqax",
            priority: 5,
            desiredCoverageSec: desiredCoverageSec,
            featureCoverageSec: 1200,
            transcriptCoverageSec: fastTranscriptCoverageEndTime,
            cueCoverageSec: 0,
            state: "queued"
        )
        _ = try await store.insertJob(job)

        return Fixture(scheduler: scheduler, store: store, asset: asset, job: job)
    }

    // MARK: - Positive trigger (the bead's whole point)

    @Test("foreground + playing + low runway: catch-up opportunity fires")
    func testForegroundPlayingLowRunwayFires() async throws {
        // Transcribed at 600 s; playhead at 590 s → 10 s runway,
        // well under the 60 s default trigger. Escalated target
        // 590 + 300 = 890, still < 900 prior — so we drop the prior
        // coverage to 600 (matches the transcript end) so the
        // escalation strictly exceeds it.
        let fx = try await makeFixture(desiredCoverageSec: 600)
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 590  // 10 s of runway behind 600 s coverage
        )

        let opportunity = await fx.scheduler.currentCatchupOpportunityForTesting()
        let unwrapped = try #require(opportunity)
        #expect(unwrapped.jobId == "job-yqax")
        #expect(unwrapped.episodeId == "ep-yqax")
        #expect(unwrapped.priorDesiredCoverageSec == 600)
        #expect(unwrapped.escalatedDesiredCoverageSec == 890,
                "Lookahead 300 s + playhead 590 s = 890 s catch-up target")
        #expect(unwrapped.transcribedAheadSec == 10)
        #expect(unwrapped.playheadPositionSec == 590)
    }

    @Test("foreground + playing: escalation strictly greater than prior coverage")
    func testCatchupEscalatesBeyondPriorCoverage() async throws {
        // Position at 700 s; transcribed at 600 s; runway is negative
        // (already past coverage). Lookahead 300 s → target 1000 s,
        // strictly greater than 900 s prior, so opportunity fires.
        let fx = try await makeFixture()
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 700
        )

        let opportunity = try #require(await fx.scheduler.currentCatchupOpportunityForTesting())
        #expect(opportunity.escalatedDesiredCoverageSec > opportunity.priorDesiredCoverageSec,
                "Catch-up only fires when the escalation strictly exceeds the persisted target — otherwise the existing tier ladder already covers it")
        #expect(opportunity.playheadPositionSec == 700)
    }

    // MARK: - 4-state admission filter

    @Test("foreground + paused: catch-up does NOT fire (existing admission already runs work)")
    func testForegroundPausedDoesNotFireCatchup() async throws {
        // Foreground + paused already admits ALL deferred work via the
        // gtt9.14 admission filter. Catch-up is the override for the
        // (foreground, playing) case where deferred work is otherwise
        // blocked, so it must NOT fire when paused — that would be
        // double-admitting.
        let fx = try await makeFixture()
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.paused)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 700
        )

        let opportunity = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(opportunity == nil,
                "(foreground, paused) admits deferred work normally; catch-up must not double-fire")
    }

    @Test("foreground + idle: catch-up does NOT fire (no episode active)")
    func testForegroundIdleDoesNotFireCatchup() async throws {
        let fx = try await makeFixture()
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.idle)
        // No `playbackStarted` — `activePlaybackEpisodeId` is nil.
        // `updatePlaybackContext(.idle)` also clears the playhead.

        let opportunity = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(opportunity == nil,
                "Without an active episode catch-up has no target")
    }

    @Test("background + playing: catch-up does NOT fire (BPS owns the window)")
    func testBackgroundPlayingDoesNotFireCatchup() async throws {
        let fx = try await makeFixture()
        await fx.scheduler.updateScenePhase(.background)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 700
        )

        let opportunity = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(opportunity == nil,
                "Catch-up is foreground-only; BackgroundProcessingService governs background pipeline")
    }

    @Test("background + paused: catch-up does NOT fire")
    func testBackgroundPausedDoesNotFireCatchup() async throws {
        let fx = try await makeFixture()
        await fx.scheduler.updateScenePhase(.background)
        await fx.scheduler.updatePlaybackContext(.paused)

        let opportunity = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(opportunity == nil,
                "Catch-up is foreground-only")
    }

    // MARK: - Backpressure (thermal critical dominates)

    @Test("thermal critical: catch-up does NOT fire even on (foreground, playing)")
    func testThermalCriticalSuppressesCatchup() async throws {
        let fx = try await makeFixture(thermalState: .critical)
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 700
        )

        let opportunity = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(opportunity == nil,
                "LaneAdmission.pauseAllWork (thermal critical) dominates every gate — catch-up must back off")
    }

    // MARK: - Disabled policy

    @Test("disabled policy: catch-up never fires")
    func testDisabledPolicyDoesNotFire() async throws {
        let fx = try await makeFixture(catchupPolicy: .disabled)
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 700
        )

        let opportunity = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(opportunity == nil,
                "PlayheadCatchupPolicy.disabled has zero thresholds; trivially-misconfigured policy guard returns nil")
    }

    // MARK: - Stale-tick filtering

    @Test("position from non-active episode is dropped (stale-tick filter)")
    func testStaleTickFromOtherEpisodeIsDropped() async throws {
        // Episode A is active; a delayed tick from episode B arrives.
        // `noteCurrentPlayheadPosition` filters by episodeId and silently
        // drops the call so the next catch-up evaluation cannot run on
        // a position from the prior episode.
        let fx = try await makeFixture()
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)

        // Stale tick from a different episode.
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: "ep-OTHER",
            position: 700
        )

        let storedPosition = await fx.scheduler.playheadPositionSecForTesting()
        #expect(storedPosition == nil,
                "A position update tagged with a non-active episodeId must be dropped")

        let opportunity = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(opportunity == nil,
                "Without a position the catch-up trigger cannot fire")
    }

    @Test("playbackStopped clears the playhead snapshot")
    func testPlaybackStoppedClearsPlayhead() async throws {
        let fx = try await makeFixture()
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 700
        )
        // Stop playback — playhead snapshot must reset so a stale tick
        // from the now-stopped session cannot leak into a subsequent
        // play-start of a different episode.
        await fx.scheduler.playbackStopped()

        let storedPosition = await fx.scheduler.playheadPositionSecForTesting()
        #expect(storedPosition == nil,
                "playbackStopped must clear the playhead snapshot in lockstep with activePlaybackEpisodeId")
    }

    // MARK: - Coverage interactions

    @Test("runway above threshold: catch-up does NOT fire")
    func testHighRunwayDoesNotFireCatchup() async throws {
        // Transcribed coverage 600 s; playhead at 100 s; runway is
        // 500 s — well above the 60 s default trigger. No catch-up.
        let fx = try await makeFixture()
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 100
        )

        let opportunity = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(opportunity == nil,
                "500 s of runway is well above the 60 s trigger; standard tier ladder owns this case")
    }

    @Test("escalated target clamped at episode duration")
    func testEscalatedTargetClampedAtEpisodeDuration() async throws {
        // Short 800 s episode; playhead at 700 s; lookahead 300 s would
        // target 1000 s. Clamp at duration (800).
        let fx = try await makeFixture(
            episodeDurationSec: 800,
            fastTranscriptCoverageEndTime: 720,
            desiredCoverageSec: 750
        )
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 700
        )

        let opportunity = try #require(await fx.scheduler.currentCatchupOpportunityForTesting())
        #expect(opportunity.escalatedDesiredCoverageSec == 800,
                "Escalated target must clamp at episode duration when the unclamped target would overshoot")
    }

    // MARK: - Strict-greater guard

    @Test("escalated target equal-or-less than prior: catch-up does NOT fire")
    func testNoFireWhenEscalationDoesNotExceedPrior() async throws {
        // Playhead at 100 s with high transcript coverage already (590 s)
        // — the 100 + 300 = 400 target is ≤ persisted 900 desired
        // coverage. Nothing to escalate.
        let fx = try await makeFixture(
            fastTranscriptCoverageEndTime: 590,
            desiredCoverageSec: 900
        )
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 540  // 50 s runway, below 60 s trigger
        )

        let opportunity = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(opportunity == nil,
                "If the escalation target is ≤ the persisted target the existing tier ladder already covers it")
    }

    // MARK: - Persistence integration

    @Test("updateJobDesiredCoverage persists the escalated target")
    func testUpdateJobDesiredCoveragePersists() async throws {
        // Direct test of the AnalysisStore mutation that
        // `dispatchForegroundCatchup` uses. Confirms the write is
        // durable and observable via re-fetch.
        let fx = try await makeFixture()
        try await fx.store.updateJobDesiredCoverage(
            jobId: fx.job.jobId,
            desiredCoverageSec: 1500
        )

        let refetched = try #require(await fx.store.fetchLatestJobForEpisode(fx.asset.episodeId))
        #expect(refetched.desiredCoverageSec == 1500,
                "Escalated coverage must round-trip through SQLite")
    }

    // MARK: - Admission-vs-persistence ordering (review-followup csp / M4)

    @Test("dispatchForegroundCatchup: admission denial does NOT persist a deeper coverage target")
    func testAdmissionDenialDoesNotPersistEscalation() async throws {
        // Review-followup (csp / M4): the prior order persisted
        // `desiredCoverageSec` BEFORE checking admission. A denied
        // admission then left the row at an inflated tier with no
        // dispatch — every subsequent dispatch saw a coverage demand
        // the runner couldn't satisfy in one pass. Pin the new order:
        // admission first, persistence only after it succeeds.
        //
        // Drive a denial deterministically by saturating the Soon-lane
        // counter via a sibling didStart. The catchup job (priority 5)
        // is in Soon (1..<20); Soon cap is 1; one outstanding sibling
        // is enough to make `canAdmit` reject.
        let fx = try await makeFixture(desiredCoverageSec: 600)

        // Saturate the Soon lane with a sibling job. The job we pass
        // to `didStart` must have a Soon-lane priority too.
        let sibling = makeAnalysisJob(
            jobId: "sibling-soon",
            jobType: "preAnalysis",
            episodeId: "ep-other",
            sourceFingerprint: "fp-other",
            priority: 5, // Soon
            desiredCoverageSec: 600,
            state: "running"
        )
        await fx.scheduler.didStart(job: sibling)

        // Construct an opportunity targeting our catchup job. Values
        // mirror the positive-trigger test: prior 600 → escalated 890.
        let opportunity = AnalysisWorkScheduler.CatchupOpportunity(
            jobId: fx.job.jobId,
            episodeId: fx.asset.episodeId,
            priorDesiredCoverageSec: 600,
            escalatedDesiredCoverageSec: 890,
            transcribedAheadSec: 10,
            playheadPositionSec: 590
        )

        await fx.scheduler.dispatchForegroundCatchupForTesting(opportunity: opportunity)

        // The persisted desiredCoverageSec must remain at the prior
        // value — admission denial bailed before any write landed.
        let after = try #require(await fx.store.fetchLatestJobForEpisode(fx.asset.episodeId))
        #expect(after.desiredCoverageSec == 600,
                "Denied admission must not persist the escalated coverage target; got \(after.desiredCoverageSec)")
    }
}

#endif
