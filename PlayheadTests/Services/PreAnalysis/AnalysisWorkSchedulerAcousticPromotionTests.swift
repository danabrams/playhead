// AnalysisWorkSchedulerAcousticPromotionTests.swift
// playhead-gtt9.24: acoustic-triggered transcription scheduling.
//
// Exercise `currentAcousticPromotionOpportunityForTesting`, the seam
// exposed by the scheduler in DEBUG builds, so we don't need the run
// loop to drive a dispatch — we assert the trigger predicate directly
// across the state space the bead specifies:
//
//   - cold-start (no persisted feature windows) → no opportunity
//   - high-likelihood region past current coverage → opportunity
//   - low-likelihood prefix region → no opportunity (existing tier
//     ladder will reach it)
//   - tail-region promoted ahead of equivalent-position clean-speech
//   - thermal critical dominates (pauseAllWork)
//   - disabled policy never fires
//   - escalation gap below the policy minimum → no opportunity

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — acoustic promotion (playhead-gtt9.24)")
struct AnalysisWorkSchedulerAcousticPromotionTests {

    // MARK: - Test fixture

    private struct Fixture {
        let scheduler: AnalysisWorkScheduler
        let store: AnalysisStore
        let asset: AnalysisAsset
        let job: AnalysisJob
    }

    private func makeFixture(
        thermalState: ThermalState = .nominal,
        promotionPolicy: AnalysisWorkScheduler.AcousticPromotionPolicy = .default,
        episodeDurationSec: Double? = 7200,           // 2 h "Conan-class" episode
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
            adDetection: StubAdDetectionProvider()
        )
        let scheduler = AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            acousticPromotionPolicy: promotionPolicy
        )

        let episodeId = "ep-gtt9.24"
        let assetId = "asset-gtt9.24"
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-gtt9.24",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/gtt9.24.m4a",
            featureCoverageEndTime: 7200,           // features extracted for the whole episode
            fastTranscriptCoverageEndTime: desiredCoverageSec,
            confirmedAdCoverageEndTime: nil,
            analysisState: "transcribing",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec
        )
        try await store.insertAsset(asset)
        // Note: `featureCoverageSec` is set to 0 on the JOB row (this
        // is per-job progress, not per-asset). The ASSET row has
        // `featureCoverageEndTime: 7200`, modelling the case where a
        // prior job (or backfill) already extracted features for the
        // whole episode — features are pre-extracted in
        // `feature_windows` and ready to be scored, but the queued job
        // hasn't yet advanced its own coverage past the prefix. This
        // makes the playback row T0-eligible per `fetchNextEligibleJob`'s
        // `(jobType = 'playback' AND featureCoverageSec < t0Threshold)`
        // branch — exactly the production state where acoustic
        // promotion targets a deeper second-pass coverage.
        let job = makeAnalysisJob(
            jobId: "job-gtt9.24",
            jobType: "playback",
            episodeId: episodeId,
            analysisAssetId: assetId,
            sourceFingerprint: "fp-gtt9.24",
            priority: 5,
            desiredCoverageSec: desiredCoverageSec,
            featureCoverageSec: 0,
            transcriptCoverageSec: 0,
            cueCoverageSec: 0,
            state: "queued"
        )
        _ = try await store.insertJob(job)

        return Fixture(scheduler: scheduler, store: store, asset: asset, job: job)
    }

    /// Synthesize a "clear ad onset" feature window for the test asset.
    /// Default-priors weights yield a score >= 0.5 here (foreground
    /// music bed + speaker change + onset).
    ///
    /// `featureVersion` matches `FeatureExtractionConfig.default.featureVersion`
    /// so `AnalysisStore.fetchFeatureWindows` (which filters on
    /// `featureVersion >= minimumFeatureVersion`, default = current) will
    /// surface these synthetic rows. Out-of-band-versioned windows are
    /// silently dropped, so this MUST track the production constant.
    private func adOnsetFeatureWindow(
        assetId: String,
        startTime: Double,
        endTime: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            rms: 0.18,
            spectralFlux: 0.40,
            musicProbability: 0.85,
            speakerChangeProxyScore: 0.7,
            musicBedChangeScore: 0.8,
            musicBedOnsetScore: 0.9,
            musicBedOffsetScore: 0.0,
            musicBedLevel: .foreground,
            pauseProbability: 0.2,
            speakerClusterId: 2,
            jingleHash: "ad-jingle",
            featureVersion: FeatureExtractionConfig.default.featureVersion
        )
    }

    /// Synthesize a clean host-conversation feature window — no music
    /// bed, no speaker change, very low flux. Default-priors weights
    /// yield a score < 0.2 here.
    ///
    /// See `adOnsetFeatureWindow` for the `featureVersion` rationale.
    private func cleanSpeechFeatureWindow(
        assetId: String,
        startTime: Double,
        endTime: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            rms: 0.08,
            spectralFlux: 0.05,
            musicProbability: 0.05,
            speakerChangeProxyScore: 0.0,
            musicBedChangeScore: 0.0,
            musicBedOnsetScore: 0.0,
            musicBedOffsetScore: 0.0,
            musicBedLevel: .none,
            pauseProbability: 0.1,
            speakerClusterId: 1,
            jingleHash: nil,
            featureVersion: FeatureExtractionConfig.default.featureVersion
        )
    }

    // MARK: - Cold start

    @Test("cold start: no feature windows persisted → no acoustic promotion fires")
    func testColdStartReturnsNil() async throws {
        let fx = try await makeFixture()
        // No `insertFeatureWindow(...)` calls — the asset has no
        // persisted feature windows yet. The promotion check must
        // gracefully fall back to nil.
        let opportunity = await fx.scheduler.currentAcousticPromotionOpportunityForTesting()
        #expect(opportunity == nil,
                "Cold start (no Stage 2 output yet) must not trigger acoustic promotion — the standard tier ladder handles the prefix")
    }

    // MARK: - Positive trigger

    @Test("ad-shaped region past current coverage triggers promotion")
    func testAdRegionPastCoverageTriggersPromotion() async throws {
        // Current desiredCoverageSec is 900 s (T2). Insert an ad-onset
        // feature window at 1800-1805 s (well past T2). Promotion
        // should fire and target the trigger window's end time.
        let fx = try await makeFixture(desiredCoverageSec: 900)
        try await fx.store.insertFeatureWindow(
            adOnsetFeatureWindow(
                assetId: fx.asset.id,
                startTime: 1800,
                endTime: 1805
            )
        )
        let opportunity = await fx.scheduler.currentAcousticPromotionOpportunityForTesting()
        let unwrapped = try #require(opportunity)
        #expect(unwrapped.jobId == fx.job.jobId)
        #expect(unwrapped.episodeId == fx.asset.episodeId)
        #expect(unwrapped.priorDesiredCoverageSec == 900)
        #expect(unwrapped.escalatedDesiredCoverageSec == 1805)
        #expect(unwrapped.triggerWindowStartSec == 1800)
        #expect(unwrapped.triggerWindowEndSec == 1805)
        #expect(unwrapped.triggerWindowScore >= 0.5,
                "Trigger window must satisfy the policy threshold; got score \(unwrapped.triggerWindowScore)")
    }

    @Test("escalation is capped at episode duration")
    func testEscalationCappedAtEpisodeDuration() async throws {
        // Episode duration 1500 s, but the ad-onset window extends to
        // 1700 s (synthetic — would not happen in production but
        // guards the cap logic). The escalated target must be 1500 s,
        // not 1700 s.
        let fx = try await makeFixture(
            episodeDurationSec: 1500,
            desiredCoverageSec: 900
        )
        try await fx.store.insertFeatureWindow(
            adOnsetFeatureWindow(
                assetId: fx.asset.id,
                startTime: 1400,
                endTime: 1700
            )
        )
        let opportunity = await fx.scheduler.currentAcousticPromotionOpportunityForTesting()
        let unwrapped = try #require(opportunity)
        #expect(unwrapped.escalatedDesiredCoverageSec == 1500,
                "Escalation must cap at episodeDurationSec when known")
    }

    // MARK: - Selection: ad region promoted ahead of equivalent-position clean speech

    @Test("ad region wins over higher-position clean speech")
    func testAdRegionWinsOverCleanSpeech() async throws {
        // Insert two feature windows past the current coverage:
        //   - clean speech at 1000-1005 (low score, would not promote)
        //   - ad onset at 2000-2005 (high score, should win)
        // Even though clean speech is closer to the head, the
        // promotion picks the ad-shaped region because the
        // bounded-additive scorer ranks it higher and clean speech is
        // below threshold.
        let fx = try await makeFixture(desiredCoverageSec: 900)
        try await fx.store.insertFeatureWindow(
            cleanSpeechFeatureWindow(
                assetId: fx.asset.id,
                startTime: 1000,
                endTime: 1005
            )
        )
        try await fx.store.insertFeatureWindow(
            adOnsetFeatureWindow(
                assetId: fx.asset.id,
                startTime: 2000,
                endTime: 2005
            )
        )
        let opportunity = await fx.scheduler.currentAcousticPromotionOpportunityForTesting()
        let unwrapped = try #require(opportunity)
        #expect(unwrapped.triggerWindowStartSec == 2000,
                "Ad-shaped region (high acoustic score) must win promotion over closer clean-speech window (below threshold)")
        #expect(unwrapped.escalatedDesiredCoverageSec == 2005)
    }

    // MARK: - Backpressure

    @Test("thermal critical dominates: no acoustic promotion")
    func testThermalCriticalSuppressesPromotion() async throws {
        let fx = try await makeFixture(thermalState: .critical)
        try await fx.store.insertFeatureWindow(
            adOnsetFeatureWindow(
                assetId: fx.asset.id,
                startTime: 1800,
                endTime: 1805
            )
        )
        let opportunity = await fx.scheduler.currentAcousticPromotionOpportunityForTesting()
        #expect(opportunity == nil,
                "LaneAdmission.pauseAllWork (thermal critical) must dominate every gate, including acoustic promotion")
    }

    // MARK: - Disabled policy

    @Test("disabled policy: acoustic promotion never fires")
    func testDisabledPolicyDoesNotFire() async throws {
        let fx = try await makeFixture(promotionPolicy: .disabled)
        try await fx.store.insertFeatureWindow(
            adOnsetFeatureWindow(
                assetId: fx.asset.id,
                startTime: 1800,
                endTime: 1805
            )
        )
        let opportunity = await fx.scheduler.currentAcousticPromotionOpportunityForTesting()
        #expect(opportunity == nil,
                "AcousticPromotionPolicy.disabled has scoreThreshold > 1.0; no window can pass — trigger must be a no-op")
    }

    // MARK: - Escalation-gap gate

    @Test("escalation gap below minimum: no acoustic promotion")
    func testEscalationGapBelowMinimumDoesNotFire() async throws {
        // Current coverage 900 s. Ad-onset window at 920-925 — 25 s
        // beyond current target, less than the 60 s minimum gap.
        // The standard tier ladder will reach this region without
        // needing a separate dispatch, so promotion must not fire.
        let fx = try await makeFixture(desiredCoverageSec: 900)
        try await fx.store.insertFeatureWindow(
            adOnsetFeatureWindow(
                assetId: fx.asset.id,
                startTime: 920,
                endTime: 925
            )
        )
        let opportunity = await fx.scheduler.currentAcousticPromotionOpportunityForTesting()
        #expect(opportunity == nil,
                "Escalation gap (5 s) below policy minimum (60 s) — promotion must defer to the standard tier ladder")
    }

    @Test("escalation gap above minimum: acoustic promotion fires")
    func testEscalationGapAboveMinimumFires() async throws {
        // Current coverage 900 s; window at 1100-1200 — gap is 300 s
        // past current target, well above the 60 s minimum.
        let fx = try await makeFixture(desiredCoverageSec: 900)
        try await fx.store.insertFeatureWindow(
            adOnsetFeatureWindow(
                assetId: fx.asset.id,
                startTime: 1100,
                endTime: 1200
            )
        )
        let opportunity = await fx.scheduler.currentAcousticPromotionOpportunityForTesting()
        let unwrapped = try #require(opportunity)
        #expect(unwrapped.escalatedDesiredCoverageSec == 1200)
    }

    // MARK: - Composition with foreground catchup (yqax)

    @Test("foreground catchup + acoustic promotion compose: catchup wins on (foreground, playing) trailing edge")
    func testCatchupWinsOverAcousticPromotionOnTrailingEdge() async throws {
        // Both signals could fire:
        //   - foreground + playing + low runway → catchup eligible
        //   - ad-onset window past current coverage → acoustic eligible
        //
        // The run loop consults catchup FIRST (more time-sensitive —
        // user is actively at the trailing edge). Acoustic promotion
        // should also report an opportunity (it's a separate
        // predicate), but the run loop's `if let opportunity = ...
        // continue` fires catchup and short-circuits before the
        // acoustic check ever runs in production.
        //
        // Here we directly verify both predicates report eligibility
        // simultaneously — the run-loop ordering is a separate
        // invariant the catchup tests already pin.
        let fx = try await makeFixture(desiredCoverageSec: 600)
        try await fx.store.insertFeatureWindow(
            adOnsetFeatureWindow(
                assetId: fx.asset.id,
                startTime: 1800,
                endTime: 1805
            )
        )
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 590  // 10 s of runway
        )
        let catchup = await fx.scheduler.currentCatchupOpportunityForTesting()
        let acoustic = await fx.scheduler.currentAcousticPromotionOpportunityForTesting()
        #expect(catchup != nil,
                "Foreground + playing + low runway must yield a catchup opportunity")
        #expect(acoustic != nil,
                "Ad-onset window past current coverage must yield an acoustic-promotion opportunity even when catchup is also eligible — the run-loop ordering picks catchup first, but both predicates report independently")
    }

    // MARK: - Admission-vs-persistence ordering (review-followup csp / H1)

    @Test("dispatchAcousticPromotion: admission denial does NOT persist a deeper coverage target")
    func testAcousticAdmissionDenialDoesNotPersistEscalation() async throws {
        // Review-followup (csp / H1): mirrors the M4 catchup fix — the
        // prior order persisted `desiredCoverageSec` BEFORE checking
        // admission. A denied admission then left the row at an
        // inflated tier with no dispatch — every subsequent dispatch
        // saw a coverage demand the runner couldn't satisfy in one
        // pass. Pin the new order: admission first, persistence only
        // after it succeeds.
        //
        // Drive a denial deterministically by saturating the Soon-lane
        // counter via a sibling didStart. The promotion job (priority
        // 5) is in Soon (1..<20); Soon cap is 1; one outstanding
        // sibling is enough to make `canAdmit` reject.
        let fx = try await makeFixture(desiredCoverageSec: 900)

        // Saturate the Soon lane with a sibling job. The job we pass
        // to `didStart` must have a Soon-lane priority too.
        let sibling = makeAnalysisJob(
            jobId: "sibling-soon",
            jobType: "preAnalysis",
            episodeId: "ep-other",
            sourceFingerprint: "fp-other",
            priority: 5, // Soon
            desiredCoverageSec: 900,
            state: "running"
        )
        await fx.scheduler.didStart(job: sibling)

        // Construct an opportunity targeting our promotion job. Values
        // mirror the positive-trigger test: prior 900 → escalated 1805.
        let opportunity = AnalysisWorkScheduler.AcousticPromotionOpportunity(
            jobId: fx.job.jobId,
            episodeId: fx.asset.episodeId,
            priorDesiredCoverageSec: 900,
            escalatedDesiredCoverageSec: 1805,
            triggerWindowStartSec: 1800,
            triggerWindowEndSec: 1805,
            triggerWindowScore: 0.95
        )

        await fx.scheduler.dispatchAcousticPromotionForTesting(opportunity: opportunity)

        // The persisted desiredCoverageSec must remain at the prior
        // value — admission denial bailed before any write landed.
        let after = try #require(await fx.store.fetchLatestJobForEpisode(fx.asset.episodeId))
        #expect(after.desiredCoverageSec == 900,
                "Denied admission must not persist the escalated coverage target; got \(after.desiredCoverageSec)")
    }
}

#endif
