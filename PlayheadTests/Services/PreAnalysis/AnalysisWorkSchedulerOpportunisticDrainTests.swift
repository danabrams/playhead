// AnalysisWorkSchedulerOpportunisticDrainTests.swift
// playhead-glo9: opportunistic backlog drain during playback.
//
// During a foreground listening session `(foreground, playing)` the
// scheduler normally BLOCKS all deferred backlog work so the audio
// decode path owns pipeline bandwidth — only the active episode's
// yqax hot-path catch-up bypasses that block. glo9 relaxes the block,
// behind a DEFAULT-OFF flag, so OTHER-episode Soon/Background backlog
// can drain when ALL of Dan's ratified charging-only gate conditions
// hold: flag ON, device charging, `QualityProfile == .nominal`, and
// the active episode's hot path comfortably caught up.
//
// These tests drive `wouldAdmitDeferredWorkForTesting()` — the DEBUG
// seam that reports the run loop's effective deferred-work admission
// decision under the current (scenePhase, playbackContext,
// QualityProfile, charge, hot-path) snapshot — so we assert the
// admission policy directly without racing the run loop.

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — opportunistic backlog drain during playback (playhead-glo9)")
struct AnalysisWorkSchedulerOpportunisticDrainTests {

    // MARK: - Test fixture

    private struct Fixture {
        let scheduler: AnalysisWorkScheduler
        let store: AnalysisStore
        let asset: AnalysisAsset
        let job: AnalysisJob
    }

    /// Builds a scheduler + store + seeded asset/job for the active
    /// episode so the hot-path-caught-up signal has real persistence to
    /// read. `flagOn` flips the glo9 feature flag; `charging`, `thermal`,
    /// and the asset/playhead knobs drive the four gate conditions.
    private func makeFixture(
        flagOn: Bool,
        charging: Bool = true,
        thermal: ThermalState = .nominal,
        isLowPowerMode: Bool = false,
        batteryLevel: Float = 0.9,
        // 600 s transcribed, 2 h episode by default.
        fastTranscriptCoverageEndTime: Double = 600,
        episodeDurationSec: Double? = 7200,
        desiredCoverageSec: Double = 900
    ) async throws -> Fixture {
        let store = try await makeTestStore()
        let capabilities = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(
                thermalState: thermal,
                isLowPowerMode: isLowPowerMode,
                isCharging: charging
            )
        )
        let battery = StubBatteryProvider()
        battery.level = batteryLevel
        battery.charging = charging

        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let config = PreAnalysisConfig(opportunisticBacklogDrainDuringPlayback: flagOn)
        let scheduler = AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            config: config
        )

        let episodeId = "ep-glo9"
        let assetId = "asset-glo9"
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-glo9",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/glo9.m4a",
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
            jobId: "job-glo9",
            jobType: "playback",
            episodeId: episodeId,
            analysisAssetId: assetId,
            sourceFingerprint: "fp-glo9",
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

    /// Drives the scheduler into `(foreground, playing)` with the given
    /// playhead position for the seeded active episode.
    private func enterForegroundPlaying(
        _ fx: Fixture,
        playhead: TimeInterval
    ) async {
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: playhead
        )
    }

    // MARK: - Flag OFF ⇒ byte-identical to pre-glo9

    @Test("flag OFF: (foreground, playing) blocks deferred work even with ALL other gate conditions met")
    func testFlagOffBlocksDespiteAllConditionsMet() async throws {
        // Charging + nominal + hot path caught up (200 s runway) — every
        // condition the relaxation needs EXCEPT the flag. With the flag
        // OFF admission must be byte-identical to pre-glo9: BLOCKED.
        let fx = try await makeFixture(flagOn: false)
        await enterForegroundPlaying(fx, playhead: 400) // 600 - 400 = 200 s runway

        let admits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits == false,
                "Flag OFF must leave the (foreground, playing) block intact regardless of charge/thermal/hot-path — byte-identical to pre-glo9")
    }

    @Test("flag OFF: (foreground, paused) still admits (unchanged gtt9.14 behavior)")
    func testFlagOffForegroundPausedStillAdmits() async throws {
        // Sanity: the relaxation must not perturb the non-blocked cases.
        // (foreground, paused) admits deferred work today and must keep
        // admitting with the flag off.
        let fx = try await makeFixture(flagOn: false)
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.paused)

        let admits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits == true,
                "(foreground, paused) admits deferred work; the glo9 change must not alter the non-blocked matrix cells")
    }

    // MARK: - Flag ON + all conditions ⇒ ADMIT (RED under today, GREEN after)

    @Test("flag ON + charging + nominal + hot-path caught up: other-episode backlog is ADMITTED")
    func testFlagOnAllConditionsAdmits() async throws {
        let fx = try await makeFixture(flagOn: true)
        await enterForegroundPlaying(fx, playhead: 400) // 200 s runway ≥ 120 s

        // Caught up ⇒ the yqax catch-up bypass is NOT pending, so this is
        // genuinely the "drain OTHER-episode work" regime (mutual
        // exclusivity of relaxation and catch-up).
        let catchup = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(catchup == nil,
                "200 s runway is well above the 60 s catch-up trigger; no catch-up should be pending")

        let admits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits == true,
                "Flag ON + charging + nominal + hot path comfortably caught up must ADMIT other-episode Soon/Background backlog during (foreground, playing)")
    }

    // MARK: - Flag ON but ONE condition false ⇒ still BLOCKED

    @Test("flag ON but NOT charging: still BLOCKED")
    func testFlagOnNotChargingBlocks() async throws {
        // High battery so the profile stays nominal even unplugged — the
        // ONLY failing condition is charge.
        let fx = try await makeFixture(flagOn: true, charging: false, batteryLevel: 0.9)
        await enterForegroundPlaying(fx, playhead: 400)

        let admission = await fx.scheduler.currentLaneAdmission()
        #expect(admission.qualityProfile == .nominal,
                "High battery unplugged must remain nominal so charge is the only failing gate condition")

        let admits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits == false,
                "Off-charge must never admit through the relaxation — the whole relaxation is charging-only")
    }

    @Test("flag ON but serious thermal: still BLOCKED")
    func testFlagOnSeriousThermalBlocks() async throws {
        let fx = try await makeFixture(flagOn: true, thermal: .serious)
        await enterForegroundPlaying(fx, playhead: 400)

        let admission = await fx.scheduler.currentLaneAdmission()
        #expect(admission.qualityProfile == .serious)

        let admits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits == false,
                "Only .nominal relaxes; .serious keeps the block (no thermal stress relaxation)")
    }

    @Test("flag ON but fair thermal: still BLOCKED")
    func testFlagOnFairThermalBlocks() async throws {
        let fx = try await makeFixture(flagOn: true, thermal: .fair)
        await enterForegroundPlaying(fx, playhead: 400)

        let admission = await fx.scheduler.currentLaneAdmission()
        #expect(admission.qualityProfile == .fair)

        let admits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits == false,
                "Only .nominal relaxes; .fair keeps the block")
    }

    @Test("flag ON but critical thermal: still BLOCKED (pauseAllWork dominates)")
    func testFlagOnCriticalThermalBlocks() async throws {
        let fx = try await makeFixture(flagOn: true, thermal: .critical)
        await enterForegroundPlaying(fx, playhead: 400)

        let admits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits == false,
                "critical → pauseAllWork; the relaxation can never fire")
    }

    @Test("flag ON + charging + nominal but hot path NOT caught up: still BLOCKED and catch-up WINS")
    func testFlagOnHotPathBehindBlocksAndCatchupWins() async throws {
        // Playhead at 590 s vs 600 s transcribed → 10 s runway, below
        // both the 120 s drain runway AND the 60 s catch-up trigger.
        // desiredCoverageSec 600 so the escalation (590 + 300 = 890)
        // strictly exceeds it and a catch-up opportunity genuinely fires.
        let fx = try await makeFixture(flagOn: true, desiredCoverageSec: 600)
        await enterForegroundPlaying(fx, playhead: 590)

        let admits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits == false,
                "When the active episode's hot path is behind, other-episode backlog must NOT be admitted — the active episode is never starved")

        let catchup = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(catchup != nil,
                "The active-episode catch-up opportunity must be pending — the run loop consults it BEFORE the relaxed block, so catch-up wins")
    }

    @Test("flag ON + all conditions but no playhead position: still BLOCKED (conservative)")
    func testFlagOnNoPlayheadBlocks() async throws {
        // Active + playing + charging + nominal, but no playhead tick has
        // been observed yet. Without a hot-path signal we cannot prove
        // the active episode is caught up, so the conservative choice is
        // to stay blocked.
        let fx = try await makeFixture(flagOn: true)
        await fx.scheduler.updateScenePhase(.foreground)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        // Deliberately NO noteCurrentPlayheadPosition.

        let admits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits == false,
                "No observed playhead ⇒ hot-path-caught-up cannot be proven ⇒ stay blocked")
    }

    // MARK: - Background never relaxed (BG granting architecture untouched)

    @Test("flag ON + (background, playing) + charging + nominal + caught up: still BLOCKED")
    func testFlagOnBackgroundPlayingNeverRelaxed() async throws {
        let fx = try await makeFixture(flagOn: true)
        await fx.scheduler.updateScenePhase(.background)
        await fx.scheduler.updatePlaybackContext(.playing)
        await fx.scheduler.playbackStarted(episodeId: fx.asset.episodeId)
        await fx.scheduler.noteCurrentPlayheadPosition(
            episodeId: fx.asset.episodeId,
            position: 400
        )

        let admits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(admits == false,
                "Background playback is never relaxed — BackgroundProcessingService owns the background window; the BG granting architecture stays untouched")
    }

    // MARK: - Direct RED→GREEN pairing on a single snapshot

    @Test("same caught-up charging-nominal snapshot: flag OFF blocks, flag ON admits")
    func testFlagFlipTogglesAdmissionOnIdenticalSnapshot() async throws {
        let off = try await makeFixture(flagOn: false)
        await enterForegroundPlaying(off, playhead: 400)
        let offAdmits = await off.scheduler.wouldAdmitDeferredWorkForTesting()

        let on = try await makeFixture(flagOn: true)
        await enterForegroundPlaying(on, playhead: 400)
        let onAdmits = await on.scheduler.wouldAdmitDeferredWorkForTesting()

        #expect(offAdmits == false,
                "Flag OFF blocks (pre-glo9 behavior)")
        #expect(onAdmits == true,
                "Flag ON admits on the identical snapshot — the ONLY difference is the flag")
    }

    // MARK: - playhead-0sro: a stale watermark silently nullifies glo9

    /// `admissionBlocksDeferred()` is the gtt9.14 (scenePhase,
    /// playbackContext) matrix and reads NO coverage. The coverage signal
    /// that decides whether that block is RELAXED is condition 5 of
    /// `opportunisticDrainRelaxationApplies` —
    /// `activeEpisodeHotPathCaughtUp()` — and it reads the RAW persisted
    /// column `AnalysisAsset.fastTranscriptCoverageEndTime`, with no
    /// read-time reconciliation against `transcript_chunks`.
    ///
    /// So the playhead-0sro stale watermark reaches it directly: on an
    /// effectively-complete episode whose watermark is frozen at 60 s,
    /// `transcribedAhead = max(0, 60 − playhead)` is 0 for any playhead
    /// past a minute, which is permanently below the 120 s runway. The
    /// relaxation can then NEVER fire for that episode — the glo9 backlog
    /// drain shipped in #285 is silently dead for the whole listening
    /// session — and the yqax catch-up trigger simultaneously fires
    /// forever on an episode that has nothing left to transcribe.
    ///
    /// This test pins both symptoms and their repair.
    @Test("stale watermark blocks the glo9 drain on a complete episode; reconciliation restores it")
    func testStaleWatermarkBlocksDrainUntilReconciled() async throws {
        // THEMOVE shape: fast transcript covering 3575.88 s of a
        // 3578.10 s episode, behind a watermark frozen at 60 s.
        let fx = try await makeFixture(
            flagOn: true,
            fastTranscriptCoverageEndTime: 60,
            episodeDurationSec: 3578.10
        )
        var chunks: [TranscriptChunk] = []
        var start = 0.0
        var index = 0
        while start < 3540 {
            chunks.append(makeFastChunk(assetId: fx.asset.id, index: index, start: start, end: start + 30))
            start += 30
            index += 1
        }
        chunks.append(makeFastChunk(assetId: fx.asset.id, index: index, start: 3540, end: 3575.88))
        try await fx.store.insertTranscriptChunks(chunks)

        // Every OTHER glo9 gate condition holds: flag ON, foreground
        // playing, charging, nominal. Only the coverage signal is stale.
        await enterForegroundPlaying(fx, playhead: 1000)

        #expect(!FastTranscriptCoverageInvariant.holds(
            watermarkSec: 60,
            chunkMaxEndSec: 3575.88,
            episodeDurationSec: 3578.10
        ), "precondition: the seeded state is the bead's stale-watermark defect")

        let staleAdmits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(staleAdmits == false,
                "the stale watermark makes activeEpisodeHotPathCaughtUp() false, permanently nullifying the glo9 drain on a complete episode")
        let staleCatchup = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(staleCatchup != nil,
                "the same stale watermark also keeps the yqax catch-up trigger firing on an episode with nothing left to transcribe")

        // Repair persisted state exactly as the V37 migration / resume /
        // finalization reconcile does — no re-transcription involved.
        #expect(try await fx.store.reconcileFastTranscriptCoverage(id: fx.asset.id) == 3575.88)

        let repairedAdmits = await fx.scheduler.wouldAdmitDeferredWorkForTesting()
        #expect(repairedAdmits == true,
                "with the watermark reconciled to real chunk coverage the hot path is 2575 s ahead of the playhead — the glo9 drain must admit")
        let repairedCatchup = await fx.scheduler.currentCatchupOpportunityForTesting()
        #expect(repairedCatchup == nil,
                "and the redundant catch-up escalation must stop")
    }

    /// Builds a `pass='fast'` chunk row for the active episode's asset.
    private func makeFastChunk(
        assetId: String,
        index: Int,
        start: Double,
        end: Double
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-c\(index)",
            analysisAssetId: assetId,
            segmentFingerprint: "\(assetId)-fp\(index)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "segment \(index)",
            normalizedText: "segment \(index)",
            pass: "fast",
            modelVersion: "test-asr",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }
}

#endif
