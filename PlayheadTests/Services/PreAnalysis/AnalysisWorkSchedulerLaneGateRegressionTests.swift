// AnalysisWorkSchedulerLaneGateRegressionTests.swift
// playhead-ewag: durable regression pin on the "queued preAnalysis jobs are
// never dispatched" starvation.
//
// Incident (clean install, 2026-07-31): five user-tapped downloads completed,
// five `preAnalysis` rows were enqueued at priority 20 with
// `desiredCoverageSec = full episode duration`, and NOT ONE was ever leased.
// `attemptCount = 0`, `lastRejectReason = NULL`, `updatedAt` untouched for 20+
// minutes on a foregrounded, charging device.
//
// Root cause: the run loop's post-selection filter classified a job's LANE
// from its coverage DEPTH (`desiredCoverageSec >= t2DepthSeconds` ⇒ Background),
// and `QualityProfile.allowBackgroundLane` is true only at `.nominal`. Since
// playhead-3xtw the user-download path stamps the full episode duration at
// enqueue, so every download of a 15-minute-or-longer episode was classified
// deep-Background on its FIRST dispatch, before one second of audio had been
// processed — and the device sits at `.serious` precisely because the rest of
// the pipeline is busy. `desiredCoverageSec` names the eventual coverage
// TARGET; the filter read it as the COST of the next dispatch.
//
// Fix (Dan, Option A): gate by the job's `schedulerLane` (priority-derived)
// using the previously-dead `LaneAdmission.allows(lane:)`, whose own doc
// comment already stated that Now-lane work is user-initiated and "must drain
// promptly even in serious thermal states".
//
// These tests drive the real `AnalysisWorkScheduler` / `AnalysisStore`
// deterministically (`drainEligible` awaits each job to completion; no
// wall-clock polling). Every test in the first three sections is RED on
// pre-fix source.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-ewag: Now-lane starvation regression")
struct AnalysisWorkSchedulerLaneGateRegressionTests {

    // MARK: - Test doubles

    /// Decode stub that always throws — routes `processJob` through the
    /// `.failed` outcome arm. Combined with a pre-stamped high `attemptCount`
    /// this drives a job to a terminal state in a single dispatch, giving
    /// `drainEligible` a deterministic fixed point. (Same proven pattern as
    /// `AnalysisPipelineStallRegressionTests`.)
    private final class FailingDecodeStub: AnalysisAudioProviding, @unchecked Sendable {
        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            throw AnalysisAudioError.decodingFailed("Operation Interrupted")
        }
    }

    // MARK: - Fixtures

    /// Duration of the field-incident episode (92 min). Any value at or above
    /// `PreAnalysisConfig.t2DepthSeconds` (900) reproduces the mis-classification;
    /// the real number is used so the fixture reads as the incident.
    private static let fullEpisodeCoverageSec: Double = 5417

    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        thermalState: ThermalState,
        audio: any AnalysisAudioProviding = StubAnalysisAudioProvider(),
        config: PreAnalysisConfig = PreAnalysisConfig()
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audio,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(
                    thermalState: thermalState,
                    isLowPowerMode: false,
                    isCharging: true
                )
            ),
            downloadManager: downloads,
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            config: config
        )
    }

    /// Insert the incident's job shape: a compute-only (cached file on disk,
    /// zero network) `preAnalysis` row whose `desiredCoverageSec` is the whole
    /// episode, at the priority the explicit-download path stamps.
    @discardableResult
    private func insertDownloadJob(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        jobId: String,
        episodeId: String,
        priority: Int,
        desiredCoverageSec: Double = fullEpisodeCoverageSec,
        attemptCount: Int = 0,
        createdAt: Double = Date().timeIntervalSince1970
    ) async throws -> AnalysisJob {
        downloads.cachedURLs[episodeId] = URL(fileURLWithPath: "/tmp/\(episodeId).m4a")
        let job = makeAnalysisJob(
            jobId: jobId,
            jobType: "preAnalysis",
            episodeId: episodeId,
            analysisAssetId: nil,
            workKey: AnalysisJob.computeWorkKey(
                fingerprint: "fp-\(jobId)",
                analysisVersion: PreAnalysisConfig.analysisVersion,
                jobType: "preAnalysis"
            ),
            sourceFingerprint: "fp-\(jobId)",
            priority: priority,
            desiredCoverageSec: desiredCoverageSec,
            state: "queued",
            attemptCount: attemptCount,
            createdAt: createdAt,
            updatedAt: createdAt
        )
        try await store.insertJob(job)
        return job
    }

    // MARK: - The bug: a user download never dispatches

    @Test("A priority-20 job with desiredCoverageSec = full episode duration dispatches at .serious",
          .timeLimit(.minutes(1)))
    func nowLaneFullEpisodeJobDispatchesAtSerious() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        // attemptCount 4 (of maxAttemptCount 5) + a failing decode gives the
        // drain a one-pass fixed point. It ALSO proves the dispatch came from
        // the lane gate and not from the progress floor, which only ever
        // admits a job with `attemptCount == 0`.
        try await insertDownloadJob(
            store: store, downloads: downloads,
            jobId: "dl-serious", episodeId: "ep-dl-serious",
            priority: 20, attemptCount: 4
        )
        let scheduler = makeScheduler(
            store: store, downloads: downloads,
            thermalState: .serious, audio: FailingDecodeStub()
        )
        // The field shape: app foregrounded, nothing playing.
        await scheduler.updateScenePhase(.foreground)

        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600))

        let stillQueued = try await store.fetchJobsByState("queued")
        #expect(
            !stillQueued.contains { $0.jobId == "dl-serious" },
            """
            A user-initiated download (priority 20 ⇒ Now lane) must dispatch at \
            QualityProfile .serious. Pre-fix it was classified Background by its \
            coverage DEPTH (5417 ≥ t2DepthSeconds 900) and silently skipped forever.
            """
        )
    }

    @Test("A Now-lane job is SELECTABLE under unrelaxed .serious (the SQL must not hide it)",
          .timeLimit(.minutes(1)))
    func nowLaneJobSelectableUnderUnrelaxedSerious() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertDownloadJob(
            store: store, downloads: downloads,
            jobId: "dl-bg-serious", episodeId: "ep-dl-bg-serious",
            priority: 20, attemptCount: 4
        )
        let scheduler = makeScheduler(
            store: store, downloads: downloads,
            thermalState: .serious, audio: FailingDecodeStub()
        )
        // `.background` scene phase with an idle transport: deferred work is
        // NOT blocked by the (scenePhase, playbackContext) matrix, but the
        // foreground-aggressive Soon relaxation does NOT apply either — so
        // `allowSoonLane` and `allowBackgroundLane` are both false and
        // `deferredWorkAllowed` binds 0. This is the path the overnight BGTask
        // drain takes.
        await scheduler.updateScenePhase(.background)

        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600))

        let stillQueued = try await store.fetchJobsByState("queued")
        #expect(
            !stillQueued.contains { $0.jobId == "dl-bg-serious" },
            """
            Under unrelaxed .serious the store predicate binds deferredWorkAllowed=0 \
            and the SELECT itself hides every non-playback row. Honouring the Now \
            lane only AFTER selection leaves the fix inert on this path — which is \
            the same path the overnight BGTask drain uses.
            """
        )
    }

    // MARK: - The bound: a hold is never silent

    @Test("A lane-gated hold writes a durable reject reason (never silence)",
          .timeLimit(.minutes(1)))
    func laneGatedHoldWritesDurableReason() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        // priority 0 ⇒ Background lane. `.serious` closes the Background lane
        // and the foreground-aggressive relaxation deliberately reopens ONLY
        // Soon, so this job is legitimately held — but the hold must be
        // recorded, not silent.
        let job = try await insertDownloadJob(
            store: store, downloads: downloads,
            jobId: "auto-held", episodeId: "ep-auto-held",
            priority: 0
        )
        let scheduler = makeScheduler(
            store: store, downloads: downloads,
            thermalState: .serious, audio: FailingDecodeStub()
        )
        await scheduler.updateScenePhase(.foreground)

        #expect(try await store.fetchJobAdmissionReject(jobId: job.jobId) == nil,
                "precondition: no advisory before the pass")

        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600))

        #expect(try await store.fetchJobsByState("queued").contains { $0.jobId == "auto-held" },
                "an auto-enqueued Background-lane job must still be thermally held at .serious")

        let advisory = try await store.fetchJobAdmissionReject(jobId: job.jobId)
        #expect(
            advisory != nil,
            """
            The lane/depth filter wrote NOTHING to the DB, which is exactly why \
            five starved jobs read `lastRejectReason = NULL` and the bug survived \
            for weeks. A hold must leave a durable, queryable trace.
            """
        )
        #expect((advisory?.at ?? 0) > 0)
    }
}
