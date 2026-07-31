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
        #expect(
            advisory?.reason == AnalysisWorkScheduler.laneGateRejectReason(profile: .serious),
            "the advisory must name the gate that fired and the profile that closed it; got \(advisory?.reason ?? "nil")"
        )
    }

    @Test("A hold counts CONSECUTIVE skips and clears the count on dispatch",
          .timeLimit(.minutes(1)))
    func holdCountsConsecutiveSkipsAndClearsOnDispatch() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let job = try await insertDownloadJob(
            store: store, downloads: downloads,
            jobId: "count-held", episodeId: "ep-count-held",
            priority: 0
        )
        let scheduler = makeScheduler(
            store: store, downloads: downloads,
            thermalState: .serious, audio: FailingDecodeStub()
        )
        await scheduler.updateScenePhase(.foreground)

        #expect(await scheduler.laneHold(forJobId: job.jobId) == nil,
                "precondition: nothing held before the first pass")

        for expected in 1...3 {
            await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600))
            let hold = try #require(await scheduler.laneHold(forJobId: job.jobId))
            #expect(
                hold.consecutiveSkips == expected,
                "each back-to-back held pass must increment the consecutive count; pass \(expected) reported \(hold.consecutiveSkips)"
            )
            #expect(hold.lane == .background)
            #expect(hold.qualityProfile == .serious)
        }
        let heldRecord = try #require(await scheduler.laneHold(forJobId: job.jobId))
        #expect(heldRecord.firstHeldAt <= heldRecord.lastHeldAt,
                "firstHeldAt must anchor the START of the run, not move with it")
        #expect(await scheduler.currentLaneHolds().count == 1)

        // The device cools. The same job dispatches, and the count must not
        // survive it — a LIFETIME counter would still read 3 here and could
        // never distinguish "stuck" from "skipped once, thirty times".
        let cool = makeScheduler(
            store: store, downloads: downloads,
            thermalState: .nominal, audio: FailingDecodeStub()
        )
        await cool.updateScenePhase(.foreground)
        await cool.drainEligible(deadline: ContinuousClock.now + .seconds(600))
        #expect(await cool.laneHold(forJobId: job.jobId) == nil,
                "a dispatch must drop the hold record so `consecutive` means consecutive")
        #expect(await cool.currentLaneHolds().isEmpty)
    }

    @Test("A held episode surfaces a cause to the Activity layer instead of silence",
          .timeLimit(.minutes(1)))
    func heldEpisodeSurfacesCause() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertDownloadJob(
            store: store, downloads: downloads,
            jobId: "surf-held", episodeId: "ep-surf-held",
            priority: 0
        )
        let scheduler = makeScheduler(
            store: store, downloads: downloads,
            thermalState: .serious, audio: FailingDecodeStub()
        )
        await scheduler.updateScenePhase(.foreground)

        #expect(await scheduler.heldEpisodeCauses().isEmpty)

        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600))

        let causes = await scheduler.heldEpisodeCauses()
        let cause = try #require(causes["ep-surf-held"])
        // Charging, battery 0.9, LPM off ⇒ the profile can only be thermal, and
        // the surface must say so rather than blame the battery.
        #expect(cause == .thermal)

        // And it maps to real user copy, not a raw enum name. This is the whole
        // point of the mandate: the queue stops being silently empty-looking.
        let attribution = CauseAttributionPolicy.attribute(
            cause,
            context: CauseAttributionContext(modelAvailableNow: true, retryBudgetRemaining: 1)
        )
        #expect(attribution.disposition == .paused)
        #expect(attribution.reason == .phoneIsHot)
        #expect(SurfaceReasonCopyTemplates.template(for: attribution.reason)
            == "Paused — phone is too hot")
    }

    // MARK: - The bound: the progress floor

    @Test("A job queued past the floor with attemptCount 0 gets one bounded slice at .serious",
          .timeLimit(.minutes(1)))
    func progressFloorAdmitsAnAncientNeverAttemptedJob() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let floor = AnalysisWorkScheduler.LaneGatePolicy.progressFloorAfterSec
        // Background lane at .serious ⇒ the lane is closed. But this row has
        // sat, never once attempted, for longer than the floor.
        try await insertDownloadJob(
            store: store, downloads: downloads,
            jobId: "floor-old", episodeId: "ep-floor-old",
            priority: 0, attemptCount: 0,
            createdAt: Date().timeIntervalSince1970 - (floor + 60)
        )
        let scheduler = makeScheduler(
            store: store, downloads: downloads,
            thermalState: .serious, audio: FailingDecodeStub()
        )
        await scheduler.updateScenePhase(.foreground)

        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600))

        let after = try #require(try await store.fetchJob(byId: "floor-old"))
        #expect(
            after.attemptCount > 0 || after.state != "queued",
            """
            "Queued forever" must be impossible. A never-attempted job that has \
            waited past the floor gets exactly one bounded slice even while its \
            lane is closed; here it stayed untouched (state=\(after.state), \
            attemptCount=\(after.attemptCount)).
            """
        )
        #expect(
            after.desiredCoverageSec == Self.fullEpisodeCoverageSec,
            "the floor caps THIS PASS only — it must never shrink the job's persisted target"
        )
    }

    @Test("The progress floor never fires at .critical",
          .timeLimit(.minutes(1)))
    func progressFloorNeverFiresAtCritical() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let floor = AnalysisWorkScheduler.LaneGatePolicy.progressFloorAfterSec
        try await insertDownloadJob(
            store: store, downloads: downloads,
            jobId: "floor-critical", episodeId: "ep-floor-critical",
            priority: 0, attemptCount: 0,
            createdAt: Date().timeIntervalSince1970 - (floor + 60)
        )
        let scheduler = makeScheduler(
            store: store, downloads: downloads,
            thermalState: .critical, audio: FailingDecodeStub()
        )
        await scheduler.updateScenePhase(.foreground)

        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(600))

        let after = try #require(try await store.fetchJob(byId: "floor-critical"))
        #expect(after.state == "queued" && after.attemptCount == 0,
                "pauseAllWork dominates the floor — a liveness bound is not a licence to cook the phone")
    }

    // MARK: - Pure policy truth table

    private func admission(_ profile: QualityProfile) -> AnalysisWorkScheduler.LaneAdmission {
        AnalysisWorkScheduler.LaneAdmission(
            qualityProfile: profile,
            policy: profile.schedulerPolicy
        )
    }

    private func gate(
        lane: AnalysisWorkScheduler.SchedulerLane,
        profile: QualityProfile,
        queuedForSec: TimeInterval = 0,
        attemptCount: Int = 0,
        desiredCoverageSec: Double = fullEpisodeCoverageSec,
        t1DepthSeconds: Double = 300
    ) -> AnalysisWorkScheduler.LaneGateOutcome {
        AnalysisWorkScheduler.evaluateLaneGate(
            lane: lane,
            admission: admission(profile),
            queuedForSec: queuedForSec,
            attemptCount: attemptCount,
            desiredCoverageSec: desiredCoverageSec,
            t1DepthSeconds: t1DepthSeconds
        )
    }

    @Test("evaluateLaneGate: the (lane × profile) admission matrix")
    func laneGateMatrix() {
        // Now lane: admitted everywhere except critical. This single row is
        // the bead — a user's explicit download must drain on a warm device.
        #expect(gate(lane: .now, profile: .nominal) == .admit)
        #expect(gate(lane: .now, profile: .fair) == .admit)
        #expect(gate(lane: .now, profile: .serious) == .admit)
        #expect(gate(lane: .now, profile: .critical)
                == .hold(reason: AnalysisWorkScheduler.laneGateRejectReason(profile: .critical)))

        // Soon: open through fair, closed at serious.
        #expect(gate(lane: .soon, profile: .nominal) == .admit)
        #expect(gate(lane: .soon, profile: .fair) == .admit)
        #expect(gate(lane: .soon, profile: .serious)
                == .hold(reason: AnalysisWorkScheduler.laneGateRejectReason(profile: .serious)))

        // Background: nominal only. This is the ratified thermal policy the
        // bead is explicitly NOT allowed to widen.
        #expect(gate(lane: .background, profile: .nominal) == .admit)
        #expect(gate(lane: .background, profile: .fair)
                == .hold(reason: AnalysisWorkScheduler.laneGateRejectReason(profile: .fair)))
        #expect(gate(lane: .background, profile: .serious)
                == .hold(reason: AnalysisWorkScheduler.laneGateRejectReason(profile: .serious)))
    }

    @Test("evaluateLaneGate: coverage DEPTH never decides admission")
    func laneGateIgnoresCoverageDepth() {
        // The precise inversion that caused the incident: at the old gate,
        // crossing t2DepthSeconds (900) flipped a job from admitted to
        // silently-skipped. Lane, not depth, must decide — so a 90-second
        // Now-lane job and a 5417-second one get identical verdicts, and so
        // do the two Background ones.
        for coverage in [90.0, 899.0, 900.0, 5417.0] {
            #expect(gate(lane: .now, profile: .serious, desiredCoverageSec: coverage) == .admit,
                    "Now lane must admit at .serious regardless of depth (\(coverage)s)")
            #expect(gate(lane: .background, profile: .fair, desiredCoverageSec: coverage)
                    == .hold(reason: AnalysisWorkScheduler.laneGateRejectReason(profile: .fair)),
                    "Background lane must hold at .fair regardless of depth (\(coverage)s)")
        }
    }

    @Test("evaluateLaneGate: the progress floor's three preconditions")
    func laneGateProgressFloorPreconditions() {
        let floor = AnalysisWorkScheduler.LaneGatePolicy.progressFloorAfterSec

        // Fires: closed lane, never attempted, waited past the floor.
        #expect(
            gate(lane: .background, profile: .serious, queuedForSec: floor, attemptCount: 0)
                == .admitProgressFloor(coverageCapSec: 300)
        )
        // Exactly AT the floor fires (>=), one second under does not.
        #expect(
            gate(lane: .background, profile: .serious, queuedForSec: floor - 1, attemptCount: 0)
                == .hold(reason: AnalysisWorkScheduler.laneGateRejectReason(profile: .serious))
        )
        // One prior attempt disables it — this is what makes the floor
        // one-shot, since every dispatch outcome moves attemptCount off zero.
        #expect(
            gate(lane: .background, profile: .serious, queuedForSec: floor * 10, attemptCount: 1)
                == .hold(reason: AnalysisWorkScheduler.laneGateRejectReason(profile: .serious))
        )
        // Never at critical, however long the wait.
        #expect(
            gate(lane: .background, profile: .critical, queuedForSec: floor * 100, attemptCount: 0)
                == .hold(reason: AnalysisWorkScheduler.laneGateRejectReason(profile: .critical))
        )
        // An open lane never routes through the floor — it admits at FULL
        // depth. A floor cap leaking onto the healthy path would silently
        // shrink every dispatch to 300s.
        #expect(
            gate(lane: .now, profile: .serious, queuedForSec: floor * 10, attemptCount: 0)
                == .admit
        )
    }

    @Test("evaluateLaneGate: the floor slice is min(desired, t1Depth)")
    func laneGateProgressFloorCap() {
        let floor = AnalysisWorkScheduler.LaneGatePolicy.progressFloorAfterSec
        // Deep job ⇒ capped at the Soon depth: a floor pass costs a Soon-lane
        // dispatch, never a Background one.
        #expect(
            gate(lane: .background, profile: .serious, queuedForSec: floor,
                 desiredCoverageSec: 5417, t1DepthSeconds: 300)
                == .admitProgressFloor(coverageCapSec: 300)
        )
        // Shallow job ⇒ capped at what it actually wants. The floor must never
        // ask for MORE coverage than the job's own target.
        #expect(
            gate(lane: .background, profile: .serious, queuedForSec: floor,
                 desiredCoverageSec: 90, t1DepthSeconds: 300)
                == .admitProgressFloor(coverageCapSec: 90)
        )
    }

    @Test("throttleCause names the real constraint, not always thermal")
    func throttleCauseNamesTheRealConstraint() {
        // Low Power Mode wins: QualityProfile.derive tests it first, so the
        // copy must not claim the phone is hot.
        #expect(AnalysisWorkScheduler.throttleCause(
            isLowPowerMode: true, batteryLevel: 0.9, isCharging: true) == .lowPowerMode)
        // Low battery off the cord.
        #expect(AnalysisWorkScheduler.throttleCause(
            isLowPowerMode: false, batteryLevel: 0.1, isCharging: false) == .batteryLowUnplugged)
        // Same level ON the cord is not a battery demotion — derive() exempts
        // a charging device, so naming the battery here would be a lie.
        #expect(AnalysisWorkScheduler.throttleCause(
            isLowPowerMode: false, batteryLevel: 0.1, isCharging: true) == .thermal)
        // -1 is UIDevice's "monitoring off" sentinel: unknown never demotes,
        // so it must never be named as the cause either.
        #expect(AnalysisWorkScheduler.throttleCause(
            isLowPowerMode: false, batteryLevel: -1, isCharging: false) == .thermal)
        #expect(AnalysisWorkScheduler.throttleCause(
            isLowPowerMode: false, batteryLevel: 0.9, isCharging: false) == .thermal)
    }

    // MARK: - Store predicate (Caveat 2)

    @Test("fetchNextEligibleJob: the Now-lane carve-out is what makes the row visible",
          .timeLimit(.minutes(1)))
    func storeCarveOutSelectsNowLaneWithDeferredDisallowed() async throws {
        let store = try await makeTestStore()
        let now = Date().timeIntervalSince1970
        try await store.insertJob(makeAnalysisJob(
            jobId: "sql-now", jobType: "preAnalysis", episodeId: "ep-sql-now",
            workKey: "fp-sql-now:1:preAnalysis", sourceFingerprint: "fp-sql-now",
            priority: AnalysisWorkScheduler.nowLanePriorityFloor,
            desiredCoverageSec: Self.fullEpisodeCoverageSec, state: "queued"
        ))

        // Pre-ewag behaviour (carve-out disabled): invisible. This is the
        // assertion that proves the SQL half was load-bearing, not decoration.
        #expect(
            try await store.fetchNextEligibleJob(
                deferredWorkAllowed: false, t0ThresholdSec: 90, now: now + 1
            ) == nil,
            "without the carve-out an unrelaxed .serious hides the row entirely"
        )
        // With it: selected.
        #expect(
            try await store.fetchNextEligibleJob(
                deferredWorkAllowed: false,
                nowLanePriorityFloor: AnalysisWorkScheduler.nowLanePriorityFloor,
                t0ThresholdSec: 90, now: now + 1
            )?.jobId == "sql-now"
        )
    }

    @Test("fetchNextEligibleJob: the carve-out does NOT widen Soon/Background",
          .timeLimit(.minutes(1)))
    func storeCarveOutDoesNotWidenLowerLanes() async throws {
        let store = try await makeTestStore()
        let now = Date().timeIntervalSince1970
        // One below the Now floor ⇒ Soon lane. Must stay hidden when deferred
        // work is disallowed, or the carve-out has quietly repealed the
        // thermal policy instead of honouring one lane.
        try await store.insertJob(makeAnalysisJob(
            jobId: "sql-soon", jobType: "preAnalysis", episodeId: "ep-sql-soon",
            workKey: "fp-sql-soon:1:preAnalysis", sourceFingerprint: "fp-sql-soon",
            priority: AnalysisWorkScheduler.nowLanePriorityFloor - 1,
            desiredCoverageSec: Self.fullEpisodeCoverageSec, state: "queued"
        ))

        #expect(
            try await store.fetchNextEligibleJob(
                deferredWorkAllowed: false,
                nowLanePriorityFloor: AnalysisWorkScheduler.nowLanePriorityFloor,
                t0ThresholdSec: 90, now: now + 1
            ) == nil,
            "a priority-19 row is Soon lane and must remain gated"
        )
        #expect(
            try await store.fetchNextEligibleJob(
                deferredWorkAllowed: true,
                nowLanePriorityFloor: AnalysisWorkScheduler.nowLanePriorityFloor,
                t0ThresholdSec: 90, now: now + 1
            )?.jobId == "sql-soon",
            "and must still be selectable the moment deferred work is allowed"
        )
    }

    @Test("The SQL floor and AnalysisJob.schedulerLane agree on where Now begins")
    func priorityFloorMatchesLaneDerivation() {
        // The SQL carve-out compares `priority >= nowLanePriorityFloor` while
        // dispatch classifies with `schedulerLane`. If those two ever disagree
        // the query selects rows the gate then refuses (or vice versa), which
        // is the exact shape of this bead's defect.
        func lane(_ priority: Int) -> AnalysisWorkScheduler.SchedulerLane {
            makeAnalysisJob(priority: priority).schedulerLane
        }
        #expect(lane(AnalysisWorkScheduler.nowLanePriorityFloor) == .now)
        #expect(lane(AnalysisWorkScheduler.nowLanePriorityFloor - 1) == .soon)
        #expect(lane(AnalysisWorkScheduler.soonLanePriorityFloor) == .soon)
        #expect(lane(AnalysisWorkScheduler.soonLanePriorityFloor - 1) == .background)
    }
}
