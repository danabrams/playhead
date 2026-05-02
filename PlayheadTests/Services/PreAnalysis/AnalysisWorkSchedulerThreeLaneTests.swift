// AnalysisWorkSchedulerThreeLaneTests.swift
// Tests for the three-lane scheduler model (Now / Soon / Background)
// introduced in playhead-r835. These tests exercise the lane derivation from
// AnalysisJob.priority, LaneAdmission gating by SchedulerLane, per-lane
// concurrency caps (with the T0 playback exemption for the Now lane), and
// the preemption-hook call site.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — Three-Lane Scheduler")
struct AnalysisWorkSchedulerThreeLaneTests {

    // MARK: - Scheduler construction helper

    /// Build a scheduler backed by in-memory / stub dependencies suitable
    /// for exercising the admission + preemption-hook call site. Matches
    /// the pattern in `AnalysisWorkSchedulerLaneAdmissionTests`.
    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider = StubDownloadProvider(),
        capabilities: any CapabilitiesProviding = StubCapabilitiesProvider(),
        battery: StubBatteryProvider = {
            let b = StubBatteryProvider()
            b.level = 0.9
            b.charging = true
            return b
        }(),
        config: PreAnalysisConfig = PreAnalysisConfig()
    ) -> AnalysisWorkScheduler {
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
            downloadManager: downloads,
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            config: config
        )
    }

    // MARK: - AnalysisJob.schedulerLane boundary derivation

    @Test("priority == 20 maps to .now (inclusive lower bound)")
    func testPriorityTwentyIsNow() {
        let job = makeAnalysisJob(priority: 20)
        #expect(job.schedulerLane == .now)
    }

    @Test("priority == 100 maps to .now (well above threshold)")
    func testPriorityHundredIsNow() {
        let job = makeAnalysisJob(priority: 100)
        #expect(job.schedulerLane == .now)
    }

    @Test("priority == 19 maps to .soon (just below Now)")
    func testPriorityNineteenIsSoon() {
        let job = makeAnalysisJob(priority: 19)
        #expect(job.schedulerLane == .soon)
    }

    @Test("priority == 1 maps to .soon (inclusive lower bound of Soon)")
    func testPriorityOneIsSoon() {
        let job = makeAnalysisJob(priority: 1)
        #expect(job.schedulerLane == .soon)
    }

    @Test("priority == 0 maps to .background (inclusive upper bound of Background)")
    func testPriorityZeroIsBackground() {
        let job = makeAnalysisJob(priority: 0)
        #expect(job.schedulerLane == .background)
    }

    @Test("priority == -1 maps to .background")
    func testPriorityNegativeOneIsBackground() {
        let job = makeAnalysisJob(priority: -1)
        #expect(job.schedulerLane == .background)
    }

    // MARK: - LaneAdmission gating by SchedulerLane

    @Test("LaneAdmission.allows(.now): nominal admits")
    func testLaneAdmissionNowNominal() {
        let admission = AnalysisWorkScheduler.LaneAdmission(
            qualityProfile: .nominal,
            policy: QualityProfile.nominal.schedulerPolicy
        )
        #expect(admission.allows(lane: .now))
    }

    @Test("LaneAdmission.allows(.now): fair admits")
    func testLaneAdmissionNowFair() {
        let admission = AnalysisWorkScheduler.LaneAdmission(
            qualityProfile: .fair,
            policy: QualityProfile.fair.schedulerPolicy
        )
        #expect(admission.allows(lane: .now))
    }

    @Test("LaneAdmission.allows(.now): serious admits — Now lane bypasses Soon/Background gates")
    func testLaneAdmissionNowSeriousAdmits() {
        let admission = AnalysisWorkScheduler.LaneAdmission(
            qualityProfile: .serious,
            policy: QualityProfile.serious.schedulerPolicy
        )
        // The spec: "thermal=serious pauses Soon + Background but admits Now."
        #expect(admission.allows(lane: .now))
        #expect(!admission.allows(lane: .soon))
        #expect(!admission.allows(lane: .background))
    }

    @Test("LaneAdmission.allows(.now): critical blocks everything")
    func testLaneAdmissionNowCriticalBlocks() {
        let admission = AnalysisWorkScheduler.LaneAdmission(
            qualityProfile: .critical,
            policy: QualityProfile.critical.schedulerPolicy
        )
        #expect(!admission.allows(lane: .now))
        #expect(!admission.allows(lane: .soon))
        #expect(!admission.allows(lane: .background))
    }

    @Test("LaneAdmission.allows(.soon): fair admits Soon, blocks Background")
    func testLaneAdmissionSoonFair() {
        let admission = AnalysisWorkScheduler.LaneAdmission(
            qualityProfile: .fair,
            policy: QualityProfile.fair.schedulerPolicy
        )
        #expect(admission.allows(lane: .soon))
        #expect(!admission.allows(lane: .background))
    }

    @Test("LaneAdmission.allows(.background): nominal admits Background")
    func testLaneAdmissionBackgroundNominal() {
        let admission = AnalysisWorkScheduler.LaneAdmission(
            qualityProfile: .nominal,
            policy: QualityProfile.nominal.schedulerPolicy
        )
        #expect(admission.allows(lane: .background))
    }

    // MARK: - Per-lane concurrency caps (actor-isolated accounting)

    @Test("Now-lane cap: admits up to 2 concurrent non-playback jobs, then rejects")
    func testNowLaneCapRejectsThirdConcurrent() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        let job1 = makeAnalysisJob(jobId: "now-1", jobType: "preAnalysis", priority: 20)
        let job2 = makeAnalysisJob(jobId: "now-2", jobType: "preAnalysis", priority: 50)
        let job3 = makeAnalysisJob(jobId: "now-3", jobType: "preAnalysis", priority: 100)

        #expect(await scheduler.canAdmit(job: job1))
        await scheduler.didStart(job: job1)

        #expect(await scheduler.canAdmit(job: job2))
        await scheduler.didStart(job: job2)

        // Third non-playback Now job is rejected (cap = 2).
        #expect(!(await scheduler.canAdmit(job: job3)))
    }

    @Test("Now-lane cap: T0 playback is exempt even when 2 non-playback Now jobs are running")
    func testT0PlaybackExemptFromNowCap() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        let preAnalysisA = makeAnalysisJob(jobId: "pa-1", jobType: "preAnalysis", priority: 20)
        let preAnalysisB = makeAnalysisJob(jobId: "pa-2", jobType: "preAnalysis", priority: 25)
        let playback = makeAnalysisJob(jobId: "pb-1", jobType: "playback", priority: 20)

        await scheduler.didStart(job: preAnalysisA)
        await scheduler.didStart(job: preAnalysisB)

        // Cap reached for non-playback jobs, but playback is exempt.
        let nextPreAnalysis = makeAnalysisJob(jobId: "pa-3", jobType: "preAnalysis", priority: 30)
        #expect(!(await scheduler.canAdmit(job: nextPreAnalysis)))
        #expect(await scheduler.canAdmit(job: playback))
    }

    @Test("Now-lane cap: T0 playback admitted even at zero-cap (defensive)")
    func testT0PlaybackExemptEvenAtSaturation() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        // Fill with many playback jobs — they are exempt so cap never binds them.
        for i in 0..<5 {
            let playback = makeAnalysisJob(
                jobId: "pb-\(i)",
                jobType: "playback",
                priority: 20
            )
            await scheduler.didStart(job: playback)
        }
        let anotherPlayback = makeAnalysisJob(jobId: "pb-extra", jobType: "playback", priority: 20)
        #expect(await scheduler.canAdmit(job: anotherPlayback))
    }

    @Test("Soon-lane cap: admits 1 concurrent, rejects 2nd")
    func testSoonLaneCapRejectsSecondConcurrent() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        let job1 = makeAnalysisJob(jobId: "soon-1", jobType: "preAnalysis", priority: 10)
        let job2 = makeAnalysisJob(jobId: "soon-2", jobType: "preAnalysis", priority: 5)

        #expect(await scheduler.canAdmit(job: job1))
        await scheduler.didStart(job: job1)
        #expect(!(await scheduler.canAdmit(job: job2)))
    }

    @Test("Background-lane cap: admits 1 concurrent, rejects 2nd")
    func testBackgroundLaneCapRejectsSecondConcurrent() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        let job1 = makeAnalysisJob(jobId: "bg-1", jobType: "preAnalysis", priority: 0)
        let job2 = makeAnalysisJob(jobId: "bg-2", jobType: "preAnalysis", priority: -5)

        #expect(await scheduler.canAdmit(job: job1))
        await scheduler.didStart(job: job1)
        #expect(!(await scheduler.canAdmit(job: job2)))
    }

    @Test("didFinish decrements the per-lane count and frees capacity")
    func testDidFinishReleasesCapacity() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        let first = makeAnalysisJob(jobId: "soon-1", jobType: "preAnalysis", priority: 5)
        await scheduler.didStart(job: first)

        let second = makeAnalysisJob(jobId: "soon-2", jobType: "preAnalysis", priority: 5)
        #expect(!(await scheduler.canAdmit(job: second)))

        await scheduler.didFinish(job: first)
        #expect(await scheduler.canAdmit(job: second))
    }

    @Test("didFinish is clamped at zero (stray double-finish does not go negative)")
    func testDidFinishClampedAtZero() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        let job = makeAnalysisJob(jobId: "soon-1", jobType: "preAnalysis", priority: 5)
        // Two finishes without a start — count must not go negative.
        await scheduler.didFinish(job: job)
        await scheduler.didFinish(job: job)

        #expect(await scheduler.laneActiveCount(.soon) == 0)
    }

    @Test("Lanes are independent: filling Soon does not block Now or Background")
    func testLanesAreIndependent() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        let soon = makeAnalysisJob(jobId: "soon-1", jobType: "preAnalysis", priority: 5)
        await scheduler.didStart(job: soon)

        let now = makeAnalysisJob(jobId: "now-1", jobType: "preAnalysis", priority: 25)
        let background = makeAnalysisJob(jobId: "bg-1", jobType: "preAnalysis", priority: 0)

        #expect(await scheduler.canAdmit(job: now))
        #expect(await scheduler.canAdmit(job: background))
    }

    // MARK: - Preemption hook call site (playhead-r835 FIX 1)

    /// Spy that records every lane handed to `preemptLowerLanes(for:)`.
    /// Used to verify the scheduler loop invokes the hook iff it admits a
    /// Now-lane job. Backed by an actor so writes from the scheduler
    /// (inside its own actor) and reads from the test (on the test task)
    /// are properly serialized under Swift 6 concurrency.
    private actor CallLog {
        private(set) var calls: [AnalysisWorkScheduler.SchedulerLane] = []
        func append(_ lane: AnalysisWorkScheduler.SchedulerLane) {
            calls.append(lane)
        }
    }

    private final class SpyHandler: LanePreemptionHandler, Sendable {
        let log = CallLog()

        func preemptLowerLanes(for incoming: AnalysisWorkScheduler.SchedulerLane) async {
            await log.append(incoming)
        }
    }

    @Test("Scheduler accepts a LanePreemptionHandler and does not invoke its methods at construction")
    func testPreemptionHandlerInstallIsInertAtSetup() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)
        let handler = SpyHandler()
        await scheduler.setLanePreemptionHandler(handler)

        // No calls should have been made simply by installing the handler.
        let calls = await handler.log.calls
        #expect(calls.isEmpty)
    }

    @Test("Preemption hook is invoked with .now when a priority>=20 job is admitted")
    func testPreemptionHookCalledForNowLaneJob() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        // Give the job a cached file so it proceeds past processJob's missing-
        // file guard; otherwise the loop would still call the hook (preemption
        // runs BEFORE processJob) but the job would flip to blocked:missingFile
        // which also works. Explicit URL makes the intent obvious.
        downloads.cachedURLs["ep-now"] = URL(fileURLWithPath: "/tmp/ep-now.mp3")

        let job = makeAnalysisJob(
            jobId: "now-admit-job",
            jobType: "preAnalysis",
            episodeId: "ep-now",
            workKey: "fp-now:1:preAnalysis",
            sourceFingerprint: "fp-now",
            priority: 20,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let handler = SpyHandler()
        await scheduler.setLanePreemptionHandler(handler)
        await scheduler.startSchedulerLoop()
        defer { Task { await scheduler.stop() } }

        let sawNow = await pollUntil {
            await handler.log.calls.contains(.now)
        }
        await scheduler.stop()
        let finalCalls = await handler.log.calls
        #expect(sawNow, "Preemption hook should be invoked with .now for a priority>=20 job")
        #expect(!finalCalls.contains(.soon),
                "Preemption hook must not receive .soon when the admitted job is Now")
        #expect(!finalCalls.contains(.background),
                "Preemption hook must not receive .background when the admitted job is Now")
    }

    @Test("Preemption hook is NOT invoked when only a Soon-lane job is admitted")
    func testPreemptionHookNotCalledForSoonLaneJob() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-soon"] = URL(fileURLWithPath: "/tmp/ep-soon.mp3")

        let job = makeAnalysisJob(
            jobId: "soon-admit-job",
            jobType: "preAnalysis",
            episodeId: "ep-soon",
            workKey: "fp-soon:1:preAnalysis",
            sourceFingerprint: "fp-soon",
            priority: 5,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let handler = SpyHandler()
        await scheduler.setLanePreemptionHandler(handler)
        await scheduler.startSchedulerLoop()
        defer { Task { await scheduler.stop() } }

        // Wait until we can confirm the scheduler has picked up the job —
        // its state must transition out of "queued" (to a terminal or blocked
        // state). Only then can we assert the hook was not called.
        let processed = await pollUntil {
            let j = try? await store.fetchJob(byId: "soon-admit-job")
            switch j?.state {
            case "queued", "paused": return false
            default: return true
            }
        }
        await scheduler.stop()
        let calls = await handler.log.calls
        #expect(processed, "Scheduler did not process soon-admit-job within deadline")
        #expect(calls.isEmpty,
                "Preemption hook must not be invoked for Soon-lane admissions")
    }

    @Test("Preemption hook is NOT invoked when only a Background-lane job is admitted")
    func testPreemptionHookNotCalledForBackgroundLaneJob() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-bg"] = URL(fileURLWithPath: "/tmp/ep-bg.mp3")

        let job = makeAnalysisJob(
            jobId: "bg-admit-job",
            jobType: "preAnalysis",
            episodeId: "ep-bg",
            workKey: "fp-bg:1:preAnalysis",
            sourceFingerprint: "fp-bg",
            priority: 0,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let scheduler = makeScheduler(store: store, downloads: downloads)
        let handler = SpyHandler()
        await scheduler.setLanePreemptionHandler(handler)
        await scheduler.startSchedulerLoop()
        defer { Task { await scheduler.stop() } }

        let processed = await pollUntil {
            let j = try? await store.fetchJob(byId: "bg-admit-job")
            switch j?.state {
            case "queued", "paused": return false
            default: return true
            }
        }
        await scheduler.stop()
        let calls = await handler.log.calls
        #expect(processed, "Scheduler did not process bg-admit-job within deadline")
        #expect(calls.isEmpty,
                "Preemption hook must not be invoked for Background-lane admissions")
    }
}
