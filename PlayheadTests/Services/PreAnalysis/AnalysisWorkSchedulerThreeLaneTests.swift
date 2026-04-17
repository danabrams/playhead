// AnalysisWorkSchedulerThreeLaneTests.swift
// Tests for the three-lane scheduler model (Now / Soon / Background)
// introduced in playhead-r835. These tests exercise the lane derivation from
// AnalysisJob.priority, LaneAdmission gating by SchedulerLane, per-lane
// concurrency caps (with the T0 playback exemption for the Now lane), and
// the preemption-hook protocol surface that later beads will implement.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — Three-Lane Scheduler")
struct AnalysisWorkSchedulerThreeLaneTests {

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

    // MARK: - Per-lane concurrency caps

    @Test("Now-lane cap: admits up to 2 concurrent non-playback jobs, then rejects")
    func testNowLaneCapRejectsThirdConcurrent() async {
        let counter = AnalysisWorkScheduler.LaneConcurrencyCounter()

        let job1 = makeAnalysisJob(jobId: "now-1", jobType: "preAnalysis", priority: 20)
        let job2 = makeAnalysisJob(jobId: "now-2", jobType: "preAnalysis", priority: 50)
        let job3 = makeAnalysisJob(jobId: "now-3", jobType: "preAnalysis", priority: 100)

        #expect(counter.canAdmit(job: job1))
        counter.didStart(job: job1)

        #expect(counter.canAdmit(job: job2))
        counter.didStart(job: job2)

        // Third non-playback Now job is rejected (cap = 2).
        #expect(!counter.canAdmit(job: job3))
    }

    @Test("Now-lane cap: T0 playback is exempt even when 2 non-playback Now jobs are running")
    func testT0PlaybackExemptFromNowCap() {
        let counter = AnalysisWorkScheduler.LaneConcurrencyCounter()

        let preAnalysisA = makeAnalysisJob(jobId: "pa-1", jobType: "preAnalysis", priority: 20)
        let preAnalysisB = makeAnalysisJob(jobId: "pa-2", jobType: "preAnalysis", priority: 25)
        let playback = makeAnalysisJob(jobId: "pb-1", jobType: "playback", priority: 20)

        counter.didStart(job: preAnalysisA)
        counter.didStart(job: preAnalysisB)

        // Cap reached for non-playback jobs, but playback is exempt.
        let nextPreAnalysis = makeAnalysisJob(jobId: "pa-3", jobType: "preAnalysis", priority: 30)
        #expect(!counter.canAdmit(job: nextPreAnalysis))
        #expect(counter.canAdmit(job: playback))
    }

    @Test("Now-lane cap: T0 playback admitted even at zero-cap (defensive)")
    func testT0PlaybackExemptEvenAtSaturation() {
        let counter = AnalysisWorkScheduler.LaneConcurrencyCounter()
        // Fill with many playback jobs — they are exempt so cap never binds them.
        for i in 0..<5 {
            let playback = makeAnalysisJob(
                jobId: "pb-\(i)",
                jobType: "playback",
                priority: 20
            )
            counter.didStart(job: playback)
        }
        let anotherPlayback = makeAnalysisJob(jobId: "pb-extra", jobType: "playback", priority: 20)
        #expect(counter.canAdmit(job: anotherPlayback))
    }

    @Test("Soon-lane cap: admits 1 concurrent, rejects 2nd")
    func testSoonLaneCapRejectsSecondConcurrent() {
        let counter = AnalysisWorkScheduler.LaneConcurrencyCounter()

        let job1 = makeAnalysisJob(jobId: "soon-1", jobType: "preAnalysis", priority: 10)
        let job2 = makeAnalysisJob(jobId: "soon-2", jobType: "preAnalysis", priority: 5)

        #expect(counter.canAdmit(job: job1))
        counter.didStart(job: job1)
        #expect(!counter.canAdmit(job: job2))
    }

    @Test("Background-lane cap: admits 1 concurrent, rejects 2nd")
    func testBackgroundLaneCapRejectsSecondConcurrent() {
        let counter = AnalysisWorkScheduler.LaneConcurrencyCounter()

        let job1 = makeAnalysisJob(jobId: "bg-1", jobType: "preAnalysis", priority: 0)
        let job2 = makeAnalysisJob(jobId: "bg-2", jobType: "preAnalysis", priority: -5)

        #expect(counter.canAdmit(job: job1))
        counter.didStart(job: job1)
        #expect(!counter.canAdmit(job: job2))
    }

    @Test("didFinish decrements the per-lane count and frees capacity")
    func testDidFinishReleasesCapacity() {
        let counter = AnalysisWorkScheduler.LaneConcurrencyCounter()

        let first = makeAnalysisJob(jobId: "soon-1", jobType: "preAnalysis", priority: 5)
        counter.didStart(job: first)

        let second = makeAnalysisJob(jobId: "soon-2", jobType: "preAnalysis", priority: 5)
        #expect(!counter.canAdmit(job: second))

        counter.didFinish(job: first)
        #expect(counter.canAdmit(job: second))
    }

    @Test("Lanes are independent: filling Soon does not block Now or Background")
    func testLanesAreIndependent() {
        let counter = AnalysisWorkScheduler.LaneConcurrencyCounter()

        let soon = makeAnalysisJob(jobId: "soon-1", jobType: "preAnalysis", priority: 5)
        counter.didStart(job: soon)

        let now = makeAnalysisJob(jobId: "now-1", jobType: "preAnalysis", priority: 25)
        let background = makeAnalysisJob(jobId: "bg-1", jobType: "preAnalysis", priority: 0)

        #expect(counter.canAdmit(job: now))
        #expect(counter.canAdmit(job: background))
    }

    // MARK: - Preemption hook protocol surface

    @Test("Scheduler accepts a LanePreemptionHandler and does not invoke its methods at construction")
    func testPreemptionHandlerProtocolExists() async throws {
        // This test verifies that the protocol surface exists and can be
        // installed on the scheduler without triggering any preemption calls
        // during setup. Actual preempt-on-admission is out of scope for this
        // bead (owned by playhead-01t8).
        final class SpyHandler: LanePreemptionHandler, @unchecked Sendable {
            var calls: [AnalysisWorkScheduler.SchedulerLane] = []
            func preemptLowerLanes(for incoming: AnalysisWorkScheduler.SchedulerLane) async {
                calls.append(incoming)
            }
        }

        let store = try await makeTestStore()
        let capabilities = StubCapabilitiesProvider()
        let battery = StubBatteryProvider()
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
            batteryProvider: battery
        )
        let handler = SpyHandler()
        await scheduler.setLanePreemptionHandler(handler)

        // No calls should have been made simply by installing the handler.
        #expect(handler.calls.isEmpty)
    }
}
