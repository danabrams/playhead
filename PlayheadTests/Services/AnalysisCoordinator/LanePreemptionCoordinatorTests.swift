// LanePreemptionCoordinatorTests.swift
// playhead-01t8: hard user preemption at checkpoint boundaries.
//
// Unit tests for the scheduler-lane preemption mechanism. The suite covers
// the acceptance criteria called out in the bead:
//
//   1. Preemption flag propagates from the coordinator to a running job's
//      `PreemptionSignal` within the 5 s HARD GATE, even when the job
//      simulates long shard processing between safe points.
//   2. Pauses land ONLY at enumerated safe points — a flag flipped mid-shard
//      does not interrupt the current unit; the job runs to the next
//      post-shard boundary and exits there.
//   3. Within a lane, FIFO registration order is preserved across a
//      preempt/resume cycle.
//   4. Promotion latency (user-tap → Now admission request landing on the
//      handler) is ≤ 100 ms for N=1000 synthetic taps.
//
// The tests drive `LanePreemptionCoordinator` directly and simulate a
// "running job" with a controllable shard-processing delay. The simulated
// job honors the preemption protocol:
//
//   - Calls `register(jobId:lane:lease:)` and polls `signal` at every
//     post-shard boundary.
//   - On seeing the flag, releases its lease with `event=.preempted,
//     cause=.userPreempted`, calls `acknowledge(jobId:)` on the
//     coordinator, and returns.
//
// The harness is deliberately agnostic of the real FeatureExtraction /
// TranscriptEngine services — the goal is to prove the MECHANISM is
// deterministic, not to integrate every subsystem. Plugging the coordinator
// into those services is the responsibility of whichever downstream bead
// chooses to use the hook, since the wiring surface is already stable:
// `setLanePreemptionHandler(_:)` on `AnalysisWorkScheduler` and the
// `LanePreemptionHandler` protocol.

import Foundation
import Testing
@testable import Playhead

@Suite("LanePreemptionCoordinator — Hard User Preemption")
struct LanePreemptionCoordinatorTests {

    // MARK: - Fixtures

    /// Build a synthetic `EpisodeExecutionLease` for a registration. The
    /// coordinator stores the lease for diagnostics only; the test does
    /// not exercise any lease-store code paths here.
    private func makeLease(episodeId: String = "ep-\(UUID().uuidString)") -> EpisodeExecutionLease {
        EpisodeExecutionLease(
            episodeId: episodeId,
            ownerWorkerId: "test-worker",
            generationID: UUID(),
            schedulerEpoch: 1,
            acquiredAt: Date().timeIntervalSince1970,
            expiresAt: Date().timeIntervalSince1970 + 30,
            currentCheckpoint: nil,
            preemptionRequested: false
        )
    }

    /// Simulates a running analysis job that polls the preemption signal
    /// at every post-shard boundary. `shardCount` is the total number of
    /// shards the job plans to process; `shardDuration` is the wall-clock
    /// delay spent between safe points. Returns the shard index at which
    /// the job paused, or `nil` if the job completed all shards without
    /// preemption.
    ///
    /// Marked `@Sendable` because it runs on a detached task in the
    /// latency tests.
    @Sendable
    private static func runSimulatedJob(
        jobId: String,
        lane: AnalysisWorkScheduler.SchedulerLane,
        lease: EpisodeExecutionLease,
        shardCount: Int,
        shardDuration: Duration,
        coordinator: LanePreemptionCoordinator
    ) async -> Int? {
        let signal = await coordinator.register(
            jobId: jobId,
            lane: lane,
            lease: lease
        )

        for shardIndex in 0..<shardCount {
            // Simulated in-shard work. This is the "unit" the bead spec
            // says must NOT be interrupted mid-flight; we only poll the
            // signal on the post-shard boundary below.
            try? await Task.sleep(for: shardDuration)

            // Post-shard safe point (a) from the bead spec.
            if await signal.isPreemptionRequested() {
                // In production the owner would: checkpoint, release the
                // lease via `AnalysisCoordinator.releaseLease(event:
                // .preempted, cause: .userPreempted)`, and exit. Here we
                // just acknowledge on the coordinator so
                // `awaitLowerLaneAck` resolves.
                await coordinator.acknowledge(jobId: jobId)
                return shardIndex
            }
        }
        await coordinator.unregister(jobId: jobId)
        return nil
    }

    // MARK: - 1. Flag propagation

    @Test("preemptLowerLanes(.now) flips the signal on Soon-lane registrations")
    func preemptionFlagFlipsForSoonLaneJob() async {
        let coordinator = LanePreemptionCoordinator()
        let signal = await coordinator.register(
            jobId: "soon-1",
            lane: .soon,
            lease: makeLease()
        )
        #expect(await signal.isPreemptionRequested() == false)

        await coordinator.preemptLowerLanes(for: .now)

        #expect(await signal.isPreemptionRequested() == true)
    }

    @Test("preemptLowerLanes(.now) flips the signal on Background-lane registrations")
    func preemptionFlagFlipsForBackgroundLaneJob() async {
        let coordinator = LanePreemptionCoordinator()
        let signal = await coordinator.register(
            jobId: "bg-1",
            lane: .background,
            lease: makeLease()
        )
        await coordinator.preemptLowerLanes(for: .now)
        #expect(await signal.isPreemptionRequested() == true)
    }

    @Test("preemptLowerLanes(.now) does NOT flip signals on a Now-lane registration")
    func preemptionFlagDoesNotFlipForNowLaneJob() async {
        let coordinator = LanePreemptionCoordinator()
        let nowSignal = await coordinator.register(
            jobId: "now-1",
            lane: .now,
            lease: makeLease()
        )
        await coordinator.preemptLowerLanes(for: .now)
        #expect(await nowSignal.isPreemptionRequested() == false,
                "Now-lane admission must not preempt other Now-lane jobs")
    }

    @Test("preemptLowerLanes(.soon) flips Background only (FIX: Soon can demote Background)")
    func soonLaneAdmissionPreemptsBackgroundOnly() async {
        let coordinator = LanePreemptionCoordinator()
        let soonSignal = await coordinator.register(
            jobId: "soon-1",
            lane: .soon,
            lease: makeLease()
        )
        let bgSignal = await coordinator.register(
            jobId: "bg-1",
            lane: .background,
            lease: makeLease()
        )

        // The production scheduler does not invoke the handler for Soon
        // admissions today — see `AnalysisWorkScheduler.runLoop` — but the
        // coordinator's lane math must still be correct so downstream
        // beads can widen the guard without re-implementing ordering.
        await coordinator.preemptLowerLanes(for: .soon)

        #expect(await soonSignal.isPreemptionRequested() == false)
        #expect(await bgSignal.isPreemptionRequested() == true)
    }

    @Test("Signal cause is .userPreempted after preemption")
    func signalCauseRecordsUserPreempted() async {
        let coordinator = LanePreemptionCoordinator()
        let signal = await coordinator.register(
            jobId: "soon-1",
            lane: .soon,
            lease: makeLease()
        )
        await coordinator.preemptLowerLanes(for: .now)
        let cause = await signal.cause
        #expect(cause == .userPreempted)
    }

    // MARK: - 2. Safe-point pause

    @Test("Pause occurs only at post-shard boundaries, not mid-shard")
    func pauseOccursOnlyAtSafePoints() async {
        let coordinator = LanePreemptionCoordinator()
        let jobRun = Task {
            await Self.runSimulatedJob(
                jobId: "soon-1",
                lane: .soon,
                lease: makeLease(),
                shardCount: 10,
                shardDuration: .milliseconds(100),
                coordinator: coordinator
            )
        }

        // Wait for the first shard to start so the mid-shard preempt
        // actually lands mid-shard. 30 ms < 100 ms shard duration.
        try? await Task.sleep(for: .milliseconds(30))

        await coordinator.preemptLowerLanes(for: .now)

        let pausedAtShard = await jobRun.value
        #expect(pausedAtShard != nil, "Job should have paused at a safe point")
        // Mid-shard preemption on shard 0 (~30 ms into a 100 ms shard)
        // must run to the end of shard 0 before pausing → pauses at
        // shard 0. A later shard index would indicate the preemption
        // was missed or delayed past the next safe point.
        if let pausedAtShard {
            #expect(pausedAtShard == 0,
                    "Expected pause at first post-shard boundary (shard 0), got \(pausedAtShard)")
        }
    }

    // MARK: - 3. HARD GATE: preemption latency ≤ 5 s

    @Test("HARD GATE: preemption latency ≤ 5 s (fast shards)", .timeLimit(.minutes(1)))
    func preemptionLatencyHardGate_fastShards() async {
        let coordinator = LanePreemptionCoordinator()
        let jobCount = 3
        let clock = ContinuousClock()

        // Three concurrent jobs spread across both lower lanes with
        // shard durations well under the budget. All must pause within
        // 5 s of the admission request.
        var runs: [Task<Int?, Never>] = []
        for i in 0..<jobCount {
            let lane: AnalysisWorkScheduler.SchedulerLane = (i % 2 == 0) ? .soon : .background
            let task = Task {
                await Self.runSimulatedJob(
                    jobId: "job-\(i)",
                    lane: lane,
                    lease: makeLease(),
                    shardCount: 100,
                    shardDuration: .milliseconds(50),
                    coordinator: coordinator
                )
            }
            runs.append(task)
        }

        // Wait until all jobs have registered and started processing.
        let registered = await pollUntil {
            await coordinator.registeredCount() == jobCount
        }
        #expect(registered, "All simulated jobs must register before admission")

        let admissionAt = clock.now
        await coordinator.preemptLowerLanes(for: .now)
        let acked = await coordinator.awaitLowerLaneAck(
            after: .now,
            within: LanePreemptionCoordinator.preemptionLatencyBudget
        )
        let elapsed = clock.now - admissionAt

        for task in runs {
            _ = await task.value
        }

        #expect(acked, "Every lower-lane job must acknowledge within the budget")
        #expect(elapsed < LanePreemptionCoordinator.preemptionLatencyBudget,
                "Preemption latency \(elapsed) exceeded HARD GATE of \(LanePreemptionCoordinator.preemptionLatencyBudget)")
    }

    @Test("HARD GATE: preemption latency still ≤ 5 s with slow shards (corpus-size independence)",
          .timeLimit(.minutes(1)))
    func preemptionLatencyHardGate_slowShards() async {
        // The bead spec: "independent of corpus size." We simulate a
        // corpus-heavy job by giving each shard a 1 s duration — the
        // pause must still land at the next safe point, i.e. within
        // one shard's worth of wall-clock time.
        let coordinator = LanePreemptionCoordinator()
        let clock = ContinuousClock()

        let runTask = Task {
            await Self.runSimulatedJob(
                jobId: "slow-shard-job",
                lane: .background,
                lease: makeLease(),
                shardCount: 30,
                shardDuration: .seconds(1),
                coordinator: coordinator
            )
        }

        // Wait for registration before requesting preemption.
        let registered = await pollUntil {
            await coordinator.registeredCount() == 1
        }
        #expect(registered)

        // Land the preemption request at an arbitrary point partway
        // into a shard so we exercise the mid-shard-to-next-safe-point
        // path.
        try? await Task.sleep(for: .milliseconds(200))

        let admissionAt = clock.now
        await coordinator.preemptLowerLanes(for: .now)
        let acked = await coordinator.awaitLowerLaneAck(
            after: .now,
            within: LanePreemptionCoordinator.preemptionLatencyBudget
        )
        let elapsed = clock.now - admissionAt

        _ = await runTask.value

        #expect(acked, "Slow-shard job must still ack within the 5 s budget")
        #expect(elapsed < LanePreemptionCoordinator.preemptionLatencyBudget,
                "Slow-shard preemption latency \(elapsed) exceeded HARD GATE of \(LanePreemptionCoordinator.preemptionLatencyBudget)")
        // The bead spec says pauses MUST land at unit boundaries.
        // A 1 s shard that started just before the preempt request
        // should ack shortly after the 1 s shard completes — i.e.
        // well under 2 s. The HARD GATE is 5 s; this belt-and-braces
        // assertion makes "we're at a safe point" explicit.
        #expect(elapsed < .seconds(2),
                "Expected pause within 2 s of request (one 1 s shard), got \(elapsed)")
    }

    // MARK: - 4. Lane-FIFO invariant

    @Test("FIFO order within a lane is preserved across preempt/resume")
    func laneFIFOPreservedAcrossPreempt() async {
        let coordinator = LanePreemptionCoordinator()

        // Register three jobs in the same lane in a known order. The
        // `registeredAt` timestamps are injected explicitly so the test
        // is deterministic across scheduler jitter.
        let t0 = ContinuousClock.now
        _ = await coordinator.register(
            jobId: "soon-a",
            lane: .soon,
            lease: makeLease(),
            registeredAt: t0
        )
        _ = await coordinator.register(
            jobId: "soon-b",
            lane: .soon,
            lease: makeLease(),
            registeredAt: t0.advanced(by: .milliseconds(1))
        )
        _ = await coordinator.register(
            jobId: "soon-c",
            lane: .soon,
            lease: makeLease(),
            registeredAt: t0.advanced(by: .milliseconds(2))
        )

        #expect(await coordinator.activeJobs(in: .soon) == ["soon-a", "soon-b", "soon-c"])

        // Preempt all of them.
        await coordinator.preemptLowerLanes(for: .now)
        await coordinator.acknowledge(jobId: "soon-a")
        await coordinator.acknowledge(jobId: "soon-b")
        await coordinator.acknowledge(jobId: "soon-c")
        #expect(await coordinator.activeJobs(in: .soon) == [])

        // Re-register in the SAME order (simulating resume).
        let t1 = ContinuousClock.now
        _ = await coordinator.register(
            jobId: "soon-a",
            lane: .soon,
            lease: makeLease(),
            registeredAt: t1
        )
        _ = await coordinator.register(
            jobId: "soon-b",
            lane: .soon,
            lease: makeLease(),
            registeredAt: t1.advanced(by: .milliseconds(1))
        )
        _ = await coordinator.register(
            jobId: "soon-c",
            lane: .soon,
            lease: makeLease(),
            registeredAt: t1.advanced(by: .milliseconds(2))
        )

        #expect(await coordinator.activeJobs(in: .soon) == ["soon-a", "soon-b", "soon-c"],
                "Lane FIFO must be identical after a preempt/resume cycle")
    }

    // MARK: - 5. Promotion latency ≤ 100 ms

    @Test("Promotion latency: preemptLowerLanes(.now) returns within 100 ms for 1000 synthetic taps",
          .timeLimit(.minutes(1)))
    func promotionLatencyUnder100ms() async {
        let coordinator = LanePreemptionCoordinator()
        let clock = ContinuousClock()
        let tapCount = 1000

        // A single registered lower-lane job so the preemption path
        // does real work on every tap (flag flip + signal notify).
        _ = await coordinator.register(
            jobId: "victim",
            lane: .background,
            lease: makeLease()
        )

        var maxElapsed: Duration = .zero
        var totalElapsed: Duration = .zero
        for _ in 0..<tapCount {
            let start = clock.now
            await coordinator.preemptLowerLanes(for: .now)
            let elapsed = clock.now - start
            totalElapsed += elapsed
            if elapsed > maxElapsed { maxElapsed = elapsed }
        }

        let avg = totalElapsed / tapCount
        #expect(maxElapsed < LanePreemptionCoordinator.promotionLatencyBudget,
                "Max promotion latency \(maxElapsed) exceeded budget of \(LanePreemptionCoordinator.promotionLatencyBudget) (avg=\(avg))")
    }

    // MARK: - 6. Miscellaneous edge cases

    @Test("awaitLowerLaneAck returns true immediately when no lower lanes are registered")
    func awaitLowerLaneAckEmptyIsInstant() async {
        let coordinator = LanePreemptionCoordinator()
        let acked = await coordinator.awaitLowerLaneAck(
            after: .now,
            within: .milliseconds(1)
        )
        #expect(acked == true)
    }

    @Test("awaitLowerLaneAck returns false when a job never acks within timeout")
    func awaitLowerLaneAckTimesOut() async {
        let coordinator = LanePreemptionCoordinator()
        _ = await coordinator.register(
            jobId: "stuck-job",
            lane: .background,
            lease: makeLease()
        )
        await coordinator.preemptLowerLanes(for: .now)
        let acked = await coordinator.awaitLowerLaneAck(
            after: .now,
            within: .milliseconds(50)
        )
        #expect(acked == false, "Job that never acks must be reported as missing ack")
    }

    @Test("acknowledge on an unregistered jobId is a no-op")
    func acknowledgeUnknownJobIsNoOp() async {
        let coordinator = LanePreemptionCoordinator()
        await coordinator.acknowledge(jobId: "never-registered")
        #expect(await coordinator.registeredCount() == 0)
    }

    @Test("Repeated preemption does not reset the requestedAt timestamp")
    func preemptionTimestampIsIdempotent() async {
        let coordinator = LanePreemptionCoordinator()
        let signal = await coordinator.register(
            jobId: "soon-1",
            lane: .soon,
            lease: makeLease()
        )
        await coordinator.preemptLowerLanes(for: .now)
        let first = await signal.requestedAt
        try? await Task.sleep(for: .milliseconds(20))
        await coordinator.preemptLowerLanes(for: .now)
        let second = await signal.requestedAt
        #expect(first == second,
                "Preemption timestamp must record the first request, not the most recent")
    }

    // MARK: - 7. Sanity: handler install is inert

    @Test("Installing the coordinator on the scheduler does not invoke its methods at construction")
    func schedulerInstallIsInert() async throws {
        let store = try await makeTestStore()
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
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: StubDownloadProvider(),
            batteryProvider: {
                let b = StubBatteryProvider()
                b.level = 0.9
                b.charging = true
                return b
            }()
        )
        let coordinator = LanePreemptionCoordinator()
        await scheduler.setLanePreemptionHandler(coordinator)

        #expect(await coordinator.preemptionRequestCount == 0,
                "Installing the coordinator must not fire a preemption request")
        #expect(await coordinator.registeredCount() == 0,
                "Installing the coordinator must not register any jobs")
    }
}

// MARK: - Lane ordering

@Suite("LanePreemptionCoordinator — Lane Ordering")
struct LanePreemptionSchedulerLaneOrderingTests {

    @Test(".background is strictly lower than .soon")
    func backgroundLowerThanSoon() {
        #expect(AnalysisWorkScheduler.SchedulerLane.background.isStrictlyLower(than: .soon))
    }

    @Test(".background is strictly lower than .now")
    func backgroundLowerThanNow() {
        #expect(AnalysisWorkScheduler.SchedulerLane.background.isStrictlyLower(than: .now))
    }

    @Test(".soon is strictly lower than .now")
    func soonLowerThanNow() {
        #expect(AnalysisWorkScheduler.SchedulerLane.soon.isStrictlyLower(than: .now))
    }

    @Test(".now is NOT strictly lower than itself")
    func nowNotLowerThanNow() {
        #expect(AnalysisWorkScheduler.SchedulerLane.now.isStrictlyLower(than: .now) == false)
    }

    @Test(".now is NOT strictly lower than .soon")
    func nowNotLowerThanSoon() {
        #expect(AnalysisWorkScheduler.SchedulerLane.now.isStrictlyLower(than: .soon) == false)
    }
}
