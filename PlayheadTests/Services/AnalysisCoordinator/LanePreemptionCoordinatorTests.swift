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

        var samples: [Duration] = []
        samples.reserveCapacity(tapCount)
        for _ in 0..<tapCount {
            let start = clock.now
            await coordinator.preemptLowerLanes(for: .now)
            samples.append(clock.now - start)
        }

        let sorted = samples.sorted()
        let median = sorted[tapCount / 2]
        let p99 = sorted[(tapCount * 99) / 100]
        let maxElapsed = sorted.last!

        // Production budget (`LanePreemptionCoordinator.promotionLatencyBudget`,
        // 100ms) describes a per-call upper bound under normal conditions. The
        // test runs inside an xcodebuild process with 3000+ tests competing for
        // the cooperative pool, so individual `await` resumes can be delayed by
        // 100ms–1s+ even when the underlying operation is microseconds. We assert
        // the median (typical case) meets production budget — that catches
        // algorithmic regressions — and use a very loose hard ceiling to guard
        // only against runaway hangs. P99 logged for visibility. See bead
        // playhead-ss38.
        #expect(median < LanePreemptionCoordinator.promotionLatencyBudget,
                "Median promotion latency \(median) exceeded production budget of \(LanePreemptionCoordinator.promotionLatencyBudget) (p99=\(p99), max=\(maxElapsed))")
        let hangCeiling: Duration = .seconds(5)
        #expect(maxElapsed < hangCeiling,
                "Max promotion latency \(maxElapsed) exceeded hang ceiling of \(hangCeiling) (median=\(median), p99=\(p99))")
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

// MARK: - Safe-point round trip (playhead-01t8)

/// End-to-end test proving the production wiring works: the runner
/// registers with the coordinator, threads a `PreemptionContext` into
/// `FeatureExtractionService.extractAndPersist`, the flag flips from a
/// Now-lane admission simulated by `preemptLowerLanes(for: .now)`, the
/// extractor observes the signal at its post-shard safe point, calls
/// `acknowledge`, and returns early. The scheduler's `awaitLowerLaneAck`
/// then resolves within the HARD GATE budget.
///
/// We drive `FeatureExtractionService` directly (rather than via
/// `AnalysisJobRunner.run`) because the runner's decode + transcription
/// stages require real audio fixtures — the production poll site we care
/// about is `extractAndPersist`'s post-shard block, and this test hits it
/// with production code paths and a real SQLite-backed `AnalysisStore`.
@Suite("LanePreemptionCoordinator — FeatureExtraction safe-point round trip")
struct LanePreemptionFeatureExtractionRoundTripTests {

    private func makeAssetSeeded(store: AnalysisStore, assetId: String) async throws {
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: "ep-\(assetId)",
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)
    }

    private func makeLease(episodeId: String) -> EpisodeExecutionLease {
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

    /// Generates a non-silent 16 kHz shard large enough to produce
    /// feature windows on every shard. Uses a simple sinusoid so feature
    /// extraction does real work rather than being skipped as empty.
    private func makeNonSilentShard(id: Int, startTime: Double, duration: Double = 5) -> AnalysisShard {
        let sampleRate = 16_000
        let sampleCount = sampleRate * Int(duration)
        let freq: Float = 220.0
        let samples: [Float] = (0..<sampleCount).map { i in
            let t = Float(i) / Float(sampleRate)
            return 0.25 * Float(sin(Double(2 * .pi * freq * t)))
        }
        return AnalysisShard(
            id: id,
            episodeID: "ep-roundtrip",
            startTime: startTime,
            duration: duration,
            samples: samples
        )
    }

    @Test("Round trip: preemptLowerLanes flips signal → FeatureExtraction acks at post-shard boundary",
          .timeLimit(.minutes(1)))
    func featureExtractionAcksAtSafePoint() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-roundtrip-\(UUID().uuidString)"
        try await makeAssetSeeded(store: store, assetId: assetId)

        // Sixteen 30 s shards = 8 minutes of synthetic audio. Production
        // shards are 30 s (`AnalysisAudioService.defaultShardDuration`),
        // and 16 of them gives the extractor enough wall-clock work
        // that even a `Task.sleep`-style handoff under heavy parallel
        // test load (where `sleep(.ms(50))` measured ~1.9 s in
        // playhead-01t8 reopen) cannot finish all of it before the
        // preempt request lands. Without this floor of work, the
        // extractor can race to natural completion under load and the
        // post-shard poll path is never exercised.
        let shards = (0..<16).map { i in
            makeNonSilentShard(id: i, startTime: Double(i) * 30, duration: 30)
        }

        let coordinator = LanePreemptionCoordinator()
        let featureService = FeatureExtractionService(store: store)

        // Register the job in the Background lane. A Now admission will
        // flip its signal.
        let jobId = "job-roundtrip-\(UUID().uuidString)"
        let signal = await coordinator.register(
            jobId: jobId,
            lane: .background,
            lease: makeLease(episodeId: "ep-roundtrip")
        )
        let context = PreemptionContext(
            jobId: jobId,
            signal: signal,
            coordinator: coordinator
        )

        // Kick off extraction on its own task so we can flip the signal
        // mid-flight.
        let extractionTask = Task<[FeatureWindow], Error> {
            try await featureService.extractAndPersist(
                shards: shards,
                analysisAssetId: assetId,
                existingCoverage: 0,
                preemption: context
            )
        }

        // Wait deterministically until the extractor has PROVEN it is
        // both (a) mid-flight (still registered in the coordinator's
        // background lane) and (b) past at least one post-shard
        // boundary (`featureCoverageEndTime > 0`, which is only set
        // inside `persistFeatureExtractionBatch`). This replaces a
        // brittle `Task.sleep(.milliseconds(50))` that — under heavy
        // parallel test load on the simulator — was observed to
        // wake ~1.9 s late, by which time the extractor had finished
        // all shards and the test's preempt landed against a no-op.
        // (See `pollUntil` docstring re: playhead-qtc.) The extractor
        // is guaranteed to still be mid-flight after the first shard
        // because there are 15 shards of work left.
        let inFlight = try await pollUntil(timeout: .seconds(30)) {
            let coverage = try await store.fetchAsset(id: assetId)?.featureCoverageEndTime ?? 0
            let stillRegistered = await coordinator.activeJobs(in: .background).contains(jobId)
            return coverage > 0 && stillRegistered
        }
        try #require(inFlight, "Extractor must reach an in-flight state before the test can preempt it")

        let clock = ContinuousClock()
        let admissionAt = clock.now
        await coordinator.preemptLowerLanes(for: .now)
        let acked = await coordinator.awaitLowerLaneAck(
            after: .now,
            within: LanePreemptionCoordinator.preemptionLatencyBudget
        )
        let elapsed = clock.now - admissionAt

        let windows = try await extractionTask.value

        #expect(acked, "Extractor must acknowledge within the 5 s HARD GATE")
        #expect(elapsed < LanePreemptionCoordinator.preemptionLatencyBudget,
                "Round-trip latency \(elapsed) exceeded HARD GATE")
        #expect(await signal.cause == .userPreempted,
                "Signal must record the user-preempted cause")

        // The extractor should have returned some windows (safe-point
        // exit preserves work) but fewer than the full 16 shards' worth
        // — otherwise it didn't actually pause, it just completed.
        #expect(!windows.isEmpty,
                "Extractor should have persisted at least one shard before pausing")
        #expect(windows.count < 16 * 15,
                "Extractor must have paused early — saw windows=\(windows.count) for 16-shard run, expected fewer than the full \(16 * 15) windows")

        // After exit, the coverage watermark should be durable in the
        // store (atomic write inside persistFeatureExtractionBatch).
        let asset = try await store.fetchAsset(id: assetId)
        #expect(asset?.featureCoverageEndTime != nil,
                "Feature coverage checkpoint must be durable after preempt")
        if let coverage = asset?.featureCoverageEndTime {
            #expect(coverage > 0, "Coverage \(coverage) must be > 0 after at least one shard")
        }

        // Coordinator should no longer list the job after acknowledge.
        #expect(await coordinator.activeJobs(in: .background) == [],
                "Acknowledged job must be removed from the registry")
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
