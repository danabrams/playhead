// AdmissionControllerTests.swift
// Tests for the minimal phase-3 backfill admission controller.

import Foundation
import Testing

@testable import Playhead

@Suite("AdmissionController")
struct AdmissionControllerTests {

    @Test("admits highest priority job and enforces serial execution")
    func testPriorityAndSerialExecution() async {
        let controller = AdmissionController()
        let snapshot = makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)

        await controller.enqueue(makeBackfillJob(
            jobId: "low-priority",
            priority: 0,
            createdAt: 20
        ))
        await controller.enqueue(makeBackfillJob(
            jobId: "high-priority",
            priority: 10,
            createdAt: 10
        ))

        let first = await controller.admitNextEligibleJob(
            snapshot: snapshot,
            batteryLevel: 0.90
        )
        let blockedWhileBusy = await controller.admitNextEligibleJob(
            snapshot: snapshot,
            batteryLevel: 0.90
        )
        await controller.finish(jobId: "high-priority")
        let second = await controller.admitNextEligibleJob(
            snapshot: snapshot,
            batteryLevel: 0.90
        )

        #expect(first.job?.jobId == "high-priority")
        #expect(first.deferReason == nil)
        #expect(first.qualityProfile == .nominal,
                "admit decisions carry the derived QualityProfile")
        #expect(blockedWhileBusy.job == nil)
        #expect(blockedWhileBusy.deferReason == .serialBusy)
        #expect(blockedWhileBusy.qualityProfile == nil,
                "serialBusy short-circuits before device-state evaluation")
        #expect(second.job?.jobId == "low-priority")
    }

    // C1 alignment: the admission gate is now `QualityProfile.schedulerPolicy.pauseAllWork`,
    // which is true today only at `.critical` thermal. Plain low-battery and
    // serious thermal demote the profile to `.serious` (which still admits at
    // the controller level — the scheduler is the layer that gates per-lane).
    @Test("defers only when QualityProfile pauses all work; admits otherwise")
    func testThermalAndBatteryGating() async {
        let controller = AdmissionController()
        await controller.enqueue(makeBackfillJob(jobId: "critical-thermal-job", priority: 9))
        await controller.enqueue(makeBackfillJob(jobId: "serious-thermal-job", priority: 8))
        await controller.enqueue(makeBackfillJob(jobId: "low-battery-job", priority: 7))
        await controller.enqueue(makeBackfillJob(jobId: "charging-low-battery-job", priority: 6))

        // Critical thermal: profile=.critical, pauseAllWork=true -> defer.
        let thermalBlocked = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(thermalState: .critical, isCharging: true),
            batteryLevel: 0.95
        )
        #expect(thermalBlocked.job == nil)
        #expect(thermalBlocked.deferReason == .thermalThrottled)
        #expect(thermalBlocked.qualityProfile == .critical,
                "critical thermal derives .critical and attaches it to the decision")

        // Serious thermal (unplugged): profile=.serious, pauseAllWork=false -> admit.
        // (The scheduler still pauses Soon+Background lanes at .serious; that's
        // a per-lane decision, not an admission decision.)
        let seriousAdmitted = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(thermalState: .serious, isCharging: false),
            batteryLevel: 0.95
        )
        #expect(seriousAdmitted.job?.jobId == "critical-thermal-job",
                "serious thermal admits at the controller level")
        #expect(seriousAdmitted.qualityProfile == .serious)
        await controller.finish(jobId: "critical-thermal-job")

        // Plain low battery (nominal thermal, unplugged): profile demotes
        // nominal->fair, pauseAllWork=false -> admit. This is the C1
        // narrowing: DAP used to defer here, QP admits because per-lane
        // throttling already handles it downstream.
        let lowBatteryAdmitted = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: false),
            batteryLevel: 0.19
        )
        #expect(lowBatteryAdmitted.job?.jobId == "serious-thermal-job",
                "plain low battery does not block admission under QualityProfile")
        #expect(lowBatteryAdmitted.qualityProfile == .fair)
        await controller.finish(jobId: "serious-thermal-job")

        // Charging at low battery: profile stays .nominal -> admit.
        let chargingAdmitted = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true),
            batteryLevel: 0.05
        )
        #expect(chargingAdmitted.job?.jobId == "low-battery-job")
        #expect(chargingAdmitted.qualityProfile == .nominal)
    }

    // C1 alignment + thermal precedence: when multiple constraints fire at
    // once (.critical thermal + LPM + unplugged + low battery), the
    // deferReason helper returns `.thermalThrottled` because thermal is
    // checked first, and the attached qualityProfile is `.critical`.
    @Test("critical thermal carries thermalThrottled defer reason and .critical profile")
    func testCriticalThermalDeferAttribution() async {
        let controller = AdmissionController()
        await controller.enqueue(makeBackfillJob(jobId: "critical", priority: 5))

        let blocked = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(
                thermalState: .critical,
                isLowPowerMode: true,
                isCharging: false
            ),
            batteryLevel: 0.05
        )

        #expect(blocked.job == nil)
        #expect(blocked.deferReason == .thermalThrottled,
                "thermal precedence wins when multiple constraints would also fire")
        #expect(blocked.qualityProfile == .critical)
    }

    // M14: runningJob exposes the whole job, not just the id.
    @Test("runningJob exposes priority and phase after admit")
    func testRunningJobExposesFullJob() async {
        let controller = AdmissionController()
        let snapshot = makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
        await controller.enqueue(makeBackfillJob(
            jobId: "inspectable",
            phase: .scanLikelyAdSlots,
            priority: 7
        ))

        _ = await controller.admitNextEligibleJob(snapshot: snapshot, batteryLevel: 0.9)

        let running = await controller.runningJob
        #expect(running?.jobId == "inspectable")
        #expect(running?.priority == 7)
        #expect(running?.phase == .scanLikelyAdSlots)
    }

    // #14: empty queue check ordering — empty under heat returns .idle, not throttle.
    @Test("empty queue returns idle even under thermal throttle")
    func testEmptyQueueReturnsIdleUnderHeat() async {
        let controller = AdmissionController()
        let throttled = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(thermalState: .serious, isCharging: true),
            batteryLevel: 0.95
        )
        #expect(throttled.job == nil)
        #expect(throttled.deferReason == nil)
        // The empty-queue branch short-circuits before device-state evaluation,
        // so qualityProfile is nil — callers must not assume a non-nil profile
        // on a decision where job and deferReason are both nil.
        #expect(throttled.qualityProfile == nil,
                "empty-queue .idle short-circuits before QualityProfile derivation")
    }

    // #13: createdAt tiebreaker — earlier wins at equal priority.
    @Test("at equal priority, earlier createdAt wins")
    func testCreatedAtTiebreaker() async {
        let controller = AdmissionController()
        let snapshot = makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)

        await controller.enqueue(makeBackfillJob(jobId: "later", priority: 5, createdAt: 200))
        await controller.enqueue(makeBackfillJob(jobId: "earlier", priority: 5, createdAt: 100))

        let admitted = await controller.admitNextEligibleJob(snapshot: snapshot, batteryLevel: 0.9)
        #expect(admitted.job?.jobId == "earlier")
    }

    // #13: jobId tiebreaker — lower jobId wins at equal priority and createdAt.
    @Test("at equal priority and createdAt, lower jobId wins")
    func testJobIdTiebreaker() async {
        let controller = AdmissionController()
        let snapshot = makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)

        await controller.enqueue(makeBackfillJob(jobId: "bbbb", priority: 5, createdAt: 100))
        await controller.enqueue(makeBackfillJob(jobId: "aaaa", priority: 5, createdAt: 100))

        let admitted = await controller.admitNextEligibleJob(snapshot: snapshot, batteryLevel: 0.9)
        #expect(admitted.job?.jobId == "aaaa")
    }

    // M13: enqueue should not be O(n log n) per insert. Smoke test the wall time.
    @Test("enqueue of 1000 jobs completes quickly")
    func testEnqueueScales() async {
        let controller = AdmissionController()
        let start = Date()
        for index in 0..<1000 {
            await controller.enqueue(makeBackfillJob(
                jobId: "job-\(index)",
                priority: Int.random(in: 0...100),
                createdAt: Double(index)
            ))
        }
        let elapsed = Date().timeIntervalSince(start)
        #expect(elapsed < 0.1, "1000 enqueues should be < 100ms (was \(elapsed)s)")
    }

    // H7: failure → requeue, retryCount increments, controller does not deadlock.
    @Test("failed(jobId:reason:) requeues under retry budget and clears running slot")
    func testFailedRequeuesUnderBudget() async {
        let controller = AdmissionController()
        let snapshot = makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
        await controller.enqueue(makeBackfillJob(jobId: "flake", priority: 5))

        _ = await controller.admitNextEligibleJob(snapshot: snapshot, batteryLevel: 0.9)
        let requeued = await controller.failed(jobId: "flake", reason: "fmTimeout")

        #expect(requeued != nil)
        #expect(requeued?.retryCount == 1)
        let running = await controller.runningJob
        #expect(running == nil)

        // Controller is alive: re-admit succeeds.
        let readmit = await controller.admitNextEligibleJob(snapshot: snapshot, batteryLevel: 0.9)
        #expect(readmit.job?.jobId == "flake")
    }

    @Test("failed(jobId:reason:) returns nil when retry budget is exhausted")
    func testFailedExhaustsRetryBudget() async {
        let controller = AdmissionController()
        let snapshot = makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
        await controller.enqueue(makeBackfillJob(
            jobId: "doomed",
            priority: 5,
            retryCount: AdmissionController.maxRetries
        ))

        _ = await controller.admitNextEligibleJob(snapshot: snapshot, batteryLevel: 0.9)
        let result = await controller.failed(jobId: "doomed", reason: "fmCrash")

        #expect(result == nil)
        let running = await controller.runningJob
        #expect(running == nil)
    }

    // H7 continued: withAdmittedJob clean-up on throw.
    @Test("withAdmittedJob clears the running slot even when the closure throws")
    func testWithAdmittedJobCleansUpOnThrow() async {
        let controller = AdmissionController()
        let snapshot = makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
        await controller.enqueue(makeBackfillJob(jobId: "explodes", priority: 5))

        struct Boom: Error {}
        do {
            _ = try await controller.withAdmittedJob(
                snapshot: snapshot,
                batteryLevel: 0.9
            ) { _ in
                throw Boom()
            }
            Issue.record("Expected throw")
        } catch is Boom {
            // expected
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        let running = await controller.runningJob
        #expect(running == nil)

        // The job was requeued (retry budget) and admit-next succeeds again.
        let readmit = await controller.admitNextEligibleJob(snapshot: snapshot, batteryLevel: 0.9)
        #expect(readmit.job?.jobId == "explodes")
    }

    @Test("C-R3-3: retry budget is consistent — 3 failed attempts total, no off-by-one")
    func retryBudgetIsConsistentBetweenRunnerAndController() async {
        // C-R3-3: the runner's skip check was
        //   `existing.retryCount >= maxRetries (3)` — 3 total attempts
        // while the admission controller was
        //   `nextRetryCount <= maxRetries (3)` — 4 total attempts.
        // These are off-by-one. Standardize on 3 total attempts: after
        // three consecutive failures the persisted retryCount is 3 and the
        // job is exhausted. The controller must NOT requeue on the third
        // failure, and a fresh runner pass must NOT re-admit.
        let controller = AdmissionController()
        let snapshot = makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
        await controller.enqueue(makeBackfillJob(jobId: "three-strikes", priority: 5))

        // Attempt 1: admit, fail. retryCount becomes 1, requeued.
        _ = await controller.admitNextEligibleJob(snapshot: snapshot, batteryLevel: 0.9)
        let afterFirst = await controller.failed(jobId: "three-strikes", reason: "fail1")
        #expect(afterFirst?.retryCount == 1, "first failure bumps to 1")

        // Attempt 2: admit, fail. retryCount becomes 2, requeued.
        _ = await controller.admitNextEligibleJob(snapshot: snapshot, batteryLevel: 0.9)
        let afterSecond = await controller.failed(jobId: "three-strikes", reason: "fail2")
        #expect(afterSecond?.retryCount == 2, "second failure bumps to 2")

        // Attempt 3: admit, fail. retryCount WOULD become 3 — this is the
        // budget boundary. The controller must NOT requeue, matching the
        // runner's `retryCount >= maxRetries` skip gate.
        _ = await controller.admitNextEligibleJob(snapshot: snapshot, batteryLevel: 0.9)
        let afterThird = await controller.failed(jobId: "three-strikes", reason: "fail3")
        #expect(afterThird == nil,
                "third failure must exhaust the retry budget, not requeue a 4th attempt")
        let running = await controller.runningJob
        #expect(running == nil)
    }

    // C1 alignment: Low Power Mode alone no longer blocks admission. LPM
    // demotes nominal->fair (or fair->serious), but neither profile sets
    // `pauseAllWork`. Per-lane throttling in `AnalysisWorkScheduler` is the
    // appropriate place to act on the demotion (e.g. pause Background lane).
    @Test("Low Power Mode alone does not block admission; profile demotes one step")
    func testLowPowerModeDoesNotBlockAdmission() async {
        let controller = AdmissionController()
        await controller.enqueue(makeBackfillJob(jobId: "lpm-job", priority: 5))

        let admitted = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(
                thermalState: .nominal,
                isLowPowerMode: true,
                isCharging: true
            ),
            batteryLevel: 0.95
        )

        #expect(admitted.job?.jobId == "lpm-job",
                "LPM no longer blocks admission — it demotes the profile only")
        #expect(admitted.deferReason == nil)
        #expect(admitted.qualityProfile == .fair,
                "nominal thermal + LPM demotes one step to .fair")
    }

    // C1 contradiction case 2 from the alignment plan: thermal fair + LPM on.
    // DAP used to defer entirely; QP demotes to .serious which still admits
    // (per-lane gates handle the throttle).
    @Test("fair thermal + Low Power Mode admits with .serious profile")
    func testFairThermalPlusLPMAdmitsWithSeriousProfile() async {
        let controller = AdmissionController()
        await controller.enqueue(makeBackfillJob(jobId: "fair-lpm-job", priority: 5))

        let admitted = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(
                thermalState: .fair,
                isLowPowerMode: true,
                isCharging: true
            ),
            batteryLevel: 0.95
        )

        #expect(admitted.job?.jobId == "fair-lpm-job")
        #expect(admitted.deferReason == nil,
                "admitted decisions must not carry a defer reason")
        #expect(admitted.qualityProfile == .serious,
                "fair thermal demoted by LPM lands on .serious")
    }

    // M-7 from review-cycle 1 / L-C2-1 from cycle 2: pin the contract that
    // `qualityProfile` reflects the snapshot passed on THIS call, not a prior
    // admit result. Today the controller stores no per-call device state, so
    // this test is a forward canary against a future refactor that caches a
    // profile across admit/defer transitions.
    @Test("admission flips correctly when profile alternates admit / defer")
    func testAdmissionFlipsAcrossProfileChanges() async {
        let controller = AdmissionController()
        await controller.enqueue(makeBackfillJob(jobId: "first", priority: 9, createdAt: 10))
        await controller.enqueue(makeBackfillJob(jobId: "second", priority: 8, createdAt: 20))

        // Call 1: nominal thermal, should admit "first" with .nominal profile.
        let admit1 = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true),
            batteryLevel: 0.95
        )
        #expect(admit1.job?.jobId == "first")
        #expect(admit1.qualityProfile == .nominal)
        await controller.finish(jobId: "first")

        // Call 2: critical thermal, must defer with .critical profile.
        let defer1 = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(thermalState: .critical, isCharging: true),
            batteryLevel: 0.95
        )
        #expect(defer1.job == nil)
        #expect(defer1.deferReason == .thermalThrottled)
        #expect(defer1.qualityProfile == .critical,
                "deferred decision must carry the .critical profile, not the prior .nominal")

        // Call 3: back to nominal, must admit "second" with fresh .nominal.
        let admit2 = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true),
            batteryLevel: 0.95
        )
        #expect(admit2.job?.jobId == "second",
                "queue head must advance after the previous job finished")
        #expect(admit2.qualityProfile == .nominal,
                "post-defer admit must derive a fresh profile, not reuse .critical")
    }
}
