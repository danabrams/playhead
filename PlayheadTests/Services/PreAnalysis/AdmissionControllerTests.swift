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
        #expect(blockedWhileBusy.job == nil)
        #expect(blockedWhileBusy.deferReason == .serialBusy)
        #expect(second.job?.jobId == "low-priority")
    }

    @Test("defers for thermal throttle or low battery, but charging overrides low battery")
    func testThermalAndBatteryGating() async {
        let controller = AdmissionController()
        await controller.enqueue(makeBackfillJob(jobId: "gated-job", priority: 5))

        let thermalBlocked = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(thermalState: .serious, isCharging: true),
            batteryLevel: 0.95
        )
        let lowBatteryBlocked = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: false),
            batteryLevel: 0.19
        )
        let chargingAdmitted = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true),
            batteryLevel: 0.05
        )

        #expect(thermalBlocked.job == nil)
        #expect(thermalBlocked.deferReason == .thermalThrottled)
        #expect(lowBatteryBlocked.job == nil)
        #expect(lowBatteryBlocked.deferReason == .batteryTooLow)
        #expect(chargingAdmitted.job?.jobId == "gated-job")
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

    @Test("defers Low Power Mode via the shared DeviceAdmissionPolicy")
    func testLowPowerModeDeferral() async {
        let controller = AdmissionController()
        await controller.enqueue(makeBackfillJob(jobId: "lpm-job", priority: 5))

        let blocked = await controller.admitNextEligibleJob(
            snapshot: makeCapabilitySnapshot(
                thermalState: .nominal,
                isLowPowerMode: true,
                isCharging: true
            ),
            batteryLevel: 0.95
        )

        #expect(blocked.job == nil)
        #expect(blocked.deferReason == .lowPowerMode)
    }
}
