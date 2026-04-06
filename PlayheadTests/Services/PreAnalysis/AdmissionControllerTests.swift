// AdmissionControllerTests.swift
// Tests for the minimal phase-3 backfill admission controller.

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
        #expect(blockedWhileBusy.deferReason == "serialBusy")
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
        #expect(thermalBlocked.deferReason == "thermalThrottled")
        #expect(lowBatteryBlocked.job == nil)
        #expect(lowBatteryBlocked.deferReason == "batteryTooLow")
        #expect(chargingAdmitted.job?.jobId == "gated-job")
    }
}
