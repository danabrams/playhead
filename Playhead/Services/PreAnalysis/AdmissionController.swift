// AdmissionController.swift
// Minimal serial queue for phase-3 backfill jobs gated by device capability.

import Foundation

struct AdmissionDecision: Sendable, Equatable {
    let job: BackfillJob?
    let deferReason: String?

    static let idle = AdmissionDecision(job: nil, deferReason: nil)

    static func admitted(_ job: BackfillJob) -> AdmissionDecision {
        AdmissionDecision(job: job, deferReason: nil)
    }

    static func deferred(_ reason: String) -> AdmissionDecision {
        AdmissionDecision(job: nil, deferReason: reason)
    }
}

actor AdmissionController {
    static let lowBatteryThreshold: Float = 0.20

    private var queuedJobs: [BackfillJob] = []
    private var runningJobId: String?

    func enqueue(_ job: BackfillJob) {
        queuedJobs.append(job)
        queuedJobs.sort(by: Self.prioritySort)
    }

    func admitNextEligibleJob(
        snapshot: CapabilitySnapshot,
        batteryLevel: Float
    ) -> AdmissionDecision {
        guard runningJobId == nil else {
            return .deferred("serialBusy")
        }

        guard !snapshot.shouldThrottleAnalysis else {
            return .deferred("thermalThrottled")
        }

        let batteryKnownAndLow = batteryLevel >= 0 && batteryLevel < Self.lowBatteryThreshold
        guard snapshot.isCharging || !batteryKnownAndLow else {
            return .deferred("batteryTooLow")
        }

        guard !queuedJobs.isEmpty else {
            return .idle
        }

        let job = queuedJobs.removeFirst()
        runningJobId = job.jobId
        return .admitted(job)
    }

    func finish(jobId: String) {
        guard runningJobId == jobId else { return }
        runningJobId = nil
    }

    private static func prioritySort(lhs: BackfillJob, rhs: BackfillJob) -> Bool {
        if lhs.priority != rhs.priority {
            return lhs.priority > rhs.priority
        }
        if lhs.createdAt != rhs.createdAt {
            return lhs.createdAt < rhs.createdAt
        }
        return lhs.jobId < rhs.jobId
    }
}
