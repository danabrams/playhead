// AdmissionController.swift
// Minimal serial queue for phase-3 backfill jobs gated by device capability.
//
// TODO: [H6] Consolidate with BackgroundProcessingService — pending user
// decision, see review. The two components currently maintain separate
// admission policies; the consolidation requires a Decision Authority sign-off
// because it changes the service/actor architecture.
//
// Lifecycle contract:
// Every successful call to ``admitNextEligibleJob(snapshot:batteryLevel:)`` —
// i.e. one that returns ``AdmissionDecision/admitted(_:)`` — MUST be followed
// by exactly one of:
//   • ``finish(jobId:)``                — on successful completion
//   • ``failed(jobId:reason:)``         — on terminal or retryable failure
// Skipping the cleanup leaves the controller's serial slot occupied and
// permanently blocks all future admissions. Prefer the higher-order helper
// ``withAdmittedJob(snapshot:batteryLevel:_:)`` which guarantees cleanup even
// when the closure throws.

import Foundation

/// Discrete reasons the controller will defer a job rather than admit it.
///
/// `BackfillJob.deferReason` remains a `String` for SQLite compatibility — the
/// mapping there uses ``rawValue`` so the wire format is unchanged.
enum AdmissionDeferReason: String, Sendable, Equatable {
    case serialBusy
    case thermalThrottled
    case batteryTooLow
    case queueEmpty
}

struct AdmissionDecision: Sendable, Equatable {
    let job: BackfillJob?
    let deferReason: AdmissionDeferReason?

    static let idle = AdmissionDecision(job: nil, deferReason: nil)

    static func admitted(_ job: BackfillJob) -> AdmissionDecision {
        AdmissionDecision(job: job, deferReason: nil)
    }

    static func deferred(_ reason: AdmissionDeferReason) -> AdmissionDecision {
        AdmissionDecision(job: nil, deferReason: reason)
    }
}

actor AdmissionController {
    static let lowBatteryThreshold: Float = 0.20
    // TODO: [H6] Consolidate with BackgroundProcessingService — pending user
    // decision, see review. Same constant lives in two places today.
    static let maxRetries: Int = 3

    private var queuedJobs: [BackfillJob] = []
    private(set) var runningJob: BackfillJob?

    func enqueue(_ job: BackfillJob) {
        // Sorted-insert keeps enqueue O(n): we walk until we find the first
        // job that should sort *after* the new one. The expected queue depth
        // is small, so the linear walk is dominated by Swift array movement
        // costs and beats `append` + `sort` (O(n log n)) for n in [10, 10k].
        let insertIndex = queuedJobs.firstIndex(where: { existing in
            Self.shouldOrder(job, before: existing)
        }) ?? queuedJobs.endIndex
        queuedJobs.insert(job, at: insertIndex)
    }

    func admitNextEligibleJob(
        snapshot: CapabilitySnapshot,
        batteryLevel: Float
    ) -> AdmissionDecision {
        // Empty queue check runs first so callers don't see confusing
        // throttle telemetry when there's nothing to schedule anyway.
        guard !queuedJobs.isEmpty else {
            return .idle
        }

        guard runningJob == nil else {
            return .deferred(.serialBusy)
        }

        guard !snapshot.shouldThrottleAnalysis else {
            return .deferred(.thermalThrottled)
        }

        let batteryKnownAndLow = batteryLevel >= 0 && batteryLevel < Self.lowBatteryThreshold
        guard snapshot.isCharging || !batteryKnownAndLow else {
            return .deferred(.batteryTooLow)
        }

        let job = queuedJobs.removeFirst()
        runningJob = job
        return .admitted(job)
    }

    func finish(jobId: String) {
        guard runningJob?.jobId == jobId else { return }
        runningJob = nil
    }

    /// Marks the running job as failed. Clears the serial slot, increments
    /// `retryCount`, and re-enqueues a copy if the job is still under the
    /// ``maxRetries`` budget.
    ///
    /// - Returns: the requeued job (with bumped retry count) when the job was
    ///   re-admitted to the queue, or `nil` when the retry budget is
    ///   exhausted. In the `nil` case the caller is responsible for
    ///   persisting the terminal failure status.
    @discardableResult
    func failed(jobId: String, reason: String) -> BackfillJob? {
        guard let running = runningJob, running.jobId == jobId else {
            return nil
        }
        runningJob = nil

        let nextRetryCount = running.retryCount + 1
        guard nextRetryCount <= Self.maxRetries else {
            return nil
        }

        let requeued = BackfillJob(
            jobId: running.jobId,
            analysisAssetId: running.analysisAssetId,
            podcastId: running.podcastId,
            phase: running.phase,
            coveragePolicy: running.coveragePolicy,
            priority: running.priority,
            progressCursor: running.progressCursor,
            retryCount: nextRetryCount,
            deferReason: reason,
            status: .queued,
            scanCohortJSON: running.scanCohortJSON,
            decisionCohortJSON: running.decisionCohortJSON,
            createdAt: running.createdAt
        )
        enqueue(requeued)
        return requeued
    }

    /// Admits the next eligible job, runs the closure, and guarantees the
    /// running slot is cleared even if the closure throws. On throw the job
    /// is funnelled through ``failed(jobId:reason:)`` so retries and the
    /// terminal-failure path stay consistent with the manual API.
    ///
    /// - Returns: the closure's value when a job was admitted and ran to
    ///   completion, or `nil` when admission was deferred / idle.
    @discardableResult
    func withAdmittedJob<T: Sendable>(
        snapshot: CapabilitySnapshot,
        batteryLevel: Float,
        _ body: (BackfillJob) async throws -> T
    ) async throws -> T? {
        let decision = admitNextEligibleJob(snapshot: snapshot, batteryLevel: batteryLevel)
        guard let job = decision.job else {
            return nil
        }

        do {
            let value = try await body(job)
            finish(jobId: job.jobId)
            return value
        } catch {
            _ = failed(jobId: job.jobId, reason: "withAdmittedJobThrew")
            throw error
        }
    }

    private static func shouldOrder(_ lhs: BackfillJob, before rhs: BackfillJob) -> Bool {
        if lhs.priority != rhs.priority {
            return lhs.priority > rhs.priority
        }
        if lhs.createdAt != rhs.createdAt {
            return lhs.createdAt < rhs.createdAt
        }
        return lhs.jobId < rhs.jobId
    }
}
