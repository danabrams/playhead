// AdmissionController.swift
// Minimal serial queue for phase-3 backfill jobs gated by device capability.
//
// Design (C1 alignment):
// `QualityProfile` is the single source of truth for analysis admission. The
// controller derives a `QualityProfile` from the snapshot + live battery
// reading via `QualityProfile.derive(...)`, then routes admission purely
// through `profile.schedulerPolicy.pauseAllWork`. `AdmissionDecision` wraps
// the derived profile together with the queue-state outcome (admitted job,
// or a queue-state / device-state defer reason).
//
// The former `DeviceAdmissionPolicy` struct has been removed; its thermal,
// low-battery, and low-power-mode precedence is now expressed as a mapping
// from profile + raw inputs back to an `AdmissionDeferReason` enum case so
// callers that care about "why was this deferred" keep their wire-compatible
// string reason (persisted in `BackfillJob.deferReason`).
//
// Lifecycle contract:
// Every successful call to ``admitNextEligibleJob(snapshot:batteryLevel:)`` —
// i.e. one that returns an `AdmissionDecision` with a non-nil `job` — MUST
// be followed by exactly one of:
//   * ``finish(jobId:)``            on successful completion
//   * ``failed(jobId:reason:)``     on terminal or retryable failure
// Skipping cleanup leaves the controller's serial slot occupied and blocks
// all future admissions. Prefer the higher-order helper
// ``withAdmittedJob(snapshot:batteryLevel:_:)`` which guarantees cleanup even
// when the closure throws.

import Foundation
import OSLog
import UIKit

/// Discrete reasons the controller will defer a job rather than admit it.
///
/// Device-state cases (`thermalThrottled`, `batteryTooLow`, `lowPowerMode`)
/// are derived from the `QualityProfile` + raw inputs at admission time.
/// Queue-state cases (`serialBusy`, `queueEmpty`) are
/// admission-controller-specific.
///
/// `BackfillJob.deferReason` remains a `String` for SQLite compatibility —
/// the mapping there uses ``rawValue`` so the wire format is unchanged.
enum AdmissionDeferReason: String, Sendable, Equatable, CaseIterable {
    case serialBusy
    case thermalThrottled
    case batteryTooLow
    case lowPowerMode
    case queueEmpty
}

struct AdmissionDecision: Sendable, Equatable {
    let job: BackfillJob?
    let deferReason: AdmissionDeferReason?
    /// The `QualityProfile` derived from the snapshot at admission time.
    ///
    /// Populated on every device-state evaluation (both admit and device-
    /// state defer paths). `nil` only on the pre-device-state short-circuits
    /// (`.idle` when the queue is empty, and `.deferred(.serialBusy)` when
    /// the serial slot is already occupied) — those outcomes don't evaluate
    /// device state at all.
    let qualityProfile: QualityProfile?

    static let idle = AdmissionDecision(
        job: nil,
        deferReason: nil,
        qualityProfile: nil
    )

    static func admitted(
        _ job: BackfillJob,
        qualityProfile: QualityProfile
    ) -> AdmissionDecision {
        AdmissionDecision(
            job: job,
            deferReason: nil,
            qualityProfile: qualityProfile
        )
    }

    static func deferred(
        _ reason: AdmissionDeferReason,
        qualityProfile: QualityProfile? = nil
    ) -> AdmissionDecision {
        AdmissionDecision(
            job: nil,
            deferReason: reason,
            qualityProfile: qualityProfile
        )
    }
}

actor AdmissionController {
    /// The number of FAILED attempts allowed before a job is considered
    /// exhausted. After `maxRetries` failures the persisted `retryCount`
    /// equals `maxRetries`, the controller stops requeueing, and the runner
    /// skips the job on future invocations (see
    /// `BackfillJobRunner.runPendingBackfill`'s `retryCount >= maxRetries`
    /// gate). Keep this aligned with the runner; the C-R3-3 fix standardized
    /// both sides on `maxRetries == 3` total attempts.
    static let maxRetries: Int = 3

    private var queuedJobs: [BackfillJob] = []
    private(set) var runningJob: BackfillJob?

    func enqueue(_ job: BackfillJob) {
        // Sorted-insert keeps enqueue O(n): we walk until we find the first
        // job that should sort after the new one. The expected queue depth is
        // small, so the linear walk is dominated by Swift array movement costs
        // and beats append + sort for the practical range here.
        let insertIndex = queuedJobs.firstIndex(where: { existing in
            Self.shouldOrder(job, before: existing)
        }) ?? queuedJobs.endIndex
        queuedJobs.insert(job, at: insertIndex)
    }

    func admitNextEligibleJob(
        snapshot: CapabilitySnapshot,
        batteryLevel: Float
    ) -> AdmissionDecision {
        // Empty queue check runs first so callers do not see confusing throttle
        // telemetry when there is nothing to schedule anyway.
        guard !queuedJobs.isEmpty else {
            return .idle
        }

        guard runningJob == nil else {
            return .deferred(.serialBusy)
        }

        // Route all thermal/battery/low-power reads through QualityProfile.
        // The snapshot carries the thermal and LPM reads captured at the same
        // instant; the live battery level comes from the caller's polled
        // value. Note: `isCharging` here is read off the snapshot, NOT the
        // live battery provider — `BackgroundProcessingService` overrides
        // the snapshot's charging bit with a BPS-cached fresher value, but
        // this controller has no such cache. In practice the divergence is
        // bounded by one capability tick and only matters at admission for
        // edge inputs that would change the profile across that boundary.
        // Tracked as tech debt under cycle 2 M-C2-3 (charging freshness).
        let profile = QualityProfile.derive(
            thermalState: snapshot.thermalState.processInfoValue,
            batteryLevel: batteryLevel,
            batteryState: snapshot.isCharging ? .charging : .unplugged,
            isLowPowerMode: snapshot.isLowPowerMode
        )

        // `pauseAllWork` is true only in `.critical` today. That matches the
        // design intent: the scheduler has four lanes but the admission
        // controller owns a single "backfill-class" lane, so we admit unless
        // the profile says pause everything.
        if profile.schedulerPolicy.pauseAllWork {
            let reason = Self.deferReason(
                snapshot: snapshot,
                batteryLevel: batteryLevel
            )
            return .deferred(reason, qualityProfile: profile)
        }

        let job = queuedJobs.removeFirst()
        runningJob = job
        return .admitted(job, qualityProfile: profile)
    }

    func finish(jobId: String) {
        guard runningJob?.jobId == jobId else { return }
        runningJob = nil
    }

    /// Marks the running job as failed. Clears the serial slot, increments
    /// `retryCount`, and re-enqueues a copy if the job is still under the
    /// retry budget.
    ///
    /// - Returns: the requeued job when the job was re-admitted to the queue,
    ///   or `nil` when the retry budget is exhausted.
    @discardableResult
    func failed(jobId: String, reason: String) -> BackfillJob? {
        guard let running = runningJob, running.jobId == jobId else {
            return nil
        }
        runningJob = nil

        // C-R3-3: budget boundary. `maxRetries` is the number of FAILED
        // attempts allowed before giving up; on the (maxRetries)-th failure
        // the retry counter reaches `maxRetries` and the job is exhausted.
        // Using `<` (rather than `<=`) matches the runner's
        // `retryCount >= maxRetries` skip gate — together they give
        // `maxRetries` total attempts, off-by-one free.
        let nextRetryCount = running.retryCount + 1
        guard nextRetryCount < Self.maxRetries else {
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
            createdAt: running.createdAt
        )
        enqueue(requeued)
        return requeued
    }

    /// Admits the next eligible job, runs the closure, and guarantees the
    /// running slot is cleared even if the closure throws. On throw the job is
    /// funnelled through ``failed(jobId:reason:)`` so retries and the
    /// terminal-failure path stay consistent with the manual API.
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

    // MARK: - Defer Reason Attribution

    /// Map a `pauseAllWork` deferral back to the most-specific
    /// `AdmissionDeferReason` enum case, preserving the wire format expected
    /// by `BackfillJob.deferReason` consumers.
    ///
    /// Precedence mirrors the former `DeviceAdmissionPolicy.evaluate` for
    /// deferral string compatibility: thermal first, then low-battery-and-
    /// unplugged, then low-power-mode. In practice only `.critical` thermal
    /// currently triggers `pauseAllWork`, so the thermal branch will always
    /// match; the battery and LPM branches are defensive guards for future
    /// `QualityProfile` expansions that might demote further. The fallback
    /// at the end logs a fault before returning `.thermalThrottled` so any
    /// future drift surfaces in Console rather than silently miscategorizing.
    private static func deferReason(
        snapshot: CapabilitySnapshot,
        batteryLevel: Float
    ) -> AdmissionDeferReason {
        if snapshot.thermalState == .critical {
            return .thermalThrottled
        }
        let batteryKnownAndLow = batteryLevel >= 0
            && batteryLevel < QualityProfile.lowBatteryThreshold
        if batteryKnownAndLow && !snapshot.isCharging {
            return .batteryTooLow
        }
        if snapshot.isLowPowerMode {
            return .lowPowerMode
        }
        // Reached only if a future `QualityProfile` expansion sets
        // `pauseAllWork` for inputs other than critical thermal / low battery
        // unplugged / LPM. Log a fault so the drift is visible, then fall
        // back to thermal as the structurally-honest wire-compatible reason.
        Self.attributionLogger.fault(
            "deferReason fell through (thermal=\(snapshot.thermalState.rawValue, privacy: .public), battery=\(batteryLevel, privacy: .public), isCharging=\(snapshot.isCharging, privacy: .public), isLowPowerMode=\(snapshot.isLowPowerMode, privacy: .public)) — QualityProfile or this helper has drifted such that pauseAllWork is set without matching any of (critical thermal | unplugged-low-battery | LPM)"
        )
        return .thermalThrottled
    }

    private static let attributionLogger = Logger(
        subsystem: "com.playhead",
        category: "AdmissionController.attribution"
    )
}
