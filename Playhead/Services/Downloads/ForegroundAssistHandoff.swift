// ForegroundAssistHandoff.swift
// playhead-44h1: decision logic for handing off a foreground-assist
// download transfer when the app backgrounds.
//
// State machine overview (Phase 1 deliverable 9, §5 Live Activity
// carve-out):
//   1. User taps Download. `DownloadManager` acquires a foreground-
//      assist work item (`UIApplication.shared.beginBackgroundTask`)
//      and starts the interactive URLSession transfer (playhead-24cm).
//   2. Scheduler promotes the job to the Now lane (playhead-r835).
//   3. When the user backgrounds the app
//      (`UIApplication.willResignActive`), this module decides whether
//      to keep the foreground-assist task alive via URLSession's own
//      background-session plumbing OR submit a
//      `BGContinuedProcessingTaskRequest` for a longer window.
//
// The decision in step 3 is a small, pure function: given a transfer's
// byte counters and an average throughput estimate, is the transfer
// "almost done"? The threshold is spec-fixed: keep the foreground-
// assist task alive if the transfer is at least 80% complete OR the
// remaining-byte ETA is at most 2 minutes. Otherwise, submit a
// BGContinuedProcessingTaskRequest and end the foreground-assist task.
//
// This file intentionally owns ONLY the decision logic, not the actual
// `beginBackgroundTask` / `willResignActive` plumbing — that wiring
// integration lives in playhead-iwiy (out of scope for this bead). The
// unit test for the 80% / 2-min rule drives this type directly.

import Foundation

// MARK: - Hand-off decision

/// Snapshot of an in-flight download used by
/// `ForegroundAssistHandoff.decide(for:)` to choose between continuing
/// with the foreground-assist task or handing off to a
/// `BGContinuedProcessingTaskRequest`.
///
/// Byte counters come from `URLSessionDownloadTask`'s standard
/// reporting (`totalBytesWritten` / `totalBytesExpectedToWrite`).
/// `averageBytesPerSecond` is the caller's best estimate of current
/// throughput — typically a short-window moving average so the ETA
/// responds to Wi-Fi / LTE transitions rather than lifetime-average
/// smearing. A non-positive throughput is treated as "unknown" and
/// forces the BG-task hand-off (the safe choice when we can't predict
/// completion).
struct ForegroundAssistTransferSnapshot: Sendable, Equatable {
    let totalBytesWritten: Int64
    let totalBytesExpectedToWrite: Int64
    let averageBytesPerSecond: Double

    init(
        totalBytesWritten: Int64,
        totalBytesExpectedToWrite: Int64,
        averageBytesPerSecond: Double
    ) {
        self.totalBytesWritten = totalBytesWritten
        self.totalBytesExpectedToWrite = totalBytesExpectedToWrite
        self.averageBytesPerSecond = averageBytesPerSecond
    }

    /// Fraction completed in the `[0, 1]` range. Returns 0 when the
    /// expected-total is unknown or non-positive so the 80% rule can't
    /// spuriously fire on a still-unsized response.
    var fractionCompleted: Double {
        guard totalBytesExpectedToWrite > 0 else { return 0 }
        let fraction = Double(totalBytesWritten) / Double(totalBytesExpectedToWrite)
        return max(0, min(1, fraction))
    }

    /// Estimated remaining wall-clock seconds. Returns `.infinity`
    /// when throughput is unknown (non-positive) so the 2-min rule
    /// never spuriously fires on startup before a throughput estimate
    /// is established.
    var remainingSeconds: Double {
        guard averageBytesPerSecond > 0 else { return .infinity }
        let remainingBytes = max(0, totalBytesExpectedToWrite - totalBytesWritten)
        return Double(remainingBytes) / averageBytesPerSecond
    }
}

/// Which background-transport strategy to use after the app
/// backgrounds with an in-flight foreground-assist transfer.
enum ForegroundAssistHandoffDecision: Sendable, Equatable {
    /// Keep the foreground-assist task alive via URLSession's own
    /// background-session plumbing. Appropriate when the transfer is
    /// near-complete (≥ 80%) or the ETA is short (≤ 2 min); the system
    /// will typically let the existing task finish before reclaiming
    /// the runtime.
    case keepForegroundAssistAlive

    /// Submit a `BGContinuedProcessingTaskRequest` and end the
    /// foreground-assist task. Appropriate when the transfer will
    /// take long enough that the 15–30 min BG window is the right
    /// fit for completing both the remaining transfer and the
    /// post-download analysis.
    case submitContinuedProcessingRequest
}

/// Pure decision module — no state, no side effects. Every input is a
/// snapshot value; every output is a decision enum. The 80% / 2-min
/// thresholds live here so tests and production read from the same
/// source of truth.
enum ForegroundAssistHandoff {

    /// Transfer-complete threshold (fraction). At or above this value
    /// the module keeps the foreground-assist task alive.
    static let completionFractionThreshold: Double = 0.80

    /// Remaining-time threshold. At or below this value the module
    /// keeps the foreground-assist task alive.
    static let remainingTimeThreshold: Duration = .seconds(120)

    /// Applies the 80% / 2-min rule to `snapshot`.
    ///
    /// Returns `.keepForegroundAssistAlive` when either:
    ///   - fraction completed is at least
    ///     `completionFractionThreshold` (0.80), OR
    ///   - remaining-byte ETA is at most `remainingTimeThreshold`
    ///     (120 s).
    /// Otherwise returns `.submitContinuedProcessingRequest`.
    ///
    /// The disjunction matches the bead spec exactly: either gate
    /// alone is sufficient. A snapshot with unknown expected-total
    /// decodes as fraction=0 and the throughput gate can still
    /// trigger keepalive if the caller has a usable throughput
    /// estimate; a snapshot with unknown throughput decodes as
    /// remainingSeconds=∞ and only the 80% gate can trigger
    /// keepalive.
    static func decide(for snapshot: ForegroundAssistTransferSnapshot) -> ForegroundAssistHandoffDecision {
        if snapshot.fractionCompleted >= completionFractionThreshold {
            return .keepForegroundAssistAlive
        }
        let remainingSecondsBudget = Double(remainingTimeThreshold.components.seconds)
        if snapshot.remainingSeconds <= remainingSecondsBudget {
            return .keepForegroundAssistAlive
        }
        return .submitContinuedProcessingRequest
    }
}
