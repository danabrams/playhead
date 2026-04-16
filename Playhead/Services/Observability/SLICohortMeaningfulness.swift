// SLICohortMeaningfulness.swift
// Encodes which (SLI × cohort) cells are meaningful. The Phase 1
// instrumentation emitter (playhead-1nl6) is expected to call
// ``isMeaningful(sli:cohort:)`` before emission and skip any cell that
// returns `false` (emit as nil / absent — NOT as zero).
//
// Rules:
//
// * `time_to_downloaded` is defined for explicit-download cohorts only,
//   and the "30–90 min" reporting scope means duration buckets
//   `between30and60m` or `between60and90m`. Other buckets / triggers
//   are tracked elsewhere and should not be emitted under this SLI.
//
// * `time_to_proximal_skip_ready` is additionally restricted to the
//   `eligibleAndAvailable` analysis mode — it has no meaning if the
//   pipeline hasn't actually started running. The other two modes
//   emit nil.
//
// * `ready_by_first_play_rate` applies across all cohorts — a
//   play-start is a play-start regardless of how the episode got
//   there.
//
// * `false_ready_rate` applies only when the pipeline claimed the
//   episode was ready, which is only possible in `eligibleAndAvailable`
//   mode.
//
// * `unattributed_pause_rate` applies across all cohorts — pauses
//   can happen at any time in any mode.

import Foundation

/// Table-driven meaningfulness check. Keep logic here — ``SLI`` delegates
/// to this type from its `isMeaningful(for:)` method.
enum SLICohortMeaningfulness {

    /// Returns `true` if the given (SLI, cohort) cell is meaningful and
    /// should be emitted. Returns `false` if the emitter should omit /
    /// null-out the cell.
    static func isMeaningful(sli: SLI, cohort: SLICohort) -> Bool {
        switch sli {
        case .timeToDownloaded:
            guard cohort.trigger == .explicitDownload else { return false }
            return isIn30To90MinuteScope(cohort.durationBucket)

        case .timeToProximalSkipReady:
            guard cohort.trigger == .explicitDownload else { return false }
            guard cohort.analysisMode == .eligibleAndAvailable else { return false }
            return isIn30To90MinuteScope(cohort.durationBucket)

        case .readyByFirstPlayRate:
            return true

        case .falseReadyRate:
            return cohort.analysisMode == .eligibleAndAvailable

        case .unattributedPauseRate:
            return true
        }
    }

    /// Whether the bucket is within the "30–90 minute" reporting scope
    /// shared by the two latency SLIs.
    private static func isIn30To90MinuteScope(_ bucket: SLIEpisodeDurationBucket) -> Bool {
        switch bucket {
        case .between30and60m, .between60and90m:
            return true
        case .under30m, .over90m:
            return false
        }
    }
}
