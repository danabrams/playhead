// AnalysisOutcome.swift
// Result type returned by AnalysisJobRunner after a bounded analysis run.

import Foundation

struct AnalysisOutcome: Sendable {
    enum StopReason: Sendable {
        case reachedTarget
        case cancelledByPlayback
        case pausedForThermal
        case blockedByModel
        case memoryPressure
        case backgroundExpired
        /// A higher-lane admission (playhead-01t8) flipped the
        /// preemption signal and the runner paused at its next safe
        /// point. Coverage fields carry whatever the job managed to
        /// persist before the pause — by contract this is always on
        /// a checkpoint boundary so the next run is resumable.
        case preempted
        case failed(String)
    }

    let assetId: String
    let requestedCoverageSec: Double
    let featureCoverageSec: Double
    let transcriptCoverageSec: Double
    let cueCoverageSec: Double
    let newCueCount: Int
    let stopReason: StopReason
}
