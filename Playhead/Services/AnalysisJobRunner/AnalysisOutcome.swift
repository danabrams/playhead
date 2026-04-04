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
