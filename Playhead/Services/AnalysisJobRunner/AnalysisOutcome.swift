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
        /// playhead-ngev (review r1): the run was displaced by something with a
        /// better claim on the shared transcript engine — a scrub, a speed
        /// change, a different episode — rather than failing.
        ///
        /// SEPARATE FROM `.failed` FOR ONE REASON: RETRY ACCOUNTING. The
        /// scheduler charges `.failed` one of five PERMANENT attempts, and at
        /// five the job is `superseded` with `nextEligibleAt: nil`. That row is
        /// terminal and cannot come back: `analysis_jobs.workKey` is UNIQUE,
        /// `insertJob` is `INSERT OR IGNORE`, and the key is
        /// `"<fingerprint>:<analysisVersion>:preAnalysis"` — stable across
        /// launches — so every later enqueue for the episode is silently
        /// dropped until an app update bumps `analysisVersion`. Charging that
        /// budget for moving the playhead would permanently kill analysis on
        /// exactly the episodes a listener engages with most.
        ///
        /// SEPARATE FROM `.preempted` TOO, deliberately: that case carries lane
        /// semantics (a higher-lane admission flipped a `PreemptionSignal`) and
        /// its journal row means something specific. This one is the same
        /// ACCOUNTING with its own diagnosis, so the row keeps saying
        /// `cancelled` or `stopped` — which is true, and is the whole point of
        /// this bead.
        ///
        /// The payload is the same `"transcription:<class>"` string `.failed`
        /// would have carried, so nothing about the reporting is lost.
        case interrupted(String)
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
