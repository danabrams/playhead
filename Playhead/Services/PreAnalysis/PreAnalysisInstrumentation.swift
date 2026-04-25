// PreAnalysisInstrumentation.swift
// OSSignposter intervals and diagnostic metric logging for the
// pre-analysis pipeline. Used by AnalysisWorkScheduler and
// AnalysisJobRunner to emit Instruments-visible intervals and
// structured log lines for tuning.

import OSLog

enum PreAnalysisInstrumentation {
    static let signposter = OSSignposter(subsystem: "com.playhead", category: "PreAnalysis")
    static let logger = Logger(subsystem: "com.playhead", category: "PreAnalysis")

    // MARK: - Queue Wait (enqueue → processJob start)

    static func beginQueueWait(jobId: String) -> OSSignpostIntervalState {
        let id = signposter.makeSignpostID()
        return signposter.beginInterval("queue_wait", id: id, "\(jobId)")
    }

    static func endQueueWait(_ state: OSSignpostIntervalState) {
        signposter.endInterval("queue_wait", state)
    }

    // MARK: - Job Duration (processJob start → outcome)

    static func beginJobDuration(jobId: String) -> OSSignpostIntervalState {
        let id = signposter.makeSignpostID()
        return signposter.beginInterval("job_duration", id: id, "\(jobId)")
    }

    static func endJobDuration(_ state: OSSignpostIntervalState) {
        signposter.endInterval("job_duration", state)
    }

    // MARK: - Pipeline Stage Duration

    static func beginStage(_ name: String) -> OSSignpostIntervalState {
        let id = signposter.makeSignpostID()
        return signposter.beginInterval("stage", id: id, "\(name)")
    }

    static func endStage(_ state: OSSignpostIntervalState) {
        signposter.endInterval("stage", state)
    }

    // MARK: - Diagnostic Metrics (logged, not shipped)

    static func logCueReadiness(episodeId: String, hadCues: Bool) {
        logger.info("metric.cue_readiness episode=\(episodeId) ready=\(hadCues)")
    }

    static func logTimeToFirstCue(episodeId: String, seconds: Double) {
        logger.info("metric.time_to_first_cue episode=\(episodeId) seconds=\(seconds, format: .fixed(precision: 2))")
    }

    static func logTierCompletion(tier: String, completed: Bool) {
        logger.info("metric.tier_completion tier=\(tier) completed=\(completed)")
    }

    static func logThermalPause(duration: Double) {
        logger.info("metric.thermal_pause duration=\(duration, format: .fixed(precision: 2))")
    }

    static func logJobOutcome(jobId: String, stopReason: String, coverageSec: Double) {
        logger.info("metric.job_outcome job=\(jobId) reason=\(stopReason) coverage=\(coverageSec, format: .fixed(precision: 1))")
    }

    /// playhead-yqax: emit a structured log line each time the
    /// scheduler fires a foreground transcript catch-up dispatch.
    /// Surfaces in FrozenTrace as `foregroundCatchUp` per the bead's
    /// telemetry acceptance criterion ("distinguishable in FrozenTrace
    /// as `foregroundCatchUp` so we can verify it actually moved Conan
    /// pipeline coverage in the next NARL capture").
    ///
    /// Fields:
    ///   - `episode`/`job`: identity
    ///   - `prior`/`escalated`: the desiredCoverageSec change
    ///   - `playhead`: live playhead position observed at trigger
    ///   - `runway`: transcribed audio remaining ahead of playhead
    ///     when the trigger fired (i.e. how late we caught it)
    static func logForegroundCatchUp(
        episodeId: String,
        jobId: String,
        priorCoverageSec: Double,
        escalatedCoverageSec: Double,
        playheadPositionSec: Double,
        transcribedAheadSec: Double
    ) {
        logger.info(
            "metric.foregroundCatchUp episode=\(episodeId, privacy: .public) job=\(jobId, privacy: .public) prior=\(priorCoverageSec, format: .fixed(precision: 1)) escalated=\(escalatedCoverageSec, format: .fixed(precision: 1)) playhead=\(playheadPositionSec, format: .fixed(precision: 1)) runway=\(transcribedAheadSec, format: .fixed(precision: 1))"
        )
    }
}
