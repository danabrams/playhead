// AnalysisRangeRequest.swift
// Input types for AnalysisJobRunner — describes a bounded range of audio to
// decode, transcribe, detect ads in, and (optionally) materialize cues for.

import Foundation

// MARK: - AnalysisRangeRequest

struct AnalysisRangeRequest: Sendable {
    enum Mode: Sendable { case preRollWarmup, playback, backgroundBackfill }

    let jobId: String
    let episodeId: String
    let podcastId: String
    let analysisAssetId: String
    let audioURL: LocalAudioURL
    let desiredCoverageSec: Double
    let mode: Mode
    let outputPolicy: OutputPolicy
    let priority: TaskPriority

    /// Scheduler lane the underlying `AnalysisJob` resolved to, used by
    /// the runner to register with `LanePreemptionCoordinator` so a
    /// higher-lane admission can preempt this job at its next safe
    /// point (playhead-01t8). Defaults to `.background` for tests and
    /// pre-01t8 callers that do not route via the scheduler; that
    /// default is conservative because Background is the most
    /// preemptible lane.
    let schedulerLane: AnalysisWorkScheduler.SchedulerLane

    init(
        jobId: String,
        episodeId: String,
        podcastId: String,
        analysisAssetId: String,
        audioURL: LocalAudioURL,
        desiredCoverageSec: Double,
        mode: Mode,
        outputPolicy: OutputPolicy,
        priority: TaskPriority,
        schedulerLane: AnalysisWorkScheduler.SchedulerLane = .background
    ) {
        self.jobId = jobId
        self.episodeId = episodeId
        self.podcastId = podcastId
        self.analysisAssetId = analysisAssetId
        self.audioURL = audioURL
        self.desiredCoverageSec = desiredCoverageSec
        self.mode = mode
        self.outputPolicy = outputPolicy
        self.priority = priority
        self.schedulerLane = schedulerLane
    }
}

// MARK: - OutputPolicy

enum OutputPolicy: Sendable {
    /// Write AdWindows and materialize SkipCues.
    case writeWindowsAndCues
    /// Write AdWindows and push live to SkipOrchestrator (no cue persistence).
    case writeWindowsAndPushLive
    /// Write AdWindows only — no cue materialization or live push.
    case writeWindowsOnly
}
