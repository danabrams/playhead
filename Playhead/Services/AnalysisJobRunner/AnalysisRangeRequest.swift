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

    /// playhead-swws: the cascade-selected window (sponsor-chapter or
    /// playhead-proximal) that the scheduler chose to dispatch this
    /// pass. `nil` when no candidate-window cascade is wired or the
    /// episode has not been seeded — in that case the runner falls
    /// back to its existing FIFO/depth-driven processing of `[0,
    /// desiredCoverageSec]`. When non-nil, this is the range the
    /// runner SHOULD prioritize within the broader job; this bead
    /// surfaces the field for observability/testing only — slice-level
    /// execution is wired by playhead-1iq1.
    let windowRange: ClosedRange<TimeInterval>?

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
        schedulerLane: AnalysisWorkScheduler.SchedulerLane = .background,
        windowRange: ClosedRange<TimeInterval>? = nil
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
        self.windowRange = windowRange
    }
}

// MARK: - DispatchableSlice (playhead-swws)

/// A peek of the next job the scheduler would dispatch on the next
/// loop iteration, paired with the cascade's first candidate window
/// for the job's episode if the cascade has been seeded for it.
///
/// Surfaced as a public, side-effect-free accessor on
/// `AnalysisWorkScheduler` (`selectNextDispatchableSlice()`) so the
/// playhead-swws ordering test can prove that proximal-first cascade
/// order overrides the store's `priority DESC, createdAt ASC` (FIFO
/// at equal priority) ordering for episodes the cascade has been
/// seeded for. When the cascade returns no windows for the job's
/// episode (no seed, or all windows consumed), `cascadeWindow` is
/// `nil` and the dispatched job is exactly what `fetchNextEligibleJob`
/// would return — preserving FIFO behavior for callers that have not
/// opted into cascade-aware dispatch.
struct DispatchableSlice: Sendable, Equatable {
    let jobId: String
    let episodeId: String
    /// First window from `CandidateWindowCascade.currentWindows(for:)`
    /// for this job's episode — sponsor-chapter windows come first,
    /// then the proximal window. `nil` when the cascade is unwired
    /// for the episode.
    let cascadeWindow: CandidateWindow?
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
