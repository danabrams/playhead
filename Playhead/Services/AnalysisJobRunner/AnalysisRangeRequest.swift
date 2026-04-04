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
