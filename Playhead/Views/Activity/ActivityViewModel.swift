// ActivityViewModel.swift
// Pure-aggregation view-model for the Activity screen. Projects a list of
// `(episodeId, title, EpisodeSurfaceStatus)` inputs into the four
// canonical sections: Now / Up Next / Paused / Recently Finished.
//
// Scope: playhead-quh7 (Phase 2 deliverable 4 — Activity screen).
//
// # Why a pure aggregator
//
// Section bucketing is a function of `EpisodeSurfaceStatus` alone. The
// reducer (`episodeSurfaceStatus`) already produces the disposition,
// reason, hint, and analysis-unavailable-reason fields the bucketing
// rule consults. The VM therefore avoids any persistence / scheduler
// dependency and stays trivially testable: a snapshot test feeds inputs
// in, asserts the four-section split out.
//
// I/O (which episodes have a status row, who is "running", finished
// timestamps) is fetched by an injected `ActivitySnapshotProvider`
// closure in production. Tests bypass the provider entirely and call
// `aggregate(inputs:now:)` directly.
//
// # SurfaceReason / ResolutionHint copy
//
// The Activity screen is the user-facing home of `SurfaceReason`
// rendering. This file does NOT duplicate copy strings — the View calls
// into `SurfaceReasonCopyTemplates.template(for:)` for SurfaceReason
// copy, `EpisodeStatusLineCopy.hintCopy(_:)` for ResolutionHint copy,
// and `EpisodeStatusLineCopy.unavailableReasonCopy(_:)` for the
// per-device unavailability fragment. The VM is structurally
// disconnected from the copy table.

import Foundation
import Observation

// MARK: - ActivityEpisodeInput

/// One row's worth of input to the Activity aggregator. Carries the
/// reducer's output (`status`) plus the small handful of UI fields
/// (`episodeTitle`, `podcastTitle`) and bucketing signals (`isRunning`,
/// `finishedAt`) that don't live on the status struct itself.
///
/// `isRunning` is sourced from the scheduler's per-job lane bookkeeping
/// (a Soon / Background lane job that has been admitted is "running");
/// `finishedAt` is the most-recent terminal-state timestamp from the
/// work journal. Both are produced by the production
/// `ActivitySnapshotProvider`; tests build them by hand.
struct ActivityEpisodeInput: Sendable, Hashable {
    let episodeId: String
    let episodeTitle: String
    let podcastTitle: String?
    let status: EpisodeSurfaceStatus
    /// `true` when the scheduler has admitted this episode's job and it
    /// is actively executing. Drives the Now-vs-Up-Next split for
    /// `disposition == .queued` rows.
    let isRunning: Bool
    /// Wall-clock timestamp when this episode reached a terminal state
    /// (done / failed / cancelled / unavailable). `nil` for in-flight
    /// jobs. Drives Recently-Finished membership and ordering.
    let finishedAt: Date?
}

// MARK: - ActivitySnapshot

/// Fully-aggregated four-section payload the Activity view renders.
struct ActivitySnapshot: Sendable, Hashable {
    let now: [ActivityNowRow]
    let upNext: [ActivityUpNextRow]
    let paused: [ActivityPausedRow]
    let recentlyFinished: [ActivityRecentlyFinishedRow]

    static let empty = ActivitySnapshot(
        now: [], upNext: [], paused: [], recentlyFinished: []
    )
}

// MARK: - Row payloads

/// "Now" row — an episode whose analysis is actively executing.
///
/// No SurfaceReason rendering here per spec — Now shows what is happening
/// (a progress phrase), not why something is blocked. The progress phrase
/// is intentionally coarse in v1: download/analysis state, no per-episode
/// time estimate.
struct ActivityNowRow: Sendable, Hashable, Identifiable {
    let episodeId: String
    let title: String
    let podcastTitle: String?
    /// Coarse progress label (e.g. "Analyzing"). v1 keeps this simple;
    /// the spec example "Analyzing next 15m" is a Phase 3 concern that
    /// requires plumbing the lookahead window into the row builder.
    let progressPhrase: String
    var id: String { episodeId }
}

/// "Up Next" row — queued, eligible, not yet running. No SurfaceReason
/// rendering per spec; reorder UI is the View's responsibility.
struct ActivityUpNextRow: Sendable, Hashable, Identifiable {
    let episodeId: String
    let title: String
    let podcastTitle: String?
    var id: String { episodeId }
}

/// "Paused" row — the user-facing home of `SurfaceReason` rendering. The
/// row carries the reducer's `(reason, hint)` pair so the View can call
/// `SurfaceReasonCopyTemplates.template(for:)` and
/// `EpisodeStatusLineCopy.hintCopy(_:)` to produce the user-visible
/// strings without the VM duplicating copy.
struct ActivityPausedRow: Sendable, Hashable, Identifiable {
    let episodeId: String
    let title: String
    let podcastTitle: String?
    let reason: SurfaceReason
    let hint: ResolutionHint
    var id: String { episodeId }
}

/// "Recently Finished" row — outcome history. Renders one of three
/// outcomes (✓ success, ✕ couldnt_analyze, ℹ analysis_unavailable). The
/// `analysis_unavailable` case carries the derived
/// `AnalysisUnavailableReason` so the View can reuse
/// `EpisodeStatusLineCopy.unavailableReasonCopy(_:)` for the sub-copy
/// without duplicating strings.
struct ActivityRecentlyFinishedRow: Sendable, Hashable, Identifiable {
    let episodeId: String
    let title: String
    let podcastTitle: String?
    let outcome: ActivityFinishedOutcome
    let finishedAt: Date
    var id: String { episodeId }
}

/// Three-case outcome bucket for Recently Finished. Bucketing rules:
/// - `success` — `disposition == .queued && playbackReadiness == .complete`
///   AND finishedAt is set (the analysis completed).
/// - `couldntAnalyze` — `disposition ∈ {.failed, .cancelled}`. Cancelled
///   is grouped here to mirror `EpisodeStatusLineCopy.failedPrimary()`'s
///   "Couldn't analyze · Retry" treatment of cancelled jobs.
/// - `analysisUnavailable(reason)` — `disposition == .unavailable`. The
///   per-device reason flows through so the sub-copy can be derived from
///   `EpisodeStatusLineCopy.unavailableReasonCopy(_:)`.
enum ActivityFinishedOutcome: Sendable, Hashable {
    case success
    case couldntAnalyze
    case analysisUnavailable(AnalysisUnavailableReason)
}

// MARK: - ActivityViewModel

/// `@Observable @MainActor` aggregator for the Activity screen. Holds an
/// `ActivitySnapshot` SwiftUI re-renders against, and exposes a
/// `refresh(from:)` entry point the production wiring calls when a
/// scheduler-state change notification arrives.
///
/// Pure aggregation lives in the `static func aggregate(...)` so unit
/// tests can exercise bucketing without instantiating the VM.
@MainActor
@Observable
final class ActivityViewModel {

    /// Most-recently aggregated snapshot. SwiftUI observes mutations
    /// through `@Observable` and re-renders the four sections.
    private(set) var snapshot: ActivitySnapshot = .empty

    init(snapshot: ActivitySnapshot = .empty) {
        self.snapshot = snapshot
    }

    /// Re-aggregate the snapshot from a fresh batch of inputs. Called by
    /// the View on appear and on refresh-notification arrival; called by
    /// SwiftUI Previews to populate fixtures.
    func refresh(from inputs: [ActivityEpisodeInput], now: Date = Date()) {
        snapshot = Self.aggregate(inputs: inputs, now: now)
    }

    /// Apply a user drag-to-reorder gesture to the Up Next section. The
    /// View's `List { … }.onMove { src, dst in vm.moveUpNext(...) }`
    /// handler funnels through here so the snapshot's `upNext` ordering
    /// is the single source of truth the view re-renders against.
    ///
    /// v1 scope (playhead-quh7 fix): the reorder lives only on the
    /// in-memory snapshot — the next `refresh(from:)` will overwrite the
    /// user's manual order with whatever the production
    /// `ActivitySnapshotProvider` hands back. Persisting the order
    /// across refreshes requires a new column on the `Episode` SwiftData
    /// model (e.g. `queuePosition: Int?`) and is intentionally deferred
    /// to a follow-up bead per the spec-fix scope guidance ("if nothing
    /// supports persisted user ordering today, STOP and ask before going
    /// wider"). The ergonomic gap is real but bounded: refreshes today
    /// arrive only on scheduler-state changes, not on a polling loop, so
    /// the manual order persists for the duration of the user's
    /// interaction with the screen.
    func moveUpNext(from source: IndexSet, to destination: Int) {
        var reordered = snapshot.upNext
        reordered.move(fromOffsets: source, toOffset: destination)
        snapshot = ActivitySnapshot(
            now: snapshot.now,
            upNext: reordered,
            paused: snapshot.paused,
            recentlyFinished: snapshot.recentlyFinished
        )
    }

    // MARK: - Pure aggregation

    /// Maximum number of Recently Finished rows the snapshot retains.
    /// Mirrors the design doc §E ("Recently finished (last 24h, capped
    /// at ~20)") — older entries are pruned.
    nonisolated static let recentlyFinishedCap = 20

    /// Recently Finished retention window (24 hours). Entries older than
    /// this are dropped even if the cap allows more rows.
    nonisolated static let recentlyFinishedWindow: TimeInterval = 24 * 60 * 60

    /// Pure projection: bucket a list of inputs into the four sections.
    /// Stable enough for snapshot tests and SwiftUI Previews.
    ///
    /// Ordering rules:
    /// - Now / Up Next / Paused: input order preserved (the production
    ///   provider hands rows in scheduler-priority order). Drag-to-
    ///   reorder for Up Next ships via `moveUpNext(from:to:)` which
    ///   mutates the in-memory snapshot after aggregation.
    /// - Recently Finished: newest-first by `finishedAt`, capped at
    ///   `recentlyFinishedCap`, filtered to the trailing 24h window
    ///   ending at `now`.
    ///
    /// `nonisolated` because the implementation only touches value
    /// types — exposing it off the main actor lets unit tests call it
    /// directly from Swift Testing's nonisolated test bodies.
    nonisolated static func aggregate(
        inputs: [ActivityEpisodeInput],
        now: Date
    ) -> ActivitySnapshot {
        var nowRows: [ActivityNowRow] = []
        var upNextRows: [ActivityUpNextRow] = []
        var pausedRows: [ActivityPausedRow] = []
        var finishedRows: [ActivityRecentlyFinishedRow] = []

        for input in inputs {
            // Terminal-state rows route to Recently Finished when a
            // finishedAt timestamp is present and inside the retention
            // window. The disposition determines the outcome label.
            if let outcome = terminalOutcome(for: input.status) {
                guard let finishedAt = input.finishedAt else { continue }
                let age = now.timeIntervalSince(finishedAt)
                if age < 0 || age > recentlyFinishedWindow { continue }
                finishedRows.append(
                    ActivityRecentlyFinishedRow(
                        episodeId: input.episodeId,
                        title: input.episodeTitle,
                        podcastTitle: input.podcastTitle,
                        outcome: outcome,
                        finishedAt: finishedAt
                    )
                )
                continue
            }

            // Non-terminal rows: Paused dominates Queued. Queued rows
            // split on isRunning into Now vs Up Next.
            switch input.status.disposition {
            case .paused:
                pausedRows.append(
                    ActivityPausedRow(
                        episodeId: input.episodeId,
                        title: input.episodeTitle,
                        podcastTitle: input.podcastTitle,
                        reason: input.status.reason,
                        hint: input.status.hint
                    )
                )

            case .queued:
                if input.isRunning {
                    nowRows.append(
                        ActivityNowRow(
                            episodeId: input.episodeId,
                            title: input.episodeTitle,
                            podcastTitle: input.podcastTitle,
                            progressPhrase: progressPhrase(for: input.status)
                        )
                    )
                } else {
                    upNextRows.append(
                        ActivityUpNextRow(
                            episodeId: input.episodeId,
                            title: input.episodeTitle,
                            podcastTitle: input.podcastTitle
                        )
                    )
                }

            case .failed, .cancelled, .unavailable:
                // Terminal dispositions without a finishedAt drop on
                // the floor — they do not have a section that surfaces
                // them in v1. The provider only emits inputs the user
                // should see; an unfinished terminal-disposition row is
                // a transitional artifact.
                continue
            }
        }

        finishedRows.sort { $0.finishedAt > $1.finishedAt }
        if finishedRows.count > recentlyFinishedCap {
            finishedRows = Array(finishedRows.prefix(recentlyFinishedCap))
        }

        return ActivitySnapshot(
            now: nowRows,
            upNext: upNextRows,
            paused: pausedRows,
            recentlyFinished: finishedRows
        )
    }

    // MARK: - Helpers

    /// Map a status to its terminal-outcome bucket for Recently Finished,
    /// or nil when the status is non-terminal.
    nonisolated private static func terminalOutcome(
        for status: EpisodeSurfaceStatus
    ) -> ActivityFinishedOutcome? {
        switch status.disposition {
        case .queued:
            // A queued status can be "finished" when its readiness has
            // reached `.complete` (the analysis pipeline produced full
            // coverage). The scheduler still classifies the underlying
            // job as queued momentarily after completion until the
            // store updates; the readiness signal closes that gap.
            if status.playbackReadiness == .complete {
                return .success
            }
            return nil
        case .failed, .cancelled:
            // Both surface as "Couldn't analyze" per
            // EpisodeStatusLineCopy.failedPrimary() rationale.
            return .couldntAnalyze
        case .unavailable:
            // Always carries an analysisUnavailableReason in production;
            // fall back to .modelTemporarilyUnavailable as the softest
            // signal if the reducer ever emits .unavailable with a nil
            // reason (impossible per the reducer's invariants).
            let reason = status.analysisUnavailableReason ?? .modelTemporarilyUnavailable
            return .analysisUnavailable(reason)
        case .paused:
            return nil
        }
    }

    /// Coarse progress label for a Now row. v1 keeps this minimal; the
    /// spec's richer phrasing ("Downloading 3 eps · 1.2 GB / 4 GB") is a
    /// Phase 3 concern that requires plumbing batch metadata in. The
    /// label here is keyed off the persisted-status / readiness signal
    /// so a backfill-running episode reads "Analyzing" rather than the
    /// generic "Working".
    nonisolated private static func progressPhrase(for status: EpisodeSurfaceStatus) -> String {
        switch status.playbackReadiness {
        case .complete:
            return "Finishing"
        case .proximal, .deferredOnly, .none:
            return "Analyzing"
        }
    }
}
