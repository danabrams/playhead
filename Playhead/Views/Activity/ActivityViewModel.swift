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
    /// Persisted user ordering for the Up Next section
    /// (playhead-cjqq). `nil` means the user has never reordered this
    /// episode — it inherits the provider's natural ordering and sorts
    /// after every numbered row. The aggregator applies the
    /// `(queuePosition asc, nil-last, episodeId tiebreak)` sort to the
    /// Up Next bucket only; Now / Paused / Recently-Finished ordering
    /// is unaffected.
    let queuePosition: Int?
    /// playhead-btoa.1: optional pipeline-progress fractions plumbed
    /// through to the in-flight row types (Now / Up Next / Paused) so
    /// the Activity screen can render a debug DL/TX/AN strip without
    /// re-querying state from the View. Sibling beads handle provider
    /// population (bd 3) and UI render (bd 4); this slot only carries.
    ///
    /// `downloadFraction`: 0.0...1.0. `nil` when no in-flight or
    /// recorded download exists for this episode this refresh.
    let downloadFraction: Double?
    /// `transcriptFraction`: clamped 0.0...1.0. `nil` when either the
    /// transcript watermark or duration is unknown / <= 0.
    let transcriptFraction: Double?
    /// `analysisFraction`: clamped 0.0...1.0. Same nil rules as
    /// transcriptFraction. Bead 3 (provider population) computes and
    /// clamps; this bead only carries.
    let analysisFraction: Double?

    init(
        episodeId: String,
        episodeTitle: String,
        podcastTitle: String?,
        status: EpisodeSurfaceStatus,
        isRunning: Bool,
        finishedAt: Date?,
        queuePosition: Int? = nil,
        downloadFraction: Double? = nil,
        transcriptFraction: Double? = nil,
        analysisFraction: Double? = nil
    ) {
        self.episodeId = episodeId
        self.episodeTitle = episodeTitle
        self.podcastTitle = podcastTitle
        self.status = status
        self.isRunning = isRunning
        self.finishedAt = finishedAt
        self.queuePosition = queuePosition
        self.downloadFraction = downloadFraction
        self.transcriptFraction = transcriptFraction
        self.analysisFraction = analysisFraction
    }
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
    /// playhead-btoa.1: pipeline-progress fractions for the optional
    /// debug DL/TX/AN strip. `nil` until bead 3 wires the provider; the
    /// View renders the strip only when at least one fraction is
    /// non-nil and the user has the debug toggle enabled (bead 4).
    let downloadFraction: Double?
    let transcriptFraction: Double?
    let analysisFraction: Double?
    var id: String { episodeId }
}

/// "Up Next" row — queued, eligible, not yet running. No SurfaceReason
/// rendering per spec; reorder UI is the View's responsibility.
struct ActivityUpNextRow: Sendable, Hashable, Identifiable {
    let episodeId: String
    let title: String
    let podcastTitle: String?
    /// playhead-btoa.1: pipeline-progress fractions. See
    /// `ActivityNowRow.downloadFraction` for contract.
    let downloadFraction: Double?
    let transcriptFraction: Double?
    let analysisFraction: Double?
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
    /// playhead-btoa.1: pipeline-progress fractions. See
    /// `ActivityNowRow.downloadFraction` for contract.
    let downloadFraction: Double?
    let transcriptFraction: Double?
    let analysisFraction: Double?
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
/// playhead-5nwy: tri-state load indicator the View consults to decide
/// between blank, skeleton, and the populated snapshot. Encoded as an
/// enum (rather than a single Bool) so the View can distinguish "first
/// fetch hasn't started yet" from "first fetch is in flight". The
/// 250ms threshold for promoting `.loading` → `showSkeleton` is
/// applied in the View; this enum carries the truth, not the timing.
enum ActivityLoadState: Sendable, Hashable {
    case idle
    case loading(startedAt: Date)
    case loaded
}

@MainActor
@Observable
final class ActivityViewModel {

    /// Most-recently aggregated snapshot. SwiftUI observes mutations
    /// through `@Observable` and re-renders the four sections.
    private(set) var snapshot: ActivitySnapshot = .empty

    /// playhead-5nwy: load-state signal the View renders the skeleton
    /// off of. Starts in `.idle`; `beginLoad()` flips to `.loading`,
    /// and `refresh(from:)` flips to `.loaded`. Once `.loaded`, a
    /// subsequent `beginLoad()` is a no-op so the View doesn't flicker
    /// back into a skeleton on every refresh tick. Repeat `refresh`
    /// calls just replace the snapshot.
    private(set) var loadState: ActivityLoadState = .idle

    /// Persistence callback invoked by `moveUpNext` to write the new
    /// `(episodeId, queuePosition)` ordering through to the SwiftData
    /// `Episode` rows. The default no-op keeps unit tests and previews
    /// pure (no SwiftData dependency); production wires a closure that
    /// updates the model context so the next refresh observes the new
    /// order (playhead-cjqq).
    ///
    /// Contract:
    ///   * Called with the renumbered ordering of all visible Up Next
    ///     rows after a successful move (`from != to`). Position 0 is
    ///     the top of the list.
    ///   * Episodes whose `queuePosition` was previously `nil` get a
    ///     concrete number on first move; episodes outside the visible
    ///     Up Next bucket are NOT touched (the callback receives only
    ///     the rows that were on screen).
    ///   * **Visibility invariant:** sequential renumber `[0, N)` only
    ///     avoids collisions with off-screen rows because the production
    ///     provider returns the FULL queued set today (no pagination /
    ///     filtering of Up Next). If a future feature pages or filters
    ///     Up Next, this assignment scheme must change (e.g. renumber
    ///     only moved rows, or use sparse indices) — otherwise the
    ///     `[0, N)` block can collide with persisted positions on
    ///     hidden episodes, producing nondeterministic sort.
    ///   * Idempotency: a no-op move (`from == to`) does NOT invoke
    ///     the callback to avoid save churn.
    private let persistQueueOrder: @MainActor ([(episodeId: String, queuePosition: Int)]) -> Void

    init(
        snapshot: ActivitySnapshot = .empty,
        persistQueueOrder: @escaping @MainActor ([(episodeId: String, queuePosition: Int)]) -> Void = { _ in }
    ) {
        self.snapshot = snapshot
        self.persistQueueOrder = persistQueueOrder
    }

    /// Re-aggregate the snapshot from a fresh batch of inputs. Called by
    /// the View on appear and on refresh-notification arrival; called by
    /// SwiftUI Previews to populate fixtures.
    func refresh(from inputs: [ActivityEpisodeInput], now: Date = Date()) {
        snapshot = Self.aggregate(inputs: inputs, now: now)
        loadState = .loaded
    }

    /// playhead-5nwy: signal the start of an inputs fetch. View calls
    /// this immediately before awaiting `inputProvider()` so the load
    /// timer can begin ticking. Idempotent once `.loaded` — repeat
    /// fetches don't reset the View into a skeleton.
    func beginLoad(now: Date = Date()) {
        switch loadState {
        case .idle:
            loadState = .loading(startedAt: now)
        case .loading, .loaded:
            break
        }
    }

    /// Apply a user drag-to-reorder gesture to the Up Next section. The
    /// View's `List { … }.onMove { src, dst in vm.moveUpNext(...) }`
    /// handler funnels through here so the snapshot's `upNext` ordering
    /// is the single source of truth the view re-renders against.
    ///
    /// Persistence (playhead-cjqq): after the in-memory reorder, the
    /// VM renumbers every visible Up Next row sequentially (0, 1, 2, …)
    /// and calls `persistQueueOrder` so the new ordering is written
    /// through to the SwiftData `Episode.queuePosition` column. The
    /// next `ActivityRefreshNotification` post then re-aggregates from
    /// persistence and the user's drag survives. Sequential renumber
    /// (vs. sparse / fractional indices) keeps the Int domain bounded
    /// no matter how many drags occur, and is trivially deterministic
    /// for the sort comparator's tiebreak.
    ///
    /// Idempotency: a no-op move (`from == to` for a single index, or
    /// `move(fromOffsets:toOffset:)` returning the same sequence) does
    /// NOT invoke `persistQueueOrder` — the snapshot is left unchanged
    /// and no save is requested. This avoids spurious SwiftData saves
    /// when SwiftUI re-emits a no-op `.onMove` callback (e.g. on a
    /// drag that ends at its origin).
    func moveUpNext(from source: IndexSet, to destination: Int) {
        var reordered = snapshot.upNext
        reordered.move(fromOffsets: source, toOffset: destination)

        // Idempotency guard: if the move did not change the ordering,
        // skip both the snapshot replacement and the persistence
        // callback. Compare by episodeId because `ActivityUpNextRow` is
        // a value type and array equality already implies same order,
        // but explicit ID comparison reads more clearly at the call
        // site.
        let beforeIds = snapshot.upNext.map(\.episodeId)
        let afterIds = reordered.map(\.episodeId)
        guard beforeIds != afterIds else { return }

        snapshot = ActivitySnapshot(
            now: snapshot.now,
            upNext: reordered,
            paused: snapshot.paused,
            recentlyFinished: snapshot.recentlyFinished
        )

        // Renumber sequentially so the persisted queuePosition column
        // matches the new on-screen ordering, then write through.
        let renumbered = reordered.enumerated().map { index, row in
            (episodeId: row.episodeId, queuePosition: index)
        }
        persistQueueOrder(renumbered)
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
    /// - Now / Paused: input order preserved (the production
    ///   provider hands rows in scheduler-priority order).
    /// - Up Next (playhead-cjqq): sort by `queuePosition` ascending,
    ///   with `nil` last, then deterministic tiebreak by `episodeId`.
    ///   This gives the user's persisted drag-reorder priority over
    ///   scheduler-derived ordering, while episodes that have never
    ///   been reordered (`queuePosition == nil`) keep their relative
    ///   provider-order at the tail. Drag-to-reorder ships via
    ///   `moveUpNext(from:to:)` which both mutates the in-memory
    ///   snapshot AND writes the new sequence through to the
    ///   `persistQueueOrder` callback so the next refresh observes
    ///   the new order.
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
        // Track Up Next rows alongside their (queuePosition, episodeId)
        // sort keys so the final sort can apply the (queuePosition asc,
        // nil-last, episodeId tiebreak) rule without re-querying the
        // input list.
        var upNextRowsWithKeys: [(row: ActivityUpNextRow, queuePosition: Int?, episodeId: String)] = []
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
                        hint: input.status.hint,
                        downloadFraction: input.downloadFraction,
                        transcriptFraction: input.transcriptFraction,
                        analysisFraction: input.analysisFraction
                    )
                )

            case .queued:
                if input.isRunning {
                    nowRows.append(
                        ActivityNowRow(
                            episodeId: input.episodeId,
                            title: input.episodeTitle,
                            podcastTitle: input.podcastTitle,
                            progressPhrase: progressPhrase(for: input.status),
                            downloadFraction: input.downloadFraction,
                            transcriptFraction: input.transcriptFraction,
                            analysisFraction: input.analysisFraction
                        )
                    )
                } else {
                    let row = ActivityUpNextRow(
                        episodeId: input.episodeId,
                        title: input.episodeTitle,
                        podcastTitle: input.podcastTitle,
                        downloadFraction: input.downloadFraction,
                        transcriptFraction: input.transcriptFraction,
                        analysisFraction: input.analysisFraction
                    )
                    upNextRowsWithKeys.append((
                        row: row,
                        queuePosition: input.queuePosition,
                        episodeId: input.episodeId
                    ))
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

        // Up Next sort (playhead-cjqq): queuePosition asc, nil-last,
        // deterministic episodeId tiebreak. Stable across calls so a
        // refresh after a drag-reorder produces the same visual order
        // the user just constructed.
        upNextRowsWithKeys.sort { lhs, rhs in
            switch (lhs.queuePosition, rhs.queuePosition) {
            case let (l?, r?):
                if l != r { return l < r }
                return lhs.episodeId < rhs.episodeId
            case (_?, nil):
                return true
            case (nil, _?):
                return false
            case (nil, nil):
                return lhs.episodeId < rhs.episodeId
            }
        }
        let upNextRows = upNextRowsWithKeys.map(\.row)

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
