// AnalysisState.swift
// Value object consumed by `episodeSurfaceStatus(...)`. This is the
// reducer's stable projection of the persisted episode analysis metadata;
// it is NOT the SwiftData / SQLite record itself.
//
// Scope: playhead-5bb3 (Phase 1.5 deliverable 1).
//
// Why a hand-rolled value object instead of plumbing the AnalysisStore
// row directly? Three reasons:
//   1. The SQLite `analysis_assets` row carries append-heavy fingerprints
//      and byte budgets the reducer does not need.
//   2. Holding AnalysisStore at arm's length keeps the reducer pure and
//      trivially testable — a snapshot test produces a golden JSON from a
//      hand-built `AnalysisState` without spinning up SQLite.
//   3. The lint contract (see `SurfaceStatusUILintTests`) forbids any UI
//      file from referencing `AnalysisStore` / `AnalysisSummary` /
//      `InternalMissCause`. The reducer is the single aggregation point;
//      UI consumers work with `EpisodeSurfaceStatus` and never need to
//      see the underlying store.
//
// The five fields below are the minimum the precedence ladder needs.
// Downstream Phase 2 beads (playhead-cthe) will likely extend this as
// `CoverageSummary` persistence lands.

import Foundation

// MARK: - AnalysisState

/// Stable value-object view of the persisted episode analysis metadata
/// that the `episodeSurfaceStatus(...)` reducer reads. Every field here
/// is a read-only snapshot taken at reducer-invocation time; the reducer
/// never mutates its input.
///
/// Parameter sources:
/// - ``persistedStatus`` is derived from `AnalysisAsset.analysisState`
///   (the `TEXT` column backed by the `queued / running / done / failed`
///   state machine). We keep it as an enum rather than a raw string so
///   the reducer's switch is exhaustive at compile time.
/// - ``hasUserPreemptedJob`` and ``hasAppForceQuitFlag`` are derived from
///   the `work_journal` cause history: they are `true` when the most-
///   recent terminal cause matches the corresponding user-initiated
///   `InternalMissCause`. Callers compute these outside the reducer so
///   the reducer's input is a pure snapshot.
/// - ``pendingSinceEnqueuedAt`` is the timestamp the current job entered
///   the queue; the reducer uses it to distinguish a fresh "queued" from
///   a "queued-but-waiting-on-resource" state once resource signals land
///   in Phase 2.
/// - ``hasAnyConfirmedAnalysis`` tells the reducer whether there is any
///   prior analysis output at all (used by Phase 2 to decide between
///   "unavailable with prior coverage" vs "unavailable with nothing").
struct AnalysisState: Sendable, Hashable, Codable {

    /// The coarse lifecycle state persisted to SQLite. Mirrors the
    /// enumerated string values the `analysis_assets.analysisState`
    /// column accepts; keep the raw-value strings in sync with the
    /// AnalysisStore migrations. Note that the column has a `DEFAULT
    /// 'new'` fallback — the reducer treats the `.new` case as
    /// equivalent to `.queued` for surfacing purposes.
    enum PersistedStatus: String, Sendable, Hashable, Codable, CaseIterable {
        case new
        case queued
        case running
        case done
        case failed
        case cancelled
    }

    let persistedStatus: PersistedStatus

    /// `true` when the most-recent terminal cause for this episode was
    /// `InternalMissCause.userPreempted`. Derived outside the reducer so
    /// the reducer stays pure.
    let hasUserPreemptedJob: Bool

    /// `true` when the episode's work journal has an
    /// `InternalMissCause.appForceQuitRequiresRelaunch` terminal cause
    /// not yet resolved by a subsequent `running`/`done` entry.
    let hasAppForceQuitFlag: Bool

    /// When the current pending entry was enqueued (nil if not pending).
    /// Phase 2 (playhead-cthe) will use this to distinguish a fresh queue
    /// admission from a long wait.
    let pendingSinceEnqueuedAt: Date?

    /// `true` if the episode has any previously-confirmed analysis
    /// output (e.g. at least one `confirmed_ad_coverage_end_time > 0`).
    let hasAnyConfirmedAnalysis: Bool

    init(
        persistedStatus: PersistedStatus,
        hasUserPreemptedJob: Bool,
        hasAppForceQuitFlag: Bool,
        pendingSinceEnqueuedAt: Date?,
        hasAnyConfirmedAnalysis: Bool
    ) {
        self.persistedStatus = persistedStatus
        self.hasUserPreemptedJob = hasUserPreemptedJob
        self.hasAppForceQuitFlag = hasAppForceQuitFlag
        self.pendingSinceEnqueuedAt = pendingSinceEnqueuedAt
        self.hasAnyConfirmedAnalysis = hasAnyConfirmedAnalysis
    }
}
