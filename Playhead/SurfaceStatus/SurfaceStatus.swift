// SurfaceStatus.swift
// The three sibling status structs produced by the SurfaceStatus reducers.
// Each struct represents the UI-facing projection of one class of
// schedulable work:
//
//   - `EpisodeSurfaceStatus` — per-episode readiness state (Library /
//     Detail / Activity rows / widgets / App Intents). Produced by
//     `episodeSurfaceStatus(...)` (see `EpisodeSurfaceStatusReducer.swift`).
//     This is the struct this bead (playhead-5bb3) focuses on.
//
//   - `BatchSurfaceStatus` — "Download-Next-N" batch state. Produced by
//     a `batchSurfaceStatus(...)` reducer whose full behavior is a later
//     Phase 2 bead's scope. For Phase 1.5 we ship the struct shape only
//     so that sibling modules can reference the type without waiting on
//     the reducer logic.
//
//   - `TaskSurfaceStatus` — BGContinuedProcessingTask Live Activity
//     state. Produced by a `taskSurfaceStatus(...)` reducer whose full
//     behavior is, similarly, Phase 2 scope. Same struct-only reasoning.
//
// Shape rationale: all three carry `(disposition, reason, hint)` because
// those three fields are the taxonomy CauseAttributionPolicy already
// produces. Per-struct fields layer on top of that shared base.

import Foundation

// MARK: - EpisodeSurfaceStatus

/// UI-facing projection of an episode's readiness state. This is the
/// single type consumed by every episode-centric surface (Library row,
/// Episode detail, Activity screen, Live Activity, App Intent responses,
/// widgets). Produced exclusively by
/// `episodeSurfaceStatus(state:cause:eligibility:coverage:readinessAnchor:)`.
///
/// The shape intentionally mirrors the four-layer taxonomy from
/// playhead-v11 so that consumers can route on `disposition` and render
/// copy keyed on `reason`. The additional fields carry Phase 2 signals
/// (`playbackReadiness`, `readinessAnchor`) and the per-device
/// unavailability reason (`analysisUnavailableReason`).
///
/// `Codable` is implemented so the snapshot test suite can render golden
/// JSON fixtures without a bespoke encoder. `analysisUnavailableReason`
/// is encoded under the snake-cased key `analysis_unavailable_reason`
/// and is present only when the reducer produced a non-nil reason (i.e.
/// the eligibility-blocks rule fired).
struct EpisodeSurfaceStatus: Sendable, Hashable, Codable {

    /// Which UI state bucket this episode falls into. Drives row/tile
    /// appearance (queued → spinner, paused → pill, unavailable → dim,
    /// failed → retry affordance, cancelled → minimal).
    let disposition: SurfaceDisposition

    /// Copy-stable reason bucket. UI copy is keyed on `reason.rawValue`
    /// so localization can vary the string without changing the reducer.
    let reason: SurfaceReason

    /// Actionable resolution hint. `hint.userFixable == false` means the
    /// UI must NOT render a call-to-action.
    let hint: ResolutionHint

    /// Per-device unavailability reason, populated only when
    /// `disposition == .unavailable`. `nil` in every other case.
    ///
    /// Populated by the reducer via
    /// `AnalysisUnavailableReason.derive(from:)` (see playhead-sueq).
    /// The eligibility-blocks rule (Rule 1) short-circuits to the
    /// `.unavailable` disposition and carries the derived reason; every
    /// other rule emits `nil`. The runtime invariant
    /// "analysisUnavailableReason non-nil ⇔ disposition == .unavailable"
    /// is wired separately in playhead-ol05.
    let analysisUnavailableReason: AnalysisUnavailableReason?

    /// Playback-readiness signal for the current readiness anchor.
    /// Phase 1.5: always `.none` because `CoverageSummary` is a stub;
    /// Phase 2 (playhead-cthe) will populate this with `.partial` / `.ready`.
    let playbackReadiness: PlaybackReadiness

    /// Pass-through of the readiness anchor argument to the reducer.
    /// Consumers that render a scrubber overlay (Activity screen) use
    /// this to draw the "analyzed up to here" marker. Optional because
    /// not every reducer invocation has an anchor (e.g. a fresh queued
    /// episode with no coverage at all).
    let readinessAnchor: TimeInterval?

    init(
        disposition: SurfaceDisposition,
        reason: SurfaceReason,
        hint: ResolutionHint,
        analysisUnavailableReason: AnalysisUnavailableReason?,
        playbackReadiness: PlaybackReadiness,
        readinessAnchor: TimeInterval?
    ) {
        self.disposition = disposition
        self.reason = reason
        self.hint = hint
        self.analysisUnavailableReason = analysisUnavailableReason
        self.playbackReadiness = playbackReadiness
        self.readinessAnchor = readinessAnchor
    }

    // MARK: - Codable
    //
    // Hand-rolled so `analysisUnavailableReason` is emitted under the
    // snake-cased key `analysis_unavailable_reason` and only when the
    // reducer produced a non-nil value. `readinessAnchor` uses
    // `encodeIfPresent` on the same grounds — the nil state is the
    // common case and the key is elided to keep fixtures minimal.

    enum CodingKeys: String, CodingKey {
        case disposition
        case reason
        case hint
        case analysisUnavailableReason = "analysis_unavailable_reason"
        case playbackReadiness
        case readinessAnchor
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.disposition = try container.decode(SurfaceDisposition.self, forKey: .disposition)
        self.reason = try container.decode(SurfaceReason.self, forKey: .reason)
        self.hint = try container.decode(ResolutionHint.self, forKey: .hint)
        self.analysisUnavailableReason = try container.decodeIfPresent(
            AnalysisUnavailableReason.self,
            forKey: .analysisUnavailableReason
        )
        self.playbackReadiness = try container.decode(PlaybackReadiness.self, forKey: .playbackReadiness)
        self.readinessAnchor = try container.decodeIfPresent(TimeInterval.self, forKey: .readinessAnchor)
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(disposition, forKey: .disposition)
        try container.encode(reason, forKey: .reason)
        try container.encode(hint, forKey: .hint)
        try container.encodeIfPresent(analysisUnavailableReason, forKey: .analysisUnavailableReason)
        try container.encode(playbackReadiness, forKey: .playbackReadiness)
        try container.encodeIfPresent(readinessAnchor, forKey: .readinessAnchor)
    }
}

// MARK: - BatchSurfaceStatus

/// UI-facing projection of a "Download-Next-N" batch. Produced by a
/// `batchSurfaceStatus(...)` reducer whose implementation is scope for
/// a later Phase 2 bead. This bead (playhead-5bb3) ships the struct
/// shape only so sibling code can reference the type.
///
/// Fields intentionally mirror `EpisodeSurfaceStatus` for the three
/// taxonomy columns plus a batch-specific progress pair. A batch does
/// not carry `playbackReadiness` (it is a fleet of episodes, not a
/// single playback target) and does not carry an `analysisUnavailableReason`
/// (the user-level unavailability is derived per-episode, not per-batch).
struct BatchSurfaceStatus: Sendable, Hashable, Codable {

    /// Batch-level disposition. A batch is `.queued` while at least one
    /// episode is still pending, `.paused` when every pending episode
    /// shares a transient-wait cause, and so on — exact precedence is
    /// the Phase 2 bead's scope.
    let disposition: SurfaceDisposition

    /// Dominant reason across the batch. The reducer picks a single
    /// reason even when the constituent episodes have different ones;
    /// Phase 2 will define the aggregation rule.
    let reason: SurfaceReason

    /// Dominant hint. Phase 2 will define the aggregation rule.
    let hint: ResolutionHint

    /// Count of episodes in the batch that have reached a terminal-done
    /// state. Phase 2 defines whether "done" includes `.cancelled`.
    let completedCount: Int

    /// Total episodes originally admitted to the batch.
    let totalCount: Int

    init(
        disposition: SurfaceDisposition,
        reason: SurfaceReason,
        hint: ResolutionHint,
        completedCount: Int,
        totalCount: Int
    ) {
        self.disposition = disposition
        self.reason = reason
        self.hint = hint
        self.completedCount = completedCount
        self.totalCount = totalCount
    }
}

// MARK: - TaskSurfaceStatus

/// UI-facing projection of a BGContinuedProcessingTask Live Activity.
/// Produced by a `taskSurfaceStatus(...)` reducer whose implementation
/// is scope for a later Phase 2 bead. This bead ships the struct shape
/// only.
///
/// A continued-processing task is a fleet of one-or-more analysis jobs
/// scheduled under a single BG task grant. The Live Activity surfaces
/// the dominant disposition/reason plus a progress pair and the task's
/// expected wall-clock expiry (so the Live Activity can render a
/// countdown when the grant is about to run out).
struct TaskSurfaceStatus: Sendable, Hashable, Codable {

    /// Task-level disposition. Follows the same taxonomy as
    /// `EpisodeSurfaceStatus.disposition` with Phase 2 defining the
    /// per-task aggregation rule.
    let disposition: SurfaceDisposition

    /// Dominant reason across the task's jobs.
    let reason: SurfaceReason

    /// Dominant hint.
    let hint: ResolutionHint

    /// Count of jobs within the task that have reached a terminal-done
    /// state.
    let processedCount: Int

    /// Total jobs the task was scheduled to run.
    let totalCount: Int

    /// Wall-clock deadline for the task grant; `nil` when no grant is
    /// currently active. The Live Activity UI uses this to render a
    /// countdown when the grant is near exhaustion.
    let grantExpiresAt: Date?

    init(
        disposition: SurfaceDisposition,
        reason: SurfaceReason,
        hint: ResolutionHint,
        processedCount: Int,
        totalCount: Int,
        grantExpiresAt: Date?
    ) {
        self.disposition = disposition
        self.reason = reason
        self.hint = hint
        self.processedCount = processedCount
        self.totalCount = totalCount
        self.grantExpiresAt = grantExpiresAt
    }
}
