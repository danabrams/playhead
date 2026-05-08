// ChapterPlanReadyEvent.swift
// playhead-au2v.1.12: Informational event fired exactly once per
// successful chapter-plan write by `ChapterGenerationPhase`.
//
// Why a separate event (not piggy-backed on `ChapterPhaseEvent.completed`):
//   * `ChapterPhaseEvent.completed` is a diagnostics-stream event; its
//     consumers are the support-engineer telemetry surface (privacy-
//     locked: hashed episode id, snake_case payload, no titles, etc.).
//   * `ChapterPlanReadyEvent` is the in-process coordination signal a
//     future async consumer (e.g., a coverage-plan refresh worker that
//     wants to re-plan after the chapter signal lands) will subscribe
//     to. It carries the *plain* `episodeContentHash` because that's the
//     cache key the consumer uses to fetch the plan it was just told
//     about. There is no PII contract on this event — it is not a
//     diagnostics-bundle wire event.
//   * The two streams have different lifetimes, different sinks, and
//     different schemas. Folding the in-process coordination into the
//     diagnostics wire would force every future consumer to subscribe
//     to a privacy-locked stream, which is wrong on multiple axes.
//
// Bead 12 ships this type and a sink seam consistent with
// `ChapterPhaseEventSink`. Real subscribers (a coverage-plan refresh
// worker, dual-FM coordination, etc.) are deferred — bead 12's job is
// to fire the event reliably and exactly once on success, never on any
// failure / abort path.
//
// Emission contract (locked by `ChapterGenerationPhase` tests in bead 12):
//   * Fired exactly once after a successful plan write.
//   * NEVER fired on:
//       - mode == .off short-circuit
//       - admission denial
//       - transcript unavailable
//       - empty candidates
//       - boundary detector failure
//       - cancellation / preemption
//       - transcript-snapshot race abort
//       - plan-assembly operational-rate abort
//   * The plan-write itself can fail at the persistence layer; in that
//     case the event still does NOT fire (we promise the consumer that
//     a `ChapterPlanReady` means a fresh plan is observable in the
//     `ChapterPlanCache` for that content hash).

import Foundation

// MARK: - ChapterPlanReadyEvent

/// In-process coordination event published by `ChapterGenerationPhase`
/// after a successful plan write. Carries enough identity for a
/// subscriber to fetch the freshly-cached plan from `ChapterPlanCache`
/// without a recompute.
///
/// All fields are value-semantic and `Sendable`. Subscribers are free to
/// store the whole struct or just the `episodeContentHash`.
///
/// Privacy: `episodeContentHash` is a content hash — it is NOT a user-
/// identifiable id. No raw episode id, transcript, or title appears
/// anywhere in this struct. The event is in-process only; if a future
/// bead persists or ships it across a process boundary, that bead is
/// responsible for re-evaluating the privacy posture (the diagnostics-
/// stream `ChapterPhaseEvent.completed` is the right vehicle for any
/// off-device surface).
struct ChapterPlanReadyEvent: Sendable, Hashable, Equatable {

    /// Stable identity of the analyzed asset. Identical to the cache
    /// key the subscriber would use to call `ChapterPlanCache.get(...)`.
    let episodeContentHash: String

    /// Duration-weighted plan confidence in `[0, 1]`. Matches
    /// `ChapterPlan.planConfidence` for the plan that was just written.
    let planConfidence: Double

    /// Number of chapters in the assembled plan. Matches
    /// `ChapterPlan.chapters.count`.
    let chapterCount: Int

    /// Wall-clock instant the plan was assembled. Matches
    /// `ChapterPlan.generatedAt`.
    let generatedAt: Date
}

// MARK: - ChapterPlanReadyEventSink

/// Pluggable sink for `ChapterPlanReadyEvent`s. Production wiring
/// (deferred to bead .13 / a later coordination bead) will adapt this
/// to whatever in-process broadcast mechanism the consuming workers
/// use; tests inject an in-memory recorder.
///
/// Implementations must be safe to call from any task / actor context.
/// `ChapterGenerationPhase` awaits each `record` so emit ordering
/// matches phase progress.
protocol ChapterPlanReadyEventSink: Sendable {
    func record(_ event: ChapterPlanReadyEvent) async
}

// MARK: - NoopChapterPlanReadyEventSink

/// Default no-op sink used when the phase is constructed without a
/// real coordination consumer. Discards every event without buffering.
/// Bead 12 wires the phase to take an injected sink with this default,
/// so the existing test fixtures keep compiling and the phase still
/// "fires the event exactly once on success" — there's just nobody
/// listening yet.
struct NoopChapterPlanReadyEventSink: ChapterPlanReadyEventSink {
    init() {}
    func record(_ event: ChapterPlanReadyEvent) async {}
}
