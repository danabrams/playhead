// SurfaceStateTransitionEntry.swift
// JSON Lines schema for the surface-status state-transition audit log.
//
// Scope: playhead-ol05 (Phase 1.5 — "State-transition audit + impossible-
// state assertions + cross-target contract test"). Extended by playhead-
// o45p to carry two additional non-violation event kinds used by the
// Wave 4 dogfood false_ready_rate metric.
//
// This is the SCHEMA OWNED BY THIS BEAD — consumed by playhead-e2a3
// (the 10-day dogfood audit). Schema stability matters:
//
//   * Field names use snake_case to match the rest of the diagnostics
//     surface area (see playhead-ghon's scheduler_events log).
//   * Optional fields are emitted only when present (`encodeIfPresent`)
//     to keep per-line payloads minimal — a multi-day session's audit
//     file is read by hand during dogfood reviews.
//   * Date encoding is ISO-8601 with the `iso8601` strategy on the
//     enclosing JSONEncoder. Callers MUST set
//     `encoder.dateEncodingStrategy = .iso8601` before encoding (the
//     in-tree logger does this).
//
// Schema:
//   {
//     "timestamp": "2026-04-19T12:34:56Z",
//     "session_id": "<UUID>",
//     "episode_id_hash": "<hex string>" | null,
//     "prior_disposition": "queued" | null,
//     "new_disposition": "paused",
//     "prior_reason": "waiting_for_time" | null,
//     "new_reason": "phone_is_hot",
//     "cause": "thermal" | null,
//     "eligibility_snapshot": { five booleans + capturedAt } | null,
//     "invariant_violation": { code, description } | null,
//     "event_type": "invariant_violation" | "ready_entered" | "auto_skip_fired",
//     "entry_trigger": "cold_start" | "analysis_completed" | "unblocked" | "other" | null,
//     "window_start_ms": <integer> | null,
//     "window_end_ms": <integer> | null
//   }
//
// Backward compatibility (playhead-o45p):
//   * `event_type` is OPTIONAL on decode and defaults to `invariant_violation`,
//     so JSON Lines files produced by the pre-o45p logger still decode.
//   * `event_type` values the decoder does not recognize fall back to
//     `invariant_violation` — downstream consumers that read JSONL with a
//     permissive decoder can keep parsing even as new event kinds are added.
//   * `entry_trigger`, `window_start_ms`, `window_end_ms` are encoded only
//     when non-nil (`encodeIfPresent`). A `ready_entered` entry with no
//     `entry_trigger` context still round-trips.

import Foundation

// MARK: - SurfaceStateTransitionEventType

/// Discriminator for what kind of event a `SurfaceStateTransitionEntry`
/// represents. Pre-o45p entries (the only kind produced by the Phase 1.5
/// ol05 logger) correspond to `.invariantViolation` — and that is the
/// default on decode when the field is absent, keeping byte-identical
/// backward compatibility with existing JSONL session files.
///
/// Added by playhead-o45p so the e2a3 dogfood audit can compute
/// `false_ready_rate = readyEntered \ autoSkipFired / readyEntered`.
enum SurfaceStateTransitionEventType: String, Sendable, Equatable, Codable, CaseIterable {
    /// An impossible-state entry. This is the original ol05 contract —
    /// entries were anomaly-only before o45p, and continue to be the
    /// default when `event_type` is absent from a decoded line.
    case invariantViolation = "invariant_violation"

    /// `EpisodeSurfaceStatus` entered a ready-for-playback disposition
    /// (queued + no blocking cause). Emitted by the reducer's consumer.
    /// Numerator/denominator of the Wave 4 false_ready_rate metric.
    case readyEntered = "ready_entered"

    /// `SkipOrchestrator` fired an auto-skip at playhead-time. Pairs with
    /// a prior `readyEntered` event on the same `episode_id_hash` to
    /// answer "did auto-skip actually fire for this ready cell?"
    case autoSkipFired = "auto_skip_fired"
}

// MARK: - SurfaceStateTransitionEntryTrigger

/// Context the reducer's consumer supplies alongside a `readyEntered`
/// event so the audit can distinguish cold-start ready transitions from
/// "analysis just completed" ones. `nil` when the consumer does not know.
enum SurfaceStateTransitionEntryTrigger: String, Sendable, Equatable, Codable, CaseIterable {
    /// First reduction after a fresh process launch — no prior state.
    case coldStart = "cold_start"

    /// Analysis finished successfully and the episode moved into queued.
    case analysisCompleted = "analysis_completed"

    /// A previously blocking cause (thermal / network / etc.) cleared and
    /// the episode is now unblocked and queued.
    case unblocked = "unblocked"

    /// Any other ready transition — the consumer has no finer signal.
    case other = "other"
}

// MARK: - SurfaceStateTransitionEntry

/// One JSON Lines entry written by `SurfaceStatusInvariantLogger`.
/// Carries either a real disposition transition (when `priorDisposition`
/// is non-nil) or a freshly-emitted state (when it is nil), plus an
/// optional `invariantViolation` payload populated by the validator.
///
/// After playhead-o45p the entry additionally discriminates on
/// `eventType` — pre-o45p entries default to `.invariantViolation` on
/// decode, so existing JSONL fixtures keep decoding without modification.
struct SurfaceStateTransitionEntry: Sendable, Equatable, Codable {

    let timestamp: Date
    let sessionId: UUID
    let episodeIdHash: String?
    let priorDisposition: SurfaceDisposition?
    let newDisposition: SurfaceDisposition
    let priorReason: SurfaceReason?
    let newReason: SurfaceReason
    let cause: InternalMissCause?
    let eligibilitySnapshot: AnalysisEligibility?
    let invariantViolation: InvariantViolation?

    /// Discriminator added by playhead-o45p. Defaults to
    /// `.invariantViolation` so entries produced before the field was
    /// introduced continue to decode with their original semantics.
    let eventType: SurfaceStateTransitionEventType

    /// Present only on `.readyEntered` events. `nil` when the consumer
    /// did not supply a trigger or the event is not a ready transition.
    let entryTrigger: SurfaceStateTransitionEntryTrigger?

    /// Present only on `.autoSkipFired` events. Milliseconds from the
    /// episode start (integer) of the skipped ad-window start / end.
    let windowStartMs: Int?
    let windowEndMs: Int?

    init(
        timestamp: Date,
        sessionId: UUID,
        episodeIdHash: String?,
        priorDisposition: SurfaceDisposition?,
        newDisposition: SurfaceDisposition,
        priorReason: SurfaceReason?,
        newReason: SurfaceReason,
        cause: InternalMissCause?,
        eligibilitySnapshot: AnalysisEligibility?,
        invariantViolation: InvariantViolation?,
        eventType: SurfaceStateTransitionEventType = .invariantViolation,
        entryTrigger: SurfaceStateTransitionEntryTrigger? = nil,
        windowStartMs: Int? = nil,
        windowEndMs: Int? = nil
    ) {
        self.timestamp = timestamp
        self.sessionId = sessionId
        self.episodeIdHash = episodeIdHash
        self.priorDisposition = priorDisposition
        self.newDisposition = newDisposition
        self.priorReason = priorReason
        self.newReason = newReason
        self.cause = cause
        self.eligibilitySnapshot = eligibilitySnapshot
        self.invariantViolation = invariantViolation
        self.eventType = eventType
        self.entryTrigger = entryTrigger
        self.windowStartMs = windowStartMs
        self.windowEndMs = windowEndMs
    }

    // MARK: - Codable (snake_case, optional fields elided when nil)

    enum CodingKeys: String, CodingKey {
        case timestamp
        case sessionId = "session_id"
        case episodeIdHash = "episode_id_hash"
        case priorDisposition = "prior_disposition"
        case newDisposition = "new_disposition"
        case priorReason = "prior_reason"
        case newReason = "new_reason"
        case cause
        case eligibilitySnapshot = "eligibility_snapshot"
        case invariantViolation = "invariant_violation"
        case eventType = "event_type"
        case entryTrigger = "entry_trigger"
        case windowStartMs = "window_start_ms"
        case windowEndMs = "window_end_ms"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.timestamp = try container.decode(Date.self, forKey: .timestamp)
        self.sessionId = try container.decode(UUID.self, forKey: .sessionId)
        self.episodeIdHash = try container.decodeIfPresent(String.self, forKey: .episodeIdHash)
        self.priorDisposition = try container.decodeIfPresent(SurfaceDisposition.self, forKey: .priorDisposition)
        self.newDisposition = try container.decode(SurfaceDisposition.self, forKey: .newDisposition)
        self.priorReason = try container.decodeIfPresent(SurfaceReason.self, forKey: .priorReason)
        self.newReason = try container.decode(SurfaceReason.self, forKey: .newReason)
        self.cause = try container.decodeIfPresent(InternalMissCause.self, forKey: .cause)
        self.eligibilitySnapshot = try container.decodeIfPresent(AnalysisEligibility.self, forKey: .eligibilitySnapshot)
        self.invariantViolation = try container.decodeIfPresent(InvariantViolation.self, forKey: .invariantViolation)

        // Backward compatibility: pre-o45p lines have no `event_type` field.
        // Treat them as invariant_violation entries (their original contract).
        // Forward compatibility: unknown `event_type` values decode to the
        // same default so downstream audit consumers skip past unknowns
        // without throwing.
        if let rawEventType = try container.decodeIfPresent(String.self, forKey: .eventType) {
            self.eventType = SurfaceStateTransitionEventType(rawValue: rawEventType) ?? .invariantViolation
        } else {
            self.eventType = .invariantViolation
        }
        self.entryTrigger = try container.decodeIfPresent(SurfaceStateTransitionEntryTrigger.self, forKey: .entryTrigger)
        self.windowStartMs = try container.decodeIfPresent(Int.self, forKey: .windowStartMs)
        self.windowEndMs = try container.decodeIfPresent(Int.self, forKey: .windowEndMs)
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(timestamp, forKey: .timestamp)
        try container.encode(sessionId, forKey: .sessionId)
        try container.encodeIfPresent(episodeIdHash, forKey: .episodeIdHash)
        try container.encodeIfPresent(priorDisposition, forKey: .priorDisposition)
        try container.encode(newDisposition, forKey: .newDisposition)
        try container.encodeIfPresent(priorReason, forKey: .priorReason)
        try container.encode(newReason, forKey: .newReason)
        try container.encodeIfPresent(cause, forKey: .cause)
        try container.encodeIfPresent(eligibilitySnapshot, forKey: .eligibilitySnapshot)
        try container.encodeIfPresent(invariantViolation, forKey: .invariantViolation)
        try container.encode(eventType, forKey: .eventType)
        try container.encodeIfPresent(entryTrigger, forKey: .entryTrigger)
        try container.encodeIfPresent(windowStartMs, forKey: .windowStartMs)
        try container.encodeIfPresent(windowEndMs, forKey: .windowEndMs)
    }
}

// MARK: - SurfaceStateTransitionContext

/// Caller-supplied non-violation fields for the
/// `SurfaceStatusInvariantLogger.recordViolations(_:context:)` helper.
/// Keeps the call-site signature tractable while still carrying every
/// field the JSON Lines schema needs.
struct SurfaceStateTransitionContext: Sendable, Equatable {
    let timestamp: Date
    let episodeIdHash: String?
    let priorDisposition: SurfaceDisposition?
    let newDisposition: SurfaceDisposition
    let priorReason: SurfaceReason?
    let newReason: SurfaceReason
    let cause: InternalMissCause?
    let eligibilitySnapshot: AnalysisEligibility?

    init(
        timestamp: Date = Date(),
        episodeIdHash: String?,
        priorDisposition: SurfaceDisposition?,
        newDisposition: SurfaceDisposition,
        priorReason: SurfaceReason?,
        newReason: SurfaceReason,
        cause: InternalMissCause?,
        eligibilitySnapshot: AnalysisEligibility?
    ) {
        self.timestamp = timestamp
        self.episodeIdHash = episodeIdHash
        self.priorDisposition = priorDisposition
        self.newDisposition = newDisposition
        self.priorReason = priorReason
        self.newReason = newReason
        self.cause = cause
        self.eligibilitySnapshot = eligibilitySnapshot
    }
}
