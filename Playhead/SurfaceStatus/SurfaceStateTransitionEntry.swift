// SurfaceStateTransitionEntry.swift
// JSON Lines schema for the surface-status state-transition audit log.
//
// Scope: playhead-ol05 (Phase 1.5 — "State-transition audit + impossible-
// state assertions + cross-target contract test").
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
//     "invariant_violation": { code, description } | null
//   }

import Foundation

// MARK: - SurfaceStateTransitionEntry

/// One JSON Lines entry written by `SurfaceStatusInvariantLogger`.
/// Carries either a real disposition transition (when `priorDisposition`
/// is non-nil) or a freshly-emitted state (when it is nil), plus an
/// optional `invariantViolation` payload populated by the validator.
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
        invariantViolation: InvariantViolation?
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
