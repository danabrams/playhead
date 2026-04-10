// AdDecisionResult.swift
// Phase 6 (playhead-4my.6.3): Models for persisted decisions and append-only events.
// Phase 7 (playhead-4my.7.1): CorrectionSource enum + expanded CorrectionEvent.
//
// DecisionResultArtifact — per-asset persistence container (ad_decision_results table).
//   The runtime per-window view (AdDecisionResult in SkipOrchestrator.swift) is derived
//   by deserializing decisionJSON from this artifact.
// DecisionEvent, CorrectionEvent — append-only audit trails.

import Foundation

// MARK: - DecisionResultArtifact

/// Per-asset persistence container for fusion decisions (ad_decision_results table).
/// decisionJSON encodes the full array of per-window AdDecisionResult values.
struct DecisionResultArtifact: Sendable, Identifiable, Equatable {
    let id: String
    let analysisAssetId: String
    let decisionCohortJSON: String  // serialized DecisionCohort
    let inputArtifactRefs: String   // JSON array of input artifact refs
    let decisionJSON: String        // JSON: array of per-window decisions + gates
    let createdAt: Double
}

// MARK: - DecisionEvent

/// Append-only record of a single window-level decision.
struct DecisionEvent: Sendable, Identifiable, Equatable {
    let id: String
    let analysisAssetId: String
    let eventType: String
    let windowId: String
    let proposalConfidence: Double
    let skipConfidence: Double
    let eligibilityGate: String    // SkipEligibilityGate.rawValue
    let policyAction: String       // SkipPolicyAction.rawValue
    let decisionCohortJSON: String
    let createdAt: Double
}

// MARK: - CorrectionSource

/// The UI gesture or mechanism that produced a user correction.
enum CorrectionSource: String, Sendable, Codable {
    /// User tapped "Listen" on a span that was auto-skipped, reverting the skip.
    case listenRevert
    /// User explicitly vetoed a span via "This isn't an ad".
    case manualVeto
}

// MARK: - CorrectionEvent

/// Append-only record of a user correction. Schema owned here; Phase 7 writes to it.
///
/// Corrections are scoped — they may apply to an exact span, a sponsor across
/// all episodes of a podcast, or a phrase/campaign on a show. The scope is
/// serialized as a `CorrectionScope` string and stored in the `scope` column.
struct CorrectionEvent: Sendable, Equatable {
    /// UUID string for the event row.
    let id: String
    /// The analysisAssetId of the episode where the correction was made.
    let analysisAssetId: String
    /// Serialized `CorrectionScope` string (e.g. "exactSpan:asset123:10:20").
    let scope: String
    /// When the correction was recorded (seconds since epoch).
    let createdAt: Double
    /// The UI mechanism that generated this correction.
    let source: CorrectionSource?
    /// The podcast feed ID, if known at correction time.
    let podcastId: String?

    init(
        id: String = UUID().uuidString,
        analysisAssetId: String,
        scope: String,
        createdAt: Double = Date().timeIntervalSince1970,
        source: CorrectionSource? = nil,
        podcastId: String? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.scope = scope
        self.createdAt = createdAt
        self.source = source
        self.podcastId = podcastId
    }
}
