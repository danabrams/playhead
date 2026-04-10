// AdDecisionResult.swift
// Phase 6 (playhead-4my.6.3): Models for persisted decisions and append-only events.
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

// MARK: - CorrectionEvent

/// Append-only record of a user correction. Schema owned here; Phase 7 writes to it.
struct CorrectionEvent: Sendable, Identifiable, Equatable {
    let id: String
    let analysisAssetId: String
    let correctionScope: String    // e.g. "window", "span", "episode"
    let atomOrdinalRange: String   // JSON: [firstOrdinal, lastOrdinal]
    let evidenceJSON: String       // JSON: user-provided evidence
    let createdAt: Double
}
