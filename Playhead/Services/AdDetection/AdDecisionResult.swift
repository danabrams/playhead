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
    /// Structured explanation trace (playhead-ef2.1.4). Compact JSON encoding of
    /// `DecisionExplanation`. Nil for events created before this field was added.
    let explanationJSON: String?

    init(
        id: String,
        analysisAssetId: String,
        eventType: String,
        windowId: String,
        proposalConfidence: Double,
        skipConfidence: Double,
        eligibilityGate: String,
        policyAction: String,
        decisionCohortJSON: String,
        createdAt: Double,
        explanationJSON: String? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.eventType = eventType
        self.windowId = windowId
        self.proposalConfidence = proposalConfidence
        self.skipConfidence = skipConfidence
        self.eligibilityGate = eligibilityGate
        self.policyAction = policyAction
        self.decisionCohortJSON = decisionCohortJSON
        self.createdAt = createdAt
        self.explanationJSON = explanationJSON
    }
}

// MARK: - ProposalAuthority

/// Indicates whether a source's contribution was strong or weak relative to its cap.
enum ProposalAuthority: String, Sendable, Codable, Equatable {
    /// Weight exceeds half the source's cap — meaningful contributor.
    case strong
    /// Weight is below half the source's cap — marginal contributor.
    case weak
}

// MARK: - SourceEvidence

/// Per-source evidence breakdown for a single decision. Aggregated across all
/// ledger entries of the same source type.
struct SourceEvidence: Sendable, Codable, Equatable {
    /// Source type name (matches EvidenceSourceType.rawValue).
    let source: String
    /// Total aggregated weight from all entries of this source type.
    let weight: Double
    /// The cap that was applied to this source type.
    let capApplied: Double
    /// Whether this source's contribution was strong or weak relative to its cap.
    let authority: ProposalAuthority
}

// MARK: - ActionRationale

/// Links the threshold, gate, and policy to the final skip eligibility determination.
struct ActionRationale: Sendable, Codable, Equatable {
    /// The skip confidence threshold used for auto-skip promotion.
    let threshold: Double
    /// The eligibility gate value at decision time.
    let gate: String
    /// The policy action applied.
    let policyAction: String
    /// Whether the decision was ultimately skip-eligible.
    let skipEligible: Bool
}

// MARK: - DecisionExplanation

/// Structured explanation trace for a single fusion decision. Stored as compact JSON
/// in DecisionEvent.explanationJSON for QA, debugging, replay, and counterfactual
/// evaluation. Not user-facing.
struct DecisionExplanation: Sendable, Codable, Equatable {
    /// Per-source evidence breakdown with calibrated weights and authority.
    let evidenceBreakdown: [SourceEvidence]
    /// Which evidence families contributed to the final score (source type names).
    let contributingFamilies: [String]
    /// Links threshold/policy/gate to the skip eligibility outcome.
    let actionRationale: ActionRationale

    /// Build an explanation from the decision ledger and result.
    ///
    /// Aggregates ledger entries by source type, computes per-source authority
    /// relative to the configured cap, and produces the action rationale from
    /// the decision result and policy action.
    static func build(
        ledger: [EvidenceLedgerEntry],
        decision: DecisionResult,
        policyAction: SkipPolicyAction,
        config: FusionWeightConfig,
        skipThreshold: Double
    ) -> DecisionExplanation {
        // Aggregate weights per source type
        var weightBySource: [EvidenceSourceType: Double] = [:]
        for entry in ledger {
            weightBySource[entry.source, default: 0.0] += entry.weight
        }

        // Stable sort order: follow EvidenceSourceType.allCases ordering
        let sortedSources = EvidenceSourceType.allCases.filter { weightBySource[$0] != nil }

        let breakdown: [SourceEvidence] = sortedSources.map { sourceType in
            let totalWeight = weightBySource[sourceType]!
            let cap = capForSource(sourceType, config: config)
            let authority: ProposalAuthority = totalWeight > cap * 0.5 ? .strong : .weak
            return SourceEvidence(
                source: sourceType.rawValue,
                weight: totalWeight,
                capApplied: cap,
                authority: authority
            )
        }

        let families = sortedSources.map { $0.rawValue }

        let isSkipEligible = policyAction == .autoSkipEligible
            && decision.eligibilityGate == .eligible

        let rationale = ActionRationale(
            threshold: skipThreshold,
            gate: decision.eligibilityGate.rawValue,
            policyAction: policyAction.rawValue,
            skipEligible: isSkipEligible
        )

        return DecisionExplanation(
            evidenceBreakdown: breakdown,
            contributingFamilies: families,
            actionRationale: rationale
        )
    }

    /// Returns the configured cap for a given source type.
    private static func capForSource(_ source: EvidenceSourceType, config: FusionWeightConfig) -> Double {
        switch source {
        case .fm: return config.fmCap
        case .lexical: return config.lexicalCap
        case .acoustic: return config.acousticCap
        case .catalog: return config.catalogCap
        case .classifier: return config.classifierCap
        case .fingerprint: return config.fingerprintCap
        case .fusedScore: return 1.0  // Fused score is post-aggregation; no per-source cap applies.
        }
    }
}

// MARK: - CorrectionSource

/// The UI gesture or mechanism that produced a user correction.
enum CorrectionSource: String, Sendable, Codable {
    /// User tapped "Listen" on a span that was auto-skipped, reverting the skip.
    case listenRevert
    /// User explicitly vetoed a span via "This isn't an ad".
    case manualVeto
    /// User reported a missed ad (false negative) — "Hearing an ad" button or transcript tap-to-mark.
    case falseNegative
}

// MARK: - CorrectionKind

/// Distinguishes whether a correction is a false positive ("not an ad") or
/// false negative ("is an ad") report. Derived from the CorrectionSource.
enum CorrectionKind: Sendable {
    /// User says the system incorrectly flagged content as an ad.
    case falsePositive
    /// User says the system missed an ad that is currently playing.
    case falseNegative
}

extension CorrectionSource {
    /// The semantic kind of correction this source represents.
    var kind: CorrectionKind {
        switch self {
        case .listenRevert, .manualVeto:
            return .falsePositive
        case .falseNegative:
            return .falseNegative
        }
    }
}

extension CorrectionKind {
    /// Map to the persisted `CorrectionType` value. FP/FN `CorrectionType`
    /// cases share names with `CorrectionKind` — this extension centralises
    /// the mapping so call sites don't reimplement the switch.
    var correctionType: CorrectionType {
        switch self {
        case .falseNegative: return .falseNegative
        case .falsePositive: return .falsePositive
        }
    }
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
    /// ef2.3.1: Semantic nature of the correction (FP/FN/boundary).
    let correctionType: CorrectionType?
    /// ef2.3.1: Pipeline component most responsible for the error.
    let causalSource: CausalSource?
    /// ef2.3.1: JSON-encoded CorrectionTargetRefs for downstream analysis.
    let targetRefs: CorrectionTargetRefs?

    init(
        id: String = UUID().uuidString,
        analysisAssetId: String,
        scope: String,
        createdAt: Double = Date().timeIntervalSince1970,
        source: CorrectionSource? = nil,
        podcastId: String? = nil,
        correctionType: CorrectionType? = nil,
        causalSource: CausalSource? = nil,
        targetRefs: CorrectionTargetRefs? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.scope = scope
        self.createdAt = createdAt
        self.source = source
        self.podcastId = podcastId
        self.correctionType = correctionType
        self.causalSource = causalSource
        self.targetRefs = targetRefs
    }
}
