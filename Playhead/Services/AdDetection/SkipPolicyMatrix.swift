// SkipPolicyMatrix.swift
// Phase 6 (playhead-4my.6.2): Policy matrix, decision cohort, and stability policy.

import Foundation

// MARK: - SkipPolicyAction

enum SkipPolicyAction: String, Sendable, Codable, Hashable, CaseIterable {
    case autoSkipEligible // fusion can promote to skip
    case detectOnly       // show banner, never skip
    case suppress         // don't even show
    case logOnly          // record for telemetry only
}

// MARK: - SkipPolicyMatrix

/// Maps (CommercialIntent, AdOwnership) → SkipPolicyAction.
///
/// v1: all spans arrive as (.unknown, .unknown) → .logOnly.
/// Phase 8 (SponsorKnowledgeStore) will populate intent/ownership from business context.
/// FM does NOT classify commercial intent in Phase 6.
struct SkipPolicyMatrix: Sendable {

    static let defaultAction: SkipPolicyAction = .logOnly

    static func action(for intent: CommercialIntent, ownership: AdOwnership) -> SkipPolicyAction {
        switch intent {
        case .paid:
            // paid + thirdParty: classic insertion — eligible for auto-skip.
            // paid + show/network: show-produced paid content (e.g. dynamic in-episode reads for
            // a brand the show also owns). The ownership attribution is ambiguous and Phase 8
            // (SponsorKnowledgeStore) must resolve it before we can act. Until then: logOnly.
            return ownership == .thirdParty ? .autoSkipEligible : .logOnly
        case .owned:
            return (ownership == .show || ownership == .network) ? .detectOnly : .logOnly
        case .affiliate:
            // affiliate dominates regardless of ownership
            return .detectOnly
        case .organic:
            // organic dominates regardless of ownership
            return .suppress
        case .unknown:
            return .logOnly
        }
    }
}

// MARK: - DecisionCohort

/// Identifies the exact pipeline configuration used to produce a decision.
///
/// Changes to any hash field trigger decision recomputation from cached scan results
/// WITHOUT triggering FM rescans (scan results are keyed separately by ScanCohort).
struct DecisionCohort: Sendable, Codable, Hashable {
    let featurePipelineHash: String
    let fusionHash: String
    let policyHash: String
    let stabilityHash: String
    let appBuild: String

    // IMPORTANT: bump each hash string manually whenever the corresponding
    // pipeline component changes. The date suffix is a documentation aid, not
    // a machine-readable field — there is no automated enforcement.
    static func production(appBuild: String) -> DecisionCohort {
        DecisionCohort(
            featurePipelineHash: "feature-v1-2026-04-10",
            fusionHash: "fusion-v1-2026-04-10",
            policyHash: "policy-v1-2026-04-10",
            stabilityHash: "stability-v1-2026-04-10",
            appBuild: appBuild
        )
    }
}

// MARK: - DecisionStabilityPolicy

/// Governs when a previously-applied skip cue can be removed.
///
/// Removal requires BOTH:
///   (a) score drops below stayThreshold  AND
///   (b) at least one of: user correction, FM negative (strong certainty + good quality),
///       or transcript content materially changed.
///
/// Cohort-only changes (same transcript, new OS model) cannot remove cues unless
/// the score drops below suppressionThreshold. User correction always overrides.
struct DecisionStabilityPolicy: Sendable {
    let stayThreshold: Double
    let suppressionThreshold: Double

    init(stayThreshold: Double = 0.45, suppressionThreshold: Double = 0.25) {
        self.stayThreshold = stayThreshold
        self.suppressionThreshold = suppressionThreshold
    }

    static let `default` = DecisionStabilityPolicy()

    func canRemoveCue(
        currentScore: Double,
        userCorrected: Bool,
        fmNegativeWithStrongCertainty: Bool,
        transcriptVersionChanged: Bool,
        cohortOnlyChange: Bool
    ) -> Bool {
        if userCorrected { return true }
        if cohortOnlyChange { return currentScore < suppressionThreshold }
        let scoreBelowThreshold = currentScore < stayThreshold
        let hasCounterEvidence = fmNegativeWithStrongCertainty || transcriptVersionChanged
        return scoreBelowThreshold && hasCounterEvidence
    }
}
