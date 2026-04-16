// EVIScorer.swift
// Expected-value-of-information scoring for FM budget allocation.
// Replaces fixed tier allocation with heuristic EVI ranking.

import Foundation

/// Why a region was prioritized for FM evaluation.
enum EVIPriorityReason: String, Codable, Sendable, Hashable, CaseIterable {
    case nearConfirmationThreshold
    case coldStartShow
    case metadataSeededUnanchored
    case boundaryUncertain
    case pharmaFallback
    case recentCorrection
    case lowTranscriptReliability
}

/// A scored candidate for FM budget allocation.
struct EVIScore: Sendable, Equatable {
    /// Probability that running FM will flip the ad/content decision.
    let decisionFlipProbability: Float

    /// Value of getting the decision correct for this region.
    let utilityGain: Float

    /// Normalized compute cost in [0, 1].
    let computeCost: Float

    /// Why this region scored highly (if it did).
    let reason: EVIPriorityReason?

    /// EVI = P(flip) x utility / max(cost, 0.01).
    var score: Float {
        decisionFlipProbability * utilityGain / max(computeCost, 0.01)
    }

    init(
        decisionFlipProbability: Float,
        utilityGain: Float,
        computeCost: Float,
        reason: EVIPriorityReason?
    ) {
        self.decisionFlipProbability = min(max(decisionFlipProbability, 0), 1)
        self.utilityGain = max(utilityGain, 0)
        self.computeCost = min(max(computeCost, 0), 1)
        self.reason = reason
    }
}

/// Heuristic EVI computation for backfill budget allocation.
///
/// The heuristic approximations used here are placeholders for the full
/// counterfactual scoring that will land later. They are intentionally
/// conservative — the goal is to rank candidates, not produce calibrated
/// probabilities.
enum EVIScorer {

    /// Heuristic: highest flip probability near 0.5 confidence.
    /// Maps confidence in [0, 1] to flip probability in [0, 1].
    static func flipProbability(currentConfidence: Float) -> Float {
        let clamped = min(max(currentConfidence, 0), 1)
        return 1 - abs(2 * clamped - 1)
    }

    /// Score a single candidate region.
    ///
    /// - Parameters:
    ///   - currentConfidence: Current ad/content confidence for the region (0 = content, 1 = ad).
    ///   - computeCost: Normalized cost of running FM on this region.
    ///   - reason: The priority reason, if one applies.
    ///   - utilityGain: Value of getting the decision right. Defaults to 1.0 (uniform).
    static func score(
        currentConfidence: Float,
        computeCost: Float,
        reason: EVIPriorityReason?,
        utilityGain: Float = 1.0
    ) -> EVIScore {
        EVIScore(
            decisionFlipProbability: flipProbability(currentConfidence: currentConfidence),
            utilityGain: utilityGain,
            computeCost: min(max(computeCost, 0), 1),
            reason: reason
        )
    }

    /// Batch-score an array of candidates and return scores sorted by
    /// descending EVI (highest value first).
    static func rank(
        _ candidates: [(confidence: Float, cost: Float, reason: EVIPriorityReason?)]
    ) -> [EVIScore] {
        candidates
            .map { score(currentConfidence: $0.confidence, computeCost: $0.cost, reason: $0.reason) }
            .sorted { $0.score > $1.score }
    }
}
