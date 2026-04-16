// BudgetAllocationPolicy.swift
// Allocates FM and DSP budget to the highest-EVI candidates,
// with near-playhead priority override.

import Foundation

/// A single allocation decision: which item gets what resources and why.
struct BudgetAllocation: Sendable, Equatable {
    let itemIndex: Int
    let fmCost: Float
    let dspCost: Float
    let priorityReason: EVIPriorityReason?
    let isNearPlayhead: Bool
}

/// Stateless allocation policy that ranks candidates by EVI score
/// and greedily assigns budget from a `BudgetCoordinator`.
enum BudgetAllocationPolicy {

    /// Allocate budget to the highest-EVI items, respecting near-playhead priority.
    ///
    /// Items within `config.nearPlayheadWindowSeconds` of `playheadPosition` are
    /// serviced first regardless of EVI score. Remaining budget is allocated in
    /// descending EVI order.
    ///
    /// - Parameters:
    ///   - items: Candidate items with their scores and distance from playhead.
    ///   - coordinator: The budget coordinator to consume from.
    /// - Returns: Allocations in processing order (near-playhead first, then by EVI).
    static func allocate(
        items: [AllocationCandidate],
        coordinator: BudgetCoordinator,
        config: BudgetCoordinatorConfig
    ) async -> [BudgetAllocation] {
        // Partition into near-playhead and background.
        var nearPlayhead: [AllocationCandidate] = []
        var background: [AllocationCandidate] = []

        for item in items {
            let distance = max(item.distanceFromPlayhead, 0)
            if distance <= config.nearPlayheadWindowSeconds {
                nearPlayhead.append(item)
            } else {
                background.append(item)
            }
        }

        // Near-playhead items sorted by distance (closest first).
        nearPlayhead.sort { max($0.distanceFromPlayhead, 0) < max($1.distanceFromPlayhead, 0) }

        // Background sorted by EVI (highest first).
        background.sort { $0.eviScore.score > $1.eviScore.score }

        var allocations: [BudgetAllocation] = []

        // Service near-playhead first.
        for candidate in nearPlayhead {
            let fmOK = await coordinator.consumeFM(candidate.fmCost)
            let dspOK = await coordinator.consumeDSP(candidate.dspCost)

            if fmOK || dspOK {
                allocations.append(BudgetAllocation(
                    itemIndex: candidate.itemIndex,
                    fmCost: fmOK ? candidate.fmCost : 0,
                    dspCost: dspOK ? candidate.dspCost : 0,
                    priorityReason: candidate.eviScore.reason,
                    isNearPlayhead: true
                ))
            }
        }

        // Then background by EVI.
        for candidate in background {
            let fmOK = await coordinator.consumeFM(candidate.fmCost)
            let dspOK = await coordinator.consumeDSP(candidate.dspCost)

            if fmOK || dspOK {
                allocations.append(BudgetAllocation(
                    itemIndex: candidate.itemIndex,
                    fmCost: fmOK ? candidate.fmCost : 0,
                    dspCost: dspOK ? candidate.dspCost : 0,
                    priorityReason: candidate.eviScore.reason,
                    isNearPlayhead: false
                ))
            }
        }

        return allocations
    }
}

/// Input to the allocation policy: a candidate region with its EVI score and
/// resource requirements.
struct AllocationCandidate: Sendable, Equatable {
    let itemIndex: Int
    let eviScore: EVIScore
    let fmCost: Float
    let dspCost: Float
    /// Absolute seconds from the current playhead position.
    let distanceFromPlayhead: Float
}
