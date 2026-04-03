// PreviewBudgetStore.swift
// Business logic layer over AnalysisStore's preview_budgets table.
// Tracks consumed analysis seconds per episode and enforces the
// free-tier preview budget with a grace window for in-progress ad breaks.

import Foundation
import OSLog

// MARK: - PreviewBudgetStore

/// Reads and writes preview budgets, enforcing the free-tier limits.
///
/// Budget rules:
/// - **Base budget**: 12 decoded minutes (720 seconds) of analysis per episode.
/// - **Grace window**: if an ad break starts before the budget expires,
///   the system finishes that break, capped at 20 total minutes (1200 seconds).
/// - Budgets are keyed by `canonicalEpisodeKey` so dynamic ad variants
///   that share the same feed GUID + feed URL share one budget.
actor PreviewBudgetStore {
    private let logger = Logger(subsystem: "com.playhead", category: "PreviewBudget")
    private let analysisStore: AnalysisStore

    /// 12 minutes in seconds.
    static let baseBudgetSeconds: Double = 720

    /// Absolute cap including grace window: 20 minutes in seconds.
    static let maxBudgetWithGraceSeconds: Double = 1200

    init(analysisStore: AnalysisStore) {
        self.analysisStore = analysisStore
    }

    // MARK: - Public API

    /// Returns the remaining analysis seconds for the given episode.
    /// A premium user should never call this (they have unlimited budget).
    func remainingBudget(for episodeKey: String) async -> Double {
        let consumed = await fetchConsumed(for: episodeKey)
        return max(0, Self.baseBudgetSeconds - consumed)
    }

    /// Returns true if the episode still has analysis budget remaining.
    func hasBudget(for episodeKey: String) async -> Bool {
        await remainingBudget(for: episodeKey) > 0
    }

    /// Record that analysis consumed `seconds` for the given episode.
    /// Returns the new remaining budget (may be negative if grace was used).
    @discardableResult
    func consumeBudget(
        for episodeKey: String,
        seconds: Double
    ) async -> Double {
        let current = await fetchConsumed(for: episodeKey)
        let newConsumed = current + seconds
        await upsert(
            episodeKey: episodeKey,
            consumed: newConsumed,
            grace: 0
        )
        let remaining = Self.baseBudgetSeconds - newConsumed
        logger.debug(
            "Consumed \(seconds, privacy: .public)s for \(episodeKey, privacy: .public), remaining=\(remaining, privacy: .public)"
        )
        return remaining
    }

    /// Check whether a grace window should be granted for an ad break
    /// that starts at `breakStartTime` (in episode seconds) while the
    /// budget is at or near zero.
    ///
    /// Returns the additional seconds the system is allowed to analyse
    /// to finish this ad break (0 if no grace applies).
    func graceAllowance(
        for episodeKey: String,
        adBreakDuration: Double
    ) async -> Double {
        let consumed = await fetchConsumed(for: episodeKey)

        // Only grant grace if we're within the base budget (ad break started
        // while the user still had budget). Exactly at the limit = exhausted.
        guard consumed < Self.baseBudgetSeconds else { return 0 }

        // How much more can we allow under the absolute cap?
        let headroom = Self.maxBudgetWithGraceSeconds - consumed
        let allowance = min(adBreakDuration, headroom)

        if allowance > 0 {
            // Record the grace window so the UI can show context.
            await upsert(
                episodeKey: episodeKey,
                consumed: consumed,
                grace: allowance
            )
            logger.info(
                "Grace window of \(allowance, privacy: .public)s granted for \(episodeKey, privacy: .public)"
            )
        }

        return max(0, allowance)
    }

    /// Returns the total consumed seconds (including any grace usage).
    func totalConsumed(for episodeKey: String) async -> Double {
        await fetchConsumed(for: episodeKey)
    }

    // MARK: - Private

    private func fetchConsumed(for episodeKey: String) async -> Double {
        do {
            if let budget = try await analysisStore.fetchBudget(key: episodeKey) {
                return budget.consumedAnalysisSeconds
            }
        } catch {
            logger.error("Failed to fetch budget for \(episodeKey, privacy: .public): \(error)")
        }
        return 0
    }

    private func upsert(
        episodeKey: String,
        consumed: Double,
        grace: Double
    ) async {
        let budget = PreviewBudget(
            canonicalEpisodeKey: episodeKey,
            consumedAnalysisSeconds: consumed,
            graceBreakWindow: grace,
            lastUpdated: Date.now.timeIntervalSince1970
        )
        do {
            try await analysisStore.upsertBudget(budget)
        } catch {
            logger.error("Failed to upsert budget for \(episodeKey, privacy: .public): \(error)")
        }
    }
}
