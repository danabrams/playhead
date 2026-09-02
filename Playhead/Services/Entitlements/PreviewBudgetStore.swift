// PreviewBudgetStore.swift
// Business logic layer over AnalysisStore's preview_budgets table.
// Tracks consumed analysis seconds per episode.
//
// ⚠️ THIS TYPE IS NOT WIRED, AND ITS DESIGN IS SUPERSEDED (playhead-i7kvl.1,
// 2026-09-02). Read this before you trust the sentence under it.
//
// IT ENFORCES NOTHING. Measured by grep over `Playhead/` and `PlayheadTests/`
// on 2026-09-02: every reference outside this file is a TEST. No production
// code constructs it, calls `hasBudget`, or calls `consumeBudget`, and nothing
// writes the `preview_budgets` table the CRUD in `AnalysisStore` maintains for
// it. The wider fact is that the app has a PURCHASE FLOW AND NO GATE —
// `EntitlementManager.isPremium` is consumed only by `SettingsView`, for a
// label and the buy/restore buttons, so buying the unlock changes nothing a
// listener can observe.
//
// This header exists because the doc comment it replaced said the type
// "enforces the free-tier preview budget", and a bead was written on that
// sentence without anyone asking whether it had a caller. That is the standing
// defect class — a value, or here a claim, that names one thing read as though
// it named another — so the claim is stated with its evidence rather than left
// for the next reader to re-derive.
//
// THE DESIGN IS ALSO SUPERSEDED. Dan decided on 2026-09-02 that the free tier
// is ONE SHOW with unlimited episodes, not a per-episode seconds budget. The
// per-episode budget demoed the wrong ad even in principle: measured ad width
// on the 24-episode gold set is 20 % pre-roll / 66 % MID / 13 % post, so a
// 12-minute allowance covers the pre-roll and expires before the mid-roll that
// carries most of the value.
//
// DO NOT WIRE THIS UP. If you are here to build the free tier, playhead-i7kvl.1
// carries the real scope and the four product questions it still needs
// answered. Left in the tree rather than deleted because deleting a tested type
// is Dan's call, not a side effect of discovering it is unused.

import Foundation
import OSLog

// MARK: - PreviewBudgetStore

/// Reads and writes preview budgets. NOT WIRED — see the file header.
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
