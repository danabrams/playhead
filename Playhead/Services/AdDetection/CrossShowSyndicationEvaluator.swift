// CrossShowSyndicationEvaluator.swift
// playhead-xsdz.13: Pure evaluator for the cross-show syndication signal. Turns
// a `CrossShowSpreadProfile` (resolved from `CrossShowSyndicationStore`) into a
// single capped `.crossShowSyndication` POSITIVE boost ledger entry.
//
// Insight (a blind-spot both models in an idea duel reached independently): ad
// campaigns are sold across show NETWORKS. A sponsor entity that recurs across
// MANY of the user's UNRELATED subscribed shows is overwhelming evidence of a
// paid network campaign; an editorial brand mention is show-specific (it
// appears in one show because of that show's topic). A one-show/one-episode
// entity gets NO boost (mild / neutral).
//
// Precision guard — BOTH conditions required:
//   1. SPREAD: the entity's spread ratio (distinct shows / observed shows) AND
//      its absolute distinct-show count must clear thresholds. A single absolute
//      OR ratio test is insufficient on its own: ratio alone over-fires on a
//      tiny library (1-of-1 = 1.0); absolute count alone over-fires once the
//      library is large.
//   2. TEMPORAL PERSISTENCE: the entity must have been seen ACROSS TIME, not in
//      a one-week burst. This avoids boosting genuine multi-show EDITORIAL
//      bursts — e.g. "Apple" mentioned across many shows the same week as a
//      product launch. A real syndicated campaign persists for weeks/months; an
//      editorial burst does not.
//
// The boost is CAPPED and MODEST with NO qualified promotion track, so it can
// never drive an auto-skip on its own — it only adds honest corroborative mass
// and bumps `distinctKinds.count`. This evaluator is PURE and makes no flag
// check itself — the caller gates the store reads and the call here, exactly
// like `CrossEpisodeMemoryEvaluator` / `RhetoricalGrammarEvidenceBuilder`.

import Foundation

// MARK: - CrossShowSyndicationEvaluator

struct CrossShowSyndicationEvaluator: Sendable {

    /// Tunables. Defaults are precision-first: the boost fires only when an
    /// entity reaches a meaningful FRACTION of the library across SEVERAL shows
    /// AND has persisted across time.
    struct Config: Sendable, Equatable {
        /// Minimum spread ratio (distinct shows / total observed shows) for the
        /// boost. 0.40 means the entity must reach at least ~40% of the user's
        /// observed library — a footprint an editorial topic almost never has.
        let minSpreadRatio: Double

        /// Minimum ABSOLUTE distinct-show count, independent of the ratio. Guards
        /// the small-library degenerate case (1-of-1 show = ratio 1.0): a single
        /// show can never be "syndication", so we require the entity to span at
        /// least 3 distinct shows regardless of library size.
        let minDistinctShows: Int

        /// Minimum temporal persistence in days. The entity's first-seen and
        /// last-seen across the library must be at least this far apart. 14 days
        /// excludes a single-week multi-show editorial burst (product launch /
        /// news cycle) while admitting any genuinely-recurring campaign.
        let minPersistenceDays: Double

        static let `default` = Config(
            minSpreadRatio: 0.40,
            minDistinctShows: 3,
            minPersistenceDays: 14.0
        )

        init(
            minSpreadRatio: Double = 0.40,
            minDistinctShows: Int = 3,
            minPersistenceDays: Double = 14.0
        ) {
            self.minSpreadRatio = Swift.max(0.0, Swift.min(1.0, minSpreadRatio))
            self.minDistinctShows = Swift.max(1, minDistinctShows)
            self.minPersistenceDays = Swift.max(0.0, minPersistenceDays)
        }
    }

    private let config: Config

    init(config: Config = .default) {
        self.config = config
    }

    /// Whether the profile clears BOTH the spread AND the temporal-persistence
    /// gates. Pure; exposed for unit testing the gate independently of weighting.
    func qualifies(_ profile: CrossShowSpreadProfile) -> Bool {
        guard profile.distinctShowCount >= config.minDistinctShows else { return false }
        guard profile.spreadRatio >= config.minSpreadRatio else { return false }
        guard profile.persistenceDays >= config.minPersistenceDays else { return false }
        return true
    }

    /// Build the (at most one) `.crossShowSyndication` boost ledger entry for an
    /// entity whose cross-show spread profile clears the precision guard.
    ///
    /// - Parameters:
    ///   - profile: the entity's cross-show spread profile, or `nil` (no
    ///     observations) ⇒ no entry.
    ///   - cap: the per-source fusion cap
    ///     (`FusionWeightConfig.crossShowSyndicationCap`).
    /// - Returns: `[entry]` when the profile qualifies, else `[]`. The emitted
    ///   weight scales with the spread ratio up to `cap`, so an entity that
    ///   blankets the whole library contributes full cap and one that just
    ///   clears the threshold contributes proportionally less. Capped and modest
    ///   — never drives a skip alone.
    func buildBoostEntries(
        profile: CrossShowSpreadProfile?,
        cap: Double
    ) -> [EvidenceLedgerEntry] {
        guard cap > 0, let profile, qualifies(profile) else { return [] }

        // Scale the weight by the spread ratio (already in `[0, 1]`), clamped to
        // the cap. A perfectly-blanketing entity (ratio 1.0) earns full cap.
        let weight = Swift.max(0.0, Swift.min(cap, profile.spreadRatio * cap))
        let entry = EvidenceLedgerEntry(
            source: .crossShowSyndication,
            weight: weight,
            // Reuse the existing `.catalog` detail variant — the closest existing
            // shape (a reference-match corroborator with a count). The
            // distinct-show count is the meaningful "how many" here. A bespoke
            // detail case would be churn for no consumer.
            detail: .catalog(entryCount: profile.distinctShowCount)
        )
        return [entry]
    }
}
