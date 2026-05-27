// CrossEpisodeMemoryEvaluator.swift
// playhead-xsdz.9: Pure evaluator for the cross-episode "memory" precision
// signal. Turns Smith-Waterman alignment results — against the HARD-NEGATIVE
// bank and the confirmed-AD (positive) bank — into two decision-path effects:
//
//   1. SUPPRESSION (the novel lever): a multiplicative factor in `[0, 1]`
//      applied to `skipConfidence` when the candidate aligns strongly to a
//      confirmed false positive. This is NOT a ledger entry — a negative ledger
//      weight would be clamped to 0 by the v0 identity calibrator (which maps
//      `raw <= 0 → 0`), so suppression cannot ride the additive ledger. It
//      follows the SAME post-fusion multiplicative idiom as the xsdz.7 evidence-
//      fragility penalty and the Phase-7.2 `correctionFactor`.
//
//   2. POSITIVE BOOST: a single capped `.crossEpisodeMemory` ledger entry when
//      the candidate aligns strongly to a confirmed-ad bank entry. This rides
//      the normal additive ledger like every other corroborator.
//
// Both effects are produced ONLY when the caller has the feature enabled
// (`AdDetectionConfig.crossEpisodeMemoryEnabled`); this evaluator is pure and
// makes no flag check itself — the caller gates the bank reads and the calls
// here, exactly like `LexicalAutoAdEvidenceBuilder` / `AudioForensicsBoundaryDetector`.

import Foundation

// MARK: - CrossEpisodeMemoryEvaluator

struct CrossEpisodeMemoryEvaluator: Sendable {

    /// Tunables. Defaults are precision-first: suppression bites only on a
    /// strong negative match, and the positive boost is a modest corroborator
    /// (it never drives a skip on its own).
    struct Config: Sendable, Equatable {
        /// Strongest possible suppression: the multiplicative factor at a
        /// perfect (similarity=1, decay=1) negative match. 0.5 halves the skip
        /// confidence — enough to pull a span that *just* cleared the auto-skip
        /// threshold back below it, without nuking it to zero (we keep the
        /// score honest and let other gates decide). Must be in `[0, 1]`.
        let maxSuppression: Double

        /// Smith-Waterman normalized score for a positive-bank match to count
        /// as a boost. Higher than the negative threshold because a positive
        /// FALSE match only adds a little mass (bounded by the cap), whereas a
        /// negative false match suppresses — so positives can afford to be more
        /// permissive. Still conservative.
        let positiveMatchThreshold: Double

        static let `default` = Config(
            maxSuppression: 0.5,
            positiveMatchThreshold: 0.80
        )

        init(maxSuppression: Double = 0.5, positiveMatchThreshold: Double = 0.80) {
            self.maxSuppression = Swift.max(0.0, Swift.min(1.0, maxSuppression))
            self.positiveMatchThreshold = Swift.max(0.0, Swift.min(1.0, positiveMatchThreshold))
        }
    }

    private let config: Config

    init(config: Config = .default) {
        self.config = config
    }

    // MARK: - Suppression (negative bank)

    /// Multiplicative suppression factor in `[1 - maxSuppression, 1]` derived
    /// from a hard-negative match. `1.0` means no suppression (no match, or a
    /// `nil` match). Stronger effective matches push toward
    /// `1 - maxSuppression`.
    ///
    /// The factor scales linearly with the match's `effectiveStrength`
    /// (`similarity * decayWeight`): a fresh, perfect negative match yields the
    /// full `1 - maxSuppression`; a decayed or weaker match suppresses
    /// proportionally less.
    func suppressionFactor(for match: NegativeFingerprintMatch?) -> Double {
        guard let match else { return 1.0 }
        let strength = Swift.max(0.0, Swift.min(1.0, match.effectiveStrength))
        let factor = 1.0 - config.maxSuppression * strength
        return Swift.max(0.0, Swift.min(1.0, factor))
    }

    /// Apply the suppression factor to a skip confidence. Pure; clamps to
    /// `[0, 1]` and leaves non-finite input untouched (defensive — matches the
    /// xsdz.7 fragility-penalty contract).
    func suppress(skipConfidence: Double, with match: NegativeFingerprintMatch?) -> Double {
        guard skipConfidence.isFinite else { return skipConfidence }
        guard match != nil else { return skipConfidence }
        let factor = suppressionFactor(for: match)
        return Swift.max(0.0, Swift.min(1.0, skipConfidence * factor))
    }

    // MARK: - Positive boost (confirmed-ad bank)

    /// Build the (at most one) `.crossEpisodeMemory` boost ledger entry for a
    /// candidate that aligns strongly to a confirmed-ad bank sequence.
    ///
    /// - Parameters:
    ///   - candidateTokens: normalized candidate token sequence.
    ///   - positiveSequences: normalized token sequences from the confirmed-ad
    ///     bank (e.g. `AdCopyFingerprintStore.activeEntries` tokenized). Caller
    ///     supplies these; the evaluator stays free of store I/O.
    ///   - cap: the per-source fusion cap (`FusionWeightConfig.crossEpisodeMemoryCap`).
    /// - Returns: `[entry]` when the best positive alignment clears
    ///   `positiveMatchThreshold`, else `[]`. The emitted weight scales with the
    ///   alignment score up to `cap`, so a near-perfect repeat of known ad copy
    ///   contributes full cap and a borderline alignment contributes less.
    func buildPositiveBoostEntries(
        candidateTokens: [String],
        positiveSequences: [[String]],
        cap: Double
    ) -> [EvidenceLedgerEntry] {
        guard !candidateTokens.isEmpty, !positiveSequences.isEmpty, cap > 0 else { return [] }

        var bestScore = 0.0
        for stored in positiveSequences where !stored.isEmpty {
            let r = SmithWatermanAligner.align(candidateTokens, stored)
            if r.normalizedScore > bestScore { bestScore = r.normalizedScore }
        }
        guard bestScore >= config.positiveMatchThreshold else { return [] }

        let weight = Swift.max(0.0, Swift.min(cap, bestScore * cap))
        let entry = EvidenceLedgerEntry(
            source: .crossEpisodeMemory,
            weight: weight,
            // Reuse the existing `.fingerprint` detail variant — the closest
            // existing shape (a cross-episode copy match with an average
            // similarity). A bespoke detail case would be churn for no consumer.
            detail: .fingerprint(matchCount: 1, averageSimilarity: bestScore)
        )
        return [entry]
    }
}
