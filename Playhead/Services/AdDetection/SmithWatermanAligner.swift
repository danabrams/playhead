// SmithWatermanAligner.swift
// playhead-xsdz.9: Deterministic Smith-Waterman LOCAL sequence alignment over
// token sequences, used by the cross-episode "memory" precision signal.
//
// Why local alignment (not Jaccard / MinHash)
// --------------------------------------------
// The existing `AdCopyFingerprintMatcher` matches recurring ad copy with a
// character-4-gram MinHash Jaccard similarity. That is order-INSENSITIVE: it
// answers "do these two texts share a lot of n-grams?" but cannot tell whether
// the shared material appears in the SAME ORDER. On-device ASR produces the
// SAME ad script with run-to-run mutations — a dropped filler word, a
// substituted homophone ("BetterHelp" → "better help"), a duplicated token —
// i.e. insertions / deletions / substitutions sprinkled through an otherwise
// identical token run. Smith-Waterman is exactly the tool for that: it finds
// the single best-scoring CONTIGUOUS-WITH-GAPS sub-alignment between two token
// sequences, tolerating those edits while still requiring the matched material
// to line up in order.
//
// This type is PURE and DETERMINISTIC: same inputs → same output, no I/O, no
// randomness, no clock. Cost is O(N·M) time / O(min(N,M)) extra space via a
// two-row rolling DP (we only need the max cell score, never the traceback),
// bounded by the caller capping sequence length (~50–100 tokens) and the
// number of stored sequences per show.
//
// Scoring follows the textbook local-alignment recurrence:
//   H[i][j] = max(
//       0,                                  // local: never go negative
//       H[i-1][j-1] + (a==b ? match : mismatch),
//       H[i-1][j]   - gap,                  // deletion in b
//       H[i][j-1]   - gap                   // insertion in b
//   )
// The alignment SCORE is max over all cells. We normalize that raw score into
// [0, 1] by dividing by the best achievable score for the SHORTER sequence
// (`match * min(n, m)`), so a perfect containment of the shorter sequence
// inside the longer one scores 1.0 regardless of length asymmetry. That makes
// the normalized score a stable, length-robust similarity that the caller can
// threshold.

import Foundation

// MARK: - SmithWatermanAligner

/// Pure Smith-Waterman local aligner over token sequences.
enum SmithWatermanAligner {

    /// Scoring parameters for the local alignment. Defaults are the common
    /// textbook choice (match +2, mismatch −1, gap −1) tuned so that a single
    /// substitution or a single-token gap costs less than the surrounding
    /// matches it sits between — the mutation profile of re-ASR'd ad copy.
    struct Scoring: Sendable, Equatable {
        /// Reward added for a matching token pair. Must be > 0 or the DP
        /// degenerates (the empty alignment always wins).
        let match: Int
        /// Penalty (subtracted) for a substitution (non-matching token pair).
        /// Stored as a non-negative magnitude; applied as `−mismatch`.
        let mismatch: Int
        /// Penalty (subtracted) for a single-token gap (insertion or deletion).
        /// Stored as a non-negative magnitude; applied as `−gap`.
        let gap: Int

        static let `default` = Scoring(match: 2, mismatch: 1, gap: 1)

        init(match: Int = 2, mismatch: Int = 1, gap: Int = 1) {
            // Local alignment only makes sense with a positive match reward and
            // non-negative penalties; clamp defensively so a misconfigured
            // caller can't invert the recurrence.
            self.match = Swift.max(1, match)
            self.mismatch = Swift.max(0, mismatch)
            self.gap = Swift.max(0, gap)
        }
    }

    /// Result of a local alignment: the raw best-cell score and the
    /// length-normalized similarity in `[0, 1]`.
    struct Result: Sendable, Equatable {
        /// Raw Smith-Waterman score (max DP cell). `0` when there is no
        /// positive-scoring local alignment (e.g. fully disjoint sequences).
        let rawScore: Int
        /// `rawScore` normalized by the best achievable score for the shorter
        /// sequence (`match * min(n, m)`), clamped to `[0, 1]`. `0` when either
        /// sequence is empty.
        let normalizedScore: Double

        static let zero = Result(rawScore: 0, normalizedScore: 0)
    }

    /// Compute the local alignment between two token sequences.
    ///
    /// - Parameters:
    ///   - a: First token sequence (already normalized by the caller).
    ///   - b: Second token sequence (already normalized by the caller).
    ///   - scoring: Match / mismatch / gap parameters.
    /// - Returns: A `Result` carrying the raw and normalized alignment scores.
    ///   Empty input on either side yields `.zero` (a deterministic, defined
    ///   answer — never a crash).
    static func align(
        _ a: [String],
        _ b: [String],
        scoring: Scoring = .default
    ) -> Result {
        let n = a.count
        let m = b.count
        guard n > 0, m > 0 else { return .zero }

        // Rolling two-row DP. We never need the traceback (only the max score),
        // so O(min(n, m)) memory suffices. Iterate rows over the LONGER
        // sequence and columns over the SHORTER one to keep the row buffers
        // small; the recurrence is symmetric so this does not change the score.
        let (outer, inner) = n >= m ? (a, b) : (b, a)
        let innerCount = inner.count

        var previous = [Int](repeating: 0, count: innerCount + 1)
        var current = [Int](repeating: 0, count: innerCount + 1)
        var best = 0

        for i in 1...outer.count {
            // Column 0 stays 0 for local alignment (a fresh start is always
            // available). `current[0]` is already 0 from initialization and we
            // overwrite indices 1...innerCount below, but reset defensively.
            current[0] = 0
            let outerToken = outer[i - 1]
            for j in 1...innerCount {
                let isMatch = outerToken == inner[j - 1]
                let diagDelta = isMatch ? scoring.match : -scoring.mismatch
                let diagonal = previous[j - 1] + diagDelta
                let up = previous[j] - scoring.gap
                let left = current[j - 1] - scoring.gap
                let cell = Swift.max(0, diagonal, up, left)
                current[j] = cell
                if cell > best { best = cell }
            }
            swap(&previous, &current)
        }

        let shorter = Swift.min(n, m)
        let bestPossible = Double(scoring.match * shorter)
        let normalized = bestPossible > 0
            ? Swift.max(0.0, Swift.min(1.0, Double(best) / bestPossible))
            : 0.0
        return Result(rawScore: best, normalizedScore: normalized)
    }

    // MARK: - Tokenization

    /// Tokenize raw transcript text into normalized alignment tokens.
    ///
    /// Reuses `MinHashUtilities.normalizeText` (lowercase, strip punctuation,
    /// drop filler words, collapse whitespace) so the negative / positive banks
    /// and the existing `AdCopyFingerprint` path share ONE normalization
    /// contract — a token mismatch caused by divergent normalization would
    /// silently sink alignment scores. The normalized text is then split on
    /// whitespace into word tokens.
    static func tokenize(_ text: String) -> [String] {
        let normalized = MinHashUtilities.normalizeText(text)
        guard !normalized.isEmpty else { return [] }
        return normalized.split(separator: " ").map(String.init)
    }
}
