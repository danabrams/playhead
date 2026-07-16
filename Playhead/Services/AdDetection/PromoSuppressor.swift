// PromoSuppressor.swift
// playhead-fl4j: eligibility-side self-promo suppression evaluator.
//
// A show promoting ITSELF (rate/review/subscribe, follow us, be a guest,
// live-show / get-tickets plugs, "new ways to watch", …) is NOT an ad the user
// wants auto-skipped. When such a segment is (mis)detected as an ad, this
// evaluator signals the fusion path to demote its eligibility gate to
// `.markOnly` so it surfaces as a play-by-default SUGGEST banner instead of
// auto-skipping.
//
// Design (mirrors `CreatorChapterSuppressionEvaluator`):
//
// 1. Pure value type, no I/O. Stateless — every entry point is `static`. Lives
//    as an enum namespace like `CandidateWindowSelector` /
//    `CreatorChapterSuppressionEvaluator`. Deterministic over its inputs; the
//    caller owns WHEN to apply it in the per-span loop and severity-guards the
//    demotion.
//
// 2. Reads only the span geometry and the transcript word stream the ledger
//    already carries (`LexicalWord`, built from the same `TranscriptChunk`s the
//    `LexicalScanner` consumes via `LexicalAnchorRefiner.buildWordStream`). No
//    audio decode.
//
// 3. EXACT normalised-token matching only. A bank phrase fires iff its
//    normalised token sequence appears CONTIGUOUSLY inside the span's word
//    slice. Both sides normalise through `LexicalAnchorNormalizer` (the bank at
//    load time, the words at stream-build time), so casing / punctuation /
//    apostrophe style never affect the match. No fuzzy matching (parity risk —
//    same rationale as the lexical-anchor refiner).
//
// 4. Scoring is honest. The evaluator returns only a Bool; it never touches
//    `proposalConfidence` / `skipConfidence`. The caller threads the gate
//    demotion through the same post-`DecisionMapper.map()` shape the
//    creator-chapter and FM-suppression paths use.

import Foundation

/// Decides whether a decoded span's transcript contains a curated self-promo
/// action phrase — the signal to demote its eligibility gate to `.markOnly`.
///
/// Stateless: every entry point is `static`. Lives as an enum namespace to
/// match `CreatorChapterSuppressionEvaluator`'s pattern.
enum PromoSuppressor {

    /// Whether `span` should have its eligibility gate demoted because its
    /// transcript contains a self-promo action phrase from `bank`.
    ///
    /// Returns `false` (no suppression) when:
    ///   * `bank.phrases` is empty (inert bank — defensive; the loader rejects
    ///     an empty bank, but a caller could construct one).
    ///   * The span has zero (or negative) duration — the word slice is
    ///     undefined, so we conservatively decline.
    ///   * No transcript word overlaps the span's `[startTime, endTime]`.
    ///   * No bank phrase's normalised token sequence appears contiguously in
    ///     the span's word slice.
    ///
    /// - Parameters:
    ///   - span: the decoded span whose gate is being decided (geometry only).
    ///   - transcriptWords: the episode word stream (whole episode is fine — it
    ///     is sliced to the span here, so a self-promo phrase ELSEWHERE in the
    ///     episode does not suppress THIS span).
    ///   - bank: the curated show-agnostic self-promo action-phrase bank.
    static func shouldSuppress(
        span: DecodedSpan,
        transcriptWords: [LexicalWord],
        bank: SelfPromoBank
    ) -> Bool {
        guard !bank.phrases.isEmpty else { return false }

        let spanDuration = span.endTime - span.startTime
        guard spanDuration > 0 else { return false }

        // Slice the episode word stream to words that overlap the span. Same
        // half-open overlap predicate the service uses to scope feature windows
        // to a span (`fw.startTime < span.endTime && fw.endTime > span.startTime`).
        let spanTokens = transcriptWords
            .filter { $0.startSeconds < span.endTime && $0.endSeconds > span.startTime }
            .map(\.norm)
        guard !spanTokens.isEmpty else { return false }

        for phrase in bank.phrases where containsSubsequence(spanTokens, phrase.tokens) {
            return true
        }
        return false
    }

    // MARK: - Private

    /// Whether `needle` appears as a CONTIGUOUS subsequence of `haystack` (exact
    /// per-token match). Same sliding-window scan `LexicalAnchorRefiner`'s
    /// `edgeMatches` uses; `needle` is guaranteed non-empty by the bank loader's
    /// `>= 2` token floor, but the `>= 1` guard keeps this total for any caller.
    private static func containsSubsequence(_ haystack: [String], _ needle: [String]) -> Bool {
        let n = needle.count
        guard n >= 1, haystack.count >= n else { return false }
        let lastStart = haystack.count - n
        var i = 0
        while i <= lastStart {
            var matched = true
            var k = 0
            while k < n {
                if haystack[i + k] != needle[k] {
                    matched = false
                    break
                }
                k += 1
            }
            if matched { return true }
            i += 1
        }
        return false
    }
}
