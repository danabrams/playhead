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
// ATTENTION → VERIFICATION (the design principle, Dan): a lexical hit is a CLUE
// about where to look, NOT a final determination. This evaluator does NOT treat
// a bare bank-phrase match as the verdict. Instead:
//
//   1. ATTENTION. A bank phrase whose normalised tokens appear contiguously in
//      the span's token slice is a CANDIDATE ("look here") — never a demotion on
//      its own.
//   2. VERIFICATION. Each candidate is handed to a list of `SelfPromoVerifier`s
//      (see `SelfPromoVerifier.swift`) that must independently corroborate that
//      the segment is the show promoting ITSELF. The one verifier that ships
//      today (`SelfReferenceVerifier`) confirms a STRONG (`.selfEvident`) phrase
//      unconditionally (it carries its own self-reference) and confirms an
//      AMBIGUOUS (`.requiresCorroboration`) phrase ONLY when a first-person or
//      show-identity marker sits in its local window. A bare AMBIGUOUS match
//      with no corroboration is a clue that FAILED verification — no demotion.
//   3. DECISION. The evaluator returns `true` (suppress) iff SOME candidate is
//      corroborated by SOME verifier (OR composition — a future semantic /
//      position verifier is additive; see the seam in `SelfPromoVerifier.swift`).
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
//    `LexicalScanner` consumes via `LexicalAnchorRefiner.buildWordStream`), plus
//    the show identity threaded from the podcast profile at the call site. No
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

/// Decides whether a decoded span's transcript is the show promoting ITSELF —
/// the signal to demote its eligibility gate to `.markOnly`. A curated bank
/// phrase matching is only ATTENTION; a `SelfPromoVerifier` must corroborate the
/// candidate before this returns `true`.
///
/// Stateless: every entry point is `static`. Lives as an enum namespace to
/// match `CreatorChapterSuppressionEvaluator`'s pattern.
enum PromoSuppressor {

    /// The verifier list applied to each attention candidate. Ships with the one
    /// verifier the bead builds — self-reference (first-person / show identity).
    /// The seam is explicit: appending a future `SelfPromoVerifier` (semantic
    /// topic-continuity, bead playhead-rqu6; or a position/fusion corroborator)
    /// adds a corroborator without touching the attention loop below.
    static let defaultVerifiers: [any SelfPromoVerifier] = [SelfReferenceVerifier()]

    /// Whether `span` should have its eligibility gate demoted because its
    /// transcript is the show promoting ITSELF: a curated self-promo phrase
    /// matches (ATTENTION) AND a verifier corroborates it (VERIFICATION).
    ///
    /// Returns `false` (no suppression) when:
    ///   * `bank.phrases` is empty (inert bank — defensive; the loader rejects
    ///     an empty bank, but a caller could construct one).
    ///   * The span has zero (or negative) duration — the word slice is
    ///     undefined, so we conservatively decline.
    ///   * No transcript word overlaps the span's `[startTime, endTime]`.
    ///   * No bank phrase's normalised token sequence appears contiguously in
    ///     the span's word slice (no candidate).
    ///   * A candidate matches but NO verifier corroborates it — i.e. an
    ///     AMBIGUOUS phrase with no self-reference marker in its local window.
    ///     The lexical hit was a clue that failed verification.
    ///
    /// - Parameters:
    ///   - span: the decoded span whose gate is being decided (geometry only).
    ///   - transcriptWords: the episode word stream (whole episode is fine — it
    ///     is sliced to the span here, so a self-promo phrase ELSEWHERE in the
    ///     episode does not suppress THIS span).
    ///   - bank: the curated show-agnostic, class-tagged self-promo phrase bank.
    ///   - showIdentity: the show's own identity tokens (title / network /
    ///     handle), threaded from the podcast profile at the call site so an
    ///     AMBIGUOUS phrase can be corroborated by the show naming itself.
    ///     Defaults to `.none` (only first-person markers can corroborate).
    ///   - verifiers: the corroboration steps (default: self-reference). Injected
    ///     for tests and for the extensibility seam.
    static func shouldSuppress(
        span: DecodedSpan,
        transcriptWords: [LexicalWord],
        bank: SelfPromoBank,
        showIdentity: SelfPromoShowIdentity = .none,
        verifiers: [any SelfPromoVerifier] = PromoSuppressor.defaultVerifiers
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

        let context = SelfPromoContext(spanTokens: spanTokens, showIdentity: showIdentity)

        // ATTENTION: walk every contiguous match of every bank phrase. Each is a
        // candidate ("look here"), NEVER a verdict on its own. A phrase can match
        // at several positions with different local windows, so all positions are
        // considered — the demotion fires on the FIRST corroborated candidate.
        for phrase in bank.phrases {
            let n = phrase.tokens.count
            guard n >= 1, spanTokens.count >= n else { continue }
            let lastStart = spanTokens.count - n
            var i = 0
            while i <= lastStart {
                if matches(spanTokens, phrase.tokens, at: i) {
                    let candidate = SelfPromoCandidate(phrase: phrase, matchRange: i..<(i + n))
                    // VERIFICATION: a bare lexical match demotes ONLY if some
                    // verifier corroborates it (OR composition — the seam).
                    if verifiers.contains(where: { $0.corroborates(candidate, in: context) }) {
                        return true
                    }
                }
                i += 1
            }
        }
        return false
    }

    // MARK: - Private

    /// Whether `needle` matches `haystack` exactly, token-for-token, starting at
    /// `start`. Same per-token compare `LexicalAnchorRefiner`'s `edgeMatches`
    /// uses; the caller has already bounds-checked `start + needle.count`.
    private static func matches(_ haystack: [String], _ needle: [String], at start: Int) -> Bool {
        var k = 0
        while k < needle.count {
            if haystack[start + k] != needle[k] { return false }
            k += 1
        }
        return true
    }
}
