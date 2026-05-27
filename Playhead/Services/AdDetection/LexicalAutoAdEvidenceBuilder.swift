// LexicalAutoAdEvidenceBuilder.swift
// playhead-xsdz.1: High-precision lexical auto-ad rule (keystone of epic
// playhead-xsdz, the multi-signal local ad scorer).
//
// Why this exists
// ---------------
// The on-device Foundation Model labeler reads blatant ad copy as `content`
// (~21% ad-region recall on the dogfood corpus), yet `LexicalScanner` already
// detects `sponsor` / `promoCode` / `urlCTA` hits over ~92% of golden ad
// spans. The existing `.lexical` ledger channel cannot convert that coverage
// into a skip: a `LexicalCandidate.confidence` of even 0.95 caps at
// `lexicalCap = 0.20`, far below the `0.80` standard auto-skip threshold. So
// blatant lexical ad signals NEVER drive an auto-skip on their own today.
//
// This builder closes that gap with a CONSERVATIVE, deterministic rule: when
// a tight time span contains a strong co-occurrence of ad-copy signals — a
// sponsor disclosure PLUS a promo code and/or a URL CTA within a short
// window — it emits a single high-weight `.lexicalAutoAd` ledger entry. That
// entry rides its own evidence kind + cap (`FusionWeightConfig.lexicalAutoAdCap`,
// 0.55) and gates `PromotionTrack.lexicalAutoAdQualified` (auto-skip floor
// `lexicalAutoAdQualifiedThreshold`, 0.50) so a confirmed combo can clear the
// auto-skip gate — which the structurally-capped `.lexical` family never can.
//
// Precision is paramount (a false-positive ad means real content is wrongly
// skipped), so:
//   • The trigger requires a STRONG co-occurrence (sponsor + promo/URL), not
//     a single phrase. A lone "brought to you by" or a bare URL does NOT fire.
//   • Metadata-origin hits (`isMetadataOrigin`) are SUPPLEMENTARY — they can
//     enrich a combo but cannot, on their own, be the sponsor or the
//     promo/URL leg. This mirrors `MetadataLexiconInjector`'s 2-hit rule.
//   • Negative-evidence guardrails suppress the combo when the brand is
//     discussed non-commercially:
//       – a show-owned-domain negative-pattern hit (e.g. the show plugging
//         its OWN site) — a HARD, always-consulted suppressor; and
//       – a news/review/editorial cue (e.g. "according to", "lawsuit",
//         "critics say") found in the matched text of a nearby hit.
//     SCOPE LIMIT (be honest about reach): this builder operates on the
//     extracted `[LexicalHit]` stream, NOT the surrounding transcript, so
//     the editorial-cue guardrail can only see cue phrases that land INSIDE
//     a hit's own `matchedText` (e.g. a multi-word show-sponsor-lexicon or
//     metadata phrase). It does NOT scan the prose between/around hits, so
//     an editorial frame in ordinary narration near the combo is not caught
//     here. That is acceptable today because PRECISION IS CARRIED BY THE
//     STRONG CO-OCCURRENCE BAR: editorial brand-talk almost never co-occurs
//     with a "brought to you by"/"sponsored by"/"supported by" sponsor
//     disclosure within `cooccurrenceWindow`. Whole-transcript editorial
//     suppression (threading chunk text into this seam) is a deliberate
//     follow-up, not a silent gap — see the corpus eval (12 episodes, 0
//     content false-positives observed: tech/design shows that mention
//     brand URLs but lack a sponsor disclosure never fire).
//
// Mirrors the deterministic-evidence-emitting pattern of
// `ChapterMetadataEvidenceBuilder`: a small, pure, `Sendable` value type that
// projects an in-memory signal into `[EvidenceLedgerEntry]`. No I/O, no async,
// no per-show state.

import Foundation
import OSLog

// MARK: - LexicalAutoAdEvidenceBuilder

struct LexicalAutoAdEvidenceBuilder: Sendable {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "LexicalAutoAdEvidenceBuilder"
    )

    // MARK: - Tunables (precision-first)

    /// Configuration knobs for the auto-ad rule. Defaults are deliberately
    /// conservative (precision over recall). Exposed so tests can probe the
    /// window / weight without reaching into the implementation, and so a
    /// future calibration bead can retune without editing this file.
    struct Config: Sendable, Equatable {
        /// Maximum gap (seconds) between the sponsor leg and the promo/URL
        /// leg of the co-occurrence. A real sponsor read places the
        /// disclosure ("brought to you by X") and the call-to-action
        /// ("use code…", "visit X.com") within a few sentences of each
        /// other; 25 s comfortably covers a host read while excluding a
        /// disclosure in one segment and an unrelated URL a minute later.
        let cooccurrenceWindow: TimeInterval

        /// Radius (seconds) around the combo within which a negative-context
        /// cue (news / review / editorial phrase) suppresses the rule. Wider
        /// than `cooccurrenceWindow` because an editorial framing ("in the
        /// news today, Company X is facing a lawsuit … by the way they also
        /// have a promo code") can lead the commercial-looking tail.
        let negativeContextRadius: TimeInterval

        /// Emitted weight for a fired combo. Chosen so a single confirmed
        /// combo, after the fusion clamp to `lexicalAutoAdCap` (0.55) and the
        /// identity calibration, lands a `proposalConfidence` at/above the
        /// `lexicalAutoAdQualifiedThreshold` (0.50) — i.e. the rule can skip
        /// on its own. It does NOT saturate to 1.0: corroborating signals
        /// (acoustic break, catalog match, FM) still add mass on top, and the
        /// honest score stays interpretable.
        let firedWeight: Double

        static let `default` = Config(
            cooccurrenceWindow: 25.0,
            negativeContextRadius: 30.0,
            firedWeight: 0.55
        )

        init(
            cooccurrenceWindow: TimeInterval = 25.0,
            negativeContextRadius: TimeInterval = 30.0,
            firedWeight: Double = 0.55
        ) {
            self.cooccurrenceWindow = cooccurrenceWindow
            self.negativeContextRadius = negativeContextRadius
            self.firedWeight = firedWeight
        }
    }

    private let config: Config

    /// Compiled news / review / editorial cue patterns. A match near a combo
    /// indicates the brand is being DISCUSSED (non-commercially), not SOLD,
    /// so the auto-ad rule suppresses. Compiled once per builder instance —
    /// builders are constructed per-backfill, not per-span, so this is cheap.
    private let negativeContextPatterns: [NSRegularExpression]

    init(config: Config = .default) {
        self.config = config
        self.negativeContextPatterns = Self.compileNegativeContextPatterns()
    }

    // MARK: - Public API

    /// Build the high-precision lexical-auto-ad ledger entry for a span, or
    /// `[]` when the rule does not fire.
    ///
    /// - Parameters:
    ///   - hits: All `LexicalHit`s for the asset (e.g. from
    ///     `LexicalScanner.collectHits`). Only hits that overlap the span's
    ///     interval are considered.
    ///   - span: The decoded span to score evidence against.
    /// - Returns: A single `.lexicalAutoAd` `EvidenceLedgerEntry` when a
    ///   strong co-occurrence fires and no negative-evidence guardrail
    ///   suppresses it; otherwise `[]`.
    func buildEntries(
        hits: [LexicalHit],
        for span: DecodedSpan
    ) -> [EvidenceLedgerEntry] {
        guard !hits.isEmpty else { return [] }

        // Restrict to hits overlapping this span's interval. A hit overlaps
        // when its [startTime, endTime] intersects [span.startTime,
        // span.endTime]. Lexical hits are near-instantaneous (a phrase), so
        // this is effectively "the hit's timestamp falls inside the span,"
        // but we use interval overlap to be robust to interpolation slop.
        let spanHits = hits.filter { hit in
            hit.startTime <= span.endTime && hit.endTime >= span.startTime
        }
        guard !spanHits.isEmpty else { return [] }

        // Negative pattern hits (show-owned domains) are score reducers, not
        // promotion evidence — and their presence is a hard suppressor for
        // the auto-ad rule. The rule's whole premise is "a third-party brand
        // is being sold here"; the show plugging its OWN domain is exactly
        // the case we must NOT auto-skip. Mirrors `LexicalScanner.mergeHits`
        // treating negative hits as a separate stream.
        if spanHits.contains(where: { $0.isNegativePattern }) {
            Self.logger.debug(
                "[xsdz.1] span=\(span.id, privacy: .public) suppressed: show-owned-domain negative pattern present"
            )
            return []
        }

        // Promotion legs: NON-metadata-origin, non-negative hits only.
        // Metadata-origin hits are supplementary (the 2-hit rule from
        // `MetadataLexiconInjector`): they cannot be the trigger on their own.
        let promotionHits = spanHits.filter {
            !$0.isNegativePattern && !$0.isMetadataOrigin
        }
        guard !promotionHits.isEmpty else { return [] }

        // The strong co-occurrence: a sponsor leg AND a promo/URL leg within
        // `cooccurrenceWindow` of each other.
        guard let combo = strongestCombo(in: promotionHits) else {
            return []
        }

        // Negative-context guardrail: a news / review / editorial cue
        // INSIDE the matched text of a nearby hit means the brand is being
        // discussed, not sold. Scope note: this only inspects each hit's own
        // `matchedText` (the regex substring), not the surrounding prose —
        // see the file header's SCOPE LIMIT. It reliably catches multi-word
        // show-sponsor-lexicon / metadata phrases that themselves carry an
        // editorial frame; whole-narration editorial context is a follow-up.
        // The strong co-occurrence bar is the primary precision control.
        let comboCenter = (combo.sponsor.startTime + combo.cta.startTime) / 2.0
        if hasNegativeContext(near: comboCenter, hits: spanHits) {
            Self.logger.debug(
                "[xsdz.1] span=\(span.id, privacy: .public) suppressed: non-commercial (news/review) context near combo"
            )
            return []
        }

        let entry = EvidenceLedgerEntry(
            source: .lexicalAutoAd,
            weight: config.firedWeight,
            // `.lexical(matchedCategories:)` is the closest existing detail
            // variant — record exactly which categories formed the combo so
            // diagnostics / NARL replay can see WHY the rule fired. A bespoke
            // detail case would be churn for no consumer today.
            detail: .lexical(matchedCategories: [
                combo.sponsor.category.rawValue,
                combo.cta.category.rawValue,
            ])
        )

        Self.logger.debug(
            "[xsdz.1] span=\(span.id, privacy: .public) FIRED: sponsor='\(combo.sponsor.matchedText, privacy: .public)' + \(combo.cta.category.rawValue, privacy: .public)='\(combo.cta.matchedText, privacy: .public)' weight=\(self.config.firedWeight, privacy: .public)"
        )
        return [entry]
    }

    // MARK: - Co-occurrence detection

    /// One strong co-occurrence: a sponsor disclosure plus a promo-code or
    /// URL-CTA hit within the configured window.
    private struct Combo {
        let sponsor: LexicalHit
        let cta: LexicalHit
    }

    /// Find the strongest sponsor + (promoCode|urlCTA) co-occurrence within
    /// `cooccurrenceWindow`. Returns `nil` when no qualifying pair exists.
    ///
    /// "Strongest" = the pair whose legs are closest together in time (the
    /// most tightly clustered, hence the most ad-like). Ties are broken by
    /// preferring a `.promoCode` CTA leg (the single most ad-specific signal)
    /// over a `.urlCTA` leg.
    private func strongestCombo(in hits: [LexicalHit]) -> Combo? {
        let sponsorHits = hits.filter { $0.category == .sponsor }
        guard !sponsorHits.isEmpty else { return nil }

        let ctaHits = hits.filter {
            $0.category == .promoCode || $0.category == .urlCTA
        }
        guard !ctaHits.isEmpty else { return nil }

        var best: Combo?
        var bestGap = Double.greatestFiniteMagnitude
        for sponsor in sponsorHits {
            for cta in ctaHits {
                let gap = abs(cta.startTime - sponsor.startTime)
                guard gap <= config.cooccurrenceWindow else { continue }

                let isCloser = gap < bestGap
                let isTieFavoringPromo = gap == bestGap
                    && cta.category == .promoCode
                    && best?.cta.category != .promoCode
                if isCloser || isTieFavoringPromo {
                    bestGap = gap
                    best = Combo(sponsor: sponsor, cta: cta)
                }
            }
        }
        return best
    }

    // MARK: - Negative-evidence guardrail

    /// `true` when a news / review / editorial cue phrase appears in any hit's
    /// matched text within `negativeContextRadius` of `center`. We re-scan the
    /// matched text of nearby hits (cheap — the strings are short) rather than
    /// re-reading the transcript, because the auto-ad rule operates on the
    /// already-extracted hit stream.
    ///
    /// Note: built-in lexical hits do not carry editorial-cue categories, so
    /// in practice we test the matched text of EVERY nearby hit (sponsor /
    /// promo / URL alike). This is intentional — a sponsor-looking phrase that
    /// itself contains an editorial frame ("a company critics say…") should
    /// suppress.
    ///
    /// Reach caveat: built-in regex `matchedText` is a short fixed substring
    /// ("brought to you by", "use code X", "acme.com") that will essentially
    /// never embed an editorial cue, so on a purely built-in hit stream this
    /// guardrail rarely engages. It DOES engage for multi-word per-show
    /// sponsor-lexicon / metadata phrases whose matched text spans an
    /// editorial frame. Surrounding-narration editorial suppression is a
    /// deliberate follow-up (would require threading transcript text into
    /// this seam); the strong co-occurrence bar is the primary precision
    /// control today.
    private func hasNegativeContext(near center: Double, hits: [LexicalHit]) -> Bool {
        for hit in hits {
            let hitCenter = (hit.startTime + hit.endTime) / 2.0
            guard abs(hitCenter - center) <= config.negativeContextRadius else { continue }
            if matchesNegativeContext(hit.matchedText) { return true }
        }
        return false
    }

    private func matchesNegativeContext(_ text: String) -> Bool {
        guard !text.isEmpty else { return false }
        let ns = text as NSString
        let range = NSRange(location: 0, length: ns.length)
        for pattern in negativeContextPatterns where
            pattern.firstMatch(in: text, range: range) != nil {
            return true
        }
        return false
    }

    /// Compile the news / review / editorial cue patterns. A match near a
    /// combo flips the rule OFF — the brand is being talked ABOUT, not sold.
    /// Kept deliberately small and high-precision; expanding this set trades
    /// recall for precision and should be done with corpus evidence.
    private static func compileNegativeContextPatterns() -> [NSRegularExpression] {
        let patterns = [
            #"\baccording to\b"#,
            #"\breportedly\b"#,
            #"\bcritics say\b"#,
            #"\bin the news\b"#,
            #"\blawsuit\b"#,
            #"\binvestigation\b"#,
            #"\bunder fire\b"#,
            #"\baccused of\b"#,
            #"\bstudy found\b"#,
            #"\bresearchers\b"#,
        ]
        return patterns.compactMap {
            try? NSRegularExpression(pattern: $0, options: [.caseInsensitive])
        }
    }
}
